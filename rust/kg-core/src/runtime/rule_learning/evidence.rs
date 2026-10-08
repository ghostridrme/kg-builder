//! Evidence collection for learned reference rules.
//!
//! An observed reference example says: a source entity of some type carried a
//! value at some path, and that value did (positive) or did not (negative)
//! complete a target type's complete key group. Aggregation groups examples
//! into candidate mapping patterns, counting *distinct* examples so repeated
//! observations of the same source/value pair cannot inflate support. Frequency
//! is context here, never a decision: a strong count proposes a rule, it never
//! activates one, and a rare pattern is still resolvable as an individual edge.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Whether the observed value completed the candidate target's key group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExampleOutcome {
    /// The value completed exactly this target type's complete key group.
    Positive,
    /// The value matched no target of this type (or completed a different
    /// type's key group), so this pattern must not link it.
    Negative,
}

/// One observed example a candidate mapping pattern would explain. The
/// `source_chain_id` plus `value_token` identify the example so duplicate
/// observations of the same pair are counted once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceExample {
    /// Proven paths for complete-key correspondence; absent evidence cannot
    /// teach composite scope inheritance.
    pub component_paths: Option<BTreeMap<String, String>>,
    pub source_chain_id: Uuid,
    /// Exact source version whose payload produced this example.
    #[serde(default)]
    pub source_version_uuid: Option<Uuid>,
    pub source_entity_type: String,
    /// Namespace of the observing source. Rules learned from a scoped source
    /// never aggregate with an otherwise identical pattern in another namespace.
    #[serde(default)]
    pub source_namespace: String,
    pub reference_path: String,
    /// Typed value token as produced by the reference tier, e.g. `s:atlas`.
    pub value_token: String,
    /// Complete typed occurrence used by the matcher. Composite references
    /// retain every component instead of validating one convenient value.
    #[serde(default)]
    pub reference_tokens: Vec<String>,
    pub target_type: String,
    pub target_key_group: Vec<String>,
    pub outcome: ExampleOutcome,
}

impl ReferenceExample {
    /// The pattern this example is evidence for.
    pub fn pattern(&self) -> PatternKey {
        PatternKey {
            component_paths: self.component_paths.clone(),
            source_entity_type: self.source_entity_type.clone(),
            source_namespace: self.source_namespace.clone(),
            reference_path: self.reference_path.clone(),
            target_type: self.target_type.clone(),
            target_key_group: self.target_key_group.clone(),
        }
    }

    /// Distinct-example identity within a pattern: one source object observed
    /// carrying one value. Two rows with the same identity are one example.
    fn identity(&self) -> (Uuid, String) {
        (self.source_chain_id, self.value_token.clone())
    }
}

/// The dimensions that make two examples evidence for the *same* rule.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PatternKey {
    pub component_paths: Option<BTreeMap<String, String>>,
    pub source_entity_type: String,
    #[serde(default)]
    pub source_namespace: String,
    pub reference_path: String,
    pub target_type: String,
    pub target_key_group: Vec<String>,
}

impl PatternKey {
    /// The source field whose observation owns any learned relationship.
    pub fn owner_slot(&self) -> String {
        format!("{}.{}", self.source_entity_type, self.reference_path)
    }
}

/// Aggregated support for one candidate pattern, over distinct examples.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceAggregate {
    pub key: PatternKey,
    /// Distinct positive examples.
    pub positives: usize,
    /// Distinct negative examples.
    pub negatives: usize,
    /// Distinct source values seen positive (a rule confirmed by one value
    /// repeated across sources is weaker than one seen across many values).
    pub distinct_values: usize,
}

/// Group examples into candidate patterns, counting distinct examples only.
/// Order is deterministic (by pattern key) so proposals are reproducible.
pub fn aggregate(examples: impl IntoIterator<Item = ReferenceExample>) -> Vec<EvidenceAggregate> {
    struct Acc {
        positives: BTreeSet<(Uuid, String)>,
        negatives: BTreeSet<(Uuid, String)>,
        positive_values: BTreeSet<String>,
    }
    let mut by_key: BTreeMap<PatternKey, Acc> = BTreeMap::new();
    for example in examples {
        let acc = by_key.entry(example.pattern()).or_insert_with(|| Acc {
            positives: BTreeSet::new(),
            negatives: BTreeSet::new(),
            positive_values: BTreeSet::new(),
        });
        match example.outcome {
            ExampleOutcome::Positive => {
                acc.positives.insert(example.identity());
                acc.positive_values.insert(example.value_token.clone());
            }
            ExampleOutcome::Negative => {
                acc.negatives.insert(example.identity());
            }
        }
    }
    by_key
        .into_iter()
        .map(|(key, acc)| EvidenceAggregate {
            key,
            positives: acc.positives.len(),
            negatives: acc.negatives.len(),
            distinct_values: acc.positive_values.len(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(chain: u128, value: &str, outcome: ExampleOutcome) -> ReferenceExample {
        ReferenceExample {
            component_paths: None,
            source_chain_id: Uuid::from_u128(chain),
            source_version_uuid: None,
            source_entity_type: "CmdbChange".into(),
            source_namespace: "prod".into(),
            reference_path: "owning_group".into(),
            value_token: value.into(),
            reference_tokens: Vec::new(),
            target_type: "CmdbGroup".into(),
            target_key_group: vec!["group_id".into()],
            outcome,
        }
    }

    #[test]
    fn duplicate_observations_of_one_pair_count_once() {
        let aggs = aggregate(vec![
            ex(1, "s:atlas", ExampleOutcome::Positive),
            ex(1, "s:atlas", ExampleOutcome::Positive), // duplicate
            ex(2, "s:nova", ExampleOutcome::Positive),
            ex(3, "s:orion", ExampleOutcome::Negative),
        ]);
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].positives, 2);
        assert_eq!(aggs[0].negatives, 1);
        assert_eq!(aggs[0].distinct_values, 2);
        assert_eq!(aggs[0].key.owner_slot(), "CmdbChange.owning_group");
    }

    #[test]
    fn distinct_patterns_are_separated_and_ordered() {
        let mut other = ex(9, "s:x", ExampleOutcome::Positive);
        other.target_type = "CmdbPerson".into();
        other.target_key_group = vec!["user_id".into()];
        let aggs = aggregate(vec![ex(1, "s:atlas", ExampleOutcome::Positive), other]);
        assert_eq!(aggs.len(), 2);
        // Deterministic order by key: CmdbGroup target sorts before CmdbPerson.
        assert_eq!(aggs[0].key.target_type, "CmdbGroup");
        assert_eq!(aggs[1].key.target_type, "CmdbPerson");
    }
}
