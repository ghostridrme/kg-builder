//! Materialize learned rules through the existing reference producer.
//! A learned rule is a [`ReferenceMapping`]; an active rule is
//! applied by feeding its mapping into the run's frozen `reference_guidance`, so
//! the deterministic reference tier produces its edges — there is no second
//! matcher and no new producer, which is exactly why this needs no separate
//! producer-enabling gate. The active revisions read for a run are frozen into a
//! [`RuleFreeze`] recorded in the run manifest; learning cannot change the rules
//! mid-run. Lifecycle callers re-evaluate affected owners through the same
//! guarded relationship path when a rule is activated or revoked.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::errors::BackendError;
use crate::runtime::extraction::{ExtractionSettings, ReferenceMapping};
use crate::traits::rule_store::RuleStore;

/// The active learned rules frozen for one ingestion run, recorded in the run
/// manifest so the exact revisions applied are auditable and reproducible.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleFreeze {
    pub source: String,
    /// `(rule id, revision)` for every active rule applied this run.
    pub rules: Vec<(Uuid, u64)>,
    pub mappings: Vec<ReferenceMapping>,
}

/// Load the active rules for a producer and return their mappings plus the
/// freeze record of the exact revisions read. Only `Active` rules are returned
/// (the store's `list_active` already filters), so proposed/uncertain/stale/
/// revoked rules never materialize.
pub async fn active_materialization(
    store: &dyn RuleStore,
    org_id: &str,
    source: &str,
) -> Result<(Vec<ReferenceMapping>, RuleFreeze), BackendError> {
    let active = store.list_active(org_id, source).await?;
    let mut mappings = Vec::with_capacity(active.len());
    let mut freeze = RuleFreeze {
        source: source.to_owned(),
        rules: Vec::with_capacity(active.len()),
        mappings: Vec::with_capacity(active.len()),
    };
    for rule in active {
        freeze.rules.push((rule.id, rule.revision));
        let mut mapping = rule.mapping;
        mapping.source_namespace = rule.namespace;
        freeze.mappings.push(mapping.clone());
        mappings.push(mapping);
    }
    Ok((mappings, freeze))
}

/// Merge learned mappings into a run's reference guidance for one source. An
/// authored mapping always wins: a learned mapping that conflicts with an
/// existing one for the same source path is dropped (never overrides explicit
/// guidance), and exact duplicates are not added twice. Returns how many learned
/// mappings were actually applied.
pub fn merge_into_guidance(
    settings: &mut ExtractionSettings,
    source: &str,
    learned: Vec<ReferenceMapping>,
) -> usize {
    let existing = settings
        .reference_guidance
        .entry(source.to_owned())
        .or_default();
    let mut applied = 0;
    for mapping in learned {
        let conflicts = existing.iter().any(|m| m.conflicts_with(&mapping));
        let duplicate = existing.contains(&mapping);
        if conflicts || duplicate {
            continue;
        }
        existing.push(mapping);
        applied += 1;
    }
    applied
}

/// Load every active rule for an org and merge them into a run's extraction
/// settings, grouped by source, returning the per-source [`RuleFreeze`] of the
/// exact revisions applied. Authored guidance always wins over a conflicting
/// learned rule. Call this once at run setup so the rules are frozen for the run.
pub async fn apply_active_rules(
    store: &dyn RuleStore,
    org_id: &str,
    settings: &mut ExtractionSettings,
) -> Result<Vec<RuleFreeze>, BackendError> {
    let active = store.list_all_active(org_id).await?;
    materialize_rules(active, settings)
}

pub fn materialize_rules(
    active: Vec<crate::traits::LearnedRule>,
    settings: &mut ExtractionSettings,
) -> Result<Vec<RuleFreeze>, BackendError> {
    let authored = settings.reference_guidance.clone();
    let mut freezes: BTreeMap<String, RuleFreeze> = BTreeMap::new();
    for rule in active {
        let mut mapping = rule.mapping;
        mapping.source_namespace = rule.namespace;
        let guidance = settings
            .reference_guidance
            .entry(rule.source.clone())
            .or_default();
        if guidance.contains(&mapping) {
            continue;
        }
        if authored.get(&rule.source).is_some_and(|mappings| {
            mappings
                .iter()
                .any(|current| current.conflicts_with(&mapping))
        }) {
            continue;
        }
        if guidance
            .iter()
            .any(|current| current.conflicts_with(&mapping))
        {
            return Err(BackendError::Query(format!(
                "active learned rules conflict for source {} and path {}",
                rule.source, mapping.reference_path
            )));
        }
        guidance.push(mapping.clone());
        let freeze = freezes
            .entry(rule.source.clone())
            .or_insert_with(|| RuleFreeze {
                source: rule.source,
                rules: Vec::new(),
                mappings: Vec::new(),
            });
        freeze.rules.push((rule.id, rule.revision));
        freeze.mappings.push(mapping);
        if freezes.len() > crate::traits::graph_commit::MAX_RULE_FREEZE_SOURCES {
            return Err(BackendError::Query(
                "active learned rules exceed the run source budget".into(),
            ));
        }
    }
    Ok(freezes.into_values().collect())
}

pub fn apply_frozen_rules(
    freezes: &[RuleFreeze],
    settings: &mut ExtractionSettings,
) -> Result<(), BackendError> {
    for freeze in freezes {
        if freeze.source.trim().is_empty() || freeze.rules.len() != freeze.mappings.len() {
            return Err(BackendError::Deserialization(
                "invalid frozen learned-rule manifest".into(),
            ));
        }
        let applied = merge_into_guidance(settings, &freeze.source, freeze.mappings.clone());
        if applied != freeze.mappings.len() {
            return Err(BackendError::Deserialization(
                "frozen learned rules conflict with run guidance".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::InMemoryRuleStore;
    use crate::traits::rule_store::{
        LearnedRule, RuleOrigin, RuleStatus, RuleValidation, MIN_PROMOTION_NEGATIVES,
        MIN_PROMOTION_POSITIVES, MIN_PROMOTION_PRECISION, MIN_PROMOTION_RECALL,
    };

    fn mapping(path: &str, target: &str) -> ReferenceMapping {
        ReferenceMapping {
            source_namespace: None,
            source_entity_type: "CmdbChange".into(),
            reference_path: path.into(),
            context_paths: Default::default(),
            target_type: target.into(),
            target_key_group: vec!["id".into()],
            shape: Default::default(),
            direction: Default::default(),
            relationship_name: format!("REFERENCES_{}", target.to_uppercase()),
            qualifiers: None,
            cardinality: Default::default(),
            case_insensitive_types: Vec::new(),
        }
    }

    fn active(id: u128, path: &str, target: &str) -> LearnedRule {
        let gate = RuleValidation {
            positives: MIN_PROMOTION_POSITIVES,
            negatives: MIN_PROMOTION_NEGATIVES,
            precision: MIN_PROMOTION_PRECISION,
            recall: MIN_PROMOTION_RECALL,
            conflicting_failures: 0,
            independent: true,
        };
        LearnedRule {
            id: Uuid::from_u128(id),
            revision: 2,
            org_id: "org".into(),
            source: "cmdb".into(),
            namespace: None,
            schema_fingerprint: "fp".into(),
            mapping: mapping(path, target),
            owner_slot: format!("CmdbChange.{path}"),
            origin: RuleOrigin::System {
                component: "rule-learning".into(),
            },
            evidence_refs: vec![],
            validation: Some(gate),
            decisions: vec![],
            status: RuleStatus::Active,
            effective_from: Some(chrono::Utc::now()),
            revoked_at: None,
        }
    }

    #[tokio::test]
    async fn only_active_rules_materialize_and_are_frozen() {
        let store = InMemoryRuleStore::new();
        store.seed(active(1, "owning_group", "CmdbGroup"));
        let mut stale = active(2, "assigned", "CmdbPerson");
        stale.status = RuleStatus::Stale;
        store.seed(stale);

        let (mappings, freeze) = active_materialization(&store, "org", "cmdb").await.unwrap();
        assert_eq!(mappings.len(), 1, "the stale rule does not materialize");
        assert_eq!(freeze.rules, vec![(Uuid::from_u128(1), 2)]);
        assert_eq!(mappings[0].reference_path, "owning_group");
    }

    #[test]
    fn authored_guidance_wins_over_a_conflicting_learned_rule() {
        let mut settings = ExtractionSettings::default();
        // Authored: owning_group -> CmdbGroup.
        settings
            .reference_guidance
            .insert("cmdb".into(), vec![mapping("owning_group", "CmdbGroup")]);

        // A learned rule that disagrees on the same path is dropped; a new
        // non-conflicting one is applied.
        let applied = merge_into_guidance(
            &mut settings,
            "cmdb",
            vec![
                mapping("owning_group", "CmdbPerson"),  // conflicts -> dropped
                mapping("linked_change", "CmdbChange"), // new -> applied
            ],
        );
        assert_eq!(applied, 1);
        let guidance = &settings.reference_guidance["cmdb"];
        assert_eq!(guidance.len(), 2);
        // The authored owning_group mapping is unchanged (still CmdbGroup).
        let owning = guidance
            .iter()
            .find(|m| m.reference_path == "owning_group")
            .unwrap();
        assert_eq!(owning.target_type, "CmdbGroup");
    }

    #[tokio::test]
    async fn apply_active_rules_merges_by_source_and_freezes_revisions() {
        let store = InMemoryRuleStore::new();
        store.seed(active(1, "owning_group", "CmdbGroup"));
        let mut other = active(2, "linked_change", "CmdbChange");
        other.source = "aws".into();
        store.seed(other);
        // A stale rule must not materialize.
        let mut stale = active(3, "assigned", "CmdbPerson");
        stale.status = RuleStatus::Stale;
        store.seed(stale);

        let mut settings = ExtractionSettings::default();
        let freezes = apply_active_rules(&store, "org", &mut settings)
            .await
            .unwrap();

        assert_eq!(freezes.len(), 2, "two sources with active rules");
        assert_eq!(settings.reference_guidance["cmdb"].len(), 1);
        assert_eq!(settings.reference_guidance["aws"].len(), 1);
        assert!(
            !settings.reference_guidance.contains_key("cmdb")
                || settings.reference_guidance["cmdb"]
                    .iter()
                    .all(|m| m.reference_path != "assigned"),
            "the stale rule never materializes"
        );
    }

    #[tokio::test]
    async fn a_revoked_rule_is_excluded_from_materialization() {
        use crate::traits::rule_store::{RuleDecision, RuleStore, RuleTransition};
        let store = InMemoryRuleStore::new();
        store.seed(active(1, "owning_group", "CmdbGroup"));
        // Two active rules materialize.
        let mut settings = ExtractionSettings::default();
        let freezes = apply_active_rules(&store, "org", &mut settings)
            .await
            .unwrap();
        assert_eq!(freezes.iter().map(|f| f.rules.len()).sum::<usize>(), 1);

        // Revoke it; revocation stops future application.
        store
            .transition(
                "org",
                Uuid::from_u128(1),
                2,
                RuleTransition {
                    to: RuleStatus::Revoked,
                    decision: RuleDecision {
                        origin: RuleOrigin::Human {
                            actor: "sre".into(),
                        },
                        at: chrono::Utc::now(),
                        note: None,
                    },
                    validation: None,
                },
            )
            .await
            .unwrap();

        let mut after = ExtractionSettings::default();
        let freezes = apply_active_rules(&store, "org", &mut after).await.unwrap();
        assert!(freezes.is_empty(), "a revoked rule no longer materializes");
        assert!(after.reference_guidance.is_empty());
    }

    #[test]
    fn duplicate_learned_mappings_are_not_added_twice() {
        let mut settings = ExtractionSettings::default();
        let m = mapping("owning_group", "CmdbGroup");
        assert_eq!(
            merge_into_guidance(&mut settings, "cmdb", vec![m.clone()]),
            1
        );
        assert_eq!(merge_into_guidance(&mut settings, "cmdb", vec![m]), 0);
        assert_eq!(settings.reference_guidance["cmdb"].len(), 1);
    }
}
