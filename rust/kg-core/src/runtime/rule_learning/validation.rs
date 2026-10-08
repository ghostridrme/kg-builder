//! Held-out validation of a learned-rule proposal.
//!
//! Promotion is never the proposing model's own confidence. A proposal is
//! measured against labeled cases it did not train on: each case carries ground
//! truth (`should_link`) and the rule's deterministic prediction is recomputed
//! from the case. Precision, recall and conflicting-case failures come out of
//! that confusion matrix and feed [`RuleValidation::meets_promotion_gate`]. The
//! train/holdout split is a deterministic hash of example identity so the same
//! evidence always validates the same way.
use crate::errors::BackendError;
use crate::runtime::extraction::ReferenceMapping;
use crate::runtime::rule_learning::evidence::ReferenceExample;
use crate::traits::rule_store::RuleValidation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A held-out example with its ground-truth label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabeledCase {
    pub example: ReferenceExample,
    /// Ground truth: should this rule link this case to its target type?
    pub should_link: bool,
    /// Target selected by independent adjudication when a link is expected.
    pub expected_target_chain_id: Option<Uuid>,
    /// Durable evidence or review identity that established the label.
    pub adjudication_ref: String,
    /// The model that established the label, when a model did (a persisted
    /// reference decision's `model_served`). Validation is independent only
    /// when the learner's reviewer is not this model.
    #[serde(default)]
    pub label_model: Option<String>,
}

/// Matcher input for one held-out case. Ground-truth labels and expected
/// targets are intentionally absent so the predictor cannot read its answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationExample {
    pub source_chain_id: Uuid,
    pub source_version_uuid: Option<Uuid>,
    pub source_entity_type: String,
    pub source_namespace: String,
    pub reference_path: String,
    pub value_token: String,
    pub reference_tokens: Vec<String>,
    pub target_type: String,
    pub target_key_group: Vec<String>,
}

impl From<&ReferenceExample> for ValidationExample {
    fn from(example: &ReferenceExample) -> Self {
        Self {
            source_chain_id: example.source_chain_id,
            source_version_uuid: example.source_version_uuid,
            source_entity_type: example.source_entity_type.clone(),
            source_namespace: example.source_namespace.clone(),
            reference_path: example.reference_path.clone(),
            value_token: example.value_token.clone(),
            reference_tokens: example.reference_tokens.clone(),
            target_type: example.target_type.clone(),
            target_key_group: example.target_key_group.clone(),
        }
    }
}

/// Executes a proposed mapping through the production deterministic matcher.
/// Expected labels are deliberately absent from this interface.
#[async_trait]
pub trait RuleValidationExecutor: Send + Sync {
    async fn predict(
        &self,
        org_id: &str,
        source: &str,
        mapping: &ReferenceMapping,
        examples: &[ValidationExample],
    ) -> Result<Vec<Option<Uuid>>, BackendError>;
}

/// Validator for evidence sources that cannot supply independently adjudicated
/// cases. It accepts an empty holdout and refuses any labeled input so callers
/// cannot accidentally promote a rule without executing a real matcher.
pub struct NoAdjudicatedValidation;

#[async_trait]
impl RuleValidationExecutor for NoAdjudicatedValidation {
    async fn predict(
        &self,
        _org_id: &str,
        _source: &str,
        _mapping: &ReferenceMapping,
        examples: &[ValidationExample],
    ) -> Result<Vec<Option<Uuid>>, BackendError> {
        if examples.is_empty() {
            Ok(Vec::new())
        } else {
            Err(BackendError::Query(
                "adjudicated rule cases require a production matcher".into(),
            ))
        }
    }
}

/// A deterministic train/holdout partition of examples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldOutSplit {
    pub train: Vec<ReferenceExample>,
    pub holdout: Vec<ReferenceExample>,
}

/// Stable non-cryptographic hash of an example's identity, so a given example
/// always lands on the same side of the split regardless of input order.
fn identity_hash(example: &ReferenceExample) -> u64 {
    // FNV-1a over the identity bytes; stable across runs and platforms.
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut mix = |bytes: &[u8]| {
        for b in bytes {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    };
    mix(example.source_chain_id.as_bytes());
    mix(example.value_token.as_bytes());
    hash
}

/// Partition examples into train and holdout. `holdout_permille` is the holdout
/// share in parts-per-thousand (e.g. 300 = 30%). Deterministic and order-free.
pub fn split(examples: &[ReferenceExample], holdout_permille: u16) -> HeldOutSplit {
    let permille = holdout_permille.min(1000) as u64;
    let mut train = Vec::new();
    let mut holdout = Vec::new();
    for example in examples {
        if identity_hash(example) % 1000 < permille {
            holdout.push(example.clone());
        } else {
            train.push(example.clone());
        }
    }
    HeldOutSplit { train, holdout }
}

/// Whether the rule deterministically links a case to its own target: the case
/// must sit under the rule's owner slot (same source type and path) and its
/// value must complete exactly the rule's target key group as a positive.
/// Measure a proposal against labeled held-out cases. `independent` is true only
/// when the caller performing the validation is not the model that proposed the
/// rule; it is recorded verbatim so the promotion gate can reject self-approval.
pub fn validate_against(
    _mapping: &ReferenceMapping,
    cases: &[LabeledCase],
    predictions: &[Option<Uuid>],
    independent: bool,
) -> Result<RuleValidation, BackendError> {
    if cases.len() != predictions.len()
        || cases.iter().any(|case| {
            case.adjudication_ref.trim().is_empty()
                || (case.should_link != case.expected_target_chain_id.is_some())
        })
    {
        return Err(BackendError::Query(
            "rule validation cases or predictions are invalid".into(),
        ));
    }
    let (mut tp, mut fp, mut fn_, mut positives, mut negatives) = (0usize, 0, 0, 0, 0);
    for (case, predicted_target) in cases.iter().zip(predictions) {
        if case.should_link {
            positives += 1;
        } else {
            negatives += 1;
        }
        match (case.expected_target_chain_id, *predicted_target) {
            (Some(expected), Some(actual)) if expected == actual => tp += 1,
            (Some(_), Some(_)) => {
                fp += 1;
                fn_ += 1;
            }
            (Some(_), None) => fn_ += 1,
            (None, Some(_)) => fp += 1,
            (None, None) => {}
        }
    }
    let ratio = |num: usize, den: usize| {
        if den == 0 {
            0.0
        } else {
            num as f64 / den as f64
        }
    };
    Ok(RuleValidation {
        positives,
        negatives,
        precision: ratio(tp, tp + fp),
        recall: ratio(tp, tp + fn_),
        // Any case the rule links that ground truth says it must not is a
        // conflicting failure; the gate requires zero.
        conflicting_failures: fp,
        independent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::rule_learning::evidence::{ExampleOutcome, ReferenceExample};
    use uuid::Uuid;

    fn predictions(cases: &[LabeledCase]) -> Vec<Option<Uuid>> {
        cases
            .iter()
            .map(|case| {
                (case.example.target_type == "CmdbGroup")
                    .then(|| Uuid::from_u128(case.example.source_chain_id.as_u128() + 10_000))
            })
            .collect()
    }

    fn mapping() -> ReferenceMapping {
        ReferenceMapping {
            source_namespace: None,
            source_entity_type: "CmdbChange".into(),
            reference_path: "owning_group".into(),
            context_paths: Default::default(),
            target_type: "CmdbGroup".into(),
            target_key_group: vec!["group_id".into()],
            shape: Default::default(),
            direction: Default::default(),
            relationship_name: "REFERENCES_CMDBGROUP".into(),
            qualifiers: None,
            cardinality: Default::default(),
            case_insensitive_types: Vec::new(),
        }
    }

    fn case(
        chain: u128,
        target_type: &str,
        outcome: ExampleOutcome,
        should_link: bool,
    ) -> LabeledCase {
        LabeledCase {
            example: ReferenceExample {
                component_paths: None,
                source_chain_id: Uuid::from_u128(chain),
                source_version_uuid: None,
                source_entity_type: "CmdbChange".into(),
                source_namespace: "prod".into(),
                reference_path: "owning_group".into(),
                value_token: format!("s:v{chain}"),
                reference_tokens: Vec::new(),
                target_type: target_type.into(),
                target_key_group: if target_type == "CmdbGroup" {
                    vec!["group_id".into()]
                } else {
                    vec!["user_id".into()]
                },
                outcome,
            },
            should_link,
            expected_target_chain_id: should_link.then(|| Uuid::from_u128(chain + 10_000)),
            adjudication_ref: format!("review-{chain}"),
            label_model: None,
        }
    }

    #[test]
    fn a_clean_rule_scores_perfectly() {
        let cases = vec![
            case(1, "CmdbGroup", ExampleOutcome::Positive, true),
            case(2, "CmdbGroup", ExampleOutcome::Positive, true),
            // A cross-type value the rule correctly does not link.
            case(3, "CmdbPerson", ExampleOutcome::Positive, false),
        ];
        let v = validate_against(&mapping(), &cases, &predictions(&cases), true).unwrap();
        assert_eq!((v.positives, v.negatives), (2, 1));
        assert_eq!(v.precision, 1.0);
        assert_eq!(v.recall, 1.0);
        assert_eq!(v.conflicting_failures, 0);
    }

    #[test]
    fn a_case_the_rule_wrongly_links_is_a_conflicting_failure() {
        // Ground truth says this owning_group value must NOT link to a group,
        // but it completes the group key, so the rule fires: a false positive.
        let cases = vec![case(5, "CmdbGroup", ExampleOutcome::Positive, false)];
        let v = validate_against(&mapping(), &cases, &predictions(&cases), true).unwrap();
        assert_eq!(v.conflicting_failures, 1);
        assert_eq!(v.precision, 0.0);
        assert!(!v.meets_promotion_gate());
    }

    #[test]
    fn split_is_deterministic_and_order_free() {
        let examples: Vec<_> = (0..200)
            .map(|i| case(i, "CmdbGroup", ExampleOutcome::Positive, true).example)
            .collect();
        let a = split(&examples, 300);
        let mut shuffled = examples.clone();
        shuffled.reverse();
        let b = split(&shuffled, 300);
        assert_eq!(a.holdout.len(), b.holdout.len());
        assert_eq!(a.train.len() + a.holdout.len(), 200);
        // Roughly the requested share, and non-trivial on both sides.
        assert!(
            a.holdout.len() > 30 && a.holdout.len() < 100,
            "got {}",
            a.holdout.len()
        );
    }

    #[tokio::test]
    async fn storage_only_validation_refuses_labeled_cases() {
        assert!(NoAdjudicatedValidation
            .predict("org", "cmdb", &mapping(), &[])
            .await
            .unwrap()
            .is_empty());
        let example = case(1, "CmdbGroup", ExampleOutcome::Positive, true).example;
        assert!(NoAdjudicatedValidation
            .predict(
                "org",
                "cmdb",
                &mapping(),
                &[ValidationExample::from(&example)]
            )
            .await
            .is_err());
    }
}
