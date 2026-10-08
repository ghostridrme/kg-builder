//! Neo4j evidence collection for learned reference rules. Reads are isolated by
//! organization and producer before limits, and every accepted row is validated
//! before it can influence a proposal.
use std::collections::BTreeSet;

use async_trait::async_trait;
use uuid::Uuid;

use crate::Neo4jGraphBackend;
use kg_core::errors::BackendError;
use kg_core::runtime::reference_resolution::{DecisionOutcome, PersistedDecision};
use kg_core::runtime::rule_learning::evidence::{ExampleOutcome, ReferenceExample};
use kg_core::runtime::rule_learning::service::{
    EvidenceBatch, LearningBounds, ReferenceEvidenceSource,
};
use kg_core::runtime::rule_learning::validation::{split, LabeledCase};
use kg_core::traits::GraphBackend;
use kg_storage_cypher::rule_evidence as cypher;
use serde_json::{Map, Value};

/// Preserve array structure while removing observation-specific positions.
fn normalize_occurrence_path(path: &str) -> String {
    let mut out = String::new();
    let mut chars = path.chars().peekable();
    while let Some(c) = chars.next() {
        out.push(c);
        if c == '\\' {
            if let Some(escaped) = chars.next() {
                out.push(escaped);
            }
        } else if c == '[' {
            while chars.peek().is_some_and(char::is_ascii_digit) {
                chars.next();
            }
        }
    }
    out
}

fn text(row: &Map<String, Value>, key: &str) -> Option<String> {
    row.get(key).and_then(Value::as_str).map(str::to_owned)
}

/// A list whose repeated members are a data error (a key group naming one
/// property twice).
fn string_list(row: &Map<String, Value>, key: &str) -> Result<Vec<String>, BackendError> {
    string_values(row, key, false)
}

/// A list whose repeated members are ordinary: the reference tokens of one
/// occurrence repeat whenever the same value sits in several source fields
/// (a volume id in a block-device mapping and in a tag). Repeats are
/// collapsed in order; they never fail the whole learning pass.
fn distinct_string_list(row: &Map<String, Value>, key: &str) -> Result<Vec<String>, BackendError> {
    string_values(row, key, true)
}

fn string_values(
    row: &Map<String, Value>,
    key: &str,
    collapse_repeats: bool,
) -> Result<Vec<String>, BackendError> {
    let values = row
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| BackendError::Deserialization(format!("rule evidence has invalid {key}")))?;
    let mut result = Vec::with_capacity(values.len());
    let mut unique = BTreeSet::new();
    for value in values {
        let value = value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                BackendError::Deserialization(format!("rule evidence has invalid {key}"))
            })?
            .to_owned();
        if !unique.insert(value.clone()) {
            if collapse_repeats {
                continue;
            }
            return Err(BackendError::Deserialization(format!(
                "rule evidence has duplicate {key}"
            )));
        }
        result.push(value);
    }
    Ok(result)
}

fn required_text(row: &Map<String, Value>, key: &str) -> Result<String, BackendError> {
    text(row, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| BackendError::Deserialization(format!("rule evidence is missing {key}")))
}

fn validate_scope(
    row: &Map<String, Value>,
    org_id: &str,
    source: &str,
    endpoint_org_keys: &[&str],
) -> Result<String, BackendError> {
    if required_text(row, "org_id")? != org_id || required_text(row, "producer_source")? != source {
        return Err(BackendError::Deserialization(
            "rule evidence escaped its requested scope".into(),
        ));
    }
    for key in endpoint_org_keys {
        if required_text(row, key)? != org_id {
            return Err(BackendError::Deserialization(
                "rule evidence escaped its requested scope".into(),
            ));
        }
    }
    required_text(row, "producer_namespace")
}

fn add_within_budget<T: serde::Serialize>(
    value: T,
    values: &mut Vec<T>,
    used: &mut usize,
    limit: usize,
) -> Result<bool, BackendError> {
    let bytes = serde_json::to_vec(&value)
        .map_err(|_| BackendError::Deserialization("invalid rule evidence".into()))?
        .len();
    let Some(next) = used.checked_add(bytes) else {
        return Ok(false);
    };
    if next > limit {
        return Ok(false);
    }
    *used = next;
    values.push(value);
    Ok(true)
}

/// The examples one persisted model decision is evidence for. An acceptance is
/// one positive example for the chosen target's type: in another type's
/// held-out set that case expects the chosen target, so a competing rule that
/// fires on it fails validation without a separate negative. A rejection is a
/// negative example for every candidate type it was offered. All examples of
/// one decision share the occurrence's identity (source chain, value token), so
/// a train/holdout split keeps them together.
pub(crate) fn decision_examples(decision: &PersistedDecision) -> Vec<ReferenceExample> {
    let audit = &decision.audit;
    let reference_path = normalize_occurrence_path(&audit.location);
    // Components are keyed by the leaf field name of each source property; the
    // occurrence's own token is the one under the location's leaf. The matcher
    // identifies a single-field occurrence by exactly that token.
    let leaf = reference_path
        .rsplit('.')
        .next()
        .unwrap_or(reference_path.as_str())
        .trim_end_matches("[]")
        .to_owned();
    let value_token = decision
        .components
        .iter()
        .find(|(path, _)| path.trim_end_matches("[]") == leaf)
        .or_else(|| decision.components.last())
        .map(|(_, token)| token.clone())
        .unwrap_or_else(|| format!("s:{}", audit.value));
    let reference_tokens = vec![value_token.clone()];
    let accepted_type = (audit.outcome == DecisionOutcome::Accepted)
        .then(|| {
            decision
                .candidates
                .iter()
                .find(|c| Some(c.chain_id) == audit.target_chain_id)
        })
        .flatten();
    let mut seen = BTreeSet::new();
    let mut examples = Vec::new();
    for candidate in &decision.candidates {
        let Some(key_group) = candidate.key_groups.first().filter(|g| !g.is_empty()) else {
            continue;
        };
        if !seen.insert(candidate.entity_type.clone()) {
            continue;
        }
        let positive = accepted_type.is_some_and(|t| t.entity_type == candidate.entity_type);
        if audit.outcome == DecisionOutcome::Accepted && !positive {
            continue;
        }
        // The accepted target's own key group names the positive.
        let key_group = match accepted_type {
            Some(target) if positive => target
                .key_groups
                .first()
                .cloned()
                .unwrap_or_else(|| key_group.clone()),
            _ => key_group.clone(),
        };
        examples.push(ReferenceExample {
            component_paths: None,
            source_chain_id: audit.source_chain_id,
            source_version_uuid: Some(audit.source_version_uuid),
            source_entity_type: decision.source_entity_type.clone(),
            source_namespace: decision.observing_namespace.clone(),
            reference_path: reference_path.clone(),
            value_token: value_token.clone(),
            reference_tokens: reference_tokens.clone(),
            target_type: candidate.entity_type.clone(),
            target_key_group: key_group,
            outcome: if positive {
                ExampleOutcome::Positive
            } else {
                ExampleOutcome::Negative
            },
        });
    }
    examples
}

/// The held-out labelled case one decision example establishes.
pub(crate) fn decision_label(
    decision: &PersistedDecision,
    example: &ReferenceExample,
) -> LabeledCase {
    let should_link = example.outcome == ExampleOutcome::Positive;
    LabeledCase {
        example: example.clone(),
        should_link,
        expected_target_chain_id: should_link
            .then_some(decision.audit.target_chain_id)
            .flatten(),
        adjudication_ref: format!("decision:{}", decision.audit.decision_id),
        label_model: decision.audit.model_served.clone(),
    }
}

/// Stable fingerprint of the observed schema shape: the sorted distinct
/// (source type, path, target type, key group) tuples. It changes exactly when
/// the mapped reference shape changes, driving drift independently of counts.
fn fingerprint(examples: &[ReferenceExample]) -> String {
    let mut shapes: BTreeSet<String> = BTreeSet::new();
    for e in examples {
        shapes.insert(format!(
            "{}|{}|{}|{}|{}|{}",
            e.source_entity_type,
            e.source_namespace,
            e.reference_path,
            e.target_type,
            e.target_key_group.join(","),
            serde_json::to_string(&e.component_paths).expect("string map is serializable")
        ));
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for shape in &shapes {
        for b in shape.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("refshape-{hash:016x}")
}

#[async_trait]
impl ReferenceEvidenceSource for Neo4jGraphBackend {
    async fn collect(
        &self,
        org_id: &str,
        source: &str,
        bounds: &LearningBounds,
        holdout_permille: u16,
    ) -> Result<EvidenceBatch, BackendError> {
        let limit = bounds.max_scanned_entities;
        if limit == 0 || bounds.max_evidence_bytes == 0 {
            return Ok(EvidenceBatch {
                examples: Vec::new(),
                labeled: Vec::new(),
                schema_fingerprint: fingerprint(&[]),
                scanned: 0,
                truncated: true,
            });
        }
        let lookahead = limit.saturating_add(1);
        let mut evidence_bytes = 0usize;
        let mut truncated = false;

        // Positive examples from live reference edges.
        let pos_query = cypher::reference_examples(org_id, source, lookahead)?;
        let mut pos_rows = self
            .execute_read(&pos_query.statement, &pos_query.parameters)
            .await?;
        if pos_rows.len() > limit {
            truncated = true;
            pos_rows.truncate(limit);
        }
        let mut positives = Vec::new();
        for row in &pos_rows {
            let namespace =
                validate_scope(row, org_id, source, &["source_org_id", "target_org_id"])?;
            if required_text(row, "owner_namespace")? != namespace {
                return Err(BackendError::Deserialization(
                    "rule evidence owner namespace disagrees with its producer namespace".into(),
                ));
            }
            let source_chain = Uuid::parse_str(&required_text(row, "source_chain_id")?)
                .map_err(|_| BackendError::Deserialization("invalid source chain id".into()))?;
            let source_version_uuid = Uuid::parse_str(&required_text(row, "source_version_uuid")?)
                .map_err(|_| BackendError::Deserialization("invalid source version id".into()))?;
            let source_type = required_text(row, "source_entity_type")?;
            let _slot = required_text(row, "slot")?;
            let target_type = required_text(row, "target_type")?;
            let key_group = string_list(row, "target_key_group")?;
            let reference_tokens = distinct_string_list(row, "reference_tokens")?;
            if key_group.is_empty() || reference_tokens.is_empty() {
                return Err(BackendError::Deserialization(
                    "rule evidence has an empty target key group".into(),
                ));
            }
            let example = ReferenceExample {
                component_paths: row
                    .get("component_paths")
                    .filter(|value| !value.is_null())
                    .map(|value| {
                        value
                            .as_str()
                            .ok_or_else(|| {
                                BackendError::Deserialization(
                                    "invalid reference component paths".into(),
                                )
                            })
                            .and_then(|raw| {
                                serde_json::from_str(raw).map_err(|_| {
                                    BackendError::Deserialization(
                                        "invalid reference component paths".into(),
                                    )
                                })
                            })
                    })
                    .transpose()?
                    .map(|paths: std::collections::BTreeMap<String, String>| {
                        paths
                            .into_iter()
                            .map(|(key, path)| (key, normalize_occurrence_path(&path)))
                            .collect()
                    }),
                source_chain_id: source_chain,
                source_version_uuid: Some(source_version_uuid),
                reference_path: normalize_occurrence_path(&required_text(
                    row,
                    "evidence_location",
                )?),
                source_entity_type: source_type,
                source_namespace: namespace,
                // A value is identified by the target it completed.
                value_token: reference_tokens.join("\u{1f}"),
                reference_tokens,
                target_type,
                target_key_group: key_group,
                outcome: ExampleOutcome::Positive,
            };
            if !add_within_budget(
                example,
                &mut positives,
                &mut evidence_bytes,
                bounds.max_evidence_bytes,
            )? {
                truncated = true;
                break;
            }
        }

        let mut scanned = pos_rows.len();
        let mut examples = positives;

        // Persisted model decisions: each is evidence for its field, split by
        // occurrence identity so train-side verdicts feed proposals and
        // holdout-side verdicts validate them, never both. Their edges were
        // excluded from the observations above, so nothing counts twice.
        let mut labeled = Vec::new();
        let remaining = limit.saturating_sub(scanned);
        if !truncated && remaining == 0 {
            // Positives exactly filled the scan bound (len == limit, not limit+1).
            // Persisted decisions were never read, so completeness is unproven:
            // report the batch as truncated rather than claiming a complete scan
            // with an empty holdout.
            truncated = true;
        }
        if !truncated && remaining > 0 {
            let mut decisions = self
                .reference_decision_labels(org_id, source, remaining.saturating_add(1))
                .await?;
            if decisions.len() > remaining {
                truncated = true;
                decisions.truncate(remaining);
            }
            scanned = scanned.saturating_add(decisions.len());
            // An occurrence the model once decided is evidence through its
            // decision only, even if its edge later became deterministic and
            // lost its audit: never once as an observation and once as a label.
            let decided: BTreeSet<(Uuid, String)> = decisions
                .iter()
                .flat_map(decision_examples)
                .map(|e| (e.source_chain_id, e.value_token))
                .collect();
            examples.retain(|e| !decided.contains(&(e.source_chain_id, e.value_token.clone())));
            'decisions: for decision in &decisions {
                if decision.producer_source != source {
                    return Err(BackendError::Deserialization(
                        "rule evidence escaped its requested scope".into(),
                    ));
                }
                let mine = decision_examples(decision);
                if mine.is_empty() {
                    continue;
                }
                let partition = split(&mine, holdout_permille);
                for example in partition.train {
                    if !add_within_budget(
                        example,
                        &mut examples,
                        &mut evidence_bytes,
                        bounds.max_evidence_bytes,
                    )? {
                        truncated = true;
                        break 'decisions;
                    }
                }
                for example in &partition.holdout {
                    if !add_within_budget(
                        decision_label(decision, example),
                        &mut labeled,
                        &mut evidence_bytes,
                        bounds.max_evidence_bytes,
                    )? {
                        truncated = true;
                        break 'decisions;
                    }
                }
            }
        }
        let schema_fingerprint = fingerprint(&examples);

        Ok(EvidenceBatch {
            examples,
            labeled,
            schema_fingerprint,
            scanned,
            truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    /// A persisted decision yields one example per offered candidate type:
    /// positive for the accepted target's type with its own key group, negative
    /// for every competitor; a rejection is negative for all. The held-out
    /// label carries the decision id and the model that served it.
    #[test]
    fn persisted_decisions_become_examples_and_labels() {
        use kg_core::runtime::reference_resolution::{
            CitedValueHash, DecisionOutcome, DecisionReason, EvidenceOrigin, PersistedCandidate,
            PersistedDecision, ReferenceDecisionAudit,
        };
        let bucket = Uuid::from_u128(0xb);
        let role = Uuid::from_u128(0xa);
        let mut decision = PersistedDecision {
            audit: ReferenceDecisionAudit {
                decision_id: Uuid::from_u128(7),
                source_chain_id: Uuid::from_u128(1),
                source_version_uuid: Uuid::from_u128(2),
                source_snapshot_id: Uuid::from_u128(3),
                source_captured_at: chrono::Utc::now(),
                slot: "AWS::EC2::Instance.Tags.Value".into(),
                location: "Tags[3].Value".into(),
                value: "archive".into(),
                evidence_origin: EvidenceOrigin::Structured,
                outcome: DecisionOutcome::Accepted,
                reason: DecisionReason::ModelAccepted,
                target_chain_id: Some(bucket),
                fact: Some("names its bucket".into()),
                supporting_evidence: Vec::new(),
                candidate_read_set: Vec::new(),
                evidence_fingerprint: "a".repeat(64),
                evidence_complete: true,
                model_configured: "m".into(),
                model_served: Some("m-served".into()),
                provider_attempts: 1,
                input_tokens: None,
                output_tokens: None,
                processing_version: "v".into(),
                decided_at: chrono::Utc::now(),
                reused: false,
                reused_from: None,
                reuse_fingerprint: "c".repeat(64),
                cited_value_hashes: vec![CitedValueHash {
                    owner_chain_id: Uuid::from_u128(1),
                    path: "@occurrence".into(),
                    sha256: "d".repeat(64),
                }],
            },
            producer_source: "aws".into(),
            observing_namespace: "prod".into(),
            source_entity_type: "AWS::EC2::Instance".into(),
            target_type: Some("AWS::S3::Bucket".into()),
            components: vec![
                ("Name".into(), "s:web-1".into()),
                ("Value".into(), "s:archive".into()),
            ],
            reference_tokens: vec!["s:web-1".into(), "s:archive".into()],
            candidates: vec![
                PersistedCandidate {
                    chain_id: role,
                    entity_type: "AWS::IAM::Role".into(),
                    key_groups: vec![vec!["Arn".into()], vec!["RoleName".into()]],
                },
                PersistedCandidate {
                    chain_id: bucket,
                    entity_type: "AWS::S3::Bucket".into(),
                    key_groups: vec![vec!["Name".into()]],
                },
            ],
            reuse_count: 0,
            last_reused_at: None,
        };
        let examples = decision_examples(&decision);
        assert_eq!(examples.len(), 1, "an acceptance is one positive example");
        let positive = &examples[0];
        assert_eq!(positive.outcome, ExampleOutcome::Positive);
        assert_eq!(positive.target_type, "AWS::S3::Bucket");
        assert_eq!(positive.target_key_group, vec!["Name".to_string()]);
        assert_eq!(positive.reference_path, "Tags[].Value");
        assert_eq!(positive.value_token, "s:archive");
        assert_eq!(
            positive.reference_tokens,
            vec!["s:archive".to_string()],
            "the occurrence's own token, as the matcher identifies it"
        );
        assert_eq!(positive.source_version_uuid, Some(Uuid::from_u128(2)));
        let label = decision_label(&decision, positive);
        assert!(label.should_link);
        assert_eq!(label.expected_target_chain_id, Some(bucket));
        assert_eq!(
            label.adjudication_ref,
            format!("decision:{}", Uuid::from_u128(7))
        );
        assert_eq!(label.label_model.as_deref(), Some("m-served"));

        decision.audit.outcome = DecisionOutcome::Rejected;
        decision.audit.reason = DecisionReason::ModelRejected;
        decision.audit.target_chain_id = None;
        decision.audit.fact = None;
        let rejected = decision_examples(&decision);
        assert!(rejected
            .iter()
            .all(|e| e.outcome == ExampleOutcome::Negative));
        assert_eq!(
            rejected.len(),
            2,
            "a rejection is negative for every offered type"
        );
        let negative = rejected
            .iter()
            .find(|e| e.target_type == "AWS::IAM::Role")
            .unwrap();
        assert_eq!(negative.target_key_group, vec!["Arn".to_string()]);
        let label = decision_label(&decision, negative);
        assert!(!label.should_link && label.expected_target_chain_id.is_none());
        // Every example of one decision shares its identity, so a split keeps them together.
        let partition = split(&rejected, 500);
        assert!(partition.train.is_empty() || partition.holdout.is_empty());
    }

    #[test]
    fn repeated_reference_tokens_collapse_while_repeated_key_members_fail() {
        let row: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "reference_tokens": ["s:DataVolume", "s:vol-1", "s:vol-1"],
                "target_key_group": ["VolumeId", "VolumeId"]
            }))
            .unwrap();
        assert_eq!(
            super::distinct_string_list(&row, "reference_tokens").unwrap(),
            vec!["s:DataVolume".to_string(), "s:vol-1".to_string()]
        );
        assert!(super::string_list(&row, "target_key_group").is_err());
    }

    use super::*;
    use serde_json::json;

    fn scoped_row() -> Map<String, Value> {
        json!({
            "org_id": "org-a",
            "producer_source": "cmdb",
            "producer_namespace": "prod",
            "owner_namespace": "prod",
            "source_org_id": "org-a",
            "target_org_id": "org-a"
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn evidence_rows_must_match_the_requested_scope() {
        let row = scoped_row();
        assert_eq!(
            validate_scope(&row, "org-a", "cmdb", &["source_org_id", "target_org_id"]).unwrap(),
            "prod"
        );
        assert!(validate_scope(&row, "org-b", "cmdb", &[]).is_err());
        assert!(validate_scope(&row, "org-a", "github", &[]).is_err());
        let mut crossed = row;
        crossed.insert("target_org_id".into(), json!("org-b"));
        assert!(validate_scope(
            &crossed,
            "org-a",
            "cmdb",
            &["source_org_id", "target_org_id"]
        )
        .is_err());
    }

    #[test]
    fn evidence_byte_budget_is_fail_closed() {
        let mut values = Vec::new();
        let mut used = 0;
        assert!(add_within_budget("small".to_string(), &mut values, &mut used, 64).unwrap());
        assert!(!add_within_budget("x".repeat(100), &mut values, &mut used, 64).unwrap());
        assert_eq!(values, vec!["small".to_string()]);
    }
}
