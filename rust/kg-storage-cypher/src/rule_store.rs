//! Prepared statements for the versioned learned-rule store.
//!
//! A learned rule is persisted as one `:LearnedRule` node keyed on
//! `(org_id, id)`. The full [`LearnedRule`] round-trips through a `body` JSON
//! property so the runtime mapping schema is reused verbatim, with no second
//! matcher); scalar columns (`source`, `status`, `revision`) exist only so the
//! active set can be listed without parsing every body. Every mutation is a
//! single guarded statement: `propose` creates only when the id is absent and
//! `transition` writes only when the stored revision matches the caller's
//! expectation, so a stale expectation returns no row and the driver maps it to
//! [`BackendError::Conflict`] rather than overwriting a concurrent update.
use crate::PreparedQuery;
use kg_core::{errors::BackendError, traits::rule_store::LearnedRule};
use serde_json::{json, Map, Value};
use uuid::Uuid;

/// Upper bound on the active rules returned for one producer in a run.
pub const MAX_ACTIVE_RULES: usize = 4096;

/// Schema installed idempotently before any rule read or write.
pub const SCHEMA: &[(&str, &str)] = &[
    (
        "learned_rule_identity",
        "CREATE CONSTRAINT learned_rule_identity IF NOT EXISTS FOR (r:LearnedRule) REQUIRE (r.org_id, r.id) IS UNIQUE",
    ),
    (
        "learned_rule_active",
        "CREATE INDEX learned_rule_active IF NOT EXISTS FOR (r:LearnedRule) ON (r.org_id, r.source, r.status)",
    ),
];

fn valid_org(org: &str) -> Result<(), BackendError> {
    if org.trim().is_empty() {
        return Err(BackendError::Query("rule store requires an org_id".into()));
    }
    Ok(())
}

/// The scalar columns plus the lossless `body`, as write parameters.
pub fn columns(rule: &LearnedRule) -> Result<Map<String, Value>, BackendError> {
    let body = serde_json::to_string(rule)
        .map_err(|e| BackendError::Query(format!("rule serialization failed: {e}")))?;
    let mut map = Map::new();
    map.insert("org".into(), json!(rule.org_id));
    map.insert("id".into(), json!(rule.id.to_string()));
    map.insert("revision".into(), json!(rule.revision));
    map.insert("source".into(), json!(rule.source));
    map.insert("status".into(), json!(status_str(rule)));
    map.insert(
        "effective_from".into(),
        rule.effective_from
            .map(|t| json!(t.to_rfc3339()))
            .unwrap_or(Value::Null),
    );
    map.insert(
        "revoked_at".into(),
        rule.revoked_at
            .map(|t| json!(t.to_rfc3339()))
            .unwrap_or(Value::Null),
    );
    map.insert("body".into(), json!(body));
    Ok(map)
}

fn status_str(rule: &LearnedRule) -> String {
    // Serialize through serde to keep the on-disk spelling identical to the enum.
    serde_json::to_value(rule.status)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Read one rule by identity.
pub fn get(org: &str, id: Uuid) -> Result<PreparedQuery, BackendError> {
    valid_org(org)?;
    Ok(PreparedQuery {
        statement: "MATCH (r:LearnedRule {org_id:$org, id:$id}) RETURN r.body AS body".into(),
        parameters: json!({"org": org, "id": id.to_string()}),
    })
}

/// Read one rule and its stored revision, for a guarded transition.
pub fn read_for_update(org: &str, id: Uuid) -> Result<PreparedQuery, BackendError> {
    valid_org(org)?;
    Ok(PreparedQuery {
        statement:
            "MATCH (r:LearnedRule {org_id:$org, id:$id}) RETURN r.body AS body, r.revision AS revision"
                .into(),
        parameters: json!({"org": org, "id": id.to_string()}),
    })
}

/// Active rules for one producer, ordered and bounded so a run freezes a
/// deterministic, size-limited set.
pub fn list_active(org: &str, source: &str) -> Result<PreparedQuery, BackendError> {
    valid_org(org)?;
    if source.trim().is_empty() {
        return Err(BackendError::Query("rule store requires a source".into()));
    }
    Ok(PreparedQuery {
        statement: "MATCH (r:LearnedRule {org_id:$org, source:$source, status:'active'}) RETURN r.body AS body ORDER BY r.id LIMIT $limit".into(),
        parameters: json!({"org": org, "source": source, "limit": (MAX_ACTIVE_RULES + 1) as i64}),
    })
}

/// Active rules for an org across every source, ordered and bounded.
pub fn list_all_active(org: &str) -> Result<PreparedQuery, BackendError> {
    valid_org(org)?;
    Ok(PreparedQuery {
        statement: "MATCH (r:LearnedRule {org_id:$org, status:'active'}) RETURN r.body AS body ORDER BY r.source, r.id LIMIT $limit".into(),
        parameters: json!({"org": org, "limit": (MAX_ACTIVE_RULES + 1) as i64}),
    })
}

/// Every rule for one producer, all statuses, ordered and bounded.
pub fn list_all(org: &str, source: &str) -> Result<PreparedQuery, BackendError> {
    valid_org(org)?;
    if source.trim().is_empty() {
        return Err(BackendError::Query("rule store requires a source".into()));
    }
    Ok(PreparedQuery {
        statement: "MATCH (r:LearnedRule {org_id:$org, source:$source}) RETURN r.body AS body ORDER BY r.id LIMIT $limit".into(),
        parameters: json!({"org": org, "source": source, "limit": (MAX_ACTIVE_RULES + 1) as i64}),
    })
}

/// Create a rule only when its id is absent; `created` is false when the id
/// already exists, which the driver reports as a conflict.
pub fn propose(rule: &LearnedRule) -> Result<PreparedQuery, BackendError> {
    valid_org(&rule.org_id)?;
    let parameters = Value::Object(columns(rule)?);
    Ok(PreparedQuery {
        statement: "OPTIONAL MATCH (existing:LearnedRule {org_id:$org, id:$id}) \
             FOREACH (_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END | \
               CREATE (r:LearnedRule {org_id:$org, id:$id, revision:$revision, source:$source, status:$status, effective_from:$effective_from, revoked_at:$revoked_at, body:$body})) \
             RETURN existing IS NULL AS created"
            .into(),
        parameters,
    })
}

/// Apply a transition when the stored revision matches `expected`. Returns the
/// new revision when the guard held, no row otherwise.
pub fn apply_transition(
    updated: &LearnedRule,
    expected_revision: u64,
) -> Result<PreparedQuery, BackendError> {
    valid_org(&updated.org_id)?;
    let mut params = columns(updated)?;
    params.insert("expected".into(), json!(expected_revision));
    Ok(PreparedQuery {
        statement: "MATCH (r:LearnedRule {org_id:$org, id:$id}) WHERE r.revision = $expected \
             SET r.revision=$revision, r.status=$status, r.effective_from=$effective_from, r.revoked_at=$revoked_at, r.body=$body \
             RETURN r.revision AS revision"
            .into(),
        parameters: Value::Object(params),
    })
}

/// Decode a stored `body` back into a validated [`LearnedRule`].
pub fn decode(row: &Map<String, Value>) -> Result<LearnedRule, BackendError> {
    let body = row
        .get("body")
        .and_then(Value::as_str)
        .ok_or_else(|| BackendError::Deserialization("learned rule missing body".into()))?;
    let rule: LearnedRule = serde_json::from_str(body)
        .map_err(|e| BackendError::Deserialization(format!("invalid learned rule body: {e}")))?;
    rule.validate().map_err(BackendError::Deserialization)?;
    Ok(rule)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use kg_core::runtime::extraction::ReferenceMapping;
    use kg_core::traits::rule_store::{
        RuleOrigin, RuleStatus, RuleValidation, MIN_PROMOTION_NEGATIVES, MIN_PROMOTION_POSITIVES,
        MIN_PROMOTION_PRECISION, MIN_PROMOTION_RECALL,
    };

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

    fn sample(status: RuleStatus) -> LearnedRule {
        LearnedRule {
            id: Uuid::from_u128(1),
            revision: 1,
            org_id: "org".into(),
            source: "cmdb".into(),
            namespace: Some("prod".into()),
            schema_fingerprint: "fp-1".into(),
            mapping: mapping(),
            owner_slot: "CmdbChange.owning_group".into(),
            origin: RuleOrigin::Model {
                model: "gpt-5.4-mini".into(),
                prompt_version: "v1".into(),
            },
            evidence_refs: vec!["ev-1".into()],
            validation: (status == RuleStatus::Active).then_some(RuleValidation {
                positives: MIN_PROMOTION_POSITIVES,
                negatives: MIN_PROMOTION_NEGATIVES,
                precision: MIN_PROMOTION_PRECISION,
                recall: MIN_PROMOTION_RECALL,
                conflicting_failures: 0,
                independent: true,
            }),
            decisions: Vec::new(),
            status,
            effective_from: (status == RuleStatus::Active).then(Utc::now),
            revoked_at: None,
        }
    }

    #[test]
    fn reads_are_parameterized_and_never_inline_values() {
        let q = get("tenant-42", Uuid::from_u128(1)).unwrap();
        assert_eq!(q.parameters["org"], json!("tenant-42"));
        assert_eq!(q.parameters["id"], json!(Uuid::from_u128(1).to_string()));
        assert!(
            !q.statement.contains("tenant-42"),
            "caller values never enter the statement text"
        );
        assert!(get("  ", Uuid::from_u128(1)).is_err());

        let q = list_active("tenant-42", "cmdb").unwrap();
        assert_eq!(q.parameters["limit"], json!(MAX_ACTIVE_RULES as i64 + 1));
        assert!(q.statement.contains("status:'active'"));
        assert!(!q.statement.contains("tenant-42"));
        assert!(list_active("tenant-42", "  ").is_err());
    }

    #[test]
    fn propose_creates_only_when_absent() {
        let q = propose(&sample(RuleStatus::Proposed)).unwrap();
        assert!(q.statement.contains("existing IS NULL AS created"));
        assert!(q.statement.contains("FOREACH"));
        assert_eq!(q.parameters["revision"], json!(1));
        assert_eq!(q.parameters["status"], json!("proposed"));
    }

    #[test]
    fn transition_guards_on_the_expected_revision() {
        let mut updated = sample(RuleStatus::Active);
        updated.revision = 2;
        let q = apply_transition(&updated, 1).unwrap();
        assert!(q.statement.contains("WHERE r.revision = $expected"));
        assert_eq!(q.parameters["expected"], json!(1));
        assert_eq!(q.parameters["revision"], json!(2));
        assert_eq!(q.parameters["status"], json!("active"));
    }

    #[test]
    fn body_round_trips_through_decode() {
        let rule = sample(RuleStatus::Active);
        let cols = columns(&rule).unwrap();
        let decoded = decode(&cols).unwrap();
        assert_eq!(decoded, rule);
    }

    #[test]
    fn decode_rejects_a_missing_or_corrupt_body() {
        assert!(decode(&Map::new()).is_err());
        let mut row = Map::new();
        row.insert("body".into(), json!("{not json"));
        assert!(decode(&row).is_err());
    }
}
