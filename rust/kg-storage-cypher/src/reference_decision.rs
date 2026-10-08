//! Persisted reference decisions (`:ReferenceDecision`): original model
//! dispositions on ambiguous references, stored as hashes and identifiers so a
//! later run can reuse an unchanged decision without a provider call and so rule
//! learning has labelled cases. Reuses only bump counters on the original.
use crate::PreparedQuery;
use kg_core::{
    errors::BackendError,
    runtime::reference_resolution::{
        DecisionReuse, PersistedCandidate, PersistedDecision, ReferenceDecisionAudit,
        MAX_DECISIONS_PER_STATEMENT,
    },
    traits::graph_reads::MAX_LOOKUP_KEYS,
};
use serde_json::{json, Map, Value};

/// Idempotent under receipt replay: an existing decision id is left untouched.
pub const RECORD_REFERENCE_DECISIONS: &str = "UNWIND $rows AS row
 MERGE (d:ReferenceDecision {org_id:$org,decision_id:row.decision_id})
 ON CREATE SET d += row.props, d.reuse_count = 0
 WITH count(d) AS written
 RETURN true AS ok";

/// A missing original (deleted by an administrator between lookup and commit)
/// is a no-op, never a batch failure. The statement itself is not idempotent
/// (a counter increment); it is safe under receipt replay only because a
/// replayed batch short-circuits on its receipt before any statement runs.
pub const NOTE_DECISION_REUSES: &str = "UNWIND $rows AS row
 OPTIONAL MATCH (o:ReferenceDecision {org_id:$org,decision_id:row.original})
 FOREACH (_ IN CASE WHEN o IS NULL THEN [] ELSE [1] END |
   SET o.reuse_count = coalesce(o.reuse_count,0)+1, o.last_reused_at = row.at, o.last_reused_decision_id = row.decision_id,
       o.last_reused_source_version_uuid = coalesce(row.source_version_uuid, o.last_reused_source_version_uuid))
 WITH count(row) AS noted
 RETURN true AS ok";

fn text_list(values: &[String]) -> Value {
    json!(values)
}

/// The stored properties of one decision. The audit itself is kept as JSON so
/// the record round-trips exactly; the indexed and queried fields are flat.
pub fn decision_row(decision: &PersistedDecision) -> Result<Value, BackendError> {
    let audit = &decision.audit;
    let audit_json = serde_json::to_string(audit)
        .map_err(|_| BackendError::Serialization("reference decision audit".into()))?;
    let candidates = serde_json::to_string(&decision.candidates)
        .map_err(|_| BackendError::Serialization("reference decision candidates".into()))?;
    let components = serde_json::to_string(&decision.components)
        .map_err(|_| BackendError::Serialization("reference decision components".into()))?;
    Ok(json!({
        "decision_id": audit.decision_id,
        "props": {
            "decision_id": audit.decision_id,
            "source_chain_id": audit.source_chain_id,
            "source_version_uuid": audit.source_version_uuid,
            "source_snapshot_id": audit.source_snapshot_id,
            "source_captured_at": audit.source_captured_at.to_rfc3339(),
            "producer_source": decision.producer_source,
            "observing_namespace": decision.observing_namespace,
            "source_entity_type": decision.source_entity_type,
            "target_type": decision.target_type,
            "slot": audit.slot,
            "location": audit.location,
            "value": audit.value,
            "outcome": serde_json::to_value(audit.outcome).unwrap_or(Value::Null),
            "reason": serde_json::to_value(audit.reason).unwrap_or(Value::Null),
            "target_chain_id": audit.target_chain_id,
            "evidence_fingerprint": audit.evidence_fingerprint,
            "reuse_fingerprint": audit.reuse_fingerprint,
            "model_served": audit.model_served,
            "provider_attempts": audit.provider_attempts,
            "processing_version": audit.processing_version,
            "decided_at": audit.decided_at.to_rfc3339(),
            "reused": false,
            "reference_tokens": text_list(&decision.reference_tokens),
            "components": components,
            "candidates": candidates,
            "audit": audit_json,
        }
    }))
}

pub fn reuse_row(reuse: &DecisionReuse) -> Value {
    json!({"original": reuse.original, "decision_id": reuse.decision_id, "at": reuse.at.to_rfc3339(), "source_version_uuid": reuse.source_version_uuid})
}

/// Writes for one mutation: at most one packed statement per kind.
pub fn writes(
    org: &str,
    decisions: &[PersistedDecision],
    reuses: &[DecisionReuse],
) -> Result<Vec<(String, Value)>, BackendError> {
    if decisions.len() > MAX_DECISIONS_PER_STATEMENT || reuses.len() > MAX_DECISIONS_PER_STATEMENT {
        return Err(BackendError::Query(
            "reference decision record exceeds one statement".into(),
        ));
    }
    let mut out = Vec::new();
    if !decisions.is_empty() {
        let rows = decisions
            .iter()
            .map(decision_row)
            .collect::<Result<Vec<_>, _>>()?;
        out.push((
            RECORD_REFERENCE_DECISIONS.to_owned(),
            json!({"org": org, "rows": rows}),
        ));
    }
    if !reuses.is_empty() {
        let rows: Vec<Value> = reuses.iter().map(reuse_row).collect();
        out.push((
            NOTE_DECISION_REUSES.to_owned(),
            json!({"org": org, "rows": rows}),
        ));
    }
    Ok(out)
}

/// Newest original decision per reuse fingerprint. Bounded by `MAX_LOOKUP_KEYS`.
pub fn by_reuse_key(org: &str, keys: &[String]) -> Result<PreparedQuery, BackendError> {
    if org.trim().is_empty() || keys.is_empty() || keys.len() > MAX_LOOKUP_KEYS {
        return Err(BackendError::Query(
            "reference decision lookup requires an organization and 1..=MAX_LOOKUP_KEYS keys"
                .into(),
        ));
    }
    Ok(PreparedQuery {
        statement: "UNWIND $keys AS key
 MATCH (d:ReferenceDecision {org_id:$org,reuse_fingerprint:key})
 WHERE d.reused = false
 WITH key, d ORDER BY d.decided_at DESC, d.decision_id DESC
 WITH key, head(collect(d)) AS d
 RETURN d.org_id AS org_id, d.producer_source AS producer_source, d.observing_namespace AS observing_namespace,
        d.source_entity_type AS source_entity_type, d.target_type AS target_type, d.reference_tokens AS reference_tokens,
        d.components AS components, d.candidates AS candidates, d.audit AS audit,
        d.reuse_count AS reuse_count, d.last_reused_at AS last_reused_at"
            .into(),
        parameters: json!({"org": org, "keys": keys}),
    })
}

/// Model decisions of one producer source whose source version is still the
/// live latest, newest per (source chain, slot, value); labelled cases for rule
/// learning. Host refusals are never stored, so every row is a model verdict.
pub fn labels(
    org: &str,
    producer_source: &str,
    limit: usize,
) -> Result<PreparedQuery, BackendError> {
    if org.trim().is_empty() || producer_source.trim().is_empty() || limit == 0 {
        return Err(BackendError::Query(
            "reference decision labels require an organization, source and positive limit".into(),
        ));
    }
    Ok(PreparedQuery {
        statement: "MATCH (d:ReferenceDecision {org_id:$org,producer_source:$source})
 WHERE d.reused = false AND d.outcome IN ['accepted','rejected']
 MATCH (s:Entity {org_id:$org,chain_id:d.source_chain_id,is_latest:true})
 WHERE s.deleted_at IS NULL AND (s.uuid = d.source_version_uuid OR s.uuid = d.last_reused_source_version_uuid)
 WITH d ORDER BY d.decided_at DESC, d.decision_id DESC
 WITH d.source_chain_id AS source, d.slot AS slot, d.location AS location, d.value AS value, head(collect(d)) AS d
 RETURN d.org_id AS org_id, d.producer_source AS producer_source, d.observing_namespace AS observing_namespace,
        d.source_entity_type AS source_entity_type, d.target_type AS target_type, d.reference_tokens AS reference_tokens,
        d.components AS components, d.candidates AS candidates, d.audit AS audit,
        d.reuse_count AS reuse_count, d.last_reused_at AS last_reused_at
 ORDER BY source, slot, location, value LIMIT $limit"
            .into(),
        parameters: json!({"org": org, "source": producer_source, "limit": limit as i64}),
    })
}

/// A row from another organization is a hard failure of the read, never a
/// skipped row: it means the query or the store is wrong.
pub fn row_organization(org: &str, row: &Map<String, Value>) -> Result<(), BackendError> {
    match row.get("org_id").and_then(Value::as_str) {
        Some(found) if found == org => Ok(()),
        _ => Err(BackendError::Deserialization(
            "invalid persisted decision: organization".into(),
        )),
    }
}

/// Decode one row; every row must belong to `org`.
pub fn decode(org: &str, row: &Map<String, Value>) -> Result<PersistedDecision, BackendError> {
    let invalid =
        |m: &str| BackendError::Deserialization(format!("invalid persisted decision: {m}"));
    let text = |key: &str| -> Result<String, BackendError> {
        row.get(key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| invalid(key))
    };
    row_organization(org, row)?;
    let audit: ReferenceDecisionAudit =
        serde_json::from_str(&text("audit")?).map_err(|_| invalid("audit"))?;
    let candidates: Vec<PersistedCandidate> =
        serde_json::from_str(&text("candidates")?).map_err(|_| invalid("candidates"))?;
    let components: Vec<(String, String)> =
        serde_json::from_str(&text("components")?).map_err(|_| invalid("components"))?;
    let reference_tokens = row
        .get("reference_tokens")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("tokens"))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let decision = PersistedDecision {
        audit,
        producer_source: text("producer_source")?,
        observing_namespace: text("observing_namespace")?,
        source_entity_type: text("source_entity_type")?,
        target_type: row
            .get("target_type")
            .and_then(Value::as_str)
            .map(str::to_owned),
        components,
        reference_tokens,
        candidates,
        reuse_count: row
            .get("reuse_count")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        last_reused_at: row
            .get("last_reused_at")
            .and_then(Value::as_str)
            .map(str::parse)
            .transpose()
            .map_err(|_| invalid("last_reused_at"))?,
    };
    decision.validate(org)?;
    if decision.audit.decision_id.is_nil() {
        return Err(invalid("decision id"));
    }
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookups_are_bounded_and_scoped() {
        assert!(by_reuse_key("org", &[]).is_err());
        let keys: Vec<String> = (0..=MAX_LOOKUP_KEYS).map(|i| format!("{i:064x}")).collect();
        assert!(by_reuse_key("org", &keys).is_err());
        assert!(by_reuse_key("org", &keys[..1]).is_ok());
        assert!(labels("", "aws", 10).is_err());
        assert!(labels("org", "aws", 0).is_err());
        let query = labels("org", "aws", 10).unwrap();
        assert!(query.statement.contains("d.reused = false"));
        assert!(query.statement.contains("s.uuid = d.source_version_uuid"));
    }

    #[test]
    fn a_row_from_another_organization_is_refused() {
        let row: Map<String, Value> = serde_json::from_value(json!({"org_id": "other"})).unwrap();
        assert!(decode("org", &row).is_err());
        assert!(row_organization("org", &row).is_err());
        assert!(row_organization("other", &row).is_ok());
    }

    fn sample() -> PersistedDecision {
        use chrono::Utc;
        use kg_core::runtime::reference_resolution::{
            CitedValueHash, DecisionOutcome, DecisionReason, EvidenceOrigin,
        };
        use uuid::Uuid;
        PersistedDecision {
            audit: ReferenceDecisionAudit {
                decision_id: Uuid::from_u128(7),
                source_chain_id: Uuid::from_u128(1),
                source_version_uuid: Uuid::from_u128(2),
                source_snapshot_id: Uuid::from_u128(3),
                source_captured_at: Utc::now(),
                slot: "Server.peer".into(),
                location: "peer".into(),
                value: "x".into(),
                evidence_origin: EvidenceOrigin::Structured,
                outcome: DecisionOutcome::Rejected,
                reason: DecisionReason::ModelRejected,
                target_chain_id: None,
                fact: None,
                supporting_evidence: Vec::new(),
                candidate_read_set: Vec::new(),
                evidence_fingerprint: "a".repeat(64),
                evidence_complete: true,
                model_configured: "m".into(),
                model_served: Some("m".into()),
                provider_attempts: 1,
                input_tokens: Some(10),
                output_tokens: Some(5),
                processing_version: "v".into(),
                decided_at: Utc::now(),
                reused: false,
                reused_from: None,
                reuse_fingerprint: "c".repeat(64),
                cited_value_hashes: vec![CitedValueHash {
                    owner_chain_id: Uuid::from_u128(1),
                    path: "@occurrence".into(),
                    sha256: "d".repeat(64),
                }],
            },
            producer_source: "cmdb".into(),
            observing_namespace: "prod".into(),
            source_entity_type: "Server".into(),
            target_type: Some("Server".into()),
            components: vec![("peer".into(), "s:x".into())],
            reference_tokens: vec!["s:x".into()],
            candidates: vec![PersistedCandidate {
                chain_id: Uuid::from_u128(4),
                entity_type: "Server".into(),
                key_groups: vec![vec!["id".into()]],
            }],
            reuse_count: 0,
            last_reused_at: None,
        }
    }

    /// The row a write produces reads back as the same decision; a row this
    /// build cannot read is an error the caller can skip, never a wrong record.
    #[test]
    fn written_rows_round_trip_and_malformed_rows_are_errors() {
        let decision = sample();
        let row = decision_row(&decision).unwrap();
        let mut stored: Map<String, Value> = row["props"].as_object().unwrap().clone();
        stored.insert("org_id".into(), json!("org"));
        stored.insert("reuse_count".into(), json!(3));
        stored.insert("last_reused_at".into(), json!("2026-09-26T00:00:00Z"));
        let read = decode("org", &stored).unwrap();
        assert_eq!(read.audit, decision.audit);
        assert_eq!(read.candidates, decision.candidates);
        assert_eq!(read.components, decision.components);
        assert_eq!(read.reference_tokens, decision.reference_tokens);
        assert_eq!(read.reuse_count, 3);
        assert!(read.last_reused_at.is_some());
        assert_eq!(row["props"]["reused"], false);
        assert_eq!(row["props"]["outcome"], "rejected");

        let mut malformed = stored.clone();
        malformed.insert("audit".into(), json!("{not json"));
        assert!(decode("org", &malformed).is_err());
        let mut missing = stored.clone();
        missing.remove("candidates");
        assert!(decode("org", &missing).is_err());
        assert!(
            row_organization("org", &missing).is_ok(),
            "malformed, but ours"
        );

        let (statement, parameters) =
            &writes("org", std::slice::from_ref(&decision), &[]).unwrap()[0];
        assert_eq!(statement, RECORD_REFERENCE_DECISIONS);
        assert_eq!(parameters["rows"].as_array().unwrap().len(), 1);
        assert_eq!(writes("org", &[], &[]).unwrap().len(), 0);
    }
}
