//! Derived summaries retain exact accepted evidence and temporal coverage separately from source data.
use crate::{
    errors::BackendError,
    traits::{relationship_timeline::IncidentVersionState, GraphProperties},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;

pub const SUMMARY_TEXT_VERSION: &str = "1";
pub const POLICY_VERSION: &str = "1";
pub const MAX_FACTS: usize = 10_000;
pub const DERIVED_PROPERTIES: &[&str] = &[
    "derived_summary",
    "summary_revision",
    "summary_as_of",
    "summary_valid_until",
    "summary_evidence_hash",
    "summary_policy_version",
    "summary_evidence_ids",
    "summary_total_evidence",
    "summary_embedding",
    "summary_embedding_model",
    "summary_embedding_text_version",
    "summary_embedding_content_hash",
];
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SummaryRequest {
    pub chain_id: Uuid,
    pub as_of: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SummaryFact {
    pub id: Uuid,
    pub source_uuid: Uuid,
    pub target_uuid: Uuid,
    pub line: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub snapshot_ids: Vec<Uuid>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SummaryEvidenceGuard {
    pub target_uuid: Uuid,
    pub target_chain_id: Uuid,
    pub expected_revision: Option<Uuid>,
    pub entity_versions: BTreeMap<Uuid, Vec<GraphProperties>>,
    pub incident_versions: Vec<IncidentVersionState>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SummaryEvidence {
    pub guard: SummaryEvidenceGuard,
    pub namespace: String,
    pub as_of: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub facts: Vec<SummaryFact>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivedSummary {
    pub revision: Uuid,
    pub text: String,
    pub as_of: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub evidence_hash: String,
    pub policy_version: String,
    pub evidence_ids: Vec<Uuid>,
    pub total_evidence: usize,
}
fn invalid() -> BackendError {
    BackendError::Query("invalid derived summary evidence or coverage".into())
}
/// Ignore vector refreshes and derived output when fencing accepted source state.
pub fn entity_state(properties: &GraphProperties) -> GraphProperties {
    properties
        .iter()
        .filter(|(key, value)| {
            !value.is_null()
                && !DERIVED_PROPERTIES.contains(&key.as_str())
                && !matches!(
                    key.as_str(),
                    "embedding"
                        | "embedding_model"
                        | "embedding_text_version"
                        | "embedding_content_hash"
                )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn validate_entity_timeline(chain: Uuid, versions: &[GraphProperties]) -> Result<(), BackendError> {
    if chain.is_nil() || versions.len() > MAX_FACTS {
        return Err(invalid());
    }
    let mut ids = HashSet::new();
    for version in versions {
        let id = version
            .get("uuid")
            .and_then(serde_json::Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(invalid)?;
        if id.is_nil()
            || !ids.insert(id)
            || version.get("chain_id").and_then(serde_json::Value::as_str)
                != Some(chain.to_string().as_str())
            || entity_state(version) != *version
        {
            return Err(invalid());
        }
        for key in ["name", "entity_type", "namespace"] {
            if version
                .get(key)
                .and_then(serde_json::Value::as_str)
                .is_none_or(|s| s.trim().is_empty())
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
impl SummaryEvidenceGuard {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.target_uuid.is_nil()
            || self.target_chain_id.is_nil()
            || self.expected_revision.is_some_and(|id| id.is_nil())
            || self.entity_versions.values().map(Vec::len).sum::<usize>() > MAX_FACTS
        {
            return Err(invalid());
        }
        for (chain, versions) in &self.entity_versions {
            validate_entity_timeline(*chain, versions)?;
        }
        if !self
            .entity_versions
            .get(&self.target_chain_id)
            .is_some_and(|versions| {
                versions.iter().any(|p| {
                    p.get("uuid").and_then(serde_json::Value::as_str)
                        == Some(self.target_uuid.to_string().as_str())
                })
            })
        {
            return Err(invalid());
        }
        crate::traits::relationship_timeline::validate_incident(
            self.target_chain_id,
            &self.incident_versions,
        )?;
        Ok(())
    }
}
impl SummaryEvidence {
    pub fn validate(&self) -> Result<(), BackendError> {
        self.guard.validate()?;
        if self.namespace.trim().is_empty()
            || self.facts.len() > MAX_FACTS
            || self.valid_until.is_some_and(|end| end <= self.as_of)
        {
            return Err(invalid());
        }
        let mut ids = HashSet::new();
        let versions: HashSet<_> = self
            .guard
            .entity_versions
            .values()
            .flatten()
            .filter_map(|p| p.get("uuid").and_then(serde_json::Value::as_str))
            .collect();
        for fact in &self.facts {
            if fact.id.is_nil()
                || !ids.insert(fact.id)
                || fact.line.trim().is_empty()
                || fact.line.chars().count() > crate::embedding::MAX_TEXT_CHARS
                || fact.valid_from > self.as_of
                || fact.valid_until.is_some_and(|end| end <= self.as_of)
                || !versions.contains(fact.source_uuid.to_string().as_str())
                || !versions.contains(fact.target_uuid.to_string().as_str())
                || fact.snapshot_ids.iter().any(Uuid::is_nil)
                || !self.guard.incident_versions.iter().any(|v| {
                    v.properties.get("uuid").and_then(serde_json::Value::as_str)
                        == Some(fact.id.to_string().as_str())
                })
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
impl DerivedSummary {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.revision.is_nil()
            || self.text.trim().is_empty()
            || self.text.chars().count() > crate::embedding::MAX_TEXT_CHARS
            || self.valid_until.is_some_and(|end| end <= self.as_of)
            || self.policy_version != POLICY_VERSION
            || self.evidence_hash.len() != 64
            || !self
                .evidence_hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.evidence_ids.is_empty()
            || self.total_evidence < self.evidence_ids.len()
            || self.total_evidence > MAX_FACTS
            || self.evidence_ids.iter().any(Uuid::is_nil)
            || self.evidence_ids.iter().collect::<HashSet<_>>().len() != self.evidence_ids.len()
        {
            return Err(invalid());
        }
        Ok(())
    }
}
pub fn embedding_text(text: &str) -> String {
    text.trim().to_owned()
}
/// Digest complete accepted evidence, independent of provider selection and map insertion order.
pub fn evidence_hash(evidence: &SummaryEvidence) -> Result<String, BackendError> {
    evidence.validate()?;
    let mut evidence = evidence.clone();
    evidence.guard.expected_revision = None;
    evidence.facts.sort_by_key(|fact| fact.id);
    for fact in &mut evidence.facts {
        fact.snapshot_ids.sort_unstable();
        fact.snapshot_ids.dedup();
    }
    for versions in evidence.guard.entity_versions.values_mut() {
        versions.sort_by_key(|p| {
            p.get("uuid")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
    }
    evidence.guard.incident_versions.sort_by_key(|v| {
        v.properties
            .get("uuid")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    });
    fn ordered(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(object) => serde_json::Value::Object(
                object
                    .into_iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .map(|(key, value)| (key, ordered(value)))
                    .collect(),
            ),
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(ordered).collect())
            }
            value => value,
        }
    }
    let bytes = serde_json::to_vec(&ordered(
        serde_json::to_value(evidence).map_err(|_| invalid())?,
    ))
    .map_err(|_| invalid())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// The first scheduled source-state transition after this reference time.
pub fn coverage_end(
    guard: &SummaryEvidenceGuard,
    as_of: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, BackendError> {
    guard.validate()?;
    let time =
        |properties: &GraphProperties, key: &str| -> Result<Option<DateTime<Utc>>, BackendError> {
            properties
                .get(key)
                .map(|value| {
                    value
                        .as_str()
                        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                        .map(|at| at.with_timezone(&Utc))
                        .ok_or_else(invalid)
                })
                .transpose()
        };
    let target = guard.entity_versions[&guard.target_chain_id]
        .iter()
        .find(|p| {
            p.get("uuid").and_then(serde_json::Value::as_str)
                == Some(guard.target_uuid.to_string().as_str())
        })
        .ok_or_else(invalid)?;
    if time(target, "valid_from")?.is_none_or(|start| start > as_of)
        || ["valid_to", "deleted_at", "merged_at"]
            .iter()
            .any(|key| time(target, key).is_ok_and(|end| end.is_some_and(|end| end <= as_of)))
    {
        return Err(invalid());
    }
    let mut next = None;
    for (properties, relationship) in guard
        .entity_versions
        .values()
        .flatten()
        .map(|properties| (properties, false))
        .chain(
            guard
                .incident_versions
                .iter()
                .map(|v| (&v.properties, true)),
        )
    {
        let start = time(properties, "valid_from")?.ok_or_else(invalid)?;
        let mut end = None;
        for key in ["valid_to", "invalid_at", "deleted_at", "merged_at"] {
            if let Some(at) = time(properties, key)? {
                end = Some(end.map_or(at, |current: DateTime<Utc>| current.min(at)));
            }
        }
        if end.is_some_and(|end| end < start) {
            return Err(invalid());
        }
        // Canceled schedules and empty intervals can never contribute accepted facts.
        if end == Some(start) || (relationship && time(properties, "cancelled_at")?.is_some()) {
            continue;
        }
        for at in std::iter::once(start).chain(end).filter(|at| *at > as_of) {
            next = Some(next.map_or(at, |current: DateTime<Utc>| current.min(at)));
        }
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn fixture() -> SummaryEvidence {
        let id = Uuid::new_v4();
        let properties=json!({"uuid":id,"chain_id":id,"org_id":"org","namespace":"prod","entity_type":"Service","name":"api","version":1,"is_latest":true,"valid_from":"2026-01-01T00:00:00Z","valid_to":"2026-03-01T00:00:00Z"}).as_object().unwrap().clone();
        SummaryEvidence {
            guard: SummaryEvidenceGuard {
                target_uuid: id,
                target_chain_id: id,
                expected_revision: None,
                entity_versions: BTreeMap::from([(id, vec![properties])]),
                incident_versions: vec![],
            },
            namespace: "prod".into(),
            as_of: "2026-02-01T00:00:00Z".parse().unwrap(),
            valid_until: Some("2026-03-01T00:00:00Z".parse().unwrap()),
            facts: vec![],
        }
    }
    #[test]
    fn source_projection_excludes_derived_output_but_keeps_temporal_state() {
        let evidence = fixture();
        let mut state = evidence.guard.entity_versions[&evidence.guard.target_chain_id][0].clone();
        let original = state.clone();
        for property in DERIVED_PROPERTIES {
            state.insert((*property).into(), json!("derived"));
        }
        state.insert("embedding".into(), json!([1.0, 0.0]));
        assert_eq!(entity_state(&state), original);
        state.insert("last_seen_at".into(), json!("2026-02-01T00:00:00Z"));
        assert_ne!(entity_state(&state), original);
    }
    #[test]
    fn coverage_expires_at_the_first_boundary_and_rejects_a_non_effective_target() {
        let evidence = fixture();
        assert_eq!(
            coverage_end(&evidence.guard, evidence.as_of).unwrap(),
            evidence.valid_until
        );
        assert!(coverage_end(&evidence.guard, evidence.valid_until.unwrap()).is_err());
        assert!(coverage_end(&evidence.guard, "2025-01-01T00:00:00Z".parse().unwrap()).is_err());
    }
    #[test]
    fn coverage_ignores_canceled_and_empty_intervals_but_rejects_reversed_bounds() {
        let mut evidence = fixture();
        let chain = evidence.guard.target_chain_id;
        let edge = json!({"uuid":Uuid::new_v4(),"chain_id":Uuid::new_v4(),
            "version":1,"is_latest":false,"valid_from":"2026-02-10T00:00:00Z",
            "valid_to":"2026-02-20T00:00:00Z","cancelled_at":"2026-02-01T00:00:00Z",
            "cancellation_snapshot_id":Uuid::new_v4()})
        .as_object()
        .unwrap()
        .clone();
        evidence.guard.incident_versions.push(
            crate::traits::relationship_timeline::IncidentVersionState {
                source_chain_id: chain,
                target_chain_id: chain,
                properties: edge,
            },
        );
        assert_eq!(
            coverage_end(&evidence.guard, evidence.as_of).unwrap(),
            evidence.valid_until
        );
        let properties = &mut evidence.guard.incident_versions[0].properties;
        properties.remove("cancelled_at");
        properties.remove("cancellation_snapshot_id");
        properties.insert("valid_to".into(), json!("2026-02-10T00:00:00Z"));
        assert_eq!(
            coverage_end(&evidence.guard, evidence.as_of).unwrap(),
            evidence.valid_until
        );
        evidence.guard.incident_versions[0]
            .properties
            .insert("valid_to".into(), json!("2026-02-09T00:00:00Z"));
        assert!(coverage_end(&evidence.guard, evidence.as_of).is_err());
        evidence.guard.incident_versions[0]
            .properties
            .insert("valid_to".into(), json!("2026-02-20T00:00:00Z"));
        assert_eq!(
            coverage_end(&evidence.guard, evidence.as_of).unwrap(),
            Some("2026-02-10T00:00:00Z".parse().unwrap())
        );
    }
    #[test]
    fn evidence_digest_ignores_refresh_revision_but_changes_with_source_state() {
        let mut evidence = fixture();
        let original = evidence_hash(&evidence).unwrap();
        evidence.guard.expected_revision = Some(Uuid::new_v4());
        assert_eq!(evidence_hash(&evidence).unwrap(), original);
        evidence
            .guard
            .entity_versions
            .get_mut(&evidence.guard.target_chain_id)
            .unwrap()[0]
            .insert("name".into(), json!("changed"));
        assert_ne!(evidence_hash(&evidence).unwrap(), original);
    }
}
