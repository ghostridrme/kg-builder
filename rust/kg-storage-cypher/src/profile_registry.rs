//! Internal profile records are not domain entities. Every lookup is organization scoped.
use crate::PreparedQuery;
use kg_core::{
    errors::BackendError,
    profiles::{validate_org, FrozenProfile, ProfileRef},
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
fn parameters(org: &str, reference: &ProfileRef) -> Result<Value, BackendError> {
    validate_org(org)?;
    reference.validate()?;
    let scope = serde_json::to_vec(&(org, &reference.profile_id, reference.revision))
        .map_err(|_| BackendError::Serialization("profile scope".into()))?;
    Ok(
        json!({"org":org,"id":reference.profile_id,"revision":reference.revision,"scope":format!("{:x}",Sha256::digest(scope))}),
    )
}
pub fn put(org: &str, value: &FrozenProfile) -> Result<PreparedQuery, BackendError> {
    value.validate()?;
    let mut params = parameters(org, &value.document.reference())?;
    params["digest"] = json!(value.digest);
    params["document"] = json!(serde_json::to_string(value)
        .map_err(|_| BackendError::Serialization("profile document".into()))?);
    Ok(PreparedQuery{statement:"MERGE (p:IngestionProfile {scope_id:$scope}) ON CREATE SET p.org_id=$org,p.profile_id=$id,p.revision=$revision,p.document=$document,p.digest=$digest WITH p WHERE p.org_id=$org AND p.profile_id=$id AND p.revision=$revision RETURN p.document AS document,p.digest AS stored_digest".into(),parameters:params})
}
pub fn get(org: &str, reference: &ProfileRef) -> Result<PreparedQuery, BackendError> {
    Ok(PreparedQuery{statement:"MATCH (p:IngestionProfile {scope_id:$scope,org_id:$org,profile_id:$id,revision:$revision}) RETURN p.document AS document,p.digest AS stored_digest".into(),parameters:parameters(org,reference)?})
}
pub fn list(
    org: &str,
    after: Option<&ProfileRef>,
    limit: usize,
) -> Result<PreparedQuery, BackendError> {
    kg_core::traits::profile_registry::validate_page(org, after, limit)?;
    Ok(PreparedQuery{statement:"MATCH (p:IngestionProfile {org_id:$org}) WHERE $after_id IS NULL OR p.profile_id>$after_id OR (p.profile_id=$after_id AND p.revision>$after_revision) RETURN p.profile_id AS profile_id,p.revision AS revision,p.digest AS digest ORDER BY p.profile_id,p.revision LIMIT $limit".into(),parameters:json!({"org":org,"after_id":after.map(|a|&a.profile_id),"after_revision":after.map(|a|a.revision),"limit":limit+1})})
}
pub fn decode(row: &Map<String, Value>) -> Result<FrozenProfile, BackendError> {
    let raw = row
        .get("document")
        .and_then(Value::as_str)
        .filter(|s| s.len() <= 300_000)
        .ok_or_else(|| BackendError::Deserialization("invalid profile record".into()))?;
    let value: FrozenProfile = serde_json::from_str(raw)
        .map_err(|_| BackendError::Deserialization("invalid profile record".into()))?;
    value.validate()?;
    if row.get("stored_digest").and_then(Value::as_str) != Some(value.digest.as_str()) {
        return Err(BackendError::Deserialization(
            "profile stored digest mismatch".into(),
        ));
    }
    Ok(value)
}

pub fn decode_entry(
    row: &Map<String, Value>,
) -> Result<kg_core::traits::profile_registry::ProfileEntry, BackendError> {
    let invalid = || BackendError::Deserialization("invalid profile list entry".into());
    let reference = ProfileRef {
        profile_id: row
            .get("profile_id")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?
            .into(),
        revision: row
            .get("revision")
            .and_then(Value::as_u64)
            .ok_or_else(invalid)?,
    };
    reference.validate()?;
    let digest = row
        .get("digest")
        .and_then(Value::as_str)
        .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(invalid)?
        .into();
    Ok(kg_core::traits::profile_registry::ProfileEntry { reference, digest })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_registry_queries_are_scoped_and_corrupt_digest_fails() {
        let document: kg_core::profiles::Profile = serde_json::from_value(
            json!({"format_version":1,"profile_id":"p","revision":1,"mode":"open","ontology":{}}),
        )
        .unwrap();
        let frozen = document.freeze().unwrap();
        let query = put("org-with-quote-'", &frozen).unwrap();
        assert!(!query.statement.contains("org-with-quote"));
        assert_eq!(query.parameters["org"], "org-with-quote-'");
        let mut row=json!({"document":serde_json::to_string(&frozen).unwrap(),"stored_digest":frozen.digest}).as_object().unwrap().clone();
        assert_eq!(decode(&row).unwrap(), frozen);
        row.insert("stored_digest".into(), json!("0".repeat(64)));
        assert!(decode(&row).is_err());
        assert!(list("org", None, 101).is_err());
    }
}
