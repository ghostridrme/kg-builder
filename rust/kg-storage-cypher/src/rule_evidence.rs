//! Bounded reads for committed reference decisions used by rule learning.
//! Every query applies organization and producer isolation before its limit.
use crate::PreparedQuery;
use kg_core::errors::BackendError;
use serde_json::json;

/// Live reference edges produced by one source, as positive examples.
pub fn reference_examples(
    org: &str,
    source: &str,
    limit: usize,
) -> Result<PreparedQuery, BackendError> {
    if org.trim().is_empty() || source.trim().is_empty() || limit == 0 {
        return Err(BackendError::Query(
            "evidence scan requires an organization, source and positive limit".into(),
        ));
    }
    Ok(PreparedQuery {
        statement: "MATCH (s:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,producer_source:$source,is_latest:true}]->(t:Entity {org_id:$org}) \
             MATCH (owner:Entity {org_id:$org,chain_id:r.reference_owner_chain_id,is_latest:true}) \
             WITH s,r,t,owner,CASE WHEN s.chain_id=owner.chain_id THEN t ELSE s END AS target \
             WHERE owner.deleted_at IS NULL AND target.is_latest=true AND target.deleted_at IS NULL \
               AND r.cancelled_at IS NULL AND r.deleted_at IS NULL \
               AND r.invalid_at IS NULL AND r.valid_to IS NULL \
               AND r.reference_slot IS NOT NULL AND r.target_key_group IS NOT NULL \
               AND r.producer_namespace IS NOT NULL AND r.reference_decision IS NULL \
               AND r.reference_owner_namespace=owner.namespace \
             RETURN owner.chain_id AS source_chain_id, owner.uuid AS source_version_uuid, owner.entity_type AS source_entity_type, owner.namespace AS owner_namespace, \
                    r.reference_slot AS slot, r.evidence_location AS evidence_location, r.target_key_group AS target_key_group, \
                    r.reference_tokens AS reference_tokens, r.reference_component_paths AS component_paths, \
                    target.entity_type AS target_type, target.chain_id AS target_chain_id, \
                    r.org_id AS org_id, r.producer_source AS producer_source, \
                    r.producer_namespace AS producer_namespace, \
                    owner.org_id AS source_org_id, target.org_id AS target_org_id \
             ORDER BY source_chain_id, target_chain_id LIMIT $limit"
            .into(),
        parameters: json!({"org": org, "source": source, "limit": limit as i64}),
    })
}

/// Strip a `EntityType.` prefix from a reference slot to recover the path.
pub fn slot_path(slot: &str, entity_type: &str) -> String {
    slot.strip_prefix(entity_type)
        .and_then(|rest| rest.strip_prefix('.'))
        .unwrap_or(slot)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_are_parameterized_and_bounded() {
        let q = reference_examples("org", "cmdb", 100).unwrap();
        assert_eq!(q.parameters["org"], json!("org"));
        assert_eq!(q.parameters["source"], json!("cmdb"));
        assert_eq!(q.parameters["limit"], json!(100));
        assert!(q.statement.contains("LIMIT $limit"));
        assert!(q.statement.contains("r.org_id AS org_id"));
        assert!(q.statement.contains("is_latest:true"));
        assert!(q.statement.contains("target.is_latest=true"));
        assert!(!q.statement.contains("cmdb"));
        assert!(reference_examples("org", "  ", 10).is_err());
        assert!(reference_examples(" ", "cmdb", 10).is_err());
    }

    #[test]
    fn slot_path_strips_the_entity_prefix() {
        assert_eq!(
            slot_path("CmdbChange.owning_group", "CmdbChange"),
            "owning_group"
        );
        assert_eq!(slot_path("A.b.c", "A"), "b.c");
        // A slot that does not carry the expected prefix is returned unchanged.
        assert_eq!(slot_path("owning_group", "CmdbChange"), "owning_group");
    }
}
