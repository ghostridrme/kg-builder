use crate::PreparedQuery;
use kg_core::{
    errors::BackendError,
    traits::{
        graph_reads::MAX_LOOKUP_KEYS, ReferenceRuleSourceQuery, UnresolvedReferenceEntry,
        UnresolvedReferenceQuery, UnresolvedReferenceRecord,
    },
};
use serde_json::{json, Map, Value};
use uuid::Uuid;

/// Page live entities covered by one rule with all scope predicates applied
/// before the limit. The extra row is a completeness lookahead.
pub fn rule_sources(
    org: &str,
    request: &ReferenceRuleSourceQuery,
) -> Result<PreparedQuery, BackendError> {
    request.validate(org)?;
    Ok(PreparedQuery {
        statement:
            "MATCH (n:Entity {org_id:$org,is_latest:true,source:$source,entity_type:$entity_type}) \
            WHERE n.deleted_at IS NULL \
              AND ($namespace IS NULL OR n.namespace=$namespace) \
              AND ($after IS NULL OR n.chain_id>$after) \
            RETURN n ORDER BY n.chain_id LIMIT $limit"
                .into(),
        parameters: json!({
            "org": org,
            "source": request.source,
            "entity_type": request.entity_type,
            "namespace": request.namespace,
            "after": request.after_chain.map(|value| value.to_string()),
            "limit": request.limit.saturating_add(1),
        }),
    })
}

pub fn read(org: &str, request: &UnresolvedReferenceQuery) -> Result<PreparedQuery, BackendError> {
    if org.trim().is_empty()
        || request.tokens.is_empty()
        || request.tokens.len() > MAX_LOOKUP_KEYS
        || request.limit == 0
        || request.limit > MAX_LOOKUP_KEYS
        || request.tokens.iter().any(|token| token.trim().is_empty())
        || request.after.as_ref().is_some_and(|cursor| {
            cursor.source_chain_id.is_nil()
                || cursor.slot.trim().is_empty()
                || cursor.token.trim().is_empty()
        })
    {
        return Err(BackendError::Query(
            "invalid confirmed reference dependency request".into(),
        ));
    }
    Ok(PreparedQuery {
        // The dependency index retains historical edges. Re-reading the live
        // owner decides whether its current payload still references this value.
        // Group by the exact cursor tuple before LIMIT; several edge versions
        // may provide the same dependency and must not consume separate rows.
        statement: "UNWIND $tokens AS token
            MATCH (d:ReferenceDependency {org_id:$org,token:token})
            MATCH (owner:Entity {org_id:$org,chain_id:d.source_chain_id,is_latest:true})
            WHERE owner.deleted_at IS NULL AND owner.merged_into IS NULL
            WITH DISTINCT d.source_chain_id AS source_chain_id,d.slot AS slot,token,
                owner.last_seen_snapshot_id AS snapshot_id,coalesce(owner.last_seen_at,owner.valid_from) AS recorded_at
            WHERE NOT $has_after OR source_chain_id > $after_source OR
                (source_chain_id=$after_source AND slot>$after_slot) OR
                (source_chain_id=$after_source AND slot=$after_slot AND token>$after_token)
            RETURN source_chain_id,slot,token,'confirmed' AS reason,snapshot_id,recorded_at
            ORDER BY source_chain_id,slot,token LIMIT $limit".into(),
        parameters: json!({
            "org": org,
            "tokens": request.tokens,
            "limit": request.limit + 1,
            "has_after": request.after.is_some(),
            "after_source": request.after.as_ref().map(|c| c.source_chain_id.to_string()).unwrap_or_default(),
            "after_slot": request.after.as_ref().map(|c| c.slot.as_str()).unwrap_or(""),
            "after_token": request.after.as_ref().map(|c| c.token.as_str()).unwrap_or(""),
        }),
    })
}

pub fn decode(row: &Map<String, Value>) -> Result<UnresolvedReferenceRecord, BackendError> {
    let invalid = || BackendError::Deserialization("invalid confirmed reference dependency".into());
    let text = |key: &str| row.get(key).and_then(Value::as_str).ok_or_else(invalid);
    let record = UnresolvedReferenceRecord {
        source_chain_id: Uuid::parse_str(text("source_chain_id")?).map_err(|_| invalid())?,
        slot: text("slot")?.to_owned(),
        entry: UnresolvedReferenceEntry {
            token: text("token")?.to_owned(),
            reason: text("reason")?.to_owned(),
            snapshot_id: row
                .get("snapshot_id")
                .and_then(Value::as_str)
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|_| invalid())?,
            recorded_at: text("recorded_at")?.parse().map_err(|_| invalid())?,
        },
    };
    record.entry.validate()?;
    Ok(record)
}

/// Maintain the derived dependency index in the edge mutation transaction.
/// Entries retain their edge UUID across endpoint repoints and historical closure.
pub(crate) fn sync(prefix: &str) -> String {
    format!("{prefix}\n{SYNC}")
}

const SYNC: &str = "WITH r
    CALL {
        WITH r
        OPTIONAL MATCH (d:ReferenceDependency {org_id:r.org_id,edge_uuid:r.uuid})
        WHERE NOT d.token IN coalesce(r.reference_tokens,[]) OR r.reference_owner_chain_id IS NULL
        DELETE d
        RETURN count(d) AS removed_dependencies
    }
    CALL {
        WITH r
        WITH r WHERE r.reference_owner_chain_id IS NOT NULL AND r.reference_slot IS NOT NULL
        UNWIND coalesce(r.reference_tokens,[]) AS token
        WITH DISTINCT r,token ORDER BY token
        MERGE (d:ReferenceDependency {org_id:r.org_id,edge_uuid:r.uuid,token:token})
        SET d.source_chain_id=r.reference_owner_chain_id,d.slot=r.reference_slot
        RETURN count(d) AS dependencies
    }
    SET r.reference_dependency_version=1
    RETURN true AS ok";

/// Startup must not silently omit dependencies from pre-index relationships.
pub const READY: &str = "MATCH ()-[r:RELATES_TO]->() WHERE r.reference_owner_chain_id IS NOT NULL AND coalesce(r.reference_dependency_version,0)<>1 RETURN r.uuid AS missing LIMIT 1";

/// Bounded, restartable maintenance. The per-edge marker is advanced atomically
/// with its index entries, so interruption never declares an unfinished edge ready.
pub fn backfill(org: &str, limit: usize) -> Result<PreparedQuery, BackendError> {
    if org.trim().is_empty() || !(1..=500).contains(&limit) {
        return Err(BackendError::Query(
            "dependency backfill requires an organization and limit 1..=500".into(),
        ));
    }
    Ok(PreparedQuery {
        statement: format!("MATCH ()-[r:RELATES_TO {{org_id:$org}}]->() WHERE r.reference_owner_chain_id IS NOT NULL AND coalesce(r.reference_dependency_version,0)<>1 WITH r ORDER BY r.uuid LIMIT $limit SET r.uuid=r.uuid {SYNC}"),
        parameters: json!({"org":org,"limit":limit}),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_source_page_scopes_before_order_and_lookahead_limit() {
        let after = Uuid::new_v4();
        let query = rule_sources(
            "org-a",
            &ReferenceRuleSourceQuery {
                source: "cmdb".into(),
                namespace: Some("prod".into()),
                entity_type: "Change".into(),
                after_chain: Some(after),
                limit: 25,
            },
        )
        .unwrap();
        assert!(query
            .statement
            .contains("org_id:$org,is_latest:true,source:$source,entity_type:$entity_type"));
        assert!(query.statement.contains("n.deleted_at IS NULL"));
        assert!(query.statement.contains("n.namespace=$namespace"));
        assert!(query.statement.contains("n.chain_id>$after"));
        assert_eq!(query.parameters["org"], "org-a");
        assert_eq!(query.parameters["limit"], 26);
        assert_eq!(query.parameters["after"], after.to_string());
    }

    #[test]
    fn rule_source_page_rejects_unbounded_or_unscoped_requests() {
        let request = ReferenceRuleSourceQuery {
            source: "cmdb".into(),
            namespace: None,
            entity_type: "Change".into(),
            after_chain: None,
            limit: 0,
        };
        assert!(rule_sources("org-a", &request).is_err());
        assert!(rule_sources(
            "",
            &ReferenceRuleSourceQuery {
                limit: 1,
                ..request
            }
        )
        .is_err());
    }
}
