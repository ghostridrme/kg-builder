//! Historical vectors are guarded by the complete semantic state, not liveness.
use kg_core::traits::GraphMutation;
use serde_json::{json, Value};

pub(super) fn parameters(mutation: &GraphMutation) -> Value {
    let GraphMutation::SetEntityVersionEmbedding {
        uuid,
        expected_properties,
        embedding,
        text_version,
        content_hash,
    } = mutation
    else {
        unreachable!("historical entity embedding parameters")
    };
    json!({"uuid": uuid, "expected": expected_properties, "values": embedding.values,
        "model": embedding.model, "text_version": text_version, "content_hash": content_hash})
}

pub(super) const WRITE: &str = "UNWIND $rows AS row
    MATCH (n:Entity {org_id:$org,uuid:row.uuid})
    SET n.uuid=n.uuid
    WITH n,row,[key IN keys(n) WHERE key IN ['name','entity_type','namespace','version','summary','structural_hash']
        OR key STARTS WITH 'prop_' OR key STARTS WITH 'property_type_'] AS actual
    WITH n,row,coalesce(size(actual)=size(keys(row.expected))
        AND all(key IN actual WHERE n[key]=row.expected[key]),false) AS matches
    FOREACH (_ IN CASE WHEN matches THEN [1] ELSE [] END |
        SET n.embedding=row.values,n.embedding_model=row.model,
            n.embedding_text_version=row.text_version,n.embedding_content_hash=row.content_hash)
    RETURN matches AS ok,NOT matches AS embedding_conflict";

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::traits::graph_backend::GraphEmbedding;
    use uuid::Uuid;

    #[test]
    fn historical_writes_group_without_weakening_the_content_guard() {
        let mutation = |uuid| GraphMutation::SetEntityVersionEmbedding {
            uuid,
            expected_properties:
                json!({"name":"api", "entity_type":"Service", "namespace":"prod", "version":1})
                    .as_object()
                    .unwrap()
                    .clone(),
            embedding: GraphEmbedding {
                model: "model".into(),
                values: vec![1.0, 0.0],
            },
            text_version: kg_core::embedding::TEXT_VERSION.into(),
            content_hash: "hash".into(),
        };
        let batch = vec![mutation(Uuid::new_v4()), mutation(Uuid::new_v4())];
        let grouped = crate::mutations("org", &batch).unwrap();
        let single = crate::mutations_ungrouped("org", &batch).unwrap();
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].expected_rows, 2);
        assert_eq!(single.len(), 2);
        assert_eq!(grouped[0].statement, single[0].statement);
        assert_eq!(
            grouped[0].parameters["rows"][0],
            single[0].parameters["rows"][0]
        );
        assert!(WRITE.contains("SET n.uuid=n.uuid"));
        assert!(WRITE.contains("size(actual)=size(keys(row.expected))"));
        assert!(!WRITE.contains("is_latest"));
        let scopes = crate::identity_revision::mutation_scopes("org", &batch).unwrap();
        assert_eq!(scopes.uuids.len(), 2);
    }
}
