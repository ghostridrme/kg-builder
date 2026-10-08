//! Explicit vector coverage scans, kept out of the per-query retrieval path.
use crate::{filters::*, PreparedQuery};
use kg_core::{embedding, errors::BackendError, search::*};
use serde_json::{json, Map, Value};

pub fn embedding_readiness(r: &EmbeddingReadinessRequest) -> Result<PreparedQuery, BackendError> {
    r.validate()?;
    let (prefix, version) = match r.scope {
        SearchScope::Nodes => (
            format!(
                "MATCH (n:Entity) WHERE {} AND {} AND {} WITH n",
                scope("n", &r.filter),
                types("n", &r.filter),
                entity_visible("n", &r.filter)
            ),
            r.entity_text_version.as_str(),
        ),
        SearchScope::Relationships => (
            format!(
                "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity)
             WHERE a.org_id=$org_id AND b.org_id=$org_id AND r.org_id=$org_id AND {}
               AND (size($relationship_types)=0 OR r.name IN $relationship_types)
             MATCH (s:Entity {{org_id: $org_id, chain_id: r.source_chain_id}})
             WHERE {} AND {}
             MATCH (t:Entity {{org_id: $org_id, chain_id: r.target_chain_id}})
             WHERE {} AND {}
             WITH r, s, t WHERE ({} OR {})
             WITH DISTINCT r AS n",
                relationship_visible("r", &r.filter),
                scope("s", &r.filter),
                fact_endpoint_visible("s", &r.filter),
                scope("t", &r.filter),
                fact_endpoint_visible("t", &r.filter),
                types("s", &r.filter),
                types("t", &r.filter)
            ),
            embedding::RELATIONSHIP_TEXT_VERSION,
        ),
        SearchScope::Communities => return crate::community_search::readiness(r),
        SearchScope::Snapshots => unreachable!("validated scope"),
    };
    let mut parameters = params(&r.filter, &None, 1);
    parameters["model"] = json!(r.model);
    parameters["dimensions"] = json!(r.dimensions);
    parameters["text_version"] = json!(version);
    let statement = format!(
        "{prefix}
        RETURN count(n) AS eligible,
        count(CASE WHEN n.embedding IS NULL THEN 1 END) AS missing,
        count(CASE WHEN n.embedding_model=$model AND n.embedding_text_version=$text_version
          AND size(n.embedding)=$dimensions
          AND all(x IN n.embedding WHERE x IS NOT NULL AND x*0=0)
          AND any(x IN n.embedding WHERE x<>0) THEN 1 END) AS compatible"
    );
    Ok(PreparedQuery {
        statement,
        parameters,
    })
}

pub fn decode_embedding_readiness(
    rows: Vec<Map<String, Value>>,
) -> Result<EmbeddingReadiness, BackendError> {
    let invalid = || BackendError::Deserialization("invalid embedding coverage counts".into());
    if rows.len() != 1 {
        return Err(invalid());
    }
    let count = |name| {
        rows[0]
            .get(name)
            .and_then(Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(invalid)
    };
    let eligible = count("eligible")?;
    let missing = count("missing")?;
    let compatible = count("compatible")?;
    let incompatible = eligible
        .checked_sub(missing)
        .and_then(|n| n.checked_sub(compatible))
        .ok_or_else(invalid)?;
    Ok(EmbeddingReadiness {
        eligible,
        compatible,
        missing,
        incompatible,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn relationship_coverage_uses_effective_time_and_keeps_entity_heads() {
        let request = EmbeddingReadinessRequest {
            entity_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: SearchFilter {
                org_id: "test".into(),
                ..Default::default()
            },
            scope: SearchScope::Relationships,
            model: "test".into(),
            dimensions: 2,
        };
        let query = embedding_readiness(&request).unwrap();
        assert!(query
            .statement
            .contains("datetime(r.valid_from) <= datetime($relationship_now)"));
        assert!(!query.statement.contains("r.is_latest"));
        assert!(query.statement.contains("s.is_latest = true"));
        assert!(query.parameters["relationship_now"].is_string());
        let population = crate::neo4j::vector::vector_population(
            &request.filter,
            "test",
            true,
            kg_core::embedding::TEXT_VERSION,
            &None,
        );
        assert!(population
            .statement
            .contains("datetime(r.valid_from) <= datetime($relationship_now)"));
        assert!(!population.statement.contains("r.is_latest"));
    }

    #[test]
    fn readiness_distinguishes_empty_missing_and_incompatible() {
        for (eligible, missing, compatible, incompatible) in
            [(0, 0, 0, 0), (3, 1, 1, 1), (3, 0, 3, 0)]
        {
            let result = decode_embedding_readiness(vec![
                json!({"eligible":eligible,"missing":missing,"compatible":compatible})
                    .as_object()
                    .unwrap()
                    .clone(),
            ])
            .unwrap();
            assert_eq!(result.incompatible, incompatible);
        }
        assert!(
            decode_embedding_readiness(vec![json!({"eligible":1,"missing":1,"compatible":1})
                .as_object()
                .unwrap()
                .clone()])
            .is_err()
        );
    }
}
