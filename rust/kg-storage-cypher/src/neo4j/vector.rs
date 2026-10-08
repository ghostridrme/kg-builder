//! Vector indexes select candidates; ordinary predicates and cosine verify them.
use crate::{filters::*, PreparedQuery};
use kg_core::{errors::BackendError, search::*};
use serde_json::{Map, Value};

/// Largest candidate budget one index call may request.
pub const MAX_VECTOR_CANDIDATES: usize = 16_384;

pub fn indexed_nodes(
    request: &NodeSearch,
    candidates: usize,
) -> Result<PreparedQuery, BackendError> {
    if candidates == 0 || candidates > MAX_VECTOR_CANDIDATES {
        return Err(BackendError::Query(
            "invalid vector candidate budget".into(),
        ));
    }
    if !matches!(request.query, NodeQuery::Similarity(_)) {
        return Err(BackendError::Query(
            "indexed retrieval requires an embedding".into(),
        ));
    }
    super::nodes::nodes_with_index(request, Some(candidates))
}

pub fn indexed_relationships(
    request: &RelationshipSimilarity,
    candidates: usize,
) -> Result<PreparedQuery, BackendError> {
    if candidates == 0 || candidates > MAX_VECTOR_CANDIDATES {
        return Err(BackendError::Query(
            "invalid vector candidate budget".into(),
        ));
    }
    super::evidence::relationship_similarity_with_index(request, Some(candidates))
}

/// Sparse scopes use exact scoring so unrelated indexed records cannot crowd them out.
pub fn vector_population(
    filter: &SearchFilter,
    model: &str,
    relationships: bool,
    entity_text_version: &str,
    chains: &Option<Vec<uuid::Uuid>>,
) -> PreparedQuery {
    let mut parameters = params(filter, chains, 1);
    parameters["model"] = model.into();
    parameters["text_version"] = if relationships {
        kg_core::embedding::RELATIONSHIP_TEXT_VERSION
    } else {
        entity_text_version
    }
    .into();
    let statement = if relationships {
        format!("MATCH (physical_s:Entity)-[r:RELATES_TO]->(physical_t:Entity) WHERE r.org_id=$org_id AND r.embedding_model=$model AND r.embedding_text_version=$text_version AND physical_s.org_id=$org_id AND physical_t.org_id=$org_id AND {} AND (size($relationship_types)=0 OR r.name IN $relationship_types) MATCH (s:Entity {{org_id: $org_id, chain_id: r.source_chain_id}}) WHERE {} AND {} MATCH (t:Entity {{org_id: $org_id, chain_id: r.target_chain_id}}) WHERE {} AND {} WITH r, s, t WHERE ({} OR {}) WITH DISTINCT r LIMIT 513 RETURN count(r) AS count", relationship_visible("r",filter),scope("s",filter),fact_endpoint_visible("s",filter),scope("t",filter),fact_endpoint_visible("t",filter),types("s",filter),types("t",filter))
    } else {
        format!("MATCH (n:Entity) WHERE {} AND {} AND {} AND n.embedding_model=$model AND n.embedding_text_version=$text_version AND ($chains IS NULL OR n.chain_id IN $chains) WITH n LIMIT 513 RETURN count(n) AS count",scope("n",filter),types("n",filter),entity_visible("n",filter))
    };
    PreparedQuery {
        statement,
        parameters,
    }
}

/// What the index returned before scope filtering: the raw neighbor count and
/// the lowest raw cosine. Every row of an indexed statement carries both; a
/// statement whose candidates were all filtered out returns one row with a null
/// record so the statistics survive.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorStats {
    pub raw_count: usize,
    /// Cosine of the weakest raw neighbor; None when the index returned nothing.
    pub frontier: Option<f64>,
}
impl VectorStats {
    /// The weakest raw neighbor scored below the cutoff, so no unreturned neighbor
    /// can qualify. This is an index judgement, not proof of exact completeness,
    /// and a raw page shorter than the budget never counts as exhaustion.
    pub fn exhausted(&self, min_score: f32) -> bool {
        self.raw_count > 0
            && self
                .frontier
                .is_some_and(|score| score < f64::from(min_score) - 1e-5)
    }
}

/// Separate the index statistics from the candidate rows; `key` is the column
/// that is null on the statistics-only row.
pub fn vector_stats(
    rows: Vec<Map<String, Value>>,
    key: &str,
) -> Result<(Vec<Map<String, Value>>, VectorStats), BackendError> {
    let invalid = || BackendError::Deserialization("invalid vector index statistics".into());
    let first = rows.first().ok_or_else(invalid)?;
    let raw_count = first
        .get("raw_count")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(invalid)?;
    let frontier = match first.get("frontier") {
        Some(Value::Null) | None => None,
        Some(value) => Some(
            value
                .as_f64()
                .filter(|f| f.is_finite())
                .ok_or_else(invalid)?,
        ),
    };
    let rows = rows
        .into_iter()
        .filter(|row| row.get(key).is_some_and(|value| !value.is_null()))
        .collect();
    Ok((
        rows,
        VectorStats {
            raw_count,
            frontier,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn statistics_survive_an_empty_candidate_page_and_short_pages_are_not_exhaustion() {
        let (rows, stats) = vector_stats(
            vec![row(
                json!({"n": null, "score": null, "raw_count": 4096, "frontier": 0.31}),
            )],
            "n",
        )
        .unwrap();
        assert!(rows.is_empty());
        assert_eq!(stats.raw_count, 4096);
        assert!(stats.exhausted(0.5));
        assert!(!stats.exhausted(0.3));
        assert!(!stats.exhausted(0.0));
        let (rows, stats) = vector_stats(
            vec![
                row(json!({"n": {"uuid": "a"}, "score": 0.9, "raw_count": 7, "frontier": -0.2})),
                row(json!({"n": {"uuid": "b"}, "score": 0.8, "raw_count": 7, "frontier": -0.2})),
            ],
            "n",
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(stats.raw_count, 7);
        assert!(stats.exhausted(0.0));
        let (_, empty) = vector_stats(
            vec![row(json!({"uuid": null, "raw_count": 0, "frontier": null}))],
            "uuid",
        )
        .unwrap();
        assert_eq!(empty.frontier, None);
        assert!(!empty.exhausted(-1.0));
        assert!(vector_stats(vec![], "n").is_err());
        assert!(vector_stats(vec![row(json!({"n": null, "frontier": 0.1}))], "n").is_err());
    }
}
