//! Scoped entity embedding queries and result decoding.

use crate::PreparedQuery;
use kg_core::{
    errors::BackendError,
    traits::graph_backend::{validate_embedding, EntityEmbeddingHit, GraphEmbedding},
};

const GET_ENTITY_EMBEDDING: &str = "MATCH (n:Entity {org_id: $org_id, uuid: $uuid, is_latest: true}) WHERE n.deleted_at IS NULL AND n.embedding IS NOT NULL AND n.embedding_model IS NOT NULL RETURN n.embedding AS values, n.embedding_model AS model";

const SEARCH_ENTITY_EMBEDDINGS: &str = "MATCH (n:Entity {org_id: $org_id, is_latest: true}) WHERE n.deleted_at IS NULL AND n.embedding_model = $model AND n.embedding_text_version=$text_version AND size(n.embedding) = size($query) AND (size($namespaces) = 0 OR n.namespace IN $namespaces) AND (size($types) = 0 OR n.entity_type IN $types) WITH n, n.embedding AS vector WITH n, vector, sqrt(reduce(norm = 0.0, v IN vector | norm + v*v)) AS norm WHERE norm > 0 WITH n, reduce(dot = 0.0, i IN range(0, size($query)-1) | dot + vector[i]*$query[i]) / (norm * $query_norm) AS score WHERE score >= $min_score RETURN n, score ORDER BY score DESC, n.uuid LIMIT $limit";

pub fn entity_embedding_record(org_id: &str, uuid: uuid::Uuid) -> PreparedQuery {
    PreparedQuery {
        statement: "MATCH (n:Entity {org_id:$org_id,uuid:$uuid,is_latest:true}) WHERE n.deleted_at IS NULL AND n.valid_to IS NULL AND n.merged_into IS NULL RETURN properties(n) AS record".into(),
        parameters: serde_json::json!({"org_id":org_id,"uuid":uuid}),
    }
}
pub fn get_entity_embedding(org_id: &str, uuid: uuid::Uuid) -> PreparedQuery {
    PreparedQuery {
        statement: GET_ENTITY_EMBEDDING.into(),
        parameters: serde_json::json!({"org_id": org_id, "uuid": uuid.to_string()}),
    }
}
pub fn decode_entity_embedding(
    row: serde_json::Map<String, serde_json::Value>,
) -> Result<GraphEmbedding, BackendError> {
    serde_json::from_value(serde_json::Value::Object(row))
        .map_err(|e| BackendError::Deserialization(e.to_string()))
}
#[allow(clippy::too_many_arguments)]
pub fn search_entity_embeddings(
    query: &[f32],
    model: &str,
    org_id: &str,
    namespaces: Option<&[&str]>,
    entity_types: Option<&[&str]>,
    limit: usize,
    min_score: f32,
    text_version: &str,
) -> Result<PreparedQuery, BackendError> {
    validate_embedding(model, query)?;
    kg_core::embedding::validate_entity_text_version(text_version)?;
    if !min_score.is_finite() {
        return Err(BackendError::Query("min_score must be finite".into()));
    }
    let limit =
        i64::try_from(limit).map_err(|_| BackendError::Query("limit is too large".into()))?;
    Ok(PreparedQuery {
        statement: SEARCH_ENTITY_EMBEDDINGS.into(),
        parameters: serde_json::json!({"org_id":org_id,"model":model,"text_version":text_version,"query":query,"query_norm":query.iter().map(|v|(*v as f64).powi(2)).sum::<f64>().sqrt(),"namespaces":namespaces.unwrap_or(&[]),"types":entity_types.unwrap_or(&[]),"min_score":min_score,"limit":limit}),
    })
}
pub fn decode_entity_embedding_hit(
    r: serde_json::Map<String, serde_json::Value>,
) -> Result<EntityEmbeddingHit, BackendError> {
    let id = |key: &str| {
        r.get(key)
            .and_then(serde_json::Value::as_str)
            .and_then(|v| uuid::Uuid::parse_str(v).ok())
            .ok_or_else(|| BackendError::Deserialization(format!("entity missing {key}")))
    };
    Ok(EntityEmbeddingHit {
        uuid: id("uuid")?,
        chain_id: id("chain_id")?,
        score: r
            .get("score")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| BackendError::Deserialization("similarity score missing".into()))?
            .clamp(-1.0, 1.0) as f32,
        payload: Some(serde_json::Value::Object(r)),
    })
}
