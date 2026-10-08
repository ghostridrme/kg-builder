//! Select relationship versions needing vectors without calling the embedding provider.
use super::{
    mutation_plan::{Plan, STAGE},
    relationship_mutation_planning::RelationshipEmbeddingTarget,
};
use kg_core::{
    embedding,
    errors::StageError,
    runtime::{stage_output::PlannedEmbedding, RuntimeContext},
    traits::graph_backend::GraphEmbedding,
};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;
pub(crate) async fn plan_relationship_embeddings(
    versions: &[RelationshipEmbeddingTarget],
    ctx: &RuntimeContext,
    plan: &mut Plan,
) -> Result<(), StageError> {
    if !ctx.embedder.is_configured() {
        return Ok(());
    }
    let mut stored = HashMap::new();
    let mut pairs: Vec<(Uuid, Uuid)> = versions.iter().filter_map(|r| r.stored_pair).collect();
    pairs.sort_unstable();
    pairs.dedup();
    for chunk in pairs.chunks(kg_core::traits::graph_reads::MAX_LOOKUP_KEYS) {
        let records = ctx
            .graph
            .find_edges(
                ctx.org_id.as_ref(),
                &kg_core::traits::EdgeLookup::HeadsByChainPairs {
                    pairs: chunk.to_vec(),
                },
            )
            .await
            .map_err(|e| StageError::StepFailed {
                stage: STAGE.into(),
                step: "read_relationship_embeddings".into(),
                cause: e.to_string(),
                retriable: e.is_transient(),
            })?;
        stored.extend(records.into_iter().map(|r| (r.uuid, r.stored)));
    }
    for relationship in versions {
        let text =
            embedding::relationship_representation(&relationship.name, &relationship.description);
        let hash = embedding::content_hash(&text);
        let reusable = stored.get(&relationship.uuid).is_some_and(|p| {
            p.get("embedding_model").and_then(Value::as_str) == Some(ctx.embedding.model.as_str())
                && p.get("embedding_text_version").and_then(Value::as_str)
                    == Some(embedding::RELATIONSHIP_TEXT_VERSION)
                && p.get("embedding_content_hash").and_then(Value::as_str) == Some(hash.as_str())
                && p.get("embedding")
                    .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                    .is_some_and(|values| {
                        values.len() == ctx.embedding.dimension
                            && GraphEmbedding {
                                model: ctx.embedding.model.clone(),
                                values,
                            }
                            .validate()
                            .is_ok()
                    })
        });
        if !reusable {
            plan.embeddings.push(PlannedEmbedding {
                write: kg_core::runtime::stage_output::PlannedEmbeddingWrite::RelationshipVersion,
                uuid: relationship.uuid,
                namespace: relationship.namespace.clone(),
                text,
                content_hash: hash,
                text_version: embedding::RELATIONSHIP_TEXT_VERSION.into(),
                reuse: None,
            });
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "live-tests"))]
mod tests;
