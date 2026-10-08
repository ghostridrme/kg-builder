//! Prepare validated vectors for a planned batch before its transaction starts.

use super::mutation_planning::{preflight, target_settings};
use async_trait::async_trait;
use kg_core::{
    embedding,
    errors::StageError,
    runtime::{
        embedding_cache::EmbeddingCacheKey,
        stage_output::{PlannedEmbeddingWrite, PreparedBatchOutput},
        RuntimeContext, StageOutput,
    },
    traits::{graph_backend::GraphEmbedding, Stage},
};
use std::collections::HashMap;

const STAGE: &str = "batch_embedding";
/// Prepare all required vectors, reusing compatible results across retries.
pub struct BatchEmbeddingStage;

#[async_trait]
impl Stage for BatchEmbeddingStage {
    fn processing_version(&self) -> String {
        kg_core::traits::stage::processing_version(
            "batch-embedding-v1",
            &[
                kg_core::embedding::RELATIONSHIP_TEXT_VERSION,
                kg_core::entity_summary::SUMMARY_TEXT_VERSION,
            ],
        )
    }

    fn name(&self) -> &str {
        STAGE
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::PlannedBatch, StageKind::PreparedBatch)]
    }
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::PlannedBatch(mut planned) = input else {
            return Err(invalid("expected a planned batch"));
        };
        preflight(&planned, ctx)?;
        let prepare = async {
            let mut ready = Vec::with_capacity(planned.embeddings.len());
            let mut texts = Vec::new();
            let mut positions = HashMap::new();
            for target in &planned.embeddings {
                let settings = target_settings(target, ctx);
                let key = match &target.write {
                    PlannedEmbeddingWrite::EntityVersion { .. } => EmbeddingCacheKey::new(
                        &ctx.org_id,
                        &target.namespace,
                        &settings,
                        &target.text,
                    ),
                    PlannedEmbeddingWrite::DerivedSummary { .. } => EmbeddingCacheKey::for_summary(
                        &ctx.org_id,
                        &target.namespace,
                        &settings,
                        &target.text,
                    ),
                    PlannedEmbeddingWrite::RelationshipVersion => {
                        EmbeddingCacheKey::for_relationship(
                            &ctx.org_id,
                            &target.namespace,
                            &settings,
                            &target.text,
                        )
                    }
                };
                let values = target
                    .reuse
                    .as_ref()
                    .filter(|vector| vector.matches(&settings, &target.content_hash))
                    .map(|vector| vector.values.clone())
                    .or_else(|| ctx.incoming_embeddings.get(&key));
                if let Some(values) = &values {
                    embedding::validate_vectors(&settings, 1, std::slice::from_ref(values))
                        .map_err(|_| invalid("reused embedding is invalid"))?;
                } else {
                    positions.entry(target.text.as_str()).or_insert_with(|| {
                        texts.push(target.text.as_str());
                        texts.len() - 1
                    });
                }
                ready.push((key, settings, values));
            }
            kg_core::telemetry::embedding_reuse(
                ready
                    .iter()
                    .filter(|(_, _, values)| values.is_some())
                    .count(),
            );
            let generated =
                embedding::embed(ctx, &texts)
                    .await
                    .map_err(|error| StageError::StepFailed {
                        stage: STAGE.into(),
                        step: "embed".into(),
                        cause: error.to_string(),
                        retriable: error.is_transient(),
                    })?;
            // embed validates the complete response set before any newly computed vector is cached.
            for (target, (key, settings, values)) in planned.embeddings.iter().zip(ready) {
                let values =
                    values.unwrap_or_else(|| generated[positions[target.text.as_str()]].clone());
                ctx.incoming_embeddings
                    .record(key, &settings, values.clone());
                let embedding = GraphEmbedding {
                    model: settings.model,
                    values,
                };
                planned
                    .batch
                    .mutations
                    .push(super::mutation_planning::embedding_mutation(
                        target, embedding,
                    ));
            }
            super::mutation_planning::validate_commit(&planned.batch, ctx)
                .map_err(|error| invalid(&error.to_string()))?;
            tracing::debug!(
                batch_kind = planned.batch.batch.kind.label(),
                batch_index = planned.batch.batch.index,
                vectors = planned.embeddings.len(),
                generated_texts = texts.len(),
                "batch embeddings prepared"
            );
            Ok(StageOutput::PreparedBatch(PreparedBatchOutput {
                batch: planned.batch,
            }))
        };
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(StageError::Cancelled { stage: STAGE.into() }),
            result = async {
                if let Some(deadline) = ctx.identity_deadline {
                    tokio::time::timeout_at(deadline, prepare).await.map_err(|_| StageError::StepFailed {
                        stage: STAGE.into(), step: "identity_budget".into(), cause: "embedding deadline exceeded before commit".into(), retriable: true,
                    })?
                } else { prepare.await }
            } => result,
        }
    }
}

fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: STAGE.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests;
