//! On-demand Saga summaries through the ingestion engine's receipted commit path.
use crate::query::SagaSummarizer;
use kg_core::{
    errors::{ConfigError, PipelineError},
    pipeline::PipelineOutput,
    policy::{
        EdgeAmbiguityMode, EdgeDiscoveryMode, EntityMatching, ExtractionMode, PipelinePolicy,
    },
    runtime::{entity_summary::SummaryMode, saga::SagaSummarySettings},
    saga::ThreadReference,
    traits::{EmbedBackend, GraphBackend, LlmBackend},
};
use kg_stages::{Backends, Engine, IngestionSettings, PipelineRecipe, SagaMaintenanceRequest};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The query service's summarizer, backed by a real engine.
pub struct EngineSummarizer(pub Arc<Engine>);

#[async_trait::async_trait]
impl SagaSummarizer for EngineSummarizer {
    async fn summarize(
        &self,
        org_id: &str,
        namespace: &str,
        saga_uuid: Uuid,
        run_id: Uuid,
        cancel: CancellationToken,
    ) -> Result<PipelineOutput, PipelineError> {
        self.0
            .summarize_saga(
                SagaMaintenanceRequest::new(
                    org_id,
                    namespace,
                    ThreadReference::Uuid { uuid: saga_uuid },
                )
                .with_run_id(run_id)
                .with_cancel(cancel),
            )
            .await
    }
}

/// An engine that only summarizes Sagas: entity-only recipe, heuristic policy,
/// no schema inference. `Model` mode needs a configured language model. Saga
/// briefs carry no vectors, so `Deterministic` mode runs with `EmbedDisabled`
/// when no embedding provider is configured; the stage would fail explicitly
/// if it ever asked for one.
///
/// `max_summary_bytes` bounds the stored brief. Deterministic briefs preserve
/// the complete source evidence, so a Saga whose members exceed the budget fails
/// explicitly rather than being abridged; the storage cap is
/// `kg_core::saga::MAX_SUMMARY_BYTES`.
pub fn summary_engine(
    graph: Arc<dyn GraphBackend>,
    llm: Arc<dyn LlmBackend>,
    embedder: Arc<dyn EmbedBackend>,
    mode: SummaryMode,
    max_summary_bytes: usize,
) -> Result<Engine, ConfigError> {
    Engine::new(
        Backends {
            graph,
            llm_extraction: llm.clone(),
            llm_disambiguation: None,
            llm_edge_discovery: None,
            llm_default: llm,
            decisions: None,
            embedder,
            ontology_store: None,
            schema_store: None,
        },
        IngestionSettings {
            recipe: PipelineRecipe::EntityOnly,
            saga_summary: SagaSummarySettings {
                enabled: true,
                mode,
                max_summary_bytes,
                ..Default::default()
            },
            policy: PipelinePolicy {
                extraction: ExtractionMode::Heuristic,
                matching: EntityMatching::Exact,
                edge_discovery: EdgeDiscoveryMode::Heuristic,
                edge_ambiguity: EdgeAmbiguityMode::Skip,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

/// Parse `KG_SUMMARY_MAX_BYTES`: the stored brief budget, defaulting to
/// the engine's 16 KiB and capped by the storage limit.
pub fn summary_budget(value: Option<&str>) -> Result<usize, String> {
    let Some(raw) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(SagaSummarySettings::default().max_summary_bytes);
    };
    raw.parse::<usize>()
        .ok()
        .filter(|n| (1..=kg_core::saga::MAX_SUMMARY_BYTES).contains(n))
        .ok_or_else(|| {
            format!(
                "KG_SUMMARY_MAX_BYTES must be 1..={}, not {raw:?}",
                kg_core::saga::MAX_SUMMARY_BYTES
            )
        })
}

/// Parse `KG_SAGA_SUMMARIES`: unset disables the feature.
pub fn summary_mode(value: Option<&str>) -> Result<Option<SummaryMode>, String> {
    match value.map(str::trim) {
        None | Some("") | Some("off") => Ok(None),
        Some("deterministic") => Ok(Some(SummaryMode::Deterministic)),
        Some("model") => Ok(Some(SummaryMode::Model)),
        Some(other) => Err(format!(
            "KG_SAGA_SUMMARIES must be deterministic, model or off, not {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_budget_defaults_and_is_capped_by_storage() {
        assert_eq!(
            summary_budget(None).unwrap(),
            SagaSummarySettings::default().max_summary_bytes
        );
        assert_eq!(summary_budget(Some(" 65536 ")).unwrap(), 64 * 1024);
        assert!(summary_budget(Some("65537")).is_err());
        assert!(summary_budget(Some("0")).is_err());
        assert!(summary_budget(Some("lots")).is_err());
    }
}
