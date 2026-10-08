//! Engine composition for awaited learned-rule lifecycle repair.

use kg_core::{
    errors::ConfigError,
    policy::{
        EdgeAmbiguityMode, EdgeDiscoveryMode, EntityMatching, ExtractionMode, PipelinePolicy,
    },
    traits::{EmbedBackend, GraphBackend, LlmDisabled},
};
use kg_stages::{Backends, Engine, IngestionSettings, PipelineRecipe};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait RuleRepairService: Send + Sync {
    async fn repair(
        &self,
        request: kg_stages::RuleMaintenanceRequest,
    ) -> Result<kg_stages::RuleMaintenanceOutput, kg_core::errors::PipelineError>;
}

#[async_trait::async_trait]
impl RuleRepairService for Engine {
    async fn repair(
        &self,
        request: kg_stages::RuleMaintenanceRequest,
    ) -> Result<kg_stages::RuleMaintenanceOutput, kg_core::errors::PipelineError> {
        self.repair_rule(request).await
    }
}

/// Build the deterministic reference pipeline used after activation/revocation.
/// It never calls a model; ambiguous references remain unresolved.
pub fn repair_engine(
    graph: Arc<dyn GraphBackend>,
    embedder: Arc<dyn EmbedBackend>,
) -> Result<Engine, ConfigError> {
    let llm = Arc::new(LlmDisabled);
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
            recipe: PipelineRecipe::DeclaredAndReferences,
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
