//! Supported recipes preserve the node commit barrier and relationship handoffs.

use std::sync::Arc;

use kg_core::errors::PipelineError;
use kg_core::traits::Stage;
use kg_pipeline::runner::PipelineRunner;
use kg_pipeline::runner::PipelineRunnerConfig;

/// Select processing scope independently of source model permissions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineRecipe {
    /// Commit entities without discovering or reconciling absent relationships.
    /// Explicit entity deletion still closes its incident relationships.
    EntityOnly,
    /// Process declared relationships and property references without text discovery.
    /// Exact identity matching; ambiguous references remain unresolved.
    #[default]
    DeclaredAndReferences,
    /// Unsupported recipe; rejected by this deterministic build.
    Full,
}

impl PipelineRecipe {
    pub(crate) fn add_relationship_stages(self, runner: PipelineRunner) -> PipelineRunner {
        if self == Self::EntityOnly {
            return runner;
        }
        let runner = runner
            .edge_stage(Arc::new(crate::DeclaredRelationshipExtractionStage))
            .edge_stage(Arc::new(crate::ReferenceExtractionStage))
            .edge_stage(Arc::new(crate::ReferenceResolutionStage));
        runner
            .edge_stage(Arc::new(crate::RelationshipAttributeEnrichmentStage))
            .edge_stage(Arc::new(crate::RelationshipTimestampExtractionStage))
            .relationship_resolution_stage(Arc::new(crate::EdgeResolutionStage))
            .relationship_resolution_stage(Arc::new(
                crate::RelationshipContradictionResolutionStage,
            ))
    }
}

/// Build the deterministic ingestion composition.
pub fn ingestion_pipeline(config: PipelineRunnerConfig) -> Result<PipelineRunner, PipelineError> {
    ingestion_pipeline_with_recipe(config, PipelineRecipe::DeclaredAndReferences)
}

/// Build a supported recipe without exposing arbitrary stage permutations.
pub fn ingestion_pipeline_with_recipe(
    config: PipelineRunnerConfig,
    recipe: PipelineRecipe,
) -> Result<PipelineRunner, PipelineError> {
    if recipe == PipelineRecipe::Full {
        return Err(PipelineError::StateValidation {
            stage: "composition".into(),
            message: "kg-builder supports entity_only and declared_and_references recipes".into(),
        });
    }
    config.validate()?;
    let runner = PipelineRunner::new()
        .with_config(config)
        .node_stage(Arc::new(crate::InputValidationStage) as Arc<dyn Stage>)
        .node_stage(Arc::new(crate::SnapshotPreparationStage) as Arc<dyn Stage>)
        .node_stage(Arc::new(crate::ContextRetrievalStage) as Arc<dyn Stage>)
        .node_stage(Arc::new(crate::EntityExtractionStage) as Arc<dyn Stage>)
        .resolution_stage(Arc::new(crate::ResolveNodesStage) as Arc<dyn Stage>)
        .resolution_stage(Arc::new(crate::FuzzyMatchStage) as Arc<dyn Stage>)
        .resolution_stage(Arc::new(crate::EntityAttributeEnrichmentStage) as Arc<dyn Stage>)
        .resolution_stage(Arc::new(crate::node::EntityVersioningStage) as Arc<dyn Stage>)
        .flush(Arc::new(crate::MutationPlanningStage) as Arc<dyn Stage>)
        .flush(Arc::new(crate::ThreadAssociationStage) as Arc<dyn Stage>)
        .flush(Arc::new(crate::BatchEmbeddingStage) as Arc<dyn Stage>)
        .flush(Arc::new(crate::PersistStage) as Arc<dyn Stage>);
    let runner = recipe
        .add_relationship_stages(runner)
        .relationship_coverage(false);
    runner.validate_topology()?;
    Ok(runner)
}
