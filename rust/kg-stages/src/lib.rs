//! # kg-stages
//!
//! Pipeline stage implementations for the KG Pipeline knowledge graph engine.
//!
//! ## Node pass (in execution order)
//! - `InputValidationStage` — generic input shape and scope checks
//! - `SnapshotPreparationStage` — one observation record with source time and metadata
//! - `ContextRetrievalStage` — selected scoped historical evidence
//! - `EntityExtractionStage` — supplied entities only; text-only input is rejected
//! - `DirectExtractionStage` — declared observations to unchanged drafts
//! - `ResolveNodesStage` — authoritative identity matching across the chunk
//! - `FuzzyMatchStage` — shared identity handoff; the engine admits exact matching only
//! - `EntityAttributeEnrichmentStage` — evidence-backed custom attributes
//! - `EntityVersioningStage` — classify observations under the existing temporal policy
//!
//! ## Relationship pass
//! - `DeclaredRelationshipExtractionStage` — configured child relationships
//! - `ReferenceExtractionStage` — deterministic references under namespace policy
//! - `ReferenceResolutionStage` — preserves unresolved ambiguity without model calls
//! - `RelationshipAttributeEnrichmentStage` — missing custom attributes from current evidence
//! - `EdgeResolutionStage` — independent relationship identities and stored history
//!
//! ## Persistence
//! - `PersistStage` — one receipted atomic commit per batch; the only graph writer
//!
//! ## Entry point
//! - `engine::Engine` — the supported stage composition behind one awaited
//!   `ingest` call for the CLI, APIs, schedulers, and connectors

#![warn(missing_docs)]

/// Supported pipeline recipes with fixed dependency ordering.
pub mod composition;
/// Edge stages: discovery and resolution.
pub mod edge;
/// The public ingestion entry point over the supported stage composition.
pub mod engine;
/// The persistence stage: receipted atomic commits.
pub mod flush;
/// Awaited, resumable administrative operations that run outside ingestion.
pub mod maintenance;
/// Validation of model answers before they become facts.
pub mod model_output;
/// Node stages: validation, extraction, resolution, fuzzy matching.
pub mod node;
mod schemas;

pub use composition::{ingestion_pipeline, ingestion_pipeline_with_recipe, PipelineRecipe};
pub use edge::{
    DeclaredRelationshipExtractionStage, EdgeResolutionStage, ReferenceExtractionStage,
    ReferenceResolutionStage, RelationshipAttributeEnrichmentStage,
    RelationshipContradictionResolutionStage, RelationshipTimestampExtractionStage,
};
pub use engine::{
    Backends, CommunityMaintenanceRequest, Engine, IngestionRequest, IngestionSettings,
    RuleMaintenanceOutput, RuleMaintenanceRequest, SagaMaintenanceRequest,
    ThreadMaintenanceRequest,
};
pub use flush::{BatchEmbeddingStage, MutationPlanningStage, PersistStage};
pub use node::SnapshotPreparationStage;
pub use node::{
    ContextRetrievalStage, DirectExtractionStage, EntityAttributeEnrichmentStage,
    EntityExtractionStage, EntityVersioningStage, FuzzyMatchStage, InputValidationStage,
    ResolveNodesStage,
};

fn next_version(version: u32, stage: &str) -> Result<u32, kg_core::errors::StageError> {
    version
        .checked_add(1)
        .ok_or_else(|| kg_core::errors::StageError::StateValidation {
            stage: stage.into(),
            message: "version counter is exhausted".into(),
        })
}

pub use flush::saga_association::ThreadAssociationStage;

mod profiles;

#[cfg(test)]
mod tests {
    pub(crate) mod prompt_contract;
}
