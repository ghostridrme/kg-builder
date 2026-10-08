/// Generic input shape and scope checks.
pub mod input_validation;
/// Local snapshot preparation.
pub mod snapshot_prepare;
pub use input_validation::InputValidationStage;
pub use snapshot_prepare::SnapshotPreparationStage;
/// Configured child entities and their declared relationships.
pub mod child_extract;
/// Scoped retrieval of selected previous observations.
pub mod context_retrieval;
/// Preserve declared observations as raw entity drafts.
pub mod direct_extract;
/// Classify changes after identity matching.
pub mod entity_versioning;
/// Semantic matching of entities that match no identity key.
pub mod fuzzy_match;
/// LLM entity extraction from raw content.
/// Deterministic property rules.
pub mod property_normalizer;
pub use entity_versioning::EntityVersioningStage;
/// Authoritative identity resolution.
pub mod resolve;
pub(crate) mod schema_inference;
/// Preparation of declared drafts for graph identity resolution.
pub mod structured_prepare;

pub use context_retrieval::ContextRetrievalStage;
pub use direct_extract::DirectExtractionStage;
pub use fuzzy_match::FuzzyMatchStage;
pub use resolve::ResolveNodesStage;
pub use structured_prepare::connector_entity_to_node;

/// Explicit routing between structured and content extraction.
pub mod entity_extraction;
pub use entity_extraction::EntityExtractionStage;

/// One optional missing-entity check over discovered mentions.
pub(crate) mod extraction_support;

mod matching_candidates;
mod matching_local;

mod matching_batch;
mod matching_decision;

mod matching_graph;

/// Schema-guided attributes after identity selection.
pub mod entity_attribute_enrichment;
pub use entity_attribute_enrichment::EntityAttributeEnrichmentStage;
