/// Preserve source-declared child relationships.
pub mod declared_relationship_extraction;
/// Evidence-backed text relationship extraction.
pub(crate) mod observation_evidence;
/// Deterministic current-property reference extraction.
pub mod reference_extraction;
/// Model decisions for ambiguous reference targets.
pub mod reference_resolution;
/// Stored relationship baselines for batch planning.
pub mod relationship_resolution;

pub use declared_relationship_extraction::DeclaredRelationshipExtractionStage;
pub use reference_extraction::ReferenceExtractionStage;
pub use reference_resolution::ReferenceResolutionStage;
pub use relationship_resolution::EdgeResolutionStage;

mod relationship_matching;

mod relationship_schema;

pub mod relationship_attribute_enrichment;
pub use relationship_attribute_enrichment::RelationshipAttributeEnrichmentStage;

/// Optional semantic naming of generic relationships; off by default.
pub(crate) mod relationship_timeline;

mod relationship_termination;

pub mod relationship_timestamp_extraction;
pub use relationship_timestamp_extraction::RelationshipTimestampExtractionStage;

pub mod relationship_contradiction_resolution;
pub use relationship_contradiction_resolution::RelationshipContradictionResolutionStage;
