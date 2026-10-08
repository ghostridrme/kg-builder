//! Awaited planning, vector preparation and receipted commits.
pub mod batch_embedding;
pub(crate) mod incident_deletion_planning;
pub(crate) mod mutation_plan;
pub mod mutation_planning;
pub(crate) mod node_mutation_planning;
pub mod persist;
pub(crate) mod reconciliation_planning;
pub(crate) mod relationship_embedding_planning;
pub(crate) mod relationship_mutation_planning;
pub mod saga_association;

pub use batch_embedding::BatchEmbeddingStage;
pub use mutation_planning::MutationPlanningStage;
pub use persist::PersistStage;

#[cfg(test)]
mod test_support;

mod summary_targets;

mod owned_child_deletion;
