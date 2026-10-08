//! Runtime dependencies, caller-owned state, and typed pipeline hand-offs.

pub mod context;
pub mod debug_export;
pub mod decisions;
pub mod execution;
pub mod history;
pub mod stage_output;

pub use context::{RuntimeContext, RuntimeContextBuilder};
pub use execution::ExecutionConfig;
pub use stage_output::StageOutput;

/// Frozen custom entity and relationship schema contracts.
pub mod schemas;

/// Bounded text discovery and omission policy.
pub mod extraction;

/// Scoped text mentions before graph identity preparation.
pub mod entity_drafts;

pub mod matching;

pub mod matching_cache;
pub mod reference_cache;
/// Bounded settings and durable audit contracts of ambiguous reference decisions.
pub mod reference_resolution;
pub mod relationship_naming_cache;
pub mod rule_learning;

pub mod embedding_cache;

pub mod entity_summary;

pub mod saga;

pub mod community;
