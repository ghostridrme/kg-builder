//! Scoped keyword, semantic and graph retrieval over caller-supplied storage.
#![warn(missing_docs)]
mod community;
mod concurrency;
/// Request orchestration and operation deadlines.
pub mod engine;
mod evidence;
/// Rank fusion, diversity selection, and model relevance ranking.
pub mod rerank;
/// Scoped graph traversal.
pub mod retrieval;
pub use engine::SearchEngine;
