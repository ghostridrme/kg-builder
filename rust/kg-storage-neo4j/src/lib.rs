//! # kg-storage-neo4j
//!
//! Neo4j GraphBackend implementation for KG Pipeline.
//! Typed reads, receipted atomic commits, and search share the Cypher crate.

pub mod driver;
pub mod settings;

pub use driver::{FaultInjection, Neo4jGraphBackend, Neo4jOptions};
pub use settings::Neo4jSettings;

mod explorer;
mod rule_evidence;
mod rule_store;
mod search;

mod cancellation;

mod telemetry;
pub use telemetry::{storage_metrics, StorageOperationMetrics};

mod profile_registry;

mod connector_checkpoint;
