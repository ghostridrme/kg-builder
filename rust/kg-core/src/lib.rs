//! Shared domain contracts and pure helpers. Callers supply storage and model backends.

pub mod config;
pub mod embedding;
pub mod embedding_rebuild;
pub mod entity_summary;
pub mod entity_type_config;
pub mod enums;
pub mod errors;
pub mod identity;
pub mod models;
pub mod pipeline;
pub mod policy;
pub mod runtime;
pub mod sanitize;
pub mod search;
pub mod telemetry;
pub mod tenant;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod traits;

/// Convenience re-exports for the most commonly used types.
pub mod prelude {
    pub use crate::enums::{EntityLifecycle, VersioningDecision};
    pub use crate::errors::{BackendError, ConfigError, PipelineError};
    pub use crate::identity::IdentityHash;
    pub use crate::models::{
        ConnectorEntity, EntityEdge, EntityNode, PropertyValue, SnapshotInput, SnapshotNode,
    };
    pub use crate::pipeline::PipelineOutput;
    pub use crate::runtime::{RuntimeContext, RuntimeContextBuilder};
    pub use crate::search::{SearchConfig, SearchHit, SearchResult};
    pub use crate::traits::{EmbedBackend, GraphBackend, LlmBackend, Stage};
}

pub mod community;
pub mod saga;

pub mod profiles;

/// Thread contracts; the legacy module remains for persisted-format compatibility.
pub mod thread {
    pub use crate::saga::*;
}
