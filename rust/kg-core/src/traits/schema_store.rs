//! Adopt-once identity schemas keyed by organization, source, and type.
//! Callers validate proposals; changing adopted primary keys requires an identity migration.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::errors::BackendError;

/// An adopted identity schema for one `(org, source, entity_type)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredSchema {
    pub org_id: String,
    pub source: String,
    pub entity_type: String,
    /// Identity properties validated by the inference caller before adoption.
    /// Storage does not validate this public data type.
    pub primary_key_properties: Vec<String>,
    /// Properties the model flagged as likely references to other entities —
    /// retained as metadata; currently unused by edge discovery.
    #[serde(default)]
    pub fk_property_hints: Vec<String>,
    /// Properties the model flagged as volatile (timestamps, counters) —
    /// merged into entity-type volatile configuration by node validation.
    #[serde(default)]
    pub volatile_property_hints: Vec<String>,
    /// Serving model that proposed this schema.
    pub inferred_by: String,
    /// Marks an unsafe fallback schema. Ingestion rejects such records.
    #[serde(default)]
    pub degraded: bool,
}

/// Adopt-once schema storage. Persistence depends on the implementation;
/// the test-support store is process-local.
#[async_trait]
pub trait SchemaStore: Send + Sync + 'static {
    /// The adopted schema for `(org, source, entity_type)`, if any.
    async fn get(
        &self,
        org_id: &str,
        source: &str,
        entity_type: &str,
    ) -> Result<Option<InferredSchema>, BackendError>;

    /// Store the first schema and return the stored winner on concurrent proposals.
    async fn adopt(&self, schema: InferredSchema) -> Result<InferredSchema, BackendError>;
}
