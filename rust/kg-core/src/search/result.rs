//! Search results with provenance and operation diagnostics.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// A single enriched search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    /// Valid derived evidence; source summary remains in properties.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_summary: Option<super::SummaryView>,
    /// Entity version valid under the request's current/as-of filter.
    pub uuid: Uuid,
    /// Stable chain identity of the matched entity.
    pub chain_id: Uuid,
    pub entity_type: String,
    pub namespace: String,
    pub name: String,
    /// Retrieval, fused, model, or MMR score; distance/frequency sorting retain it.
    pub score: f32,
    /// Retrieval/reranking scores by method; scales differ and need not sum to `score`.
    pub score_breakdown: HashMap<String, f32>,
    /// Graph property map with the embedding vector removed.
    pub properties: serde_json::Value,
    /// Hops from traversal seeds or the distance-ranking center; None if not measured or reached.
    pub graph_distance: Option<usize>,
    /// Stored model-specific embedding, used internally for diversity ranking.
    #[serde(skip)]
    pub embedding: Option<crate::traits::graph_backend::GraphEmbedding>,
    /// Scoped distinct snapshots observing this chain; None when not loaded.
    pub observation_count: Option<usize>,
    /// Distinct source chains with an incoming relationship under the request filters;
    /// None when not loaded.
    pub dependent_count: Option<usize>,
    /// Stored `valid_from` of this version, when known; not the last property update.
    pub last_changed_at: Option<String>,
    /// Value of the graph `prop_owner` property, when present.
    pub owner: Option<String>,
}

/// Search results may be partial: inspect diagnostics as well as `truncated`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchResult {
    /// At least one contributing retrieval used approximate candidates. Visibility
    /// and scores remain exact; exhaustive fallbacks do not set this flag.
    pub approximate: bool,
    /// Entity-node hits, reranked and enriched.
    pub hits: Vec<SearchHit>,
    /// Relationship evidence or explicit relationship search results.
    pub relationships: Vec<super::RelationshipHit>,
    /// Source evidence or explicit snapshot search results.
    pub snapshots: Vec<super::SnapshotHit>,
    pub communities: Vec<super::CommunityHit>,
    /// Per-operation outcomes and timings, including degraded behavior.
    pub diagnostics: Vec<SearchDiagnostic>,
    /// Known matches were omitted by a limit: a retrieval, traversal, evidence, or
    /// fused candidate budget overflowed, or hydration dropped versions. Index
    /// approximation is reported by `approximate`, failures by diagnostics, and
    /// excerpt truncation on each snapshot. The final node limit alone does not set this flag.
    pub truncated: bool,
    /// Distinct recalled chains before the fused budget, hydration, and final limit;
    /// not the total number of database matches.
    pub total_candidates: usize,
    /// Successful retrieval methods and explicitly requested evidence scopes, including empty results.
    pub methods_used: Vec<String>,
    /// Search execution time in milliseconds.
    pub duration_ms: u64,
}

/// Observable outcome of a retrieval, reranking, or enrichment operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDiagnostic {
    pub operation: String,
    /// Engine-emitted status: success, failed, or truncated.
    pub status: String,
    /// Elapsed milliseconds, including permit waits for external calls.
    /// Zero for count-only summaries; overlapping operations must not be summed.
    pub duration_ms: u64,
    /// Operation-specific count: returned or excluded items, as named by the operation.
    pub count: usize,
    /// Known items removed by this operation; excludes unknown matches beyond a storage limit.
    pub dropped_count: usize,
    /// Safe category only; backend messages and request contents are never included.
    pub failure: Option<SearchFailure>,
    /// Backend timeout budget, retained when it differs from the operation budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Provider retry advice, without including its response body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// Failure categories shared by response diagnostics and tracing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchFailure {
    Timeout,
    Connection,
    Authentication,
    RateLimited,
    InvalidResponse,
    InvalidRequest,
    Unavailable,
    NotFound,
    Conflict,
    Transaction,
    Serialization,
    Other,
}

impl From<&crate::errors::BackendError> for SearchFailure {
    fn from(error: &crate::errors::BackendError) -> Self {
        use crate::errors::BackendError;
        match error {
            BackendError::Timeout(_) => Self::Timeout,
            BackendError::Connection(_) => Self::Connection,
            BackendError::Auth(_) => Self::Authentication,
            BackendError::RateLimited { .. } => Self::RateLimited,
            BackendError::Deserialization(_) => Self::InvalidResponse,
            BackendError::Query(_) => Self::InvalidRequest,
            BackendError::RelationshipHistoryLimit { .. } => Self::InvalidResponse,
            BackendError::Unavailable(_) => Self::Unavailable,
            BackendError::NotConfigured(_) | BackendError::AttemptBudgetExhausted => {
                Self::InvalidRequest
            }
            BackendError::NotFound(_) => Self::NotFound,
            BackendError::Conflict(_)
            | BackendError::CollectionOwnershipConflict(_)
            | BackendError::IdentityRevisionChanged => Self::Conflict,
            BackendError::Transaction(_) | BackendError::UnknownCommit(_) => Self::Transaction,
            BackendError::Serialization(_) => Self::Serialization,
            BackendError::IncompleteResponse | BackendError::Refused | BackendError::Other(_) => {
                Self::Other
            }
        }
    }
}
