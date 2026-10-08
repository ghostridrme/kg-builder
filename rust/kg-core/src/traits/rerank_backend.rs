//! Model-independent relevance ranking contract.
use crate::errors::BackendError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Maximum UTF-8 bytes in one complete candidate, including labels and metadata.
pub const MAX_CANDIDATE_BYTES: usize = 16_384;

/// Candidate with stable identity and untrusted retrieval text.
/// The caller bounds text before sending it to a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankCandidate {
    /// Stable entity chain or evidence record identity.
    pub id: Uuid,
    pub text: String,
}
/// Model score for one candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankScore {
    /// Identity supplied in the request.
    pub id: Uuid,
    /// Finite relevance score, higher means more relevant.
    pub score: f32,
}
/// Pluggable relevance model. Implementations must return each candidate exactly once.
#[async_trait]
pub trait RerankBackend: Send + Sync {
    /// Evaluate query and candidate text together, preserving candidate IDs.
    /// Output order is unspecified; callers match by ID and validate finite scores.
    async fn rank(
        &self,
        query: &str,
        candidates: &[RankCandidate],
    ) -> Result<Vec<RankScore>, BackendError>;
}
