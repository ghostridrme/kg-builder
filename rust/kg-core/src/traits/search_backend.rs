//! Shared typed search contract for graph storage adapters.
use crate::errors::BackendError;
use async_trait::async_trait;

/// Typed search with adapter-enforced organization, filters, ordering, and budgets.
/// Unsupported operations must return errors, not empty success.
///
/// Adapters may enforce their own per-call deadline. The engine's operation
/// deadline wraps every call, so the effective limit is the smaller of the two;
/// a timeout error carries the budget that actually expired.
#[async_trait]
pub trait SearchBackend: Send + Sync + 'static {
    async fn search_communities(
        &self,
        _request: &crate::search::CommunitySearch,
        _indexed: bool,
    ) -> Result<crate::search::SearchPage<crate::search::CommunityHit>, BackendError> {
        Err(BackendError::NotConfigured(
            "community search is unsupported".into(),
        ))
    }

    async fn search_entity_summaries(
        &self,
        _request: &crate::search::SummarySearch,
        _indexed: bool,
    ) -> Result<crate::search::NodePage, BackendError> {
        Err(BackendError::NotConfigured(
            "derived summary search is unsupported".into(),
        ))
    }
    async fn summary_readiness(
        &self,
        _request: &crate::search::EmbeddingReadinessRequest,
    ) -> Result<crate::search::SummaryReadiness, BackendError> {
        Err(BackendError::NotConfigured(
            "derived summary readiness is unsupported".into(),
        ))
    }

    /// Optional approximate acceleration. Exact implementations satisfy this contract too.
    /// Adapters must preserve scope, compatibility, temporal visibility and exact scores.
    async fn search_nodes_indexed(
        &self,
        request: &crate::search::NodeSearch,
    ) -> Result<crate::search::NodePage, BackendError> {
        self.search_nodes(request).await
    }
    async fn search_relationships_indexed(
        &self,
        request: &crate::search::RelationshipSimilarity,
    ) -> Result<crate::search::SearchPage<crate::search::RelationshipHit>, BackendError> {
        self.search_relationship_similarity(request).await
    }

    /// Explicit readiness inspection; unsupported adapters fail rather than claim coverage.
    async fn embedding_readiness(
        &self,
        _request: &crate::search::EmbeddingReadinessRequest,
    ) -> Result<crate::search::EmbeddingReadiness, BackendError> {
        Err(BackendError::Unavailable(
            "embedding readiness is unsupported".into(),
        ))
    }
    /// Scoped typed entity retrieval. Unsupported adapters fail explicitly.
    async fn search_nodes(
        &self,
        _request: &crate::search::NodeSearch,
    ) -> Result<crate::search::NodePage, BackendError> {
        Err(BackendError::Unavailable(
            "typed entity search is unsupported".into(),
        ))
    }
    /// Search relationship facts or expand the neighborhood of entity chains.
    async fn search_relationships(
        &self,
        _request: &crate::search::EvidenceSearch,
    ) -> Result<crate::search::SearchPage<crate::search::RelationshipHit>, BackendError> {
        Err(BackendError::Unavailable(
            "relationship search is unsupported".into(),
        ))
    }
    /// Scoped fact-vector recall with compatible model, dimensions and text version.
    async fn search_relationship_similarity(
        &self,
        _request: &crate::search::RelationshipSimilarity,
    ) -> Result<crate::search::SearchPage<crate::search::RelationshipHit>, BackendError> {
        Err(BackendError::Unavailable(
            "semantic relationship search is unsupported".into(),
        ))
    }
    /// Relationship facts attached to ranked anchors, newest first per anchor.
    async fn attached_relationships(
        &self,
        _request: &crate::search::AttachedEvidence,
    ) -> Result<
        crate::search::SearchPage<crate::search::Attached<crate::search::RelationshipHit>>,
        BackendError,
    > {
        Err(BackendError::Unavailable(
            "attached relationship evidence is unsupported".into(),
        ))
    }
    /// Source snapshots attached to ranked anchors, newest capture first per anchor.
    async fn attached_snapshots(
        &self,
        _request: &crate::search::AttachedEvidence,
    ) -> Result<
        crate::search::SearchPage<crate::search::Attached<crate::search::SnapshotHit>>,
        BackendError,
    > {
        Err(BackendError::Unavailable(
            "attached snapshot evidence is unsupported".into(),
        ))
    }
    /// Search source snapshots or fetch evidence for entity chains.
    async fn search_snapshots(
        &self,
        _request: &crate::search::EvidenceSearch,
    ) -> Result<crate::search::SearchPage<crate::search::SnapshotHit>, BackendError> {
        Err(BackendError::Unavailable(
            "snapshot search is unsupported".into(),
        ))
    }
}
