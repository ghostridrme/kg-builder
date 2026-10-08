//! Shared limits include permit waiting in the caller's operation deadline.
use async_trait::async_trait;
use kg_core::telemetry::{OperationKind, QueueGuard};
use kg_core::{errors::BackendError, search::*, traits::*};
use std::sync::Arc;
use tokio::sync::Semaphore;

pub(crate) struct Limited<T: ?Sized> {
    inner: Arc<T>,
    permits: Arc<Semaphore>,
}
impl<T: ?Sized> Limited<T> {
    pub(crate) fn new(inner: Arc<T>, permits: Arc<Semaphore>) -> Self {
        Self { inner, permits }
    }
    async fn permit(&self, kind: OperationKind) -> tokio::sync::SemaphorePermit<'_> {
        let mut waiting = QueueGuard::new(kind);
        let permit = self
            .permits
            .acquire()
            .await
            .expect("search semaphore stays open");
        waiting.acquired();
        permit
    }
}

#[async_trait]
impl SearchBackend for Limited<dyn SearchBackend> {
    async fn search_communities(
        &self,
        request: &CommunitySearch,
        indexed: bool,
    ) -> Result<SearchPage<CommunityHit>, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_communities(request, indexed).await
    }

    async fn search_entity_summaries(
        &self,
        request: &SummarySearch,
        indexed: bool,
    ) -> Result<NodePage, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_entity_summaries(request, indexed).await
    }
    async fn summary_readiness(
        &self,
        request: &EmbeddingReadinessRequest,
    ) -> Result<SummaryReadiness, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.summary_readiness(request).await
    }

    async fn search_nodes_indexed(&self, request: &NodeSearch) -> Result<NodePage, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_nodes_indexed(request).await
    }
    async fn search_relationships_indexed(
        &self,
        request: &RelationshipSimilarity,
    ) -> Result<SearchPage<RelationshipHit>, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_relationships_indexed(request).await
    }
    async fn embedding_readiness(
        &self,
        request: &EmbeddingReadinessRequest,
    ) -> Result<EmbeddingReadiness, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.embedding_readiness(request).await
    }
    async fn search_nodes(&self, request: &NodeSearch) -> Result<NodePage, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_nodes(request).await
    }
    async fn search_relationships(
        &self,
        request: &EvidenceSearch,
    ) -> Result<SearchPage<RelationshipHit>, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_relationships(request).await
    }
    async fn search_relationship_similarity(
        &self,
        request: &RelationshipSimilarity,
    ) -> Result<SearchPage<RelationshipHit>, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_relationship_similarity(request).await
    }
    async fn attached_relationships(
        &self,
        request: &AttachedEvidence,
    ) -> Result<SearchPage<Attached<RelationshipHit>>, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.attached_relationships(request).await
    }
    async fn attached_snapshots(
        &self,
        request: &AttachedEvidence,
    ) -> Result<SearchPage<Attached<SnapshotHit>>, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.attached_snapshots(request).await
    }
    async fn search_snapshots(
        &self,
        request: &EvidenceSearch,
    ) -> Result<SearchPage<SnapshotHit>, BackendError> {
        let _permit = self.permit(OperationKind::GraphRead).await;
        self.inner.search_snapshots(request).await
    }
}
#[async_trait]
impl EmbedBackend for Limited<dyn EmbedBackend> {
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
        let _permit = self.permit(OperationKind::Embedding).await;
        self.inner.embed_batch(texts).await
    }
    fn dimension(&self) -> usize {
        self.inner.dimension()
    }
    fn max_batch_size(&self) -> usize {
        self.inner.max_batch_size()
    }
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
}
#[async_trait]
impl RerankBackend for Limited<dyn RerankBackend> {
    async fn rank(
        &self,
        query: &str,
        candidates: &[RankCandidate],
    ) -> Result<Vec<RankScore>, BackendError> {
        let _permit = self.permit(OperationKind::Rerank).await;
        self.inner.rank(query, candidates).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Communities;
    #[async_trait]
    impl SearchBackend for Communities {
        async fn search_communities(
            &self,
            request: &CommunitySearch,
            _: bool,
        ) -> Result<SearchPage<CommunityHit>, BackendError> {
            Ok(SearchPage::bounded(vec![], request.limit))
        }
    }

    #[tokio::test]
    async fn the_limiter_forwards_overridden_default_methods_and_releases_permits() {
        let permits = Arc::new(Semaphore::new(1));
        let inner: Arc<dyn SearchBackend> = Arc::new(Communities);
        let limited = Limited::new(inner, permits.clone());
        let request = CommunitySearch {
            filter: SearchFilter {
                org_id: "acme".into(),
                ..Default::default()
            },
            query: NodeQuery::ByChain,
            uuids: Some(vec![uuid::Uuid::from_u128(1)]),
            limit: 10,
            min_score: 0.0,
            member_limit: 0,
        };
        // The trait default answers NotConfigured; success proves the override was reached.
        assert!(limited.search_communities(&request, false).await.is_ok());
        assert_eq!(permits.available_permits(), 1);
    }
}
