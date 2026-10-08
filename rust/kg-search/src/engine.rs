//! Search orchestration: scoped recall, graph expansion, ranking, and evidence.
use crate::{
    rerank::{fuse_versions, maximal_marginal_relevance},
    retrieval::traversal::neighborhood,
};
use kg_core::{
    errors::BackendError,
    search::*,
    traits::{
        graph_backend::GraphEmbedding, EmbedBackend, RankCandidate, RerankBackend, SearchBackend,
    },
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures::FutureExt;
use tokio::time::Instant;

pub(crate) type QueryEmbedding<'a> = futures::future::Shared<
    futures::future::BoxFuture<'a, (Option<GraphEmbedding>, Vec<SearchDiagnostic>)>,
>;

/// Rebuild a typed error from a recorded failure so callers keep the category,
/// the budget that expired, and retry advice without any backend text.
pub(crate) fn typed_failure(
    diagnostic: Option<&SearchDiagnostic>,
    message: &str,
    timeout_ms: u64,
) -> BackendError {
    let message = message.to_owned();
    match diagnostic.and_then(|d| d.failure) {
        Some(SearchFailure::Timeout) => {
            BackendError::Timeout(diagnostic.and_then(|d| d.timeout_ms).unwrap_or(timeout_ms))
        }
        Some(SearchFailure::Authentication) => BackendError::Auth(message),
        Some(SearchFailure::Connection) => BackendError::Connection(message),
        Some(SearchFailure::RateLimited) => BackendError::RateLimited {
            retry_after_ms: diagnostic.and_then(|d| d.retry_after_ms).unwrap_or(0),
        },
        Some(SearchFailure::InvalidResponse) => BackendError::Deserialization(message),
        Some(SearchFailure::InvalidRequest) => BackendError::Query(message),
        Some(SearchFailure::NotFound) => BackendError::NotFound(message),
        Some(SearchFailure::Conflict) => BackendError::Conflict(message),
        Some(SearchFailure::Transaction) => BackendError::Transaction(message),
        Some(SearchFailure::Serialization) => BackendError::Serialization(message),
        Some(SearchFailure::Other) => BackendError::Other(message),
        _ => BackendError::Unavailable(message),
    }
}

fn retrieval_failure(diagnostics: &[SearchDiagnostic], timeout_ms: u64) -> BackendError {
    typed_failure(
        diagnostics.iter().find(|d| d.failure.is_some()),
        "no requested retrieval operation succeeded",
        timeout_ms,
    )
}

/// Graph-backed search with caller-supplied storage and optional models.
/// Clones share limits of eight storage calls and four embedding/ranking calls.
#[derive(Clone)]
pub struct SearchEngine {
    entity_embedding_fields: kg_core::embedding::EntityEmbeddingFields,
    graph: Arc<dyn SearchBackend>,
    model_permits: Arc<tokio::sync::Semaphore>,
    embedder: Option<Arc<dyn EmbedBackend>>,
    reranker: Option<Arc<dyn RerankBackend>>,
    /// Shared by clones so every handle sees the same cached queries.
    query_cache: Arc<QueryEmbeddingCache>,
}

/// Default number of distinct query embeddings kept in process.
pub const QUERY_EMBEDDING_CACHE_CAPACITY: usize = 4_096;

/// Bounded in-process cache of query embeddings keyed by embedding model and the
/// normalised query text. Repeated questions skip the provider round trip; the
/// oldest entry leaves when the cache is full. Ingestion caches by content hash
/// separately and is unaffected.
/// (embedding model, normalised query).
type QueryKey = (String, String);
/// Cached vectors plus their insertion order for eviction.
type QueryEntries = (
    HashMap<QueryKey, Vec<f32>>,
    std::collections::VecDeque<QueryKey>,
);

struct QueryEmbeddingCache {
    capacity: usize,
    entries: std::sync::Mutex<QueryEntries>,
    hits: std::sync::atomic::AtomicUsize,
    misses: std::sync::atomic::AtomicUsize,
}

/// Hit and miss counts plus the number of cached queries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueryEmbeddingCacheStats {
    /// Queries answered from the cache without a provider call.
    pub hits: usize,
    /// Queries that had to be embedded by the provider.
    pub misses: usize,
    /// Distinct (model, normalised query) pairs currently cached.
    pub entries: usize,
}

impl QueryEmbeddingCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Default::default(),
            hits: Default::default(),
            misses: Default::default(),
        }
    }
    /// Case, surrounding whitespace and repeated whitespace do not change a question.
    fn normalise(query: &str) -> String {
        query
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>()
            .join(" ")
    }
    fn get(&self, model: &str, query: &str) -> Option<Vec<f32>> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.capacity == 0 {
            return None;
        }
        let key = (model.to_owned(), Self::normalise(query));
        let found = self.entries.lock().ok()?.0.get(&key).cloned();
        match &found {
            Some(_) => self.hits.fetch_add(1, Relaxed),
            None => self.misses.fetch_add(1, Relaxed),
        };
        found
    }
    fn insert(&self, model: &str, query: &str, values: &[f32]) {
        if self.capacity == 0 {
            return;
        }
        let key = (model.to_owned(), Self::normalise(query));
        if let Ok(mut guard) = self.entries.lock() {
            let (map, order) = &mut *guard;
            if map.insert(key.clone(), values.to_vec()).is_none() {
                order.push_back(key);
            }
            while map.len() > self.capacity {
                match order.pop_front() {
                    Some(oldest) => {
                        map.remove(&oldest);
                    }
                    None => break,
                }
            }
        }
    }
    fn stats(&self) -> QueryEmbeddingCacheStats {
        use std::sync::atomic::Ordering::Relaxed;
        QueryEmbeddingCacheStats {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            entries: self.entries.lock().map(|g| g.0.len()).unwrap_or(0),
        }
    }
}

/// Bound one external operation and record its outcome without exposing backend
/// error text or query contents in the public diagnostics.
pub(crate) async fn observe<T>(
    name: &str,
    timeout: u64,
    future: impl std::future::Future<Output = Result<T, BackendError>>,
    diagnostics: &mut Vec<SearchDiagnostic>,
) -> Result<T, BackendError> {
    let start = Instant::now();
    let result = within_deadline(timeout, future).await;
    diagnostics.push(SearchDiagnostic {
        operation: name.into(),
        status: if result.is_ok() { "success" } else { "failed" }.into(),
        duration_ms: start.elapsed().as_millis() as u64,
        count: 0,
        dropped_count: 0,
        failure: result.as_ref().err().map(SearchFailure::from),
        timeout_ms: match &result {
            Err(BackendError::Timeout(ms)) => Some(*ms),
            _ => None,
        },
        retry_after_ms: match &result {
            Err(BackendError::RateLimited { retry_after_ms }) => Some(*retry_after_ms),
            _ => None,
        },
    });
    if let Err(error) = &result {
        tracing::debug!(
            operation = name,
            duration_ms = start.elapsed().as_millis() as u64,
            failure = ?SearchFailure::from(error),
            "search operation failed"
        );
    }
    result
}

fn trace_diagnostic(d: &SearchDiagnostic) {
    tracing::debug!(
        operation = d.operation,
        status = d.status,
        duration_ms = d.duration_ms,
        count = d.count,
        dropped_count = d.dropped_count,
        failure = ?d.failure,
        "search operation"
    );
}

/// Record a successful in-memory selection step: how many candidates entered,
/// how many survived, and how long it took.
pub(crate) fn selection_diagnostic(
    diagnostics: &mut Vec<SearchDiagnostic>,
    operation: &str,
    start: Instant,
    before: usize,
    after: usize,
) {
    diagnostics.push(SearchDiagnostic {
        operation: operation.into(),
        status: "success".into(),
        duration_ms: start.elapsed().as_millis() as u64,
        count: after,
        dropped_count: before.saturating_sub(after),
        failure: None,
        timeout_ms: None,
        retry_after_ms: None,
    });
}

/// Record that `dropped` already-retrieved records were discarded because their
/// visibility changed between retrieval and hydration. No storage call is timed,
/// so the duration is zero; `count` mirrors `dropped_count` for readers that
/// only look at one of the two fields.
pub(crate) fn truncation_diagnostic(operation: &str, dropped: usize) -> SearchDiagnostic {
    SearchDiagnostic {
        operation: operation.into(),
        status: "truncated".into(),
        duration_ms: 0,
        count: dropped,
        dropped_count: dropped,
        failure: None,
        timeout_ms: None,
        retry_after_ms: None,
    }
}

/// The single evaluation instant for one request. `search_inner` always pins
/// `relationship_now`, so this cannot fail on a filter the engine built itself.
pub(crate) fn search_clock(filter: &SearchFilter) -> chrono::DateTime<chrono::Utc> {
    filter
        .as_of
        .or(filter.relationship_now)
        .expect("search clock pinned")
}

/// True when any selected scope needs a query embedding.
fn semantic_requested(config: &SearchConfig) -> bool {
    let nodes = config.scopes.contains(&SearchScope::Nodes);
    (nodes
        && config
            .methods
            .iter()
            .any(|m| matches!(m, SearchMethod::Vector | SearchMethod::GraphAnchored)))
        || (config.scopes.contains(&SearchScope::Relationships)
            && config.relationship_methods.contains(&SearchMethod::Vector))
        || (config.scopes.contains(&SearchScope::Communities)
            && config.community_methods.contains(&SearchMethod::Vector))
}

/// True when any selected scope, or attached evidence, ranks with the model.
fn model_ranking_requested(config: &SearchConfig) -> bool {
    let nodes = config.scopes.contains(&SearchScope::Nodes);
    (config.scopes.contains(&SearchScope::Communities)
        && config.community_reranker == RerankMethod::Model)
        || (nodes && config.reranker == RerankMethod::Model)
        || (config.evidence_reranker == EvidenceReranker::Model
            && (config.include_evidence
                || config
                    .scopes
                    .iter()
                    .any(|s| matches!(s, SearchScope::Relationships | SearchScope::Snapshots))))
}

/// Run `future` with a hard deadline. Tokio polls the inner future before
/// checking the timer, so the future is guarded on both sides of each poll:
/// a permit waiter that wakes after expiry cannot start work, and work that
/// finishes after expiry cannot report success.
async fn within_deadline<T>(
    timeout: u64,
    future: impl std::future::Future<Output = Result<T, BackendError>>,
) -> Result<T, BackendError> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout);
    tokio::pin!(future);
    let guarded = std::future::poll_fn(|cx| {
        if tokio::time::Instant::now() >= deadline {
            std::task::Poll::Ready(Err(BackendError::Timeout(timeout)))
        } else {
            let result = future.as_mut().poll(cx);
            if tokio::time::Instant::now() >= deadline {
                std::task::Poll::Ready(Err(BackendError::Timeout(timeout)))
            } else {
                result
            }
        }
    });
    tokio::time::timeout_at(deadline, guarded)
        .await
        .unwrap_or(Err(BackendError::Timeout(timeout)))
}

pub(crate) fn page_diagnostic<T>(diagnostics: &mut [SearchDiagnostic], page: &SearchPage<T>) {
    if let Some(d) = diagnostics.last_mut() {
        d.count = page.items.len();
        if page.truncated {
            d.status = "truncated".into();
        }
    }
}

impl SearchEngine {
    /// Create an engine with an authoritative graph adapter.
    pub fn new(graph: Arc<dyn SearchBackend>) -> Self {
        Self {
            entity_embedding_fields: Default::default(),
            graph: Arc::new(crate::concurrency::Limited::new(
                graph,
                Arc::new(tokio::sync::Semaphore::new(8)),
            )),
            model_permits: Arc::new(tokio::sync::Semaphore::new(4)),
            embedder: None,
            reranker: None,
            query_cache: Arc::new(QueryEmbeddingCache::new(QUERY_EMBEDDING_CACHE_CAPACITY)),
        }
    }
    /// Bound the query-embedding cache; zero disables it.
    pub fn with_query_embedding_cache(mut self, capacity: usize) -> Self {
        self.query_cache = Arc::new(QueryEmbeddingCache::new(capacity));
        self
    }
    /// Query-embedding cache counters since the engine was created.
    pub fn query_embedding_cache_stats(&self) -> QueryEmbeddingCacheStats {
        self.query_cache.stats()
    }
    /// Use the same entity text policy as ingestion and embedding rebuild.
    pub fn with_entity_embedding_fields(
        mut self,
        fields: kg_core::embedding::EntityEmbeddingFields,
    ) -> Result<Self, BackendError> {
        fields.validate()?;
        self.entity_embedding_fields = fields;
        Ok(self)
    }
    /// Configure the same embedding model used for stored entity embeddings.
    pub fn with_embedder(mut self, embedder: Arc<dyn EmbedBackend>) -> Self {
        self.embedder = Some(Arc::new(crate::concurrency::Limited::new(
            embedder,
            self.model_permits.clone(),
        )));
        self
    }
    /// Supply relevance ranking for node or evidence model-ranking configurations.
    pub fn with_reranker(mut self, reranker: Arc<dyn RerankBackend>) -> Self {
        self.reranker = Some(Arc::new(crate::concurrency::Limited::new(
            reranker,
            self.model_permits.clone(),
        )));
        self
    }
    /// Inspect vector coverage explicitly after ingestion or a representation change.
    /// Uses the configured embedder and an operation deadline; it can scan the scope.
    pub async fn embedding_readiness(
        &self,
        filter: SearchFilter,
        scope: SearchScope,
        timeout_ms: u64,
    ) -> Result<EmbeddingReadiness, BackendError> {
        if !(1..=120_000).contains(&timeout_ms) {
            return Err(BackendError::Query("invalid readiness timeout".into()));
        }
        let model = self
            .embedder
            .as_ref()
            .ok_or_else(|| BackendError::Query("readiness requires an embedder".into()))?;
        let request = EmbeddingReadinessRequest {
            entity_text_version: self.entity_embedding_fields.text_version(),
            filter,
            scope,
            model: model.model_id().into(),
            dimensions: model.dimension(),
        };
        request.validate()?;
        within_deadline(timeout_ms, self.graph.embedding_readiness(&request)).await
    }

    /// Optional derived summaries have separate coverage; absent summaries never inflate base missing counts.
    pub async fn summary_readiness(
        &self,
        filter: SearchFilter,
        timeout_ms: u64,
    ) -> Result<SummaryReadiness, BackendError> {
        if !(1..=120_000).contains(&timeout_ms) {
            return Err(BackendError::Query("invalid readiness timeout".into()));
        }
        let model = self
            .embedder
            .as_ref()
            .ok_or_else(|| BackendError::Query("readiness requires an embedder".into()))?;
        let request = EmbeddingReadinessRequest {
            entity_text_version: self.entity_embedding_fields.text_version(),
            filter,
            scope: SearchScope::Nodes,
            model: model.model_id().into(),
            dimensions: model.dimension(),
        };
        request.validate()?;
        within_deadline(timeout_ms, self.graph.summary_readiness(&request)).await
    }

    /// Await retrieval, ranking, and evidence within the configured request deadline.
    /// The caller must authorize access to `org_id`. Partial failures appear in
    /// diagnostics; failed visibility checks or a request timeout return an error.
    #[tracing::instrument(name = "search", skip_all, fields(otel.status_code = tracing::field::Empty))]
    pub async fn search(
        &self,
        query: &str,
        org_id: &str,
        config: &SearchConfig,
    ) -> Result<SearchResult, BackendError> {
        let mut operation =
            kg_core::telemetry::OperationGuard::new(kg_core::telemetry::OperationKind::Search);
        let outcome = self.search_inner(query, org_id, config).await;
        operation.finish_backend(&outcome);
        tracing::Span::current().record(
            "otel.status_code",
            if outcome.is_ok() { "OK" } else { "ERROR" },
        );
        outcome
    }

    async fn search_inner(
        &self,
        query: &str,
        org_id: &str,
        config: &SearchConfig,
    ) -> Result<SearchResult, BackendError> {
        config.validate()?;
        let filter = SearchFilter {
            org_id: org_id.into(),
            namespaces: config.namespaces.clone(),
            entity_types: config.entity_types.clone(),
            relationship_types: config.relationship_types.clone(),
            as_of: config.as_of,
            relationship_now: Some(chrono::Utc::now()),
            saga_uuid: config.saga_uuid,
        };
        filter.validate()?;
        if query.len() > 8192 {
            return Err(BackendError::Query("query exceeds 8192 bytes".into()));
        }
        if semantic_requested(config) && self.embedder.is_none() {
            return Err(BackendError::Query(
                "semantic search requires an embedding model".into(),
            ));
        }
        if model_ranking_requested(config) && self.reranker.is_none() {
            return Err(BackendError::Query(
                "model ranking requires a relevance model".into(),
            ));
        }
        // A blank query has nothing to match; only a seeded pure traversal can proceed.
        let seeded_traversal = config.scopes.contains(&SearchScope::Nodes)
            && config.methods == [SearchMethod::Bfs]
            && config.seed_chain_id.is_some();
        if query.trim().is_empty() && !seeded_traversal {
            return Ok(SearchResult::default());
        }
        let start = Instant::now();
        let outcome =
            within_deadline(config.timeout_ms, self.execute(query, &filter, config)).await;
        if let Err(error) = &outcome {
            tracing::debug!(
                duration_ms = start.elapsed().as_millis() as u64,
                failure = ?SearchFailure::from(error),
                "search failed"
            );
        }
        outcome
    }

    /// Hydrate the union in storage-sized batches before discarding any votes.
    /// Each retrieval source is bounded to 500 candidates; no refill is attempted.
    async fn validated_fusion(
        &self,
        lists: Vec<Vec<SearchHit>>,
        filter: &SearchFilter,
        config: &SearchConfig,
        result: &mut SearchResult,
        projection: NodeProjection,
        signals: NodeSignals,
    ) -> Result<Vec<SearchHit>, BackendError> {
        let mut chains: Vec<_> = lists
            .iter()
            .flatten()
            .map(|h| h.chain_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        chains.sort();
        let mut fresh = HashMap::new();
        for batch in chains.chunks(500) {
            let request = NodeSearch {
                embedding_text_version: self.entity_embedding_fields.text_version(),
                filter: filter.clone(),
                query: NodeQuery::ByChain,
                chain_ids: Some(batch.to_vec()),
                limit: 500,
                min_score: 0.0,
                signals,
                projection,
            };
            let page = observe(
                "hydrate",
                config.operation_timeout_ms,
                self.graph.search_nodes(&request),
                &mut result.diagnostics,
            )
            .await?;
            page_diagnostic(&mut result.diagnostics, &page);
            result.truncated |= page.truncated;
            for hit in page.items {
                if (signals.observations && hit.observation_count.is_none())
                    || (signals.dependents && hit.dependent_count.is_none())
                {
                    return Err(BackendError::Deserialization(
                        "hydration omitted requested counts".into(),
                    ));
                }
                if batch.binary_search(&hit.chain_id).is_err()
                    || fresh.insert(hit.chain_id, hit).is_some()
                {
                    return Err(BackendError::Deserialization(
                        "hydration returned an unexpected or duplicate chain".into(),
                    ));
                }
            }
        }
        let fusion_start = Instant::now();
        let visible = |hit: &SearchHit| {
            fresh.get(&hit.chain_id).is_some_and(|current: &SearchHit| {
                current.uuid == hit.uuid
                    && hit
                        .derived_summary
                        .as_ref()
                        .filter(|summary| summary.contributed)
                        .is_none_or(|candidate| {
                            current.derived_summary.as_ref().is_some_and(|summary| {
                                summary.revision == candidate.revision
                                    && summary.evidence_hash == candidate.evidence_hash
                                    && summary.valid_at(search_clock(filter))
                            })
                        })
            })
        };
        let rejected = lists
            .iter()
            .flatten()
            .filter(|h| !visible(h))
            .map(|h| (h.chain_id, h.uuid))
            .collect::<HashSet<_>>()
            .len();
        if rejected > 0 {
            result.truncated = true;
            result.diagnostics.push(truncation_diagnostic(
                "hydrate_visibility_changed",
                rejected,
            ));
        }
        let versions = lists
            .iter()
            .flatten()
            .map(|h| (h.chain_id, h.uuid))
            .collect::<HashSet<_>>()
            .len();
        let hits = fuse_versions(
            lists,
            usize::MAX,
            crate::rerank::Fusion {
                k: config.rrf_k,
                exact_match_first: config.exact_match_first,
            },
            visible,
        );
        if versions > 0 {
            selection_diagnostic(
                &mut result.diagnostics,
                "version_validation",
                fusion_start,
                versions,
                hits.len(),
            );
        }
        Ok(hits
            .into_iter()
            .map(|old| {
                let mut new = fresh.remove(&old.chain_id).expect("validated version");
                new.score = old.score;
                new.score_breakdown = old.score_breakdown;
                new.graph_distance = old.graph_distance;
                new
            })
            .collect())
    }

    async fn execute_nodes<'a>(
        &'a self,
        query: &'a str,
        filter: &'a SearchFilter,
        config: &'a SearchConfig,
        query_embedding: QueryEmbedding<'a>,
    ) -> Result<(SearchResult, usize), BackendError> {
        let mut result = SearchResult::default();
        let nodes = config.scopes.contains(&SearchScope::Nodes);
        let anchored_embedding = query_embedding.clone();
        let embedding_task = async {
            let (embedding, mut diagnostics) = query_embedding.await;
            let page = if nodes && config.methods.contains(&SearchMethod::Vector) {
                if let Some(e) = &embedding {
                    let request = NodeSearch {
                        embedding_text_version: self.entity_embedding_fields.text_version(),
                        filter: filter.clone(),
                        query: NodeQuery::Similarity(e.clone()),
                        chain_ids: None,
                        limit: config.prefetch,
                        min_score: config.min_score,
                        projection: NodeProjection::Candidate,
                        signals: NodeSignals::default(),
                    };
                    Some(
                        observe(
                            "vector",
                            config.operation_timeout_ms,
                            async {
                                if config.vector_retrieval == VectorRetrieval::Indexed {
                                    self.graph.search_nodes_indexed(&request).await
                                } else {
                                    self.graph.search_nodes(&request).await
                                }
                            },
                            &mut diagnostics,
                        )
                        .await,
                    )
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(Ok(page)) = &page {
                page_diagnostic(&mut diagnostics, page);
            }
            (embedding, page, diagnostics)
        };
        let keyword_task = async {
            let mut diagnostics = Vec::new();
            let page = if nodes && config.methods.contains(&SearchMethod::Fulltext) {
                let request = NodeSearch {
                    embedding_text_version: self.entity_embedding_fields.text_version(),
                    filter: filter.clone(),
                    query: NodeQuery::Fulltext(query.into()),
                    chain_ids: None,
                    limit: config.prefetch,
                    min_score: config.min_score,
                    projection: NodeProjection::Candidate,
                    signals: NodeSignals::default(),
                };
                Some(
                    observe(
                        "fulltext",
                        config.operation_timeout_ms,
                        self.graph.search_nodes(&request),
                        &mut diagnostics,
                    )
                    .await,
                )
            } else {
                None
            };
            if let Some(Ok(page)) = &page {
                page_diagnostic(&mut diagnostics, page);
            }
            (page, diagnostics)
        };
        let ((embedding, vector, embedding_diagnostics), (keyword, keyword_diagnostics)) =
            tokio::join!(embedding_task, keyword_task);
        result.diagnostics.extend(embedding_diagnostics);
        result.diagnostics.extend(keyword_diagnostics);
        let mut lists = vec![];
        let mut succeeded = 0usize;
        if let Some(Ok(mut page)) = keyword {
            succeeded += 1;
            result.truncated |= page.truncated;
            result.methods_used.push("fulltext".into());
            for hit in &mut page.items {
                hit.score_breakdown.insert("fulltext".into(), hit.score);
            }
            lists.push(page.items);
        }
        let mut traversal = None;
        if nodes && config.include_summaries {
            let summary_call = |kind: NodeQuery, label: &'static str| async move {
                let request = SummarySearch {
                    node: NodeSearch {
                        embedding_text_version: self.entity_embedding_fields.text_version(),
                        filter: filter.clone(),
                        query: kind,
                        chain_ids: None,
                        limit: config.prefetch,
                        min_score: config.min_score,
                        projection: NodeProjection::Candidate,
                        signals: NodeSignals::default(),
                    },
                };
                let mut diagnostics = Vec::new();
                let page = observe(
                    label,
                    config.operation_timeout_ms,
                    self.graph.search_entity_summaries(
                        &request,
                        config.vector_retrieval == VectorRetrieval::Indexed,
                    ),
                    &mut diagnostics,
                )
                .await;
                (label, page, diagnostics)
            };
            let mut calls = Vec::new();
            if config.methods.contains(&SearchMethod::Fulltext) {
                calls.push(summary_call(
                    NodeQuery::Fulltext(query.into()),
                    "summary_fulltext",
                ));
            }
            if config.methods.contains(&SearchMethod::Vector) {
                if let Some(e) = &embedding {
                    calls.push(summary_call(
                        NodeQuery::Similarity(e.clone()),
                        "summary_vector",
                    ));
                }
            }
            for (label, page, diagnostics) in futures::future::join_all(calls).await {
                result.diagnostics.extend(diagnostics);
                if let Ok(mut page) = page {
                    succeeded += 1;
                    result.truncated |= page.truncated;
                    result.approximate |= page.approximate;
                    page_diagnostic(&mut result.diagnostics, &page);
                    result.methods_used.push(label.into());
                    for hit in &mut page.items {
                        let summary = hit.derived_summary.as_mut().ok_or_else(|| {
                            BackendError::Deserialization(
                                "summary retrieval omitted revision evidence".into(),
                            )
                        })?;
                        if !summary.valid_at(search_clock(filter)) {
                            return Err(BackendError::Deserialization(
                                "summary retrieval returned invalid coverage".into(),
                            ));
                        }
                        summary.contributed = true;
                        hit.score_breakdown.insert(label.into(), hit.score);
                    }
                    lists.push(page.items);
                }
            }
        }
        if nodes {
            if let Some(Ok(mut page)) = vector {
                succeeded += 1;
                result.truncated |= page.truncated;
                result.approximate |= page.approximate;
                result.methods_used.push("vector".into());
                for hit in &mut page.items {
                    hit.score_breakdown.insert("vector".into(), hit.score);
                }
                lists.push(page.items);
            }
            if config
                .methods
                .iter()
                .any(|m| matches!(m, SearchMethod::Bfs | SearchMethod::GraphAnchored))
            {
                let mut seed_versions = HashMap::new();
                let seeds = if let Some(id) = config.seed_chain_id {
                    vec![id]
                } else {
                    self.validated_fusion(
                        lists.clone(),
                        filter,
                        config,
                        &mut result,
                        NodeProjection::Candidate,
                        NodeSignals::default(),
                    )
                    .await?
                    .into_iter()
                    .take(config.expansion_seeds)
                    .map(|h| {
                        seed_versions.insert(h.chain_id, h.uuid);
                        h.chain_id
                    })
                    .collect()
                };
                if !seeds.is_empty() {
                    let attempt = neighborhood(
                        &self.entity_embedding_fields.text_version(),
                        self.graph.as_ref(),
                        filter,
                        seeds,
                        config.seed_chain_id.is_none().then_some(&seed_versions),
                        config,
                        &mut result.diagnostics,
                    )
                    .await;
                    if let Ok(page) = attempt {
                        result.truncated |= page.truncated;
                        traversal = Some(page);
                    }
                }
            }
            if config.methods.contains(&SearchMethod::Bfs) {
                if let Some(page) = &traversal {
                    succeeded += 1;
                    result.methods_used.push("bfs".into());
                    lists.push(page.hits.clone());
                }
            }
            if config.methods.contains(&SearchMethod::GraphAnchored) {
                if let (Some(page), Some(e)) = (&traversal, &embedding) {
                    let mut chains: Vec<_> = page.distances.keys().copied().collect();
                    chains.sort();
                    let request = NodeSearch {
                        embedding_text_version: self.entity_embedding_fields.text_version(),
                        filter: filter.clone(),
                        query: NodeQuery::Similarity(e.clone()),
                        chain_ids: Some(chains),
                        limit: config.prefetch,
                        min_score: config.min_score,
                        projection: NodeProjection::Candidate,
                        signals: NodeSignals::default(),
                    };
                    if let Ok(page) = observe(
                        "graph_anchored",
                        config.operation_timeout_ms,
                        self.graph.search_nodes(&request),
                        &mut result.diagnostics,
                    )
                    .await
                    {
                        page_diagnostic(&mut result.diagnostics, &page);
                        result.truncated |= page.truncated;
                        succeeded += 1;
                        result.methods_used.push("graph_anchored".into());
                        lists.push(page.items);
                    }
                }
            }
        }
        result.total_candidates = lists
            .iter()
            .flatten()
            .map(|h| h.chain_id)
            .collect::<HashSet<_>>()
            .len();
        let mut hits = self
            .validated_fusion(
                lists,
                filter,
                config,
                &mut result,
                NodeProjection::Full,
                NodeSignals {
                    observations: config.include_signals
                        || config.reranker == RerankMethod::ObservationFrequency,
                    dependents: config.include_signals,
                },
            )
            .await?;
        let selection_start = Instant::now();
        let before_budget = hits.len();
        if hits.len() > config.prefetch {
            result.truncated = true;
            hits.truncate(config.prefetch);
        }
        if before_budget > 0 {
            selection_diagnostic(
                &mut result.diagnostics,
                "candidate_budget",
                selection_start,
                before_budget,
                hits.len(),
            );
        }
        if !hits.is_empty() {
            let ranking_start = Instant::now();
            let before_ranking = hits.len();
            match config.reranker {
                RerankMethod::Rrf => {}
                RerankMethod::Mmr => {
                    hits = maximal_marginal_relevance(hits, config.mmr_lambda, config.limit).await
                }
                RerankMethod::ObservationFrequency => hits.sort_by(|a, b| {
                    b.observation_count
                        .cmp(&a.observation_count)
                        .then(b.score.total_cmp(&a.score))
                        .then(a.chain_id.cmp(&b.chain_id))
                }),
                RerankMethod::NodeDistance => {
                    let center = config
                        .center_chain_id
                        .or(config.seed_chain_id)
                        .expect("validated center");
                    // Only an explicit single seed has distances from this exact center.
                    let page = if config.seed_chain_id == Some(center) && traversal.is_some() {
                        traversal.take()
                    } else {
                        neighborhood(
                            &self.entity_embedding_fields.text_version(),
                            self.graph.as_ref(),
                            filter,
                            vec![center],
                            None,
                            config,
                            &mut result.diagnostics,
                        )
                        .await
                        .ok()
                    };
                    for hit in &mut hits {
                        hit.graph_distance = None;
                    }
                    if let Some(page) = page {
                        result.truncated |= page.truncated;
                        for hit in &mut hits {
                            hit.graph_distance = page.distances.get(&hit.chain_id).copied();
                        }
                        hits.sort_by(|a, b| {
                            a.graph_distance
                                .unwrap_or(usize::MAX)
                                .cmp(&b.graph_distance.unwrap_or(usize::MAX))
                                .then(b.score.total_cmp(&a.score))
                                .then(a.chain_id.cmp(&b.chain_id))
                        });
                    }
                }
                RerankMethod::Model => {
                    let candidates: Vec<_> = hits
                        .iter()
                        .map(|h| RankCandidate {
                            id: h.chain_id,
                            text: crate::rerank::hit_text(h),
                        })
                        .collect();
                    let model = self.reranker.as_ref().expect("validated reranker");
                    let ranked = observe(
                        "rerank_model",
                        config.operation_timeout_ms,
                        async {
                            let scores = model.rank(query, &candidates).await?;
                            let expected: HashSet<_> = candidates.iter().map(|c| c.id).collect();
                            let actual: HashSet<_> = scores.iter().map(|s| s.id).collect();
                            if scores.len() != expected.len()
                                || expected != actual
                                || scores.iter().any(|s| !s.score.is_finite())
                            {
                                return Err(BackendError::Deserialization(
                                    "reranker must return one finite score per candidate".into(),
                                ));
                            }
                            Ok(scores)
                        },
                        &mut result.diagnostics,
                    )
                    .await;
                    if ranked.is_err() && config.model_min_score.is_some() {
                        return Err(typed_failure(
                            result.diagnostics.last(),
                            "required entity relevance ranking failed",
                            config.operation_timeout_ms,
                        ));
                    }
                    if let Ok(scores) = ranked {
                        result
                            .diagnostics
                            .last_mut()
                            .expect("recorded model call")
                            .count = scores.len();
                        let scores: HashMap<_, _> =
                            scores.into_iter().map(|s| (s.id, s.score)).collect();
                        for hit in &mut hits {
                            hit.score = scores[&hit.chain_id];
                            hit.score_breakdown.insert("model".into(), hit.score);
                        }
                        if let Some(minimum) = config.model_min_score {
                            hits.retain(|hit| hit.score >= minimum);
                        }
                        hits.sort_by(|a, b| {
                            b.score
                                .total_cmp(&a.score)
                                .then(a.chain_id.cmp(&b.chain_id))
                        });
                    }
                }
            }
            if config.exact_match_first && config.reranker != RerankMethod::Rrf {
                // Diversity, frequency and model ranking reorder the fused list; a
                // literal identifier or name match still comes first.
                let (exact, rest): (Vec<_>, Vec<_>) =
                    hits.into_iter().partition(crate::rerank::exact_match);
                hits = exact.into_iter().chain(rest).collect();
            }
            hits.truncate(config.limit);
            selection_diagnostic(
                &mut result.diagnostics,
                "node_selection",
                ranking_start,
                before_ranking,
                hits.len(),
            );
        }
        result.hits = hits;
        let attached = crate::evidence::retrieve_scopes(
            crate::evidence::EvidenceContext {
                graph: self.graph.as_ref(),
                model: self.reranker.as_deref(),
                query,
                filter,
                config,
                query_embedding: semantic_requested(config).then_some(anchored_embedding),
            },
            &result.hits,
            false,
        )
        .await?;
        merge_evidence(&mut result, attached.0);
        Ok((result, succeeded))
    }

    async fn execute(
        &self,
        query: &str,
        filter: &SearchFilter,
        config: &SearchConfig,
    ) -> Result<SearchResult, BackendError> {
        let start = Instant::now();
        // Embed the query at most once per request. The shared future is awaited by
        // the node, evidence and community paths; only the node path records its
        // diagnostics so `embed_query` appears once in the result.
        let query_embedding = async {
            let mut diagnostics = Vec::new();
            let mut embedding = None;
            if semantic_requested(config) {
                let embedder = self.embedder.as_ref().expect("validated model");
                if let Some(values) = self.query_cache.get(embedder.model_id(), query) {
                    // Validated when it was cached; the provider is not consulted.
                    diagnostics.push(SearchDiagnostic {
                        operation: "embed_query_cached".into(),
                        status: "success".into(),
                        duration_ms: 0,
                        count: 1,
                        dropped_count: 0,
                        failure: None,
                        timeout_ms: None,
                        retry_after_ms: None,
                    });
                    embedding = Some(GraphEmbedding {
                        model: embedder.model_id().into(),
                        values,
                    });
                } else {
                    let attempt = observe(
                        "embed_query",
                        config.operation_timeout_ms,
                        async {
                            let mut vectors = embedder.embed_batch(&[query]).await?;
                            if vectors.len() != 1 || vectors[0].len() != embedder.dimension() {
                                return Err(BackendError::Deserialization(
                                    "embedding response shape mismatch".into(),
                                ));
                            }
                            let e = GraphEmbedding {
                                model: embedder.model_id().into(),
                                values: vectors.remove(0),
                            };
                            e.validate()?;
                            Ok(e)
                        },
                        &mut diagnostics,
                    )
                    .await;
                    if let Ok(e) = attempt {
                        self.query_cache
                            .insert(embedder.model_id(), query, &e.values);
                        embedding = Some(e);
                    }
                }
            }
            (embedding, diagnostics)
        }
        .boxed()
        .shared();

        let (nodes, evidence, communities) = tokio::try_join!(
            self.execute_nodes(query, filter, config, query_embedding.clone()),
            crate::evidence::retrieve_scopes(
                crate::evidence::EvidenceContext {
                    graph: self.graph.as_ref(),
                    model: self.reranker.as_deref(),
                    query,
                    filter,
                    config,
                    query_embedding: Some(query_embedding.clone()),
                },
                &[],
                true,
            ),
            crate::community::retrieve(
                self.graph.as_ref(),
                self.reranker.as_deref(),
                query,
                filter,
                config,
                query_embedding
            ),
        )?;
        let (mut result, node_successes) = nodes;
        let (evidence, evidence_successes) = evidence;
        merge_evidence(&mut result, evidence);
        let (communities, community_successes) = communities;
        merge_evidence(&mut result, communities);
        let succeeded = node_successes + evidence_successes + community_successes;
        if succeeded == 0 {
            return Err(retrieval_failure(
                &result.diagnostics,
                config.operation_timeout_ms,
            ));
        }
        result.duration_ms = start.elapsed().as_millis() as u64;
        for diagnostic in &result.diagnostics {
            trace_diagnostic(diagnostic);
        }
        tracing::debug!(
            duration_ms = result.duration_ms,
            candidates = result.total_candidates,
            hits = result.hits.len(),
            communities = result.communities.len(),
            truncated = result.truncated,
            "search completed"
        );
        Ok(result)
    }
}

fn merge_evidence(result: &mut SearchResult, evidence: SearchResult) {
    result.relationships.extend(evidence.relationships);
    result.snapshots.extend(evidence.snapshots);
    result.communities.extend(evidence.communities);
    result.diagnostics.extend(evidence.diagnostics);
    result.methods_used.extend(evidence.methods_used);
    result.truncated |= evidence.truncated;
    result.approximate |= evidence.approximate;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn work_finishing_after_its_deadline_cannot_return_success() {
        let result = within_deadline(5, async {
            std::thread::sleep(Duration::from_millis(15));
            Ok(())
        })
        .await;
        assert!(matches!(result, Err(BackendError::Timeout(5))));
    }

    #[tokio::test(start_paused = true)]
    async fn operation_timeout_reports_elapsed_time_and_safe_category() {
        let mut diagnostics = Vec::new();
        let outcome = observe::<()>("test", 25, std::future::pending(), &mut diagnostics).await;
        assert!(matches!(outcome, Err(BackendError::Timeout(25))));
        assert_eq!(diagnostics[0].failure, Some(SearchFailure::Timeout));
        assert_eq!(diagnostics[0].duration_ms, 25);
        assert_eq!(diagnostics[0].status, "failed");
    }

    #[tokio::test]
    async fn failure_diagnostics_never_serialize_backend_messages() {
        let cases = [
            (
                BackendError::Auth("secret-provider-body".into()),
                SearchFailure::Authentication,
            ),
            (
                BackendError::RateLimited { retry_after_ms: 10 },
                SearchFailure::RateLimited,
            ),
            (
                BackendError::Deserialization("secret-provider-body".into()),
                SearchFailure::InvalidResponse,
            ),
            (
                BackendError::Connection("secret-provider-body".into()),
                SearchFailure::Connection,
            ),
            (
                BackendError::Unavailable("secret-provider-body".into()),
                SearchFailure::Unavailable,
            ),
        ];
        for (error, expected) in cases {
            let mut diagnostics = Vec::new();
            assert!(
                observe::<()>("test", 100, async { Err(error) }, &mut diagnostics)
                    .await
                    .is_err()
            );
            assert_eq!(diagnostics[0].failure, Some(expected));
            assert!(!serde_json::to_string(&diagnostics)
                .unwrap()
                .contains("secret-provider-body"));
        }
        let mut diagnostics = Vec::new();
        observe("test", 100, async { Ok(()) }, &mut diagnostics)
            .await
            .unwrap();
        assert_eq!(diagnostics[0].failure, None);
    }

    // ---- merged from `mod query_cache_tests`

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EmptyGraph;
    impl SearchBackend for EmptyGraph {}

    struct CountingEmbedder(AtomicUsize);
    #[async_trait::async_trait]
    impl EmbedBackend for CountingEmbedder {
        async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(texts.iter().map(|_| vec![0.6, 0.8]).collect())
        }
        fn dimension(&self) -> usize {
            2
        }
        fn max_batch_size(&self) -> usize {
            8
        }
        fn model_id(&self) -> &str {
            "counting"
        }
    }

    #[tokio::test]
    async fn repeated_questions_embed_once_and_report_the_cached_step() {
        let embedder = Arc::new(CountingEmbedder(AtomicUsize::new(0)));
        let engine = SearchEngine::new(Arc::new(EmptyGraph)).with_embedder(embedder.clone());
        let config = SearchConfig {
            include_evidence: false,
            ..SearchConfig::hybrid_rrf()
        };
        // Storage answers nothing, so the search fails, but the embedding step still runs.
        let _ = engine.search("Payments Database", "org", &config).await;
        let _ = engine.search("payments   database ", "org", &config).await;
        let _ = engine.search("payments database", "org", &config).await;
        assert_eq!(embedder.0.load(Ordering::Relaxed), 1);
        let stats = engine.query_embedding_cache_stats();
        assert_eq!((stats.hits, stats.misses, stats.entries), (2, 1, 1));
        let uncached = SearchEngine::new(Arc::new(EmptyGraph))
            .with_embedder(embedder.clone())
            .with_query_embedding_cache(0);
        let _ = uncached.search("payments database", "org", &config).await;
        let _ = uncached.search("payments database", "org", &config).await;
        assert_eq!(embedder.0.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn the_cache_is_bounded_and_evicts_the_oldest_query() {
        let cache = QueryEmbeddingCache::new(2);
        cache.insert("m", "a", &[1.0]);
        cache.insert("m", "b", &[2.0]);
        cache.insert("m", "c", &[3.0]);
        assert!(cache.get("m", "a").is_none());
        assert_eq!(cache.get("m", "b"), Some(vec![2.0]));
        assert_eq!(cache.get("m", "c"), Some(vec![3.0]));
        // Another model never shares a vector.
        assert!(cache.get("other", "c").is_none());
        assert_eq!(cache.stats().entries, 2);
    }

    // ---- merged from `mod embedding_policy_tests`

    struct PolicyGraph(std::sync::Mutex<Vec<String>>);
    #[async_trait::async_trait]
    impl SearchBackend for PolicyGraph {
        async fn search_nodes(&self, request: &NodeSearch) -> Result<NodePage, BackendError> {
            self.0
                .lock()
                .unwrap()
                .push(request.embedding_text_version.clone());
            Ok(SearchPage::bounded(vec![], request.limit))
        }
    }
    #[tokio::test]
    async fn configured_policy_reaches_storage_even_for_keyword_reads() {
        let graph = Arc::new(PolicyGraph(Default::default()));
        let mut fields = kg_core::embedding::EntityEmbeddingFields::default();
        fields
            .by_entity_type
            .insert("Service".into(), vec!["owner".into()]);
        let expected = fields.text_version();
        let engine = SearchEngine::new(graph.clone())
            .with_entity_embedding_fields(fields)
            .unwrap();
        engine
            .search("checkout", "org", &SearchConfig::keyword_only())
            .await
            .unwrap();
        assert_eq!(*graph.0.lock().unwrap(), vec![expected]);
    }

    // ---- merged from `mod contract_tests`

    fn recorded(failure: SearchFailure) -> SearchDiagnostic {
        SearchDiagnostic {
            operation: "op".into(),
            status: "failed".into(),
            duration_ms: 1,
            count: 0,
            dropped_count: 0,
            failure: Some(failure),
            timeout_ms: Some(7),
            retry_after_ms: Some(9),
        }
    }

    #[test]
    fn typed_failure_preserves_every_category_and_its_budget_fields() {
        let cases = [
            (SearchFailure::Timeout, BackendError::Timeout(7)),
            (
                SearchFailure::Authentication,
                BackendError::Auth("m".into()),
            ),
            (
                SearchFailure::Connection,
                BackendError::Connection("m".into()),
            ),
            (
                SearchFailure::RateLimited,
                BackendError::RateLimited { retry_after_ms: 9 },
            ),
            (
                SearchFailure::InvalidResponse,
                BackendError::Deserialization("m".into()),
            ),
            (
                SearchFailure::InvalidRequest,
                BackendError::Query("m".into()),
            ),
            (SearchFailure::NotFound, BackendError::NotFound("m".into())),
            (SearchFailure::Conflict, BackendError::Conflict("m".into())),
            (
                SearchFailure::Transaction,
                BackendError::Transaction("m".into()),
            ),
            (
                SearchFailure::Serialization,
                BackendError::Serialization("m".into()),
            ),
            (SearchFailure::Other, BackendError::Other("m".into())),
            (
                SearchFailure::Unavailable,
                BackendError::Unavailable("m".into()),
            ),
        ];
        for (failure, expected) in cases {
            let actual = typed_failure(Some(&recorded(failure)), "m", 99);
            assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected),
                "{failure:?} produced {actual:?}"
            );
            match actual {
                BackendError::Timeout(ms) => assert_eq!(ms, 7),
                BackendError::RateLimited { retry_after_ms } => assert_eq!(retry_after_ms, 9),
                _ => {}
            }
        }
        // Without a recorded failure the caller's message and category stand in.
        assert!(matches!(typed_failure(None, "m", 99), BackendError::Unavailable(m) if m == "m"));
        // A timeout that did not record its budget falls back to the operation budget.
        let mut unbudgeted = recorded(SearchFailure::Timeout);
        unbudgeted.timeout_ms = None;
        assert!(matches!(
            typed_failure(Some(&unbudgeted), "m", 99),
            BackendError::Timeout(99)
        ));
    }

    #[test]
    fn dependency_predicates_follow_selected_scopes_only() {
        let keyword = SearchConfig::keyword_only();
        assert!(!semantic_requested(&keyword));
        assert!(!model_ranking_requested(&keyword));
        assert!(semantic_requested(&SearchConfig::semantic_only()));
        assert!(semantic_requested(&SearchConfig::graph_anchored(
            uuid::Uuid::from_u128(1)
        )));
        assert!(semantic_requested(&SearchConfig::relationship_hybrid()));
        assert!(semantic_requested(&SearchConfig {
            scopes: vec![SearchScope::Communities],
            community_methods: vec![SearchMethod::Vector],
            ..SearchConfig::keyword_only()
        }));
        // Vector methods on an unselected scope do not demand an embedder.
        assert!(!semantic_requested(&SearchConfig {
            scopes: vec![SearchScope::Snapshots],
            ..SearchConfig::hybrid_rrf()
        }));
        assert!(model_ranking_requested(&SearchConfig::hybrid_model()));
        let evidence_model = SearchConfig {
            evidence_reranker: EvidenceReranker::Model,
            ..SearchConfig::keyword_only()
        };
        assert!(model_ranking_requested(&evidence_model));
        assert!(!model_ranking_requested(&SearchConfig {
            include_evidence: false,
            ..evidence_model
        }));
    }

    struct NoGraph;
    impl SearchBackend for NoGraph {}

    struct Embedder;
    #[async_trait::async_trait]
    impl EmbedBackend for Embedder {
        async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
            Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
        }
        fn dimension(&self) -> usize {
            2
        }
        fn max_batch_size(&self) -> usize {
            8
        }
        fn model_id(&self) -> &str {
            "test"
        }
    }

    #[tokio::test]
    async fn readiness_checks_reject_invalid_requests_before_storage() {
        let engine = SearchEngine::new(Arc::new(NoGraph));
        let filter = SearchFilter {
            org_id: "acme".into(),
            ..Default::default()
        };
        // Neither check can name the expected model without an embedder.
        assert!(matches!(
            engine.summary_readiness(filter.clone(), 100).await,
            Err(BackendError::Query(_))
        ));
        let engine = engine.with_embedder(Arc::new(Embedder));
        let blank = SearchFilter {
            org_id: " ".into(),
            ..Default::default()
        };
        assert!(matches!(
            engine.summary_readiness(blank.clone(), 100).await,
            Err(BackendError::Query(_))
        ));
        assert!(matches!(
            engine
                .embedding_readiness(blank, SearchScope::Nodes, 100)
                .await,
            Err(BackendError::Query(_))
        ));
        assert!(matches!(
            engine
                .embedding_readiness(filter.clone(), SearchScope::Snapshots, 100)
                .await,
            Err(BackendError::Query(_))
        ));
        assert!(matches!(
            engine.summary_readiness(filter.clone(), 0).await,
            Err(BackendError::Query(_))
        ));
        // A valid request reaches storage, whose unsupported answer is not a request error.
        assert!(matches!(
            engine.summary_readiness(filter, 100).await,
            Err(BackendError::NotConfigured(_))
        ));
    }
}
