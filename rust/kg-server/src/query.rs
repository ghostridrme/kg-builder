//! Shared, scoped graph reads for HTTP and MCP clients: one service behind
//! both transports so scope, limits and Saga rules cannot drift between them.
use chrono::{DateTime, Utc};
use kg_core::{
    errors::{BackendError, PipelineError},
    models::ThreadNode,
    pipeline::PipelineOutput,
    saga::{SagaMemberPage, SagaRead, SagaReadResult, ThreadReference},
    search::{SearchConfig, SearchPage, SearchResult},
    traits::graph_explorer::{ExplorerQuery, ExplorerRequest, GraphExplorerBackend},
};
use kg_search::SearchEngine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("invalid graph query")]
    Invalid,
    #[error("Graph continuation expired; refresh the view")]
    RestartRequired,
    #[error("Graph result exceeds its node, evidence or byte budget; narrow the filters or focus a smaller neighborhood")]
    ViewLimit,
    #[error("semantic search is not configured")]
    SemanticUnavailable,
    #[error("too many active graph queries")]
    Busy,
    #[error("record is not visible in this scope")]
    NotFound,
    #[error("Thread summaries are not configured")]
    SummariesUnavailable,
    /// An on-demand summary run failed after validation; retry with the same run id.
    #[error(transparent)]
    Summary(#[from] PipelineError),
    #[error(transparent)]
    Backend(#[from] BackendError),
}

/// Runs the receipted Saga summary pipeline. Implemented by the ingestion engine;
/// the query service only validates scope and shapes the outcome.
#[async_trait::async_trait]
pub trait SagaSummarizer: Send + Sync {
    /// Summarize every member no committed summary covers. The same run id
    /// replays committed pages instead of repeating provider work.
    async fn summarize(
        &self,
        org_id: &str,
        namespace: &str,
        saga_uuid: Uuid,
        run_id: Uuid,
        cancel: CancellationToken,
    ) -> Result<PipelineOutput, PipelineError>;
}

/// Result of an on-demand summary: what the run committed and the Saga afterwards.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SagaSummaryOutcome {
    /// Pass this back to replay an interrupted run instead of starting another.
    pub run_id: Uuid,
    /// Batches acknowledged by this call, including replays of earlier commits.
    pub batches: usize,
    pub replayed_batches: usize,
    /// Members the run's summary pages covered across every attempt.
    pub memberships_summarized: usize,
    pub summaries_updated: usize,
    pub incomplete_followups: Vec<kg_core::saga::IncompleteSagaSummary>,
    /// The Saga after the run. `None` only when the run committed but the
    /// follow-up read failed; `saga_read_error` says why and the commit stands.
    #[serde(rename = "thread", alias = "saga")]
    pub saga: Option<SagaView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "thread_read_error", alias = "saga_read_error")]
    pub saga_read_error: Option<String>,
}

/// Scope for Saga reads. The namespace is mandatory because a Saga name is
/// unique only within one organization and namespace.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SagaScope {
    pub namespace: String,
    /// Source-valid instant. Members captured later are hidden, and a summary
    /// that covers later observations is withheld rather than shown as history.
    pub as_of: Option<DateTime<Utc>>,
}

/// Why a Saga view carries no summary text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryWithheld {
    /// No summary has been committed for this Saga yet.
    NotSummarized,
    /// The stored summary covers observations captured after the requested `as_of`.
    CoversLaterObservations,
    /// The stored summary has no coverage watermark, so its place on the
    /// observation timeline is unknown and it cannot be shown historically.
    CoverageUnknown,
}

/// A Saga as interactive clients see it: identity, membership bounds, and the
/// summary with its coverage, or the reason the summary is withheld.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SagaView {
    pub uuid: Uuid,
    pub name: String,
    pub namespace: String,
    /// Capture time of the observation that created the Saga.
    pub created_at: DateTime<Utc>,
    /// Earliest capture time among every member, including backfills captured
    /// before `created_at`; the Saga is visible to historical reads from here.
    pub earliest_captured_at: DateTime<Utc>,
    /// Members ever associated, regardless of `as_of`; ordinals are stable.
    pub total_members: u64,
    pub first_snapshot_uuid: Option<Uuid>,
    pub last_snapshot_uuid: Option<Uuid>,
    pub summary: Option<String>,
    pub summary_withheld: Option<SummaryWithheld>,
    pub summary_revision: Option<Uuid>,
    pub summary_supporting_snapshot_uuids: Vec<Uuid>,
    /// Highest membership ordinal the summary accounts for.
    pub summary_covers_members_through: u64,
    /// Latest capture time among every observation the summary accounts for,
    /// cited or not. A historical read at or after this instant may show the summary.
    pub summary_covers_captured_through: Option<DateTime<Utc>>,
    /// Ingestion-clock time of the last summary commit.
    pub summarized_at: Option<DateTime<Utc>>,
}

impl SagaView {
    fn from_node(node: ThreadNode, as_of: Option<DateTime<Utc>>) -> Self {
        let withheld = if node.summary.trim().is_empty() {
            Some(SummaryWithheld::NotSummarized)
        } else {
            match (as_of, node.last_summarized_snapshot_captured_at) {
                (None, _) => None,
                (Some(_), None) => Some(SummaryWithheld::CoverageUnknown),
                (Some(at), Some(covered)) if at < covered => {
                    Some(SummaryWithheld::CoversLaterObservations)
                }
                _ => None,
            }
        };
        let earliest_captured_at = earliest_capture(&node);
        let shown = withheld.is_none();
        Self {
            uuid: node.uuid,
            name: node.name,
            namespace: node.namespace,
            created_at: node.created_at,
            earliest_captured_at,
            total_members: node.last_membership_ordinal,
            first_snapshot_uuid: node.first_snapshot_uuid,
            last_snapshot_uuid: node.last_snapshot_uuid,
            summary: shown.then_some(node.summary),
            summary_withheld: withheld,
            summary_revision: shown.then_some(node.summary_revision).flatten(),
            summary_supporting_snapshot_uuids: if shown {
                node.summary_supporting_snapshot_uuids
            } else {
                vec![]
            },
            summary_covers_members_through: node.summary_cursor,
            summary_covers_captured_through: node.last_summarized_snapshot_captured_at,
            summarized_at: node.last_summarized_at,
        }
    }
}

/// When the Saga starts on the observation timeline: its earliest member's
/// capture time, which a backfill can move before the minting observation.
/// Falls back to `created_at` only for nodes stored before the field existed;
/// the Cypher listing predicate (`coalesce(first_captured_at, created_at)`)
/// must agree with this exactly.
fn earliest_capture(node: &ThreadNode) -> DateTime<Utc> {
    node.first_captured_at.unwrap_or(node.created_at)
}
fn saga_visible(node: &ThreadNode, as_of: Option<DateTime<Utc>>) -> bool {
    as_of.is_none_or(|at| earliest_capture(node) <= at)
}

fn scoped_text_valid(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

#[derive(Clone)]
pub struct GraphQueryService {
    pub(crate) graph: Arc<dyn GraphExplorerBackend>,
    search: Arc<SearchEngine>,
    semantic_available: bool,
    reranking_available: bool,
    summarizer: Option<Arc<dyn SagaSummarizer>>,
    pub(crate) permits: Arc<tokio::sync::Semaphore>,
    pub(crate) mcp_permits: Arc<tokio::sync::Semaphore>,
    pub(crate) agent_pages: Arc<crate::investigation::ResultPages>,
    pub(crate) graph_cache: Arc<crate::graph::GraphCache>,
}

/// Admission includes cleanup scheduled when a timed-out read future is dropped.
/// The owned permit moves into the cleanup task instead of returning early.
pub(crate) struct ReadPermit {
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    backend: Arc<dyn GraphExplorerBackend>,
}
impl ReadPermit {
    pub(crate) async fn complete(mut self) {
        // A completed read has settled its own operation. Unrelated active reads
        // must not delay this response; only abandoned futures use the drop path.
        self.permit.take();
    }
}
impl Drop for ReadPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            let backend = self.backend.clone();
            tokio::spawn(async move {
                backend.settle_cancelled_reads().await;
                drop(permit);
            });
        }
    }
}
impl GraphQueryService {
    pub(crate) fn admit_read(&self) -> Result<ReadPermit, QueryError> {
        Ok(ReadPermit {
            permit: Some(
                self.permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| QueryError::Busy)?,
            ),
            backend: self.graph.clone(),
        })
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryScope {
    pub namespace: Option<String>,
    pub as_of: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Public recipes that require no model reranker or caller-supplied graph center.
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchRecipe {
    Keyword,
    Hybrid,
    Semantic,
    Diverse,
}

impl SearchRecipe {
    fn config(self) -> SearchConfig {
        match self {
            Self::Keyword => SearchConfig::keyword_only(),
            Self::Hybrid => SearchConfig::hybrid_rrf(),
            Self::Semantic => SearchConfig::semantic_only(),
            Self::Diverse => SearchConfig::hybrid_mmr(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchQuery {
    /// Optional advanced engine settings; request scope remains authoritative.
    #[serde(default)]
    pub config: Option<SearchConfig>,
    #[serde(default)]
    pub include_relationships: bool,
    #[serde(default)]
    pub recipe: Option<SearchRecipe>,
    #[serde(default)]
    pub include_evidence: bool,
    #[serde(default)]
    pub include_signals: Option<bool>,
    pub query: String,
    pub namespace: Option<String>,
    pub as_of: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub semantic: bool,
    /// Only entities of these types; empty means every type.
    #[serde(default)]
    pub entity_types: Vec<String>,
    /// Search the member observations of one Saga instead of entities. Requires
    /// `namespace`; the Saga must exist there. Keyword retrieval only.
    #[serde(default)]
    #[serde(rename = "thread", alias = "saga")]
    pub saga: Option<ThreadReference>,
}

impl GraphQueryService {
    pub fn new(
        graph: Arc<dyn GraphExplorerBackend>,
        search: Arc<SearchEngine>,
        semantic_available: bool,
    ) -> Self {
        Self {
            graph,
            mcp_permits: Arc::new(tokio::sync::Semaphore::new(32)),
            agent_pages: Arc::new(crate::investigation::ResultPages::default()),
            graph_cache: Arc::new(crate::graph::GraphCache::default()),
            search,
            semantic_available,
            reranking_available: false,
            summarizer: None,
            permits: Arc::new(tokio::sync::Semaphore::new(32)),
        }
    }

    /// Enable on-demand Saga summaries. Without this, requests fail with
    /// [`QueryError::SummariesUnavailable`] before touching storage.
    pub fn with_saga_summarizer(mut self, summarizer: Arc<dyn SagaSummarizer>) -> Self {
        self.summarizer = Some(summarizer);
        self
    }

    pub fn with_reranking_available(mut self, available: bool) -> Self {
        self.reranking_available = available;
        self
    }
    pub fn reranking_available(&self) -> bool {
        self.reranking_available
    }

    pub fn semantic_available(&self) -> bool {
        self.semantic_available
    }

    pub fn saga_summaries_available(&self) -> bool {
        self.summarizer.is_some()
    }

    pub async fn explore(
        &self,
        org_id: &str,
        scope: QueryScope,
        query: ExplorerQuery,
    ) -> Result<SearchPage<Value>, QueryError> {
        let request = ExplorerRequest {
            org_id: org_id.to_owned(),
            namespace: scope.namespace,
            as_of: scope.as_of,
            limit: scope.limit.unwrap_or(100),
            offset: scope.offset.unwrap_or(0),
            query,
        };
        request.validate().map_err(|_| QueryError::Invalid)?;
        let _permit = self.admit_read()?;
        let result = tokio::time::timeout(Duration::from_secs(30), self.graph.explore(&request))
            .await
            .map_err(|_| BackendError::Timeout(30_000))??;
        _permit.complete().await;
        Ok(result)
    }

    pub async fn search(
        &self,
        org_id: &str,
        request: SearchQuery,
    ) -> Result<SearchResult, QueryError> {
        let limit = request
            .config
            .as_ref()
            .map(|c| c.limit)
            .or(request.limit)
            .unwrap_or(20);
        if request.query.trim().is_empty()
            || request.query.len() > 2000
            || !(1..=100).contains(&limit)
            || request.namespace.as_deref().is_some_and(|s| {
                s.trim().is_empty() || s.len() > 256 || s.chars().any(char::is_control)
            })
            || request.entity_types.len() > kg_core::traits::graph_explorer::MAX_ENTITY_TYPE_FILTERS
            || request
                .entity_types
                .iter()
                .any(|t| t.trim().is_empty() || t.len() > 256 || t.chars().any(char::is_control))
        {
            return Err(QueryError::Invalid);
        }
        let recipe = request.recipe.unwrap_or(if request.semantic {
            SearchRecipe::Hybrid
        } else {
            SearchRecipe::Keyword
        });
        let custom = request.config.is_some();
        let mut config = request.config.clone().unwrap_or_else(|| recipe.config());
        let uses_vectors = if custom {
            config
                .methods
                .iter()
                .chain(&config.relationship_methods)
                .chain(&config.community_methods)
                .any(|m| {
                    matches!(
                        m,
                        kg_core::search::SearchMethod::Vector
                            | kg_core::search::SearchMethod::GraphAnchored
                    )
                })
        } else {
            !matches!(recipe, SearchRecipe::Keyword)
        };
        // A Saga-scoped search is keyword-only and namespace-bound; check that
        // before provider availability so the caller sees the shape error first.
        if request.saga.is_some() && (custom || uses_vectors || request.namespace.is_none()) {
            return Err(QueryError::Invalid);
        }
        if uses_vectors && !self.semantic_available {
            return Err(QueryError::SemanticUnavailable);
        }
        if request.include_relationships && !custom {
            if request.saga.is_some() {
                return Err(QueryError::Invalid);
            }
            config
                .scopes
                .push(kg_core::search::SearchScope::Relationships);
            config.relationship_methods = config.methods.clone();
            config.evidence_prefetch = (limit * 3).min(300);
        }
        config.limit = limit;
        if !custom {
            config.prefetch = (limit * 3).min(300);
            config.include_evidence = request.include_evidence;
            config.include_signals = request.include_signals.unwrap_or(true);
        }
        config.saga_uuid = None;
        config.namespaces = request.namespace.clone().into_iter().collect();
        config.entity_types = request.entity_types.clone();
        config.as_of = request.as_of;
        if let Some(reference) = request.saga {
            // Resolve and verify the Saga in the caller's scope; storage then applies
            // membership before ranking limits, so the page holds members only.
            let scope = SagaScope {
                namespace: request.namespace.expect("checked above"),
                as_of: request.as_of,
            };
            let saga = self
                .saga(org_id, scope, reference)
                .await?
                .ok_or(QueryError::NotFound)?;
            config.scopes = vec![kg_core::search::SearchScope::Snapshots];
            config.saga_uuid = Some(saga.uuid);
            config.include_signals = false;
        }
        config.validate().map_err(|_| QueryError::Invalid)?;
        let _permit = self.admit_read()?;
        let result = self.search.search(&request.query, org_id, &config).await?;
        // A usable partial result can include a dropped, timed-out child read.
        // Return its data promptly but retain admission until cleanup finishes.
        if result
            .diagnostics
            .iter()
            .any(|d| d.failure == Some(kg_core::search::SearchFailure::Timeout))
        {
            drop(_permit);
        } else {
            _permit.complete().await;
        }
        Ok(result)
    }

    async fn read_saga(
        &self,
        org_id: &str,
        request: SagaRead,
    ) -> Result<SagaReadResult, QueryError> {
        request.validate(org_id).map_err(|_| QueryError::Invalid)?;
        let _permit = self.admit_read()?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            self.graph.read_saga(org_id, &request),
        )
        .await
        .map_err(|_| BackendError::Timeout(30_000))??;
        _permit.complete().await;
        Ok(result)
    }

    /// Sagas in one namespace, ordered by name. Historical visibility is
    /// applied before pagination, using each Saga's earliest member capture.
    pub async fn list_sagas(
        &self,
        org_id: &str,
        scope: SagaScope,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<SearchPage<SagaView>, QueryError> {
        if !scoped_text_valid(&scope.namespace) {
            return Err(QueryError::Invalid);
        }
        // Storage applies `as_of` before SKIP/LIMIT so offsets count visible rows
        // and a response trimmer can derive the next cursor from returned items.
        let request = SagaRead::List {
            namespace: scope.namespace,
            offset: offset.unwrap_or(0),
            limit: limit.unwrap_or(50),
            as_of: scope.as_of,
        };
        match self.read_saga(org_id, request).await? {
            SagaReadResult::Sagas(page) => Ok(SearchPage {
                items: page
                    .sagas
                    .into_iter()
                    .inspect(|node| debug_assert!(saga_visible(node, scope.as_of)))
                    .map(|node| SagaView::from_node(node, scope.as_of))
                    .collect(),
                truncated: page.truncated,
                approximate: false,
            }),
            _ => Err(unexpected_saga_result()),
        }
    }

    /// One Saga by stable UUID or by name. A name resolves to its deterministic
    /// UUID and is still verified to exist in the caller's organization and
    /// namespace. `None` when the Saga is absent or was created after `as_of`.
    pub async fn saga(
        &self,
        org_id: &str,
        scope: SagaScope,
        reference: ThreadReference,
    ) -> Result<Option<SagaView>, QueryError> {
        if !scoped_text_valid(&scope.namespace) {
            return Err(QueryError::Invalid);
        }
        let request = SagaRead::State {
            namespace: scope.namespace,
            reference,
        };
        match self.read_saga(org_id, request).await? {
            SagaReadResult::State(node) => Ok(node
                .filter(|node| saga_visible(node, scope.as_of))
                .map(|node| SagaView::from_node(node, scope.as_of))),
            _ => Err(unexpected_saga_result()),
        }
    }

    /// Members after `after_ordinal` in membership order. With `as_of`, members
    /// captured later are hidden; ordinals never change, so the last returned
    /// ordinal is always a valid cursor. Fails with `NotFound` when the Saga is
    /// not visible in the scope, so an empty page means no more members.
    pub async fn saga_members(
        &self,
        org_id: &str,
        scope: SagaScope,
        saga_uuid: Uuid,
        after_ordinal: u64,
        limit: Option<usize>,
    ) -> Result<SagaMemberPage, QueryError> {
        self.saga(
            org_id,
            scope.clone(),
            ThreadReference::Uuid { uuid: saga_uuid },
        )
        .await?
        .ok_or(QueryError::NotFound)?;
        let request = SagaRead::Members {
            namespace: scope.namespace,
            saga_uuid,
            after_ordinal,
            through_ordinal: None,
            limit: limit.unwrap_or(50),
            captured_through: scope.as_of,
        };
        match self.read_saga(org_id, request).await? {
            SagaReadResult::Members(page) => Ok(page),
            _ => Err(unexpected_saga_result()),
        }
    }
}

impl GraphQueryService {
    /// Summarize one Saga's uncovered members and return the Saga afterwards.
    /// The Saga must exist in the namespace before any run is registered. Callers
    /// own authorization: this writes to the graph and may spend model tokens.
    ///
    /// `cancel` reaches the pipeline, which stops between pages; dropping this
    /// future (a closed HTTP connection, an MCP cancellation) cancels the same
    /// way instead of abandoning a write mid-flight. Once the run has started,
    /// every outcome keeps `run_id` and the committed counts: a failed follow-up
    /// read returns the outcome with `saga: None` rather than an error.
    pub async fn summarize_saga(
        &self,
        org_id: &str,
        namespace: String,
        reference: ThreadReference,
        run_id: Uuid,
        cancel: CancellationToken,
    ) -> Result<SagaSummaryOutcome, QueryError> {
        let summarizer = self
            .summarizer
            .clone()
            .ok_or(QueryError::SummariesUnavailable)?;
        if run_id.is_nil() {
            return Err(QueryError::Invalid);
        }
        let scope = SagaScope {
            namespace,
            as_of: None,
        };
        let saga_uuid = self
            .saga(org_id, scope.clone(), reference)
            .await?
            .ok_or(QueryError::NotFound)?
            .uuid;
        let output = {
            // The permit travels with the task: a cancelled or abandoned caller
            // must not free capacity while the pipeline is still finishing.
            let permit = self
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| QueryError::Busy)?;
            let token = cancel.child_token();
            let guard = token.clone().drop_guard();
            let task = tokio::spawn({
                let summarizer = summarizer.clone();
                let org = org_id.to_owned();
                let namespace = scope.namespace.clone();
                async move {
                    let _permit = permit;
                    summarizer
                        .summarize(&org, &namespace, saga_uuid, run_id, token)
                        .await
                }
            });
            let result = task.await.map_err(|join| {
                QueryError::Summary(PipelineError::TaskPanic(format!(
                    "Thread summary task ended abnormally: {join}"
                )))
            })?;
            drop(guard);
            result?
        };
        let (saga, saga_read_error) = match self
            .saga(org_id, scope, ThreadReference::Uuid { uuid: saga_uuid })
            .await
        {
            Ok(Some(view)) => (Some(view), None),
            Ok(None) => (None, Some("not_found".to_owned())),
            Err(error) => (None, Some(read_error_code(&error).to_owned())),
        };
        Ok(SagaSummaryOutcome {
            run_id: output.run_id,
            batches: output.batches.len(),
            replayed_batches: output.replayed_batches(),
            memberships_summarized: output.committed.saga_memberships_summarized,
            summaries_updated: output.committed.saga_summaries_updated,
            incomplete_followups: output.incomplete_followups,
            saga,
            saga_read_error,
        })
    }
}

/// Stable, text-free code for a follow-up read failure after a committed run.
fn read_error_code(error: &QueryError) -> &'static str {
    match error {
        QueryError::Busy => "busy",
        QueryError::NotFound => "not_found",
        QueryError::Invalid => "invalid",
        QueryError::RestartRequired => "restart_required",
        QueryError::ViewLimit => "view_limit",
        QueryError::Backend(BackendError::Timeout(_)) => "timeout",
        QueryError::Backend(_) => "backend",
        QueryError::SemanticUnavailable
        | QueryError::SummariesUnavailable
        | QueryError::Summary(_) => "unavailable",
    }
}

fn unexpected_saga_result() -> QueryError {
    QueryError::Backend(BackendError::Deserialization(
        "unexpected Thread read result".into(),
    ))
}

#[cfg(test)]
mod saga_tests {
    use super::*;
    use kg_core::saga::{saga_uuid, SagaMember, SagaPage};
    use serde_json::json;
    use std::sync::Mutex;

    fn day(n: u32) -> DateTime<Utc> {
        format!("2026-01-{n:02}T00:00:00Z").parse().unwrap()
    }
    fn node(name: &str, created: u32, summary: &str, covered: Option<u32>) -> ThreadNode {
        ThreadNode {
            summary_incomplete_reason: None,
            summary_incomplete_from_ordinal: None,
            summary_supporting_snapshot_uuids: if summary.is_empty() {
                vec![]
            } else {
                vec![Uuid::from_u128(7)]
            },
            revision: 3,
            last_membership_ordinal: 3,
            summary_revision: (!summary.is_empty()).then(|| Uuid::from_u128(9)),
            summary_cursor: if summary.is_empty() { 0 } else { 2 },
            uuid: saga_uuid("org", "prod", name),
            org_id: "org".into(),
            namespace: "prod".into(),
            name: name.into(),
            labels: vec![],
            created_at: day(created),
            summary: summary.into(),
            first_snapshot_uuid: Some(Uuid::from_u128(1)),
            last_snapshot_uuid: Some(Uuid::from_u128(3)),
            last_summarized_at: covered.map(|_| day(20)),
            last_summarized_snapshot_captured_at: covered.map(day),
            first_captured_at: None,
        }
    }

    struct Sagas {
        nodes: Vec<ThreadNode>,
        reads: Mutex<Vec<SagaRead>>,
    }
    #[async_trait::async_trait]
    impl GraphExplorerBackend for Sagas {
        async fn explore(&self, _: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
            unreachable!("saga reads never browse entities")
        }
        async fn read_saga(
            &self,
            org: &str,
            request: &SagaRead,
        ) -> Result<SagaReadResult, BackendError> {
            assert_eq!(org, "org");
            assert_eq!(request.namespace(), "prod");
            self.reads.lock().unwrap().push(request.clone());
            Ok(match request {
                SagaRead::State { reference, .. } => {
                    let wanted = match reference {
                        ThreadReference::Name { name } => saga_uuid(org, "prod", name),
                        ThreadReference::Uuid { uuid } => *uuid,
                    };
                    SagaReadResult::State(self.nodes.iter().find(|n| n.uuid == wanted).cloned())
                }
                SagaRead::List {
                    offset,
                    limit,
                    as_of,
                    ..
                } => {
                    let sagas: Vec<_> = self
                        .nodes
                        .iter()
                        .filter(|n| as_of.is_none_or(|at| earliest_capture(n) <= at))
                        .skip(*offset)
                        .take(limit + 1)
                        .cloned()
                        .collect();
                    let truncated = sagas.len() > *limit;
                    SagaReadResult::Sagas(SagaPage {
                        sagas: sagas.into_iter().take(*limit).collect(),
                        truncated,
                    })
                }
                SagaRead::Members {
                    captured_through, ..
                } => {
                    let members = (1..=3u64)
                        .map(|ordinal| SagaMember {
                            snapshot_uuid: Uuid::from_u128(ordinal.into()),
                            captured_at: day(ordinal as u32 * 4),
                            created_at: day(20),
                            ordinal,
                            previous_snapshot_uuid: None,
                            snapshot_name: None,
                            snapshot_source: None,
                        })
                        .filter(|m| captured_through.is_none_or(|t| m.captured_at <= t))
                        .collect();
                    SagaReadResult::Members(SagaMemberPage {
                        members,
                        truncated: false,
                    })
                }
                _ => unreachable!("interactive reads only"),
            })
        }
    }
    struct NoSearch;
    #[async_trait::async_trait]
    impl kg_core::traits::SearchBackend for NoSearch {}

    /// Answers snapshot searches with one hit and records the filters it saw.
    struct SnapshotSearch(Mutex<Vec<kg_core::search::SearchFilter>>);
    #[async_trait::async_trait]
    impl kg_core::traits::SearchBackend for SnapshotSearch {
        async fn search_snapshots(
            &self,
            request: &kg_core::search::EvidenceSearch,
        ) -> Result<SearchPage<kg_core::search::SnapshotHit>, BackendError> {
            request.validate()?;
            self.0.lock().unwrap().push(request.filter.clone());
            Ok(SearchPage::bounded(
                vec![kg_core::search::SnapshotHit {
                    uuid: Uuid::from_u128(1),
                    name: "observation".into(),
                    source: "logs".into(),
                    namespace: "prod".into(),
                    captured_at: Some("2026-01-04T00:00:00Z".into()),
                    content: "checkout failed over".into(),
                    content_truncated: false,
                    content_start: 0,
                    content_end: 20,
                    selection_kind: kg_core::search::ExcerptSelection::Matched,
                    selection_limited: false,
                    score: 1.0,
                    model_score: None,
                }],
                request.limit,
            ))
        }
    }

    #[tokio::test]
    async fn saga_search_can_use_the_last_available_query_permit() {
        let graph = Arc::new(Sagas {
            nodes: vec![node("incident", 1, "", None)],
            reads: Mutex::new(vec![]),
        });
        let snapshots = Arc::new(SnapshotSearch(Mutex::new(vec![])));
        let service = GraphQueryService::new(graph, Arc::new(SearchEngine::new(snapshots)), false);
        let _occupied = service.permits.acquire_many(31).await.unwrap();
        let result = service
            .search(
                "org",
                SearchQuery {
                    config: None,
                    include_relationships: false,
                    recipe: None,
                    include_evidence: false,
                    include_signals: None,
                    query: "checkout".into(),
                    namespace: Some("prod".into()),
                    as_of: None,
                    limit: Some(5),
                    semantic: false,
                    entity_types: Vec::new(),
                    saga: Some(by_name("incident")),
                },
            )
            .await
            .unwrap();
        assert_eq!(result.snapshots.len(), 1);
        assert_eq!(service.permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn saga_scoped_search_resolves_the_saga_and_searches_snapshots_only() {
        let graph = Arc::new(Sagas {
            nodes: vec![node("incident", 1, "", None)],
            reads: Mutex::new(vec![]),
        });
        let snapshots = Arc::new(SnapshotSearch(Mutex::new(vec![])));
        let service = GraphQueryService::new(
            graph,
            Arc::new(kg_search::SearchEngine::new(snapshots.clone())),
            false,
        );
        let request =
            |saga: Option<ThreadReference>, namespace: Option<&str>, semantic| SearchQuery {
                config: None,
                include_relationships: false,
                recipe: None,
                include_evidence: false,
                include_signals: None,
                query: "checkout".into(),
                namespace: namespace.map(Into::into),
                as_of: None,
                limit: Some(5),
                semantic,
                entity_types: Vec::new(),
                saga,
            };
        let result = service
            .search(
                "org",
                request(Some(by_name("incident")), Some("prod"), false),
            )
            .await
            .unwrap();
        assert!(result.hits.is_empty());
        assert_eq!(result.snapshots.len(), 1);
        let seen = snapshots.0.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].saga_uuid,
            Some(saga_uuid("org", "prod", "incident"))
        );
        assert_eq!(seen[0].namespaces, ["prod"]);

        assert!(matches!(
            service
                .search("org", request(Some(by_name("incident")), None, false))
                .await,
            Err(QueryError::Invalid)
        ));
        assert!(matches!(
            service
                .search(
                    "org",
                    request(Some(by_name("incident")), Some("prod"), true)
                )
                .await,
            Err(QueryError::Invalid)
        ));
        assert!(matches!(
            service
                .search(
                    "org",
                    request(Some(by_name("unknown")), Some("prod"), false)
                )
                .await,
            Err(QueryError::NotFound)
        ));
        assert_eq!(
            snapshots.0.lock().unwrap().len(),
            1,
            "rejected requests never reach storage"
        );
    }

    fn service(nodes: Vec<ThreadNode>) -> (Arc<Sagas>, GraphQueryService) {
        let graph = Arc::new(Sagas {
            nodes,
            reads: Mutex::new(vec![]),
        });
        let service = GraphQueryService::new(
            graph.clone(),
            Arc::new(kg_search::SearchEngine::new(Arc::new(NoSearch))),
            false,
        );
        (graph, service)
    }

    /// Records what it was asked to summarize and answers with a fixed run.
    struct Recorder {
        calls: Mutex<Vec<(String, String, Uuid, Uuid)>>,
        fail: bool,
    }
    #[async_trait::async_trait]
    impl SagaSummarizer for Recorder {
        async fn summarize(
            &self,
            org_id: &str,
            namespace: &str,
            saga_uuid: Uuid,
            run_id: Uuid,
            _cancel: CancellationToken,
        ) -> Result<PipelineOutput, PipelineError> {
            self.calls
                .lock()
                .unwrap()
                .push((org_id.into(), namespace.into(), saga_uuid, run_id));
            if self.fail {
                return Err(PipelineError::Cancelled);
            }
            let counts = kg_core::pipeline::CommittedCounts {
                saga_memberships_summarized: 3,
                saga_summaries_updated: 1,
                ..Default::default()
            };
            Ok(PipelineOutput {
                run_id,
                committed: counts,
                newly_committed: counts,
                batches: vec![
                    kg_core::pipeline::BatchOutcome {
                        kind: kg_core::traits::BatchKind::SagaSummary,
                        index: 0,
                        replayed: true,
                        counts: Default::default(),
                    },
                    kg_core::pipeline::BatchOutcome {
                        kind: kg_core::traits::BatchKind::SagaSummary,
                        index: 1,
                        replayed: false,
                        counts,
                    },
                ],
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn on_demand_summaries_require_a_configured_summarizer_and_a_visible_saga() {
        let (_, plain) = service(vec![node("incident", 1, "", None)]);
        let run = Uuid::from_u128(42);
        assert!(matches!(
            plain
                .summarize_saga(
                    "org",
                    "prod".into(),
                    by_name("incident"),
                    run,
                    CancellationToken::new()
                )
                .await,
            Err(QueryError::SummariesUnavailable)
        ));
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(vec![]),
            fail: false,
        });
        let service = plain.with_saga_summarizer(recorder.clone());
        assert!(matches!(
            service
                .summarize_saga(
                    "org",
                    "prod".into(),
                    by_name("unknown"),
                    run,
                    CancellationToken::new()
                )
                .await,
            Err(QueryError::NotFound)
        ));
        assert!(matches!(
            service
                .summarize_saga(
                    "org",
                    "prod".into(),
                    by_name("incident"),
                    Uuid::nil(),
                    CancellationToken::new()
                )
                .await,
            Err(QueryError::Invalid)
        ));
        assert!(matches!(
            service
                .summarize_saga(
                    "org",
                    " ".into(),
                    by_name("incident"),
                    run,
                    CancellationToken::new()
                )
                .await,
            Err(QueryError::Invalid)
        ));
        assert!(
            recorder.calls.lock().unwrap().is_empty(),
            "rejected before any run"
        );

        let outcome = service
            .summarize_saga(
                "org",
                "prod".into(),
                by_name("incident"),
                run,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(outcome.run_id, run);
        assert_eq!((outcome.batches, outcome.replayed_batches), (2, 1));
        assert_eq!(outcome.memberships_summarized, 3);
        assert_eq!(outcome.summaries_updated, 1);
        assert_eq!(outcome.saga.as_ref().unwrap().name, "incident");
        assert_eq!(outcome.saga_read_error, None);
        let expected = saga_uuid("org", "prod", "incident");
        assert_eq!(
            *recorder.calls.lock().unwrap(),
            vec![("org".to_owned(), "prod".to_owned(), expected, run)]
        );
        let by_uuid = service
            .summarize_saga(
                "org",
                "prod".into(),
                ThreadReference::Uuid { uuid: expected },
                run,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(by_uuid.saga.unwrap().uuid, expected);
    }

    #[tokio::test]
    async fn a_failed_summary_run_is_reported_as_a_summary_error() {
        let (_, plain) = service(vec![node("incident", 1, "", None)]);
        let service = plain.with_saga_summarizer(Arc::new(Recorder {
            calls: Mutex::new(vec![]),
            fail: true,
        }));
        let failed = service
            .summarize_saga(
                "org",
                "prod".into(),
                by_name("incident"),
                Uuid::from_u128(1),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(
            failed,
            Err(QueryError::Summary(PipelineError::Cancelled))
        ));
    }
    fn scope(as_of: Option<u32>) -> SagaScope {
        SagaScope {
            namespace: "prod".into(),
            as_of: as_of.map(day),
        }
    }

    #[tokio::test]
    async fn a_summary_without_a_coverage_watermark_is_never_shown_historically() {
        // Storage validation rejects such a node; the view is the second line of defence.
        let mut node = node(
            "legacy",
            1,
            "Summarized before watermarks existed.",
            Some(10),
        );
        node.last_summarized_snapshot_captured_at = None;
        assert!(node.validate().is_err(), "storage must reject the node");
        let current = SagaView::from_node(node.clone(), None);
        assert_eq!(current.summary_withheld, None);
        assert!(current.summary.is_some());
        let historical = SagaView::from_node(node, Some(day(30)));
        assert_eq!(historical.summary, None);
        assert_eq!(
            historical.summary_withheld,
            Some(SummaryWithheld::CoverageUnknown)
        );
        assert!(historical.summary_supporting_snapshot_uuids.is_empty());
    }

    #[tokio::test]
    async fn a_backfilled_member_makes_the_saga_visible_before_its_minting_observation() {
        // Minted by a January 10 observation, then backfilled with a January 2 capture.
        let mut backfilled = node("release", 10, "", None);
        backfilled.first_captured_at = Some(day(2));
        let (graph, service) = service(vec![node("incident", 8, "", None), backfilled]);
        let at_5 = service
            .saga("org", scope(Some(5)), by_name("release"))
            .await
            .unwrap()
            .expect("visible from its earliest capture");
        assert_eq!(at_5.created_at, day(10));
        assert_eq!(at_5.earliest_captured_at, day(2));
        assert!(service
            .saga("org", scope(Some(1)), by_name("release"))
            .await
            .unwrap()
            .is_none());
        // Listing pushes the same cutoff into storage instead of filtering afterwards,
        // so the page offset counts visible rows and the incident (day 8) is skipped.
        let page = service
            .list_sagas("org", scope(Some(5)), Some(1), Some(0))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].name, "release");
        assert!(!page.truncated);
        let reads = graph.reads.lock().unwrap();
        assert!(matches!(
            reads.last(),
            Some(SagaRead::List { as_of: Some(at), offset: 0, limit: 1, .. }) if *at == day(5)
        ));
    }

    /// Saga storage whose reads start failing after a number of successful ones.
    struct FlakyReads {
        inner: Sagas,
        allowed: Mutex<usize>,
    }
    #[async_trait::async_trait]
    impl GraphExplorerBackend for FlakyReads {
        async fn explore(&self, _: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
            unreachable!("saga reads never browse entities")
        }
        async fn read_saga(
            &self,
            org: &str,
            request: &SagaRead,
        ) -> Result<SagaReadResult, BackendError> {
            let exhausted = {
                let mut allowed = self.allowed.lock().unwrap();
                if *allowed == 0 {
                    true
                } else {
                    *allowed -= 1;
                    false
                }
            };
            if exhausted {
                return Err(BackendError::Timeout(30));
            }
            self.inner.read_saga(org, request).await
        }
    }

    #[tokio::test]
    async fn a_failed_read_after_a_committed_run_keeps_the_run_id_and_counts() {
        let graph = Arc::new(FlakyReads {
            inner: Sagas {
                nodes: vec![node("incident", 1, "", None)],
                reads: Mutex::new(vec![]),
            },
            allowed: Mutex::new(1), // the existence check succeeds; the follow-up read times out
        });
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(vec![]),
            fail: false,
        });
        let service = GraphQueryService::new(
            graph,
            Arc::new(kg_search::SearchEngine::new(Arc::new(NoSearch))),
            false,
        )
        .with_saga_summarizer(recorder.clone());
        let run = Uuid::from_u128(77);
        let outcome = service
            .summarize_saga(
                "org",
                "prod".into(),
                by_name("incident"),
                run,
                CancellationToken::new(),
            )
            .await
            .expect("the commit stands even though the follow-up read failed");
        assert_eq!(outcome.run_id, run);
        assert_eq!(outcome.memberships_summarized, 3);
        assert_eq!(outcome.summaries_updated, 1);
        assert!(outcome.saga.is_none());
        assert_eq!(outcome.saga_read_error.as_deref(), Some("timeout"));
        assert_eq!(recorder.calls.lock().unwrap().len(), 1);
        let json = serde_json::to_value(&outcome).unwrap();
        assert_eq!(json["run_id"], json!(run));
        assert_eq!(json["thread"], Value::Null);
        assert_eq!(json["thread_read_error"], "timeout");
    }

    /// Blocks until cancelled and reports that it saw the cancellation.
    struct Blocking {
        cancelled: Arc<std::sync::atomic::AtomicBool>,
        started: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl SagaSummarizer for Blocking {
        async fn summarize(
            &self,
            _: &str,
            _: &str,
            _: Uuid,
            _: Uuid,
            cancel: CancellationToken,
        ) -> Result<PipelineOutput, PipelineError> {
            self.started.notify_one();
            cancel.cancelled().await;
            self.cancelled
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Err(PipelineError::Cancelled)
        }
    }

    #[tokio::test]
    async fn cancellation_reaches_the_summarizer_from_the_token_and_from_a_dropped_caller() {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let blocking = Arc::new(Blocking {
            cancelled: flag.clone(),
            started: tokio::sync::Notify::new(),
        });
        let (_, plain) = service(vec![node("incident", 1, "", None)]);
        let service = Arc::new(plain.with_saga_summarizer(blocking.clone()));

        // Explicit cancellation: the run ends with the pipeline's cancellation error.
        let token = CancellationToken::new();
        let pending = tokio::spawn({
            let service = service.clone();
            let token = token.clone();
            async move {
                service
                    .summarize_saga(
                        "org",
                        "prod".into(),
                        by_name("incident"),
                        Uuid::from_u128(1),
                        token,
                    )
                    .await
            }
        });
        blocking.started.notified().await;
        token.cancel();
        let result = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .expect("cancellation must end the run")
            .unwrap();
        assert!(matches!(
            result,
            Err(QueryError::Summary(PipelineError::Cancelled))
        ));
        assert!(flag.swap(false, std::sync::atomic::Ordering::SeqCst));

        // An abandoned caller (dropped future) cancels cooperatively as well.
        let abandoned = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .summarize_saga(
                        "org",
                        "prod".into(),
                        by_name("incident"),
                        Uuid::from_u128(2),
                        CancellationToken::new(),
                    )
                    .await
            }
        });
        blocking.started.notified().await;
        abandoned.abort();
        let _ = abandoned.await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !flag.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dropping the caller must cancel the pipeline");
    }
    fn by_name(name: &str) -> ThreadReference {
        ThreadReference::Name { name: name.into() }
    }

    #[tokio::test]
    async fn summary_is_withheld_when_it_covers_observations_after_as_of() {
        let (_, service) = service(vec![
            node("incident", 1, "Payments failed over.", Some(10)),
            node("fresh", 1, "", None),
        ]);
        let current = service
            .saga("org", scope(None), by_name("incident"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.summary.as_deref(), Some("Payments failed over."));
        assert_eq!(current.summary_withheld, None);
        assert_eq!(
            current.summary_supporting_snapshot_uuids,
            vec![Uuid::from_u128(7)]
        );
        assert_eq!(current.summary_covers_captured_through, Some(day(10)));

        let early = service
            .saga("org", scope(Some(5)), by_name("incident"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(early.summary, None);
        assert_eq!(
            early.summary_withheld,
            Some(SummaryWithheld::CoversLaterObservations)
        );
        assert!(early.summary_supporting_snapshot_uuids.is_empty());
        assert_eq!(early.summary_revision, None);
        // Coverage metadata stays visible so a client can pick a later as_of.
        assert_eq!(early.summary_covers_captured_through, Some(day(10)));
        assert_eq!(early.total_members, 3);

        let exact = service
            .saga("org", scope(Some(10)), by_name("incident"))
            .await
            .unwrap()
            .unwrap();
        assert!(exact.summary.is_some());

        let fresh = service
            .saga("org", scope(None), by_name("fresh"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fresh.summary, None);
        assert_eq!(fresh.summary_withheld, Some(SummaryWithheld::NotSummarized));
    }

    #[tokio::test]
    async fn sagas_created_after_as_of_do_not_exist_in_that_scope() {
        let (_, service) = service(vec![node("later", 8, "", None), node("early", 1, "", None)]);
        assert!(service
            .saga("org", scope(Some(5)), by_name("later"))
            .await
            .unwrap()
            .is_none());
        assert!(service
            .saga("org", scope(Some(8)), by_name("later"))
            .await
            .unwrap()
            .is_some());
        let listed = service
            .list_sagas("org", scope(Some(5)), None, None)
            .await
            .unwrap();
        assert_eq!(
            listed
                .items
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            ["early"]
        );
        let members = service
            .saga_members(
                "org",
                scope(Some(5)),
                saga_uuid("org", "prod", "later"),
                0,
                None,
            )
            .await;
        assert!(matches!(members, Err(QueryError::NotFound)));
        let missing = service
            .saga_members("org", scope(None), Uuid::from_u128(99), 0, None)
            .await;
        assert!(matches!(missing, Err(QueryError::NotFound)));
    }

    #[tokio::test]
    async fn members_use_as_of_as_the_capture_cutoff_in_storage() {
        let (graph, service) = service(vec![node("incident", 1, "", None)]);
        let saga = saga_uuid("org", "prod", "incident");
        let page = service
            .saga_members("org", scope(Some(9)), saga, 0, Some(10))
            .await
            .unwrap();
        assert_eq!(
            page.members.iter().map(|m| m.ordinal).collect::<Vec<_>>(),
            [1, 2]
        );
        let cutoff_reached_storage = graph.reads.lock().unwrap().iter().any(|r| {
            matches!(
                r,
                SagaRead::Members { captured_through: Some(t), after_ordinal: 0, limit: 10, .. } if *t == day(9)
            )
        });
        assert!(cutoff_reached_storage);
        let all = service
            .saga_members("org", scope(None), saga, 1, None)
            .await
            .unwrap();
        assert_eq!(
            all.members.iter().map(|m| m.ordinal).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[tokio::test]
    async fn listing_pages_by_offset_and_rejects_invalid_scope() {
        let nodes: Vec<_> = (1..=3)
            .map(|n| node(&format!("saga-{n}"), 1, "", None))
            .collect();
        let (_, service) = service(nodes);
        let first = service
            .list_sagas("org", scope(None), Some(2), None)
            .await
            .unwrap();
        assert_eq!(first.items.len(), 2);
        assert!(first.truncated);
        let second = service
            .list_sagas("org", scope(None), Some(2), Some(2))
            .await
            .unwrap();
        assert_eq!(second.items.len(), 1);
        assert!(!second.truncated);
        for (namespace, limit, offset) in [
            (" ", 1, 0),
            ("prod", 0, 0),
            ("prod", 201, 0),
            ("prod", 1, 100_001),
            (
                "a
b", 1, 0,
            ),
        ] {
            let scope = SagaScope {
                namespace: namespace.into(),
                as_of: None,
            };
            assert!(
                matches!(
                    service
                        .list_sagas("org", scope, Some(limit), Some(offset))
                        .await,
                    Err(QueryError::Invalid)
                ),
                "{namespace:?} {limit} {offset}"
            );
        }
        assert!(matches!(
            service
                .saga(
                    "org",
                    scope(None),
                    ThreadReference::Name { name: " ".into() }
                )
                .await,
            Err(QueryError::Invalid)
        ));
    }
}

#[cfg(test)]
mod recipe_contract_tests {
    use super::*;
    #[test]
    fn public_recipes_select_existing_engine_configs_and_reject_unknown_names() {
        for (name, expected) in [
            ("keyword", SearchConfig::keyword_only()),
            ("hybrid", SearchConfig::hybrid_rrf()),
            ("semantic", SearchConfig::semantic_only()),
            ("diverse", SearchConfig::hybrid_mmr()),
        ] {
            let request: SearchQuery =
                serde_json::from_value(serde_json::json!({"query":"orders", "recipe":name}))
                    .unwrap();
            assert_eq!(
                serde_json::to_value(request.recipe.unwrap().config()).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
        assert!(serde_json::from_value::<SearchQuery>(
            serde_json::json!({"query":"orders", "recipe":"invented"})
        )
        .is_err());
    }
}

#[cfg(test)]
mod admission_cleanup_tests {
    use super::*;
    struct Cleanup {
        started: tokio::sync::Notify,
        finish: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl GraphExplorerBackend for Cleanup {
        async fn explore(&self, _: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
            unreachable!()
        }
        async fn settle_cancelled_reads(&self) {
            self.started.notify_one();
            self.finish.notified().await;
        }
    }
    #[tokio::test]
    async fn completed_read_does_not_wait_for_unrelated_active_work() {
        let backend = Arc::new(Cleanup {
            started: tokio::sync::Notify::new(),
            finish: tokio::sync::Notify::new(),
        });
        let semaphore = Arc::new(tokio::sync::Semaphore::new(2));
        let abandoned = ReadPermit {
            permit: Some(semaphore.clone().try_acquire_owned().unwrap()),
            backend: backend.clone(),
        };
        drop(abandoned);
        backend.started.notified().await;
        let complete = ReadPermit {
            permit: Some(semaphore.clone().try_acquire_owned().unwrap()),
            backend: backend.clone(),
        };
        tokio::time::timeout(Duration::from_millis(100), complete.complete())
            .await
            .expect("successful read must not wait for unrelated cleanup");
        assert_eq!(semaphore.available_permits(), 1);
        backend.finish.notify_one();
    }
    #[tokio::test]
    async fn dropped_read_keeps_admission_until_cleanup_finishes() {
        let backend = Arc::new(Cleanup {
            started: tokio::sync::Notify::new(),
            finish: tokio::sync::Notify::new(),
        });
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let guard = ReadPermit {
            permit: Some(semaphore.clone().try_acquire_owned().unwrap()),
            backend: backend.clone(),
        };
        drop(guard);
        backend.started.notified().await;
        assert_eq!(semaphore.available_permits(), 0);
        backend.finish.notify_one();
        let released = tokio::time::timeout(Duration::from_secs(1), semaphore.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(released);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
