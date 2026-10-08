//! Graph explorer HTTP reads and authorized Saga summaries. Organization is server-configured.
use axum::{
    extract::{DefaultBodyLimit, MatchedPath, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use kg_core::{
    errors::BackendError,
    saga::ThreadReference,
    traits::graph_explorer::{ExplorerDirection, ExplorerQuery},
};
use query::{GraphQueryService, QueryError, QueryScope, SagaScope, SearchQuery};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub mod details;
pub mod graph;
pub mod investigation;
pub mod mcp;

pub mod query;
pub mod rules;
pub mod summaries;

/// Minimum length of the bearer token that authorizes HTTP writes.
pub const MIN_WRITE_TOKEN_LEN: usize = 32;

#[derive(Clone)]
pub struct AppState {
    pub query: Arc<GraphQueryService>,
    pub org_id: String,
    /// Bearer token required by every writing route. `None` disables them.
    pub write_token: Option<Arc<str>>,
    /// Learned-rule store for the admin routes. `None` disables them (404).
    pub rules: Option<Arc<dyn kg_core::traits::rule_store::RuleStore>>,
    /// Engine used to synchronously repair graph effects after rule lifecycle writes.
    pub rule_repair_engine: Option<Arc<dyn rules::RuleRepairService>>,
}

pub fn router(state: AppState) -> Router {
    let requests = Arc::new(tokio::sync::Semaphore::new(32));
    let cleanup_backend = state.query.graph.clone();
    Router::new()
        .route(
            "/api/v1/health",
            get(|| async { Json(json!({"status":"ready"})) }),
        )
        .route(
            "/api/v1/graph/expand",
            post(graph_expand).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route(
            "/api/v1/graph/view",
            post(graph_view).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route("/api/v1/graph/revision", get(graph_revision))
        .route("/api/v1/capabilities", get(capabilities))
        .route("/api/v1/graph/schema", get(graph_schema))
        .route("/api/v1/graph/changes", post(graph_changes))
        .route("/api/v1/graph/paths", post(graph_paths))
        .route("/api/v1/graph/filters", get(graph_filters))
        .route("/api/v1/relationships/{edge_id}", get(relationship))
        .route("/api/v1/snapshots/{snapshot_id}", get(snapshot_detail))
        .route("/api/v1/details", get(details::detail))
        .route("/api/v1/catalog", get(catalog))
        .route("/api/v1/search", post(search))
        .route("/api/v1/search/config", get(search_configuration))
        .route("/api/v1/entities/{entity_type}/{chain_id}", get(entity))
        .route(
            "/api/v1/entities/{entity_type}/{chain_id}/versions",
            get(versions),
        )
        .route(
            "/api/v1/entities/{entity_type}/{chain_id}/neighbors",
            get(neighbors),
        )
        .route("/api/v1/threads", get(sagas))
        .route("/api/v1/threads/by-name/{name}", get(saga_by_name))
        .route("/api/v1/threads/{saga_uuid}", get(saga))
        .route("/api/v1/threads/{saga_uuid}/members", get(saga_members))
        .route("/api/v1/threads/{saga_uuid}/summary", post(summarize_saga))
        // Legacy routes share the same authorization and handlers.
        .route("/api/v1/sagas", get(sagas))
        .route("/api/v1/sagas/by-name/{name}", get(saga_by_name))
        .route("/api/v1/sagas/{saga_uuid}", get(saga))
        .route("/api/v1/sagas/{saga_uuid}/members", get(saga_members))
        .route("/api/v1/sagas/{saga_uuid}/summary", post(summarize_saga))
        .route("/api/v1/rules/{source}", get(list_rules))
        .route("/api/v1/rules/{source}/{id}", get(rule_detail))
        .route("/api/v1/rules/{source}/{id}/activate", post(activate_rule))
        .route("/api/v1/rules/{source}/{id}/reject", post(reject_rule))
        .route("/api/v1/rules/{source}/{id}/revoke", post(revoke_rule))
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let requests = requests.clone();
                let backend = cleanup_backend.clone();
                async move {
                    let Ok(_permit) = requests.try_acquire_owned() else {
                        return ApiError(
                            StatusCode::TOO_MANY_REQUESTS,
                            "Too many active graph requests",
                        )
                        .into_response();
                    };
                    let route = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(|p| p.as_str().to_owned())
                        .unwrap_or_else(|| "unmatched".into());
                    let started = std::time::Instant::now();
                    let cancel = CancellationToken::new();
                    let _cancel_on_drop = cancel.clone().drop_guard();
                    // Keep HTTP capacity until the handler and cancelled DB reads settle.
                    let task = tokio::spawn(async move {
                        let (response, abandoned) = tokio::select! {
                            response = next.run(request) => (response, false),
                            _ = cancel.cancelled() => (StatusCode::REQUEST_TIMEOUT.into_response(), true),
                        };
                        // The dropped handler schedules its own cleanup. Only an abandoned
                        // request waits on the backend's conservative cleanup barrier.
                        if abandoned { backend.settle_cancelled_reads().await; }
                        drop(_permit);
                        response
                    });
                    let response = task
                        .await
                        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
                    tracing::info!(
                        route,
                        status = response.status().as_u16(),
                        duration_ms = started.elapsed().as_millis() as u64,
                        "graph HTTP request"
                    );
                    response
                }
            },
        ))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .with_state(state)
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Scope {
    representation: Option<Representation>,
    namespace: Option<String>,
    as_of: Option<DateTime<Utc>>,
    limit: Option<usize>,
    offset: Option<usize>,
    #[serde(default)]
    direction: ExplorerDirection,
    /// Comma-separated entity types; neighbours only. Empty means every type.
    entity_types: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Representation {
    Typed,
    Overview,
}

async fn graph_expand(
    State(state): State<AppState>,
    Json(request): Json<graph::GraphRequest>,
) -> Result<Json<graph::GraphPage>, ApiError> {
    Ok(Json(
        state
            .query
            .graph_view(&state.org_id, request, false)
            .await?,
    ))
}
// This conservative marker is scoped to the configured organization, not a
// namespace. Changes elsewhere in that organization may trigger an extra read.
async fn graph_revision(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let page = state
        .query
        .explore(
            &state.org_id,
            QueryScope {
                limit: Some(64),
                ..Default::default()
            },
            ExplorerQuery::GraphRevision,
        )
        .await?;
    Ok((
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"revision": page.items})),
    ))
}
async fn graph_view(
    State(state): State<AppState>,
    Json(request): Json<graph::GraphRequest>,
) -> Result<Json<graph::GraphPage>, ApiError> {
    Ok(Json(
        state.query.graph_view(&state.org_id, request, true).await?,
    ))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilterRequest {
    dimension: String,
    namespace: Option<String>,
    as_of: Option<DateTime<Utc>>,
    #[serde(default)]
    search: String,
    #[serde(default)]
    offset: usize,
}
async fn graph_filters(
    State(state): State<AppState>,
    Query(request): Query<FilterRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        state
            .query
            .graph_filters(
                &state.org_id,
                request.namespace,
                request.as_of,
                &request.dimension,
                request.search,
                request.offset,
            )
            .await?,
    ))
}

fn entity_type_list(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
        .collect()
}
/// Query parameters for Saga reads. The namespace is required because Saga
/// names are unique only within an organization and namespace.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SagaQuery {
    namespace: Option<String>,
    as_of: Option<DateTime<Utc>>,
    limit: Option<usize>,
    offset: Option<usize>,
    after_ordinal: Option<u64>,
}
impl SagaQuery {
    fn scope(&self) -> Result<SagaScope, ApiError> {
        Ok(SagaScope {
            namespace: self.namespace.clone().ok_or(ApiError(
                StatusCode::BAD_REQUEST,
                "Thread reads require a namespace",
            ))?,
            as_of: self.as_of,
        })
    }
}
struct ApiError(StatusCode, &'static str);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}
impl From<QueryError> for ApiError {
    fn from(error: QueryError) -> Self {
        match error {
            QueryError::RestartRequired => {
                return Self(
                    StatusCode::GONE,
                    "Graph continuation expired; refresh the view",
                )
            }
            QueryError::ViewLimit => {
                return Self(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "Saved graph exceeds the visible node limit; narrow filters",
                )
            }
            QueryError::Invalid => return Self(StatusCode::BAD_REQUEST, "Invalid request"),
            QueryError::SemanticUnavailable => {
                return Self(StatusCode::BAD_REQUEST, "Semantic search is not configured")
            }
            QueryError::Busy => {
                return Self(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Too many active graph requests",
                )
            }
            QueryError::NotFound => {
                return Self(StatusCode::NOT_FOUND, "Record is not visible in this scope")
            }
            QueryError::SummariesUnavailable => {
                return Self(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Thread summaries are not configured",
                )
            }
            QueryError::Summary(_) => {
                return Self(StatusCode::BAD_GATEWAY, "Thread summary did not complete")
            }
            QueryError::Backend(BackendError::Timeout(_)) => {
                return Self(StatusCode::GATEWAY_TIMEOUT, "Graph read timed out")
            }
            _ => {}
        }
        // Backend text can contain source properties or provider details.
        if let QueryError::Backend(error) = &error {
            tracing::warn!(error_kind = ?kg_core::search::SearchFailure::from(error), "graph request failed");
        }
        Self(
            StatusCode::SERVICE_UNAVAILABLE,
            "Graph storage is unavailable; retry the request",
        )
    }
}
async fn read(state: &AppState, scope: Scope, query: ExplorerQuery) -> Result<Value, ApiError> {
    let request = QueryScope {
        namespace: scope.namespace,
        as_of: scope.as_of,
        limit: scope.limit,
        offset: scope.offset,
    };
    let typed = scope.representation.is_some();
    let neighbor = matches!(
        &query,
        ExplorerQuery::Neighbors { .. } | ExplorerQuery::CanvasNeighbors { .. }
    );
    let mut page = state
        .query
        .explore(&state.org_id, request.clone(), query)
        .await?;
    if typed {
        page.items = page
            .items
            .iter()
            .map(|v| {
                if neighbor {
                    graph::presentation::typed_neighbor(v)
                } else {
                    graph::presentation::typed_record(v)
                }
            })
            .collect();
    }
    Ok(
        json!({"items":page.items,"truncated":page.truncated,"next_offset":page.truncated.then_some(request.offset.unwrap_or(0)+request.limit.unwrap_or(100))}),
    )
}
async fn catalog(
    State(state): State<AppState>,
    Query(scope): Query<Scope>,
) -> Result<Json<Value>, ApiError> {
    let mut result = read(&state, scope, ExplorerQuery::Catalog).await?;
    result["org_id"] = json!(state.org_id);
    result["semantic_available"] = json!(state.query.semantic_available());
    Ok(Json(result))
}
async fn snapshot_detail(
    State(state): State<AppState>,
    Path(uuid): Path<Uuid>,
    Query(scope): Query<QueryScope>,
) -> Result<Json<Value>, ApiError> {
    let result = state
        .query
        .explore(&state.org_id, scope, ExplorerQuery::Snapshot { uuid })
        .await?;
    Ok(Json(result.items.into_iter().next().ok_or(ApiError(
        StatusCode::NOT_FOUND,
        "Snapshot is not visible in this scope",
    ))?))
}
async fn relationship(
    State(state): State<AppState>,
    Path(edge_id): Path<Uuid>,
    Query(mut scope): Query<Scope>,
) -> Result<Json<Value>, ApiError> {
    let query = if matches!(scope.representation, Some(Representation::Overview)) {
        ExplorerQuery::CanvasRelationship { edge_id }
    } else {
        ExplorerQuery::Relationship { edge_id }
    };
    scope.representation = Some(Representation::Typed);
    let page = read(&state, scope, query).await?;
    let item = page["items"]
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .ok_or(ApiError(
            StatusCode::NOT_FOUND,
            "Relationship is not visible in this scope",
        ))?;
    Ok(Json(item))
}
async fn entity(
    State(state): State<AppState>,
    Path((entity_type, chain_id)): Path<(String, Uuid)>,
    Query(scope): Query<Scope>,
) -> Result<Json<Value>, ApiError> {
    let overview = matches!(scope.representation, Some(Representation::Overview));
    let page = read(
        &state,
        scope,
        if overview {
            ExplorerQuery::CanvasEntity {
                entity_type,
                chain_id,
            }
        } else {
            ExplorerQuery::Entity {
                entity_type,
                chain_id,
            }
        },
    )
    .await?;
    let item = page["items"]
        .as_array()
        .and_then(|v| v.first())
        .cloned()
        .ok_or(ApiError(
            StatusCode::NOT_FOUND,
            "Entity is not visible in this scope",
        ))?;
    Ok(Json(item))
}
async fn versions(
    State(state): State<AppState>,
    Path((entity_type, chain_id)): Path<(String, Uuid)>,
    Query(scope): Query<Scope>,
) -> Result<Json<Value>, ApiError> {
    let overview = matches!(scope.representation, Some(Representation::Overview));
    Ok(Json(
        read(
            &state,
            scope,
            if overview {
                ExplorerQuery::VersionHeaders {
                    entity_type,
                    chain_id,
                }
            } else {
                ExplorerQuery::Versions {
                    entity_type,
                    chain_id,
                }
            },
        )
        .await?,
    ))
}
async fn neighbors(
    State(state): State<AppState>,
    Path((entity_type, chain_id)): Path<(String, Uuid)>,
    Query(scope): Query<Scope>,
) -> Result<Json<Value>, ApiError> {
    let direction = scope.direction;
    let entity_types = entity_type_list(scope.entity_types.as_deref());
    let query = if matches!(scope.representation, Some(Representation::Overview)) {
        ExplorerQuery::CanvasNeighbors {
            entity_type,
            chain_id,
            direction,
            entity_types,
        }
    } else {
        ExplorerQuery::Neighbors {
            entity_type,
            chain_id,
            direction,
            entity_types,
        }
    };
    Ok(Json(read(&state, scope, query).await?))
}
async fn sagas(
    State(state): State<AppState>,
    Query(query): Query<SagaQuery>,
) -> Result<Json<Value>, ApiError> {
    let scope = query.scope()?;
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(50);
    let page = state
        .query
        .list_sagas(&state.org_id, scope, Some(limit), Some(offset))
        .await?;
    Ok(Json(json!({
        "items": page.items,
        "offset": offset,
        "truncated": page.truncated,
        "next_offset": page.truncated.then_some(offset + limit),
    })))
}
async fn saga_view(
    state: &AppState,
    query: &SagaQuery,
    reference: ThreadReference,
) -> Result<Json<Value>, ApiError> {
    let view = state
        .query
        .saga(&state.org_id, query.scope()?, reference)
        .await?
        .ok_or(ApiError(
            StatusCode::NOT_FOUND,
            "Thread is not visible in this scope",
        ))?;
    serde_json::to_value(view)
        .map(Json)
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Invalid Thread response"))
}
async fn saga(
    State(state): State<AppState>,
    Path(saga_uuid): Path<Uuid>,
    Query(query): Query<SagaQuery>,
) -> Result<Json<Value>, ApiError> {
    saga_view(&state, &query, ThreadReference::Uuid { uuid: saga_uuid }).await
}
async fn saga_by_name(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<SagaQuery>,
) -> Result<Json<Value>, ApiError> {
    saga_view(&state, &query, ThreadReference::Name { name }).await
}
async fn saga_members(
    State(state): State<AppState>,
    Path(saga_uuid): Path<Uuid>,
    Query(query): Query<SagaQuery>,
) -> Result<Json<Value>, ApiError> {
    let after_ordinal = query.after_ordinal.unwrap_or(0);
    let page = state
        .query
        .saga_members(
            &state.org_id,
            query.scope()?,
            saga_uuid,
            after_ordinal,
            query.limit,
        )
        .await?;
    let next = page
        .truncated
        .then(|| page.members.last().map(|m| m.ordinal))
        .flatten();
    Ok(Json(json!({
        "items": page.members,
        "after_ordinal": after_ordinal,
        "truncated": page.truncated,
        "next_after_ordinal": next,
    })))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SummaryBody {
    namespace: String,
    /// Replay identity. Omit to start a new run; repeat to finish an interrupted one.
    run_id: Option<Uuid>,
}

/// Constant-time bearer check against the configured write token.
fn authorize_write(state: &AppState, headers: &axum::http::HeaderMap) -> Result<(), ApiError> {
    use subtle::ConstantTimeEq;
    let expected = state.write_token.as_deref().ok_or(ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "Graph writes are not enabled on this server",
    ))?;
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "Write token required"))?;
    if presented.len() != expected.len()
        || presented.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() != 1
    {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "Invalid write token"));
    }
    Ok(())
}

// ---------------------------------------------------------------- rule admin

fn rule_store(
    state: &AppState,
) -> Result<&Arc<dyn kg_core::traits::rule_store::RuleStore>, ApiError> {
    state.rules.as_ref().ok_or(ApiError(
        StatusCode::NOT_FOUND,
        "Learned-rule administration is not enabled on this server",
    ))
}

fn rule_view(rule: &kg_core::traits::rule_store::LearnedRule) -> serde_json::Value {
    json!({
        "id": rule.id,
        "revision": rule.revision,
        "source": rule.source,
        "status": rule.status,
        "owner_slot": rule.owner_slot,
        "target_type": rule.mapping.target_type,
        "relationship_name": rule.mapping.relationship_name,
        "schema_fingerprint": rule.schema_fingerprint,
        "origin": rule.origin,
        "evidence_refs": rule.evidence_refs,
        "validation": rule.validation,
        "decisions": rule.decisions,
    })
}

#[derive(serde::Deserialize)]
struct RuleListParams {
    status: Option<String>,
}

async fn list_rules(
    State(state): State<AppState>,
    Path(source): Path<String>,
    axum::extract::Query(params): axum::extract::Query<RuleListParams>,
) -> Response {
    let store = match rule_store(&state) {
        Ok(store) => store,
        Err(error) => return error.into_response(),
    };
    let status = match params.status.as_deref() {
        Some(raw) => match serde_json::from_value(json!(raw)) {
            Ok(status) => Some(status),
            Err(_) => {
                return ApiError(StatusCode::BAD_REQUEST, "Unknown rule status").into_response()
            }
        },
        None => None,
    };
    match kg_core::runtime::rule_learning::admin::list(
        store.as_ref(),
        &state.org_id,
        &source,
        status,
    )
    .await
    {
        Ok(rules) => {
            let view: Vec<_> = rules.iter().map(rule_view).collect();
            Json(json!({"count": view.len(), "rules": view})).into_response()
        }
        Err(error) => rule_error(error).into_response(),
    }
}

async fn rule_detail(
    State(state): State<AppState>,
    Path((source, id)): Path<(String, Uuid)>,
) -> Response {
    let store = match rule_store(&state) {
        Ok(store) => store,
        Err(error) => return error.into_response(),
    };
    match kg_core::runtime::rule_learning::admin::detail_for_source(
        store.as_ref(),
        &state.org_id,
        &source,
        id,
    )
    .await
    {
        Ok(Some(rule)) => Json(rule_view(&rule)).into_response(),
        Ok(None) => ApiError(StatusCode::NOT_FOUND, "No such rule").into_response(),
        Err(error) => rule_error(error).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct RuleDecisionBody {
    actor: Option<String>,
    note: Option<String>,
    after_chain: Option<Uuid>,
    limit: Option<usize>,
}

fn http_decision(body: &RuleDecisionBody) -> kg_core::traits::rule_store::RuleDecision {
    kg_core::traits::rule_store::RuleDecision {
        origin: kg_core::traits::rule_store::RuleOrigin::Human {
            actor: body.actor.clone().unwrap_or_else(|| "http-admin".into()),
        },
        at: chrono::Utc::now(),
        note: body.note.clone(),
    }
}

fn rule_error(error: kg_core::errors::BackendError) -> ApiError {
    use kg_core::errors::BackendError;
    match error {
        BackendError::NotFound(_) => ApiError(StatusCode::NOT_FOUND, "No such rule"),
        BackendError::Conflict(_) => {
            ApiError(StatusCode::CONFLICT, "Rule changed since it was read")
        }
        BackendError::Query(message) if message.starts_with("activation requires") => ApiError(
            StatusCode::BAD_REQUEST,
            "Activation requires independent held-out validation",
        ),
        BackendError::Query(_) => ApiError(StatusCode::BAD_REQUEST, "Invalid rule transition"),
        _ => ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Rule store error"),
    }
}

fn pipeline_error_kind(error: &kg_core::errors::PipelineError) -> &'static str {
    use kg_core::errors::PipelineError;
    match error.root_cause() {
        PipelineError::Cancelled => "cancelled",
        PipelineError::IdentityRevisionChanged => "identity_revision_changed",
        PipelineError::StepExecution { .. } => "step_execution",
        PipelineError::StageExecution { .. } => "stage_execution",
        PipelineError::StateValidation { .. } => "state_validation",
        PipelineError::RetryExhausted { .. } => "retry_exhausted",
        PipelineError::TaskPanic(_) => "task_panic",
        PipelineError::Aborted { .. } => "aborted",
        PipelineError::Other(_) => "other",
    }
}

async fn activate_rule(
    State(state): State<AppState>,
    Path((source, id)): Path<(String, Uuid)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<RuleDecisionBody>,
) -> Response {
    if let Err(error) = authorize_write(&state, &headers) {
        return error.into_response();
    }
    let store = match rule_store(&state) {
        Ok(store) => store,
        Err(error) => return error.into_response(),
    };
    let Some(engine) = &state.rule_repair_engine else {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Rule lifecycle repair is not configured",
        )
        .into_response();
    };
    match kg_core::runtime::rule_learning::admin::activate(
        store.as_ref(),
        &state.org_id,
        &source,
        id,
        http_decision(&body),
    )
    .await
    {
        Ok(rule) => match engine
            .repair(kg_stages::RuleMaintenanceRequest {
                org_id: state.org_id.clone(),
                rule: rule.clone(),
                after_chain: body.after_chain,
                limit: body.limit.unwrap_or(100),
                cancel: None,
                trace_id: None,
            })
            .await
        {
            Ok(repair) => Json(json!({"rule": rule_view(&rule), "repair": repair})).into_response(),
            Err(error) => {
                tracing::error!(rule_id = %id, error_kind = pipeline_error_kind(&error), "rule graph repair failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "rule graph repair failed",
                        "rule": rule_view(&rule),
                        "repair_complete": false,
                        "retry_after_chain": body.after_chain,
                    })),
                )
                    .into_response()
            }
        },
        Err(error) => rule_error(error).into_response(),
    }
}

async fn reject_rule(
    State(state): State<AppState>,
    Path((source, id)): Path<(String, Uuid)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<RuleDecisionBody>,
) -> Response {
    if let Err(error) = authorize_write(&state, &headers) {
        return error.into_response();
    }
    let store = match rule_store(&state) {
        Ok(store) => store,
        Err(error) => return error.into_response(),
    };
    match kg_core::runtime::rule_learning::admin::reject(
        store.as_ref(),
        &state.org_id,
        &source,
        id,
        http_decision(&body),
    )
    .await
    {
        Ok(rule) => Json(rule_view(&rule)).into_response(),
        Err(error) => rule_error(error).into_response(),
    }
}

async fn revoke_rule(
    State(state): State<AppState>,
    Path((source, id)): Path<(String, Uuid)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<RuleDecisionBody>,
) -> Response {
    if let Err(error) = authorize_write(&state, &headers) {
        return error.into_response();
    }
    let store = match rule_store(&state) {
        Ok(store) => store,
        Err(error) => return error.into_response(),
    };
    let Some(engine) = &state.rule_repair_engine else {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Rule lifecycle repair is not configured",
        )
        .into_response();
    };
    match kg_core::runtime::rule_learning::admin::revoke(
        store.as_ref(),
        &state.org_id,
        &source,
        id,
        http_decision(&body),
    )
    .await
    {
        Ok(rule) => match engine
            .repair(kg_stages::RuleMaintenanceRequest {
                org_id: state.org_id.clone(),
                rule: rule.clone(),
                after_chain: body.after_chain,
                limit: body.limit.unwrap_or(100),
                cancel: None,
                trace_id: None,
            })
            .await
        {
            Ok(repair) => Json(json!({"rule": rule_view(&rule), "repair": repair})).into_response(),
            Err(error) => {
                tracing::error!(rule_id = %id, error_kind = pipeline_error_kind(&error), "rule graph repair failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "rule graph repair failed",
                        "rule": rule_view(&rule),
                        "repair_complete": false,
                        "retry_after_chain": body.after_chain,
                    })),
                )
                    .into_response()
            }
        },
        Err(error) => rule_error(error).into_response(),
    }
}

/// Summarize a Saga's uncovered members and return the Saga afterwards. The
/// response always names the run so an interrupted call can be replayed.
async fn summarize_saga(
    State(state): State<AppState>,
    Path(saga_uuid): Path<Uuid>,
    headers: axum::http::HeaderMap,
    Json(body): Json<SummaryBody>,
) -> Response {
    if let Err(error) = authorize_write(&state, &headers) {
        return error.into_response();
    }
    if !state.query.saga_summaries_available() {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Thread summaries are not configured",
        )
        .into_response();
    }
    let run_id = body.run_id.unwrap_or_else(Uuid::new_v4);
    match state
        .query
        .summarize_saga(
            &state.org_id,
            body.namespace,
            ThreadReference::Uuid { uuid: saga_uuid },
            run_id,
            // No HTTP-level cancellation signal; the service cancels the run
            // cooperatively when this handler future is dropped.
            CancellationToken::new(),
        )
        .await
    {
        Ok(outcome) => match serde_json::to_value(outcome) {
            Ok(value) => Json(value).into_response(),
            Err(_) => ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid summary response",
            )
            .into_response(),
        },
        Err(QueryError::Summary(error)) => {
            // Provider errors may contain source text; record only safe fields.
            tracing::warn!(%run_id, retriable = error.is_retriable(), "Thread summary failed");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "error": "Thread summary did not complete; retry with the same run_id",
                    "run_id": run_id,
                    "retriable": error.is_retriable(),
                })),
            )
                .into_response()
        }
        Err(error) => {
            let ApiError(status, message) = ApiError::from(error);
            (status, Json(json!({"error": message, "run_id": run_id}))).into_response()
        }
    }
}
async fn search_configuration(State(state): State<AppState>) -> Json<Value> {
    use kg_core::search::SearchConfig;
    Json(json!({
        "defaults": SearchConfig::hybrid_rrf(),
        "semantic_available": state.query.semantic_available(),
        "model_reranking_available": state.query.reranking_available(),
        "presets": {"keyword":SearchConfig::keyword_only(),"hybrid":SearchConfig::hybrid_rrf(),"semantic":SearchConfig::semantic_only(),"diverse":SearchConfig::hybrid_mmr(),"model":SearchConfig::hybrid_model()},
        "fulltext": "Neo4j/Lucene fulltext. BM25 tuning parameters are not exposed by this adapter."
    }))
}
async fn search(
    State(state): State<AppState>,
    Json(request): Json<SearchQuery>,
) -> Result<Response, ApiError> {
    if let Some(config) = &request.config {
        if let Err(error) = config.validate() {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":error.to_string()})),
            )
                .into_response());
        }
    }
    let scope = QueryScope {
        namespace: request.namespace.clone(),
        as_of: request.as_of,
        limit: Some(200),
        offset: None,
    };
    let result = state.query.search(&state.org_id, request).await?;
    let chains: std::collections::BTreeSet<_> = result
        .relationships
        .iter()
        .flat_map(|r| [r.source_chain_id, r.target_chain_id])
        .collect();
    let endpoints = if chains.is_empty() {
        Vec::new()
    } else {
        state
            .query
            .explore(
                &state.org_id,
                scope,
                ExplorerQuery::EntitiesByChains {
                    chains: chains.into_iter().collect(),
                },
            )
            .await?
            .items
    };
    let mut response = serde_json::to_value(result)
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Invalid search response"))?;
    response["relationship_entities"] = json!(endpoints);
    Ok(Json(response).into_response())
}

#[cfg(test)]
pub(crate) mod test_support {
    use chrono::{DateTime, Utc};
    use kg_core::{errors::BackendError, models::ThreadNode, saga::*};
    use uuid::Uuid;

    pub fn day(n: u32) -> DateTime<Utc> {
        format!("2026-01-{n:02}T00:00:00Z").parse().unwrap()
    }
    /// One summarized "incident" Saga created on day 1 whose summary covers
    /// observations captured through day 10.
    pub fn incident(org: &str) -> ThreadNode {
        ThreadNode {
            summary_incomplete_reason: None,
            summary_incomplete_from_ordinal: None,
            summary_supporting_snapshot_uuids: vec![Uuid::from_u128(7)],
            revision: 3,
            last_membership_ordinal: 3,
            summary_revision: Some(Uuid::from_u128(9)),
            summary_cursor: 2,
            uuid: saga_uuid(org, "prod", "incident"),
            org_id: org.into(),
            namespace: "prod".into(),
            name: "incident".into(),
            labels: vec![],
            created_at: day(1),
            summary: "Payments failed over to the standby region.".into(),
            first_snapshot_uuid: Some(Uuid::from_u128(1)),
            last_snapshot_uuid: Some(Uuid::from_u128(3)),
            last_summarized_at: Some(day(20)),
            last_summarized_snapshot_captured_at: Some(day(10)),
            first_captured_at: Some(day(1)),
        }
    }
    /// One snapshot hit for any Saga-scoped keyword search; panics if the Saga filter is missing.
    pub fn saga_snapshot_search(
        request: &kg_core::search::EvidenceSearch,
    ) -> Result<kg_core::search::SearchPage<kg_core::search::SnapshotHit>, BackendError> {
        request.validate()?;
        assert!(
            request.filter.saga_uuid.is_some(),
            "saga filter must reach storage"
        );
        Ok(kg_core::search::SearchPage::bounded(
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
    /// Answers every summary request with one replayed manifest and one new page.
    pub struct FakeSummarizer;
    #[async_trait::async_trait]
    impl crate::query::SagaSummarizer for FakeSummarizer {
        async fn summarize(
            &self,
            _: &str,
            _: &str,
            _: Uuid,
            run_id: Uuid,
            _: tokio_util::sync::CancellationToken,
        ) -> Result<kg_core::pipeline::PipelineOutput, kg_core::errors::PipelineError> {
            let counts = kg_core::pipeline::CommittedCounts {
                saga_memberships_summarized: 3,
                saga_summaries_updated: 1,
                ..Default::default()
            };
            let batch = |index, replayed| kg_core::pipeline::BatchOutcome {
                kind: kg_core::traits::BatchKind::SagaSummary,
                index,
                replayed,
                counts: if replayed { Default::default() } else { counts },
            };
            Ok(kg_core::pipeline::PipelineOutput {
                run_id,
                committed: counts,
                newly_committed: counts,
                batches: vec![batch(0, true), batch(1, false)],
                ..Default::default()
            })
        }
    }
    /// Canned Saga storage: the incident Saga in `prod` with members captured on days 4, 8 and 12.
    pub fn saga_read(org: &str, request: &SagaRead) -> Result<SagaReadResult, BackendError> {
        request.validate(org)?;
        let node = incident(org);
        let in_scope = request.namespace() == "prod";
        Ok(match request {
            SagaRead::State { reference, .. } => {
                let wanted = match reference {
                    ThreadReference::Name { name } => saga_uuid(org, "prod", name),
                    ThreadReference::Uuid { uuid } => *uuid,
                };
                SagaReadResult::State((in_scope && wanted == node.uuid).then_some(node))
            }
            SagaRead::List { offset, as_of, .. } => SagaReadResult::Sagas(SagaPage {
                // Storage filters visibility before paging, exactly like the Cypher.
                sagas: if in_scope && *offset == 0 && as_of.is_none_or(|at| day(1) <= at) {
                    vec![node]
                } else {
                    vec![]
                },
                truncated: false,
            }),
            SagaRead::Members {
                saga_uuid,
                after_ordinal,
                captured_through,
                limit,
                ..
            } => {
                let mut members: Vec<_> = (1..=3u64)
                    .filter(|_| in_scope && *saga_uuid == node.uuid)
                    .filter(|ordinal| ordinal > after_ordinal)
                    .map(|ordinal| SagaMember {
                        snapshot_uuid: Uuid::from_u128(ordinal.into()),
                        captured_at: day(ordinal as u32 * 4),
                        created_at: day(20),
                        ordinal,
                        previous_snapshot_uuid: (ordinal > 1)
                            .then(|| Uuid::from_u128((ordinal - 1).into())),
                        snapshot_name: None,
                        snapshot_source: None,
                    })
                    .filter(|m| captured_through.is_none_or(|t| m.captured_at <= t))
                    .collect();
                let truncated = members.len() > *limit;
                members.truncate(*limit);
                SagaReadResult::Members(SagaMemberPage { members, truncated })
            }
            _ => return Err(BackendError::NotConfigured("unused Thread read".into())),
        })
    }
}

#[cfg(test)]
mod saga_routes {
    use super::*;
    use crate::test_support::{day, saga_read};
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use kg_core::{
        saga::{saga_uuid, SagaRead, SagaReadResult},
        search::SearchPage,
        traits::{
            graph_explorer::{ExplorerRequest, GraphExplorerBackend},
            SearchBackend,
        },
    };
    use tower::ServiceExt;

    struct SagaGraph;
    #[async_trait::async_trait]
    impl GraphExplorerBackend for SagaGraph {
        async fn explore(&self, _: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
            unreachable!("saga routes never browse entities")
        }
        async fn read_saga(
            &self,
            org: &str,
            request: &SagaRead,
        ) -> Result<SagaReadResult, BackendError> {
            saga_read(org, request)
        }
    }
    struct SnapshotSearch;
    #[async_trait::async_trait]
    impl SearchBackend for SnapshotSearch {
        async fn search_snapshots(
            &self,
            request: &kg_core::search::EvidenceSearch,
        ) -> Result<SearchPage<kg_core::search::SnapshotHit>, BackendError> {
            crate::test_support::saga_snapshot_search(request)
        }
    }

    struct FakeRuleRepair;
    #[async_trait::async_trait]
    impl crate::rules::RuleRepairService for FakeRuleRepair {
        async fn repair(
            &self,
            request: kg_stages::RuleMaintenanceRequest,
        ) -> Result<kg_stages::RuleMaintenanceOutput, kg_core::errors::PipelineError> {
            Ok(kg_stages::RuleMaintenanceOutput {
                rule_id: request.rule.id,
                rule_revision: request.rule.revision,
                sources_processed: 0,
                next_after: None,
                complete: true,
                run_ids: vec![],
                committed: Default::default(),
            })
        }
    }

    fn query(summaries: bool) -> Arc<GraphQueryService> {
        let mut service = GraphQueryService::new(
            Arc::new(SagaGraph),
            Arc::new(kg_search::SearchEngine::new(Arc::new(SnapshotSearch))),
            false,
        );
        if summaries {
            service = service.with_saga_summarizer(Arc::new(crate::test_support::FakeSummarizer));
        }
        Arc::new(service)
    }
    fn app() -> Router {
        router(AppState {
            query: query(false),
            org_id: "org-a".into(),
            write_token: None,
            rules: None,
            rule_repair_engine: None,
        })
    }
    async fn send(app: Router, request: Request<Body>) -> (StatusCode, Value) {
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    async fn get(path: &str) -> (StatusCode, Value) {
        send(
            app(),
            Request::builder().uri(path).body(Body::empty()).unwrap(),
        )
        .await
    }

    fn active_rule(
        id: u128,
        status: kg_core::traits::rule_store::RuleStatus,
    ) -> kg_core::traits::rule_store::LearnedRule {
        use kg_core::traits::rule_store::*;
        let gate = RuleValidation {
            positives: MIN_PROMOTION_POSITIVES,
            negatives: MIN_PROMOTION_NEGATIVES,
            precision: MIN_PROMOTION_PRECISION,
            recall: MIN_PROMOTION_RECALL,
            conflicting_failures: 0,
            independent: true,
        };
        LearnedRule {
            id: Uuid::from_u128(id),
            revision: if status == RuleStatus::Active { 2 } else { 1 },
            org_id: "org-a".into(),
            source: "cmdb".into(),
            namespace: None,
            schema_fingerprint: "fp".into(),
            mapping: kg_core::runtime::extraction::ReferenceMapping {
                source_namespace: None,
                source_entity_type: "CmdbChange".into(),
                reference_path: "owning_group".into(),
                context_paths: Default::default(),
                target_type: "CmdbGroup".into(),
                target_key_group: vec!["group_id".into()],
                shape: Default::default(),
                direction: Default::default(),
                relationship_name: "REFERENCES_CMDBGROUP".into(),
                qualifiers: None,
                cardinality: Default::default(),
                case_insensitive_types: Vec::new(),
            },
            owner_slot: "CmdbChange.owning_group".into(),
            origin: RuleOrigin::System {
                component: "rule-learning".into(),
            },
            evidence_refs: vec!["positives=20".into()],
            validation: if status == RuleStatus::Active {
                Some(gate)
            } else {
                None
            },
            decisions: vec![],
            status,
            effective_from: None,
            revoked_at: None,
        }
    }

    fn rules_app(token: Option<&str>) -> Router {
        use kg_core::test_support::InMemoryRuleStore;
        use kg_core::traits::rule_store::RuleStatus;
        let store = InMemoryRuleStore::new();
        store.seed(active_rule(1, RuleStatus::Active));
        store.seed(active_rule(2, RuleStatus::Proposed));
        router(AppState {
            query: query(false),
            org_id: "org-a".into(),
            write_token: token.map(|t| t.into()),
            rules: Some(Arc::new(store)),
            rule_repair_engine: Some(Arc::new(FakeRuleRepair)),
        })
    }

    #[tokio::test]
    async fn rule_admin_lists_filters_and_writes_under_the_token() {
        // Disabled when no rule store is configured.
        let (status, _) = get("/api/v1/rules/cmdb").await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // List all, then filter by status.
        let (status, body) = send(
            rules_app(None),
            Request::builder()
                .uri("/api/v1/rules/cmdb")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["count"], 2);
        let (_, body) = send(
            rules_app(None),
            Request::builder()
                .uri("/api/v1/rules/cmdb?status=active")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(body["count"], 1);
        assert_eq!(body["rules"][0]["status"], "active");

        // Detail resolves by id.
        let id = Uuid::from_u128(1);
        let (status, body) = send(
            rules_app(None),
            Request::builder()
                .uri(format!("/api/v1/rules/cmdb/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["id"], id.to_string());

        // A write without the token is rejected.
        let revoke = |token: Option<&str>| {
            let mut req = Request::builder()
                .method("POST")
                .uri(format!("/api/v1/rules/cmdb/{id}/revoke"))
                .header("content-type", "application/json");
            if let Some(t) = token {
                req = req.header("authorization", format!("Bearer {t}"));
            }
            req.body(Body::from(
                json!({"actor":"sre","note":"deprecating"}).to_string(),
            ))
            .unwrap()
        };
        let secret = "x".repeat(MIN_WRITE_TOKEN_LEN);
        let (status, _) = send(rules_app(Some(&secret)), revoke(None)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // With the token, the active rule is revoked.
        let (status, body) = send(rules_app(Some(&secret)), revoke(Some(&secret))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rule"]["status"], "revoked");
        assert_eq!(body["repair"]["complete"], true);
    }

    #[tokio::test]
    async fn saga_scoped_search_returns_member_snapshots_or_a_clear_error() {
        let post = |body: Value| {
            Request::builder()
                .method("POST")
                .uri("/api/v1/search")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let (status, body) = send(
            app(),
            post(json!({"query":"checkout","namespace":"prod","thread":{"kind":"name","name":"incident"}})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["hits"], json!([]));
        assert_eq!(body["snapshots"][0]["content"], "checkout failed over");
        let (status, _) = send(
            app(),
            post(json!({"query":"checkout","thread":{"kind":"name","name":"incident"}})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = send(
            app(),
            post(
                json!({"query":"checkout","namespace":"prod","thread":{"kind":"name","name":"nope"}}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(
            app(),
            post(json!({"query":"checkout","namespace":"prod","semantic":true,"thread":{"kind":"name","name":"incident"}})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn summaries_need_a_write_token_and_a_configured_engine() {
        let token = "t".repeat(MIN_WRITE_TOKEN_LEN);
        let uuid = saga_uuid("org-a", "prod", "incident");
        let post = |bearer: Option<&str>, body: Value, path: String| {
            let mut request = Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json");
            if let Some(bearer) = bearer {
                request = request.header("authorization", format!("Bearer {bearer}"));
            }
            request.body(Body::from(body.to_string())).unwrap()
        };
        let path = format!("/api/v1/threads/{uuid}/summary");
        let body = json!({"namespace":"prod"});

        // Writes are disabled entirely without a configured token.
        let (status, _) = send(app(), post(Some(&token), body.clone(), path.clone())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let armed = |summaries: bool| {
            router(AppState {
                query: query(summaries),
                org_id: "org-a".into(),
                write_token: Some(token.clone().into()),
                rules: None,
                rule_repair_engine: None,
            })
        };
        let (status, error) = send(armed(true), post(None, body.clone(), path.clone())).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(error["error"], "Write token required");
        let (status, _) = send(
            armed(true),
            post(Some(&"x".repeat(32)), body.clone(), path.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = send(armed(false), post(Some(&token), body.clone(), path.clone())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let run = Uuid::from_u128(77);
        let (status, outcome) = send(
            armed(true),
            post(
                Some(&token),
                json!({"namespace":"prod","run_id":run}),
                path.clone(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{outcome}");
        assert_eq!(outcome["run_id"], json!(run));
        assert_eq!(outcome["memberships_summarized"], 3);
        assert_eq!(outcome["thread"]["name"], "incident");

        let (status, missing) = send(
            armed(true),
            post(
                Some(&token),
                json!({"namespace":"prod","run_id":run}),
                format!("/api/v1/threads/{}/summary", Uuid::from_u128(5)),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["run_id"], json!(run));
        let (status, _) = send(
            armed(true),
            post(Some(&token), json!({"namespace":"prod","extra":1}), path),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn saga_routes_require_a_namespace_and_page_members_by_ordinal() {
        // Old bookmarks and new Thread routes must reach the same scoped data.
        let legacy = get("/api/v1/sagas?namespace=prod&limit=10").await;
        let current = get("/api/v1/threads?namespace=prod&limit=10").await;
        assert_eq!(legacy, current);

        let (status, body) = get("/api/v1/threads").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "Thread reads require a namespace");

        let (status, body) = get("/api/v1/threads?namespace=prod&limit=10").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"][0]["name"], "incident");
        assert_eq!(body["items"][0]["total_members"], 3);
        assert_eq!(body["truncated"], false);
        assert_eq!(body["next_offset"], Value::Null);

        let (status, body) = get("/api/v1/threads?namespace=staging").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"].as_array().unwrap().len(), 0);

        let uuid = saga_uuid("org-a", "prod", "incident");
        let (status, body) = get(&format!(
            "/api/v1/threads/{uuid}/members?namespace=prod&limit=2"
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"].as_array().unwrap().len(), 2);
        assert_eq!(body["truncated"], true);
        assert_eq!(body["next_after_ordinal"], 2);
        let (status, body) = get(&format!(
            "/api/v1/threads/{uuid}/members?namespace=prod&after_ordinal=2"
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"][0]["ordinal"], 3);
        assert_eq!(body["truncated"], false);

        let (status, _) = get(&format!(
            "/api/v1/threads/{}/members?namespace=prod",
            Uuid::from_u128(5)
        ))
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = get("/api/v1/threads?namespace=prod&bogus=1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn historical_saga_reads_withhold_later_summaries_and_members() {
        let (status, body) = get("/api/v1/threads/by-name/incident?namespace=prod").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["summary"],
            "Payments failed over to the standby region."
        );
        assert_eq!(body["summary_withheld"], Value::Null);
        assert_eq!(body["summary_covers_captured_through"], json!(day(10)));

        let (status, body) =
            get("/api/v1/threads/by-name/incident?namespace=prod&as_of=2026-01-05T00:00:00Z").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["summary"], Value::Null);
        assert_eq!(body["summary_withheld"], "covers_later_observations");
        assert_eq!(body["summary_supporting_snapshot_uuids"], json!([]));
        assert_eq!(body["summary_covers_captured_through"], json!(day(10)));

        let (status, body) =
            get("/api/v1/threads/by-name/incident?namespace=prod&as_of=2026-01-10T00:00:00Z").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["summary"].is_string());

        let uuid = saga_uuid("org-a", "prod", "incident");
        let (status, body) = get(&format!("/api/v1/threads/{uuid}?namespace=prod")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["uuid"], json!(uuid));
        let (status, body) = get(&format!(
            "/api/v1/threads/{uuid}/members?namespace=prod&as_of=2026-01-09T00:00:00Z"
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["ordinal"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [1, 2]
        );

        // The Saga's earliest capture is day 1; before that it does not exist on
        // the observation timeline.
        let (status, _) =
            get("/api/v1/threads/by-name/incident?namespace=prod&as_of=2025-12-31T00:00:00Z").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = get("/api/v1/threads?namespace=prod&as_of=2025-12-31T00:00:00Z").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"].as_array().unwrap().len(), 0);
        let (status, body) = get("/api/v1/threads?namespace=prod&as_of=2026-01-01T00:00:00Z").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"][0]["name"], "incident");
        assert_eq!(body["items"][0]["earliest_captured_at"], json!(day(1)));
        let (status, _) = get("/api/v1/threads/by-name/unknown?namespace=prod").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}

async fn capabilities(State(state): State<AppState>) -> Json<Value> {
    Json(state.query.capabilities())
}
async fn graph_schema(State(state): State<AppState>) -> Json<Value> {
    Json(state.query.graph_schema())
}
async fn graph_changes(
    State(state): State<AppState>,
    Json(request): Json<investigation::ChangesRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.query.changes(&state.org_id, request).await?))
}
async fn graph_paths(
    State(state): State<AppState>,
    Json(request): Json<investigation::PathsRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.query.paths(&state.org_id, request).await?))
}
