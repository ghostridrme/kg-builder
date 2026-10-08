//! The public ingestion entry point: one awaited call that validates the
//! request, runs the supported stage composition, commits every batch, and
//! returns the committed result. CLI, API, cron, and connector callers use
//! this and never assemble stages themselves.

use std::collections::HashMap;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use kg_core::entity_type_config::EntityTypeConfig;
use kg_core::errors::{ConfigError, PipelineError};
use kg_core::models::SnapshotInput;
use kg_core::pipeline::{CommittedCounts, PipelineOutput};
use kg_core::policy::{
    EntityMatching, PipelinePolicy, PolicyOverride, PolicyResolver, TenantPipelinePolicy,
};
use kg_core::runtime::execution::ExecutionConfig;
use kg_core::runtime::RuntimeContextBuilder;
use kg_core::tenant::NamespacePolicy;
use kg_core::traits::{
    EmbedBackend, EntityLookup, GraphBackend, LlmBackend, OntologyStore, SchemaStore, Stage,
};
use kg_pipeline::runner::{PipelineRunner, PipelineRunnerConfig};

use crate::composition::ingestion_pipeline_with_recipe;

/// Explicit full-build request; ingestion uses the configured incremental path.
pub struct CommunityMaintenanceRequest {
    /// Organization owning the graph.
    pub org_id: String,
    /// Required namespace; a build never spans namespaces.
    pub namespace: String,
    /// Stable identity for receipt recovery.
    pub run_id: Option<Uuid>,
    /// Cooperative cancellation of unfinished work.
    pub cancel: Option<CancellationToken>,
    /// Optional trace correlation supplied by the caller.
    pub trace_id: Option<String>,
}
impl CommunityMaintenanceRequest {
    /// A namespace build without a supplied identity or cancellation token.
    pub fn new(org_id: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            org_id: org_id.into(),
            namespace: namespace.into(),
            run_id: None,
            cancel: None,
            trace_id: None,
        }
    }
    /// Resume the same immutable maintenance request from its durable receipts.
    pub fn with_run_id(mut self, run_id: Uuid) -> Self {
        self.run_id = Some(run_id);
        self
    }
}

/// Summarize one Saga's members that no committed summary covers yet.
pub struct ThreadMaintenanceRequest {
    /// Organization owning the graph.
    pub org_id: String,
    /// Required namespace; a Saga name is unique only inside it.
    pub namespace: String,
    /// The Saga, by stable UUID or by name resolved inside the namespace.
    pub saga: kg_core::saga::ThreadReference,
    /// Stable identity for receipt recovery; the same id replays committed pages.
    pub run_id: Option<Uuid>,
    /// Cooperative cancellation of unfinished work.
    pub cancel: Option<CancellationToken>,
    /// Optional trace correlation supplied by the caller.
    pub trace_id: Option<String>,
}
impl ThreadMaintenanceRequest {
    /// A summary request without a supplied identity or cancellation token.
    pub fn new(
        org_id: impl Into<String>,
        namespace: impl Into<String>,
        saga: kg_core::saga::ThreadReference,
    ) -> Self {
        Self {
            org_id: org_id.into(),
            namespace: namespace.into(),
            saga,
            run_id: None,
            cancel: None,
            trace_id: None,
        }
    }
    /// Resume the same immutable request from its durable receipts.
    pub fn with_run_id(mut self, run_id: Uuid) -> Self {
        self.run_id = Some(run_id);
        self
    }
    /// Stop between pages when this token is cancelled; committed pages stay
    /// replayable by run id.
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }
}

/// One bounded page of sources to re-evaluate after a learned rule changes.
pub struct RuleMaintenanceRequest {
    /// Organization that owns the rule and affected graph data.
    pub org_id: String,
    /// The committed active or revoked rule revision being applied.
    pub rule: kg_core::traits::LearnedRule,
    /// Exclusive source-chain cursor from the previous page.
    pub after_chain: Option<Uuid>,
    /// Maximum source owners to process in this call.
    pub limit: usize,
    /// Cooperative cancellation between reads and source runs.
    pub cancel: Option<CancellationToken>,
    /// Optional caller trace correlation.
    pub trace_id: Option<String>,
}

/// Awaited graph effects of one rule-lifecycle page.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuleMaintenanceOutput {
    /// Rule whose graph effects were reconciled.
    pub rule_id: Uuid,
    /// Exact rule revision used by every source run.
    pub rule_revision: u64,
    /// Source owners completed in this page.
    pub sources_processed: usize,
    /// Cursor for the next page; absent when the scan is complete.
    pub next_after: Option<Uuid>,
    /// Whether every matching live source was covered.
    pub complete: bool,
    /// Deterministic per-source receipt identities.
    pub run_ids: Vec<Uuid>,
    /// Acknowledged graph changes across the page.
    pub committed: CommittedCounts,
}

/// Providers the engine calls. One LLM may serve every slot; unset
/// disambiguation and edge-discovery slots use `llm_default`.
pub struct Backends {
    /// The graph every batch commits to.
    pub graph: Arc<dyn GraphBackend>,
    /// Text extraction model.
    pub llm_extraction: Arc<dyn LlmBackend>,
    /// Fuzzy-match verdict model; `llm_default` when None.
    pub llm_disambiguation: Option<Arc<dyn LlmBackend>>,
    /// Relationship discovery model; `llm_default` when None.
    pub llm_edge_discovery: Option<Arc<dyn LlmBackend>>,
    /// Fallback model for unset slots.
    pub llm_default: Arc<dyn LlmBackend>,
    /// Typed-decision provider; used only when `typed_decisions.enabled`.
    pub decisions: Option<Arc<dyn kg_core::traits::DecisionBackend>>,
    /// The shared embedding provider every entity vector is computed with.
    pub embedder: Arc<dyn EmbedBackend>,
    /// Source-specific ontology guidance; none when None.
    pub ontology_store: Option<Arc<dyn OntologyStore>>,
    /// Adopt-once identity schemas; entities without declared keys are
    /// rejected when None.
    pub schema_store: Option<Arc<dyn SchemaStore>>,
}

/// Processing settings shared by every request of an engine. Every value is
/// validated at construction; nothing here is silently ignored.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestionSettings {
    /// Explicit full builds and optional awaited incremental Community maintenance.
    pub community: kg_core::runtime::community::CommunitySettings,
    /// Awaited summaries of explicitly associated Saga observations.
    #[serde(alias = "thread_summary")]
    pub saga_summary: kg_core::runtime::saga::SagaSummarySettings,
    /// Optional bounded entity summaries after all ingestion commits.
    pub entity_summary: kg_core::runtime::entity_summary::EntitySummarySettings,
    /// Ordered descriptive entity fields shared with matching and rebuild.
    pub entity_embedding_fields: kg_core::embedding::EntityEmbeddingFields,
    /// Which relationship stages run; model permissions remain source-policy controlled.
    pub recipe: crate::composition::PipelineRecipe,
    /// Bounded contextual identity matching.
    pub matching: kg_core::runtime::matching::MatchingSettings,
    /// Typed decisions before the language-model path; off by default.
    pub typed_decisions: kg_core::runtime::decisions::TypedDecisionSettings,
    /// Bounded evidence-backed decisions on ambiguous references; whether the
    /// model is consulted at all remains the `edge_ambiguity` policy.
    pub reference_resolution: kg_core::runtime::reference_resolution::ReferenceResolutionSettings,
    /// Bounded text discovery, attribute enrichment and optional timestamp inference.
    pub extraction: kg_core::runtime::extraction::ExtractionSettings,
    /// Source-content persistence and bounded previous-observation retrieval.
    pub context: kg_core::runtime::history::ContextSettings,
    /// Snapshots per processing chunk; one node and one relationship batch
    /// commit per chunk.
    pub chunk_size: usize,
    /// Bounded channel capacity between stages.
    pub channel_capacity: usize,
    /// Snapshots one stage processes concurrently.
    pub stage_concurrency: usize,
    /// Statement budget of one reconciliation batch.
    pub reconciliation_batch_statements: usize,
    /// General concurrency and the default size of every provider pool.
    pub max_concurrency: usize,
    /// Embedding call pool; `max_concurrency` when None.
    pub embed_concurrency: Option<usize>,
    /// Extraction, disambiguation, and edge-discovery LLM pools.
    pub llm_slot_concurrency: Option<(usize, usize, usize)>,
    /// Heuristic-versus-LLM policy for every request.
    pub policy: PipelinePolicy,
    /// Caller-selected overrides for each input source.
    pub source_policies: HashMap<String, PolicyOverride>,
    /// Normalization and versioning rules by entity type.
    pub entity_type_configs: HashMap<String, EntityTypeConfig>,
    /// Cross-namespace relationship policy; open when None.
    pub namespace_policy: Option<NamespacePolicy>,
    /// Record failing snapshots and continue instead of aborting; the run is
    /// then incomplete and never reconciles.
    pub continue_on_step_error: bool,
    /// Maximum pages in one automatic learned-rule repair; manual repair is resumable.
    pub rule_repair_max_pages: usize,
}

impl Default for IngestionSettings {
    fn default() -> Self {
        let runner = PipelineRunnerConfig::default();
        Self {
            community: Default::default(),
            entity_summary: Default::default(),
            saga_summary: Default::default(),
            entity_embedding_fields: Default::default(),
            recipe: Default::default(),
            matching: Default::default(),
            typed_decisions: Default::default(),
            reference_resolution: Default::default(),
            extraction: Default::default(),
            context: Default::default(),
            chunk_size: runner.chunk_size,
            channel_capacity: runner.channel_capacity,
            stage_concurrency: runner.stage_concurrency,
            reconciliation_batch_statements: runner.reconciliation_batch_statements,
            max_concurrency: 20,
            embed_concurrency: None,
            llm_slot_concurrency: None,
            policy: PipelinePolicy {
                extraction: kg_core::policy::ExtractionMode::Heuristic,
                edge_discovery: kg_core::policy::EdgeDiscoveryMode::Heuristic,
                ..Default::default()
            },
            source_policies: HashMap::new(),
            entity_type_configs: HashMap::new(),
            namespace_policy: None,
            continue_on_step_error: false,
            rule_repair_max_pages: 10_000,
        }
    }
}

impl IngestionSettings {
    /// Validate all processing bounds without constructing providers or opening a database.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |field: &str, message: String| ConfigError::InvalidValue {
            field: field.into(),
            message,
        };
        use kg_core::policy::{EdgeAmbiguityMode, EdgeDiscoveryMode, ExtractionMode, SchemaMode};
        for p in std::iter::once(self.policy)
            .chain(self.source_policies.values().map(|p| p.apply(self.policy)))
        {
            if p.extraction != ExtractionMode::Heuristic
                || p.matching != EntityMatching::Exact
                || p.edge_discovery != EdgeDiscoveryMode::Heuristic
                || p.edge_ambiguity != EdgeAmbiguityMode::Skip
                || p.schema != SchemaMode::Declared
            {
                return Err(invalid("policy", "kg-builder requires heuristic extraction/discovery, exact matching, declared keys and skip ambiguity".into()));
            }
        }
        if self.recipe == crate::composition::PipelineRecipe::Full
            || self.typed_decisions.enabled
            || self.extraction.relationship_naming.enabled
            || self.entity_summary.enabled
            || self.saga_summary.enabled
            || self.community.incremental_enabled
            || self.extraction.relationship_timestamps.enabled
            || self.extraction.relationship_contradictions.enabled
            || self.extraction.source_guidance.values().any(|g| {
                g.relationship_timestamps
                    .as_ref()
                    .is_some_and(|s| s.enabled)
                    || g.relationship_contradictions
                        .as_ref()
                        .is_some_and(|s| s.enabled)
            })
        {
            return Err(invalid(
                "processing",
                "model and derived-summary operations are unavailable in kg-builder".into(),
            ));
        }
        if !(1..=100_000).contains(&self.rule_repair_max_pages) {
            return Err(invalid(
                "rule_repair_max_pages",
                "must be between 1 and 100000".into(),
            ));
        }
        self.runner_config()
            .validate()
            .map_err(|e| invalid("runner", e.to_string()))?;
        for (field, result) in [
            ("community", self.community.validate()),
            ("saga_summary", self.saga_summary.validate()),
            ("entity_summary", self.entity_summary.validate()),
            (
                "matching",
                self.matching.validate().map_err(|e| e.to_string()),
            ),
            (
                "typed_decisions",
                self.typed_decisions.validate().map_err(|e| e.to_string()),
            ),
            (
                "reference_resolution",
                self.reference_resolution
                    .validate()
                    .map_err(|e| e.to_string()),
            ),
            (
                "extraction",
                self.extraction.validate().map_err(|e| e.to_string()),
            ),
            (
                "context",
                self.context.validate().map_err(|e| e.to_string()),
            ),
            (
                "entity_embedding_fields",
                self.entity_embedding_fields
                    .validate()
                    .map_err(|e| e.to_string()),
            ),
        ] {
            result.map_err(|message| invalid(field, message))?;
        }
        let (extraction, disambiguation, edge) = self.llm_slot_concurrency.unwrap_or((
            self.max_concurrency,
            self.max_concurrency,
            self.max_concurrency,
        ));
        for (field, value) in [
            ("max_concurrency", self.max_concurrency),
            (
                "embed_concurrency",
                self.embed_concurrency.unwrap_or(self.max_concurrency),
            ),
            ("llm_extraction_concurrency", extraction),
            ("llm_disambiguation_concurrency", disambiguation),
            ("llm_edge_concurrency", edge),
        ] {
            if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&value) {
                return Err(invalid(field, "invalid semaphore capacity".into()));
            }
        }
        if self
            .source_policies
            .keys()
            .any(|source| source.trim().is_empty() || source.chars().any(char::is_control))
        {
            return Err(invalid(
                "source_policies",
                "source names must be nonblank and contain no control characters".into(),
            ));
        }
        if let Some(policy) = &self.namespace_policy {
            policy
                .validate()
                .map_err(|e| invalid("namespace_policy", e))?;
        }
        Ok(())
    }

    /// Validate model permissions before application factories perform any network work.
    pub fn validate_models(
        &self,
        extraction: bool,
        disambiguation: bool,
        edge: bool,
        default: bool,
    ) -> Result<(), ConfigError> {
        use kg_core::policy::{EdgeAmbiguityMode, EdgeDiscoveryMode, ExtractionMode};
        let invalid = |field: &str| ConfigError::InvalidValue {
            field: field.into(),
            message: "requires a configured language model".into(),
        };
        for policy in std::iter::once(self.policy)
            .chain(self.source_policies.values().map(|p| p.apply(self.policy)))
        {
            if policy.extraction == ExtractionMode::Llm && !extraction {
                return Err(invalid("policy.extraction"));
            }
            if policy.matching == EntityMatching::Semantic && !disambiguation {
                return Err(invalid("policy.matching"));
            }
            if self.recipe == crate::composition::PipelineRecipe::Full
                && matches!(
                    policy.edge_discovery,
                    EdgeDiscoveryMode::Llm | EdgeDiscoveryMode::HeuristicThenLlm
                )
                && !edge
            {
                return Err(invalid("policy.edge_discovery"));
            }
            if self.recipe != crate::composition::PipelineRecipe::EntityOnly
                && policy.edge_ambiguity == EdgeAmbiguityMode::Llm
                && !disambiguation
            {
                return Err(invalid("policy.edge_ambiguity"));
            }
        }
        // Optional relationship naming uses the edge-discovery model slot; when
        // enabled with any relationship-bearing recipe it must have a configured
        // model, so enabling it never silently falls back to no naming or to an
        // accidental paid default.
        if self.extraction.relationship_naming.enabled
            && self.recipe != crate::composition::PipelineRecipe::EntityOnly
            && !edge
        {
            return Err(invalid("extraction.relationship_naming"));
        }
        use kg_core::runtime::entity_summary::SummaryMode;
        for (field, required) in [
            (
                "entity_summary.mode",
                self.entity_summary.enabled && self.entity_summary.mode == SummaryMode::Model,
            ),
            (
                "saga_summary.mode",
                self.saga_summary.enabled && self.saga_summary.mode == SummaryMode::Model,
            ),
            ("community.mode", self.community.mode == SummaryMode::Model),
        ] {
            if required && !default {
                return Err(invalid(field));
            }
        }
        Ok(())
    }

    /// Whether any source may request adopt-once identity inference.
    pub fn requires_schema_store(&self) -> bool {
        std::iter::once(self.policy)
            .chain(self.source_policies.values().map(|p| p.apply(self.policy)))
            .any(|p| p.schema == kg_core::policy::SchemaMode::InferOnce)
    }

    fn runner_config(&self) -> PipelineRunnerConfig {
        PipelineRunnerConfig {
            chunk_size: self.chunk_size,
            channel_capacity: self.channel_capacity,
            stage_concurrency: self.stage_concurrency,
            reconciliation_batch_statements: self.reconciliation_batch_statements,
        }
    }
}

/// One ingestion request: the organization it belongs to, its snapshots,
/// and an optional idempotency key.
pub struct IngestionRequest {
    /// Organization scope; every snapshot and entity must agree with it.
    pub org_id: String,
    /// Idempotency key. Retrying with the same key and input replays committed
    /// batches and finishes the rest; a fresh key is used when None.
    pub run_id: Option<Uuid>,
    /// The snapshots to ingest; an empty request registers its identity without data batches.
    pub snapshots: Vec<kg_core::models::IngestionInput>,
    /// Cooperative cancellation; completed writes are never rolled back.
    pub cancel: Option<CancellationToken>,
    /// Log correlation id; a fresh UUID when None.
    pub trace_id: Option<String>,
}

impl IngestionRequest {
    /// A request without a caller-supplied run id, cancellation, or trace id.
    pub fn new(org_id: impl Into<String>, snapshots: Vec<SnapshotInput>) -> Self {
        Self {
            org_id: org_id.into(),
            run_id: None,
            snapshots: snapshots.into_iter().map(Into::into).collect(),
            cancel: None,
            trace_id: None,
        }
    }

    /// Mix fresh ingestion and immutable snapshot reuse in caller order.
    pub fn from_inputs(
        org_id: impl Into<String>,
        snapshots: Vec<kg_core::models::IngestionInput>,
    ) -> Self {
        Self {
            org_id: org_id.into(),
            run_id: None,
            snapshots,
            cancel: None,
            trace_id: None,
        }
    }

    /// Set the idempotency key.
    pub fn with_run_id(mut self, run_id: Uuid) -> Self {
        self.run_id = Some(run_id);
        self
    }
}

/// The supported stage composition behind one awaited call. Build one engine
/// per set of providers and settings and share it across requests; each
/// request gets its own scoped runtime context.
pub struct Engine {
    profile_registry: Option<Arc<dyn kg_core::traits::ProfileRegistry>>,
    backends: Backends,
    settings: IngestionSettings,
    runner: PipelineRunner,
    pools: ProviderPools,
    matching_cache: Arc<kg_core::runtime::matching_cache::MatchingCache>,
    relationship_naming_cache:
        Arc<kg_core::runtime::relationship_naming_cache::RelationshipNamingCache>,
}

struct ProviderPools {
    general: Arc<tokio::sync::Semaphore>,
    extraction: Arc<tokio::sync::Semaphore>,
    disambiguation: Arc<tokio::sync::Semaphore>,
    edge_discovery: Arc<tokio::sync::Semaphore>,
    embedding: Arc<tokio::sync::Semaphore>,
    summary: Arc<tokio::sync::Semaphore>,
    saga_summary: Arc<tokio::sync::Semaphore>,
    community: Arc<tokio::sync::Semaphore>,
    decision: Arc<tokio::sync::Semaphore>,
}

impl Engine {
    /// Validate settings and providers. Concurrency bounds, the runner
    /// configuration, and the embedding provider's reported model and
    /// dimension are checked here, before any request runs.
    pub fn new(backends: Backends, settings: IngestionSettings) -> Result<Self, ConfigError> {
        settings.validate()?;
        if backends.embedder.is_configured()
            || backends.llm_default.is_configured()
            || backends.llm_extraction.is_configured()
            || backends
                .llm_disambiguation
                .as_ref()
                .is_some_and(|m| m.is_configured())
            || backends
                .llm_edge_discovery
                .as_ref()
                .is_some_and(|m| m.is_configured())
            || backends.decisions.is_some()
        {
            return Err(ConfigError::InvalidValue {
                field: "backends".into(),
                message: "kg-builder does not accept model or embedding providers".into(),
            });
        }
        let runner = ingestion_pipeline_with_recipe(settings.runner_config(), settings.recipe)
            .map_err(|e| ConfigError::InvalidValue {
                field: "runner".into(),
                message: e.to_string(),
            })?;
        if settings.requires_schema_store() && backends.schema_store.is_none() {
            return Err(ConfigError::InvalidValue {
                field: "policy.schema".into(),
                message: "infer_once requires an injected durable schema store".into(),
            });
        }
        settings.validate_models(
            backends.llm_extraction.is_configured(),
            backends
                .llm_disambiguation
                .as_ref()
                .unwrap_or(&backends.llm_default)
                .is_configured(),
            backends
                .llm_edge_discovery
                .as_ref()
                .unwrap_or(&backends.llm_default)
                .is_configured(),
            backends.llm_default.is_configured(),
        )?;
        let probe = Self::build_context(&backends, &settings, "probe", None, None)?;
        Ok(Self {
            profile_registry: None,
            backends,
            settings,
            runner,
            matching_cache: probe.matching_cache,
            relationship_naming_cache: probe.relationship_naming_cache,
            pools: ProviderPools {
                general: probe.semaphore,
                extraction: probe.llm_extraction_semaphore,
                disambiguation: probe.llm_disambiguation_semaphore,
                edge_discovery: probe.llm_edge_semaphore,
                embedding: probe.embed_semaphore,
                summary: probe.entity_summary_semaphore,
                saga_summary: probe.saga_summary_semaphore,
                decision: probe.decision_semaphore,
                community: probe.community_semaphore,
            },
        })
    }

    /// Resolve source contracts for connector preparation. This does not register a run or call models.
    pub async fn prepare_profiles(
        &self,
        org: &str,
        bindings: kg_core::profiles::ProfileBindings,
        cancel: Option<CancellationToken>,
    ) -> Result<kg_core::runtime::schemas::RunSchemaManifest, PipelineError> {
        let mut ctx =
            self.context(org, cancel, None)
                .map_err(|e| PipelineError::StateValidation {
                    stage: "profile_admission".into(),
                    message: e.to_string(),
                })?;
        ctx.profile_registry = self.profile_registry.clone();
        ctx.profile_bindings = bindings;
        kg_pipeline::resolve_profile_manifest(&ctx).await
    }

    /// Attach immutable profile storage without changing legacy backend constructors.
    pub fn with_profile_registry(
        mut self,
        registry: Arc<dyn kg_core::traits::ProfileRegistry>,
    ) -> Self {
        self.profile_registry = Some(registry);
        self
    }

    /// The settings this engine runs with.
    pub fn settings(&self) -> &IngestionSettings {
        &self.settings
    }

    /// Answer typed questions about caller-supplied state with the configured
    /// decision backend: the same provider, permit pool and timeout the
    /// pipeline uses, no ingestion and no run identity. Callers correlate,
    /// triage or gate their own data with it before ingesting.
    pub async fn decide(
        &self,
        state: &serde_json::Value,
        questions: &std::collections::BTreeMap<String, kg_core::traits::Question>,
    ) -> Result<kg_core::traits::Decided, kg_core::errors::BackendError> {
        use kg_core::errors::BackendError;
        let backend = match (
            &self.settings.typed_decisions.enabled,
            &self.backends.decisions,
        ) {
            (true, Some(backend)) => backend,
            _ => {
                return Err(BackendError::NotConfigured(
                    "typed decisions are not enabled for this engine".into(),
                ))
            }
        };
        kg_core::traits::decision_backend::validate_request(state, questions, &backend.limits())?;
        let _permit = kg_core::telemetry::acquire(
            &self.pools.decision,
            kg_core::telemetry::OperationKind::Decision,
        )
        .await
        .map_err(|_| BackendError::Unavailable("engine is closed".into()))?;
        let timeout = std::time::Duration::from_millis(self.settings.typed_decisions.timeout_ms);
        tokio::time::timeout(timeout, backend.decide(state, questions))
            .await
            .map_err(|_| BackendError::Timeout(timeout.as_millis() as u64))?
    }

    /// Ingest one request: validate, extract, match, resolve, embed, commit
    /// every batch, reconcile declared collections, and return the committed
    /// result. `Ok` with an incomplete output means some snapshots failed or
    /// were skipped. Inspect collection outcomes: skipped extraction suppresses
    /// its scope's absence sweep, while explicit deletions may still commit.
    /// `Err` carries the committed progress of an aborted run.
    pub async fn ingest(&self, request: IngestionRequest) -> Result<PipelineOutput, PipelineError> {
        self.ingest_with_profiles(request, Default::default()).await
    }

    /// Ingest with exact organization-scoped profile revisions selected per producer source.
    /// Empty bindings preserve the legacy ingestion contract.
    pub async fn ingest_with_profiles(
        &self,
        request: IngestionRequest,
        profiles: kg_core::profiles::ProfileBindings,
    ) -> Result<PipelineOutput, PipelineError> {
        kg_core::profiles::validate_bindings(&profiles).map_err(|e| {
            PipelineError::StateValidation {
                stage: "profile_admission".into(),
                message: e.to_string(),
            }
        })?;
        if request.snapshots.is_empty() {
            kg_core::models::validate_request(&request.org_id, &[]).map_err(|error| {
                PipelineError::StateValidation {
                    stage: "input_validation".into(),
                    message: error.to_string(),
                }
            })?;
        }
        for (index, input) in request.snapshots.iter().enumerate() {
            input
                .validate_request(&request.org_id)
                .map_err(|mut error| {
                    error.field = format!("snapshots[{index}].{}", error.field);
                    PipelineError::StateValidation {
                        stage: "input_validation".into(),
                        message: error.to_string(),
                    }
                })?;
        }
        let run_id = request.run_id.unwrap_or_else(Uuid::new_v4);
        let mut effective = self.settings.clone();
        let admission_cancel = request.cancel.clone().unwrap_or_default();
        let admission_timeout = std::time::Duration::from_millis(effective.context.read_timeout_ms);
        let stored_run = tokio::select! {
            biased;
            _ = admission_cancel.cancelled() => return Err(PipelineError::Cancelled),
            result = tokio::time::timeout(admission_timeout, self.backends.graph.read_run(&request.org_id, run_id)) =>
                result.map_err(|_| PipelineError::StepExecution {
                    stage: "rule_admission".into(), step: "read_run".into(),
                    cause: "rule admission read timed out".into(), retriable: true,
                })?,
        }
        .map_err(|error| PipelineError::StepExecution {
            stage: "rule_admission".into(),
            step: "read_run".into(),
            cause: error.to_string(),
            retriable: error.is_transient(),
        })?;
        let rule_freezes = match stored_run {
            Some(header) => {
                kg_core::runtime::rule_learning::materialize::apply_frozen_rules(
                    &header.rule_freezes,
                    &mut effective.extraction,
                )
                .map_err(|error| PipelineError::StateValidation {
                    stage: "rule_admission".into(),
                    message: error.to_string(),
                })?;
                header.rule_freezes
            }
            None => {
                let active = tokio::select! {
                    biased;
                    _ = admission_cancel.cancelled() => return Err(PipelineError::Cancelled),
                    result = tokio::time::timeout(admission_timeout, self.backends.graph.active_learned_rules(&request.org_id)) =>
                        result.map_err(|_| PipelineError::StepExecution {
                            stage: "rule_admission".into(), step: "list_active".into(),
                            cause: "rule admission read timed out".into(), retriable: true,
                        })?,
                }
                .map_err(|error| PipelineError::StepExecution {
                    stage: "rule_admission".into(),
                    step: "list_active".into(),
                    cause: error.to_string(),
                    retriable: error.is_transient(),
                })?;
                kg_core::runtime::rule_learning::materialize::materialize_rules(
                    active,
                    &mut effective.extraction,
                )
                .map_err(|error| PipelineError::StateValidation {
                    stage: "rule_admission".into(),
                    message: error.to_string(),
                })?
            }
        };
        let mut ctx = self
            .context_with_settings(
                &effective,
                &request.org_id,
                request.cancel,
                request.trace_id,
            )
            .map_err(|e| PipelineError::StateValidation {
                stage: "request".into(),
                message: e.to_string(),
            })?;
        ctx.profile_registry = self.profile_registry.clone();
        ctx.profile_bindings = profiles;
        ctx.rule_freezes = Arc::new(rule_freezes);
        self.runner
            .run_inputs_with_id(request.snapshots, Arc::new(ctx), run_id)
            .await
    }

    /// Build and publish Communities for one namespace before returning.
    pub async fn rebuild_communities(
        &self,
        request: CommunityMaintenanceRequest,
    ) -> Result<PipelineOutput, PipelineError> {
        let ctx = self
            .context(&request.org_id, request.cancel, request.trace_id)
            .map_err(|error| PipelineError::StateValidation {
                stage: "community_request".into(),
                message: error.to_string(),
            })?;
        self.runner
            .rebuild_communities(
                request.namespace,
                Arc::new(ctx),
                request.run_id.unwrap_or_else(Uuid::new_v4),
            )
            .await
    }

    /// Summarize one Saga on demand through the same receipted commit path
    /// ingestion uses. A name resolves to its deterministic UUID; the run then
    /// verifies the Saga exists in the organization and namespace.
    pub async fn summarize_thread(
        &self,
        request: ThreadMaintenanceRequest,
    ) -> Result<PipelineOutput, PipelineError> {
        self.summarize_saga(request).await
    }

    /// Legacy summary entry point; new callers should use `summarize_thread`.
    pub async fn summarize_saga(
        &self,
        request: ThreadMaintenanceRequest,
    ) -> Result<PipelineOutput, PipelineError> {
        let ctx = self
            .context(&request.org_id, request.cancel, request.trace_id)
            .map_err(|error| PipelineError::StateValidation {
                stage: "saga_request".into(),
                message: error.to_string(),
            })?;
        let saga_uuid = match request.saga {
            kg_core::saga::ThreadReference::Uuid { uuid } => uuid,
            kg_core::saga::ThreadReference::Name { name } => {
                kg_core::saga::saga_uuid(&request.org_id, &request.namespace, &name)
            }
        };
        self.runner
            .summarize_saga(
                request.namespace,
                saga_uuid,
                Arc::new(ctx),
                request.run_id.unwrap_or_else(Uuid::new_v4),
            )
            .await
    }

    /// Re-evaluate one bounded page of source owners after activation or
    /// revocation. Every source uses a deterministic receipted run, so retrying
    /// the same page replays completed work and resumes at the failed source.
    pub async fn repair_rule(
        &self,
        request: RuleMaintenanceRequest,
    ) -> Result<RuleMaintenanceOutput, PipelineError> {
        use kg_core::traits::{ReferenceRuleSourceQuery, RuleStatus};

        if request.rule.org_id != request.org_id
            || !matches!(
                request.rule.status,
                RuleStatus::Active | RuleStatus::Stale | RuleStatus::Revoked
            )
            || !(1..=1000).contains(&request.limit)
        {
            return Err(PipelineError::StateValidation {
                stage: "rule_repair".into(),
                message:
                    "rule repair requires a scoped active, stale or revoked rule and a page of 1..=1000"
                        .into(),
            });
        }
        let cancel = request.cancel.clone().unwrap_or_default();
        let timeout = std::time::Duration::from_millis(self.settings.context.read_timeout_ms);
        let current = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(PipelineError::Cancelled),
            result = tokio::time::timeout(timeout, self.backends.graph.learned_rule(&request.org_id, request.rule.id)) =>
                result.unwrap_or(Err(kg_core::errors::BackendError::Timeout(self.settings.context.read_timeout_ms))),
        }
            .map_err(|error| PipelineError::StepExecution {
                stage: "rule_repair".into(),
                step: "revision_fence".into(),
                cause: error.to_string(),
                retriable: error.is_transient(),
            })?
            .ok_or_else(|| PipelineError::StateValidation {
                stage: "rule_repair".into(),
                message: "rule revision no longer exists".into(),
            })?;
        if current.revision != request.rule.revision
            || current.status != request.rule.status
            || current.mapping != request.rule.mapping
        {
            return Err(PipelineError::StateValidation {
                stage: "rule_repair".into(),
                message: "rule revision changed before repair".into(),
            });
        }
        let effective_at = match request.rule.status {
            RuleStatus::Active => request.rule.effective_from,
            RuleStatus::Stale => request.rule.decisions.last().map(|decision| decision.at),
            RuleStatus::Revoked => request.rule.revoked_at,
            _ => None,
        }
        .ok_or_else(|| PipelineError::StateValidation {
            stage: "rule_repair".into(),
            message: "rule lifecycle timestamp is missing".into(),
        })?;
        let source_query = ReferenceRuleSourceQuery {
            source: request.rule.source.clone(),
            namespace: request.rule.namespace.clone(),
            entity_type: request.rule.mapping.source_entity_type.clone(),
            after_chain: request.after_chain,
            limit: request.limit,
        };
        let page = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(PipelineError::Cancelled),
            result = tokio::time::timeout(timeout, self.backends.graph.reference_rule_sources(
                &request.org_id,
                &source_query,
            )) => result.map_err(|_| PipelineError::StepExecution {
                stage: "rule_repair".into(),
                step: "source_page".into(),
                cause: "rule source read timed out".into(),
                retriable: true,
            })?,
        }
        .map_err(|error| PipelineError::StepExecution {
            stage: "rule_repair".into(),
            step: "source_page".into(),
            cause: error.to_string(),
            retriable: error.is_transient(),
        })?;

        let mut effective = self.settings.clone();
        let active = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(PipelineError::Cancelled),
            result = tokio::time::timeout(timeout, self.backends.graph.active_learned_rules(&request.org_id)) =>
                result.map_err(|_| PipelineError::StepExecution {
                    stage: "rule_repair".into(), step: "list_active".into(),
                    cause: "active-rule read timed out".into(), retriable: true,
                })?,
        }
        .map_err(|error| PipelineError::StepExecution {
            stage: "rule_repair".into(),
            step: "list_active".into(),
            cause: error.to_string(),
            retriable: error.is_transient(),
        })?;
        let freezes = kg_core::runtime::rule_learning::materialize::materialize_rules(
            active,
            &mut effective.extraction,
        )
        .map_err(|error| PipelineError::StateValidation {
            stage: "rule_repair".into(),
            message: error.to_string(),
        })?;
        let freeze_fingerprint =
            serde_json::to_vec(&freezes).map_err(|_| PipelineError::StateValidation {
                stage: "rule_repair".into(),
                message: "active rules cannot be fingerprinted".into(),
            })?;

        let mut committed = CommittedCounts::default();
        let mut run_ids = Vec::with_capacity(page.records.len());
        for record in page.records {
            if cancel.is_cancelled() {
                return Err(PipelineError::Cancelled);
            }
            let mut seed = Vec::new();
            seed.extend_from_slice(request.rule.id.as_bytes());
            seed.extend_from_slice(&request.rule.revision.to_be_bytes());
            seed.extend_from_slice(record.chain_id.as_bytes());
            seed.extend_from_slice(&record.version.to_be_bytes());
            seed.extend_from_slice(&freeze_fingerprint);
            let run_id = Uuid::new_v5(
                &Uuid::from_u128(0x4d6a_7f1c_03b2_49a8_a132_d40c_1f52_b710),
                &seed,
            );
            let mut ctx = self
                .context_with_settings(
                    &effective,
                    &request.org_id,
                    Some(cancel.clone()),
                    request.trace_id.clone(),
                )
                .map_err(|error| PipelineError::StateValidation {
                    stage: "rule_repair".into(),
                    message: error.to_string(),
                })?;
            ctx.rule_freezes = Arc::new(freezes.clone());
            ctx.rule_maintenance_guard =
                Some((request.rule.id, request.rule.revision, request.rule.status));
            ctx.rule_maintenance_effective_at = Some(effective_at);
            let output = self
                .runner
                .repair_reference_owner(
                    record,
                    request.rule.owner_slot.clone(),
                    effective_at,
                    Arc::new(ctx),
                    run_id,
                )
                .await?;
            committed
                .add(&output.committed)
                .map_err(|error| PipelineError::StateValidation {
                    stage: "rule_repair".into(),
                    message: error.to_string(),
                })?;
            run_ids.push(run_id);
        }
        Ok(RuleMaintenanceOutput {
            rule_id: request.rule.id,
            rule_revision: request.rule.revision,
            sources_processed: run_ids.len(),
            next_after: page.next_after,
            complete: page.next_after.is_none(),
            run_ids,
            committed,
        })
    }

    fn context(
        &self,
        org_id: &str,
        cancel: Option<CancellationToken>,
        trace_id: Option<String>,
    ) -> Result<kg_core::runtime::RuntimeContext, ConfigError> {
        self.context_with_settings(&self.settings, org_id, cancel, trace_id)
    }

    fn context_with_settings(
        &self,
        settings: &IngestionSettings,
        org_id: &str,
        cancel: Option<CancellationToken>,
        trace_id: Option<String>,
    ) -> Result<kg_core::runtime::RuntimeContext, ConfigError> {
        let mut context = Self::build_context(&self.backends, settings, org_id, cancel, trace_id)?;
        context.matching_cache = self.matching_cache.clone();
        context.relationship_naming_cache = self.relationship_naming_cache.clone();
        context.semaphore = self.pools.general.clone();
        context.llm_extraction_semaphore = self.pools.extraction.clone();
        context.llm_disambiguation_semaphore = self.pools.disambiguation.clone();
        context.llm_edge_semaphore = self.pools.edge_discovery.clone();
        context.embed_semaphore = self.pools.embedding.clone();
        context.entity_summary_semaphore = self.pools.summary.clone();
        context.saga_summary_semaphore = self.pools.saga_summary.clone();
        context.community_semaphore = self.pools.community.clone();
        context.decision_semaphore = self.pools.decision.clone();
        Ok(context)
    }

    fn build_context(
        backends: &Backends,
        settings: &IngestionSettings,
        org_id: &str,
        cancel: Option<CancellationToken>,
        trace_id: Option<String>,
    ) -> Result<kg_core::runtime::RuntimeContext, ConfigError> {
        let mut builder = RuntimeContextBuilder::new(org_id)
            .matching_settings(settings.matching.clone())
            .reference_resolution_settings(settings.reference_resolution.clone())
            .extraction_settings(settings.extraction.clone())
            .context_settings(settings.context.clone())
            .entity_summary_settings(settings.entity_summary.clone())
            .saga_summary_settings(settings.saga_summary.clone())
            .community_settings(settings.community.clone())
            .graph(backends.graph.clone())
            .llm_extraction(backends.llm_extraction.clone())
            .llm_default(backends.llm_default.clone())
            .typed_decisions(settings.typed_decisions.clone())
            .embedder(backends.embedder.clone())
            .entity_embedding_fields(settings.entity_embedding_fields.clone())
            .policy(PolicyResolver::with_tenant(
                settings.policy,
                TenantPipelinePolicy {
                    per_source: settings.source_policies.clone(),
                    ..Default::default()
                },
            ))
            .entity_type_configs(settings.entity_type_configs.clone())
            .exec_config(ExecutionConfig {
                continue_on_step_error: settings.continue_on_step_error,
            })
            .max_concurrency(settings.max_concurrency);
        if let Some(llm) = &backends.llm_disambiguation {
            builder = builder.llm_disambiguation(llm.clone());
        }
        if let Some(llm) = &backends.llm_edge_discovery {
            builder = builder.llm_edge_discovery(llm.clone());
        }
        if let Some(decisions) = &backends.decisions {
            builder = builder.decisions(decisions.clone());
        }
        if let Some(store) = &backends.ontology_store {
            builder = builder.ontology_store(store.clone());
        }
        if let Some(store) = &backends.schema_store {
            builder = builder.schema_store(store.clone());
        }
        if let Some(policy) = &settings.namespace_policy {
            builder = builder.namespace_policy(policy.clone());
        }
        if let Some(n) = settings.embed_concurrency {
            builder = builder.embed_concurrency(n);
        }
        if let Some((extraction, disambiguation, edge)) = settings.llm_slot_concurrency {
            builder = builder.llm_slot_concurrency(extraction, disambiguation, edge);
        }
        if let Some(cancel) = cancel {
            builder = builder.cancel_token(cancel);
        }
        if let Some(trace_id) = trace_id {
            builder = builder.trace_id(trace_id);
        }
        builder.build()
    }
}

fn advancing_repair_cursor(
    previous: Option<Uuid>,
    next: Option<Uuid>,
) -> Result<Uuid, kg_core::errors::BackendError> {
    let next = next.filter(|next| !next.is_nil() && previous.is_none_or(|prior| *next > prior));
    next.ok_or_else(|| {
        kg_core::errors::BackendError::Deserialization(
            "incomplete learned-rule repair must advance its cursor".into(),
        )
    })
}

#[async_trait::async_trait]
impl kg_core::runtime::rule_learning::RuleLifecycleRepair for Engine {
    async fn reconcile(
        &self,
        rule: &kg_core::traits::LearnedRule,
    ) -> Result<(), kg_core::errors::BackendError> {
        let mut after_chain = None;
        for _ in 0..self.settings.rule_repair_max_pages {
            let output = self
                .repair_rule(RuleMaintenanceRequest {
                    org_id: rule.org_id.clone(),
                    rule: rule.clone(),
                    after_chain,
                    limit: 1000,
                    cancel: None,
                    trace_id: None,
                })
                .await
                .map_err(|error| {
                    if error.is_retriable() {
                        kg_core::errors::BackendError::Unavailable(format!(
                            "learned-rule graph repair did not complete: {error}"
                        ))
                    } else {
                        kg_core::errors::BackendError::Query(format!(
                            "learned-rule graph repair was rejected: {error}"
                        ))
                    }
                })?;
            if output.complete {
                return Ok(());
            }
            after_chain = Some(advancing_repair_cursor(after_chain, output.next_after)?);
        }
        Err(kg_core::errors::BackendError::Unavailable(
            format!("learned-rule repair page budget exhausted after cursor {after_chain:?}; resume manual repair from this cursor")))
    }
}

#[async_trait::async_trait]
impl kg_core::runtime::rule_learning::validation::RuleValidationExecutor for Engine {
    async fn predict(
        &self,
        org_id: &str,
        source: &str,
        mapping: &kg_core::runtime::extraction::ReferenceMapping,
        examples: &[kg_core::runtime::rule_learning::ValidationExample],
    ) -> Result<Vec<Option<Uuid>>, kg_core::errors::BackendError> {
        let mut settings = self.settings.clone();
        settings
            .extraction
            .reference_guidance
            .insert(source.to_owned(), vec![mapping.clone()]);
        let ctx = self
            .context_with_settings(&settings, org_id, None, None)
            .map_err(|_| {
                kg_core::errors::BackendError::Query("rule validation context is invalid".into())
            })?;
        let expected_slot = format!("{}.{}", mapping.source_entity_type, mapping.reference_path);
        let mut predictions = Vec::with_capacity(examples.len());
        for example in examples {
            let source_version_uuid = example.source_version_uuid.ok_or_else(|| {
                kg_core::errors::BackendError::Query(
                    "rule validation example has no frozen source version".into(),
                )
            })?;
            if example.reference_tokens.is_empty() {
                return Err(kg_core::errors::BackendError::Query(
                    "rule validation example has no structured reference occurrence".into(),
                ));
            }
            let records = tokio::time::timeout(
                std::time::Duration::from_millis(settings.context.read_timeout_ms),
                self.backends.graph.find_entities(
                    org_id,
                    &EntityLookup::LatestByChain {
                        chain_ids: vec![example.source_chain_id],
                    },
                ),
            )
            .await
            .map_err(|_| {
                kg_core::errors::BackendError::Timeout(settings.context.read_timeout_ms)
            })??;
            // A source re-versioned since the case was labelled is no longer
            // frozen evidence: the case yields no prediction rather than
            // failing the whole learning pass.
            let Some(record) = records
                .into_iter()
                .find(|record| record.uuid == source_version_uuid)
            else {
                predictions.push(None);
                continue;
            };
            if record.source.as_deref() != Some(source)
                || record.entity_type != example.source_entity_type
                || record.namespace != example.source_namespace
                || !record.is_latest
                || record.deleted_at.is_some()
            {
                return Err(kg_core::errors::BackendError::Query(
                    "rule validation source escaped its frozen live evidence scope".into(),
                ));
            }
            let snapshot_id = record
                .stored
                .get("last_seen_snapshot_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok());
            let observed_at = record.last_seen_at.or(record.valid_from).ok_or_else(|| {
                kg_core::errors::BackendError::Deserialization(
                    "rule validation source has no observation timestamp".into(),
                )
            })?;
            let resolution = kg_pipeline::runner::stored_reference_resolution(
                &ctx,
                &record,
                (snapshot_id, observed_at),
                vec![],
            )
            .map_err(|_| {
                kg_core::errors::BackendError::Query(
                    "rule validation source cannot be rehydrated".into(),
                )
            })?;
            let output = crate::ReferenceExtractionStage
                .process(
                    kg_core::runtime::StageOutput::EdgeExtraction(
                        kg_core::runtime::stage_output::EdgeExtractionOutput {
                            relationship_times: Default::default(),
                            relationship_directives: Default::default(),
                            reference_report: Default::default(),
                            snapshot_nodes: resolution.snapshot_nodes.clone(),
                            resolved_nodes: resolution.live_entities(),
                            resolution: Arc::new(resolution),
                            edges: Default::default(),
                            pending_references: Default::default(),
                        },
                    ),
                    &ctx,
                )
                .await
                .map_err(|_| {
                    kg_core::errors::BackendError::Query("rule validation matcher failed".into())
                })?;
            let kg_core::runtime::StageOutput::EdgeExtraction(output) = output else {
                return Err(kg_core::errors::BackendError::Deserialization(
                    "rule validation matcher returned the wrong output".into(),
                ));
            };
            let selected = output
                .edges
                .iter()
                .filter(|edge| {
                    edge.discovered_by.as_deref() == Some("guided_fk")
                        && edge.reference_evidence.as_ref().is_some_and(|evidence| {
                            evidence.slot == expected_slot
                                && evidence
                                    .reference_tokens
                                    .iter()
                                    .collect::<std::collections::BTreeSet<_>>()
                                    == example
                                        .reference_tokens
                                        .iter()
                                        .collect::<std::collections::BTreeSet<_>>()
                        })
                })
                .map(|edge| {
                    if edge.source_chain_id == example.source_chain_id {
                        edge.target_chain_id
                    } else {
                        edge.source_chain_id
                    }
                })
                .collect::<std::collections::BTreeSet<_>>();
            predictions.push((selected.len() == 1).then(|| *selected.first().unwrap()));
        }
        Ok(predictions)
    }
}

/// Legacy Rust request name; use ThreadMaintenanceRequest for new callers.
pub type SagaMaintenanceRequest = ThreadMaintenanceRequest;

#[cfg(test)]
mod builder_tests {
    use super::*;
    #[test]
    fn deterministic_defaults_and_overrides_are_validated() {
        let settings = IngestionSettings::default();
        settings.validate().unwrap();
        for changed in [
            serde_json::json!({"recipe":"full"}),
            serde_json::json!({"policy":{"extraction":"llm"}}),
            serde_json::json!({"policy":{"matching":"semantic"}}),
            serde_json::json!({"source_policies":{"aws":{"edge_ambiguity":"llm"}}}),
            serde_json::json!({"extraction":{"relationship_naming":{"enabled":true}}}),
            serde_json::json!({"entity_summary":{"enabled":true}}),
            serde_json::json!({"saga_summary":{"enabled":true}}),
            serde_json::json!({"community":{"incremental_enabled":true}}),
        ] {
            let mut json = serde_json::to_value(&settings).unwrap();
            for (key, value) in changed.as_object().unwrap() {
                json[key] = value.clone();
            }
            let settings: IngestionSettings = serde_json::from_value(json).unwrap();
            assert!(settings.validate().is_err(), "{changed}");
        }
    }
}
