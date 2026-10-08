//! Caller-provided runtime dependencies and builder.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::embedding::EmbeddingSettings;
use crate::entity_type_config::EntityTypeConfig;
use crate::errors::ConfigError;
use crate::runtime::execution::ExecutionConfig;
use crate::tenant::NamespacePolicy;
use crate::traits::{EmbedBackend, GraphBackend, LlmBackend};

/// Caller-supplied backends, policies, and state for one pipeline run.
///
/// Build one context per request: organization and cancellation are request-local.
/// Backends may be shared across requests.
#[derive(Clone)]
pub struct RuntimeContext {
    /// Deadline for graph-dependent planning. Never cancels a submitted commit.
    pub identity_deadline: Option<tokio::time::Instant>,
    /// Selected by run registration; stages must not refresh mutable definitions.
    pub profile_registry: Option<Arc<dyn crate::traits::ProfileRegistry>>,
    pub profile_bindings: crate::profiles::ProfileBindings,
    pub run_schemas: Option<Arc<super::schemas::RunSchemaManifest>>,
    pub rule_freezes: Arc<Vec<super::rule_learning::materialize::RuleFreeze>>,
    /// Lifecycle revision that must remain current for each maintenance commit.
    pub rule_maintenance_guard: Option<(uuid::Uuid, u64, crate::traits::RuleStatus)>,
    /// Effective lifecycle time used when stale or revoked rules retire their
    /// previously materialized relationships.
    pub rule_maintenance_effective_at: Option<chrono::DateTime<chrono::Utc>>,
    pub observation_manifest: Option<Arc<super::saga::RunObservationManifest>>,
    pub original_inputs: Arc<Vec<crate::models::IngestionInput>>,
    /// Original caller position supplied by the worker, independent of execution order.
    pub snapshot_index: Option<usize>,
    pub graph: Arc<dyn GraphBackend>,
    pub llm_extraction: Arc<dyn LlmBackend>,
    pub llm_disambiguation: Arc<dyn LlmBackend>,
    pub llm_edge_discovery: Arc<dyn LlmBackend>,
    /// Fallback model for unset disambiguation and edge-discovery slots.
    pub llm_default: Arc<dyn LlmBackend>,
    /// Typed decisions (identity, contradictions, caller questions); None when
    /// no decision backend is configured.
    pub decisions: Option<Arc<dyn crate::traits::DecisionBackend>>,
    pub typed_decisions: super::decisions::TypedDecisionSettings,
    /// Decision calls must hold a permit; the pool is shared per engine.
    pub decision_semaphore: Arc<Semaphore>,
    pub embedder: Arc<dyn EmbedBackend>,
    /// Model, dimensions, and text representation included in the request fingerprint.
    pub embedding: EmbeddingSettings,
    pub matching_settings: super::matching::MatchingSettings,
    /// Bounds of evidence-backed ambiguous reference decisions.
    pub reference_resolution_settings: super::reference_resolution::ReferenceResolutionSettings,
    pub incoming_embeddings: Arc<super::embedding_cache::IncomingEmbeddingCache>,
    pub matching_cache: Arc<super::matching_cache::MatchingCache>,
    /// Paid reference decisions reused across evidence refreshes within one run.
    pub reference_decisions: Arc<super::reference_cache::ReferenceDecisionCache>,
    /// Semantic relationship-naming decisions reused while their evidence is
    /// unchanged; the engine shares one bounded cache across its requests.
    pub relationship_naming_cache: Arc<super::relationship_naming_cache::RelationshipNamingCache>,
    pub extraction_settings: super::extraction::ExtractionSettings,
    pub context_settings: super::history::ContextSettings,
    pub community_settings: super::community::CommunitySettings,
    pub saga_summary_settings: super::saga::SagaSummarySettings,
    pub entity_summary_settings: super::entity_summary::EntitySummarySettings,

    /// Organization scope. The builder rejects blank values; callers must preserve
    /// this boundary when supplying backends and constructing inputs.
    pub org_id: Arc<str>,
    pub trace_id: String,

    pub namespace_policy: Arc<NamespacePolicy>,
    pub exec_config: Arc<ExecutionConfig>,
    pub entity_type_configs: Arc<HashMap<String, EntityTypeConfig>>,

    /// Adopt-once schemas. Without a store, entities lacking declared primary keys are rejected.
    pub schema_store: Option<Arc<dyn crate::traits::SchemaStore>>,

    /// Source-specific ontology guidance; `None` disables it.
    pub ontology_store: Option<Arc<dyn crate::traits::OntologyStore>>,

    pub policy: Arc<crate::policy::PolicyResolver>,

    pub semaphore: Arc<Semaphore>,
    /// Extraction calls must hold a permit; the pool defaults to `max_concurrency`.
    pub llm_extraction_semaphore: Arc<Semaphore>,
    pub llm_disambiguation_semaphore: Arc<Semaphore>,
    pub llm_edge_semaphore: Arc<Semaphore>,
    /// Embedding calls must hold a permit; the pool defaults to `max_concurrency`.
    pub embed_semaphore: Arc<Semaphore>,
    pub community_model_calls: Arc<std::sync::atomic::AtomicUsize>,
    pub community_semaphore: Arc<Semaphore>,
    pub saga_summary_semaphore: Arc<Semaphore>,
    pub entity_summary_semaphore: Arc<Semaphore>,
    /// Cooperative cancellation observed by the runner and workers.
    /// Cancellation does not roll back completed writes.
    pub cancel: CancellationToken,
}

impl std::fmt::Debug for RuntimeContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeContext")
            .field("org_id", &self.org_id)
            .field("trace_id", &self.trace_id)
            .finish_non_exhaustive()
    }
}

/// Validates organization, concurrency bounds, and required backends.
/// Requires explicit graph, LLM, and embedding backends. Disabled model backends
/// are allowed for paths that do not call them.
/// Unset disambiguation and edge LLM slots use `llm_default`.
pub struct RuntimeContextBuilder {
    matching_settings: super::matching::MatchingSettings,
    reference_resolution_settings: super::reference_resolution::ReferenceResolutionSettings,
    extraction_settings: super::extraction::ExtractionSettings,
    context_settings: super::history::ContextSettings,
    community_settings: super::community::CommunitySettings,
    saga_summary_settings: super::saga::SagaSummarySettings,
    entity_summary_settings: super::entity_summary::EntitySummarySettings,
    graph: Option<Arc<dyn GraphBackend>>,
    llm_extraction: Option<Arc<dyn LlmBackend>>,
    llm_disambiguation: Option<Arc<dyn LlmBackend>>,
    llm_edge_discovery: Option<Arc<dyn LlmBackend>>,
    llm_default: Option<Arc<dyn LlmBackend>>,
    decisions: Option<Arc<dyn crate::traits::DecisionBackend>>,
    typed_decisions: super::decisions::TypedDecisionSettings,
    entity_embedding_fields: crate::embedding::EntityEmbeddingFields,
    embedder: Option<Arc<dyn EmbedBackend>>,
    org_id: Arc<str>,
    trace_id: Option<String>,
    namespace_policy: Option<Arc<NamespacePolicy>>,
    exec_config: Option<Arc<ExecutionConfig>>,
    entity_type_configs: Option<Arc<HashMap<String, EntityTypeConfig>>>,
    schema_store: Option<Arc<dyn crate::traits::SchemaStore>>,
    ontology_store: Option<Arc<dyn crate::traits::OntologyStore>>,
    policy: Option<Arc<crate::policy::PolicyResolver>>,
    max_concurrency: usize,
    llm_slot_concurrency: Option<(usize, usize, usize)>,
    embed_concurrency: Option<usize>,
    cancel: Option<CancellationToken>,
}

impl RuntimeContextBuilder {
    pub fn new(org_id: impl Into<Arc<str>>) -> Self {
        Self {
            matching_settings: Default::default(),
            reference_resolution_settings: Default::default(),
            extraction_settings: Default::default(),
            context_settings: Default::default(),
            entity_summary_settings: Default::default(),
            community_settings: Default::default(),
            saga_summary_settings: Default::default(),
            graph: None,
            llm_extraction: None,
            llm_disambiguation: None,
            llm_edge_discovery: None,
            llm_default: None,
            decisions: None,
            typed_decisions: Default::default(),
            entity_embedding_fields: Default::default(),
            embedder: None,
            org_id: org_id.into(),
            trace_id: None,
            namespace_policy: None,
            exec_config: None,
            entity_type_configs: None,
            schema_store: None,
            ontology_store: None,
            policy: None,
            max_concurrency: 20,
            llm_slot_concurrency: None,
            embed_concurrency: None,
            cancel: None,
        }
    }

    /// Configure text discovery without changing source identity or versioning.
    pub fn extraction_settings(mut self, settings: super::extraction::ExtractionSettings) -> Self {
        self.extraction_settings = settings;
        self
    }

    pub fn matching_settings(mut self, settings: super::matching::MatchingSettings) -> Self {
        self.matching_settings = settings;
        self
    }

    /// Bound the evidence-backed ambiguous reference decisions.
    pub fn reference_resolution_settings(
        mut self,
        settings: super::reference_resolution::ReferenceResolutionSettings,
    ) -> Self {
        self.reference_resolution_settings = settings;
        self
    }

    pub fn community_settings(mut self, settings: super::community::CommunitySettings) -> Self {
        self.community_settings = settings;
        self
    }

    pub fn saga_summary_settings(mut self, settings: super::saga::SagaSummarySettings) -> Self {
        self.saga_summary_settings = settings;
        self
    }

    pub fn entity_summary_settings(
        mut self,
        settings: super::entity_summary::EntitySummarySettings,
    ) -> Self {
        self.entity_summary_settings = settings;
        self
    }

    pub fn context_settings(mut self, settings: super::history::ContextSettings) -> Self {
        self.context_settings = settings;
        self
    }

    pub fn graph(mut self, g: Arc<dyn GraphBackend>) -> Self {
        self.graph = Some(g);
        self
    }

    pub fn llm_extraction(mut self, l: Arc<dyn LlmBackend>) -> Self {
        self.llm_extraction = Some(l);
        self
    }

    pub fn llm_disambiguation(mut self, l: Arc<dyn LlmBackend>) -> Self {
        self.llm_disambiguation = Some(l);
        self
    }

    pub fn llm_edge_discovery(mut self, l: Arc<dyn LlmBackend>) -> Self {
        self.llm_edge_discovery = Some(l);
        self
    }

    pub fn llm_default(mut self, l: Arc<dyn LlmBackend>) -> Self {
        self.llm_default = Some(l);
        self
    }

    /// The typed-decision backend; the switch itself is `typed_decisions`.
    pub fn decisions(mut self, d: Arc<dyn crate::traits::DecisionBackend>) -> Self {
        self.decisions = Some(d);
        self
    }

    pub fn typed_decisions(mut self, settings: super::decisions::TypedDecisionSettings) -> Self {
        self.typed_decisions = settings;
        self
    }

    pub fn entity_embedding_fields(
        mut self,
        fields: crate::embedding::EntityEmbeddingFields,
    ) -> Self {
        self.entity_embedding_fields = fields;
        self
    }

    pub fn embedder(mut self, e: Arc<dyn EmbedBackend>) -> Self {
        self.embedder = Some(e);
        self
    }

    /// Set the log-correlation id (defaults to a fresh UUIDv4).
    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.trace_id = Some(id.into());
        self
    }

    /// Set the cross-namespace edge-resolution policy (defaults to same namespace).
    pub fn namespace_policy(mut self, p: NamespacePolicy) -> Self {
        self.namespace_policy = Some(Arc::new(p));
        self
    }

    /// Set pipeline execution behavior (defaults to fail-fast).
    pub fn exec_config(mut self, c: ExecutionConfig) -> Self {
        self.exec_config = Some(Arc::new(c));
        self
    }

    pub fn schema_store(mut self, store: Arc<dyn crate::traits::SchemaStore>) -> Self {
        self.schema_store = Some(store);
        self
    }

    pub fn policy(mut self, resolver: crate::policy::PolicyResolver) -> Self {
        self.policy = Some(Arc::new(resolver));
        self
    }

    pub fn ontology_store(mut self, store: Arc<dyn crate::traits::OntologyStore>) -> Self {
        self.ontology_store = Some(store);
        self
    }

    /// Set per-entity-type normalization configs (defaults to empty).
    pub fn entity_type_configs(mut self, c: HashMap<String, EntityTypeConfig>) -> Self {
        self.entity_type_configs = Some(Arc::new(c));
        self
    }

    /// General concurrency and default LLM pool size: 20; valid range `1..=Semaphore::MAX_PERMITS`.
    pub fn max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n;
        self
    }

    /// Extraction, disambiguation, and edge LLM pool sizes.
    /// Each defaults to `max_concurrency` and must be in `1..=Semaphore::MAX_PERMITS`.
    pub fn llm_slot_concurrency(
        mut self,
        extraction: usize,
        disambiguation: usize,
        edge: usize,
    ) -> Self {
        self.llm_slot_concurrency = Some((extraction, disambiguation, edge));
        self
    }

    /// Embedding pool size; defaults to `max_concurrency` and must be in `1..=Semaphore::MAX_PERMITS`.
    pub fn embed_concurrency(mut self, n: usize) -> Self {
        self.embed_concurrency = Some(n);
        self
    }

    /// Provide a cancellation token (defaults to a fresh, uncancelled one).
    pub fn cancel_token(mut self, t: CancellationToken) -> Self {
        self.cancel = Some(t);
        self
    }

    /// Validate organization scope, all semaphore bounds, and required backends.
    /// Invalid concurrency returns [`ConfigError::InvalidValue`] before allocation.
    pub fn build(self) -> Result<RuntimeContext, ConfigError> {
        self.community_settings
            .validate()
            .map_err(|message| ConfigError::InvalidValue {
                field: "community".into(),
                message,
            })?;
        self.saga_summary_settings
            .validate()
            .map_err(|message| ConfigError::InvalidValue {
                field: "saga_summary".into(),
                message,
            })?;
        self.entity_summary_settings
            .validate()
            .map_err(|message| ConfigError::InvalidValue {
                field: "summary".into(),
                message,
            })?;
        self.matching_settings
            .validate()
            .map_err(|e| ConfigError::InvalidValue {
                field: "matching".into(),
                message: e.to_string(),
            })?;
        self.reference_resolution_settings
            .validate()
            .map_err(|e| ConfigError::InvalidValue {
                field: "reference_resolution".into(),
                message: e.to_string(),
            })?;
        self.extraction_settings
            .validate()
            .map_err(|e| ConfigError::InvalidValue {
                field: "extraction".into(),
                message: e.to_string(),
            })?;
        self.context_settings
            .validate()
            .map_err(|e| ConfigError::InvalidValue {
                field: "context".into(),
                message: e.to_string(),
            })?;
        if self.org_id.trim().is_empty() {
            return Err(ConfigError::MissingField("org_id".into()));
        }
        let (extraction, disambiguation, edge) = self.llm_slot_concurrency.unwrap_or((
            self.max_concurrency,
            self.max_concurrency,
            self.max_concurrency,
        ));
        let embed = self.embed_concurrency.unwrap_or(self.max_concurrency);
        for (field, value) in [
            ("max_concurrency", self.max_concurrency),
            ("llm_extraction_concurrency", extraction),
            ("llm_disambiguation_concurrency", disambiguation),
            ("llm_edge_concurrency", edge),
            ("embed_concurrency", embed),
        ] {
            if !(1..=Semaphore::MAX_PERMITS).contains(&value) {
                return Err(ConfigError::InvalidValue {
                    field: field.into(),
                    message: format!("must be between 1 and {}", Semaphore::MAX_PERMITS),
                });
            }
        }
        if let Some(policy) = &self.namespace_policy {
            policy
                .validate()
                .map_err(|message| ConfigError::InvalidValue {
                    field: "namespace_policy".into(),
                    message,
                })?;
        }
        self.typed_decisions
            .validate()
            .map_err(|e| ConfigError::InvalidValue {
                field: "typed_decisions".into(),
                message: e.to_string(),
            })?;
        if self.typed_decisions.enabled && self.decisions.is_none() {
            return Err(ConfigError::MissingField("decisions".into()));
        }
        let graph = self
            .graph
            .ok_or_else(|| ConfigError::MissingField("graph".into()))?;
        let llm_extraction = self
            .llm_extraction
            .ok_or_else(|| ConfigError::MissingField("llm_extraction".into()))?;
        let llm_default = self
            .llm_default
            .ok_or_else(|| ConfigError::MissingField("llm_default".into()))?;
        let embedder = self
            .embedder
            .ok_or_else(|| ConfigError::MissingField("embedder".into()))?;
        let embedding = EmbeddingSettings::of(embedder.as_ref())?
            .with_entity_fields(self.entity_embedding_fields)
            .map_err(|e| ConfigError::InvalidValue {
                field: "entity_embedding_fields".into(),
                message: e.to_string(),
            })?;
        let llm_disambiguation = self
            .llm_disambiguation
            .unwrap_or_else(|| llm_default.clone());
        let llm_edge_discovery = self
            .llm_edge_discovery
            .unwrap_or_else(|| llm_default.clone());

        Ok(RuntimeContext {
            identity_deadline: None,
            profile_registry: None,
            profile_bindings: Default::default(),
            run_schemas: None,
            rule_freezes: Default::default(),
            rule_maintenance_guard: None,
            rule_maintenance_effective_at: None,
            observation_manifest: None,
            original_inputs: Arc::new(Vec::new()),
            snapshot_index: None,
            graph,
            llm_extraction,
            llm_disambiguation,
            llm_edge_discovery,
            llm_default,
            decisions: self.decisions,
            decision_semaphore: Arc::new(Semaphore::new(self.typed_decisions.max_concurrent)),
            typed_decisions: self.typed_decisions,
            embedder,
            embedding,
            matching_settings: self.matching_settings,
            reference_resolution_settings: self.reference_resolution_settings,
            incoming_embeddings: Arc::new(super::embedding_cache::IncomingEmbeddingCache::default()),
            matching_cache: Arc::new(super::matching_cache::MatchingCache::default()),
            reference_decisions: Arc::new(super::reference_cache::ReferenceDecisionCache::default()),
            relationship_naming_cache: Arc::new(
                super::relationship_naming_cache::RelationshipNamingCache::default(),
            ),
            extraction_settings: self.extraction_settings,
            context_settings: self.context_settings,
            entity_summary_semaphore: Arc::new(Semaphore::new(
                self.entity_summary_settings.max_concurrent_batches,
            )),
            saga_summary_semaphore: Arc::new(Semaphore::new(
                self.saga_summary_settings.max_concurrent_batches,
            )),
            community_model_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            community_semaphore: Arc::new(Semaphore::new(
                self.community_settings.max_concurrent_batches,
            )),
            community_settings: self.community_settings,
            saga_summary_settings: self.saga_summary_settings,
            entity_summary_settings: self.entity_summary_settings,
            org_id: self.org_id,
            trace_id: self
                .trace_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            namespace_policy: self.namespace_policy.unwrap_or_else(|| {
                Arc::new(NamespacePolicy {
                    environment_tiers: vec![],
                    cross_namespace_rules: vec![],
                    open_policy: false,
                })
            }),
            exec_config: self
                .exec_config
                .unwrap_or_else(|| Arc::new(ExecutionConfig::default())),
            schema_store: self.schema_store,
            ontology_store: self.ontology_store,
            policy: self.policy.unwrap_or_default(),
            entity_type_configs: self
                .entity_type_configs
                .unwrap_or_else(|| Arc::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(self.max_concurrency)),
            llm_extraction_semaphore: Arc::new(Semaphore::new(extraction)),
            llm_disambiguation_semaphore: Arc::new(Semaphore::new(disambiguation)),
            llm_edge_semaphore: Arc::new(Semaphore::new(edge)),
            embed_semaphore: Arc::new(Semaphore::new(embed)),
            cancel: self.cancel.unwrap_or_default(),
        })
    }
}

impl RuntimeContext {
    /// Freeze run schemas while reusing providers and their shared capacity limits.
    pub fn with_run_schemas(&self, manifest: super::schemas::RunSchemaManifest) -> Self {
        let mut extraction_settings = self.extraction_settings.clone();
        for (source, profile) in &manifest.profiles {
            let guidance = profile
                .document
                .source_guidance
                .get(source)
                .cloned()
                .unwrap_or_default();
            let target = extraction_settings
                .source_guidance
                .entry(source.clone())
                .or_default();
            target.shared_instructions = guidance.shared_instructions;
            target.instructions = guidance.instructions;
            target.relationship_instructions = guidance.relationship_instructions;
            target.identity_instructions = guidance.identity_instructions;
        }
        Self {
            profile_registry: self.profile_registry.clone(),
            profile_bindings: self.profile_bindings.clone(),
            identity_deadline: self.identity_deadline,
            run_schemas: Some(Arc::new(manifest)),
            rule_freezes: self.rule_freezes.clone(),
            rule_maintenance_guard: self.rule_maintenance_guard,
            rule_maintenance_effective_at: self.rule_maintenance_effective_at,
            observation_manifest: self.observation_manifest.clone(),
            original_inputs: self.original_inputs.clone(),
            snapshot_index: self.snapshot_index,
            graph: self.graph.clone(),
            llm_extraction: self.llm_extraction.clone(),
            llm_disambiguation: self.llm_disambiguation.clone(),
            llm_edge_discovery: self.llm_edge_discovery.clone(),
            llm_default: self.llm_default.clone(),
            decisions: self.decisions.clone(),
            typed_decisions: self.typed_decisions.clone(),
            decision_semaphore: self.decision_semaphore.clone(),
            embedder: self.embedder.clone(),
            embedding: self.embedding.clone(),
            matching_settings: self.matching_settings.clone(),
            reference_resolution_settings: self.reference_resolution_settings.clone(),
            incoming_embeddings: self.incoming_embeddings.clone(),
            matching_cache: self.matching_cache.clone(),
            reference_decisions: self.reference_decisions.clone(),
            relationship_naming_cache: self.relationship_naming_cache.clone(),
            extraction_settings,
            context_settings: self.context_settings.clone(),
            community_settings: self.community_settings.clone(),
            community_model_calls: self.community_model_calls.clone(),
            community_semaphore: self.community_semaphore.clone(),
            saga_summary_settings: self.saga_summary_settings.clone(),
            saga_summary_semaphore: self.saga_summary_semaphore.clone(),
            entity_summary_settings: self.entity_summary_settings.clone(),
            entity_summary_semaphore: self.entity_summary_semaphore.clone(),
            org_id: self.org_id.clone(),
            trace_id: self.trace_id.clone(),
            namespace_policy: self.namespace_policy.clone(),
            exec_config: self.exec_config.clone(),
            entity_type_configs: self.entity_type_configs.clone(),
            schema_store: self.schema_store.clone(),
            ontology_store: self.ontology_store.clone(),
            policy: self.policy.clone(),
            semaphore: self.semaphore.clone(),
            llm_extraction_semaphore: self.llm_extraction_semaphore.clone(),
            llm_disambiguation_semaphore: self.llm_disambiguation_semaphore.clone(),
            llm_edge_semaphore: self.llm_edge_semaphore.clone(),
            embed_semaphore: self.embed_semaphore.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

impl RuntimeContext {
    pub fn frozen_observation(&self) -> Option<&super::saga::FrozenObservation> {
        self.observation_manifest
            .as_ref()?
            .entries
            .get(self.snapshot_index?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_concurrency_returns_an_error_without_panicking() {
        for value in [0, Semaphore::MAX_PERMITS + 1] {
            let error = RuntimeContextBuilder::new("org")
                .max_concurrency(value)
                .build()
                .unwrap_err();
            assert!(
                matches!(error, ConfigError::InvalidValue { field, .. } if field == "max_concurrency")
            );
            for (slots, expected) in [
                ((value, 1, 1), "llm_extraction_concurrency"),
                ((1, value, 1), "llm_disambiguation_concurrency"),
                ((1, 1, value), "llm_edge_concurrency"),
            ] {
                assert!(matches!(
                    RuntimeContextBuilder::new("org")
                        .llm_slot_concurrency(slots.0, slots.1, slots.2)
                        .build(),
                    Err(ConfigError::InvalidValue { field, .. }) if field == expected
                ));
            }
            assert!(matches!(
                RuntimeContextBuilder::new("org")
                    .embed_concurrency(value)
                    .build(),
                Err(ConfigError::InvalidValue { field, .. }) if field == "embed_concurrency"
            ));
        }
    }

    #[test]
    fn valid_concurrency_boundaries_reach_backend_validation() {
        for value in [1, Semaphore::MAX_PERMITS] {
            assert!(matches!(
                RuntimeContextBuilder::new("org")
                    .max_concurrency(value)
                    .llm_slot_concurrency(value, value, value)
                    .build(),
                Err(ConfigError::MissingField(field)) if field == "graph"
            ));
        }
    }
}
