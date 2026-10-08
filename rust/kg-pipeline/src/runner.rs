use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use tracing::Instrument;
use uuid::Uuid;

use kg_core::enums::EntityLifecycle;
use kg_core::errors::{BackendError, PipelineError, StageError};
use kg_core::models::CollectionRef;
use kg_core::models::{
    validate_collection_scopes, validate_request, EntityNode, IngestionInput, SnapshotDataType,
    SnapshotInput, SnapshotKind, SnapshotNode,
};
use kg_core::pipeline::{
    BatchOutcome, CollectionOutcome, CommittedCounts, IncompleteSnapshot, PipelineOutput,
    SnapshotFailure,
};
use kg_core::runtime::stage_output::{
    BatchRecovery, ChainsMerged, CollectionScan, CommitOutput, FlushBatchOutput, FlushWork,
    NodeBatch, NodeCheckpoint, NodeResolutionOutput, Observed, ObservedEntityProperties,
    ReconciliationBatch, ReferenceOwnerSelector, ReferenceReport, RelationshipBaseline,
    RelationshipBatch, RelationshipDeclineReason, StaleEdge, StaleEntity,
};
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::graph_commit::MAX_STATEMENTS_PER_BATCH;
use kg_core::traits::{
    BatchIdentity, BatchKind, CommittedBatch, EdgeLookup, EntityLookup, PlannedBatch,
    RequestFingerprint, RunRegistration, Stage,
};

use crate::message::PipelineMessage;
use crate::phase::run_phase;

/// Receipt compatibility version. Bump when processing or recovery semantics change.
pub const SETTINGS_VERSION: &str = "56";

/// Statements one stale entity costs in a reconciliation batch
/// (version, freshness, ownership, live-set and history checks, and deletion).
const RECONCILE_ENTITY_STATEMENTS: usize = 6;
/// Statements one membership release costs (version and ownership checks, one release).
const RECONCILE_RELEASE_STATEMENTS: usize = 3;
/// Conservative per-interval allowance; shared timeline and owner guards are budgeted separately.
const RECONCILE_EDGE_STATEMENTS: usize = 4;
/// Re-reads of a collection after a rejected reconciliation commit.
const RECONCILE_MAX_REJECTIONS: u32 = 3;
/// Reconciliation batch indices are `ordinal * STRIDE + page`: every declared
/// collection's pages have their own deterministic identities, and a resumed
/// run recovers each collection's progress from its receipts.
pub const RECONCILE_INDEX_STRIDE: u32 = 1 << 16;

/// Configuration for the pipeline runner.
#[derive(Debug, Clone)]
pub struct PipelineRunnerConfig {
    /// How many snapshots per processing chunk; one node batch and one
    /// relationship batch commit per chunk.
    pub chunk_size: usize,
    /// Bounded channel capacity between stages.
    pub channel_capacity: usize,
    /// How many snapshots a single stage processes concurrently.
    pub stage_concurrency: usize,
    /// Statement budget of one reconciliation batch, bounded by the storage
    /// batch limit; smaller values make smaller sweep transactions.
    pub reconciliation_batch_statements: usize,
}

impl Default for PipelineRunnerConfig {
    fn default() -> Self {
        Self {
            chunk_size: 50,
            channel_capacity: 64,
            stage_concurrency: 10,
            reconciliation_batch_statements: MAX_STATEMENTS_PER_BATCH,
        }
    }
}

impl PipelineRunnerConfig {
    /// Every bound must be at least one; zero would stall or panic. The
    /// reconciliation budget must hold at least one deletion and stay within
    /// the storage limit.
    pub fn validate(&self) -> Result<(), PipelineError> {
        for (field, value) in [
            ("chunk_size", self.chunk_size),
            ("channel_capacity", self.channel_capacity),
            ("stage_concurrency", self.stage_concurrency),
        ] {
            if value == 0 {
                return Err(PipelineError::StateValidation {
                    stage: "config".into(),
                    message: format!("{field} must be at least 1"),
                });
            }
        }
        if self.channel_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(PipelineError::StateValidation {
                stage: "config".into(),
                message: "channel_capacity exceeds Tokio's semaphore limit".into(),
            });
        }
        if !(RECONCILE_ENTITY_STATEMENTS..=MAX_STATEMENTS_PER_BATCH)
            .contains(&self.reconciliation_batch_statements)
        {
            return Err(PipelineError::StateValidation {
                stage: "config".into(),
                message: format!(
                    "reconciliation_batch_statements must be between {RECONCILE_ENTITY_STATEMENTS} and {MAX_STATEMENTS_PER_BATCH}"
                ),
            });
        }
        Ok(())
    }
}

/// Awaited ingestion runner: every required write is committed before it returns.
///
/// ```text
/// register run header (fingerprint, frozen capture default, batch plan)
/// node pass, per chunk:
///   inputs →(ch)→ [node stage] →(ch)→ … → collector
///   chunk-wide entity index attached
///   PersistStage commits node batch #chunk (snapshots, versions, merges, deletions, evidence)
/// relationship pass, per chunk:
///   node results →(ch)→ [edge stage] →(ch)→ … → collector
///   PersistStage commits relationship batch #chunk
/// reconciliation, per declared collection, when nothing failed:
///   PersistStage commits reconciliation batches
/// ```
///
/// Each stage runs as an independent tokio task; bounded channels provide
/// backpressure. The relationship pass starts after every node batch is
/// committed, so relationship discovery reads every entity of the run from
/// the graph, whichever chunk delivered it.
///
/// Retrying with the same `run_id` and input replays committed batches from
/// their receipts and commits the rest. Committed batches stay committed
/// when a later batch fails; the error reports that progress.
pub struct PipelineRunner {
    /// Preparation stages; produce NodeExtraction when resolution stages are supplied.
    pub node_stages: Vec<Arc<dyn Stage>>,
    /// Graph-dependent stages rerun from frozen extraction after revision rejection.
    pub resolution_stages: Vec<Arc<dyn Stage>>,
    /// Relationship extraction stages; their outputs are frozen before graph-dependent resolution.
    pub edge_stages: Vec<Arc<dyn Stage>>,
    /// Relationship matching stages rerun after an explicitly rejected commit.
    pub relationship_resolution_stages: Vec<Arc<dyn Stage>>,
    /// Ordered batch planning, embedding and commit stages.
    pub flush_stages: Vec<Arc<dyn Stage>>,
    /// Awaited follow-up stages over a frozen, receipted target manifest.
    pub summary_stages: Vec<Arc<dyn Stage>>,
    /// Awaited incremental Saga summaries over receipted membership intervals.
    pub saga_summary_stages: Vec<Arc<dyn Stage>>,
    /// Community detection, summary and name embedding stages, each independently checkpointed.
    pub community_stages: Vec<Arc<dyn Stage>>,
    /// Runner configuration.
    pub config: PipelineRunnerConfig,
    relationship_coverage: bool,
}

impl PipelineRunner {
    /// Empty runner — add stages with the builder methods below.
    pub fn new() -> Self {
        Self {
            node_stages: Vec::new(),
            resolution_stages: Vec::new(),
            edge_stages: Vec::new(),
            relationship_resolution_stages: Vec::new(),
            flush_stages: Vec::new(),
            summary_stages: Vec::new(),
            saga_summary_stages: Vec::new(),
            community_stages: Vec::new(),
            config: PipelineRunnerConfig::default(),
            relationship_coverage: false,
        }
    }

    /// Replace the runner configuration (chunking/channels/concurrency).
    pub fn with_config(mut self, config: PipelineRunnerConfig) -> Self {
        self.config = config;
        self
    }

    /// Permit relationship absence sweeps only when this composition observes
    /// every relationship category promised by the input collection contract.
    /// Explicit entity deletion still closes incident relationships without this.
    pub fn relationship_coverage(mut self, complete: bool) -> Self {
        self.relationship_coverage = complete;
        self
    }

    /// Append a node pass stage; order is execution order.
    pub fn node_stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.node_stages.push(stage);
        self
    }

    /// Append graph-dependent processing. The preparation stages must produce NodeExtraction.
    pub fn resolution_stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.resolution_stages.push(stage);
        self
    }

    /// Append a relationship pass stage; order is execution order.
    pub fn edge_stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.edge_stages.push(stage);
        self
    }

    /// Append graph-dependent relationship processing over frozen EdgeExtraction outputs.
    pub fn relationship_resolution_stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.relationship_resolution_stages.push(stage);
        self
    }

    /// Append a batch stage; the complete chain must end in an acknowledged commit.
    pub fn flush(mut self, stage: Arc<dyn Stage>) -> Self {
        self.flush_stages.push(stage);
        self
    }

    /// Append an awaited summary follow-up stage, independently of ingestion stages.
    pub fn summary_stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.summary_stages.push(stage);
        self
    }

    /// Append one stage in the awaited Saga-summary follow-up chain.
    pub fn saga_summary_stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.saga_summary_stages.push(stage);
        self
    }

    /// Append a typed Community maintenance stage.
    pub fn community_stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.community_stages.push(stage);
        self
    }

    /// Reject incompatible stage ordering before registration or provider calls.
    pub fn validate_topology(&self) -> Result<(), PipelineError> {
        use kg_core::traits::StageKind;
        let invalid = |message: String| PipelineError::StateValidation {
            stage: "topology".into(),
            message,
        };
        if self.flush_stages.is_empty() {
            return Err(invalid("ingestion requires batch commit stages".into()));
        }
        if self.node_stages.is_empty() {
            return Err(invalid("ingestion requires at least one node stage".into()));
        }
        let mut names = HashSet::new();
        for stage in self
            .node_stages
            .iter()
            .chain(&self.resolution_stages)
            .chain(&self.edge_stages)
            .chain(&self.relationship_resolution_stages)
            .chain(&self.flush_stages)
        {
            if stage.name().trim().is_empty() || !names.insert(stage.name()) {
                return Err(invalid(format!(
                    "stage name must be nonblank and unique: {:?}",
                    stage.name()
                )));
            }
            if stage.contract().is_empty() {
                return Err(invalid(format!(
                    "stage {} has no handoff contract",
                    stage.name()
                )));
            }
        }
        use kg_core::traits::StageCapability;
        let mut capabilities = HashSet::new();
        let mut pending_references = false;
        for stage in &self.edge_stages {
            for capability in stage.capabilities() {
                if *capability == StageCapability::ReferenceResolution
                    && !capabilities.contains(&StageCapability::ReferenceExtraction)
                {
                    return Err(invalid(
                        "reference resolution requires earlier reference extraction".into(),
                    ));
                }
                match capability {
                    StageCapability::ReferenceExtraction => pending_references = true,
                    StageCapability::ReferenceResolution => pending_references = false,
                    _ => {}
                }
                if !capabilities.insert(*capability) {
                    return Err(invalid(format!("multiple stages own {capability:?}")));
                }
            }
        }
        if pending_references {
            return Err(invalid(
                "reference extraction requires subsequent reference resolution".into(),
            ));
        }
        if self.relationship_coverage {
            for required in [
                StageCapability::DeclaredRelationships,
                StageCapability::ReferenceExtraction,
                StageCapability::ReferenceResolution,
                StageCapability::TextRelationships,
            ] {
                if !capabilities.contains(&required) {
                    return Err(invalid(format!(
                        "relationship coverage requires {required:?}"
                    )));
                }
            }
        }
        let validate_path = |stages: &[Arc<dyn Stage>], start: StageKind, end: StageKind| {
            let mut possible = HashSet::from([start]);
            for stage in stages {
                let mut next = HashSet::new();
                for input in possible {
                    let outputs: Vec<_> = stage
                        .contract()
                        .iter()
                        .filter(|(accepted, _)| *accepted == input)
                        .map(|(_, output)| *output)
                        .collect();
                    if outputs.is_empty() {
                        return Err(invalid(format!(
                            "stage {} does not accept {:?}",
                            stage.name(),
                            input
                        )));
                    }
                    next.extend(outputs);
                }
                possible = next;
            }
            if possible != HashSet::from([end]) {
                return Err(invalid(format!(
                    "phase must finish with {end:?}; received {possible:?}"
                )));
            }
            Ok(())
        };
        let node_end = if self.resolution_stages.is_empty() {
            StageKind::NodeResolution
        } else {
            StageKind::NodeExtraction
        };
        validate_path(&self.node_stages, StageKind::Input, node_end)?;
        validate_path(&self.resolution_stages, node_end, StageKind::NodeResolution)?;
        if self.edge_stages.is_empty() {
            if self.relationship_coverage {
                return Err(invalid(
                    "relationship coverage requires relationship stages".into(),
                ));
            }
            if !self.relationship_resolution_stages.is_empty() {
                return Err(invalid(
                    "relationship resolution requires extraction stages".into(),
                ));
            }
        } else {
            let edge_end = if self.relationship_resolution_stages.is_empty() {
                StageKind::EdgeResolution
            } else {
                StageKind::EdgeExtraction
            };
            validate_path(&self.edge_stages, StageKind::NodeResolution, edge_end)?;
            validate_path(
                &self.relationship_resolution_stages,
                edge_end,
                StageKind::EdgeResolution,
            )?;
        }
        validate_path(
            &self.flush_stages,
            StageKind::FlushBatch,
            StageKind::Committed,
        )?;
        for (stages, input_kind) in [
            (&self.summary_stages, StageKind::SummaryBatch),
            (&self.saga_summary_stages, StageKind::SagaSummaryBatch),
        ] {
            if stages.is_empty() {
                continue;
            }
            validate_summary_chain(stages, input_kind)?;
        }
        if !self.community_stages.is_empty() {
            self.validate_community_chain()?;
        }
        Ok(())
    }

    /// Run the full pipeline with a freshly generated run id.
    ///
    /// Prefer [`Self::run_with_id`] when the caller has an idempotency key:
    /// only then can a retry replay committed batches instead of redoing them.
    pub async fn run(
        &self,
        snapshots: Vec<SnapshotInput>,
        ctx: Arc<RuntimeContext>,
    ) -> Result<PipelineOutput, PipelineError> {
        self.run_with_id(snapshots, ctx, Uuid::new_v4()).await
    }

    /// Run the pipeline under a caller-provided run identity. The same run id
    /// must always carry the same input and settings; a different fingerprint
    /// is rejected before any write.
    ///
    /// An empty request registers its identity but commits no data batches:
    /// without a declared collection, absence never means deletion.
    pub async fn run_with_id(
        &self,
        snapshots: Vec<SnapshotInput>,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        self.run_inputs_with_id(snapshots.into_iter().map(Into::into).collect(), ctx, run_id)
            .await
    }

    /// Await fresh observations and immutable snapshot associations under one replay identity.
    #[tracing::instrument(
        name = "pipeline.run",
        skip_all,
        fields(
            otel.status_code = tracing::field::Empty,
            run_id = %run_id,
            trace_id = %ctx.trace_id,
            snapshots = inputs.len()
        )
    )]
    pub async fn run_inputs_with_id(
        &self,
        inputs: Vec<IngestionInput>,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut measurement =
            kg_core::telemetry::OperationGuard::new(kg_core::telemetry::OperationKind::Run);
        let result: Result<PipelineOutput, PipelineError> = async {
            let start = Instant::now();
            let total_snapshots = inputs.len();
            if inputs.len() > kg_core::runtime::saga::MAX_OBSERVATIONS {
                return Err(recovery_error("request exceeds observation bound"));
            }
            let snapshots: Vec<SnapshotInput> = inputs
                .iter()
                .filter_map(|input| match input {
                    IngestionInput::Fresh(input) => Some(input.as_ref().clone()),
                    _ => None,
                })
                .collect();

            self.config.validate()?;
            self.validate_topology()?;
            ctx.saga_summary_settings.validate().map_err(|message| {
                PipelineError::StateValidation {
                    stage: "saga_summary".into(),
                    message,
                }
            })?;
            if ctx.saga_summary_settings.enabled && self.saga_summary_stages.is_empty() {
                return Err(PipelineError::StateValidation {
                    stage: "topology".into(),
                    message: "enabled Saga summaries require an awaited summary chain".into(),
                });
            }
            ctx.entity_summary_settings.validate().map_err(|message| {
                PipelineError::StateValidation {
                    stage: "summary".into(),
                    message,
                }
            })?;
            if ctx.entity_summary_settings.enabled && self.summary_stages.is_empty() {
                return Err(PipelineError::StateValidation {
                    stage: "topology".into(),
                    message: "enabled summaries require an awaited summary chain".into(),
                });
            }
            ctx.matching_settings
                .validate()
                .map_err(|_| PipelineError::StateValidation {
                    stage: "config".into(),
                    message: "invalid matching settings".into(),
                })?;
            check_cancel(&ctx)?;
            let flush = &self.flush_stages;
            if self.node_stages.is_empty() {
                return Err(PipelineError::StateValidation {
                    stage: "topology".into(),
                    message: "ingestion requires at least one node stage".into(),
                });
            }
            if inputs.iter().any(|input| input.saga().is_some()) {
                let capable = self.flush_stages.iter().any(|stage| {
                    stage
                        .capabilities()
                        .contains(&kg_core::traits::StageCapability::ThreadAssociation)
                        && stage.contract().contains(&(
                            kg_core::traits::StageKind::PlannedBatch,
                            kg_core::traits::StageKind::PlannedBatch,
                        ))
                });
                if !capable {
                    return Err(PipelineError::StateValidation {
                        stage: "topology".into(),
                        message: "Saga input requires the awaited association stage".into(),
                    });
                }
            }
            let validation_started = std::time::Instant::now();
            if inputs.is_empty() {
                validate_request(ctx.org_id.as_ref(), &snapshots).map_err(|error| {
                    PipelineError::StateValidation {
                        stage: "input_validation".into(),
                        message: error.to_string(),
                    }
                })?;
            }
            let validation = inputs.iter().enumerate().try_for_each(|(index, input)| {
                input.validate_request(&ctx.org_id).map_err(|mut error| {
                    error.field = format!("snapshots[{index}].{}", error.field);
                    error
                })
            });
            if let Err(error) = validation {
                tracing::warn!(stage = "input_validation", reason = %error.reason,
                elapsed_ms = validation_started.elapsed().as_millis() as u64,
                "request validation failed");
                return Err(PipelineError::StateValidation {
                    stage: "input_validation".into(),
                    message: error.to_string(),
                });
            }
            let accepts_relationships = self.edge_stages.iter().any(|stage| {
                stage
                    .capabilities()
                    .contains(&kg_core::traits::StageCapability::DeclaredRelationships)
            });
            for snapshot in &snapshots {
                if !snapshot.relationship_changes.is_empty() && !accepts_relationships {
                    return Err(PipelineError::StateValidation {
                        stage: "topology".into(),
                        message: "relationship commands require declared relationship extraction"
                            .into(),
                    });
                }
            }
            tracing::debug!(
                stage = "input_validation",
                snapshots = snapshots.len(),
                entities = snapshots.iter().map(|s| s.entities.len()).sum::<usize>(),
                elapsed_ms = validation_started.elapsed().as_millis() as u64,
                "request validation succeeded"
            );
            // Existing reconciliation authorization remains separate from generic input validation.
            validate_collection_scopes(&snapshots).map_err(|message| {
                PipelineError::StateValidation {
                    stage: "collection_scope".into(),
                    message,
                }
            })?;

            if declared_scopes(&snapshots).len() as u64 * u64::from(RECONCILE_INDEX_STRIDE)
                > u64::from(u32::MAX)
            {
                return Err(PipelineError::StateValidation {
                    stage: "request".into(),
                    message: format!(
                        "a request may declare at most {} collections",
                        u32::MAX / RECONCILE_INDEX_STRIDE
                    ),
                });
            }

            for input in &snapshots {
                kg_core::runtime::history::validate_input(&ctx.context_settings, input).map_err(
                    |message| PipelineError::StateValidation {
                        stage: "input_validation".into(),
                        message: message.into(),
                    },
                )?;
            }
            // The fingerprint covers the caller's input as given; capture
            // defaults are frozen in the header, not hashed.
            let fingerprint = RequestFingerprint::compute_inputs(
                ctx.org_id.as_ref(),
                &inputs,
                &self.settings(&ctx),
            )
            .map_err(|e| PipelineError::Other(format!("request fingerprint: {e}")))?;
            let header = crate::schema_admission::header(
                &ctx,
                &inputs,
                run_id,
                fingerprint.clone(),
                self.batch_plan(&snapshots),
                SETTINGS_VERSION,
                self.config.chunk_size,
            )
            .await?;
            let registration = register_run(&ctx, ctx.graph.register_run(&header)).await?;
            let (capture_default, resumed, manifest, observation_manifest) = match registration {
                RunRegistration::Registered => (
                    header.capture_default,
                    Vec::new(),
                    header.schema_manifest.clone(),
                    header.observation_manifest.clone(),
                ),
                RunRegistration::Resumed {
                    observation_manifest,
                    schema_manifest,
                    capture_default,
                    committed,
                } => {
                    tracing::info!(
                        run_id = %run_id,
                        committed_batches = committed.len(),
                        "resuming run; committed batches will replay from receipts"
                    );
                    (
                        capture_default,
                        committed,
                        schema_manifest,
                        observation_manifest,
                    )
                }
            };
            manifest
                .validate_inputs(&ctx.org_id, &snapshots)
                .map_err(|message| PipelineError::StateValidation {
                    stage: "schema_admission".into(),
                    message,
                })?;
            let registered_profiles = manifest
                .profiles
                .iter()
                .map(|(source, profile)| (source.clone(), profile.document.reference()))
                .collect::<kg_core::profiles::ProfileBindings>();
            if registered_profiles != ctx.profile_bindings {
                return Err(recovery_error(
                    "registered profile bindings differ from request",
                ));
            }
            observation_manifest
                .validate_inputs(&inputs, self.config.chunk_size)
                .map_err(recovery_error)?;
            let mut scoped = ctx.with_run_schemas(manifest);
            scoped.observation_manifest = Some(Arc::new(observation_manifest));
            scoped.original_inputs = Arc::new(inputs);
            let ctx = Arc::new(scoped);
            drop(snapshots);

            tracing::info!(
                run_id = %run_id,
                snapshots = total_snapshots,
                node_stages = self.node_stages.len(),
                resolution_stages = self.resolution_stages.len(),
                edge_stages = self.edge_stages.len(),
                relationship_resolution_stages = self.relationship_resolution_stages.len(),
                chunk_size = self.config.chunk_size,
                channel_capacity = self.config.channel_capacity,
                stage_concurrency = self.config.stage_concurrency,
                "pipeline starting"
            );

            let mut progress = Progress::default();
            let result = self
                .execute(
                    &ctx.original_inputs,
                    &ctx,
                    run_id,
                    &fingerprint,
                    flush,
                    &resumed,
                    capture_default,
                    &mut progress,
                )
                .await;

            let duration_ms = start.elapsed().as_millis() as u64;
            if let Err(cause) = result {
                tracing::error!(
                    run_id = %run_id,
                    failure_kind = failure_kind(&cause),
                    retriable = cause.is_retriable(),
                    batches_committed = progress.batches.len(),
                    commit_unknown = progress.commit_unknown,
                    duration_ms,
                    "pipeline aborted"
                );
                let error = PipelineError::Aborted {
                    run_id,
                    committed: Box::new(progress.committed),
                    batches_committed: progress.batches.len(),
                    commit_unknown: progress.commit_unknown,
                    cause: Box::new(cause),
                };
                return Err(error);
            }

            let snapshots_completed = PipelineOutput::count_completed(
                total_snapshots,
                &progress.failed,
                &progress.incomplete,
            );
            let output = PipelineOutput {
                profile_diagnostics: progress.profile_diagnostics,
                run_id,
                committed: progress.committed,
                newly_committed: progress.newly_committed,
                batches: progress.batches,
                snapshots_total: total_snapshots,
                snapshots_completed,
                failed_snapshots: progress.failed,
                incomplete_extractions: progress.incomplete,
                incomplete_followups: progress.incomplete_followups,
                skipped_summaries: progress.skipped_summaries,
                relationship_declines: progress.relationship_declines,
                reference_decisions: progress.reference_decisions,
                collections: progress.collections,
                duration_ms,
            };

            tracing::info!(
                run_id = %run_id,
                snapshots = total_snapshots,
                completed = output.snapshots_completed,
                failed = output.failed_snapshots.len(),
                incomplete = output.incomplete_extractions.len(),
                complete = output.is_complete(),
                duration_ms,
                batches = output.batches.len(),
                replayed = output.replayed_batches(),
                entities_created = output.committed.entities_created,
                entities_updated = output.committed.entities_updated,
                entities_deleted = output.committed.entities_deleted,
                memberships_released = output.committed.memberships_released,
                edges_created = output.committed.edges_created,
                "pipeline finished"
            );

            Ok(output)
        }
        .await;
        let outcome = match &result {
            Ok(output) if !output.is_complete() => kg_core::telemetry::Outcome::Failed,
            Ok(output)
                if !output.batches.is_empty()
                    && output.replayed_batches() == output.batches.len() =>
            {
                kg_core::telemetry::Outcome::Replayed
            }
            Ok(_) => kg_core::telemetry::Outcome::Success,
            Err(error) => kg_core::telemetry::Outcome::from(error),
        };
        measurement.finish(outcome);
        if matches!(outcome, kg_core::telemetry::Outcome::Failed) || result.is_err() {
            tracing::Span::current().record("otel.status_code", "ERROR");
        }
        result
    }

    /// Effective processing settings hashed into the request fingerprint.
    fn settings(&self, ctx: &RuntimeContext) -> serde_json::Value {
        let entity_type_configs: BTreeMap<&String, &kg_core::entity_type_config::EntityTypeConfig> =
            ctx.entity_type_configs.iter().collect();
        let stage_settings = |stage: &Arc<dyn Stage>| {
            serde_json::json!({
                "name": stage.name(), "contract": stage.contract(), "batch": stage.is_batch(),
                "capabilities": stage.capabilities(),
                "processing_version": stage.processing_version(),
            })
        };
        let mut descriptor = serde_json::json!({
            "settings_version": SETTINGS_VERSION,
            "stages": {
                "nodes": self.node_stages.iter().map(stage_settings).collect::<Vec<_>>(),
                "resolution": self.resolution_stages.iter().map(stage_settings).collect::<Vec<_>>(),
                "relationships": self.edge_stages.iter().map(stage_settings).collect::<Vec<_>>(),
                "relationship_resolution": self.relationship_resolution_stages.iter().map(stage_settings).collect::<Vec<_>>(),
                "flush": self.flush_stages.iter().map(stage_settings).collect::<Vec<_>>(),
                "summary": self.summary_stages.iter().map(stage_settings).collect::<Vec<_>>(),
                "saga_summary": self.saga_summary_stages.iter().map(stage_settings).collect::<Vec<_>>(),
                "community": self.community_stages.iter().map(stage_settings).collect::<Vec<_>>(),
            },
            "relationship_coverage": self.relationship_coverage,
            "chunk_size": self.config.chunk_size,
            "reconciliation_batch_statements": self.config.reconciliation_batch_statements,
            "embedding": ctx.embedding,
            "context": ctx.context_settings,
            "summary": ctx.entity_summary_settings,
            "saga_summary": ctx.saga_summary_settings,
            "extraction": ctx.extraction_settings,
            "learned_rules": &*ctx.rule_freezes,
            "matching": ctx.matching_settings,
            "typed_decisions": ctx.typed_decisions,
            "reference_resolution": ctx.reference_resolution_settings,
            "community": ctx.community_settings,
            "models": {
                "extraction": ctx.llm_extraction.processing_descriptor(),
                "disambiguation": ctx.llm_disambiguation.processing_descriptor(),
                "edge_discovery": ctx.llm_edge_discovery.processing_descriptor(),
                "default": ctx.llm_default.processing_descriptor(),
                "decisions": ctx.decisions.as_ref().map(|d| d.processing_descriptor()),
            },
            "embedding_provider": ctx.embedder.processing_descriptor(),
            "relationship_embedding_text_version": kg_core::embedding::RELATIONSHIP_TEXT_VERSION,
            "exec": &*ctx.exec_config,
            "policy": ctx.policy.engine_default(),
            "tenant_policy": ctx.policy.tenant(),
            "namespace_policy": &*ctx.namespace_policy,
            "entity_type_configs": entity_type_configs,
        });
        if !ctx.profile_bindings.is_empty() {
            // Version opt-in enforcement independently; legacy run fingerprints remain unchanged.
            descriptor["profiles"] =
                serde_json::json!({"processing_version":1,"bindings":ctx.profile_bindings});
        }
        descriptor
    }

    /// Node and relationship batches per chunk, then the first reconciliation
    /// batch of every declared collection; reconciliation adds pages when a
    /// collection exceeds the statement budget.
    fn batch_plan(&self, snapshots: &[SnapshotInput]) -> Vec<PlannedBatch> {
        let mut plan = Vec::new();
        for (index, chunk) in snapshots.chunks(self.config.chunk_size).enumerate() {
            for kind in [BatchKind::Node, BatchKind::Relationship] {
                plan.push(PlannedBatch {
                    kind,
                    index: index as u32,
                    items: chunk.len() as u32,
                });
            }
        }
        for (ordinal, _) in declared_scopes(snapshots).iter().enumerate() {
            plan.push(PlannedBatch {
                kind: BatchKind::Reconciliation,
                index: ordinal as u32 * RECONCILE_INDEX_STRIDE,
                items: 1,
            });
        }
        if !self.summary_stages.is_empty() {
            plan.push(PlannedBatch {
                kind: BatchKind::Summary,
                index: 0,
                items: 0,
            });
        }
        if !self.community_stages.is_empty() {
            plan.push(PlannedBatch {
                kind: BatchKind::Community,
                index: 0,
                items: 0,
            });
        }
        if !self.saga_summary_stages.is_empty() {
            plan.push(PlannedBatch {
                kind: BatchKind::SagaSummary,
                index: 0,
                items: 0,
            });
        }
        plan
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        snapshots: &[IngestionInput],
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        flush: &[Arc<dyn Stage>],
        resumed: &[CommittedBatch],
        summary_as_of: chrono::DateTime<chrono::Utc>,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        for receipt in resumed {
            let commit = restore_commit(run_id, receipt)?;
            progress
                .record(&commit)
                .map_err(|error| recovery_error(error.to_string()))?;
            if let Some(recovery) = &commit.recovery {
                progress.failed.extend(recovery.failures.clone());
            }
        }
        if resumed
            .iter()
            .any(|receipt| receipt.kind == BatchKind::Summary)
        {
            let manifest = progress
                .summary_manifest
                .as_ref()
                .ok_or_else(|| recovery_error("summary receipts have no frozen manifest"))?;
            manifest.validate().map_err(recovery_error)?;
            if manifest.as_of != summary_as_of
                || manifest.batch_size != ctx.entity_summary_settings.batch_size
            {
                return Err(recovery_error(
                    "summary manifest disagrees with registered settings",
                ));
            }
        }
        if resumed
            .iter()
            .any(|receipt| receipt.kind == BatchKind::SagaSummary)
        {
            let manifest = progress
                .saga_summary_manifest
                .as_ref()
                .ok_or_else(|| recovery_error("Saga summary receipts lack a manifest"))?;
            manifest.validate().map_err(recovery_error)?;
            if manifest.page_size != ctx.saga_summary_settings.page_size {
                return Err(recovery_error(
                    "Saga manifest disagrees with registered page size",
                ));
            }
        }
        if progress.summary_manifest.is_some()
            || progress.saga_summary_manifest.is_some()
            || progress.community_checkpoints.contains_key(&0)
        {
            let observations = ctx
                .observation_manifest
                .as_ref()
                .ok_or_else(|| recovery_error("follow-up receipt lacks observation manifest"))?;
            for index in 0..observations.node_batches.len() {
                for kind in [BatchKind::Node, BatchKind::Relationship] {
                    if !resumed
                        .iter()
                        .any(|receipt| receipt.kind == kind && receipt.index as usize == index)
                    {
                        return Err(recovery_error(
                            "follow-up manifest precedes required ingestion receipt",
                        ));
                    }
                }
            }
        }
        let mut observed = ObservedSet::default();
        let manifest = ctx
            .observation_manifest
            .as_ref()
            .ok_or_else(|| recovery_error("missing accepted observation manifest"))?;
        let fresh: Vec<_> = snapshots
            .iter()
            .filter_map(|input| match input {
                IngestionInput::Fresh(input) => {
                    let mut snapshot = input.as_ref().clone();
                    snapshot.captured_at = Some(snapshot.captured_at.unwrap_or(summary_as_of));
                    Some(snapshot)
                }
                _ => None,
            })
            .collect();
        let scopes = declared_scopes(&fresh);
        drop(fresh);

        // Every commit of the run proves it still owns the declared scans.
        let scans: Vec<CollectionScan> = scopes.iter().map(|s| s.scan.clone()).collect();

        // Node pass: every chunk's versions commit before relationships are
        // discovered, so later lookups see earlier chunks in the graph.
        let mut node_outputs: Vec<Vec<PipelineMessage>> = Vec::new();
        for (chunk_index, chunk) in manifest.node_batches.iter().enumerate() {
            check_cancel(ctx)?;
            let resumed_page = if resumed
                .iter()
                .any(|r| r.kind == BatchKind::Node && r.index == chunk_index as u32)
            {
                None
            } else {
                ctx.graph
                    .resume_commit(
                        &ctx.org_id,
                        BatchIdentity {
                            run_id,
                            kind: BatchKind::Node,
                            index: chunk_index as u32,
                        },
                        fingerprint,
                        &ctx.cancel,
                    )
                    .await
                    .map_err(|error| {
                        let uncertain =
                            matches!(&error, kg_core::errors::BackendError::UnknownCommit(_));
                        progress.commit_unknown |= uncertain;
                        if ctx.cancel.is_cancelled() && !uncertain {
                            PipelineError::Cancelled
                        } else {
                            PipelineError::StepExecution {
                                stage: "persist".into(),
                                step: "resume_pages".into(),
                                cause: error.to_string(),
                                retriable: error.is_transient(),
                            }
                        }
                    })?
            };
            if let Some(receipt) = &resumed_page {
                let commit = restore_commit(run_id, receipt)?;
                progress
                    .record(&commit)
                    .map_err(|e| recovery_error(e.to_string()))?;
                // A resumed page carries the frozen batch's recorded failures.
                // Restore them so a partial run cannot report success and sweep
                // what the failed snapshots never observed (mirrors the
                // registration path above).
                if let Some(recovery) = &commit.recovery {
                    progress.failed.extend(recovery.failures.clone());
                }
            }
            if let Some(receipt) = resumed_page.as_ref().or_else(|| {
                resumed
                    .iter()
                    .find(|r| r.kind == BatchKind::Node && r.index == chunk_index as u32)
            }) {
                let recovery = restore_commit(run_id, receipt)?
                    .recovery
                    .ok_or_else(|| recovery_error("node receipt has no recovery data"))?;
                let outputs = restore_nodes(recovery, snapshots, run_id, ctx, chunk_index)?;
                progress.incomplete.extend(collect_incomplete(&outputs));
                observed.record_nodes(&outputs);
                node_outputs.push(outputs);
                continue;
            }
            let messages: Vec<PipelineMessage> = chunk
                .iter()
                .map(|index| {
                    let state = match &snapshots[*index] {
                        IngestionInput::Fresh(snapshot) => StageOutput::Input(snapshot.clone()),
                        IngestionInput::Existing(_) => {
                            let entry = &manifest.entries[*index];
                            let kg_core::runtime::saga::FrozenObservationKind::Existing {
                                evidence_digest,
                            } = &entry.kind
                            else {
                                return Err(recovery_error("manifest kind mismatch"));
                            };
                            StageOutput::ReusedSnapshot(
                                kg_core::runtime::stage_output::ReusedSnapshotOutput {
                                    snapshot_index: *index,
                                    snapshot_uuid: entry.snapshot_uuid,
                                    namespace: entry.namespace.clone(),
                                    evidence_digest: evidence_digest.clone(),
                                },
                            )
                        }
                    };
                    Ok(PipelineMessage {
                        snapshot_index: *index,
                        run_id,
                        state,
                    })
                })
                .collect::<Result<Vec<_>, PipelineError>>()?;
            // Instrument futures so concurrent runs retain separate span context.
            let span = tracing::info_span!(
                "pipeline.chunk",
                run_id = %run_id,
                chunk = chunk_index,
                pass = "node"
            );
            let outputs = self
                .node_chunk(
                    messages,
                    chunk_index,
                    snapshots,
                    ctx,
                    run_id,
                    fingerprint,
                    flush,
                    &scans,
                    progress,
                )
                .instrument(span)
                .await?;
            progress.incomplete.extend(collect_incomplete(&outputs));
            observed.record_nodes(&outputs);
            node_outputs.push(outputs);
        }

        // Relationship pass. Every chunk sees the run-wide entity index, so a
        // reference to an entity delivered by another chunk resolves without
        // depending on input order; the graph answers cross-run references.
        let needs_targets = (!self.edge_stages.is_empty()
            || !self.relationship_resolution_stages.is_empty())
            && (0..node_outputs.len()).any(|index| {
                !resumed.iter().any(|receipt| {
                    receipt.kind == BatchKind::Relationship && receipt.index == index as u32
                })
            });
        let relationship_chunk_count = node_outputs.len();
        let mut changed_target_chains = HashSet::new();
        let mut changed_target_namespaces = BTreeSet::new();
        let mut observed_source_chains = HashSet::new();
        let mut changed_target_tokens = HashSet::new();
        let mut repair_effective_at = None;
        for message in node_outputs.iter().flatten() {
            if let StageOutput::NodeResolution(resolution) = &message.state {
                observed_source_chains.extend(resolution.observed_chain_ids());
                for entity in resolution.live_entities().iter() {
                    repair_effective_at = repair_effective_at.max(entity.last_seen_at);
                    changed_target_chains.insert(entity.chain_id);
                    changed_target_namespaces.insert(entity.namespace.clone());
                    changed_target_tokens.extend(
                        kg_core::runtime::stage_output::RelationshipTarget::from_node(entity)
                            .key_value_tokens(),
                    );
                }
                // Losing a candidate can resolve a previously ambiguous slot.
                // Wake tokens removed by version changes, not only new values.
                for change in resolution
                    .nodes_new_version
                    .iter()
                    .chain(resolution.nodes_volatile.iter())
                {
                    for property in &change.changes {
                        if let Some(value) = property.old_value.clone().and_then(|value| {
                            serde_json::from_value::<kg_core::models::PropertyValue>(value).ok()
                        }) {
                            if let Some(text) = value.as_identity_key() {
                                changed_target_tokens.insert(format!(
                                    "{}:{}",
                                    kg_core::traits::property_codec::type_tag(&value),
                                    text
                                ));
                            }
                        }
                    }
                }
                changed_target_chains.extend(
                    resolution
                        .nodes_deleted
                        .iter()
                        .map(|entity| entity.chain_id),
                );
                for entity in resolution.nodes_deleted.iter() {
                    changed_target_namespaces.insert(entity.namespace.clone());
                    changed_target_tokens.extend(
                        kg_core::runtime::stage_output::RelationshipTarget::from_node(entity)
                            .key_value_tokens(),
                    );
                    repair_effective_at = repair_effective_at.max(
                        entity
                            .deleted_at
                            .or(entity.last_seen_at)
                            .or(Some(entity.valid_from)),
                    );
                }
                for merge in resolution.chains_merged.iter() {
                    let prior = reference_read(
                        ctx,
                        "merged_target_keys",
                        ctx.graph.find_entities(
                            &ctx.org_id,
                            &EntityLookup::VersionsByChain {
                                chain_ids: vec![merge.loser_chain_id, merge.winner_chain_id],
                            },
                        ),
                    )
                    .await?;
                    for record in prior {
                        changed_target_tokens.extend(
                            kg_core::runtime::stage_output::RelationshipTarget::from(record)
                                .key_value_tokens(),
                        );
                    }
                    changed_target_chains.extend([merge.loser_chain_id, merge.winner_chain_id]);
                    repair_effective_at = repair_effective_at.max(Some(merge.effective_at));
                }
            }
        }
        let node_outputs = if needs_targets {
            attach_run_entities(node_outputs, ctx).await?
        } else {
            node_outputs
        };
        for (chunk_index, outputs) in node_outputs.into_iter().enumerate() {
            check_cancel(ctx)?;
            let resumed_page = if resumed
                .iter()
                .any(|r| r.kind == BatchKind::Relationship && r.index == chunk_index as u32)
            {
                None
            } else {
                ctx.graph
                    .resume_commit(
                        &ctx.org_id,
                        BatchIdentity {
                            run_id,
                            kind: BatchKind::Relationship,
                            index: chunk_index as u32,
                        },
                        fingerprint,
                        &ctx.cancel,
                    )
                    .await
                    .map_err(|error| {
                        let uncertain =
                            matches!(&error, kg_core::errors::BackendError::UnknownCommit(_));
                        progress.commit_unknown |= uncertain;
                        if ctx.cancel.is_cancelled() && !uncertain {
                            PipelineError::Cancelled
                        } else {
                            PipelineError::StepExecution {
                                stage: "persist".into(),
                                step: "resume_pages".into(),
                                cause: error.to_string(),
                                retriable: error.is_transient(),
                            }
                        }
                    })?
            };
            if let Some(receipt) = &resumed_page {
                let commit = restore_commit(run_id, receipt)?;
                progress
                    .record(&commit)
                    .map_err(|e| recovery_error(e.to_string()))?;
                // Restore the resumed relationship page's recorded failures too,
                // so the sweep stays suppressed after a partial run.
                if let Some(recovery) = &commit.recovery {
                    progress.failed.extend(recovery.failures.clone());
                }
            }
            if let Some(receipt) = resumed_page.as_ref().or_else(|| {
                resumed
                    .iter()
                    .find(|r| r.kind == BatchKind::Relationship && r.index == chunk_index as u32)
            }) {
                let recovery = restore_commit(run_id, receipt)?
                    .recovery
                    .ok_or_else(|| recovery_error("relationship receipt has no recovery data"))?;
                observed
                    .relationship_chains
                    .extend(recovery.observed_relationships);
                continue;
            }
            let mut outputs: Vec<_> = outputs
                .into_iter()
                .filter(|message| !matches!(message.state, StageOutput::ReusedSnapshot(_)))
                .collect();
            for output in &mut outputs {
                if let StageOutput::NodeResolution(resolution) = &mut output.state {
                    kg_core::runtime::history::hydrate(ctx, resolution)
                        .await
                        .map_err(|e| match e {
                            StageError::Cancelled { .. } => PipelineError::Cancelled,
                            StageError::StateValidation { stage, message } => {
                                PipelineError::StateValidation { stage, message }
                            }
                            StageError::StepFailed {
                                stage,
                                step,
                                cause,
                                retriable,
                            } => PipelineError::StepExecution {
                                stage,
                                step,
                                cause,
                                retriable,
                            },
                            other => PipelineError::StageExecution {
                                stage: "context_retrieval".into(),
                                error_count: 1,
                                errors: vec![other],
                            },
                        })?;
                }
            }
            let span = tracing::info_span!(
                "pipeline.chunk",
                run_id = %run_id,
                chunk = chunk_index,
                pass = "relationship"
            );
            let relationship_chains = self
                .relationship_chunk(
                    outputs,
                    chunk_index,
                    ctx,
                    run_id,
                    fingerprint,
                    flush,
                    &scans,
                    progress,
                    &self.edge_stages,
                    &[],
                )
                .instrument(span)
                .await?;
            observed.relationship_chains.extend(relationship_chains);
        }

        // Continue-mode failures do not suppress repair of already committed
        // independent targets. Fail-fast exits before reaching this boundary.
        if !self.edge_stages.is_empty() {
            let repaired = self
                .repair_references_for_targets(
                    changed_target_tokens,
                    changed_target_chains,
                    changed_target_namespaces,
                    observed_source_chains,
                    repair_effective_at.unwrap_or_else(Utc::now),
                    relationship_chunk_count,
                    resumed,
                    ctx,
                    run_id,
                    fingerprint,
                    flush,
                    &scans,
                    progress,
                )
                .await?;
            observed.relationship_chains.extend(repaired);
        }

        // Reconciliation only after a run in which nothing failed: a partial
        // run must never delete what it failed to observe.
        if let Some(manifest) = &progress.summary_manifest {
            // The durable manifest is the ingestion/reconciliation completion barrier.
            progress.collections = manifest.collections.clone();
        } else if let Some(manifest) = &progress.saga_summary_manifest {
            progress.collections = manifest.collections.clone();
        } else if let Some(kg_core::runtime::community::CommunityCheckpoint {
            step: kg_core::runtime::community::CommunityCheckpointStep::Targets(manifest),
            ..
        }) = progress.community_checkpoints.get(&0)
        {
            progress.collections = manifest.collections.clone();
        } else if progress.failed.is_empty() {
            self.reconcile(
                &scopes,
                ctx,
                run_id,
                fingerprint,
                flush,
                &scans,
                &observed,
                resumed,
                progress,
            )
            .await?;
        } else if !scopes.is_empty() {
            tracing::warn!(
                run_id = %run_id,
                failed = progress.failed.len(),
                "reconciliation skipped: snapshots failed this run"
            );
            for scope in &scopes {
                progress.collections.push(CollectionOutcome {
                    swept: false,
                    reason: Some("snapshots failed this run".into()),
                    ..scope.outcome()
                });
            }
        }
        check_cancel(ctx)?;
        if ctx.entity_summary_settings.enabled {
            self.summarize(
                ctx,
                run_id,
                fingerprint,
                &scans,
                summary_as_of,
                resumed,
                progress,
            )
            .await?;
        }
        if ctx.saga_summary_settings.enabled {
            self.summarize_sagas(ctx, run_id, fingerprint, &scans, resumed, progress)
                .await?;
        }
        if ctx.community_settings.incremental_enabled {
            self.update_communities(ctx, run_id, fingerprint, summary_as_of, progress)
                .await?;
        }
        Ok(())
    }

    /// One chunk of the node pass: stages, checkpoint, commit, and the
    /// outputs the relationship pass continues from (restored from the
    /// receipt when the batch had already committed).
    #[allow(clippy::too_many_arguments)]
    async fn node_chunk(
        &self,
        messages: Vec<PipelineMessage>,
        chunk_index: usize,
        snapshots: &[IngestionInput],
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        flush: &[Arc<dyn Stage>],
        scans: &[CollectionScan],
        progress: &mut Progress,
    ) -> Result<Vec<PipelineMessage>, PipelineError> {
        let failed_before = progress.failed.len();
        let (reused, fresh): (Vec<_>, Vec<_>) = messages
            .into_iter()
            .partition(|message| matches!(message.state, StageOutput::ReusedSnapshot(_)));
        let prepared = self
            .run_stage_phase("node_pipeline", &self.node_stages, fresh, ctx, progress)
            .await?;
        if !self.resolution_stages.is_empty()
            && prepared
                .iter()
                .any(|m| !matches!(m.state, StageOutput::NodeExtraction(_)))
        {
            return Err(PipelineError::StateValidation {
                stage: "identity_resolution".into(),
                message: "resolution stages require frozen NodeExtraction outputs".into(),
            });
        }
        let identity = BatchIdentity {
            run_id,
            kind: BatchKind::Node,
            index: chunk_index as u32,
        };
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(ctx.matching_settings.replan_timeout_ms);
        let extraction_failures = progress.failed.len();
        let incoming_embeddings =
            Arc::new(kg_core::runtime::embedding_cache::IncomingEmbeddingCache::default());
        for attempt in 0..=ctx.matching_settings.max_replans {
            check_cancel(ctx)?;
            if tokio::time::Instant::now() >= deadline {
                return Err(identity_budget_error());
            }
            let mut attempt_context = ctx.as_ref().clone();
            attempt_context.incoming_embeddings = incoming_embeddings.clone();
            attempt_context.cancel = ctx.cancel.child_token();
            attempt_context.identity_deadline = Some(deadline);
            let attempt_context = Arc::new(attempt_context);
            let mut local = Progress::default();
            let outputs = {
                let phase = self.run_stage_phase(
                    "identity_resolution",
                    &self.resolution_stages,
                    prepared.clone(),
                    &attempt_context,
                    &mut local,
                );
                tokio::pin!(phase);
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => {
                        attempt_context.cancel.cancel();
                        let _ = phase.await; // joins every worker before returning
                        return Err(identity_budget_error());
                    },
                    result = &mut phase => result,
                }
            };
            let mut outputs = match outputs {
                Ok(outputs) => outputs,
                Err(PipelineError::IdentityRevisionChanged)
                    if !self.resolution_stages.is_empty() =>
                {
                    wait_resolution_replan(ctx, identity, attempt, deadline).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(identity_budget_error());
            }
            outputs.extend(reused.clone());
            outputs.sort_by_key(|message| message.snapshot_index);
            let mut failures = progress.failed[failed_before..extraction_failures].to_vec();
            failures.extend(local.failed.clone());
            let recovery = checkpoint_nodes(
                &outputs,
                failures,
                &ctx.extraction_settings,
                ctx.run_schemas.as_deref(),
            )?;
            let batch = match merge_node_batch(&outputs) {
                Ok(batch) => batch,
                Err(PipelineError::IdentityRevisionChanged) => {
                    wait_resolution_replan(ctx, identity, attempt, deadline).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let mut commit_context = ctx.as_ref().clone();
            commit_context.identity_deadline = Some(deadline);
            let commit_context = Arc::new(commit_context);
            // Do not timeout this future: persistence limits planning itself and
            // must await any transaction it has already submitted.
            match self
                .commit_raw(
                    flush,
                    &commit_context,
                    identity,
                    fingerprint,
                    scans,
                    FlushWork::Nodes(batch),
                    Some(recovery),
                    progress,
                )
                .await
            {
                Ok(commit) => {
                    if commit.replayed {
                        let recovery = commit.recovery.ok_or_else(|| {
                            recovery_error("replayed node batch has no recovery data")
                        })?;
                        progress.failed.truncate(failed_before);
                        progress.failed.extend(recovery.failures.clone());
                        return restore_nodes(recovery, snapshots, run_id, ctx, chunk_index);
                    }
                    progress.failed.extend(local.failed);
                    return Ok(outputs);
                }
                Err(StageError::IdentityRevisionChanged | StageError::CommitRejected { .. }) => {
                    wait_resolution_replan(ctx, identity, attempt, deadline).await?;
                }
                Err(error) => return Err(commit_failure(identity, error, progress)),
            }
        }
        Err(identity_budget_error())
    }

    #[allow(clippy::too_many_arguments)]
    async fn repair_references_for_targets(
        &self,
        tokens: HashSet<String>,
        target_chains: HashSet<Uuid>,
        target_namespaces: BTreeSet<String>,
        mut current_sources: HashSet<Uuid>,
        effective_at: DateTime<Utc>,
        ordinary_chunk_count: usize,
        resumed: &[CommittedBatch],
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        flush: &[Arc<dyn Stage>],
        scans: &[CollectionScan],
        progress: &mut Progress,
    ) -> Result<Vec<Uuid>, PipelineError> {
        use kg_core::traits::graph_reads::MAX_LOOKUP_KEYS;

        // Reconciliation starts only after reference repair has finished. A
        // receipt from that phase is the durable boundary: graph reads made on
        // replay can now see the repair's own new edges and must not schedule a
        // second wave that the original completed run never planned.
        if resumed
            .iter()
            .any(|receipt| receipt.kind == BatchKind::Reconciliation)
        {
            let mut repaired = Vec::new();
            for receipt in resumed.iter().filter(|receipt| {
                receipt.kind == BatchKind::Relationship
                    && receipt.index as usize > ordinary_chunk_count
            }) {
                let recovery = restore_commit(run_id, receipt)?.recovery.ok_or_else(|| {
                    recovery_error("reference repair receipt has no recovery data")
                })?;
                kg_core::telemetry::reference_activity(
                    kg_core::telemetry::ReferenceActivity::RepairReplayed,
                    recovery.reference_repair_sources.len(),
                );
                repaired.extend(recovery.observed_relationships);
            }
            return Ok(repaired);
        }
        if tokens.is_empty() && target_chains.is_empty() {
            return Ok(Vec::new());
        }
        let mut repaired = Vec::new();
        let mut next_index = ordinary_chunk_count
            .checked_add(1)
            .ok_or_else(|| recovery_error("reference repair batch index overflow"))?;
        for receipt in resumed.iter().filter(|receipt| {
            receipt.kind == BatchKind::Relationship && receipt.index as usize > ordinary_chunk_count
        }) {
            let recovery = restore_commit(run_id, receipt)?
                .recovery
                .ok_or_else(|| recovery_error("reference repair receipt has no recovery data"))?;
            if recovery.reference_repair_sources.is_empty() {
                return Err(recovery_error(
                    "reference repair receipt lacks its source manifest",
                ));
            }
            kg_core::telemetry::reference_activity(
                kg_core::telemetry::ReferenceActivity::RepairReplayed,
                recovery.reference_repair_sources.len(),
            );
            current_sources.extend(recovery.reference_repair_sources);
            repaired.extend(recovery.observed_relationships);
            next_index = next_index.max(receipt.index as usize + 1);
        }
        let mut waiting = BTreeMap::<
            Uuid,
            (
                Option<Uuid>,
                DateTime<Utc>,
                BTreeSet<ReferenceOwnerSelector>,
            ),
        >::new();
        let mut tokens: Vec<_> = tokens.into_iter().collect();
        tokens.sort();
        for page in tokens.chunks(MAX_LOOKUP_KEYS) {
            let mut after = None;
            let mut cursors = ReferenceCursorProgress::default();
            loop {
                check_cancel(ctx)?;
                let found = reference_read(
                    ctx,
                    "unresolved_dependencies",
                    ctx.graph.unresolved_references(
                        ctx.org_id.as_ref(),
                        &kg_core::traits::UnresolvedReferenceQuery {
                            tokens: page.to_vec(),
                            after: after.clone(),
                            limit: MAX_LOOKUP_KEYS,
                        },
                    ),
                )
                .await?;
                for record in found.records {
                    if !current_sources.contains(&record.source_chain_id) {
                        waiting
                            .entry(record.source_chain_id)
                            .and_modify(|current| {
                                let recorded_at = record.entry.recorded_at.max(effective_at);
                                if recorded_at > current.1 {
                                    current.0 = record.entry.snapshot_id;
                                    current.1 = recorded_at;
                                }
                            })
                            .or_insert((
                                record.entry.snapshot_id,
                                record.entry.recorded_at.max(effective_at),
                                BTreeSet::new(),
                            ));
                    }
                }
                validate_reference_work(&waiting)?;
                let Some(next) = found.next_after else {
                    break;
                };
                cursors.advance(&next)?;
                after = Some(next);
            }

            let mut after = None;
            let mut cursors = ReferenceCursorProgress::default();
            loop {
                check_cancel(ctx)?;
                let found = reference_read(
                    ctx,
                    "confirmed_token_dependencies",
                    ctx.graph.confirmed_reference_dependencies(
                        ctx.org_id.as_ref(),
                        &kg_core::traits::UnresolvedReferenceQuery {
                            tokens: page.to_vec(),
                            after: after.clone(),
                            limit: MAX_LOOKUP_KEYS,
                        },
                    ),
                )
                .await?;
                for record in found.records {
                    if !current_sources.contains(&record.source_chain_id) {
                        waiting
                            .entry(record.source_chain_id)
                            .and_modify(|current| {
                                let recorded_at = record.entry.recorded_at.max(effective_at);
                                if recorded_at > current.1 {
                                    current.0 = record.entry.snapshot_id;
                                    current.1 = recorded_at;
                                }
                            })
                            .or_insert((
                                record.entry.snapshot_id,
                                record.entry.recorded_at.max(effective_at),
                                BTreeSet::new(),
                            ));
                    }
                }
                validate_reference_work(&waiting)?;
                let Some(next) = found.next_after else {
                    break;
                };
                cursors.advance(&next)?;
                after = Some(next);
            }
        }

        let mut target_chains: Vec<_> = target_chains.into_iter().collect();
        target_chains.sort_unstable();
        for chains in target_chains.chunks(MAX_LOOKUP_KEYS) {
            check_cancel(ctx)?;
            let records = reference_read(
                ctx,
                "confirmed_dependencies",
                ctx.graph.find_edges(
                    ctx.org_id.as_ref(),
                    &EdgeLookup::VersionsByEndpointChains {
                        chain_ids: chains.to_vec(),
                    },
                ),
            )
            .await?;
            for record in records {
                let owner = record
                    .stored
                    .get("reference_owner_chain_id")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok());
                let snapshot = record
                    .stored
                    .get("last_seen_snapshot_id")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok());
                let observed_at: Option<DateTime<Utc>> = record
                    .stored
                    .get("last_seen_at")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| value.parse().ok());
                if let (Some(owner), Some(observed_at)) = (owner, observed_at) {
                    if !current_sources.contains(&owner) {
                        let namespace = record
                            .stored
                            .get("reference_owner_namespace")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned);
                        let slot = record
                            .stored
                            .get("reference_slot")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned);
                        let observed_at = observed_at.max(effective_at);
                        let entry = waiting.entry(owner).or_insert((
                            snapshot,
                            observed_at,
                            BTreeSet::new(),
                        ));
                        if observed_at > entry.1 {
                            entry.0 = snapshot;
                            entry.1 = observed_at;
                        }
                        if let (Some(namespace), Some(slot)) = (namespace, slot) {
                            entry.2.insert(ReferenceOwnerSelector {
                                chain_id: owner,
                                namespace,
                                slot,
                            });
                        }
                    }
                }
            }
            validate_reference_work(&waiting)?;
        }
        if waiting.is_empty() {
            return Ok(repaired);
        }

        let reference_stages: Vec<_> = self
            .edge_stages
            .iter()
            .filter(|stage| {
                stage.capabilities().iter().any(|capability| {
                    matches!(
                        capability,
                        kg_core::traits::StageCapability::ReferenceExtraction
                            | kg_core::traits::StageCapability::ReferenceResolution
                    )
                })
            })
            .cloned()
            .collect();
        if reference_stages.is_empty() {
            return Ok(repaired);
        }
        let sources: Vec<_> = waiting.keys().copied().collect();
        // Reload only one commit-sized page at a time; source payloads are not
        // accumulated across the organization in memory.
        for chains in sources.chunks(self.config.chunk_size.min(500)) {
            check_cancel(ctx)?;
            let mut records = reference_read(
                ctx,
                "source_reload",
                ctx.graph.find_entities(
                    ctx.org_id.as_ref(),
                    &EntityLookup::LatestByChain {
                        chain_ids: chains.to_vec(),
                    },
                ),
            )
            .await?;
            records.sort_by_key(|record| record.chain_id);
            // A waiting source is woken only if the namespace policy lets it
            // reference at least one namespace whose targets changed in this run.
            // Under a closed policy a target arriving in another namespace is not
            // this source's business: no reload, no evidence, no provider attempt,
            // and its unresolved record keeps waiting for its own namespace.
            let before = records.len();
            records.retain(|record| {
                target_namespaces
                    .iter()
                    .any(|target| ctx.namespace_policy.allows(&record.namespace, target))
            });
            if records.len() < before {
                tracing::info!(
                    skipped = before - records.len(),
                    "reference repair skipped sources outside the namespace policy of the changed targets"
                );
            }
            if records.is_empty() {
                continue;
            }
            let group = records.as_slice();
            let mut messages = Vec::with_capacity(group.len());
            for (position, record) in group.iter().enumerate() {
                let evidence = waiting.get(&record.chain_id).ok_or_else(|| {
                    recovery_error(format!(
                        "reference repair source {} lost its dependency",
                        record.chain_id
                    ))
                })?;
                let resolution = stored_reference_resolution(
                    ctx,
                    record,
                    (evidence.0, evidence.1),
                    evidence.2.iter().cloned().collect(),
                )?;
                let resolution = Arc::new(resolution);
                messages.push(PipelineMessage {
                    snapshot_index: position,
                    run_id,
                    state: StageOutput::EdgeExtraction(
                        kg_core::runtime::stage_output::EdgeExtractionOutput {
                            relationship_times: Default::default(),
                            relationship_directives: Default::default(),
                            reference_report: Default::default(),
                            snapshot_nodes: resolution.snapshot_nodes.clone(),
                            resolved_nodes: resolution.live_entities(),
                            resolution,
                            edges: Default::default(),
                            pending_references: Default::default(),
                        },
                    ),
                });
            }
            let repair_sources = group
                .iter()
                .map(|record| record.chain_id)
                .collect::<Vec<_>>();
            let index = u32::try_from(next_index)
                .map_err(|_| recovery_error("reference repair batch index overflow"))?
                as usize;
            next_index += 1;
            repaired.extend(
                self.relationship_chunk(
                    messages,
                    index,
                    ctx,
                    run_id,
                    fingerprint,
                    flush,
                    scans,
                    progress,
                    &reference_stages,
                    &repair_sources,
                )
                .await?,
            );
        }
        Ok(repaired)
    }

    /// Freeze relationship extraction, then replan rejected commits against fresh graph state.
    #[allow(clippy::too_many_arguments)]
    async fn relationship_chunk(
        &self,
        outputs: Vec<PipelineMessage>,
        chunk_index: usize,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        flush: &[Arc<dyn Stage>],
        scans: &[CollectionScan],
        progress: &mut Progress,
        edge_stages: &[Arc<dyn Stage>],
        repair_sources: &[Uuid],
    ) -> Result<Vec<Uuid>, PipelineError> {
        let failed_before = progress.failed.len();
        let mut prepared = self
            .run_edge_stages("edge_pipeline", edge_stages, outputs, ctx, progress)
            .await?;
        // Keep valid edge observations, but persist the partial failure in the
        // receipt. Failed snapshots suppress collection sweeps and checkpoint ack.
        for message in &prepared {
            if let StageOutput::EdgeExtraction(output) = &message.state {
                let declined = output
                    .reference_report
                    .relationship_declines
                    .iter()
                    .filter(|decline| {
                        decline.reason == RelationshipDeclineReason::InvalidTimestampEvidence
                    })
                    .count();
                if declined > 0 {
                    progress.failed.push(SnapshotFailure {
                        profile: None,
                        snapshot_index: message.snapshot_index,
                        stage: "relationship_timestamp_extraction".into(),
                        error: format!("timestamp evidence invalid for {declined} relationship observations; valid observations retained"),
                        retriable: false,
                    });
                }
            }
        }
        // Stages that rebuild reference candidates and confirmations; re-run alone
        // after invalidated evidence so declared and model relationships (and their
        // paid calls) are not redone.
        let refresh_stages: Vec<Arc<dyn Stage>> = edge_stages
            .iter()
            .skip_while(|stage| {
                !stage
                    .capabilities()
                    .contains(&kg_core::traits::StageCapability::ReferenceExtraction)
            })
            .filter(|stage| {
                !stage
                    .capabilities()
                    .contains(&kg_core::traits::StageCapability::TextRelationships)
            })
            .cloned()
            .collect();
        let reference_refresh_source = if refresh_stages.is_empty() {
            Vec::new()
        } else {
            reference_refresh_source(&prepared)?
        };
        let mut refresh_references = false;
        if !self.relationship_resolution_stages.is_empty()
            && prepared
                .iter()
                .any(|message| !matches!(message.state, StageOutput::EdgeExtraction(_)))
        {
            return Err(PipelineError::StateValidation {
                stage: "relationship_resolution".into(),
                message: "relationship resolution requires frozen EdgeExtraction outputs".into(),
            });
        }
        let identity = BatchIdentity {
            run_id,
            kind: BatchKind::Relationship,
            index: chunk_index as u32,
        };
        let extraction_failures = progress.failed.len();
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(ctx.matching_settings.replan_timeout_ms);
        let incoming_embeddings =
            Arc::new(kg_core::runtime::embedding_cache::IncomingEmbeddingCache::default());
        for attempt in 0..=ctx.matching_settings.max_replans {
            check_cancel(ctx)?;
            if tokio::time::Instant::now() >= deadline {
                return Err(relationship_budget_error());
            }
            let mut attempt_context = ctx.as_ref().clone();
            attempt_context.cancel = ctx.cancel.child_token();
            attempt_context.identity_deadline = Some(deadline);
            attempt_context.incoming_embeddings = incoming_embeddings.clone();
            let attempt_context = Arc::new(attempt_context);
            let mut local = Progress::default();
            if refresh_references && !refresh_stages.is_empty() {
                // Reference evidence was invalidated: rebuild candidates and
                // confirmations against fresh graph state instead of re-committing
                // frozen output. Unchanged evidence reuses cached decisions.
                prepared = self
                    .run_edge_stages(
                        "reference_refresh",
                        &refresh_stages,
                        reference_refresh_source.clone(),
                        &attempt_context,
                        &mut local,
                    )
                    .await?;
            }
            let outputs = {
                let phase = self.run_stage_phase(
                    "relationship_resolution",
                    &self.relationship_resolution_stages,
                    prepared.clone(),
                    &attempt_context,
                    &mut local,
                );
                tokio::pin!(phase);
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => {
                        attempt_context.cancel.cancel();
                        let _ = phase.await;
                        return Err(relationship_budget_error());
                    },
                    result = &mut phase => result,
                }
            };
            let outputs = match outputs {
                Ok(outputs) => outputs,
                Err(PipelineError::IdentityRevisionChanged)
                    if !self.relationship_resolution_stages.is_empty() =>
                {
                    refresh_references = true;
                    wait_resolution_replan(ctx, identity, attempt, deadline).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(relationship_budget_error());
            }
            let mut observed = ObservedSet::default();
            observed.record_edges(&outputs);
            let batch = match merge_relationship_batch(
                &outputs,
                self.edge_stages.is_empty() && self.relationship_resolution_stages.is_empty(),
            ) {
                Ok(batch) => batch,
                Err(PipelineError::IdentityRevisionChanged)
                    if !self.relationship_resolution_stages.is_empty() =>
                {
                    refresh_references = true;
                    wait_resolution_replan(ctx, identity, attempt, deadline).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let mut failures = progress.failed[failed_before..extraction_failures].to_vec();
            failures.extend(local.failed.clone());
            let mut profile_diagnostics = kg_core::profiles::ProfileDiagnostics::default();
            for output in &outputs {
                if let StageOutput::EdgeResolution(edges) = &output.state {
                    for edge in edges.observed.iter() {
                        if let Some(snapshot) = edge.last_seen_snapshot_id {
                            profile_diagnostics.undeclared(
                                ctx.run_schemas.as_deref(),
                                &edge.producer_source,
                                snapshot,
                                &edge.name,
                                true,
                            );
                        }
                    }
                }
            }
            let recovery = BatchRecovery {
                profile_diagnostics,
                reference_repair_sources: repair_sources.to_vec(),
                nodes: Vec::new(),
                failures,
                observed_relationships: observed.relationship_chains.into_iter().collect(),
                incomplete_reference_sources: batch.reference_report.incomplete_sources.clone(),
                relationship_declines: batch.reference_report.relationship_declines.clone(),
                reference_decisions: batch.reference_report.decisions.clone(),
                ..Default::default()
            };
            // Persistence must await any transaction already submitted; never cancel an unknown commit.
            match self
                .commit_raw(
                    flush,
                    &attempt_context,
                    identity,
                    fingerprint,
                    scans,
                    FlushWork::Relationships(batch),
                    Some(recovery),
                    progress,
                )
                .await
            {
                Ok(commit) => {
                    if !repair_sources.is_empty()
                        && commit.recovery.as_ref().is_none_or(|recovery| {
                            recovery.reference_repair_sources != repair_sources
                        })
                    {
                        return Err(recovery_error("reference repair receipt belongs to a different source page; resume from its durable source manifest"));
                    }
                    kg_core::telemetry::reference_activity(
                        if commit.replayed {
                            kg_core::telemetry::ReferenceActivity::RepairReplayed
                        } else {
                            kg_core::telemetry::ReferenceActivity::RepairCommitted
                        },
                        repair_sources.len(),
                    );
                    if commit.replayed {
                        if let Some(recovery) = &commit.recovery {
                            progress.failed.truncate(failed_before);
                            progress.failed.extend(recovery.failures.clone());
                        }
                    } else {
                        progress.failed.extend(local.failed);
                    }
                    return Ok(commit
                        .recovery
                        .map(|recovery| recovery.observed_relationships)
                        .unwrap_or_default());
                }
                Err(StageError::CommitRejected { .. } | StageError::IdentityRevisionChanged)
                    if !self.relationship_resolution_stages.is_empty() =>
                {
                    refresh_references = true;
                    wait_resolution_replan(ctx, identity, attempt, deadline).await?;
                }
                Err(error) => return Err(commit_failure(identity, error, progress)),
            }
        }
        Err(relationship_budget_error())
    }

    /// Run one phase's stages; every recorded snapshot failure and a
    /// failure that stops the run is recorded on the output.
    /// Run edge stages with a chunk-wide barrier before the decision stage: the
    /// stages up to reference extraction run over every message first, the
    /// chunk's pending occurrences (plus audits carried from a replan) are
    /// checked against the relationship-batch audit caps, and only then do the
    /// remaining stages (evidence decisions and later discovery) run. A chunk
    /// that could never commit its audits fails here with zero provider attempts.
    async fn run_edge_stages(
        &self,
        name: &str,
        stages: &[Arc<dyn Stage>],
        inputs: Vec<PipelineMessage>,
        ctx: &Arc<RuntimeContext>,
        progress: &mut Progress,
    ) -> Result<Vec<PipelineMessage>, PipelineError> {
        let Some(split) = stages.iter().position(|stage| {
            stage
                .capabilities()
                .contains(&kg_core::traits::StageCapability::ReferenceResolution)
        }) else {
            return self
                .run_stage_phase(name, stages, inputs, ctx, progress)
                .await;
        };
        let (head, tail) = stages.split_at(split);
        let prepared = self
            .run_stage_phase(name, head, inputs, ctx, progress)
            .await?;
        preflight_chunk_decisions(&prepared, ctx)?;
        let decisions = format!("{name}_decisions");
        self.run_stage_phase(&decisions, tail, prepared, ctx, progress)
            .await
    }

    async fn run_stage_phase(
        &self,
        name: &str,
        stages: &[Arc<dyn Stage>],
        inputs: Vec<PipelineMessage>,
        ctx: &Arc<RuntimeContext>,
        progress: &mut Progress,
    ) -> Result<Vec<PipelineMessage>, PipelineError> {
        tracing::debug!(phase = name, inputs = inputs.len(), "phase starting");
        let (outputs, failures) = match run_phase(
            stages,
            inputs,
            ctx.clone(),
            Some(self.config.channel_capacity),
            Some(self.config.stage_concurrency),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                return Err(error);
            }
        };
        progress.failed.extend(failures);
        Ok(outputs)
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_raw(
        &self,
        flush: &[Arc<dyn Stage>],
        ctx: &Arc<RuntimeContext>,
        identity: BatchIdentity,
        fingerprint: &RequestFingerprint,
        scans: &[CollectionScan],
        work: FlushWork,
        recovery: Option<BatchRecovery>,
        progress: &mut Progress,
    ) -> Result<CommitOutput, StageError> {
        let started = Instant::now();
        let result = self
            .submit_commit(
                flush,
                ctx,
                identity,
                fingerprint,
                scans,
                work,
                recovery,
                progress,
            )
            .await;
        tracing::info!(
            batch_kind = identity.kind.label(),
            batch_index = identity.index,
            outcome = match &result {
                Ok(commit) if commit.replayed => "replayed",
                Ok(_) => "committed",
                Err(StageError::CommitOutcomeUnknown { .. }) => "unknown",
                Err(StageError::IdentityRevisionChanged) => "identity_revision_changed",
                Err(StageError::CommitRejected { .. }) => "conflict",
                Err(StageError::Cancelled { .. }) => "cancelled",
                Err(_) => "error",
            },
            elapsed_ms = started.elapsed().as_millis(),
            "pipeline batch finished"
        );
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn submit_commit(
        &self,
        flush: &[Arc<dyn Stage>],
        ctx: &Arc<RuntimeContext>,
        identity: BatchIdentity,
        fingerprint: &RequestFingerprint,
        scans: &[CollectionScan],
        work: FlushWork,
        recovery: Option<BatchRecovery>,
        progress: &mut Progress,
    ) -> Result<CommitOutput, StageError> {
        let output = StageOutput::FlushBatch(FlushBatchOutput {
            batch: identity,
            fingerprint: fingerprint.clone(),
            scans: scans.to_vec(),
            work,
            recovery,
        });
        self.submit_output(flush, ctx, identity, fingerprint, output, progress)
            .await
    }

    async fn submit_output(
        &self,
        flush: &[Arc<dyn Stage>],
        ctx: &Arc<RuntimeContext>,
        identity: BatchIdentity,
        fingerprint: &RequestFingerprint,
        mut output: StageOutput,
        progress: &mut Progress,
    ) -> Result<CommitOutput, StageError> {
        for stage in flush {
            let name = stage.name();
            let stage_span = tracing::info_span!(
                "pipeline.batch_stage",
                stage = name,
                batch_kind = identity.kind.label(),
                batch_index = identity.index,
                otel.status_code = tracing::field::Empty,
            );
            let mut measurement =
                stage_span.in_scope(|| kg_core::telemetry::OperationGuard::stage(name));
            let input_kind = kg_core::traits::StageKind::of(&output);
            let result = if ctx.cancel.is_cancelled() {
                Err(StageError::Cancelled { stage: name.into() })
            } else if !stage
                .contract()
                .iter()
                .any(|(input, _)| *input == input_kind)
            {
                Err(StageError::StateValidation {
                    stage: name.into(),
                    message: "batch stage received an unsupported payload".into(),
                })
            } else {
                stage
                    .process(output, ctx)
                    .instrument(stage_span.clone())
                    .await
                    .and_then(|next| {
                        let output_kind = kg_core::traits::StageKind::of(&next);
                        if stage.contract().contains(&(input_kind, output_kind)) {
                            let mutation_batch = match &next {
                                StageOutput::PlannedBatch(plan) => Some(&plan.batch),
                                StageOutput::PreparedBatch(prepared) => Some(&prepared.batch),
                                _ => None,
                            };
                            if mutation_batch.is_some_and(|batch| {
                                batch.batch != identity
                                    || batch.org_id != ctx.org_id.as_ref()
                                    || &batch.fingerprint != fingerprint
                            }) {
                                return Err(StageError::StateValidation {
                                    stage: name.into(),
                                    message: "batch stage changed request identity or organization"
                                        .into(),
                                });
                            }
                            if let StageOutput::Committed(commit) = &next {
                                if commit.batch != identity {
                                    return Err(StageError::StateValidation {
                                        stage: name.into(),
                                        message: "commit acknowledged a different batch".into(),
                                    });
                                }
                                progress.record(commit).map_err(|error| {
                                    StageError::StateValidation {
                                        stage: name.into(),
                                        message: error.to_string(),
                                    }
                                })?;
                            }
                            Ok(next)
                        } else {
                            Err(StageError::StateValidation {
                                stage: name.into(),
                                message: "batch stage violated its handoff contract".into(),
                            })
                        }
                    })
            };
            stage_span.record(
                "otel.status_code",
                if result.is_ok() { "OK" } else { "ERROR" },
            );
            measurement.finish(match &result {
                Ok(StageOutput::Committed(commit)) if commit.replayed => {
                    kg_core::telemetry::Outcome::Replayed
                }
                Ok(_) => kg_core::telemetry::Outcome::Success,
                Err(error) => kg_core::telemetry::Outcome::from(error),
            });
            match result {
                Ok(next) => {
                    output = next;
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
        let StageOutput::Committed(commit) = output else {
            return Err(StageError::StateValidation {
                stage: "batch_commit".into(),
                message: format!(
                    "persistence stage returned {} instead of a commit for batch {}#{}",
                    state_kind(&output),
                    identity.kind.label(),
                    identity.index
                ),
            });
        };
        Ok(commit)
    }

    /// Mark-and-sweep reconciliation per declared collection. Stale members
    /// are read from the graph and partitioned: members this run or another
    /// collection still observes release the scanned collection's
    /// membership; members a later observation of another run protects are
    /// left alone; the rest are tombstoned with their live relationships
    /// closed. Source-owned relationships of sole-owned members are
    /// invalidated when the scan declared relationship coverage.
    ///
    /// Every batch is fenced by the collection's scan ownership and committed
    /// within the statement budget under a deterministic identity. A
    /// rejected commit re-reads the collection a bounded number of times; a
    /// resumed run counts the pages its receipts already hold and continues
    /// after them.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile(
        &self,
        scopes: &[DeclaredScope],
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        flush: &[Arc<dyn Stage>],
        scans: &[CollectionScan],
        observed: &ObservedSet,
        resumed: &[CommittedBatch],
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        let incomplete_scopes: HashSet<(String, String)> = progress
            .incomplete
            .iter()
            .map(|i| (i.extraction.namespace.clone(), i.extraction.source.clone()))
            .collect();
        for (ordinal, scope) in scopes.iter().enumerate() {
            let span = tracing::info_span!(
                "pipeline.reconcile",
                run_id = %run_id,
                collection = %scope.scan.collection,
                generation = scope.scan.generation
            );
            self.reconcile_scope(
                ordinal,
                scope,
                &incomplete_scopes,
                ctx,
                run_id,
                fingerprint,
                flush,
                scans,
                observed,
                resumed,
                progress,
            )
            .instrument(span)
            .await?;
        }
        Ok(())
    }

    /// Reconcile one declared collection.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile_scope(
        &self,
        ordinal: usize,
        scope: &DeclaredScope,
        incomplete_scopes: &HashSet<(String, String)>,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        flush: &[Arc<dyn Stage>],
        scans: &[CollectionScan],
        observed: &ObservedSet,
        resumed: &[CommittedBatch],
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        {
            let collection = &scope.scan.collection;
            let mut outcome = scope.outcome();
            if incomplete_scopes
                .contains(&(collection.namespace.clone(), collection.source.clone()))
            {
                tracing::warn!(
                    "reconciliation suppressed: extraction incomplete in the collection's scope this run"
                );
                outcome.swept = false;
                outcome.reason =
                    Some("extraction incomplete in the collection's namespace and source".into());
                progress.collections.push(outcome);
                return Ok(());
            }
            let base = ordinal as u32 * RECONCILE_INDEX_STRIDE;

            // Pages an earlier attempt committed count toward the run and are
            // never re-read: their deletions are already in the graph.
            let mut page: u32 = 0;
            for receipt in resumed.iter().filter(|b| {
                b.kind == BatchKind::Reconciliation
                    && b.index >= base
                    && b.index < base + RECONCILE_INDEX_STRIDE
            }) {
                let counts: CommittedCounts = serde_json::from_value(receipt.result.clone())
                    .map_err(|e| PipelineError::StateValidation {
                        stage: "reconcile".into(),
                        message: format!(
                            "receipt of batch reconciliation#{} holds an unreadable result: {e}",
                            receipt.index
                        ),
                    })?;
                progress
                    .record(&CommitOutput {
                        batch: BatchIdentity {
                            run_id,
                            kind: BatchKind::Reconciliation,
                            index: receipt.index,
                        },
                        replayed: true,
                        committed_at: receipt.committed_at,
                        counts,
                        recovery: None,
                    })
                    .map_err(|error| recovery_error(error.to_string()))?;
                outcome
                    .add(&counts)
                    .map_err(|error| recovery_error(error.to_string()))?;
                page = page.max(receipt.index - base + 1);
            }

            let mut rejections = 0u32;
            loop {
                check_cancel(ctx)?;
                let stale = tokio::select! {
                    biased;
                    _ = ctx.cancel.cancelled() => return Err(PipelineError::Cancelled),
                    result = self.stale_members(ctx, scope, observed) => result?,
                };
                outcome.entities_protected = stale.protected;
                if stale.is_empty() {
                    break;
                }
                if page >= RECONCILE_INDEX_STRIDE {
                    return Err(PipelineError::StateValidation {
                        stage: "reconcile".into(),
                        message: format!(
                            "collection {collection} needs more than {RECONCILE_INDEX_STRIDE} reconciliation batches"
                        ),
                    });
                }
                let batch = stale.within_budget(
                    self.config
                        .reconciliation_batch_statements
                        .min(MAX_STATEMENTS_PER_BATCH.saturating_sub(scans.len())),
                    scope.scan.clone(),
                    scope.captured_at,
                )?;
                let identity = BatchIdentity {
                    run_id,
                    kind: BatchKind::Reconciliation,
                    index: base + page,
                };
                match self
                    .commit_raw(
                        flush,
                        ctx,
                        identity,
                        fingerprint,
                        scans,
                        FlushWork::Reconciliation(batch),
                        None,
                        progress,
                    )
                    .await
                {
                    Ok(commit) => {
                        page += 1;
                        rejections = 0;
                        outcome
                            .add(&commit.counts)
                            .map_err(|error| recovery_error(error.to_string()))?;
                        if commit.replayed {
                            tracing::info!(
                                batch_index = identity.index,
                                "reconciliation batch replayed; re-reading remaining stale records"
                            );
                        }
                    }
                    Err(StageError::CommitRejected { message, .. }) => {
                        rejections += 1;
                        tracing::warn!(
                            batch_index = identity.index,
                            attempt = rejections,
                            "reconciliation commit rejected; re-reading collection"
                        );
                        if rejections > RECONCILE_MAX_REJECTIONS {
                            return Err(commit_failure(
                                identity,
                                StageError::CommitRejected {
                                    stage: "batch_commit".into(),
                                    message,
                                },
                                progress,
                            ));
                        }
                    }
                    Err(e) => return Err(commit_failure(identity, e, progress)),
                }
            }
            progress.collections.push(outcome);
        }
        Ok(())
    }

    /// Read and partition the collection's stale members and relationships.
    async fn stale_members(
        &self,
        ctx: &RuntimeContext,
        scope: &DeclaredScope,
        observed: &ObservedSet,
    ) -> Result<StaleMembers, PipelineError> {
        let query_error = |what: &str, e: BackendError| PipelineError::StepExecution {
            step: what.into(),
            stage: "reconcile".into(),
            cause: e.to_string(),
            retriable: e.is_transient(),
        };
        let org = ctx.org_id.as_ref();
        let collection = &scope.scan.collection;
        let mut stale = StaleMembers::default();
        let records = ctx
            .graph
            .find_entities(
                org,
                &EntityLookup::StaleInCollection {
                    collection: collection.clone(),
                    before_generation: scope.scan.generation,
                },
            )
            .await
            .map_err(|e| query_error("stale_entities", e))?;
        for record in records {
            let entity = StaleEntity {
                chain_id: record.chain_id,
                uuid: record.uuid,
                version: record.version,
                entity_type: record.entity_type.clone(),
                name: record.name.clone(),
                collections: record.collections.clone(),
            };
            if record.is_shared_beyond(collection) {
                // Another collection still holds it: only this collection lets go.
                stale.released.push(entity);
            } else if observed.chains.contains(&record.chain_id)
                || record
                    .last_seen_at
                    .is_some_and(|seen| seen > scope.captured_at)
            {
                // Observed this run (a stale, older-capture observation does not
                // advance the membership generation, so it still appears here) or
                // by a later run: the resource is live. Protect it rather than
                // releasing its sole collection, which would drop the membership
                // and leave nothing able to sweep it if it is later deleted.
                stale.protected += 1;
            } else {
                stale.entities.push(entity);
            }
        }

        let deleted_chains: HashSet<Uuid> = stale.entities.iter().map(|e| e.chain_id).collect();
        let mut anchors = deleted_chains.clone();
        let mut absence = HashMap::new();
        if scope.relationships_complete && self.relationship_coverage {
            let released: HashSet<Uuid> = stale.released.iter().map(|e| e.chain_id).collect();
            let records = ctx
                .graph
                .find_edges(
                    org,
                    &EdgeLookup::ScheduledStaleInCollection {
                        collection: collection.clone(),
                        before_generation: scope.scan.generation,
                        effective_at: scope.captured_at,
                    },
                )
                .await
                .map_err(|e| query_error("stale_edges", e))?;
            if records.len() > kg_core::traits::relationship_timeline::MAX_VERSIONS {
                return Err(query_error(
                    "stale_edges",
                    BackendError::Query(
                        "stale relationship history exceeds the version budget".into(),
                    ),
                ));
            }
            for record in records {
                let seen = relationship_timestamp(&record.stored, "last_seen_at")?;
                let transition = relationship_timestamp(&record.stored, "last_transition_at")?;
                if seen.is_some_and(|t| t > scope.captured_at)
                    || transition.is_some_and(|t| t > scope.captured_at)
                    || !record.is_source_owned()
                    || observed
                        .relationship_chains
                        .contains(&relationship_chain_id(&record)?)
                    || released.contains(&record.source_chain_id)
                    || record.source_shared_beyond(collection)
                {
                    continue;
                }
                anchors.insert(record.source_chain_id);
                absence.insert(
                    record.uuid,
                    kg_core::traits::relationship_timeline::state(&record.stored),
                );
            }
        }
        let owner_keys: Vec<_> = anchors.difference(&deleted_chains).copied().collect();
        for chains in owner_keys.chunks(kg_core::traits::graph_reads::MAX_LOOKUP_KEYS) {
            let records = ctx
                .graph
                .find_entities(
                    org,
                    &EntityLookup::LatestByChain {
                        chain_ids: chains.to_vec(),
                    },
                )
                .await
                .map_err(|e| query_error("relationship_owners", e))?;
            let requested: HashSet<_> = chains.iter().copied().collect();
            let mut returned = HashSet::new();
            for record in records {
                if !requested.contains(&record.chain_id) || !returned.insert(record.chain_id) {
                    return Err(query_error(
                        "relationship_owners",
                        BackendError::Query("invalid relationship owner lookup result".into()),
                    ));
                }
                if !record.is_latest
                    || record.deleted_at.is_some()
                    || record.merged_into.is_some()
                    || record.collections.len() != 1
                    || record.collections[0].collection != *collection
                {
                    continue;
                }
                stale.relationship_owners.insert(
                    record.chain_id,
                    StaleEntity {
                        chain_id: record.chain_id,
                        uuid: record.uuid,
                        version: record.version,
                        entity_type: record.entity_type,
                        name: record.name,
                        collections: record.collections,
                    },
                );
            }
        }
        let mut anchors: Vec<_> = anchors.into_iter().collect();
        anchors.sort_unstable();
        let mut selected = HashSet::new();
        for anchor in &anchors {
            stale.incident_timelines.insert(*anchor, Vec::new());
        }
        for chain in &deleted_chains {
            stale.live_incident.insert(*chain, Vec::new());
        }
        let deletion_keys: Vec<_> = deleted_chains.iter().copied().collect();
        for chains in deletion_keys.chunks(kg_core::traits::graph_reads::MAX_LOOKUP_KEYS) {
            let records = ctx
                .graph
                .find_edges(
                    org,
                    &EdgeLookup::LiveByEndpointChains {
                        chain_ids: chains.to_vec(),
                    },
                )
                .await
                .map_err(|e| query_error("deleted_member_edges", e))?;
            for record in records {
                for chain in [record.source_chain_id, record.target_chain_id]
                    .into_iter()
                    .collect::<HashSet<_>>()
                {
                    if let Some(ids) = stale.live_incident.get_mut(&chain) {
                        ids.push(record.uuid);
                    }
                }
            }
        }
        for ids in stale.live_incident.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
        for chains in anchors.chunks(kg_core::traits::graph_reads::MAX_LOOKUP_KEYS) {
            let records = ctx
                .graph
                .find_edges(
                    org,
                    &EdgeLookup::VersionsByEndpointChains {
                        chain_ids: chains.to_vec(),
                    },
                )
                .await
                .map_err(|e| query_error("reconciliation_timeline", e))?;
            if records.len() > kg_core::traits::relationship_timeline::MAX_VERSIONS {
                return Err(query_error(
                    "reconciliation_timeline",
                    BackendError::Query("incident history exceeds the version budget".into()),
                ));
            }
            let keys: HashSet<_> = chains.iter().copied().collect();
            for record in records {
                if !keys.contains(&record.source_chain_id)
                    && !keys.contains(&record.target_chain_id)
                {
                    return Err(query_error(
                        "reconciliation_timeline",
                        BackendError::Query(
                            "incident history returned an unrelated relationship".into(),
                        ),
                    ));
                }
                let properties = kg_core::traits::relationship_timeline::state(&record.stored);
                let state = kg_core::traits::relationship_timeline::IncidentVersionState {
                    source_chain_id: record.source_chain_id,
                    target_chain_id: record.target_chain_id,
                    properties: properties.clone(),
                };
                for chain in [record.source_chain_id, record.target_chain_id]
                    .into_iter()
                    .collect::<HashSet<_>>()
                {
                    if keys.contains(&chain) {
                        stale
                            .incident_timelines
                            .entry(chain)
                            .or_default()
                            .push(state.clone());
                    }
                }
                if (deleted_chains.contains(&record.source_chain_id)
                    || deleted_chains.contains(&record.target_chain_id)
                    || (stale
                        .relationship_owners
                        .contains_key(&record.source_chain_id)
                        && absence
                            .get(&record.uuid)
                            .is_some_and(|original| original == &properties)))
                    && relationship_needs_retirement(&properties, scope.captured_at)?
                    && selected.insert(record.uuid)
                {
                    stale.edges.push(StaleEdge {
                        uuid: record.uuid,
                        version: record.version,
                        source_chain_id: record.source_chain_id,
                        target_chain_id: record.target_chain_id,
                        name: record.name,
                        properties,
                    });
                }
            }
        }
        for (chain, versions) in &mut stale.incident_timelines {
            versions.sort_by(|a, b| {
                a.properties
                    .get("uuid")
                    .and_then(serde_json::Value::as_str)
                    .cmp(&b.properties.get("uuid").and_then(serde_json::Value::as_str))
            });
            kg_core::traits::relationship_timeline::validate_incident(*chain, versions)
                .map_err(|e| query_error("reconciliation_timeline", e))?;
        }
        stale.edges.sort_by_key(|e| e.uuid);
        Ok(stale)
    }
}

/// Rehydrate one stored live entity as reference-stage input without treating
/// it as a new observation or mutating its node history.
pub fn stored_reference_resolution(
    ctx: &RuntimeContext,
    record: &kg_core::traits::EntityVersionRecord,
    evidence: (Option<Uuid>, DateTime<Utc>),
    owner_refresh: Vec<ReferenceOwnerSelector>,
) -> Result<NodeResolutionOutput, PipelineError> {
    let expected = kg_core::profiles::evidence_contract(
        ctx.run_schemas.as_deref(),
        record.source.as_deref().unwrap_or_default(),
    )
    .map_err(|e| recovery_error(e.to_string()))?;
    let stored_contract = record
        .stored
        .get("profile_contract")
        .filter(|v| !v.is_null());
    if stored_contract.is_some_and(|v| v.as_str().is_none())
        || stored_contract.and_then(serde_json::Value::as_str) != expected.as_deref()
    {
        return Err(recovery_error("stored reference source has a different profile contract; reingest that source with its intended profile before reference repair"));
    }
    let org = ctx.org_id.as_ref();
    let snapshot_id = record
        .stored
        .get("last_seen_snapshot_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .or(evidence.0)
        .ok_or_else(|| recovery_error("reference repair dependency has no snapshot"))?;
    let properties = record
        .typed_source_properties()
        .map_err(|_| recovery_error("reference repair source properties are invalid"))?;
    let text = |key: &str| {
        record
            .stored
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let uuid = |key: &str| text(key).and_then(|value| Uuid::parse_str(&value).ok());
    let primary_key_properties = record
        .stored
        .get("primary_key_properties")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let additional_key_properties = text("additional_key_properties")
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|_| recovery_error("reference repair source has invalid alternative keys"))?
        .unwrap_or_default();
    let identity_hash = record
        .identity_hash
        .as_ref()
        .ok_or_else(|| recovery_error("reference repair source has no identity hash"))
        .and_then(|value| {
            serde_json::from_value(serde_json::Value::String(value.clone()))
                .map_err(|_| recovery_error("reference repair source has invalid identity hash"))
        })?;
    let labels = record
        .stored
        .get("labels")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let tags = record
        .stored
        .iter()
        .filter_map(|(key, value)| {
            Some((
                key.strip_prefix(kg_core::traits::graph_mutation::TAG_PROPERTY_PREFIX)?
                    .to_owned(),
                value.as_str()?.to_owned(),
            ))
        })
        .collect();
    let entity = EntityNode {
        uuid: record.uuid,
        chain_id: record.chain_id,
        org_id: org.into(),
        namespace: record.namespace.clone(),
        entity_type: record.entity_type.clone(),
        name: record.name.clone(),
        all_properties: properties,
        primary_key_properties,
        additional_key_properties,
        identity_hash,
        lifecycle: EntityLifecycle::Active,
        version: record.version,
        is_latest: true,
        previous_version_uuid: uuid("previous_version_uuid"),
        embedding: None,
        valid_from: record.valid_from.unwrap_or(evidence.1),
        valid_to: record.valid_to,
        deleted_at: record.deleted_at,
        deleted_by: text("deleted_by"),
        deletion_reason: text("deletion_reason"),
        source: record
            .source
            .clone()
            .ok_or_else(|| recovery_error("reference repair source has no producer"))?,
        extracted_by: text("extracted_by").unwrap_or_else(|| "stored".into()),
        resolved_by: text("resolved_by"),
        first_seen_snapshot_id: uuid("first_seen_snapshot_id"),
        last_seen_snapshot_id: Some(snapshot_id),
        last_seen_at: Some(evidence.1),
        sync_generation: record.sync_generation,
        collections: record.collections.clone(),
        labels,
        inherited_labels: Vec::new(),
        tags,
        summary: text("summary"),
        structural_hash: record
            .structural_hash
            .as_deref()
            .and_then(|value| value.parse().ok())
            .unwrap_or_default(),
        needs_llm_review: record
            .stored
            .get("needs_llm_review")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    };
    entity
        .validate()
        .map_err(|_| recovery_error("reference repair source model is invalid"))?;
    let observed = ObservedEntityProperties::from_entity(&entity, snapshot_id);
    let snapshot = SnapshotNode {
        uuid: snapshot_id,
        org_id: org.into(),
        namespace: entity.namespace.clone(),
        name: format!("reference-repair-{snapshot_id}"),
        source_description: None,
        data_type: SnapshotDataType::Entities,
        snapshot_kind: SnapshotKind::Incremental,
        sync_generation: entity.sync_generation,
        complete: false,
        collection: None,
        source: entity.source.clone(),
        content: None,
        captured_at: evidence.1,
        entities: vec![entity.uuid],
        entity_edges: vec![],
        labels: vec![],
        tags: Default::default(),
        created_at: Utc::now(),
    };
    let schemas = if let Some(manifest) = &ctx.run_schemas {
        // Repair runs only the reference stages. A prior source may not appear
        // in this request's schema manifest, and reference matching does not
        // consume entity or edge schema definitions.
        let definition = manifest
            .sources
            .get(&entity.source)
            .cloned()
            .unwrap_or_default();
        HashMap::from([(
            snapshot_id,
            kg_core::runtime::schemas::ObservationSchemas {
                org_id: org.into(),
                source: entity.source.clone(),
                definitions: std::collections::BTreeMap::from([(
                    entity.source.clone(),
                    definition,
                )]),
            },
        )])
    } else {
        HashMap::new()
    };
    let exclusions: Vec<String> = record.stored.get("reference_exclusions")
        .cloned().ok_or_else(||recovery_error(format!("stored source {} lacks reference-exclusion metadata; reingest that source before reference repair", record.chain_id)))
        .and_then(|value|serde_json::from_value(value).map_err(|_|recovery_error("stored reference exclusions are invalid")))?;
    Ok(NodeResolutionOutput {
        reference_source_reads: Arc::new(vec![
            kg_core::runtime::stage_output::ReferenceSourceRead {
                chain_id: record.chain_id,
                version_uuid: record.uuid,
                version: record.version,
                observed_at: record
                    .last_seen_at
                    .or(record.valid_from)
                    .ok_or_else(|| recovery_error("stored source has no observation time"))?,
            },
        ]),
        fk_exclusions: Arc::new(HashMap::from([(snapshot_id, exclusions)])),
        observed_properties: Arc::new(vec![observed]),
        schemas: Arc::new(schemas),
        snapshot_nodes: Arc::new(vec![snapshot]),
        nodes_unchanged: Arc::new(vec![Observed::new(entity.uuid, entity)]),
        reference_owner_refresh: Arc::new(owner_refresh),
        ..Default::default()
    })
}

/// Preserve authoritative and model-derived relationships while removing only
/// the derived reference attempt. A retry then reloads targets from storage;
/// it never treats its prior reference output as declared evidence.
/// The chunk-level audit-cap check (see `run_edge_stages`).
fn preflight_chunk_decisions(
    prepared: &[PipelineMessage],
    ctx: &RuntimeContext,
) -> Result<(), PipelineError> {
    let mut pending = 0usize;
    let mut carried = Vec::new();
    for message in prepared {
        if let StageOutput::EdgeExtraction(output) = &message.state {
            pending = pending.saturating_add(output.pending_references.len());
            carried.extend(output.reference_report.decisions.iter().cloned());
        }
    }
    kg_core::runtime::reference_resolution::preflight_chunk_decisions(
        pending,
        &carried,
        ctx.reference_resolution_settings.max_audit_bytes,
    )
    .map_err(|message| PipelineError::StateValidation {
        stage: "reference_resolution".into(),
        message,
    })
}

fn reference_refresh_source(
    prepared: &[PipelineMessage],
) -> Result<Vec<PipelineMessage>, PipelineError> {
    prepared
        .iter()
        .cloned()
        .map(|mut message| {
            let StageOutput::EdgeExtraction(mut extraction) = message.state else {
                return Err(PipelineError::StateValidation {
                    stage: "reference_refresh".into(),
                    message: format!(
                        "reference refresh requires edge extraction input, got {}",
                        state_kind(&message.state)
                    ),
                });
            };
            let retained: Vec<_> = extraction
                .edges
                .iter()
                .filter(|edge| edge.origin != kg_core::models::RelationshipOrigin::Reference)
                .cloned()
                .collect();
            let retained_ids: HashSet<_> = retained.iter().map(|edge| edge.uuid).collect();
            extraction.edges = Arc::new(retained);
            extraction.relationship_times = Arc::new(
                extraction
                    .relationship_times
                    .iter()
                    .filter(|(uuid, _)| retained_ids.contains(uuid))
                    .map(|(uuid, time)| (*uuid, time.clone()))
                    .collect(),
            );
            extraction.pending_references = Default::default();
            extraction.reference_report = Default::default();
            let mut resolution = extraction.resolution.as_ref().clone();
            resolution.chunk_entities = None;
            extraction.resolution = Arc::new(resolution);
            message.state = StageOutput::EdgeExtraction(extraction);
            Ok(message)
        })
        .collect()
}

/// Committed work and recorded failures accumulated during one call.
#[derive(Default)]
struct Progress {
    profile_diagnostics: kg_core::profiles::ProfileDiagnostics,
    incomplete_reference_sources: BTreeSet<Uuid>,
    incomplete_followups: Vec<kg_core::saga::IncompleteSagaSummary>,
    skipped_summaries: Vec<kg_core::pipeline::output::SkippedSummary>,
    relationship_declines: Vec<kg_core::runtime::stage_output::RelationshipDecline>,
    reference_decisions: Vec<kg_core::runtime::reference_resolution::ReferenceDecisionAudit>,
    community_checkpoints: BTreeMap<u32, kg_core::runtime::community::CommunityCheckpoint>,
    summary_affected_chains: std::collections::BTreeSet<Uuid>,
    summary_manifest: Option<kg_core::runtime::stage_output::SummaryManifest>,
    saga_summary_manifest: Option<kg_core::runtime::stage_output::SagaSummaryManifest>,
    saga_associations: BTreeMap<(String, Uuid), u64>,
    committed: CommittedCounts,
    newly_committed: CommittedCounts,
    batches: Vec<BatchOutcome>,
    commit_unknown: bool,
    failed: Vec<SnapshotFailure>,
    incomplete: Vec<IncompleteSnapshot>,
    collections: Vec<CollectionOutcome>,
}

impl Progress {
    fn record(&mut self, commit: &CommitOutput) -> Result<(), kg_core::pipeline::CountOverflow> {
        if self
            .batches
            .iter()
            .any(|b| b.kind == commit.batch.kind && b.index == commit.batch.index)
        {
            return Ok(());
        }
        if let Some(recovery) = &commit.recovery {
            self.profile_diagnostics
                .extend(&recovery.profile_diagnostics);
            self.incomplete_reference_sources
                .extend(recovery.incomplete_reference_sources.iter().copied());
            self.relationship_declines
                .extend(recovery.relationship_declines.clone());
            // Receipts carry the batch's own decisions; one batch is recorded
            // once (guarded above), so plain extension is exact.
            self.reference_decisions
                .extend(recovery.reference_decisions.clone());
            self.skipped_summaries
                .extend(recovery.skipped_summaries.clone());
            self.incomplete_followups
                .extend(recovery.incomplete_saga_summaries.clone());
            if let Some(checkpoint) = &recovery.community_checkpoint {
                self.community_checkpoints
                    .insert(commit.batch.index, checkpoint.clone());
            }
            for association in &recovery.saga_associations {
                self.saga_associations
                    .entry((association.namespace.clone(), association.saga_uuid))
                    .and_modify(|ordinal| *ordinal = (*ordinal).max(association.membership_ordinal))
                    .or_insert(association.membership_ordinal);
            }
            if commit.batch.kind == BatchKind::SagaSummary && commit.batch.index == 0 {
                self.saga_summary_manifest = recovery.saga_summary_manifest.clone();
            }
            self.summary_affected_chains
                .extend(recovery.summary_affected_chains.iter().copied());
            if commit.batch.kind == BatchKind::Summary && commit.batch.index == 0 {
                self.summary_manifest = recovery.summary_manifest.clone();
            }
        }
        // Acknowledgement remains known even when receipt counts cannot be summed.
        self.batches.push(BatchOutcome {
            kind: commit.batch.kind,
            index: commit.batch.index,
            replayed: commit.replayed,
            counts: commit.counts,
        });
        let mut committed = self.committed;
        let mut newly_committed = self.newly_committed;
        committed.add(&commit.counts)?;
        if !commit.replayed {
            newly_committed.add(&commit.counts)?;
        }
        self.committed = committed;
        self.newly_committed = newly_committed;
        Ok(())
    }
}

// Registration writes only replay metadata. If interrupted, the same run id
// recovers its header; no graph data batch has been submitted yet.
async fn register_run(
    ctx: &RuntimeContext,
    operation: impl std::future::Future<Output = Result<RunRegistration, BackendError>>,
) -> Result<RunRegistration, PipelineError> {
    let result = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return Err(PipelineError::Cancelled),
        result = tokio::time::timeout(
            std::time::Duration::from_millis(ctx.context_settings.read_timeout_ms),
            operation,
        ) => result.unwrap_or(Err(BackendError::Timeout(ctx.context_settings.read_timeout_ms))),
    };
    result.map_err(|error| match error {
        BackendError::Conflict(_) => PipelineError::StateValidation {
            stage: "run".into(),
            message: "run id was registered with different input or settings".into(),
        },
        error => PipelineError::StepExecution {
            stage: "run".into(),
            step: "register_run".into(),
            cause: error.to_string(),
            retriable: error.is_transient(),
        },
    })
}

fn validate_summary_chain(
    stages: &[Arc<dyn Stage>],
    input: kg_core::traits::StageKind,
) -> Result<(), PipelineError> {
    let invalid = || PipelineError::StateValidation {
        stage: "topology".into(),
        message:
            "summary stages require unique nonblank names and a complete typed path to Committed"
                .into(),
    };
    let mut names = HashSet::new();
    let mut possible = HashSet::from([input]);
    for stage in stages {
        if stage.name().trim().is_empty() || !names.insert(stage.name()) {
            return Err(invalid());
        }
        let mut next = HashSet::new();
        for input in possible {
            let outputs: Vec<_> = stage
                .contract()
                .iter()
                .filter(|(accepted, _)| *accepted == input)
                .map(|(_, output)| *output)
                .collect();
            if outputs.is_empty() {
                return Err(invalid());
            }
            next.extend(outputs);
        }
        possible = next;
    }
    if possible != HashSet::from([kg_core::traits::StageKind::Committed]) {
        return Err(invalid());
    }
    Ok(())
}

fn recovery_error(message: impl Into<String>) -> PipelineError {
    PipelineError::StateValidation {
        stage: "recovery".into(),
        message: message.into(),
    }
}

fn restore_commit(run_id: Uuid, receipt: &CommittedBatch) -> Result<CommitOutput, PipelineError> {
    Ok(CommitOutput {
        batch: BatchIdentity {
            run_id,
            kind: receipt.kind,
            index: receipt.index,
        },
        replayed: true,
        committed_at: receipt.committed_at,
        counts: serde_json::from_value(receipt.result.clone())
            .map_err(|e| recovery_error(format!("invalid receipt counts: {e}")))?,
        recovery: receipt
            .result
            .get("recovery")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| recovery_error(format!("invalid recovery data: {e}")))?,
    })
}

/// Bound the durable handoff before any writes, without allocating a second JSON buffer.
fn validate_node_checkpoint_size(recovery: &BatchRecovery) -> Result<(), PipelineError> {
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("node checkpoint exceeds 16 MiB"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Budget(16 * 1024 * 1024), recovery)
        .map_err(|_| recovery_error("node checkpoint exceeds 16 MiB or is not serializable"))
}

fn checkpoint_nodes(
    outputs: &[PipelineMessage],
    failures: Vec<SnapshotFailure>,
    settings: &kg_core::runtime::extraction::ExtractionSettings,
    manifest: Option<&kg_core::runtime::schemas::RunSchemaManifest>,
) -> Result<BatchRecovery, PipelineError> {
    let mut profile_diagnostics = kg_core::profiles::ProfileDiagnostics::default();
    let mut nodes = Vec::with_capacity(outputs.len());
    let mut reused_snapshots = Vec::new();
    for output in outputs {
        if let StageOutput::ReusedSnapshot(reused) = &output.state {
            reused_snapshots.push(reused.clone());
            continue;
        }
        let StageOutput::NodeResolution(resolution) = &output.state else {
            return Err(recovery_error("node checkpoint requires resolved nodes"));
        };
        resolution
            .validate_raw_text_drafts(settings)
            .map_err(recovery_error)?;
        for entity in resolution.live_entities().iter() {
            if let Some(snapshot) = entity.last_seen_snapshot_id {
                profile_diagnostics.undeclared(
                    manifest,
                    &entity.source,
                    snapshot,
                    &entity.entity_type,
                    false,
                );
            }
        }
        let mut resolution = resolution.clone();
        resolution.chunk_entities = None;
        for snapshot in Arc::make_mut(&mut resolution.snapshot_nodes) {
            snapshot.content = None;
        }
        nodes.push(NodeCheckpoint {
            snapshot_index: output.snapshot_index,
            resolution,
        });
    }
    let recovery = BatchRecovery {
        profile_diagnostics,
        reused_snapshots,
        nodes,
        failures,
        observed_relationships: Vec::new(),
        ..Default::default()
    };
    validate_node_checkpoint_size(&recovery)?;
    Ok(recovery)
}

fn restore_nodes(
    recovery: BatchRecovery,
    snapshots: &[IngestionInput],
    run_id: Uuid,
    ctx: &RuntimeContext,
    chunk_index: usize,
) -> Result<Vec<PipelineMessage>, PipelineError> {
    validate_node_checkpoint_size(&recovery)?;
    let manifest = ctx
        .observation_manifest
        .as_ref()
        .ok_or_else(|| recovery_error("missing frozen observation manifest"))?;
    let expected = manifest
        .node_batches
        .get(chunk_index)
        .ok_or_else(|| recovery_error("receipt batch outside manifest"))?;
    let mut actual = HashSet::new();
    for index in recovery
        .nodes
        .iter()
        .map(|n| n.snapshot_index)
        .chain(recovery.reused_snapshots.iter().map(|n| n.snapshot_index))
        .chain(recovery.failures.iter().map(|f| f.snapshot_index))
    {
        if !expected.contains(&index) || !actual.insert(index) {
            return Err(recovery_error(
                "receipt has duplicate or foreign input ordinal",
            ));
        }
    }
    if actual.len() != expected.len() {
        return Err(recovery_error("receipt omits a frozen input ordinal"));
    }
    let mut outputs = Vec::new();
    for mut node in recovery.nodes {
        let Some(IngestionInput::Fresh(input)) = snapshots.get(node.snapshot_index) else {
            return Err(recovery_error("fresh checkpoint refers to reused input"));
        };
        let entry = &manifest.entries[node.snapshot_index];
        for snapshot in Arc::make_mut(&mut node.resolution.snapshot_nodes) {
            if snapshot.uuid != entry.snapshot_uuid
                || snapshot.namespace != entry.namespace
                || snapshot.org_id != ctx.org_id.as_ref()
            {
                return Err(recovery_error(
                    "checkpoint snapshot identity differs from manifest",
                ));
            }
            snapshot.content = kg_core::runtime::history::source_content(input)
                .map_err(|_| recovery_error("cannot reconstruct source evidence"))?;
        }
        node.resolution
            .validate_raw_text_drafts(&ctx.extraction_settings)
            .map_err(recovery_error)?;
        outputs.push(PipelineMessage {
            snapshot_index: node.snapshot_index,
            run_id,
            state: StageOutput::NodeResolution(node.resolution),
        });
    }
    for reused in recovery.reused_snapshots {
        let entry = &manifest.entries[reused.snapshot_index];
        if !matches!(&entry.kind, kg_core::runtime::saga::FrozenObservationKind::Existing { evidence_digest } if evidence_digest == &reused.evidence_digest)
            || entry.snapshot_uuid != reused.snapshot_uuid
            || entry.namespace != reused.namespace
        {
            return Err(recovery_error(
                "reused checkpoint differs from accepted evidence",
            ));
        }
        outputs.push(PipelineMessage {
            snapshot_index: reused.snapshot_index,
            run_id,
            state: StageOutput::ReusedSnapshot(reused),
        });
    }
    outputs.sort_by_key(|message| message.snapshot_index);
    Ok(outputs)
}

fn check_cancel(ctx: &RuntimeContext) -> Result<(), PipelineError> {
    if ctx.cancel.is_cancelled() {
        Err(PipelineError::Cancelled)
    } else {
        Ok(())
    }
}

/// Map a persistence failure to the run error, noting an unknown outcome.
fn commit_failure(
    identity: BatchIdentity,
    error: StageError,
    progress: &mut Progress,
) -> PipelineError {
    let step = format!("commit {}#{}", identity.kind.label(), identity.index);
    match error {
        StageError::Cancelled { .. } => PipelineError::Cancelled,
        StageError::CommitOutcomeUnknown { stage, message } => {
            progress.commit_unknown = true;
            PipelineError::StepExecution {
                step,
                stage,
                cause: message,
                retriable: true,
            }
        }
        StageError::CommitRejected { stage, message } => PipelineError::StepExecution {
            step,
            stage,
            cause: message,
            retriable: true,
        },
        StageError::StateValidation { stage, message } => {
            PipelineError::StateValidation { stage, message }
        }
        other => PipelineError::StepExecution {
            step,
            retriable: other.is_retriable(),
            stage: "persist".into(),
            cause: other.to_string(),
        },
    }
}

/// One collection the request declares a complete scan of.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DeclaredScope {
    scan: CollectionScan,
    /// Latest capture time among the collection's pages: tombstones land
    /// when the source was observed without the record, never at commit time.
    captured_at: DateTime<Utc>,
    relationships_complete: bool,
}

impl DeclaredScope {
    fn outcome(&self) -> CollectionOutcome {
        CollectionOutcome {
            collection: self.scan.collection.clone(),
            generation: self.scan.generation,
            swept: true,
            reason: None,
            entities_deleted: 0,
            memberships_released: 0,
            edges_invalidated: 0,
            entities_protected: 0,
        }
    }
}

/// The collections a request declares, in a stable order. Request
/// validation has already made every page of a collection agree on its
/// generation and coverage.
fn declared_scopes(snapshots: &[SnapshotInput]) -> Vec<DeclaredScope> {
    let mut scopes: BTreeMap<CollectionRef, DeclaredScope> = BTreeMap::new();
    for snapshot in snapshots {
        let (Some(collection), Some(generation), Some(scope)) = (
            CollectionRef::of(snapshot),
            snapshot.sync_generation,
            snapshot.collection.as_ref(),
        ) else {
            continue;
        };
        let captured_at = snapshot.captured_at.unwrap_or(DateTime::<Utc>::MIN_UTC);
        scopes
            .entry(collection.clone())
            .and_modify(|e| e.captured_at = e.captured_at.max(captured_at))
            .or_insert(DeclaredScope {
                scan: CollectionScan {
                    collection,
                    generation,
                },
                captured_at,
                relationships_complete: scope.relationships_complete,
            });
    }
    scopes.into_values().collect()
}

/// Stale records of one collection, partitioned by what the sweep may do.
#[derive(Default)]
struct StaleMembers {
    entities: Vec<StaleEntity>,
    released: Vec<StaleEntity>,
    edges: Vec<StaleEdge>,
    live_incident: BTreeMap<Uuid, Vec<Uuid>>,
    relationship_owners: BTreeMap<Uuid, StaleEntity>,
    incident_timelines:
        BTreeMap<Uuid, Vec<kg_core::traits::relationship_timeline::IncidentVersionState>>,
    protected: usize,
}

impl StaleMembers {
    fn is_empty(&self) -> bool {
        self.entities.is_empty() && self.released.is_empty() && self.edges.is_empty()
    }

    /// The next batch within the statement budget; the rest is re-read after
    /// the commit.
    fn within_budget(
        self,
        mut budget: usize,
        scan: CollectionScan,
        captured_at: DateTime<Utc>,
    ) -> Result<ReconciliationBatch, PipelineError> {
        let capacity = budget;
        let mut entities = Vec::new();
        let mut released = Vec::new();
        let mut edges = Vec::new();
        let mut selected = HashSet::new();
        let mut selected_chains = HashSet::new();
        let mut adjacency: std::collections::HashMap<Uuid, Vec<&StaleEdge>> =
            std::collections::HashMap::new();
        for edge in &self.edges {
            adjacency
                .entry(edge.source_chain_id)
                .or_default()
                .push(edge);
            if edge.target_chain_id != edge.source_chain_id {
                adjacency
                    .entry(edge.target_chain_id)
                    .or_default()
                    .push(edge);
            }
        }
        for entity in &self.entities {
            let incident: Vec<_> = adjacency
                .get(&entity.chain_id)
                .into_iter()
                .flatten()
                .copied()
                .filter(|edge| !selected.contains(&edge.uuid))
                .collect();
            let cost = RECONCILE_ENTITY_STATEMENTS + incident.len() * RECONCILE_EDGE_STATEMENTS;
            if cost > capacity {
                return Err(PipelineError::StateValidation {
                    stage: "reconcile".into(),
                    message: format!(
                        "deleting chain {} with its relationships exceeds the reconciliation statement budget",
                        entity.chain_id
                    ),
                });
            }
            if cost > budget {
                continue;
            }
            budget -= cost;
            entities.push(entity.clone());
            selected_chains.insert(entity.chain_id);
            for edge in incident {
                selected.insert(edge.uuid);
                edges.push(edge.clone());
            }
        }
        for entity in self.released {
            if budget < RECONCILE_RELEASE_STATEMENTS {
                break;
            }
            budget -= RECONCILE_RELEASE_STATEMENTS;
            released.push(entity);
        }
        let pending: HashSet<_> = self
            .entities
            .iter()
            .filter(|e| !selected_chains.contains(&e.chain_id))
            .map(|e| e.chain_id)
            .collect();
        let mut guarded_chains = selected_chains.clone();
        for edge in self.edges {
            let guard_cost = 3 * usize::from(!guarded_chains.contains(&edge.source_chain_id));
            if budget < RECONCILE_EDGE_STATEMENTS + guard_cost {
                break;
            }
            if selected.contains(&edge.uuid)
                || pending.contains(&edge.source_chain_id)
                || pending.contains(&edge.target_chain_id)
            {
                continue;
            }
            budget -= RECONCILE_EDGE_STATEMENTS + guard_cost;
            guarded_chains.insert(edge.source_chain_id);
            edges.push(edge);
        }
        if entities.is_empty() && released.is_empty() && edges.is_empty() {
            return Err(PipelineError::StateValidation {
                stage: "reconcile".into(),
                message:
                    "reconciliation budget cannot hold one atomic change and its ownership checks"
                        .into(),
            });
        }
        let mut incident_timelines = BTreeMap::new();
        for chain in &guarded_chains {
            let versions = self.incident_timelines.get(chain).ok_or_else(|| {
                PipelineError::StateValidation {
                    stage: "reconcile".into(),
                    message: "selected deletion lacks its relationship history".into(),
                }
            })?;
            incident_timelines.insert(*chain, versions.clone());
        }
        let live_incident = entities
            .iter()
            .map(|entity| {
                self.live_incident
                    .get(&entity.chain_id)
                    .cloned()
                    .map(|ids| (entity.chain_id, ids))
                    .ok_or_else(|| PipelineError::StateValidation {
                        stage: "reconcile".into(),
                        message: "selected deletion lacks its live relationship baseline".into(),
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let relationship_owners = guarded_chains
            .difference(&selected_chains)
            .map(|chain| {
                self.relationship_owners
                    .get(chain)
                    .cloned()
                    .map(|owner| (*chain, owner))
                    .ok_or_else(|| PipelineError::StateValidation {
                        stage: "reconcile".into(),
                        message: "relationship sweep lacks its source ownership baseline".into(),
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(ReconciliationBatch {
            relationship_owners,
            live_incident,
            incident_timelines,
            scan,
            captured_at,
            entities,
            released,
            edges,
        })
    }
}

fn relationship_timestamp(
    properties: &kg_core::traits::GraphProperties,
    key: &str,
) -> Result<Option<DateTime<Utc>>, PipelineError> {
    match properties.get(key).filter(|value| !value.is_null()) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|time| Some(time.with_timezone(&Utc)))
            .ok_or_else(|| PipelineError::StateValidation {
                stage: "reconcile".into(),
                message: format!("stored relationship has invalid {key}"),
            }),
    }
}

fn relationship_needs_retirement(
    properties: &kg_core::traits::GraphProperties,
    at: DateTime<Utc>,
) -> Result<bool, PipelineError> {
    if relationship_timestamp(properties, "cancelled_at")?.is_some()
        || relationship_timestamp(properties, "deleted_at")?.is_some()
    {
        return Ok(false);
    }
    let start = relationship_timestamp(properties, "valid_from")?.ok_or_else(|| {
        PipelineError::StateValidation {
            stage: "reconcile".into(),
            message: "stored relationship lacks valid_from".into(),
        }
    })?;
    let end = [
        relationship_timestamp(properties, "valid_to")?,
        relationship_timestamp(properties, "invalid_at")?,
    ]
    .into_iter()
    .flatten()
    .min();
    Ok(end.is_none_or(|end| end > at && end > start))
}

fn relationship_chain_id(record: &kg_core::traits::EdgeRecord) -> Result<Uuid, PipelineError> {
    record
        .stored
        .get("chain_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(|| PipelineError::StateValidation {
            stage: "reconcile".into(),
            message: "stored relationship lacks a valid chain_id".into(),
        })
}

/// Everything a run observed: chains seen by any snapshot in any state and
/// relationship chains re-observed. Reconciliation never deletes a chain in
/// this set. It is rebuilt from the run's own stage outputs on every
/// attempt, so a resumed run recovers it before its sweep.
#[derive(Default)]
struct ObservedSet {
    chains: HashSet<Uuid>,
    relationship_chains: HashSet<Uuid>,
}

impl ObservedSet {
    fn record_nodes(&mut self, outputs: &[PipelineMessage]) {
        for msg in outputs {
            let StageOutput::NodeResolution(r) = &msg.state else {
                continue;
            };
            let mut note = |entity: &kg_core::models::EntityNode| {
                self.chains.insert(entity.chain_id);
            };
            r.nodes_to_create
                .iter()
                .for_each(|observation| note(&observation.value));
            r.nodes_new_version.iter().for_each(|rc| note(&rc.entity));
            r.nodes_volatile.iter().for_each(|rc| note(&rc.entity));
            r.nodes_unchanged
                .iter()
                .for_each(|observation| note(&observation.value));
            r.nodes_stale
                .iter()
                .for_each(|observation| note(&observation.value));
            r.nodes_recreated
                .iter()
                .for_each(|observation| note(&observation.value));
            r.nodes_deleted
                .iter()
                .for_each(|observation| note(&observation.value));
        }
    }

    fn record_edges(&mut self, outputs: &[PipelineMessage]) {
        for msg in outputs {
            let StageOutput::EdgeResolution(o) = &msg.state else {
                continue;
            };
            for edge in o.observed.iter() {
                self.relationship_chains.insert(edge.chain_id);
            }
        }
    }
}

/// Incomplete extractions with the input index of the message that carried
/// them; restored checkpoints carry the original index.
fn collect_incomplete(outputs: &[PipelineMessage]) -> Vec<IncompleteSnapshot> {
    outputs
        .iter()
        .filter_map(|m| match &m.state {
            StageOutput::NodeResolution(r) => Some((m.snapshot_index, r)),
            _ => None,
        })
        .flat_map(|(snapshot_index, r)| {
            r.incomplete_extractions
                .iter()
                .map(move |extraction| IncompleteSnapshot {
                    snapshot_index,
                    extraction: extraction.clone(),
                })
        })
        .collect()
}

/// Read current committed targets once for the relationship pass. Historical
/// observations remain on their own messages; they never masquerade as live targets.
async fn attach_run_entities(
    mut chunks: Vec<Vec<PipelineMessage>>,
    ctx: &RuntimeContext,
) -> Result<Vec<Vec<PipelineMessage>>, PipelineError> {
    let mut chains = HashSet::new();
    for message in chunks.iter().flatten() {
        if let StageOutput::NodeResolution(resolution) = &message.state {
            chains.extend(resolution.live_entities().iter().map(|node| node.chain_id));
            chains.extend(resolution.nodes_deleted.iter().map(|node| node.chain_id));
        }
    }
    let mut chains: Vec<_> = chains.into_iter().collect();
    chains.sort_unstable();
    let mut targets = Vec::new();
    for group in chains.chunks(kg_core::traits::graph_reads::MAX_LOOKUP_KEYS) {
        check_cancel(ctx)?;
        let lookup = EntityLookup::LatestByChain {
            chain_ids: group.to_vec(),
        };
        let records = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(PipelineError::Cancelled),
            result = ctx.graph.find_entities(ctx.org_id.as_ref(), &lookup) => result,
        }
        .map_err(|error| PipelineError::StepExecution {
            step: "current relationship targets".into(),
            stage: "edge_pipeline".into(),
            retriable: error.is_transient(),
            cause: error.to_string(),
        })?;
        targets.extend(
            records
                .into_iter()
                .map(kg_core::runtime::stage_output::RelationshipTarget::from),
        );
    }
    targets.sort_by_key(|record| record.chain_id);
    let shared = Arc::new(targets);
    for message in chunks.iter_mut().flatten() {
        if let StageOutput::NodeResolution(resolution) = &mut message.state {
            resolution.chunk_entities = Some(shared.clone());
        }
    }
    Ok(chunks)
}

/// Merge every snapshot's node resolution into one batch. Every observation
/// is kept; persistence orders a chain's observations by capture time.
pub(crate) fn merge_node_batch(outputs: &[PipelineMessage]) -> Result<NodeBatch, PipelineError> {
    let mut batch = NodeBatchParts::default();
    let mut identity_revisions = BTreeMap::new();
    for msg in outputs {
        if matches!(msg.state, StageOutput::ReusedSnapshot(_)) {
            continue;
        }
        let StageOutput::NodeResolution(r) = &msg.state else {
            return Err(PipelineError::StateValidation {
                stage: "persist".into(),
                message: format!(
                    "snapshot {} reached the node commit with {} instead of a node resolution",
                    msg.snapshot_index,
                    state_kind(&msg.state)
                ),
            });
        };
        for revision in r.identity_revisions.iter() {
            if identity_revisions
                .insert(revision.scope.clone(), revision.revision)
                .is_some_and(|previous| previous != revision.revision)
            {
                return Err(PipelineError::IdentityRevisionChanged);
            }
        }
        batch.fk_exclusions.extend(
            r.fk_exclusions
                .iter()
                .map(|(id, paths)| (*id, paths.clone())),
        );
        batch
            .snapshot_nodes
            .extend(r.snapshot_nodes.iter().cloned());
        batch
            .nodes_to_create
            .extend(r.nodes_to_create.iter().cloned());
        batch
            .nodes_new_version
            .extend(r.nodes_new_version.iter().cloned());
        batch
            .nodes_volatile
            .extend(r.nodes_volatile.iter().cloned());
        batch
            .nodes_unchanged
            .extend(r.nodes_unchanged.iter().cloned());
        batch.nodes_stale.extend(r.nodes_stale.iter().cloned());
        batch
            .nodes_recreated
            .extend(r.nodes_recreated.iter().cloned());
        batch
            .pending_child_edges
            .extend(r.sub_edges.iter().cloned());
        batch.nodes_deleted.extend(r.nodes_deleted.iter().cloned());
        batch.chains_merged.extend(r.chains_merged.iter().cloned());
        batch
            .observed_properties
            .extend(r.observed_properties.iter().cloned());
    }
    Ok(NodeBatch {
        fk_exclusions: Arc::new(batch.fk_exclusions),
        pending_child_edges: Arc::new(batch.pending_child_edges),
        identity_revisions: Arc::new(
            identity_revisions
                .into_iter()
                .map(|(scope, revision)| kg_core::traits::IdentityRevision { scope, revision })
                .collect(),
        ),
        observed_properties: Arc::new(batch.observed_properties),
        snapshot_nodes: Arc::new(batch.snapshot_nodes),
        nodes_to_create: Arc::new(batch.nodes_to_create),
        nodes_new_version: Arc::new(batch.nodes_new_version),
        nodes_volatile: Arc::new(batch.nodes_volatile),
        nodes_unchanged: Arc::new(batch.nodes_unchanged),
        nodes_stale: Arc::new(batch.nodes_stale),
        nodes_recreated: Arc::new(batch.nodes_recreated),
        nodes_deleted: Arc::new(batch.nodes_deleted),
        chains_merged: Arc::new(batch.chains_merged),
    })
}

#[derive(Default)]
struct NodeBatchParts {
    fk_exclusions: HashMap<Uuid, Vec<String>>,
    pending_child_edges: Vec<kg_core::models::EntityEdge>,
    observed_properties: Vec<kg_core::runtime::stage_output::ObservedEntityProperties>,
    snapshot_nodes: Vec<kg_core::models::SnapshotNode>,
    nodes_to_create: Vec<kg_core::runtime::stage_output::Observed<kg_core::models::EntityNode>>,
    nodes_new_version: Vec<
        kg_core::runtime::stage_output::Observed<kg_core::runtime::stage_output::ResolvedChange>,
    >,
    nodes_volatile: Vec<
        kg_core::runtime::stage_output::Observed<kg_core::runtime::stage_output::ResolvedChange>,
    >,
    nodes_unchanged: Vec<kg_core::runtime::stage_output::Observed<kg_core::models::EntityNode>>,
    nodes_stale: Vec<kg_core::runtime::stage_output::Observed<kg_core::models::EntityNode>>,
    nodes_recreated: Vec<kg_core::runtime::stage_output::Observed<kg_core::models::EntityNode>>,
    nodes_deleted: Vec<kg_core::runtime::stage_output::Observed<kg_core::models::EntityNode>>,
    chains_merged: Vec<ChainsMerged>,
}

/// Merge every snapshot's relationship observations into one batch.
///
/// Every observation is kept: persistence orders a pair's observations by
/// capture time and keeps one current target per single-target relation.
/// The snapshots' baselines must agree; a pair or relation two snapshots
/// read differently was changed by a concurrent writer, and the chunk must
/// be resolved again. Without relationship stages the node results pass
/// through unchanged and the batch is empty.
pub(crate) fn merge_relationship_batch(
    outputs: &[PipelineMessage],
    no_edge_stages: bool,
) -> Result<RelationshipBatch, PipelineError> {
    let mut snapshot_nodes = Vec::new();
    let mut observed = Vec::new();
    let mut relationship_directives = Vec::new();
    let mut relationship_assessments = Vec::new();
    let mut contradiction_timelines = std::collections::BTreeMap::new();
    let mut baseline = RelationshipBaseline::default();
    let mut baselines = Vec::with_capacity(outputs.len());
    let mut reference_report = ReferenceReport::default();
    for msg in outputs {
        let o = match &msg.state {
            StageOutput::EdgeResolution(o) => o,
            StageOutput::NodeResolution(_) if no_edge_stages => continue,
            other => {
                return Err(PipelineError::StateValidation {
                    stage: "persist".into(),
                    message: format!(
                        "snapshot {} reached the relationship commit with {} instead of an edge resolution",
                        msg.snapshot_index,
                        state_kind(other)
                    ),
                });
            }
        };
        snapshot_nodes.extend(o.snapshot_nodes.iter().cloned());
        observed.extend(o.observed.iter().cloned());
        relationship_directives.extend(o.relationship_directives.iter().cloned());
        relationship_assessments.extend(o.relationship_assessments.iter().cloned());
        reference_report.merge(&o.reference_report);
        for (anchor, versions) in &o.contradiction_timelines {
            if contradiction_timelines
                .insert(*anchor, versions.clone())
                .is_some_and(|prior| prior != *versions)
            {
                return Err(PipelineError::IdentityRevisionChanged);
            }
        }
        baselines.push(o.baseline.as_ref());
    }
    baseline
        .merge_all(baselines)
        .map_err(|_| PipelineError::IdentityRevisionChanged)?;
    Ok(RelationshipBatch {
        relationship_assessments: Arc::new(relationship_assessments),
        contradiction_timelines,
        relationship_directives: Arc::new(relationship_directives),
        reference_report,
        snapshot_nodes: Arc::new(snapshot_nodes),
        observed: Arc::new(observed),
        baseline: Arc::new(baseline),
    })
}

impl Default for PipelineRunner {
    fn default() -> Self {
        Self::new()
    }
}

fn identity_budget_error() -> PipelineError {
    PipelineError::StepExecution {
        stage: "identity_resolution".into(),
        step: "identity_budget".into(),
        cause: "identity resolution deadline exceeded before commit".into(),
        retriable: true,
    }
}

fn relationship_budget_error() -> PipelineError {
    PipelineError::StepExecution {
        stage: "relationship_resolution".into(),
        step: "relationship_budget".into(),
        cause: "relationship resolution deadline exceeded before commit".into(),
        retriable: true,
    }
}

/// Variant-only diagnostics avoid copying source content into errors and logs.
fn state_kind(state: &StageOutput) -> &'static str {
    match state {
        StageOutput::CommunityRequest(_) => "Community request",
        StageOutput::CommunityClusters(_) => "Community clusters",
        StageOutput::CommunityDrafts(_) => "Community drafts",
        StageOutput::CommunityPrepared(_) => "Community prepared",
        StageOutput::SagaSummaryBatch(_) => "Saga summary batch",
        StageOutput::ReusedSnapshot(_) => "reused snapshot",
        StageOutput::Empty => "empty",
        StageOutput::Input(_) => "input",
        StageOutput::ValidatedInput(_) => "validated_input",
        StageOutput::PreparedSnapshot(_) => "prepared snapshot",
        StageOutput::StructuredDrafts(_) => "structured drafts",
        StageOutput::TextDrafts(_) => "text drafts",
        StageOutput::NodeExtraction(_) => "node extraction",
        StageOutput::NodeIdentity(_) => "node identity",
        StageOutput::NodeResolution(_) => "node resolution",
        StageOutput::EdgeExtraction(_) => "edge extraction",
        StageOutput::EdgeResolution(_) => "edge resolution",
        StageOutput::FlushBatch(_) => "flush batch",
        StageOutput::SummaryBatch(_) => "summary batch",
        StageOutput::PlannedBatch(_) => "planned batch",
        StageOutput::PreparedBatch(_) => "prepared batch",
        StageOutput::Committed(_) => "committed",
    }
}

/// Both stale reads and rejected commits consume the same frozen-chunk retry budget.
async fn wait_resolution_replan(
    ctx: &RuntimeContext,
    identity: BatchIdentity,
    attempt: u32,
    deadline: tokio::time::Instant,
) -> Result<(), PipelineError> {
    let relationship = identity.kind == BatchKind::Relationship;
    let phase = if relationship {
        "relationship_resolution"
    } else {
        "identity_resolution"
    };
    let budget_error = || {
        if relationship {
            relationship_budget_error()
        } else {
            identity_budget_error()
        }
    };
    check_cancel(ctx)?;
    if tokio::time::Instant::now() >= deadline {
        return Err(budget_error());
    }
    if attempt >= ctx.matching_settings.max_replans {
        tracing::warn!(
            batch_index = identity.index,
            attempts = attempt + 1,
            phase,
            "resolution replanning exhausted"
        );
        return Err(PipelineError::RetryExhausted {
            step: phase.into(),
            attempts: attempt + 1,
            last_error: "graph evidence changed before commit".into(),
        });
    }
    tracing::info!(
        batch_index = identity.index,
        attempt = attempt + 1,
        phase,
        "graph evidence changed; recomputing resolution"
    );
    let wake = tokio::time::Instant::now()
        + std::time::Duration::from_millis(25 * (u64::from(attempt) + 1));
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(PipelineError::Cancelled),
        _ = tokio::time::sleep_until(deadline) => Err(budget_error()),
        _ = tokio::time::sleep_until(wake) => Ok(()),
    }
}

fn failure_kind(error: &PipelineError) -> &'static str {
    match error.root_cause() {
        PipelineError::Cancelled => "cancelled",
        PipelineError::StateValidation { .. } => "validation",
        PipelineError::TaskPanic(_) => "panic",
        PipelineError::RetryExhausted { .. } => "retry_exhausted",
        PipelineError::StepExecution { .. } | PipelineError::StageExecution { .. } => "stage",
        _ => "other",
    }
}

#[path = "saga_followup.rs"]
mod saga_followup;
#[path = "summary_followup.rs"]
mod summary_followup;

#[path = "community_followup.rs"]
mod community_followup;

#[path = "reference_followup.rs"]
mod reference_followup;
pub use reference_followup::ReferenceRebuildProgress;

#[cfg(test)]
mod tests;

/// Bounds the owner queue independently of token/database page sizes. Hitting
/// either limit fails explicitly with committed counts; it never declares
/// discovery complete or retires references on a partial set.
type ReferenceRepairQueue = BTreeMap<
    Uuid,
    (
        Option<Uuid>,
        DateTime<Utc>,
        BTreeSet<ReferenceOwnerSelector>,
    ),
>;

fn validate_reference_work(waiting: &ReferenceRepairQueue) -> Result<(), PipelineError> {
    const MAX_OWNERS: usize = 10_000;
    const MAX_BYTES: usize = 16 * 1024 * 1024;
    let bytes = waiting.values().try_fold(0usize, |total, (_, _, slots)| {
        slots
            .iter()
            .try_fold(total.checked_add(128)?, |size, slot| {
                size.checked_add(128)?
                    .checked_add(slot.namespace.len())?
                    .checked_add(slot.slot.len())
            })
    });
    if waiting.len() > MAX_OWNERS || bytes.is_none_or(|size| size > MAX_BYTES) {
        return Err(recovery_error("reference repair work budget exceeded (10000 owners / 16 MiB); required repair is incomplete; split the target input into smaller runs"));
    }
    Ok(())
}

/// Database adapters own string collation. Detect repeated/cycling cursors
/// without imposing Rust's Unicode order, and bound the tracking memory.
#[derive(Default)]
struct ReferenceCursorProgress {
    seen: BTreeSet<kg_core::traits::UnresolvedReferenceCursor>,
    bytes: usize,
}
impl ReferenceCursorProgress {
    fn advance(
        &mut self,
        next: &kg_core::traits::UnresolvedReferenceCursor,
    ) -> Result<(), PipelineError> {
        self.bytes = self
            .bytes
            .saturating_add(128)
            .saturating_add(next.slot.len())
            .saturating_add(next.token.len());
        if self.seen.len() >= 10_000 || self.bytes > 16 * 1024 * 1024 {
            return Err(recovery_error(
                "reference repair cursor budget exceeded; required repair is incomplete",
            ));
        }
        if !self.seen.insert(next.clone()) {
            return Err(recovery_error(
                "reference repair cursor repeated; required repair is incomplete",
            ));
        }
        Ok(())
    }
}

/// Reads can be cancelled, unlike a submitted commit whose outcome must be
/// awaited. Apply the same deadline even to custom GraphBackend adapters.
async fn reference_read<T>(
    ctx: &RuntimeContext,
    step: &str,
    read: impl std::future::Future<Output = Result<T, BackendError>>,
) -> Result<T, PipelineError> {
    let millis = ctx.context_settings.read_timeout_ms;
    tokio::select! {
        biased;
        _=ctx.cancel.cancelled()=>Err(PipelineError::Cancelled),
        result=tokio::time::timeout(std::time::Duration::from_millis(millis),read)=>{
            result.unwrap_or(Err(BackendError::Timeout(millis))).map_err(|e|PipelineError::StepExecution {
                stage:"reference_repair".into(),step:step.into(),cause:e.to_string(),retriable:e.is_transient(),
            })
        }
    }
}
