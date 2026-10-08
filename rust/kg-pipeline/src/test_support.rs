//! Test-only stage implementations for validating the pipeline runner.
//!
//! `MaterializeStage` and `RecordingFlush` exercise the runner's orchestration
//! (passes, chunking, batch identity, abort reporting) without a database;
//! database behavior is covered by the stage crate's tests against the real
//! persistence stage. `UnreachableGraph` fails every storage call, so a phase
//! test that reaches storage by mistake fails instead of passing on a fake.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use kg_core::errors::{BackendError, StageError};
use kg_core::identity::IdentityHash;
use kg_core::models::{EntityNode, SnapshotNode};
use kg_core::models::{SnapshotDataType, SnapshotKind};
use kg_core::pipeline::CommittedCounts;
use kg_core::runtime::stage_output::{CommitOutput, FlushWork, NodeResolutionOutput};
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::{
    BatchIdentity, BatchKind, CommittedBatch, EdgeLookup, EdgeRecord, EntityLookup,
    EntityVersionRecord, GraphBackend, GraphMutation, MutationBatch, RunHeader, RunRegistration,
    SearchBackend, Stage,
};

/// A graph no test may reach: every call fails. The phase tests build their
/// context over it because they never touch storage; the runner tests use a
/// live Neo4j instead.
pub struct UnreachableGraph;

fn no_graph<T>() -> Result<T, BackendError> {
    Err(BackendError::Unavailable(
        "this test has no graph; storage must not be reached".into(),
    ))
}

#[async_trait]
impl SearchBackend for UnreachableGraph {}

#[async_trait]
impl GraphBackend for UnreachableGraph {
    async fn apply_mutations(&self, _: &str, _: &[GraphMutation]) -> Result<(), BackendError> {
        no_graph()
    }

    async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
        no_graph()
    }

    async fn commit_batch(&self, _: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        no_graph()
    }

    async fn committed_batches(
        &self,
        _: &str,
        _: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        no_graph()
    }

    async fn find_entities(
        &self,
        _: &str,
        _: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        no_graph()
    }

    async fn find_edges(&self, _: &str, _: &EdgeLookup) -> Result<Vec<EdgeRecord>, BackendError> {
        no_graph()
    }

    async fn health(&self) -> Result<(), BackendError> {
        no_graph()
    }

    async fn connect(&self) -> Result<(), BackendError> {
        no_graph()
    }

    async fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
}

/// A stage that counts how many times it was called.
pub struct CountingStage {
    name: String,
    pub count: Arc<AtomicUsize>,
}

impl CountingStage {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Stage for CountingStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn process(
        &self,
        input: StageOutput,
        _ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(input)
    }
}

/// A stage that adds a small delay (simulates LLM call).
pub struct SlowStage {
    name: String,
    delay_ms: u64,
    pub count: Arc<AtomicUsize>,
}

impl SlowStage {
    pub fn new(name: impl Into<String>, delay_ms: u64) -> Self {
        Self {
            name: name.into(),
            delay_ms,
            count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Stage for SlowStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn process(
        &self,
        input: StageOutput,
        _ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        tokio::time::sleep(tokio::time::Duration::from_millis(self.delay_ms)).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(input)
    }
}

/// A stage that always fails.
pub struct FailingStage {
    name: String,
}

impl FailingStage {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl Stage for FailingStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn process(
        &self,
        _input: StageOutput,
        _ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        Err(StageError::StepFailed {
            stage: self.name.clone(),
            step: "fail".into(),
            cause: "intentional failure".into(),
            retriable: false,
        })
    }
}

/// Fails every Nth call, used by the continue-on-error tests.
pub struct FailEveryNth {
    name: String,
    nth: usize,
    pub calls: Arc<AtomicUsize>,
}

impl FailEveryNth {
    pub fn new(name: impl Into<String>, nth: usize) -> Self {
        Self {
            name: name.into(),
            nth,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Stage for FailEveryNth {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn process(
        &self,
        input: StageOutput,
        _ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if (call + 1).is_multiple_of(self.nth) {
            return Err(StageError::StateValidation {
                stage: self.name.clone(),
                message: format!("synthetic failure on call {call}"),
            });
        }
        Ok(input)
    }
}

/// Panics on every call — pins the TaskPanic containment path.
pub struct PanickingStage;

#[async_trait::async_trait]
impl Stage for PanickingStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        "panicking"
    }

    async fn process(
        &self,
        _input: StageOutput,
        _ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        panic!("synthetic stage panic");
    }
}

/// Sleeps a per-call pseudo-random duration so completion order scrambles —
/// pins the deterministic output ordering contract.
pub struct JitterStage {
    pub count: Arc<AtomicUsize>,
}

impl JitterStage {
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Default for JitterStage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Stage for JitterStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        "jitter"
    }

    async fn process(
        &self,
        input: StageOutput,
        _ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let n = self.count.fetch_add(1, Ordering::SeqCst);
        // Earlier calls sleep LONGER — guarantees out-of-order completion.
        tokio::time::sleep(std::time::Duration::from_millis(((7 - (n % 8)) * 3) as u64)).await;
        Ok(input)
    }
}

/// Turns each input snapshot into a node resolution with one new entity
/// named after the snapshot, the minimum the node commit accepts.
pub struct MaterializeStage;

#[async_trait::async_trait]
impl Stage for MaterializeStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::Input, StageKind::NodeResolution)]
    }

    fn name(&self) -> &str {
        "materialize"
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::Input(snapshot) = input else {
            return Err(StageError::StateValidation {
                stage: "materialize".into(),
                message: "expected Input".into(),
            });
        };
        let captured_at = snapshot
            .captured_at
            .or_else(|| ctx.frozen_observation().map(|entry| entry.captured_at))
            .ok_or_else(|| StageError::StateValidation {
                stage: "materialize".into(),
                message: "materialization requires explicit or frozen capture time".into(),
            })?;
        let node = SnapshotNode {
            uuid: ctx
                .frozen_observation()
                .map(|entry| entry.snapshot_uuid)
                .unwrap_or_else(Uuid::new_v4),
            org_id: ctx.org_id.to_string(),
            namespace: snapshot.namespace.clone(),
            name: snapshot.name.clone(),
            source_description: None,
            data_type: SnapshotDataType::Entities,
            snapshot_kind: SnapshotKind::Incremental,
            sync_generation: None,
            complete: false,
            collection: None,
            source: snapshot.source.clone(),
            content: None,
            captured_at,
            entities: vec![],
            entity_edges: vec![],
            labels: vec![],
            tags: Default::default(),
            created_at: captured_at,
        };
        let entity = EntityNode {
            labels: Vec::new(),
            inherited_labels: Vec::new(),
            uuid: Uuid::new_v4(),
            chain_id: Uuid::new_v4(),
            org_id: ctx.org_id.to_string(),
            namespace: snapshot.namespace.clone(),
            entity_type: "Thing".into(),
            name: snapshot.name.clone(),
            all_properties: Default::default(),
            primary_key_properties: vec!["name".into()],
            additional_key_properties: vec![],
            identity_hash: IdentityHash::compute(
                ctx.org_id.as_ref(),
                &snapshot.namespace,
                "Thing",
                &[("name", snapshot.name.as_str())],
            ),
            lifecycle: Default::default(),
            version: 1,
            is_latest: true,
            previous_version_uuid: None,
            embedding: None,
            valid_from: captured_at,
            valid_to: None,
            deleted_at: None,
            deleted_by: None,
            deletion_reason: None,
            source: snapshot.source.clone(),
            extracted_by: "test".into(),
            resolved_by: None,
            first_seen_snapshot_id: Some(node.uuid),
            last_seen_snapshot_id: Some(node.uuid),
            last_seen_at: None,
            sync_generation: None,
            tags: Default::default(),
            summary: None,
            structural_hash: 1,
            needs_llm_review: false,
            collections: Vec::new(),
        };
        Ok(StageOutput::NodeResolution(NodeResolutionOutput {
            relationship_changes: Default::default(),
            observed_properties: Arc::new(vec![
                kg_core::runtime::stage_output::ObservedEntityProperties::from_entity(
                    &entity, node.uuid,
                ),
            ]),
            history: Default::default(),
            snapshot_nodes: Arc::new(vec![node]),
            nodes_to_create: Arc::new(vec![kg_core::runtime::stage_output::Observed::new(
                entity.uuid,
                entity,
            )]),
            ..Default::default()
        }))
    }
}

/// How the recording flush answers one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(feature = "live-tests"), allow(dead_code))]
pub enum FlushBehavior {
    Commit,
    Replay,
    Fail,
    Unknown,
}

/// Records every batch identity the runner hands to persistence and answers
/// with synthetic counts. It performs no graph writes.
pub struct RecordingFlush {
    pub batches: Mutex<Vec<BatchIdentity>>,
    behaviors: Mutex<Vec<(BatchKind, u32, FlushBehavior)>>,
    default: FlushBehavior,
}

impl RecordingFlush {
    pub fn new() -> Self {
        Self::with_default(FlushBehavior::Commit)
    }

    pub fn with_default(default: FlushBehavior) -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            behaviors: Mutex::new(Vec::new()),
            default,
        }
    }

    #[cfg(feature = "live-tests")]
    pub fn on(self, kind: BatchKind, index: u32, behavior: FlushBehavior) -> Self {
        self.behaviors.lock().unwrap().push((kind, index, behavior));
        self
    }

    #[cfg(feature = "live-tests")]
    pub fn recorded(&self) -> Vec<BatchIdentity> {
        self.batches.lock().unwrap().clone()
    }
}

impl Default for RecordingFlush {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Stage for RecordingFlush {
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::FlushBatch, StageKind::Committed)]
    }

    fn name(&self) -> &str {
        "recording_flush"
    }

    async fn process(
        &self,
        input: StageOutput,
        _ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::FlushBatch(flush) = input else {
            return Err(StageError::StateValidation {
                stage: "recording_flush".into(),
                message: "expected FlushBatch".into(),
            });
        };
        let identity = flush.batch;
        let mut counts = CommittedCounts::default();
        match (&flush.work, identity.kind) {
            (FlushWork::Nodes(batch), BatchKind::Node) => {
                counts.snapshots = batch.snapshot_nodes.len();
                counts.entities_created = batch.nodes_to_create.len();
                counts.entities_unchanged = batch.nodes_unchanged.len();
            }
            (FlushWork::Relationships(batch), BatchKind::Relationship) => {
                counts.edges_created = batch.observed.len();
            }
            (FlushWork::Reconciliation(batch), BatchKind::Reconciliation) => {
                counts.entities_deleted = batch.entities.len();
            }
            _ => {
                return Err(StageError::StateValidation {
                    stage: "recording_flush".into(),
                    message: "batch kind does not match its work".into(),
                })
            }
        }
        self.batches.lock().unwrap().push(identity);
        let behavior = self
            .behaviors
            .lock()
            .unwrap()
            .iter()
            .find(|(kind, index, _)| *kind == identity.kind && *index == identity.index)
            .map(|(_, _, behavior)| *behavior)
            .unwrap_or(self.default);
        match behavior {
            FlushBehavior::Commit | FlushBehavior::Replay => {
                Ok(StageOutput::Committed(CommitOutput {
                    recovery: flush.recovery,
                    batch: identity,
                    replayed: behavior == FlushBehavior::Replay,
                    committed_at: Utc::now(),
                    counts,
                }))
            }
            FlushBehavior::Fail => Err(StageError::StepFailed {
                stage: "recording_flush".into(),
                step: "commit".into(),
                cause: "injected commit failure".into(),
                retriable: true,
            }),
            FlushBehavior::Unknown => Err(StageError::CommitOutcomeUnknown {
                stage: "recording_flush".into(),
                message: "injected acknowledgement loss".into(),
            }),
        }
    }
}
