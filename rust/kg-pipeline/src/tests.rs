mod topology;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use kg_core::errors::PipelineError;
use kg_core::models::SnapshotDataType;
use kg_core::models::SnapshotInput;
use kg_core::runtime::{RuntimeContext, RuntimeContextBuilder, StageOutput};
#[cfg(feature = "live-tests")]
use kg_core::traits::BatchKind;
use kg_core::traits::Stage;
use uuid::Uuid;

use kg_core::test_support::{MockEmbedBackend, MockLlmBackend};
#[cfg(feature = "live-tests")]
use kg_storage_neo4j::Neo4jGraphBackend;

use crate::message::PipelineMessage;

use crate::phase::run_phase;
use crate::runner::PipelineRunner;
#[cfg(feature = "live-tests")]
use crate::runner::PipelineRunnerConfig;
#[cfg(feature = "live-tests")]
use crate::test_support::FlushBehavior;
use crate::test_support::{
    CountingStage, FailingStage, MaterializeStage, RecordingFlush, SlowStage, UnreachableGraph,
};

fn ctx_over(graph: Arc<dyn kg_core::traits::GraphBackend>) -> Arc<RuntimeContext> {
    let llm = Arc::new(MockLlmBackend::empty());
    Arc::new(
        RuntimeContextBuilder::new("test-org")
            .graph(graph)
            .llm_extraction(llm.clone())
            .llm_default(llm)
            .embedder(Arc::new(MockEmbedBackend::default_dimension()))
            .build()
            .expect("failed to build test RuntimeContext"),
    )
}

/// Mock providers and no graph: the phase tests never reach storage.
fn test_ctx() -> Arc<RuntimeContext> {
    ctx_over(Arc::new(UnreachableGraph))
}

/// Mock providers over the disposable Neo4j the stage suites use; the runner
/// registers runs and reads receipts there. Runs use fresh ids, so the
/// database is neither emptied nor leased.
#[cfg(feature = "live-tests")]
async fn live_ctx() -> Arc<RuntimeContext> {
    let uri = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .uri;
    let password = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .password;
    let user = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .user;
    let graph = Neo4jGraphBackend::new(&uri, &user, &password)
        .await
        .expect("connect to the disposable Neo4j");
    graph.ensure_indexes().await.expect("indexes");
    ctx_over(Arc::new(graph))
}

fn make_snapshots(count: usize) -> Vec<SnapshotInput> {
    (0..count)
        .map(|i| SnapshotInput {
            relationship_changes: Default::default(),
            saga: None,
            previous_snapshot_uuids: vec![],
            labels: Vec::new(),
            tags: Default::default(),
            namespace: "test".into(),
            name: format!("snapshot-{i}"),
            source_description: None,
            data_type: SnapshotDataType::Entities,
            source: "test".into(),
            entities: vec![],
            content: None,
            entity_types: None,
            edge_types: None,
            edge_type_map: None,
            exclude_fk_properties: vec![],
            ignore_change_properties: vec![],
            snapshot_kind: Default::default(),
            sync_generation: None,
            complete: false,
            org_id: None,
            captured_at: None,
            collection: None,
        })
        .collect()
}

fn make_messages(count: usize) -> Vec<PipelineMessage> {
    let snapshots = make_snapshots(count);
    snapshots
        .into_iter()
        .enumerate()
        .map(|(i, _ep)| PipelineMessage {
            snapshot_index: i,
            run_id: Uuid::new_v4(),
            state: StageOutput::Empty,
        })
        .collect()
}

#[cfg(feature = "live-tests")]
fn runner(chunk_size: usize, flush: Arc<dyn Stage>) -> PipelineRunner {
    PipelineRunner::new()
        .with_config(PipelineRunnerConfig {
            chunk_size,
            channel_capacity: 8,
            stage_concurrency: 2,
            ..PipelineRunnerConfig::default()
        })
        .node_stage(Arc::new(MaterializeStage))
        .flush(flush)
}

// Stage-channel contracts.

#[tokio::test]
async fn phase_single_stage_processes_all_inputs() {
    let ctx = test_ctx();
    let stage = Arc::new(CountingStage::new("counter"));
    let count = stage.count.clone();

    let inputs = make_messages(10);
    let (outputs, failures) = run_phase(&[stage as Arc<dyn Stage>], inputs, ctx, None, None)
        .await
        .unwrap();

    assert_eq!(outputs.len(), 10);
    assert!(failures.is_empty());
    assert_eq!(count.load(Ordering::Relaxed), 10);
}

#[tokio::test]
async fn phase_multiple_stages_chain() {
    let ctx = test_ctx();
    let s1 = Arc::new(CountingStage::new("s1"));
    let s2 = Arc::new(CountingStage::new("s2"));
    let s3 = Arc::new(CountingStage::new("s3"));
    let c1 = s1.count.clone();
    let c2 = s2.count.clone();
    let c3 = s3.count.clone();

    let inputs = make_messages(5);
    let stages: Vec<Arc<dyn Stage>> = vec![s1, s2, s3];

    let (outputs, _) = run_phase(&stages, inputs, ctx, None, None).await.unwrap();

    assert_eq!(outputs.len(), 5);
    assert_eq!(c1.load(Ordering::Relaxed), 5);
    assert_eq!(c2.load(Ordering::Relaxed), 5);
    assert_eq!(c3.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn phase_empty_stages_passes_through() {
    let ctx = test_ctx();
    let inputs = make_messages(3);

    let (outputs, _) = run_phase(&[], inputs, ctx, None, None).await.unwrap();

    assert_eq!(outputs.len(), 3);
}

#[tokio::test]
async fn phase_slow_stage_still_completes() {
    let ctx = test_ctx();
    let slow = Arc::new(SlowStage::new("slow", 10));
    let count = slow.count.clone();

    let inputs = make_messages(5);
    let stages: Vec<Arc<dyn Stage>> = vec![slow];

    let (outputs, _) = run_phase(&stages, inputs, ctx, None, None).await.unwrap();

    assert_eq!(outputs.len(), 5);
    assert_eq!(count.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn phase_failing_stage_propagates_error() {
    let ctx = test_ctx();
    let failing = Arc::new(FailingStage::new("boom"));
    let stages: Vec<Arc<dyn Stage>> = vec![failing];

    let inputs = make_messages(3);
    let result = run_phase(&stages, inputs, ctx, None, None).await;

    assert!(result.is_err());
}

/// A failing worker in the middle of a longer chain must not leave the
/// feeder or the other workers running: the call returns the error after
/// every task stopped, even with a slow upstream and a tiny channel.
#[tokio::test]
async fn phase_failure_aborts_and_joins_every_task() {
    let ctx = test_ctx();
    let slow = Arc::new(SlowStage::new("slow", 5));
    let stages: Vec<Arc<dyn Stage>> = vec![
        slow.clone(),
        Arc::new(FailingStage::new("boom")),
        Arc::new(CountingStage::new("after")),
    ];
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_phase(&stages, make_messages(64), ctx, Some(1), Some(1)),
    )
    .await
    .expect("phase returns promptly after a failure");
    assert!(result.is_err());
    assert!(
        slow.count.load(Ordering::Relaxed) < 64,
        "upstream stopped before draining every input"
    );
}

// Runner contracts.

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_two_pass_execution_commits_node_then_relationship_batches() {
    let ctx = live_ctx().await;
    let node_s2 = Arc::new(CountingStage::new("after_materialize"));
    let edge_s1 = Arc::new(CountingStage::new("edge_extract"));
    let n2_count = node_s2.count.clone();
    let e1_count = edge_s1.count.clone();
    let flush = Arc::new(RecordingFlush::new());

    let runner = PipelineRunner::new()
        .with_config(PipelineRunnerConfig {
            chunk_size: 100,
            channel_capacity: 32,
            stage_concurrency: 5,
            ..PipelineRunnerConfig::default()
        })
        .node_stage(Arc::new(MaterializeStage))
        .node_stage(node_s2)
        .edge_stage(edge_s1)
        .flush(flush.clone());

    // Invalid relationship handoffs reject before even the node pass can write.
    let error = runner
        .run(make_snapshots(3), ctx.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(error.root_cause(), PipelineError::StateValidation { .. }),
        "{error}"
    );
    assert_eq!(n2_count.load(Ordering::Relaxed), 0);
    assert_eq!(e1_count.load(Ordering::Relaxed), 0);
    assert!(
        flush.recorded().is_empty(),
        "invalid topology wrote a node batch"
    );

    let flush = Arc::new(RecordingFlush::new());
    let runner = runner_without_edges(100, flush.clone());
    let output = runner.run(make_snapshots(10), ctx).await.unwrap();
    assert_eq!(output.snapshots_completed, 10);
    assert!(output.is_complete());
    assert_eq!(output.committed.entities_created, 10);
    assert_eq!(output.committed.snapshots, 10);
    assert_eq!(output.newly_committed, output.committed);
    let kinds: Vec<(BatchKind, u32)> = flush.recorded().iter().map(|b| (b.kind, b.index)).collect();
    assert_eq!(
        kinds,
        vec![(BatchKind::Node, 0), (BatchKind::Relationship, 0)],
        "node batch commits before the relationship batch"
    );
    assert_eq!(output.batches.len(), 2);
}

#[cfg(feature = "live-tests")]

fn runner_without_edges(chunk_size: usize, flush: Arc<RecordingFlush>) -> PipelineRunner {
    runner(chunk_size, flush)
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_chunking_commits_one_batch_pair_per_chunk() {
    let ctx = live_ctx().await;
    let flush = Arc::new(RecordingFlush::new());
    let runner = runner(3, flush.clone());

    let output = runner.run(make_snapshots(10), ctx).await.unwrap();

    assert_eq!(output.snapshots_completed, 10);
    assert_eq!(output.committed.entities_created, 10);
    let recorded = flush.recorded();
    let nodes: Vec<u32> = recorded
        .iter()
        .filter(|b| b.kind == BatchKind::Node)
        .map(|b| b.index)
        .collect();
    let rels: Vec<u32> = recorded
        .iter()
        .filter(|b| b.kind == BatchKind::Relationship)
        .map(|b| b.index)
        .collect();
    assert_eq!(nodes, vec![0, 1, 2, 3]);
    assert_eq!(rels, vec![0, 1, 2, 3]);
    // Every node batch commits before any relationship batch.
    let first_rel = recorded
        .iter()
        .position(|b| b.kind == BatchKind::Relationship)
        .unwrap();
    assert!(recorded[..first_rel]
        .iter()
        .all(|b| b.kind == BatchKind::Node));
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_cancellation_reports_committed_progress() {
    let ctx = live_ctx().await;
    struct CancelAfterCommit(RecordingFlush);
    #[async_trait::async_trait]
    impl Stage for CancelAfterCommit {
        fn contract(&self) -> kg_core::traits::StageContract {
            use kg_core::traits::StageKind;
            &[(StageKind::FlushBatch, StageKind::Committed)]
        }

        fn name(&self) -> &str {
            "cancel_after_commit"
        }
        async fn process(
            &self,
            input: StageOutput,
            ctx: &RuntimeContext,
        ) -> Result<StageOutput, kg_core::errors::StageError> {
            let result = self.0.process(input, ctx).await;
            ctx.cancel.cancel();
            result
        }
    }
    let runner = runner(2, Arc::new(CancelAfterCommit(RecordingFlush::new())));
    let error = runner.run(make_snapshots(5), ctx).await.unwrap_err();
    match error {
        PipelineError::Aborted {
            batches_committed,
            commit_unknown,
            cause,
            ..
        } => {
            assert_eq!(batches_committed, 1);
            assert!(!commit_unknown);
            assert!(matches!(*cause, PipelineError::Cancelled));
        }
        other => panic!("expected Aborted, got {other:?}"),
    }
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_empty_input_completes_without_batches() {
    let ctx = live_ctx().await;
    let runner = runner(10, Arc::new(RecordingFlush::new()));
    let output = runner.run(vec![], ctx).await.unwrap();
    assert_eq!(output.snapshots_completed, 0);
    assert!(output.batches.is_empty());
    assert!(output.is_complete());
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_rejects_missing_persistence_empty_topology_and_zero_bounds() {
    let ctx = live_ctx().await;

    let error = PipelineRunner::new()
        .node_stage(Arc::new(MaterializeStage))
        .run(make_snapshots(1), ctx.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, PipelineError::StateValidation { message, .. } if message.contains("batch commit stages")),
        "{error}"
    );

    let error = PipelineRunner::new()
        .flush(Arc::new(RecordingFlush::new()))
        .run(make_snapshots(1), ctx.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, PipelineError::StateValidation { message, .. } if message.contains("node stage")),
        "{error}"
    );

    for config in [
        PipelineRunnerConfig {
            chunk_size: 0,
            ..Default::default()
        },
        PipelineRunnerConfig {
            channel_capacity: 0,
            ..Default::default()
        },
        PipelineRunnerConfig {
            stage_concurrency: 0,
            ..Default::default()
        },
    ] {
        let error = runner(1, Arc::new(RecordingFlush::new()))
            .with_config(config)
            .run(make_snapshots(1), ctx.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, PipelineError::StateValidation { stage, .. } if stage == "config"),
            "{error}"
        );
    }

    // Node stages that do not resolve leave inputs in place: rejected, nothing committed.
    let flush = Arc::new(RecordingFlush::new());
    let error = PipelineRunner::new()
        .node_stage(Arc::new(CountingStage::new("noop")))
        .flush(flush.clone())
        .run(make_snapshots(2), ctx)
        .await
        .unwrap_err();
    assert!(
        matches!(error.root_cause(), PipelineError::StateValidation { .. }),
        "{error}"
    );
    assert!(flush.recorded().is_empty());
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_failure_after_commits_reports_progress_and_unknown_outcomes() {
    let ctx = live_ctx().await;
    let flush = Arc::new(RecordingFlush::new().on(BatchKind::Relationship, 1, FlushBehavior::Fail));
    let error = runner(3, flush.clone())
        .run(make_snapshots(10), ctx.clone())
        .await
        .unwrap_err();
    match &error {
        PipelineError::Aborted {
            committed,
            batches_committed,
            commit_unknown,
            cause,
            ..
        } => {
            assert_eq!(
                *batches_committed, 5,
                "four node batches and one relationship batch"
            );
            assert_eq!(committed.entities_created, 10);
            assert!(!commit_unknown);
            assert!(cause.is_retriable());
        }
        other => panic!("expected Aborted, got {other:?}"),
    }
    assert!(error.is_retriable());
    assert_eq!(
        flush.recorded().len(),
        6,
        "nothing runs after the failed commit"
    );

    let flush = Arc::new(RecordingFlush::new().on(BatchKind::Node, 2, FlushBehavior::Unknown));
    let error = runner(3, flush.clone())
        .run(make_snapshots(10), ctx)
        .await
        .unwrap_err();
    match &error {
        PipelineError::Aborted {
            batches_committed,
            commit_unknown,
            ..
        } => {
            assert_eq!(*batches_committed, 2);
            assert!(
                commit_unknown,
                "lost acknowledgement is reported, not guessed"
            );
        }
        other => panic!("expected Aborted, got {other:?}"),
    }
    assert!(error.to_string().contains("outcome unknown"), "{error}");
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_replayed_batches_count_toward_totals_but_not_new_work() {
    let ctx = live_ctx().await;
    let flush = Arc::new(RecordingFlush::with_default(FlushBehavior::Replay));
    let output = runner(5, flush).run(make_snapshots(10), ctx).await.unwrap();
    assert_eq!(output.committed.entities_created, 10);
    assert!(output.newly_committed.is_empty());
    assert_eq!(output.replayed_batches(), 4);
    assert!(output.is_complete());
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn runner_rejects_reused_run_id_with_different_input() {
    let ctx = live_ctx().await;
    let run_id = Uuid::new_v4();
    let runner = runner(10, Arc::new(RecordingFlush::new()));
    runner
        .run_with_id(make_snapshots(2), ctx.clone(), run_id)
        .await
        .unwrap();
    // Same run id, same input: accepted (batches replay through receipts).
    runner
        .run_with_id(make_snapshots(2), ctx.clone(), run_id)
        .await
        .unwrap();
    let empty_error = runner
        .run_with_id(Vec::new(), ctx.clone(), run_id)
        .await
        .unwrap_err();
    assert!(matches!(empty_error, PipelineError::StateValidation { .. }));
    let empty_id = Uuid::new_v4();
    runner
        .run_with_id(Vec::new(), ctx.clone(), empty_id)
        .await
        .unwrap();
    let nonempty_error = runner
        .run_with_id(make_snapshots(1), ctx.clone(), empty_id)
        .await
        .unwrap_err();
    assert!(matches!(
        nonempty_error,
        PipelineError::StateValidation { .. }
    ));
    let error = runner
        .run_with_id(make_snapshots(3), ctx, run_id)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, PipelineError::StateValidation { stage, .. } if stage == "run"),
        "{error}"
    );
}

// Cancellation and backpressure.

/// continue_on_step_error = true: failures are RECORDED and survivors
/// complete — failed snapshots never silently vanish (worker contract).
#[tokio::test]
async fn phase_continue_on_error_records_failures_and_survivors_complete() {
    let ctx = Arc::new(
        RuntimeContextBuilder::new("test-org")
            .graph(Arc::new(UnreachableGraph))
            .llm_extraction(Arc::new(MockLlmBackend::empty()))
            .llm_default(Arc::new(MockLlmBackend::empty()))
            .embedder(Arc::new(MockEmbedBackend::default_dimension()))
            .exec_config(kg_core::runtime::ExecutionConfig {
                continue_on_step_error: true,
            })
            .build()
            .unwrap(),
    );

    // Fails every 3rd call: 9 inputs → 3 failures, 6 survivors.
    let stage = Arc::new(crate::test_support::FailEveryNth::new("flaky", 3));
    let stages: Vec<Arc<dyn Stage>> = vec![stage];

    let (outputs, failures) = run_phase(&stages, make_messages(9), ctx, None, None)
        .await
        .unwrap();

    assert_eq!(outputs.len(), 6, "survivors flow through");
    assert_eq!(failures.len(), 3, "every failure is recorded, none vanish");
    for f in &failures {
        assert_eq!(f.stage, "flaky");
        assert!(f.error.contains("synthetic failure"));
    }
    // No snapshot is both a survivor and a failure.
    let survived: std::collections::HashSet<usize> =
        outputs.iter().map(|m| m.snapshot_index).collect();
    for f in &failures {
        assert!(!survived.contains(&f.snapshot_index));
    }
}

/// A panicking stage becomes PipelineError::TaskPanic — contained, never a
/// process abort, never a hang.
#[tokio::test]
async fn phase_stage_panic_is_contained_as_task_panic() {
    let ctx = test_ctx();
    let stages: Vec<Arc<dyn Stage>> = vec![Arc::new(crate::test_support::PanickingStage)];

    let err = run_phase(&stages, make_messages(3), ctx, None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, PipelineError::TaskPanic(_)), "got: {err:?}");
}

/// Backpressure: input count far above channel capacity AND concurrency
/// completes without loss (bounded channels throttle, never drop).
#[tokio::test]
async fn phase_tight_capacity_backpressure_loses_nothing() {
    let ctx = test_ctx();
    let slow = Arc::new(SlowStage::new("slow", 1));
    let count = slow.count.clone();
    let stages: Vec<Arc<dyn Stage>> = vec![slow];

    // capacity 2, concurrency 2, 64 messages — every one must arrive.
    let (outputs, failures) = run_phase(&stages, make_messages(64), ctx, Some(2), Some(2))
        .await
        .unwrap();
    assert_eq!(outputs.len(), 64);
    assert!(failures.is_empty());
    assert_eq!(count.load(Ordering::Relaxed), 64);
}

/// Outputs are returned in snapshot_index order even when completion order
/// scrambles (the batch merge and failure attribution rely on this).
#[tokio::test]
async fn phase_outputs_sorted_despite_out_of_order_completion() {
    let ctx = test_ctx();
    let stages: Vec<Arc<dyn Stage>> = vec![Arc::new(crate::test_support::JitterStage::new())];

    let (outputs, _) = run_phase(&stages, make_messages(16), ctx, None, Some(8))
        .await
        .unwrap();
    let indices: Vec<usize> = outputs.iter().map(|m| m.snapshot_index).collect();
    let mut sorted = indices.clone();
    sorted.sort_unstable();
    assert_eq!(indices, sorted, "deterministic output ordering");
    assert_eq!(outputs.len(), 16);
}

/// Independent observation of a deterministic relationship for batch merge tests.
fn new_edge(source: Uuid, target: Uuid) -> kg_core::models::EntityEdge {
    kg_core::models::EntityEdge {
        time_evidence: None,
        uuid: Uuid::new_v4(),
        chain_id: Uuid::new_v4(),
        identity_hash: Some(
            kg_core::identity::relationship_identity_hash(
                kg_core::identity::RelationshipIdentityScope {
                    org_id: "test-org",
                    namespace: "prod",
                    source: "aws",
                    origin: kg_core::models::RelationshipOrigin::Reference,
                },
                source,
                target,
                "DEPENDS_ON",
                Some("dependency_id"),
                &[],
            )
            .unwrap(),
        ),
        cardinality_key: None,
        origin: kg_core::models::RelationshipOrigin::Reference,
        producer_source: "aws".into(),
        org_id: "test-org".into(),
        source_chain_id: source,
        target_chain_id: target,
        name: "DEPENDS_ON".into(),
        identity_name: None,
        description: String::new(),
        all_properties: Default::default(),
        discovered_by: Some("heuristic_fk".into()),
        resolved_by: None,
        source_property: None,
        target_identity_field: None,
        reference_evidence: None,
        confidence: 0.9,
        justification: None,
        first_seen_snapshot_id: None,
        last_seen_snapshot_id: None,
        last_seen_at: None,
        sync_generation: None,
        valid_from: chrono::Utc::now(),
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        valid_to: None,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        created_at: chrono::Utc::now(),
    }
}

fn resolution_with_edges(
    edges: Vec<kg_core::models::EntityEdge>,
    baseline: kg_core::runtime::stage_output::RelationshipBaseline,
) -> kg_core::runtime::stage_output::EdgeResolutionOutput {
    kg_core::runtime::stage_output::EdgeResolutionOutput {
        relationship_assessments: Default::default(),
        contradiction_timelines: Default::default(),
        relationship_directives: Default::default(),
        reference_report: Default::default(),
        snapshot_nodes: Arc::new(vec![]),
        resolution: Arc::new(Default::default()),
        resolved_nodes: Arc::new(vec![]),
        observed: Arc::new(edges),
        baseline: Arc::new(baseline),
    }
}

/// Every snapshot's observation of a pair reaches the batch; persistence
/// orders them. Baselines two snapshots read differently reject the chunk;
/// a scope only one snapshot found is merged, not a conflict.
#[test]
fn relationship_merge_keeps_every_observation_and_rejects_moved_baselines() {
    use kg_core::runtime::stage_output::{
        ConnectorScope, PairBaseline, RelationshipBaseline, StoredRelationship,
    };
    let run_id = Uuid::new_v4();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let baseline = |stored: Option<StoredRelationship>| RelationshipBaseline {
        ended: vec![],
        pairs: vec![PairBaseline {
            versions: vec![],
            source_chain_id: source,
            target_chain_id: target,
            live: stored.into_iter().collect(),
        }],
        relations: vec![],
        orphan_targets: vec![],
        reference_owners: vec![],
    };
    let message = |index: usize, baseline: RelationshipBaseline| PipelineMessage {
        snapshot_index: index,
        run_id,
        state: StageOutput::EdgeResolution(resolution_with_edges(
            vec![new_edge(source, target)],
            baseline,
        )),
    };

    let msgs = vec![message(0, baseline(None)), message(1, baseline(None))];
    let batch = crate::runner::merge_relationship_batch(&msgs, false).unwrap();
    assert_eq!(
        batch.observed.len(),
        2,
        "both observations reach persistence"
    );
    assert_eq!(batch.baseline.pairs.len(), 1, "one baseline entry per pair");

    let stored = StoredRelationship {
        time_evidence: None,
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        valid_from: chrono::Utc::now(),
        ended_at: None,
        uuid: Uuid::new_v4(),
        chain_id: Uuid::new_v4(),
        identity_hash: Some(
            kg_core::identity::relationship_identity_hash(
                kg_core::identity::RelationshipIdentityScope {
                    org_id: "test-org",
                    namespace: "prod",
                    source: "aws",
                    origin: kg_core::models::RelationshipOrigin::Reference,
                },
                source,
                target,
                "DEPENDS_ON",
                Some("dependency_id"),
                &[],
            )
            .unwrap(),
        ),
        cardinality_key: None,
        origin: kg_core::models::RelationshipOrigin::Reference,
        all_properties: Default::default(),
        first_seen_snapshot_id: None,
        source_chain_id: source,
        target_chain_id: target,
        name: "DEPENDS_ON".into(),
        version: 1,
        confidence: 0.9,
        description: String::new(),
        latest_observation: None,
        scope: None,
        reference_evidence: None,
    };
    let msgs = vec![
        message(0, baseline(None)),
        message(1, baseline(Some(stored.clone()))),
    ];
    let error = crate::runner::merge_relationship_batch(&msgs, false).unwrap_err();
    assert!(error.is_retriable(), "{error}");
    assert!(matches!(error, PipelineError::IdentityRevisionChanged));

    // Snapshots of different connector scopes read the same state; the
    // relationship is in one of them. The merge carries that scope.
    let scope = ConnectorScope {
        namespace: "prod".into(),
        source: "aws".into(),
    };
    let in_scope = StoredRelationship {
        time_evidence: None,
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        scope: Some(scope.clone()),
        ..stored.clone()
    };
    let msgs = vec![
        message(0, baseline(Some(stored.clone()))),
        message(1, baseline(Some(in_scope.clone()))),
        message(2, baseline(Some(stored.clone()))),
    ];
    let batch = crate::runner::merge_relationship_batch(&msgs, false).unwrap();
    assert_eq!(
        batch.baseline.pairs[0].live[0].scope,
        Some(scope),
        "a scope found by any snapshot stands"
    );
    let elsewhere = StoredRelationship {
        time_evidence: None,
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        scope: Some(ConnectorScope {
            namespace: "prod".into(),
            source: "cmdb".into(),
        }),
        ..stored.clone()
    };
    let msgs = vec![
        message(0, baseline(Some(in_scope))),
        message(1, baseline(Some(elsewhere))),
    ];
    let error = crate::runner::merge_relationship_batch(&msgs, false).unwrap_err();
    assert!(matches!(error, PipelineError::IdentityRevisionChanged));

    // Without relationship stages node resolutions pass through as an empty batch.
    let msgs = vec![PipelineMessage {
        snapshot_index: 0,
        run_id,
        state: StageOutput::NodeResolution(Default::default()),
    }];
    assert!(crate::runner::merge_relationship_batch(&msgs, true)
        .unwrap()
        .observed
        .is_empty());
    assert!(crate::runner::merge_relationship_batch(&msgs, false).is_err());
}

#[tokio::test]
async fn saturated_worker_cancellation_is_prompt() {
    let ctx = test_ctx();
    let cancel = ctx.cancel.clone();
    let stages: Vec<Arc<dyn Stage>> = vec![Arc::new(SlowStage::new("blocked", 60_000))];
    let work =
        tokio::spawn(
            async move { run_phase(&stages, make_messages(64), ctx, Some(1), Some(1)).await },
        );
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), work)
        .await
        .expect("cancellation must interrupt a saturated worker")
        .unwrap();
    assert!(matches!(result, Err(PipelineError::Cancelled)));
}

#[tokio::test]
async fn cancellation_during_worker_drain_is_not_success() {
    let ctx = test_ctx();
    let cancel = ctx.cancel.clone();
    let stages: Vec<Arc<dyn Stage>> = vec![Arc::new(SlowStage::new("blocked", 60_000))];
    let work =
        tokio::spawn(
            async move { run_phase(&stages, make_messages(1), ctx, Some(1), Some(2)).await },
        );
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), work)
        .await
        .expect("cancellation must interrupt draining")
        .unwrap();
    assert!(matches!(result, Err(PipelineError::Cancelled)));
}

struct HoldsPermitWhileOtherOutputsFill {
    permits: Arc<tokio::sync::Semaphore>,
    started: Arc<tokio::sync::Notify>,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Stage for HoldsPermitWhileOtherOutputsFill {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        "holds_permit"
    }
    async fn process(
        &self,
        input: StageOutput,
        _: &RuntimeContext,
    ) -> Result<StageOutput, kg_core::errors::StageError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let _permit = self.permits.acquire().await.unwrap();
            self.started.notify_one();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(input)
    }
}

#[tokio::test]
async fn output_backpressure_does_not_freeze_inflight_provider_calls() {
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let started = Arc::new(tokio::sync::Notify::new());
    let stage = Arc::new(HoldsPermitWhileOtherOutputsFill {
        permits: permits.clone(),
        started: started.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let ctx = test_ctx();
    let cancel = ctx.cancel.clone();
    let (input, receiver) = tokio::sync::mpsc::channel(3);
    for message in make_messages(3) {
        input.send(message).await.unwrap();
    }
    drop(input);
    let (output, _blocked_receiver) = tokio::sync::mpsc::channel(1);
    let work = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        crate::worker::run_stage_worker(stage, ctx, receiver, output, 3).await
    }));
    started.notified().await;
    let released = tokio::time::timeout(std::time::Duration::from_millis(500), permits.acquire())
        .await
        .is_ok();
    cancel.cancel();
    assert!(matches!(work.await.unwrap(), Err(PipelineError::Cancelled)));
    assert!(
        released,
        "a full output channel must not freeze a provider future holding a shared permit"
    );
}

/// Chunk spans of concurrent runs carry their own run id and are parented
/// to their own run span, and every stage-worker event lands inside the
/// chunk span of the run whose message it processed, even when the runs
/// interleave on one thread: the work is instrumented, not entered across
/// awaits, and spawned workers run in the spawning span.
#[cfg(feature = "live-tests")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "live: Neo4j"]
async fn concurrent_runs_keep_their_own_span_fields() {
    use std::collections::HashMap;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id};
    use tracing::Subscriber;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

    #[derive(Default)]
    struct Fields(HashMap<String, String>);

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    /// For every chunk span: its own `run_id` and the `run_id` of its parent
    /// span. For every stage-worker event: the `run_id` it carries and the
    /// `run_id` of the chunk span it was emitted in.
    #[derive(Default)]
    struct Recorded {
        chunks: Vec<(String, String)>,
        events: Vec<(String, String)>,
    }

    struct SpanFields(Arc<Mutex<Recorded>>);

    impl<S> Layer<S> for SpanFields
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            let mut fields = Fields::default();
            attrs.record(&mut fields);
            let span = ctx.span(id).expect("span exists");
            if span.name() == "pipeline.chunk" {
                let own = fields.0.get("run_id").cloned().unwrap_or_default();
                let parent = span
                    .parent()
                    .and_then(|p| {
                        p.extensions()
                            .get::<Fields>()
                            .and_then(|f| f.0.get("run_id").cloned())
                    })
                    .unwrap_or_else(|| "none".into());
                self.0.lock().unwrap().chunks.push((own, parent));
            }
            span.extensions_mut().insert(fields);
        }

        fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
            let mut fields = Fields::default();
            event.record(&mut fields);
            // The message is recorded through `Debug`, so it carries quotes.
            let message = fields.0.get("message").map(|m| m.trim_matches('"'));
            if message != Some("stage finished") {
                return;
            }
            let own = fields.0.get("run_id").cloned().unwrap_or_default();
            let chunk = ctx
                .event_span(event)
                .into_iter()
                .flat_map(|span| span.scope())
                .find(|span| span.name() == "pipeline.chunk")
                .and_then(|span| {
                    span.extensions()
                        .get::<Fields>()
                        .and_then(|f| f.0.get("run_id").cloned())
                })
                .unwrap_or_else(|| "none".into());
            self.0.lock().unwrap().events.push((own, chunk));
        }
    }

    let seen = Arc::new(Mutex::new(Recorded::default()));
    // tracing-core's single-dispatch fast path computes newly registered
    // callsite interest from the registering thread's default. Other parallel
    // tests may first reach these callsites without a subscriber and cache
    // `never`. Keep a second inert dispatch alive so interest is aggregated
    // across registered dispatches; only this thread records into `seen`.
    let _interest_scope = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let subscriber = tracing_subscriber::registry().with(SpanFields(seen.clone()));
    let ctx = live_ctx().await;
    let runs = async {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let runner_a = runner(2, Arc::new(RecordingFlush::new()));
        let runner_b = runner(2, Arc::new(RecordingFlush::new()));
        let first = runner_a.run_with_id(make_snapshots(6), ctx.clone(), a);
        let second = runner_b.run_with_id(make_snapshots(4), ctx.clone(), b);
        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();
        (a, b)
    };
    // A thread default, not a future-scoped one: spawned worker tasks run
    // on this current-thread runtime and must report to the same layer, as
    // they report to the process-wide subscriber in production.
    let guard = tracing::subscriber::set_default(subscriber);
    let (a, b) = runs.await;
    drop(guard);

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.chunks.len(),
        10,
        "3 + 2 chunks in two passes: {:?}",
        seen.chunks
    );
    for (own, parent) in seen.chunks.iter() {
        assert_eq!(
            own, parent,
            "a chunk span belongs to its own run: {:?}",
            seen.chunks
        );
    }
    for run in [a, b] {
        assert!(
            seen.chunks.iter().any(|(own, _)| *own == run.to_string()),
            "run {run} produced chunk spans: {:?}",
            seen.chunks
        );
    }
    assert_eq!(
        seen.events.len(),
        10,
        "one stage-worker event per snapshot and stage: {:?}",
        seen.events
    );
    for (own, chunk) in seen.events.iter() {
        assert_eq!(
            own, chunk,
            "a worker event is emitted inside the chunk span of its own run: {:?}",
            seen.events
        );
    }
}

#[tokio::test]
async fn invalid_last_snapshot_rejects_before_registration_or_any_stage() {
    let mut snapshots = make_snapshots(3);
    snapshots[2].org_id = Some("private-other-org".into());
    let stage = CountingStage::new("must_not_run");
    let count = stage.count.clone();
    let mut ctx = test_ctx();
    Arc::get_mut(&mut ctx).unwrap().exec_config =
        Arc::new(kg_core::runtime::execution::ExecutionConfig {
            continue_on_step_error: true,
        });
    let result = PipelineRunner::new()
        .node_stage(Arc::new(stage))
        .node_stage(Arc::new(MaterializeStage))
        .flush(Arc::new(RecordingFlush::new()))
        .run(snapshots, ctx)
        .await;
    let error = result.unwrap_err();
    assert!(
        matches!(error, PipelineError::StateValidation { ref stage, .. } if stage == "input_validation")
    );
    assert!(error.to_string().contains("snapshots[2].org_id"));
    assert!(!error.to_string().contains("private-other-org"));
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn validation_telemetry_correlates_without_payload_values() {
    use std::io::Write;
    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = Buffer(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    let _interest = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let _guard = tracing::subscriber::set_default(subscriber);
    let mut snapshots = make_snapshots(1);
    snapshots[0].org_id = Some("SECRET_ORG".into());
    snapshots[0].content = Some("SECRET_CONTENT".into());
    snapshots[0]
        .tags
        .insert("owner".into(), "SECRET_TAG".into());
    let run_id = Uuid::new_v4();
    let error = PipelineRunner::new()
        .node_stage(Arc::new(MaterializeStage))
        .flush(Arc::new(RecordingFlush::new()))
        .run_with_id(snapshots, test_ctx(), run_id)
        .await
        .unwrap_err();
    assert!(matches!(error, PipelineError::StateValidation { .. }));
    let logs = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("request validation failed"), "{logs}");
    assert!(logs.contains(&run_id.to_string()), "{logs}");
    for value in ["SECRET_ORG", "SECRET_CONTENT", "SECRET_TAG", "test-org"] {
        assert!(!logs.contains(value), "{logs}");
    }
}

struct BatchProbe {
    calls: Arc<Mutex<Vec<Vec<String>>>>,
    behavior: &'static str,
}
#[async_trait::async_trait]
impl Stage for BatchProbe {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        "batch_probe"
    }
    fn is_batch(&self) -> bool {
        true
    }
    async fn process(
        &self,
        _: StageOutput,
        _: &RuntimeContext,
    ) -> Result<StageOutput, kg_core::errors::StageError> {
        panic!("batch hook must be used")
    }
    async fn process_batch(
        &self,
        mut inputs: Vec<StageOutput>,
        _: &RuntimeContext,
    ) -> Result<Vec<Result<StageOutput, kg_core::errors::StageError>>, kg_core::errors::StageError>
    {
        self.calls.lock().unwrap().push(
            inputs
                .iter()
                .map(|state| match state {
                    StageOutput::Input(input) => input.name.clone(),
                    _ => "empty".into(),
                })
                .collect(),
        );
        match self.behavior {
            "short" => {
                inputs.pop();
            }
            "panic" => panic!("batch panic"),
            "error" => {
                return Err(kg_core::errors::StageError::StateValidation {
                    stage: self.name().into(),
                    message: "batch rejected".into(),
                });
            }
            _ => {}
        }
        Ok(inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                if self.behavior == "decline" && index == 1 {
                    Err(kg_core::errors::StageError::StateValidation {
                        stage: self.name().into(),
                        message: "uncertain identity".into(),
                    })
                } else {
                    Ok(input)
                }
            })
            .collect())
    }
}

#[tokio::test]
async fn phase_batch_barrier_preserves_order_and_envelopes_across_segments() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let after = Arc::new(CountingStage::new("after_batch"));
    let stages: Vec<Arc<dyn Stage>> = vec![
        Arc::new(crate::test_support::JitterStage::new()),
        Arc::new(BatchProbe {
            calls: calls.clone(),
            behavior: "ok",
        }),
        after.clone(),
        Arc::new(BatchProbe {
            calls: calls.clone(),
            behavior: "ok",
        }),
    ];
    let mut inputs = make_messages(16);
    for (message, input) in inputs.iter_mut().zip(make_snapshots(16)) {
        message.state = StageOutput::Input(Box::new(input));
    }
    let expected: Vec<_> = inputs
        .iter()
        .map(|message| (message.snapshot_index, message.run_id))
        .collect();
    inputs.reverse();
    let (outputs, failures) = run_phase(&stages, inputs, test_ctx(), Some(1), Some(8))
        .await
        .unwrap();
    assert!(failures.is_empty());
    assert_eq!(
        outputs
            .iter()
            .map(|message| (message.snapshot_index, message.run_id))
            .collect::<Vec<_>>(),
        expected
    );
    let names: Vec<_> = (0..16).map(|index| format!("snapshot-{index}")).collect();
    assert_eq!(*calls.lock().unwrap(), vec![names.clone(), names]);
    assert_eq!(after.count.load(Ordering::Relaxed), 16);
}

#[tokio::test]
async fn phase_batch_wrong_cardinality_and_panic_fail_closed() {
    for behavior in ["short", "panic"] {
        let stages: Vec<Arc<dyn Stage>> = vec![Arc::new(BatchProbe {
            calls: Default::default(),
            behavior,
        })];
        let error = run_phase(&stages, make_messages(2), test_ctx(), None, None)
            .await
            .unwrap_err();
        if behavior == "short" {
            assert!(matches!(error, PipelineError::StateValidation { .. }));
        } else {
            assert!(matches!(error, PipelineError::TaskPanic(_)));
        }
    }
}

#[tokio::test]
async fn phase_batch_error_attributes_every_participant_without_running_downstream() {
    let mut ctx = test_ctx();
    Arc::get_mut(&mut ctx).unwrap().exec_config = Arc::new(kg_core::runtime::ExecutionConfig {
        continue_on_step_error: true,
    });
    let after = Arc::new(CountingStage::new("after_batch"));
    let stages: Vec<Arc<dyn Stage>> = vec![
        Arc::new(crate::test_support::FailEveryNth::new("before_batch", 3)),
        Arc::new(BatchProbe {
            calls: Default::default(),
            behavior: "error",
        }),
        after.clone(),
    ];
    let (outputs, failures) = run_phase(&stages, make_messages(9), ctx, None, None)
        .await
        .unwrap();
    assert!(outputs.is_empty());
    assert_eq!(
        failures
            .iter()
            .map(|failure| failure.snapshot_index)
            .collect::<Vec<_>>(),
        (0..9).collect::<Vec<_>>()
    );
    assert_eq!(
        failures
            .iter()
            .filter(|failure| failure.stage == "batch_probe")
            .count(),
        6
    );
    assert_eq!(after.count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn phase_batch_cancellation_drops_inflight_work_before_return() {
    struct BlockingBatch {
        entered: Arc<tokio::sync::Notify>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    struct Guard(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl Stage for BlockingBatch {
        fn contract(&self) -> kg_core::traits::StageContract {
            kg_core::traits::StageKind::IDENTITY
        }

        fn name(&self) -> &str {
            "blocking_batch"
        }
        fn is_batch(&self) -> bool {
            true
        }
        async fn process(
            &self,
            _: StageOutput,
            _: &RuntimeContext,
        ) -> Result<StageOutput, kg_core::errors::StageError> {
            unreachable!()
        }
        async fn process_batch(
            &self,
            _: Vec<StageOutput>,
            _: &RuntimeContext,
        ) -> Result<
            Vec<Result<StageOutput, kg_core::errors::StageError>>,
            kg_core::errors::StageError,
        > {
            let _guard = Guard(self.dropped.clone());
            self.entered.notify_one();
            std::future::pending().await
        }
    }
    let ctx = test_ctx();
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stages: Vec<Arc<dyn Stage>> = vec![Arc::new(BlockingBatch {
        entered: entered.clone(),
        dropped: dropped.clone(),
    })];
    let (result, ()) = tokio::join!(
        run_phase(&stages, make_messages(2), ctx.clone(), None, None),
        async {
            entered.notified().await;
            ctx.cancel.cancel();
        }
    );
    assert!(matches!(result, Err(PipelineError::Cancelled)));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn observation_admission_rejects_invalid_and_aggregate_input_before_storage() {
    use kg_core::models::IngestionInput;
    use kg_core::runtime::saga::{MAX_CONTEXT_REFERENCES, MAX_MANIFEST_BYTES};
    let ctx = test_ctx();
    // The graph rejects every read: these must remain input errors, not backend errors.
    let mut malformed = make_snapshots(2);
    malformed[1].namespace.clear();
    let mut oversized = make_snapshots(1);
    oversized[0].namespace = "n".repeat(MAX_MANIFEST_BYTES);
    let mut referenced = make_snapshots(1).pop().unwrap();
    referenced.content = Some("source evidence".into());
    referenced.previous_snapshot_uuids = (0..100).map(|_| Uuid::new_v4()).collect();
    referenced.validate_request(&ctx.org_id).unwrap();
    let aggregate = vec![referenced; MAX_CONTEXT_REFERENCES / 100 + 1];
    for inputs in [malformed, oversized, aggregate] {
        let inputs: Vec<IngestionInput> = inputs.into_iter().map(Into::into).collect();
        let error = crate::observation_admission::prepare(&ctx, &inputs, chrono::Utc::now(), 10)
            .await
            .unwrap_err();
        assert!(
            matches!(error, PipelineError::StateValidation { .. }),
            "{error:?}"
        );
    }
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn concurrent_registration_discards_losing_observation_identities_and_context() {
    use kg_core::models::IngestionInput;
    use kg_core::runtime::history::EvidenceRef;
    use kg_core::runtime::saga::FrozenContextRef;
    use kg_core::traits::{RequestFingerprint, RunRegistration};
    let ctx = live_ctx().await;
    let mut snapshots = make_snapshots(2);
    for input in &mut snapshots {
        input.content = Some("immutable source".into());
    }
    let inputs: Vec<IngestionInput> = snapshots.into_iter().map(Into::into).collect();
    let run = Uuid::new_v4();
    let fingerprint =
        RequestFingerprint::compute_inputs(&ctx.org_id, &inputs, &serde_json::json!({})).unwrap();
    let left = crate::schema_admission::header(
        &ctx,
        &inputs,
        run,
        fingerprint.clone(),
        vec![],
        "manifest-race",
        1,
    )
    .await
    .unwrap();
    let mut right = crate::schema_admission::header(
        &ctx,
        &inputs,
        run,
        fingerprint.clone(),
        vec![],
        "manifest-race",
        1,
    )
    .await
    .unwrap();
    // Both preparations happened before registration, so their generated identities differ.
    assert_ne!(left.observation_manifest, right.observation_manifest);
    let prior = right
        .observation_manifest
        .fresh_evidence(&ctx.org_id, &inputs, 0)
        .unwrap();
    right.observation_manifest.entries[1]
        .history
        .push(FrozenContextRef {
            reference: EvidenceRef {
                uuid: prior.uuid,
                digest: prior.digest(),
            },
            input_index: Some(0),
        });
    right
        .observation_manifest
        .validate_inputs(&inputs, 1)
        .unwrap();
    let (a, b) = tokio::join!(
        ctx.graph.register_run(&left),
        ctx.graph.register_run(&right)
    );
    let (winner, resumed) = match (a.unwrap(), b.unwrap()) {
        (
            RunRegistration::Registered,
            RunRegistration::Resumed {
                observation_manifest,
                ..
            },
        ) => (left, observation_manifest),
        (
            RunRegistration::Resumed {
                observation_manifest,
                ..
            },
            RunRegistration::Registered,
        ) => (right, observation_manifest),
        other => panic!("one registration must win: {other:?}"),
    };
    assert_eq!(resumed, winner.observation_manifest);
    let restored = crate::schema_admission::header(
        &ctx,
        &inputs,
        run,
        fingerprint,
        vec![],
        "manifest-race",
        1,
    )
    .await
    .unwrap();
    assert_eq!(restored.observation_manifest, winner.observation_manifest);
    assert_eq!(restored.capture_default, winner.capture_default);
    assert_eq!(restored.batch_plan, winner.batch_plan);
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn saga_name_and_uuid_aliases_share_bulk_order_and_frozen_context() {
    use kg_core::models::{IngestionInput, ThreadAssociation};
    use kg_core::saga::{ThreadAssociationWrite, ThreadReference};
    use kg_core::traits::GraphMutation;
    let ctx = live_ctx().await;
    let name = format!("alias-{}", Uuid::new_v4());
    let saga = kg_core::saga::saga_uuid(&ctx.org_id, "test", &name);
    let anchor = Uuid::new_v4();
    let at: chrono::DateTime<chrono::Utc> = "2026-01-10T00:00:00Z".parse().unwrap();
    ctx.graph.apply_mutations(&ctx.org_id, &[
        GraphMutation::UpsertSnapshot { uuid: anchor, properties: serde_json::json!({
            "namespace":"test", "name":"anchor", "source":"test", "data_type":"text",
            "captured_at":at.to_rfc3339(), "created_at":at.to_rfc3339(), "content":"later stored source"
        }).as_object().unwrap().clone() },
        GraphMutation::AssociateSagaSnapshot { association: Box::new(ThreadAssociationWrite {
            namespace:"test".into(), saga_uuid:saga, name:name.clone(), created_at:at,
            expected_revision:0, snapshot_uuid:anchor, previous_snapshot_uuid:None,
            membership_uuid:Uuid::new_v4(), next_uuid:None,
        }) },
    ]).await.unwrap();
    let mut inputs = make_snapshots(3);
    for (index, input) in inputs.iter_mut().enumerate() {
        input.content = Some(format!("immutable observation {index}"));
        input.captured_at = Some(at - chrono::Duration::days(if index == 0 { 1 } else { 2 }));
        input.saga = Some(ThreadAssociation {
            saga: if index == 1 {
                ThreadReference::Uuid { uuid: saga }
            } else {
                ThreadReference::Name { name: name.clone() }
            },
            previous_snapshot_uuid: None,
        });
    }
    let inputs: Vec<IngestionInput> = inputs.into_iter().map(Into::into).collect();
    for chunk_size in [1, 2, 3] {
        let manifest = crate::observation_admission::prepare(&ctx, &inputs, at, chunk_size)
            .await
            .unwrap();
        assert_eq!(
            manifest
                .node_batches
                .iter()
                .flatten()
                .copied()
                .collect::<Vec<_>>(),
            vec![1, 2, 0]
        );
        assert!(manifest
            .entries
            .iter()
            .all(|entry| entry.saga.as_ref().unwrap().saga_uuid == saga));
        let mut histories: Vec<Vec<usize>> = manifest
            .entries
            .iter()
            .map(|entry| {
                entry
                    .history
                    .iter()
                    .map(|reference| {
                        reference
                            .input_index
                            .expect("only earlier local evidence qualifies")
                    })
                    .collect()
            })
            .collect();
        // Presentation may use UUID ties; membership must be identical at every chunk size.
        for history in &mut histories {
            history.sort_unstable();
        }
        assert_eq!(histories, vec![vec![1, 2], vec![], vec![1]]);
    }
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn changed_processing_version_rejects_resume_before_stage_or_flush() {
    struct VersionedMaterialize {
        revision: &'static str,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl Stage for VersionedMaterialize {
        fn name(&self) -> &str {
            MaterializeStage.name()
        }
        fn processing_version(&self) -> String {
            self.revision.into()
        }
        fn contract(&self) -> kg_core::traits::StageContract {
            MaterializeStage.contract()
        }
        async fn process(
            &self,
            input: StageOutput,
            ctx: &RuntimeContext,
        ) -> Result<StageOutput, kg_core::errors::StageError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            MaterializeStage.process(input, ctx).await
        }
    }
    let ctx = live_ctx().await;
    let run_id = Uuid::new_v4();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let flush = Arc::new(RecordingFlush::new());
    let mut pipeline = runner(10, flush.clone());
    pipeline.node_stages[0] = Arc::new(VersionedMaterialize {
        revision: "prompt-v1",
        calls: calls.clone(),
    });
    pipeline
        .run_with_id(make_snapshots(1), ctx.clone(), run_id)
        .await
        .unwrap();
    pipeline
        .run_with_id(make_snapshots(1), ctx.clone(), run_id)
        .await
        .unwrap();
    let before = calls.load(Ordering::SeqCst);
    let commits = flush.batches.lock().unwrap().len();
    pipeline.node_stages[0] = Arc::new(VersionedMaterialize {
        revision: "prompt-v2",
        calls: calls.clone(),
    });
    let error = pipeline
        .run_with_id(make_snapshots(1), ctx, run_id)
        .await
        .unwrap_err();
    assert!(matches!(error,PipelineError::StateValidation{ref stage,..} if stage=="run"));
    assert_eq!(calls.load(Ordering::SeqCst), before);
    assert_eq!(flush.batches.lock().unwrap().len(), commits);
}

#[tokio::test]
async fn partial_batch_outcomes_preserve_order_and_obey_fail_fast() {
    for continue_on_step_error in [true, false] {
        let mut ctx = test_ctx();
        Arc::get_mut(&mut ctx).unwrap().exec_config = Arc::new(kg_core::runtime::ExecutionConfig {
            continue_on_step_error,
        });
        let after = Arc::new(CountingStage::new("after_decline"));
        let stages: Vec<Arc<dyn Stage>> = vec![
            Arc::new(BatchProbe {
                calls: Default::default(),
                behavior: "decline",
            }),
            after.clone(),
        ];
        let result = run_phase(&stages, make_messages(3), ctx, None, None).await;
        if continue_on_step_error {
            let (outputs, failures) = result.unwrap();
            assert_eq!(
                outputs.iter().map(|o| o.snapshot_index).collect::<Vec<_>>(),
                vec![0, 2]
            );
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].snapshot_index, 1);
            assert_eq!(after.count.load(Ordering::Relaxed), 2);
        } else {
            assert!(matches!(result, Err(PipelineError::StepExecution { .. })));
            assert_eq!(after.count.load(Ordering::Relaxed), 0);
        }
    }
}
