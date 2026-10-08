use super::*;
use crate::test_support::{CountingStage, MaterializeStage, RecordingFlush, UnreachableGraph};
use kg_core::runtime::RuntimeContextBuilder;
use kg_core::test_support::{MockEmbedBackend, MockLlmBackend};
use std::sync::Mutex;

fn context() -> Arc<RuntimeContext> {
    let llm = Arc::new(MockLlmBackend::empty());
    Arc::new(
        RuntimeContextBuilder::new("org")
            .graph(Arc::new(UnreachableGraph))
            .llm_extraction(llm.clone())
            .llm_default(llm)
            .embedder(Arc::new(MockEmbedBackend::default_dimension()))
            .build()
            .unwrap(),
    )
}

fn runner() -> PipelineRunner {
    PipelineRunner::new()
        .node_stage(Arc::new(MaterializeStage))
        .flush(Arc::new(RecordingFlush::new()))
}

#[tokio::test]
async fn cancelled_request_never_reaches_storage() {
    let ctx = context();
    ctx.cancel.cancel();
    let error = runner().run(Vec::new(), ctx).await.unwrap_err();
    assert!(matches!(error, PipelineError::Cancelled));
}

#[tokio::test]
async fn phase_rejects_invalid_bounds_even_without_stages() {
    for (capacity, concurrency) in [(0, 1), (1, 0), (tokio::sync::Semaphore::MAX_PERMITS + 1, 1)] {
        assert!(matches!(
            run_phase(
                &[],
                Vec::new(),
                context(),
                Some(capacity),
                Some(concurrency)
            )
            .await,
            Err(PipelineError::StateValidation { .. })
        ));
    }
    assert!(PipelineRunnerConfig {
        channel_capacity: tokio::sync::Semaphore::MAX_PERMITS + 1,
        ..Default::default()
    }
    .validate()
    .is_err());
    let ctx = context();
    ctx.cancel.cancel();
    assert!(matches!(
        run_phase(&[], Vec::new(), ctx, None, None).await,
        Err(PipelineError::Cancelled)
    ));
}

#[test]
fn stage_order_and_identity_are_in_the_replay_fingerprint() {
    let ctx = context();
    let a = runner()
        .node_stage(Arc::new(CountingStage::new("one")))
        .node_stage(Arc::new(CountingStage::new("two")));
    let b = runner()
        .node_stage(Arc::new(CountingStage::new("two")))
        .node_stage(Arc::new(CountingStage::new("one")));
    let c = runner().edge_stage(Arc::new(CountingStage::new("edge")));
    let fingerprints: Vec<_> = [&a, &b, &c, &runner()]
        .iter()
        .map(|runner| RequestFingerprint::compute("org", &[], &runner.settings(&ctx)).unwrap())
        .collect();
    for i in 0..fingerprints.len() {
        for j in i + 1..fingerprints.len() {
            assert_ne!(fingerprints[i], fingerprints[j]);
        }
    }
}

struct CancelStage;
#[async_trait::async_trait]
impl Stage for CancelStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        kg_core::traits::StageKind::IDENTITY
    }

    fn name(&self) -> &str {
        "cancel"
    }
    async fn process(&self, _: StageOutput, _: &RuntimeContext) -> Result<StageOutput, StageError> {
        Err(StageError::Cancelled {
            stage: self.name().into(),
        })
    }
}

#[tokio::test]
async fn stage_cancellation_is_not_a_continuable_snapshot_failure() {
    let mut ctx = context();
    Arc::get_mut(&mut ctx).unwrap().exec_config =
        Arc::new(kg_core::runtime::execution::ExecutionConfig {
            continue_on_step_error: true,
        });
    let result = run_phase(
        &[Arc::new(CancelStage)],
        vec![PipelineMessage {
            snapshot_index: 0,
            run_id: Uuid::new_v4(),
            state: StageOutput::Empty,
        }],
        ctx,
        None,
        None,
    )
    .await;
    assert!(matches!(result, Err(PipelineError::Cancelled)));
}

#[test]
fn unexpected_stage_output_does_not_expose_source_content() {
    let message = PipelineMessage {
        snapshot_index: 0,
        run_id: Uuid::new_v4(),
        state: StageOutput::PreparedSnapshot(Box::new(
            kg_core::runtime::stage_output::PreparedSnapshotInput::new(
                kg_core::models::ValidatedSnapshotInput::new(
                    serde_json::from_value(serde_json::json!({
                        "namespace": "test", "name": "observation", "source": "test",
                        "data_type": "text", "entities": [], "content": "SECRET_CONTENT",
                        "exclude_fk_properties": [], "ignore_change_properties": []
                    }))
                    .unwrap(),
                    "org",
                )
                .unwrap(),
                "org",
            )
            .unwrap(),
        )),
    };
    let payload = format!("{:?}", message.state);
    let node_error = merge_node_batch(std::slice::from_ref(&message))
        .unwrap_err()
        .to_string();
    let edge_error = merge_relationship_batch(&[message], false)
        .unwrap_err()
        .to_string();
    for error in [node_error, edge_error] {
        assert!(error.contains("prepared snapshot"));
        assert!(!error.contains(&payload));
        assert!(!error.contains("SECRET_CONTENT"));
    }
}

#[test]
fn persistence_contract_cannot_replay_older_receipts() {
    let ctx = context();
    let settings = runner().settings(&ctx);
    for version in ["7", "8", "9", "10", "11", "12", "13", "14", "15"] {
        assert_ne!(settings["settings_version"], version);
        let mut old_settings = settings.clone();
        old_settings["settings_version"] = version.into();
        assert_ne!(
            RequestFingerprint::compute("org", &[], &settings).unwrap(),
            RequestFingerprint::compute("org", &[], &old_settings).unwrap(),
        );
    }
}

#[tokio::test]
async fn malformed_custom_schema_fails_before_run_storage() {
    let input: SnapshotInput = serde_json::from_value(serde_json::json!({
        "name":"snapshot","namespace":"prod","source":"test","data_type":"text","entities":[],"content":"test",
        "entity_types":[{"name":"Service","attributes":{"type":"object","$ref":"https://example.invalid/schema"}}]
    })).unwrap();
    let error = runner().run(vec![input], context()).await.unwrap_err();
    assert!(
        matches!(error,PipelineError::StateValidation {ref stage,..} if stage == "input_validation")
    );
}

#[test]
fn extraction_contract_changes_invalidate_replay_fingerprints() {
    let ctx = context();
    let mut settings = runner().settings(&ctx);
    let baseline = RequestFingerprint::compute("org", &[], &settings).unwrap();
    for (field, value) in [
        ("omission_check", serde_json::json!(false)),
        (
            "source_guidance",
            serde_json::json!({"github":{"instructions":"code symbols","excluded_entity_types":[]}}),
        ),
        ("shared_instructions", serde_json::json!("Domain context")),
        ("instructions", serde_json::json!("Only named services")),
        (
            "relationship_instructions",
            serde_json::json!("Name relations as verbs"),
        ),
        (
            "identity_instructions",
            serde_json::json!("Pod names carry a random suffix"),
        ),
        (
            "summary_instructions",
            serde_json::json!("Lead with the operational state"),
        ),
        ("entity_kind_words", serde_json::json!(["database"])),
        ("excluded_entity_types", serde_json::json!(["Person"])),
        ("timeout_ms", serde_json::json!(100)),
        ("max_output_tokens", serde_json::json!(100)),
        ("max_response_bytes", serde_json::json!(4096)),
        ("max_entities", serde_json::json!(10)),
        ("max_property_depth", serde_json::json!(2)),
    ] {
        let original = settings["extraction"][field].clone();
        settings["extraction"][field] = value;
        assert_ne!(
            baseline,
            RequestFingerprint::compute("org", &[], &settings).unwrap(),
            "{field}"
        );
        settings["extraction"][field] = original;
    }
}

#[test]
fn revision_expectations_survive_checkpoints_and_conflicting_revisions_are_rejected() {
    use kg_core::{
        runtime::stage_output::NodeResolutionOutput,
        traits::{IdentityRevision, IdentityScope},
    };
    let revision = IdentityRevision {
        scope: IdentityScope {
            namespace: "prod".into(),
            entity_type: "Service".into(),
        },
        revision: 7,
    };
    let make = |revision| PipelineMessage {
        snapshot_index: 0,
        run_id: Uuid::nil(),
        state: StageOutput::NodeResolution(NodeResolutionOutput {
            relationship_changes: Default::default(),
            identity_revisions: Arc::new(vec![revision]),
            ..Default::default()
        }),
    };
    let checkpoint =
        checkpoint_nodes(&[make(revision.clone())], vec![], &Default::default(), None).unwrap();
    let checkpoint: BatchRecovery =
        serde_json::from_value(serde_json::to_value(checkpoint).unwrap()).unwrap();
    assert_eq!(
        checkpoint.nodes[0].resolution.identity_revisions.as_ref(),
        &vec![revision.clone()]
    );
    let batch = merge_node_batch(&[make(revision.clone()), make(revision.clone())]).unwrap();
    assert_eq!(batch.identity_revisions.len(), 1);
    let mut newer = revision.clone();
    newer.revision += 1;
    assert!(matches!(
        merge_node_batch(&[make(revision), make(newer)]),
        Err(PipelineError::IdentityRevisionChanged)
    ));
}

struct ContractIdentity(kg_core::traits::StageContract, bool);
#[async_trait::async_trait]
impl Stage for ContractIdentity {
    fn name(&self) -> &str {
        "contract_identity"
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        self.0
    }
    fn is_batch(&self) -> bool {
        self.1
    }
    async fn process(
        &self,
        input: StageOutput,
        _: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        Ok(input)
    }
}

#[test]
fn handoff_and_batch_contract_changes_invalidate_replay_fingerprints() {
    use kg_core::traits::StageKind;
    let ctx = context();
    let mut fingerprints = HashSet::new();
    for (contract, batch) in [
        (
            &[(StageKind::NodeResolution, StageKind::NodeResolution)][..],
            false,
        ),
        (
            &[
                (StageKind::NodeResolution, StageKind::NodeResolution),
                (StageKind::Empty, StageKind::Empty),
            ][..],
            false,
        ),
        (
            &[(StageKind::NodeResolution, StageKind::NodeResolution)][..],
            true,
        ),
    ] {
        let pipeline = runner().node_stage(Arc::new(ContractIdentity(contract, batch)));
        pipeline.validate_topology().unwrap();
        assert!(fingerprints.insert(
            RequestFingerprint::compute("org", &[], &pipeline.settings(&ctx))
                .unwrap()
                .0
        ));
    }
}

#[tokio::test]
async fn workers_reject_outputs_outside_the_declared_contract() {
    use kg_core::traits::StageKind;
    for batch in [false, true] {
        let stage: Arc<dyn Stage> = Arc::new(ContractIdentity(
            &[(StageKind::Empty, StageKind::NodeExtraction)],
            batch,
        ));
        let error = run_phase(
            &[stage],
            vec![PipelineMessage {
                snapshot_index: 0,
                run_id: Uuid::new_v4(),
                state: StageOutput::Empty,
            }],
            context(),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("undeclared payload"));
    }
}

struct BatchPass {
    name: &'static str,
    fail: bool,
    /// start/complete/error events, in order, shared by every stage of one chain.
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Stage for BatchPass {
    fn name(&self) -> &str {
        self.name
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind::*;
        match self.name {
            "plan" => &[(FlushBatch, PlannedBatch)],
            "embed" => &[(PlannedBatch, PreparedBatch)],
            _ => &[(PreparedBatch, Committed)],
        }
    }
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        use kg_core::runtime::stage_output::{PlannedBatchOutput, PreparedBatchOutput};
        self.events
            .lock()
            .unwrap()
            .push(format!("start:{}", self.name));
        if self.fail {
            self.events
                .lock()
                .unwrap()
                .push(format!("error:{}", self.name));
            return Err(StageError::StateValidation {
                stage: self.name.into(),
                message: "injected failure".into(),
            });
        }
        Ok(match input {
            StageOutput::FlushBatch(flush) => StageOutput::PlannedBatch(PlannedBatchOutput {
                batch: kg_core::traits::MutationBatch {
                    org_id: ctx.org_id.to_string(),
                    batch: flush.batch,
                    fingerprint: flush.fingerprint,
                    preconditions: vec![],
                    mutations: vec![],
                    result: serde_json::json!({}),
                },
                embeddings: vec![],
            }),
            StageOutput::PlannedBatch(plan) => {
                StageOutput::PreparedBatch(PreparedBatchOutput { batch: plan.batch })
            }
            StageOutput::PreparedBatch(prepared) => StageOutput::Committed(CommitOutput {
                batch: prepared.batch.batch,
                replayed: false,
                committed_at: Utc::now(),
                counts: CommittedCounts::default(),
                recovery: None,
            }),
            _ => panic!("unexpected test handoff"),
        })
        .inspect(|_| {
            self.events
                .lock()
                .unwrap()
                .push(format!("complete:{}", self.name));
        })
    }
}

#[tokio::test]
async fn batch_chain_awaits_all_handoffs_and_attributes_failure_to_its_stage() {
    for fail in [None, Some("plan"), Some("embed"), Some("commit")] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let runner = PipelineRunner::new();
        let chain: Vec<Arc<dyn Stage>> = ["plan", "embed", "commit"]
            .into_iter()
            .map(|name| {
                Arc::new(BatchPass {
                    name,
                    fail: fail == Some(name),
                    events: events.clone(),
                }) as Arc<dyn Stage>
            })
            .collect();
        let identity = BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Node,
            index: 0,
        };
        let mut progress = Progress::default();
        let result = runner
            .commit_raw(
                &chain,
                &context(),
                identity,
                &RequestFingerprint::compute("org", &[], &serde_json::json!({})).unwrap(),
                &[],
                FlushWork::Nodes(NodeBatch::default()),
                None,
                &mut progress,
            )
            .await;
        assert_eq!(result.is_ok(), fail.is_none());
        assert_eq!(progress.batches.len(), usize::from(fail.is_none()));
        let mut expected = Vec::new();
        for name in ["plan", "embed", "commit"] {
            expected.push(format!("start:{name}"));
            if fail == Some(name) {
                expected.push(format!("error:{name}"));
                break;
            }
            expected.push(format!("complete:{name}"));
        }
        assert_eq!(*events.lock().unwrap(), expected);
    }
}

#[derive(Default)]
struct SummaryRecorder {
    calls: Mutex<Vec<u32>>,
    commits: Mutex<Vec<CommitOutput>>,
    fail_second: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl Stage for SummaryRecorder {
    fn name(&self) -> &str {
        "summary_recorder"
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::SummaryBatch, StageKind::Committed)]
    }
    async fn process(
        &self,
        input: StageOutput,
        _: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        use kg_core::runtime::stage_output::{SummaryManifest, SummaryWork};
        let StageOutput::SummaryBatch(input) = input else {
            panic!("expected summary input")
        };
        self.calls.lock().unwrap().push(input.batch.index);
        if input.batch.index == 2
            && self
                .fail_second
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "injected follow-up failure".into(),
            });
        }
        let mut recovery = BatchRecovery::default();
        let mut counts = CommittedCounts::default();
        match input.work {
            SummaryWork::Manifest {
                chain_ids,
                batch_size,
                collections,
            } => {
                recovery.summary_manifest = Some(SummaryManifest {
                    as_of: input.as_of,
                    chain_ids,
                    batch_size,
                    collections,
                });
            }
            SummaryWork::Refresh { chain_ids } => {
                counts.summaries_updated = chain_ids.len();
            }
        }
        let commit = CommitOutput {
            batch: input.batch,
            replayed: false,
            committed_at: Utc::now(),
            counts,
            recovery: Some(recovery),
        };
        self.commits.lock().unwrap().push(commit.clone());
        Ok(StageOutput::Committed(commit))
    }
}

#[tokio::test]
async fn summary_manifest_freezes_targets_and_resume_skips_completed_followups() {
    use kg_core::runtime::entity_summary::EntitySummarySettings;
    let mut context = context();
    Arc::get_mut(&mut context).unwrap().entity_summary_settings = EntitySummarySettings {
        enabled: true,
        batch_size: 2,
        ..Default::default()
    };
    let stage = Arc::new(SummaryRecorder::default());
    stage
        .fail_second
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let runner = PipelineRunner::new().summary_stage(stage.clone());
    let run = Uuid::new_v4();
    let fingerprint = RequestFingerprint::compute("org", &[], &serde_json::json!({})).unwrap();
    let at = Utc::now();
    let mut progress = Progress::default();
    progress.summary_affected_chains.extend([
        Uuid::from_u128(10),
        Uuid::from_u128(20),
        Uuid::from_u128(30),
    ]);
    assert!(runner
        .summarize(&context, run, &fingerprint, &[], at, &[], &mut progress)
        .await
        .is_err());
    assert_eq!(progress.committed.summaries_updated, 2);
    assert_eq!(*stage.calls.lock().unwrap(), vec![0, 1, 2]);
    let commits = stage.commits.lock().unwrap().clone();
    let mut resumed = Progress::default();
    for mut commit in commits {
        commit.replayed = true;
        resumed.record(&commit).unwrap();
    }
    // Additional current graph state cannot shift the already receipted partitions.
    resumed.summary_affected_chains.insert(Uuid::from_u128(1));
    runner
        .summarize(&context, run, &fingerprint, &[], at, &[], &mut resumed)
        .await
        .unwrap();
    assert_eq!(*stage.calls.lock().unwrap(), vec![0, 1, 2, 2]);
    assert_eq!(resumed.committed.summaries_updated, 3);
    assert_eq!(resumed.newly_committed.summaries_updated, 1);
    assert_eq!(resumed.summary_manifest.unwrap().chain_ids.len(), 3);
}

#[tokio::test]
async fn registration_preserves_retryability_and_conflict_classification() {
    for (error, retriable) in [
        (BackendError::Unavailable("offline".into()), true),
        (BackendError::Timeout(1000), true),
        (BackendError::Auth("denied".into()), false),
        (BackendError::Conflict("changed request".into()), false),
    ] {
        let conflict = matches!(error, BackendError::Conflict(_));
        let result = register_run(&context(), async { Err(error) })
            .await
            .unwrap_err();
        assert_eq!(result.is_retriable(), retriable);
        assert_eq!(
            matches!(result, PipelineError::StateValidation { .. }),
            conflict
        );
    }
}

#[tokio::test(start_paused = true)]
async fn registration_deadline_bounds_a_backend_that_never_returns() {
    let error = register_run(&context(), std::future::pending())
        .await
        .unwrap_err();
    assert!(
        matches!(error, PipelineError::StepExecution { ref step, retriable: true, .. } if step == "register_run")
    );
}

#[tokio::test]
async fn cancellation_interrupts_registration_after_it_starts() {
    let ctx = context();
    let cancel = ctx.cancel.clone();
    let (started, waiting) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        register_run(&ctx, async {
            started.send(()).unwrap();
            std::future::pending().await
        })
        .await
    });
    waiting.await.unwrap();
    cancel.cancel();
    assert!(matches!(task.await.unwrap(), Err(PipelineError::Cancelled)));
}

#[tokio::test]
async fn saga_maintenance_rejects_invalid_chain_before_storage() {
    let mut ctx = context();
    Arc::make_mut(&mut ctx).saga_summary_settings.enabled = true;
    let runner = PipelineRunner::new()
        .saga_summary_stage(Arc::new(CountingStage::new("not_a_summary_chain")));
    let error = runner
        .summarize_saga("test".into(), Uuid::new_v4(), ctx, Uuid::new_v4())
        .await
        .unwrap_err();
    assert!(
        matches!(error, PipelineError::StateValidation { ref stage, .. } if stage == "topology")
    );
}

#[tokio::test]
async fn batch_stage_rejects_unsupported_input_before_invoking_stage() {
    let stage = Arc::new(MaterializeStage);
    let identity = BatchIdentity {
        run_id: Uuid::new_v4(),
        kind: BatchKind::Node,
        index: 0,
    };
    let fingerprint = RequestFingerprint::compute("org", &[], &serde_json::json!({})).unwrap();
    let error = runner()
        .submit_output(
            &[stage],
            &context(),
            identity,
            &fingerprint,
            StageOutput::Empty,
            &mut Progress::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StageError::StateValidation { ref message, .. } if message == "batch stage received an unsupported payload")
    );
}

struct RevisionRace {
    attempts: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl Stage for RevisionRace {
    fn name(&self) -> &str {
        "revision_race"
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind::*;
        &[(NodeExtraction, NodeResolution)]
    }
    fn is_batch(&self) -> bool {
        true
    }
    async fn process(&self, _: StageOutput, _: &RuntimeContext) -> Result<StageOutput, StageError> {
        panic!("batch only")
    }
    async fn process_batch(
        &self,
        inputs: Vec<StageOutput>,
        _: &RuntimeContext,
    ) -> Result<Vec<Result<StageOutput, kg_core::errors::StageError>>, StageError> {
        use kg_core::traits::{IdentityRevision, IdentityScope};
        let attempt = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                let StageOutput::NodeExtraction(extraction) = input else {
                    panic!("extraction required")
                };
                let mut nodes = Vec::new();
                let mut observed = Vec::new();
                for (snapshot, entities) in extraction.entities_by_snapshot.iter() {
                    for entity in entities {
                        let mut original =
                            kg_core::runtime::stage_output::ObservedEntityProperties::from_entity(
                                entity, *snapshot,
                            );
                        original.raw_mention_id = extraction
                            .raw_text_drafts
                            .iter()
                            .flat_map(|draft| &draft.mentions)
                            .find(|mention| mention.observation_uuid == entity.uuid)
                            .map(|mention| mention.id);
                        observed.push(original);
                        let mut canonical = entity.clone();
                        canonical.name = format!("canonical-attempt-{attempt}");
                        canonical.uuid = Uuid::new_v4();
                        nodes.push(kg_core::runtime::stage_output::Observed::new(
                            entity.uuid,
                            canonical,
                        ));
                    }
                }
                StageOutput::NodeResolution(kg_core::runtime::stage_output::NodeResolutionOutput {
                    raw_text_drafts: extraction.raw_text_drafts,
                    schemas: extraction.schemas,
                    snapshot_nodes: extraction.snapshot_nodes,
                    observed_properties: Arc::new(observed),
                    nodes_unchanged: Arc::new(nodes),
                    identity_revisions: Arc::new(vec![IdentityRevision {
                        scope: IdentityScope {
                            namespace: "prod".into(),
                            entity_type: "Service".into(),
                        },
                        revision: if attempt == 0 { index as u64 } else { 2 },
                    }]),
                    ..Default::default()
                })
            })
            .map(Ok)
            .collect())
    }
}

#[tokio::test]
async fn mixed_node_revisions_replan_without_repeating_extraction_or_committing_stale_work() {
    use kg_core::runtime::stage_output::NodeExtractionOutput;
    let ctx = context();
    let extraction = Arc::new(CountingStage::new("frozen_extraction"));
    let resolution = Arc::new(RevisionRace {
        attempts: Default::default(),
    });
    let flush = Arc::new(RecordingFlush::new());
    let runner = PipelineRunner::new()
        .node_stage(extraction.clone())
        .resolution_stage(resolution.clone())
        .flush(flush.clone());
    let run_id = Uuid::new_v4();
    let mut messages = Vec::new();
    let mut expected_raw = Vec::new();
    for snapshot_index in 0..2 {
        let (_, fixture) = raw_resolution_fixture(&ctx).await;
        expected_raw.push(serde_json::to_value(&fixture.raw_text_drafts).unwrap());
        messages.push(PipelineMessage {
            snapshot_index,
            run_id,
            state: StageOutput::NodeExtraction(NodeExtractionOutput {
                raw_text_drafts: fixture.raw_text_drafts,
                schemas: fixture.schemas,
                entities_by_snapshot: Arc::new(vec![(
                    fixture.snapshot_nodes[0].uuid,
                    fixture
                        .nodes_to_create
                        .iter()
                        .map(|n| n.value.clone())
                        .collect(),
                )]),
                snapshot_nodes: fixture.snapshot_nodes,
                relationship_changes: Default::default(),
                version_exclusions: Default::default(),
                text_observation_ids: Default::default(),
                fk_exclusions: Default::default(),
                history: Default::default(),
                source_deleted: Default::default(),
                sub_edges: Default::default(),
                incomplete_extractions: Default::default(),
            }),
        });
    }
    let fingerprint = RequestFingerprint::compute("org", &[], &serde_json::json!({})).unwrap();
    let mut progress = Progress::default();
    let outputs = runner
        .node_chunk(
            messages,
            0,
            &[],
            &ctx,
            run_id,
            &fingerprint,
            &runner.flush_stages,
            &[],
            &mut progress,
        )
        .await
        .unwrap();
    assert_eq!(outputs.len(), 2);
    for (output, expected) in outputs.iter().zip(expected_raw) {
        let StageOutput::NodeResolution(resolved) = &output.state else {
            panic!("resolution required")
        };
        assert_eq!(
            serde_json::to_value(&resolved.raw_text_drafts).unwrap(),
            expected
        );
        assert_eq!(resolved.nodes_unchanged[0].name, "canonical-attempt-1");
        resolved
            .validate_raw_text_drafts(&ctx.extraction_settings)
            .unwrap();
    }

    assert_eq!(
        extraction.count.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(
        resolution
            .attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(flush.batches.lock().unwrap().len(), 1);
    assert_eq!(progress.batches.len(), 1);
}

#[tokio::test]
async fn raw_mentions_pass_real_checkpoint_and_reject_changed_settings_before_commit() {
    let ctx = context();
    let (input, resolution) = raw_resolution_fixture(&ctx).await;
    let messages = [PipelineMessage {
        snapshot_index: 0,
        run_id: Uuid::new_v4(),
        state: StageOutput::NodeResolution(resolution),
    }];
    let receipt = checkpoint_nodes(&messages, vec![], &ctx.extraction_settings, None).unwrap();
    let restored: BatchRecovery =
        serde_json::from_slice(&serde_json::to_vec(&receipt).unwrap()).unwrap();
    let resolution = &restored.nodes[0].resolution;
    assert!(resolution.snapshot_nodes[0].content.is_none());
    assert_eq!(
        resolution.raw_text_drafts[0].mentions[0].evidence.name,
        "original-name"
    );
    resolution
        .validate_raw_text_drafts(&ctx.extraction_settings)
        .unwrap();
    let mut changed = ctx.extraction_settings.clone();
    changed.instructions = Some("different instructions".into());
    assert!(checkpoint_nodes(&messages, vec![], &changed, None).is_err());
    use kg_core::runtime::saga::{
        FrozenObservation, FrozenObservationKind, RunObservationManifest,
    };
    let snapshot = &resolution.snapshot_nodes[0];
    let mut replay_context = ctx.as_ref().clone();
    replay_context.observation_manifest = Some(Arc::new(RunObservationManifest {
        entries: vec![FrozenObservation {
            snapshot_uuid: snapshot.uuid,
            namespace: snapshot.namespace.clone(),
            created_at: snapshot.created_at,
            captured_at: snapshot.captured_at,
            kind: FrozenObservationKind::Fresh,
            saga: None,
            history: vec![],
        }],
        node_batches: vec![vec![0]],
    }));
    let inputs = [IngestionInput::Fresh(Box::new(input))];
    let replay = restore_nodes(
        restored.clone(),
        &inputs,
        messages[0].run_id,
        &replay_context,
        0,
    )
    .unwrap();
    let StageOutput::NodeResolution(replayed) = &replay[0].state else {
        panic!()
    };
    assert_eq!(
        replayed.snapshot_nodes[0].content.as_deref(),
        Some("original-name calls another service")
    );
    assert_eq!(
        replayed.raw_mention_mappings().unwrap(),
        resolution.raw_mention_mappings().unwrap()
    );
    let mut foreign_context = replay_context.clone();
    foreign_context.org_id = "other-org".into();
    assert!(restore_nodes(
        restored.clone(),
        &inputs,
        messages[0].run_id,
        &foreign_context,
        0,
    )
    .is_err());
    replay_context.extraction_settings = changed;
    assert!(restore_nodes(
        restored.clone(),
        &inputs,
        messages[0].run_id,
        &replay_context,
        0
    )
    .is_err());
    let mut oversized = restored;
    Arc::make_mut(&mut oversized.nodes[0].resolution.raw_text_drafts)[0].mentions[0]
        .evidence
        .name = "x".repeat(16 * 1024 * 1024);
    assert!(validate_node_checkpoint_size(&oversized).is_err());
}

async fn raw_resolution_fixture(
    ctx: &RuntimeContext,
) -> (
    SnapshotInput,
    kg_core::runtime::stage_output::NodeResolutionOutput,
) {
    use kg_core::runtime::{
        entity_drafts::{EntityMention, MentionId, RawMention, RawTextDraft},
        schemas::ObservationSchemas,
    };
    let input: SnapshotInput = serde_json::from_value(serde_json::json!({
        "namespace":"prod","name":"original-name","source":"logs","data_type":"text",
        "content":"original-name calls another service","entities":[],"captured_at":"2025-01-01T00:00:00Z"
    })).unwrap();
    let output = MaterializeStage
        .process(StageOutput::Input(Box::new(input.clone())), ctx)
        .await
        .unwrap();
    let StageOutput::NodeResolution(mut resolution) = output else {
        panic!()
    };
    let snapshot = &resolution.snapshot_nodes[0];
    let node = &resolution.nodes_to_create[0];
    let mention = EntityMention {
        name: "original-name".into(),
        entity_type: node.entity_type.clone(),
        properties: Default::default(),
        extracted_by: "llm:test".into(),
    };
    let schemas = ObservationSchemas {
        org_id: "org".into(),
        source: snapshot.source.clone(),
        definitions: std::collections::BTreeMap::from([(
            snapshot.source.clone(),
            Default::default(),
        )]),
    };
    let draft = RawTextDraft::new(
        snapshot,
        &schemas,
        &ctx.extraction_settings.for_source(&snapshot.source),
        vec![RawMention {
            id: MentionId::for_mention(snapshot.uuid, &mention).unwrap(),
            observation_uuid: node.observation_uuid,
            evidence: mention,
        }],
    )
    .unwrap();
    Arc::make_mut(&mut resolution.observed_properties)[0].raw_mention_id =
        Some(draft.mentions[0].id);
    resolution.schemas = Arc::new(HashMap::from([(snapshot.uuid, schemas)]));
    resolution.raw_text_drafts = Arc::new(vec![draft]);
    (input, resolution)
}

/// A graph that resumes both the node and relationship page of a run by
/// returning frozen receipts, and answers every read as empty. The node
/// receipt carries one recorded failure, as an interrupted partial page does.
struct ResumeWithFailure {
    node: CommittedBatch,
    relationship: CommittedBatch,
}

#[async_trait::async_trait]
impl kg_core::traits::SearchBackend for ResumeWithFailure {}

#[async_trait::async_trait]
impl kg_core::traits::GraphBackend for ResumeWithFailure {
    async fn resume_commit(
        &self,
        _org: &str,
        batch: BatchIdentity,
        _fingerprint: &RequestFingerprint,
        _cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Option<CommittedBatch>, BackendError> {
        Ok(Some(match batch.kind {
            BatchKind::Node => self.node.clone(),
            BatchKind::Relationship => self.relationship.clone(),
            other => panic!("unexpected resume for {other:?}"),
        }))
    }
    async fn apply_mutations(
        &self,
        _: &str,
        _: &[kg_core::traits::GraphMutation],
    ) -> Result<(), BackendError> {
        Ok(())
    }
    async fn register_run(
        &self,
        _: &kg_core::traits::RunHeader,
    ) -> Result<RunRegistration, BackendError> {
        Err(BackendError::Unavailable(
            "register_run is unused here".into(),
        ))
    }
    async fn commit_batch(
        &self,
        _: &kg_core::traits::MutationBatch,
    ) -> Result<CommittedBatch, BackendError> {
        Err(BackendError::Unavailable(
            "commit_batch is unused here".into(),
        ))
    }
    async fn committed_batches(
        &self,
        _: &str,
        _: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        Ok(Vec::new())
    }
    async fn find_entities(
        &self,
        _: &str,
        _: &EntityLookup,
    ) -> Result<Vec<kg_core::traits::EntityVersionRecord>, BackendError> {
        Ok(Vec::new())
    }
    async fn find_edges(
        &self,
        _: &str,
        _: &EdgeLookup,
    ) -> Result<Vec<kg_core::traits::EdgeRecord>, BackendError> {
        Ok(Vec::new())
    }
    async fn health(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn connect(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
}

fn resumed_receipt(run_id: Uuid, kind: BatchKind, recovery: &BatchRecovery) -> CommittedBatch {
    let mut result = serde_json::to_value(CommittedCounts::default()).unwrap();
    result["recovery"] = serde_json::to_value(recovery).unwrap();
    CommittedBatch {
        batch_id: Uuid::new_v4(),
        run_id,
        kind,
        index: 0,
        committed_at: Utc::now(),
        result,
        replayed: true,
    }
}

/// Regression for E1: a resumed paged commit must restore the batch's recorded
/// failures, so a partial run cannot report success and sweep the collection.
/// Pre-fix the resumed page's failure was dropped, `progress.failed` was empty,
/// and reconciliation ran; after the fix the failure is restored and the sweep
/// is suppressed with the "snapshots failed this run" outcome. Both resumed
/// pages changed and either can regress alone, so the same owner test drives the
/// failure from the node page and from the relationship page.
#[tokio::test]
async fn resumed_page_restores_failures_and_suppresses_reconciliation() {
    use kg_core::runtime::saga::{
        FrozenObservation, FrozenObservationKind, RunObservationManifest,
    };

    for failing in [BatchKind::Node, BatchKind::Relationship] {
        let run_id = Uuid::new_v4();
        let at = Utc::now();
        let settings = kg_core::runtime::extraction::ExtractionSettings::default();
        let snapshot = SnapshotInput {
            relationship_changes: Default::default(),
            saga: None,
            previous_snapshot_uuids: vec![],
            labels: Vec::new(),
            tags: Default::default(),
            namespace: "prod".into(),
            name: "s".into(),
            source_description: None,
            data_type: SnapshotDataType::Entities,
            snapshot_kind: SnapshotKind::Full,
            sync_generation: Some(2),
            complete: true,
            org_id: None,
            source: "test".into(),
            entities: vec![],
            content: None,
            entity_types: None,
            edge_types: None,
            edge_type_map: None,
            exclude_fk_properties: vec![],
            ignore_change_properties: vec![],
            captured_at: Some(at),
            collection: Some(kg_core::models::collection::CollectionScope {
                key: "services".into(),
                relationships_complete: false,
            }),
        };
        let clean_context = context();
        let clean_output = MaterializeStage
            .process(
                StageOutput::Input(Box::new(snapshot.clone())),
                &clean_context,
            )
            .await
            .unwrap();
        let StageOutput::NodeResolution(ref resolution) = clean_output else {
            panic!("materialization output")
        };
        let frozen_uuid = resolution.snapshot_nodes[0].uuid;
        let successful_node = checkpoint_nodes(
            &[PipelineMessage {
                snapshot_index: 0,
                run_id,
                state: clean_output,
            }],
            vec![],
            &settings,
            None,
        )
        .unwrap();
        // The interrupted page recorded one snapshot failure; its ordinal is the
        // only member of the frozen batch, so restoration covers the manifest.
        let with_failure = checkpoint_nodes(
            &[],
            vec![SnapshotFailure {
                profile: None,
                snapshot_index: 0,
                stage: "identity_resolution".into(),
                error: "snapshot failed before the page was interrupted".into(),
                retriable: false,
            }],
            &settings,
            None,
        )
        .unwrap();
        let empty_recovery = checkpoint_nodes(&[], vec![], &settings, None).unwrap();
        // Place the frozen failure in the page under test and leave the other
        // page clean; a node-shaped empty recovery already serves the
        // relationship receipt, so the only variable is which page carries it.
        let (node_recovery, relationship_recovery) = match failing {
            BatchKind::Node => (&with_failure, &empty_recovery),
            BatchKind::Relationship => (&successful_node, &with_failure),
            other => panic!("unexpected failing kind {other:?}"),
        };
        let graph = Arc::new(ResumeWithFailure {
            node: resumed_receipt(run_id, BatchKind::Node, node_recovery),
            relationship: resumed_receipt(run_id, BatchKind::Relationship, relationship_recovery),
        });

        let llm = Arc::new(MockLlmBackend::empty());
        let mut ctx_inner = RuntimeContextBuilder::new("org")
            .graph(graph)
            .llm_extraction(llm.clone())
            .llm_default(llm)
            .embedder(Arc::new(MockEmbedBackend::default_dimension()))
            .build()
            .unwrap();
        ctx_inner.observation_manifest = Some(Arc::new(RunObservationManifest {
            entries: vec![FrozenObservation {
                snapshot_uuid: frozen_uuid,
                namespace: "prod".into(),
                created_at: at,
                captured_at: at,
                kind: FrozenObservationKind::Fresh,
                saga: None,
                history: vec![],
            }],
            node_batches: vec![vec![0]],
        }));
        let ctx = Arc::new(ctx_inner);

        let snapshots = vec![IngestionInput::Fresh(Box::new(snapshot))];

        let fingerprint = RequestFingerprint::compute("org", &[], &serde_json::json!({})).unwrap();
        let mut progress = Progress::default();
        PipelineRunner::new()
            .execute(
                &snapshots,
                &ctx,
                run_id,
                &fingerprint,
                &[],
                &[],
                at,
                &mut progress,
            )
            .await
            .unwrap();

        assert_eq!(
            progress.failed.len(),
            1,
            "{failing:?}: the resumed page's recorded failure must be restored"
        );
        let outcome = progress
            .collections
            .iter()
            .find(|c| c.collection.key == "services")
            .expect("the declared collection has an outcome");
        assert!(!outcome.swept, "{failing:?}: a partial run must not sweep");
        assert_eq!(
            outcome.reason.as_deref(),
            Some("snapshots failed this run"),
            "{failing:?}: reconciliation must be suppressed after a resumed partial run"
        );
    }
}
