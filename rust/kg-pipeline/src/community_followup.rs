//! Receipted Community preparation keeps paid work separate from atomic publication.
use super::*;
use kg_core::{
    community::*,
    runtime::{community::*, stage_output::PreparedBatchOutput},
    traits::{GraphMutation, MutationBatch, RunHeader, StageKind},
};

const SCOPE_STRIDE: u32 = 32_000_000;
const ATTEMPT_STRIDE: u32 = 1_000_000;
const SUMMARY_OFFSET: u32 = 100_001;
const BEGIN_OFFSET: u32 = 200_001;
const PARTITION_OFFSET: u32 = 200_002;
const PUBLISH_OFFSET: u32 = 400_003;

impl PipelineRunner {
    pub(super) fn validate_community_chain(&self) -> Result<(), PipelineError> {
        let expected = [
            (StageKind::CommunityRequest, StageKind::CommunityClusters),
            (StageKind::CommunityClusters, StageKind::CommunityDrafts),
            (StageKind::CommunityDrafts, StageKind::CommunityPrepared),
        ];
        if self.community_stages.len() != expected.len()
            || self
                .community_stages
                .iter()
                .zip(expected)
                .any(|(stage, transition)| !stage.contract().contains(&transition))
            || self.flush_stages.last().is_none_or(|stage| {
                !stage
                    .contract()
                    .contains(&(StageKind::PreparedBatch, StageKind::Committed))
            })
        {
            return Err(recovery_error(
                "Community maintenance requires detection, summary, name embedding and commit stages",
            ));
        }
        Ok(())
    }

    /// Rebuild one namespace and await complete guarded publication. Retrying the
    /// same run identity restores source fragments, summaries and vectors from receipts.
    #[tracing::instrument(name="pipeline.community_maintenance",skip_all,fields(run_id=%run_id,namespace=%namespace,otel.status_code=tracing::field::Empty))]
    pub async fn rebuild_communities(
        &self,
        namespace: String,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        use kg_core::telemetry::{OperationGuard, OperationKind, Outcome};
        let mut operation = OperationGuard::new(OperationKind::Maintenance);
        let result = self.rebuild_communities_inner(namespace, ctx, run_id).await;
        let outcome = match &result {
            Ok(output) if !output.is_complete() => Outcome::Failed,
            Ok(output)
                if !output.batches.is_empty()
                    && output.replayed_batches() == output.batches.len() =>
            {
                Outcome::Replayed
            }
            Ok(_) => Outcome::Success,
            Err(error) => Outcome::from(error),
        };
        operation.finish(outcome);
        tracing::Span::current().record(
            "otel.status_code",
            if matches!(outcome, Outcome::Success | Outcome::Replayed) {
                "OK"
            } else {
                "ERROR"
            },
        );
        result
    }

    async fn rebuild_communities_inner(
        &self,
        namespace: String,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        self.validate_community_chain()?;
        if namespace.trim().is_empty() || ctx.org_id.trim().is_empty() || run_id.is_nil() {
            return Err(recovery_error(
                "Community maintenance requires scope and a nonnil run id",
            ));
        }
        ctx.community_settings.validate().map_err(recovery_error)?;
        check_cancel(&ctx)?;
        let started = Instant::now();
        let fingerprint=RequestFingerprint::compute_inputs(&ctx.org_id,&[],&serde_json::json!({"maintenance":"communities","namespace":namespace,"settings":self.settings(&ctx)})).map_err(|_|recovery_error("invalid maintenance fingerprint"))?;
        let header = RunHeader {
            org_id: ctx.org_id.to_string(),
            run_id,
            fingerprint: fingerprint.clone(),
            settings_version: SETTINGS_VERSION.into(),
            capture_default: Utc::now(),
            batch_plan: vec![PlannedBatch {
                kind: BatchKind::Community,
                index: 0,
                items: 1,
            }],
            observation_manifest: Default::default(),
            rule_freezes: vec![],
            schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
                profiles: Default::default(),
                org_id: ctx.org_id.to_string(),
                sources: Default::default(),
            },
        };
        let registration = register_run(&ctx, ctx.graph.register_run(&header)).await?;
        let (at, receipts) = match registration {
            RunRegistration::Registered => (header.capture_default, vec![]),
            RunRegistration::Resumed {
                capture_default,
                committed,
                ..
            } => (capture_default, committed),
        };
        let mut progress = Progress::default();
        for receipt in &receipts {
            progress
                .record(&restore_commit(run_id, receipt)?)
                .map_err(|error| recovery_error(error.to_string()))?;
        }
        let request = CommunityRequestOutput {
            namespace,
            generation: Uuid::nil(),
            operation: CommunityOperation::Full,
            as_of: at,
        };
        let result = self
            .community_targets(&ctx, run_id, &fingerprint, vec![request], &mut progress)
            .await;
        if let Err(cause) = result {
            let error = PipelineError::Aborted {
                run_id,
                committed: Box::new(progress.committed),
                batches_committed: progress.batches.len(),
                commit_unknown: progress.commit_unknown,
                cause: Box::new(cause),
            };
            return Err(error);
        }
        tracing::info!(
            batches = progress.batches.len(),
            communities = progress.committed.communities_updated,
            duration_ms = started.elapsed().as_millis() as u64,
            "Community maintenance committed"
        );
        Ok(PipelineOutput {
            profile_diagnostics: progress.profile_diagnostics,
            run_id,
            committed: progress.committed,
            newly_committed: progress.newly_committed,
            batches: progress.batches,
            duration_ms: started.elapsed().as_millis() as u64,
            ..Default::default()
        })
    }

    pub(super) async fn update_communities(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        as_of: DateTime<Utc>,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        let requests = if let Some(CommunityCheckpoint {
            step: CommunityCheckpointStep::Targets(manifest),
            ..
        }) = progress.community_checkpoints.get(&0)
        {
            manifest.requests.clone()
        } else {
            let chains = progress
                .summary_affected_chains
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let mut grouped = BTreeMap::<String, Vec<Uuid>>::new();
            let read = async {
                let _permit = ctx
                    .semaphore
                    .acquire()
                    .await
                    .map_err(|_| PipelineError::Cancelled)?;
                for page in chains.chunks(1000) {
                    let result = ctx
                        .graph
                        .read_community(
                            &ctx.org_id,
                            &CommunityRead::Scopes {
                                chain_ids: page.to_vec(),
                            },
                        )
                        .await
                        .map_err(|_| recovery_error("Community scope routing read failed"))?;
                    let CommunityReadResult::Scopes(scopes) = result else {
                        return Err(recovery_error(
                            "Community scope routing returned wrong result",
                        ));
                    };
                    let mut seen = HashSet::new();
                    for scope in scopes {
                        if scope.namespace.trim().is_empty()
                            || !page.contains(&scope.chain_id)
                            || !seen.insert(scope.chain_id)
                        {
                            return Err(recovery_error(
                                "Community scope routing returned invalid chains",
                            ));
                        }
                        grouped
                            .entry(scope.namespace)
                            .or_default()
                            .push(scope.chain_id);
                    }
                    if seen.len() != page.len() {
                        return Err(recovery_error("Community affected chain scope is missing"));
                    }
                }
                Ok::<(), PipelineError>(())
            };
            tokio::select! {biased;_=ctx.cancel.cancelled()=>return Err(PipelineError::Cancelled),result=tokio::time::timeout(std::time::Duration::from_millis(ctx.community_settings.timeout_ms),read)=>result.map_err(|_|recovery_error("Community scope routing timed out"))??,}
            grouped
                .into_iter()
                .map(|(namespace, chain_ids)| CommunityRequestOutput {
                    namespace,
                    generation: Uuid::nil(),
                    operation: CommunityOperation::Incremental { chain_ids },
                    as_of,
                })
                .collect()
        };
        self.community_targets(ctx, run_id, fingerprint, requests, progress)
            .await
    }

    pub(super) async fn community_targets(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        requests: Vec<CommunityRequestOutput>,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        self.validate_community_chain()?;
        let mut context = ctx.as_ref().clone();
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(ctx.community_settings.max_run_ms);
        context.identity_deadline = Some(
            context
                .identity_deadline
                .map_or(deadline, |existing| existing.min(deadline)),
        );
        let context = Arc::new(context);
        let ctx = &context;
        if requests.len() > ctx.community_settings.max_namespaces {
            return Err(recovery_error("Community namespace limit exceeded"));
        }
        let manifest = if let Some(checkpoint) = progress.community_checkpoints.get(&0) {
            let CommunityCheckpointStep::Targets(targets) = &checkpoint.step else {
                return Err(recovery_error("Community target receipt is malformed"));
            };
            targets.clone()
        } else {
            let manifest = CommunityTargetManifest {
                requests,
                collections: progress.collections.clone(),
            };
            self.community_commit(
                ctx,
                run_id,
                fingerprint,
                0,
                CommunityCheckpoint {
                    namespace: String::new(),
                    attempt: 0,
                    step: CommunityCheckpointStep::Targets(manifest.clone()),
                },
                vec![],
                Default::default(),
                progress,
            )
            .await?;
            manifest
        };
        if manifest.requests.len() > ctx.community_settings.max_namespaces {
            return Err(recovery_error(
                "Community target receipt exceeds namespace bounds",
            ));
        }
        let mut scopes = HashSet::new();
        for (ordinal, request) in manifest.requests.into_iter().enumerate() {
            if request.namespace.trim().is_empty() || !scopes.insert(request.namespace.clone()) {
                return Err(recovery_error("Community target scopes are invalid"));
            }
            self.community_scope(ctx, run_id, fingerprint, ordinal, request, progress)
                .await?;
        }
        Ok(())
    }

    async fn community_scope(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        ordinal: usize,
        request: CommunityRequestOutput,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        let prior = progress
            .community_checkpoints
            .values()
            .filter(|checkpoint| checkpoint.namespace == request.namespace)
            .collect::<Vec<_>>();
        if prior
            .iter()
            .any(|checkpoint| matches!(checkpoint.step, CommunityCheckpointStep::Published))
        {
            return Ok(());
        }
        let first = prior
            .iter()
            .map(|checkpoint| checkpoint.attempt)
            .max()
            .unwrap_or(0);
        for attempt in first..=ctx.community_settings.max_replans {
            check_cancel(ctx)?;
            let base = 1 + (ordinal as u32) * SCOPE_STRIDE + attempt * ATTEMPT_STRIDE;
            let mut selected = request.clone();
            selected.generation = Uuid::new_v5(
                &run_id,
                format!("community:{}:{attempt}", selected.namespace).as_bytes(),
            );
            if attempt > 0 {
                selected.as_of = Utc::now();
            }
            match self
                .community_attempt(ctx, run_id, fingerprint, base, attempt, selected, progress)
                .await
            {
                Ok(()) => return Ok(()),
                Err(PipelineError::StepExecution {
                    ref stage,
                    ref step,
                    ..
                }) if stage == "community"
                    && step == "replan"
                    && attempt < ctx.community_settings.max_replans =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(recovery_error("Community replan budget exhausted"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn community_attempt(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        base: u32,
        attempt: u32,
        request: CommunityRequestOutput,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        let existing_plan = match progress.community_checkpoints.get(&base) {
            Some(CommunityCheckpoint {
                step: CommunityCheckpointStep::Plan(plan),
                ..
            }) => Some(plan.clone()),
            Some(_) => return Err(recovery_error("Community plan checkpoint has wrong kind")),
            None => None,
        };
        let complete = existing_plan.as_ref().is_some_and(|plan| {
            (0..plan.fragment_hashes.len()).all(|index| {
                progress
                    .community_checkpoints
                    .contains_key(&(base + 1 + index as u32))
            })
        });
        let plan = if complete {
            existing_plan.unwrap()
        } else {
            let selected = existing_plan
                .as_ref()
                .map(|plan| plan.request.clone())
                .unwrap_or(request);
            let output = self
                .run_community_stage(ctx, 0, StageOutput::CommunityRequest(selected))
                .await?;
            let StageOutput::CommunityClusters(clusters) = output else {
                return Err(recovery_error("Community detection returned wrong handoff"));
            };
            let (plan, fragments) = freeze_clusters(clusters, &ctx.community_settings)?;
            if let Some(old) = &existing_plan {
                if checkpoint_hash(old).map_err(recovery_error)?
                    != checkpoint_hash(&plan).map_err(recovery_error)?
                {
                    return Err(replan(
                        "Community projection changed before checkpoint completion",
                    ));
                }
            } else {
                self.community_commit(
                    ctx,
                    run_id,
                    fingerprint,
                    base,
                    CommunityCheckpoint {
                        namespace: plan.request.namespace.clone(),
                        attempt,
                        step: CommunityCheckpointStep::Plan(plan.clone()),
                    },
                    vec![],
                    Default::default(),
                    progress,
                )
                .await?;
            }
            for (index, fragment) in fragments.into_iter().enumerate() {
                self.community_commit(
                    ctx,
                    run_id,
                    fingerprint,
                    base + 1 + index as u32,
                    CommunityCheckpoint {
                        namespace: plan.request.namespace.clone(),
                        attempt,
                        step: CommunityCheckpointStep::Members(fragment),
                    },
                    vec![],
                    Default::default(),
                    progress,
                )
                .await?;
            }
            plan
        };
        if plan.valid_until.is_some_and(|until| until <= Utc::now()) {
            return Err(replan("Community projection expired before publication"));
        }
        let clusters = restore_clusters(&plan, base, progress)?;
        let mut prepared = Vec::with_capacity(clusters.len());
        for (index, cluster) in clusters.into_iter().enumerate() {
            let summary_index = base + SUMMARY_OFFSET + index as u32 * 2;
            let vector_index = summary_index + 1;
            let members = cluster
                .members
                .iter()
                .map(|member| CommunityMember {
                    entity_uuid: member.uuid,
                    chain_id: member.chain_id,
                })
                .collect::<Vec<_>>();
            let definition = if let Some(checkpoint) =
                progress.community_checkpoints.get(&vector_index)
            {
                let CommunityCheckpointStep::Vector {
                    cluster_index,
                    definition,
                } = &checkpoint.step
                else {
                    return Err(recovery_error("Community vector checkpoint is malformed"));
                };
                if *cluster_index != index {
                    return Err(recovery_error(
                        "Community vector checkpoint changed cluster",
                    ));
                }
                definition.clone()
            } else {
                let definition = if let Some(checkpoint) =
                    progress.community_checkpoints.get(&summary_index)
                {
                    let CommunityCheckpointStep::Summary {
                        cluster_index,
                        definition,
                    } = &checkpoint.step
                    else {
                        return Err(recovery_error("Community summary checkpoint is malformed"));
                    };
                    if *cluster_index != index {
                        return Err(recovery_error(
                            "Community summary checkpoint changed cluster",
                        ));
                    }
                    definition.clone()
                } else {
                    let output = self
                        .run_community_stage(
                            ctx,
                            1,
                            StageOutput::CommunityClusters(CommunityClustersOutput {
                                request: plan.request.clone(),
                                state: plan.state.clone(),
                                valid_until: plan.valid_until,
                                clusters: vec![cluster.clone()],
                            }),
                        )
                        .await?;
                    let StageOutput::CommunityDrafts(mut drafts) = output else {
                        return Err(recovery_error("Community summary returned wrong handoff"));
                    };
                    if drafts.drafts.len() != 1 {
                        return Err(recovery_error(
                            "Community summary changed cluster cardinality",
                        ));
                    }
                    let draft = drafts.drafts.pop().unwrap();
                    if draft.members != members
                        || draft.definition.node.uuid != cluster.uuid
                        || draft.expected_revision != cluster.expected_revision
                    {
                        return Err(recovery_error(
                            "Community summary changed membership or identity",
                        ));
                    }
                    self.community_commit(
                        ctx,
                        run_id,
                        fingerprint,
                        summary_index,
                        CommunityCheckpoint {
                            namespace: plan.request.namespace.clone(),
                            attempt,
                            step: CommunityCheckpointStep::Summary {
                                cluster_index: index,
                                definition: draft.definition.clone(),
                            },
                        },
                        vec![],
                        Default::default(),
                        progress,
                    )
                    .await?;
                    draft.definition
                };
                let output = self
                    .run_community_stage(
                        ctx,
                        2,
                        StageOutput::CommunityDrafts(CommunityDraftsOutput {
                            request: plan.request.clone(),
                            state: plan.state.clone(),
                            valid_until: plan.valid_until,
                            drafts: vec![CommunityDraft {
                                definition,
                                expected_revision: cluster.expected_revision,
                                members: members.clone(),
                            }],
                        }),
                    )
                    .await?;
                let StageOutput::CommunityPrepared(mut output) = output else {
                    return Err(recovery_error("Community embedding returned wrong handoff"));
                };
                if output.drafts.len() != 1 {
                    return Err(recovery_error(
                        "Community embedding changed cluster cardinality",
                    ));
                }
                let draft = output.drafts.pop().unwrap();
                if draft.members != members || draft.definition.node.uuid != cluster.uuid {
                    return Err(recovery_error(
                        "Community embedding changed membership or identity",
                    ));
                }
                self.community_commit(
                    ctx,
                    run_id,
                    fingerprint,
                    vector_index,
                    CommunityCheckpoint {
                        namespace: plan.request.namespace.clone(),
                        attempt,
                        step: CommunityCheckpointStep::Vector {
                            cluster_index: index,
                            definition: draft.definition.clone(),
                        },
                    },
                    vec![],
                    Default::default(),
                    progress,
                )
                .await?;
                draft.definition
            };
            definition
                .validate(&ctx.org_id, &plan.request.namespace)
                .map_err(|_| recovery_error("Community vector receipt is invalid"))?;
            prepared.push(CommunityDraft {
                definition,
                expected_revision: cluster.expected_revision,
                members,
            });
        }
        self.publish_communities(
            ctx,
            run_id,
            fingerprint,
            base,
            attempt,
            &plan,
            prepared,
            progress,
        )
        .await
    }

    async fn run_community_stage(
        &self,
        ctx: &Arc<RuntimeContext>,
        index: usize,
        input: StageOutput,
    ) -> Result<StageOutput, PipelineError> {
        let stage = &self.community_stages[index];
        let span = tracing::info_span!(
            "pipeline.community_stage",
            stage = stage.name(),
            otel.status_code = tracing::field::Empty
        );
        async {
        use kg_core::telemetry::{OperationGuard, Outcome};
        let mut measurement = OperationGuard::stage(stage.name());
        if let Err(error) = check_cancel(ctx) {
            measurement.finish(Outcome::from(&error));
            tracing::Span::current().record("otel.status_code", "ERROR");
            return Err(error);
        }
        let input_kind = StageKind::of(&input);
        let deadline = ctx.identity_deadline.unwrap_or_else(|| {
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(ctx.community_settings.timeout_ms)
        });
        let result = tokio::select! {
            biased;
            _=ctx.cancel.cancelled()=>Err(StageError::Cancelled{stage:stage.name().into()}),
            result=tokio::time::timeout_at(deadline,stage.process(input,ctx))=>result.unwrap_or_else(|_|Err(StageError::StepFailed{stage:stage.name().into(),step:"maintenance_budget".into(),cause:"Community maintenance deadline exceeded".into(),retriable:true})),
        };
        let measured_outcome = result.as_ref().map_or_else(Outcome::from, |_| Outcome::Success);
        let result = match result {
            Ok(output)
                if stage
                    .contract()
                    .contains(&(input_kind, StageKind::of(&output))) =>
            {
                Ok(output)
            }
            Ok(_) => Err(recovery_error("Community stage violated its typed handoff")),
            Err(error) => {
                match error {
                    StageError::Cancelled { .. } => Err(PipelineError::Cancelled),
                    StageError::CommitRejected { .. } | StageError::IdentityRevisionChanged => {
                        Err(replan("Community source changed during preparation"))
                    }
                    _ => Err(PipelineError::StepExecution {
                        stage: stage.name().into(),
                        step: "process".into(),
                        cause: error.to_string(),
                        retriable: error.is_retriable(),
                    }),
                }
            }
        };
        measurement.finish(if result.is_err() && measured_outcome == Outcome::Success { Outcome::Failed } else { measured_outcome });
        tracing::Span::current().record("otel.status_code", if result.is_ok() { "OK" } else { "ERROR" });
        result
        }.instrument(span).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn community_commit(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        index: u32,
        checkpoint: CommunityCheckpoint,
        mutations: Vec<GraphMutation>,
        counts: CommittedCounts,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        if let Some(stored) = progress.community_checkpoints.get(&index) {
            if checkpoint_hash(stored).map_err(recovery_error)?
                != checkpoint_hash(&checkpoint).map_err(recovery_error)?
            {
                return Err(recovery_error(
                    "Community receipt identity was reused with changed work",
                ));
            }
            return Ok(());
        }
        if serde_json::to_vec(&checkpoint)
            .map_err(|_| recovery_error("invalid Community checkpoint"))?
            .len()
            > ctx.community_settings.max_checkpoint_bytes
        {
            return Err(recovery_error(
                "Community checkpoint exceeds configured byte bound",
            ));
        }
        let identity = BatchIdentity {
            run_id,
            kind: BatchKind::Community,
            index,
        };
        let expected_checkpoint_hash = checkpoint_hash(&checkpoint).map_err(recovery_error)?;
        let batch = MutationBatch {
            org_id: ctx.org_id.to_string(),
            batch: identity,
            fingerprint: fingerprint.clone(),
            preconditions: vec![],
            mutations,
            result: {
                let mut result = serde_json::to_value(counts)
                    .map_err(|_| recovery_error("invalid Community commit counts"))?;
                result["recovery"] = serde_json::to_value(BatchRecovery {
                    community_checkpoint: Some(checkpoint),
                    ..Default::default()
                })
                .map_err(|_| recovery_error("invalid Community recovery"))?;
                result
            },
        };
        let stages = &self.flush_stages[self.flush_stages.len() - 1..];
        match self
            .submit_output(
                stages,
                ctx,
                identity,
                fingerprint,
                StageOutput::PreparedBatch(PreparedBatchOutput { batch }),
                progress,
            )
            .await
        {
            Ok(_) => {
                let stored = progress.community_checkpoints.get(&index).ok_or_else(|| {
                    recovery_error("Community commit omitted recovery checkpoint")
                })?;
                if checkpoint_hash(stored).map_err(recovery_error)? != expected_checkpoint_hash {
                    return Err(PipelineError::StepExecution{stage:"community".into(),step:"receipt_winner".into(),cause:"concurrent preparation selected another durable checkpoint; resume from its receipt".into(),retriable:true});
                }
                Ok(())
            }
            Err(StageError::CommitRejected { .. } | StageError::IdentityRevisionChanged) => {
                Err(replan("Community publication source changed"))
            }
            Err(error) => Err(commit_failure(identity, error, progress)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_communities(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        base: u32,
        attempt: u32,
        plan: &CommunityPlan,
        drafts: Vec<CommunityDraft>,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        let checkpoint = |step| CommunityCheckpoint {
            namespace: plan.request.namespace.clone(),
            attempt,
            step,
        };
        match &plan.request.operation {
            CommunityOperation::Full => {
                let partitions =
                    partition_drafts(&drafts, ctx.community_settings.partition_members)?;
                let hashes = partitions
                    .iter()
                    .map(partition_hash)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| recovery_error("invalid Community partition"))?;
                let begin = BeginCommunityGeneration {
                    generation: plan.request.generation,
                    expected_state: plan.state.clone(),
                    projected_at: plan.request.as_of,
                    valid_until: plan.valid_until,
                    partition_hashes: hashes,
                    community_count: drafts.len() as u64,
                    member_count: drafts.iter().map(|draft| draft.members.len() as u64).sum(),
                };
                self.community_commit(
                    ctx,
                    run_id,
                    fingerprint,
                    base + BEGIN_OFFSET,
                    checkpoint(CommunityCheckpointStep::Begun),
                    vec![GraphMutation::BeginCommunityGeneration {
                        generation: Box::new(begin),
                    }],
                    Default::default(),
                    progress,
                )
                .await?;
                for (index, partition) in partitions.into_iter().enumerate() {
                    let counts = CommittedCounts {
                        communities_updated: partition.definitions.len(),
                        embeddings: partition.definitions.len(),
                        community_memberships: partition
                            .memberships
                            .iter()
                            .map(|chunk| chunk.members.len())
                            .sum(),
                        ..Default::default()
                    };
                    self.community_commit(
                        ctx,
                        run_id,
                        fingerprint,
                        base + PARTITION_OFFSET + index as u32,
                        checkpoint(CommunityCheckpointStep::Partition { index }),
                        vec![GraphMutation::StageCommunityPartition {
                            partition: Box::new(StageCommunityPartition {
                                namespace: plan.request.namespace.clone(),
                                generation: plan.request.generation,
                                index,
                                partition,
                            }),
                        }],
                        counts,
                        progress,
                    )
                    .await?;
                }
                self.community_commit(
                    ctx,
                    run_id,
                    fingerprint,
                    base + PUBLISH_OFFSET,
                    checkpoint(CommunityCheckpointStep::Published),
                    vec![GraphMutation::PublishCommunityGeneration {
                        publication: Box::new(PublishCommunityGeneration {
                            generation: plan.request.generation,
                            expected_state: plan.state.clone(),
                            publication_revision: Uuid::new_v5(
                                &plan.request.generation,
                                b"publication",
                            ),
                        }),
                    }],
                    CommittedCounts {
                        community_generations: 1,
                        ..Default::default()
                    },
                    progress,
                )
                .await
            }
            CommunityOperation::Incremental { .. } => {
                let mut pages = Vec::<Vec<GuardedCommunityWrite>>::new();
                let mut page = Vec::new();
                let mut members = 0usize;
                for draft in drafts {
                    if draft.members.len() > MAX_PAGE_SIZE {
                        return Err(recovery_error(
                            "incremental Community exceeds atomic membership bound",
                        ));
                    }
                    if members + draft.members.len() > MAX_PAGE_SIZE {
                        pages.push(page);
                        page = Vec::new();
                        members = 0;
                    }
                    members += draft.members.len();
                    page.push(GuardedCommunityWrite {
                        expected_revision: draft.expected_revision.ok_or_else(|| {
                            recovery_error("incremental Community lacks prior revision")
                        })?,
                        definition: draft.definition,
                        members: draft.members,
                    });
                }
                if !page.is_empty() {
                    pages.push(page);
                }
                let mut expected_state = plan.state.clone();
                for (index, communities) in pages.into_iter().enumerate() {
                    let publication_revision = Uuid::new_v5(
                        &BatchIdentity {
                            run_id,
                            kind: BatchKind::Community,
                            index: base + PARTITION_OFFSET + index as u32,
                        }
                        .batch_id(),
                        b"publication",
                    );
                    let counts = CommittedCounts {
                        communities_updated: communities.len(),
                        community_memberships: communities
                            .iter()
                            .map(|write| write.members.len())
                            .sum(),
                        embeddings: communities.len(),
                        ..Default::default()
                    };
                    self.community_commit(
                        ctx,
                        run_id,
                        fingerprint,
                        base + PARTITION_OFFSET + index as u32,
                        checkpoint(CommunityCheckpointStep::Partition { index }),
                        vec![GraphMutation::UpdateCommunities {
                            update: Box::new(UpdateCommunities {
                                generation: plan.request.generation,
                                expected_state: expected_state.clone(),
                                publication_revision,
                                communities,
                            }),
                        }],
                        counts,
                        progress,
                    )
                    .await?;
                    expected_state.publication_revision = Some(publication_revision);
                }
                self.community_commit(
                    ctx,
                    run_id,
                    fingerprint,
                    base + PUBLISH_OFFSET,
                    checkpoint(CommunityCheckpointStep::Published),
                    vec![GraphMutation::AssertCommunityState {
                        state: Box::new(expected_state),
                    }],
                    Default::default(),
                    progress,
                )
                .await
            }
        }
    }
}

fn freeze_clusters(
    output: CommunityClustersOutput,
    settings: &CommunitySettings,
) -> Result<(CommunityPlan, Vec<CommunityMemberFragment>), PipelineError> {
    if output.request.namespace.trim().is_empty()
        || output.state.namespace != output.request.namespace
        || output
            .valid_until
            .is_some_and(|until| until <= output.request.as_of)
        || (matches!(output.request.operation, CommunityOperation::Full)
            && output.request.generation.is_nil())
        || output
            .clusters
            .iter()
            .map(|cluster| cluster.members.len())
            .sum::<usize>()
            > settings.max_entities
        || serde_json::to_vec(&output)
            .map_err(|_| recovery_error("invalid Community projection"))?
            .len()
            > settings.max_projection_bytes
    {
        return Err(recovery_error(
            "Community projection exceeds scope or coverage bounds",
        ));
    }
    if output.clusters.len() > settings.max_clusters {
        return Err(recovery_error("Community cluster bound exceeded"));
    }
    if matches!(
        output.request.operation,
        CommunityOperation::Incremental { .. }
    ) && output
        .clusters
        .iter()
        .any(|cluster| cluster.members.len() > MAX_PAGE_SIZE)
    {
        return Err(recovery_error(
            "incremental Community membership exceeds atomic bounds; request a full namespace rebuild",
        ));
    }
    let mut headers = Vec::new();
    let mut fragments = Vec::new();
    let mut chains = HashSet::new();
    for (index, mut cluster) in output.clusters.into_iter().enumerate() {
        cluster.members.sort_by_key(|member| member.chain_id);
        if cluster.uuid.is_nil() || cluster.members.is_empty() {
            return Err(recovery_error(
                "Community detection returned an empty or unidentified cluster",
            ));
        }
        headers.push(CommunityClusterHeader {
            uuid: cluster.uuid,
            expected_revision: cluster.expected_revision,
            member_count: cluster.members.len(),
        });
        let mut fragment = CommunityMemberFragment {
            cluster_index: index,
            members: vec![],
        };
        let mut bytes = 1024usize;
        for member in cluster.members {
            if !chains.insert(member.chain_id) {
                return Err(recovery_error(
                    "Community detection assigned a chain more than once",
                ));
            }
            let size = serde_json::to_vec(&member)
                .map_err(|_| recovery_error("invalid Community member"))?
                .len()
                + 1;
            if size + 1024 > settings.max_checkpoint_bytes {
                return Err(recovery_error("Community member exceeds checkpoint bound"));
            }
            if bytes + size > settings.max_checkpoint_bytes {
                fragments.push(fragment);
                fragment = CommunityMemberFragment {
                    cluster_index: index,
                    members: vec![],
                };
                bytes = 1024;
            }
            bytes += size;
            fragment.members.push(member);
        }
        if !fragment.members.is_empty() {
            fragments.push(fragment);
        }
    }
    if fragments.len() > 100_000 {
        return Err(recovery_error("Community fragment limit exceeded"));
    }
    let fragment_hashes = fragments
        .iter()
        .map(checkpoint_hash)
        .collect::<Result<Vec<_>, _>>()
        .map_err(recovery_error)?;
    let plan = CommunityPlan {
        request: output.request,
        state: output.state,
        valid_until: output.valid_until,
        clusters: headers,
        fragment_hashes,
    };
    if serde_json::to_vec(&plan)
        .map_err(|_| recovery_error("invalid Community plan"))?
        .len()
        + 1024
        > settings.max_checkpoint_bytes
    {
        return Err(recovery_error(
            "Community plan metadata exceeds checkpoint bound",
        ));
    }
    Ok((plan, fragments))
}
fn restore_clusters(
    plan: &CommunityPlan,
    base: u32,
    progress: &Progress,
) -> Result<Vec<CommunityCluster>, PipelineError> {
    let mut clusters = plan
        .clusters
        .iter()
        .map(|header| CommunityCluster {
            uuid: header.uuid,
            expected_revision: header.expected_revision,
            members: vec![],
        })
        .collect::<Vec<_>>();
    for (index, hash) in plan.fragment_hashes.iter().enumerate() {
        let checkpoint = progress
            .community_checkpoints
            .get(&(base + 1 + index as u32))
            .ok_or_else(|| recovery_error("Community member checkpoint is missing"))?;
        let CommunityCheckpointStep::Members(fragment) = &checkpoint.step else {
            return Err(recovery_error("Community member checkpoint has wrong kind"));
        };
        if &checkpoint_hash(fragment).map_err(recovery_error)? != hash {
            return Err(recovery_error("Community member checkpoint digest changed"));
        }
        clusters
            .get_mut(fragment.cluster_index)
            .ok_or_else(|| recovery_error("Community member checkpoint names unknown cluster"))?
            .members
            .extend(fragment.members.clone());
    }
    if clusters
        .iter()
        .zip(&plan.clusters)
        .any(|(cluster, header)| cluster.members.len() != header.member_count)
    {
        return Err(recovery_error(
            "Community checkpoint membership is incomplete",
        ));
    }
    Ok(clusters)
}
fn partition_drafts(
    drafts: &[CommunityDraft],
    members_per_page: usize,
) -> Result<Vec<CommunityPartition>, PipelineError> {
    let mut partitions = Vec::new();
    for draft in drafts {
        partitions.push(CommunityPartition {
            definitions: vec![draft.definition.clone()],
            memberships: vec![],
        });
        for members in draft.members.chunks(members_per_page) {
            partitions.push(CommunityPartition {
                definitions: vec![],
                memberships: vec![CommunityMembershipChunk {
                    community_uuid: draft.definition.node.uuid,
                    members: members.to_vec(),
                }],
            });
        }
    }
    if partitions.len() > MAX_PARTITIONS {
        return Err(recovery_error(
            "Community publication exceeds partition limit",
        ));
    }
    Ok(partitions)
}
fn replan(message: &str) -> PipelineError {
    PipelineError::StepExecution {
        stage: "community".into(),
        step: "replan".into(),
        cause: message.into(),
        retriable: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn projection(count: usize) -> CommunityClustersOutput {
        let at = Utc::now();
        CommunityClustersOutput {
            request: CommunityRequestOutput {
                namespace: "prod".into(),
                generation: Uuid::new_v4(),
                operation: CommunityOperation::Full,
                as_of: at,
            },
            state: CommunityState {
                namespace: "prod".into(),
                source_revision: Default::default(),
                active_generation: None,
                publication_revision: None,
            },
            valid_until: None,
            clusters: vec![CommunityCluster {
                uuid: Uuid::new_v4(),
                expected_revision: None,
                members: (0..count)
                    .map(|_| CommunityEntity {
                        uuid: Uuid::new_v4(),
                        chain_id: Uuid::new_v4(),
                        name: "service".into(),
                        entity_type: "Service".into(),
                        text: "x".repeat(16 * 1024),
                        text_hash: "a".repeat(64),
                        valid_until: None,
                    })
                    .collect(),
            }],
        }
    }
    #[test]
    fn giant_cluster_freezes_as_bounded_fragments_and_restores_exact_membership() {
        let settings = CommunitySettings {
            max_checkpoint_bytes: 128 * 1024,
            ..Default::default()
        };
        let mut original = projection(31);
        original.clusters[0]
            .members
            .sort_by_key(|member| member.chain_id);
        let ids = original.clusters[0]
            .members
            .iter()
            .map(|member| member.uuid)
            .collect::<Vec<_>>();
        let (plan, fragments) = freeze_clusters(original, &settings).unwrap();
        assert!(fragments.len() > 1);
        let mut progress = Progress::default();
        for (index, fragment) in fragments.into_iter().enumerate() {
            let checkpoint = CommunityCheckpoint {
                namespace: "prod".into(),
                attempt: 0,
                step: CommunityCheckpointStep::Members(fragment),
            };
            assert!(
                serde_json::to_vec(&checkpoint).unwrap().len() <= settings.max_checkpoint_bytes
            );
            progress
                .community_checkpoints
                .insert(2 + index as u32, checkpoint);
        }
        let restored = restore_clusters(&plan, 1, &progress).unwrap();
        assert_eq!(
            restored[0]
                .members
                .iter()
                .map(|member| member.uuid)
                .collect::<Vec<_>>(),
            ids
        );
        progress.community_checkpoints.remove(&2);
        assert!(restore_clusters(&plan, 1, &progress).is_err());
    }
    #[test]
    fn frozen_source_digests_reject_changed_fragments() {
        let (plan, mut fragments) =
            freeze_clusters(projection(1), &CommunitySettings::default()).unwrap();
        fragments[0].members[0].text = "changed evidence".into();
        let mut progress = Progress::default();
        progress.community_checkpoints.insert(
            2,
            CommunityCheckpoint {
                namespace: "prod".into(),
                attempt: 0,
                step: CommunityCheckpointStep::Members(fragments.remove(0)),
            },
        );
        assert!(restore_clusters(&plan, 1, &progress).is_err());
    }
    #[test]
    fn incremental_atomic_limit_is_checked_before_summary_or_embeddings() {
        let mut output = projection(1001);
        output.request.operation = CommunityOperation::Incremental {
            chain_ids: vec![output.clusters[0].members[0].chain_id],
        };
        assert!(freeze_clusters(output, &CommunitySettings::default()).is_err());
    }
}
