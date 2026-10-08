//! Stable receipt pages consume membership intervals, independently of source timestamps.
use super::*;
use kg_core::{
    models::ThreadNode,
    runtime::stage_output::{
        SagaSummaryBatchOutput, SagaSummaryManifest, SagaSummaryTarget, SagaSummaryWork,
    },
    saga::{SagaRead, SagaReadResult, ThreadReference},
    traits::RunHeader,
};

/// Stage name of the validation error returned when an on-demand summary names
/// a Saga that does not exist in the caller's organization and namespace.
pub const SAGA_REQUEST_STAGE: &str = "saga_request";

impl PipelineRunner {
    /// Summarize one Saga's members that no committed summary covers yet, and
    /// await the commit. The uncovered interval is frozen in the first receipt,
    /// so members arriving during the run wait for the next request. Retrying
    /// the same run id replays committed pages and finishes the rest.
    #[tracing::instrument(name="pipeline.saga_maintenance",skip_all,fields(run_id=%run_id,namespace=%namespace,saga=%saga_uuid,otel.status_code=tracing::field::Empty))]
    pub async fn summarize_saga(
        &self,
        namespace: String,
        saga_uuid: Uuid,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        use kg_core::telemetry::{OperationGuard, OperationKind, Outcome};
        let mut operation = OperationGuard::new(OperationKind::Maintenance);
        let result = self
            .summarize_saga_inner(namespace, saga_uuid, ctx, run_id)
            .await;
        let outcome = match &result {
            Ok(output) if !output.is_complete() => Outcome::IncompleteResponse,
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

    async fn summarize_saga_inner(
        &self,
        namespace: String,
        saga_uuid: Uuid,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        if namespace.trim().is_empty()
            || ctx.org_id.trim().is_empty()
            || run_id.is_nil()
            || saga_uuid.is_nil()
        {
            return Err(recovery_error(
                "Saga maintenance requires scope, a Saga identity and a nonnil run id",
            ));
        }
        ctx.saga_summary_settings
            .validate()
            .map_err(|message| PipelineError::StateValidation {
                stage: "saga_summary".into(),
                message,
            })?;
        if !ctx.saga_summary_settings.enabled {
            return Err(PipelineError::StateValidation {
                stage: "saga_summary".into(),
                message: "Saga summaries are disabled for this engine".into(),
            });
        }
        if self.saga_summary_stages.is_empty() {
            return Err(PipelineError::StateValidation {
                stage: "topology".into(),
                message: "Saga maintenance requires the awaited summary chain".into(),
            });
        }
        validate_summary_chain(
            &self.saga_summary_stages,
            kg_core::traits::StageKind::SagaSummaryBatch,
        )?;
        check_cancel(&ctx)?;
        // Existence is checked before any run is registered, so an unknown Saga
        // leaves no header behind and maps to a not-found answer.
        let saga = saga_state(&ctx, &namespace, saga_uuid)
            .await?
            .ok_or_else(|| PipelineError::StateValidation {
                stage: SAGA_REQUEST_STAGE.into(),
                message: "Saga is not visible in this scope".into(),
            })?;
        let started = Instant::now();
        let fingerprint = RequestFingerprint::compute_inputs(
            &ctx.org_id,
            &[],
            &serde_json::json!({"maintenance":"saga_summary","namespace":namespace,"saga":saga_uuid,"settings":self.settings(&ctx)}),
        )
        .map_err(|_| recovery_error("invalid maintenance fingerprint"))?;
        let header = RunHeader {
            org_id: ctx.org_id.to_string(),
            run_id,
            fingerprint: fingerprint.clone(),
            settings_version: SETTINGS_VERSION.into(),
            capture_default: Utc::now(),
            batch_plan: vec![PlannedBatch {
                kind: BatchKind::SagaSummary,
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
        let receipts = match registration {
            RunRegistration::Registered => vec![],
            RunRegistration::Resumed { committed, .. } => committed,
        };
        let mut progress = Progress::default();
        for receipt in &receipts {
            progress
                .record(&restore_commit(run_id, receipt)?)
                .map_err(|error| recovery_error(error.to_string()))?;
        }
        let result = self
            .saga_maintenance_pages(
                &ctx,
                run_id,
                &fingerprint,
                &namespace,
                &saga,
                &receipts,
                &mut progress,
            )
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
            memberships = progress.committed.saga_memberships_summarized,
            duration_ms = started.elapsed().as_millis() as u64,
            "Saga maintenance committed"
        );
        Ok(PipelineOutput {
            profile_diagnostics: progress.profile_diagnostics,
            run_id,
            incomplete_followups: progress.incomplete_followups,
            committed: progress.committed,
            newly_committed: progress.newly_committed,
            batches: progress.batches,
            duration_ms: started.elapsed().as_millis() as u64,
            ..Default::default()
        })
    }

    /// Freeze the uncovered interval in the manifest receipt, then run its pages.
    #[allow(clippy::too_many_arguments)]
    async fn saga_maintenance_pages(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        namespace: &str,
        saga: &ThreadNode,
        resumed: &[CommittedBatch],
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        if progress.saga_summary_manifest.is_none() {
            if saga.summary_cursor > saga.last_membership_ordinal {
                return Err(recovery_error(
                    "Saga summary cursor is ahead of its membership",
                ));
            }
            let targets = if saga.summary_cursor < saga.last_membership_ordinal {
                vec![SagaSummaryTarget {
                    namespace: namespace.to_owned(),
                    saga_uuid: saga.uuid,
                    after_ordinal: saga.summary_cursor,
                    through_ordinal: saga.last_membership_ordinal,
                }]
            } else {
                vec![]
            };
            self.saga_summary_batch(
                ctx,
                run_id,
                fingerprint,
                &[],
                0,
                SagaSummaryWork::Manifest {
                    targets,
                    collections: vec![],
                },
                progress,
            )
            .await?;
        }
        let manifest = progress
            .saga_summary_manifest
            .clone()
            .ok_or_else(|| recovery_error("Saga manifest receipt lacks frozen work"))?;
        // A replayed manifest may only describe the Saga this request named.
        if manifest
            .targets
            .iter()
            .any(|target| target.namespace != namespace || target.saga_uuid != saga.uuid)
        {
            return Err(recovery_error(
                "Saga manifest receipt describes a different Saga",
            ));
        }
        self.run_saga_summary_pages(ctx, run_id, fingerprint, &[], resumed, manifest, progress)
            .await
    }

    pub(super) async fn summarize_sagas(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        scans: &[CollectionScan],
        resumed: &[CommittedBatch],
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        if progress.saga_summary_manifest.is_none() {
            let targets = saga_targets(ctx, &progress.saga_associations).await?;
            self.saga_summary_batch(
                ctx,
                run_id,
                fingerprint,
                scans,
                0,
                SagaSummaryWork::Manifest {
                    targets,
                    collections: progress.collections.clone(),
                },
                progress,
            )
            .await?;
        }
        let manifest = progress
            .saga_summary_manifest
            .clone()
            .ok_or_else(|| recovery_error("Saga manifest receipt lacks frozen work"))?;
        for target in &manifest.targets {
            if progress
                .saga_associations
                .get(&(target.namespace.clone(), target.saga_uuid))
                != Some(&target.through_ordinal)
            {
                return Err(recovery_error(
                    "Saga manifest target is not supported by association receipts",
                ));
            }
        }
        self.run_saga_summary_pages(ctx, run_id, fingerprint, scans, resumed, manifest, progress)
            .await
    }

    /// Commit every page of a frozen manifest that has no receipt yet, in order.
    #[allow(clippy::too_many_arguments)]
    async fn run_saga_summary_pages(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        scans: &[CollectionScan],
        resumed: &[CommittedBatch],
        manifest: SagaSummaryManifest,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        manifest.validate().map_err(recovery_error)?;
        if manifest.page_size != ctx.saga_summary_settings.page_size {
            return Err(recovery_error(
                "Saga manifest disagrees with configured page size",
            ));
        }
        let mut stopped_sagas = std::collections::BTreeSet::new();
        for incomplete in &progress.incomplete_followups {
            if !stopped_sagas.insert((&incomplete.namespace, incomplete.saga_uuid))
                || !manifest.targets.iter().any(|target| {
                    target.namespace == incomplete.namespace
                        && target.saga_uuid == incomplete.saga_uuid
                        && incomplete.from_ordinal > target.after_ordinal
                        && incomplete.from_ordinal <= target.through_ordinal
                })
            {
                return Err(recovery_error(
                    "incomplete Saga receipt is outside its frozen interval",
                ));
            }
        }
        if progress.committed.saga_summaries_incomplete != progress.incomplete_followups.len() {
            return Err(recovery_error(
                "incomplete Saga counts disagree with recovery records",
            ));
        }
        let pages = manifest.page_count().map_err(recovery_error)?;
        if resumed
            .iter()
            .any(|receipt| receipt.kind == BatchKind::SagaSummary && receipt.index > pages)
        {
            return Err(recovery_error(
                "Saga summary receipt is outside the frozen manifest",
            ));
        }
        let mut index = 1u64;
        for target in manifest.targets {
            let mut after = target.after_ordinal;
            while after < target.through_ordinal {
                let through = target
                    .through_ordinal
                    .min(after.saturating_add(manifest.page_size as u64));
                let page_index =
                    u32::try_from(index).map_err(|_| recovery_error("Saga page index overflow"))?;
                let stopped = progress.incomplete_followups.iter().any(|item| {
                    item.saga_uuid == target.saga_uuid && item.namespace == target.namespace
                });
                if !stopped
                    && !progress.batches.iter().any(|batch| {
                        batch.kind == BatchKind::SagaSummary && batch.index == page_index
                    })
                {
                    self.saga_summary_batch(
                        ctx,
                        run_id,
                        fingerprint,
                        scans,
                        page_index,
                        SagaSummaryWork::Page {
                            target: target.clone(),
                            after_ordinal: after,
                            through_ordinal: through,
                        },
                        progress,
                    )
                    .await?;
                }
                after = through;
                index += 1;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn saga_summary_batch(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        scans: &[CollectionScan],
        index: u32,
        work: SagaSummaryWork,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        let identity = BatchIdentity {
            run_id,
            kind: BatchKind::SagaSummary,
            index,
        };
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(ctx.saga_summary_settings.timeout_ms);
        for attempt in 0..=ctx.saga_summary_settings.max_replans {
            check_cancel(ctx)?;
            let mut context = ctx.as_ref().clone();
            context.identity_deadline = Some(deadline);
            context.cancel = ctx.cancel.child_token();
            let context = Arc::new(context);
            let output = StageOutput::SagaSummaryBatch(SagaSummaryBatchOutput {
                batch: identity,
                fingerprint: fingerprint.clone(),
                scans: scans.to_vec(),
                work: work.clone(),
            });
            let result = self.submit_output(&self.saga_summary_stages, &context, identity, fingerprint, output, progress)
                .instrument(tracing::info_span!("pipeline.saga_summary_batch", run_id = %run_id, batch_index = index, attempt)).await;
            match result {
                Ok(_) => return Ok(()),
                Err(StageError::CommitRejected { .. } | StageError::IdentityRevisionChanged)
                    if attempt < ctx.saga_summary_settings.max_replans
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::select! {
                        biased;
                        _ = ctx.cancel.cancelled() => return Err(PipelineError::Cancelled),
                        _ = tokio::time::sleep_until(deadline) => {},
                        _ = tokio::time::sleep(std::time::Duration::from_millis(25 * (u64::from(attempt) + 1))) => {},
                    }
                }
                Err(error) => return Err(commit_failure(identity, error, progress)),
            }
        }
        Err(recovery_error("Saga summary retry budget exhausted"))
    }
}

/// Current Saga state, or `None` when no Saga has that identity in the scope.
async fn saga_state(
    ctx: &RuntimeContext,
    namespace: &str,
    saga_uuid: Uuid,
) -> Result<Option<ThreadNode>, PipelineError> {
    let read = async {
        let _permit = ctx
            .semaphore
            .acquire()
            .await
            .map_err(|_| PipelineError::Cancelled)?;
        let response = ctx
            .graph
            .read_saga(
                &ctx.org_id,
                &SagaRead::State {
                    namespace: namespace.to_owned(),
                    reference: ThreadReference::Uuid { uuid: saga_uuid },
                },
            )
            .await
            .map_err(|error| PipelineError::StepExecution {
                stage: "saga_summary".into(),
                step: "state".into(),
                cause: "Saga state read failed".into(),
                retriable: error.is_transient(),
            })?;
        match response {
            SagaReadResult::State(Some(saga))
                if saga.org_id == ctx.org_id.as_ref()
                    && saga.namespace == namespace
                    && saga.uuid == saga_uuid =>
            {
                Ok(Some(saga))
            }
            SagaReadResult::State(None) => Ok(None),
            _ => Err(recovery_error("Saga state read returned another scope")),
        }
    };
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(PipelineError::Cancelled),
        result = tokio::time::timeout(std::time::Duration::from_millis(ctx.saga_summary_settings.timeout_ms), read) => result.map_err(|_| PipelineError::StepExecution { stage: "saga_summary".into(), step: "state".into(), cause: "Saga state read deadline exceeded".into(), retriable: true })?,
    }
}

async fn saga_targets(
    ctx: &RuntimeContext,
    affected: &BTreeMap<(String, Uuid), u64>,
) -> Result<Vec<SagaSummaryTarget>, PipelineError> {
    let read = async {
        let _permit = ctx
            .semaphore
            .acquire()
            .await
            .map_err(|_| PipelineError::Cancelled)?;
        let mut targets = Vec::new();
        for ((namespace, uuid), through) in affected {
            let response = ctx
                .graph
                .read_saga(
                    &ctx.org_id,
                    &SagaRead::State {
                        namespace: namespace.clone(),
                        reference: ThreadReference::Uuid { uuid: *uuid },
                    },
                )
                .await
                .map_err(|error| PipelineError::StepExecution {
                    stage: "saga_summary".into(),
                    step: "manifest".into(),
                    cause: "Saga state read failed".into(),
                    retriable: error.is_transient(),
                })?;
            let SagaReadResult::State(Some(saga)) = response else {
                return Err(recovery_error("associated Saga state is unavailable"));
            };
            if saga.org_id != ctx.org_id.as_ref()
                || saga.namespace != *namespace
                || saga.uuid != *uuid
                || *through == 0
                || *through > saga.last_membership_ordinal
                || saga.summary_cursor > saga.last_membership_ordinal
            {
                return Err(recovery_error(
                    "Saga state disagrees with committed associations",
                ));
            }
            if saga.summary_cursor < *through {
                targets.push(SagaSummaryTarget {
                    namespace: namespace.clone(),
                    saga_uuid: *uuid,
                    after_ordinal: saga.summary_cursor,
                    through_ordinal: *through,
                });
            }
        }
        Ok(targets)
    };
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(PipelineError::Cancelled),
        result = tokio::time::timeout(std::time::Duration::from_millis(ctx.saga_summary_settings.timeout_ms), read) => result.map_err(|_| PipelineError::StepExecution { stage: "saga_summary".into(), step: "manifest".into(), cause: "Saga manifest read deadline exceeded".into(), retriable: true })?,
    }
}
