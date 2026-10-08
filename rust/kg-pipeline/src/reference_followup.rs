//! Receipted re-evaluation of one stored reference owner after rule changes.

use super::*;
use kg_core::traits::{EntityVersionRecord, RunHeader};

impl PipelineRunner {
    /// Re-run one stored source through reference extraction, relationship
    /// resolution and the normal guarded commit chain. The caller derives a
    /// stable run id from the rule revision and source version, so retries replay.
    #[tracing::instrument(
        name = "pipeline.reference_rule_repair",
        skip_all,
        fields(run_id=%run_id,source_chain=%record.chain_id,slot=%owner_slot,otel.status_code=tracing::field::Empty)
    )]
    pub async fn repair_reference_owner(
        &self,
        record: EntityVersionRecord,
        owner_slot: String,
        effective_at: DateTime<Utc>,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        use kg_core::telemetry::{OperationGuard, OperationKind, Outcome};
        let mut operation = OperationGuard::new(OperationKind::Maintenance);
        let result = self
            .repair_reference_owner_inner(record, owner_slot, effective_at, ctx, run_id)
            .await;
        let outcome = match &result {
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

    async fn repair_reference_owner_inner(
        &self,
        record: EntityVersionRecord,
        owner_slot: String,
        effective_at: DateTime<Utc>,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
    ) -> Result<PipelineOutput, PipelineError> {
        if run_id.is_nil()
            || record.chain_id.is_nil()
            || owner_slot.trim().is_empty()
            || record.source.as_deref().is_none_or(str::is_empty)
        {
            return Err(recovery_error(
                "reference-rule repair requires a stored source, owner slot and nonnil run id",
            ));
        }
        check_cancel(&ctx)?;
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
        if reference_stages.len() != 2 {
            return Err(recovery_error(
                "reference-rule repair requires extraction and resolution stages",
            ));
        }
        let started = Instant::now();
        let fingerprint = RequestFingerprint::compute_inputs(
            &ctx.org_id,
            &[],
            &serde_json::json!({
                "maintenance": "reference_rule_repair",
                "source_chain": record.chain_id,
                "source_version": record.version,
                "owner_slot": owner_slot,
                "effective_at": effective_at,
                "settings": self.settings(&ctx),
            }),
        )
        .map_err(|_| recovery_error("invalid reference repair fingerprint"))?;
        let header = RunHeader {
            org_id: ctx.org_id.to_string(),
            run_id,
            fingerprint: fingerprint.clone(),
            settings_version: SETTINGS_VERSION.into(),
            capture_default: effective_at,
            batch_plan: vec![PlannedBatch {
                kind: BatchKind::Relationship,
                index: 0,
                items: 1,
            }],
            observation_manifest: Default::default(),
            rule_freezes: ctx.rule_freezes.as_ref().clone(),
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

        let snapshot_id = record
            .stored
            .get("last_seen_snapshot_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok());
        let observed_at = record
            .last_seen_at
            .unwrap_or(effective_at)
            .max(effective_at);
        let selector = ReferenceOwnerSelector {
            chain_id: record.chain_id,
            namespace: record.namespace.clone(),
            slot: owner_slot,
        };
        let resolution =
            stored_reference_resolution(&ctx, &record, (snapshot_id, observed_at), vec![selector])?;
        let message = PipelineMessage {
            snapshot_index: 0,
            run_id,
            state: StageOutput::EdgeExtraction(
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
        };
        let result = self
            .relationship_chunk(
                vec![message],
                0,
                &ctx,
                run_id,
                &fingerprint,
                &self.flush_stages,
                &[],
                &mut progress,
                &reference_stages,
                &[record.chain_id],
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
}

/// One bounded call of an organization-scoped source-reference rebuild. The
/// cursor lives in commit receipts, so callers resume with the same run id.
#[derive(Debug, Clone)]
pub struct ReferenceRebuildProgress {
    /// Maintenance identity to use again after interruption or a bounded call.
    pub run_id: Uuid,
    /// Last committed source chain, not the last source merely read.
    pub last_chain: Option<Uuid>,
    /// Sources committed or replayed across all calls of this run.
    pub sources_processed: usize,
    /// True only after an empty final source page has a committed receipt.
    pub complete: bool,
    /// Durable relationship and unresolved-reference changes of the whole run.
    pub committed: CommittedCounts,
}

impl PipelineRunner {
    /// Rebuild reference coverage from stored live sources using the normal
    /// matcher and guarded relationship commits. This maintenance entry point
    /// refuses model-enabled ambiguity policies, does not mutate entity history,
    /// and never resets data. Call repeatedly with the same run/time/settings
    /// until `complete`; current writers cover sources arriving behind its cursor.
    #[tracing::instrument(name="pipeline.reference_rebuild", skip_all, fields(%run_id))]
    pub async fn rebuild_references(
        &self,
        ctx: Arc<RuntimeContext>,
        run_id: Uuid,
        effective_at: DateTime<Utc>,
        max_sources: usize,
    ) -> Result<ReferenceRebuildProgress, PipelineError> {
        use kg_core::traits::RunHeader;
        if run_id.is_nil() || !(1..=1000).contains(&max_sources) {
            return Err(recovery_error(
                "reference rebuild requires a run id and source budget 1..=1000",
            ));
        }
        let stages: Vec<_> = self
            .edge_stages
            .iter()
            .filter(|stage| {
                stage.capabilities().iter().any(|cap| {
                    matches!(
                        cap,
                        kg_core::traits::StageCapability::ReferenceExtraction
                            | kg_core::traits::StageCapability::ReferenceResolution
                    )
                })
            })
            .cloned()
            .collect();
        if stages.len() != 2 {
            return Err(recovery_error(
                "reference rebuild requires reference extraction and resolution stages",
            ));
        }
        let fingerprint = RequestFingerprint::compute_inputs(&ctx.org_id, &[], &serde_json::json!({
            "maintenance":"reference_rebuild_v1", "effective_at":effective_at, "settings":self.settings(&ctx),
        })).map_err(|_| recovery_error("invalid reference rebuild fingerprint"))?;
        let header = RunHeader {
            org_id: ctx.org_id.to_string(),
            run_id,
            fingerprint: fingerprint.clone(),
            settings_version: SETTINGS_VERSION.into(),
            capture_default: effective_at,
            batch_plan: vec![],
            observation_manifest: Default::default(),
            rule_freezes: ctx.rule_freezes.as_ref().clone(),
            schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
                profiles: Default::default(),
                org_id: ctx.org_id.to_string(),
                sources: Default::default(),
            },
        };
        let registration = register_run(&ctx, ctx.graph.register_run(&header)).await?;
        let mut receipts = match registration {
            RunRegistration::Registered => vec![],
            RunRegistration::Resumed { committed, .. } => committed,
        };
        receipts.sort_by_key(|receipt| receipt.index);
        let mut progress = Progress::default();
        let mut output = ReferenceRebuildProgress {
            run_id,
            last_chain: None,
            sources_processed: 0,
            complete: false,
            committed: Default::default(),
        };
        let result = async {
        let mut index = 0u32;
        for receipt in receipts {
            if receipt.kind != BatchKind::Relationship || receipt.index != index {
                return Err(recovery_error(
                    "reference rebuild receipts are not contiguous",
                ));
            }
            let commit = restore_commit(run_id, &receipt)?;
            let recovery = commit
                .recovery
                .as_ref()
                .ok_or_else(|| recovery_error("reference rebuild receipt has no recovery data"))?;
            if !recovery.failures.is_empty() || !recovery.incomplete_reference_sources.is_empty() {
                return Err(recovery_error("reference rebuild receipt contains incomplete work; correct the cause and use a new run"));
            }
            if recovery.reference_rebuild_complete {
                output.complete = true;
            } else {
                let [chain] = recovery.reference_repair_sources.as_slice() else {
                    return Err(recovery_error(
                        "reference rebuild receipt has no unique source",
                    ));
                };
                if output.last_chain.is_some_and(|last| *chain <= last) {
                    return Err(recovery_error("reference rebuild cursor did not advance"));
                }
                output.last_chain = Some(*chain);
                output.sources_processed += 1;
            }
            progress
                .record(&commit)
                .map_err(|e| recovery_error(e.to_string()))?;
            index = index
                .checked_add(1)
                .ok_or_else(|| recovery_error("reference rebuild receipt overflow"))?;
        }
        output.committed = progress.committed;
        if output.complete {
            return Ok(output);
        }
        let mut processed = 0;
        while processed < max_sources {
            check_cancel(&ctx)?;
            let limit = (max_sources - processed).min(self.config.chunk_size.max(1));
            let lookup = EntityLookup::LiveReferenceSources {
                after_chain: output.last_chain,
                limit,
            };
            let page = reference_read(
                &ctx,
                "rebuild_source_page",
                ctx.graph.find_entities(&ctx.org_id, &lookup),
            )
            .await?;
            if page.is_empty() {
                self.commit_raw(
                    &self.flush_stages,
                    &ctx,
                    BatchIdentity {
                        run_id,
                        kind: BatchKind::Relationship,
                        index,
                    },
                    &fingerprint,
                    &[],
                    FlushWork::Relationships(RelationshipBatch::default()),
                    Some(BatchRecovery {
                        reference_rebuild_complete: true,
                        ..Default::default()
                    }),
                    &mut progress,
                )
                .await
                .map_err(|e| PipelineError::StageExecution {
                    stage: "reference_rebuild".into(),
                    error_count: 1,
                    errors: vec![e],
                })?;
                output.complete = true;
                break;
            }
            for record in page {
                check_cancel(&ctx)?;
                if output
                    .last_chain
                    .is_some_and(|last| record.chain_id <= last)
                {
                    return Err(recovery_error("reference rebuild cursor did not advance"));
                }
                let source = record.source.as_deref().ok_or_else(|| {
                    recovery_error("reference rebuild source lacks producer metadata")
                })?;
                if ctx.policy.for_source(source).edge_ambiguity
                    != kg_core::policy::EdgeAmbiguityMode::Skip
                {
                    return Err(recovery_error("reference rebuild requires deterministic ambiguity policy for every source"));
                }
                let snapshot = record
                    .stored
                    .get("last_seen_snapshot_id")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok());
                let resolution = Arc::new(stored_reference_resolution(
                    &ctx,
                    &record,
                    (
                        snapshot,
                        record
                            .last_seen_at
                            .unwrap_or(effective_at)
                            .max(effective_at),
                    ),
                    vec![],
                )?);
                let message = PipelineMessage {
                    snapshot_index: 0,
                    run_id,
                    state: StageOutput::EdgeExtraction(
                        kg_core::runtime::stage_output::EdgeExtractionOutput {
                            snapshot_nodes: resolution.snapshot_nodes.clone(),
                            resolved_nodes: resolution.live_entities(),
                            resolution,
                            relationship_times: Default::default(),
                            relationship_directives: Default::default(),
                            reference_report: Default::default(),
                            edges: Default::default(),
                            pending_references: Default::default(),
                        },
                    ),
                };
                self.relationship_chunk(
                    vec![message],
                    index as usize,
                    &ctx,
                    run_id,
                    &fingerprint,
                    &self.flush_stages,
                    &[],
                    &mut progress,
                    &stages,
                    &[record.chain_id],
                )
                .await?;
                if !progress.failed.is_empty() || !progress.incomplete_reference_sources.is_empty()
                {
                    return Err(recovery_error("reference rebuild source failed; receipt retains the failure; use a new run after correcting it"));
                }
                output.last_chain = Some(record.chain_id);
                output.sources_processed += 1;
                processed += 1;
                index = index
                    .checked_add(1)
                    .ok_or_else(|| recovery_error("reference rebuild receipt overflow"))?;
            }
        }
        output.committed = progress.committed;
        Ok(output)
        }.await;
        result.map_err(|cause| PipelineError::Aborted {
            run_id,
            committed: Box::new(progress.committed),
            batches_committed: progress.batches.len(),
            commit_unknown: progress.commit_unknown,
            cause: Box::new(cause),
        })
    }
}
