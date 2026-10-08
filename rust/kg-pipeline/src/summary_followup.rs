//! Receipted target manifests keep summary partitions stable across resumed runs.
use super::*;
use kg_core::runtime::stage_output::{SummaryBatchOutput, SummaryWork};

impl PipelineRunner {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn summarize(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        scans: &[CollectionScan],
        as_of: chrono::DateTime<chrono::Utc>,
        resumed: &[CommittedBatch],
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        if progress.summary_manifest.is_none() {
            let chain_ids = progress.summary_affected_chains.iter().copied().collect();
            self.summary_batch(
                ctx,
                run_id,
                fingerprint,
                scans,
                as_of,
                0,
                SummaryWork::Manifest {
                    chain_ids,
                    batch_size: ctx.entity_summary_settings.batch_size,
                    collections: progress.collections.clone(),
                },
                progress,
            )
            .await?;
        }
        let manifest = progress
            .summary_manifest
            .clone()
            .ok_or_else(|| recovery_error("summary manifest commit has no target manifest"))?;
        manifest.validate().map_err(recovery_error)?;
        if manifest.as_of != as_of || manifest.batch_size != ctx.entity_summary_settings.batch_size
        {
            return Err(recovery_error(
                "summary manifest disagrees with registered reference time or settings",
            ));
        }
        let batches = manifest.chain_ids.len().div_ceil(manifest.batch_size);
        if resumed
            .iter()
            .any(|receipt| receipt.kind == BatchKind::Summary && receipt.index as usize > batches)
        {
            return Err(recovery_error(
                "summary receipt is outside the frozen manifest",
            ));
        }
        for (offset, chains) in manifest.chain_ids.chunks(manifest.batch_size).enumerate() {
            let index = offset as u32 + 1;
            if progress
                .batches
                .iter()
                .any(|batch| batch.kind == BatchKind::Summary && batch.index == index)
            {
                continue;
            }
            self.summary_batch(
                ctx,
                run_id,
                fingerprint,
                scans,
                manifest.as_of,
                index,
                SummaryWork::Refresh {
                    chain_ids: chains.to_vec(),
                },
                progress,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn summary_batch(
        &self,
        ctx: &Arc<RuntimeContext>,
        run_id: Uuid,
        fingerprint: &RequestFingerprint,
        scans: &[CollectionScan],
        as_of: chrono::DateTime<chrono::Utc>,
        index: u32,
        work: SummaryWork,
        progress: &mut Progress,
    ) -> Result<(), PipelineError> {
        let identity = BatchIdentity {
            run_id,
            kind: BatchKind::Summary,
            index,
        };
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(ctx.entity_summary_settings.timeout_ms);
        let incoming_embeddings =
            Arc::new(kg_core::runtime::embedding_cache::IncomingEmbeddingCache::default());
        for attempt in 0..=ctx.entity_summary_settings.max_replans {
            check_cancel(ctx)?;
            let mut context = ctx.as_ref().clone();
            context.identity_deadline = Some(deadline);
            context.incoming_embeddings = incoming_embeddings.clone();
            context.cancel = ctx.cancel.child_token();
            let context = Arc::new(context);
            let output = StageOutput::SummaryBatch(SummaryBatchOutput {
                batch: identity,
                fingerprint: fingerprint.clone(),
                scans: scans.to_vec(),
                as_of,
                work: work.clone(),
            });
            let result = self.submit_output(&self.summary_stages, &context, identity, fingerprint, output, progress)
                .instrument(tracing::info_span!("pipeline.summary_batch", run_id = %run_id, batch_index = index, attempt))
                .await;
            match result {
                Ok(_) => return Ok(()),
                Err(StageError::CommitRejected { .. } | StageError::IdentityRevisionChanged)
                    if attempt < ctx.entity_summary_settings.max_replans
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
        unreachable!("each final summary attempt returns its result")
    }
}
