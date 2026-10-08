use async_trait::async_trait;
use kg_core::errors::StageError;
use kg_core::runtime::history::{self, SnapshotEvidenceRequest};
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::Stage;

/// Hydrate selected history without turning it into current evidence.
pub struct ContextRetrievalStage;

#[async_trait]
impl Stage for ContextRetrievalStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[
            (StageKind::PreparedSnapshot, StageKind::PreparedSnapshot),
            (StageKind::Empty, StageKind::Empty),
        ]
    }

    fn name(&self) -> &str {
        "context_retrieval"
    }
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let mut prepared = match input {
            StageOutput::PreparedSnapshot(p) => p,
            StageOutput::Empty => return Ok(StageOutput::Empty),
            _ => return Err(invalid("expected prepared snapshot or empty input")),
        };
        prepared
            .check_org(&ctx.org_id)
            .map_err(|_| invalid("scope_mismatch"))?;
        let input = prepared.input();
        if ctx.policy.for_source(&input.source).extraction
            == kg_core::policy::ExtractionMode::Heuristic
        {
            return Ok(StageOutput::PreparedSnapshot(prepared));
        }
        let start = std::time::Instant::now();
        let (ids, records) = if ctx.observation_manifest.is_some() {
            let index = ctx
                .snapshot_index
                .ok_or_else(|| invalid("missing observation ordinal"))?;
            let frozen = ctx
                .frozen_observation()
                .ok_or_else(|| invalid("missing frozen observation"))?;
            if frozen.snapshot_uuid != prepared.snapshot().uuid
                || frozen.namespace != prepared.snapshot().namespace
                || frozen.captured_at != prepared.snapshot().captured_at
                || frozen.created_at != prepared.snapshot().created_at
                || frozen.history.len() > ctx.context_settings.max_records
            {
                return Err(invalid("frozen observation disagrees with prepared input"));
            }
            let ids = frozen
                .history
                .iter()
                .map(|item| item.reference.uuid)
                .collect();
            let records = history::load_frozen(ctx, index).await?;
            (ids, records)
        } else {
            let ids = input.previous_snapshot_uuids.clone();
            if ids.is_empty() {
                return Ok(StageOutput::PreparedSnapshot(prepared));
            }
            let records = history::load(
                ctx,
                &SnapshotEvidenceRequest {
                    namespace: input.namespace.clone(),
                    ids: ids.clone(),
                    captured_before: prepared.snapshot().captured_at,
                    max_bytes: ctx.context_settings.max_history_bytes,
                },
            )
            .await?;
            (ids, records)
        };
        let count = records.len();
        let bytes: usize = records.iter().map(|record| record.byte_len()).sum();
        prepared
            .attach_selected_history(
                &ctx.org_id,
                ids,
                records,
                ctx.context_settings.max_history_bytes,
            )
            .map_err(|_| invalid("required snapshot evidence unavailable or exceeds limits"))?;
        tracing::info!(
            records = count,
            bytes,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "snapshot context loaded"
        );
        Ok(StageOutput::PreparedSnapshot(prepared))
    }
}
fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: "context_retrieval".into(),
        message: message.into(),
    }
}
