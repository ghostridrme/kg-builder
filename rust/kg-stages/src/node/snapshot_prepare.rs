//! One local observation record shared by subsequent extraction stages.

use async_trait::async_trait;
use kg_core::errors::StageError;
use kg_core::runtime::stage_output::PreparedSnapshotInput;
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::Stage;

/// Allocate a snapshot from validated input without provider or storage calls.
pub struct SnapshotPreparationStage;

#[async_trait]
impl Stage for SnapshotPreparationStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[
            (StageKind::ValidatedInput, StageKind::PreparedSnapshot),
            (StageKind::Empty, StageKind::Empty),
        ]
    }

    fn name(&self) -> &str {
        "snapshot_preparation"
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        match input {
            StageOutput::ValidatedInput(input) => {
                kg_core::runtime::history::validate_input(&ctx.context_settings, input.input())
                    .map_err(|message| StageError::StateValidation {
                        stage: self.name().into(),
                        message: message.into(),
                    })?;
                let mut prepared =
                    PreparedSnapshotInput::new(*input, &ctx.org_id).map_err(|error| {
                        StageError::StateValidation {
                            stage: self.name().into(),
                            message: error.to_string(),
                        }
                    })?;
                if let Some(entry) = ctx.frozen_observation() {
                    prepared.apply_frozen_identity(entry).map_err(|message| {
                        StageError::StateValidation {
                            stage: self.name().into(),
                            message,
                        }
                    })?;
                } else if ctx.observation_manifest.is_some() {
                    return Err(StageError::StateValidation {
                        stage: self.name().into(),
                        message: "missing frozen observation ordinal".into(),
                    });
                }
                if let Some(manifest) = &ctx.run_schemas {
                    prepared
                        .attach_schemas(&ctx.org_id, manifest)
                        .map_err(|message| StageError::StateValidation {
                            stage: self.name().into(),
                            message,
                        })?;
                }
                tracing::debug!(snapshot = %prepared.snapshot().uuid, "snapshot prepared");
                Ok(StageOutput::PreparedSnapshot(Box::new(prepared)))
            }
            StageOutput::Empty => Ok(StageOutput::Empty),
            _ => Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "expected validated input or empty input".into(),
            }),
        }
    }
}
