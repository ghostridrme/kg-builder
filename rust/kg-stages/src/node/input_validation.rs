//! Generic input checks shared with whole-request admission.

use async_trait::async_trait;
use kg_core::errors::StageError;
use kg_core::models::ValidatedSnapshotInput;
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::Stage;

/// Validate input without provider calls, mutation or source-specific decisions.
pub struct InputValidationStage;

#[async_trait]
impl Stage for InputValidationStage {
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[
            (StageKind::Input, StageKind::ValidatedInput),
            (StageKind::Empty, StageKind::Empty),
        ]
    }

    fn name(&self) -> &str {
        "input_validation"
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        match input {
            StageOutput::Input(input) => {
                let validated =
                    ValidatedSnapshotInput::new(*input, &ctx.org_id).map_err(|error| {
                        StageError::StateValidation {
                            stage: self.name().into(),
                            message: error.to_string(),
                        }
                    })?;
                Ok(StageOutput::ValidatedInput(Box::new(validated)))
            }
            StageOutput::Empty => Ok(StageOutput::Empty),
            _ => Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "expected raw input or empty input".into(),
            }),
        }
    }
}
