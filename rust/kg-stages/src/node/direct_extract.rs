use async_trait::async_trait;

use kg_core::errors::StageError;

use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::Stage;

/// Preserve declared source entities as drafts without model or storage calls.
pub struct DirectExtractionStage;

#[async_trait]
impl Stage for DirectExtractionStage {
    fn processing_version(&self) -> String {
        "structured-extraction-v1".into()
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::PreparedSnapshot, StageKind::StructuredDrafts)]
    }

    fn name(&self) -> &str {
        "direct_extraction"
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        if ctx.cancel.is_cancelled() {
            return Err(StageError::Cancelled {
                stage: self.name().into(),
            });
        }
        let StageOutput::PreparedSnapshot(prepared) = input else {
            return Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "expected prepared structured snapshot".into(),
            });
        };
        let drafts =
            kg_core::runtime::stage_output::StructuredEntityDrafts::new(*prepared, &ctx.org_id)?;
        tracing::debug!(
            stage = self.name(),
            entities = drafts.drafts().len(),
            deletions = drafts
                .drafts()
                .iter()
                .filter(|draft| draft.is_deleted())
                .count(),
            "declared entity drafts prepared"
        );
        Ok(StageOutput::StructuredDrafts(Box::new(drafts)))
    }
}
