use super::DirectExtractionStage;
use kg_core::traits::Stage;

/// Routes prepared observations through the supported structured or text branch.
pub struct EntityExtractionStage;

#[async_trait::async_trait]
impl Stage for EntityExtractionStage {
    fn processing_version(&self) -> String {
        kg_core::traits::stage::processing_version(
            "entity-extraction-declared-builder-v1",
            &[
                &DirectExtractionStage.processing_version(),
                &super::structured_prepare::processing_version(),
            ],
        )
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[
            (StageKind::PreparedSnapshot, StageKind::NodeExtraction),
            (StageKind::Empty, StageKind::NodeExtraction),
        ]
    }

    fn name(&self) -> &str {
        "entity_extraction"
    }

    async fn process(
        &self,
        input: kg_core::runtime::StageOutput,
        ctx: &kg_core::runtime::RuntimeContext,
    ) -> Result<kg_core::runtime::StageOutput, kg_core::errors::StageError> {
        use kg_core::errors::StageError;
        use kg_core::runtime::{stage_output::NodeExtractionOutput, StageOutput};
        use tracing::Instrument;
        if ctx.cancel.is_cancelled() {
            return Err(StageError::Cancelled {
                stage: self.name().into(),
            });
        }
        match input {
            StageOutput::PreparedSnapshot(prepared) => {
                prepared
                    .check_org(&ctx.org_id)
                    .map_err(|error| StageError::StateValidation {
                        stage: self.name().into(),
                        message: error.to_string(),
                    })?;
                // Retain collection validation until the separate reconciliation review.
                prepared.input().validate_scope().map_err(|message| {
                    StageError::StateValidation {
                        stage: self.name().into(),
                        message,
                    }
                })?;
                if prepared.input().entities.is_empty() && prepared.input().content.is_some() {
                    Err(StageError::StateValidation {
                        stage: self.name().into(),
                        message:
                            "kg-builder requires supplied entities; text extraction is unavailable"
                                .into(),
                    })
                } else {
                    let result = DirectExtractionStage
                        .process(StageOutput::PreparedSnapshot(prepared), ctx)
                        .instrument(tracing::debug_span!("direct_extraction"))
                        .await?;
                    let StageOutput::StructuredDrafts(drafts) = result else {
                        return Err(StageError::StateValidation {
                            stage: self.name().into(),
                            message: "direct extraction did not return declared drafts".into(),
                        });
                    };
                    super::structured_prepare::prepare_structured(*drafts, ctx).await
                }
            }
            StageOutput::Empty => Ok(StageOutput::NodeExtraction(NodeExtractionOutput {
                raw_text_drafts: Default::default(),
                relationship_changes: Default::default(),
                version_exclusions: Default::default(),
                text_observation_ids: Default::default(),
                fk_exclusions: Default::default(),
                schemas: Default::default(),
                history: Default::default(),
                snapshot_nodes: Default::default(),
                entities_by_snapshot: Default::default(),
                source_deleted: Default::default(),
                sub_edges: Default::default(),
                incomplete_extractions: Default::default(),
            })),
            _ => Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "expected prepared snapshot or empty input".into(),
            }),
        }
    }
}
