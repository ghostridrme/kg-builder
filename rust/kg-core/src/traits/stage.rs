//! Awaited processing steps composed by the pipeline runner.

use async_trait::async_trait;

use crate::errors::StageError;
use crate::runtime::context::RuntimeContext;
use crate::runtime::stage_output::StageOutput;

/// The payload handed between stages; this carries no observation data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum StageKind {
    CommunityRequest,
    CommunityClusters,
    CommunityDrafts,
    CommunityPrepared,
    SagaSummaryBatch,
    ReusedSnapshot,
    Empty,
    Input,
    ValidatedInput,
    PreparedSnapshot,
    StructuredDrafts,
    TextDrafts,
    NodeExtraction,
    NodeIdentity,
    NodeResolution,
    EdgeExtraction,
    EdgeResolution,
    FlushBatch,
    SummaryBatch,
    PlannedBatch,
    PreparedBatch,
    Committed,
}

/// Relationship work required before an observation can authorize absence-based deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum StageCapability {
    // Persisted processing identity predates the public Thread terminology.
    #[serde(rename = "SagaAssociation")]
    ThreadAssociation,
    DeclaredRelationships,
    ReferenceExtraction,
    ReferenceResolution,
    TextRelationships,
}

/// Every successful input/output combination a stage can produce.
pub type StageContract = &'static [(StageKind, StageKind)];

impl StageKind {
    /// Returns the payload kind without inspecting its contents.
    pub fn of(output: &StageOutput) -> Self {
        match output {
            StageOutput::CommunityRequest(_) => Self::CommunityRequest,
            StageOutput::CommunityClusters(_) => Self::CommunityClusters,
            StageOutput::CommunityDrafts(_) => Self::CommunityDrafts,
            StageOutput::CommunityPrepared(_) => Self::CommunityPrepared,
            StageOutput::SagaSummaryBatch(_) => Self::SagaSummaryBatch,
            StageOutput::ReusedSnapshot(_) => Self::ReusedSnapshot,
            StageOutput::Empty => Self::Empty,
            StageOutput::Input(_) => Self::Input,
            StageOutput::ValidatedInput(_) => Self::ValidatedInput,
            StageOutput::PreparedSnapshot(_) => Self::PreparedSnapshot,
            StageOutput::StructuredDrafts(_) => Self::StructuredDrafts,
            StageOutput::TextDrafts(_) => Self::TextDrafts,
            StageOutput::NodeExtraction(_) => Self::NodeExtraction,
            StageOutput::NodeIdentity(_) => Self::NodeIdentity,
            StageOutput::NodeResolution(_) => Self::NodeResolution,
            StageOutput::EdgeExtraction(_) => Self::EdgeExtraction,
            StageOutput::EdgeResolution(_) => Self::EdgeResolution,
            StageOutput::SummaryBatch(_) => Self::SummaryBatch,
            StageOutput::FlushBatch(_) => Self::FlushBatch,
            StageOutput::PlannedBatch(_) => Self::PlannedBatch,
            StageOutput::PreparedBatch(_) => Self::PreparedBatch,
            StageOutput::Committed(_) => Self::Committed,
        }
    }

    /// Explicit contract for observation-only stages that preserve every payload.
    pub const IDENTITY: StageContract = &[
        (Self::CommunityRequest, Self::CommunityRequest),
        (Self::CommunityClusters, Self::CommunityClusters),
        (Self::CommunityDrafts, Self::CommunityDrafts),
        (Self::CommunityPrepared, Self::CommunityPrepared),
        (Self::SagaSummaryBatch, Self::SagaSummaryBatch),
        (Self::ReusedSnapshot, Self::ReusedSnapshot),
        (Self::Empty, Self::Empty),
        (Self::Input, Self::Input),
        (Self::ValidatedInput, Self::ValidatedInput),
        (Self::PreparedSnapshot, Self::PreparedSnapshot),
        (Self::StructuredDrafts, Self::StructuredDrafts),
        (Self::TextDrafts, Self::TextDrafts),
        (Self::NodeExtraction, Self::NodeExtraction),
        (Self::NodeIdentity, Self::NodeIdentity),
        (Self::NodeResolution, Self::NodeResolution),
        (Self::EdgeExtraction, Self::EdgeExtraction),
        (Self::EdgeResolution, Self::EdgeResolution),
        (Self::SummaryBatch, Self::SummaryBatch),
        (Self::FlushBatch, Self::FlushBatch),
        (Self::PlannedBatch, Self::PlannedBatch),
        (Self::PreparedBatch, Self::PreparedBatch),
        (Self::Committed, Self::Committed),
    ];
}

/// Transforms supported [`StageOutput`] variants; invalid input returns [`StageError`].
/// Concurrent calls must preserve runtime organization scope. Errors do not roll back writes.
#[async_trait]
pub trait Stage: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// Durable processing identity, independent of credentials and instrumentation.
    /// Override when behavior, prompts or output formats change; custom stages must
    /// include any effective constructor settings not carried by RuntimeContext.
    fn processing_version(&self) -> String {
        "1".into()
    }

    /// Declares all successful handoffs, including any intentional empty output.
    fn contract(&self) -> StageContract;

    /// Semantic work performed by this stage. Unclassified custom stages grant no coverage.
    fn capabilities(&self) -> &'static [StageCapability] {
        &[]
    }

    /// A batch stage observes all surviving snapshots in the current chunk together.
    fn is_batch(&self) -> bool {
        false
    }

    /// Return one result per input, in order. A per-input error declines that
    /// snapshot; a batch error invalidates every result. Cancellation and stale
    /// identity revisions always abort or replan the whole batch.
    async fn process_batch(
        &self,
        inputs: Vec<StageOutput>,
        ctx: &RuntimeContext,
    ) -> Result<Vec<Result<StageOutput, StageError>>, StageError> {
        let mut outputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            if ctx.cancel.is_cancelled() {
                return Err(StageError::Cancelled {
                    stage: self.name().into(),
                });
            }
            outputs.push(self.process(input, ctx).await);
        }
        Ok(outputs)
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError>;
}

/// Fingerprint explicit processing revisions and the actual prompt/text policies.
/// Length framing prevents different component boundaries sharing an identity.
pub fn processing_version(revision: &str, components: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for part in [revision, crate::sanitize::SOURCE_DATA_FORMAT]
        .into_iter()
        .chain(components.iter().copied())
    {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    format!("{revision}:{:x}", hash.finalize())
}

#[cfg(test)]
mod processing_tests {
    use super::processing_version;
    #[test]
    fn revisions_prompts_and_component_boundaries_are_distinct() {
        let original = processing_version("v1", &["a", "bc"]);
        assert_ne!(original, processing_version("v2", &["a", "bc"]));
        assert_ne!(original, processing_version("v1", &["ab", "c"]));
        assert_ne!(original, processing_version("v1", &["a", "changed prompt"]));
        assert_eq!(original, processing_version("v1", &["a", "bc"]));
    }
}
