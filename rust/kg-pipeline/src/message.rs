use uuid::Uuid;

use kg_core::runtime::StageOutput;

/// One snapshot's accumulated state flowing through the stage channels. The
/// snapshot travels inside `state` (it enters as [`StageOutput::Input`] and
/// each stage transforms it); the envelope carries no second copy, because
/// a snapshot can be megabytes of raw payload.
#[derive(Debug, Clone)]
pub struct PipelineMessage {
    /// Index of this snapshot in the original input batch — failure
    /// attribution and deterministic output ordering key.
    pub snapshot_index: usize,
    /// The run this snapshot belongs to.
    pub run_id: Uuid,
    /// Current accumulated state for this snapshot.
    pub state: StageOutput,
}
