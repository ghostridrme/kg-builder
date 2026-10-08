//! Pipeline run results and failures.

pub mod output;

pub use output::{
    BatchOutcome, CollectionOutcome, CommittedCounts, CountOverflow, IncompleteSnapshot,
    PipelineOutput, SnapshotFailure,
};
