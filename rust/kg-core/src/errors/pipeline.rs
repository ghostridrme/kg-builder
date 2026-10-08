use thiserror::Error;
use uuid::Uuid;

use super::stage::StageError;
use crate::pipeline::CommittedCounts;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("pipeline execution was cancelled")]
    Cancelled,

    #[error("identity evidence changed during resolution")]
    IdentityRevisionChanged,

    #[error("step '{step}' in stage '{stage}' failed: {cause}")]
    StepExecution {
        step: String,
        stage: String,
        cause: String,
        /// Whether retrying could plausibly succeed (transient failure).
        retriable: bool,
    },

    #[error("stage '{stage}' failed with {error_count} errors")]
    StageExecution {
        stage: String,
        error_count: usize,
        errors: Vec<StageError>,
    },

    /// Invalid stage input variant, topology, or configuration.
    #[error("state validation failed in stage '{stage}': {message}")]
    StateValidation { stage: String, message: String },

    #[error("retry exhausted for step '{step}' after {attempts} attempts: {last_error}")]
    RetryExhausted {
        step: String,
        attempts: u32,
        last_error: String,
    },

    #[error("task panicked: {0}")]
    TaskPanic(String),

    /// The run stopped after some batches committed. Committed batches stay
    /// committed; retrying the same run id replays them and resumes the rest.
    /// `commit_unknown` means the last commit's outcome could not be read and
    /// the receipt must be consulted before assuming anything.
    #[error(
        "run {run_id} aborted after {batches_committed} committed batch(es){}: {cause}",
        if *.commit_unknown { ", last commit outcome unknown" } else { "" }
    )]
    Aborted {
        run_id: Uuid,
        committed: Box<CommittedCounts>,
        batches_committed: usize,
        commit_unknown: bool,
        #[source]
        cause: Box<PipelineError>,
    },

    #[error("{0}")]
    Other(String),
}

impl PipelineError {
    /// Advisory retry hint; callers own budgets and replay safety.
    /// Cancellation, unknown failures, and batches with any retriable error qualify.
    pub fn is_retriable(&self) -> bool {
        match self {
            PipelineError::Cancelled | PipelineError::IdentityRevisionChanged => true,
            PipelineError::StepExecution { retriable, .. } => *retriable,
            PipelineError::StageExecution { errors, .. } => {
                errors.iter().any(StageError::is_retriable)
            }
            PipelineError::StateValidation { .. } => false,
            PipelineError::RetryExhausted { .. } => false,
            PipelineError::TaskPanic(_) => false,
            PipelineError::Aborted { cause, .. } => cause.is_retriable(),
            PipelineError::Other(_) => true,
        }
    }

    /// The failure that stopped the run, unwrapping abort context.
    pub fn root_cause(&self) -> &PipelineError {
        let mut error = self;
        while let Self::Aborted { cause, .. } = error {
            error = cause;
        }
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn aborted(cause: PipelineError, commit_unknown: bool) -> PipelineError {
        PipelineError::Aborted {
            run_id: Uuid::new_v4(),
            committed: Box::default(),
            batches_committed: 1,
            commit_unknown,
            cause: Box::new(cause),
        }
    }

    #[test]
    fn aborted_run_exposes_nested_causes_without_losing_commit_context() {
        let error = aborted(
            aborted(
                PipelineError::StateValidation {
                    stage: "persist".into(),
                    message: "conflicting input".into(),
                },
                false,
            ),
            true,
        );
        let source = error.source().unwrap();
        assert!(source
            .to_string()
            .contains("aborted after 1 committed batch(es)"));
        let root = source.source().unwrap();
        assert!(root.source().is_none());
        assert_eq!(root.to_string(), error.root_cause().to_string());
        assert!(matches!(
            error.root_cause(),
            PipelineError::StateValidation { .. }
        ));
        assert!(!error.is_retriable());
        assert!(error.to_string().contains("last commit outcome unknown"));
    }

    #[test]
    fn aborted_run_preserves_the_causes_retry_hint() {
        for retriable in [false, true] {
            let error = aborted(
                PipelineError::StepExecution {
                    stage: "persist".into(),
                    step: "commit".into(),
                    cause: "storage failure".into(),
                    retriable,
                },
                true,
            );
            assert_eq!(error.is_retriable(), retriable);
            assert!(matches!(
                error.root_cause(),
                PipelineError::StepExecution { .. }
            ));
        }
    }
}
