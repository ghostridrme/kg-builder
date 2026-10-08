use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ModelFailureKind {
    #[error("timeout")]
    Timeout,
    #[error("rate limited")]
    RateLimited,
    #[error("authentication failed")]
    Authentication,
    #[error("provider unavailable")]
    Unavailable,
    #[error("invalid response")]
    InvalidResponse,
    #[error("incomplete response")]
    IncompleteResponse,
    #[error("response refused")]
    Refused,
    #[error("provider configuration error")]
    Configuration,
    #[error("provider failed")]
    Other,
}
impl ModelFailureKind {
    pub fn is_retriable(self) -> bool {
        matches!(
            self,
            Self::Timeout
                | Self::RateLimited
                | Self::Unavailable
                | Self::IncompleteResponse
                | Self::Other
        )
    }
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum StageError {
    #[error("profile validation failed: {0:?}")]
    ProfileViolation(Box<crate::profiles::ProfileViolation>),
    /// Recompute graph-dependent decisions; extraction remains valid.
    #[error("identity evidence changed before commit")]
    IdentityRevisionChanged,
    #[error("stage '{stage}' model call failed: {kind}")]
    ModelCall {
        stage: String,
        kind: ModelFailureKind,
    },
    #[error("stage '{stage}' step '{step}' failed: {cause}")]
    StepFailed {
        stage: String,
        step: String,
        cause: String,
        /// Whether retrying could plausibly succeed (transient failure).
        retriable: bool,
    },

    #[error("stage '{stage}' cancelled")]
    Cancelled { stage: String },

    #[error("stage '{stage}' state validation failed: {message}")]
    StateValidation { stage: String, message: String },

    /// The storage adapter rejected a commit: a precondition failed, a newer
    /// version exists, or a run or batch identity was reused with different
    /// content. Nothing was written; the run must recompute before retrying.
    #[error("stage '{stage}' commit rejected: {message}")]
    CommitRejected { stage: String, message: String },

    /// The commit acknowledgement was lost and the receipt could not be read.
    /// The batch may or may not be committed; read the receipt before replay.
    #[error("stage '{stage}' commit outcome unknown: {message}")]
    CommitOutcomeUnknown { stage: String, message: String },

    #[error("{0}")]
    Other(String),
}

impl StageError {
    pub fn profile_violation(&self) -> Option<crate::profiles::ProfileViolation> {
        match self {
            Self::ProfileViolation(detail) => Some(detail.as_ref().clone()),
            _ => None,
        }
    }

    /// Advisory retry hint; callers own budgets and replay safety.
    /// Cancellation, rejected commits (after recomputation), unknown commit
    /// outcomes, and unknown failures qualify; invalid input does not.
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::ProfileViolation(_) => false,
            Self::IdentityRevisionChanged => true,
            Self::ModelCall { kind, .. } => kind.is_retriable(),
            StageError::StepFailed { retriable, .. } => *retriable,
            StageError::Cancelled { .. } => true,
            StageError::StateValidation { .. } => false,
            StageError::CommitRejected { .. } => true,
            StageError::CommitOutcomeUnknown { .. } => true,
            StageError::Other(_) => true,
        }
    }
}
