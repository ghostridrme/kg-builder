use thiserror::Error;

/// Backend failures. Messages are displayed verbatim; omit secrets and payloads.
#[derive(Debug, Error)]
pub enum BackendError {
    /// Recompute graph-dependent identity decisions; replaying the same plan cannot help.
    #[error("identity evidence changed before commit")]
    IdentityRevisionChanged,

    #[error("connection failed: {0}")]
    Connection(String),

    #[error("query execution failed: {0}")]
    Query(String),

    /// No partial relationship history is returned. Optional derived work may skip;
    /// correctness-critical planning must fail rather than use truncated evidence.
    #[error("relationship timeline exceeds the version budget ({limit})")]
    RelationshipHistoryLimit { limit: usize },

    /// Transaction failure; commit status may be unknown.
    #[error("transaction failed: {0}")]
    Transaction(String),

    /// Deadline exceeded; milliseconds are zero when the provider reports no budget.
    #[error("timeout after {0}ms")]
    Timeout(u64),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("rate limited: retry after {retry_after_ms}ms")]
    RateLimited {
        /// Backend-suggested delay before retrying, in milliseconds.
        retry_after_ms: u64,
    },

    #[error("model output was truncated")]
    IncompleteResponse,

    #[error("model refused the request")]
    Refused,

    /// Undecodable response, including malformed LLM output.
    #[error("deserialization failed: {0}")]
    Deserialization(String),

    #[error("serialization failed: {0}")]
    Serialization(String),

    #[error("resource not found: {0}")]
    NotFound(String),

    /// The backend is unavailable or does not support the requested operation.
    #[error("backend unavailable: {0}")]
    Unavailable(String),

    /// The run has no backend of this kind configured; no retry can help.
    #[error("not configured: {0}")]
    NotConfigured(String),

    /// The caller's request-scoped provider attempt allowance is spent. No
    /// further HTTP dispatch (retry, fallback or correction) may happen for it.
    #[error("provider attempt budget exhausted")]
    AttemptBudgetExhausted,

    /// A transactional precondition failed, a newer version exists, or a run or
    /// batch identity was reused with different content. Nothing was written;
    /// retrying the same batch cannot succeed.
    #[error("commit rejected: {0}")]
    Conflict(String),

    /// A collection generation is already owned by a newer generation or another
    /// run. Nothing was written; recomputing graph decisions cannot acquire it.
    #[error("collection ownership rejected: {0}")]
    CollectionOwnershipConflict(String),

    /// The commit acknowledgement was lost and reading the receipt could not
    /// establish whether the batch committed. Read the receipt before any replay.
    #[error("commit outcome unknown: {0}")]
    UnknownCommit(String),

    #[error("{0}")]
    Other(String),
}

impl BackendError {
    /// Advisory recovery hint, not permission to replay a write. Transaction,
    /// unknown-commit and unclassified failures qualify alongside transport
    /// failures; callers must establish commit status before retrying.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::IncompleteResponse
                | Self::Connection(_)
                | Self::Transaction(_)
                | Self::Timeout(_)
                | Self::RateLimited { .. }
                | Self::Unavailable(_)
                | Self::UnknownCommit(_)
                | Self::Other(_)
        )
    }
}
