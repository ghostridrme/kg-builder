//! Durable provider cursors. A revision fences stale writers even when cursors repeat.
use crate::errors::BackendError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const MAX_CURSOR_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointScope {
    pub namespace: String,
    /// SDK digest of organization, namespace, connector, source and selection.
    pub key: String,
}
impl CheckpointScope {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        if org.trim().is_empty()
            || self.namespace.trim().is_empty()
            || self.namespace.len() > 1024
            || self.key.len() != 64
            || !self
                .key
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(BackendError::Query(
                "invalid connector checkpoint scope".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointState {
    pub revision: u64,
    pub cursor: Value,
}
impl CheckpointState {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.revision > i64::MAX as u64 || (self.revision == 0 && !self.cursor.is_null()) {
            return Err(BackendError::Query("invalid checkpoint revision".into()));
        }
        validate_cursor(&self.cursor)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointAdvance {
    pub scope: CheckpointScope,
    pub expected: CheckpointState,
    pub proposed: Value,
    pub run_id: Uuid,
}
impl CheckpointAdvance {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        self.scope.validate(org)?;
        self.expected.validate()?;
        validate_cursor(&self.proposed)?;
        if self.run_id.is_nil() || self.expected.revision >= i64::MAX as u64 {
            return Err(BackendError::Query("invalid checkpoint advancement".into()));
        }
        Ok(())
    }
    pub fn result(&self) -> CheckpointState {
        CheckpointState {
            revision: self.expected.revision + 1,
            cursor: self.proposed.clone(),
        }
    }
}
fn validate_cursor(value: &Value) -> Result<(), BackendError> {
    if serde_json::to_vec(value)
        .map_err(|e| BackendError::Serialization(e.to_string()))?
        .len()
        > MAX_CURSOR_BYTES
    {
        return Err(BackendError::Query(
            "connector cursor exceeds 64 KiB".into(),
        ));
    }
    Ok(())
}
#[async_trait]
pub trait ConnectorCheckpointStore: Send + Sync {
    async fn checkpoint(
        &self,
        org: &str,
        scope: &CheckpointScope,
    ) -> Result<CheckpointState, BackendError>;
    /// Exact retries return their original result even after a later run advanced.
    /// Different contents under a reused run id, or a stale revision, conflict.
    async fn advance_checkpoint(
        &self,
        org: &str,
        request: &CheckpointAdvance,
    ) -> Result<CheckpointState, BackendError>;
}
