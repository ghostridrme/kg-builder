//! Shared cursor operations use the native runtime's admission and lifecycle.
use kg_core::{
    errors::BackendError,
    traits::connector_checkpoint::{CheckpointAdvance, CheckpointScope, ConnectorCheckpointStore},
};
use kg_storage_neo4j::Neo4jGraphBackend;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Get { scope: CheckpointScope },
    Advance { request: CheckpointAdvance },
}
pub async fn run(
    graph: &Neo4jGraphBackend,
    org: &str,
    request: Request,
    cancel: &CancellationToken,
) -> Value {
    if cancel.is_cancelled() {
        return json!({"ok":false,"result":{"cause":"cancelled","retriable":true}});
    }
    // Once submitted, await the bounded adapter transaction, including commit uncertainty.
    let result = match request {
        Request::Get { scope } => graph.checkpoint(org, &scope).await,
        Request::Advance { request } => graph.advance_checkpoint(org, &request).await,
    };
    match result {
        Ok(state) => json!({"ok":true,"result":{"state":state}}),
        Err(error) => {
            json!({"ok":false,"result":{"cause":crate::outcome::backend_kind(&error),"retriable":error.is_transient(),"commit_unknown":matches!(error,BackendError::UnknownCommit(_))}})
        }
    }
}
