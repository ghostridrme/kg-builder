//! Revision-fenced, receipted connector progress shared by independent engines.
use crate::{
    driver::{build_query, row_to_json, CallError},
    Neo4jGraphBackend,
};
use async_trait::async_trait;
use kg_core::{
    errors::BackendError,
    traits::connector_checkpoint::{
        CheckpointAdvance, CheckpointScope, CheckpointState, ConnectorCheckpointStore,
    },
};
use kg_storage_cypher::{connector_checkpoint as queries, PreparedQuery};
use serde_json::{Map, Value};

async fn row(
    txn: &mut neo4rs::Txn,
    query: PreparedQuery,
) -> Result<Option<Map<String, Value>>, BackendError> {
    let mut rows = txn
        .execute(build_query(&query.statement, &query.parameters))
        .await
        .map_err(|e| CallError::Driver(e).into_backend("checkpoint statement"))?;
    rows.next(txn)
        .await
        .map_err(|e| CallError::Driver(e).into_backend("checkpoint result"))?
        .map(|r| row_to_json(&r).map_err(|e| e.into_backend("checkpoint decode")))
        .transpose()
}
#[async_trait]
impl ConnectorCheckpointStore for Neo4jGraphBackend {
    async fn checkpoint(
        &self,
        org: &str,
        scope: &CheckpointScope,
    ) -> Result<CheckpointState, BackendError> {
        let query = queries::read(org, scope)?;
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        rows.first()
            .map(queries::state)
            .transpose()
            .map(|s| s.unwrap_or_default())
    }
    async fn advance_checkpoint(
        &self,
        org: &str,
        request: &CheckpointAdvance,
    ) -> Result<CheckpointState, BackendError> {
        request.validate(org)?;
        let duration = self.options.timeout;
        let timeout = || BackendError::Timeout(duration.as_millis() as u64);
        let mut txn = tokio::time::timeout(duration, self.graph.start_txn_with_timeout(duration))
            .await
            .map_err(|_| timeout())?
            .map_err(|e| CallError::Driver(e).into_backend("checkpoint transaction start"))?;
        let write = async {
            let current = queries::state(
                &row(&mut txn, queries::lock(org, &request.scope)?)
                    .await?
                    .ok_or_else(|| BackendError::Conflict("checkpoint lock unavailable".into()))?,
            )?;
            if let Some(receipt) = row(&mut txn, queries::receipt(org, request)?).await? {
                if queries::stored_request(org, &receipt)? != *request {
                    return Err(BackendError::Conflict(
                        "checkpoint run reused with different contents".into(),
                    ));
                }
                return Ok(request.result());
            }
            if current != request.expected {
                return Err(BackendError::Conflict(
                    "checkpoint advanced since collection started".into(),
                ));
            }
            let result = queries::state(
                &row(&mut txn, queries::advance(org, request)?)
                    .await?
                    .ok_or_else(|| BackendError::Conflict("checkpoint revision changed".into()))?,
            )?;
            if result != request.result() {
                return Err(BackendError::Deserialization(
                    "checkpoint result disagrees with proposal".into(),
                ));
            }
            Ok(result)
        };
        let result = tokio::time::timeout(duration, write)
            .await
            .map_err(|_| timeout())
            .and_then(|r| r);
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let _ = tokio::time::timeout(duration, txn.rollback()).await;
                return Err(error);
            }
        };
        match tokio::time::timeout(duration, txn.commit()).await {
            Ok(Ok(())) => Ok(result),
            Ok(Err(e)) if matches!(e, neo4rs::Error::Neo4j(_)) => {
                Err(CallError::Driver(e).into_backend("checkpoint commit rejected"))
            }
            _ => Err(BackendError::UnknownCommit(
                "checkpoint acknowledgment uncertain; retry the same run and proposal".into(),
            )),
        }
    }
}
