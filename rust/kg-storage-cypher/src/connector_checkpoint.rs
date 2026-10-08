//! Checkpoint writes run under a scoped record lock in one adapter transaction.
use crate::PreparedQuery;
use kg_core::{
    errors::BackendError,
    traits::connector_checkpoint::{
        CheckpointAdvance, CheckpointScope, CheckpointState, MAX_CURSOR_BYTES,
    },
};
use serde_json::{json, Map, Value};
fn params(org: &str, scope: &CheckpointScope) -> Result<Value, BackendError> {
    scope.validate(org)?;
    Ok(json!({"org":org,"namespace":scope.namespace,"key":scope.key}))
}
pub fn read(org: &str, scope: &CheckpointScope) -> Result<PreparedQuery, BackendError> {
    Ok(PreparedQuery { statement: "MATCH (c:ConnectorCheckpoint {org_id:$org,namespace:$namespace,key:$key}) RETURN c.revision AS revision,c.cursor AS cursor".into(), parameters: params(org,scope)? })
}
pub fn lock(org: &str, scope: &CheckpointScope) -> Result<PreparedQuery, BackendError> {
    // The dependent SET obtains the write lock before the revision is returned.
    Ok(PreparedQuery { statement: "MERGE (c:ConnectorCheckpoint {org_id:$org,namespace:$namespace,key:$key}) ON CREATE SET c.revision=0,c.cursor='null' SET c.lock_count=coalesce(c.lock_count,0)+1 RETURN c.revision AS revision,c.cursor AS cursor".into(), parameters: params(org,scope)? })
}
pub fn receipt(org: &str, request: &CheckpointAdvance) -> Result<PreparedQuery, BackendError> {
    request.validate(org)?;
    let mut parameters = params(org, &request.scope)?;
    parameters["run"] = json!(request.run_id);
    Ok(PreparedQuery { statement: "MATCH (r:ConnectorCheckpointReceipt {org_id:$org,namespace:$namespace,key:$key,run_id:$run}) RETURN r.request AS request".into(),parameters })
}
pub fn advance(org: &str, request: &CheckpointAdvance) -> Result<PreparedQuery, BackendError> {
    request.validate(org)?;
    let mut parameters = params(org, &request.scope)?;
    parameters["run"] = json!(request.run_id);
    parameters["expected"] = json!(request.expected.revision);
    parameters["cursor"] = json!(serde_json::to_string(&request.proposed)
        .map_err(|e| BackendError::Serialization(e.to_string()))?);
    parameters["request"] =
        json!(serde_json::to_string(request)
            .map_err(|e| BackendError::Serialization(e.to_string()))?);
    Ok(PreparedQuery { statement: "MATCH (c:ConnectorCheckpoint {org_id:$org,namespace:$namespace,key:$key}) WHERE c.revision=$expected SET c.revision=c.revision+1,c.cursor=$cursor CREATE (r:ConnectorCheckpointReceipt {org_id:$org,namespace:$namespace,key:$key,run_id:$run,request:$request}) RETURN c.revision AS revision,c.cursor AS cursor".into(),parameters })
}
pub fn state(row: &Map<String, Value>) -> Result<CheckpointState, BackendError> {
    let invalid = || BackendError::Deserialization("invalid connector checkpoint".into());
    let revision = row
        .get("revision")
        .and_then(Value::as_u64)
        .ok_or_else(invalid)?;
    let raw = row
        .get("cursor")
        .and_then(Value::as_str)
        .filter(|v| v.len() <= MAX_CURSOR_BYTES)
        .ok_or_else(invalid)?;
    let state = CheckpointState {
        revision,
        cursor: serde_json::from_str(raw).map_err(|_| invalid())?,
    };
    state.validate()?;
    Ok(state)
}
pub fn stored_request(
    org: &str,
    row: &Map<String, Value>,
) -> Result<CheckpointAdvance, BackendError> {
    let invalid = || BackendError::Deserialization("invalid checkpoint receipt".into());
    let raw = row
        .get("request")
        .and_then(Value::as_str)
        .filter(|v| v.len() <= MAX_CURSOR_BYTES * 2 + 8192)
        .ok_or_else(invalid)?;
    let request: CheckpointAdvance = serde_json::from_str(raw).map_err(|_| invalid())?;
    request.validate(org)?;
    Ok(request)
}
