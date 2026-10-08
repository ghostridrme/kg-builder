//! Scoped source evidence and its replay identity.

use crate::{errors::BackendError, models::SnapshotDataType};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const MAX_CONTEXT_RECORDS: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSettings {
    pub store_content: bool,
    pub max_stored_bytes: usize,
    pub max_records: usize,
    pub max_history_bytes: usize,
    pub read_timeout_ms: u64,
}
impl Default for ContextSettings {
    fn default() -> Self {
        Self {
            store_content: true,
            max_stored_bytes: 1024 * 1024,
            max_records: 10,
            max_history_bytes: 64 * 1024,
            read_timeout_ms: 30_000,
        }
    }
}
impl ContextSettings {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.max_records == 0
            || self.max_records > MAX_CONTEXT_RECORDS
            || self.max_history_bytes == 0
            || self.max_history_bytes > 16 * 1024 * 1024
            || self.max_stored_bytes == 0
            || self.max_stored_bytes > 16 * 1024 * 1024
            || self.read_timeout_ms == 0
            || self.read_timeout_ms > 300_000
        {
            return Err(BackendError::Query("invalid context limits".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotEvidenceRequest {
    pub namespace: String,
    pub ids: Vec<Uuid>,
    pub captured_before: DateTime<Utc>,
    pub max_bytes: usize,
}
impl SnapshotEvidenceRequest {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        let unique: std::collections::HashSet<_> = self.ids.iter().collect();
        if org.trim().is_empty()
            || self.namespace.trim().is_empty()
            || self.ids.len() > MAX_CONTEXT_RECORDS
            || unique.len() != self.ids.len()
            || self.ids.iter().any(Uuid::is_nil)
            || self.max_bytes == 0
            || self.max_bytes > 16 * 1024 * 1024
        {
            return Err(BackendError::Query(
                "invalid snapshot evidence request".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotEvidence {
    pub uuid: Uuid,
    pub org_id: String,
    pub namespace: String,
    pub source: String,
    pub data_type: SnapshotDataType,
    pub source_description: Option<String>,
    pub captured_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub content: String,
}
impl SnapshotEvidence {
    pub fn byte_len(&self) -> usize {
        self.content
            .len()
            .saturating_add(self.source.len())
            .saturating_add(self.org_id.len())
            .saturating_add(self.namespace.len())
            .saturating_add(self.source_description.as_ref().map_or(0, String::len))
            .saturating_add(256)
    }
    /// Versioned, length-framed UTF-8 fields with canonical UTC timestamps.
    pub fn digest(&self) -> String {
        let mut hash = Sha256::new();
        // Frozen protocol bytes: changing branding must not change existing evidence/cache keys.
        hash.update(b"astrolabe-snapshot-evidence-v1\0");
        for field in [
            self.uuid.to_string(),
            self.org_id.clone(),
            self.namespace.clone(),
            self.source.clone(),
            self.data_type.to_string(),
            self.captured_at.to_rfc3339(),
            self.created_at.to_rfc3339(),
            self.content.clone(),
        ] {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
        match &self.source_description {
            None => hash.update([0]),
            Some(description) => {
                hash.update([1]);
                hash.update((description.len() as u64).to_be_bytes());
                hash.update(description.as_bytes());
            }
        }
        format!("v1:{:x}", hash.finalize())
    }
}

/// The one storage error that means "no retained content answered this exact
/// request" (missing, out of scope or over the byte cap), as opposed to a
/// failed read. Callers that need the distinction match on this text.
pub const EVIDENCE_UNAVAILABLE: &str = "required snapshot evidence unavailable or exceeds limits";

pub fn validate_evidence(
    org: &str,
    request: &SnapshotEvidenceRequest,
    records: &mut [SnapshotEvidence],
) -> Result<(), BackendError> {
    request.validate(org)?;
    let unique: std::collections::HashSet<_> = records.iter().map(|r| r.uuid).collect();
    let bytes = records
        .iter()
        .try_fold(0usize, |n, r| n.checked_add(r.byte_len()));
    if records.len() != request.ids.len()
        || unique.len() != records.len()
        || bytes.is_none_or(|n| n > request.max_bytes)
        || records.iter().any(|r| {
            !request.ids.contains(&r.uuid)
                || r.org_id != org
                || r.namespace != request.namespace
                || r.captured_at > request.captured_before
                || r.content.trim().is_empty()
        })
    {
        return Err(BackendError::Query(EVIDENCE_UNAVAILABLE.into()));
    }
    records.sort_by_key(|r| (r.captured_at, r.created_at, r.uuid));
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceRef {
    pub uuid: Uuid,
    pub digest: String,
}

/// Only references are serialized into receipts; text is reloaded and verified.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotHistory {
    pub references: Vec<EvidenceRef>,
    #[serde(skip)]
    pub records: Vec<SnapshotEvidence>,
}
impl SnapshotHistory {
    pub fn new(records: Vec<SnapshotEvidence>) -> Self {
        Self {
            references: records
                .iter()
                .map(|r| EvidenceRef {
                    uuid: r.uuid,
                    digest: r.digest(),
                })
                .collect(),
            records,
        }
    }
    pub fn validate_for(
        &self,
        snapshot: &crate::models::SnapshotNode,
        org: &str,
        settings: &ContextSettings,
    ) -> bool {
        if snapshot.org_id != org
            || self.references.len() > settings.max_records
            || !self.verify(&self.records)
        {
            return false;
        }
        let request = SnapshotEvidenceRequest {
            namespace: snapshot.namespace.clone(),
            ids: self.references.iter().map(|r| r.uuid).collect(),
            captured_before: snapshot.captured_at,
            max_bytes: settings.max_history_bytes,
        };
        let mut records = self.records.clone();
        validate_evidence(org, &request, &mut records).is_ok() && self.verify(&records)
    }

    pub fn verify(&self, records: &[SnapshotEvidence]) -> bool {
        self.references.len() == records.len()
            && self
                .references
                .iter()
                .zip(records)
                .all(|(a, b)| a.uuid == b.uuid && a.digest == b.digest())
    }
}

/// Shared by context retrieval and pending relationship recovery.
pub async fn load(
    ctx: &super::RuntimeContext,
    request: &SnapshotEvidenceRequest,
) -> Result<Vec<SnapshotEvidence>, crate::errors::StageError> {
    use crate::errors::StageError;
    let stage = "context_retrieval";
    if request.validate(&ctx.org_id).is_err()
        || request.ids.len() > ctx.context_settings.max_records
        || request.max_bytes > ctx.context_settings.max_history_bytes
    {
        return Err(StageError::StateValidation {
            stage: stage.into(),
            message: "invalid context read limits or scope".into(),
        });
    }
    let work = async {
        let _permit =
            crate::telemetry::acquire(&ctx.semaphore, crate::telemetry::OperationKind::GraphRead)
                .await
                .map_err(|_| StageError::Cancelled {
                    stage: stage.into(),
                })?;
        let mut records = ctx
            .graph
            .snapshot_evidence(&ctx.org_id, request)
            .await
            .map_err(|error| StageError::StepFailed {
                stage: stage.into(),
                step: "snapshot_evidence".into(),
                cause: "snapshot evidence read failed".into(),
                retriable: error.is_transient(),
            })?;
        validate_evidence(&ctx.org_id, request, &mut records).map_err(|_| {
            StageError::StateValidation {
                stage: stage.into(),
                message: "required snapshot evidence unavailable or exceeds limits".into(),
            }
        })?;
        Ok(records)
    };
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(StageError::Cancelled { stage: stage.into() }),
        result = tokio::time::timeout(std::time::Duration::from_millis(ctx.context_settings.read_timeout_ms), work) => result.unwrap_or_else(|_| Err(StageError::StepFailed {
            stage: stage.into(), step: "snapshot_evidence".into(), cause: "snapshot evidence deadline exceeded".into(), retriable: true,
        })),
    }
}

/// Preserve explicit source text; otherwise retain structured source facts as canonical JSON.
pub fn source_content(
    input: &crate::models::SnapshotInput,
) -> Result<Option<String>, BackendError> {
    if let Some(content) = &input.content {
        return Ok(Some(content.clone()));
    }
    if input.entities.is_empty() && input.relationship_changes.is_empty() {
        return Ok(None);
    }
    fn ordered(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(values) => {
                let sorted: std::collections::BTreeMap<_, _> = values
                    .into_iter()
                    .map(|(key, value)| (key, ordered(value)))
                    .collect();
                serde_json::Value::Object(sorted.into_iter().collect())
            }
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(ordered).collect())
            }
            value => value,
        }
    }
    let value = serde_json::to_value((&input.entities, &input.relationship_changes))
        .map_err(|_| BackendError::Serialization("structured source evidence".into()))?;
    let serde_json::Value::Array(mut parts) = value else {
        return Err(BackendError::Serialization(
            "structured source shape".into(),
        ));
    };
    let changes = parts
        .pop()
        .ok_or_else(|| BackendError::Serialization("missing source relationships".into()))?;
    let entities = parts
        .pop()
        .ok_or_else(|| BackendError::Serialization("missing source entities".into()))?;
    serde_json::to_string(&ordered(
        serde_json::json!({"entities":entities,"relationship_changes":changes}),
    ))
    .map(Some)
    .map_err(|_| BackendError::Serialization("structured source evidence".into()))
}

pub fn validate_input(
    settings: &ContextSettings,
    input: &crate::models::SnapshotInput,
) -> Result<(), &'static str> {
    if input.previous_snapshot_uuids.len() > settings.max_records {
        return Err("too many context references");
    }
    let content = source_content(input).map_err(|_| "cannot encode source evidence")?;
    if settings.store_content
        && content
            .as_ref()
            .is_some_and(|s| s.len() > settings.max_stored_bytes)
    {
        return Err("snapshot content exceeds storage limit");
    }
    if input
        .source_description
        .as_ref()
        .is_some_and(|s| s.len() > settings.max_history_bytes)
    {
        return Err("source description exceeds limit");
    }
    Ok(())
}

/// Preserve explicit current/history boundaries, including format and source time.
pub fn prompt_evidence(
    snapshot: &crate::models::SnapshotNode,
    history: Option<&SnapshotHistory>,
) -> String {
    let mut text = format!(
        "CURRENT OBSERVATION id={} captured_at={} format={}\nSource description: {}\n{}",
        snapshot.uuid,
        snapshot.captured_at,
        snapshot.data_type,
        crate::sanitize::fence_untrusted(snapshot.source_description.as_deref().unwrap_or("")),
        crate::sanitize::fence_untrusted(snapshot.content.as_deref().unwrap_or(""))
    );
    if let Some(history) = history {
        for record in &history.records {
            text.push_str(&format!(
                "\nPREVIOUS CONTEXT id={} captured_at={} format={}\nSource description: {}\n{}",
                record.uuid,
                record.captured_at,
                record.data_type,
                crate::sanitize::fence_untrusted(
                    record.source_description.as_deref().unwrap_or("")
                ),
                crate::sanitize::fence_untrusted(&record.content)
            ));
        }
    }
    text
}

/// Conservative byte-based upper estimate, including framing and reserved output.
/// It intentionally favors rejecting a too-large prompt over provider truncation.
/// `stage` names the calling stage so a rejected budget is attributed to the
/// stage that built the prompt, not to extraction in general.
pub fn check_prompt_budget(
    stage: &str,
    backend: &dyn crate::traits::LlmBackend,
    messages: &[crate::traits::llm_backend::LlmMessage],
    schema: &serde_json::Value,
    output: u32,
) -> Result<(), crate::errors::StageError> {
    if !backend.is_configured() {
        return Err(crate::errors::StageError::StateValidation {
            stage: stage.into(),
            message: "no language model is configured for this run".into(),
        });
    }
    let bytes = messages
        .iter()
        .fold(schema.to_string().len().saturating_add(1024), |n, m| {
            n.saturating_add(m.content.len())
        });
    if bytes.saturating_add(output as usize) > backend.context_window() {
        return Err(crate::errors::StageError::StateValidation {
            stage: stage.into(),
            message: "required evidence exceeds model context budget".into(),
        });
    }
    Ok(())
}

/// Restore the admitted mixture of stored and same-run evidence without reselecting it.
pub async fn load_frozen(
    ctx: &super::RuntimeContext,
    index: usize,
) -> Result<Vec<SnapshotEvidence>, crate::errors::StageError> {
    let manifest = ctx
        .observation_manifest
        .as_ref()
        .ok_or_else(|| frozen_error("missing observation manifest"))?;
    let frozen = manifest
        .entries
        .get(index)
        .ok_or_else(|| frozen_error("missing frozen observation"))?;
    if frozen.history.len() > ctx.context_settings.max_records {
        return Err(frozen_error("too many frozen context references"));
    }
    let ids: Vec<_> = frozen
        .history
        .iter()
        .map(|item| item.reference.uuid)
        .collect();
    let request = SnapshotEvidenceRequest {
        namespace: frozen.namespace.clone(),
        ids,
        captured_before: frozen.captured_at,
        max_bytes: ctx.context_settings.max_history_bytes,
    };
    request
        .validate(&ctx.org_id)
        .map_err(|_| frozen_error("invalid frozen context request"))?;
    let mut records = Vec::with_capacity(frozen.history.len());
    let mut stored = Vec::new();
    let mut local_bytes = 0usize;
    for item in &frozen.history {
        if ctx.cancel.is_cancelled() {
            return Err(crate::errors::StageError::Cancelled {
                stage: "context_retrieval".into(),
            });
        }
        if let Some(previous) = item.input_index {
            let source = manifest
                .entries
                .get(previous)
                .ok_or_else(|| frozen_error("missing local history input"))?;
            if source.snapshot_uuid != item.reference.uuid
                || source.captured_at > frozen.captured_at
                || (source.captured_at == frozen.captured_at && previous >= index)
            {
                return Err(frozen_error("local history is not an earlier observation"));
            }
            let record = manifest
                .fresh_evidence(&ctx.org_id, &ctx.original_inputs, previous)
                .map_err(|_| frozen_error("local history evidence unavailable"))?;
            local_bytes = local_bytes
                .checked_add(record.byte_len())
                .filter(|bytes| *bytes <= request.max_bytes)
                .ok_or_else(|| frozen_error("local history exceeds context budget"))?;
            records.push(record);
        } else {
            stored.push(item.reference.uuid);
        }
    }
    if !stored.is_empty() {
        let remaining = request.max_bytes.saturating_sub(local_bytes);
        if remaining == 0 {
            return Err(frozen_error("history exceeds context budget"));
        }
        records.extend(
            load(
                ctx,
                &SnapshotEvidenceRequest {
                    ids: stored,
                    max_bytes: remaining,
                    ..request.clone()
                },
            )
            .await?,
        );
    }
    validate_evidence(&ctx.org_id, &request, &mut records)
        .map_err(|_| frozen_error("frozen history scope or completeness mismatch"))?;
    if frozen.history.iter().any(|item| {
        records
            .iter()
            .find(|record| record.uuid == item.reference.uuid)
            .is_none_or(|record| record.digest() != item.reference.digest)
    }) {
        return Err(frozen_error("snapshot context changed since selection"));
    }
    Ok(records)
}
fn frozen_error(message: &str) -> crate::errors::StageError {
    crate::errors::StageError::StateValidation {
        stage: "context_retrieval".into(),
        message: message.into(),
    }
}

pub async fn hydrate(
    ctx: &super::RuntimeContext,
    resolution: &mut super::stage_output::NodeResolutionOutput,
) -> Result<(), crate::errors::StageError> {
    for (id, history) in std::sync::Arc::make_mut(&mut resolution.history) {
        if history.verify(&history.records) {
            continue;
        }
        let snapshot = resolution
            .snapshot_nodes
            .iter()
            .find(|s| s.uuid == *id)
            .ok_or_else(|| crate::errors::StageError::StateValidation {
                stage: "context_retrieval".into(),
                message: "context has no current observation".into(),
            })?;
        let request = SnapshotEvidenceRequest {
            namespace: snapshot.namespace.clone(),
            ids: history.references.iter().map(|r| r.uuid).collect(),
            captured_before: snapshot.captured_at,
            max_bytes: ctx.context_settings.max_history_bytes,
        };
        let records = if let Some(manifest) = &ctx.observation_manifest {
            let index = manifest
                .entries
                .iter()
                .position(|entry| {
                    entry.snapshot_uuid == *id
                        && matches!(entry.kind, super::saga::FrozenObservationKind::Fresh)
                })
                .ok_or_else(|| frozen_error("recovered observation missing from manifest"))?;
            let entry = &manifest.entries[index];
            if entry.namespace != snapshot.namespace
                || entry.created_at != snapshot.created_at
                || entry.captured_at != snapshot.captured_at
            {
                return Err(frozen_error(
                    "recovered observation disagrees with frozen identity",
                ));
            }
            load_frozen(ctx, index).await?
        } else {
            load(ctx, &request).await?
        };
        if !history.verify(&records) {
            return Err(crate::errors::StageError::StateValidation {
                stage: "context_retrieval".into(),
                message: "snapshot context changed since node commit".into(),
            });
        }
        history.records = records;
    }
    Ok(())
}
