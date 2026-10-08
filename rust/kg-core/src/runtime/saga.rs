//! Frozen observation identities and bounded context shared by ingestion attempts.
use crate::{models::IngestionInput, runtime::history::EvidenceRef};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

pub const MAX_OBSERVATIONS: usize = 10_000;
pub const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONTEXT_REFERENCES: usize = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FrozenThreadAssociation {
    pub saga_uuid: Uuid,
    pub name: String,
    /// Only the caller-selected predecessor; implicit append is planned against current membership.
    pub previous_snapshot_uuid: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FrozenObservationKind {
    Fresh,
    Existing { evidence_digest: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FrozenContextRef {
    pub reference: EvidenceRef,
    /// Original input ordinal only for immutable fresh evidence in this request.
    pub input_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FrozenObservation {
    pub snapshot_uuid: Uuid,
    pub namespace: String,
    pub created_at: DateTime<Utc>,
    pub captured_at: DateTime<Utc>,
    pub kind: FrozenObservationKind,
    pub saga: Option<FrozenThreadAssociation>,
    pub history: Vec<FrozenContextRef>,
}

/// Entries retain caller order; batches contain original ordinals in execution order.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunObservationManifest {
    pub entries: Vec<FrozenObservation>,
    pub node_batches: Vec<Vec<usize>>,
}
impl RunObservationManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.entries.len() > MAX_OBSERVATIONS {
            return Err("too many run observations".into());
        }
        let mut seen = HashSet::new();
        let mut refs = 0usize;
        for entry in &self.entries {
            refs = refs
                .checked_add(entry.history.len())
                .ok_or("context reference overflow")?;
            if entry.snapshot_uuid.is_nil()
                || entry.namespace.trim().is_empty()
                || entry.history.len() > super::history::MAX_CONTEXT_RECORDS
                || entry.history.iter().any(|reference| {
                    reference.reference.uuid.is_nil()
                        || reference.reference.uuid == entry.snapshot_uuid
                        || reference.reference.digest.trim().is_empty()
                })
                || entry
                    .history
                    .iter()
                    .map(|reference| reference.reference.uuid)
                    .collect::<HashSet<_>>()
                    .len()
                    != entry.history.len()
            {
                return Err("invalid frozen observation identity or history".into());
            }
            if let Some(saga) = &entry.saga {
                if saga.saga_uuid.is_nil()
                    || saga.name.trim().is_empty()
                    || saga
                        .previous_snapshot_uuid
                        .is_some_and(|id| id.is_nil() || id == entry.snapshot_uuid)
                {
                    return Err("invalid frozen Saga association".into());
                }
            }
            if matches!(&entry.kind, FrozenObservationKind::Existing { evidence_digest } if evidence_digest.trim().is_empty() || entry.saga.is_none())
            {
                return Err("invalid reused observation".into());
            }
        }
        for entry in &self.entries {
            for context in &entry.history {
                if let Some(index) = context.input_index {
                    let source = self
                        .entries
                        .get(index)
                        .ok_or("context ordinal outside request")?;
                    if source.snapshot_uuid != context.reference.uuid
                        || source.namespace != entry.namespace
                        || source.captured_at > entry.captured_at
                        || !matches!(source.kind, FrozenObservationKind::Fresh)
                    {
                        return Err("invalid local context source".into());
                    }
                }
            }
        }
        for batch in &self.node_batches {
            if batch.is_empty() {
                return Err("empty frozen node batch".into());
            }
            for index in batch {
                if *index >= self.entries.len() || !seen.insert(*index) {
                    return Err("invalid frozen node batch membership".into());
                }
            }
        }
        let positions: std::collections::HashMap<_, _> = self
            .node_batches
            .iter()
            .flatten()
            .enumerate()
            .map(|(position, index)| (*index, position))
            .collect();
        for (index, entry) in self.entries.iter().enumerate() {
            for context in &entry.history {
                if context
                    .input_index
                    .is_some_and(|prior| positions.get(&prior) >= positions.get(&index))
                {
                    return Err("local context does not precede its observation".into());
                }
            }
        }
        if seen.len() != self.entries.len()
            || refs > MAX_CONTEXT_REFERENCES
            || serde_json::to_vec(self)
                .map_err(|_| "cannot encode observation manifest")?
                .len()
                > MAX_MANIFEST_BYTES
        {
            return Err("incomplete or oversized observation manifest".into());
        }
        Ok(())
    }

    pub fn validate_inputs(
        &self,
        inputs: &[IngestionInput],
        chunk_size: usize,
    ) -> Result<(), String> {
        self.validate()?;
        if inputs.len() != self.entries.len()
            || chunk_size == 0
            || self
                .node_batches
                .iter()
                .any(|batch| batch.len() > chunk_size)
        {
            return Err("observation manifest disagrees with request shape".into());
        }
        for (entry, input) in self.entries.iter().zip(inputs) {
            if entry.namespace != input.namespace()
                || entry.saga.is_some() != input.saga().is_some()
            {
                return Err("observation manifest scope or association mismatch".into());
            }
            if let (Some(frozen), Some(requested)) = (&entry.saga, input.saga()) {
                let reference_agrees = match &requested.saga {
                    crate::saga::ThreadReference::Name { name } => name == &frozen.name,
                    crate::saga::ThreadReference::Uuid { uuid } => uuid == &frozen.saga_uuid,
                };
                if !reference_agrees
                    || requested.previous_snapshot_uuid != frozen.previous_snapshot_uuid
                {
                    return Err("frozen Saga association differs from request".into());
                }
            }
            match (&entry.kind, input) {
                (FrozenObservationKind::Fresh, IngestionInput::Fresh(input)) => {
                    if input.captured_at.is_some_and(|at| at != entry.captured_at) {
                        return Err("observation capture time mismatch".into());
                    }
                }
                (FrozenObservationKind::Existing { .. }, IngestionInput::Existing(input))
                    if input.snapshot_uuid == entry.snapshot_uuid => {}
                _ => return Err("observation manifest input kind mismatch".into()),
            }
        }
        Ok(())
    }
}

impl RunObservationManifest {
    pub fn fresh_evidence(
        &self,
        org: &str,
        inputs: &[IngestionInput],
        index: usize,
    ) -> Result<super::history::SnapshotEvidence, String> {
        let entry = self
            .entries
            .get(index)
            .ok_or("observation ordinal outside manifest")?;
        let Some(IngestionInput::Fresh(input)) = inputs.get(index) else {
            return Err("local evidence must reference a fresh input".into());
        };
        input
            .validate_request(org)
            .map_err(|_| "invalid local evidence input")?;
        if !matches!(entry.kind, FrozenObservationKind::Fresh)
            || entry.namespace != input.namespace
            || input.captured_at.is_some_and(|at| at != entry.captured_at)
        {
            return Err("local evidence disagrees with manifest".into());
        }
        let content = super::history::source_content(input)
            .map_err(|_| "cannot encode local source evidence")?
            .filter(|s| !s.trim().is_empty())
            .ok_or("local context has no source content")?;
        Ok(super::history::SnapshotEvidence {
            uuid: entry.snapshot_uuid,
            org_id: org.into(),
            namespace: entry.namespace.clone(),
            source: input.source.clone(),
            data_type: input.data_type,
            source_description: input.source_description.clone(),
            captured_at: entry.captured_at,
            created_at: entry.created_at,
            content,
        })
    }
}

/// Complete incremental Saga summaries are optional and always awaited.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SagaSummarySettings {
    pub enabled: bool,
    pub mode: super::entity_summary::SummaryMode,
    pub page_size: usize,
    pub max_evidence_bytes: usize,
    pub max_summary_bytes: usize,
    pub max_output_tokens: u32,
    pub max_replans: u32,
    pub timeout_ms: u64,
    pub max_concurrent_batches: usize,
}
impl Default for SagaSummarySettings {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: super::entity_summary::SummaryMode::Deterministic,
            page_size: 32,
            max_evidence_bytes: 256 * 1024,
            max_summary_bytes: 16 * 1024,
            max_output_tokens: 4096,
            max_replans: 3,
            timeout_ms: 60_000,
            max_concurrent_batches: 2,
        }
    }
}
impl SagaSummarySettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=super::history::MAX_CONTEXT_RECORDS).contains(&self.page_size)
            || !(1..=8 * 1024 * 1024).contains(&self.max_evidence_bytes)
            || !(1..=crate::saga::MAX_SUMMARY_BYTES).contains(&self.max_summary_bytes)
            || !(1..=32_768).contains(&self.max_output_tokens)
            || self.max_replans > 16
            || !(1..=600_000).contains(&self.timeout_ms)
            || !(1..=16).contains(&self.max_concurrent_batches)
        {
            return Err("invalid Saga summary bounds".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn summary_pages_are_bounded_by_the_evidence_read_contract() {
        let mut settings = SagaSummarySettings::default();
        settings.validate().unwrap();
        settings.page_size = 101;
        assert!(settings.validate().is_err());
        settings.page_size = 100;
        settings.max_summary_bytes = 128;
        settings.validate().unwrap();
    }
    #[test]
    fn summary_manifest_rejects_duplicate_targets_and_index_overflow() {
        use crate::runtime::stage_output::{SagaSummaryManifest, SagaSummaryTarget};
        let target = SagaSummaryTarget {
            namespace: "prod".into(),
            saga_uuid: Uuid::new_v4(),
            after_ordinal: 2,
            through_ordinal: 7,
        };
        let mut manifest = SagaSummaryManifest {
            targets: vec![target.clone()],
            page_size: 2,
            collections: vec![],
        };
        assert_eq!(manifest.page_count().unwrap(), 3);
        manifest.validate().unwrap();
        manifest.targets.push(target);
        assert!(manifest.validate().is_err());
        manifest.targets.pop();
        manifest.targets[0].through_ordinal = i64::MAX as u64;
        assert!(manifest.validate().is_err());
    }
    fn entry() -> FrozenObservation {
        FrozenObservation {
            snapshot_uuid: Uuid::new_v4(),
            namespace: "prod".into(),
            created_at: Utc::now(),
            captured_at: Utc::now(),
            kind: FrozenObservationKind::Fresh,
            saga: None,
            history: vec![],
        }
    }
    #[test]
    fn batch_membership_is_an_exact_permutation_of_original_ordinals() {
        let mut manifest = RunObservationManifest {
            entries: vec![entry(), entry()],
            node_batches: vec![vec![1], vec![0]],
        };
        manifest.validate().unwrap();
        manifest.node_batches = vec![vec![1], vec![1]];
        assert!(manifest.validate().is_err());
        manifest.node_batches = vec![vec![1]];
        assert!(manifest.validate().is_err());
    }
    #[test]
    fn local_history_cannot_cross_namespaces_or_forge_its_identity() {
        let mut manifest = RunObservationManifest {
            entries: vec![entry(), entry()],
            node_batches: vec![vec![0, 1]],
        };
        let previous = manifest.entries[0].snapshot_uuid;
        manifest.entries[1].history.push(FrozenContextRef {
            reference: EvidenceRef {
                uuid: previous,
                digest: "digest".into(),
            },
            input_index: Some(0),
        });
        manifest.validate().unwrap();
        manifest.entries[0].namespace = "foreign".into();
        assert!(manifest.validate().is_err());
    }
}
