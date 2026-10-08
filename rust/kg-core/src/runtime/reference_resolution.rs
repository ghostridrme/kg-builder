//! Contracts of evidence-backed reference decisions: bounded settings, the
//! durable audit record every accepted, rejected or uncertain occurrence leaves
//! behind, and the deterministic identity that dedupes repeated handoffs.
//!
//! The stage that produces these lives in `kg-stages`; storage, receipts
//! and the public output only carry the types defined here.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::BackendError;
use crate::models::edges::ReadVersion;

/// Resource bounds of one ambiguity decision. These are safety limits, not
/// truncation permission: evidence that does not fit is reported as explicit
/// uncertainty before any model call, never trimmed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReferenceResolutionSettings {
    /// One deadline per decision: permit wait, evidence loading, the single
    /// provider attempt and answer validation.
    pub timeout_ms: u64,
    /// Rendered prompt bytes (both messages plus the answer schema). The
    /// stricter of this and the matching prompt budget applies.
    pub max_prompt_bytes: usize,
    /// Output token cap; a normal accepted answer fits in far less.
    pub max_output_tokens: u32,
    /// Evidence items one accepted answer may cite.
    pub max_supporting_items: usize,
    /// Characters of the model's relationship fact.
    pub max_fact_chars: usize,
    /// Serialized bytes of one durable decision audit.
    pub max_audit_bytes: usize,
    /// Reuse persisted decisions of earlier runs whose reuse fingerprint and
    /// cited values are unchanged (a rebinding, never a new claim). Off for
    /// evaluation controls that must pay for every occurrence.
    #[serde(default = "default_persisted_reuse")]
    pub persisted_reuse: bool,
    /// Property paths (index-free, `Parent.child` form) never sent to a model
    /// as evidence, from any source or candidate. Distinct from reference
    /// discovery exclusions, which only stop a value from producing an edge.
    pub disclosure_restricted_paths: Vec<String>,
}

fn default_persisted_reuse() -> bool {
    true
}

impl Default for ReferenceResolutionSettings {
    fn default() -> Self {
        Self {
            timeout_ms: 60_000,
            max_prompt_bytes: 64 * 1024,
            max_output_tokens: 256,
            max_supporting_items: 4,
            max_fact_chars: 240,
            // 8 KiB rather than the plan's 4 KiB starting figure: a complete
            // frozen candidate set (up to the per-value carrier cap plus in-run
            // targets) with observation clocks must always fit its audit.
            max_audit_bytes: 8192,
            persisted_reuse: true,
            disclosure_restricted_paths: Vec::new(),
        }
    }
}

impl ReferenceResolutionSettings {
    pub fn validate(&self) -> Result<(), BackendError> {
        if !(1..=300_000).contains(&self.timeout_ms)
            || !(1024..=4_194_304).contains(&self.max_prompt_bytes)
            || !(1..=8192).contains(&self.max_output_tokens)
            || !(1..=MAX_SUPPORTING_ITEMS).contains(&self.max_supporting_items)
            || !(16..=MAX_FACT_CHARS).contains(&self.max_fact_chars)
            || !(512..=65_536).contains(&self.max_audit_bytes)
            || self.disclosure_restricted_paths.len() > 256
            || self.disclosure_restricted_paths.iter().any(|path| {
                path.trim().is_empty()
                    || path.trim() != path
                    || path.len() > 512
                    || path.chars().any(char::is_control)
            })
        {
            return Err(BackendError::Query(
                "invalid reference resolution settings".into(),
            ));
        }
        Ok(())
    }
}

/// Hard ceilings the settings cannot exceed; the audit validator enforces them
/// regardless of configuration so old receipts stay bounded.
/// Most decision audits one relationship batch may carry; planning refuses a
/// batch above it, and the runner and stage refuse before any provider attempt.
pub const MAX_DECISION_AUDITS_PER_BATCH: usize = 2048;
/// Most serialized audit bytes one relationship batch may carry.
pub const MAX_DECISION_AUDIT_BYTES_PER_BATCH: usize = 4 * 1024 * 1024;

/// Chunk-level preflight run by the pipeline before the decision stage: every
/// pending occurrence is charged its full `max_audit_bytes` allowance (the
/// per-occurrence preflight later uses the exact worst case), audits already
/// carried by the chunk (from a replan) are charged their serialized size. An
/// error means the chunk could never commit its audits, so nothing may be
/// dispatched; the message names the counts and the remedy.
pub fn preflight_chunk_decisions(
    pending: usize,
    carried: &[ReferenceDecisionAudit],
    max_audit_bytes: usize,
) -> Result<(), String> {
    let count = pending.saturating_add(carried.len());
    let bytes = carried
        .iter()
        .map(|audit| serde_json::to_vec(audit).map_or(usize::MAX, |b| b.len()))
        .fold(
            pending.saturating_mul(max_audit_bytes),
            usize::saturating_add,
        );
    if count > MAX_DECISION_AUDITS_PER_BATCH || bytes > MAX_DECISION_AUDIT_BYTES_PER_BATCH {
        return Err(format!(
            "this chunk carries {count} reference decisions with an audit allowance of {bytes} bytes; a relationship batch commits at most {MAX_DECISION_AUDITS_PER_BATCH} audits and {MAX_DECISION_AUDIT_BYTES_PER_BATCH} bytes, so no provider attempt was made (reduce chunk_size or reference_max_values)"
        ));
    }
    Ok(())
}

pub const MAX_SUPPORTING_ITEMS: usize = 16;
pub const MAX_FACT_CHARS: usize = 1024;
/// Host-assigned evidence identifiers stay short and plain so a model never
/// has to repeat a property path.
pub const MAX_EVIDENCE_ID_LEN: usize = 32;

/// Where a source observation's evidence came from. Persisted with every
/// decision; a reconstructed snapshot never implies structured authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceOrigin {
    /// Supplied structured entity properties.
    Structured,
    /// Properties a model extracted from free-form content; the original text
    /// is the proof, the properties are an interpretation.
    TextExtracted,
    /// Legacy or stored observations without recorded provenance.
    Unknown,
}

impl EvidenceOrigin {
    /// Derive from an entity's recorded extractor. `direct` and rule-derived
    /// children are structured input; `llm:<model>` marks text extraction;
    /// anything else (including the repair fallback `stored`) is unknown.
    pub fn of_extractor(extracted_by: &str) -> Self {
        match extracted_by {
            "direct" | "sub_entity_rule" => Self::Structured,
            other if other.starts_with("llm:") => Self::TextExtracted,
            _ => Self::Unknown,
        }
    }
}

/// The three dispositions of one occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionOutcome {
    Accepted,
    Rejected,
    Unsure,
}

/// Objective cause recorded by the host. Model dispositions carry no invented
/// explanation; host refusals name the missing or oversized evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    ModelAccepted,
    ModelRejected,
    ModelUnsure,
    /// The complete permitted evidence does not fit the prompt budget.
    EvidenceBudgetExceeded,
    /// A text-derived observation whose original content is not retained.
    SourceContentUnavailable,
    /// Required proof is absent or disclosure-restricted.
    EvidenceUnavailable,
    /// The occurrence is one of the source's own single-field identity keys:
    /// its value is the source's identifier, and a candidate that happens to
    /// carry the same value shares a name with it. Not decided, never dispatched.
    OwnIdentityValue,
}

impl DecisionReason {
    /// Reasons the host records without any provider attempt.
    pub fn is_host_refusal(self) -> bool {
        matches!(
            self,
            Self::EvidenceBudgetExceeded
                | Self::SourceContentUnavailable
                | Self::OwnIdentityValue
                | Self::EvidenceUnavailable
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    StructuredProperty,
    TextExcerpt,
}

/// One evidence item an accepted answer cited, resolved by the host to its
/// owner and exact location. Text offsets are Unicode scalar positions,
/// end-exclusive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceCitation {
    pub id: String,
    pub owner_chain_id: Uuid,
    pub kind: EvidenceKind,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub snapshot_uuid: Option<Uuid>,
    #[serde(default)]
    pub start_char: Option<usize>,
    #[serde(default)]
    pub end_char: Option<usize>,
}

/// The durable record of one occurrence's decision. One type serves the
/// batch receipt, the public run output and the accepted edge's provenance;
/// prompts and payloads are never duplicated into it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceDecisionAudit {
    /// Deterministic identity of this occurrence and its evidence (see [`decision_id`]).
    pub decision_id: Uuid,
    pub source_chain_id: Uuid,
    pub source_version_uuid: Uuid,
    pub source_snapshot_id: Uuid,
    pub source_captured_at: DateTime<Utc>,
    pub slot: String,
    /// Exact indexed location of the occurrence.
    pub location: String,
    pub value: String,
    pub evidence_origin: EvidenceOrigin,
    pub outcome: DecisionOutcome,
    pub reason: DecisionReason,
    pub target_chain_id: Option<Uuid>,
    pub fact: Option<String>,
    pub supporting_evidence: Vec<EvidenceCitation>,
    /// Every frozen candidate version the decision was made against.
    pub candidate_read_set: Vec<ReadVersion>,
    /// SHA-256 (hex) of the canonical evidence, settings and model identity.
    pub evidence_fingerprint: String,
    /// Whether the permitted evidence universe was shown in full.
    pub evidence_complete: bool,
    pub model_configured: String,
    /// The model that actually served the answer, when a call was made.
    pub model_served: Option<String>,
    /// Actual provider dispatches this decision caused (including failed ones).
    pub provider_attempts: u32,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub processing_version: String,
    /// Host clock at the decision; never a semantic relationship time.
    pub decided_at: DateTime<Utc>,
    /// The decision was reused from an earlier identical evidence packet in this
    /// run or from a persisted decision of an earlier run; model fields
    /// describe the original call.
    #[serde(default)]
    pub reused: bool,
    /// The original (non-reused) decision this one was rebound from.
    #[serde(default)]
    pub reused_from: Option<Uuid>,
    /// SHA-256 (hex) of what the answer depends on and nothing volatile: the
    /// key persisted decisions are looked up by. See `evidence::prepare`.
    #[serde(default)]
    pub reuse_fingerprint: String,
    /// The canonical value of every cited structured property at decision
    /// time; a reuse requires each of them unchanged.
    #[serde(default)]
    pub cited_value_hashes: Vec<CitedValueHash>,
}

/// The value one accepted citation pointed at, hashed so a later reuse can
/// prove the cited evidence did not change. Keyed by owner and path, never by
/// the packet-positional evidence id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CitedValueHash {
    pub owner_chain_id: Uuid,
    pub path: String,
    pub sha256: String,
}

/// Most decision records (or reuse notes) compiled into one storage statement.
pub const MAX_DECISIONS_PER_STATEMENT: usize = 500;

/// One frozen candidate of a persisted decision: identity and key shape only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedCandidate {
    pub chain_id: Uuid,
    pub entity_type: String,
    pub key_groups: Vec<Vec<String>>,
}

/// A model decision as stored in the graph: the audit plus what rule learning
/// and cross-run reuse need beyond it. Hashes and identifiers only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedDecision {
    pub audit: ReferenceDecisionAudit,
    pub producer_source: String,
    pub observing_namespace: String,
    pub source_entity_type: String,
    pub target_type: Option<String>,
    /// Context and value tokens of the occurrence (`PendingReference.components`).
    pub components: Vec<(String, String)>,
    pub reference_tokens: Vec<String>,
    pub candidates: Vec<PersistedCandidate>,
    #[serde(default)]
    pub reuse_count: u64,
    #[serde(default)]
    pub last_reused_at: Option<DateTime<Utc>>,
}

impl PersistedDecision {
    pub fn validate(&self, org_id: &str) -> Result<(), BackendError> {
        let invalid = |m: &str| BackendError::Query(format!("invalid persisted decision: {m}"));
        if self.audit.reused || self.audit.reason.is_host_refusal() {
            return Err(invalid("only original model dispositions are persisted"));
        }
        if self.audit.reuse_fingerprint.len() != 64 {
            return Err(invalid("reuse fingerprint"));
        }
        if org_id.trim().is_empty()
            || self.producer_source.trim().is_empty()
            || self.observing_namespace.trim().is_empty()
            || self.source_entity_type.trim().is_empty()
        {
            return Err(invalid("scope"));
        }
        if self.candidates.is_empty()
            || self
                .candidates
                .iter()
                .any(|c| c.chain_id.is_nil() || c.entity_type.is_empty())
            || self.components.len() > 64
            || self.reference_tokens.len() > 64
        {
            return Err(invalid("candidates or tokens"));
        }
        Ok(())
    }
}

/// What a decision needs beyond its audit to be persisted (§ decision memory):
/// carried next to the audits in the batch report and joined by decision id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionContext {
    pub decision_id: Uuid,
    pub producer_source: String,
    pub observing_namespace: String,
    pub source_entity_type: String,
    pub target_type: Option<String>,
    pub components: Vec<(String, String)>,
    pub reference_tokens: Vec<String>,
    pub candidates: Vec<PersistedCandidate>,
}

/// One reuse of a persisted decision by a later occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionReuse {
    pub original: Uuid,
    pub decision_id: Uuid,
    pub at: DateTime<Utc>,
    /// The reusing occurrence's live source version. Recorded on the original so
    /// rule-learning labels still recognise the decision once the source has
    /// rebound to this version; otherwise the labelled case would decay the
    /// moment the source produced a new version that merely reused the verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version_uuid: Option<Uuid>,
}

impl ReferenceDecisionAudit {
    /// Structural and combinational checks plus the serialized byte bound.
    pub fn validate(&self, max_bytes: usize) -> Result<(), BackendError> {
        let invalid = |message: &str| {
            BackendError::Query(format!("invalid reference decision audit: {message}"))
        };
        if self.decision_id.is_nil()
            || self.source_chain_id.is_nil()
            || self.source_version_uuid.is_nil()
            || self.source_snapshot_id.is_nil()
        {
            return Err(invalid("nil identifier"));
        }
        for (field, value, limit) in [
            ("slot", &self.slot, 512),
            ("location", &self.location, 512),
            ("value", &self.value, 4096),
            ("model_configured", &self.model_configured, 256),
            ("processing_version", &self.processing_version, 512),
        ] {
            if value.trim().is_empty() || value.len() > limit || value.chars().any(char::is_control)
            {
                return Err(invalid(field));
            }
        }
        if self.evidence_fingerprint.len() != 64
            || !self
                .evidence_fingerprint
                .chars()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err(invalid("evidence fingerprint"));
        }
        if self.reused != self.reused_from.is_some() {
            return Err(invalid(
                "reused decision must name its original and only then",
            ));
        }
        if self.reused_from.is_some_and(|id| id.is_nil()) {
            return Err(invalid("reused_from"));
        }
        if !self.reason.is_host_refusal()
            && (self.reuse_fingerprint.len() != 64
                || !self
                    .reuse_fingerprint
                    .chars()
                    .all(|c| c.is_ascii_hexdigit()))
        {
            return Err(invalid("reuse fingerprint"));
        }
        // Every citation plus the occurrence and its enclosing element.
        if self.cited_value_hashes.len() > MAX_SUPPORTING_ITEMS + 2
            || self
                .cited_value_hashes
                .iter()
                .any(|h| h.owner_chain_id.is_nil() || h.path.is_empty() || h.sha256.len() != 64)
        {
            return Err(invalid("cited value hashes"));
        }
        let accepted = self.outcome == DecisionOutcome::Accepted;
        let consistent = match (self.outcome, self.reason) {
            (DecisionOutcome::Accepted, DecisionReason::ModelAccepted)
            | (DecisionOutcome::Rejected, DecisionReason::ModelRejected)
            | (DecisionOutcome::Unsure, DecisionReason::ModelUnsure) => true,
            (DecisionOutcome::Unsure, reason) => reason.is_host_refusal(),
            _ => false,
        };
        if !consistent {
            return Err(invalid("outcome and reason disagree"));
        }
        if self.reason.is_host_refusal()
            && (self.provider_attempts != 0 || self.model_served.is_some())
        {
            return Err(invalid("a host refusal records no provider attempt"));
        }
        if !self.reason.is_host_refusal()
            && (self.provider_attempts == 0 || self.model_served.is_none())
        {
            return Err(invalid(
                "a model disposition records its attempt and served model",
            ));
        }
        if accepted != self.target_chain_id.is_some_and(|id| !id.is_nil())
            || accepted != self.fact.is_some()
            || accepted == self.supporting_evidence.is_empty()
        {
            return Err(invalid("decision fields disagree with the outcome"));
        }
        if let Some(fact) = &self.fact {
            if fact.trim().is_empty()
                || fact.chars().count() > MAX_FACT_CHARS
                || fact.chars().any(char::is_control)
            {
                return Err(invalid("fact"));
            }
        }
        if self.supporting_evidence.len() > MAX_SUPPORTING_ITEMS {
            return Err(invalid("too many supporting items"));
        }
        let mut ids = std::collections::BTreeSet::new();
        for citation in &self.supporting_evidence {
            if citation.id.is_empty()
                || citation.id.len() > MAX_EVIDENCE_ID_LEN
                || !citation.id.is_ascii()
                || citation.owner_chain_id.is_nil()
                || !ids.insert(citation.id.as_str())
                || match citation.kind {
                    EvidenceKind::StructuredProperty => {
                        citation
                            .path
                            .as_ref()
                            .is_none_or(|p| p.is_empty() || p.len() > 512)
                            || citation.start_char.is_some()
                            || citation.end_char.is_some()
                    }
                    EvidenceKind::TextExcerpt => {
                        citation.snapshot_uuid.is_none_or(|id| id.is_nil())
                            || citation
                                .start_char
                                .zip(citation.end_char)
                                .is_none_or(|(start, end)| start >= end)
                    }
                }
            {
                return Err(invalid("supporting evidence citation"));
            }
        }
        if accepted
            && !self
                .supporting_evidence
                .iter()
                .any(|citation| citation.owner_chain_id == self.source_chain_id)
        {
            return Err(invalid("acceptance without current source evidence"));
        }
        if let Some(target) = self.target_chain_id {
            if !self
                .candidate_read_set
                .iter()
                .any(|read| read.chain_id == target)
            {
                return Err(invalid("target outside the frozen candidate set"));
            }
        }
        if self.candidate_read_set.is_empty()
            || self
                .candidate_read_set
                .iter()
                .any(|read| read.chain_id.is_nil() || read.version_uuid.is_nil())
        {
            return Err(invalid("candidate read set"));
        }
        if self
            .model_served
            .as_ref()
            .is_some_and(|m| m.trim().is_empty() || m.len() > 256)
        {
            return Err(invalid("served model"));
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|e| BackendError::Serialization(e.to_string()))?
            .len();
        if bytes > max_bytes {
            return Err(BackendError::Query(format!(
                "reference decision audit is {bytes} bytes; the limit is {max_bytes}"
            )));
        }
        Ok(())
    }

    /// Rebind a reused decision to the observation that now carries it. Model
    /// metadata, fact and citations stay those of the original call; the
    /// location, read set and evidence fingerprint are the current
    /// occurrence's (the reuse key is index-free, so the same tag may sit at
    /// another array position), so commit fences and the audit trail describe
    /// what was actually committed against. `reused_from` always names the
    /// original decision, never a rebinding.
    #[allow(clippy::too_many_arguments)]
    pub fn rebound(
        &self,
        decision_id: Uuid,
        source_version_uuid: Uuid,
        snapshot_id: Uuid,
        captured_at: DateTime<Utc>,
        location: String,
        decided_at: DateTime<Utc>,
        read_set: Vec<ReadVersion>,
        evidence_fingerprint: String,
    ) -> Self {
        Self {
            decision_id,
            source_version_uuid,
            source_snapshot_id: snapshot_id,
            source_captured_at: captured_at,
            location,
            decided_at,
            reused: true,
            reused_from: Some(self.reused_from.unwrap_or(self.decision_id)),
            candidate_read_set: read_set,
            evidence_fingerprint,
            ..self.clone()
        }
    }
}

const DECISION_NAMESPACE: Uuid = Uuid::from_u128(0x6f2d_2a1e_5c4b_4d3e_9a8f_7b6c_5d4e_3f2a);

/// Deterministic identity of one occurrence decision: organization, source
/// chain, snapshot and capture time, slot, exact indexed location, value and
/// the evidence fingerprint. Repeated handoffs of the same occurrence dedupe on
/// it; different array members, captures or evidence never collide.
#[allow(clippy::too_many_arguments)]
pub fn decision_id(
    org: &str,
    source_chain_id: Uuid,
    snapshot_id: Uuid,
    captured_at: DateTime<Utc>,
    slot: &str,
    location: &str,
    value: &str,
    evidence_fingerprint: &str,
) -> Uuid {
    let mut bytes = Vec::new();
    for part in [
        org,
        &source_chain_id.to_string(),
        &snapshot_id.to_string(),
        &captured_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        slot,
        location,
        value,
        evidence_fingerprint,
    ] {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    Uuid::new_v5(&DECISION_NAMESPACE, &bytes)
}

/// Combine audits from several producers: identical records dedupe on their
/// decision id; two different records with one id are a validation error.
pub fn merge_audits(
    existing: &mut Vec<ReferenceDecisionAudit>,
    incoming: &[ReferenceDecisionAudit],
) -> Result<(), BackendError> {
    for audit in incoming {
        match existing
            .iter()
            .find(|known| known.decision_id == audit.decision_id)
        {
            Some(known) if known == audit => {}
            Some(_) => {
                return Err(BackendError::Query(format!(
                    "conflicting reference decision audits for {}",
                    audit.decision_id
                )));
            }
            None => existing.push(audit.clone()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunk_preflight_applies_the_batch_caps_before_any_attempt() {
        assert!(preflight_chunk_decisions(0, &[], 8192).is_ok());
        assert!(
            preflight_chunk_decisions(512, &[], 8192).is_ok(),
            "512 * 8 KiB is exactly 4 MiB"
        );
        let error = preflight_chunk_decisions(513, &[], 8192).unwrap_err();
        assert!(error.contains("513 reference decisions") && error.contains("no provider attempt"));
        assert!(preflight_chunk_decisions(2048, &[], 1).is_ok());
        assert!(preflight_chunk_decisions(2049, &[], 1).is_err());
        assert!(
            preflight_chunk_decisions(usize::MAX, &[], 8192).is_err(),
            "saturating arithmetic"
        );
    }

    use super::*;

    fn audit() -> ReferenceDecisionAudit {
        let source = Uuid::from_u128(1);
        let target = Uuid::from_u128(2);
        ReferenceDecisionAudit {
            decision_id: Uuid::from_u128(9),
            source_chain_id: source,
            source_version_uuid: Uuid::from_u128(11),
            source_snapshot_id: Uuid::from_u128(12),
            source_captured_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            slot: "AWS::EC2::Instance.Tags.Value".into(),
            location: "Tags[0].Value".into(),
            value: "archive".into(),
            evidence_origin: EvidenceOrigin::Structured,
            outcome: DecisionOutcome::Accepted,
            reason: DecisionReason::ModelAccepted,
            target_chain_id: Some(target),
            fact: Some("instance names bucket archive as its backup bucket".into()),
            supporting_evidence: vec![
                EvidenceCitation {
                    id: "src:occ".into(),
                    owner_chain_id: source,
                    kind: EvidenceKind::StructuredProperty,
                    path: Some("Tags[0]".into()),
                    snapshot_uuid: None,
                    start_char: None,
                    end_char: None,
                },
                EvidenceCitation {
                    id: "c1:p1".into(),
                    owner_chain_id: target,
                    kind: EvidenceKind::StructuredProperty,
                    path: Some("Name".into()),
                    snapshot_uuid: None,
                    start_char: None,
                    end_char: None,
                },
            ],
            candidate_read_set: vec![
                ReadVersion {
                    chain_id: target,
                    version_uuid: Uuid::from_u128(21),
                    version: 1,
                    observed_at: None,
                },
                ReadVersion {
                    chain_id: Uuid::from_u128(3),
                    version_uuid: Uuid::from_u128(31),
                    version: 2,
                    observed_at: None,
                },
            ],
            evidence_fingerprint: "ab".repeat(32),
            evidence_complete: true,
            model_configured: "fixture-model".into(),
            model_served: Some("fixture-model-2026".into()),
            provider_attempts: 1,
            input_tokens: Some(700),
            output_tokens: Some(40),
            processing_version: "reference-resolution-v7:abc".into(),
            decided_at: DateTime::from_timestamp(1_700_000_100, 0).unwrap(),
            reused: false,
            reused_from: None,
            reuse_fingerprint: "c".repeat(64),
            cited_value_hashes: Vec::new(),
        }
    }

    #[test]
    fn settings_defaults_validate_and_bounds_are_enforced() {
        let defaults = ReferenceResolutionSettings::default();
        defaults.validate().unwrap();
        assert_eq!(defaults.max_output_tokens, 256);
        assert_eq!(defaults.max_prompt_bytes, 65_536);
        let parsed: ReferenceResolutionSettings = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed, defaults);
        assert!(serde_json::from_str::<ReferenceResolutionSettings>(r#"{"timeout":5}"#).is_err());
        for broken in [
            ReferenceResolutionSettings {
                timeout_ms: 0,
                ..Default::default()
            },
            ReferenceResolutionSettings {
                max_prompt_bytes: 10,
                ..Default::default()
            },
            ReferenceResolutionSettings {
                max_output_tokens: 0,
                ..Default::default()
            },
            ReferenceResolutionSettings {
                max_supporting_items: 17,
                ..Default::default()
            },
            ReferenceResolutionSettings {
                max_fact_chars: 2000,
                ..Default::default()
            },
            ReferenceResolutionSettings {
                max_audit_bytes: 100,
                ..Default::default()
            },
            ReferenceResolutionSettings {
                disclosure_restricted_paths: vec![" secret".into()],
                ..Default::default()
            },
        ] {
            assert!(broken.validate().is_err());
        }
    }

    #[test]
    fn evidence_origin_follows_the_recorded_extractor_not_the_snapshot_type() {
        assert_eq!(
            EvidenceOrigin::of_extractor("direct"),
            EvidenceOrigin::Structured
        );
        assert_eq!(
            EvidenceOrigin::of_extractor("sub_entity_rule"),
            EvidenceOrigin::Structured
        );
        assert_eq!(
            EvidenceOrigin::of_extractor("llm:gpt"),
            EvidenceOrigin::TextExtracted
        );
        assert_eq!(
            EvidenceOrigin::of_extractor("stored"),
            EvidenceOrigin::Unknown
        );
        assert_eq!(EvidenceOrigin::of_extractor(""), EvidenceOrigin::Unknown);
    }

    #[test]
    fn accepted_audit_requires_source_support_frozen_target_and_bounded_size() {
        let ok = audit();
        ok.validate(4096).unwrap();
        assert!(ok.validate(100).is_err(), "byte bound");
        let mut only_target = ok.clone();
        only_target.supporting_evidence.remove(0);
        assert!(only_target.validate(4096).is_err(), "target metadata alone");
        let mut foreign = ok.clone();
        foreign.target_chain_id = Some(Uuid::from_u128(77));
        assert!(foreign.validate(4096).is_err(), "target outside candidates");
        let mut no_fact = ok.clone();
        no_fact.fact = None;
        assert!(no_fact.validate(4096).is_err());
        let mut duplicate = ok.clone();
        duplicate.supporting_evidence[1].id = "src:occ".into();
        assert!(duplicate.validate(4096).is_err(), "duplicate citation");
        let mut long_id = ok.clone();
        long_id.supporting_evidence[0].id = "x".repeat(33);
        assert!(long_id.validate(4096).is_err());
    }

    #[test]
    fn negative_and_host_dispositions_have_consistent_fields() {
        let mut rejected = audit();
        rejected.outcome = DecisionOutcome::Rejected;
        rejected.reason = DecisionReason::ModelRejected;
        rejected.target_chain_id = None;
        rejected.fact = None;
        rejected.supporting_evidence.clear();
        rejected.validate(4096).unwrap();
        let mut wrong_reason = rejected.clone();
        wrong_reason.reason = DecisionReason::ModelUnsure;
        assert!(wrong_reason.validate(4096).is_err());
        let mut budget = rejected.clone();
        budget.outcome = DecisionOutcome::Unsure;
        budget.reason = DecisionReason::EvidenceBudgetExceeded;
        assert!(
            budget.validate(4096).is_err(),
            "a host refusal made no call"
        );
        budget.provider_attempts = 0;
        budget.model_served = None;
        budget.input_tokens = None;
        budget.output_tokens = None;
        budget.validate(4096).unwrap();
        let mut accepted_host = budget.clone();
        accepted_host.outcome = DecisionOutcome::Accepted;
        assert!(accepted_host.validate(4096).is_err());
    }

    #[test]
    fn decision_ids_are_deterministic_and_distinguish_members_and_evidence() {
        let at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let base = decision_id(
            "org",
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            at,
            "T.Tags.Value",
            "Tags[0].Value",
            "v",
            &"a".repeat(64),
        );
        assert_eq!(
            base,
            decision_id(
                "org",
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                at,
                "T.Tags.Value",
                "Tags[0].Value",
                "v",
                &"a".repeat(64)
            )
        );
        assert_ne!(
            base,
            decision_id(
                "org",
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                at,
                "T.Tags.Value",
                "Tags[1].Value",
                "v",
                &"a".repeat(64)
            ),
            "array member"
        );
        assert_ne!(
            base,
            decision_id(
                "org",
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                at,
                "T.Tags.Value",
                "Tags[0].Value",
                "v",
                &"b".repeat(64)
            ),
            "evidence"
        );
        assert_ne!(
            base,
            decision_id(
                "org",
                Uuid::from_u128(1),
                Uuid::from_u128(3),
                at,
                "T.Tags.Value",
                "Tags[0].Value",
                "v",
                &"a".repeat(64)
            ),
            "capture"
        );
        assert_ne!(
            base,
            decision_id(
                "other",
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                at,
                "T.Tags.Value",
                "Tags[0].Value",
                "v",
                &"a".repeat(64)
            ),
            "organization"
        );
    }

    #[test]
    fn audits_dedupe_identical_records_and_reject_conflicts() {
        let first = audit();
        let mut merged = vec![first.clone()];
        merge_audits(&mut merged, std::slice::from_ref(&first)).unwrap();
        assert_eq!(merged.len(), 1);
        let mut changed = first.clone();
        changed.fact = Some("another claim".into());
        assert!(merge_audits(&mut merged, &[changed]).is_err());
        let mut other = first.clone();
        other.decision_id = Uuid::from_u128(10);
        merge_audits(&mut merged, &[other]).unwrap();
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn rebinding_keeps_model_metadata_and_marks_reuse() {
        let original = audit();
        let current_read_set = vec![ReadVersion {
            chain_id: Uuid::from_u128(2),
            version_uuid: Uuid::from_u128(22),
            version: 2,
            observed_at: None,
        }];
        let rebound = original.rebound(
            Uuid::from_u128(99),
            Uuid::from_u128(98),
            Uuid::from_u128(97),
            DateTime::from_timestamp(1_800_000_000, 0).unwrap(),
            "Tags[3].Value".into(),
            DateTime::from_timestamp(1_800_000_001, 0).unwrap(),
            current_read_set.clone(),
            "d".repeat(64),
        );
        assert!(rebound.reused);
        assert_eq!(
            rebound.location, "Tags[3].Value",
            "the current occurrence's location"
        );
        assert_eq!(rebound.reused_from, Some(original.decision_id));
        assert_eq!(rebound.candidate_read_set, current_read_set);
        assert_eq!(rebound.evidence_fingerprint, "d".repeat(64));
        // Rebinding a rebinding still names the original.
        let twice = rebound.rebound(
            Uuid::from_u128(100),
            Uuid::from_u128(98),
            Uuid::from_u128(97),
            DateTime::from_timestamp(1_800_000_002, 0).unwrap(),
            original.location.clone(),
            DateTime::from_timestamp(1_800_000_003, 0).unwrap(),
            current_read_set,
            "e".repeat(64),
        );
        assert_eq!(twice.reused_from, Some(original.decision_id));
        assert_eq!(rebound.decision_id, Uuid::from_u128(99));
        assert_eq!(rebound.source_snapshot_id, Uuid::from_u128(97));
        assert_eq!(rebound.model_served, original.model_served);
        assert_ne!(rebound.evidence_fingerprint, original.evidence_fingerprint);
        rebound.validate(4096).unwrap();
    }

    #[test]
    fn old_records_without_reuse_flag_still_decode() {
        let mut value = serde_json::to_value(audit()).unwrap();
        value.as_object_mut().unwrap().remove("reused");
        let decoded: ReferenceDecisionAudit = serde_json::from_value(value).unwrap();
        assert!(!decoded.reused);
    }
}
