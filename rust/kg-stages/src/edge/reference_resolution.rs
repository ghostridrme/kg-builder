//! Resolve only references whose eligible endpoints remain ambiguous.
//!
//! Deterministic discovery has already confirmed every reference it could.
//! For each remaining occurrence this stage assembles the complete permitted
//! evidence (the producing observation, its original text when the entity was
//! extracted from text, and every frozen candidate hydrated to its exact stored
//! record), spends at most one actual provider attempt on it, validates the
//! four-field answer against what was shown, and leaves one durable audit record
//! whether the outcome is accepted, rejected or unsure. Host refusals (evidence
//! that cannot be shown or does not fit) are recorded without any model call.
use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use kg_core::{
    errors::StageError,
    runtime::{
        reference_cache::{CachedOutcome, Claim, ReferenceDecisionKey},
        reference_resolution::{
            decision_id, DecisionOutcome, DecisionReason, EvidenceOrigin, ReferenceDecisionAudit,
        },
        stage_output::PendingReference,
        RuntimeContext, StageOutput,
    },
    telemetry::{reference_resolution_outcome as outcome_metric, ReferenceResolutionOutcome},
    traits::{llm_backend::CallBudget, Stage},
};

use decision::{
    answer_schema, parse_answer, system_prompt, Answer, PROMPT_VERSION, SCHEMA_VERSION,
};
use evidence::{hydrate_candidates, prepare, render, Inputs, Preparation, Prepared, PACKET_SCHEMA};

/// Confirm one eligible target with evidence or abstain after deterministic extraction.
pub struct ReferenceResolutionStage;
const STAGE: &str = "reference_resolution";
const REVISION: &str = "reference-resolution-v8-restricted-key-evidence";

/// Whether `location` is, by itself, a complete declared key of `source`
/// (a single-field primary key or single-field additional key group).
fn own_single_field_key(source: &kg_core::models::EntityNode, location: &str) -> bool {
    let single = |group: &[String]| group.len() == 1 && group[0] == location;
    single(&source.primary_key_properties)
        || source
            .additional_key_properties
            .iter()
            .any(|group| single(group))
}

fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: STAGE.into(),
        message: message.into(),
    }
}

pub(crate) fn processing_version() -> String {
    kg_core::traits::stage::processing_version(
        REVISION,
        &[
            decision::SYSTEM_PROMPT_TEMPLATE,
            &answer_schema().to_string(),
            PACKET_SCHEMA,
            PROMPT_VERSION,
            SCHEMA_VERSION,
        ],
    )
}

#[async_trait]
impl Stage for ReferenceResolutionStage {
    fn processing_version(&self) -> String {
        processing_version()
    }

    fn capabilities(&self) -> &'static [kg_core::traits::StageCapability] {
        &[kg_core::traits::StageCapability::ReferenceResolution]
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::EdgeExtraction, StageKind::EdgeExtraction)]
    }

    fn name(&self) -> &str {
        STAGE
    }

    #[tracing::instrument(name = "reference_resolution", skip_all)]
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::EdgeExtraction(mut output) = input else {
            return Err(invalid("expected extracted references"));
        };
        for reference in output.pending_references.iter() {
            validate_pending(reference, &output, ctx)?;
        }
        if output.pending_references.is_empty() {
            kg_core::telemetry::reference_decisions(STAGE, 0, 0, 0, 0);
            return Ok(StageOutput::EdgeExtraction(output));
        }
        let backend = ctx.llm_disambiguation.as_ref();
        if !backend.is_configured() {
            return Err(invalid("no language model is configured for this run"));
        }
        if !backend.supports_bounded_attempts() {
            return Err(invalid(
                "the configured language model backend does not declare bounded provider attempts",
            ));
        }
        let settings = &ctx.reference_resolution_settings;
        // Initial evidence loading is ordinary Rust preparation under one deadline.
        let hydration_deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(settings.timeout_ms);
        let hydrated = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(StageError::Cancelled { stage: STAGE.into() }),
            result = tokio::time::timeout_at(hydration_deadline, hydrate_candidates(ctx, STAGE, &output.pending_references)) => {
                result.unwrap_or_else(|_| Err(StageError::StepFailed {
                    stage: STAGE.into(),
                    step: "candidate_hydration".into(),
                    cause: "candidate hydration deadline exceeded".into(),
                    retriable: true,
                }))
            }
        }?;
        let schema = answer_schema();
        let prompt = system_prompt(settings);
        let descriptor = backend.processing_descriptor();
        let version = processing_version();
        let prompt_budget = settings
            .max_prompt_bytes
            .min(ctx.matching_settings.max_prompt_bytes);

        // Pass 1: prepare every occurrence of this snapshot (free) and bound it
        // before any attempt is spent. The runner already applied the batch caps
        // to the whole chunk with a per-occurrence allowance of `max_audit_bytes`;
        // this pass re-applies them with each occurrence's exact worst-case record.
        struct Planned<'a> {
            reference: &'a PendingReference,
            guidance: Option<String>,
            prepared: Box<Prepared>,
            refusal: Option<DecisionReason>,
            id: uuid::Uuid,
            worst_case_bytes: usize,
        }
        let mut planned = Vec::with_capacity(output.pending_references.len());
        for reference in output.pending_references.iter() {
            let snapshot = output
                .snapshot_nodes
                .iter()
                .find(|snapshot| Some(snapshot.uuid) == reference.source.last_seen_snapshot_id)
                .ok_or_else(|| invalid("pending reference has no producing snapshot"))?;
            let guidance = ctx
                .extraction_settings
                .for_source(&reference.source.source)
                .relationship_instructions;
            let mut snapshot = snapshot.clone();
            if snapshot.content.is_none()
                && EvidenceOrigin::of_extractor(&reference.source.extracted_by)
                    == EvidenceOrigin::TextExtracted
            {
                snapshot.content = original_content(ctx, &snapshot).await?;
            }
            let preparation = prepare(
                &Inputs {
                    reference,
                    snapshot: &snapshot,
                    hydrated: &hydrated,
                    settings,
                    guidance: guidance.as_deref(),
                    model_id: backend.model_id(),
                    provider_descriptor: &descriptor,
                    processing_version: &version,
                    prompt_version: PROMPT_VERSION,
                    schema_version: SCHEMA_VERSION,
                    prompt_budget_bytes: prompt_budget,
                    context_window: backend.context_window(),
                    answer_schema: &schema,
                    system_prompt: &prompt,
                },
                STAGE,
            )?;
            let (prepared, refusal) = match preparation {
                Preparation::Ready(prepared) => (prepared, None),
                Preparation::Refused { reason, prepared } => (prepared, Some(reason)),
            };
            tracing::debug!(
                location = %reference.intent.location,
                candidates = prepared.candidates.len(),
                withheld = prepared.omitted.len(),
                refusal = ?refusal,
                "reference evidence prepared"
            );
            let id = decision_id(
                ctx.org_id.as_ref(),
                reference.source.chain_id,
                prepared.snapshot_id,
                prepared.captured_at,
                &reference.intent.slot,
                &reference.intent.location,
                &reference.value,
                &prepared.fingerprint,
            );
            // The audit must be recordable before any attempt is spent: a
            // worst-case accepted record (longest fact, most citations) that
            // does not fit the audit budget is refused with zero calls.
            let worst_case_bytes = Draft {
                reference,
                prepared: &prepared,
                id,
                version: &version,
                model: backend.model_id(),
            }
            .worst_case_bytes(settings);
            let refusal = refusal.or_else(|| {
                (worst_case_bytes > settings.max_audit_bytes)
                    .then_some(DecisionReason::EvidenceBudgetExceeded)
            });
            // The source's own single-field identity key is its identifier, not a
            // pointer at whichever other entity carries the same value: the
            // classic bucket-name/role-name coincidence. Composite keys and
            // non-key fields are untouched, so join and membership records still
            // reference the entities their key parts name.
            let refusal = refusal.or_else(|| {
                own_single_field_key(&reference.source, &reference.intent.location)
                    .then_some(DecisionReason::OwnIdentityValue)
            });
            planned.push(Planned {
                reference,
                guidance,
                prepared,
                refusal,
                id,
                worst_case_bytes,
            });
        }
        {
            use kg_core::runtime::reference_resolution::{
                MAX_DECISION_AUDITS_PER_BATCH, MAX_DECISION_AUDIT_BYTES_PER_BATCH,
            };
            let carried = &output.reference_report.decisions;
            let count = planned.len() + carried.len();
            let bytes = planned
                .iter()
                .map(|p| p.worst_case_bytes.min(settings.max_audit_bytes))
                .chain(
                    carried
                        .iter()
                        .map(|d| serde_json::to_vec(d).map_or(usize::MAX, |b| b.len())),
                )
                .fold(0usize, usize::saturating_add);
            if count > MAX_DECISION_AUDITS_PER_BATCH || bytes > MAX_DECISION_AUDIT_BYTES_PER_BATCH {
                outcome_metric(
                    ReferenceResolutionOutcome::EvidenceBudgetRefused,
                    planned.len(),
                );
                return Err(invalid(&format!(
                    "this chunk carries {count} reference decisions with a worst-case audit size of {bytes} bytes; a relationship batch commits at most {MAX_DECISION_AUDITS_PER_BATCH} audits and {MAX_DECISION_AUDIT_BYTES_PER_BATCH} bytes, so no provider attempt was made (reduce chunk_size or reference_max_values)"
                )));
            }
        }

        let reuse_keys: Vec<String> = planned
            .iter()
            .filter(|p| p.refusal.is_none())
            .map(|p| p.prepared.reuse_fingerprint.clone())
            .collect();
        let persisted = persisted_decisions(ctx, &reuse_keys).await?;

        let mut resolved = Vec::new();
        let mut confirmed_tokens = Vec::new();
        let mut unconfirmed_tokens = HashSet::new();
        let mut decisions = Vec::new();
        let mut contexts = Vec::new();
        let mut counts = Counts::default();
        // Pass 2: one bounded decision per occurrence.
        for Planned {
            reference,
            guidance,
            prepared,
            refusal,
            id,
            ..
        } in planned
        {
            let prepared = &prepared;
            let audit = match refusal {
                Some(reason) => {
                    outcome_metric(
                        match reason {
                            DecisionReason::EvidenceBudgetExceeded => {
                                ReferenceResolutionOutcome::EvidenceBudgetRefused
                            }
                            DecisionReason::OwnIdentityValue => {
                                ReferenceResolutionOutcome::OwnIdentityRefused
                            }
                            _ => ReferenceResolutionOutcome::ContentUnavailable,
                        },
                        1,
                    );
                    let audit = Draft {
                        reference,
                        prepared,
                        id,
                        version: &version,
                        model: backend.model_id(),
                    }
                    .host_refusal(reason);
                    audit
                        .validate(settings.max_audit_bytes)
                        .map_err(|error| invalid(&error.to_string()))?;
                    audit
                }
                None => {
                    let packet_schema = decision::packet_answer_schema(&schema, prepared);
                    decide(
                        ctx,
                        reference,
                        prepared,
                        &Rendering {
                            system_prompt: &prompt,
                            guidance: guidance.as_deref(),
                            schema: &packet_schema,
                        },
                        Draft {
                            reference,
                            prepared,
                            id,
                            version: &version,
                            model: backend.model_id(),
                        },
                        persisted.get(&prepared.reuse_fingerprint),
                    )
                    .await?
                }
            };
            if !audit.reused && !audit.reason.is_host_refusal() {
                contexts.push(kg_core::runtime::reference_resolution::DecisionContext {
                    decision_id: audit.decision_id,
                    producer_source: reference.source.source.clone(),
                    observing_namespace: reference.source.namespace.clone(),
                    source_entity_type: reference.source.entity_type.clone(),
                    target_type: audit.target_chain_id.and_then(|t| prepared.target_type(t)),
                    components: reference.components.clone(),
                    reference_tokens: reference
                        .components
                        .iter()
                        .map(|(_, t)| t.clone())
                        .collect(),
                    candidates: prepared.candidate_details.clone(),
                });
            }
            let occurrence = (
                reference.source.chain_id,
                reference.intent.slot.clone(),
                prepared.captured_at,
                prepared.snapshot_id,
                reference.components.last().map(|(_, token)| token.clone()),
            );
            match audit.outcome {
                DecisionOutcome::Accepted => {
                    let target_chain = audit
                        .target_chain_id
                        .ok_or_else(|| invalid("accepted decision without a target"))?;
                    let target = reference
                        .candidates
                        .iter()
                        .find(|candidate| candidate.target.chain_id == target_chain)
                        .ok_or_else(|| invalid("accepted target left the frozen candidate set"))?;
                    let mut intent = reference.intent.clone();
                    intent.target_key_group = target.matched_key_group.clone();
                    if intent.policy_fingerprint == "generic-reference-v1" {
                        intent.relationship_name = "RELATES_TO".into();
                    }
                    let mut edge = super::reference_extraction::build_reference_edge(
                        &reference.source,
                        &target.target,
                        &target.target.entity_type,
                        &reference.value,
                        &intent,
                        true,
                    )?;
                    if let Some(evidence) = edge.reference_evidence.as_mut() {
                        evidence.read_set = prepared.read_set.clone();
                        evidence.decision = Some(audit.clone());
                    }
                    counts.accepted += 1;
                    confirmed_tokens.push(occurrence);
                    resolved.push(edge);
                }
                DecisionOutcome::Rejected => {
                    counts.rejected += 1;
                    unconfirmed_tokens.insert(occurrence);
                }
                DecisionOutcome::Unsure => {
                    counts.unsure += 1;
                    unconfirmed_tokens.insert(occurrence);
                }
            }
            decisions.push(audit);
        }
        // Array occurrences may share one durable slot/token. One confirmation
        // cannot erase an abstaining sibling's evidence in that same capture.
        confirmed_tokens.retain(|decision| !unconfirmed_tokens.contains(decision));
        tracing::debug!(
            references = output.pending_references.len(),
            accepted = counts.accepted,
            rejected = counts.rejected,
            unsure = counts.unsure,
            "reference decisions complete"
        );
        // References this stage confirms move from unresolved to confirmed in the
        // receipt counts; rejections and uncertainty stay counted as unresolved.
        let newly_confirmed = resolved.len();
        output.reference_report.confirmed += newly_confirmed;
        output.reference_report.unresolved = output
            .reference_report
            .unresolved
            .saturating_sub(newly_confirmed);
        kg_core::runtime::reference_resolution::merge_audits(
            &mut output.reference_report.decisions,
            &decisions,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        for context in contexts {
            if !output
                .reference_report
                .decision_contexts
                .iter()
                .any(|known| known.decision_id == context.decision_id)
            {
                output.reference_report.decision_contexts.push(context);
            }
        }
        for (source_chain_id, slot, decided_at, decision_id, tokens) in confirmed_tokens {
            for decisions in [
                &mut output.reference_report.unresolved_slots,
                &mut output.reference_report.retirement_decisions,
            ] {
                if let Some(token) = &tokens {
                    clear_confirmed_occurrence(
                        decisions,
                        source_chain_id,
                        &slot,
                        decided_at,
                        decision_id,
                        token,
                    );
                }
            }
        }
        outcome_metric(ReferenceResolutionOutcome::Accepted, counts.accepted);
        outcome_metric(ReferenceResolutionOutcome::Rejected, counts.rejected);
        outcome_metric(ReferenceResolutionOutcome::Unsure, counts.unsure);
        // These counters describe only this stage's own contribution: the
        // references it newly confirmed, so extraction and resolution never
        // double-count a reference across the two stages.
        kg_core::telemetry::reference_decisions(STAGE, newly_confirmed, 0, 0, 0);
        Arc::make_mut(&mut output.edges).extend(resolved);
        output.pending_references = Default::default();
        Ok(StageOutput::EdgeExtraction(output))
    }
}

#[derive(Default)]
struct Counts {
    accepted: usize,
    rejected: usize,
    unsure: usize,
}

/// Structural checks every pending occurrence must pass before any evidence is read.
fn validate_pending(
    reference: &PendingReference,
    output: &kg_core::runtime::stage_output::EdgeExtractionOutput,
    ctx: &RuntimeContext,
) -> Result<(), StageError> {
    if ctx
        .policy
        .for_source(&reference.source.source)
        .edge_ambiguity
        != kg_core::policy::EdgeAmbiguityMode::Llm
    {
        return Err(invalid(
            "pending reference is not permitted by source policy",
        ));
    }
    if !output.snapshot_nodes.iter().any(|snapshot| {
        Some(snapshot.uuid) == reference.source.last_seen_snapshot_id
            && snapshot.org_id == reference.source.org_id
    }) {
        return Err(invalid("pending reference has no producing snapshot"));
    }
    if reference.source.org_id != ctx.org_id.as_ref()
        || reference.source.chain_id.is_nil()
        || reference.candidates.is_empty()
        || reference.intent.location.trim().is_empty()
        || reference.intent.slot.trim().is_empty()
        || reference.intent.relationship_name.trim().is_empty()
        || reference.value.trim().is_empty()
    {
        return Err(invalid("invalid pending reference"));
    }
    let mut chains = HashSet::new();
    for candidate in &reference.candidates {
        if candidate.target.chain_id.is_nil()
            || candidate.target.chain_id == reference.source.chain_id
            || !chains.insert(candidate.target.chain_id)
            || !ctx
                .namespace_policy
                .allows(&reference.source.namespace, &candidate.target.namespace)
        {
            return Err(invalid("invalid pending reference target"));
        }
    }
    if !reference.intent.lookup_complete
        || reference.intent.observing_chain_id != reference.source.chain_id
        || reference.intent.observing_namespace != reference.source.namespace
        || reference.intent.observing_entity_type != reference.source.entity_type
        || reference.intent.producer_source != reference.source.source
        || reference.intent.components != reference.components
        || reference.candidates.iter().any(|candidate| {
            (!reference.intent.target_type.is_empty()
                && candidate.target.entity_type != reference.intent.target_type)
                || reference
                    .intent
                    .allowed_namespaces
                    .as_ref()
                    .is_some_and(|namespaces| !namespaces.contains(&candidate.target.namespace))
                || (!reference.intent.target_key_group.is_empty()
                    && candidate.matched_key_group != reference.intent.target_key_group)
        })
    {
        return Err(invalid(
            "reference intent disagrees with candidate evidence",
        ));
    }
    Ok(())
}

/// The original content of a text-derived observation whose in-memory snapshot
/// carries none (reverse repair reconstructs snapshots without content). Read
/// only by the authorized snapshot identity within its namespace and capture
/// time. Unavailable content is `None` (explicit uncertainty follows); a
/// transport failure stays a typed operational failure.
async fn original_content(
    ctx: &RuntimeContext,
    snapshot: &kg_core::models::SnapshotNode,
) -> Result<Option<String>, StageError> {
    let request = kg_core::runtime::history::SnapshotEvidenceRequest {
        namespace: snapshot.namespace.clone(),
        ids: vec![snapshot.uuid],
        captured_before: snapshot.captured_at,
        // The storage cap, not the prompt budget: retained text that is too
        // large for the prompt is classified by `prepare` as a budget refusal,
        // never mislabelled as unavailable.
        max_bytes: 16 * 1024 * 1024,
    };
    match ctx
        .graph
        .snapshot_evidence(ctx.org_id.as_ref(), &request)
        .await
    {
        Ok(records) => Ok(records
            .into_iter()
            .find(|record| record.uuid == snapshot.uuid)
            .map(|record| record.content)),
        // Storage answered: nothing retained for this exact snapshot.
        Err(kg_core::errors::BackendError::Query(message))
            if message == kg_core::runtime::history::EVIDENCE_UNAVAILABLE =>
        {
            Ok(None)
        }
        // Every other failure (permission, unsupported read, transport) is an
        // operational failure, never disguised as missing evidence.
        Err(error) => Err(StageError::StepFailed {
            stage: STAGE.into(),
            step: "source_content".into(),
            cause: "original source content read failed".into(),
            retriable: error.is_transient(),
        }),
    }
}

/// Holds the single-flight key while one attempt is in progress. If the
/// decision future is dropped or fails after the attempt was consumed, the key
/// settles as a failed attempt so identical evidence never dispatches again in
/// this run; before any consumption the key is simply released.
struct AttemptGuard<'a> {
    leader: Option<kg_core::runtime::reference_cache::LeaderGuard<'a>>,
    budget: &'a CallBudget,
}

impl AttemptGuard<'_> {
    fn settle(mut self, outcome: CachedOutcome) {
        if let Some(leader) = self.leader.take() {
            leader.settle(outcome);
        }
    }
}

impl Drop for AttemptGuard<'_> {
    fn drop(&mut self) {
        if let Some(leader) = self.leader.take() {
            if self.budget.consumed() > 0 {
                leader.settle(CachedOutcome::Failed {
                    cause: "attempt abandoned before a decision was recorded".into(),
                    attempts: self.budget.consumed(),
                });
            }
        }
    }
}

struct Rendering<'a> {
    system_prompt: &'a str,
    guidance: Option<&'a str>,
    schema: &'a serde_json::Value,
}

/// Everything an audit record needs besides the outcome.
struct Draft<'a> {
    reference: &'a PendingReference,
    prepared: &'a Prepared,
    id: uuid::Uuid,
    version: &'a str,
    model: &'a str,
}

impl Draft<'_> {
    fn base(&self, outcome: DecisionOutcome, reason: DecisionReason) -> ReferenceDecisionAudit {
        ReferenceDecisionAudit {
            decision_id: self.id,
            source_chain_id: self.reference.source.chain_id,
            source_version_uuid: self.prepared.source_version_uuid,
            source_snapshot_id: self.prepared.snapshot_id,
            source_captured_at: self.prepared.captured_at,
            slot: self.reference.intent.slot.clone(),
            location: self.reference.intent.location.clone(),
            value: self.reference.value.clone(),
            evidence_origin: self.prepared.origin,
            outcome,
            reason,
            target_chain_id: None,
            fact: None,
            supporting_evidence: Vec::new(),
            candidate_read_set: self.prepared.read_set.clone(),
            evidence_fingerprint: self.prepared.fingerprint.clone(),
            evidence_complete: true,
            model_configured: self.model.to_owned(),
            model_served: None,
            provider_attempts: 0,
            input_tokens: None,
            output_tokens: None,
            processing_version: self.version.to_owned(),
            decided_at: Utc::now(),
            reused: false,
            reused_from: None,
            reuse_fingerprint: self.prepared.reuse_fingerprint.clone(),
            cited_value_hashes: Vec::new(),
        }
    }

    fn host_refusal(&self, reason: DecisionReason) -> ReferenceDecisionAudit {
        self.base(DecisionOutcome::Unsure, reason)
    }

    /// The serialized size of the largest accepted record this packet can yield.
    fn worst_case_bytes(
        &self,
        settings: &kg_core::runtime::reference_resolution::ReferenceResolutionSettings,
    ) -> usize {
        let mut audit = self.base(DecisionOutcome::Accepted, DecisionReason::ModelAccepted);
        audit.target_chain_id = self.prepared.candidates.first().copied();
        // Four bytes per character is the largest a validated fact can serialize to.
        audit.fact = Some("\u{1F600}".repeat(settings.max_fact_chars));
        audit.model_served = Some("m".repeat(256));
        audit.provider_attempts = 1;
        audit.input_tokens = Some(u32::MAX);
        audit.output_tokens = Some(u32::MAX);
        let mut longest: Vec<_> = self
            .prepared
            .manifest
            .iter()
            .map(|m| m.citation())
            .collect();
        longest.sort_by_key(|c| std::cmp::Reverse(serde_json::json!(c).to_string().len()));
        audit.supporting_evidence = longest
            .into_iter()
            .take(settings.max_supporting_items)
            .map(|mut citation| {
                citation.id = "i".repeat(32);
                citation
            })
            .collect();
        audit.reused_from = Some(uuid::Uuid::from_u128(1));
        // Every citation plus the occurrence and its enclosing element.
        audit.cited_value_hashes = audit
            .supporting_evidence
            .iter()
            .map(|c| (c.owner_chain_id, c.path.clone().unwrap_or_default()))
            .chain([
                (
                    self.reference.source.chain_id,
                    evidence::OCCURRENCE_PATH.to_owned(),
                ),
                (
                    self.reference.source.chain_id,
                    evidence::ENCLOSING_PATH.to_owned(),
                ),
            ])
            .map(
                |(owner_chain_id, path)| kg_core::runtime::reference_resolution::CitedValueHash {
                    owner_chain_id,
                    path,
                    sha256: "f".repeat(64),
                },
            )
            .collect();
        serde_json::to_vec(&audit).map_or(usize::MAX, |bytes| bytes.len())
    }

    fn model_decision(
        &self,
        answer: Answer,
        response: &kg_core::traits::llm_backend::LlmResponse,
        attempts: u32,
    ) -> ReferenceDecisionAudit {
        let mut audit = match &answer {
            Answer::Accept { .. } => {
                self.base(DecisionOutcome::Accepted, DecisionReason::ModelAccepted)
            }
            Answer::Reject => self.base(DecisionOutcome::Rejected, DecisionReason::ModelRejected),
            Answer::Unsure => self.base(DecisionOutcome::Unsure, DecisionReason::ModelUnsure),
        };
        // Every model verdict rests at least on the occurrence and its enclosing
        // element (the tag key of a tag value, the record of a field); an
        // acceptance also on what it cited. Those values are hashed so a later
        // reuse can prove they are unchanged. Paths are symbolic for the
        // occurrence (see `Prepared::symbolic_path`), deduplicated by owner and path.
        let source = self.reference.source.chain_id;
        let mut hashes: Vec<kg_core::runtime::reference_resolution::CitedValueHash> =
            [evidence::OCCURRENCE_PATH, evidence::ENCLOSING_PATH]
                .iter()
                .filter_map(|path| self.prepared.cited_hash(source, path))
                .collect();
        if let Answer::Accept {
            target,
            fact,
            citations,
        } = answer
        {
            audit.target_chain_id = Some(target);
            audit.fact = Some(fact);
            for citation in citations.iter().filter(|c| {
                c.kind == kg_core::runtime::reference_resolution::EvidenceKind::StructuredProperty
            }) {
                let Some(hash) = citation
                    .path
                    .as_deref()
                    .and_then(|path| self.prepared.cited_hash(citation.owner_chain_id, path))
                else {
                    continue;
                };
                if !hashes
                    .iter()
                    .any(|h| h.owner_chain_id == hash.owner_chain_id && h.path == hash.path)
                {
                    hashes.push(hash);
                }
            }
            audit.supporting_evidence = citations;
        }
        audit.cited_value_hashes = hashes;
        audit.model_served = Some(response.model.clone());
        audit.provider_attempts = attempts;
        audit.input_tokens = response.input_tokens;
        audit.output_tokens = response.output_tokens;
        audit
    }
}

/// One decision for one prepared packet: reuse an identical decision of this
/// run, wait for a concurrent identical one, or lead with exactly one provider
/// attempt. Every consumed attempt is remembered, so a later identical packet
/// in this run never dispatches again whatever the first outcome was.
async fn decide(
    ctx: &RuntimeContext,
    reference: &PendingReference,
    prepared: &Prepared,
    rendering: &Rendering<'_>,
    draft: Draft<'_>,
    persisted: Option<&kg_core::runtime::reference_resolution::PersistedDecision>,
) -> Result<ReferenceDecisionAudit, StageError> {
    let settings = &ctx.reference_resolution_settings;
    let key = ReferenceDecisionKey::new(ctx.org_id.as_ref(), &prepared.fingerprint);
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(settings.timeout_ms);
    loop {
        match ctx.reference_decisions.claim(key) {
            Claim::Settled(outcome) => return reuse(&outcome, prepared, &draft, settings),
            Claim::Wait(waiter) => {
                let shared = tokio::select! {
                    biased;
                    _ = ctx.cancel.cancelled() => return Err(StageError::Cancelled { stage: STAGE.into() }),
                    _ = tokio::time::sleep_until(deadline) => return Err(StageError::ModelCall {
                        stage: STAGE.into(),
                        kind: kg_core::errors::stage::ModelFailureKind::Timeout,
                    }),
                    outcome = waiter.outcome() => outcome,
                };
                match shared {
                    Some(outcome) => return reuse(&outcome, prepared, &draft, settings),
                    None => continue,
                }
            }
            Claim::Leader(leader) => {
                let budget = CallBudget::single();
                let leader = AttemptGuard {
                    leader: Some(leader),
                    budget: &budget,
                };
                // An earlier decision with the same reuse key, still naming an
                // offered target and with every cited value unchanged, is rebound
                // without any provider attempt: one decided earlier in this run
                // (another snapshot of the same source, a different packet) or
                // one persisted by an earlier run.
                let reuse_key =
                    ReferenceDecisionKey::new(ctx.org_id.as_ref(), &prepared.reuse_fingerprint);
                let earlier = ctx
                    .reference_decisions
                    .original(&reuse_key)
                    .map(|audit| (ReferenceResolutionOutcome::CacheHit, audit))
                    .or_else(|| {
                        persisted.map(|hit| {
                            (
                                ReferenceResolutionOutcome::PersistedHit,
                                Arc::new(hit.audit.clone()),
                            )
                        })
                    })
                    .filter(|(_, audit)| reusable(audit, prepared));
                if let Some((metric, original)) = earlier {
                    let rebound = original.rebound(
                        draft.id,
                        prepared.source_version_uuid,
                        prepared.snapshot_id,
                        prepared.captured_at,
                        prepared.location.clone(),
                        Utc::now(),
                        prepared.read_set.clone(),
                        prepared.fingerprint.clone(),
                    );
                    match rebound.validate(settings.max_audit_bytes) {
                        Ok(()) => {
                            outcome_metric(metric, 1);
                            leader.settle(CachedOutcome::Decided(Box::new(rebound.clone())));
                            return Ok(rebound);
                        }
                        Err(error) => {
                            // A record this run may not keep (a lowered audit
                            // budget) is decided again, never a failure.
                            tracing::warn!(
                                decision = %original.decision_id,
                                error = %error,
                                "an earlier reference decision cannot be rebound under the current audit budget; deciding again"
                            );
                        }
                    }
                }
                outcome_metric(ReferenceResolutionOutcome::Attempted, 1);
                let messages = render(prepared, rendering.system_prompt, rendering.guidance);
                let result = crate::node::extraction_support::call_provider_bounded(
                    ctx,
                    STAGE,
                    &messages,
                    rendering.schema,
                    ctx.llm_disambiguation.as_ref(),
                    &ctx.llm_disambiguation_semaphore,
                    deadline,
                    settings.max_output_tokens,
                    &budget,
                )
                .await;
                let response = match result {
                    Ok(response) => response,
                    Err(error) => {
                        if budget.consumed() > 0 {
                            // The attempt happened; never pay for it twice in this run.
                            leader.settle(CachedOutcome::Failed {
                                cause: error.to_string(),
                                attempts: budget.consumed(),
                            });
                        }
                        return Err(error);
                    }
                };
                match parse_answer(
                    &response.content,
                    prepared,
                    reference.source.chain_id,
                    settings,
                ) {
                    Ok(answer) => {
                        let audit = draft.model_decision(answer, &response, budget.consumed());
                        if let Err(error) = audit.validate(settings.max_audit_bytes) {
                            // The attempt was spent; a record that cannot be kept is a failure, never a retry.
                            leader.settle(CachedOutcome::Failed {
                                cause: format!("unrecordable decision: {error}"),
                                attempts: budget.consumed(),
                            });
                            return Err(invalid(&error.to_string()));
                        }
                        if audit.outcome != DecisionOutcome::Unsure {
                            ctx.reference_decisions.note_original(reuse_key, &audit);
                        }
                        leader.settle(CachedOutcome::Decided(Box::new(audit.clone())));
                        return Ok(audit);
                    }
                    Err(error) => {
                        outcome_metric(ReferenceResolutionOutcome::InvalidResponse, 1);
                        leader.settle(CachedOutcome::Failed {
                            cause: format!("invalid decision: {error}"),
                            attempts: budget.consumed(),
                        });
                        return Err(crate::node::extraction_support::output_error(STAGE, error));
                    }
                }
            }
        }
    }
}

/// Whether an earlier decision may be rebound to this occurrence: only original
/// model accept/reject verdicts under the same reuse key, the target still among
/// the frozen candidates, and every cited structured value unchanged in the
/// current packet.
fn reusable(audit: &ReferenceDecisionAudit, prepared: &Prepared) -> bool {
    if audit.reused
        || audit.reason.is_host_refusal()
        || audit.outcome == DecisionOutcome::Unsure
        || audit.reuse_fingerprint != prepared.reuse_fingerprint
    {
        return false;
    }
    if audit
        .target_chain_id
        .is_some_and(|target| !prepared.candidates.contains(&target))
    {
        return false;
    }
    audit.cited_value_hashes.iter().all(|cited| {
        prepared
            .current_value_hash(cited.owner_chain_id, &cited.path)
            .is_some_and(|now| now == cited.sha256)
    })
}

/// One bounded read of persisted decisions for every occurrence of this
/// snapshot that may dispatch. A backend without the store means no reuse;
/// any other failure is a typed operational failure before any attempt.
async fn persisted_decisions(
    ctx: &RuntimeContext,
    keys: &[String],
) -> Result<
    std::collections::HashMap<String, kg_core::runtime::reference_resolution::PersistedDecision>,
    StageError,
> {
    use kg_core::traits::graph_reads::MAX_LOOKUP_KEYS;
    let mut found = std::collections::HashMap::new();
    if !ctx.reference_resolution_settings.persisted_reuse || keys.is_empty() {
        return Ok(found);
    }
    let mut unique: Vec<String> = keys.to_vec();
    unique.sort();
    unique.dedup();
    for page in unique.chunks(MAX_LOOKUP_KEYS) {
        match ctx
            .graph
            .reference_decisions_by_reuse_key(ctx.org_id.as_ref(), page)
            .await
        {
            Ok(rows) => {
                for row in rows {
                    found.insert(row.audit.reuse_fingerprint.clone(), row);
                }
            }
            Err(kg_core::errors::BackendError::NotConfigured(_)) => {
                outcome_metric(ReferenceResolutionOutcome::PersistedUnavailable, page.len());
                return Ok(std::collections::HashMap::new());
            }
            Err(error) => {
                return Err(StageError::StepFailed {
                    stage: STAGE.into(),
                    step: "persisted_decisions".into(),
                    cause: "persisted reference decision read failed".into(),
                    retriable: error.is_transient(),
                })
            }
        }
    }
    Ok(found)
}

/// An identical packet was already decided in this run. A model decision is
/// rebound to the current observation (same evidence, same fingerprint, new
/// capture provenance); a consumed failed attempt is re-raised without dispatch.
fn reuse(
    outcome: &CachedOutcome,
    prepared: &Prepared,
    draft: &Draft<'_>,
    settings: &kg_core::runtime::reference_resolution::ReferenceResolutionSettings,
) -> Result<ReferenceDecisionAudit, StageError> {
    match outcome {
        CachedOutcome::Decided(audit) => {
            if audit
                .target_chain_id
                .is_some_and(|target| !prepared.candidates.contains(&target))
            {
                return Err(invalid(
                    "cached decision names a target outside the frozen candidate set",
                ));
            }
            outcome_metric(ReferenceResolutionOutcome::CacheHit, 1);
            let rebound = audit.rebound(
                draft.id,
                prepared.source_version_uuid,
                prepared.snapshot_id,
                prepared.captured_at,
                prepared.location.clone(),
                Utc::now(),
                prepared.read_set.clone(),
                prepared.fingerprint.clone(),
            );
            rebound
                .validate(settings.max_audit_bytes)
                .map_err(|error| invalid(&error.to_string()))?;
            Ok(rebound)
        }
        CachedOutcome::Failed { cause, attempts } => {
            outcome_metric(ReferenceResolutionOutcome::AttemptBudgetExhausted, 1);
            Err(StageError::StepFailed {
                stage: STAGE.into(),
                step: "decision".into(),
                cause: format!(
                    "the provider attempt for this evidence was already spent in this run ({attempts} attempt(s)): {cause}"
                ),
                retriable: false,
            })
        }
    }
}

fn clear_confirmed_occurrence(
    decisions: &mut Vec<kg_core::runtime::stage_output::UnresolvedSlot>,
    source: uuid::Uuid,
    slot: &str,
    at: chrono::DateTime<chrono::Utc>,
    snapshot: uuid::Uuid,
    token: &str,
) {
    let mut has_slot = false;
    for decision in decisions.iter_mut() {
        if decision.source_chain_id != source || decision.slot != slot {
            continue;
        }
        has_slot = true;
        // Another capture's uncertainty cannot be resolved by this observation.
        if (decision.decided_at, decision.decision_id) == (at, snapshot) {
            decision.entries.retain(|entry| {
                entry.token != token
                    || !super::reference_extraction::model_resolvable_reason(&entry.reason)
            });
        }
    }
    if !has_slot {
        decisions.push(kg_core::runtime::stage_output::UnresolvedSlot {
            source_chain_id: source,
            slot: slot.into(),
            decided_at: at,
            decision_id: snapshot,
            entries: Vec::new(),
        });
    }
}

/// The four-field answer contract and host validation.
mod decision;
/// Complete evidence packets, hydration guards and fingerprints.
mod evidence;

#[cfg(test)]
mod tests;
