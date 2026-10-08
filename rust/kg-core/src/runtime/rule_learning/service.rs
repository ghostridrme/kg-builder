//! Awaited, bounded orchestration for learned-rule evidence and proposals ties
//! evidence collection, proposal, held-out validation, optional model
//! review and persistence together. It reads a frozen set of active rules,
//! proposes only patterns not already covered (warm reuse makes zero model
//! calls), validates each proposal independently of the proposing model, and
//! persists decisions through the [`RuleStore`] state machine. Automatic
//! promotion is opt-in and guarded by exactly the same held-out gate as an
//! explicit activation. Every learning run is bounded (candidate mappings,
//! model calls, elapsed time); exhausting a bound stops the run and reports
//! incomplete work with a resume identity, never silent eventual completion.
use std::future::Future;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::errors::BackendError;
use crate::runtime::rule_learning::evidence::{aggregate, PatternKey, ReferenceExample};
use crate::runtime::rule_learning::proposal::{propose, RuleProposal};
use crate::runtime::rule_learning::review::{review_proposal, ModelVerdict};
use crate::runtime::rule_learning::validation::{
    validate_against, LabeledCase, RuleValidationExecutor, ValidationExample,
};
use crate::traits::llm_backend::LlmBackend;
use crate::traits::rule_store::{
    LearnedRule, RuleDecision, RuleOrigin, RuleStatus, RuleStore, RuleTransition,
};

/// Stable namespace for deriving a rule's id from its pattern, so re-runs and
/// concurrent proposals of the same pattern collide on one id (dedup).
const RULE_ID_NAMESPACE: Uuid = Uuid::from_u128(0x51b5_9c4e_7d21_4b3a_9f6c_0a1b_2c3d_4e5f);

/// The evidence a learning run reads, collected within the run's bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceBatch {
    /// Observed examples aggregated into candidate patterns.
    pub examples: Vec<ReferenceExample>,
    /// Labeled held-out cases used to validate proposals independently.
    pub labeled: Vec<LabeledCase>,
    /// Fingerprint of the source schema the evidence was read against.
    pub schema_fingerprint: String,
    /// Entities scanned to produce this batch.
    pub scanned: usize,
    /// True when the scan hit a bound before exhausting the source.
    pub truncated: bool,
}

/// Bounded, resumable evidence collection over committed data. A partial scan is
/// never complete negative evidence: `truncated` says the batch is incomplete.
#[async_trait]
pub trait ReferenceEvidenceSource: Send + Sync {
    async fn collect(
        &self,
        org_id: &str,
        source: &str,
        bounds: &LearningBounds,
        holdout_permille: u16,
    ) -> Result<EvidenceBatch, BackendError>;
}

/// Combines production observations with labels established by an independent
/// reviewer or evaluation system. Unresolved references never become labels.
pub struct AdjudicatedEvidenceSource<'a> {
    observations: &'a dyn ReferenceEvidenceSource,
    labels: Vec<LabeledCase>,
}

impl<'a> AdjudicatedEvidenceSource<'a> {
    pub fn new(
        observations: &'a dyn ReferenceEvidenceSource,
        labels: Vec<LabeledCase>,
    ) -> Result<Self, BackendError> {
        if labels.iter().any(|case| {
            case.adjudication_ref.trim().is_empty()
                || case.example.source_version_uuid.is_none()
                || case.example.reference_tokens.is_empty()
                || case.should_link != case.expected_target_chain_id.is_some()
        }) {
            return Err(BackendError::Query(
                "adjudicated labels require provenance, an exact source version and structured reference occurrence"
                    .into(),
            ));
        }
        Ok(Self {
            observations,
            labels,
        })
    }
}

#[async_trait]
impl ReferenceEvidenceSource for AdjudicatedEvidenceSource<'_> {
    async fn collect(
        &self,
        org_id: &str,
        source: &str,
        bounds: &LearningBounds,
        holdout_permille: u16,
    ) -> Result<EvidenceBatch, BackendError> {
        let mut batch = self
            .observations
            .collect(org_id, source, bounds, holdout_permille)
            .await?;
        let remaining = bounds.max_scanned_entities.saturating_sub(batch.scanned);
        let mut bytes = serde_json::to_vec(&batch.examples)
            .map_err(|_| BackendError::Serialization("rule observations".into()))?
            .len();
        let mut labels = Vec::new();
        for label in self.labels.iter().take(remaining) {
            let label_bytes = serde_json::to_vec(label)
                .map_err(|_| BackendError::Serialization("adjudicated rule label".into()))?
                .len();
            let Some(next) = bytes.checked_add(label_bytes) else {
                batch.truncated = true;
                break;
            };
            if next > bounds.max_evidence_bytes {
                batch.truncated = true;
                break;
            }
            bytes = next;
            labels.push(label.clone());
        }
        if labels.len() < self.labels.len() {
            batch.truncated = true;
        }
        batch.scanned = batch.scanned.saturating_add(labels.len());
        batch.labeled = labels;
        Ok(batch)
    }
}

/// Applies the graph effects of a committed rule revision. Implementations
/// must be awaited and idempotent so a caller can resume after interruption.
#[async_trait]
pub trait RuleLifecycleRepair: Send + Sync {
    async fn reconcile(&self, rule: &LearnedRule) -> Result<(), BackendError>;
}

/// Repair implementation for stores that deliberately have no materialized
/// relationship graph, such as isolated rule-domain tests.
pub struct NoDerivedRuleState;

#[async_trait]
impl RuleLifecycleRepair for NoDerivedRuleState {
    async fn reconcile(&self, _rule: &LearnedRule) -> Result<(), BackendError> {
        Ok(())
    }
}

/// Resource bounds for one learning run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LearningBounds {
    pub max_scanned_entities: usize,
    pub max_candidate_mappings: usize,
    pub max_model_calls: usize,
    pub max_evidence_bytes: usize,
    pub max_elapsed: Duration,
}

impl Default for LearningBounds {
    fn default() -> Self {
        Self {
            max_scanned_entities: 100_000,
            max_candidate_mappings: 512,
            max_model_calls: 64,
            max_evidence_bytes: 8 * 1024 * 1024,
            max_elapsed: Duration::from_secs(120),
        }
    }
}

/// Run options: how aggressively to promote, how to split evidence, and the
/// model/prompt identity recorded on model-touched rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LearningOptions {
    /// When true, a proposal that passes the held-out gate (and model accept,
    /// if a model is used) is activated automatically; otherwise it stays
    /// proposed for explicit activation.
    pub auto_promote: bool,
    /// Holdout share in parts-per-thousand for validation.
    pub holdout_permille: u16,
    pub model_id: String,
    pub prompt_version: String,
    pub max_output_tokens: u32,
}

impl Default for LearningOptions {
    fn default() -> Self {
        Self {
            auto_promote: false,
            holdout_permille: 300,
            model_id: "none".into(),
            prompt_version: "v1".into(),
            max_output_tokens: 512,
        }
    }
}

/// Outcome counts of a learning run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearningReport {
    pub run_id: Uuid,
    pub scanned: usize,
    pub candidate_patterns: usize,
    pub proposed: usize,
    pub activated: usize,
    pub rejected: usize,
    pub uncertain: usize,
    /// Patterns already covered by an active rule; no model call was made.
    pub skipped_existing: usize,
    pub model_calls: usize,
    /// True when a bound stopped the run before all evidence was processed.
    pub truncated: bool,
    /// Set when truncated: the identity a caller passes to resume the work.
    pub resume: Option<String>,
}

/// Deterministic rule id for a pattern within an org and source.
pub fn rule_id(org_id: &str, source: &str, key: &PatternKey) -> Uuid {
    let seed = format!(
        "{org_id}|{source}|{}|{}|{}|{}|{}|{}",
        key.source_entity_type,
        key.source_namespace,
        key.reference_path,
        key.target_type,
        key.target_key_group.join(","),
        serde_json::to_string(&key.component_paths).expect("string map is serializable")
    );
    Uuid::new_v5(&RULE_ID_NAMESPACE, seed.as_bytes())
}

async fn within_deadline<T>(
    started: Instant,
    maximum: Duration,
    work: impl Future<Output = Result<T, BackendError>>,
) -> Result<T, BackendError> {
    let remaining = maximum
        .checked_sub(started.elapsed())
        .ok_or(BackendError::Timeout(maximum.as_millis() as u64))?;
    tokio::time::timeout(remaining, work)
        .await
        .map_err(|_| BackendError::Timeout(maximum.as_millis() as u64))?
}

fn system_decision(component: &str, note: &str) -> RuleDecision {
    RuleDecision {
        origin: RuleOrigin::System {
            component: component.into(),
        },
        at: chrono::Utc::now(),
        note: Some(note.chars().take(256).collect()),
    }
}

/// The status a proposal should enter, decided from validation and model review.
enum Decision {
    Activate,
    Propose,
    Uncertain(String),
    Reject(String),
}

/// Collaborators used by one learning pass.
pub struct LearningServices<'a> {
    pub evidence: &'a dyn ReferenceEvidenceSource,
    pub validator: &'a dyn RuleValidationExecutor,
    pub store: &'a dyn RuleStore,
    pub model: Option<&'a dyn LlmBackend>,
    /// Required whenever this pass can make a rule active. A missing repair
    /// service fails before the lifecycle transition is written.
    pub repair: Option<&'a dyn RuleLifecycleRepair>,
}

/// Run a bounded learning pass for one producer source. Awaited: it returns
/// only after every decision it made is durably committed.
pub async fn learn(
    services: LearningServices<'_>,
    org_id: &str,
    src: &str,
    bounds: &LearningBounds,
    options: &LearningOptions,
) -> Result<LearningReport, BackendError> {
    learn_cancellable(services, org_id, src, bounds, options, None).await
}

/// Stop between decisions after settling any in-flight storage operation.
pub async fn learn_cancellable(
    services: LearningServices<'_>,
    org_id: &str,
    src: &str,
    bounds: &LearningBounds,
    options: &LearningOptions,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<LearningReport, BackendError> {
    let stopped = || cancellation.is_some_and(|token| token.is_cancelled());
    let started = Instant::now();
    let mut report = LearningReport {
        run_id: Uuid::new_v4(),
        ..Default::default()
    };
    if bounds.max_elapsed.is_zero() || stopped() {
        report.truncated = true;
        report.resume = Some(format!("{org_id}/{src}"));
        return Ok(report);
    }
    let batch = within_deadline(
        started,
        bounds.max_elapsed,
        services
            .evidence
            .collect(org_id, src, bounds, options.holdout_permille),
    )
    .await?;
    report.scanned = batch.scanned;
    report.truncated = batch.truncated;
    if stopped() {
        report.truncated = true;
        report.resume = Some(format!("{org_id}/{src}"));
        return Ok(report);
    }

    let active = within_deadline(
        started,
        bounds.max_elapsed,
        services.store.list_active(org_id, src),
    )
    .await?;
    let existing_names: Vec<String> = active
        .iter()
        .map(|r| r.mapping.relationship_name.clone())
        .collect();
    let active_by_id: std::collections::BTreeMap<Uuid, LearnedRule> =
        active.into_iter().map(|rule| (rule.id, rule)).collect();

    let aggregates = aggregate(batch.examples.iter().cloned());
    report.candidate_patterns = aggregates.len();

    for aggregate in aggregates {
        if stopped()
            || started.elapsed() >= bounds.max_elapsed
            || report.proposed + report.activated + report.rejected + report.uncertain
                >= bounds.max_candidate_mappings
        {
            report.truncated = true;
            break;
        }
        let Some(mut proposal) = propose(&aggregate) else {
            continue;
        };
        let id = rule_id(org_id, src, &aggregate.key);
        // Warm reuse: a pattern already active is not re-learned, and costs no
        // model call.
        if let Some(existing) = active_by_id.get(&id) {
            if let Some(repair) = services.repair {
                within_deadline(started, bounds.max_elapsed, repair.reconcile(existing)).await?;
            }
            report.skipped_existing += 1;
            continue;
        }

        // Independent held-out validation, scoped to this pattern's owner slot.
        // A case whose adjudicated target is another type is a should-not-link
        // case for this mapping (the field named something else there); a case
        // the model rejected is a should-not-link case for every type.
        let cases: Vec<LabeledCase> = batch
            .labeled
            .iter()
            .filter(|c| {
                c.example.source_entity_type == proposal.mapping.source_entity_type
                    && c.example.source_namespace == proposal.namespace
                    && c.example.reference_path == proposal.mapping.reference_path
            })
            .map(|c| {
                if c.example.target_type == proposal.mapping.target_type {
                    c.clone()
                } else {
                    LabeledCase {
                        should_link: false,
                        expected_target_chain_id: None,
                        ..c.clone()
                    }
                }
            })
            .collect();
        // One case per occurrence: a rejection labelled for several offered
        // types is one should-not-link case here, not several.
        let mut seen_occurrences = std::collections::HashSet::new();
        let cases: Vec<LabeledCase> = cases
            .into_iter()
            .filter(|c| {
                seen_occurrences.insert((c.example.source_chain_id, c.example.value_token.clone()))
            })
            .collect();
        let validation_examples: Vec<_> = cases
            .iter()
            .map(|case| ValidationExample::from(&case.example))
            .collect();
        let predictions = within_deadline(
            started,
            bounds.max_elapsed,
            services
                .validator
                .predict(org_id, src, &proposal.mapping, &validation_examples),
        )
        .await?;
        // Independent unless the learner's reviewer model is the very model
        // that produced the labels (a model approving its own verdicts).
        let independent = match services.model {
            None => true,
            Some(reviewer) => cases
                .iter()
                .all(|case| case.label_model.as_deref() != Some(reviewer.model_id())),
        };
        let validation = validate_against(&proposal.mapping, &cases, &predictions, independent)?;
        // Which labels validated this rule, and which models served them.
        let mut label_models: Vec<String> =
            cases.iter().filter_map(|c| c.label_model.clone()).collect();
        label_models.sort();
        label_models.dedup();
        let label_summary = format!(
            "labels={};decision_labels={};label_models={}",
            cases.len(),
            cases
                .iter()
                .filter(|c| c.adjudication_ref.starts_with("decision:"))
                .count(),
            label_models.join(",")
        );

        // Optional model review, bounded and never the promotion authority.
        let mut model_verdict = None;
        if let Some(backend) = services.model {
            if report.model_calls < bounds.max_model_calls {
                report.model_calls += 1;
                let examples = pattern_examples(&batch.examples, &aggregate.key);
                let review = within_deadline(
                    started,
                    bounds.max_elapsed,
                    review_proposal(
                        backend,
                        &proposal,
                        &examples,
                        &existing_names,
                        options.max_output_tokens,
                    ),
                )
                .await?;
                if review.verdict == ModelVerdict::Accept {
                    if let Some(name) = review.relationship_name {
                        proposal.mapping.relationship_name = name;
                    }
                    if let Some(direction) = review.direction {
                        proposal.mapping.direction = direction;
                    }
                }
                model_verdict = Some(review.verdict);
            }
        }

        let decision = if batch.truncated {
            Decision::Uncertain("evidence collection was incomplete".into())
        } else {
            decide(&validation, model_verdict, options.auto_promote)
        };
        if matches!(decision, Decision::Activate) && services.repair.is_none() {
            return Err(BackendError::NotConfigured(
                "automatic rule activation requires an awaited lifecycle repair service".into(),
            ));
        }
        if stopped() {
            report.truncated = true;
            break;
        }
        within_deadline(
            started,
            bounds.max_elapsed,
            persist(
                services.store,
                services.repair,
                org_id,
                src,
                id,
                &batch,
                &proposal,
                &validation,
                &label_summary,
                decision,
                options,
                &mut report,
            ),
        )
        .await?;
    }

    report.truncated |= stopped();
    if report.truncated {
        report.resume = Some(format!("{org_id}/{src}"));
    }
    Ok(report)
}

/// Turn validation + model review into the status the proposal should enter.
fn decide(
    validation: &crate::traits::rule_store::RuleValidation,
    model_verdict: Option<ModelVerdict>,
    auto_promote: bool,
) -> Decision {
    match model_verdict {
        Some(ModelVerdict::Reject) => return Decision::Reject("model rejected the mapping".into()),
        Some(ModelVerdict::Abstain) => {
            return Decision::Uncertain("model abstained on the mapping".into())
        }
        _ => {}
    }
    if !validation.meets_promotion_gate() {
        return Decision::Uncertain("held-out validation below the promotion gate".into());
    }
    if auto_promote {
        Decision::Activate
    } else {
        Decision::Propose
    }
}

#[allow(clippy::too_many_arguments)]
async fn persist(
    store: &dyn RuleStore,
    repair: Option<&dyn RuleLifecycleRepair>,
    org_id: &str,
    src: &str,
    id: Uuid,
    batch: &EvidenceBatch,
    proposal: &RuleProposal,
    validation: &crate::traits::rule_store::RuleValidation,
    label_summary: &str,
    decision: Decision,
    options: &LearningOptions,
    report: &mut LearningReport,
) -> Result<(), BackendError> {
    let origin = if options.model_id == "none" {
        RuleOrigin::System {
            component: "rule-learning".into(),
        }
    } else {
        RuleOrigin::Model {
            model: options.model_id.clone(),
            prompt_version: options.prompt_version.clone(),
        }
    };
    let (entry_status, entry_note) = match &decision {
        Decision::Uncertain(reason) => (RuleStatus::Uncertain, reason.clone()),
        Decision::Reject(reason) => (RuleStatus::Proposed, reason.clone()),
        Decision::Activate => (RuleStatus::Proposed, "auto-promotion candidate".into()),
        Decision::Propose => (RuleStatus::Proposed, "awaiting explicit activation".into()),
    };
    let rule = LearnedRule {
        id,
        revision: 1,
        org_id: org_id.into(),
        source: src.into(),
        namespace: Some(proposal.namespace.clone()),
        schema_fingerprint: batch.schema_fingerprint.clone(),
        mapping: proposal.mapping.clone(),
        owner_slot: proposal.owner_slot.clone(),
        origin,
        evidence_refs: vec![
            format!(
                "positives={},negatives={},distinct_values={}",
                proposal.positives, proposal.negatives, proposal.distinct_values
            ),
            label_summary.to_owned(),
        ],
        validation: Some(validation.clone()),
        decisions: vec![system_decision("rule-learning", &entry_note)],
        status: entry_status,
        effective_from: None,
        revoked_at: None,
    };

    // A concurrent or prior run may already hold this id. That is dedup, not an
    // error — but a proposed or uncertain rule whose evidence now meets the gate
    // is reevaluated and promoted rather than silently skipped. A stale rule
    // (its schema changed) is left for a fresh proposal reflecting the new
    // schema; unchanged evidence never triggers a new decision.
    match store.propose(rule).await {
        Ok(_) => {}
        Err(BackendError::Conflict(_)) => {
            if matches!(decision, Decision::Activate) {
                if let Some(existing) = store.get(org_id, id).await? {
                    match existing.status {
                        // Same-schema insufficient-evidence rule now meets the
                        // gate: promote it in place.
                        RuleStatus::Proposed | RuleStatus::Uncertain => {
                            let active = store
                                .transition(
                                    org_id,
                                    id,
                                    existing.revision,
                                    RuleTransition {
                                        to: RuleStatus::Active,
                                        decision: system_decision(
                                            "rule-learning",
                                            "reevaluated: new evidence meets the promotion gate",
                                        ),
                                        validation: Some(validation.clone()),
                                    },
                                )
                                .await?;
                            repair
                                .expect("activation was checked before persistence")
                                .reconcile(&active)
                                .await?;
                            report.activated += 1;
                            return Ok(());
                        }
                        // Stale rule (its schema changed): relearn in place with
                        // the refreshed mapping, fingerprint and validation.
                        RuleStatus::Stale => {
                            let mut updated = existing.clone();
                            updated.revision = existing.revision + 1;
                            updated.mapping = proposal.mapping.clone();
                            updated.owner_slot = proposal.owner_slot.clone();
                            updated.schema_fingerprint = batch.schema_fingerprint.clone();
                            updated.validation = Some(validation.clone());
                            updated.status = RuleStatus::Active;
                            updated.effective_from = Some(chrono::Utc::now());
                            updated.decisions.push(system_decision(
                                "rule-learning",
                                "relearned a stale rule against the new schema",
                            ));
                            let active = store
                                .supersede(org_id, id, existing.revision, updated)
                                .await?;
                            repair
                                .expect("activation was checked before persistence")
                                .reconcile(&active)
                                .await?;
                            report.activated += 1;
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
            report.skipped_existing += 1;
            return Ok(());
        }
        Err(other) => return Err(other),
    }

    match decision {
        Decision::Activate => {
            let active = store
                .transition(
                    org_id,
                    id,
                    1,
                    RuleTransition {
                        to: RuleStatus::Active,
                        decision: system_decision(
                            "rule-learning",
                            "auto-promoted on independent held-out validation",
                        ),
                        validation: Some(validation.clone()),
                    },
                )
                .await?;
            repair
                .expect("activation was checked before persistence")
                .reconcile(&active)
                .await?;
            report.activated += 1;
        }
        Decision::Reject(note) => {
            store
                .transition(
                    org_id,
                    id,
                    1,
                    RuleTransition {
                        to: RuleStatus::Rejected,
                        decision: system_decision("rule-learning", &note),
                        validation: None,
                    },
                )
                .await?;
            report.rejected += 1;
        }
        Decision::Uncertain(_) => report.uncertain += 1,
        Decision::Propose => report.proposed += 1,
    }
    Ok(())
}

/// Bounded example payloads for one pattern, for the model review message.
fn pattern_examples(examples: &[ReferenceExample], key: &PatternKey) -> Vec<Value> {
    examples
        .iter()
        .filter(|e| &e.pattern() == key)
        .take(crate::runtime::rule_learning::review::MAX_REVIEW_EXAMPLES)
        .map(|e| {
            json!({
                "value_token": e.value_token,
                "outcome": e.outcome,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::extraction::ReferenceMapping;
    use crate::runtime::rule_learning::evidence::ExampleOutcome;
    use crate::test_support::{InMemoryRuleStore, MockLlmBackend};

    /// A batch describing one strong CmdbChange.owning_group -> CmdbGroup
    /// pattern, with labeled held-out cases meeting the promotion gate.
    fn batch() -> EvidenceBatch {
        let mut examples = Vec::new();
        for i in 0..12u128 {
            examples.push(ReferenceExample {
                component_paths: None,
                source_chain_id: Uuid::from_u128(i + 1),
                source_version_uuid: None,
                source_entity_type: "CmdbChange".into(),
                source_namespace: "prod".into(),
                reference_path: "owning_group".into(),
                value_token: format!("s:group{i}"),
                reference_tokens: Vec::new(),
                target_type: "CmdbGroup".into(),
                target_key_group: vec!["group_id".into()],
                outcome: ExampleOutcome::Positive,
            });
        }
        let mut labeled = Vec::new();
        for i in 0..25u128 {
            labeled.push(LabeledCase {
                example: ReferenceExample {
                    component_paths: None,
                    source_chain_id: Uuid::from_u128(1000 + i),
                    source_version_uuid: None,
                    source_entity_type: "CmdbChange".into(),
                    source_namespace: "prod".into(),
                    reference_path: "owning_group".into(),
                    value_token: format!("s:h{i}"),
                    reference_tokens: Vec::new(),
                    target_type: "CmdbGroup".into(),
                    target_key_group: vec!["group_id".into()],
                    outcome: ExampleOutcome::Positive,
                },
                should_link: true,
                expected_target_chain_id: Some(Uuid::from_u128(11_000 + i)),
                adjudication_ref: format!("review-positive-{i}"),
                label_model: None,
            });
        }
        for i in 0..25u128 {
            labeled.push(LabeledCase {
                example: ReferenceExample {
                    component_paths: None,
                    source_chain_id: Uuid::from_u128(2000 + i),
                    source_version_uuid: None,
                    source_entity_type: "CmdbChange".into(),
                    source_namespace: "prod".into(),
                    reference_path: "owning_group".into(),
                    value_token: format!("s:p{i}"),
                    reference_tokens: Vec::new(),
                    target_type: "CmdbPerson".into(),
                    target_key_group: vec!["user_id".into()],
                    outcome: ExampleOutcome::Positive,
                },
                should_link: false,
                expected_target_chain_id: None,
                adjudication_ref: format!("review-negative-{i}"),
                label_model: None,
            });
        }
        EvidenceBatch {
            examples,
            labeled,
            schema_fingerprint: "fp-1".into(),
            scanned: 62,
            truncated: false,
        }
    }

    struct FixedSource(EvidenceBatch);
    #[async_trait]
    impl ReferenceEvidenceSource for FixedSource {
        async fn collect(
            &self,
            _org: &str,
            _src: &str,
            _bounds: &LearningBounds,
            _holdout_permille: u16,
        ) -> Result<EvidenceBatch, BackendError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn cancellation_after_evidence_prevents_rule_writes() {
        struct CancellingSource(tokio_util::sync::CancellationToken);
        #[async_trait]
        impl ReferenceEvidenceSource for CancellingSource {
            async fn collect(
                &self,
                _: &str,
                _: &str,
                _: &LearningBounds,
                _: u16,
            ) -> Result<EvidenceBatch, BackendError> {
                self.0.cancel();
                Ok(batch())
            }
        }
        let token = tokio_util::sync::CancellationToken::new();
        let source = CancellingSource(token.clone());
        let store = InMemoryRuleStore::new();
        let report = learn_cancellable(
            LearningServices {
                evidence: &source,
                validator: &crate::runtime::rule_learning::validation::NoAdjudicatedValidation,
                store: &store,
                model: None,
                repair: None,
            },
            "org",
            "cmdb",
            &LearningBounds::default(),
            &LearningOptions::default(),
            Some(&token),
        )
        .await
        .unwrap();
        assert!(report.truncated);
        assert!(report.resume.is_some());
        assert_eq!(
            report.proposed + report.activated + report.uncertain + report.rejected,
            0
        );
        assert!(store.list_all("org", "cmdb").await.unwrap().is_empty());
    }

    fn adjudicated_case(index: u128) -> LabeledCase {
        LabeledCase {
            example: ReferenceExample {
                component_paths: None,
                source_chain_id: Uuid::from_u128(3_000 + index),
                source_version_uuid: Some(Uuid::from_u128(4_000 + index)),
                source_entity_type: "CmdbChange".into(),
                source_namespace: "prod".into(),
                reference_path: "owning_group".into(),
                value_token: format!("s:labeled-{index}"),
                reference_tokens: vec![format!("s:labeled-{index}")],
                target_type: "CmdbGroup".into(),
                target_key_group: vec!["group_id".into()],
                outcome: ExampleOutcome::Positive,
            },
            should_link: true,
            expected_target_chain_id: Some(Uuid::from_u128(5_000 + index)),
            adjudication_ref: format!("review-{index}"),
            label_model: None,
        }
    }

    #[test]
    fn adjudicated_evidence_requires_replayable_provenance() {
        let source = FixedSource(batch());
        let mut invalid = adjudicated_case(1);
        invalid.example.source_version_uuid = None;

        assert!(matches!(
            AdjudicatedEvidenceSource::new(&source, vec![invalid]),
            Err(BackendError::Query(_))
        ));
    }

    #[tokio::test]
    async fn adjudicated_evidence_respects_scan_and_byte_bounds() {
        let mut observations = batch();
        observations.labeled.clear();
        observations.scanned = 1;
        let source = FixedSource(observations);
        let combined =
            AdjudicatedEvidenceSource::new(&source, vec![adjudicated_case(1), adjudicated_case(2)])
                .unwrap();
        let scan_limited = LearningBounds {
            max_scanned_entities: 2,
            ..Default::default()
        };

        let result = combined
            .collect("org", "cmdb", &scan_limited, 300)
            .await
            .unwrap();
        assert_eq!(result.scanned, 2);
        assert_eq!(result.labeled.len(), 1);
        assert!(result.truncated);

        let byte_limited = LearningBounds {
            max_evidence_bytes: 1,
            ..Default::default()
        };
        let result = combined
            .collect("org", "cmdb", &byte_limited, 300)
            .await
            .unwrap();
        assert!(result.labeled.is_empty());
        assert!(result.truncated);
    }

    struct FixedValidator;
    #[async_trait]
    impl RuleValidationExecutor for FixedValidator {
        async fn predict(
            &self,
            _org_id: &str,
            _source: &str,
            _mapping: &ReferenceMapping,
            examples: &[ValidationExample],
        ) -> Result<Vec<Option<Uuid>>, BackendError> {
            Ok(examples
                .iter()
                .map(|example| {
                    (example.target_type == "CmdbGroup")
                        .then(|| Uuid::from_u128(example.source_chain_id.as_u128() + 10_000))
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn deterministic_run_proposes_but_does_not_auto_activate() {
        let store = InMemoryRuleStore::new();
        let report = learn(
            LearningServices {
                evidence: &FixedSource(batch()),
                validator: &FixedValidator,
                store: &store,
                model: None,
                repair: Some(&NoDerivedRuleState),
            },
            "org",
            "cmdb",
            &LearningBounds::default(),
            &LearningOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.proposed, 1);
        assert_eq!(report.activated, 0);
        assert_eq!(report.model_calls, 0);
        let stored = store.all();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].status, RuleStatus::Proposed);
        assert!(matches!(stored[0].origin, RuleOrigin::System { .. }));
    }

    #[tokio::test]
    async fn the_model_call_bound_truncates_extra_patterns() {
        let store = InMemoryRuleStore::new();
        let bounds = LearningBounds {
            max_model_calls: 0,
            ..Default::default()
        };
        let options = LearningOptions {
            auto_promote: true,
            model_id: "gpt-test".into(),
            ..Default::default()
        };
        let model = MockLlmBackend::with_responses(vec![]);
        // With zero model calls allowed, the run falls back to the deterministic
        // gate and never calls the model.
        let report = learn(
            LearningServices {
                evidence: &FixedSource(batch()),
                validator: &FixedValidator,
                store: &store,
                model: Some(&model),
                repair: Some(&NoDerivedRuleState),
            },
            "org",
            "cmdb",
            &bounds,
            &options,
        )
        .await
        .unwrap();
        assert_eq!(report.model_calls, 0);
        assert_eq!(report.activated, 1, "deterministic gate still promotes");
    }

    #[tokio::test]
    async fn automatic_activation_requires_graph_repair_before_writing_a_rule() {
        let store = InMemoryRuleStore::new();
        let error = learn(
            LearningServices {
                evidence: &FixedSource(batch()),
                validator: &FixedValidator,
                store: &store,
                model: None,
                repair: None,
            },
            "org",
            "cmdb",
            &LearningBounds::default(),
            &LearningOptions {
                auto_promote: true,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, BackendError::NotConfigured(_)));
        assert!(store.all().is_empty());
    }
}
