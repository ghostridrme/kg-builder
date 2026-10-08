//! Versioned learned-rule storage contract.
//!
//! A learned rule is a promoted [`ReferenceMapping`] with provenance, validation
//! evidence and a status lifecycle. Ingestion freezes the active revisions it
//! reads; learning cannot change rules under a run. Promotion is gated by
//! held-out validation, never by the proposing model's own acceptance.
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::BackendError;
use crate::runtime::extraction::ReferenceMapping;

/// Evidence required before a proposed rule can be promoted automatically.
pub const MIN_PROMOTION_POSITIVES: usize = 20;
pub const MIN_PROMOTION_NEGATIVES: usize = 20;
pub const MIN_PROMOTION_PRECISION: f64 = 0.99;
pub const MIN_PROMOTION_RECALL: f64 = 0.95;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleStatus {
    Proposed,
    Active,
    Rejected,
    Uncertain,
    Stale,
    Revoked,
}

impl RuleStatus {
    /// Permitted lifecycle transitions. Rejection is scoped to a revision, so a
    /// rejected or uncertain rule may be re-proposed; a revoked rule is terminal.
    pub fn can_transition(self, to: RuleStatus) -> bool {
        use RuleStatus::*;
        matches!(
            (self, to),
            (Proposed, Active)
                | (Proposed, Rejected)
                | (Proposed, Uncertain)
                | (Uncertain, Proposed)
                | (Uncertain, Active)
                | (Uncertain, Rejected)
                | (Rejected, Proposed)
                | (Active, Stale)
                | (Active, Revoked)
                | (Stale, Active)
                | (Stale, Revoked)
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, RuleStatus::Revoked)
    }
}

/// Who made a decision; model and human provenance never blur.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleOrigin {
    Human {
        actor: String,
    },
    Model {
        model: String,
        prompt_version: String,
    },
    /// An autonomous deterministic proposal or decision made by the learning
    /// system itself (never a model self-approval, never a human approval).
    System {
        component: String,
    },
}

/// Held-out validation of one rule revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleValidation {
    pub positives: usize,
    pub negatives: usize,
    pub precision: f64,
    pub recall: f64,
    pub conflicting_failures: usize,
    /// True only when the validation was not performed by the proposing model.
    pub independent: bool,
}

impl RuleValidation {
    /// The promotion gate: enough distinct held-out examples, the frozen
    /// accuracy floors, zero conflicting-case failures and independent validation.
    pub fn meets_promotion_gate(&self) -> bool {
        self.independent
            && self.positives >= MIN_PROMOTION_POSITIVES
            && self.negatives >= MIN_PROMOTION_NEGATIVES
            && self.precision >= MIN_PROMOTION_PRECISION
            && self.recall >= MIN_PROMOTION_RECALL
            && self.conflicting_failures == 0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleDecision {
    pub origin: RuleOrigin,
    pub at: DateTime<Utc>,
    pub note: Option<String>,
}

/// One revision of a learned reference rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LearnedRule {
    pub id: Uuid,
    /// Optimistic-concurrency revision; every transition increments it.
    pub revision: u64,
    pub org_id: String,
    pub source: String,
    pub namespace: Option<String>,
    /// Fingerprint of the source schema the rule was learned against; drift
    /// invalidates applicability independently of counts.
    pub schema_fingerprint: String,
    pub mapping: ReferenceMapping,
    /// The observing source slot that owns edges created by this rule.
    pub owner_slot: String,
    pub origin: RuleOrigin,
    pub evidence_refs: Vec<String>,
    pub validation: Option<RuleValidation>,
    /// Append-only decision audit trail (proposal, validation, activation,
    /// rejection, revocation), oldest first. Retains decision reasons and keeps
    /// model and human provenance distinct across the rule's lifetime.
    #[serde(default)]
    pub decisions: Vec<RuleDecision>,
    pub status: RuleStatus,
    pub effective_from: Option<DateTime<Utc>>,
    /// Revocation takes effect at its committed transaction time.
    pub revoked_at: Option<DateTime<Utc>>,
}

impl LearnedRule {
    /// Structural validity independent of storage.
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_nil()
            || self.org_id.trim().is_empty()
            || self.source.trim().is_empty()
            || self.schema_fingerprint.trim().is_empty()
            || self.owner_slot.trim().is_empty()
            || self.evidence_refs.len() > 4096
            || self.decisions.len() > 4096
        {
            return Err("invalid learned rule".into());
        }
        if self.status == RuleStatus::Active
            && !self
                .validation
                .as_ref()
                .is_some_and(RuleValidation::meets_promotion_gate)
        {
            return Err("an active rule requires validation that meets the promotion gate".into());
        }
        if self.status == RuleStatus::Revoked && self.revoked_at.is_none() {
            return Err("a revoked rule records its revocation time".into());
        }
        Ok(())
    }

    /// Produce the next revision of this rule after an approved transition, or a
    /// [`BackendError::Query`] when the transition is illegal or activation is
    /// unvalidated. Shared by every [`RuleStore`] so the state machine and the
    /// promotion gate are enforced identically regardless of backend. The
    /// caller enforces the optimistic revision guard (this bumps to
    /// `expected_revision + 1`).
    pub fn with_transition(
        &self,
        expected_revision: u64,
        transition: &RuleTransition,
    ) -> Result<LearnedRule, BackendError> {
        if !self.status.can_transition(transition.to) {
            return Err(BackendError::Query(format!(
                "illegal rule transition {:?} -> {:?}",
                self.status, transition.to
            )));
        }
        if transition.to == RuleStatus::Active
            && !transition
                .validation
                .as_ref()
                .is_some_and(RuleValidation::meets_promotion_gate)
        {
            return Err(BackendError::Query(
                "activation requires held-out validation that meets the promotion gate".into(),
            ));
        }
        let mut updated = self.clone();
        updated.revision = expected_revision + 1;
        updated.status = transition.to;
        updated.decisions.push(transition.decision.clone());
        match transition.to {
            RuleStatus::Active => {
                updated.validation = transition.validation.clone();
                updated.effective_from = Some(transition.decision.at);
            }
            RuleStatus::Revoked => updated.revoked_at = Some(transition.decision.at),
            _ => {}
        }
        updated.validate().map_err(BackendError::Query)?;
        Ok(updated)
    }
}

/// A requested status change with its decision provenance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleTransition {
    pub to: RuleStatus,
    pub decision: RuleDecision,
    /// Required when moving to `Active`.
    pub validation: Option<RuleValidation>,
}

/// Versioned rule storage. Every write is an optimistic transition against an
/// expected revision; a stale expectation is [`BackendError::Conflict`], never a
/// silent overwrite. Storage does not decide promotion — callers pass validated
/// transitions and storage enforces the state machine and revision check.
#[async_trait]
pub trait RuleStore: Send + Sync + 'static {
    async fn get(&self, org_id: &str, id: Uuid) -> Result<Option<LearnedRule>, BackendError>;

    /// Active rules for one producer, frozen by the caller for the run.
    async fn list_active(
        &self,
        org_id: &str,
        source: &str,
    ) -> Result<Vec<LearnedRule>, BackendError>;

    /// Every rule for one producer, all statuses, ordered by id and bounded.
    /// Backs administrative list/detail views.
    async fn list_all(&self, org_id: &str, source: &str) -> Result<Vec<LearnedRule>, BackendError>;

    /// Active rules for an org across every source, so an ingestion run can
    /// freeze and apply all of them at once (materialization).
    async fn list_all_active(&self, org_id: &str) -> Result<Vec<LearnedRule>, BackendError>;

    /// Store a new proposed rule at revision 1; an existing id is a conflict.
    async fn propose(&self, rule: LearnedRule) -> Result<LearnedRule, BackendError>;

    /// Apply a transition when `expected_revision` matches; returns the new revision.
    async fn transition(
        &self,
        org_id: &str,
        id: Uuid,
        expected_revision: u64,
        transition: RuleTransition,
    ) -> Result<LearnedRule, BackendError>;

    /// Replace a rule's full state (mapping, schema fingerprint, validation,
    /// status) with `updated` when `expected_revision` matches. Unlike
    /// [`RuleStore::transition`], this refreshes the mapping/fingerprint, so a
    /// stale rule can be relearned in place against a changed schema. `updated`
    /// must carry `revision == expected_revision + 1` and the same id/org.
    async fn supersede(
        &self,
        org_id: &str,
        id: Uuid,
        expected_revision: u64,
        updated: LearnedRule,
    ) -> Result<LearnedRule, BackendError>;

    /// Install storage schema (indexes/constraints); idempotent. Default: nothing to install.
    async fn install_schema(&self) -> Result<(), BackendError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validation(independent: bool) -> RuleValidation {
        RuleValidation {
            positives: 20,
            negatives: 20,
            precision: 0.99,
            recall: 0.95,
            conflicting_failures: 0,
            independent,
        }
    }

    #[test]
    fn promotion_gate_needs_independent_validation_and_frozen_floors() {
        assert!(validation(true).meets_promotion_gate());
        assert!(
            !validation(false).meets_promotion_gate(),
            "model self-approval is not validation"
        );
        let mut v = validation(true);
        v.positives = 19;
        assert!(!v.meets_promotion_gate());
        let mut v = validation(true);
        v.precision = 0.98;
        assert!(!v.meets_promotion_gate());
        let mut v = validation(true);
        v.conflicting_failures = 1;
        assert!(!v.meets_promotion_gate());
    }

    #[test]
    fn status_machine_is_explicit_and_revocation_is_terminal() {
        use RuleStatus::*;
        assert!(Proposed.can_transition(Active));
        assert!(Active.can_transition(Revoked));
        assert!(
            Rejected.can_transition(Proposed),
            "rejection is scoped to a revision"
        );
        assert!(!Revoked.can_transition(Active));
        assert!(!Revoked.can_transition(Proposed));
        assert!(
            !Proposed.can_transition(Revoked),
            "only active/stale rules are revoked"
        );
        assert!(Revoked.is_terminal());
    }

    fn proposed_rule() -> LearnedRule {
        LearnedRule {
            id: Uuid::from_u128(7),
            revision: 1,
            org_id: "org".into(),
            source: "cmdb".into(),
            namespace: None,
            schema_fingerprint: "fp".into(),
            mapping: crate::runtime::extraction::ReferenceMapping {
                source_namespace: None,
                source_entity_type: "CmdbChange".into(),
                reference_path: "owning_group".into(),
                context_paths: Default::default(),
                target_type: "CmdbGroup".into(),
                target_key_group: vec!["group_id".into()],
                shape: Default::default(),
                direction: Default::default(),
                relationship_name: "REFERENCES_CMDBGROUP".into(),
                qualifiers: None,
                cardinality: Default::default(),
                case_insensitive_types: Vec::new(),
            },
            owner_slot: "CmdbChange.owning_group".into(),
            origin: RuleOrigin::Model {
                model: "m".into(),
                prompt_version: "v1".into(),
            },
            evidence_refs: vec![],
            validation: None,
            decisions: vec![],
            status: RuleStatus::Proposed,
            effective_from: None,
            revoked_at: None,
        }
    }

    fn transition(to: RuleStatus, validation: Option<RuleValidation>) -> RuleTransition {
        RuleTransition {
            to,
            decision: RuleDecision {
                origin: RuleOrigin::Human {
                    actor: "sre".into(),
                },
                at: Utc::now(),
                note: None,
            },
            validation,
        }
    }

    #[test]
    fn with_transition_enforces_the_gate_and_bumps_revision() {
        let proposed = proposed_rule();
        assert!(
            proposed
                .with_transition(1, &transition(RuleStatus::Active, None))
                .is_err(),
            "activation without validation is rejected"
        );
        let mut weak = validation(true);
        weak.independent = false;
        assert!(
            proposed
                .with_transition(1, &transition(RuleStatus::Active, Some(weak)))
                .is_err(),
            "model self-approval cannot activate"
        );
        let active = proposed
            .with_transition(1, &transition(RuleStatus::Active, Some(validation(true))))
            .unwrap();
        assert_eq!(active.revision, 2);
        assert_eq!(active.status, RuleStatus::Active);
        assert!(active.effective_from.is_some());
        assert_eq!(active.decisions.len(), 1);

        let revoked = active
            .with_transition(2, &transition(RuleStatus::Revoked, None))
            .unwrap();
        assert_eq!(revoked.revision, 3);
        assert!(revoked.revoked_at.is_some());
        assert_eq!(revoked.decisions.len(), 2);
        assert!(
            revoked
                .with_transition(3, &transition(RuleStatus::Active, Some(validation(true))))
                .is_err(),
            "revoked is terminal"
        );
    }
}
