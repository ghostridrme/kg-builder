//! Administrative rule operations shared by CLI, HTTP and MCP surfaces. Every
//! write goes through the same
//! [`RuleStore`] state machine and optimistic-revision guard as autonomous
//! learning, so an explicit activation is held to the identical held-out gate;
//! a human decision and a model decision never blur. Reads are org- and
//! source-scoped; a write reads the current revision and transitions against it,
//! so a concurrent change surfaces as a conflict rather than a lost update.
use uuid::Uuid;

use crate::errors::BackendError;
use crate::traits::rule_store::{
    LearnedRule, RuleDecision, RuleStatus, RuleStore, RuleTransition, RuleValidation,
};

/// List rules for a producer, optionally filtered to one status. Ordered by id.
pub async fn list(
    store: &dyn RuleStore,
    org_id: &str,
    source: &str,
    status: Option<RuleStatus>,
) -> Result<Vec<LearnedRule>, BackendError> {
    let all = store.list_all(org_id, source).await?;
    Ok(match status {
        Some(want) => all.into_iter().filter(|r| r.status == want).collect(),
        None => all,
    })
}

/// One rule with its full decision/evidence history.
pub async fn detail(
    store: &dyn RuleStore,
    org_id: &str,
    id: Uuid,
) -> Result<Option<LearnedRule>, BackendError> {
    store.get(org_id, id).await
}

/// The evidence and decision trail of one rule, for an evidence view.
pub async fn evidence(
    store: &dyn RuleStore,
    org_id: &str,
    id: Uuid,
) -> Result<Option<(Vec<String>, Vec<RuleDecision>, Option<RuleValidation>)>, BackendError> {
    Ok(store
        .get(org_id, id)
        .await?
        .map(|r| (r.evidence_refs, r.decisions, r.validation)))
}

async fn transition_current(
    store: &dyn RuleStore,
    org_id: &str,
    id: Uuid,
    to: RuleStatus,
    decision: RuleDecision,
    validation: Option<RuleValidation>,
) -> Result<LearnedRule, BackendError> {
    let current = store
        .get(org_id, id)
        .await?
        .ok_or_else(|| BackendError::NotFound("learned rule".into()))?;
    store
        .transition(
            org_id,
            id,
            current.revision,
            RuleTransition {
                to,
                decision,
                validation,
            },
        )
        .await
}

async fn require_source(
    store: &dyn RuleStore,
    org_id: &str,
    source: &str,
    id: Uuid,
) -> Result<LearnedRule, BackendError> {
    let rule = store
        .get(org_id, id)
        .await?
        .ok_or_else(|| BackendError::NotFound("learned rule".into()))?;
    if rule.source != source {
        return Err(BackendError::NotFound("learned rule".into()));
    }
    Ok(rule)
}

/// Read one rule only when its producer matches the scoped route or command.
pub async fn detail_for_source(
    store: &dyn RuleStore,
    org_id: &str,
    source: &str,
    id: Uuid,
) -> Result<Option<LearnedRule>, BackendError> {
    match store.get(org_id, id).await? {
        Some(rule) if rule.source == source => Ok(Some(rule)),
        _ => Ok(None),
    }
}

/// Explicitly activate a proposed/uncertain rule. `validation` must be
/// independent and meet the promotion gate (the store enforces it); a human
/// decision cannot substitute for held-out validation.
pub async fn activate(
    store: &dyn RuleStore,
    org_id: &str,
    source: &str,
    id: Uuid,
    decision: RuleDecision,
) -> Result<LearnedRule, BackendError> {
    let current = require_source(store, org_id, source, id).await?;
    if current.status == RuleStatus::Active {
        return Ok(current);
    }
    let validation = current.validation.clone().ok_or_else(|| {
        BackendError::Query("activation requires stored trusted validation".into())
    })?;
    store
        .transition(
            org_id,
            id,
            current.revision,
            RuleTransition {
                to: RuleStatus::Active,
                decision,
                validation: Some(validation),
            },
        )
        .await
}

/// Reject a proposed/uncertain rule for this revision (not a permanent ban).
pub async fn reject(
    store: &dyn RuleStore,
    org_id: &str,
    source: &str,
    id: Uuid,
    decision: RuleDecision,
) -> Result<LearnedRule, BackendError> {
    require_source(store, org_id, source, id).await?;
    transition_current(store, org_id, id, RuleStatus::Rejected, decision, None).await
}

/// Revoke an active/stale rule. Callers then await reference-owner repair before
/// reporting the lifecycle change complete; retrying an already revoked rule
/// returns its current revision so that repair can resume idempotently.
pub async fn revoke(
    store: &dyn RuleStore,
    org_id: &str,
    source: &str,
    id: Uuid,
    decision: RuleDecision,
) -> Result<LearnedRule, BackendError> {
    let current = require_source(store, org_id, source, id).await?;
    if current.status == RuleStatus::Revoked {
        return Ok(current);
    }
    transition_current(store, org_id, id, RuleStatus::Revoked, decision, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::InMemoryRuleStore;
    use crate::traits::rule_store::{
        RuleOrigin, MIN_PROMOTION_NEGATIVES, MIN_PROMOTION_POSITIVES, MIN_PROMOTION_PRECISION,
        MIN_PROMOTION_RECALL,
    };
    use chrono::Utc;

    fn proposed(id: u128) -> LearnedRule {
        LearnedRule {
            id: Uuid::from_u128(id),
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
            origin: RuleOrigin::System {
                component: "rule-learning".into(),
            },
            evidence_refs: vec!["positives=5".into()],
            validation: None,
            decisions: vec![],
            status: RuleStatus::Proposed,
            effective_from: None,
            revoked_at: None,
        }
    }

    fn human(actor: &str) -> RuleDecision {
        RuleDecision {
            origin: RuleOrigin::Human {
                actor: actor.into(),
            },
            at: Utc::now(),
            note: None,
        }
    }

    fn gate() -> RuleValidation {
        RuleValidation {
            positives: MIN_PROMOTION_POSITIVES,
            negatives: MIN_PROMOTION_NEGATIVES,
            precision: MIN_PROMOTION_PRECISION,
            recall: MIN_PROMOTION_RECALL,
            conflicting_failures: 0,
            independent: true,
        }
    }

    #[tokio::test]
    async fn activate_reject_revoke_and_scoped_listing() {
        let store = InMemoryRuleStore::new();
        let mut first = proposed(1);
        first.validation = Some(gate());
        store.propose(first).await.unwrap();
        store.propose(proposed(2)).await.unwrap();

        let active = activate(&store, "org", "cmdb", Uuid::from_u128(1), human("sre"))
            .await
            .unwrap();
        assert_eq!(active.status, RuleStatus::Active);
        // The activating decision is human; the proposing origin stays system.
        assert!(matches!(
            active.decisions.last().unwrap().origin,
            RuleOrigin::Human { .. }
        ));
        assert!(matches!(active.origin, RuleOrigin::System { .. }));

        reject(&store, "org", "cmdb", Uuid::from_u128(2), human("sre"))
            .await
            .unwrap();

        let active_only = list(&store, "org", "cmdb", Some(RuleStatus::Active))
            .await
            .unwrap();
        assert_eq!(active_only.len(), 1);
        assert_eq!(list(&store, "org", "cmdb", None).await.unwrap().len(), 2);

        // Revocation is terminal.
        revoke(&store, "org", "cmdb", Uuid::from_u128(1), human("sre"))
            .await
            .unwrap();
        assert!(list(&store, "org", "cmdb", Some(RuleStatus::Active))
            .await
            .unwrap()
            .is_empty());
        let ev = evidence(&store, "org", Uuid::from_u128(1))
            .await
            .unwrap()
            .unwrap();
        assert!(!ev.1.is_empty(), "decision trail retained");
    }

    #[tokio::test]
    async fn a_missing_rule_is_not_found() {
        let store = InMemoryRuleStore::new();
        assert!(matches!(
            revoke(&store, "org", "cmdb", Uuid::from_u128(9), human("sre")).await,
            Err(BackendError::NotFound(_))
        ));
    }
}
