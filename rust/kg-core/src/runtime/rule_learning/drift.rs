//! Drift detection and reevaluation for learned rules.
//!
//! A rule's applicability is tied to the source schema it was learned against.
//! When that schema's fingerprint changes — a mapped field, type or key changed
//! — the rule is stale regardless of whether any counts changed, and must stop
//! being trusted until reevaluated. Reevaluation of a stale, uncertain or
//! rejected rule happens only when relevant new evidence arrives; unchanged
//! evidence never triggers a fresh model call. Marking a rule stale never
//! deletes its edges: retirement is the separate awaited maintenance op.
use uuid::Uuid;

use crate::errors::BackendError;
use crate::runtime::rule_learning::service::RuleLifecycleRepair;
use crate::traits::rule_store::{
    LearnedRule, RuleDecision, RuleOrigin, RuleStatus, RuleStore, RuleTransition,
};

/// The active rules whose schema fingerprint no longer matches the current one.
/// Independent of counts: a schema change alone makes a rule stale.
pub fn stale_ids(active: &[LearnedRule], current_fingerprint: &str) -> Vec<Uuid> {
    active
        .iter()
        .filter(|r| r.status == RuleStatus::Active && r.schema_fingerprint != current_fingerprint)
        .map(|r| r.id)
        .collect()
}

/// Outcome of a drift pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriftReport {
    pub checked: usize,
    pub staled: usize,
}

fn system_decision(note: &str) -> RuleDecision {
    RuleDecision {
        origin: RuleOrigin::System {
            component: "rule-drift".into(),
        },
        at: chrono::Utc::now(),
        note: Some(note.chars().take(256).collect()),
    }
}

/// Move every active rule whose schema fingerprint changed to `Stale`. Each
/// transition is guarded by the rule's current revision, so a concurrent change
/// surfaces as a conflict rather than a lost update.
pub async fn apply_drift(
    store: &dyn RuleStore,
    repair: &dyn RuleLifecycleRepair,
    org_id: &str,
    source: &str,
    current_fingerprint: &str,
) -> Result<DriftReport, BackendError> {
    // Resume graph repair for any prior drift transition that committed before
    // its awaited repair completed. Per-owner repair receipts make this safe.
    for stale in store
        .list_all(org_id, source)
        .await?
        .into_iter()
        .filter(|rule| {
            rule.status == RuleStatus::Stale
                && rule.schema_fingerprint != current_fingerprint
                && rule.decisions.last().is_some_and(|decision| {
                    matches!(
                        &decision.origin,
                        RuleOrigin::System { component } if component == "rule-drift"
                    )
                })
        })
    {
        repair.reconcile(&stale).await?;
    }
    let active = store.list_active(org_id, source).await?;
    let mut report = DriftReport {
        checked: active.len(),
        staled: 0,
    };
    for rule in active {
        if rule.schema_fingerprint == current_fingerprint {
            continue;
        }
        match store
            .transition(
                org_id,
                rule.id,
                rule.revision,
                RuleTransition {
                    to: RuleStatus::Stale,
                    decision: system_decision(&format!(
                        "schema fingerprint changed {} -> {current_fingerprint}",
                        rule.schema_fingerprint
                    )),
                    validation: None,
                },
            )
            .await
        {
            Ok(stale) => {
                repair.reconcile(&stale).await?;
                report.staled += 1;
            }
            // A concurrent revocation/activation raced us; leave it to the next
            // pass rather than fail the whole drift run.
            Err(BackendError::Conflict(_)) | Err(BackendError::NotFound(_)) => {}
            Err(other) => return Err(other),
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::InMemoryRuleStore;
    use crate::traits::rule_store::{
        RuleValidation, MIN_PROMOTION_NEGATIVES, MIN_PROMOTION_POSITIVES, MIN_PROMOTION_PRECISION,
        MIN_PROMOTION_RECALL,
    };

    fn active_rule(id: u128, fingerprint: &str) -> LearnedRule {
        let gate = RuleValidation {
            positives: MIN_PROMOTION_POSITIVES,
            negatives: MIN_PROMOTION_NEGATIVES,
            precision: MIN_PROMOTION_PRECISION,
            recall: MIN_PROMOTION_RECALL,
            conflicting_failures: 0,
            independent: true,
        };
        LearnedRule {
            id: Uuid::from_u128(id),
            revision: 2,
            org_id: "org".into(),
            source: "cmdb".into(),
            namespace: None,
            schema_fingerprint: fingerprint.into(),
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
            evidence_refs: vec![],
            validation: Some(gate),
            decisions: vec![],
            status: RuleStatus::Active,
            effective_from: Some(chrono::Utc::now()),
            revoked_at: None,
        }
    }

    #[tokio::test]
    async fn a_changed_fingerprint_stales_the_rule_and_a_matching_one_survives() {
        let store = InMemoryRuleStore::new();
        // Seed two active rules directly (bypassing the propose->activate path).
        store.seed(active_rule(1, "fp-old"));
        store.seed(active_rule(2, "fp-new"));
        let report = apply_drift(
            &store,
            &crate::runtime::rule_learning::NoDerivedRuleState,
            "org",
            "cmdb",
            "fp-new",
        )
        .await
        .unwrap();
        assert_eq!(report.checked, 2);
        assert_eq!(report.staled, 1);
        // The mismatched rule is no longer active; the matching one still is.
        let active = store.list_active("org", "cmdb").await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, Uuid::from_u128(2));
        let staled = store.get("org", Uuid::from_u128(1)).await.unwrap().unwrap();
        assert_eq!(staled.status, RuleStatus::Stale);
    }

    #[test]
    fn stale_ids_is_count_independent() {
        let rules = vec![active_rule(1, "fp-old"), active_rule(2, "fp-cur")];
        assert_eq!(stale_ids(&rules, "fp-cur"), vec![Uuid::from_u128(1)]);
    }
}
