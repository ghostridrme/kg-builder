//! Process-local [`RuleStore`] for tests, enforcing the same optimistic-revision
//! and state-machine contract as the Neo4j store so the learning service can be
//! exercised offline.
use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use uuid::Uuid;

use crate::errors::BackendError;
use crate::traits::rule_store::{LearnedRule, RuleStatus, RuleStore, RuleTransition};

/// In-memory rule store keyed on `(org_id, id)`.
#[derive(Debug, Default)]
pub struct InMemoryRuleStore {
    rules: RwLock<HashMap<(String, Uuid), LearnedRule>>,
}

impl InMemoryRuleStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a rule directly, bypassing the propose/transition path. For tests
    /// that need to seed an arbitrary status (e.g. an already-active rule).
    pub fn seed(&self, rule: LearnedRule) {
        self.rules
            .write()
            .unwrap()
            .insert((rule.org_id.clone(), rule.id), rule);
    }

    /// Every stored rule, for test assertions.
    pub fn all(&self) -> Vec<LearnedRule> {
        let mut rules: Vec<_> = self.rules.read().unwrap().values().cloned().collect();
        rules.sort_by(|a, b| a.id.cmp(&b.id));
        rules
    }
}

#[async_trait]
impl RuleStore for InMemoryRuleStore {
    async fn get(&self, org_id: &str, id: Uuid) -> Result<Option<LearnedRule>, BackendError> {
        Ok(self
            .rules
            .read()
            .unwrap()
            .get(&(org_id.into(), id))
            .cloned())
    }

    async fn list_active(
        &self,
        org_id: &str,
        source: &str,
    ) -> Result<Vec<LearnedRule>, BackendError> {
        let mut active: Vec<_> = self
            .rules
            .read()
            .unwrap()
            .values()
            .filter(|r| r.org_id == org_id && r.source == source && r.status == RuleStatus::Active)
            .cloned()
            .collect();
        active.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(active)
    }

    async fn list_all(&self, org_id: &str, source: &str) -> Result<Vec<LearnedRule>, BackendError> {
        let mut rules: Vec<_> = self
            .rules
            .read()
            .unwrap()
            .values()
            .filter(|r| r.org_id == org_id && r.source == source)
            .cloned()
            .collect();
        rules.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rules)
    }

    async fn list_all_active(&self, org_id: &str) -> Result<Vec<LearnedRule>, BackendError> {
        let mut rules: Vec<_> = self
            .rules
            .read()
            .unwrap()
            .values()
            .filter(|r| r.org_id == org_id && r.status == RuleStatus::Active)
            .cloned()
            .collect();
        rules.sort_by(|a, b| (a.source.clone(), a.id).cmp(&(b.source.clone(), b.id)));
        Ok(rules)
    }

    async fn propose(&self, rule: LearnedRule) -> Result<LearnedRule, BackendError> {
        rule.validate().map_err(BackendError::Query)?;
        if rule.revision != 1 {
            return Err(BackendError::Query(
                "a newly proposed rule starts at revision 1".into(),
            ));
        }
        if !matches!(rule.status, RuleStatus::Proposed | RuleStatus::Uncertain) {
            return Err(BackendError::Query(
                "a new rule enters as proposed or uncertain".into(),
            ));
        }
        let mut rules = self.rules.write().unwrap();
        let key = (rule.org_id.clone(), rule.id);
        if rules.contains_key(&key) {
            return Err(BackendError::Conflict(
                "a learned rule with this id already exists".into(),
            ));
        }
        rules.insert(key, rule.clone());
        Ok(rule)
    }

    async fn transition(
        &self,
        org_id: &str,
        id: Uuid,
        expected_revision: u64,
        transition: RuleTransition,
    ) -> Result<LearnedRule, BackendError> {
        let mut rules = self.rules.write().unwrap();
        let current = rules
            .get(&(org_id.into(), id))
            .cloned()
            .ok_or_else(|| BackendError::NotFound("learned rule".into()))?;
        if current.revision != expected_revision {
            return Err(BackendError::Conflict(
                "learned rule was modified since it was read".into(),
            ));
        }
        let updated = current.with_transition(expected_revision, &transition)?;
        rules.insert((org_id.into(), id), updated.clone());
        Ok(updated)
    }

    async fn supersede(
        &self,
        org_id: &str,
        id: Uuid,
        expected_revision: u64,
        updated: LearnedRule,
    ) -> Result<LearnedRule, BackendError> {
        updated.validate().map_err(BackendError::Query)?;
        if updated.id != id || updated.org_id != org_id || updated.revision != expected_revision + 1
        {
            return Err(BackendError::Query(
                "superseding rule must keep id/org and bump to expected_revision + 1".into(),
            ));
        }
        let mut rules = self.rules.write().unwrap();
        let current = rules
            .get(&(org_id.into(), id))
            .cloned()
            .ok_or_else(|| BackendError::NotFound("learned rule".into()))?;
        if current.revision != expected_revision {
            return Err(BackendError::Conflict(
                "learned rule was modified since it was read".into(),
            ));
        }
        rules.insert((org_id.into(), id), updated.clone());
        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::rule_store::{
        RuleDecision, RuleOrigin, RuleValidation, MIN_PROMOTION_NEGATIVES, MIN_PROMOTION_POSITIVES,
        MIN_PROMOTION_PRECISION, MIN_PROMOTION_RECALL,
    };
    use chrono::Utc;

    fn rule(id: u128) -> LearnedRule {
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
    async fn propose_dedups_and_transition_guards_revision() {
        let store = InMemoryRuleStore::new();
        store.propose(rule(1)).await.unwrap();
        assert!(matches!(
            store.propose(rule(1)).await,
            Err(BackendError::Conflict(_))
        ));
        let activate = RuleTransition {
            to: RuleStatus::Active,
            decision: RuleDecision {
                origin: RuleOrigin::Human {
                    actor: "sre".into(),
                },
                at: Utc::now(),
                note: None,
            },
            validation: Some(gate()),
        };
        let active = store
            .transition("org", Uuid::from_u128(1), 1, activate.clone())
            .await
            .unwrap();
        assert_eq!(active.revision, 2);
        assert_eq!(store.list_active("org", "cmdb").await.unwrap().len(), 1);
        // Stale expectation conflicts.
        assert!(matches!(
            store
                .transition("org", Uuid::from_u128(1), 1, activate)
                .await,
            Err(BackendError::Conflict(_))
        ));
    }
}
