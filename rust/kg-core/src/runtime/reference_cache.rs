//! Run-local reuse of ambiguous-reference decisions while their evidence is unchanged.
//!
//! A decision is keyed by the organization and the complete evidence
//! fingerprint (source evidence, candidate versions and properties, guidance,
//! bounds, model and prompt identity). Identical evidence reuses the decision;
//! changed evidence pays once more. The cache is single-flight: concurrent
//! callers for one key share the leader's outcome instead of each dispatching,
//! and a consumed attempt that ended in failure is remembered so re-entering the
//! stage or replanning identical evidence cannot dispatch again in this run.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use tokio::sync::watch;

use super::reference_resolution::ReferenceDecisionAudit;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ReferenceDecisionKey([u8; 32]);

impl ReferenceDecisionKey {
    /// Organization plus the canonical evidence fingerprint of one occurrence.
    pub fn new(org: &str, evidence_fingerprint: &str) -> Self {
        let mut hasher = Sha256::new();
        for part in [org, evidence_fingerprint] {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        Self(hasher.finalize().into())
    }
}

/// What one evidence packet settled to in this run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CachedOutcome {
    /// A model or host decision; reused decisions are rebound to the current observation.
    Decided(Box<ReferenceDecisionAudit>),
    /// The provider attempt was consumed but produced no usable decision
    /// (transport failure, invalid answer). Replaying identical evidence in this
    /// run re-raises the failure instead of paying again.
    Failed {
        /// Operational description safe for diagnostics; never model output.
        cause: String,
        attempts: u32,
    },
}

enum Entry {
    InFlight(watch::Receiver<Option<Arc<CachedOutcome>>>),
    Settled(Arc<CachedOutcome>),
}

/// Decisions and in-flight leaders of this run.
#[derive(Default)]
pub struct ReferenceDecisionCache {
    entries: Mutex<HashMap<ReferenceDecisionKey, Entry>>,
    /// Original model verdicts of this run by reuse key (organization plus
    /// reuse fingerprint): a later occurrence with the same key but a
    /// different packet (another snapshot of the same source) is rebound to
    /// them without reading the store or dispatching.
    originals: Mutex<HashMap<ReferenceDecisionKey, Arc<ReferenceDecisionAudit>>>,
}

/// The result of asking who decides a key.
pub enum Claim<'a> {
    /// This caller decides; it must settle or drop the guard.
    Leader(LeaderGuard<'a>),
    /// Another caller is deciding the same evidence right now.
    Wait(Waiter),
    /// Already decided in this run.
    Settled(Arc<CachedOutcome>),
}

/// Held by the caller that decides a key. Settling publishes the outcome to
/// every waiter; dropping without settling releases the key so a waiter can
/// take over (the leader was cancelled before it produced anything durable).
pub struct LeaderGuard<'a> {
    cache: &'a ReferenceDecisionCache,
    key: ReferenceDecisionKey,
    sender: Option<watch::Sender<Option<Arc<CachedOutcome>>>>,
}

impl LeaderGuard<'_> {
    pub fn settle(mut self, outcome: CachedOutcome) -> Arc<CachedOutcome> {
        let outcome = Arc::new(outcome);
        if let Ok(mut entries) = self.cache.entries.lock() {
            entries.insert(self.key, Entry::Settled(outcome.clone()));
        }
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Some(outcome.clone()));
        }
        outcome
    }
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        if self.sender.take().is_some() {
            if let Ok(mut entries) = self.cache.entries.lock() {
                if matches!(entries.get(&self.key), Some(Entry::InFlight(_))) {
                    entries.remove(&self.key);
                }
            }
            // Dropping the sender wakes waiters with a closed channel; they re-claim.
        }
    }
}

/// A caller sharing an in-flight leader's outcome.
pub struct Waiter {
    receiver: watch::Receiver<Option<Arc<CachedOutcome>>>,
}

impl Waiter {
    /// `None` means the leader abandoned the key without an outcome; re-claim.
    pub async fn outcome(mut self) -> Option<Arc<CachedOutcome>> {
        loop {
            if let Some(outcome) = self.receiver.borrow().clone() {
                return Some(outcome);
            }
            if self.receiver.changed().await.is_err() {
                return self.receiver.borrow().clone();
            }
        }
    }
}

impl ReferenceDecisionCache {
    pub fn claim(&self, key: ReferenceDecisionKey) -> Claim<'_> {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        match entries.get(&key) {
            Some(Entry::Settled(outcome)) => Claim::Settled(outcome.clone()),
            Some(Entry::InFlight(receiver)) => Claim::Wait(Waiter {
                receiver: receiver.clone(),
            }),
            None => {
                let (sender, receiver) = watch::channel(None);
                entries.insert(key, Entry::InFlight(receiver));
                Claim::Leader(LeaderGuard {
                    cache: self,
                    key,
                    sender: Some(sender),
                })
            }
        }
    }

    /// Remember an original model verdict of this run under its reuse key.
    /// Rebindings and host refusals are never originals; the first verdict
    /// for a key stays.
    pub fn note_original(&self, reuse_key: ReferenceDecisionKey, audit: &ReferenceDecisionAudit) {
        if audit.reused || audit.reason.is_host_refusal() {
            return;
        }
        if let Ok(mut originals) = self.originals.lock() {
            originals
                .entry(reuse_key)
                .or_insert_with(|| Arc::new(audit.clone()));
        }
    }

    /// The original verdict this run recorded under a reuse key, if any.
    pub fn original(
        &self,
        reuse_key: &ReferenceDecisionKey,
    ) -> Option<Arc<ReferenceDecisionAudit>> {
        self.originals.lock().ok()?.get(reuse_key).cloned()
    }

    /// A settled outcome, if any, without claiming.
    pub fn get(&self, key: &ReferenceDecisionKey) -> Option<Arc<CachedOutcome>> {
        match self.entries.lock().ok()?.get(key) {
            Some(Entry::Settled(outcome)) => Some(outcome.clone()),
            _ => None,
        }
    }

    /// Settled decisions in this run (in-flight keys excluded).
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .map(|entries| {
                entries
                    .values()
                    .filter(|entry| matches!(entry, Entry::Settled(_)))
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed(cause: &str) -> CachedOutcome {
        CachedOutcome::Failed {
            cause: cause.into(),
            attempts: 1,
        }
    }

    #[test]
    fn keys_follow_the_evidence_fingerprint_and_organization() {
        let a = ReferenceDecisionKey::new("org", &"a".repeat(64));
        assert_eq!(a, ReferenceDecisionKey::new("org", &"a".repeat(64)));
        assert_ne!(a, ReferenceDecisionKey::new("org", &"b".repeat(64)));
        assert_ne!(a, ReferenceDecisionKey::new("other", &"a".repeat(64)));
    }

    #[tokio::test]
    async fn one_leader_decides_and_waiters_share_the_outcome_or_retake_an_abandoned_key() {
        let cache = Arc::new(ReferenceDecisionCache::default());
        let key = ReferenceDecisionKey::new("org", &"c".repeat(64));
        let Claim::Leader(leader) = cache.claim(key) else {
            panic!("first claim leads")
        };
        let Claim::Wait(waiter) = cache.claim(key) else {
            panic!("second claim waits")
        };
        assert!(cache.is_empty(), "in-flight keys are not settled decisions");
        let shared = tokio::spawn(waiter.outcome());
        let outcome = leader.settle(failed("provider unavailable"));
        assert_eq!(*shared.await.unwrap().unwrap(), *outcome);
        assert!(matches!(cache.claim(key), Claim::Settled(_)));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&key).as_deref(), Some(&*outcome));

        // A leader that gives up without an outcome releases the key.
        let other = ReferenceDecisionKey::new("org", &"d".repeat(64));
        let Claim::Leader(abandoned) = cache.claim(other) else {
            panic!()
        };
        let Claim::Wait(waiter) = cache.claim(other) else {
            panic!()
        };
        drop(abandoned);
        assert!(
            waiter.outcome().await.is_none(),
            "waiters learn the leader quit"
        );
        assert!(
            matches!(cache.claim(other), Claim::Leader(_)),
            "the key is free again"
        );
        assert!(cache.get(&other).is_none());
    }

    #[test]
    fn originals_are_kept_by_reuse_key_and_only_the_first_verdict_stays() {
        use crate::runtime::reference_resolution::{
            DecisionOutcome, DecisionReason, EvidenceOrigin,
        };
        let cache = ReferenceDecisionCache::default();
        let key = ReferenceDecisionKey::new("org", &"e".repeat(64));
        let mut audit = ReferenceDecisionAudit {
            decision_id: uuid::Uuid::from_u128(1),
            source_chain_id: uuid::Uuid::from_u128(2),
            source_version_uuid: uuid::Uuid::from_u128(3),
            source_snapshot_id: uuid::Uuid::from_u128(4),
            source_captured_at: chrono::Utc::now(),
            slot: "T.p".into(),
            location: "p".into(),
            value: "v".into(),
            evidence_origin: EvidenceOrigin::Structured,
            outcome: DecisionOutcome::Rejected,
            reason: DecisionReason::ModelRejected,
            target_chain_id: None,
            fact: None,
            supporting_evidence: Vec::new(),
            candidate_read_set: Vec::new(),
            evidence_fingerprint: "a".repeat(64),
            evidence_complete: true,
            model_configured: "m".into(),
            model_served: Some("m".into()),
            provider_attempts: 1,
            input_tokens: None,
            output_tokens: None,
            processing_version: "v".into(),
            decided_at: chrono::Utc::now(),
            reused: false,
            reused_from: None,
            reuse_fingerprint: "e".repeat(64),
            cited_value_hashes: Vec::new(),
        };
        assert!(cache.original(&key).is_none());
        let mut refusal = audit.clone();
        refusal.reason = DecisionReason::OwnIdentityValue;
        refusal.outcome = DecisionOutcome::Unsure;
        cache.note_original(key, &refusal);
        assert!(
            cache.original(&key).is_none(),
            "host refusals are not originals"
        );
        cache.note_original(key, &audit);
        assert_eq!(cache.original(&key).unwrap().decision_id, audit.decision_id);
        audit.decision_id = uuid::Uuid::from_u128(9);
        cache.note_original(key, &audit);
        assert_eq!(
            cache.original(&key).unwrap().decision_id,
            uuid::Uuid::from_u128(1),
            "the first verdict stays"
        );
        let mut rebound = audit.clone();
        rebound.reused = true;
        rebound.reused_from = Some(uuid::Uuid::from_u128(1));
        let other = ReferenceDecisionKey::new("org", &"f".repeat(64));
        cache.note_original(other, &rebound);
        assert!(
            cache.original(&other).is_none(),
            "rebindings are not originals"
        );
    }
}
