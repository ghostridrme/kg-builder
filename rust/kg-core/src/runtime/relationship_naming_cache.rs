//! Bounded reuse of semantic relationship-naming decisions.
//!
//! One entry is one model decision for one exact evidence packet: the
//! organization, model, stage version, instruction, permitted labels, both
//! complete endpoint records and the edge evidence. The engine shares one cache
//! across its requests, so identical evidence is decided once per process;
//! nothing here is durable. Only answered decisions are recorded — a label that
//! passed validation, or the model's explicit abstention — never a failed,
//! cancelled or malformed call.
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use super::matching_cache::CacheKey;

#[derive(Default)]
struct Entries {
    values: HashMap<CacheKey, Option<String>>,
    order: VecDeque<CacheKey>,
}

pub struct RelationshipNamingCache {
    capacity: usize,
    entries: Mutex<Entries>,
}

impl Default for RelationshipNamingCache {
    fn default() -> Self {
        Self::with_capacity(10_000)
    }
}

impl RelationshipNamingCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: Mutex::new(Entries::default()),
        }
    }

    /// `Some(None)` is a remembered abstention; `None` is undecided.
    pub fn get(&self, key: &CacheKey) -> Option<Option<String>> {
        self.entries.lock().ok()?.values.get(key).cloned()
    }

    /// First in, first out once full; re-recording a key keeps its slot.
    pub fn record(&self, key: CacheKey, decision: Option<String>) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        if !entries.values.contains_key(&key) {
            if entries.values.len() == self.capacity {
                if let Some(oldest) = entries.order.pop_front() {
                    entries.values.remove(&oldest);
                }
            }
            entries.order.push_back(key);
        }
        entries.values.insert(key, decision);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Gate: (1) the cache is bounded and distinguishes an abstention from an
    /// undecided key; (2) an unbounded map, or eviction of the newest entry,
    /// passes every stage test; (3) the stage tests never fill it; (4) no seam.
    #[test]
    fn oldest_decision_leaves_first_and_abstentions_are_remembered() {
        let cache = RelationshipNamingCache::with_capacity(2);
        let key = |n: u8| CacheKey::from_value(&json!(n));
        cache.record(key(1), Some("USES".into()));
        cache.record(key(2), None);
        cache.record(key(1), Some("OWNS".into()));
        cache.record(key(3), None);
        assert_eq!(cache.get(&key(1)), None);
        assert_eq!(cache.get(&key(2)), Some(None));
        assert_eq!(cache.get(&key(3)), Some(None));
        cache.record(key(4), Some("USES".into()));
        assert_eq!(cache.get(&key(2)), None);
        assert_eq!(cache.get(&key(4)), Some(Some("USES".into())));
    }
}
