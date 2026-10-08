//! Bounded reuse of fully validated identity decisions. Keys must cover the complete decision context.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey([u8; 32]);

impl CacheKey {
    pub fn from_value(value: &Value) -> Self {
        fn bytes(hash: &mut Sha256, value: &[u8]) {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        fn canonical(hash: &mut Sha256, value: &Value) {
            match value {
                Value::Null => hash.update(b"n"),
                Value::Bool(value) => hash.update(if *value { b"t" } else { b"f" }),
                Value::Number(value) => {
                    hash.update(b"d");
                    bytes(hash, value.to_string().as_bytes());
                }
                Value::String(value) => {
                    hash.update(b"s");
                    bytes(hash, value.as_bytes());
                }
                Value::Array(values) => {
                    hash.update(b"a");
                    hash.update((values.len() as u64).to_be_bytes());
                    for value in values {
                        canonical(hash, value);
                    }
                }
                Value::Object(values) => {
                    hash.update(b"o");
                    hash.update((values.len() as u64).to_be_bytes());
                    let mut entries: Vec<_> = values.iter().collect();
                    entries.sort_unstable_by_key(|(key, _)| *key);
                    for (key, value) in entries {
                        bytes(hash, key.as_bytes());
                        canonical(hash, value);
                    }
                }
            }
        }
        let mut hash = Sha256::new();
        // Frozen protocol bytes: changing branding must not change existing evidence/cache keys.
        hash.update(b"astrolabe_sdk.identity-decision.v1");
        canonical(&mut hash, value);
        Self(hash.finalize().into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheWrite {
    pub key: CacheKey,
    pub chain_id: Uuid,
    pub version_uuid: Uuid,
}

#[derive(Default)]
struct Entries {
    values: HashMap<CacheKey, CacheWrite>,
    order: VecDeque<CacheKey>,
}

pub struct MatchingCache {
    capacity: usize,
    entries: Mutex<Entries>,
}

impl Default for MatchingCache {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            entries: Mutex::new(Entries::default()),
        }
    }
}

impl MatchingCache {
    pub fn get(&self, key: &CacheKey) -> Option<CacheWrite> {
        self.entries.lock().ok()?.values.get(key).cloned()
    }

    /// Publish only after the entire component graph and its adoptions validate.
    pub fn record(&self, write: CacheWrite) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        if !entries.values.contains_key(&write.key) {
            if entries.values.len() == self.capacity {
                if let Some(oldest) = entries.order.pop_front() {
                    entries.values.remove(&oldest);
                }
            }
            entries.order.push_back(write.key);
        }
        entries.values.insert(write.key, write);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write(value: Value) -> CacheWrite {
        CacheWrite {
            key: CacheKey::from_value(&value),
            chain_id: Uuid::new_v4(),
            version_uuid: Uuid::new_v4(),
        }
    }

    #[test]
    fn canonical_keys_preserve_types_array_order_and_every_context_field() {
        assert_eq!(
            CacheKey::from_value(&json!({"b": {"y": 2, "x": 1}, "a": 0})),
            CacheKey::from_value(&json!({"a": 0, "b": {"x": 1, "y": 2}}))
        );
        for (a, b) in [
            (json!(1), json!("1")),
            (json!([1, 2]), json!([2, 1])),
            (json!(["ab", "c"]), json!(["a", "bc"])),
        ] {
            assert_ne!(CacheKey::from_value(&a), CacheKey::from_value(&b));
        }
        let context = json!({"org":"org", "namespace":"prod", "revision":2,
            "model":"model", "prompt":"v1", "history":[1], "schema":{"id":"string"}});
        for key in context.as_object().unwrap().keys() {
            let mut changed = context.clone();
            changed[key] = Value::Null;
            assert_ne!(
                CacheKey::from_value(&context),
                CacheKey::from_value(&changed)
            );
        }
    }

    #[test]
    fn fifo_is_bounded_and_replacing_an_entry_does_not_duplicate_its_slot() {
        let cache = MatchingCache {
            capacity: 2,
            entries: Mutex::new(Entries::default()),
        };
        let first = write(json!(1));
        let second = write(json!(2));
        let third = write(json!(3));
        cache.record(first.clone());
        cache.record(second.clone());
        cache.record(CacheWrite {
            version_uuid: Uuid::new_v4(),
            ..first.clone()
        });
        cache.record(third.clone());
        assert!(cache.get(&first.key).is_none());
        assert_eq!(cache.get(&second.key), Some(second));
        assert_eq!(cache.get(&third.key), Some(third));
        assert_eq!(cache.entries.lock().unwrap().order.len(), 2);
    }

    #[test]
    fn concurrent_readers_and_writers_share_complete_entries() {
        let cache = MatchingCache::default();
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let cache = &cache;
                scope.spawn(move || {
                    for index in 0..100 {
                        let entry = write(json!([worker, index]));
                        cache.record(entry.clone());
                        assert_eq!(cache.get(&entry.key), Some(entry));
                    }
                });
            }
        });
        assert_eq!(cache.entries.lock().unwrap().values.len(), 800);
    }
}
