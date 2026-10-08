//! Chunk-local reuse of validated incoming vectors while graph identity is replanned.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use sha2::{Digest, Sha256};

use crate::embedding::{validate_vectors, EmbeddingSettings};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EmbeddingCacheKey([u8; 32]);

impl EmbeddingCacheKey {
    pub fn new(org: &str, namespace: &str, settings: &EmbeddingSettings, text: &str) -> Self {
        Self::for_kind("entity", org, namespace, settings, text)
    }

    pub fn for_relationship(
        org: &str,
        namespace: &str,
        settings: &EmbeddingSettings,
        text: &str,
    ) -> Self {
        Self::for_kind("relationship", org, namespace, settings, text)
    }

    pub fn for_summary(
        org: &str,
        namespace: &str,
        settings: &EmbeddingSettings,
        text: &str,
    ) -> Self {
        Self::for_kind("derived_summary", org, namespace, settings, text)
    }

    fn for_kind(
        kind: &str,
        org: &str,
        namespace: &str,
        settings: &EmbeddingSettings,
        text: &str,
    ) -> Self {
        let mut hash = Sha256::new();
        // Frozen protocol bytes: changing branding must not change existing evidence/cache keys.
        hash.update(b"astrolabe_sdk.incoming-embedding.v1");
        for part in [
            kind,
            org,
            namespace,
            &settings.model,
            &settings.text_version,
            text,
        ] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        hash.update((settings.dimension as u64).to_be_bytes());
        Self(hash.finalize().into())
    }
}

#[derive(Default)]
struct Entries {
    values: HashMap<EmbeddingCacheKey, Box<[f32]>>,
    order: VecDeque<EmbeddingCacheKey>,
    bytes: usize,
}

/// At most 16 MiB of vector data and 1,024 fixed-size keys. No source text is retained.
/// The runner creates a fresh instance for each node chunk, shared only by its retries.
pub struct IncomingEmbeddingCache {
    max_entries: usize,
    max_bytes: usize,
    entries: Mutex<Entries>,
}

impl Default for IncomingEmbeddingCache {
    fn default() -> Self {
        Self {
            max_entries: 1024,
            max_bytes: 16 * 1024 * 1024,
            entries: Mutex::new(Entries::default()),
        }
    }
}

impl IncomingEmbeddingCache {
    pub fn get(&self, key: &EmbeddingCacheKey) -> Option<Vec<f32>> {
        self.entries
            .lock()
            .ok()?
            .values
            .get(key)
            .map(|values| values.to_vec())
    }

    /// Invalid or oversized vectors are never retained; cache failures are harmless misses.
    pub fn record(&self, key: EmbeddingCacheKey, settings: &EmbeddingSettings, values: Vec<f32>) {
        if validate_vectors(settings, 1, std::slice::from_ref(&values)).is_err() {
            return;
        }
        let bytes = values.len().saturating_mul(std::mem::size_of::<f32>());
        if bytes > self.max_bytes || self.max_entries == 0 {
            return;
        }
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        if entries.values.contains_key(&key) {
            return;
        }
        while entries.values.len() >= self.max_entries || entries.bytes > self.max_bytes - bytes {
            let Some(oldest) = entries.order.pop_front() else {
                return;
            };
            if let Some(old) = entries.values.remove(&oldest) {
                entries.bytes -= old.len() * std::mem::size_of::<f32>();
            }
        }
        entries.bytes += bytes;
        entries.order.push_back(key);
        entries.values.insert(key, values.into_boxed_slice());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> EmbeddingSettings {
        EmbeddingSettings {
            entity_fields: Default::default(),
            model: "test".into(),
            dimension: 2,
            text_version: "1".into(),
        }
    }

    #[test]
    fn compatibility_covers_scope_settings_and_exact_text() {
        let cache = IncomingEmbeddingCache::default();
        let settings = settings();
        let key = EmbeddingCacheKey::new("org", "ns", &settings, "text");
        cache.record(key, &settings, vec![1.0, 2.0]);
        assert!(cache
            .get(&EmbeddingCacheKey::for_relationship(
                "org", "ns", &settings, "text"
            ))
            .is_none());
        assert_eq!(cache.get(&key), Some(vec![1.0, 2.0]));
        for (org, ns, text) in [
            ("other", "ns", "text"),
            ("org", "other", "text"),
            ("org", "ns", "text "),
        ] {
            assert!(cache
                .get(&EmbeddingCacheKey::new(org, ns, &settings, text))
                .is_none());
        }
        for changed in [
            EmbeddingSettings {
                model: "other".into(),
                ..settings.clone()
            },
            EmbeddingSettings {
                dimension: 3,
                ..settings.clone()
            },
            EmbeddingSettings {
                text_version: "2".into(),
                ..settings.clone()
            },
        ] {
            assert!(cache
                .get(&EmbeddingCacheKey::new("org", "ns", &changed, "text"))
                .is_none());
        }
        assert!(IncomingEmbeddingCache::default().get(&key).is_none());
    }

    #[test]
    fn invalid_vectors_and_oversized_values_are_not_cached() {
        let cache = IncomingEmbeddingCache {
            max_bytes: 8,
            ..Default::default()
        };
        let settings = settings();
        let key = EmbeddingCacheKey::new("org", "ns", &settings, "text");
        for invalid in [
            vec![],
            vec![1.0],
            vec![0.0, 0.0],
            vec![f32::NAN, 1.0],
            vec![f32::INFINITY, 1.0],
        ] {
            cache.record(key, &settings, invalid);
            assert!(cache.get(&key).is_none());
        }
        let wide = EmbeddingSettings {
            dimension: 3,
            ..settings
        };
        cache.record(key, &wide, vec![1.0; 3]);
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn fifo_eviction_respects_entry_and_vector_byte_caps() {
        for (max_entries, max_bytes) in [(2, 100), (100, 16)] {
            let cache = IncomingEmbeddingCache {
                max_entries,
                max_bytes,
                entries: Mutex::default(),
            };
            let settings = settings();
            let keys: Vec<_> = ["a", "b", "c"]
                .iter()
                .map(|text| EmbeddingCacheKey::new("org", "ns", &settings, text))
                .collect();
            for key in &keys {
                cache.record(*key, &settings, vec![1.0, 2.0]);
            }
            assert!(cache.get(&keys[0]).is_none());
            assert!(cache.get(&keys[1]).is_some());
            assert!(cache.get(&keys[2]).is_some());
            assert_eq!(cache.entries.lock().unwrap().bytes, 16);
        }
    }
}
