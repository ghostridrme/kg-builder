//! Resource limits for contextual identity matching.

use crate::errors::BackendError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MatchingSettings {
    /// Additional identity attempts after a confirmed stale decision.
    pub max_replans: u32,
    /// Total resolution/planning budget per chunk; an active commit is always awaited.
    pub replan_timeout_ms: u64,
    /// Budget for embedding preparation or one component comparison, including permit waits.
    pub timeout_ms: u64,
    /// Concurrent component retrievals or decisions within one chunk.
    pub max_concurrent_components: usize,
    /// Maximum compatible identity components in one model request; 1 disables
    /// batching. Default 8: one request carries the shared rules and context for
    /// up to eight components (measured 2026-09-28 on the development split:
    /// identical decisions, about a quarter of the identity input tokens).
    pub max_batch_components: usize,
    /// Output token cap for a multi-component identity response.
    pub max_batch_output_tokens: u32,
    pub max_batch_response_bytes: usize,
    pub max_output_tokens: u32,
    pub max_response_bytes: usize,
    pub max_prompt_bytes: usize,
    pub candidate_limit: usize,
    /// Maximum expanded frontier across name, vector, and property sources.
    pub max_candidate_limit: usize,
    /// Cosine floor for a stored or incoming entity to be offered to a text
    /// mention (no authoritative keys) on vector evidence alone; name and
    /// property evidence still qualify a candidate below it. A mention with no
    /// candidate above the floor and no lexical evidence is a new chain without
    /// a model decision. The default floor is 0.6. Keyed
    /// components keep their complete ranked frontier.
    pub candidate_min_similarity: f32,
}

impl Default for MatchingSettings {
    fn default() -> Self {
        Self {
            max_replans: 3,
            replan_timeout_ms: 180_000,
            timeout_ms: 60_000,
            max_concurrent_components: 4,
            max_batch_components: 8,
            max_batch_output_tokens: 8192,
            max_batch_response_bytes: 131_072,
            max_output_tokens: 1024,
            max_response_bytes: 16_384,
            max_prompt_bytes: 262_144,
            candidate_limit: 15,
            max_candidate_limit: 100,
            candidate_min_similarity: 0.6,
        }
    }
}

impl MatchingSettings {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.max_replans > 10
            || !(1..=900_000).contains(&self.replan_timeout_ms)
            || !(1..=300_000).contains(&self.timeout_ms)
            || !(1..=32).contains(&self.max_concurrent_components)
            || !(1..=32).contains(&self.max_batch_components)
            || !(1..=32_768).contains(&self.max_batch_output_tokens)
            || !(1..=1_048_576).contains(&self.max_batch_response_bytes)
            || self.max_batch_output_tokens < self.max_output_tokens
            || !(1..=8192).contains(&self.max_output_tokens)
            || !(1..=1_048_576).contains(&self.max_response_bytes)
            || !(1..=4_194_304).contains(&self.max_prompt_bytes)
            || !(1..=100).contains(&self.candidate_limit)
            || self.max_candidate_limit < self.candidate_limit
            || self.max_candidate_limit > 100
            || !self.candidate_min_similarity.is_finite()
            || !(-1.0..=1.0).contains(&self.candidate_min_similarity)
        {
            return Err(BackendError::Query("invalid matching settings".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_limits_reject_unbounded_values() {
        assert!(MatchingSettings::default().validate().is_ok());
        for limit in [1, 32] {
            assert!(MatchingSettings {
                max_concurrent_components: limit,
                ..Default::default()
            }
            .validate()
            .is_ok());
        }
        for settings in [
            MatchingSettings {
                max_batch_components: 0,
                ..Default::default()
            },
            MatchingSettings {
                max_batch_components: 33,
                ..Default::default()
            },
            MatchingSettings {
                max_batch_output_tokens: 0,
                ..Default::default()
            },
            MatchingSettings {
                max_batch_output_tokens: 32_769,
                ..Default::default()
            },
            MatchingSettings {
                max_batch_response_bytes: 0,
                ..Default::default()
            },
            MatchingSettings {
                max_concurrent_components: 0,
                ..Default::default()
            },
            MatchingSettings {
                max_concurrent_components: 33,
                ..Default::default()
            },
            MatchingSettings {
                max_replans: 11,
                ..Default::default()
            },
            MatchingSettings {
                replan_timeout_ms: 0,
                ..Default::default()
            },
            MatchingSettings {
                timeout_ms: 0,
                ..Default::default()
            },
            MatchingSettings {
                candidate_limit: 101,
                ..Default::default()
            },
            MatchingSettings {
                max_prompt_bytes: 0,
                ..Default::default()
            },
            MatchingSettings {
                candidate_min_similarity: 1.5,
                ..Default::default()
            },
            MatchingSettings {
                candidate_min_similarity: f32::NAN,
                ..Default::default()
            },
        ] {
            assert!(settings.validate().is_err());
        }
    }
}
