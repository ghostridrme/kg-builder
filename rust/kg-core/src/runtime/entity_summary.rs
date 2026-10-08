//! Bounded, awaited summary follow-ups over accepted graph evidence.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryMode {
    #[default]
    Deterministic,
    Model,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EntitySummarySettings {
    pub enabled: bool,
    pub batch_size: usize,
    pub mode: SummaryMode,
    pub max_evidence: usize,
    pub max_text_chars: usize,
    pub max_replans: u32,
    pub timeout_ms: u64,
    pub max_concurrent_batches: usize,
}
impl Default for EntitySummarySettings {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 16,
            mode: SummaryMode::Deterministic,
            max_evidence: 256,
            max_text_chars: 4096,
            max_replans: 3,
            timeout_ms: 60_000,
            max_concurrent_batches: 2,
        }
    }
}
impl EntitySummarySettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=256).contains(&self.batch_size)
            || !(1..=4096).contains(&self.max_evidence)
            || !(1..=4096).contains(&self.max_text_chars)
            || self.max_replans > 16
            || !(1..=600_000).contains(&self.timeout_ms)
            || !(1..=16).contains(&self.max_concurrent_batches)
        {
            return Err("invalid entity summary bounds".into());
        }
        Ok(())
    }
}
