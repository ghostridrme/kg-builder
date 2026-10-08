//! The typed-decision switch: off by default; when on, decision points ask the
//! configured decision backend first and keep the language-model path as the
//! fallback for anything below the confidence floor or any backend failure.
use crate::errors::BackendError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TypedDecisionSettings {
    pub enabled: bool,
    /// A typed answer below this probability is not used; the language-model
    /// path decides instead. 0.8: on the identity and contradiction fixtures
    /// (2026-09-28) every confident answer above it was right or a defensible
    /// abstention, with zero false merges; 0.7 admitted the first wrong
    /// contradiction verdict.
    pub min_confidence: f32,
    /// One attempt per decision; a slow or failing backend falls back to the
    /// language-model path rather than spending the component's budget.
    pub timeout_ms: u64,
    /// Concurrent decision requests across a whole engine.
    pub max_concurrent: usize,
}

impl Default for TypedDecisionSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            min_confidence: 0.8,
            timeout_ms: 5_000,
            max_concurrent: 8,
        }
    }
}

impl TypedDecisionSettings {
    pub fn validate(&self) -> Result<(), BackendError> {
        if !self.min_confidence.is_finite()
            || !(0.5..=1.0).contains(&self.min_confidence)
            || !(100..=120_000).contains(&self.timeout_ms)
            || !(1..=64).contains(&self.max_concurrent)
        {
            return Err(BackendError::Query(
                "invalid typed decision settings".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_off_and_bounds_are_enforced() {
        let settings = TypedDecisionSettings::default();
        assert!(!settings.enabled);
        assert!(settings.validate().is_ok());
        for bad in [
            TypedDecisionSettings {
                min_confidence: 0.2,
                ..Default::default()
            },
            TypedDecisionSettings {
                min_confidence: f32::NAN,
                ..Default::default()
            },
            TypedDecisionSettings {
                timeout_ms: 0,
                ..Default::default()
            },
            TypedDecisionSettings {
                max_concurrent: 0,
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err());
        }
        let parsed: TypedDecisionSettings = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed, settings);
    }
}
