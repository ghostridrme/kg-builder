//! [`ExecutionConfig`] — pipeline failure-handling behavior.

use serde::{Deserialize, Serialize};

/// Snapshot failure handling; backend retries are configured separately.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// `false` aborts on snapshot failure; `true` records failures and continues.
    /// Neither mode rolls back earlier writes.
    #[serde(default)]
    pub continue_on_step_error: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_fail_fast() {
        assert!(!ExecutionConfig::default().continue_on_step_error);
    }

    #[test]
    fn unknown_execution_settings_are_rejected() {
        let parsed = serde_json::from_value::<ExecutionConfig>(serde_json::json!({
            "continue_on_step_error": true, "enable_retries": true
        }));
        assert!(parsed.is_err());
    }
}
