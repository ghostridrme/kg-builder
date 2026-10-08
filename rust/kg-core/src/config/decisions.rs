use secrecy::SecretString;
use serde::Deserialize;

/// Typed-decision provider settings. Credentials never appear in Debug output.
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionConfig {
    Disabled {},
    /// A System One endpoint (TypeSafe's `/v1/systemone` contract).
    #[serde(rename = "system_one")]
    SystemOne {
        /// Model name; the provider's default when omitted.
        model: Option<String>,
        /// Read from `JEV_API_KEY` when omitted.
        api_key: Option<SecretString>,
        /// Complete endpoint URL; the provider's public endpoint when omitted.
        endpoint: Option<String>,
    },
}

impl DecisionConfig {
    pub fn is_configured(&self) -> bool {
        !matches!(self, Self::Disabled {})
    }
}

impl std::fmt::Debug for DecisionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled {} => f.write_str("DecisionConfig(disabled)"),
            Self::SystemOne {
                model, endpoint, ..
            } => f
                .debug_struct("DecisionConfig(system_one)")
                .field("model", model)
                .field("endpoint", endpoint)
                .finish_non_exhaustive(),
        }
    }
}
