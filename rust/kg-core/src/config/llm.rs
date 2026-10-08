use secrecy::SecretString;
use serde::{Deserialize, Serialize};

/// Explicit effort for OpenAI-compatible reasoning models; omission keeps the provider default.
/// Supported levels depend on the selected model and are validated by the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

/// Provider construction settings. Credentials are never included in Debug output.
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LlmConfig {
    Disabled {},
    #[serde(rename = "openai")]
    OpenAi {
        model: String,
        api_key: SecretString,
        context_window: usize,
        max_tokens: Option<u32>,
        reasoning_effort: Option<ReasoningEffort>,
        token_prices: Option<crate::telemetry::TokenPrices>,
    },
    Anthropic {
        model: String,
        api_key: SecretString,
        context_window: usize,
        max_tokens: Option<u32>,
        token_prices: Option<crate::telemetry::TokenPrices>,
    },
    #[serde(rename = "openai_compatible")]
    OpenAiCompatible {
        model: String,
        api_key: SecretString,
        endpoint: String,
        context_window: usize,
        max_tokens: Option<u32>,
        reasoning_effort: Option<ReasoningEffort>,
        token_prices: Option<crate::telemetry::TokenPrices>,
    },
    Bedrock {
        model: String,
        region: String,
        context_window: usize,
        max_tokens: Option<u32>,
        token_prices: Option<crate::telemetry::TokenPrices>,
    },
}
impl LlmConfig {
    /// Optional telemetry-only price table; excluded from processing identity.
    pub fn token_prices(&self) -> Option<&crate::telemetry::TokenPrices> {
        match self {
            Self::Disabled {} => None,
            Self::OpenAi { token_prices, .. }
            | Self::Anthropic { token_prices, .. }
            | Self::OpenAiCompatible { token_prices, .. }
            | Self::Bedrock { token_prices, .. } => token_prices.as_ref(),
        }
    }

    pub fn is_configured(&self) -> bool {
        !matches!(self, Self::Disabled {})
    }
}
impl std::fmt::Debug for LlmConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let provider = match self {
            Self::Disabled {} => "disabled",
            Self::OpenAi { .. } => "openai",
            Self::Anthropic { .. } => "anthropic",
            Self::OpenAiCompatible { .. } => "openai_compatible",
            Self::Bedrock { .. } => "bedrock",
        };
        f.debug_struct("LlmConfig")
            .field("provider", &provider)
            .finish_non_exhaustive()
    }
}
