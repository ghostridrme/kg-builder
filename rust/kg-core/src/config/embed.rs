use secrecy::SecretString;
use serde::Deserialize;

/// Required vector provider; ingestion and search must share its effective model.
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EmbedConfig {
    #[serde(rename = "openai")]
    OpenAi {
        model: String,
        dimension: usize,
        api_key: SecretString,
    },
    Ollama {
        model: String,
        dimension: usize,
        base_url: Option<String>,
    },
    #[serde(rename = "openai_compatible")]
    OpenAiCompatible {
        model: String,
        dimension: usize,
        api_key: SecretString,
        endpoint: String,
    },
    Bedrock {
        model: String,
        dimension: usize,
        region: String,
    },
}
impl std::fmt::Debug for EmbedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let provider = match self {
            Self::OpenAi { .. } => "openai",
            Self::Ollama { .. } => "ollama",
            Self::OpenAiCompatible { .. } => "openai_compatible",
            Self::Bedrock { .. } => "bedrock",
        };
        f.debug_struct("EmbedConfig")
            .field("provider", &provider)
            .finish_non_exhaustive()
    }
}
