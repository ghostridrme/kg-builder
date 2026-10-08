//! Strict provider schemas; adapters validate and construct the configured services.

pub mod decisions;
pub mod embed;
pub mod graph;
pub mod llm;
pub use decisions::DecisionConfig;
pub use embed::EmbedConfig;
pub use graph::GraphBackendConfig;
pub use llm::LlmConfig;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unsupported_providers_and_unknown_options_are_rejected() {
        for provider in ["deepseek", "ollama", "mock"] {
            assert!(
                serde_json::from_value::<LlmConfig>(serde_json::json!({"type":provider})).is_err()
            );
        }
        assert!(serde_json::from_value::<LlmConfig>(
            serde_json::json!({"type":"disabled","max_tokens":1})
        )
        .is_err());
        assert!(serde_json::from_value::<EmbedConfig>(
            serde_json::json!({"type":"mock","dimension":3})
        )
        .is_err());
        assert!(
            serde_json::from_value::<GraphBackendConfig>(serde_json::json!({"type":"memory"}))
                .is_err()
        );
    }
    #[test]
    fn embedding_prices_are_rejected_without_reported_token_usage() {
        assert!(serde_json::from_value::<EmbedConfig>(serde_json::json!({"type":"openai","model":"m","api_key":"secret","dimension":3,"token_prices":{"input_per_million_usd":1.0,"output_per_million_usd":0.0}})).is_err());
    }

    #[test]
    fn debug_hides_secrets_endpoints_and_model_routes() {
        let sentinel = "secret-sentinel";
        let config: LlmConfig = serde_json::from_value(serde_json::json!({"type":"openai_compatible", "model":sentinel,"api_key":sentinel,"endpoint":sentinel,"context_window":4096})).unwrap();
        assert!(!format!("{config:?}").contains(sentinel));
        let config: EmbedConfig = serde_json::from_value(
            serde_json::json!({"type":"openai", "model":sentinel,"api_key":sentinel,"dimension":3}),
        )
        .unwrap();
        assert!(!format!("{config:?}").contains(sentinel));
    }
}
