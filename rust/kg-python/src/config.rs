//! Host settings for deterministic graph ingestion.
use kg_core::config::{EmbedConfig, GraphBackendConfig, LlmConfig};
use kg_stages::IngestionSettings;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    pub graph: GraphBackendConfig,
    #[serde(default)]
    pub models: Models,
    pub embedder: Option<EmbedConfig>,
    #[serde(default)]
    pub processing: IngestionSettings,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Models {
    pub default: LlmConfig,
    pub decisions: Option<kg_core::config::DecisionConfig>,
    pub extraction: Option<LlmConfig>,
    pub disambiguation: Option<LlmConfig>,
    pub edge_discovery: Option<LlmConfig>,
}
impl Default for Models {
    fn default() -> Self {
        Self {
            default: LlmConfig::Disabled {},
            decisions: None,
            extraction: None,
            disambiguation: None,
            edge_discovery: None,
        }
    }
}
impl ApplicationConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.embedder.is_some()
            || self.models.default.is_configured()
            || [
                &self.models.extraction,
                &self.models.disambiguation,
                &self.models.edge_discovery,
            ]
            .into_iter()
            .any(|m| m.as_ref().is_some_and(LlmConfig::is_configured))
            || self
                .models
                .decisions
                .as_ref()
                .is_some_and(|m| !matches!(m, kg_core::config::DecisionConfig::Disabled {}))
        {
            return Err("kg-builder requires disabled models and no embedder".into());
        }
        self.processing.validate().map_err(|e| e.to_string())?;
        kg_storage_neo4j::Neo4jSettings::from_config(&self.graph)
            .map_err(|_| "invalid graph configuration")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn graph_only_configuration_needs_no_provider_and_rejects_model_options() {
        let base = serde_json::json!({"graph":{"type":"neo4j","uri":"bolt://localhost:7687","username":"neo4j","password":"local-test"}});
        let config: ApplicationConfig = serde_json::from_value(base.clone()).unwrap();
        config.validate().unwrap();
        for (field, value) in [
            (
                "embedder",
                serde_json::json!({"type":"openai","model":"text-embedding-3-small","dimension":1536,"api_key":"test"}),
            ),
            (
                "models",
                serde_json::json!({"default":{"type":"openai","model":"gpt-4o-mini","api_key":"test","context_window":128000}}),
            ),
            ("processing", serde_json::json!({"recipe":"full"})),
        ] {
            let mut raw = base.clone();
            raw[field] = value;
            if let Ok(config) = serde_json::from_value::<ApplicationConfig>(raw) {
                assert!(config.validate().is_err());
            }
        }
    }
}
