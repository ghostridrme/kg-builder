//! Batched text embeddings, returned in input order.

use async_trait::async_trait;

use crate::errors::BackendError;

#[async_trait]
pub trait EmbedBackend: Send + Sync + 'static {
    /// Nonsecret effective settings used to reject incompatible run resumes.
    /// Custom backends must override this when route, defaults, fallback order or
    /// other output-affecting settings are not represented by these trait methods.
    /// Never return credentials, raw authorization headers or credential-bearing URLs.
    fn processing_descriptor(&self) -> serde_json::Value {
        serde_json::json!({"implementation": std::any::type_name::<Self>(),
            "model": self.model_id(), "dimension": self.dimension(),
            "max_batch_size": self.max_batch_size()})
    }

    /// Return one vector per input text, in input order. Callers must validate
    /// dimensions and finite, nonzero vectors before storage or similarity search.
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError>;

    /// Number of components in each returned vector.
    fn dimension(&self) -> usize;

    /// Provider batch size. An adapter may split larger requests internally.
    fn max_batch_size(&self) -> usize;

    fn model_id(&self) -> &str;

    /// False only for the disabled placeholder, which can never embed; runs
    /// that need vectors fail at their first embedding request.
    fn is_configured(&self) -> bool {
        true
    }
}

/// Placeholder for runs that must not embed anything, such as deterministic
/// Saga summaries, which carry no vectors. Any embedding request fails
/// explicitly instead of producing an empty or fabricated vector.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmbedDisabled;

#[async_trait]
impl EmbedBackend for EmbedDisabled {
    async fn embed_batch(&self, _texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
        Err(BackendError::NotConfigured(
            "no embedding provider is configured for this run".into(),
        ))
    }
    fn dimension(&self) -> usize {
        0
    }
    fn max_batch_size(&self) -> usize {
        0
    }
    fn model_id(&self) -> &str {
        "none"
    }
    fn is_configured(&self) -> bool {
        false
    }
}
