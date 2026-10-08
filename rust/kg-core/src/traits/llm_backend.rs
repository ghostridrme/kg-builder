//! LLM completion and typed response decoding.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::errors::BackendError;

/// Role of a message in an LLM conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// System instruction (trusted — never carries source data unfenced).
    System,
    /// User-turn content (untrusted source data goes here, fenced via
    /// [`crate::sanitize::fence_untrusted`]).
    User,
    /// A prior model response in the conversation.
    Assistant,
}

/// A single message in an LLM conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: MessageRole,
    pub content: String,
}

/// Completion status is independent of whether the returned text parses as JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionStatus {
    Complete,
    Truncated,
    Refused,
}

/// Response from an LLM completion call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResponse {
    pub status: CompletionStatus,
    /// Provider output text. A requested schema is not proof of valid JSON.
    pub content: String,
    /// Which model actually served the call (FallbackLlm may switch).
    pub model: String,
    /// Prompt tokens consumed, when the provider reports them.
    pub input_tokens: Option<u32>,
    /// Completion tokens produced, when the provider reports them.
    pub output_tokens: Option<u32>,
}

/// A request-scoped allowance of actual provider attempts. Adapters consume one
/// unit immediately before every HTTP dispatch — first attempt, retry or
/// fallback alike — so a caller that hands the same budget to a whole
/// completion knows exactly how many wire attempts it can have caused.
/// `consumed` keeps counting even when the allowance is refused, so callers
/// can report every dispatch that actually happened.
#[derive(Debug)]
pub struct CallBudget {
    remaining: std::sync::atomic::AtomicU32,
    consumed: std::sync::atomic::AtomicU32,
}

impl CallBudget {
    /// An allowance of `attempts` actual provider dispatches; zero is a valid,
    /// already-exhausted budget.
    pub fn new(attempts: u32) -> Self {
        Self {
            remaining: std::sync::atomic::AtomicU32::new(attempts),
            consumed: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Exactly one actual provider attempt.
    pub fn single() -> Self {
        Self::new(1)
    }

    /// Take one attempt, or fail without dispatching when none is left.
    pub fn consume(&self) -> Result<(), BackendError> {
        use std::sync::atomic::Ordering;
        let mut current = self.remaining.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return Err(BackendError::AttemptBudgetExhausted);
            }
            match self.remaining.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.consumed.fetch_add(1, Ordering::AcqRel);
                    return Ok(());
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Attempts still allowed.
    pub fn remaining(&self) -> u32 {
        self.remaining.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Attempts actually dispatched (or handed to a transport) so far.
    pub fn consumed(&self) -> u32 {
        self.consumed.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Object-safe LLM provider. Callers own concurrency and cancellation;
/// pipeline callers acquire the corresponding runtime semaphore.
#[async_trait]
pub trait LlmBackend: Send + Sync + 'static {
    /// Nonsecret effective settings used to reject incompatible run resumes.
    /// Custom backends must override this when route, defaults, fallback order or
    /// other output-affecting settings are not represented by these trait methods.
    /// Never return credentials, raw authorization headers or credential-bearing URLs.
    fn processing_descriptor(&self) -> serde_json::Value {
        serde_json::json!({"implementation": std::any::type_name::<Self>(),
            "model": self.model_id(), "context_window": self.context_window(),
            "configured": self.is_configured()})
    }

    /// Complete a conversation, optionally with a JSON schema for structured output.
    async fn complete(
        &self,
        messages: &[LlmMessage],
        schema: Option<&serde_json::Value>,
        max_tokens: Option<u32>,
    ) -> Result<LlmResponse, BackendError>;

    /// Complete under a request-scoped attempt allowance. An implementation
    /// must consume one unit of `budget` immediately before every actual
    /// provider dispatch, including its own retries and any fallback backend,
    /// and must stop instead of dispatching when the allowance is spent. It
    /// must never repair, reformat or re-ask on the caller's behalf.
    ///
    /// Custom backends do not inherit that guarantee: the default refuses, so a
    /// caller that requires bounded spending fails closed instead of trusting
    /// an adapter that may retry internally. Implementations that satisfy the
    /// contract override this and [`Self::supports_bounded_attempts`].
    async fn complete_bounded(
        &self,
        _messages: &[LlmMessage],
        _schema: Option<&serde_json::Value>,
        _max_tokens: Option<u32>,
        _budget: &CallBudget,
    ) -> Result<LlmResponse, BackendError> {
        Err(BackendError::NotConfigured(
            "this language model backend does not declare bounded provider attempts".into(),
        ))
    }

    /// Whether [`Self::complete_bounded`] honors the attempt allowance for
    /// every dispatch path. Callers check this before paid work so an
    /// unsupported configuration is rejected up front, never at spend time.
    fn supports_bounded_attempts(&self) -> bool {
        false
    }

    /// Configured model identifier; a fallback may serve the response.
    fn model_id(&self) -> &str;

    /// Maximum context window in tokens.
    fn context_window(&self) -> usize;

    /// Whether calls can be served. `LlmDisabled` answers false, so settings
    /// that need a model are rejected at construction, not at the first call.
    fn is_configured(&self) -> bool {
        true
    }
}

/// The backend for runs configured without a language model. The policy
/// must route every decision to its heuristic path; a call that still reaches
/// this backend fails explicitly instead of returning empty output.
#[derive(Debug, Default, Clone, Copy)]
pub struct LlmDisabled;

#[async_trait]
impl LlmBackend for LlmDisabled {
    async fn complete(
        &self,
        _messages: &[LlmMessage],
        _schema: Option<&serde_json::Value>,
        _max_tokens: Option<u32>,
    ) -> Result<LlmResponse, BackendError> {
        Err(BackendError::NotConfigured(
            "no language model is configured for this run".into(),
        ))
    }

    fn model_id(&self) -> &str {
        "none"
    }

    fn context_window(&self) -> usize {
        0
    }

    fn is_configured(&self) -> bool {
        false
    }
}

/// Decode a completion as JSON. Callers provide concurrency limits and validate domain rules.
pub async fn complete_as<T: DeserializeOwned>(
    backend: &dyn LlmBackend,
    messages: &[LlmMessage],
    max_tokens: Option<u32>,
) -> Result<T, BackendError> {
    let response = backend.complete(messages, None, max_tokens).await?;
    match response.status {
        CompletionStatus::Complete => {}
        CompletionStatus::Truncated => return Err(BackendError::IncompleteResponse),
        CompletionStatus::Refused => return Err(BackendError::Refused),
    }
    serde_json::from_str(&response.content).map_err(|e| {
        BackendError::Deserialization(format!(
            "LLM response does not match the requested type ({:?}, line {}, column {})",
            e.classify(),
            e.line(),
            e.column()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_counts_every_consumed_attempt_and_refuses_when_spent() {
        let budget = CallBudget::single();
        assert_eq!((budget.remaining(), budget.consumed()), (1, 0));
        budget.consume().unwrap();
        assert_eq!((budget.remaining(), budget.consumed()), (0, 1));
        assert!(matches!(
            budget.consume(),
            Err(BackendError::AttemptBudgetExhausted)
        ));
        assert_eq!(budget.consumed(), 1, "a refused attempt is not a dispatch");
        assert!(!BackendError::AttemptBudgetExhausted.is_transient());
        let empty = CallBudget::new(0);
        assert!(empty.consume().is_err());
    }

    #[tokio::test]
    async fn custom_backends_do_not_inherit_a_bounded_attempt_guarantee() {
        assert!(!LlmDisabled.supports_bounded_attempts());
        let error = LlmDisabled
            .complete_bounded(&[], None, None, &CallBudget::single())
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::NotConfigured(_)));
    }

    // ---- merged from `mod tests`

    use crate::test_support::MockLlmBackend;

    #[tokio::test]
    async fn typed_response_errors_do_not_echo_model_content() {
        let secret = "private-model-output-sentinel";
        let backend = MockLlmBackend::with_responses(vec![format!("\"{secret}\"")]);
        let error = complete_as::<u64>(&backend, &[], None).await.unwrap_err();
        assert!(matches!(error, BackendError::Deserialization(_)));
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
        assert!(error.to_string().contains("line 1"));
    }

    // ---- merged from `mod status_tests`

    struct Backend(CompletionStatus);
    #[async_trait]
    impl LlmBackend for Backend {
        async fn complete(
            &self,
            _: &[LlmMessage],
            _: Option<&serde_json::Value>,
            _: Option<u32>,
        ) -> Result<LlmResponse, BackendError> {
            Ok(LlmResponse {
                status: self.0,
                content: "42".into(),
                model: "test".into(),
                input_tokens: None,
                output_tokens: None,
            })
        }
        fn model_id(&self) -> &str {
            "test"
        }
        fn context_window(&self) -> usize {
            100
        }
    }

    #[tokio::test]
    async fn valid_json_does_not_override_completion_status() {
        assert!(matches!(
            complete_as::<u64>(&Backend(CompletionStatus::Truncated), &[], None).await,
            Err(BackendError::IncompleteResponse)
        ));
        assert!(matches!(
            complete_as::<u64>(&Backend(CompletionStatus::Refused), &[], None).await,
            Err(BackendError::Refused)
        ));
        assert_eq!(
            complete_as::<u64>(&Backend(CompletionStatus::Complete), &[], None)
                .await
                .unwrap(),
            42
        );
    }
}
