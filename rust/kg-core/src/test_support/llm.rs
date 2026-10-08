use std::sync::{atomic::AtomicUsize, atomic::Ordering, Mutex};

use async_trait::async_trait;

use crate::errors::BackendError;
use crate::traits::llm_backend::{LlmBackend, LlmMessage, LlmResponse, MessageRole};

/// Canned, echoing, empty, or failing LLM for exercising pipeline flow.
pub struct MockLlmBackend {
    responses: Mutex<Vec<String>>,
    call_count: AtomicUsize,
    mode: MockMode,
}

enum MockMode {
    /// Return responses in order, cycling if exhausted.
    Canned,
    /// Return the last user message as the response.
    Echo,
    /// Return the empty answer of the requested schema.
    Empty,
    /// Every call fails, as a provider outage would.
    Failing,
}

impl MockLlmBackend {
    /// Returns canned responses in order. Cycles if more calls than responses.
    pub fn with_responses(responses: Vec<String>) -> Self {
        Self {
            responses: Mutex::new(responses),
            call_count: AtomicUsize::new(0),
            mode: MockMode::Canned,
        }
    }

    /// Returns the last user message as the LLM response content.
    pub fn echo() -> Self {
        Self {
            responses: Mutex::new(vec![]),
            call_count: AtomicUsize::new(0),
            mode: MockMode::Echo,
        }
    }

    /// Empty values for required fields with basic JSON types; nullable fields
    /// use null. This stub does not enforce nested schemas or value constraints.
    pub fn empty() -> Self {
        Self {
            responses: Mutex::new(vec![]),
            call_count: AtomicUsize::new(0),
            mode: MockMode::Empty,
        }
    }

    /// Every call returns an error, for outage tests.
    pub fn failing() -> Self {
        Self {
            responses: Mutex::new(vec![]),
            call_count: AtomicUsize::new(0),
            mode: MockMode::Failing,
        }
    }

    /// How many times `complete` has been called.
    pub fn call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for MockLlmBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockLlmBackend")
            .field("call_count", &self.call_count())
            .finish()
    }
}

#[async_trait]
impl LlmBackend for MockLlmBackend {
    async fn complete(
        &self,
        messages: &[LlmMessage],
        schema: Option<&serde_json::Value>,
        _max_tokens: Option<u32>,
    ) -> Result<LlmResponse, BackendError> {
        let idx = self.call_count.fetch_add(1, Ordering::Relaxed);

        let content = match &self.mode {
            MockMode::Canned => {
                let responses = self.responses.lock().unwrap();
                if responses.is_empty() {
                    "{}".to_string()
                } else {
                    responses[idx % responses.len()].clone()
                }
            }
            MockMode::Echo => messages
                .iter()
                .rev()
                .find(|m| m.role == MessageRole::User)
                .map(|m| m.content.clone())
                .unwrap_or_else(|| "{}".to_string()),
            MockMode::Empty => empty_answer(schema).to_string(),
            MockMode::Failing => {
                return Err(BackendError::Unavailable(
                    "mock LLM configured to fail (simulated outage)".into(),
                ));
            }
        };

        Ok(LlmResponse {
            status: crate::traits::llm_backend::CompletionStatus::Complete,
            content,
            model: "mock".to_string(),
            input_tokens: Some(0),
            output_tokens: Some(0),
        })
    }

    /// One canned answer per consumed attempt; the mock never retries or falls back.
    async fn complete_bounded(
        &self,
        messages: &[LlmMessage],
        schema: Option<&serde_json::Value>,
        max_tokens: Option<u32>,
        budget: &crate::traits::llm_backend::CallBudget,
    ) -> Result<LlmResponse, BackendError> {
        budget.consume()?;
        self.complete(messages, schema, max_tokens).await
    }

    fn supports_bounded_attempts(&self) -> bool {
        true
    }

    fn model_id(&self) -> &str {
        "mock-llm"
    }

    fn context_window(&self) -> usize {
        128_000
    }
}

/// The empty answer of a JSON schema: an object whose required properties
/// hold their empty values.
fn empty_answer(schema: Option<&serde_json::Value>) -> serde_json::Value {
    let mut answer = serde_json::Map::new();
    let Some(schema) = schema else {
        return serde_json::Value::Object(answer);
    };
    let properties = schema.get("properties");
    for name in schema
        .get("required")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
    {
        let kind = properties
            .and_then(|p| p.get(name))
            .and_then(|p| p.get("type"))
            .map(|t| match t {
                serde_json::Value::Array(types) => types
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .find(|t| *t == "null")
                    .or_else(|| types.first().and_then(serde_json::Value::as_str))
                    .unwrap_or("null")
                    .to_string(),
                other => other.as_str().unwrap_or("null").to_string(),
            })
            .unwrap_or_else(|| "null".to_string());
        let value = match kind.as_str() {
            "array" => serde_json::json!([]),
            "object" => serde_json::json!({}),
            "string" => serde_json::json!(""),
            "boolean" => serde_json::json!(false),
            "integer" | "number" => serde_json::json!(0),
            _ => serde_json::Value::Null,
        };
        answer.insert(name.to_string(), value);
    }
    serde_json::Value::Object(answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn canned_responses() {
        let backend = MockLlmBackend::with_responses(vec![
            r#"{"entities":[]}"#.into(),
            r#"{"edges":[]}"#.into(),
        ]);

        let msg = vec![LlmMessage {
            role: MessageRole::User,
            content: "extract entities".into(),
        }];

        let r1 = backend.complete(&msg, None, None).await.unwrap();
        assert_eq!(r1.content, r#"{"entities":[]}"#);

        let r2 = backend.complete(&msg, None, None).await.unwrap();
        assert_eq!(r2.content, r#"{"edges":[]}"#);

        let r3 = backend.complete(&msg, None, None).await.unwrap();
        assert_eq!(r3.content, r#"{"entities":[]}"#);

        assert_eq!(backend.call_count(), 3);
    }

    #[tokio::test]
    async fn echo_mode() {
        let backend = MockLlmBackend::echo();
        let msg = vec![LlmMessage {
            role: MessageRole::User,
            content: "hello world".into(),
        }];

        let r = backend.complete(&msg, None, None).await.unwrap();
        assert_eq!(r.content, "hello world");
    }

    #[tokio::test]
    async fn empty_mode_answers_the_requested_schema() {
        let backend = MockLlmBackend::empty();
        let msg = vec![LlmMessage {
            role: MessageRole::User,
            content: "anything".into(),
        }];

        let r = backend.complete(&msg, None, None).await.unwrap();
        assert_eq!(r.content, "{}");
        let schema = serde_json::json!({
            "type": "object",
            "required": ["entities", "same", "target_index", "label"],
            "properties": {
                "entities": { "type": "array" },
                "same": { "type": "boolean" },
                "target_index": { "type": ["integer", "null"] },
                "label": { "type": "string" }
            }
        });
        let r = backend.complete(&msg, Some(&schema), None).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&r.content).unwrap(),
            serde_json::json!({"entities": [], "same": false, "target_index": null, "label": ""})
        );
    }
}
