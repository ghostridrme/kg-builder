//! Provider-bound contract assertions shared by stage unit tests.
use kg_core::{
    errors::BackendError,
    runtime::extraction::ExtractionSettings,
    traits::llm_backend::{CallBudget, LlmBackend, LlmMessage, LlmResponse, MessageRole},
};
use serde_json::Value;
use std::sync::{Arc, Mutex};

type RecordedRequest = (Vec<LlmMessage>, Option<Value>, Option<u32>);

pub(crate) struct RecordingModel {
    inner: Arc<dyn LlmBackend>,
    descriptor: Value,
    calls: Mutex<Vec<RecordedRequest>>,
}
impl RecordingModel {
    pub(crate) fn new(inner: Arc<dyn LlmBackend>) -> Arc<Self> {
        let descriptor = inner.processing_descriptor();
        Self::with_descriptor(inner, descriptor)
    }
    pub(crate) fn with_descriptor(inner: Arc<dyn LlmBackend>, descriptor: Value) -> Arc<Self> {
        Arc::new(Self {
            inner,
            descriptor,
            calls: Mutex::new(Vec::new()),
        })
    }
    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.calls.lock().unwrap().clone()
    }
    pub(crate) fn assert_guidance(&self, required: &[&str], forbidden: &[&str]) {
        let calls = self.calls.lock().unwrap();
        assert!(
            !calls.is_empty(),
            "the contract must exercise provider dispatch"
        );
        for (messages, schema, tokens) in calls.iter() {
            assert!(schema.as_ref().is_some_and(Value::is_object));
            assert!(tokens.is_some_and(|n| n > 0));
            let system = messages
                .iter()
                .filter(|m| m.role == MessageRole::System)
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            for marker in required {
                assert_eq!(system.matches(marker).count(), 1, "{marker}: {system}");
            }
            for marker in forbidden {
                assert!(!system.contains(marker), "{marker}: {system}");
            }
            for pair in required.windows(2) {
                assert!(system.find(pair[0]).unwrap() < system.find(pair[1]).unwrap());
            }
        }
    }
}
#[async_trait::async_trait]
impl LlmBackend for RecordingModel {
    async fn complete(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        tokens: Option<u32>,
    ) -> Result<LlmResponse, BackendError> {
        self.calls
            .lock()
            .unwrap()
            .push((messages.to_vec(), schema.cloned(), tokens));
        self.inner.complete(messages, schema, tokens).await
    }
    fn supports_bounded_attempts(&self) -> bool {
        self.inner.supports_bounded_attempts()
    }
    async fn complete_bounded(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        tokens: Option<u32>,
        budget: &CallBudget,
    ) -> Result<LlmResponse, BackendError> {
        self.calls
            .lock()
            .unwrap()
            .push((messages.to_vec(), schema.cloned(), tokens));
        self.inner
            .complete_bounded(messages, schema, tokens, budget)
            .await
    }
    fn processing_descriptor(&self) -> Value {
        self.descriptor.clone()
    }
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn context_window(&self) -> usize {
        self.inner.context_window()
    }
}
pub(crate) fn settings() -> ExtractionSettings {
    ExtractionSettings {
        shared_instructions: Some("SHARED_CONTEXT_MARKER".into()),
        instructions: Some("ENTITY_TASK_MARKER".into()),
        relationship_instructions: Some("RELATION_TASK_MARKER".into()),
        identity_instructions: Some("MATCH_TASK_MARKER".into()),
        summary_instructions: Some("SUMMARY_TASK_MARKER".into()),
        ..Default::default()
    }
}

pub(crate) fn configure(ctx: &mut kg_core::runtime::RuntimeContext) {
    let guidance = settings();
    ctx.extraction_settings.shared_instructions = guidance.shared_instructions;
    ctx.extraction_settings.instructions = guidance.instructions;
    ctx.extraction_settings.relationship_instructions = guidance.relationship_instructions;
    ctx.extraction_settings.identity_instructions = guidance.identity_instructions;
    ctx.extraction_settings.summary_instructions = guidance.summary_instructions;
}
