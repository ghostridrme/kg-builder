//! One metering wrapper for paid evaluations: counts calls, sums reported
//! tokens, records every response, and refuses calls past an optional cap.
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::errors::BackendError;
use crate::traits::llm_backend::{CallBudget, LlmBackend, LlmMessage, LlmResponse};

/// Wraps a real provider adapter so an evaluation can report calls, tokens and
/// per-call records without every harness re-implementing the counter.
pub struct MeteredLlm {
    inner: Arc<dyn LlmBackend>,
    cap: Option<usize>,
    calls: AtomicUsize,
    input: AtomicU64,
    output: AtomicU64,
    records: Mutex<Vec<Value>>,
}

impl MeteredLlm {
    /// `cap`: maximum calls forwarded; further calls fail with a query error so a
    /// runaway evaluation cannot spend past its budget.
    pub fn new(inner: Arc<dyn LlmBackend>, cap: Option<usize>) -> Self {
        Self {
            inner,
            cap,
            calls: AtomicUsize::new(0),
            input: AtomicU64::new(0),
            output: AtomicU64::new(0),
            records: Mutex::new(Vec::new()),
        }
    }
    /// Calls attempted, including those refused by the cap.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    pub fn input_tokens(&self) -> u64 {
        self.input.load(Ordering::SeqCst)
    }
    pub fn output_tokens(&self) -> u64 {
        self.output.load(Ordering::SeqCst)
    }
    pub fn tokens(&self) -> (u64, u64) {
        (self.input_tokens(), self.output_tokens())
    }
    /// One record per forwarded call: `ok`, `status`, `content`, token counts, `elapsed_ms`.
    pub fn records(&self) -> Vec<Value> {
        self.records.lock().unwrap().clone()
    }
    /// The `{status, content}` of every successful response, in call order.
    pub fn responses(&self) -> Vec<Value> {
        self.records()
            .into_iter()
            .filter(|r| r["ok"] == json!(true))
            .map(|r| json!({"status": r["status"], "content": r["content"]}))
            .collect()
    }
}

impl MeteredLlm {
    async fn measured_call(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        max_tokens: Option<u32>,
        budget: Option<&CallBudget>,
    ) -> Result<LlmResponse, BackendError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.cap.is_some_and(|cap| n >= cap) {
            return Err(BackendError::Query("evaluation call cap reached".into()));
        }
        let started = std::time::Instant::now();
        let result = match budget {
            Some(budget) => {
                self.inner
                    .complete_bounded(messages, schema, max_tokens, budget)
                    .await
            }
            None => self.inner.complete(messages, schema, max_tokens).await,
        };
        let record = match &result {
            Ok(r) => {
                self.input
                    .fetch_add(u64::from(r.input_tokens.unwrap_or(0)), Ordering::SeqCst);
                self.output
                    .fetch_add(u64::from(r.output_tokens.unwrap_or(0)), Ordering::SeqCst);
                json!({"ok": true, "status": format!("{:?}", r.status), "content": r.content,
                    "input_tokens": r.input_tokens, "output_tokens": r.output_tokens,
                    "elapsed_ms": started.elapsed().as_millis()})
            }
            Err(_) => json!({"ok": false, "elapsed_ms": started.elapsed().as_millis()}),
        };
        self.records.lock().unwrap().push(record);
        result
    }
}

#[async_trait]
impl LlmBackend for MeteredLlm {
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn context_window(&self) -> usize {
        self.inner.context_window()
    }
    fn is_configured(&self) -> bool {
        self.inner.is_configured()
    }
    fn processing_descriptor(&self) -> Value {
        json!({"inner": self.inner.processing_descriptor(), "evaluation_call_cap": self.cap})
    }
    fn supports_bounded_attempts(&self) -> bool {
        self.inner.supports_bounded_attempts()
    }
    async fn complete(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        max_tokens: Option<u32>,
    ) -> Result<LlmResponse, BackendError> {
        self.measured_call(messages, schema, max_tokens, None).await
    }
    async fn complete_bounded(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        max_tokens: Option<u32>,
        budget: &CallBudget,
    ) -> Result<LlmResponse, BackendError> {
        self.measured_call(messages, schema, max_tokens, Some(budget))
            .await
    }
}
