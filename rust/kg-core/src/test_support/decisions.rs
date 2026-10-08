use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

use async_trait::async_trait;
use serde_json::Value;

use crate::errors::BackendError;
use crate::traits::decision_backend::{Answer, Decided, DecisionBackend, DecisionLimits, Question};

/// Scripted typed decisions for pipeline tests: answers by question key, or an
/// outage on every call.
pub struct MockDecisionBackend {
    answers: Mutex<BTreeMap<String, Answer>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<(Value, BTreeMap<String, Question>)>>,
    failing: bool,
}

impl MockDecisionBackend {
    /// Every question key present in `answers` is answered with that answer;
    /// an unknown key is an invalid response.
    pub fn with_answers(answers: BTreeMap<String, Answer>) -> Self {
        Self {
            answers: Mutex::new(answers),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            failing: false,
        }
    }

    /// Every call fails as a provider outage would.
    pub fn failing() -> Self {
        Self {
            answers: Mutex::new(BTreeMap::new()),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            failing: true,
        }
    }

    /// Replace the scripted answers.
    pub fn set_answers(&self, answers: BTreeMap<String, Answer>) {
        *self.answers.lock().unwrap() = answers;
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Every (state, questions) pair received, in order.
    pub fn requests(&self) -> Vec<(Value, BTreeMap<String, Question>)> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl DecisionBackend for MockDecisionBackend {
    async fn decide(
        &self,
        state: &Value,
        questions: &BTreeMap<String, Question>,
    ) -> Result<Decided, BackendError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests
            .lock()
            .unwrap()
            .push((state.clone(), questions.clone()));
        if self.failing {
            return Err(BackendError::Unavailable(
                "simulated decision outage".into(),
            ));
        }
        let scripted = self.answers.lock().unwrap();
        let mut answers = BTreeMap::new();
        for key in questions.keys() {
            let answer = scripted
                .get(key)
                .cloned()
                .ok_or_else(|| BackendError::Deserialization("unscripted question".into()))?;
            answers.insert(key.clone(), answer);
        }
        Ok(Decided {
            answers,
            model: self.model_id().into(),
            input_tokens: Some(0),
        })
    }

    fn model_id(&self) -> &str {
        "mock-decisions"
    }

    fn limits(&self) -> DecisionLimits {
        DecisionLimits {
            max_options: 255,
            max_levels: 10,
            max_state_bytes: 1 << 20,
        }
    }
}
