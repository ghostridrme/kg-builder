//! Typed decisions: a state and named questions in, typed answers with
//! probabilities out. Decision backends choose among options the caller
//! supplies; they never author content. Provider-neutral: limits and wire
//! shapes belong to the implementation.
use crate::errors::BackendError;
use async_trait::async_trait;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// One question over the shared state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Question {
    /// A statement to judge true or false; the answer is its probability.
    YesNo { instructions: String },
    /// Pick one option; keys are the caller's identifiers, values describe them.
    Choice {
        instructions: String,
        options: IndexMap<String, Value>,
    },
    /// Rate on ordered levels, lowest first.
    Score {
        instructions: String,
        levels: Vec<Value>,
    },
}

/// What one implementation can take. Enforced before any request is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionLimits {
    pub max_options: usize,
    pub max_levels: usize,
    pub max_state_bytes: usize,
}

/// One answer. `value` is the chosen option key, the yes probability, or the
/// score; `confidence` is the probability of the returned value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Answer {
    pub value: Value,
    pub confidence: f32,
    #[serde(default)]
    pub probabilities: BTreeMap<String, f32>,
}

/// Every answer of one request plus the model that produced them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Decided {
    pub answers: BTreeMap<String, Answer>,
    pub model: String,
    pub input_tokens: Option<u64>,
}

impl Question {
    pub fn instructions(&self) -> &str {
        match self {
            Self::YesNo { instructions }
            | Self::Choice { instructions, .. }
            | Self::Score { instructions, .. } => instructions,
        }
    }

    /// Provider-independent checks plus the backend's own limits.
    pub fn validate(&self, limits: &DecisionLimits) -> Result<(), BackendError> {
        let invalid = |message: &str| BackendError::Query(format!("invalid question: {message}"));
        if self.instructions().trim().is_empty() {
            return Err(invalid("blank instructions"));
        }
        match self {
            Self::YesNo { .. } => {}
            Self::Choice { options, .. } => {
                if options.len() < 2 || options.len() > limits.max_options {
                    return Err(invalid("option count out of range"));
                }
                if options.keys().any(|key| key.trim().is_empty()) {
                    return Err(invalid("blank option key"));
                }
            }
            Self::Score { levels, .. } => {
                if levels.len() < 2 || levels.len() > limits.max_levels {
                    return Err(invalid("level count out of range"));
                }
            }
        }
        Ok(())
    }
}

/// Validate a whole request against a backend's limits.
pub fn validate_request(
    state: &Value,
    questions: &BTreeMap<String, Question>,
    limits: &DecisionLimits,
) -> Result<(), BackendError> {
    if questions.is_empty() {
        return Err(BackendError::Query("no questions".into()));
    }
    if questions.keys().any(|key| key.trim().is_empty()) {
        return Err(BackendError::Query("blank question key".into()));
    }
    if state.to_string().len() > limits.max_state_bytes {
        return Err(BackendError::Query(
            "state exceeds the backend limit".into(),
        ));
    }
    questions
        .values()
        .try_for_each(|question| question.validate(limits))
}

impl Answer {
    /// An answer is trusted only when its probabilities are finite, within
    /// 0..=1, and agree with the returned value.
    pub fn validate(&self, question: &Question) -> Result<(), BackendError> {
        let invalid = || BackendError::Deserialization("invalid typed answer".into());
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(invalid());
        }
        if self
            .probabilities
            .values()
            .any(|p| !p.is_finite() || !(0.0..=1.0).contains(p))
        {
            return Err(invalid());
        }
        match question {
            Question::Choice { options, .. } => {
                let key = self.value.as_str().ok_or_else(invalid)?;
                if !options.contains_key(key) {
                    return Err(invalid());
                }
                if let Some(best) = self
                    .probabilities
                    .iter()
                    .max_by(|a, b| a.1.total_cmp(b.1))
                    .map(|(k, _)| k.as_str())
                {
                    if best != key && self.probabilities[best] > self.probabilities[key] {
                        return Err(invalid());
                    }
                }
            }
            Question::YesNo { .. } | Question::Score { .. } => {
                if !self.value.as_f64().is_some_and(f64::is_finite) {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
}

/// Object-safe typed-decision provider. Callers own concurrency, deadlines and
/// cancellation.
#[async_trait]
pub trait DecisionBackend: Send + Sync + 'static {
    async fn decide(
        &self,
        state: &Value,
        questions: &BTreeMap<String, Question>,
    ) -> Result<Decided, BackendError>;

    fn model_id(&self) -> &str;

    fn limits(&self) -> DecisionLimits;

    /// Nonsecret effective settings that take part in run identity.
    fn processing_descriptor(&self) -> Value {
        serde_json::json!({"implementation": std::any::type_name::<Self>(), "model": self.model_id()})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const LIMITS: DecisionLimits = DecisionLimits {
        max_options: 3,
        max_levels: 3,
        max_state_bytes: 64,
    };

    fn choice() -> Question {
        Question::Choice {
            instructions: "which".into(),
            options: IndexMap::from([("a".to_string(), json!("A")), ("b".to_string(), json!("B"))]),
        }
    }

    #[test]
    fn questions_and_requests_respect_the_backend_limits() {
        assert!(choice().validate(&LIMITS).is_ok());
        let too_many = Question::Choice {
            instructions: "which".into(),
            options: (0..4).map(|i| (i.to_string(), json!(i))).collect(),
        };
        assert!(too_many.validate(&LIMITS).is_err());
        assert!(Question::YesNo {
            instructions: "  ".into()
        }
        .validate(&LIMITS)
        .is_err());
        assert!(Question::Score {
            instructions: "rate".into(),
            levels: vec![json!("low")]
        }
        .validate(&LIMITS)
        .is_err());
        let questions = BTreeMap::from([("q".to_string(), choice())]);
        assert!(validate_request(&json!({"k":"v"}), &questions, &LIMITS).is_ok());
        assert!(validate_request(&json!("x".repeat(100)), &questions, &LIMITS).is_err());
        assert!(validate_request(&json!({}), &BTreeMap::new(), &LIMITS).is_err());
    }

    #[test]
    fn answers_must_agree_with_their_probabilities() {
        let ok = Answer {
            value: json!("a"),
            confidence: 0.9,
            probabilities: BTreeMap::from([("a".into(), 0.9), ("b".into(), 0.1)]),
        };
        assert!(ok.validate(&choice()).is_ok());
        let disagreeing = Answer {
            value: json!("b"),
            ..ok.clone()
        };
        assert!(disagreeing.validate(&choice()).is_err());
        let unknown = Answer {
            value: json!("zzz"),
            ..ok.clone()
        };
        assert!(unknown.validate(&choice()).is_err());
        let nan = Answer {
            confidence: f32::NAN,
            ..ok
        };
        assert!(nan.validate(&choice()).is_err());
        let yes = Answer {
            value: json!(0.4),
            confidence: 0.6,
            probabilities: BTreeMap::new(),
        };
        assert!(yes
            .validate(&Question::YesNo {
                instructions: "is".into()
            })
            .is_ok());
    }
}
