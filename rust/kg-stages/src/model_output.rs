//! Validation of model answers before any record becomes a graph fact.
//!
//! A provider that honours a JSON schema can still return prose, a truncated
//! document, or a placeholder name. None of those is an empty result: the
//! snapshot that required the call fails, and its scope's absence sweep stays
//! suppressed.

use std::fmt;

use serde_json::{Map, Value};

struct UniqueJsonKeys;

impl<'de> serde::Deserialize<'de> for UniqueJsonKeys {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJsonKeys;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("JSON with unique object keys")
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(UniqueJsonKeys)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(UniqueJsonKeys)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(UniqueJsonKeys)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(UniqueJsonKeys)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(UniqueJsonKeys)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueJsonKeys)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                while sequence.next_element::<UniqueJsonKeys>()?.is_some() {}
                Ok(UniqueJsonKeys)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut keys = std::collections::HashSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(serde::de::Error::custom("duplicate JSON object key"));
                    }
                    map.next_value::<UniqueJsonKeys>()?;
                }
                Ok(UniqueJsonKeys)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

/// Decode one complete, bounded model JSON document. Bare JSON and one exact
/// markdown fence are accepted; surrounding prose, duplicate keys and trailing
/// content are rejected before domain-specific validation runs.
pub(crate) fn parse_json(raw: &str, max_bytes: usize) -> Result<Value, ModelOutputError> {
    if raw.len() > max_bytes {
        return Err(ModelOutputError::WrongShape(
            "model response exceeds its byte limit".into(),
        ));
    }
    let text = raw.trim();
    let text = if let Some(body) = text
        .strip_prefix("```json\n")
        .or_else(|| text.strip_prefix("```\n"))
    {
        body.strip_suffix("```")
            .ok_or_else(|| ModelOutputError::Incomplete("JSON fence is not closed".into()))?
            .trim()
    } else {
        text
    };
    let decode = |error: serde_json::Error| {
        if error.is_eof() {
            ModelOutputError::Incomplete("JSON ended before completion".into())
        } else {
            ModelOutputError::Malformed("invalid JSON document".into())
        }
    };
    serde_json::from_str::<UniqueJsonKeys>(text).map_err(decode)?;
    serde_json::from_str(text).map_err(decode)
}

/// Why a model answer cannot be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelOutputError {
    /// The JSON ends mid-value: the provider stopped before the answer closed.
    Incomplete(String),
    /// The text is not JSON.
    Malformed(String),
    /// JSON of another shape than the requested one.
    WrongShape(String),
    /// The object holds no array under the requested key.
    MissingArray(String),
    /// A record lacks a required field or carries it with the wrong type.
    MissingField {
        /// Position in the answer.
        record: usize,
        /// The absent field.
        field: String,
    },
    /// A record's value cannot become a fact: a placeholder or garbage name.
    RejectedRecord {
        /// Position in the answer.
        record: usize,
        /// Why the value is unusable.
        reason: String,
    },
    /// A relationship names an endpoint the prompt did not offer.
    UnknownEndpoint {
        /// Position in the answer.
        record: usize,
        /// The name the model used.
        endpoint: String,
    },
}

impl ModelOutputError {
    /// Truncated or unparsable output may differ on a retry; a rejected or
    /// mislabeled record is the model's answer, not a transport accident.
    pub fn is_retriable(&self) -> bool {
        matches!(self, Self::Incomplete(_) | Self::Malformed(_))
    }
}

impl fmt::Display for ModelOutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete(detail) => write!(f, "incomplete output: {detail}"),
            Self::Malformed(detail) => write!(f, "malformed output: {detail}"),
            Self::WrongShape(detail) => write!(f, "wrong shape: {detail}"),
            Self::MissingArray(key) => write!(f, "wrong shape: no `{key}` array"),
            Self::MissingField { record, field } => {
                write!(f, "record {record} has no string `{field}`")
            }
            Self::RejectedRecord { record, reason } => {
                write!(f, "record {record} rejected: {reason}")
            }
            Self::UnknownEndpoint { record, endpoint } => {
                write!(f, "record {record} names unknown endpoint `{endpoint}`")
            }
        }
    }
}

impl std::error::Error for ModelOutputError {}

/// The records of a model answer: a JSON array, or an object holding the
/// array under `key`. A single markdown fence around the JSON is tolerated
/// because some providers add one even under a schema; anything else fails.
pub fn records(
    response: &str,
    key: &str,
    max_bytes: usize,
) -> Result<Vec<Map<String, Value>>, ModelOutputError> {
    let items = match parse_json(response, max_bytes)? {
        Value::Array(items) => items,
        Value::Object(mut object) => match object.remove(key) {
            Some(Value::Array(items)) => items,
            Some(other) => {
                return Err(ModelOutputError::WrongShape(format!(
                    "`{key}` is {} instead of an array",
                    kind(&other)
                )))
            }
            None => return Err(ModelOutputError::MissingArray(key.to_string())),
        },
        other => {
            return Err(ModelOutputError::WrongShape(format!(
                "expected an object with a `{key}` array, got {}",
                kind(&other)
            )))
        }
    };
    items
        .into_iter()
        .enumerate()
        .map(|(record, item)| match item {
            Value::Object(fields) => Ok(fields),
            other => Err(ModelOutputError::WrongShape(format!(
                "record {record} is {} instead of an object",
                kind(&other)
            ))),
        })
        .collect()
}

/// A required string field of one record.
pub fn required_str<'a>(
    fields: &'a Map<String, Value>,
    record: usize,
    field: &str,
) -> Result<&'a str, ModelOutputError> {
    fields
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ModelOutputError::MissingField {
            record,
            field: field.to_string(),
        })
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_empty_array_is_a_result_not_an_error() {
        assert!(records(r#"{"entities":[]}"#, "entities", 4096)
            .unwrap()
            .is_empty());
        assert!(records("[]", "entities", 4096).unwrap().is_empty());
        let fenced = "```json\n{\"entities\":[{\"name\":\"a\"}]}\n```";
        assert_eq!(records(fenced, "entities", 4096).unwrap().len(), 1);
    }

    #[test]
    fn each_failure_kind_is_distinguished() {
        assert!(matches!(
            records("not json", "entities", 4096),
            Err(ModelOutputError::Malformed(_))
        ));
        assert!(matches!(
            records(r#"{"entities":[{"name":"a"#, "entities", 4096),
            Err(ModelOutputError::Incomplete(_))
        ));
        assert!(matches!(
            records(r#"{"items":[]}"#, "entities", 4096),
            Err(ModelOutputError::MissingArray(_))
        ));
        assert!(matches!(
            records(r#"{"entities":3}"#, "entities", 4096),
            Err(ModelOutputError::WrongShape(_))
        ));
        assert!(matches!(
            records(r#""text""#, "entities", 4096),
            Err(ModelOutputError::WrongShape(_))
        ));
        assert!(matches!(
            records(r#"{"entities":[1]}"#, "entities", 4096),
            Err(ModelOutputError::WrongShape(_))
        ));
        let record = records(r#"[{"name": 4}]"#, "entities", 4096)
            .unwrap()
            .remove(0);
        assert_eq!(
            required_str(&record, 0, "name"),
            Err(ModelOutputError::MissingField {
                record: 0,
                field: "name".into()
            })
        );
        assert!(ModelOutputError::Incomplete(String::new()).is_retriable());
        assert!(!ModelOutputError::RejectedRecord {
            record: 0,
            reason: String::new()
        }
        .is_retriable());
    }

    #[test]
    fn shared_decoder_rejects_ambiguous_or_unbounded_documents() {
        assert!(matches!(
            parse_json(r#"{"a":1,"a":2}"#, 4096),
            Err(ModelOutputError::Malformed(_))
        ));
        assert!(parse_json("prose\n```json\n{}\n```", 4096).is_err());
        assert!(parse_json("{} trailing", 4096).is_err());
        assert!(matches!(
            parse_json("{\"long\":true}", 4),
            Err(ModelOutputError::WrongShape(_))
        ));
        assert_eq!(
            parse_json("```\n{}\n```", 4096).unwrap(),
            serde_json::json!({})
        );
    }
}
