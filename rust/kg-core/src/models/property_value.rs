//! Typed entity and edge properties.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Tagged JSON, e.g. `{"t":"s","v":"hello"}`. Lists retain their order here;
/// structural hashing canonicalizes them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum PropertyValue {
    #[serde(rename = "s")]
    String(String),
    #[serde(rename = "i")]
    Integer(i64),
    #[serde(rename = "f")]
    Float(f64),
    #[serde(rename = "b")]
    Bool(bool),
    #[serde(rename = "ts")]
    Timestamp(DateTime<Utc>),
    /// Duration in milliseconds.
    #[serde(rename = "d")]
    Duration(i64),
    #[serde(rename = "u")]
    UuidRef(Uuid),

    #[serde(rename = "sl")]
    StringList(Vec<String>),
    #[serde(rename = "il")]
    IntegerList(Vec<i64>),
    #[serde(rename = "fl")]
    FloatList(Vec<f64>),

    /// Opaque JSON text; this type does not validate its contents.
    #[serde(rename = "j")]
    Json(String),
    #[serde(rename = "bl")]
    Blob(String),
    /// Explicit null, distinct from an absent property in the model.
    #[serde(rename = "n")]
    Null,
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(v) => write!(f, "{v}"),
            Self::Integer(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::Timestamp(v) => write!(f, "{v}"),
            Self::Duration(v) => write!(f, "{v}ms"),
            Self::UuidRef(v) => write!(f, "{v}"),
            Self::StringList(v) => write!(f, "{v:?}"),
            Self::IntegerList(v) => write!(f, "{v:?}"),
            Self::FloatList(v) => write!(f, "{v:?}"),
            Self::Json(v) => write!(f, "{v}"),
            Self::Blob(_) => write!(f, "<blob>"),
            Self::Null => write!(f, "null"),
        }
    }
}

impl From<String> for PropertyValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for PropertyValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<i64> for PropertyValue {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<f64> for PropertyValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<bool> for PropertyValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<DateTime<Utc>> for PropertyValue {
    fn from(v: DateTime<Utc>) -> Self {
        Self::Timestamp(v)
    }
}

impl From<Uuid> for PropertyValue {
    fn from(v: Uuid) -> Self {
        Self::UuidRef(v)
    }
}

impl From<Vec<String>> for PropertyValue {
    fn from(v: Vec<String>) -> Self {
        Self::StringList(v)
    }
}

impl PropertyValue {
    /// Decode source JSON without interpreting strings or reordering arrays.
    pub fn from_source(value: &serde_json::Value) -> Self {
        use serde_json::Value as J;
        match value {
            J::Null => Self::Null,
            J::String(v) => Self::String(v.clone()),
            J::Bool(v) => Self::Bool(*v),
            J::Number(v) if v.as_i64().is_some() => Self::Integer(v.as_i64().unwrap()),
            J::Number(v) if v.as_u64().is_none() => match v.as_f64() {
                Some(f) if f.is_finite() => Self::Float(if f == 0.0 { 0.0 } else { f }),
                _ => Self::Json(v.to_string()),
            },
            _ => Self::Json(value.to_string()),
        }
    }

    /// The source representation, without the model's tagged serialization.
    pub fn to_source(&self) -> Result<serde_json::Value, String> {
        match self {
            Self::Json(raw) => {
                serde_json::from_str(raw).map_err(|_| "invalid JSON property".into())
            }
            Self::Float(v) if !v.is_finite() => Err("non-finite property".into()),
            Self::FloatList(v) if v.iter().any(|f| !f.is_finite()) => {
                Err("non-finite property".into())
            }
            _ => Ok(self.to_bolt_json()),
        }
    }

    /// Stable dotted paths; arrays and empty objects remain whole values.
    /// Ambiguous source paths are rejected rather than silently overwritten.
    pub fn flatten_source(
        value: &serde_json::Value,
        opaque_paths: &[String],
    ) -> Result<indexmap::IndexMap<String, Self>, String> {
        fn walk(
            object: &serde_json::Map<String, serde_json::Value>,
            prefix: Option<&str>,
            opaque: &[String],
            seen: &mut std::collections::HashSet<String>,
            out: &mut indexmap::IndexMap<String, PropertyValue>,
        ) -> Result<(), String> {
            for (key, value) in object {
                let path = prefix.map_or_else(|| key.clone(), |p| format!("{p}.{key}"));
                if !seen.insert(path.clone()) {
                    return Err("ambiguous dotted property path".into());
                }
                if let Some(nested) = value.as_object().filter(|o| !o.is_empty()) {
                    if !opaque.contains(&path) {
                        walk(nested, Some(&path), opaque, seen, out)?;
                        continue;
                    }
                }
                out.insert(path, PropertyValue::from_source(value));
            }
            Ok(())
        }
        let object = value.as_object().ok_or("properties must be an object")?;
        let mut out = indexmap::IndexMap::new();
        // Validate paths independently of opaque rules so those rules cannot hide collisions.
        walk(object, None, &[], &mut Default::default(), &mut out)?;
        Self::validate_flat_paths(&out)?;
        if !opaque_paths.is_empty() {
            out.clear();
            walk(
                object,
                None,
                opaque_paths,
                &mut Default::default(),
                &mut out,
            )?;
        }
        Self::validate_flat_paths(&out)?;
        Ok(out)
    }

    /// A flattened value cannot also be the parent of another value.
    pub fn validate_flat_paths(
        properties: &indexmap::IndexMap<String, Self>,
    ) -> Result<(), String> {
        for key in properties.keys() {
            for (index, _) in key.match_indices('.') {
                if properties.contains_key(&key[..index]) {
                    return Err("ambiguous ancestor and descendant property paths".into());
                }
            }
        }
        Ok(())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Finite scalar identity text. Reject `None` rather than hashing a partial key.
    pub fn as_identity_key(&self) -> Option<String> {
        match self {
            Self::Float(f) if !f.is_finite() => None,
            Self::String(value) if value.trim().is_empty() => None,
            Self::String(_)
            | Self::Integer(_)
            | Self::Float(_)
            | Self::Bool(_)
            | Self::Timestamp(_)
            | Self::Duration(_)
            | Self::UuidRef(_) => Some(self.to_string()),
            Self::StringList(_)
            | Self::IntegerList(_)
            | Self::FloatList(_)
            | Self::Json(_)
            | Self::Blob(_)
            | Self::Null => None,
        }
    }

    /// Timestamps, UUIDs and opaque values become strings; non-finite floats become
    /// null, including in lists. Backend validation is still required.
    pub fn to_bolt_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        let float = |f: f64| serde_json::Number::from_f64(f).map_or(J::Null, J::Number);
        match self {
            Self::String(v) => J::String(v.clone()),
            Self::Integer(v) => J::from(*v),
            Self::Float(v) => float(*v),
            Self::Bool(v) => J::Bool(*v),
            Self::Timestamp(v) => J::String(v.to_rfc3339()),
            Self::Duration(v) => J::from(*v),
            Self::UuidRef(v) => J::String(v.to_string()),
            Self::StringList(v) => J::Array(v.iter().cloned().map(J::String).collect()),
            Self::IntegerList(v) => J::Array(v.iter().map(|i| J::from(*i)).collect()),
            Self::FloatList(v) => J::Array(v.iter().map(|f| float(*f)).collect()),
            Self::Json(v) => J::String(v.clone()),
            Self::Blob(v) => J::String(v.clone()),
            Self::Null => J::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_paths_are_stable_and_collisions_are_rejected() {
        let plain = serde_json::json!({"spec":{"id":3}});
        let complex =
            serde_json::json!({"spec":{"id":3,"containers":[{"id":1}],"empty":{}},"nil":null});
        let a = PropertyValue::flatten_source(&plain, &[]).unwrap();
        let b = PropertyValue::flatten_source(&complex, &[]).unwrap();
        assert_eq!(a["spec.id"], b["spec.id"]);
        assert_eq!(b["spec.empty"], PropertyValue::Json("{}".into()));
        assert_eq!(b["nil"], PropertyValue::Null);
        for raw in [
            serde_json::json!({"spec.id":1,"spec":{"id":2}}),
            serde_json::json!({"spec":null,"spec.id":1}),
            serde_json::json!({"spec":{},"spec.id":1}),
            serde_json::json!({"spec":[1],"spec.id":1}),
            serde_json::json!({"spec.id":{"x":1},"spec":{"id":{"y":2}}}),
        ] {
            assert!(PropertyValue::flatten_source(&raw, &[]).is_err());
            assert!(PropertyValue::flatten_source(&raw, &["spec".into()]).is_err());
        }
        assert_eq!(
            PropertyValue::from_source(&serde_json::json!(u64::MAX)),
            PropertyValue::Json(u64::MAX.to_string())
        );
        assert_eq!(
            PropertyValue::from_source(&serde_json::json!("null")),
            PropertyValue::String("null".into())
        );
    }

    #[test]
    fn non_finite_float_is_not_an_identity_key() {
        assert!(PropertyValue::Float(2.5).as_identity_key().is_some());
        assert!(PropertyValue::Float(f64::NAN).as_identity_key().is_none());
        assert!(PropertyValue::Float(f64::INFINITY)
            .as_identity_key()
            .is_none());
        assert!(PropertyValue::Float(f64::NEG_INFINITY)
            .as_identity_key()
            .is_none());
    }

    #[test]
    fn serde_round_trip_all_variants() {
        let cases: Vec<PropertyValue> = vec![
            PropertyValue::String("hello".into()),
            PropertyValue::Integer(42),
            PropertyValue::Float(2.5),
            PropertyValue::Bool(true),
            PropertyValue::Timestamp(Utc::now()),
            PropertyValue::Duration(5000),
            PropertyValue::UuidRef(Uuid::new_v4()),
            PropertyValue::StringList(vec!["a".into(), "b".into()]),
            PropertyValue::IntegerList(vec![1, 2, 3]),
            PropertyValue::FloatList(vec![1.0, 2.0]),
            PropertyValue::Json(r#"{"key":"value"}"#.into()),
            PropertyValue::Blob("binary-data".into()),
            PropertyValue::Null,
        ];

        for pv in cases {
            let json = serde_json::to_string(&pv).unwrap();
            let back: PropertyValue = serde_json::from_str(&json).unwrap();
            assert_eq!(pv, back, "Failed round-trip for: {json}");
        }
    }

    #[test]
    fn tagged_json_format() {
        let pv = PropertyValue::String("hello".into());
        let json = serde_json::to_string(&pv).unwrap();
        assert_eq!(json, r#"{"t":"s","v":"hello"}"#);
    }

    #[test]
    fn from_impls() {
        assert!(matches!(
            PropertyValue::from("test"),
            PropertyValue::String(_)
        ));
        assert!(matches!(
            PropertyValue::from(42i64),
            PropertyValue::Integer(42)
        ));
        assert!(matches!(
            PropertyValue::from(true),
            PropertyValue::Bool(true)
        ));
    }
}
