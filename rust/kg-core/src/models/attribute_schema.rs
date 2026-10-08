//! Portable attribute schemas shared by custom entity and relationship types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Bounded JSON Schema subset. Validation never fills defaults or coerces values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AttributeSchema(pub Value);

impl AttributeSchema {
    pub fn validate(&self) -> Result<(), String> {
        if !within_json_limit(&self.0, 65_536) {
            return Err("attribute schema exceeds byte limit".into());
        }
        if self.0.get("type").and_then(Value::as_str) != Some("object") {
            return Err("attribute schema root must be an object".into());
        }
        let mut nodes = 0;
        validate_node(&self.0, 0, &mut nodes)?;
        jsonschema::meta::validate(&self.0).map_err(|_| "invalid attribute schema".to_string())?;
        Ok(())
    }

    /// Missing and explicit null values are governed by the schema, not rewritten.
    pub fn validate_attributes(&self, attributes: &Value) -> Result<(), String> {
        self.validate()?;
        let validator = jsonschema::validator_for(&self.0)
            .map_err(|_| "invalid attribute schema".to_string())?;
        if validator.is_valid(attributes) {
            Ok(())
        } else {
            Err("attributes do not match the declared schema".into())
        }
    }
}

fn validate_node(value: &Value, depth: usize, nodes: &mut usize) -> Result<(), String> {
    *nodes += 1;
    if depth > 16 || *nodes > 512 {
        return Err("attribute schema exceeds complexity limit".into());
    }
    let object = value.as_object().ok_or("schema node must be an object")?;
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "type"
                | "description"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "enum"
                | "minimum"
                | "maximum"
                | "minLength"
                | "maxLength"
                | "minItems"
                | "maxItems"
        ) {
            return Err("unsupported attribute schema keyword".into());
        }
    }
    let types: Vec<&str> = match object.get("type") {
        Some(Value::String(kind)) => vec![kind],
        Some(Value::Array(kinds)) => kinds
            .iter()
            .map(|v| v.as_str().ok_or("invalid schema type"))
            .collect::<Result<_, _>>()?,
        _ => return Err("schema nodes require an explicit type".into()),
    };
    if types.is_empty()
        || types.iter().any(|kind| {
            !matches!(
                *kind,
                "object" | "array" | "string" | "integer" | "number" | "boolean" | "null"
            )
        })
    {
        return Err("unsupported attribute type".into());
    }
    if let Some(props) = object.get("properties") {
        let props = props.as_object().ok_or("properties must be an object")?;
        if !types.contains(&"object") || props.len() > 128 {
            return Err("invalid schema properties".into());
        }
        for (name, property) in props {
            if name.trim().is_empty() || name.len() > 256 {
                return Err("invalid attribute name".into());
            }
            validate_node(property, depth + 1, nodes)?;
        }
    }
    if let Some(required) = object.get("required") {
        let required = required.as_array().ok_or("required must be a list")?;
        let props = object
            .get("properties")
            .and_then(Value::as_object)
            .ok_or("required fields need properties")?;
        for name in required {
            if !name.as_str().is_some_and(|name| props.contains_key(name)) {
                return Err("required attribute is not declared".into());
            }
        }
    }
    if let Some(extra) = object.get("additionalProperties") {
        if !types.contains(&"object") {
            return Err("additionalProperties requires object type".into());
        }
        if !extra.is_boolean() {
            validate_node(extra, depth + 1, nodes)?;
        }
    }
    if let Some(items) = object.get("items") {
        if !types.contains(&"array") {
            return Err("items requires array type".into());
        }
        validate_node(items, depth + 1, nodes)?;
    } else if types.contains(&"array") {
        return Err("array attributes require items".into());
    }
    for (keywords, applicable) in [
        (
            ["minimum", "maximum"],
            types.contains(&"integer") || types.contains(&"number"),
        ),
        (["minLength", "maxLength"], types.contains(&"string")),
        (["minItems", "maxItems"], types.contains(&"array")),
    ] {
        if !applicable && keywords.iter().any(|key| object.contains_key(*key)) {
            return Err("constraint does not apply to attribute type".into());
        }
    }
    for (lo, hi) in [
        ("minimum", "maximum"),
        ("minLength", "maxLength"),
        ("minItems", "maxItems"),
    ] {
        if let (Some(lo), Some(hi)) = (
            object.get(lo).and_then(Value::as_f64),
            object.get(hi).and_then(Value::as_f64),
        ) {
            if lo > hi {
                return Err("schema minimum exceeds maximum".into());
            }
        }
    }
    Ok(())
}

/// Stop serialization at the limit without allocating a copy of a rejected payload.
pub(crate) fn within_json_limit(value: &impl Serialize, limit: usize) -> bool {
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("JSON limit exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Budget(limit), value).is_ok()
}
