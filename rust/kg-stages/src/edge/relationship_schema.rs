//! Validate relationship attributes before and after semantic identity adoption.
use indexmap::IndexMap;
use kg_core::{models::PropertyValue, traits::Ontology};
use serde_json::{Map, Value};

pub(super) fn validate(
    name: &str,
    properties: &IndexMap<String, PropertyValue>,
    ontology: &Ontology,
) -> Result<(), String> {
    if ontology.canonical_relationship(name).as_deref() != Some(name) {
        return Err("relationship name violates current schema".into());
    }
    let Some(schema) = ontology
        .edge_types
        .iter()
        .find(|definition| definition.name == name)
        .and_then(|definition| definition.attributes.as_ref())
    else {
        return Ok(());
    };
    schema.validate_attributes(&reconstruct(properties)?)
}

pub(super) fn reconstruct(properties: &IndexMap<String, PropertyValue>) -> Result<Value, String> {
    PropertyValue::validate_flat_paths(properties)?;
    let mut attributes = Map::new();
    for (path, value) in properties {
        insert(
            &mut attributes,
            &path.split('.').collect::<Vec<_>>(),
            value.to_source()?,
        )?;
    }
    Ok(Value::Object(attributes))
}

/// Missing object fields can be enriched later; supplied array items remain complete.
pub(super) fn validate_present(
    schema: &kg_core::models::AttributeSchema,
    value: &Value,
) -> Result<(), String> {
    fn optional(value: &mut Value) {
        if let Some(object) = value.as_object_mut() {
            object.remove("required");
            if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
                for child in properties.values_mut() {
                    optional(child);
                }
            }
        }
    }
    let mut present = schema.0.clone();
    optional(&mut present);
    kg_core::models::AttributeSchema(present).validate_attributes(value)
}

fn insert(object: &mut Map<String, Value>, path: &[&str], value: Value) -> Result<(), String> {
    let (name, rest) = path.split_first().ok_or("empty attribute path")?;
    if name.is_empty() {
        return Err("empty attribute path".into());
    }
    if rest.is_empty() {
        if object.insert((*name).into(), value).is_some() {
            return Err("ambiguous attribute path".into());
        }
    } else {
        let child = object
            .entry((*name).to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        insert(
            child.as_object_mut().ok_or("ambiguous attribute path")?,
            rest,
            value,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn validates_nested_types_and_rejects_forbidden_names_or_properties() {
        let onto:Ontology=serde_json::from_value(json!({"relationship_vocabulary":["CALLS"],"edge_types":[{"name":"CALLS","attributes":{"type":"object","required":["transport"],"additionalProperties":false,"properties":{"transport":{"type":"object","required":["port"],"additionalProperties":false,"properties":{"port":{"type":"integer"}}}}}}]})).unwrap();
        let properties =
            PropertyValue::flatten_source(&json!({"transport":{"port":443}}), &[]).unwrap();
        assert!(validate("CALLS", &properties, &onto).is_ok());
        assert!(validate("CONNECTS_TO", &properties, &onto).is_err());
        assert!(validate(
            "CALLS",
            &PropertyValue::flatten_source(&json!({"transport":{"port":"443"}}), &[]).unwrap(),
            &onto
        )
        .is_err());
        let mut extra = properties.clone();
        extra.insert("unexpected".into(), PropertyValue::Bool(true));
        assert!(validate("CALLS", &extra, &onto).is_err());
    }
}
