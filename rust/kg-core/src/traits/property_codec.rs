//! Queryable source values with explicit types for lossless graph round trips.
use crate::models::PropertyValue;
use indexmap::IndexMap;
use serde_json::{json, Map, Value};

pub const TYPE_PREFIX: &str = "property_type_";
const VALUE_PREFIX: &str = "prop_";

/// A patch changes the value and its type together. None removes the property;
/// a typed Null retains presence even though the graph removes its native value.
pub fn write_property(out: &mut Map<String, Value>, key: &str, value: Option<&PropertyValue>) {
    let (tag, native) = match value {
        Some(value) => (Value::String(type_tag(value).into()), value.to_bolt_json()),
        None => (Value::Null, Value::Null),
    };
    out.insert(format!("{VALUE_PREFIX}{key}"), native);
    out.insert(format!("{TYPE_PREFIX}{key}"), tag);
}

/// The stored type marker of a value. Typed identity comparison requires equal
/// markers as well as equal canonical text; an integer never equals a string.
pub fn type_tag(value: &PropertyValue) -> &'static str {
    match value {
        PropertyValue::String(_) => "s",
        PropertyValue::Integer(_) => "i",
        PropertyValue::Float(_) => "f",
        PropertyValue::Bool(_) => "b",
        PropertyValue::Timestamp(_) => "ts",
        PropertyValue::Duration(_) => "d",
        PropertyValue::UuidRef(_) => "u",
        PropertyValue::StringList(_) => "sl",
        PropertyValue::IntegerList(_) => "il",
        PropertyValue::FloatList(_) => "fl",
        PropertyValue::Json(_) => "j",
        PropertyValue::Blob(_) => "bl",
        PropertyValue::Null => "n",
    }
}

/// Decode one stored source property by name. Unlike [`read_properties`] this
/// tolerates unrelated malformed properties elsewhere in `stored`, so a target
/// index can read its key components even when another property is stale.
/// `Ok(None)` when the property is absent.
pub fn read_property(
    stored: &Map<String, Value>,
    name: &str,
) -> Result<Option<PropertyValue>, String> {
    let Some(tag) = stored.get(&format!("{TYPE_PREFIX}{name}")) else {
        return Ok(None);
    };
    let tag = tag.as_str().ok_or("invalid source property type marker")?;
    let native = stored.get(&format!("{VALUE_PREFIX}{name}"));
    let property = if tag == "n" {
        if native.is_some_and(|v| !v.is_null()) {
            return Err("null property has a native value".into());
        }
        PropertyValue::Null
    } else {
        let native = native
            .filter(|v| !v.is_null())
            .ok_or("typed property has no value")?;
        serde_json::from_value::<PropertyValue>(json!({"t":tag,"v":native}))
            .map_err(|_| "source property does not match its type marker")?
    };
    property.to_source()?;
    Ok(Some(property))
}

pub fn read_properties(
    stored: &Map<String, Value>,
) -> Result<IndexMap<String, PropertyValue>, String> {
    let mut out = IndexMap::new();
    for (key, value) in stored {
        if let Some(name) = key.strip_prefix(VALUE_PREFIX) {
            if !stored.contains_key(&format!("{TYPE_PREFIX}{name}")) {
                return Err("source property has no type marker; re-ingestion is required".into());
            }
            if value.is_object() {
                return Err("stored source property is not a graph primitive".into());
            }
        }
        let Some(name) = key.strip_prefix(TYPE_PREFIX) else {
            continue;
        };
        let property = read_property(stored, name)?.ok_or("typed property has no value")?;
        out.insert(name.to_owned(), property);
    }
    Ok(out)
}

/// Validate a patch without requiring unrelated properties to be included.
pub fn validate_patch(properties: &Map<String, Value>) -> Result<(), String> {
    for key in properties
        .keys()
        .filter_map(|key| key.strip_prefix(VALUE_PREFIX))
    {
        if !properties.contains_key(&format!("{TYPE_PREFIX}{key}")) {
            return Err("property patch omits its type marker".into());
        }
    }
    let mut paired = Map::new();
    for (key, tag) in properties
        .iter()
        .filter(|(k, _)| k.starts_with(TYPE_PREFIX))
    {
        let name = &key[TYPE_PREFIX.len()..];
        let value_key = format!("{VALUE_PREFIX}{name}");
        let native = properties
            .get(&value_key)
            .ok_or("property patch omits its value")?;
        if tag.is_null() {
            if !native.is_null() {
                return Err("removed property has a value".into());
            }
        } else {
            paired.insert(key.clone(), tag.clone());
            paired.insert(value_key, native.clone());
        }
    }
    read_properties(&paired).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_types_survive_native_storage_and_patches() {
        let cases = vec![
            PropertyValue::String("{\"x\":1}".into()),
            PropertyValue::Integer(4),
            PropertyValue::Float(4.0),
            PropertyValue::Bool(true),
            PropertyValue::Timestamp(chrono::Utc::now()),
            PropertyValue::Duration(-4),
            PropertyValue::UuidRef(uuid::Uuid::new_v4()),
            PropertyValue::StringList(vec![]),
            PropertyValue::IntegerList(vec![]),
            PropertyValue::FloatList(vec![]),
            PropertyValue::Json("{\"x\":[null,{}]}".into()),
            PropertyValue::Blob("blob".into()),
            PropertyValue::Null,
        ];
        let mut stored = Map::new();
        for (i, value) in cases.iter().enumerate() {
            write_property(&mut stored, &i.to_string(), Some(value));
        }
        validate_patch(&stored).unwrap();
        stored.retain(|_, v| !v.is_null());
        let decoded = read_properties(&stored).unwrap();
        for (i, value) in cases.iter().enumerate() {
            assert_eq!(&decoded[&i.to_string()], value);
        }
        let mut patch = Map::new();
        write_property(&mut patch, "0", None);
        validate_patch(&patch).unwrap();
        stored.extend(patch);
        stored.retain(|_, v| !v.is_null());
        assert!(!read_properties(&stored).unwrap().contains_key("0"));
    }
    #[test]
    fn corrupt_metadata_and_nonfinite_values_are_rejected() {
        for value in [
            json!({"prop_x":1}),
            json!({"property_type_x":"bad","prop_x":1}),
            json!({"property_type_x":"i"}),
            json!({"property_type_x":"n","prop_x":1}),
            json!({"property_type_x":"j","prop_x":"{"}),
        ] {
            assert!(read_properties(value.as_object().unwrap()).is_err());
        }
        for patch in [
            json!({"prop_x":1}),
            json!({"prop_x":null}),
            json!({"property_type_x":"i"}),
        ] {
            assert!(validate_patch(patch.as_object().unwrap()).is_err());
        }
        for value in [
            PropertyValue::Float(f64::INFINITY),
            PropertyValue::FloatList(vec![f64::NAN]),
        ] {
            let mut patch = Map::new();
            write_property(&mut patch, "x", Some(&value));
            assert!(validate_patch(&patch).is_err());
        }
    }
}
