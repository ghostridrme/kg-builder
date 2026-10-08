//! Public typed records: storage encoding is decoded once, outside the browser.
use serde_json::{json, Map, Value};

pub fn typed_record(value: &Value) -> Value {
    let Some(record) = value.as_object() else {
        return value.clone();
    };
    let mut properties = Map::new();
    let mut metadata = Map::new();
    let mut diagnostics = Vec::new();
    for (key, value) in record {
        if key == "embedding" || key == "summary_embedding" || key.starts_with("property_type_") {
            continue;
        }
        if let Some(name) = key.strip_prefix("prop_") {
            let decoded = if record
                .get(&format!("property_type_{name}"))
                .and_then(Value::as_str)
                == Some("j")
            {
                match value.as_str().map(serde_json::from_str::<Value>) {
                    Some(Ok(parsed)) => parsed,
                    Some(Err(_)) => {
                        diagnostics.push(json!({"property":name,"reason":"invalid_stored_json"}));
                        value.clone()
                    }
                    None => value.clone(),
                }
            } else {
                value.clone()
            };
            properties.insert(name.to_owned(), decoded);
        } else {
            metadata.insert(key.clone(), value.clone());
        }
    }
    let mut result = Map::new();
    for field in [
        "chain_id",
        "uuid",
        "entity_type",
        "name",
        "namespace",
        "version",
        "is_latest",
        "deleted_at",
    ] {
        if let Some(value) = record.get(field) {
            result.insert(field.into(), value.clone());
        }
    }
    result.insert("properties".into(), Value::Object(properties));
    result.insert("metadata".into(), Value::Object(metadata));
    result.insert("diagnostics".into(), json!(diagnostics));
    Value::Object(result)
}

pub fn compact_entity(value: &Value) -> Value {
    let mut record = Map::new();
    for field in [
        "chain_id",
        "uuid",
        "entity_type",
        "name",
        "namespace",
        "version",
        "is_latest",
        "deleted_at",
    ] {
        if let Some(value) = value.get(field) {
            record.insert(field.into(), value.clone());
        }
    }
    Value::Object(record)
}

pub fn typed_neighbor(value: &Value) -> Value {
    let mut result = value.clone();
    result["entity"] = typed_record(&value["entity"]);
    result["relationship"] = typed_record(&value["relationship"]);
    result
}
