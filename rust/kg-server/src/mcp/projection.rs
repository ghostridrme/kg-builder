//! Exact, bounded record projections. Oversized values have explicit range retrieval.
use super::{tool_error, CallToolResult};
use serde_json::{json, Value};

/// An overview never copies raw properties, tags, or evidence into agent context.
pub(super) fn overview(record: &Value) -> Value {
    let typed = typed_record(record);
    let mut result = super::compact_entity(record, false);
    let properties = typed["properties"].as_object();
    let names: Vec<_> = properties
        .into_iter()
        .flat_map(|p| p.keys())
        .filter(|name| name.len() <= 80)
        .take(8)
        .cloned()
        .collect();
    let count = properties.map_or(0, |p| p.len());
    result["property_count"] = json!(count);
    result["property_names_truncated"] = json!(count > names.len());
    result["property_names"] = json!(names);
    result["detail_available"] = json!(count > 0);
    result
}

pub(super) fn entity(record: &Value, fields: Option<&[String]>) -> Result<Value, CallToolResult> {
    let typed = typed_record(record);
    let mut result = super::compact_entity(record, false);
    let properties = typed["properties"]
        .as_object()
        .ok_or_else(|| tool_error("invalid_response", "Missing property map"))?;
    if fields.is_some_and(|v| v.len() > 24 || v.iter().any(|f| f.len() > 512)) {
        return Err(tool_error(
            "invalid_input",
            "Select at most 24 property names, each at most 512 bytes",
        ));
    }
    let mut selected = serde_json::Map::new();
    let mut omitted = Vec::new();
    let mut used = 0;
    for (name, value) in properties {
        if fields.is_some_and(|names| !names.contains(name)) {
            continue;
        }
        let size = value.to_string().len() + name.len();
        if selected.len() >= 24 || used + size > 2000 {
            omitted.push(json!({"name":name,"serialized_bytes":value.to_string().len(),"retrieve":"get_entity with property_path and value_start"}));
        } else {
            used += size;
            selected.insert(name.clone(), value.clone());
        }
    }
    result["properties"] = Value::Object(selected);
    result["properties_omitted"] = json!(omitted.len());
    result["omitted_property_count"] = json!(omitted.len());
    result["omitted_properties_truncated"] = json!(omitted.len() > 4);
    result["all_properties_retrieval"] =
        json!("get_entity property_path=/properties returns exact JSON ranges");
    result["omitted_properties"] = json!(omitted.into_iter().take(4).collect::<Vec<_>>());
    result["property_count"] = json!(properties.len());
    for field in ["tags", "labels"] {
        if let Some(value) = typed.get(field) {
            if value.to_string().len() <= 600 {
                result[field] = value.clone();
            } else {
                result[format!("{field}_omitted")] = json!(true);
            }
        }
    }
    Ok(result)
}

/// JSON pointer into the typed record, returned as an exact serialized JSON range.
/// Concatenate ranges before parsing: a range is not presented as a complete typed value.
pub(super) fn field_range(
    record: &Value,
    path: &str,
    start: usize,
    limit: usize,
) -> Result<Value, CallToolResult> {
    if path.len() > 1024
        || !(path == "/properties"
            || path.starts_with("/properties/")
            || path == "/tags"
            || path.starts_with("/tags/")
            || path == "/labels"
            || path == "/name")
        || !(1..=2000).contains(&limit)
        || start > 2_000_000
    {
        return Err(tool_error("invalid_input","Use /properties/<JSON-pointer-escaped name>, a valid range, and at most 2000 characters"));
    }
    let typed = typed_record(record);
    let value = typed
        .pointer(path)
        .ok_or_else(|| tool_error("not_found", "Property is not present"))?;
    let serialized = value.to_string();
    let total = serialized.chars().count();
    if start > total {
        return Err(tool_error(
            "invalid_input",
            "Range begins beyond the stored value",
        ));
    }
    let content = excerpt(&serialized, start, limit);
    let end = start + content.chars().count();
    Ok(
        json!({"chain_id":record["chain_id"],"version_id":record["uuid"],"property_path":path,
        "encoding":"json","offset_unit":"unicode_scalar","content":content,"start":start,"end":end,
        "total_characters":total,"next_start":(end<total).then_some(end),"truncated":end<total}),
    )
}

/// Bound the serialized wire excerpt as well as characters (Unicode and escapes matter).
pub(super) fn excerpt(text: &str, start: usize, limit: usize) -> String {
    let mut value = String::new();
    let mut bytes = 0;
    for c in text.chars().skip(start).take(limit) {
        let cost = serde_json::to_string(&c.to_string()).map_or(12, |v| v.len());
        if bytes + cost > 2400 {
            break;
        }
        bytes += cost;
        value.push(c);
    }
    value
}

fn typed_record(record: &Value) -> Value {
    let mut typed = crate::graph::presentation::typed_record(record);
    if typed["properties"]
        .as_object()
        .is_some_and(|p| p.is_empty())
    {
        if let Some(properties) = record.get("all_properties").filter(|v| v.is_object()) {
            typed["properties"] = properties.clone();
        }
    }
    let tags: serde_json::Map<String, Value> = record
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| k.strip_prefix("tag_").map(|k| (k.to_owned(), v.clone())))
        .collect();
    if !tags.is_empty() {
        typed["tags"] = Value::Object(tags);
    } else if let Some(tags) = record.get("tags") {
        typed["tags"] = tags.clone();
    }
    if let Some(labels) = record.get("labels") {
        typed["labels"] = labels.clone();
    }
    typed
}

/// Keep the endpoints beside each edge so a small graph page is useful on its own.
/// Every record is emitted once; remaining isolated nodes follow the relationships.
pub(super) fn graph_records(items: Vec<Value>) -> Vec<Value> {
    let mut nodes: std::collections::BTreeMap<String, Value> = items
        .iter()
        .filter(|item| item["kind"] == "entity")
        .filter_map(|item| {
            item["chain_id"]
                .as_str()
                .map(|chain| (chain.to_owned(), item.clone()))
        })
        .collect();
    let mut ordered = Vec::with_capacity(items.len());
    for edge in items
        .into_iter()
        .filter(|item| item["kind"] == "relationship")
    {
        for field in ["source_chain_id", "target_chain_id"] {
            if let Some(chain) = edge[field].as_str() {
                if let Some(node) = nodes.remove(chain) {
                    ordered.push(node);
                }
            }
        }
        ordered.push(edge);
    }
    ordered.extend(nodes.into_values());
    ordered
}

/// Preserve correctness signals; routine successful timings do not belong in an overview.
pub(super) fn search_diagnostics(value: Value) -> Value {
    let Some(rows) = value.as_array() else {
        return value;
    };
    json!(rows
        .iter()
        .filter(|row| row["status"] != "success"
            || row["dropped_count"].as_u64().unwrap_or(0) > 0
            || !row["failure"].is_null())
        .map(|row| {
            let mut result = json!({"operation":row["operation"],"status":row["status"]});
            if row["dropped_count"].as_u64().unwrap_or(0) > 0 {
                result["dropped_count"] = row["dropped_count"].clone();
            }
            if !row["failure"].is_null() {
                result["failure"] = row["failure"].clone();
            }
            result
        })
        .collect::<Vec<_>>())
}
