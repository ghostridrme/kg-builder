use super::*;
pub(super) fn clip(text: &str) -> String {
    let mut chars = text.chars();
    let prefix: String = chars.by_ref().take(MAX_TEXT).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

pub(super) fn compact_value(value: &Value, depth: usize) -> Value {
    match value {
        Value::String(s) => json!(clip(s)),
        Value::Array(items) if depth < 2 => Value::Array(
            items
                .iter()
                .take(8)
                .map(|v| compact_value(v, depth + 1))
                .collect(),
        ),
        Value::Object(fields) if depth < 2 => Value::Object(
            fields
                .iter()
                .take(12)
                .map(|(k, v)| (k.clone(), compact_value(v, depth + 1)))
                .collect(),
        ),
        Value::Array(items) => json!(format!("[{} items]", items.len())),
        Value::Object(fields) => json!(format!("{{{} fields}}", fields.len())),
        _ => value.clone(),
    }
}

pub(super) fn compact_entity(entity: &Value, include_properties: bool) -> Value {
    let mut result = Map::new();
    for key in [
        "chain_id",
        "uuid",
        "entity_type",
        "namespace",
        "name",
        "version",
        "valid_from",
        "valid_to",
        "deleted_at",
        "is_latest",
        "source",
        "extracted_by",
        "resolved_by",
        "lifecycle",
    ] {
        if let Some(value) = entity.get(key) {
            let value = if matches!(
                key,
                "entity_type" | "namespace" | "name" | "chain_id" | "uuid"
            ) {
                value.clone()
            } else {
                compact_value(value, 0)
            };
            result.insert(key.into(), value);
        }
    }
    if include_properties {
        if let Some(properties) = entity.as_object() {
            let stored: Vec<_> = properties
                .iter()
                .filter_map(|(key, value)| key.strip_prefix("prop_").map(|name| (name, value)))
                .collect();
            if !stored.is_empty() {
                let selected = stored
                    .iter()
                    .take(24)
                    .map(|(name, value)| {
                        let decoded = if properties.get(&format!("property_type_{name}"))
                            == Some(&json!("j"))
                        {
                            value
                                .as_str()
                                .filter(|text| text.len() <= 4096)
                                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                                .unwrap_or_else(|| (*value).clone())
                        } else {
                            (*value).clone()
                        };
                        (name.to_string(), compact_value(&decoded, 0))
                    })
                    .collect();
                result.insert("properties".into(), Value::Object(selected));
                result.insert(
                    "omitted_property_count".into(),
                    json!(stored.len().saturating_sub(24)),
                );
            }
            let stored_tags: Vec<_> = properties
                .iter()
                .filter_map(|(key, value)| key.strip_prefix("tag_").map(|name| (name, value)))
                .collect();
            if !stored_tags.is_empty() {
                let tags: Map<String, Value> = stored_tags
                    .iter()
                    .take(12)
                    .map(|(name, value)| (name.to_string(), compact_value(value, 0)))
                    .collect();
                result.insert("tags".into(), Value::Object(tags));
                result.insert(
                    "omitted_tag_count".into(),
                    json!(stored_tags.len().saturating_sub(12)),
                );
            }
        }
        if !result.contains_key("properties") {
            if let Some(properties) = entity.get("all_properties").and_then(Value::as_object) {
                result.insert(
                    "properties".into(),
                    Value::Object(
                        properties
                            .iter()
                            .take(24)
                            .map(|(key, value)| (key.clone(), compact_value(value, 0)))
                            .collect(),
                    ),
                );
                result.insert(
                    "omitted_property_count".into(),
                    json!(properties.len().saturating_sub(24)),
                );
            }
        }
        for key in ["tags", "labels"] {
            if result.contains_key(key) {
                continue;
            }
            if let Some(value) = entity.get(key) {
                result.insert(key.into(), compact_value(value, 0));
            }
        }
    }
    Value::Object(result)
}

pub(super) fn compact_neighbor(item: &Value) -> Value {
    let relationship = item.get("relationship").unwrap_or(&Value::Null);
    json!({
        "entity":compact_entity(item.get("entity").unwrap_or(&Value::Null),false),
        "relationship":{
            "id":item.get("edge_id"),"name":item.get("via").map(|v|compact_value(v,0)),
            "source_chain":item.get("src_chain"),"target_chain":item.get("dst_chain"),
            "valid_from":relationship.get("valid_from"),"valid_to":relationship.get("valid_to"),
            "discovered_by":relationship.get("discovered_by"),"confidence":relationship.get("confidence"),
        }
    })
}

pub(super) fn backend_error(error: BackendError) -> CallToolResult {
    let kind = kg_core::search::SearchFailure::from(&error);
    tracing::warn!(failure = ?kind, "MCP graph request failed");
    match error {
        BackendError::Timeout(_) => tool_error(
            "deadline",
            "Graph read exceeded its deadline; narrow the query",
        ),
        BackendError::Connection(_) | BackendError::Unavailable(_) => {
            tool_error("backend_unavailable", "Graph backend is unavailable")
        }
        BackendError::RateLimited { .. } => {
            tool_error("rate_limited", "Graph backend is at capacity")
        }
        BackendError::NotConfigured(_) => {
            tool_error("unavailable", "Requested capability is not configured")
        }
        _ => tool_error(
            "query_failed",
            "Graph request failed; check arguments before retrying",
        ),
    }
}

/// Exactly one of a UUID or a name identifies a Saga in tool arguments.
pub(super) fn saga_reference(
    saga_uuid: Option<String>,
    name: Option<String>,
) -> Result<ThreadReference, CallToolResult> {
    match (saga_uuid, name) {
        (Some(uuid), None) => Ok(ThreadReference::Uuid {
            uuid: Uuid::parse_str(&uuid)
                .map_err(|_| tool_error("invalid_input", "Invalid Thread UUID"))?,
        }),
        (None, Some(name)) => Ok(ThreadReference::Name { name }),
        _ => Err(tool_error(
            "invalid_input",
            "Supply exactly one of thread_uuid and name",
        )),
    }
}

pub(super) fn page_limit(limit: Option<usize>, cap: usize) -> Result<usize, CallToolResult> {
    match limit {
        Some(n) if n == 0 || n > cap => Err(tool_error(
            "invalid_input",
            &format!("limit must be between 1 and {cap}; omit it to use {cap}"),
        )),
        Some(n) => Ok(n),
        None => Ok(cap),
    }
}

/// Saga view with the summary bounded for tool output. Coverage fields are kept
/// whole so an agent can choose a later `as_of` when a summary is withheld.
pub(super) fn compact_saga(view: &SagaView) -> Value {
    let mut value = serde_json::to_value(view).unwrap_or(Value::Null);
    if let Some(summary) = view.summary.as_deref() {
        let mut chars = summary.chars();
        let prefix: String = chars.by_ref().take(MAX_SUMMARY_TEXT).collect();
        let truncated = chars.next().is_some();
        value["summary"] = json!(prefix);
        value["summary_truncated"] = json!(truncated);
    }
    value
}

pub(super) fn query_error(error: QueryError) -> CallToolResult {
    match error {
        QueryError::RestartRequired => tool_error(
            "restart_required",
            "Graph continuation expired; refresh the view",
        ),
        QueryError::ViewLimit => tool_error("view_limit", "Narrow the graph scope"),
        QueryError::Invalid => tool_error("invalid_input", "Invalid graph query or scope"),
        QueryError::NotFound => tool_error("not_found", "Record is not visible in this scope"),
        QueryError::SummariesUnavailable => {
            tool_error("unavailable", "Thread summaries are not configured")
        }
        QueryError::Summary(_) => tool_error(
            "summary_failed",
            "Thread summary did not complete; retry with the same run_id",
        ),
        QueryError::SemanticUnavailable => {
            tool_error("unavailable", "Semantic search is not configured")
        }
        QueryError::Busy => tool_error("busy", "Too many active graph requests; retry later"),
        QueryError::Backend(error) => backend_error(error),
    }
}

pub(super) fn tool_error(code: &str, message: &str) -> CallToolResult {
    CallToolResult::structured_error(json!({"code":code,"message":message,
        "retryable":matches!(code,"busy"|"deadline"|"backend_unavailable"|"rate_limited"),
        "request_id":Uuid::new_v4(),"retry_after_ms":matches!(code,"busy"|"rate_limited").then_some(1000)}))
}

/// Shrink an oversized value in place. Pages drop trailing items and move
/// their cursor back to what remains (offset pages count visible rows, member
/// pages use the last returned ordinal). A committed summary outcome sheds
/// optional detail instead, so `run_id` and the counts always survive.
pub(super) fn shrink(value: &mut Value) -> bool {
    // Exact summary/evidence ranges cannot be shortened without updating both cursors.
    if value.get("summary_start").is_some() || value.get("continuation").is_some() {
        return false;
    }
    if let Some(items) = value.get_mut("items").and_then(Value::as_array_mut) {
        if items.len() <= 1 {
            // A single oversized Saga still answers with its identity and coverage.
            return items.first_mut().is_some_and(shed_saga_detail);
        }
        items.pop();
        let count = items.len() as u64;
        let last_ordinal = items.last().and_then(|m| m.get("ordinal")).cloned();
        value["truncated"] = json!(true);
        if value.get("after_ordinal").is_some() {
            value["next_after_ordinal"] = last_ordinal.unwrap_or(Value::Null);
        } else if value.get("next_offset").is_some() {
            let offset = value.get("offset").and_then(Value::as_u64).unwrap_or(0);
            value["next_offset"] = json!(offset + count);
        }
        return true;
    }
    if value.get("run_id").is_some() {
        let Some(saga) = value.get_mut("thread").filter(|s| s.is_object()) else {
            return false;
        };
        if shed_saga_detail(saga) {
            return true;
        }
        value["thread"] = Value::Null;
        value["thread_read_error"] = json!("result_too_large");
        return true;
    }
    // Ranked results have no paging cursor; keep the highest-ranked prefix.
    for key in ["hits", "snapshots"] {
        if let Some(items) = value.get_mut(key).and_then(Value::as_array_mut) {
            if items.len() > 1 {
                items.pop();
                value["truncated"] = json!(true);
                return true;
            }
        }
    }
    // `get_saga` answers with the Saga view itself.
    shed_saga_detail(value)
}

/// Drop one layer of optional detail from a Saga view: first the supporting
/// snapshot list (kept as a count), then the summary text (flagged as
/// truncated). Identity, membership bounds and coverage always remain.
/// Returns false when the value is not a Saga view or nothing is left to shed.
pub(super) fn shed_saga_detail(saga: &mut Value) -> bool {
    if saga.get("summary_covers_members_through").is_none() {
        return false;
    }
    if let Some(support) = saga
        .get("summary_supporting_snapshot_uuids")
        .and_then(Value::as_array)
        .filter(|s| !s.is_empty())
    {
        saga["summary_supporting_snapshot_count"] = json!(support.len());
        saga["summary_supporting_snapshot_uuids"] = json!([]);
        return true;
    }
    if saga
        .get("summary")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty())
    {
        saga["summary"] = Value::Null;
        saga["summary_truncated"] = json!(true);
        return true;
    }
    false
}

pub(super) fn respond(result: Result<Value, CallToolResult>) -> CallToolResult {
    match result {
        Ok(mut value) => loop {
            // Include actual wire envelope and compatibility text, not just structured JSON.
            let mut response = CallToolResult::structured(value.clone());
            response.content = vec![ContentBlock::text(value.to_string())];
            let bytes = serde_json::to_vec(&response).map_or(usize::MAX, |v| v.len());
            if bytes + 512 <= result_budget() {
                return response;
            }
            if !shrink(&mut value) {
                return tool_error("result_too_large","Result exceeds context budget; use a smaller limit or exact field/content range");
            }
        },
        Err(error) => error,
    }
}
