//! Bounded browser details. Every continuation pins an exact record revision.
use crate::{graph::presentation::typed_record, query::QueryScope, ApiError, AppState};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use kg_core::traits::graph_explorer::ExplorerQuery;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetailRequest {
    kind: String,
    uuid: Option<Uuid>,
    chain_id: Option<Uuid>,
    entity_type: Option<String>,
    namespace: Option<String>,
    as_of: Option<DateTime<Utc>>,
    #[serde(default)]
    offset: usize,
    path: Option<String>,
    #[serde(default)]
    start: usize,
    revision: Option<String>,
}
pub(crate) async fn detail(
    State(state): State<AppState>,
    Query(q): Query<DetailRequest>,
) -> Result<Json<Value>, ApiError> {
    let bad = || ApiError(StatusCode::BAD_REQUEST, "Invalid detail request");
    if q.offset > 100_000
        || q.start > 10_000_000
        || q.path.as_ref().is_some_and(|p| p.len() > 2048)
        || ((q.offset > 0 || q.start > 0) && q.revision.is_none())
    {
        return Err(bad());
    }
    let query = match q.kind.as_str() {
        "entity" => ExplorerQuery::Entity {
            entity_type: q.entity_type.ok_or_else(bad)?,
            chain_id: q.chain_id.ok_or_else(bad)?,
        },
        "version" => ExplorerQuery::EntityVersion {
            uuid: q.uuid.ok_or_else(bad)?,
        },
        "snapshot" => ExplorerQuery::Snapshot {
            uuid: q.uuid.ok_or_else(bad)?,
        },
        "relationship" => ExplorerQuery::Relationship {
            edge_id: q.uuid.ok_or_else(bad)?,
        },
        "community" | "community_members" => ExplorerQuery::Community {
            uuid: q.uuid.ok_or_else(bad)?,
            member_offset: if q.kind == "community_members" {
                q.offset
            } else {
                0
            },
            member_limit: if q.kind == "community_members" { 20 } else { 1 },
        },
        _ => return Err(bad()),
    };
    let page = state
        .query
        .explore(
            &state.org_id,
            QueryScope {
                namespace: q.namespace,
                as_of: q.as_of,
                limit: Some(1),
                offset: Some(0),
            },
            query,
        )
        .await?;
    let mut raw = page.items.into_iter().next().ok_or(ApiError(
        StatusCode::NOT_FOUND,
        "Record not visible in this scope",
    ))?;
    if q.kind == "community_members" {
        let revision = raw["revision"].as_str().ok_or_else(bad)?;
        if q.revision
            .as_deref()
            .is_some_and(|expected| expected != revision)
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Community changed; restart member paging",
            ));
        }
        let members = raw["members"].as_array().ok_or_else(bad)?;
        return Ok(Json(
            json!({"revision":revision,"items":members.iter().take(20).collect::<Vec<_>>(),"next_offset":(members.len()>20).then_some(q.offset+20)}),
        ));
    }
    if q.kind == "community" {
        raw.as_object_mut().unwrap().remove("members");
    }
    let record = if q.kind == "snapshot" || q.kind == "community" {
        json!({"uuid":raw["uuid"],"properties":raw,"metadata":{}})
    } else {
        typed_record(&raw)
    };
    // UUID alone is insufficient for mutable summaries. Include full visible content in
    // the fingerprint so a concurrent summary update cannot splice two revisions.
    let revision = record_revision(&record);
    if q.revision.as_ref().is_some_and(|r| r != &revision) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "Record changed; reopen details to restart",
        ));
    }
    if let Some(path) = q.path {
        let value = record.pointer(&path).ok_or_else(bad)?;
        let encoded = serde_json::to_string(value).map_err(|_| bad())?;
        let total = encoded.chars().count();
        if q.start > total {
            return Err(bad());
        }
        let content: String = encoded.chars().skip(q.start).take(4096).collect();
        let end = q.start + content.chars().count();
        return Ok(Json(
            json!({"revision":revision,"path":path,"content":content,"start":q.start,"end":end,"next_start":(end<total).then_some(end),"total_characters":total,"encoding":"JSON","offset_unit":"unicode_scalar"}),
        ));
    }
    let fields = detail_fields(&record);
    if q.offset > fields.len() {
        return Err(bad());
    }
    let mut items = Vec::new();
    let mut bytes = 0;
    for (group, name, value) in fields.iter().skip(q.offset).take(30) {
        let path = format!("/{}/{}", group, name.replace('~', "~0").replace('/', "~1"));
        if path.len() > 2048 {
            return Err(ApiError(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Property name exceeds detail path limit",
            ));
        }
        let deferred = !fits_json_budget(value, 2048);
        let row = json!({"name":name,"group":group,"path":path,"deferred":deferred,"value":if deferred{Value::Null}else{(*value).clone()}});
        let size = row.to_string().len();
        if bytes + size > 16_000 {
            break;
        }
        bytes += size;
        items.push(row);
    }
    let end = q.offset + items.len();
    Ok(Json(
        json!({"uuid":record["uuid"],"revision":revision,"items":items,"total_fields":fields.len(),"next_offset":(end<fields.len()).then_some(end)}),
    ))
}

fn detail_fields(record: &Value) -> Vec<(&'static str, &String, &Value)> {
    let mut fields = Vec::new();
    for group in ["properties", "metadata"] {
        if let Some(values) = record[group].as_object() {
            for (name, value) in values {
                // Explicit null is source data; only absent optional metadata is hidden.
                if group == "properties" || !value.is_null() {
                    fields.push((group, name, value));
                }
            }
        }
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_source_null_survives_detail_projection() {
        let record = typed_record(&json!({
            "prop_optional": "null", "property_type_optional": "j", "deleted_at": null
        }));
        let fields = detail_fields(&record);
        assert_eq!(fields.len(), 1);
        assert_eq!(
            (fields[0].0, fields[0].1.as_str()),
            ("properties", "optional")
        );
        assert!(fields[0].2.is_null());
    }
}

/// Fingerprint the exact JSON without allocating a second complete record string.
fn record_revision(record: &Value) -> String {
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    serde_json::to_writer(&mut writer, record)
        .expect("JSON Value serializes to infallible hash writer");
    format!("{:x}", writer.0.finalize())
}

/// Stop as soon as a field requires deferred fetching, including escaped strings.
fn fits_json_budget(value: &Value, budget: usize) -> bool {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0 {
                return Err(std::io::Error::other("deferred field"));
            }
            self.0 -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(budget), value).is_ok()
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    #[test]
    fn streamed_revision_and_deferred_budget_preserve_json_contract() {
        for value in [
            json!({"nested":[null,1,true,"é\n"]}),
            json!("x".repeat(100_000)),
        ] {
            let encoded = value.to_string();
            assert_eq!(
                record_revision(&value),
                format!("{:x}", Sha256::digest(encoded.as_bytes()))
            );
            assert!(fits_json_budget(&value, encoded.len()));
            assert!(!fits_json_budget(&value, encoded.len() - 1));
        }
    }
}
