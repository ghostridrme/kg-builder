use kg_core::{errors::BackendError, search::*, traits::graph_backend::GraphEmbedding};
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;
fn text<'a>(p: &'a Map<String, Value>, k: &str) -> &'a str {
    p.get(k).and_then(Value::as_str).unwrap_or("")
}
fn id(p: &Map<String, Value>, k: &str) -> Result<Uuid, BackendError> {
    Uuid::parse_str(text(p, k))
        .map_err(|_| BackendError::Deserialization(format!("invalid {k} in search record")))
}
fn optional(p: &Map<String, Value>, k: &str) -> Option<String> {
    p.get(k).and_then(Value::as_str).map(str::to_owned)
}
fn score(p: &Map<String, Value>) -> Result<f32, BackendError> {
    let s = p
        .get("score")
        .and_then(Value::as_f64)
        .ok_or_else(|| BackendError::Deserialization("missing search score".into()))?
        as f32;
    if !s.is_finite() {
        return Err(BackendError::Deserialization(
            "nonfinite search score".into(),
        ));
    }
    Ok(s)
}
fn count(p: &Map<String, Value>, key: &str) -> Result<Option<usize>, BackendError> {
    p.get(key)
        .map(|value| {
            value
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| BackendError::Deserialization(format!("invalid {key} count")))
        })
        .transpose()
}
pub fn decode_node(
    mut p: Map<String, Value>,
    expected_text_version: &str,
) -> Result<SearchHit, BackendError> {
    // Map projections are nested; full nodes may already be flattened by the driver.
    if let Some(Value::Object(mut node)) = p.remove("n") {
        node.extend(p);
        p = node;
    }
    let derived_summary = match p.remove("summary_state") {
        Some(value) if !value.is_null() => {
            Some(serde_json::from_value::<SummaryView>(value).map_err(|_| {
                BackendError::Deserialization("invalid derived summary metadata".into())
            })?)
        }
        _ => None,
    };
    p.retain(|key, _| key != "derived_summary" && !key.starts_with("summary_"));
    if text(&p, "embedding_text_version") != expected_text_version {
        p.remove("embedding");
    }
    let embedding = if let (Some(values), Some(model)) =
        (p.remove("embedding"), optional(&p, "embedding_model"))
    {
        let values = if let Some(s) = values.as_str() {
            serde_json::from_str(&format!("[{s}]"))
        } else {
            serde_json::from_value(values)
        }
        .map_err(|e| BackendError::Deserialization(format!("invalid embedding: {e}")))?;
        let e = GraphEmbedding { model, values };
        e.validate()?;
        Some(e)
    } else {
        None
    };
    Ok(SearchHit {
        derived_summary,
        uuid: id(&p, "uuid")?,
        chain_id: id(&p, "chain_id")?,
        entity_type: text(&p, "entity_type").into(),
        namespace: text(&p, "namespace").into(),
        name: text(&p, "name").into(),
        score: score(&p)?,
        score_breakdown: HashMap::new(),
        graph_distance: None,
        embedding,
        observation_count: count(&p, "observations")?,
        dependent_count: count(&p, "dependents")?,
        last_changed_at: optional(&p, "valid_from"),
        owner: optional(&p, "prop_owner"),
        properties: Value::Object(p),
    })
}

fn relationship_hit(r: &Map<String, Value>) -> Result<RelationshipHit, BackendError> {
    Ok(RelationshipHit {
        model_score: None,
        uuid: id(r, "uuid")?,
        source_chain_id: id(r, "source_chain_id")?,
        target_chain_id: id(r, "target_chain_id")?,
        name: text(r, "name").into(),
        description: text(r, "description").into(),
        valid_from: optional(r, "valid_from"),
        valid_to: optional(r, "valid_to"),
        snapshot_id: optional(r, "snapshot_id")
            .map(|s| {
                Uuid::parse_str(&s)
                    .map_err(|_| BackendError::Deserialization("invalid snapshot ID".into()))
            })
            .transpose()?,
        score: score(r)?,
    })
}

/// Decode relationship facts without dropping malformed identities.
pub fn decode_relationships(
    rows: Vec<Map<String, Value>>,
    limit: usize,
) -> Result<SearchPage<RelationshipHit>, BackendError> {
    let hits = rows
        .iter()
        .map(relationship_hit)
        .collect::<Result<Vec<_>, BackendError>>()?;
    Ok(SearchPage::bounded(hits, limit))
}

fn snapshot_hit(
    r: &Map<String, Value>,
    passage_query: Option<&str>,
) -> Result<SnapshotHit, BackendError> {
    let content = r
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| BackendError::Deserialization("missing snapshot content".into()))?;
    let length = r
        .get("content_length")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| BackendError::Deserialization("invalid snapshot content length".into()))?;
    let scanned = content.chars().count();
    if scanned != length.min(crate::SOURCE_SCAN_CHARS) {
        return Err(BackendError::Deserialization(
            "inconsistent snapshot content length".into(),
        ));
    }
    let excerpt = crate::source_excerpt(content, passage_query);
    Ok(SnapshotHit {
        model_score: None,
        uuid: id(r, "uuid")?,
        name: text(r, "name").into(),
        source: text(r, "source").into(),
        namespace: text(r, "namespace").into(),
        captured_at: optional(r, "captured_at"),
        content: excerpt.content,
        content_truncated: excerpt.end - excerpt.start < length,
        content_start: excerpt.start,
        content_end: excerpt.end,
        selection_kind: excerpt.selection,
        selection_limited: length > crate::SOURCE_SCAN_CHARS,
        score: score(r)?,
    })
}

/// Decode source evidence with explicit excerpt truncation.
pub fn decode_snapshots(
    rows: Vec<Map<String, Value>>,
    limit: usize,
    passage_query: Option<&str>,
) -> Result<SearchPage<SnapshotHit>, BackendError> {
    let hits = rows
        .iter()
        .map(|r| snapshot_hit(r, passage_query))
        .collect::<Result<Vec<_>, BackendError>>()?;
    Ok(SearchPage::bounded(hits, limit))
}

/// Group attached rows by anchor, keeping storage order, and cut each anchor at
/// `per_anchor`; any anchor with more rows marks the page truncated.
fn attached<T>(
    rows: Vec<Map<String, Value>>,
    per_anchor: usize,
    decode: impl Fn(&Map<String, Value>) -> Result<T, BackendError>,
) -> Result<SearchPage<Attached<T>>, BackendError> {
    let mut counts = HashMap::<Uuid, usize>::new();
    let mut items = Vec::with_capacity(rows.len());
    let mut truncated = false;
    for row in &rows {
        let anchor = id(row, "anchor")?;
        let seen = counts.entry(anchor).or_default();
        *seen += 1;
        if *seen > per_anchor {
            truncated = true;
            continue;
        }
        items.push(Attached {
            anchor,
            record: decode(row)?,
        });
    }
    Ok(SearchPage {
        items,
        truncated,
        approximate: false,
    })
}

pub fn decode_attached_relationships(
    rows: Vec<Map<String, Value>>,
    per_anchor: usize,
) -> Result<SearchPage<Attached<RelationshipHit>>, BackendError> {
    attached(rows, per_anchor, relationship_hit)
}

pub fn decode_attached_snapshots(
    rows: Vec<Map<String, Value>>,
    per_anchor: usize,
    passage_query: Option<&str>,
) -> Result<SearchPage<Attached<SnapshotHit>>, BackendError> {
    attached(rows, per_anchor, |r| snapshot_hit(r, passage_query))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projected_nodes_preserve_identity_and_column_scores() {
        let uuid = Uuid::from_u128(1);
        let row = json!({"n": {"uuid": uuid, "chain_id": uuid,
            "entity_type": "Service", "namespace": "prod", "name": "checkout",
            "score": 99.0}, "score": 0.75});
        let hit = decode_node(
            row.as_object().unwrap().clone(),
            kg_core::embedding::TEXT_VERSION,
        )
        .unwrap();
        assert_eq!(hit.uuid, uuid);
        assert_eq!(hit.entity_type, "Service");
        assert_eq!(hit.score, 0.75);
        assert!(hit.embedding.is_none());
        assert!(hit.properties.get("n").is_none());
    }

    #[test]
    fn snapshot_decoder_preserves_offsets_identity_and_page_truncation() {
        let uuid = Uuid::from_u128(1);
        let content = format!("{}İ Needle", "🌍".repeat(5000));
        let row = json!({"uuid": uuid, "name": "runbook", "source": "docs",
            "namespace": "prod", "content": content,
            "content_length": content.chars().count(), "score": 1.0})
        .as_object()
        .unwrap()
        .clone();
        let page = decode_snapshots(vec![row.clone(), row.clone()], 1, Some("needle")).unwrap();
        assert!(page.truncated);
        let hit = &page.items[0];
        assert_eq!(hit.uuid, uuid);
        assert_eq!(hit.selection_kind, ExcerptSelection::Matched);
        assert_eq!(hit.content_start, 4746);
        assert_eq!(hit.content_end, 5008);
        assert!(hit.content.ends_with("İ Needle"));
        for length in [json!(null), json!(-1), json!(0), json!(5009)] {
            let mut malformed = row.clone();
            malformed.insert("content_length".into(), length);
            assert!(decode_snapshots(vec![malformed], 1, None).is_err());
        }
    }

    // ---- merged from `mod attached_tests`

    #[test]
    fn attached_rows_are_cut_per_anchor_and_shared_records_stay_under_each_anchor() {
        let fact = |anchor: u128, n: u128| {
            json!({"anchor": Uuid::from_u128(anchor), "uuid": Uuid::from_u128(n),
                "source_chain_id": Uuid::from_u128(1), "target_chain_id": Uuid::from_u128(2),
                "name": "USES", "description": "d", "score": 1.0})
            .as_object()
            .unwrap()
            .clone()
        };
        let page = decode_attached_relationships(
            vec![
                fact(1, 10),
                fact(1, 11),
                fact(1, 12),
                fact(2, 10),
                fact(3, 30),
            ],
            2,
        )
        .unwrap();
        assert!(page.truncated);
        assert!(!page.approximate);
        assert_eq!(
            page.items
                .iter()
                .map(|a| (a.anchor, a.record.uuid))
                .collect::<Vec<_>>(),
            vec![
                (Uuid::from_u128(1), Uuid::from_u128(10)),
                (Uuid::from_u128(1), Uuid::from_u128(11)),
                (Uuid::from_u128(2), Uuid::from_u128(10)),
                (Uuid::from_u128(3), Uuid::from_u128(30)),
            ]
        );
        assert!(
            !decode_attached_relationships(vec![fact(1, 10), fact(1, 11)], 2)
                .unwrap()
                .truncated
        );
        let mut broken = fact(1, 10);
        broken.insert("anchor".into(), json!("not-a-uuid"));
        assert!(decode_attached_relationships(vec![broken], 2).is_err());
    }

    // ---- merged from `mod count_tests`

    #[test]
    fn unloaded_zero_and_invalid_counts_are_distinct() {
        let mut row =
            json!({"uuid": Uuid::from_u128(1), "chain_id": Uuid::from_u128(1), "score": 1.0})
                .as_object()
                .unwrap()
                .clone();
        assert_eq!(
            decode_node(row.clone(), kg_core::embedding::TEXT_VERSION)
                .unwrap()
                .observation_count,
            None
        );
        row.insert("observations".into(), json!(0));
        assert_eq!(
            decode_node(row.clone(), kg_core::embedding::TEXT_VERSION)
                .unwrap()
                .observation_count,
            Some(0)
        );
        for invalid in [json!(-1), json!(1.5), json!("2"), json!(null)] {
            row.insert("observations".into(), invalid);
            assert!(decode_node(row.clone(), kg_core::embedding::TEXT_VERSION).is_err());
        }
    }
}
