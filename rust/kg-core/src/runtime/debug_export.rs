//! Complete diagnostic views of native stage handoffs.
//!
//! Several handoff fields are `#[serde(skip)]` because they are transient work
//! regenerated from durable checkpoints; ordinary serialization silently omits
//! them, so a capture made that way cannot show what a stage really received or
//! returned. These exporters render every field, including the skipped ones,
//! for evidence captures and pass-through comparisons. They are read-only and
//! never feed back into processing.
use serde_json::{json, Map, Value};

use super::stage_output::{
    EdgeExtractionOutput, PendingReference, ReferenceCandidate, RelationshipTarget,
};

/// A run target exactly as reference discovery indexed it.
pub fn relationship_target(target: &RelationshipTarget) -> Value {
    json!({
        "chain_id": target.chain_id,
        "name": target.name,
        "entity_type": target.entity_type,
        "namespace": target.namespace,
        "version_uuid": target.version_uuid,
        "version": target.version,
        "key_groups": target.key_groups.iter().map(|group| json!({
            "components": group.components.iter().map(|component| json!({
                "property": component.property,
                "type_tag": component.type_tag,
                "value": component.value,
                "token": component.token(),
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

fn reference_candidate(candidate: &ReferenceCandidate) -> Value {
    json!({
        "target": relationship_target(&candidate.target),
        "matched_key_group": candidate.matched_key_group,
    })
}

/// One pending occurrence: the frozen source version, the intent and the
/// frozen candidate set.
pub fn pending_reference(reference: &PendingReference) -> Value {
    json!({
        "source": reference.source,
        "intent": reference.intent,
        "value": reference.value,
        "components": reference.components,
        "candidates": reference.candidates.iter().map(reference_candidate).collect::<Vec<_>>(),
    })
}

/// The complete edge-extraction handoff, skipped fields included. The
/// `serialized` member is what plain serialization would have produced, so a
/// reader can see exactly which fields ordinary captures lose.
pub fn edge_extraction_handoff(output: &EdgeExtractionOutput) -> Value {
    let resolution = &output.resolution;
    json!({
        "relationship_times": output.relationship_times,
        "reference_report": output.reference_report,
        "relationship_directives": output.relationship_directives,
        "pending_references": output.pending_references.iter().map(pending_reference).collect::<Vec<_>>(),
        "snapshot_nodes": output.snapshot_nodes,
        "resolution": {
            "serialized": resolution.as_ref(),
            "reference_owner_refresh": resolution.reference_owner_refresh,
            "chunk_entities": resolution.chunk_entities.as_ref().map(|targets| {
                targets.iter().map(relationship_target).collect::<Vec<_>>()
            }),
        },
        "resolved_nodes": output.resolved_nodes,
        "edges": output.edges,
        "serialized": output,
    })
}

/// Field-by-field comparison of two handoff views. `changed` lists the
/// top-level members whose rendered value differs; everything else passed
/// through unchanged. Nested members under `resolution` are compared
/// individually so a pass-through failure names the exact member.
pub fn handoff_diff(before: &Value, after: &Value) -> Value {
    fn members(value: &Value, prefix: &str, out: &mut Map<String, Value>) {
        if let Some(object) = value.as_object() {
            for (key, member) in object {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                if key == "resolution" && prefix.is_empty() {
                    members(member, &path, out);
                } else {
                    out.insert(path, member.clone());
                }
            }
        }
    }
    let mut left = Map::new();
    let mut right = Map::new();
    members(before, "", &mut left);
    members(after, "", &mut right);
    let mut unchanged = Vec::new();
    let mut changed = Vec::new();
    let mut only_before = Vec::new();
    let mut only_after = Vec::new();
    for (key, value) in &left {
        match right.get(key) {
            Some(other) if other == value => unchanged.push(key.clone()),
            Some(_) => changed.push(key.clone()),
            None => only_before.push(key.clone()),
        }
    }
    for key in right.keys() {
        if !left.contains_key(key) {
            only_after.push(key.clone());
        }
    }
    json!({
        "unchanged": unchanged,
        "changed": changed,
        "only_before": only_before,
        "only_after": only_after,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_export_carries_every_skipped_field_and_the_diff_names_changes() {
        let mut output = EdgeExtractionOutput {
            relationship_times: Default::default(),
            reference_report: Default::default(),
            relationship_directives: Default::default(),
            pending_references: Default::default(),
            snapshot_nodes: Default::default(),
            resolution: Default::default(),
            resolved_nodes: Default::default(),
            edges: Default::default(),
        };
        let before = edge_extraction_handoff(&output);
        for key in [
            "pending_references",
            "snapshot_nodes",
            "resolved_nodes",
            "edges",
            "serialized",
        ] {
            assert!(before.get(key).is_some(), "{key} missing");
        }
        assert!(before["serialized"].get("pending_references").is_none());
        assert!(before["resolution"].get("chunk_entities").is_some());
        output.reference_report.attempted = 3;
        let after = edge_extraction_handoff(&output);
        let diff = handoff_diff(&before, &after);
        assert_eq!(diff["changed"], json!(["reference_report", "serialized"]));
        assert!(diff["unchanged"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k == "resolution.chunk_entities"));
        assert!(diff["only_before"].as_array().unwrap().is_empty());
        assert!(diff["only_after"].as_array().unwrap().is_empty());
    }
}
