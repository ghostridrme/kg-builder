//! Complete evidence packets for one ambiguous reference occurrence.
//!
//! Everything a decision may rest on is assembled here, before any model call:
//! the producing observation's own properties (or original text), the frozen
//! candidates hydrated to their complete stored records, and a manifest of
//! short host-assigned evidence identifiers the answer may cite. Nothing is
//! trimmed to fit; a packet that exceeds its budget, lacks required content or
//! has its occurrence disclosure-restricted is reported as an explicit host
//! refusal with zero provider attempts.
use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use kg_core::{
    errors::StageError,
    models::{edges::ReadVersion, EntityNode, PropertyValue, SnapshotNode},
    runtime::{
        extraction::ReferencePath,
        reference_resolution::{
            CitedValueHash, DecisionReason, EvidenceCitation, EvidenceKind, EvidenceOrigin,
            ReferenceResolutionSettings, MAX_EVIDENCE_ID_LEN,
        },
        stage_output::{PendingReference, RelationshipTarget},
        RuntimeContext,
    },
    traits::{
        graph_reads::MAX_LOOKUP_KEYS, llm_backend::LlmMessage, EntityLookup, EntityVersionRecord,
    },
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::edge::reference_extraction::path_without_indexes;

/// Packet layout revision; part of every evidence fingerprint.
pub(super) const PACKET_SCHEMA: &str = "reference-resolution-evidence-v1";

/// Bookkeeping never offered as semantic evidence.
const EXCLUDED_SYSTEM_FIELDS: [&str; 2] = ["embedding", "operational_graph_metadata"];

/// A frozen candidate, hydrated to the exact stored version discovery read.
#[derive(Debug, Clone)]
pub(super) struct HydratedCandidate {
    pub target: RelationshipTarget,
    pub matched_key_group: Vec<String>,
    pub source: Option<String>,
    pub primary_key_properties: Vec<String>,
    pub additional_key_properties: Vec<Vec<String>>,
    pub properties: indexmap::IndexMap<String, PropertyValue>,
    pub labels: Vec<String>,
    pub tags: BTreeMap<String, String>,
    /// Observation clock of the hydrated version; fenced at commit because
    /// volatile properties can change without a new version.
    pub observed_at: Option<DateTime<Utc>>,
}

/// One evidence item the answer may cite, with the resolution Rust keeps for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ManifestItem {
    pub id: String,
    pub owner_chain_id: Uuid,
    pub kind: EvidenceKind,
    pub path: Option<String>,
    pub snapshot_uuid: Option<Uuid>,
    pub start_char: Option<usize>,
    pub end_char: Option<usize>,
}

impl ManifestItem {
    pub fn citation(&self) -> EvidenceCitation {
        EvidenceCitation {
            id: self.id.clone(),
            owner_chain_id: self.owner_chain_id,
            kind: self.kind,
            path: self.path.clone(),
            snapshot_uuid: self.snapshot_uuid,
            start_char: self.start_char,
            end_char: self.end_char,
        }
    }
}

/// A permitted property withheld from the model by a disclosure restriction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Omitted {
    pub owner_chain_id: Uuid,
    pub path: String,
}

/// A complete packet ready to render, plus what the host must remember about it.
#[derive(Debug, Clone)]
pub(super) struct Prepared {
    pub packet: Value,
    pub manifest: Vec<ManifestItem>,
    pub fingerprint: String,
    /// What the answer depends on and nothing volatile: the persisted-reuse key.
    pub reuse_fingerprint: String,
    /// Candidate identities and key shapes, for the persisted record.
    pub candidate_details: Vec<kg_core::runtime::reference_resolution::PersistedCandidate>,
    pub origin: EvidenceOrigin,
    pub snapshot_id: Uuid,
    pub captured_at: DateTime<Utc>,
    pub source_version_uuid: Uuid,
    /// Frozen candidates in packet order (by chain id).
    pub candidates: Vec<Uuid>,
    pub read_set: Vec<ReadVersion>,
    pub omitted: Vec<Omitted>,
    pub source_chain_id: Uuid,
    /// The occurrence's indexed location in this observation.
    pub location: String,
}

impl Prepared {
    pub fn item(&self, id: &str) -> Option<&ManifestItem> {
        self.manifest.iter().find(|item| item.id == id)
    }

    /// SHA-256 of the canonical JSON value shown for one structured property,
    /// addressed by owner and path (never by the packet-positional id).
    pub fn value_hash(&self, owner: Uuid, path: &str) -> Option<String> {
        let owner = owner.to_string();
        self.packet["evidence_items"]
            .as_array()?
            .iter()
            .find(|item| item["owner_id"] == owner && item["path"] == path)
            .and_then(|item| serde_json::to_vec(&item["value"]).ok())
            .map(|bytes| format!("{:x}", Sha256::digest(&bytes)))
    }

    /// The path a persisted cited-value hash records for one shown property.
    /// Source-owned paths that name the occurrence or its enclosing element are
    /// stored symbolically, so a later observation that carries the same tag at
    /// another array position still compares the same values (the reuse key is
    /// index-free by design). Every other path is stored as shown.
    pub fn symbolic_path(&self, owner: Uuid, path: &str) -> String {
        if owner == self.source_chain_id {
            if path == self.location {
                return OCCURRENCE_PATH.into();
            }
            if enclosing_path(&self.location).as_deref() == Some(path) {
                return ENCLOSING_PATH.into();
            }
        }
        path.to_owned()
    }

    /// The path in this packet that a persisted cited path refers to.
    pub fn concrete_path(&self, owner: Uuid, path: &str) -> Option<String> {
        if owner != self.source_chain_id {
            return Some(path.to_owned());
        }
        match path {
            OCCURRENCE_PATH => Some(self.location.clone()),
            ENCLOSING_PATH => enclosing_path(&self.location),
            other => Some(other.to_owned()),
        }
    }

    /// The hash of the value currently shown for a persisted cited path.
    pub fn current_value_hash(&self, owner: Uuid, path: &str) -> Option<String> {
        let concrete = self.concrete_path(owner, path)?;
        self.value_hash(owner, &concrete)
    }

    /// A cited-value hash for one shown property, keyed symbolically.
    pub fn cited_hash(&self, owner: Uuid, path: &str) -> Option<CitedValueHash> {
        let concrete = self.concrete_path(owner, path)?;
        Some(CitedValueHash {
            owner_chain_id: owner,
            path: self.symbolic_path(owner, &concrete),
            sha256: self.value_hash(owner, &concrete)?,
        })
    }

    pub fn target_type(&self, chain: Uuid) -> Option<String> {
        self.candidate_details
            .iter()
            .find(|c| c.chain_id == chain)
            .map(|c| c.entity_type.clone())
    }
}

/// The host's verdict before any dispatch.
#[derive(Debug, Clone)]
pub(super) enum Preparation {
    /// Complete permitted evidence that fits the budget.
    Ready(Box<Prepared>),
    /// Recorded as explicit uncertainty with zero provider attempts.
    Refused {
        reason: DecisionReason,
        prepared: Box<Prepared>,
    },
}

fn read_failure(stage: &str, error: kg_core::errors::BackendError) -> StageError {
    StageError::StepFailed {
        stage: stage.into(),
        step: "candidate_hydration".into(),
        cause: "frozen candidate hydration read failed".into(),
        retriable: error.is_transient(),
    }
}

/// Load every frozen candidate's exact stored version in bounded batched reads
/// and verify it is still the version discovery matched: same UUID, revision,
/// namespace, type and complete key groups, live and latest. Any difference is
/// changed identity evidence; the caller replans within its existing bounds
/// rather than deciding against a newer row.
pub(super) async fn hydrate_candidates(
    ctx: &RuntimeContext,
    stage: &str,
    pending: &[PendingReference],
) -> Result<HashMap<Uuid, HydratedCandidate>, StageError> {
    let mut frozen: BTreeMap<Uuid, &kg_core::runtime::stage_output::ReferenceCandidate> =
        BTreeMap::new();
    for reference in pending {
        for candidate in &reference.candidates {
            match frozen.get(&candidate.target.chain_id) {
                Some(known) if known.target.version_uuid != candidate.target.version_uuid => {
                    // Two occurrences froze different versions of one chain: the
                    // evidence is already inconsistent within this snapshot.
                    return Err(StageError::IdentityRevisionChanged);
                }
                Some(_) => {}
                None => {
                    frozen.insert(candidate.target.chain_id, candidate);
                }
            }
        }
    }
    let chain_ids: Vec<Uuid> = frozen.keys().copied().collect();
    let mut records: HashMap<Uuid, EntityVersionRecord> = HashMap::new();
    for chunk in chain_ids.chunks(MAX_LOOKUP_KEYS) {
        let found = ctx
            .graph
            .find_entities(
                ctx.org_id.as_ref(),
                &EntityLookup::LatestByChain {
                    chain_ids: chunk.to_vec(),
                },
            )
            .await
            .map_err(|error| read_failure(stage, error))?;
        for record in found {
            if records.insert(record.chain_id, record).is_some() {
                return Err(StageError::StateValidation {
                    stage: stage.into(),
                    message: "candidate hydration returned one chain twice".into(),
                });
            }
        }
    }
    let mut hydrated = HashMap::with_capacity(frozen.len());
    for (chain_id, candidate) in frozen {
        let Some(record) = records.remove(&chain_id) else {
            return Err(StageError::IdentityRevisionChanged);
        };
        let target = &candidate.target;
        let current = RelationshipTarget::from(record.clone());
        if record.uuid != target.version_uuid
            || record.version != target.version
            || record.namespace != target.namespace
            || record.entity_type != target.entity_type
            || !record.is_latest
            || record.deleted_at.is_some()
            || record.merged_into.is_some()
            || current.key_groups != target.key_groups
        {
            kg_core::telemetry::reference_resolution_outcome(
                kg_core::telemetry::ReferenceResolutionOutcome::HydrationChanged,
                1,
            );
            return Err(StageError::IdentityRevisionChanged);
        }
        let properties =
            record
                .typed_source_properties()
                .map_err(|_| StageError::StateValidation {
                    stage: stage.into(),
                    message: "hydrated candidate has invalid stored properties".into(),
                })?;
        let strings = |key: &str| -> Vec<String> {
            record
                .stored
                .get(key)
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default()
        };
        let additional_key_properties: Vec<Vec<String>> = record
            .stored
            .get("additional_key_properties")
            .and_then(|value| match value {
                Value::String(text) => serde_json::from_str(text).ok(),
                Value::Array(_) => serde_json::from_value(value.clone()).ok(),
                _ => None,
            })
            .unwrap_or_default();
        let tags = record
            .stored
            .iter()
            .filter_map(|(key, value)| {
                Some((
                    key.strip_prefix(kg_core::traits::graph_mutation::TAG_PROPERTY_PREFIX)?
                        .to_owned(),
                    value.as_str()?.to_owned(),
                ))
            })
            .collect();
        hydrated.insert(
            chain_id,
            HydratedCandidate {
                target: target.clone(),
                matched_key_group: candidate.matched_key_group.clone(),
                source: record.source.clone(),
                primary_key_properties: strings("primary_key_properties"),
                additional_key_properties,
                properties,
                labels: strings("labels"),
                tags,
                observed_at: record.last_seen_at,
            },
        );
    }
    Ok(hydrated)
}

/// Whether `path` (indexed or not) falls under a configured restriction.
fn restricted(path: &str, restrictions: &[String]) -> bool {
    let path = path_without_indexes(path);
    restrictions.iter().any(|restriction| {
        let restriction = path_without_indexes(restriction);
        // Either the shown path lies under the restriction, or the restriction
        // points inside a value the store keeps whole (an array element field,
        // say). Whole values are never partially redacted: the entire property
        // is withheld and listed, so a restriction can never be bypassed by the
        // enclosing value's serialization.
        path == restriction
            || path.starts_with(&format!("{restriction}."))
            || restriction.starts_with(&format!("{path}."))
    })
}

/// A source-form JSON view of one typed property; opaque JSON is expanded.
fn property_json(value: &PropertyValue) -> Value {
    value.to_source().unwrap_or_else(|_| value.to_bolt_json())
}

/// The value at an exact indexed path inside flattened properties, using the
/// reference path grammar (escaped dots, `seg[n]`), never naive splitting.
pub(super) fn value_at(
    properties: &indexmap::IndexMap<String, PropertyValue>,
    location: &str,
) -> Option<Value> {
    let path = ReferencePath::parse(location).ok()?;
    if path.raw || path.segments.is_empty() {
        return None;
    }
    // The flattened key is the longest run of leading segments that names a
    // stored property; the rest is navigated inside its JSON value.
    for split in (1..=path.segments.len()).rev() {
        let (head, tail) = path.segments.split_at(split);
        if head[..split - 1]
            .iter()
            .any(|segment| segment.index.is_some())
        {
            continue;
        }
        let key = head
            .iter()
            .map(|segment| segment.key.as_str())
            .collect::<Vec<_>>()
            .join(".");
        let Some(value) = properties.get(&key) else {
            continue;
        };
        let mut current = property_json(value);
        if let Some(Some(index)) = head[split - 1].index {
            current = current.as_array()?.get(index)?.clone();
        } else if head[split - 1].index.is_some() {
            return None;
        }
        for segment in tail {
            current = current.as_object()?.get(&segment.key)?.clone();
            match segment.index {
                Some(Some(index)) => current = current.as_array()?.get(index)?.clone(),
                Some(None) => return None,
                None => {}
            }
        }
        return Some(current);
    }
    None
}

/// The enclosing object's path of an indexed or nested location, if any.
fn enclosing_path(location: &str) -> Option<String> {
    let path = ReferencePath::parse(location).ok()?;
    if path.raw {
        return None;
    }
    let mut segments = path.segments;
    segments.pop()?;
    if segments.is_empty() {
        // `Tags[0]` encloses `Tags[0].Value`; a bare top-level scalar has no
        // enclosing object beyond the record itself.
        return None;
    }
    Some(
        segments
            .iter()
            .map(|segment| {
                let key = segment.key.chars().fold(String::new(), |mut out, c| {
                    if matches!(c, '.' | '\\' | '[' | ']') {
                        out.push('\\');
                    }
                    out.push(c);
                    out
                });
                match segment.index {
                    Some(Some(index)) => format!("{key}[{index}]"),
                    Some(None) => format!("{key}[]"),
                    None => key,
                }
            })
            .collect::<Vec<_>>()
            .join("."),
    )
}

/// Render a candidate's key groups, withholding any component whose property is
/// disclosure-restricted. A restricted key value is never sent to the model as
/// evidence, matching the contract in `ReferenceResolutionSettings`; the withheld
/// component is recorded under `omitted` like any other restricted path.
fn key_groups_json(
    target: &RelationshipTarget,
    owner_chain_id: Uuid,
    restrictions: &[String],
    omitted: &mut Vec<Omitted>,
) -> Value {
    Value::Array(
        target
            .key_groups
            .iter()
            .map(|group| {
                Value::Array(
                    group
                        .components
                        .iter()
                        .filter_map(|component| {
                            if restricted(&component.property, restrictions) {
                                omitted.push(Omitted {
                                    owner_chain_id,
                                    path: component.property.clone(),
                                });
                                None
                            } else {
                                Some(json!({"property": component.property, "type": component.type_tag, "value": component.value}))
                            }
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}

struct Item {
    view: Value,
    manifest: ManifestItem,
}

fn property_item(
    id: String,
    owner: Uuid,
    path: &str,
    value: Value,
    interpretation_only: bool,
) -> Item {
    debug_assert!(id.len() <= MAX_EVIDENCE_ID_LEN && id.is_ascii());
    Item {
        view: json!({"id": id, "owner_id": owner, "kind": "structured_property", "path": path, "value": value, "interpretation_only": interpretation_only}),
        manifest: ManifestItem {
            id,
            owner_chain_id: owner,
            kind: EvidenceKind::StructuredProperty,
            path: Some(path.to_owned()),
            snapshot_uuid: None,
            start_char: None,
            end_char: None,
        },
    }
}

/// Everything the packet, fingerprint and rendering depend on.
pub(super) struct Inputs<'a> {
    pub reference: &'a PendingReference,
    pub snapshot: &'a SnapshotNode,
    pub hydrated: &'a HashMap<Uuid, HydratedCandidate>,
    pub settings: &'a ReferenceResolutionSettings,
    pub guidance: Option<&'a str>,
    pub model_id: &'a str,
    pub provider_descriptor: &'a Value,
    pub processing_version: &'a str,
    pub prompt_version: &'a str,
    pub schema_version: &'a str,
    /// Rendered prompt budget already reduced to the stricter configured bound.
    pub prompt_budget_bytes: usize,
    pub context_window: usize,
    pub answer_schema: &'a Value,
    pub system_prompt: &'a str,
}

/// What of the provider descriptor identifies the decider: the adapter, the
/// provider, the model and the reasoning effort. Deployment addressing (a proxy
/// port, an endpoint) changes between runs, and operational limits (context
/// window, default output budget) do not change an answer; neither is evidence.
/// An allowlist, so a new descriptor field never silently enters the key.
pub(super) fn reuse_descriptor(descriptor: &Value) -> Value {
    const KEPT: [&str; 4] = ["adapter_version", "model", "provider", "reasoning_effort"];
    match descriptor {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(key, _)| KEPT.contains(&key.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        // A non-object descriptor identifies nothing the key does not already
        // carry (the model id is a separate field).
        _ => Value::Null,
    }
}

/// Symbolic path of the occurrence itself in a persisted cited-value hash.
pub const OCCURRENCE_PATH: &str = "@occurrence";
/// Symbolic path of the occurrence's enclosing element in a persisted cited-value hash.
pub const ENCLOSING_PATH: &str = "@enclosing";

/// Assemble the complete permitted packet for one occurrence and decide, before
/// any dispatch, whether it can be sent at all.
pub(super) fn prepare(inputs: &Inputs<'_>, stage: &str) -> Result<Preparation, StageError> {
    let reference = inputs.reference;
    let source: &EntityNode = &reference.source;
    let restrictions = &inputs.settings.disclosure_restricted_paths;
    let origin = EvidenceOrigin::of_extractor(&source.extracted_by);
    let snapshot_id = source
        .last_seen_snapshot_id
        .filter(|id| *id == inputs.snapshot.uuid)
        .ok_or_else(|| StageError::StateValidation {
            stage: stage.into(),
            message: "pending reference does not belong to the supplied snapshot".into(),
        })?;
    let captured_at = source.last_seen_at.unwrap_or(source.valid_from);
    let interpretation_only = origin == EvidenceOrigin::TextExtracted;
    let mut omitted = Vec::new();
    let mut items: Vec<Item> = Vec::new();

    // Source properties: every flattened property is an item; restricted paths
    // are withheld and listed, never silently absent.
    let mut source_properties = BTreeMap::new();
    for (index, (path, value)) in source.all_properties.iter().enumerate() {
        if restricted(path, restrictions) {
            omitted.push(Omitted {
                owner_chain_id: source.chain_id,
                path: path.clone(),
            });
            continue;
        }
        let view = property_json(value);
        source_properties.insert(path.clone(), view.clone());
        items.push(property_item(
            format!("src:p{}", index + 1),
            source.chain_id,
            path,
            view,
            interpretation_only,
        ));
    }
    let occurrence_restricted = restricted(&reference.intent.location, restrictions);
    if !occurrence_restricted {
        // The exact occurrence. When the indexed path cannot be walked (guided
        // `raw:` paths, every-element paths), the item says so explicitly and
        // carries discovery's canonical value instead of claiming a resolved path.
        let mut item = match value_at(&source.all_properties, &reference.intent.location) {
            Some(value) => property_item(
                "src:occ".into(),
                source.chain_id,
                &reference.intent.location,
                value,
                interpretation_only,
            ),
            None => property_item(
                "src:occ".into(),
                source.chain_id,
                &reference.intent.location,
                Value::String(reference.value.clone()),
                interpretation_only,
            ),
        };
        item.view["exact_path_resolved"] =
            Value::Bool(value_at(&source.all_properties, &reference.intent.location).is_some());
        items.push(item);
        if let Some(parent) = enclosing_path(&reference.intent.location) {
            if !restricted(&parent, restrictions) {
                if let Some(value) = value_at(&source.all_properties, &parent) {
                    items.push(property_item(
                        "src:ctx".into(),
                        source.chain_id,
                        &parent,
                        value,
                        interpretation_only,
                    ));
                }
            }
        }
    }
    let content = (origin == EvidenceOrigin::TextExtracted)
        .then(|| inputs.snapshot.content.clone())
        .flatten();
    if let Some(text) = &content {
        let chars = text.chars().count();
        items.push(Item {
            view: json!({"id": "src:text", "owner_id": source.chain_id, "kind": "text_excerpt", "snapshot_id": snapshot_id, "start_char": 0, "end_char": chars, "text": text, "interpretation_only": false}),
            manifest: ManifestItem {
                id: "src:text".into(),
                owner_chain_id: source.chain_id,
                kind: EvidenceKind::TextExcerpt,
                path: None,
                snapshot_uuid: Some(snapshot_id),
                start_char: Some(0),
                end_char: Some(chars),
            },
        });
    }

    // Candidates in chain order: the order is not evidence and never reaches
    // the fingerprint as a distinguishing feature.
    let mut candidates: Vec<&HydratedCandidate> = reference
        .candidates
        .iter()
        .map(|candidate| {
            inputs
                .hydrated
                .get(&candidate.target.chain_id)
                .filter(|hydrated| hydrated.target.version_uuid == candidate.target.version_uuid)
                .ok_or_else(|| StageError::StateValidation {
                    stage: stage.into(),
                    message: "pending reference candidate was not hydrated".into(),
                })
        })
        .collect::<Result<_, _>>()?;
    candidates.sort_by_key(|candidate| candidate.target.chain_id);
    // A matched key group whose components are disclosure-restricted cannot be
    // described to the model without leaking the restricted identity value (the
    // reference value itself is that value), and withholding only some of its
    // components would make a partial group look complete. Either case is an
    // ordinary host refusal, recorded after the packet is assembled — never a
    // pipeline state failure that would sink unrelated snapshots.
    let mut matched_key_restricted = false;
    let mut candidate_views = Vec::with_capacity(candidates.len());
    let mut canonical_candidates = Vec::with_capacity(candidates.len());
    let mut read_set = Vec::with_capacity(candidates.len());
    for (position, candidate) in candidates.iter().enumerate() {
        let chain = candidate.target.chain_id;
        // The group the candidate was matched on carries the reference value. If
        // any component of it is restricted the packet cannot honestly describe
        // the match without disclosing that value (a partially shown group would
        // look complete), so flag a refusal rather than leak it.
        if candidate
            .matched_key_group
            .iter()
            .any(|property| restricted(property, restrictions))
        {
            matched_key_restricted = true;
        }
        let key_groups_view = key_groups_json(&candidate.target, chain, restrictions, &mut omitted);
        let mut properties = BTreeMap::new();
        for (index, (path, value)) in candidate.properties.iter().enumerate() {
            if restricted(path, restrictions) {
                omitted.push(Omitted {
                    owner_chain_id: chain,
                    path: path.clone(),
                });
                continue;
            }
            let view = property_json(value);
            properties.insert(path.clone(), view.clone());
            items.push(property_item(
                format!("c{}:p{}", position + 1, index + 1),
                chain,
                path,
                view,
                false,
            ));
        }
        candidate_views.push(json!({
            "candidate_id": chain,
            "matched_key_group": candidate.matched_key_group,
            "entity": {
                "chain_id": chain,
                "version_uuid": candidate.target.version_uuid,
                "version": candidate.target.version,
                "org_id": source.org_id,
                "namespace": candidate.target.namespace,
                "source": candidate.source,
                "entity_type": candidate.target.entity_type,
                "display_name": candidate.target.name,
                "primary_key_properties": candidate.primary_key_properties,
                "additional_key_properties": candidate.additional_key_properties,
                "key_groups": key_groups_view.clone(),
                "properties": properties,
                "labels": candidate.labels,
                "tags": candidate.tags,
            }
        }));
        canonical_candidates.push(json!({
            "chain_id": chain,
            "version_uuid": candidate.target.version_uuid,
            "version": candidate.target.version,
            "observed_at": candidate.observed_at,
            "namespace": candidate.target.namespace,
            "entity_type": candidate.target.entity_type,
            "name": candidate.target.name,
            "producer": candidate.source,
            "primary_key_properties": candidate.primary_key_properties,
            "additional_key_properties": candidate.additional_key_properties,
            "labels": candidate.labels,
            "tags": candidate.tags,
            "matched_key_group": candidate.matched_key_group,
            "key_groups": key_groups_view,
            "properties": properties,
        }));
        read_set.push(ReadVersion {
            chain_id: chain,
            version_uuid: candidate.target.version_uuid,
            version: candidate.target.version,
            observed_at: candidate.observed_at,
        });
    }

    let mut sorted_components = reference.components.clone();
    sorted_components.sort();
    // A restricted key property can be recorded both as a candidate property and
    // as a withheld key component; collapse the duplicates so each owner/path is
    // listed once.
    omitted.sort_by(|a, b| (a.owner_chain_id, &a.path).cmp(&(b.owner_chain_id, &b.path)));
    omitted.dedup_by(|a, b| a.owner_chain_id == b.owner_chain_id && a.path == b.path);
    let mut omitted_paths: Vec<Value> = omitted
        .iter()
        .map(|entry| json!({"owner_id": entry.owner_chain_id, "path": entry.path, "reason": "disclosure_restricted"}))
        .collect();
    omitted_paths.sort_by_key(ToString::to_string);
    let mut restriction_list = restrictions.clone();
    restriction_list.sort();

    let canonical = json!({
        "schema": PACKET_SCHEMA,
        "org": source.org_id,
        "source": {
            "chain_id": source.chain_id,
            "version_uuid": source.uuid,
            "version": source.version,
            "namespace": source.namespace,
            "producer": source.source,
            "entity_type": source.entity_type,
            "name": source.name,
            "primary_key_properties": source.primary_key_properties,
            "additional_key_properties": source.additional_key_properties,
            "properties": source_properties,
            "labels": source.labels,
            "tags": source.tags,
            "origin": origin,
            "content": content,
            "captured_at": captured_at,
        },
        "intent": reference.intent,
        "value": reference.value,
        "components": sorted_components,
        "candidates": canonical_candidates,
        "guidance": inputs.guidance,
        "restrictions": restriction_list,
        "omitted": omitted_paths,
        "settings": {
            "max_prompt_bytes": inputs.prompt_budget_bytes,
            "max_output_tokens": inputs.settings.max_output_tokens,
            "max_supporting_items": inputs.settings.max_supporting_items,
            "max_fact_chars": inputs.settings.max_fact_chars,
        },
        "model": inputs.model_id,
        "provider": inputs.provider_descriptor,
        "prompt_version": inputs.prompt_version,
        "schema_version": inputs.schema_version,
        "processing_version": inputs.processing_version,
        "system_prompt": inputs.system_prompt,
    });
    let canonical_bytes =
        serde_json::to_vec(&canonical).map_err(|_| StageError::StateValidation {
            stage: stage.into(),
            message: "evidence packet cannot be serialized".into(),
        })?;
    let fingerprint = format!("{:x}", Sha256::digest(&canonical_bytes));

    // The reuse key: everything the answer depends on, nothing volatile. No
    // non-key properties, versions, clocks, snapshot ids, array indexes or
    // candidate order; see `reuse_descriptor` and the reuse key fields below.
    let candidate_details: Vec<kg_core::runtime::reference_resolution::PersistedCandidate> =
        candidates
            .iter()
            .map(
                |candidate| kg_core::runtime::reference_resolution::PersistedCandidate {
                    chain_id: candidate.target.chain_id,
                    entity_type: candidate.target.entity_type.clone(),
                    key_groups: candidate
                        .target
                        .key_groups
                        .iter()
                        .map(|group| {
                            group
                                .components
                                .iter()
                                .map(|c| c.property.clone())
                                .collect()
                        })
                        .collect(),
                },
            )
            .collect();
    // The occurrence's immediate context: the enclosing element as shown (the
    // tag element of a tag value, the record of a nested field). Two members of
    // one slot with the same value and different context (`BackupBucket` and
    // `Team` tags both naming `archive`) are different decisions and must not
    // share one record. A top-level scalar has no enclosing element.
    let context_sha256 = enclosing_path(&reference.intent.location)
        .filter(|parent| !restricted(parent, restrictions))
        .and_then(|parent| value_at(&source.all_properties, &parent))
        .and_then(|value| serde_json::to_vec(&value).ok())
        .map(|bytes| format!("{:x}", Sha256::digest(&bytes)));
    let reuse_canonical = json!({
        "schema": PACKET_SCHEMA,
        "org": source.org_id,
        "producer": source.source,
        "source_chain_id": source.chain_id,
        "source_entity_type": source.entity_type,
        // Shown to the model and not citable as structured properties: part of
        // what the verdict may rest on, so part of the key.
        "source_name": source.name,
        "source_labels": source.labels,
        "source_tags": source.tags,
        "slot": reference.intent.slot,
        // Not the source's typed components: those carry every property token
        // of the source, so any non-key drift would re-key every decision. The
        // matched identity is the value plus each candidate's key tokens below.
        "value": reference.value,
        "context_sha256": context_sha256,
        "origin": origin,
        "content_sha256": content.as_ref().map(|text| format!("{:x}", Sha256::digest(text.as_bytes()))),
        "candidates": candidates.iter().map(|candidate| json!({
            "chain_id": candidate.target.chain_id,
            "entity_type": candidate.target.entity_type,
            "namespace": candidate.target.namespace,
            "name": candidate.target.name,
            "labels": candidate.labels,
            "tags": candidate.tags,
            "key_groups": candidate.target.key_groups.iter().map(|group| group.components.iter().map(|c| json!([c.property, c.token()])).collect::<Vec<_>>()).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "direction": reference.intent.direction,
        "cardinality": reference.intent.cardinality,
        "target_key_group": reference.intent.target_key_group,
        "relationship_name": reference.intent.relationship_name,
        "policy_fingerprint": reference.intent.policy_fingerprint,
        "guidance": inputs.guidance,
        "restrictions": restriction_list,
        "processing_version": inputs.processing_version,
        // The configured model: a verdict served by a fallback model is keyed
        // under the primary it stood in for (the audit records `model_served`).
        "model": inputs.model_id,
        "provider": reuse_descriptor(inputs.provider_descriptor),
    });
    let reuse_fingerprint = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&reuse_canonical).map_err(|_| {
            StageError::StateValidation {
                stage: stage.into(),
                message: "reuse key cannot be serialized".into(),
            }
        })?)
    );

    let value_type = reference
        .components
        .last()
        .and_then(|(_, token)| token.split_once(':'))
        .map(|(tag, _)| tag.to_owned())
        .unwrap_or_else(|| "s".into());
    let (item_views, manifest): (Vec<Value>, Vec<ManifestItem>) = items
        .into_iter()
        .map(|item| (item.view, item.manifest))
        .unzip();
    let packet = json!({
        "schema": PACKET_SCHEMA,
        "source_observation": {
            "snapshot_id": snapshot_id,
            "captured_at": captured_at,
            "evidence_origin": origin,
            "content": content,
            "entity": {
                "chain_id": source.chain_id,
                "version_uuid": source.uuid,
                "version": source.version,
                "org_id": source.org_id,
                "namespace": source.namespace,
                "source": source.source,
                "entity_type": source.entity_type,
                "display_name": source.name,
                "primary_key_properties": source.primary_key_properties,
                "additional_key_properties": source.additional_key_properties,
                "properties": source_properties,
                "labels": source.labels,
                "tags": source.tags,
            }
        },
        "reference": {
            "location": reference.intent.location,
            "slot": reference.intent.slot,
            "value": reference.value,
            "value_type": value_type,
            "direction": reference.intent.direction,
            "cardinality": reference.intent.cardinality,
            "relationship_name": reference.intent.relationship_name,
            "target_key_group": reference.intent.target_key_group,
        },
        "candidate_lookup": {"complete": reference.intent.lookup_complete, "truncated": false},
        "candidates": candidate_views,
        "policy": {
            "org_id": source.org_id,
            "allowed_target_namespaces": reference.intent.allowed_namespaces,
            "edge_ambiguity": "llm",
            "relationship_guidance": inputs.guidance,
            "disclosure_restricted_paths": restriction_list,
        },
        "evidence_items": item_views,
        "coverage": {
            "permitted_evidence_complete": true,
            "omitted_items": omitted_paths,
            "excluded_system_fields": EXCLUDED_SYSTEM_FIELDS,
        }
    });
    let prepared = Box::new(Prepared {
        packet,
        manifest,
        fingerprint,
        reuse_fingerprint,
        candidate_details,
        origin,
        snapshot_id,
        captured_at,
        source_version_uuid: source.uuid,
        candidates: candidates.iter().map(|c| c.target.chain_id).collect(),
        read_set,
        omitted,
        source_chain_id: source.chain_id,
        location: reference.intent.location.clone(),
    });
    if occurrence_restricted || matched_key_restricted {
        return Ok(Preparation::Refused {
            reason: DecisionReason::EvidenceUnavailable,
            prepared,
        });
    }
    if origin == EvidenceOrigin::TextExtracted && content.is_none() {
        return Ok(Preparation::Refused {
            reason: DecisionReason::SourceContentUnavailable,
            prepared,
        });
    }
    let messages = render(&prepared, inputs.system_prompt, inputs.guidance);
    let schema = super::decision::packet_answer_schema(inputs.answer_schema, &prepared);
    let bytes = rendered_bytes(&messages, &schema);
    if bytes > inputs.prompt_budget_bytes
        || bytes.saturating_add(inputs.settings.max_output_tokens as usize) > inputs.context_window
    {
        return Ok(Preparation::Refused {
            reason: DecisionReason::EvidenceBudgetExceeded,
            prepared,
        });
    }
    Ok(Preparation::Ready(prepared))
}

/// The two messages actually sent: generic instructions (with subordinate
/// source guidance) and the fenced packet. The fence nonce is random and is
/// excluded from every fingerprint.
pub(super) fn render(
    prepared: &Prepared,
    system_prompt: &str,
    guidance: Option<&str>,
) -> Vec<LlmMessage> {
    use kg_core::traits::llm_backend::MessageRole;
    vec![
        LlmMessage {
            role: MessageRole::System,
            content: crate::node::extraction_support::with_guidance(system_prompt, guidance),
        },
        LlmMessage {
            role: MessageRole::User,
            content: kg_core::sanitize::fence_untrusted(&prepared.packet.to_string()),
        },
    ]
}

/// The same accounting as the shared prompt budget check: message bytes, the
/// schema and a fixed allowance for provider framing.
pub(super) fn rendered_bytes(messages: &[LlmMessage], schema: &Value) -> usize {
    messages
        .iter()
        .fold(schema.to_string().len().saturating_add(1024), |n, m| {
            n.saturating_add(m.content.len())
        })
}

/// Identifiers cited by an answer resolve back to manifest items; unknown or
/// repeated identifiers are rejected by the caller.
pub(super) fn cited<'a>(prepared: &'a Prepared, ids: &[String]) -> Option<Vec<&'a ManifestItem>> {
    let mut seen = BTreeSet::new();
    ids.iter()
        .map(|id| {
            if !seen.insert(id.as_str()) {
                return None;
            }
            prepared.item(id)
        })
        .collect()
}
