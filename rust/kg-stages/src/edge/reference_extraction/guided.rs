//! Provider-neutral guided reference matching.
//!
//! Guidance is caller configuration, never a provider branch: a [`ReferenceMapping`]
//! names a source entity type, a reference path, a target type and one **complete**
//! target key group, and may declare composite completion from other source paths
//! (`context_paths`), per-type case folding (`case_insensitive_types`), an explicit
//! semantic `relationship_name`, `direction` and `cardinality`. The engine applies
//! these generically; any provider-specific *shape* (e.g. a packed relative path)
//! is the connector's job to normalize into clean typed fields before ingestion.
//!
//! A composite is still only confirmed when every declared component is present and
//! key-equal to one eligible target. Missing components or no eligible
//! target become durable unresolved decisions; more than one eligible target is
//! ambiguous and routes to the model resolution stage — guidance never invents a
//! component or bypasses identity, scope or liveness checks.

use std::collections::BTreeMap;

use uuid::Uuid;

use kg_core::models::edges::EntityEdge;
use kg_core::models::{EntityNode, PropertyValue};
use kg_core::runtime::extraction::ReferenceShape;
use kg_core::runtime::extraction::{ReferenceMapping, ReferencePath};
use kg_core::runtime::stage_output::{
    PendingReference, ReferenceCandidate, ReferenceIntent, RelationshipTarget,
};
use kg_core::runtime::RuntimeContext;
use kg_core::traits::UnresolvedReferenceEntry;

use super::{read_versions, reference_slot, Match, StoredTarget};

/// What one source's applicable guidance produced.
#[derive(Default)]
pub(super) struct GuidedOutcome {
    pub edges: Vec<EntityEdge>,
    pub pending: Vec<PendingReference>,
    /// Durable unresolved decisions as `(slot, entry)` for guided references the run
    /// could not confirm (incomplete composite or no eligible target).
    pub unresolved_entries: Vec<(String, UnresolvedReferenceEntry)>,
    /// Slots this pass confirmed to a single target, so the caller can clear any
    /// prior durable unresolved record for them (clear-on-confirm).
    pub confirmed_slots: Vec<String>,
    /// Index-stripped reference paths this pass owns; the generic pass skips any
    /// observation located under them so one reference is never discovered twice.
    pub handled_prefixes: Vec<String>,
    pub attempted: usize,
    pub unresolved_count: usize,
    pub incomplete: bool,
}

/// A desired target component: its property, stored type tag and canonical text.
#[derive(Clone)]
struct DesiredComponent {
    property: String,
    type_tag: String,
    value: String,
}

fn mapping_prefix(mapping: &ReferenceMapping) -> String {
    ReferencePath::parse(&mapping.reference_path)
        .map(|path| {
            path.segments
                .into_iter()
                .map(|segment| segment.key)
                .collect::<Vec<_>>()
                .join(".")
        })
        .unwrap_or_default()
}

pub(super) fn lookup_requests(
    source: &EntityNode,
    mappings: &[ReferenceMapping],
    namespaces: Option<Vec<String>>,
    excluded: &[&str],
    max_values: usize,
) -> Vec<kg_core::traits::graph_reads::WantedKeyValue> {
    let mut requests = Vec::new();
    for mapping in mappings.iter().filter(|mapping| {
        mapping.source_entity_type == source.entity_type
            && mapping
                .source_namespace
                .as_ref()
                .is_none_or(|namespace| namespace == &source.namespace)
    }) {
        let prefix = mapping_prefix(mapping);
        if mapping_is_excluded(mapping, excluded) {
            continue;
        }
        let fold_strings = mapping
            .case_insensitive_types
            .contains(&mapping.target_type);
        for (_, components) in desired_occurrences(source, mapping, &prefix, max_values)
            .into_iter()
            .take(max_values)
        {
            for component in components {
                let text = fold(&component.type_tag, &component.value, fold_strings);
                requests.push(kg_core::traits::graph_reads::WantedKeyValue {
                    token: format!("{}:{text}", component.type_tag),
                    namespaces: namespaces.clone(),
                    target_types: Some(vec![mapping.target_type.clone()]),
                    case_insensitive_string: fold_strings && component.type_tag == "s",
                });
            }
        }
    }
    requests
}

fn mapping_is_excluded(mapping: &ReferenceMapping, excluded: &[&str]) -> bool {
    std::iter::once(mapping.reference_path.as_str())
        .chain(mapping.context_paths.values().map(String::as_str))
        .any(|path| super::path_is_excluded(path, excluded))
}

/// Case-fold a component's canonical text only for a string key of a type the
/// provider contract declared case-insensitive; never global lowercasing.
fn fold(type_tag: &str, value: &str, fold: bool) -> String {
    if fold && type_tag == "s" {
        value.to_lowercase()
    } else {
        value.to_owned()
    }
}

/// Resolve a guidance path against a source entity's flattened properties. `raw:`
/// binds the entity's own scope fields (namespace/name/entity_type) or original
/// properties; the default form binds the flattened dotted key. Array indexes are
/// not used to complete a composite — a context component must be a scalar.
fn resolve_source_path(source: &EntityNode, path: &str) -> Option<PropertyValue> {
    let parsed = ReferencePath::parse(path).ok()?;
    let joined = parsed
        .segments
        .iter()
        .map(|segment| segment.key.as_str())
        .collect::<Vec<_>>()
        .join(".");
    if parsed.raw {
        match joined.as_str() {
            "namespace" => return Some(PropertyValue::String(source.namespace.clone())),
            "name" => return Some(PropertyValue::String(source.name.clone())),
            "entity_type" => return Some(PropertyValue::String(source.entity_type.clone())),
            _ => {}
        }
    }
    if parsed
        .segments
        .iter()
        .any(|segment| segment.index.is_some())
    {
        let first = parsed.segments.first()?;
        let mut current = property_json(source.all_properties.get(&first.key)?)?;
        current = select_index(current, first.index)?;
        for segment in parsed.segments.iter().skip(1) {
            current = current.as_object()?.get(&segment.key)?.clone();
            current = select_index(current, segment.index)?;
        }
        return Some(PropertyValue::from_source(&current));
    }
    let mut value = source.all_properties.get(&joined).cloned()?;
    if let Some(index) = parsed.segments.last().and_then(|segment| segment.index) {
        let values: Vec<PropertyValue> = match value {
            PropertyValue::StringList(values) => {
                values.into_iter().map(PropertyValue::String).collect()
            }
            PropertyValue::IntegerList(values) => {
                values.into_iter().map(PropertyValue::Integer).collect()
            }
            PropertyValue::FloatList(values) => {
                values.into_iter().map(PropertyValue::Float).collect()
            }
            PropertyValue::Json(raw) => serde_json::from_str::<serde_json::Value>(&raw)
                .ok()?
                .as_array()?
                .iter()
                .map(PropertyValue::from_source)
                .collect(),
            _ => return None,
        };
        value = match index {
            Some(index) => values.get(index)?.clone(),
            None if values.len() == 1 => values.into_iter().next()?,
            None => return None,
        };
    }
    Some(value)
}

fn property_json(value: &PropertyValue) -> Option<serde_json::Value> {
    match value {
        PropertyValue::Json(raw) => serde_json::from_str(raw).ok(),
        PropertyValue::StringList(values) => Some(serde_json::json!(values)),
        PropertyValue::IntegerList(values) => Some(serde_json::json!(values)),
        PropertyValue::FloatList(values) => Some(serde_json::json!(values)),
        _ => None,
    }
}

fn select_index(
    value: serde_json::Value,
    index: Option<Option<usize>>,
) -> Option<serde_json::Value> {
    match index {
        None => Some(value),
        Some(Some(index)) => value.as_array()?.get(index).cloned(),
        Some(None) => {
            let mut values = value.as_array()?.iter();
            let only = values.next()?.clone();
            values.next().is_none().then_some(only)
        }
    }
}

/// The desired component values for a mapping's target key group, drawn from the
/// reference object's fields (`<path>.<component>`), overridden by `context_paths`,
/// and — for a single-component group — the reference path's own scalar value.
/// `None` when any component is missing: an incomplete composite is never a match.
fn desired_components(
    source: &EntityNode,
    mapping: &ReferenceMapping,
    reference_prefix: &str,
) -> Option<Vec<DesiredComponent>> {
    let mut out = Vec::with_capacity(mapping.target_key_group.len());
    for component in &mapping.target_key_group {
        let value = if let Some(path) = mapping.context_paths.get(component) {
            resolve_source_path(source, path)
        } else if let Some(value) = source
            .all_properties
            .get(&format!("{reference_prefix}.{component}"))
        {
            Some(value.clone())
        } else if mapping.target_key_group.len() == 1 {
            // A single-field group may be filled by the reference path's own scalar.
            resolve_source_path(source, &mapping.reference_path)
        } else {
            None
        };
        let value = value?;
        let type_tag = kg_core::traits::property_codec::type_tag(&value).to_owned();
        let text = value.as_identity_key()?;
        out.push(DesiredComponent {
            property: component.clone(),
            type_tag,
            value: text,
        });
    }
    Some(out)
}

fn list_values(value: &PropertyValue, max_values: usize) -> Option<Vec<PropertyValue>> {
    match value {
        PropertyValue::StringList(values) => Some(
            values
                .iter()
                .take(max_values + 1)
                .cloned()
                .map(PropertyValue::String)
                .collect(),
        ),
        PropertyValue::IntegerList(values) => Some(
            values
                .iter()
                .take(max_values + 1)
                .copied()
                .map(PropertyValue::Integer)
                .collect(),
        ),
        PropertyValue::FloatList(values) => Some(
            values
                .iter()
                .take(max_values + 1)
                .copied()
                .map(PropertyValue::Float)
                .collect(),
        ),
        PropertyValue::Json(raw) => Some(
            serde_json::from_str::<serde_json::Value>(raw)
                .ok()?
                .as_array()?
                .iter()
                .take(max_values + 1)
                .map(PropertyValue::from_source)
                .collect(),
        ),
        _ => None,
    }
}

/// Select one bounded array, optionally projecting a scalar from each object.
/// The array's parent may already have been flattened into a dotted property.
/// Multiple array expansions cannot be combined into one reference occurrence.
fn list_reference_values(
    source: &EntityNode,
    mapping: &ReferenceMapping,
    max_values: usize,
) -> Option<(Vec<(String, PropertyValue)>, bool)> {
    let parsed = ReferencePath::parse(&mapping.reference_path).ok()?;
    let indexed: Vec<_> = parsed
        .segments
        .iter()
        .enumerate()
        .filter(|(_, segment)| segment.index.is_some())
        .collect();
    if indexed.len() > 1 {
        return None;
    }
    let array_index = indexed
        .first()
        .map(|(index, _)| *index)
        .unwrap_or(parsed.segments.len().checked_sub(1)?);
    let array_path = parsed.segments[..=array_index]
        .iter()
        .map(|segment| segment.key.as_str())
        .collect::<Vec<_>>()
        .join(".");
    let selected_index = parsed.segments[array_index].index.flatten();
    let values = list_values(source.all_properties.get(&array_path)?, max_values)?;
    let truncated = values.len() > max_values;
    let mut out = Vec::new();
    for (index, value) in values.into_iter().enumerate() {
        if selected_index.is_some_and(|selected| selected != index) {
            continue;
        }
        let suffix = &parsed.segments[array_index + 1..];
        if suffix.is_empty() {
            out.push((format!("{array_path}[{index}]"), value));
            continue;
        }
        let Some(mut object) = property_json(&value) else {
            continue;
        };
        let mut location = format!("{array_path}[{index}]");
        let mut valid = true;
        for segment in suffix {
            let Some(child) = object
                .as_object()
                .and_then(|object| object.get(&segment.key))
            else {
                valid = false;
                break;
            };
            object = child.clone();
            location.push('.');
            location.push_str(&segment.key);
        }
        if valid {
            out.push((location, PropertyValue::from_source(&object)));
        }
    }
    Some((out, truncated))
}

/// Bind wildcard context only to the same array occurrence as the reference.
/// A different array prefix cannot borrow its index by position.
pub(super) fn bind_occurrence_path(path: &str, location: &str) -> Option<String> {
    let parsed = ReferencePath::parse(path).ok()?;
    let occurrence = ReferencePath::parse(location).ok()?;
    let mut result = String::new();
    for (i, segment) in parsed.segments.iter().enumerate() {
        if i > 0 {
            result.push('.');
        }
        for c in segment.key.chars() {
            if matches!(c, '.' | '[' | ']' | '\\') {
                result.push('\\');
            }
            result.push(c);
        }
        if let Some(index) = segment.index {
            let index = match index {
                Some(index) => index,
                None => {
                    if !parsed.segments[..=i]
                        .iter()
                        .zip(&occurrence.segments)
                        .all(|(a, b)| a.key == b.key)
                    {
                        return None;
                    }
                    occurrence.segments.get(i)?.index??
                }
            };
            result.push_str(&format!("[{index}]"));
        }
    }
    if parsed.raw {
        result.insert_str(0, "raw:");
    }
    Some(result)
}

/// Produce independent reference objects. List elements never share components,
/// and an incompatible declared shape produces no occurrence.
fn desired_occurrences(
    source: &EntityNode,
    mapping: &ReferenceMapping,
    prefix: &str,
    max_values: usize,
) -> Vec<(String, Vec<DesiredComponent>)> {
    match mapping.shape {
        ReferenceShape::Scalar => {
            let Some(components) = desired_components(source, mapping, prefix) else {
                return Vec::new();
            };
            if mapping.target_key_group.len() == 1
                && resolve_source_path(source, &mapping.reference_path)
                    .and_then(|value| value.as_identity_key())
                    .is_none()
            {
                return Vec::new();
            }
            vec![(prefix.to_owned(), components)]
        }
        ReferenceShape::Object => {
            let parsed = ReferencePath::parse(&mapping.reference_path).ok();
            let selected_index = parsed
                .as_ref()
                .and_then(|path| path.segments.last())
                .and_then(|segment| segment.index)
                .flatten();
            let json_objects: Vec<(String, serde_json::Map<String, serde_json::Value>)> = source
                .all_properties
                .get(prefix)
                .and_then(|value| match value {
                    PropertyValue::Json(raw) => serde_json::from_str(raw).ok(),
                    _ => None,
                })
                .map(|value: serde_json::Value| match value {
                    serde_json::Value::Object(object) => vec![(prefix.to_owned(), object)],
                    serde_json::Value::Array(items) => items
                        .into_iter()
                        .enumerate()
                        .filter(|(index, _)| selected_index.is_none_or(|wanted| wanted == *index))
                        .filter_map(|(index, value)| {
                            value
                                .as_object()
                                .cloned()
                                .map(|object| (format!("{prefix}[{index}]"), object))
                        })
                        .take(max_values + 1)
                        .collect(),
                    _ => Vec::new(),
                })
                .unwrap_or_default();
            if !json_objects.is_empty() {
                return json_objects
                    .into_iter()
                    .filter_map(|(location, object)| {
                        let components: Option<Vec<_>> = mapping
                            .target_key_group
                            .iter()
                            .map(|component| {
                                let value = if let Some(path) = mapping.context_paths.get(component)
                                {
                                    resolve_source_path(source, path)?
                                } else {
                                    PropertyValue::from_source(object.get(component)?)
                                };
                                Some(DesiredComponent {
                                    property: component.clone(),
                                    type_tag: kg_core::traits::property_codec::type_tag(&value)
                                        .to_owned(),
                                    value: value.as_identity_key()?,
                                })
                            })
                            .collect();
                        components.map(|components| (location, components))
                    })
                    .collect();
            }
            let flattened = mapping.target_key_group.iter().all(|component| {
                mapping.context_paths.contains_key(component)
                    || source
                        .all_properties
                        .contains_key(&format!("{prefix}.{component}"))
            });
            if flattened {
                desired_components(source, mapping, prefix)
                    .map(|components| vec![(prefix.to_owned(), components)])
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        }
        ReferenceShape::List => {
            let varying: Vec<_> = mapping
                .target_key_group
                .iter()
                .filter(|component| !mapping.context_paths.contains_key(*component))
                .collect();
            if varying.len() != 1 {
                return Vec::new();
            }
            let Some((values, _)) = list_reference_values(source, mapping, max_values) else {
                return Vec::new();
            };
            values
                .into_iter()
                .filter_map(|(location, value)| {
                    let type_tag = kg_core::traits::property_codec::type_tag(&value).to_owned();
                    let value = value.as_identity_key()?;
                    let mut components = Vec::new();
                    for (component, path) in &mapping.context_paths {
                        let path = bind_occurrence_path(path, &location)?;
                        let fixed = resolve_source_path(source, &path)?;
                        components.push(DesiredComponent {
                            property: component.clone(),
                            type_tag: kg_core::traits::property_codec::type_tag(&fixed).to_owned(),
                            value: fixed.as_identity_key()?,
                        });
                    }
                    components.push(DesiredComponent {
                        property: varying[0].clone(),
                        type_tag,
                        value,
                    });
                    Some((location, components))
                })
                .collect()
        }
    }
}

/// Does `target` have a complete declared group equal (under folding) to exactly
/// the mapping's target key group and the desired component values?
fn group_matches(
    target: &RelationshipTarget,
    mapping: &ReferenceMapping,
    desired: &[DesiredComponent],
    fold_strings: bool,
) -> bool {
    let wanted: std::collections::BTreeSet<&str> = mapping
        .target_key_group
        .iter()
        .map(String::as_str)
        .collect();
    target.key_groups.iter().any(|group| {
        let group_props: std::collections::BTreeSet<&str> = group
            .components
            .iter()
            .map(|c| c.property.as_str())
            .collect();
        if group_props != wanted {
            return false;
        }
        desired.iter().all(|want| {
            group.components.iter().any(|have| {
                have.property == want.property
                    && have.type_tag == want.type_tag
                    && fold(&have.type_tag, &have.value, fold_strings)
                        == fold(&want.type_tag, &want.value, fold_strings)
            })
        })
    })
}

/// Apply all guidance mappings that select this source entity type. Confirmed
/// references become guided edges; incomplete/absent ones become durable
/// unresolved decisions; ambiguous ones route to the model resolution stage.
#[allow(clippy::too_many_arguments)]
pub(super) fn source_matches(
    ctx: &RuntimeContext,
    source: &EntityNode,
    snapshot_id: Option<Uuid>,
    mappings: &[ReferenceMapping],
    targets: &[RelationshipTarget],
    stored: &std::collections::HashMap<String, Vec<StoredTarget>>,
    scan: Option<&kg_core::models::CollectionMembership>,
    declared_pairs: &std::collections::HashSet<(Uuid, Uuid)>,
    lookup_truncated: &std::collections::HashSet<String>,
    excluded: &[&str],
) -> Result<GuidedOutcome, super::StageError> {
    let max_values = ctx.extraction_settings.reference_max_values;
    let mut outcome = GuidedOutcome::default();
    for mapping in mappings {
        if mapping.source_entity_type != source.entity_type
            || mapping
                .source_namespace
                .as_ref()
                .is_some_and(|namespace| namespace != &source.namespace)
        {
            continue;
        }
        let prefix = mapping_prefix(mapping);
        if mapping_is_excluded(mapping, excluded) {
            continue;
        }
        // A guided reference must actually be present in the payload.
        let present = (mapping.shape == ReferenceShape::List
            && list_reference_values(source, mapping, max_values).is_some())
            || resolve_source_path(source, &mapping.reference_path).is_some()
            || source.all_properties.contains_key(&prefix)
            || source
                .all_properties
                .keys()
                .any(|key| key.starts_with(&format!("{prefix}.")));
        if !present {
            continue;
        }
        if mapping.shape == ReferenceShape::List
            && list_reference_values(source, mapping, max_values)
                .is_some_and(|(_, truncated)| truncated)
        {
            // Count the original array before projecting/filtering invalid leaves;
            // sparse or malformed elements must not hide traversal truncation.
            outcome.incomplete = true;
        }
        let mut occurrences = desired_occurrences(source, mapping, &prefix, max_values);
        if occurrences.len() > max_values {
            occurrences.truncate(max_values);
            outcome.incomplete = true;
        }
        if occurrences.is_empty() {
            // Incomplete composite: deterministic confirmation cannot proceed.
            outcome.unresolved_count += 1;
            let slot = reference_slot(&source.entity_type, &prefix);
            outcome.incomplete = true;
            let (observations, truncated) = super::source_observations_with_limits(
                source,
                excluded,
                super::ScanLimits {
                    values: max_values,
                    depth: ctx.extraction_settings.reference_max_depth,
                },
            );
            outcome.incomplete |= truncated;
            for observation in observations {
                let path = super::path_without_indexes(&observation.location);
                if (path == prefix || path.starts_with(&format!("{prefix}.")))
                    && observation.token.len() <= super::MAX_UNRESOLVED_TOKEN_LEN
                {
                    outcome.unresolved_entries.push((
                        slot.clone(),
                        UnresolvedReferenceEntry {
                            token: observation.token,
                            reason: "partial-key".into(),
                            snapshot_id: snapshot_id.filter(|id| !id.is_nil()),
                            recorded_at: super::observation_time(source),
                        },
                    ));
                }
            }
            continue;
        }
        outcome.handled_prefixes.push(prefix.clone());
        let fold_strings = mapping
            .case_insensitive_types
            .contains(&mapping.target_type);
        for (location, desired) in occurrences {
            outcome.attempted += 1;
            let slot = reference_slot(&source.entity_type, &location);
            if desired.iter().any(|component| {
                let request = kg_core::traits::graph_reads::WantedKeyValue {
                    token: format!(
                        "{}:{}",
                        component.type_tag,
                        fold(&component.type_tag, &component.value, fold_strings)
                    ),
                    namespaces: ctx.namespace_policy.allowed_targets(&source.namespace),
                    target_types: Some(vec![mapping.target_type.clone()]),
                    case_insensitive_string: fold_strings && component.type_tag == "s",
                };
                lookup_truncated.contains(&request.request_id())
            }) {
                outcome.unresolved_count += 1;
                // The durable slot decision protects only this incomplete lookup.
                record_unresolved(
                    &mut outcome,
                    &slot,
                    &desired,
                    "lookup-truncated",
                    source,
                    snapshot_id,
                );
                continue;
            }

            // Candidate targets of the declared type, in-run first then stored, deduped
            // by chain and filtered by scope, self-reference and declared suppression.
            let mut matches: BTreeMap<Uuid, Match> = BTreeMap::new();
            let eligible = |target: &RelationshipTarget| -> Option<Match> {
                if target.chain_id == source.chain_id
                    || target.entity_type != mapping.target_type
                    || !ctx
                        .namespace_policy
                        .allows(&source.namespace, &target.namespace)
                    || declared_pairs.contains(&(source.chain_id, target.chain_id))
                    || !group_matches(target, mapping, &desired, fold_strings)
                {
                    return None;
                }
                Some(Match {
                    target: target.clone(),
                    matched_key_group: mapping.target_key_group.clone(),
                    // A guided mapping matches a declared complete key group, so it
                    // confirms deterministically like any hard match.
                    display_only: false,
                    structural: true,
                })
            };
            for target in targets {
                if let Some(matched) = eligible(target) {
                    matches.entry(matched.target.chain_id).or_insert(matched);
                }
            }
            // Stored candidates surface by any exact desired token; a folded string key
            // cannot be found by exact token (the documented storage follow-on), but a
            // same-run target is already covered by the in-run scan above.
            for want in &desired {
                let token = format!(
                    "{}:{}",
                    want.type_tag,
                    fold(&want.type_tag, &want.value, fold_strings)
                );
                for stored_target in stored.get(&token).into_iter().flatten() {
                    if stored_target.swept_by(scan)
                        || matches.contains_key(&stored_target.target.chain_id)
                    {
                        continue;
                    }
                    if let Some(matched) = eligible(&stored_target.target) {
                        matches.entry(matched.target.chain_id).or_insert(matched);
                    }
                }
            }

            let matches: Vec<Match> = matches.into_values().collect();
            let value_str = desired
                .iter()
                .map(|c| c.value.as_str())
                .collect::<Vec<_>>()
                .join("+");
            match matches.len() {
                0 => {
                    outcome.unresolved_count += 1;
                    record_unresolved(
                        &mut outcome,
                        &slot,
                        &desired,
                        "target-not-found",
                        source,
                        snapshot_id,
                    );
                }
                1 => {
                    let selected = &matches[0];
                    let candidates = vec![ReferenceCandidate {
                        target: selected.target.clone(),
                        matched_key_group: selected.matched_key_group.clone(),
                    }];
                    let intent = reference_intent(ctx, source, mapping, &location, &desired);
                    let mut edge = super::build_reference_edge(
                        source,
                        &selected.target,
                        &selected.target.entity_type,
                        &value_str,
                        &intent,
                        false,
                    )?;
                    edge.discovered_by = Some("guided_fk".into());
                    if let Some(evidence) = edge.reference_evidence.as_mut() {
                        evidence.read_set = read_versions(&candidates);
                    }
                    outcome.confirmed_slots.push(slot.clone());
                    outcome.edges.push(edge);
                }
                _ => {
                    // Ambiguous under explicit guidance still abstains to the model
                    // stage rather than guessing (subject to policy).
                    outcome.unresolved_count += 1;
                    record_unresolved(
                        &mut outcome,
                        &slot,
                        &desired,
                        "multiple-candidates",
                        source,
                        snapshot_id,
                    );
                    let candidates = matches
                        .iter()
                        .map(|m| ReferenceCandidate {
                            target: m.target.clone(),
                            matched_key_group: m.matched_key_group.clone(),
                        })
                        .collect();
                    let components = desired
                        .iter()
                        .map(|component| {
                            (
                                component.property.clone(),
                                format!("{}:{}", component.type_tag, component.value),
                            )
                        })
                        .collect();
                    super::queue_reference(
                        ctx,
                        source,
                        reference_intent(ctx, source, mapping, &location, &desired),
                        &value_str,
                        components,
                        candidates,
                        &mut outcome.pending,
                    );
                }
            }
        }
    }
    Ok(outcome)
}

fn record_unresolved(
    outcome: &mut GuidedOutcome,
    slot: &str,
    desired: &[DesiredComponent],
    reason: &str,
    source: &EntityNode,
    snapshot_id: Option<Uuid>,
) {
    for component in desired {
        let token = format!("{}:{}", component.type_tag, component.value);
        if token.len() > super::MAX_UNRESOLVED_TOKEN_LEN {
            continue;
        }
        outcome.unresolved_entries.push((
            slot.to_owned(),
            UnresolvedReferenceEntry {
                token,
                reason: reason.into(),
                snapshot_id: snapshot_id.filter(|id| !id.is_nil()),
                recorded_at: super::observation_time(source),
            },
        ));
    }
}

/// Build a confirmed guided edge with the mapping's semantic name, direction and
/// cardinality. An inverse mapping points target→source but keeps the source's
/// owner slot and cardinality; a `many` cardinality carries no single-target
/// key so array/list references never supersede one another.
fn reference_intent(
    ctx: &RuntimeContext,
    source: &EntityNode,
    mapping: &ReferenceMapping,
    location: &str,
    desired: &[DesiredComponent],
) -> ReferenceIntent {
    let slot = reference_slot(&source.entity_type, location);
    ReferenceIntent {
        observing_chain_id: source.chain_id,
        observing_namespace: source.namespace.clone(),
        observing_entity_type: source.entity_type.clone(),
        producer_source: source.source.clone(),
        location: location.to_owned(),
        slot,
        relationship_name: mapping.relationship_name.clone(),
        direction: mapping.direction,
        cardinality: mapping.cardinality,
        target_key_group: mapping.target_key_group.clone(),
        target_type: mapping.target_type.clone(),
        components: desired
            .iter()
            .map(|component| {
                (
                    component.property.clone(),
                    format!("{}:{}", component.type_tag, component.value),
                )
            })
            .collect(),
        allowed_namespaces: ctx.namespace_policy.allowed_targets(&source.namespace),
        lookup_complete: true,
        policy_fingerprint: serde_json::to_string(mapping).unwrap_or_default(),
    }
}

#[cfg(test)]
mod occurrence_tests {
    use super::*;
    #[test]
    fn wildcard_context_stays_in_its_own_array_element() {
        assert_eq!(
            bind_occurrence_path("disks[].OwnerId", "disks[3].VolumeId"),
            Some("disks[3].OwnerId".into())
        );
        assert!(bind_occurrence_path("other[].OwnerId", "disks[3].VolumeId").is_none());
        assert!(bind_occurrence_path("disks[].OwnerId", "disks.VolumeId").is_none());
    }
}
