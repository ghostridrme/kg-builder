use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use kg_core::errors::StageError;
use kg_core::models::edges::{EntityEdge, CONFIDENCE_HEURISTIC, CONFIDENCE_LLM_MAX};
use kg_core::models::CollectionMembership;
use kg_core::models::{EntityNode, PropertyValue};
use kg_core::runtime::stage_output::{
    EdgeExtractionOutput, PendingReference, ReferenceCandidate, ReferenceIntent,
    RelationshipTarget, KEY_VALUE_SEPARATOR,
};
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::graph_reads::{WantedKeyValue, MAX_LOOKUP_KEYS};
use kg_core::traits::Stage;

/// Discover references deterministically by typed identity value: every
/// scalar in a source payload is a candidate reference; a candidate target is
/// confirmed only when the value satisfies a **complete** declared key group of
/// exactly one eligible target. Ambiguity is queued for the resolution stage.
pub struct ReferenceExtractionStage;

fn observation_target(node: &EntityNode) -> RelationshipTarget {
    RelationshipTarget::from_node(node)
}

/// The exact typed key-value token for a scalar identity value
/// (`<type_tag>:<canonical>`), matching how targets store `key_values`. `None`
/// for lists, JSON, blobs, nulls and blank strings — they are not identities.
fn value_token(value: &PropertyValue) -> Option<String> {
    let canonical = value.as_identity_key()?;
    let tag = kg_core::traits::property_codec::type_tag(value);
    Some(format!("{tag}{KEY_VALUE_SEPARATOR}{canonical}"))
}

/// The scalar elements of a typed list, each its own reference observation.
/// One typed reference observation from a source payload: the evidence location,
/// its identity token, whether it came from a collection (array/list), and the
/// tokens of its enclosing object that can complete a composite target key.
#[derive(Debug, Clone)]
struct Observation {
    /// Evidence path (`SubnetId`, `SecurityGroups.GroupId`); array indexes are
    /// not part of durable identity, so they never enter the location.
    location: String,
    /// `<type_tag>:<canonical>` of this value.
    token: String,
    /// The canonical identity text, for the edge value and self-identity guard.
    canonical: String,
    /// Field name associated with this value inside its enclosing object.
    component: String,
    /// A collection element gets no single-target cardinality key.
    from_collection: bool,
    /// Tokens available in the enclosing object to complete a composite group.
    context: Arc<Vec<(String, String)>>,
}

fn context_target_types(
    observation: &Observation,
    in_run: &HashMap<String, Vec<&RelationshipTarget>>,
) -> Option<Vec<String>> {
    if observation.location == "_astrolabe_namespace" {
        return Some(vec!["Astrolabe::Namespace".into()]);
    }
    if !observation.location.starts_with("_astrolabe_scope.") {
        return None;
    }
    // Scope values occur on every resource. Restrict their lookup to in-run
    // entities whose whole, single-field primary or alternative key is that
    // value; composite resource keys are not context nodes.
    let mut types = BTreeSet::new();
    for target in in_run.get(&observation.token).into_iter().flatten() {
        if target.key_groups.iter().any(|group| {
            group.components.len() == 1
                && group.components[0].property == observation.component
                && group.components[0].token() == observation.token
        }) {
            types.insert(target.entity_type.clone());
        }
    }
    (!types.is_empty()).then(|| types.into_iter().collect())
}

#[cfg(test)]
const MAX_TRAVERSAL_VALUES: usize = 128;
#[derive(Clone, Copy)]
struct ScanLimits {
    values: usize,
    depth: usize,
}
#[cfg(test)]
fn source_observations(source: &EntityNode, excluded: &[&str]) -> (Vec<Observation>, bool) {
    source_observations_with_limits(
        source,
        excluded,
        ScanLimits {
            values: 128,
            depth: 5,
        },
    )
}
/// Longest typed token recorded as a durable unresolved decision; matches the
/// `UnresolvedReferenceEntry` validator so a large free-text value (never a real
/// identity) is skipped rather than rejected at commit.
const MAX_UNRESOLVED_TOKEN_LEN: usize = 4096;

fn observation_time(source: &EntityNode) -> chrono::DateTime<Utc> {
    source.last_seen_at.unwrap_or(source.valid_from)
}

type DecisionKey = (Uuid, String, chrono::DateTime<Utc>, Uuid);

fn decision_key(source: &EntityNode, slot: String) -> DecisionKey {
    (
        source.chain_id,
        slot,
        observation_time(source),
        source.last_seen_snapshot_id.unwrap_or(source.chain_id),
    )
}

pub(crate) fn model_resolvable_reason(reason: &str) -> bool {
    matches!(
        reason,
        "multiple-candidates" | "insufficient-reference-evidence"
    )
}

fn record_entry(
    entries: &mut Vec<kg_core::traits::UnresolvedReferenceEntry>,
    incoming: kg_core::traits::UnresolvedReferenceEntry,
) {
    if let Some(existing) = entries
        .iter_mut()
        .find(|entry| entry.token == incoming.token)
    {
        // Same-token array elements can have different context. A missing key or
        // truncated lookup cannot be cleared by confirming another element.
        // Absence is weaker than uncertainty. Only all-not-found occurrences
        // can prove an old slot absent; a partial key must survive confirmation.
        let strength = |reason: &str| {
            if reason == "target-not-found" {
                0
            } else if model_resolvable_reason(reason) {
                1
            } else {
                2
            }
        };
        if strength(&incoming.reason) > strength(&existing.reason) {
            *existing = incoming;
        }
    } else {
        entries.push(incoming);
    }
}

fn record_unresolved_token(
    slots: &mut BTreeMap<DecisionKey, Vec<kg_core::traits::UnresolvedReferenceEntry>>,
    source: &EntityNode,
    snapshot_id: Option<Uuid>,
    observation: &Observation,
    reason: &str,
) {
    if observation.token.len() > MAX_UNRESOLVED_TOKEN_LEN {
        return;
    }
    let entries = slots
        .entry(decision_key(
            source,
            reference_slot(&source.entity_type, &observation.location),
        ))
        .or_default();
    record_entry(
        entries,
        kg_core::traits::UnresolvedReferenceEntry {
            token: observation.token.clone(),
            reason: reason.into(),
            snapshot_id: snapshot_id.filter(|id| !id.is_nil()),
            recorded_at: observation_time(source),
        },
    );
}

fn candidate_revision_scopes(wanted: &[WantedKeyValue]) -> Vec<kg_core::traits::IdentityScope> {
    let mut scopes = BTreeSet::new();
    for request in wanted {
        let namespaces = request
            .namespaces
            .clone()
            .unwrap_or_else(|| vec!["*".into()]);
        let entity_types = request
            .target_types
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec!["*".into()]);
        for namespace in &namespaces {
            for entity_type in &entity_types {
                scopes.insert(kg_core::traits::IdentityScope {
                    namespace: namespace.clone(),
                    entity_type: entity_type.clone(),
                });
            }
        }
    }
    scopes.into_iter().collect()
}

async fn read_identity_revisions(
    ctx: &RuntimeContext,
    scopes: &[kg_core::traits::IdentityScope],
    stage: &str,
) -> Result<Vec<kg_core::traits::IdentityRevision>, StageError> {
    let mut revisions = Vec::with_capacity(scopes.len());
    for chunk in scopes.chunks(MAX_LOOKUP_KEYS) {
        let response = ctx
            .graph
            .identity_revisions(ctx.org_id.as_ref(), chunk)
            .await
            .map_err(|error| StageError::StepFailed {
                stage: stage.into(),
                step: "reference_identity_revisions".into(),
                cause: "reference identity revision lookup failed".into(),
                retriable: error.is_transient(),
            })?;
        let returned: BTreeSet<_> = response.iter().map(|value| value.scope.clone()).collect();
        if response.len() != chunk.len()
            || returned.len() != chunk.len()
            || chunk.iter().any(|scope| !returned.contains(scope))
        {
            return Err(StageError::StateValidation {
                stage: stage.into(),
                message: "reference identity revision response is incomplete".into(),
            });
        }
        revisions.extend(response);
    }
    revisions.sort_by(|left, right| left.scope.cmp(&right.scope));
    Ok(revisions)
}

/// Turn one source payload into typed reference observations. Every scalar
/// is a candidate; there is no field-name allowlist and no lowercasing. Reports
/// whether traversal hit its value or depth bound (`traversal-truncated`).
fn source_observations_with_limits(
    source: &EntityNode,
    excluded: &[&str],
    limits: ScanLimits,
) -> (Vec<Observation>, bool) {
    let mut out = Vec::new();
    let mut truncated = false;
    // A composite key completes only from siblings in the same object. `all_properties` is flattened to dotted
    // keys, so an object's scope is the set of scalars sharing its parent prefix
    // (`""` is the root object). Grouping by parent prevents a nested field (a
    // tag, say) from borrowing top-level identity components it does not own.
    fn scope_of(key: &str) -> &str {
        key.rsplit_once('.').map(|(parent, _)| parent).unwrap_or("")
    }
    // Scope coordinates are appended as dedicated observations after bounded
    // payload traversal; they must not consume the payload's value budget.
    let is_excluded =
        |key: &str| key.starts_with("_astrolabe_scope.") || path_is_excluded(key, excluded);
    fn component_of(key: &str) -> &str {
        key.rsplit_once('.').map_or(key, |(_, leaf)| leaf)
    }
    let mut scope_tokens: HashMap<&str, Vec<(String, String)>> = HashMap::new();
    for (key, value) in &source.all_properties {
        if is_excluded(key) || matches!(value, PropertyValue::Json(_)) {
            continue;
        }
        if let Some(token) = value_token(value) {
            scope_tokens
                .entry(scope_of(key))
                .or_default()
                .push((component_of(key).to_owned(), token));
        }
    }
    let scope_tokens: HashMap<_, Arc<Vec<(String, String)>>> = scope_tokens
        .into_iter()
        .map(|(scope, tokens)| (scope, Arc::new(tokens)))
        .collect();
    // Pass 1: every non-excluded scalar within the depth bound is an
    // identity-bearing observation, carrying its own object's context. A scalar
    // nested past the depth bound is not observed and the source is reported
    // incomplete (`traversal-truncated`), matching nested-JSON traversal — the
    // dotted depth equals the object nesting `flatten_source` collapsed.
    for (key, value) in &source.all_properties {
        if is_excluded(key) || matches!(value, PropertyValue::Json(_)) {
            continue;
        }
        if key.matches('.').count() > limits.depth {
            truncated = true;
            continue;
        }
        if let Some(token) = value_token(value) {
            if out.len() == limits.values {
                truncated = true;
                break;
            }
            out.push(Observation {
                location: key.clone(),
                canonical: value.as_identity_key().unwrap_or_default(),
                token,
                component: component_of(key).to_owned(),
                from_collection: false,
                context: scope_tokens.get(scope_of(key)).cloned().unwrap_or_default(),
            });
        }
    }
    // Pass 2: descend into lists and JSON under the value/depth bound. Scalars are
    // already captured; a bound hit here reports `traversal-truncated`.
    for (key, value) in &source.all_properties {
        if is_excluded(key) {
            continue;
        }
        match value {
            PropertyValue::Json(raw) => {
                if out.len() >= limits.values {
                    truncated = true;
                    break;
                }
                let Ok(json) = serde_json::from_str::<serde_json::Value>(raw) else {
                    continue;
                };
                if !json.is_array() && !json.is_object() {
                    continue;
                }
                collect_json(
                    &json,
                    key,
                    0,
                    limits,
                    &scope_tokens.get(scope_of(key)).cloned().unwrap_or_default(),
                    excluded,
                    &mut out,
                    &mut truncated,
                );
            }
            PropertyValue::StringList(items) => {
                for (index, item) in items.iter().enumerate() {
                    if out.len() >= limits.values {
                        truncated = true;
                        break;
                    }
                    let element = PropertyValue::String(item.clone());
                    if let Some(token) = value_token(&element) {
                        out.push(Observation {
                            location: format!("{key}[{index}]"),
                            canonical: element.as_identity_key().unwrap_or_default(),
                            token,
                            component: component_of(key).to_owned(),
                            from_collection: true,
                            context: scope_tokens.get(scope_of(key)).cloned().unwrap_or_default(),
                        });
                    }
                }
            }
            PropertyValue::IntegerList(items) => {
                for (index, item) in items.iter().enumerate() {
                    if out.len() >= limits.values {
                        truncated = true;
                        break;
                    }
                    let element = PropertyValue::Integer(*item);
                    if let Some(token) = value_token(&element) {
                        out.push(Observation {
                            location: format!("{key}[{index}]"),
                            canonical: element.as_identity_key().unwrap_or_default(),
                            token,
                            component: component_of(key).to_owned(),
                            from_collection: true,
                            context: scope_tokens.get(scope_of(key)).cloned().unwrap_or_default(),
                        });
                    }
                }
            }
            PropertyValue::FloatList(items) => {
                for (index, item) in items.iter().enumerate() {
                    if out.len() >= limits.values {
                        truncated = true;
                        break;
                    }
                    let element = PropertyValue::Float(*item);
                    if let Some(token) = value_token(&element) {
                        out.push(Observation {
                            location: format!("{key}[{index}]"),
                            canonical: element.as_identity_key().unwrap_or_default(),
                            token,
                            component: component_of(key).to_owned(),
                            from_collection: true,
                            context: scope_tokens.get(scope_of(key)).cloned().unwrap_or_default(),
                        });
                    }
                }
            }
            _ => {}
        }
    }
    (out, truncated)
}

/// Generic bounded JSON traversal. Each leaf scalar becomes a collection
/// observation whose context is the tokens of its nearest enclosing object, so a
/// composite key can only be completed from within one object (never across
/// array elements.
#[allow(clippy::too_many_arguments)]
fn collect_json(
    value: &serde_json::Value,
    path: &str,
    depth: usize,
    limits: ScanLimits,
    context: &Arc<Vec<(String, String)>>,
    excluded: &[&str],
    out: &mut Vec<Observation>,
    truncated: &mut bool,
) {
    if path_is_excluded(path, excluded) {
        return;
    }
    if depth > limits.depth || out.len() >= limits.values {
        *truncated = true;
        return;
    }
    match value {
        serde_json::Value::Object(map) => {
            let child_context = Arc::new(
                map.iter()
                    .filter(|(field, _)| !path_is_excluded(&format!("{path}.{field}"), excluded))
                    .filter_map(|(field, value)| {
                        value_token(&PropertyValue::from_source(value))
                            .map(|token| (field.clone(), token))
                    })
                    .collect(),
            );
            for (field, item) in map {
                if out.len() >= limits.values {
                    *truncated = true;
                    break;
                }
                collect_json(
                    item,
                    &format!("{path}.{field}"),
                    depth + 1,
                    limits,
                    &child_context,
                    excluded,
                    out,
                    truncated,
                );
            }
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                if out.len() >= limits.values {
                    *truncated = true;
                    break;
                }
                collect_json(
                    item,
                    &format!("{path}[{index}]"),
                    depth + 1,
                    limits,
                    context,
                    excluded,
                    out,
                    truncated,
                );
            }
        }
        scalar => {
            let value = PropertyValue::from_source(scalar);
            if let Some(token) = value_token(&value) {
                out.push(Observation {
                    location: path.to_owned(),
                    canonical: value.as_identity_key().unwrap_or_default(),
                    token,
                    component: path
                        .rsplit_once('.')
                        .map_or(path, |(_, leaf)| leaf)
                        .split('[')
                        .next()
                        .unwrap_or(path)
                        .to_owned(),
                    from_collection: true,
                    context: context.clone(),
                });
            }
        }
    }
}

fn path_is_excluded(path: &str, excluded: &[&str]) -> bool {
    let path = path_without_indexes(path);
    excluded.iter().any(|candidate| {
        let candidate = path_without_indexes(candidate);
        path == candidate || path.starts_with(&format!("{candidate}."))
    })
}

/// A stored reference target (a prior run's live version) with the collection
/// memberships the retirement fence needs.
struct StoredTarget {
    target: RelationshipTarget,
    collections: Vec<CollectionMembership>,
}

impl StoredTarget {
    /// A member of the observing scan's collection older than the scan's
    /// generation was not re-observed and will be tombstoned by reconciliation
    /// unless another collection still owns it; linking it would resurrect the
    /// fact. A membership already at this generation survives.
    fn swept_by(&self, scan: Option<&CollectionMembership>) -> bool {
        let Some(scan) = scan else {
            return false;
        };
        let stale_member = self
            .collections
            .iter()
            .any(|m| m.collection == scan.collection && m.generation < scan.generation);
        let shared = self
            .collections
            .iter()
            .any(|m| m.collection != scan.collection);
        stale_member && !shared
    }
}

/// One eligible complete-group match of a source observation onto a target.
struct Match {
    target: RelationshipTarget,
    /// Complete target key group satisfied by the observation.
    matched_key_group: Vec<String>,
    /// The observation matched only the display name, which is not a declared
    /// identity group. It may be reported as a candidate but cannot be offered
    /// to the model as an eligible endpoint.
    display_only: bool,
    /// Payload structure independently indicates that this identity value is a
    /// relationship. A complete identity without this evidence may be confirmed
    /// by the model; a display-only match may not.
    structural: bool,
}

/// Find a complete primary/additional group from one reference context. A
/// declared `name` key follows the same exact-value rules as every other key.
fn complete_group_match(
    source: &EntityNode,
    target: &RelationshipTarget,
    observation: &Observation,
) -> Option<Vec<String>> {
    key_match::complete_group(target, observation)
        .or_else(|| inherited_scope_group_match(source, target, observation))
}

mod key_match;

fn inherited_scope_group_match(
    source: &EntityNode,
    target: &RelationshipTarget,
    observation: &Observation,
) -> Option<Vec<String>> {
    // TODO(reference-scope): Define justified scope completion for bare references
    // such as EC2 Ebs.VolumeId when the target key also requires account/region.
    // The provider-prefix fallback below is not proof of shared ownership: AWS
    // cross-account references can carry their own OwnerId/Region (VPC peering).
    // Match explicit target context despite differing field names; never treat
    // source ownership or a unique partial-key hit as proof of a complete key.
    let explicit_scope_conflicts = |component: &str, expected: &str| {
        let aliases: &[&str] = match component {
            "_astrolabe_scope.account_id" => &["account_id", "AccountId", "OwnerId"],
            "_astrolabe_scope.region" => &["region", "Region"],
            _ => &[],
        };
        observation
            .context
            .iter()
            .any(|(property, token)| aliases.contains(&property.as_str()) && token != expected)
    };
    for group in &target.key_groups {
        if !group
            .components
            .iter()
            .any(|c| c.property.starts_with("_astrolabe_scope."))
        {
            continue;
        }
        let Some(anchor) = group.components.iter().find(|component| {
            component.property == observation.component && component.token() == observation.token
        }) else {
            continue;
        };
        let complete = group.components.iter().all(|component| {
            let component_token = component.token();
            (component.property == anchor.property && component_token == observation.token)
                || observation.context.iter().any(|(property, token)| {
                    property == &component.property && token == &component_token
                })
                // The connector's trusted scan scope applies to nested
                // resource references too. Only reserved scope components may
                // come from outside the enclosing AWS response object.
                || (component.property.starts_with("_astrolabe_scope.")
                    && source.entity_type.split("::").next()
                        == target.entity_type.split("::").next()
                    && !explicit_scope_conflicts(&component.property, &component_token)
                    && source
                        .all_properties
                        .get(&component.property)
                        .and_then(value_token)
                        .as_deref()
                        == Some(component_token.as_str()))
        });
        if complete {
            return Some(
                group
                    .components
                    .iter()
                    .map(|c| c.property.clone())
                    .collect(),
            );
        }
    }
    None
}

#[async_trait]
impl Stage for ReferenceExtractionStage {
    fn capabilities(&self) -> &'static [kg_core::traits::StageCapability] {
        &[kg_core::traits::StageCapability::ReferenceExtraction]
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::EdgeExtraction, StageKind::EdgeExtraction)]
    }

    fn processing_version(&self) -> String {
        "15-slot-local-reference-coverage".into()
    }

    fn name(&self) -> &str {
        "reference_extraction"
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let existing = match input {
            StageOutput::EdgeExtraction(existing) => existing,
            _ => {
                return Err(StageError::StateValidation {
                    stage: self.name().into(),
                    message: "expected declared relationship output".into(),
                });
            }
        };
        if !existing.pending_references.is_empty() {
            return Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "reference extraction already has pending decisions".into(),
            });
        }
        let resolution = existing.resolution.clone();

        // Sources: this snapshot's live entities. Targets: the whole run when the
        // runner attached it, else the same set.
        let own_entities = existing.resolved_nodes.clone();
        let observations = super::observation_evidence::current_observations(
            &resolution,
            &ctx.org_id,
            self.name(),
        )?;
        let source_snapshots: Vec<_> = observations
            .iter()
            .map(|observation| Some(observation.snapshot_uuid))
            .collect();
        if !resolution.fk_exclusions.is_empty()
            && source_snapshots
                .iter()
                .flatten()
                .any(|snapshot| !resolution.fk_exclusions.contains_key(snapshot))
        {
            return Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "missing source observation for reference exclusions".into(),
            });
        }
        let sources: Vec<_> = observations
            .into_iter()
            .map(|observation| observation.node)
            .collect();
        let fallback;
        let targets: &[RelationshipTarget] = match &resolution.chunk_entities {
            Some(targets) => targets,
            None => {
                fallback = own_entities
                    .iter()
                    .map(observation_target)
                    .collect::<Vec<_>>();
                &fallback
            }
        };

        // In-run index: every target keyed by each of its typed key tokens, plus
        // its display name as a string key (a complete single-field `name` group).
        let mut in_run: HashMap<String, Vec<&RelationshipTarget>> = HashMap::new();
        for target in targets {
            if let Some(token) = value_token(&PropertyValue::String(target.name.clone())) {
                in_run.entry(token).or_default().push(target);
            }
            for token in target.key_value_tokens() {
                in_run.entry(token).or_default().push(target);
            }
        }

        // Gather every source observation once, then look up stored candidates in
        // one bounded batched read per key page.
        let per_source: Vec<(Vec<Observation>, bool)> = sources
            .iter()
            .zip(&source_snapshots)
            .map(|(source, snapshot)| {
                let excluded =
                    excluded_properties(ctx, source, *snapshot, &resolution.fk_exclusions);
                let (mut observations, truncated) = source_observations_with_limits(
                    source,
                    &excluded,
                    ScanLimits {
                        values: ctx.extraction_settings.reference_max_values,
                        depth: ctx.extraction_settings.reference_max_depth,
                    },
                );
                if source.labels.iter().any(|label| label == "astrolabe:scope") {
                    observations.clear();
                } else {
                    // Scan context identifies the resource even when its AWS
                    // response exceeds the bounded reference traversal. Give
                    // these two context links their own observations so they
                    // cannot disappear behind an unrelated payload field.
                    for field in ["account_id", "region"] {
                        let path = format!("_astrolabe_scope.{field}");
                        if let Some(value) = source.all_properties.get(&path) {
                            if let Some(token) = value_token(value) {
                                observations.push(Observation {
                                    location: path,
                                    token,
                                    canonical: value.as_identity_key().unwrap_or_default(),
                                    component: field.into(),
                                    from_collection: false,
                                    context: Arc::new(Vec::new()),
                                });
                            }
                        }
                    }
                }
                // Namespace is ingestion context, not an AWS response field.
                if source.entity_type != "Astrolabe::Namespace"
                    && targets.iter().any(|target| {
                        target.entity_type == "Astrolabe::Namespace"
                            && target.name == source.namespace
                            && target.namespace == source.namespace
                    })
                {
                    let value = PropertyValue::String(source.namespace.clone());
                    if let Some(token) = value_token(&value) {
                        observations.push(Observation {
                            location: "_astrolabe_namespace".into(),
                            token,
                            canonical: source.namespace.clone(),
                            component: "name".into(),
                            from_collection: false,
                            context: Arc::new(Vec::new()),
                        });
                    }
                }
                (observations, truncated)
            })
            .collect();
        let mut wanted_by_key: BTreeMap<String, WantedKeyValue> = BTreeMap::new();
        for ((source, snapshot), (observations, _)) in
            sources.iter().zip(&source_snapshots).zip(&per_source)
        {
            let allowed = ctx.namespace_policy.allowed_targets(&source.namespace);
            let mappings = ctx
                .extraction_settings
                .for_source(&source.source)
                .reference_guidance
                .get(&source.source)
                .cloned()
                .unwrap_or_default();
            // Fetch the generic candidate universe even when guidance might
            // handle this path. Applicability depends on runtime context; a
            // skipped mapping must never turn a missing read into uniqueness.
            for observation in observations {
                let mut request = WantedKeyValue::exact(observation.token.clone(), allowed.clone());
                if let Some(target_types) = context_target_types(observation, &in_run) {
                    request.target_types = Some(target_types);
                }
                wanted_by_key.insert(serde_json::to_string(&request).unwrap_or_default(), request);
            }
            let excluded = excluded_properties(ctx, source, *snapshot, &resolution.fk_exclusions);
            for request in guided::lookup_requests(
                source,
                &mappings,
                allowed.clone(),
                &excluded,
                ctx.extraction_settings.reference_max_values,
            ) {
                wanted_by_key.insert(serde_json::to_string(&request).unwrap_or_default(), request);
            }
        }
        let wanted: Vec<WantedKeyValue> = wanted_by_key.into_values().collect();
        let revision_scopes = candidate_revision_scopes(&wanted);
        let revisions_before = read_identity_revisions(ctx, &revision_scopes, self.name()).await?;
        let (stored, lookup_truncated) = stored_candidates(ctx, &wanted).await?;

        // Local evidence does not prove the same value has no stored competitors.
        let scan = resolution
            .snapshot_nodes
            .first()
            .and_then(CollectionMembership::of_node);
        // A pair already joined by a declared (or child) edge is not re-derived as
        // a reference, in either orientation: a declared incoming child edge and a
        // reference would otherwise both connect the same two entities.
        let declared_pairs: HashSet<_> = existing
            .edges
            .iter()
            .flat_map(|edge| {
                [
                    (edge.source_chain_id, edge.target_chain_id),
                    (edge.target_chain_id, edge.source_chain_id),
                ]
            })
            .collect();

        let mut edges = Vec::new();
        let mut pending_references = Vec::new();
        let mut attempted = 0usize;
        let mut complete_key_candidates = 0usize;
        let mut partial_key_candidates = 0usize;
        let mut excluded_count = 0usize;
        let mut unresolved = 0usize;
        let mut incomplete_sources: Vec<Uuid> = Vec::new();
        // R4: durable unresolved decisions, one entry per (source chain, slot) the
        // run could not confirm to a target (`target-not-found`/`partial-key`), so
        // a target appearing later finds the sources waiting on its typed token.
        let mut guided_slots = std::collections::HashSet::new();
        let mut unresolved_by_slot: BTreeMap<
            DecisionKey,
            Vec<kg_core::traits::UnresolvedReferenceEntry>,
        > = BTreeMap::new();
        // Clear-on-confirm: (source chain, slot) pairs that produced a confirmed
        // edge this run. Captured at the confirmation site — never derived from an
        // edge's `source_chain_id`, which an inverse guided direction points at the
        // target. Any such slot with no remaining unresolved entries emits an
        // empty-entry decision, retracting a prior run's durable record.
        let mut confirmed_slots: BTreeSet<DecisionKey> = BTreeSet::new();
        let mark_incomplete = |source: &EntityNode, incomplete: &mut Vec<Uuid>| {
            if !incomplete.contains(&source.chain_id) {
                incomplete.push(source.chain_id);
            }
        };
        // Applicable guidance frozen per producer source (never a provider branch):
        // `for_source` merges global and source-scoped mappings once per source.
        let mut guidance_cache: HashMap<
            String,
            Arc<Vec<kg_core::runtime::extraction::ReferenceMapping>>,
        > = HashMap::new();

        for ((source, snapshot_id), (source_observations, traversal_truncated)) in
            sources.iter().zip(&source_snapshots).zip(&per_source)
        {
            excluded_count +=
                excluded_properties(ctx, source, *snapshot_id, &resolution.fk_exclusions)
                    .iter()
                    .filter(|key| source.all_properties.contains_key(**key))
                    .count();
            if *traversal_truncated {
                mark_incomplete(source, &mut incomplete_sources);
            }
            // Guided pass first: it owns its reference paths, so the generic pass
            // below skips any observation under one and a reference is never
            // discovered twice with two names.
            let source_mappings = guidance_cache
                .entry(source.source.clone())
                .or_insert_with(|| {
                    Arc::new(
                        ctx.extraction_settings
                            .for_source(&source.source)
                            .reference_guidance
                            .get(&source.source)
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .clone();
            let mut handled_prefixes: Vec<String> = Vec::new();
            if !source_mappings.is_empty() {
                let excluded =
                    excluded_properties(ctx, source, *snapshot_id, &resolution.fk_exclusions);
                let guided = guided::source_matches(
                    ctx,
                    source,
                    *snapshot_id,
                    &source_mappings,
                    targets,
                    &stored,
                    scan.as_ref(),
                    &declared_pairs,
                    &lookup_truncated,
                    &excluded,
                )?;
                if guided.incomplete {
                    mark_incomplete(source, &mut incomplete_sources);
                }
                attempted += guided.attempted;
                unresolved += guided.unresolved_count;
                edges.extend(guided.edges);
                pending_references.extend(guided.pending);
                for (slot, entry) in guided.unresolved_entries {
                    guided_slots.insert((source.chain_id, slot.clone()));
                    let entries = unresolved_by_slot
                        .entry(decision_key(source, slot))
                        .or_default();
                    record_entry(entries, entry);
                }
                for slot in guided.confirmed_slots {
                    confirmed_slots.insert(decision_key(source, slot));
                }
                handled_prefixes = guided.handled_prefixes;
            }
            for observation in source_observations {
                // A guided reference path owns its evidence; do not re-derive it.
                if handled_prefixes.iter().any(|prefix| {
                    let location = path_without_indexes(&observation.location);
                    location == *prefix || location.starts_with(&format!("{prefix}."))
                }) {
                    continue;
                }
                attempted += 1;
                // A truncated lookup means the candidate universe is incomplete;
                // A capped page does not prove that the candidate is unique.
                let mut request = WantedKeyValue::exact(
                    observation.token.clone(),
                    ctx.namespace_policy.allowed_targets(&source.namespace),
                );
                let context_types = context_target_types(observation, &in_run);
                if let Some(target_types) = &context_types {
                    request.target_types = Some(target_types.clone());
                }
                if lookup_truncated.contains(&request.request_id()) {
                    // Candidate uncertainty belongs to this slot/token. Its durable
                    // decision protects that reference during retirement; unrelated
                    // authoritative empty paths must still retire their old facts.
                    record_unresolved_token(
                        &mut unresolved_by_slot,
                        source,
                        *snapshot_id,
                        observation,
                        "lookup-truncated",
                    );
                    unresolved += 1;
                    continue;
                }
                let mut complete: BTreeMap<Uuid, Match> = BTreeMap::new();
                let mut component_only = false;
                let mut correspondence_ambiguous = false;
                // In-run candidates win over stored for the same chain.
                for target in in_run.get(&observation.token).into_iter().flatten() {
                    if context_types
                        .as_ref()
                        .is_some_and(|types| !types.contains(&target.entity_type))
                    {
                        continue;
                    }
                    if target.chain_id == source.chain_id
                        || !ctx
                            .namespace_policy
                            .allows(&source.namespace, &target.namespace)
                    {
                        continue;
                    }
                    match matched_group(source, target, observation) {
                        Some((group, display_only)) => {
                            let structural = observation.location == "_astrolabe_namespace"
                                || has_structural_reference(
                                    &observation.location,
                                    &target.entity_type,
                                    &group,
                                );
                            complete.entry(target.chain_id).or_insert_with(|| Match {
                                target: (*target).clone(),
                                matched_key_group: group,
                                display_only,
                                structural,
                            });
                        }
                        None => {
                            component_only = true;
                            correspondence_ambiguous |=
                                key_match::ambiguous_correspondence(target, observation);
                        }
                    }
                }
                for stored in stored.get(&observation.token).into_iter().flatten() {
                    let target = &stored.target;
                    if context_types
                        .as_ref()
                        .is_some_and(|types| !types.contains(&target.entity_type))
                    {
                        continue;
                    }
                    if target.chain_id == source.chain_id
                        || complete.contains_key(&target.chain_id)
                        || !ctx
                            .namespace_policy
                            .allows(&source.namespace, &target.namespace)
                        || stored.swept_by(scan.as_ref())
                    {
                        continue;
                    }
                    match matched_group(source, target, observation) {
                        Some((group, display_only)) => {
                            let structural = observation.location == "_astrolabe_namespace"
                                || has_structural_reference(
                                    &observation.location,
                                    &target.entity_type,
                                    &group,
                                );
                            complete.entry(target.chain_id).or_insert_with(|| Match {
                                target: target.clone(),
                                matched_key_group: group,
                                display_only,
                                structural,
                            });
                        }
                        None => {
                            component_only = true;
                            correspondence_ambiguous |=
                                key_match::ambiguous_correspondence(target, observation);
                        }
                    }
                }

                let had_declared_match = complete
                    .keys()
                    .any(|chain| declared_pairs.contains(&(source.chain_id, *chain)));
                let matches: Vec<Match> = complete
                    .into_values()
                    .filter(|m| !declared_pairs.contains(&(source.chain_id, m.target.chain_id)))
                    .collect();
                complete_key_candidates += matches.iter().filter(|m| !m.display_only).count();
                partial_key_candidates += usize::from(component_only);
                if matches.is_empty() && had_declared_match {
                    // The only complete match is already joined by a declared
                    // edge, so the reference is satisfied. Mark the slot confirmed
                    // (no edge, no durable unresolved entry) rather than recording
                    // a `target-not-found` that inflates the count and leaves a
                    // waiting record that never clears.
                    confirmed_slots.insert(decision_key(
                        source,
                        reference_slot(&source.entity_type, &observation.location),
                    ));
                    continue;
                }
                if matches.is_empty() {
                    // A component-only match cannot prove a composite identity.
                    // Both cases are durable unresolved decisions: a partial
                    // key match waits as `partial-key`, no candidate at all as
                    // `target-not-found`. Neither authorizes retirement.
                    unresolved += 1;
                    let reason = if correspondence_ambiguous {
                        "ambiguous-correspondence"
                    } else if component_only {
                        "partial-key"
                    } else {
                        "target-not-found"
                    };
                    // A value too long to be a stored key token can never match a
                    // future target, so recording it would only fail the entry
                    // validator; skip it but keep the counters.
                    if observation.token.len() <= MAX_UNRESOLVED_TOKEN_LEN {
                        let slot = reference_slot(&source.entity_type, &observation.location);
                        let entries = unresolved_by_slot
                            .entry(decision_key(source, slot))
                            .or_default();
                        record_entry(
                            entries,
                            kg_core::traits::UnresolvedReferenceEntry {
                                token: observation.token.clone(),
                                reason: reason.into(),
                                // Optional provenance: a nil snapshot is dropped
                                // (the entry validator rejects a nil id).
                                snapshot_id: (*snapshot_id).filter(|id| !id.is_nil()),
                                recorded_at: observation_time(source),
                            },
                        );
                    }
                    continue;
                }

                let eligible: Vec<&Match> = matches
                    .iter()
                    .filter(|candidate| !candidate.display_only)
                    .collect();
                let candidates: Vec<ReferenceCandidate> = eligible
                    .iter()
                    .map(|m| ReferenceCandidate {
                        target: m.target.clone(),
                        matched_key_group: m.matched_key_group.clone(),
                    })
                    .collect();
                // Deterministic confirmation requires exactly one candidate that
                // satisfies a complete DECLARED key group. Multiple candidates, or a
                // value equal to the source's own identity, are genuine ambiguity for
                // the resolution stage. A lone match that
                // is only the target's display name (name not a declared key) is a
                // candidate at most: a deterministic `display-name-only`
                // decision: never an edge and not worth a model call (display
                // name not declared as an identity → candidate only; unresolved").
                let components: Vec<_> = observation
                    .context
                    .iter()
                    .cloned()
                    .chain(std::iter::once((
                        observation.component.clone(),
                        observation.token.clone(),
                    )))
                    .collect();
                let base_intent = ReferenceIntent {
                    observing_chain_id: source.chain_id,
                    observing_namespace: source.namespace.clone(),
                    observing_entity_type: source.entity_type.clone(),
                    producer_source: source.source.clone(),
                    location: observation.location.clone(),
                    slot: reference_slot(&source.entity_type, &observation.location),
                    relationship_name: "RELATES_TO".into(),
                    direction: kg_core::runtime::extraction::ReferenceDirection::SourceToTarget,
                    cardinality: if observation.from_collection {
                        kg_core::runtime::extraction::ReferenceCardinality::Many
                    } else {
                        kg_core::runtime::extraction::ReferenceCardinality::One
                    },
                    target_key_group: eligible
                        .first()
                        .filter(|first| {
                            eligible.iter().all(|candidate| {
                                candidate.matched_key_group == first.matched_key_group
                            })
                        })
                        .map(|candidate| candidate.matched_key_group.clone())
                        .unwrap_or_default(),
                    target_type: eligible
                        .first()
                        .filter(|first| {
                            eligible.iter().all(|candidate| {
                                candidate.target.entity_type == first.target.entity_type
                            })
                        })
                        .map(|candidate| candidate.target.entity_type.clone())
                        .unwrap_or_default(),
                    components: components.clone(),
                    allowed_namespaces: ctx.namespace_policy.allowed_targets(&source.namespace),
                    lookup_complete: true,
                    policy_fingerprint: "generic-reference-v1".into(),
                };
                if eligible.len() > 1 {
                    unresolved += 1;
                    record_unresolved_token(
                        &mut unresolved_by_slot,
                        source,
                        *snapshot_id,
                        observation,
                        "multiple-candidates",
                    );
                    queue_reference(
                        ctx,
                        source,
                        base_intent,
                        &observation.canonical,
                        components,
                        candidates,
                        &mut pending_references,
                    );
                } else if eligible.is_empty() {
                    unresolved += 1;
                    if observation.token.len() <= MAX_UNRESOLVED_TOKEN_LEN {
                        let slot = reference_slot(&source.entity_type, &observation.location);
                        let entries = unresolved_by_slot
                            .entry(decision_key(source, slot))
                            .or_default();
                        record_entry(
                            entries,
                            kg_core::traits::UnresolvedReferenceEntry {
                                token: observation.token.clone(),
                                reason: "display-name-only".into(),
                                snapshot_id: (*snapshot_id).filter(|id| !id.is_nil()),
                                recorded_at: observation_time(source),
                            },
                        );
                    }
                } else if !eligible[0].structural {
                    unresolved += 1;
                    record_unresolved_token(
                        &mut unresolved_by_slot,
                        source,
                        *snapshot_id,
                        observation,
                        "insufficient-reference-evidence",
                    );
                    queue_reference(
                        ctx,
                        source,
                        base_intent,
                        &observation.canonical,
                        components,
                        candidates,
                        &mut pending_references,
                    );
                } else {
                    let selected = eligible[0];
                    let edge = build_reference_edge(
                        source,
                        &selected.target,
                        &selected.target.entity_type,
                        &observation.canonical,
                        &base_intent,
                        false,
                    )?;
                    let mut edge = edge;
                    if let Some(evidence) = edge.reference_evidence.as_mut() {
                        evidence.read_set = read_versions(&candidates);
                    }
                    confirmed_slots.insert(decision_key(
                        source,
                        reference_slot(&source.entity_type, &observation.location),
                    ));
                    edges.push(edge);
                }
            }
        }

        // A confirmed slot with no remaining unresolved entries clears its durable
        // record: `or_default` inserts an empty entry set, and leaves a slot that
        // still has unresolved entries this run untouched (a keyed array where some
        // elements confirmed and others did not stays recorded for the rest).
        for key in confirmed_slots {
            unresolved_by_slot.entry(key).or_default();
        }
        // Keep every capture for temporal planning. Only the newest decision per
        // slot is durable; an earlier observation must not lend its clock to later
        // ambiguity or overwrite a later clear.
        let retirement_decisions: Vec<_> = unresolved_by_slot
            .into_iter()
            .map(|((chain, slot, at, snapshot), entries)| {
                kg_core::runtime::stage_output::UnresolvedSlot {
                    source_chain_id: chain,
                    slot,
                    decided_at: at,
                    decision_id: snapshot,
                    entries,
                }
            })
            .collect();
        let mut durable = BTreeMap::new();
        for decision in &retirement_decisions {
            let mut decision = decision.clone();
            let key = (decision.source_chain_id, decision.slot.clone());
            if !guided_slots.contains(&key)
                && sources
                    .iter()
                    .find(|source| source.chain_id == decision.source_chain_id)
                    .is_some_and(|source| {
                        !ctx.policy
                            .for_source(&source.source)
                            .record_generic_unresolved
                    })
            {
                decision.entries.clear();
            }
            // DecisionKey iteration orders the capture and tie-breaker, so the
            // last insert is exactly the existing slot freshness contract.
            durable.insert(key, decision);
        }
        let unresolved_slots: Vec<_> = durable.into_values().collect();
        let candidate_revisions =
            read_identity_revisions(ctx, &revision_scopes, self.name()).await?;
        if revisions_before != candidate_revisions {
            return Err(StageError::IdentityRevisionChanged);
        }
        let source_coverage = sources
            .iter()
            .zip(&source_snapshots)
            .map(
                |(source, snapshot_id)| kg_core::runtime::stage_output::ReferenceSourceCoverage {
                    chain_id: source.chain_id,
                    namespace: source.namespace.clone(),
                    captured_at: observation_time(source),
                    complete: resolution
                        .snapshot_nodes
                        .iter()
                        .any(|snapshot| Some(snapshot.uuid) == *snapshot_id && snapshot.complete),
                    paths: source
                        .all_properties
                        .keys()
                        .map(|path| path_without_indexes(path))
                        .collect(),
                    excluded_paths: excluded_properties(
                        ctx,
                        source,
                        *snapshot_id,
                        &resolution.fk_exclusions,
                    )
                    .into_iter()
                    .map(path_without_indexes)
                    .collect(),
                },
            )
            .collect();
        let reference_report = kg_core::runtime::stage_output::ReferenceReport {
            source_coverage,
            retirement_decisions,
            source_histories: Vec::new(),
            source_reads: resolution.reference_source_reads.as_ref().clone(),
            relationship_declines: existing.reference_report.relationship_declines.clone(),
            attempted,
            confirmed: edges.len(),
            unresolved,
            excluded: excluded_count,
            incomplete_sources,
            candidate_revisions,
            unresolved_slots,
            decisions: Vec::new(),
            decision_contexts: Vec::new(),
        };
        use kg_core::telemetry::{reference_activity, reference_uncertainty, ReferenceActivity};
        reference_activity(
            if resolution.reference_source_reads.is_empty() {
                ReferenceActivity::ForwardSource
            } else {
                ReferenceActivity::ReverseSource
            },
            sources.len(),
        );
        reference_activity(
            ReferenceActivity::CompleteKeyCandidate,
            complete_key_candidates,
        );
        reference_activity(
            ReferenceActivity::PartialKeyCandidate,
            partial_key_candidates,
        );
        let mut reasons = std::collections::BTreeMap::<&str, usize>::new();
        for slot in &reference_report.unresolved_slots {
            for entry in &slot.entries {
                *reasons.entry(&entry.reason).or_default() += 1;
            }
        }
        for (reason, count) in reasons {
            reference_uncertainty(reason, count);
        }
        // These counters describe only this stage's reference decisions.
        // correlated with the stage span. Extraction's contribution is its
        // deterministic confirms, the slots it left unresolved, exclusions, and
        // the sources it truncated; resolution later reports its own confirms.
        kg_core::telemetry::reference_decisions(
            "reference_extraction",
            reference_report.confirmed,
            reference_report.unresolved,
            reference_report.excluded,
            reference_report.incomplete_sources.len(),
        );
        // A refresh rebuilds this stage's own reference edges from fresh evidence
        // (only build_fk_edge attaches reference evidence); declared, child and
        // model relationships arriving from earlier stages are kept as they are.
        edges.extend(
            existing
                .edges
                .iter()
                .filter(|edge| edge.reference_evidence.is_none())
                .cloned(),
        );
        Ok(StageOutput::EdgeExtraction(EdgeExtractionOutput {
            relationship_times: existing.relationship_times.clone(),
            relationship_directives: existing.relationship_directives.clone(),
            reference_report,
            pending_references: Arc::new(pending_references),
            snapshot_nodes: resolution.snapshot_nodes.clone(),
            resolved_nodes: own_entities,
            resolution,
            edges: Arc::new(edges),
        }))
    }
}

/// Generic extraction confirms a declared key only when the payload structure
/// names that exact key or encloses it under the target type. Similar suffixes
/// and arbitrary fields remain candidates for guidance or model confirmation.
fn has_structural_reference(location: &str, target_type: &str, key_group: &[String]) -> bool {
    let canonical = |value: &str| {
        value
            .chars()
            .filter(|character| character.is_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect::<String>()
    };
    let target = canonical(target_type);
    // Resource types may be qualified (AWS::IAM::Role, vendor.Service).
    // Qualification scopes the type; it is not part of a payload field's name.
    let local_target = canonical(target_type.rsplit([':', '.']).next().unwrap_or(target_type));
    let names_target =
        |value: &str| !value.is_empty() && (value == target || value == local_target);
    // Id and Identifier are spelling variants only at a word boundary. This
    // compares field/key names, never identifier values or partial key groups.
    let canonical_key = |value: &str| {
        if let Some(prefix) = value
            .strip_suffix("Identifier")
            .or_else(|| value.strip_suffix("_identifier"))
            .or_else(|| value.strip_suffix("-identifier"))
        {
            format!("{}id", canonical(prefix))
        } else if value.eq_ignore_ascii_case("identifier") {
            "id".to_owned()
        } else {
            canonical(value)
        }
    };
    let segments: Vec<_> = location
        .split('.')
        .map(|segment| segment.split('[').next().unwrap_or(segment))
        .collect();
    let enclosing_type = segments
        .iter()
        .take(segments.len().saturating_sub(1))
        .any(|segment| names_target(&canonical(segment)));
    let Some(segment) = segments.last() else {
        return false;
    };
    let field = canonical(segment);
    let singular = field.strip_suffix('s').unwrap_or(&field);
    enclosing_type
        || names_target(&field)
        || names_target(singular)
        || key_group.iter().any(|key| {
            let key = canonical_key(key.rsplit('.').next().unwrap_or(key));
            if key.is_empty() {
                return false;
            }
            if canonical_key(segment) == key
                || canonical_key(segment.strip_suffix('s').unwrap_or(segment)) == key
            {
                return true;
            }
            let original = segment.strip_suffix('s').unwrap_or(segment);
            // The suffix gets the same Identifier→Id normalization as the key, so
            // `ReadReplicaSourceDBInstanceIdentifier` names `DBInstanceIdentifier`.
            let Some((offset, _)) = original.char_indices().find(|(offset, character)| {
                character.is_alphanumeric() && canonical_key(&original[*offset..]) == key
            }) else {
                return false;
            };
            let prefix = &original[..offset];
            let boundary = prefix.chars().last().is_some_and(|before| {
                !before.is_alphanumeric()
                    || (before.is_lowercase()
                        && original[offset..]
                            .chars()
                            .next()
                            .is_some_and(char::is_uppercase))
            });
            boundary
                && (!matches!(key.as_str(), "id" | "name" | "arn" | "uid")
                    || names_target(&canonical(prefix)))
        })
}

/// The target key property an observation satisfies as a **complete** group, or
/// `None` when it only matches one component of a composite (partial key). A
/// display-name match satisfies the `name` group.
/// A match of a source observation onto a target, and whether it is only a
/// display-name (soft) match. A complete declared key group is a hard match; a
/// bare display name that is not a declared key is soft (a candidate only).
fn matched_group(
    source: &EntityNode,
    target: &RelationshipTarget,
    observation: &Observation,
) -> Option<(Vec<String>, bool)> {
    if let Some(group) = complete_group_match(source, target, observation) {
        return Some((group, false));
    }
    // A complete single-field identity can enter model confirmation even when
    // the source field has a different name. Value equality discovers the
    // candidate; without structural field association it is never deterministic.
    if let Some(group) = target.key_groups.iter().find(|group| {
        group.components.len() == 1 && group.components[0].token() == observation.token
    }) {
        return Some((
            group
                .components
                .iter()
                .map(|c| c.property.clone())
                .collect(),
            false,
        ));
    }
    // The display name discovers a candidate even when `name` is not a declared
    // key, but it never confirms deterministically; mark it soft so a
    // single such match still routes to the resolution stage / unresolved.
    if value_token(&PropertyValue::String(target.name.clone())).as_deref()
        == Some(&observation.token)
    {
        return Some((vec!["name".into()], true));
    }
    None
}

fn queue_reference(
    ctx: &RuntimeContext,
    source: &EntityNode,
    intent: ReferenceIntent,
    value: &str,
    components: Vec<(String, String)>,
    candidates: Vec<ReferenceCandidate>,
    pending: &mut Vec<PendingReference>,
) {
    if ctx.policy.for_source(&source.source).edge_ambiguity
        == kg_core::policy::EdgeAmbiguityMode::Llm
    {
        pending.push(PendingReference {
            source: source.clone(),
            intent,
            value: value.into(),
            components,
            candidates,
        });
    } else {
        tracing::debug!(
            candidates = candidates.len(),
            "ambiguous reference skipped by policy"
        );
    }
}

/// Exclude configured fields. Source identity fields remain eligible because a
/// join entity can legitimately point to the entities named by its key parts.
fn excluded_properties<'a>(
    ctx: &'a RuntimeContext,
    source: &'a EntityNode,
    snapshot_id: Option<Uuid>,
    exclusions: &'a HashMap<Uuid, Vec<String>>,
) -> Vec<&'a str> {
    let mut excluded: Vec<&str> = ctx
        .entity_type_configs
        .get(&source.entity_type)
        .map(|c| c.drop_properties.iter().map(String::as_str).collect())
        .unwrap_or_default();
    if let Some(properties) = snapshot_id.and_then(|id| exclusions.get(&id)) {
        excluded.extend(properties.iter().map(String::as_str));
    }
    excluded
}

/// The read set behind a confirmation: every eligible candidate's exact version.
pub(crate) fn read_versions(
    candidates: &[ReferenceCandidate],
) -> Vec<kg_core::models::edges::ReadVersion> {
    candidates
        .iter()
        .map(|candidate| kg_core::models::edges::ReadVersion {
            chain_id: candidate.target.chain_id,
            version_uuid: candidate.target.version_uuid,
            version: candidate.target.version,
            observed_at: None,
        })
        .collect()
}

/// A reference path with its array indexes removed, so every element of one list
/// shares one stable location (`network_interfaces[0].subnetwork` →
/// `network_interfaces.subnetwork`). Array indexes locate evidence, not durable
/// relationship identity.
pub(crate) fn path_without_indexes(path: &str) -> String {
    let Ok(parsed) = kg_core::runtime::extraction::ReferencePath::parse(path) else {
        return path.to_owned();
    };
    parsed
        .segments
        .iter()
        .map(|segment| {
            segment
                .key
                .chars()
                .fold(String::new(), |mut out, character| {
                    if matches!(character, '.' | '\\' | '[' | ']') {
                        out.push('\\');
                    }
                    out.push(character);
                    out
                })
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// The stable owner slot of a reference path: entity type plus the index-stripped
/// path, so every element of one list shares one slot.
fn reference_slot(entity_type: &str, path: &str) -> String {
    format!("{entity_type}.{}", path_without_indexes(path))
}

/// Recover a demonstrable mapping from the exact frozen reference context.
pub(super) fn reference_correspondence(
    target: &RelationshipTarget,
    intent: &ReferenceIntent,
) -> Option<BTreeMap<String, String>> {
    if intent.policy_fingerprint != "generic-reference-v1" {
        use kg_core::runtime::extraction::{ReferenceMapping, ReferenceShape};
        let mapping: ReferenceMapping = serde_json::from_str(&intent.policy_fingerprint).ok()?;
        mapping.validate().ok()?;
        return mapping
            .target_key_group
            .iter()
            .map(|component| {
                let path = if let Some(path) = mapping.context_paths.get(component) {
                    guided::bind_occurrence_path(path, &intent.location)?
                } else if mapping.shape == ReferenceShape::Object
                    || (mapping.shape == ReferenceShape::Scalar
                        && mapping.target_key_group.len() > 1)
                {
                    format!("{}.{}", intent.location, component)
                } else {
                    intent.location.clone()
                };
                Some((component.clone(), path))
            })
            .collect();
    }
    let component = intent.location.rsplit('.').next()?.split('[').next()?;
    let token = intent
        .components
        .iter()
        .find(|(field, _)| field == component)?
        .1
        .clone();
    let observation = Observation {
        location: intent.location.clone(),
        component: component.to_owned(),
        token,
        canonical: String::new(),
        from_collection: false,
        context: Arc::new(intent.components.clone()),
    };
    key_match::complete_mapping(target, &observation)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_reference_edge(
    source: &EntityNode,
    target: &RelationshipTarget,
    target_entity_type: &str,
    value_str: &str,
    intent: &ReferenceIntent,
    llm_resolved: bool,
) -> Result<EntityEdge, StageError> {
    let cardinality_key = match intent.cardinality {
        kg_core::runtime::extraction::ReferenceCardinality::One => Some(
            kg_core::identity::relationship_cardinality_key(
                &source.org_id,
                source.chain_id,
                &intent.relationship_name,
                Some(&intent.slot),
                &[],
            )
            .map_err(|message| StageError::StateValidation {
                stage: "reference_extraction".into(),
                message,
            })?,
        ),
        kg_core::runtime::extraction::ReferenceCardinality::Many => None,
    };
    let (source_chain_id, target_chain_id) = match intent.direction {
        kg_core::runtime::extraction::ReferenceDirection::SourceToTarget => {
            (source.chain_id, target.chain_id)
        }
        kg_core::runtime::extraction::ReferenceDirection::Inverse => {
            (target.chain_id, source.chain_id)
        }
    };
    Ok(EntityEdge {
        time_evidence: None,
        chain_id: Uuid::new_v4(),
        identity_hash: None,
        cardinality_key,
        producer_source: source.source.clone(),
        origin: kg_core::models::edges::RelationshipOrigin::Reference,
        uuid: Uuid::new_v4(),
        org_id: source.org_id.clone(),
        source_chain_id,
        target_chain_id,
        name: intent.relationship_name.clone(),
        identity_name: None,
        // The fact names both endpoints by type and name, and the source's labels
        // (scope words such as an account or region a connector attaches), so its
        // embedding and its display carry meaning; the matched value stays for
        // the record. Sibling facts in other scopes otherwise embed within a few
        // hundredths of each other (measured 2026-09-27).
        description: format!(
            "{} {}{} references {} {} via property '{}' ({})",
            source.entity_type,
            source.name,
            if source.labels.is_empty() {
                String::new()
            } else {
                format!(" [labels: {}]", source.labels.join(", "))
            },
            target_entity_type,
            target.name,
            intent.slot,
            value_str
        ),
        all_properties: indexmap::IndexMap::new(),
        discovered_by: Some(if llm_resolved {
            "llm_fk_disambiguation".into()
        } else {
            "heuristic_fk".into()
        }),
        resolved_by: None,
        source_property: Some(intent.location.clone()),
        target_identity_field: Some(intent.target_key_group.join("+")),
        reference_evidence: Some(kg_core::models::edges::ReferenceEvidence {
            component_paths: reference_correspondence(target, intent),
            observing_chain_id: source.chain_id,
            observing_namespace: source.namespace.clone(),
            slot: intent.slot.clone(),
            location: intent.location.clone(),
            target_key_group: intent.target_key_group.clone(),
            reference_tokens: intent
                .components
                .iter()
                .map(|(_, token)| token.clone())
                .collect(),
            read_set: Vec::new(),
            decision: None,
        }),
        // LLM-resolved edges are capped BELOW heuristics.
        confidence: if llm_resolved {
            CONFIDENCE_LLM_MAX
        } else {
            CONFIDENCE_HEURISTIC
        },
        justification: Some(format!(
            "Property '{}' value '{value_str}' matches {target_entity_type} key group [{}]",
            intent.slot,
            intent.target_key_group.join(", ")
        )),
        first_seen_snapshot_id: source.last_seen_snapshot_id,
        last_seen_snapshot_id: source.last_seen_snapshot_id,
        last_seen_at: source.last_seen_at,
        sync_generation: source.sync_generation,
        valid_from: observation_time(source),
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        valid_to: None,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        created_at: Utc::now(),
    })
}

#[cfg(test)]
pub(crate) fn build_fk_edge(
    source: &EntityNode,
    target_chain_id: Uuid,
    target_entity_type: &str,
    value: &str,
    location: &str,
    matched_field: &str,
    llm_resolved: bool,
) -> Result<EntityEdge, StageError> {
    let target = RelationshipTarget {
        chain_id: target_chain_id,
        version_uuid: Uuid::new_v4(),
        version: 1,
        name: value.to_owned(),
        entity_type: target_entity_type.to_owned(),
        namespace: source.namespace.clone(),
        key_groups: Vec::new(),
    };
    let intent = ReferenceIntent {
        observing_chain_id: source.chain_id,
        observing_namespace: source.namespace.clone(),
        observing_entity_type: source.entity_type.clone(),
        producer_source: source.source.clone(),
        location: location.to_owned(),
        slot: reference_slot(&source.entity_type, location),
        relationship_name: format!("REFERENCES_{}", target_entity_type.to_uppercase()),
        direction: kg_core::runtime::extraction::ReferenceDirection::SourceToTarget,
        cardinality: if matches!(
            source.all_properties.get(location),
            Some(PropertyValue::String(_))
        ) {
            kg_core::runtime::extraction::ReferenceCardinality::One
        } else {
            kg_core::runtime::extraction::ReferenceCardinality::Many
        },
        target_key_group: vec![matched_field.to_owned()],
        target_type: target_entity_type.to_owned(),
        components: vec![(matched_field.to_owned(), format!("s:{value}"))],
        allowed_namespaces: Some(vec![source.namespace.clone()]),
        lookup_complete: true,
        policy_fingerprint: "test-reference".into(),
    };
    build_reference_edge(
        source,
        &target,
        target_entity_type,
        value,
        &intent,
        llm_resolved,
    )
}

/// Resolve wanted typed key tokens against the persisted graph via the bounded
/// reference-candidate read (`MAX_CARRIERS_PER_IDENTIFYING_VALUE` + 1 per value).
/// Returns `token -> stored targets` and the set of tokens whose candidate
/// enumeration was truncated (never treated as unique). A read failure fails the
/// stage: a silently empty index would drop relationships.
async fn stored_candidates(
    ctx: &RuntimeContext,
    wanted: &[WantedKeyValue],
) -> Result<(HashMap<String, Vec<StoredTarget>>, HashSet<String>), StageError> {
    let mut index: HashMap<String, Vec<StoredTarget>> = HashMap::new();
    let mut truncated: HashSet<String> = HashSet::new();
    if wanted.is_empty() {
        return Ok((index, truncated));
    }
    for chunk in wanted.chunks(MAX_LOOKUP_KEYS) {
        let wanted = chunk.to_vec();
        let candidates = ctx
            .graph
            .find_reference_candidates(ctx.org_id.as_ref(), wanted)
            .await
            .map_err(|e| StageError::StepFailed {
                stage: "reference_extraction".into(),
                step: "reference_candidates".into(),
                cause: e.to_string(),
                retriable: e.is_transient(),
            })?;
        truncated.extend(candidates.truncated_requests);
        for record in candidates.records {
            if record.name.is_empty() || record.entity_type.is_empty() {
                continue;
            }
            let collections = record.collections.clone();
            let target = RelationshipTarget::from(record);
            for request in chunk
                .iter()
                .filter(|request| request.matches_target(&target))
            {
                let entries = index.entry(request.token.clone()).or_default();
                if entries.iter().any(|entry| {
                    entry.target.chain_id == target.chain_id
                        && entry.target.version_uuid == target.version_uuid
                }) {
                    continue;
                }
                entries.push(StoredTarget {
                    target: target.clone(),
                    collections: collections.clone(),
                });
            }
        }
    }
    Ok((index, truncated))
}

mod guided;

#[cfg(test)]
mod tests;
