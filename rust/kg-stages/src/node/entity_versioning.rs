use kg_core::runtime::stage_output::Observed;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use kg_core::embedding::ComputedEmbedding;
use kg_core::enums::versioning::PropertyChange;
use kg_core::enums::{EntityLifecycle, VersioningDecision};
use kg_core::errors::StageError;
use kg_core::identity::IdentityHash;
use kg_core::models::{CollectionMembership, SnapshotKind};
use kg_core::models::{EntityNode, PropertyValue};
use kg_core::runtime::stage_output::{NodeResolutionOutput, ResolvedChange};
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::{EntityVersionRecord, Stage};

/// Apply the existing temporal policy after identity matching has finished.
pub struct EntityVersioningStage;

/// What resolution knows about a version already in the graph.
#[derive(Debug, Clone)]
struct ExistingVersion {
    uuid: Uuid,
    chain_id: Uuid,
    version: u32,
    name: String,
    summary: Option<String>,
    /// Hash excludes properties configured not to create versions.
    structural_hash: u64,
    valid_from: Option<DateTime<Utc>>,
    /// Latest recorded observation; an older capture cannot advance the version.
    last_seen_at: Option<DateTime<Utc>>,
    last_transition_at: Option<DateTime<Utc>>,
    /// When a tombstone was deleted; only a strictly newer observation restores it.
    deleted_at: Option<DateTime<Utc>>,
    properties: indexmap::IndexMap<String, PropertyValue>,
    /// Labels stored on this version; a partial observation adds to them.
    labels: Vec<String>,
    /// The vector stored on this version, when it carries its provenance.
    /// Persistence reuses it while the shared settings and content match.
    embedding: Option<Arc<ComputedEmbedding>>,
    /// Latest producer, for deletion authority.
    source: Option<String>,
    /// Collections the version belongs to; carried to whatever continues it.
    collections: Vec<CollectionMembership>,
}

fn stored_structural_hash(value: Option<&str>) -> Result<u64, StageError> {
    value
        .map(str::parse::<u64>)
        .transpose()
        .map_err(|_| StageError::StateValidation {
            stage: "entity_versioning".into(),
            message: "stored structural hash is invalid".into(),
        })
        .map(|hash| hash.unwrap_or(0))
}

impl ExistingVersion {
    /// A source may delete a version it produced or a collection of its own
    /// still owns; it never deletes another collection's entity.
    fn deletable_by(&self, source: &str) -> bool {
        self.source.as_deref() == Some(source)
            || self
                .collections
                .iter()
                .any(|m| m.collection.source == source)
    }
}

#[async_trait]
impl Stage for EntityVersioningStage {
    fn processing_version(&self) -> String {
        "entity-versioning-batch-conflict-isolation-v6".into()
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::NodeIdentity, StageKind::NodeResolution)]
    }

    fn name(&self) -> &str {
        "entity_versioning"
    }

    fn is_batch(&self) -> bool {
        true
    }

    async fn process_batch(
        &self,
        inputs: Vec<StageOutput>,
        ctx: &RuntimeContext,
    ) -> Result<Vec<Result<StageOutput, StageError>>, StageError> {
        let (mut rejected, new_chains) = partial_batch_conflicts(&inputs)?;
        let mut outputs = Vec::with_capacity(inputs.len());
        for (index, input) in inputs.into_iter().enumerate() {
            if ctx.cancel.is_cancelled() {
                return Err(StageError::Cancelled {
                    stage: self.name().into(),
                });
            }
            let output = if rejected.contains(&index) {
                Err(batch_conflict())
            } else {
                self.process(input, ctx).await
            };
            if output.is_err() {
                rejected.insert(index);
            }
            outputs.push(output);
        }
        // A refused snapshot cannot provide the new identity anchor for a survivor.
        loop {
            let before = rejected.len();
            for members in new_chains.values() {
                if members.iter().any(|index| rejected.contains(index)) {
                    rejected.extend(members);
                }
            }
            if rejected.len() == before {
                break;
            }
        }
        for index in rejected {
            if outputs[index].is_ok() {
                outputs[index] = Err(batch_conflict());
            }
        }
        if !ctx.exec_config.continue_on_step_error {
            if let Some(error) = outputs.iter().find_map(|output| output.as_ref().err()) {
                return Err(error.clone());
            }
        }
        Ok(outputs)
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let identity = match input {
            StageOutput::NodeIdentity(identity) => identity,
            _ => {
                return Err(StageError::StateValidation {
                    stage: self.name().into(),
                    message: "expected resolved identities".into(),
                });
            }
        };
        let extraction = &identity.extraction;
        // Snapshot kind per snapshot uuid — drives partial-diff semantics.
        let snapshot_kinds: HashMap<Uuid, SnapshotKind> = extraction
            .snapshot_nodes
            .iter()
            .map(|s| (s.uuid, s.snapshot_kind))
            .collect();

        let observed_properties = extraction
            .entities_by_snapshot
            .iter()
            .flat_map(|(_, nodes)| nodes.iter())
            .chain(extraction.source_deleted.iter())
            .map(|entity| {
                let mut observed = identity
                    .observations
                    .get(&entity.uuid)
                    .cloned()
                    .ok_or_else(|| StageError::StateValidation {
                        stage: self.name().into(),
                        message: "missing original observation properties".into(),
                    })?;
                observed.identity_hash = entity.identity_hash;
                Ok(observed)
            })
            .collect::<Result<Vec<_>, StageError>>()?;
        let combined_properties = combine_partial_observations(&identity, &snapshot_kinds)?;
        let observations: Vec<_> = extraction
            .entities_by_snapshot
            .iter()
            .flat_map(|(_, entities)| entities.iter())
            .collect();
        let mut nodes_to_create: Vec<Observed<EntityNode>> = Vec::new();
        let mut nodes_new_version: Vec<Observed<ResolvedChange>> = Vec::new();
        let mut nodes_volatile: Vec<Observed<ResolvedChange>> = Vec::new();
        let mut nodes_unchanged: Vec<Observed<EntityNode>> = Vec::new();
        let mut nodes_stale: Vec<Observed<EntityNode>> = Vec::new();
        let mut nodes_recreated: Vec<Observed<EntityNode>> = Vec::new();
        let mut nodes_deleted: Vec<Observed<EntityNode>> = Vec::new();

        for original in observations {
            let matched_identity = identity.matches.get(&original.uuid).ok_or_else(|| {
                StageError::StateValidation {
                    stage: self.name().into(),
                    message: "observation has no identity decision".into(),
                }
            })?;
            use kg_core::runtime::stage_output::IdentityOutcome;
            if matched_identity.outcome == IdentityOutcome::Unresolved
                || (matched_identity.outcome == IdentityOutcome::Matched)
                    != matched_identity.existing.is_some()
            {
                return Err(StageError::StateValidation {
                    stage: self.name().into(),
                    message: "observation identity remains unresolved or inconsistent".into(),
                });
            }
            let exclusions = identity
                .observations
                .get(&original.uuid)
                .map(|o| o.version_exclusions.as_slice())
                .unwrap_or(&[]);
            let mut observation = original.clone();
            observation.chain_id = matched_identity.chain_id;
            if let Some(properties) = original.last_seen_snapshot_id.and_then(|snapshot| {
                combined_properties.get(&(snapshot, matched_identity.chain_id))
            }) {
                // Source maps remain individual; only the effective classified state is combined.
                for (key, value) in properties {
                    observation
                        .all_properties
                        .insert(key.clone(), value.clone());
                }
                observation.structural_hash = structural_hash_of(
                    ctx,
                    &observation.entity_type,
                    &observation.all_properties,
                    exclusions,
                );
            }
            observation.resolved_by = identity.methods.get(&original.uuid).cloned();
            let entity = &observation;
            let kind = entity
                .last_seen_snapshot_id
                .and_then(|id| snapshot_kinds.get(&id).copied())
                .unwrap_or_default();

            let stored = matched_identity
                .existing
                .as_ref()
                .map(ExistingVersion::from_record)
                .transpose()?;
            let matched = stored.as_ref();
            if let Some(existing) = matched.filter(|version| version.deleted_at.is_none()) {
                resolve_against_existing(
                    entity,
                    existing,
                    kind,
                    ctx,
                    exclusions,
                    identity
                        .methods
                        .get(&original.uuid)
                        .map(String::as_str)
                        .unwrap_or("identity"),
                    &mut nodes_new_version,
                    &mut nodes_volatile,
                    &mut nodes_unchanged,
                    &mut nodes_stale,
                )?;
                continue;
            }

            // Restoration continues the deleted chain.
            if let Some(tomb) = matched.filter(|version| version.deleted_at.is_some()) {
                // Only an observation strictly newer than the deletion
                // restores the chain; at or before it, the deletion stands
                // and the observation is evidence against the tombstone. It
                // carries the tombstone's `deleted_at`: the chain has no live
                // version, so it is no relationship endpoint.
                if observation_is_stale(
                    entity.valid_from,
                    tomb.valid_from,
                    tomb.last_seen_at,
                    tomb.last_transition_at,
                    tomb.deleted_at,
                ) {
                    let mut stale = entity.clone();
                    stale.chain_id = tomb.chain_id;
                    stale.uuid = tomb.uuid;
                    stale.version = tomb.version;
                    stale.deleted_at = tomb.deleted_at;
                    stale.embedding = tomb.embedding.clone();
                    stale.collections = tomb.collections.clone();
                    nodes_stale.push(Observed::new(entity.uuid, stale));
                    continue;
                }
                let mut resurrected = entity.clone();
                resurrected.chain_id = tomb.chain_id;
                resurrected.version = crate::next_version(tomb.version, "entity_versioning")?;
                resurrected.previous_version_uuid = Some(tomb.uuid);
                resurrected.lifecycle = EntityLifecycle::Active;
                resurrected.deleted_at = None;
                resurrected.resolved_by = Some("resurrection".into());
                resurrected.embedding = tomb.embedding.clone();
                // Memberships restart from this observation, not the tombstone's.
                resurrected.collections = Vec::new();
                nodes_recreated.push(Observed::new(entity.uuid, resurrected));
                continue;
            }

            nodes_to_create.push(Observed::new(entity.uuid, entity.clone()));
        }

        // Unknown source deletions never create an entity.
        for entity in extraction.source_deleted.iter() {
            let existing = identity
                .matches
                .get(&entity.uuid)
                .and_then(|m| m.existing.as_ref())
                .map(ExistingVersion::from_record)
                .transpose()?;
            if let Some(existing) = existing {
                if !existing.deletable_by(&entity.source) {
                    tracing::warn!(
                        chain_id = %entity.chain_id,
                        "source-reported deletion ignored: the source owns no version of this entity"
                    );
                    continue;
                }
                // Reconfirmed absence advances capture freshness without moving the
                // first effective deletion boundary or creating another version.
                if existing.deleted_at.is_some()
                    && existing
                        .deleted_at
                        .max(existing.last_transition_at)
                        .is_some_and(|at| entity.valid_from <= at)
                {
                    continue;
                }
                // A rejected commit can refresh this identity to a newer observation.
                // Once that conflict is known, another identical deletion plan cannot
                // succeed. Keep the commit-time fence for observations arriving later.
                if observation_is_stale(
                    entity.valid_from,
                    existing.valid_from,
                    existing.last_seen_at,
                    existing.last_transition_at,
                    existing.deleted_at,
                ) {
                    if ctx.policy.for_source(&entity.source).stale_deletions
                        == kg_core::policy::StaleDeletionPolicy::Record
                    {
                        let mut stale = entity.clone();
                        stale.chain_id = existing.chain_id;
                        stale.uuid = existing.uuid;
                        stale.version = existing.version;
                        stale.deleted_at = existing.deleted_at;
                        stale.deletion_reason = Some("stale source deletion ignored".into());
                        nodes_stale.push(Observed::new(entity.uuid, stale));
                        continue;
                    }
                    return Err(StageError::StateValidation {
                        stage: self.name().into(),
                        message: "source deletion conflicts with an entity observed after its capture time".into(),
                    });
                }
                let mut deleted = entity.clone();
                deleted.chain_id = existing.chain_id;
                // The live version the deletion must find unchanged.
                deleted.uuid = existing.uuid;
                deleted.version = existing.version;
                deleted.deleted_at = existing.deleted_at;
                deleted.deleted_by = Some("source".into());
                deleted.deletion_reason = Some("source reported lifecycle=deleted".into());
                nodes_deleted.push(Observed::new(entity.uuid, deleted));
            } else {
                tracing::debug!(
                    chain_id = %entity.chain_id,
                    "source-deleted entity unknown to the graph: dropped, never created"
                );
            }
        }

        // Child edges were minted against extraction-time chain ids; point
        // them at the chains their endpoints resolved to. Evidence against a
        // tombstone resolves to no live endpoint.
        let resolved: HashMap<IdentityHash, Uuid> = nodes_to_create
            .iter()
            .map(|e| &e.value)
            .chain(nodes_new_version.iter().map(|r| &r.entity))
            .chain(nodes_volatile.iter().map(|r| &r.entity))
            .chain(nodes_unchanged.iter().map(|e| &e.value))
            .chain(
                nodes_stale
                    .iter()
                    .filter(|e| e.deleted_at.is_none())
                    .map(|e| &e.value),
            )
            .chain(nodes_recreated.iter().map(|e| &e.value))
            .map(|e| (e.identity_hash, e.chain_id))
            .collect();
        let dead: HashSet<IdentityHash> = nodes_stale
            .iter()
            .filter(|e| e.deleted_at.is_some())
            .map(|e| e.identity_hash)
            .collect();
        let sub_edges = remap_child_edges(extraction, &resolved, &dead)?;

        let stale_ids: HashSet<_> = nodes_stale
            .iter()
            .map(|node| node.observation_uuid)
            .collect();
        let mut alias_observations = HashMap::new();
        for (_, entities) in extraction.entities_by_snapshot.iter() {
            for entity in entities {
                let any_current = alias_observations
                    .entry((entity.chain_id, entity.valid_from))
                    .or_insert(false);
                *any_current |= !stale_ids.contains(&entity.uuid);
            }
        }
        // Provenance-only observations cannot grant aliases before their target existed.
        let chains_merged = identity
            .chains_merged
            .into_iter()
            .filter(|merge| {
                alias_observations
                    .get(&(merge.loser_chain_id, merge.effective_at))
                    .copied()
                    .unwrap_or(true)
            })
            .collect();

        Ok(StageOutput::NodeResolution(NodeResolutionOutput {
            reference_source_reads: Default::default(),
            raw_text_drafts: extraction.raw_text_drafts.clone(),
            relationship_changes: extraction.relationship_changes.clone(),
            identity_revisions: identity.identity_revisions.clone(),
            observed_properties: Arc::new(observed_properties),
            fk_exclusions: extraction.fk_exclusions.clone(),
            schemas: extraction.schemas.clone(),
            history: extraction.history.clone(),
            snapshot_nodes: extraction.snapshot_nodes.clone(),
            nodes_to_create: Arc::new(nodes_to_create),
            nodes_new_version: Arc::new(nodes_new_version),
            nodes_volatile: Arc::new(nodes_volatile),
            nodes_unchanged: Arc::new(nodes_unchanged),
            nodes_stale: Arc::new(nodes_stale),
            nodes_recreated: Arc::new(nodes_recreated),
            nodes_deleted: Arc::new(nodes_deleted),
            sub_edges: Arc::new(sub_edges),
            reference_owner_refresh: Arc::new(Vec::new()),
            incomplete_extractions: extraction.incomplete_extractions.clone(),
            chunk_entities: None,
            chains_merged: Arc::new(chains_merged),
        }))
    }
}

/// Child edges with their endpoints moved from the chain ids extraction
/// minted to the chains those entities resolved to. An edge touching an
/// endpoint that is stale evidence against a tombstone (`dead`) is dropped:
/// the deletion stands, and the observation is recorded on the tombstone
/// only. Every other endpoint was extracted alongside its edge, so one
/// without a resolution is a bug.
fn remap_child_edges(
    extraction: &kg_core::runtime::stage_output::NodeExtractionOutput,
    resolved: &HashMap<IdentityHash, Uuid>,
    dead: &HashSet<IdentityHash>,
) -> Result<Vec<kg_core::models::EntityEdge>, StageError> {
    if extraction.sub_edges.is_empty() {
        return Ok(Vec::new());
    }
    let extracted: HashMap<Uuid, IdentityHash> = extraction
        .entities_by_snapshot
        .iter()
        .flat_map(|(_, entities)| entities.iter())
        .map(|e| (e.chain_id, e.identity_hash))
        .collect();
    let hash_of = |extracted_chain: Uuid| {
        extracted
            .get(&extracted_chain)
            .copied()
            .ok_or_else(|| StageError::StateValidation {
                stage: "entity_versioning".into(),
                message: format!("child edge endpoint {extracted_chain} was not extracted"),
            })
    };
    let mut edges = Vec::with_capacity(extraction.sub_edges.len());
    for edge in extraction.sub_edges.iter() {
        let (source, target) = (
            hash_of(edge.source_chain_id)?,
            hash_of(edge.target_chain_id)?,
        );
        if dead.contains(&source) || dead.contains(&target) {
            tracing::debug!(
                edge = %edge.name,
                "child edge dropped: an endpoint is evidence against a tombstone"
            );
            continue;
        }
        let chain_of = |hash: IdentityHash, extracted_chain: Uuid| {
            resolved
                .get(&hash)
                .copied()
                .ok_or_else(|| StageError::StateValidation {
                    stage: "entity_versioning".into(),
                    message: format!("child edge endpoint {extracted_chain} was not resolved"),
                })
        };
        let mut edge = edge.clone();
        edge.source_chain_id = chain_of(source, edge.source_chain_id)?;
        edge.target_chain_id = chain_of(target, edge.target_chain_id)?;
        edges.push(edge);
    }
    Ok(edges)
}

type SnapshotProperties = HashMap<(Uuid, Uuid), indexmap::IndexMap<String, PropertyValue>>;
type ChainInputs = HashMap<Uuid, HashSet<usize>>;
type BatchConflicts = (HashSet<usize>, ChainInputs);

fn batch_conflict() -> StageError {
    StageError::StateValidation {
        stage: "entity_versioning".into(),
        message: "conflicting simultaneous partial observations or dependent new identity".into(),
    }
}

/// Validate the same simultaneous partial state the persistence planner will assemble.
fn partial_batch_conflicts(inputs: &[StageOutput]) -> Result<BatchConflicts, StageError> {
    type State = (
        HashSet<usize>,
        indexmap::IndexMap<String, PropertyValue>,
        bool,
    );
    let mut states: HashMap<(Uuid, DateTime<Utc>), State> = HashMap::new();
    let mut new_chains: HashMap<Uuid, HashSet<usize>> = HashMap::new();
    for (index, input) in inputs.iter().enumerate() {
        let StageOutput::NodeIdentity(identity) = input else {
            return Err(StageError::StateValidation {
                stage: "entity_versioning".into(),
                message: "expected resolved identities".into(),
            });
        };
        // A source-reported deletion of an entity the graph does not know has
        // no identity decision: `process` drops it as a no-op, so it neither
        // anchors a chain nor states a simultaneous value here.
        let observed = identity
            .extraction
            .entities_by_snapshot
            .iter()
            .flat_map(|(_, nodes)| nodes.iter())
            .map(|entity| (entity, true))
            .chain(
                identity
                    .extraction
                    .source_deleted
                    .iter()
                    .map(|entity| (entity, false)),
            );
        for (entity, required) in observed {
            let matched = match (identity.matches.get(&entity.uuid), required) {
                (Some(matched), _) => matched,
                (None, true) => return Err(batch_conflict()),
                (None, false) => continue,
            };
            if matched.existing.is_none() {
                new_chains
                    .entry(matched.chain_id)
                    .or_default()
                    .insert(index);
            }
            let original = match (identity.observations.get(&entity.uuid), required) {
                (Some(original), _) => original,
                (None, true) => return Err(batch_conflict()),
                (None, false) => continue,
            };
            let snapshot = identity
                .extraction
                .snapshot_nodes
                .iter()
                .find(|snapshot| snapshot.uuid == original.snapshot_uuid)
                .ok_or_else(batch_conflict)?;
            if snapshot.snapshot_kind == SnapshotKind::Full {
                continue;
            }
            let (members, properties, conflict) = states
                .entry((matched.chain_id, entity.valid_from))
                .or_default();
            members.insert(index);
            for (key, value) in &original.properties {
                *conflict |= properties
                    .get(key)
                    .is_some_and(|previous| previous != value);
                properties.insert(key.clone(), value.clone());
            }
            *conflict |= PropertyValue::validate_flat_paths(properties).is_err();
        }
    }
    let rejected = states
        .into_values()
        .filter(|(_, _, conflict)| *conflict)
        .flat_map(|(members, _, _)| members)
        .collect();
    Ok((rejected, new_chains))
}

/// Complementary mentions in one partial snapshot describe one simultaneous state.
fn combine_partial_observations(
    identity: &kg_core::runtime::stage_output::NodeIdentityOutput,
    kinds: &HashMap<Uuid, SnapshotKind>,
) -> Result<SnapshotProperties, StageError> {
    let mut groups: HashMap<(Uuid, Uuid), (usize, indexmap::IndexMap<String, PropertyValue>)> =
        HashMap::new();
    for (snapshot, entities) in identity.extraction.entities_by_snapshot.iter() {
        if kinds.get(snapshot) == Some(&SnapshotKind::Full) {
            continue;
        }
        for entity in entities {
            let matched =
                identity
                    .matches
                    .get(&entity.uuid)
                    .ok_or_else(|| StageError::StateValidation {
                        stage: "entity_versioning".into(),
                        message: "observation has no identity decision".into(),
                    })?;
            let source = identity.observations.get(&entity.uuid).ok_or_else(|| {
                StageError::StateValidation {
                    stage: "entity_versioning".into(),
                    message: "missing original observation properties".into(),
                }
            })?;
            let (count, properties) = groups.entry((*snapshot, matched.chain_id)).or_default();
            *count += 1;
            for (key, value) in &source.properties {
                if properties
                    .get(key)
                    .is_some_and(|previous| previous != value)
                {
                    return Err(StageError::StateValidation {
                        stage: "entity_versioning".into(),
                        message: "contradictory observations of one identity in the same snapshot"
                            .into(),
                    });
                }
                properties.insert(key.clone(), value.clone());
            }
            PropertyValue::validate_flat_paths(properties).map_err(|message| {
                StageError::StateValidation {
                    stage: "entity_versioning".into(),
                    message,
                }
            })?;
        }
    }
    Ok(groups
        .into_iter()
        .filter_map(|(key, (count, values))| (count > 1).then_some((key, values)))
        .collect())
}

/// Stale observations add evidence but cannot change current entity state.
pub(crate) fn observation_is_stale(
    captured_at: DateTime<Utc>,
    valid_from: Option<DateTime<Utc>>,
    last_seen_at: Option<DateTime<Utc>>,
    last_transition_at: Option<DateTime<Utc>>,
    deleted_at: Option<DateTime<Utc>>,
) -> bool {
    if let Some(deleted_at) = deleted_at {
        captured_at <= deleted_at || last_transition_at.is_some_and(|at| captured_at <= at)
    } else {
        valid_from
            .max(last_seen_at)
            .max(last_transition_at)
            .is_some_and(|at| captured_at < at)
    }
}

/// Classify an observation against a known live version.
#[allow(clippy::too_many_arguments)]
fn resolve_against_existing(
    entity: &EntityNode,
    existing: &ExistingVersion,
    kind: SnapshotKind,
    ctx: &RuntimeContext,
    exclusions: &[String],
    resolved_by: &str,
    nodes_new_version: &mut Vec<Observed<ResolvedChange>>,
    nodes_volatile: &mut Vec<Observed<ResolvedChange>>,
    nodes_unchanged: &mut Vec<Observed<EntityNode>>,
    nodes_stale: &mut Vec<Observed<EntityNode>>,
) -> Result<(), StageError> {
    // Time-aware versioning. A snapshot captured before the stored version's
    // validity start or before its latest observation must never supersede
    // or re-bookkeep it (redelivery, connector retry). It still records
    // provenance against the stored version.
    let newest = existing
        .valid_from
        .max(existing.last_seen_at)
        .max(existing.last_transition_at);
    if let Some(newest) = newest {
        if observation_is_stale(
            entity.valid_from,
            existing.valid_from,
            existing.last_seen_at,
            existing.last_transition_at,
            existing.deleted_at,
        ) {
            tracing::warn!(
                chain_id = %entity.chain_id,
                incoming = %entity.valid_from,
                stored = %newest,
                "observation older than the stored version: provenance only"
            );
            let mut stale = entity.clone();
            stale.chain_id = existing.chain_id;
            stale.uuid = existing.uuid;
            stale.version = existing.version;
            stale.embedding = existing.embedding.clone();
            stale.collections = existing.collections.clone();
            nodes_stale.push(Observed::new(entity.uuid, stale));
            return Ok(());
        }
    }

    // A partial payload describes only the keys it carries; the version it
    // continues keeps everything else, so the entity carries the full state.
    let properties = merge_partial(kind, &entity.all_properties, &existing.properties);
    let unchanged_hash = existing.structural_hash;
    match classify_properties(entity, &existing.properties, kind, ctx, exclusions) {
        VersioningDecision::Unchanged => {
            let mut unchanged = entity.clone();
            unchanged.name = existing.name.clone();
            unchanged.summary = existing.summary.clone();
            unchanged.inherited_labels = inherited_labels(kind, &existing.labels);
            unchanged.all_properties = properties;
            unchanged.structural_hash = unchanged_hash;
            unchanged.chain_id = existing.chain_id;
            unchanged.uuid = existing.uuid;
            unchanged.version = existing.version;
            unchanged.embedding = existing.embedding.clone();
            unchanged.collections = existing.collections.clone();
            nodes_unchanged.push(Observed::new(entity.uuid, unchanged));
        }
        VersioningDecision::MergeInPlace { changed } => {
            // MergeInPlace updates the EXISTING version — it adopts the
            // existing uuid and mints nothing.
            let mut merged = entity.clone();
            merged.name = existing.name.clone();
            merged.summary = existing.summary.clone();
            merged.inherited_labels = inherited_labels(kind, &existing.labels);
            merged.structural_hash =
                structural_hash_of(ctx, &entity.entity_type, &properties, exclusions);
            merged.all_properties = properties;
            merged.uuid = existing.uuid;
            merged.chain_id = existing.chain_id;
            merged.version = existing.version;
            merged.previous_version_uuid = None;
            merged.resolved_by = Some(resolved_by.into());
            merged.embedding = existing.embedding.clone();
            merged.collections = existing.collections.clone();
            nodes_volatile.push(Observed::new(
                entity.uuid,
                ResolvedChange {
                    entity: merged,
                    changes: changed,
                },
            ));
        }
        VersioningDecision::NewVersion { changed } => {
            let mut updated = entity.clone();
            updated.inherited_labels = inherited_labels(kind, &existing.labels);
            updated.structural_hash =
                structural_hash_of(ctx, &entity.entity_type, &properties, exclusions);
            updated.all_properties = properties;
            updated.chain_id = existing.chain_id;
            updated.version = crate::next_version(existing.version, "entity_versioning")?;
            updated.previous_version_uuid = Some(existing.uuid);
            updated.resolved_by = Some(resolved_by.into());
            updated.embedding = existing.embedding.clone();
            updated.collections = existing.collections.clone();
            nodes_new_version.push(Observed::new(
                entity.uuid,
                ResolvedChange {
                    entity: updated,
                    changes: changed,
                },
            ));
        }
    }
    Ok(())
}

impl ExistingVersion {
    /// What resolution needs from a stored version: system fields plus the
    /// source properties in `PropertyValue` form.
    fn from_record(record: &EntityVersionRecord) -> Result<Self, StageError> {
        let properties =
            record
                .typed_source_properties()
                .map_err(|message| StageError::StateValidation {
                    stage: "entity_versioning".into(),
                    message,
                })?;
        Ok(Self {
            uuid: record.uuid,
            chain_id: record.chain_id,
            version: record.version,
            name: record.name.clone(),
            summary: record
                .stored
                .get("summary")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            structural_hash: stored_structural_hash(record.structural_hash.as_deref())?,
            valid_from: record.valid_from,
            last_seen_at: record.last_seen_at,
            last_transition_at: record.last_transition_at,
            deleted_at: record.deleted_at,
            properties,
            labels: record
                .stored
                .get("labels")
                .and_then(serde_json::Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
            embedding: ComputedEmbedding::from_stored(record).map(Arc::new),
            source: record.source.clone(),
            collections: record.collections.clone(),
        })
    }
}

/// Compare incoming entity against existing version to decide versioning.
pub(crate) fn classify_properties(
    incoming: &EntityNode,
    existing_properties: &indexmap::IndexMap<String, PropertyValue>,
    kind: SnapshotKind,
    ctx: &RuntimeContext,
    exclusions: &[String],
) -> VersioningDecision {
    let mut effective = ctx
        .entity_type_configs
        .get(&incoming.entity_type)
        .map(|config| config.hash_exclusions())
        .unwrap_or_default();
    effective.extend_from_slice(exclusions);
    // Stored hashes may reflect different exclusion rules; the property diff is authoritative.
    let changes = diff_properties(&incoming.all_properties, existing_properties, kind);
    if changes.is_empty() {
        return VersioningDecision::Unchanged;
    }
    if changes
        .iter()
        .all(|change| effective.contains(&change.property))
    {
        return VersioningDecision::MergeInPlace { changed: changes };
    }
    VersioningDecision::NewVersion { changed: changes }
}

/// The full state after an observation: a partial payload fills in what it
/// omitted from the version it continues; a full payload is the state.
/// The labels a version carries after an observation: a full snapshot states
/// them; a partial one adds to the stored set (storage unions the same way), so
/// the entity, and the text embedded for it, match what is stored.
/// A full observation replaces labels; a partial one adds to the stored ones, which
/// the entity then inherits for its embedding text (`EntityNode::effective_labels`).
pub(crate) fn inherited_labels(kind: SnapshotKind, stored: &[String]) -> Vec<String> {
    if kind == SnapshotKind::Full {
        Vec::new()
    } else {
        stored.to_vec()
    }
}

pub(crate) fn merge_partial(
    kind: SnapshotKind,
    incoming: &indexmap::IndexMap<String, PropertyValue>,
    stored: &indexmap::IndexMap<String, PropertyValue>,
) -> indexmap::IndexMap<String, PropertyValue> {
    if kind == SnapshotKind::Full {
        return incoming.clone();
    }
    let mut merged = stored.clone();
    // Explicit paths replace incompatible stored ancestors or descendants. Siblings survive.
    merged.retain(|stored_key, _| {
        !incoming.keys().any(|incoming_key| {
            stored_key
                .strip_prefix(incoming_key.as_str())
                .is_some_and(|suffix| suffix.starts_with('.'))
                || incoming_key
                    .strip_prefix(stored_key.as_str())
                    .is_some_and(|suffix| suffix.starts_with('.'))
        })
    });
    for (key, value) in incoming {
        merged.insert(key.clone(), value.clone());
    }
    merged
}

/// Hash completed state using the same rules as the original observation.
pub(crate) fn structural_hash_of(
    ctx: &RuntimeContext,
    entity_type: &str,
    properties: &indexmap::IndexMap<String, PropertyValue>,
    observation_exclusions: &[String],
) -> u64 {
    let mut exclusions = ctx
        .entity_type_configs
        .get(entity_type)
        .map(|c| c.hash_exclusions())
        .unwrap_or_default();
    exclusions.extend_from_slice(observation_exclusions);
    kg_core::identity::structural_hash::compute_structural_hash(properties, &exclusions)
}

/// Diff two property maps and return the changes.
///
/// Partial input preserves omitted fields except ancestors/subtrees replaced by an explicit path.
/// Full input replaces the complete state.
pub(crate) fn diff_properties(
    incoming: &indexmap::IndexMap<String, PropertyValue>,
    existing: &indexmap::IndexMap<String, PropertyValue>,
    kind: SnapshotKind,
) -> Vec<PropertyChange> {
    let effective = merge_partial(kind, incoming, existing);
    let mut changes = Vec::new();

    for (key, new_val) in &effective {
        match existing.get(key) {
            Some(old_val) if old_val != new_val => {
                changes.push(PropertyChange {
                    property: key.clone(),
                    old_value: Some(serde_json::to_value(old_val).unwrap_or_default()),
                    new_value: Some(serde_json::to_value(new_val).unwrap_or_default()),
                });
            }
            None => {
                changes.push(PropertyChange {
                    property: key.clone(),
                    old_value: None,
                    new_value: Some(serde_json::to_value(new_val).unwrap_or_default()),
                });
            }
            _ => {} // Same value, no change
        }
    }

    {
        for key in existing.keys() {
            if !effective.contains_key(key) {
                changes.push(PropertyChange {
                    property: key.clone(),
                    old_value: Some(serde_json::to_value(&existing[key]).unwrap_or_default()),
                    new_value: None,
                });
            }
        }
    }

    changes
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn corrupt_stored_hash_is_not_treated_as_zero() {
        assert_eq!(stored_structural_hash(None).unwrap(), 0);
        assert_eq!(stored_structural_hash(Some("0")).unwrap(), 0);
        assert_eq!(
            stored_structural_hash(Some("18446744073709551615")).unwrap(),
            u64::MAX
        );
        for value in ["", "invalid", "-1", "18446744073709551616"] {
            assert!(stored_structural_hash(Some(value)).is_err());
        }
    }

    fn props(pairs: &[(&str, i64)]) -> indexmap::IndexMap<String, PropertyValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), PropertyValue::Integer(*v)))
            .collect()
    }

    #[test]
    fn diff_detects_changes() {
        let old_props = props(&[("replicas", 3), ("port", 80)]);
        let mut new_props = props(&[("replicas", 5), ("port", 80)]);
        new_props.insert("ready".into(), PropertyValue::Bool(true));

        let changes = diff_properties(&new_props, &old_props, SnapshotKind::Full);
        assert_eq!(changes.len(), 2); // replicas changed, ready added
    }

    #[test]
    fn partial_payload_absence_is_not_removal() {
        // Incremental payload missing a key must NOT count it removed
        let stored = props(&[("replicas", 3), ("port", 80)]);
        let partial = props(&[("replicas", 3)]);

        let incremental = diff_properties(&partial, &stored, SnapshotKind::Incremental);
        assert!(
            incremental.is_empty(),
            "partial payload must not fabricate removals, got {incremental:?}"
        );

        let full = diff_properties(&partial, &stored, SnapshotKind::Full);
        assert_eq!(full.len(), 1, "full payload absence IS removal");
        assert!(full[0].new_value.is_none());
    }

    #[test]
    fn partial_parent_replacement_removes_subtree_but_preserves_unrelated_fields() {
        let stored = props(&[
            ("deployment.region", 1),
            ("deployment.zone", 2),
            ("deployment_other", 3),
        ]);
        let incoming = indexmap::IndexMap::from([("deployment".into(), PropertyValue::Null)]);
        let merged = merge_partial(SnapshotKind::Incremental, &incoming, &stored);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged["deployment"], PropertyValue::Null);
        assert_eq!(merged["deployment_other"], PropertyValue::Integer(3));
        let changes = diff_properties(&incoming, &stored, SnapshotKind::Incremental);
        assert_eq!(changes.len(), 3);
        assert_eq!(
            changes
                .iter()
                .filter(|change| change.new_value.is_none())
                .count(),
            2
        );
    }

    #[test]
    fn partial_child_replacement_removes_atomic_parent_and_keeps_flat_siblings() {
        let incoming = props(&[("deployment.region", 2)]);
        for parent in [
            PropertyValue::Null,
            PropertyValue::Json(r#"{"zone":1}"#.into()),
            PropertyValue::String("unknown".into()),
        ] {
            let stored = indexmap::IndexMap::from([
                ("deployment".into(), parent),
                ("other".into(), PropertyValue::Integer(4)),
            ]);
            let merged = merge_partial(SnapshotKind::Incremental, &incoming, &stored);
            assert!(!merged.contains_key("deployment"));
            assert!(
                !merged.contains_key("deployment.zone"),
                "opaque values are atomic"
            );
            assert_eq!(merged["other"], PropertyValue::Integer(4));
            assert_eq!(
                diff_properties(&incoming, &stored, SnapshotKind::Incremental).len(),
                2
            );
        }
        let stored = props(&[("deployment.region", 1), ("deployment.zone", 3)]);
        let merged = merge_partial(SnapshotKind::Incremental, &incoming, &stored);
        assert_eq!(merged["deployment.zone"], PropertyValue::Integer(3));
        assert_eq!(merged["deployment.region"], PropertyValue::Integer(2));
        assert_eq!(
            merge_partial(SnapshotKind::Full, &incoming, &stored),
            incoming
        );
    }

    fn context(config: kg_core::entity_type_config::EntityTypeConfig) -> RuntimeContext {
        use kg_core::{
            runtime::RuntimeContextBuilder,
            test_support::{MockEmbedBackend, MockLlmBackend},
        };
        let mut ctx = RuntimeContextBuilder::new("org")
            .graph(Arc::new(kg_core::test_support::UnreachableGraph))
            .llm_default(Arc::new(MockLlmBackend::empty()))
            .llm_extraction(Arc::new(MockLlmBackend::empty()))
            .embedder(Arc::new(MockEmbedBackend::default_dimension()))
            .build()
            .unwrap();
        ctx.entity_type_configs = Arc::new(HashMap::from([("Type".into(), config)]));
        ctx
    }

    #[test]
    fn excluded_changes_merge_even_with_partial_or_equal_hashes() {
        let ctx = context(kg_core::entity_type_config::EntityTypeConfig {
            exclude_from_hash: vec!["noise".into()],
            volatile_properties: vec!["heartbeat".into()],
            ..Default::default()
        });
        let stored = props(&[
            ("id", 1),
            ("noise", 1),
            ("heartbeat", 1),
            ("snapshot_only", 1),
        ]);
        for kind in [SnapshotKind::Full, SnapshotKind::Incremental] {
            for hash in [0, 55, 99] {
                let mut node = test_entity("api");
                node.all_properties =
                    props(&[("noise", 2), ("heartbeat", 2), ("snapshot_only", 2)]);
                if kind == SnapshotKind::Full {
                    node.all_properties
                        .insert("id".into(), PropertyValue::Integer(1));
                }
                node.structural_hash = hash;
                let result =
                    classify_properties(&node, &stored, kind, &ctx, &["snapshot_only".into()]);
                let VersioningDecision::MergeInPlace { changed } = result else {
                    panic!("{result:?}")
                };
                assert_eq!(changed.len(), 3);
            }
        }
    }

    #[test]
    fn forced_versions_and_snapshot_override_use_the_diff_not_old_hash() {
        let ctx = context(kg_core::entity_type_config::EntityTypeConfig {
            exclude_from_hash: vec!["noise".into()],
            volatile_properties: vec!["noise".into()],
            always_version_on_change: vec!["noise".into()],
            ..Default::default()
        });
        let mut node = test_entity("api");
        node.all_properties = props(&[("noise", 2)]);
        node.structural_hash = 55;
        let stored = props(&[("noise", 1)]);
        assert!(matches!(
            classify_properties(&node, &stored, SnapshotKind::Full, &ctx, &[]),
            VersioningDecision::NewVersion { .. }
        ));
        assert!(matches!(
            classify_properties(&node, &stored, SnapshotKind::Full, &ctx, &["noise".into()]),
            VersioningDecision::MergeInPlace { .. }
        ));
        node.all_properties
            .insert("structural".into(), PropertyValue::Integer(3));
        let VersioningDecision::NewVersion { changed } =
            classify_properties(&node, &stored, SnapshotKind::Full, &ctx, &["noise".into()])
        else {
            panic!("mixed changes must version")
        };
        assert_eq!(changed.len(), 2);
    }

    #[test]
    fn full_removal_and_explicit_null_remain_distinct_from_partial_omission() {
        let ctx = context(Default::default());
        let mut node = test_entity("api");
        let stored = props(&[("noise", 1)]);
        let exclusions = vec!["noise".into()];
        let VersioningDecision::MergeInPlace { changed } =
            classify_properties(&node, &stored, SnapshotKind::Full, &ctx, &exclusions)
        else {
            panic!("full removal must persist")
        };
        assert!(changed[0].new_value.is_none());
        assert!(matches!(
            classify_properties(&node, &stored, SnapshotKind::Incremental, &ctx, &exclusions),
            VersioningDecision::Unchanged
        ));
        node.all_properties
            .insert("noise".into(), PropertyValue::Null);
        let VersioningDecision::MergeInPlace { changed } =
            classify_properties(&node, &stored, SnapshotKind::Incremental, &ctx, &exclusions)
        else {
            panic!("explicit null must persist")
        };
        assert!(changed[0].new_value.is_some());
        let mut observed = kg_core::runtime::stage_output::ObservedEntityProperties::from_entity(
            &node,
            Uuid::new_v4(),
        );
        observed.version_exclusions = exclusions.clone();
        let restored: kg_core::runtime::stage_output::ObservedEntityProperties =
            serde_json::from_value(serde_json::to_value(&observed).unwrap()).unwrap();
        assert_eq!(restored.version_exclusions, exclusions);
        assert_eq!(restored.properties, node.all_properties);
        let checkpoint = kg_core::runtime::stage_output::NodeCheckpoint {
            snapshot_index: 0,
            resolution: NodeResolutionOutput {
                relationship_changes: Default::default(),
                observed_properties: Arc::new(vec![observed]),
                ..Default::default()
            },
        };
        let restored: kg_core::runtime::stage_output::NodeCheckpoint =
            serde_json::from_value(serde_json::to_value(checkpoint).unwrap()).unwrap();
        assert_eq!(
            restored.resolution.observed_properties[0].version_exclusions,
            exclusions
        );
    }

    #[test]
    fn unchanged_and_volatile_versions_keep_persisted_native_name_and_summary() {
        let ctx = context(Default::default());
        for volatile in [false, true] {
            for summary in [None, Some("stored description".to_string())] {
                let mut incoming = test_entity("renamed display name");
                incoming.primary_key_properties = vec!["resource_id".into()];
                incoming.all_properties = props(&[
                    ("resource_id", 1),
                    ("heartbeat", if volatile { 2 } else { 1 }),
                ]);
                incoming.summary = Some("incoming description".into());
                let existing = ExistingVersion {
                    uuid: Uuid::new_v4(),
                    chain_id: incoming.chain_id,
                    version: 3,
                    name: "canonical display name".into(),
                    summary: summary.clone(),
                    structural_hash: 0,
                    valid_from: None,
                    last_seen_at: None,
                    last_transition_at: None,
                    deleted_at: None,
                    properties: props(&[("resource_id", 1), ("heartbeat", 1)]),
                    labels: Vec::new(),
                    embedding: None,
                    source: None,
                    collections: vec![],
                };
                let (mut new, mut changed, mut unchanged, mut stale) =
                    (vec![], vec![], vec![], vec![]);
                resolve_against_existing(
                    &incoming,
                    &existing,
                    SnapshotKind::Full,
                    &ctx,
                    &["heartbeat".into()],
                    "primary_key",
                    &mut new,
                    &mut changed,
                    &mut unchanged,
                    &mut stale,
                )
                .unwrap();
                assert!(new.is_empty() && stale.is_empty());
                let adopted = if volatile {
                    &changed[0].entity
                } else {
                    &unchanged[0].value
                };
                assert_eq!(adopted.uuid, existing.uuid);
                assert_eq!(adopted.version, 3);
                assert_eq!(adopted.name, "canonical display name");
                assert_eq!(adopted.summary, summary);
            }
        }
    }

    pub(crate) fn test_entity(name: &str) -> EntityNode {
        EntityNode {
            labels: Vec::new(),
            inherited_labels: Vec::new(),
            uuid: Uuid::new_v4(),
            chain_id: Uuid::new_v4(),
            org_id: "org".into(),
            namespace: "ns".into(),
            entity_type: "Type".into(),
            name: name.into(),
            all_properties: indexmap::IndexMap::new(),
            primary_key_properties: vec!["name".into()],
            additional_key_properties: vec![],
            identity_hash: IdentityHash::compute("org", "ns", "Type", &[("name", name)]),
            lifecycle: EntityLifecycle::Active,
            version: 1,
            is_latest: true,
            previous_version_uuid: None,
            embedding: None,
            valid_from: Utc::now(),
            valid_to: None,
            deleted_at: None,
            deleted_by: None,
            deletion_reason: None,
            source: "test".into(),
            extracted_by: "test".into(),
            resolved_by: None,
            first_seen_snapshot_id: None,
            last_seen_snapshot_id: None,
            last_seen_at: None,
            sync_generation: None,
            tags: indexmap::IndexMap::new(),
            summary: None,
            structural_hash: 0,
            needs_llm_review: false,
            collections: Vec::new(),
        }
    }
}
