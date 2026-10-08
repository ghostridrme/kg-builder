//! Plan each entity lineage and retain finalized content for every written version.
use super::incident_deletion_planning::plan_source_deletions;
use super::mutation_plan::{invalid, set_string, set_uuid, Plan, STAGE};
use crate::node::entity_versioning::{classify_properties, merge_partial, structural_hash_of};
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use kg_core::traits::graph_mutation::{entity_embedding_state, TAG_PROPERTY_PREFIX};
use kg_core::{
    embedding::{self, EmbeddingSettings},
    enums::versioning::PropertyChange,
    errors::StageError,
    models::{
        CollectionMembership, EntityNode, PropertyValue, SnapshotKind, SnapshotNode,
        GENERATIONS_PROPERTY, MEMBERS_PROPERTY,
    },
    runtime::{
        stage_output::{NodeBatch, PlannedEmbedding},
        RuntimeContext,
    },
    traits::{GraphMutation, GraphProperties, Precondition},
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use uuid::Uuid;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Create,
    NewVersion,
    Volatile,
    Unchanged,
    Stale,
    Recreated,
    Deleted,
}

/// One snapshot's observation of a chain, as the resolver classified it.
struct Observation<'a> {
    provided: &'a kg_core::runtime::stage_output::ObservedEntityProperties,
    entity: &'a EntityNode,
    changes: &'a [PropertyChange],
    decision: Decision,
    snapshot_uuid: Uuid,
    snapshot_index: usize,
    kind: SnapshotKind,
    /// The collection the observing snapshot declares, at its generation.
    membership: Option<CollectionMembership>,
}

impl Observation<'_> {
    /// The memberships a version carries after this observation.
    fn memberships(&self, carried: &[CollectionMembership]) -> Vec<CollectionMembership> {
        CollectionMembership::merge(carried, self.membership.as_ref())
    }
}

/// One mutation with its position in the dependency order: observation
/// round within the chain, then mutation rank, then chain order. Sorting by
/// this key keeps each chain's lineage in order while grouping like
/// mutations across chains.
struct Step {
    round: usize,
    rank: u8,
    chain: usize,
    mutation: GraphMutation,
}

const RANK_CREATE: u8 = 0;
const RANK_SUPERSEDE: u8 = 1;
const RANK_UPSERT_SUCCESSOR: u8 = 2;
const RANK_REPOINT: u8 = 3;
const RANK_UPDATE: u8 = 4;
const RANK_METADATA: u8 = 5;
const RANK_OBSERVE: u8 = 6;
const RANK_DELETE: u8 = 7;

/// The version a chain's later observations are decided against.
#[derive(Clone)]
struct Head<'a> {
    uuid: Uuid,
    version: u32,
    structural_hash: u64,
    properties: IndexMap<String, PropertyValue>,
    valid_from: DateTime<Utc>,
    /// Written by this batch, so no external precondition can name it.
    in_batch: bool,
    /// Content writes can clear a stored vector even when rendered text is unchanged.
    content_written: bool,
    /// Collections the version belongs to after the observations so far.
    collections: Vec<CollectionMembership>,
    /// Native fields of the version actually written or adopted from storage.
    entity: &'a EntityNode,
}

impl<'a> Head<'a> {
    fn in_batch(
        entity: &'a EntityNode,
        uuid: Uuid,
        version: u32,
        collections: Vec<CollectionMembership>,
    ) -> Self {
        Self::written(
            entity,
            uuid,
            version,
            entity.all_properties.clone(),
            entity.structural_hash,
            collections,
        )
    }

    /// A version this batch writes with `properties` as its full state.
    fn written(
        entity: &'a EntityNode,
        uuid: Uuid,
        version: u32,
        properties: IndexMap<String, PropertyValue>,
        structural_hash: u64,
        collections: Vec<CollectionMembership>,
    ) -> Self {
        Self {
            uuid,
            version,
            structural_hash,
            properties,
            valid_from: entity.valid_from,
            in_batch: true,
            content_written: true,
            collections,
            entity,
        }
    }

    /// The stored version, with the memberships it holds after this observation.
    fn stored(entity: &'a EntityNode, collections: Vec<CollectionMembership>) -> Self {
        Self {
            in_batch: false,
            content_written: false,
            ..Self::in_batch(entity, entity.uuid, entity.version, collections)
        }
    }
}

pub(crate) async fn plan_nodes(
    batch: &NodeBatch,
    ctx: &RuntimeContext,
) -> Result<Plan, StageError> {
    let org = ctx.org_id.as_ref();
    let mut plan = Plan::default();
    plan.preconditions.extend(
        batch
            .identity_revisions
            .iter()
            .cloned()
            .map(Precondition::IdentityRevisionIs),
    );

    let mut snapshots: HashMap<Uuid, (usize, &SnapshotNode)> = HashMap::new();
    for (index, snapshot) in batch.snapshot_nodes.iter().enumerate() {
        if ctx.context_settings.store_content
            && snapshot
                .content
                .as_ref()
                .is_some_and(|s| s.len() > ctx.context_settings.max_stored_bytes)
        {
            return Err(invalid("snapshot content exceeds storage limit".into()));
        }
        if snapshots.insert(snapshot.uuid, (index, snapshot)).is_some() {
            return Err(invalid(format!(
                "snapshot {} appears twice in the batch",
                snapshot.uuid
            )));
        }
        plan.mutations.push(GraphMutation::UpsertSnapshot {
            uuid: snapshot.uuid,
            properties: snapshot_properties(snapshot, ctx.context_settings.store_content),
        });
        plan.counts.snapshots += 1;
    }

    let mut provided = HashMap::new();
    for observation in batch.observed_properties.iter() {
        if provided
            .insert(observation.observation_uuid, observation)
            .is_some()
        {
            return Err(invalid("duplicate original observation UUID".into()));
        }
    }
    let mut by_chain: HashMap<Uuid, Vec<Observation<'_>>> = HashMap::new();
    for entity in batch.nodes_to_create.iter() {
        collect(
            &mut by_chain,
            &snapshots,
            &mut provided,
            entity.observation_uuid,
            entity,
            &[],
            Decision::Create,
        )?;
    }
    for change in batch.nodes_new_version.iter() {
        collect(
            &mut by_chain,
            &snapshots,
            &mut provided,
            change.observation_uuid,
            &change.entity,
            &change.changes,
            Decision::NewVersion,
        )?;
    }
    for change in batch.nodes_volatile.iter() {
        collect(
            &mut by_chain,
            &snapshots,
            &mut provided,
            change.observation_uuid,
            &change.entity,
            &change.changes,
            Decision::Volatile,
        )?;
    }
    for entity in batch.nodes_unchanged.iter() {
        collect(
            &mut by_chain,
            &snapshots,
            &mut provided,
            entity.observation_uuid,
            entity,
            &[],
            Decision::Unchanged,
        )?;
    }
    for entity in batch.nodes_stale.iter() {
        collect(
            &mut by_chain,
            &snapshots,
            &mut provided,
            entity.observation_uuid,
            entity,
            &[],
            Decision::Stale,
        )?;
    }
    for entity in batch.nodes_recreated.iter() {
        collect(
            &mut by_chain,
            &snapshots,
            &mut provided,
            entity.observation_uuid,
            entity,
            &[],
            Decision::Recreated,
        )?;
    }
    for entity in batch.nodes_deleted.iter() {
        collect(
            &mut by_chain,
            &snapshots,
            &mut provided,
            entity.observation_uuid,
            entity,
            &[],
            Decision::Deleted,
        )?;
    }

    // Deterministic order: capture time then snapshot position within a
    // chain; first snapshot position then chain id across chains.
    let mut chains: Vec<(Uuid, Vec<Observation<'_>>)> = by_chain.into_iter().collect();
    for (_, observations) in &mut chains {
        observations.sort_by_key(|o| (o.entity.valid_from, o.snapshot_index));
    }
    chains.sort_by_key(|(chain, observations)| {
        (
            observations
                .iter()
                .map(|o| o.snapshot_index)
                .min()
                .unwrap_or(0),
            *chain,
        )
    });

    let mut steps: Vec<Step> = Vec::new();
    let mut links: Vec<GraphMutation> = Vec::new();

    for (order, (chain, observations)) in chains.iter().enumerate() {
        for head in linearize(
            *chain,
            observations,
            &batch.fk_exclusions,
            order,
            org,
            ctx,
            &mut plan,
            &mut steps,
            &mut links,
        )? {
            if ctx.embedder.is_configured() {
                if let Some(target) = embedding_target(&head, observations, &ctx.embedding, org) {
                    plan.embeddings.push(target);
                }
            }
        }
    }
    steps.sort_by_key(|s| (s.round, s.rank, s.chain));
    plan.mutations.extend(steps.into_iter().map(|s| s.mutation));
    let mut deletion_evidence = batch
        .nodes_deleted
        .iter()
        .map(|entity| {
            let snapshot = entity
                .last_seen_snapshot_id
                .and_then(|id| snapshots.get(&id).map(|(_, snapshot)| *snapshot))
                .ok_or_else(|| invalid("source deletion has no observing snapshot".into()))?;
            Ok(((entity.chain_id, snapshot.captured_at), snapshot.uuid))
        })
        .collect::<Result<HashMap<_, _>, StageError>>()?;
    super::owned_child_deletion::plan(
        &mut plan,
        &mut deletion_evidence,
        &batch.pending_child_edges,
        ctx,
    )
    .await?;
    plan_source_deletions(&mut plan, &deletion_evidence, ctx).await?;

    // Merges follow entity versions: the winner's adopted version exists by now.
    let mut merges = batch.chains_merged.iter().collect::<Vec<_>>();
    merges.sort_by_key(|m| (m.effective_at, m.loser_chain_id, m.winner_chain_id));
    let mut merged = HashSet::new();
    for merge in merges {
        let mut hashes = merge.merged_identity_hashes.clone();
        hashes.sort();
        hashes.dedup();
        if !merged.insert((
            merge.loser_chain_id,
            merge.winner_chain_id,
            merge.effective_at,
            hashes.clone(),
        )) {
            continue;
        }
        plan.mutations.push(GraphMutation::MergeChains {
            effective_at: merge.effective_at,
            loser_chain_id: merge.loser_chain_id,
            winner_chain_id: merge.winner_chain_id,
            identity_hashes: hashes,
        });
        plan.counts.chains_merged += 1;
    }

    // Evidence last: every observed version and snapshot exists by now.
    plan.mutations.extend(links);
    Ok(plan)
}

fn collect<'a>(
    by_chain: &mut HashMap<Uuid, Vec<Observation<'a>>>,
    snapshots: &HashMap<Uuid, (usize, &'a SnapshotNode)>,
    provided: &mut HashMap<Uuid, &'a kg_core::runtime::stage_output::ObservedEntityProperties>,
    observation_uuid: Uuid,
    entity: &'a EntityNode,
    changes: &'a [PropertyChange],
    decision: Decision,
) -> Result<(), StageError> {
    let snapshot_uuid = entity.last_seen_snapshot_id.ok_or_else(|| {
        invalid(format!(
            "entity `{}` ({}) carries no observing snapshot",
            entity.name, entity.entity_type
        ))
    })?;
    let (snapshot_index, snapshot) = snapshots.get(&snapshot_uuid).copied().ok_or_else(|| {
        invalid(format!(
            "entity `{}` names snapshot {snapshot_uuid}, which is not in this batch",
            entity.name
        ))
    })?;
    let original = provided
        .remove(&observation_uuid)
        .ok_or_else(|| invalid("missing or reused original observation UUID".into()))?;
    if original.snapshot_uuid != snapshot_uuid || original.identity_hash != entity.identity_hash {
        return Err(invalid(
            "original observation does not match classified entity".into(),
        ));
    }
    by_chain
        .entry(entity.chain_id)
        .or_default()
        .push(Observation {
            provided: original,
            entity,
            changes,
            decision,
            snapshot_uuid,
            snapshot_index,
            kind: snapshot.snapshot_kind,
            membership: CollectionMembership::of_node(snapshot),
        });
    Ok(())
}

/// Apply one chain's observations in capture order and record the version
/// each snapshot actually observed.
/// Returns the final content of every version observed or written by this batch.
#[allow(clippy::too_many_arguments)]
fn linearize<'a>(
    chain: Uuid,
    observations: &[Observation<'a>],
    fk_exclusions: &HashMap<Uuid, Vec<String>>,
    order: usize,
    org: &str,
    ctx: &RuntimeContext,
    plan: &mut Plan,
    steps: &mut Vec<Step>,
    links: &mut Vec<GraphMutation>,
) -> Result<Vec<Head<'a>>, StageError> {
    // A chain new to the graph: whichever observation won the claim, the
    // earliest capture creates it and the rest continue from there.
    let created_here = observations.iter().any(|o| o.decision == Decision::Create);
    // Compatible partial observations at one instant describe one state, even
    // when separate snapshots supplied the fields. Original evidence stays separate.
    let mut supplied_by_time: HashMap<DateTime<Utc>, (usize, IndexMap<String, PropertyValue>)> =
        HashMap::new();
    for observation in observations {
        if observation.kind == SnapshotKind::Full {
            continue;
        }
        let (count, supplied) = supplied_by_time
            .entry(observation.entity.valid_from)
            .or_default();
        *count += 1;
        for (key, value) in &observation.provided.properties {
            if supplied.get(key).is_some_and(|previous| previous != value) {
                return Err(contradiction(
                    observation.entity,
                    "conflicting partial source properties at the same capture time",
                ));
            }
            supplied.insert(key.clone(), value.clone());
        }
        PropertyValue::validate_flat_paths(supplied).map_err(|_| {
            contradiction(
                observation.entity,
                "conflicting partial property paths at the same capture time",
            )
        })?;
    }
    supplied_by_time.retain(|_, (count, _)| *count > 1);
    let mut finalized = BTreeMap::new();
    let mut head: Option<Head> = None;
    let mut deleted_at: Option<DateTime<Utc>> = None;
    // The version a deletion of this batch tombstoned, for a later restoration.
    let mut tombstone: Option<(Uuid, u32)> = None;

    for (round, observation) in observations.iter().enumerate() {
        let entity = observation.entity;
        let new_uuid = observation.provided.observation_uuid;
        let mut new_entity = entity.clone();
        new_entity.uuid = new_uuid;
        let mut push = |rank: u8, mutation: GraphMutation| {
            steps.push(Step {
                round,
                rank,
                chain: order,
                mutation,
            })
        };
        if let Some(at) = deleted_at {
            if observation.decision == Decision::Deleted && entity.valid_from >= at {
                if entity.valid_from > at {
                    push(
                        RANK_DELETE,
                        GraphMutation::DeleteEntity {
                            chain_id: chain,
                            deleted_at: entity.valid_from,
                            deleted_by: entity.deleted_by.clone(),
                            reason: entity.deletion_reason.clone(),
                        },
                    );
                    deleted_at = Some(entity.valid_from);
                }
                plan.counts.entities_unchanged += 1;
                continue;
            }
            if entity.valid_from <= at {
                return Err(contradiction(
                    entity,
                    &format!(
                        "observed at {} after a source deletion at {at}",
                        entity.valid_from
                    ),
                ));
            }
            // A strictly newer observation restores the chain this batch
            // deleted, continuing it from the tombstoned version.
            let (previous, version) = tombstone.take().ok_or_else(|| {
                invalid(format!(
                    "chain {chain} was deleted in this batch without a recorded tombstone"
                ))
            })?;
            let version = crate::next_version(version, STAGE)?;
            let restored = new_uuid;
            let memberships = observation.memberships(&[]);
            let fresh = supplied_properties(
                observation,
                supplied_by_time
                    .get(&observation.entity.valid_from)
                    .filter(|_| observation.kind != SnapshotKind::Full)
                    .map(|(_, properties)| properties),
            );
            let fresh_hash = structural_hash_of(
                ctx,
                &entity.entity_type,
                &fresh,
                &observation.provided.version_exclusions,
            );
            let mut properties = entity_properties(
                entity,
                org,
                version,
                Some(previous),
                &fresh,
                fresh_hash,
                &memberships,
            );
            properties.insert("uuid".into(), restored.to_string().into());
            push(
                RANK_UPSERT_SUCCESSOR,
                GraphMutation::UpsertEntity {
                    uuid: restored,
                    properties,
                },
            );
            push(
                RANK_REPOINT,
                GraphMutation::RepointEntity {
                    previous_uuid: previous,
                    new_uuid: restored,
                    chain_id: chain,
                },
            );
            push(
                RANK_METADATA,
                metadata_mutation(
                    kg_core::profiles::evidence_contract(
                        ctx.run_schemas.as_deref(),
                        &entity.source,
                    )
                    .map_err(|e| invalid(e.to_string()))?,
                    entity,
                    restored,
                    Some(previous),
                    observation.kind,
                    fk_exclusions
                        .get(&observation.snapshot_uuid)
                        .cloned()
                        .unwrap_or_default(),
                ),
            );
            register_additional_keys(&mut push, entity, restored)?;
            plan.counts.entities_recreated += 1;
            head = Some(Head {
                uuid: restored,
                version,
                structural_hash: fresh_hash,
                properties: fresh,
                valid_from: entity.valid_from,
                in_batch: true,
                content_written: true,
                collections: memberships,
                entity,
            });
            deleted_at = None;
            links.push(GraphMutation::RecordObservation {
                uuid: Uuid::new_v5(&observation.snapshot_uuid, restored.as_bytes()),
                snapshot_uuid: observation.snapshot_uuid,
                entity_uuid: restored,
                entity_chain_id: chain,
                observed_at: entity.valid_from,
                reconciliations: observation.provided.reconciliations.clone(),
            });
            plan.counts.observations += 1;
            if let Some(head) = &head {
                finalized.insert(head.uuid, head.clone());
            }
            continue;
        }
        let metadata_previous = head.as_ref().map(|h| h.uuid).or_else(|| {
            (!created_here
                && matches!(
                    observation.decision,
                    Decision::NewVersion | Decision::Recreated
                ))
            .then_some(entity.previous_version_uuid)
            .flatten()
        });
        let observed: Uuid = match (&head, observation.decision) {
            (_, Decision::Deleted) => {
                match &head {
                    Some(h) if h.valid_from == entity.valid_from => {
                        return Err(contradiction(
                            entity,
                            "deleted and observed alive at the same capture time",
                        ));
                    }
                    Some(h) if !h.in_batch => {
                        plan.require(Precondition::LatestVersionIs {
                            chain_id: chain,
                            uuid: h.uuid,
                            version: h.version,
                        });
                        plan.require(Precondition::NotObservedAfter {
                            uuid: h.uuid,
                            observed_at: entity.valid_from,
                        });
                    }
                    Some(_) => {}
                    None if entity.deleted_at.is_some() => {
                        plan.require(Precondition::LatestDeletedVersionIs {
                            chain_id: chain,
                            uuid: entity.uuid,
                            restored_at: entity.valid_from,
                        });
                    }
                    None => {
                        plan.require(Precondition::LatestVersionIs {
                            chain_id: chain,
                            uuid: entity.uuid,
                            version: entity.version,
                        });
                        plan.require(Precondition::NotObservedAfter {
                            uuid: entity.uuid,
                            observed_at: entity.valid_from,
                        });
                    }
                }
                tombstone = Some(match &head {
                    Some(h) => (h.uuid, h.version),
                    None => (entity.uuid, entity.version),
                });
                push(
                    RANK_DELETE,
                    GraphMutation::DeleteEntity {
                        chain_id: chain,
                        deleted_at: entity.valid_from,
                        deleted_by: entity.deleted_by.clone(),
                        reason: entity.deletion_reason.clone(),
                    },
                );
                if head.is_none() && entity.deleted_at.is_some() {
                    plan.counts.entities_unchanged += 1;
                } else {
                    plan.counts.entities_deleted += 1;
                }
                deleted_at = Some(entity.valid_from);
                continue;
            }
            (_, Decision::Stale) => {
                // Older than the stored version: provenance only, no bookkeeping.
                plan.counts.entities_unchanged += 1;
                entity.uuid
            }
            (None, _) if created_here => {
                plan.require(Precondition::NoLiveVersionFor {
                    hashes: vec![entity.identity_hash.to_hex()],
                });
                let memberships = observation.memberships(&entity.collections);
                let combined = supplied_by_time
                    .get(&entity.valid_from)
                    .filter(|_| observation.kind != SnapshotKind::Full);
                let properties = combined.map_or_else(
                    || entity.all_properties.clone(),
                    |(_, properties)| supplied_properties(observation, Some(properties)),
                );
                let structural_hash = structural_hash_of(
                    ctx,
                    &entity.entity_type,
                    &properties,
                    &observation.provided.version_exclusions,
                );
                push(
                    RANK_CREATE,
                    GraphMutation::UpsertEntity {
                        uuid: new_uuid,
                        properties: entity_properties(
                            &new_entity,
                            org,
                            1,
                            None,
                            &properties,
                            structural_hash,
                            &memberships,
                        ),
                    },
                );
                plan.counts.entities_created += 1;
                head = Some(Head::written(
                    entity,
                    new_uuid,
                    1,
                    properties,
                    structural_hash,
                    memberships,
                ));
                new_uuid
            }
            (None, Decision::Create) => {
                return Err(invalid(format!(
                    "entity `{}` is marked new but its chain is not",
                    entity.name
                )));
            }
            (None, Decision::NewVersion) => {
                let previous = predecessor(entity)?;
                plan.require(Precondition::LatestVersionIs {
                    chain_id: chain,
                    uuid: previous,
                    version: entity.version - 1,
                });
                plan.require(Precondition::NotObservedAfter {
                    uuid: previous,
                    observed_at: entity.valid_from,
                });
                let memberships = observation.memberships(&entity.collections);
                push_successor(
                    &mut push,
                    entity,
                    new_uuid,
                    previous,
                    entity.version,
                    org,
                    &entity.all_properties,
                    entity.structural_hash,
                    &memberships,
                );
                plan.counts.entities_updated += 1;
                head = Some(Head::in_batch(
                    entity,
                    new_uuid,
                    entity.version,
                    memberships,
                ));
                new_uuid
            }
            (None, Decision::Recreated) => {
                let tombstone = predecessor(entity)?;
                // Resolution compared capture time with the tombstone it
                // read; the commit rechecks under the lock.
                plan.require(Precondition::LatestDeletedVersionIs {
                    chain_id: chain,
                    uuid: tombstone,
                    restored_at: entity.valid_from,
                });
                // A continued chain starts its memberships from this observation.
                let memberships = observation.memberships(&[]);
                push(
                    RANK_UPSERT_SUCCESSOR,
                    GraphMutation::UpsertEntity {
                        uuid: new_uuid,
                        properties: entity_properties(
                            &new_entity,
                            org,
                            entity.version,
                            Some(tombstone),
                            &entity.all_properties,
                            entity.structural_hash,
                            &memberships,
                        ),
                    },
                );
                push(
                    RANK_REPOINT,
                    GraphMutation::RepointEntity {
                        previous_uuid: tombstone,
                        new_uuid,
                        chain_id: chain,
                    },
                );
                plan.counts.entities_recreated += 1;
                head = Some(Head::in_batch(
                    entity,
                    new_uuid,
                    entity.version,
                    memberships,
                ));
                new_uuid
            }
            (None, Decision::Volatile) => {
                plan.require(Precondition::LatestVersionIs {
                    chain_id: chain,
                    uuid: entity.uuid,
                    version: entity.version,
                });
                plan.require(Precondition::NotObservedAfter {
                    uuid: entity.uuid,
                    observed_at: entity.valid_from,
                });
                push(
                    RANK_UPDATE,
                    GraphMutation::UpdateEntity {
                        uuid: entity.uuid,
                        properties: in_place_properties(
                            observation.changes,
                            entity.structural_hash,
                        )?,
                    },
                );
                push(RANK_OBSERVE, observe(chain, observation));
                plan.counts.entities_updated += 1;
                let mut updated =
                    Head::stored(entity, observation.memberships(&entity.collections));
                updated.content_written = true;
                head = Some(updated);
                entity.uuid
            }
            (None, Decision::Unchanged) => {
                plan.require(Precondition::LatestVersionIs {
                    chain_id: chain,
                    uuid: entity.uuid,
                    version: entity.version,
                });
                plan.require(Precondition::NotObservedAfter {
                    uuid: entity.uuid,
                    observed_at: entity.valid_from,
                });
                push(RANK_OBSERVE, observe(chain, observation));
                plan.counts.entities_unchanged += 1;
                head = Some(Head::stored(
                    entity,
                    observation.memberships(&entity.collections),
                ));
                entity.uuid
            }
            (Some(h), _) => {
                // A later observation is decided against the version before
                // it, not against the graph the resolver saw.
                let mut incoming = entity.clone();
                // One partial snapshot can supply complementary mentions of the same chain.
                // Their union is effective input; each provenance record keeps its own source map.
                let combined = supplied_by_time
                    .get(&observation.entity.valid_from)
                    .filter(|_| observation.kind != SnapshotKind::Full);
                incoming.all_properties =
                    supplied_properties(observation, combined.map(|(_, properties)| properties));
                let decision = classify_properties(
                    &incoming,
                    &h.properties,
                    observation.kind,
                    ctx,
                    &observation.provided.version_exclusions,
                );
                let structural_change = matches!(
                    decision,
                    kg_core::enums::VersioningDecision::NewVersion { .. }
                );
                if structural_change {
                    if h.valid_from == entity.valid_from {
                        let changed_paths: Vec<&str> = incoming
                            .all_properties
                            .iter()
                            .filter(|(key, value)| h.properties.get(*key) != Some(*value))
                            .map(|(key, _)| key.as_str())
                            .collect();
                        tracing::warn!(
                            entity_chain_id = %chain,
                            snapshot_uuid = %observation.snapshot_uuid,
                            ?changed_paths,
                            "same-time entity content conflict"
                        );
                        return Err(contradiction(
                            entity,
                            "two different contents captured at the same time",
                        ));
                    }
                    if !h.in_batch {
                        plan.require(Precondition::LatestVersionIs {
                            chain_id: chain,
                            uuid: h.uuid,
                            version: h.version,
                        });
                        plan.require(Precondition::NotObservedAfter {
                            uuid: h.uuid,
                            observed_at: entity.valid_from,
                        });
                    }
                    let version = crate::next_version(h.version, STAGE)?;
                    let memberships = observation.memberships(&h.collections);
                    let properties =
                        merge_partial(observation.kind, &incoming.all_properties, &h.properties);
                    let structural_hash = structural_hash_of(
                        ctx,
                        &entity.entity_type,
                        &properties,
                        &observation.provided.version_exclusions,
                    );
                    push_successor(
                        &mut push,
                        entity,
                        new_uuid,
                        h.uuid,
                        version,
                        org,
                        &properties,
                        structural_hash,
                        &memberships,
                    );
                    plan.counts.entities_updated += 1;
                    head = Some(Head::written(
                        entity,
                        new_uuid,
                        version,
                        properties,
                        structural_hash,
                        memberships,
                    ));
                    new_uuid
                } else {
                    let volatile_changes = match decision {
                        kg_core::enums::VersioningDecision::MergeInPlace { changed } => changed,
                        _ => Vec::new(),
                    };
                    if !h.in_batch {
                        plan.require(Precondition::LatestVersionIs {
                            chain_id: chain,
                            uuid: h.uuid,
                            version: h.version,
                        });
                        plan.require(Precondition::NotObservedAfter {
                            uuid: h.uuid,
                            observed_at: entity.valid_from,
                        });
                    }
                    let merged_properties =
                        merge_partial(observation.kind, &incoming.all_properties, &h.properties);
                    let merged_hash = structural_hash_of(
                        ctx,
                        &entity.entity_type,
                        &merged_properties,
                        &observation.provided.version_exclusions,
                    );
                    if volatile_changes.is_empty() {
                        plan.counts.entities_unchanged += 1;
                    } else {
                        push(
                            RANK_UPDATE,
                            GraphMutation::UpdateEntity {
                                uuid: h.uuid,
                                properties: in_place_properties(&volatile_changes, merged_hash)?,
                            },
                        );
                        plan.counts.entities_updated += 1;
                    }
                    push(RANK_OBSERVE, observe(chain, observation));
                    let observed = h.uuid;
                    let mut next = head.take().expect("head is present");
                    if !volatile_changes.is_empty() {
                        next.content_written = true;
                        next.structural_hash = merged_hash;
                    }
                    for change in &volatile_changes {
                        match change
                            .new_value
                            .as_ref()
                            .map(|v| serde_json::from_value::<PropertyValue>(v.clone()))
                            .transpose()
                            .map_err(|_| invalid("invalid typed property change".into()))?
                        {
                            Some(value) => {
                                next.properties.insert(change.property.clone(), value);
                            }
                            None => {
                                next.properties.shift_remove(&change.property);
                            }
                        }
                    }
                    next.valid_from = next.valid_from.max(entity.valid_from);
                    next.collections = observation.memberships(&next.collections);
                    head = Some(next);
                    observed
                }
            }
        };

        if observation.decision != Decision::Stale {
            register_additional_keys(&mut push, entity, observed)?;
        }
        if observation.decision != Decision::Stale {
            push(
                RANK_METADATA,
                metadata_mutation(
                    kg_core::profiles::evidence_contract(
                        ctx.run_schemas.as_deref(),
                        &entity.source,
                    )
                    .map_err(|e| invalid(e.to_string()))?,
                    entity,
                    observed,
                    metadata_previous.filter(|previous| *previous != observed),
                    observation.kind,
                    fk_exclusions
                        .get(&observation.snapshot_uuid)
                        .cloned()
                        .unwrap_or_default(),
                ),
            );
        }
        links.push(GraphMutation::RecordObservation {
            uuid: Uuid::new_v5(&observation.snapshot_uuid, observed.as_bytes()),
            snapshot_uuid: observation.snapshot_uuid,
            entity_uuid: observed,
            entity_chain_id: chain,
            observed_at: entity.valid_from,
            reconciliations: observation.provided.reconciliations.clone(),
        });
        plan.counts.observations += 1;
        if let Some(head) = &head {
            finalized.insert(head.uuid, head.clone());
        }
    }
    Ok(finalized.into_values().collect())
}

#[allow(clippy::too_many_arguments)]
fn push_successor(
    push: &mut impl FnMut(u8, GraphMutation),
    entity: &EntityNode,
    new_uuid: Uuid,
    previous: Uuid,
    version: u32,
    org: &str,
    properties: &IndexMap<String, PropertyValue>,
    structural_hash: u64,
    collections: &[CollectionMembership],
) {
    let mut successor = entity.clone();
    successor.uuid = new_uuid;
    push(
        RANK_SUPERSEDE,
        GraphMutation::SupersedeEntity {
            uuid: previous,
            chain_id: entity.chain_id,
            valid_to: entity.valid_from,
        },
    );
    push(
        RANK_UPSERT_SUCCESSOR,
        GraphMutation::UpsertEntity {
            uuid: new_uuid,
            properties: entity_properties(
                &successor,
                org,
                version,
                Some(previous),
                properties,
                structural_hash,
                collections,
            ),
        },
    );
    push(
        RANK_REPOINT,
        GraphMutation::RepointEntity {
            previous_uuid: previous,
            new_uuid,
            chain_id: entity.chain_id,
        },
    );
}
fn predecessor(entity: &EntityNode) -> Result<Uuid, StageError> {
    match entity.previous_version_uuid {
        Some(previous) if previous != entity.uuid && entity.version >= 2 => Ok(previous),
        _ => Err(invalid(format!(
            "successor version of `{}` ({}) has no distinct predecessor",
            entity.name, entity.entity_type
        ))),
    }
}
fn register_additional_keys(
    push: &mut impl FnMut(u8, GraphMutation),
    entity: &EntityNode,
    uuid: Uuid,
) -> Result<(), StageError> {
    if entity.additional_key_properties.is_empty() {
        return Ok(());
    }
    let mut hashes = entity.additional_identity_hashes().map_err(invalid)?;
    hashes.push(entity.identity_hash);
    let mut properties = GraphProperties::new();
    properties.insert(
        "identity_hashes".into(),
        serde_json::json!(hashes.iter().map(|h| h.to_hex()).collect::<Vec<_>>()),
    );
    properties.insert(
        "additional_key_properties".into(),
        serde_json::json!(serde_json::to_string(&entity.additional_key_properties)
            .map_err(|_| invalid("invalid additional keys".into()))?),
    );
    push(
        RANK_METADATA,
        GraphMutation::UpdateEntity { uuid, properties },
    );
    Ok(())
}
fn metadata_mutation(
    profile_contract: Option<String>,
    entity: &EntityNode,
    uuid: Uuid,
    previous_uuid: Option<Uuid>,
    kind: SnapshotKind,
    reference_exclusions: Vec<String>,
) -> GraphMutation {
    GraphMutation::ApplyEntityMetadata {
        profile_contract,
        reference_exclusions,
        uuid,
        previous_uuid,
        tags: entity.tags.clone(),
        labels: entity.labels.clone(),
        replace: kind == SnapshotKind::Full,
        observed_at: entity.valid_from,
    }
}
fn observe(chain: Uuid, observation: &Observation<'_>) -> GraphMutation {
    let entity = observation.entity;
    GraphMutation::ObserveEntity {
        chain_id: chain,
        observed_at: entity.valid_from,
        sync_generation: entity.sync_generation,
        snapshot_id: entity.last_seen_snapshot_id,
        collection: observation.membership.clone(),
    }
}
fn supplied_properties(
    observation: &Observation<'_>,
    combined: Option<&IndexMap<String, PropertyValue>>,
) -> IndexMap<String, PropertyValue> {
    let mut properties = combined.unwrap_or(&observation.provided.properties).clone();
    let entity = observation.entity;
    for key in entity
        .primary_key_properties
        .iter()
        .chain(entity.additional_key_properties.iter().flatten())
    {
        if !properties.contains_key(key) {
            if let Some(value) = entity.all_properties.get(key) {
                properties.insert(key.clone(), value.clone());
            }
        }
    }
    properties
}
fn in_place_properties(
    changes: &[PropertyChange],
    structural_hash: u64,
) -> Result<GraphProperties, StageError> {
    let mut properties = change_properties(changes)?;
    properties.insert("structural_hash".into(), structural_hash.to_string().into());
    Ok(properties)
}
fn change_properties(changes: &[PropertyChange]) -> Result<GraphProperties, StageError> {
    let mut props = GraphProperties::new();
    for change in changes {
        let value = change
            .new_value
            .as_ref()
            .map(|v| serde_json::from_value::<PropertyValue>(v.clone()))
            .transpose()
            .map_err(|_| invalid("invalid typed property change".into()))?;
        kg_core::traits::property_codec::write_property(
            &mut props,
            &change.property,
            value.as_ref(),
        );
    }
    Ok(props)
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn entity_properties(
    entity: &EntityNode,
    org: &str,
    version: u32,
    previous: Option<Uuid>,
    properties: &IndexMap<String, PropertyValue>,
    structural_hash: u64,
    collections: &[CollectionMembership],
) -> GraphProperties {
    let mut props = GraphProperties::new();
    props.insert("uuid".into(), entity.uuid.to_string().into());
    props.insert("chain_id".into(), entity.chain_id.to_string().into());
    props.insert("org_id".into(), org.into());
    props.insert("entity_type".into(), entity.entity_type.as_str().into());
    props.insert("name".into(), entity.name.as_str().into());
    props.insert("namespace".into(), entity.namespace.as_str().into());
    let identity = entity.identity_hash.to_hex();
    props.insert(
        "hash_version".into(),
        format!("{identity}:{version}").into(),
    );
    props.insert("identity_hash".into(), identity.into());
    props.insert("is_latest".into(), true.into());
    props.insert("version".into(), version.into());
    set_uuid(&mut props, "previous_version_uuid", previous);
    props.insert("valid_from".into(), entity.valid_from.to_rfc3339().into());
    props.insert("last_seen_at".into(), entity.valid_from.to_rfc3339().into());
    if let Some(generation) = entity.sync_generation {
        props.insert("sync_generation".into(), generation.into());
    }
    if !collections.is_empty() {
        let (members, generations) = CollectionMembership::to_properties(collections);
        props.insert(MEMBERS_PROPERTY.into(), members);
        props.insert(GENERATIONS_PROPERTY.into(), generations);
    }
    set_uuid(
        &mut props,
        "first_seen_snapshot_id",
        entity.first_seen_snapshot_id,
    );
    set_uuid(
        &mut props,
        "last_seen_snapshot_id",
        entity.last_seen_snapshot_id,
    );
    set_string(&mut props, "source", &entity.source);
    set_string(&mut props, "extracted_by", &entity.extracted_by);
    set_string(
        &mut props,
        "resolved_by",
        entity.resolved_by.as_deref().unwrap_or(""),
    );
    props.insert("lifecycle".into(), entity.lifecycle.to_string().into());
    set_string(
        &mut props,
        "summary",
        entity.summary.as_deref().unwrap_or(""),
    );
    if structural_hash != 0 {
        props.insert("structural_hash".into(), structural_hash.to_string().into());
    }
    if entity.needs_llm_review {
        props.insert("needs_llm_review".into(), true.into());
    }
    {
        props.insert(
            "primary_key_properties".into(),
            Value::Array(
                entity
                    .primary_key_properties
                    .iter()
                    .map(|k| Value::String(k.clone()))
                    .collect(),
            ),
        );
    }
    // Typed tokens of every key component, from the same canonicalization as the
    // reference target index, so stored targets are found by exact typed value.
    props.insert(
        "key_values".into(),
        serde_json::json!(
            kg_core::runtime::stage_output::RelationshipTarget::from_node(entity)
                .key_value_tokens()
        ),
    );
    props.insert("labels".into(), serde_json::json!(entity.labels));
    for (key, value) in &entity.tags {
        props.insert(format!("{TAG_PROPERTY_PREFIX}{key}"), value.as_str().into());
    }
    for (key, value) in properties {
        kg_core::traits::property_codec::write_property(&mut props, key, Some(value));
    }
    props
}
fn snapshot_properties(snapshot: &SnapshotNode, store_content: bool) -> GraphProperties {
    let mut props = GraphProperties::new();
    if store_content {
        if let Some(content) = &snapshot.content {
            props.insert("content".into(), content.as_str().into());
        }
    }
    props.insert("namespace".into(), snapshot.namespace.as_str().into());
    props.insert("name".into(), snapshot.name.as_str().into());
    props.insert("data_type".into(), snapshot.data_type.to_string().into());
    if let Some(description) = &snapshot.source_description {
        props.insert("source_description".into(), description.as_str().into());
    }
    props.insert(
        "snapshot_kind".into(),
        snapshot.snapshot_kind.to_string().into(),
    );
    if let Some(generation) = snapshot.sync_generation {
        props.insert("sync_generation".into(), generation.into());
    }
    props.insert("complete".into(), snapshot.complete.into());
    if let Some(collection) = &snapshot.collection {
        props.insert("collection_key".into(), collection.key.as_str().into());
        props.insert(
            "collection_relationships_complete".into(),
            collection.relationships_complete.into(),
        );
    }
    props.insert("source".into(), snapshot.source.as_str().into());
    props.insert(
        "captured_at".into(),
        snapshot.captured_at.to_rfc3339().into(),
    );
    props.insert("created_at".into(), snapshot.created_at.to_rfc3339().into());
    if !snapshot.labels.is_empty() {
        props.insert(
            "labels".into(),
            Value::Array(
                snapshot
                    .labels
                    .iter()
                    .map(|l| Value::String(l.clone()))
                    .collect(),
            ),
        );
    }
    for (key, value) in &snapshot.tags {
        props.insert(format!("{TAG_PROPERTY_PREFIX}{key}"), value.as_str().into());
    }
    props
}
#[track_caller]
fn contradiction(entity: &EntityNode, detail: &str) -> StageError {
    tracing::warn!(
        entity_chain_id = %entity.chain_id,
        entity_uuid = %entity.uuid,
        "contradictory observations of one entity"
    );
    invalid(format!(
        "contradictory observations of `{}` ({}) in one batch: {detail}",
        entity.name, entity.entity_type
    ))
}

fn embedding_target(
    head: &Head<'_>,
    observations: &[Observation<'_>],
    settings: &EmbeddingSettings,
    org: &str,
) -> Option<PlannedEmbedding> {
    let keys = embedding::key_properties(
        &head.entity.primary_key_properties,
        &head.entity.additional_key_properties,
    );
    let labels = head.entity.effective_labels();
    let text = embedding::representation(
        &embedding::EntityText {
            entity_type: &head.entity.entity_type,
            name: &head.entity.name,
            summary: head.entity.summary.as_deref(),
            properties: &head.properties,
            key_properties: &keys,
            labels: &labels,
        },
        &settings.entity_fields,
    );
    let hash = embedding::content_hash(&text);
    let reuse = std::iter::once(head.entity)
        .chain(observations.iter().map(|o| o.entity))
        .filter_map(|entity| entity.embedding.as_deref())
        .find(|vector| {
            vector.matches(settings, &hash)
                && embedding::validate_vectors(settings, 1, std::slice::from_ref(&vector.values))
                    .is_ok()
        })
        .cloned();
    if !head.content_written && reuse.is_some() {
        return None;
    }
    let properties = entity_properties(
        head.entity,
        org,
        head.version,
        None,
        &head.properties,
        head.structural_hash,
        &head.collections,
    );
    Some(PlannedEmbedding {
        uuid: head.uuid,
        namespace: head.entity.namespace.clone(),
        text,
        content_hash: hash,
        text_version: settings.text_version.clone(),
        reuse,
        write: kg_core::runtime::stage_output::PlannedEmbeddingWrite::EntityVersion {
            expected_properties: entity_embedding_state(&properties),
            labels,
            primary_key_properties: head.entity.primary_key_properties.clone(),
            additional_key_properties: head.entity.additional_key_properties.clone(),
        },
    })
}

#[cfg(test)]
mod tests;
