//! Partition oversized writes at entity/relationship ownership boundaries.
//!
//! Pages are a storage protocol, not independent ingestion receipts. All original
//! preconditions are checked before the first mutation; the adapter must fence
//! intervening graph writes and publish the parent receipt only after every page.
use super::{GraphMutation, MutationBatch, Precondition};
use crate::errors::BackendError;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

pub const MAX_PLAN_BYTES: usize = 64 * 1024 * 1024;

#[derive(Default)]
struct Components(HashMap<String, String>);
impl Components {
    fn root(&mut self, key: &str) -> String {
        // Path halving: repoint each node at its grandparent while walking up, so
        // repeated unions on a hot owner key (a source with many edges) stay
        // near-flat instead of costing O(k) per lookup. The returned root is
        // unchanged by compression, so page grouping and ordering are preserved.
        let mut x = key.to_owned();
        loop {
            let Some(parent) = self.0.get(&x).cloned() else {
                break;
            };
            if parent == x {
                break;
            }
            let grand = self
                .0
                .get(&parent)
                .cloned()
                .unwrap_or_else(|| parent.clone());
            self.0.insert(x.clone(), grand.clone());
            x = grand;
        }
        x
    }
    fn join(&mut self, keys: &[String]) {
        if let Some(first) = keys.first() {
            let root = self.root(first);
            for key in &keys[1..] {
                let other = self.root(key);
                if other != root {
                    self.0.insert(other, root.clone());
                }
            }
        }
    }
}
fn node(id: Uuid) -> String {
    format!("node:{id}")
}
fn edge(id: Uuid) -> String {
    format!("edge:{id}")
}
fn field(properties: &super::GraphProperties, name: &str) -> Option<Uuid> {
    properties.get(name)?.as_str()?.parse().ok()
}

/// Return fully validated atomic pages. Unknown/complex follow-up operations stay
/// together rather than silently weakening their atomicity. The fast path preserves
/// existing ordering and cost for batches that already fit.
pub fn partition(batch: &MutationBatch) -> Result<Vec<MutationBatch>, BackendError> {
    if batch.validate().is_ok() {
        return Ok(vec![batch.clone()]);
    }
    if serde_json::to_vec(batch)
        .map_err(|e| BackendError::Serialization(e.to_string()))?
        .len()
        > MAX_PLAN_BYTES
    {
        return Err(BackendError::Query(
            "frozen commit plan exceeds 64 MiB".into(),
        ));
    }
    // Chain merges, splits and community publication have graph-wide side effects.
    // They remain indivisible instead of guessing dependencies from their IDs.
    if batch.mutations.iter().any(|m| {
        matches!(
            m,
            GraphMutation::MergeChains { .. }
                | GraphMutation::SplitChain { .. }
                | GraphMutation::AssertCommunityState { .. }
                | GraphMutation::BeginCommunityGeneration { .. }
                | GraphMutation::StageCommunityPartition { .. }
                | GraphMutation::PublishCommunityGeneration { .. }
                | GraphMutation::UpdateCommunities { .. }
                | GraphMutation::SetDerivedSummary { .. }
                | GraphMutation::ClearDerivedSummary { .. }
                | GraphMutation::SetSagaSummary { .. }
        )
    }) {
        return Err(BackendError::Query(
            "oversized merge or derived-publication batch is indivisible".into(),
        ));
    }
    let empty = || MutationBatch {
        org_id: batch.org_id.clone(),
        batch: batch.batch,
        fingerprint: batch.fingerprint.clone(),
        preconditions: batch
            .preconditions
            .iter()
            .filter(|guard| {
                matches!(
                    guard,
                    Precondition::OwnsCollection { .. } | Precondition::RuleRevisionIs { .. }
                )
            })
            .cloned()
            .collect(),
        mutations: vec![],
        result: json!({}),
    };
    // Guard validation is itself atomic and precedes all graph mutations. Receipt
    // recovery remains bounded; oversized indivisible evidence fails before writes.
    let mut header = empty();
    header.result = batch.result.clone();
    header.validate()?;
    let mut components = Components::default();
    let mut mutable_nodes = std::collections::HashSet::new();
    for mutation in &batch.mutations {
        match mutation {
            GraphMutation::UpsertEntity { uuid, properties } => {
                mutable_nodes.insert(node(*uuid));
                if let Some(chain) = field(properties, "chain_id") {
                    mutable_nodes.insert(node(chain));
                }
            }
            GraphMutation::UpdateEntity { uuid, .. }
            | GraphMutation::ApplyEntityMetadata { uuid, .. } => {
                mutable_nodes.insert(node(*uuid));
            }
            GraphMutation::DeleteEntity { chain_id, .. }
            | GraphMutation::ObserveEntity { chain_id, .. }
            | GraphMutation::SupersedeEntity { chain_id, .. }
            | GraphMutation::RepointEntity { chain_id, .. } => {
                mutable_nodes.insert(node(*chain_id));
            }
            _ => {}
        }
    }
    // Map changed stored UUIDs to chains before attaching incident dependencies.
    for guard in &batch.preconditions {
        if let Precondition::LatestVersionIs { chain_id, uuid, .. }
        | Precondition::LatestDeletedVersionIs { chain_id, uuid, .. } = guard
        {
            components.join(&[node(*chain_id), node(*uuid)]);
            if mutable_nodes.contains(&node(*uuid)) {
                mutable_nodes.insert(node(*chain_id));
            }
        }
    }
    fn link_history(
        components: &mut Components,
        source: Uuid,
        target: Uuid,
        properties: &super::GraphProperties,
        mutable_nodes: &std::collections::HashSet<String>,
    ) {
        let mut keys = vec![format!("owner:{source}")];
        for name in ["uuid", "chain_id", "previous_version_uuid"] {
            if let Some(id) = field(properties, name) {
                keys.push(edge(id));
            }
        }
        for id in [source, target] {
            if mutable_nodes.contains(&node(id)) {
                keys.push(node(id));
            }
        }
        components.join(&keys);
    }
    for guard in &batch.preconditions {
        match guard {
            Precondition::LatestVersionIs { chain_id, uuid, .. }
            | Precondition::LatestDeletedVersionIs { chain_id, uuid, .. } => {
                components.join(&[node(*chain_id), node(*uuid)])
            }
            Precondition::EdgeHeadIs {
                source_chain_id,
                target_chain_id,
                uuid,
                chain_id,
                ..
            } => {
                let mut keys = vec![
                    format!("owner:{source_chain_id}"),
                    edge(*uuid),
                    edge(*chain_id),
                ];
                for id in [source_chain_id, target_chain_id] {
                    if mutable_nodes.contains(&node(*id)) {
                        keys.push(node(*id));
                    }
                }
                components.join(&keys);
            }
            Precondition::IncidentHistoryIs { versions, .. }
            | Precondition::IncidentTimelineIs { versions, .. } => {
                for state in versions {
                    link_history(
                        &mut components,
                        state.source_chain_id,
                        state.target_chain_id,
                        &state.properties,
                        &mutable_nodes,
                    );
                }
            }
            Precondition::ReferenceOwnerTimelineIs { owner, versions } => {
                for state in versions {
                    link_history(
                        &mut components,
                        state.source_chain_id,
                        state.target_chain_id,
                        &state.properties,
                        &mutable_nodes,
                    );
                    if let Some(id) = field(&state.properties, "uuid") {
                        components.join(&[format!("owner:{}", owner.chain_id), edge(id)]);
                    }
                }
            }
            Precondition::RelationshipTimelineIs {
                source_chain_id,
                target_chain_id,
                versions,
            } => {
                for properties in versions {
                    link_history(
                        &mut components,
                        *source_chain_id,
                        *target_chain_id,
                        properties,
                        &mutable_nodes,
                    );
                }
            }
            Precondition::RelationTimelineIs {
                source_chain_id,
                versions,
                ..
            } => {
                for state in versions {
                    link_history(
                        &mut components,
                        *source_chain_id,
                        state.target_chain_id,
                        &state.properties,
                        &mutable_nodes,
                    );
                }
            }
            Precondition::LiveEdgesForPairAre {
                source_chain_id,
                uuids,
                ..
            }
            | Precondition::LiveEdgesForRelationAre {
                source_chain_id,
                uuids,
                ..
            } => {
                for id in uuids {
                    components.join(&[format!("owner:{source_chain_id}"), edge(*id)]);
                }
            }
            Precondition::LiveEdgesForReferenceOwnerAre { owner, uuids } => {
                for id in uuids {
                    components.join(&[format!("owner:{}", owner.chain_id), edge(*id)]);
                }
            }
            Precondition::LiveIncidentEdgesAre { chain_id, uuids } => {
                for id in uuids {
                    components.join(&[node(*chain_id), edge(*id)]);
                }
            }
            _ => {}
        }
    }
    let mut keys = Vec::with_capacity(batch.mutations.len());
    for mutation in &batch.mutations {
        use GraphMutation::*;
        let mut group = match mutation {
            UpsertSnapshot { uuid, .. } => vec![format!("snapshot:{uuid}")],
            UpsertEntity { uuid, properties } => {
                let mut ids = vec![node(*uuid)];
                for key in ["chain_id", "previous_version_uuid"] {
                    if let Some(id) = field(properties, key) {
                        ids.push(node(id));
                    }
                }
                if let Some(hash) = properties.get("identity_hash").and_then(|v| v.as_str()) {
                    ids.push(format!("hash:{hash}"));
                }
                if let Some(hashes) = properties.get("identity_hashes").and_then(|v| v.as_array()) {
                    ids.extend(
                        hashes
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(|v| format!("hash:{v}")),
                    );
                }
                ids
            }
            ReleaseMembership { uuid, .. }
            | SetEmbedding { uuid, .. }
            | SetEntityVersionEmbedding { uuid, .. } => vec![node(*uuid)],
            UpdateEntity { uuid, properties } => {
                let mut ids = vec![node(*uuid)];
                if let Some(hash) = properties.get("identity_hash").and_then(|v| v.as_str()) {
                    ids.push(format!("hash:{hash}"));
                }
                if let Some(hashes) = properties.get("identity_hashes").and_then(|v| v.as_array()) {
                    ids.extend(
                        hashes
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(|v| format!("hash:{v}")),
                    );
                }
                ids
            }
            ApplyEntityMetadata {
                uuid,
                previous_uuid,
                ..
            } => {
                let mut ids = vec![node(*uuid)];
                if let Some(previous) = previous_uuid {
                    ids.push(node(*previous));
                }
                ids
            }
            SupersedeEntity { uuid, chain_id, .. } => vec![node(*uuid), node(*chain_id)],
            DeleteEntity { chain_id, .. } | ObserveEntity { chain_id, .. } => vec![node(*chain_id)],
            RepointEntity {
                previous_uuid,
                new_uuid,
                chain_id,
            } => vec![node(*previous_uuid), node(*new_uuid), node(*chain_id)],
            MergeChains {
                loser_chain_id,
                winner_chain_id,
                ..
            } => vec![node(*loser_chain_id), node(*winner_chain_id)],
            SplitChain {
                split_chain_id,
                from_chain_id,
                ..
            } => vec![node(*split_chain_id), node(*from_chain_id)],
            RecordObservation {
                entity_uuid,
                entity_chain_id,
                ..
            } => vec![node(*entity_uuid), node(*entity_chain_id)],
            UpsertEdge {
                uuid,
                source_chain_id,
                target_chain_id,
                properties,
            } => {
                let mut ids = vec![edge(*uuid), format!("owner:{source_chain_id}")];
                for name in ["chain_id", "previous_version_uuid"] {
                    if let Some(id) = field(properties, name) {
                        ids.push(edge(id));
                    }
                }
                for id in [source_chain_id, target_chain_id] {
                    if mutable_nodes.contains(&node(*id)) {
                        ids.push(node(*id));
                    }
                }
                ids
            }
            UpdateEdge { uuid, .. }
            | CancelEdge { uuid, .. }
            | SetRelationshipEmbedding { uuid, .. } => vec![edge(*uuid)],
            RecordUnresolvedReferences {
                source_chain_id, ..
            } => vec![format!("owner:{source_chain_id}")],
            // Association/publication and audit writes run only after their graph
            // dependencies. A large inseparable follow-up remains an explicit error.
            _ => vec!["finalization".into()],
        };
        components.join(&group);
        keys.push(group.remove(0));
    }
    let mut groups: BTreeMap<usize, Vec<(usize, GraphMutation)>> = BTreeMap::new();
    let mut first = HashMap::new();
    for (index, (key, mutation)) in keys.iter().zip(&batch.mutations).enumerate() {
        let root = components.root(key);
        let position = *first.entry(root).or_insert(index);
        groups
            .entry(position)
            .or_default()
            .push((index, mutation.clone()));
    }
    // Shared snapshots must exist before provenance links; association/audit is last.
    let mut ordered: Vec<_> = groups.into_iter().collect();
    ordered.sort_by_key(|(i, items)| {
        (
            if matches!(items[0].1, GraphMutation::UpsertSnapshot { .. }) {
                0
            } else if components.root(&keys[*i]) == "finalization" {
                2
            } else {
                1
            },
            *i,
        )
    });
    let mut pages = vec![header];
    // Validate large guard sets before any data pages. The storage revision fence
    // makes each subsequent guard page reject an intervening graph write.
    let guards: Vec<_> = batch
        .preconditions
        .iter()
        .filter(|guard| {
            !matches!(
                guard,
                Precondition::OwnsCollection { .. } | Precondition::RuleRevisionIs { .. }
            )
        })
        .collect();
    let mut offset = 0;
    while offset < guards.len() {
        let mut low = offset;
        let mut high = guards.len();
        let mut accepted = None;
        while low < high {
            let end = low + (high - low).div_ceil(2);
            let mut page = empty();
            page.preconditions
                .extend(guards[offset..end].iter().map(|guard| (*guard).clone()));
            if page.validate().is_ok() {
                low = end;
                accepted = Some(page);
            } else {
                high = end - 1;
            }
        }
        if low == offset {
            return Err(BackendError::Query(
                "indivisible commit guard exceeds a budget or is invalid".into(),
            ));
        }
        pages.push(accepted.expect("validated guard page"));
        offset = low;
    }
    let mut start = 0;
    while start < ordered.len() {
        // Find the largest fitting prefix. Revalidating the whole growing page
        // after every entity would serialize quadratic amounts of inventory data.
        let mut low = start;
        let mut high = ordered.len();
        let mut accepted = None;
        while low < high {
            let end = low + (high - low).div_ceil(2);
            let mut candidate = empty();
            let mut members: Vec<_> = ordered[start..end]
                .iter()
                .flat_map(|(_, group)| group.iter().cloned())
                .collect();
            // Keep the original order within each page, including compiler bulk
            // runs. Atomic grouping decides page membership, not statement order.
            members.sort_by_key(|(index, _)| *index);
            candidate.mutations = members.into_iter().map(|(_, mutation)| mutation).collect();
            if candidate.validate().is_ok() {
                low = end;
                accepted = Some(candidate);
            } else {
                high = end - 1;
            }
        }
        if low == start {
            let mut unit = empty();
            unit.mutations = ordered[start]
                .1
                .iter()
                .map(|(_, mutation)| mutation.clone())
                .collect();
            let error = unit.validate().expect_err("non-fitting atomic component");
            return Err(BackendError::Query(format!(
                "indivisible commit component exceeds a budget or is invalid: {error}"
            )));
        }
        pages.push(accepted.expect("a fitting prefix was validated"));
        start = low;
    }
    let mut parent = batch.clone();
    parent.mutations.clear();
    parent.preconditions.clear();
    // Include both recovery copies and envelope overhead before embeddings or
    // any page can be dispatched. The adapter persists these exact boundaries.
    let bytes = serde_json::to_vec(&pages)
        .map_err(|e| BackendError::Serialization(e.to_string()))?
        .len()
        .saturating_add(
            serde_json::to_vec(&parent)
                .map_err(|e| BackendError::Serialization(e.to_string()))?
                .len(),
        )
        .saturating_add(256);
    if bytes > MAX_PLAN_BYTES {
        return Err(BackendError::Query(
            "frozen commit pages and recovery exceed 64 MiB".into(),
        ));
    }
    Ok(pages)
}
