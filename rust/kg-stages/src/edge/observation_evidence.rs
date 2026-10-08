//! Current source evidence attached to resolved graph identities.
use std::collections::{HashMap, HashSet};

use kg_core::{
    errors::StageError, models::EntityNode, runtime::stage_output::NodeResolutionOutput,
};
use uuid::Uuid;

pub(crate) struct CurrentObservation {
    pub snapshot_uuid: Uuid,
    pub node: EntityNode,
}

/// Stored properties may identify an endpoint, but cannot establish a fresh relationship.
pub(crate) fn current_observations(
    resolution: &NodeResolutionOutput,
    org_id: &str,
    stage: &str,
) -> Result<Vec<CurrentObservation>, StageError> {
    let invalid = |message: &str| StageError::StateValidation {
        stage: stage.into(),
        message: message.into(),
    };
    let mut snapshots = HashMap::new();
    for snapshot in resolution.snapshot_nodes.iter() {
        if snapshot.uuid.is_nil()
            || snapshot.org_id != org_id
            || snapshots.insert(snapshot.uuid, snapshot).is_some()
        {
            return Err(invalid("invalid or duplicate observation snapshot"));
        }
    }
    let mut originals = HashMap::new();
    for record in resolution.observed_properties.iter() {
        if record.observation_uuid.is_nil()
            || originals.insert(record.observation_uuid, record).is_some()
        {
            return Err(invalid(
                "invalid or duplicate original observation metadata",
            ));
        }
        if !snapshots.contains_key(&record.snapshot_uuid) {
            return Err(invalid("original observation has no snapshot"));
        }
    }
    let mut seen = HashSet::new();
    resolution
        .live_observations()
        .into_iter()
        .map(|(observation_uuid, entity)| {
            if !seen.insert(observation_uuid) {
                return Err(invalid("duplicate resolved observation"));
            }
            if entity.org_id != org_id {
                return Err(invalid("resolved observation scope mismatch"));
            }
            let original = originals
                .get(&observation_uuid)
                .ok_or_else(|| invalid("missing current observation properties"))?;
            let mut node = entity.clone();
            node.all_properties = original.properties.clone();
            node.last_seen_snapshot_id = Some(original.snapshot_uuid);
            Ok(CurrentObservation {
                snapshot_uuid: original.snapshot_uuid,
                node,
            })
        })
        .collect()
}
