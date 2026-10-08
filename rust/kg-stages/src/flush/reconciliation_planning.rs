//! Plan complete-collection membership changes under captured ownership and history guards.
use super::{
    incident_deletion_planning::plan_interval_deletion,
    mutation_plan::{invalid, Plan},
};
use kg_core::{
    errors::StageError,
    runtime::stage_output::ReconciliationBatch,
    traits::{relationship_timeline, BatchIdentity, GraphMutation, Precondition},
};
use std::collections::HashMap;
pub(crate) fn plan_reconciliation(
    batch: &ReconciliationBatch,
    identity: BatchIdentity,
) -> Result<Plan, StageError> {
    let mut plan = Plan::default();
    let reason = format!(
        "absent from generation {} of collection {}",
        batch.scan.generation, batch.scan.collection
    );
    let mut captured = HashMap::new();
    for (chain_id, versions) in &batch.incident_timelines {
        relationship_timeline::validate_incident(*chain_id, versions)
            .map_err(|error| invalid(error.to_string()))?;
        plan.require(Precondition::IncidentTimelineIs {
            chain_id: *chain_id,
            versions: versions.clone(),
        });
        for version in versions {
            let uuid = version.properties["uuid"]
                .as_str()
                .ok_or_else(|| invalid("incident timeline has no UUID".into()))?;
            if let Some(previous) = captured.insert(uuid.to_owned(), version) {
                if previous != version {
                    return Err(invalid(
                        "reconciliation captured inconsistent incident histories".into(),
                    ));
                }
            }
        }
    }
    for (chain, owner) in &batch.relationship_owners {
        if *chain != owner.chain_id {
            return Err(invalid(
                "relationship ownership baseline has a different source chain".into(),
            ));
        }
        plan.require(Precondition::SoleCollectionOwnerIs {
            uuid: owner.uuid,
            collection: batch.scan.collection.clone(),
        });
        plan.require(Precondition::LatestVersionIs {
            chain_id: owner.chain_id,
            uuid: owner.uuid,
            version: owner.version,
        });
    }
    for released in &batch.released {
        plan.require(Precondition::CollectionMembershipsAre {
            uuid: released.uuid,
            memberships: released.collections.clone(),
        });
        plan.require(Precondition::LatestVersionIs {
            chain_id: released.chain_id,
            uuid: released.uuid,
            version: released.version,
        });
        plan.mutations.push(GraphMutation::ReleaseMembership {
            uuid: released.uuid,
            collection: batch.scan.collection.clone(),
        });
        plan.counts.memberships_released += 1;
    }
    for stale in &batch.entities {
        let versions = batch
            .incident_timelines
            .get(&stale.chain_id)
            .ok_or_else(|| {
                invalid("reconciliation is missing an incident timeline baseline".into())
            })?;
        relationship_timeline::validate_incident(stale.chain_id, versions)
            .map_err(|error| invalid(error.to_string()))?;
        plan.require(Precondition::IncidentTimelineIs {
            chain_id: stale.chain_id,
            versions: versions.clone(),
        });
        plan.require(Precondition::SoleCollectionOwnerIs {
            uuid: stale.uuid,
            collection: batch.scan.collection.clone(),
        });
        plan.require(Precondition::LiveIncidentEdgesAre {
            chain_id: stale.chain_id,
            uuids: batch
                .live_incident
                .get(&stale.chain_id)
                .cloned()
                .ok_or_else(|| {
                    invalid("reconciliation is missing its live incident baseline".into())
                })?,
        });
        plan.require(Precondition::LatestVersionIs {
            chain_id: stale.chain_id,
            uuid: stale.uuid,
            version: stale.version,
        });
        plan.require(Precondition::NotObservedAfter {
            uuid: stale.uuid,
            observed_at: batch.captured_at,
        });
        plan.mutations.push(GraphMutation::DeleteEntity {
            chain_id: stale.chain_id,
            deleted_at: batch.captured_at,
            deleted_by: Some("sweep".into()),
            reason: Some(reason.clone()),
        });
        plan.counts.entities_deleted += 1;
    }
    let mut affected = std::collections::BTreeMap::new();
    for entity in &batch.entities {
        for version in &batch.incident_timelines[&entity.chain_id] {
            let uuid = version.properties["uuid"]
                .as_str()
                .ok_or_else(|| invalid("incident timeline has no UUID".into()))?;
            if let Some(previous) = affected.insert(uuid.to_owned(), &version.properties) {
                if previous != &version.properties {
                    return Err(invalid(
                        "reconciliation captured inconsistent incident histories".into(),
                    ));
                }
            }
        }
    }
    for stale in &batch.edges {
        let covered = captured
            .get(&stale.uuid.to_string())
            .is_some_and(|version| {
                version.properties == stale.properties
                    && version.source_chain_id == stale.source_chain_id
                    && version.target_chain_id == stale.target_chain_id
            });
        if !covered {
            return Err(invalid(
                "stale relationship is missing its incident timeline baseline".into(),
            ));
        }
        let incident_deletion = batch.entities.iter().any(|entity| {
            entity.chain_id == stale.source_chain_id || entity.chain_id == stale.target_chain_id
        });
        if !incident_deletion
            && !batch
                .relationship_owners
                .contains_key(&stale.source_chain_id)
        {
            return Err(invalid(
                "standalone relationship deletion has no source ownership baseline".into(),
            ));
        }
        affected.insert(stale.uuid.to_string(), &stale.properties);
    }
    for properties in affected.values() {
        plan_interval_deletion(
            &mut plan,
            properties,
            batch.captured_at,
            None,
            Some(kg_core::models::CancellationContext::Batch { batch: identity }),
        )?;
    }
    Ok(plan)
}
