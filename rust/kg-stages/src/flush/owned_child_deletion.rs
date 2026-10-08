//! Cascade only explicit ownership; shared and unowned children remain live.
use super::mutation_plan::{invalid, Plan};
use chrono::{DateTime, Utc};
use kg_core::{
    errors::StageError,
    models::{EntityEdge, PropertyValue},
    runtime::RuntimeContext,
    traits::{
        relationship_timeline, EdgeLookup, EdgeRecord, EntityLookup, GraphMutation, Precondition,
    },
};
use std::collections::{BTreeMap, HashMap, HashSet};
use uuid::Uuid;

fn endpoints(edge: &EdgeRecord) -> Option<(Uuid, Uuid)> {
    if edge.stored.get("discovered_by")?.as_str()? != "sub_entity_rule" {
        return None;
    }
    match edge.stored.get("prop_child_parent_endpoint")?.as_str()? {
        "source" => Some((edge.source_chain_id, edge.target_chain_id)),
        "target" => Some((edge.target_chain_id, edge.source_chain_id)),
        _ => None,
    }
}

async fn edges(
    ctx: &RuntimeContext,
    chain: Uuid,
    history: bool,
) -> Result<Vec<EdgeRecord>, StageError> {
    let lookup = if history {
        EdgeLookup::VersionsByEndpointChains {
            chain_ids: vec![chain],
        }
    } else {
        EdgeLookup::LiveByEndpointChains {
            chain_ids: vec![chain],
        }
    };
    ctx.graph
        .find_edges(&ctx.org_id, &lookup)
        .await
        .map_err(|error| StageError::StepFailed {
            stage: "mutation_planning".into(),
            step: "owned_child_deletion".into(),
            retriable: error.is_transient(),
            cause: error.to_string(),
        })
}

pub(super) async fn plan(
    plan: &mut Plan,
    evidence: &mut HashMap<(Uuid, DateTime<Utc>), Uuid>,
    pending: &[EntityEdge],
    ctx: &RuntimeContext,
) -> Result<(), StageError> {
    let deadline = std::time::Duration::from_millis(ctx.context_settings.read_timeout_ms);
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(StageError::Cancelled { stage: "mutation_planning".into() }),
        result = tokio::time::timeout(deadline, collect(plan, evidence, pending, ctx)) => result.unwrap_or_else(|_| Err(StageError::StepFailed {
            stage: "mutation_planning".into(), step: "owned_child_deletion".into(), retriable: true, cause: "owned child lookup timed out".into(),
        })),
    }
}

async fn collect(
    plan: &mut Plan,
    evidence: &mut HashMap<(Uuid, DateTime<Utc>), Uuid>,
    pending: &[EntityEdge],
    ctx: &RuntimeContext,
) -> Result<(), StageError> {
    let mut deleting = BTreeMap::new();
    for mutation in &plan.mutations {
        if let GraphMutation::DeleteEntity {
            chain_id,
            deleted_at,
            ..
        } = mutation
        {
            deleting.insert(*chain_id, *deleted_at);
        }
    }
    let mut inspected = HashSet::new();
    loop {
        let frontier: Vec<_> = deleting
            .keys()
            .filter(|id| !inspected.contains(*id))
            .copied()
            .collect();
        if frontier.is_empty() {
            return Ok(());
        }
        if deleting.len() > relationship_timeline::MAX_VERSIONS {
            return Err(invalid("owned child cascade exceeds planning limit".into()));
        }
        let mut candidates = BTreeMap::new();
        for parent in frontier {
            inspected.insert(parent);
            for edge in edges(ctx, parent, false).await? {
                if let Some((owner, child)) = endpoints(&edge) {
                    if owner == parent
                        && owner != child
                        && edge
                            .stored
                            .get("prop_child_owned")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                    {
                        candidates.insert(child, parent);
                    }
                }
            }
        }
        for (child, parent) in candidates {
            if deleting.contains_key(&child) {
                continue;
            }
            // A parent admitted in this chunk may not have committed its child links yet.
            if pending.iter().any(|edge| {
                let owner = match edge.all_properties.get("child_parent_endpoint") {
                    Some(PropertyValue::String(s))
                        if s == "source" && edge.target_chain_id == child =>
                    {
                        Some(edge.source_chain_id)
                    }
                    Some(PropertyValue::String(s))
                        if s == "target" && edge.source_chain_id == child =>
                    {
                        Some(edge.target_chain_id)
                    }
                    _ => None,
                };
                owner.is_some_and(|owner| !deleting.contains_key(&owner))
            }) {
                continue;
            }
            let incident = edges(ctx, child, false).await?;
            let mut at = deleting[&parent];
            let mut evidence_parent = parent;
            let mut shared = false;
            for edge in &incident {
                if let Some((owner, target)) = endpoints(edge) {
                    if target != child {
                        continue;
                    }
                    match deleting.get(&owner) {
                        Some(time)
                            if edge
                                .stored
                                .get("prop_child_owned")
                                .and_then(serde_json::Value::as_bool)
                                == Some(true) =>
                        {
                            if *time > at {
                                at = *time;
                                evidence_parent = owner;
                            }
                        }
                        _ => shared = true,
                    }
                } else if edge
                    .stored
                    .get("discovered_by")
                    .and_then(serde_json::Value::as_str)
                    == Some("sub_entity_rule")
                {
                    // Older links have no explicit ownership direction.
                    shared = true;
                }
            }
            if shared {
                continue;
            }
            let history = edges(ctx, child, true).await?;
            let versions = history
                .into_iter()
                .map(|edge| relationship_timeline::IncidentVersionState {
                    source_chain_id: edge.source_chain_id,
                    target_chain_id: edge.target_chain_id,
                    properties: relationship_timeline::state(&edge.stored),
                })
                .collect::<Vec<_>>();
            relationship_timeline::validate_incident(child, &versions)
                .map_err(|e| invalid(e.to_string()))?;
            let records = ctx
                .graph
                .find_entities(
                    &ctx.org_id,
                    &EntityLookup::LatestByChain {
                        chain_ids: vec![child],
                    },
                )
                .await
                .map_err(|error| StageError::StepFailed {
                    stage: "mutation_planning".into(),
                    step: "owned_child_deletion".into(),
                    retriable: error.is_transient(),
                    cause: error.to_string(),
                })?;
            let Some(record) = records.first() else {
                return Err(invalid("owned child has no current head".into()));
            };
            if records.len() != 1 || record.chain_id != child {
                return Err(invalid("invalid owned child head".into()));
            }
            if record.deleted_at.is_some() {
                continue;
            }
            let snapshot = evidence
                .get(&(evidence_parent, at))
                .copied()
                .ok_or_else(|| invalid("owned child deletion has no capture evidence".into()))?;
            plan.require(Precondition::LiveIncidentEdgesAre {
                chain_id: child,
                uuids: incident.iter().map(|edge| edge.uuid).collect(),
            });
            plan.require(Precondition::IncidentHistoryIs {
                chain_id: child,
                versions,
            });
            plan.require(Precondition::LatestVersionIs {
                chain_id: child,
                uuid: record.uuid,
                version: record.version,
            });
            plan.require(Precondition::NotObservedAfter {
                uuid: record.uuid,
                observed_at: at,
            });
            plan.mutations.push(GraphMutation::DeleteEntity {
                chain_id: child,
                deleted_at: at,
                deleted_by: Some("owned_parent".into()),
                reason: Some("all owning parents deleted".into()),
            });
            plan.mutations.push(GraphMutation::RecordObservation {
                uuid: Uuid::new_v5(&snapshot, record.uuid.as_bytes()),
                snapshot_uuid: snapshot,
                entity_uuid: record.uuid,
                entity_chain_id: child,
                observed_at: at,
                reconciliations: Vec::new(),
            });
            plan.counts.entities_deleted += 1;
            plan.counts.observations += 1;
            evidence.insert((child, at), snapshot);
            deleting.insert(child, at);
        }
    }
}
