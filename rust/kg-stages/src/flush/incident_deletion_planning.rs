//! Close or cancel incident relationship intervals without deleting their history.
use super::mutation_plan::{closed_edge, invalid, Plan, STAGE};
use chrono::{DateTime, Utc};
use kg_core::traits::relationship_timeline::{self, IncidentVersionState};
use kg_core::{
    errors::{BackendError, StageError},
    runtime::RuntimeContext,
    traits::{EdgeLookup, EntityLookup, GraphMutation, GraphProperties, Precondition},
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
pub(crate) async fn plan_source_deletions(
    plan: &mut Plan,
    evidence: &HashMap<(Uuid, DateTime<Utc>), Uuid>,
    ctx: &RuntimeContext,
) -> Result<(), StageError> {
    let mut deletions = std::collections::BTreeMap::<Uuid, DateTime<Utc>>::new();
    for mutation in &plan.mutations {
        if let GraphMutation::DeleteEntity {
            chain_id,
            deleted_at,
            ..
        } = mutation
        {
            deletions
                .entry(*chain_id)
                .and_modify(|at| *at = (*at).min(*deleted_at))
                .or_insert(*deleted_at);
        }
    }
    if deletions.is_empty() {
        return Ok(());
    }
    let chains: Vec<_> = deletions.keys().copied().collect();
    let mut incident = std::collections::BTreeMap::new();
    let mut history = std::collections::BTreeMap::new();
    let mut stored = Vec::new();
    let read_error = |error: BackendError| StageError::StepFailed {
        stage: STAGE.into(),
        step: "source_deletion_edges".into(),
        retriable: error.is_transient(),
        cause: error.to_string(),
    };
    for chunk in chains.chunks(kg_core::traits::graph_reads::MAX_LOOKUP_KEYS) {
        stored.extend(
            ctx.graph
                .find_entities(
                    &ctx.org_id,
                    &EntityLookup::LatestByChain {
                        chain_ids: chunk.to_vec(),
                    },
                )
                .await
                .map_err(read_error)?,
        );
        let mut returned = HashSet::new();
        let history_rows = ctx
            .graph
            .find_edges(
                &ctx.org_id,
                &EdgeLookup::VersionsByEndpointChains {
                    chain_ids: chunk.to_vec(),
                },
            )
            .await
            .map_err(read_error)?;
        if history_rows.len() > relationship_timeline::MAX_VERSIONS {
            return Err(invalid("incident history exceeds the lookup limit".into()));
        }
        for edge in history_rows {
            if !chunk.contains(&edge.source_chain_id) && !chunk.contains(&edge.target_chain_id) {
                return Err(invalid(
                    "incident history lookup returned an unrelated relationship".into(),
                ));
            }
            if !returned.insert(edge.uuid) {
                return Err(invalid(
                    "incident history lookup returned a duplicate relationship".into(),
                ));
            }
            let state = IncidentVersionState {
                source_chain_id: edge.source_chain_id,
                target_chain_id: edge.target_chain_id,
                properties: relationship_timeline::state(&edge.stored),
            };
            if !history.contains_key(&edge.uuid)
                && history.len() >= relationship_timeline::MAX_VERSIONS
            {
                return Err(invalid(
                    "combined incident history exceeds the planning limit".into(),
                ));
            }
            if let Some(previous) = history.insert(edge.uuid, state.clone()) {
                if previous != state {
                    return Err(StageError::CommitRejected {
                        stage: STAGE.into(),
                        message: "incident relationship changed between lookup batches".into(),
                    });
                }
            }
        }
        for edge in ctx
            .graph
            .find_edges(
                &ctx.org_id,
                &EdgeLookup::LiveByEndpointChains {
                    chain_ids: chunk.to_vec(),
                },
            )
            .await
            .map_err(read_error)?
        {
            incident.insert(edge.uuid, edge);
        }
    }
    let mut adjacency = HashMap::<Uuid, Vec<Uuid>>::new();
    for edge in incident.values() {
        for chain in [edge.source_chain_id, edge.target_chain_id] {
            if deletions.contains_key(&chain) {
                let uuids = adjacency.entry(chain).or_default();
                // A self-loop belongs to its endpoint only once.
                if uuids.last() != Some(&edge.uuid) {
                    uuids.push(edge.uuid);
                }
            }
        }
    }
    let mut stored_chains = HashSet::new();
    for entity in stored {
        if !deletions.contains_key(&entity.chain_id) || !stored_chains.insert(entity.chain_id) {
            return Err(invalid(
                "source deletion lookup returned unrelated or duplicate entity heads".into(),
            ));
        }
        let versions: Vec<_> = history
            .values()
            .filter(|version| {
                version.source_chain_id == entity.chain_id
                    || version.target_chain_id == entity.chain_id
            })
            .cloned()
            .collect();
        relationship_timeline::validate_incident(entity.chain_id, &versions).map_err(read_error)?;
        plan.require(Precondition::IncidentTimelineIs {
            chain_id: entity.chain_id,
            versions,
        });
        plan.require(Precondition::LiveIncidentEdgesAre {
            chain_id: entity.chain_id,
            uuids: adjacency.remove(&entity.chain_id).unwrap_or_default(),
        });
    }
    for edge in history.values() {
        let (chain, at) = [edge.source_chain_id, edge.target_chain_id]
            .into_iter()
            .filter_map(|chain| deletions.get(&chain).map(|at| (chain, *at)))
            .min_by_key(|(chain, at)| (*at, *chain))
            .ok_or_else(|| invalid("incident history returned an unrelated relationship".into()))?;
        let snapshot = evidence
            .get(&(chain, at))
            .copied()
            .ok_or_else(|| invalid("source deletion is missing capture evidence".into()))?;
        plan_interval_deletion(plan, &edge.properties, at, Some(snapshot), None)?;
    }
    Ok(())
}
pub(crate) fn plan_interval_deletion(
    plan: &mut Plan,
    properties: &GraphProperties,
    at: DateTime<Utc>,
    snapshot: Option<Uuid>,
    context: Option<kg_core::models::CancellationContext>,
) -> Result<(), StageError> {
    let time = |key: &str| -> Result<Option<DateTime<Utc>>, StageError> {
        properties
            .get(key)
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                    .map(|value| value.with_timezone(&Utc))
                    .ok_or_else(|| invalid(format!("incident relationship has invalid {key}")))
            })
            .transpose()
    };
    if time("cancelled_at")?.is_some() || time("deleted_at")?.is_some() {
        return Ok(());
    }
    let start = time("valid_from")?
        .ok_or_else(|| invalid("incident relationship has no validity start".into()))?;
    let end = [time("invalid_at")?, time("valid_to")?]
        .into_iter()
        .flatten()
        .min();
    if end.is_some_and(|end| end <= at || end <= start) {
        return Ok(());
    }
    if [time("last_seen_at")?, time("last_transition_at")?]
        .into_iter()
        .flatten()
        .any(|capture| capture > at)
    {
        return Err(StageError::CommitRejected {
            stage: STAGE.into(),
            message: "relationship was observed or changed after deletion capture".into(),
        });
    }
    let uuid = properties
        .get("uuid")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| invalid("incident relationship has invalid UUID".into()))?;
    if start > at {
        plan.mutations.push(GraphMutation::CancelEdge {
            uuid,
            cancelled_at: at,
            cancellation_snapshot_id: snapshot,
            cancellation_context: context,
            observed_at: at,
        });
    } else {
        plan.mutations.push(GraphMutation::UpdateEdge {
            uuid,
            properties: closed_edge(at),
        });
    }
    plan.counts.edges_invalidated += 1;
    Ok(())
}

#[cfg(test)]
mod tests;
