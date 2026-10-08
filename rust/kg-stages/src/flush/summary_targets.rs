//! Receipt targets include neighbors whose accepted facts change with this batch.
use super::mutation_plan::{invalid, Plan};
use kg_core::{
    errors::StageError,
    runtime::{stage_output::FlushWork, RuntimeContext},
    traits::{
        relationship_timeline::{self, IncidentVersionState},
        EdgeLookup, Precondition,
    },
};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) async fn collect(
    work: &FlushWork,
    plan: &mut Plan,
    ctx: &RuntimeContext,
    skipped: &mut Vec<kg_core::pipeline::output::SkippedSummary>,
) -> Result<Vec<Uuid>, StageError> {
    let mut chains = BTreeSet::new();
    match work {
        FlushWork::Nodes(batch) => {
            for nodes in [
                &batch.nodes_to_create,
                &batch.nodes_unchanged,
                &batch.nodes_recreated,
                &batch.nodes_deleted,
                &batch.nodes_stale,
            ] {
                chains.extend(nodes.iter().map(|node| node.chain_id));
            }
            for nodes in [&batch.nodes_new_version, &batch.nodes_volatile] {
                chains.extend(nodes.iter().map(|node| node.entity.chain_id));
            }
            for merge in batch.chains_merged.iter() {
                chains.extend([merge.loser_chain_id, merge.winner_chain_id]);
            }
            let anchors: Vec<_> = chains.iter().copied().collect();
            let mut history = BTreeMap::new();
            for chunk in anchors.chunks(1) {
                let rows = match ctx
                    .graph
                    .find_edges(
                        &ctx.org_id,
                        &EdgeLookup::VersionsByEndpointChains {
                            chain_ids: chunk.to_vec(),
                        },
                    )
                    .await
                {
                    Ok(rows) => rows,
                    Err(kg_core::errors::BackendError::RelationshipHistoryLimit { .. }) => {
                        skipped.push(kg_core::pipeline::output::SkippedSummary {
                            chain_id: chunk[0],
                            reason: "incident_history_above_summary_limit".into(),
                        });
                        continue;
                    }
                    Err(error) => {
                        return Err(StageError::StepFailed {
                            stage: "mutation_planning".into(),
                            step: "summary_targets".into(),
                            retriable: error.is_transient(),
                            cause: error.to_string(),
                        })
                    }
                };
                if rows.len() > relationship_timeline::MAX_VERSIONS {
                    skipped.push(kg_core::pipeline::output::SkippedSummary {
                        chain_id: chunk[0],
                        reason: "incident_history_above_summary_limit".into(),
                    });
                    continue;
                }
                let mut returned = BTreeSet::new();
                for edge in rows {
                    if !returned.insert(edge.uuid)
                        || (!chunk.contains(&edge.source_chain_id)
                            && !chunk.contains(&edge.target_chain_id))
                    {
                        return Err(invalid("invalid summary target history".into()));
                    }
                    let state = IncidentVersionState {
                        source_chain_id: edge.source_chain_id,
                        target_chain_id: edge.target_chain_id,
                        properties: relationship_timeline::state(&edge.stored),
                    };
                    chains.extend([edge.source_chain_id, edge.target_chain_id]);
                    if let Some(previous) = history.insert(edge.uuid, state.clone()) {
                        if previous != state {
                            return Err(StageError::CommitRejected {
                                stage: "mutation_planning".into(),
                                message: "summary target history changed during planning".into(),
                            });
                        }
                    }
                }
            }
            for chain_id in anchors {
                if skipped.iter().any(|item| item.chain_id == chain_id) {
                    continue;
                }
                let versions: Vec<_> = history
                    .values()
                    .filter(|state| {
                        state.source_chain_id == chain_id || state.target_chain_id == chain_id
                    })
                    .cloned()
                    .collect();
                relationship_timeline::validate_incident(chain_id, &versions)
                    .map_err(|error| invalid(error.to_string()))?;
                plan.require(Precondition::IncidentHistoryIs { chain_id, versions });
            }
        }
        FlushWork::Relationships(batch) => {
            for edge in batch.observed.iter() {
                chains.extend([edge.source_chain_id, edge.target_chain_id]);
            }
            for pair in &batch.baseline.pairs {
                chains.extend([pair.source_chain_id, pair.target_chain_id]);
            }
            for relation in &batch.baseline.relations {
                chains.insert(relation.source_chain_id);
                chains.extend(
                    relation
                        .versions
                        .iter()
                        .map(|version| version.target_chain_id),
                );
                for edge in &relation.live {
                    chains.extend([edge.source_chain_id, edge.target_chain_id]);
                }
            }
            for edge in &batch.baseline.ended {
                chains.extend([edge.source_chain_id, edge.target_chain_id]);
            }
            for versions in batch.contradiction_timelines.values() {
                for edge in versions {
                    chains.extend([edge.source_chain_id, edge.target_chain_id]);
                }
            }
        }
        FlushWork::Reconciliation(batch) => {
            chains.extend(
                batch
                    .entities
                    .iter()
                    .chain(&batch.released)
                    .map(|entity| entity.chain_id),
            );
            for edge in &batch.edges {
                chains.extend([edge.source_chain_id, edge.target_chain_id]);
            }
            for versions in batch.incident_timelines.values() {
                for edge in versions {
                    chains.extend([edge.source_chain_id, edge.target_chain_id]);
                }
            }
        }
    }
    Ok(chains
        .into_iter()
        .filter(|chain| !skipped.iter().any(|item| item.chain_id == *chain))
        .collect())
}
