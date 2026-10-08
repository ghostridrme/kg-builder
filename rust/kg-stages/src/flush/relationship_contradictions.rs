//! Translate semantic assessments into scoped, guarded interval changes.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use kg_core::errors::StageError;
use kg_core::models::{EntityEdge, RelationshipOrigin, RelationshipTarget};
use kg_core::runtime::stage_output::{
    ConnectorScope, PairBaseline, RelationshipAssessmentDecision, RelationshipBatch,
};
use kg_core::traits::{GraphMutation, GraphProperties, Precondition};
use uuid::Uuid;

use super::{close, closed_edge, contradiction, invalid, load_timeline, overlaps, Plan, Version};

pub(super) fn load_candidates(
    batch: &RelationshipBatch,
    plan: &mut Plan,
    chains: &mut BTreeMap<Uuid, Vec<Version>>,
    owners: &mut BTreeMap<Uuid, ConnectorScope>,
) -> Result<(), StageError> {
    for (anchor, versions) in &batch.contradiction_timelines {
        kg_core::traits::relationship_timeline::validate_incident(*anchor, versions)
            .map_err(|_| invalid("invalid contradiction incident timeline".into()))?;
        plan.require(Precondition::IncidentTimelineIs {
            chain_id: *anchor,
            versions: versions.clone(),
        });
        let mut pairs = BTreeMap::<(Uuid, Uuid), Vec<GraphProperties>>::new();
        for version in versions {
            pairs
                .entry((version.source_chain_id, version.target_chain_id))
                .or_default()
                .push(version.properties.clone());
        }
        for ((source_chain_id, target_chain_id), versions) in pairs {
            load_timeline(
                &PairBaseline {
                    source_chain_id,
                    target_chain_id,
                    versions,
                    live: vec![],
                },
                chains,
                owners,
            )?;
        }
    }
    Ok(())
}

pub(super) fn validate_assessments(batch: &RelationshipBatch) -> Result<(), StageError> {
    let observations: HashSet<_> = batch.observed.iter().map(|edge| edge.uuid).collect();
    let mut pairs = HashSet::new();
    for assessment in batch.relationship_assessments.iter() {
        let (kind, candidate) = match assessment.candidate {
            RelationshipTarget::StoredVersion { uuid } => (0, uuid),
            RelationshipTarget::PriorObservation { observation_uuid } => {
                if !observations.contains(&observation_uuid) {
                    return Err(invalid(
                        "semantic candidate observation is outside this batch".into(),
                    ));
                }
                (1, observation_uuid)
            }
        };
        if candidate.is_nil()
            || !observations.contains(&assessment.observation_uuid)
            || !pairs.insert((assessment.observation_uuid, kind, candidate))
            || assessment
                .protected_properties
                .iter()
                .any(|key| key.trim().is_empty())
        {
            return Err(invalid("invalid or duplicate semantic assessment".into()));
        }
    }
    Ok(())
}

pub(super) struct Changes {
    older: Vec<(Uuid, Version, bool)>,
    bound_by: Option<Uuid>,
}

pub(super) fn prepare(
    batch: &RelationshipBatch,
    edge: &mut EntityEdge,
    scope: &ConnectorScope,
    chains: &BTreeMap<Uuid, Vec<Version>>,
    owners: &BTreeMap<Uuid, ConnectorScope>,
    planned: &HashMap<Uuid, Uuid>,
) -> Result<Changes, StageError> {
    let mut changes = Changes {
        older: vec![],
        bound_by: None,
    };
    if batch.relationship_assessments.iter().any(|assessment| {
        assessment.observation_uuid == edge.uuid
            && assessment.decision == RelationshipAssessmentDecision::Contradiction
    }) {
        if let Some(canonical) = chains
            .get(&edge.chain_id)
            .into_iter()
            .flatten()
            .find(|version| {
                version.cancelled_at.is_none()
                    && version.scope.as_ref() == Some(scope)
                    && version.same_content(edge)
                    && version
                        .latest_observation
                        .is_none_or(|latest| edge.last_seen_at.is_some_and(|at| at >= latest))
                    && version.valid_from <= edge.valid_from
                    && version.ended_at.is_none_or(|end| edge.valid_from < end)
            })
        {
            // Re-observation preserves the fact's effective start; capture is not a new interval.
            edge.valid_from = canonical.valid_from;
            if let Some(end) = canonical.ended_at {
                edge.valid_to = Some(edge.valid_to.map_or(end, |incoming| incoming.min(end)));
            }
        }
    }
    let mut seen = HashSet::new();
    for assessment in batch
        .relationship_assessments
        .iter()
        .filter(|a| a.observation_uuid == edge.uuid)
    {
        if assessment.decision != RelationshipAssessmentDecision::Contradiction {
            continue;
        }
        let uuid = match assessment.candidate {
            RelationshipTarget::StoredVersion { uuid } => uuid,
            RelationshipTarget::PriorObservation { observation_uuid } => {
                *planned.get(&observation_uuid).ok_or_else(|| {
                    invalid("semantic contradiction references an unapplied observation".into())
                })?
            }
        };
        if !seen.insert(uuid) {
            continue;
        }
        let (chain_id, candidate) = chains
            .iter()
            .find_map(|(id, versions)| versions.iter().find(|v| v.uuid == uuid).map(|v| (*id, v)))
            .ok_or_else(|| {
                invalid("semantic contradiction target is absent from guarded history".into())
            })?;
        if edge.origin != RelationshipOrigin::Fact
            || candidate.origin != RelationshipOrigin::Fact
            || edge.chain_id == chain_id
            || candidate.scope.as_ref() != Some(scope)
            || owners
                .get(&edge.chain_id)
                .is_some_and(|owner| owner != scope)
            || (edge.source_chain_id != candidate.source_chain_id
                && edge.target_chain_id != candidate.target_chain_id)
            || edge
                .identity_hash
                .as_ref()
                .is_some_and(|hash| candidate.identity_hash.as_ref() == Some(hash))
            || edge
                .time_evidence
                .as_ref()
                .is_some_and(|time| time.end_only())
            || assessment.protected_properties.iter().any(|key| {
                edge.all_properties
                    .get(key)
                    .is_none_or(|value| candidate.all_properties.get(key) != Some(value))
            })
        {
            return Err(invalid(
                "semantic contradiction violates identity or producer authority".into(),
            ));
        }
        let shared_anchor = if edge.source_chain_id == candidate.source_chain_id {
            edge.source_chain_id
        } else {
            edge.target_chain_id
        };
        if !batch.contradiction_timelines.contains_key(&shared_anchor) {
            return Err(invalid(
                "semantic contradiction has no incident history fence".into(),
            ));
        }
        if !overlaps(edge.valid_from, edge.valid_to, candidate) {
            continue;
        }
        let provisional_same_capture =
            !candidate.stored && candidate.latest_observation == edge.last_seen_at;
        if provisional_same_capture
            && !same_snapshot_dated_pair(batch, edge, &assessment.candidate, candidate)
        {
            return Err(contradiction(
                "same-capture semantic history requires dated evidence from one snapshot".into(),
            ));
        }
        if candidate.valid_from == edge.valid_from {
            return Err(contradiction(
                "semantic contradictions have the same effective start".into(),
            ));
        }
        if candidate.valid_from > edge.valid_from {
            if edge.valid_to.is_none_or(|end| candidate.valid_from < end) {
                edge.valid_to = Some(candidate.valid_from);
                changes.bound_by = Some(uuid);
            }
        } else {
            changes
                .older
                .push((chain_id, candidate.clone(), provisional_same_capture));
        }
    }
    // A later contradictory interval can make another assessment disjoint.
    changes
        .older
        .retain(|(_, version, _)| overlaps(edge.valid_from, edge.valid_to, version));
    Ok(changes)
}

fn same_snapshot_dated_pair(
    batch: &RelationshipBatch,
    edge: &EntityEdge,
    target: &RelationshipTarget,
    candidate: &Version,
) -> bool {
    let RelationshipTarget::PriorObservation { observation_uuid } = target else {
        return false;
    };
    let Some(prior) = batch
        .observed
        .iter()
        .find(|prior| prior.uuid == *observation_uuid)
    else {
        return false;
    };
    let dated = |value: &EntityEdge, start| {
        value.time_evidence.as_ref().is_some_and(|time| {
            time.validate().is_ok()
                && value.last_seen_snapshot_id == Some(time.snapshot_id)
                && value.last_seen_at == Some(time.captured_at)
                && time.start.as_ref().is_some_and(|bound| bound.at == start)
        })
    };
    prior.last_seen_snapshot_id == edge.last_seen_snapshot_id
        && prior.last_seen_at == edge.last_seen_at
        && dated(prior, candidate.valid_from)
        && dated(edge, edge.valid_from)
}

#[derive(serde::Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
enum ClosureEvidence {
    SemanticContradiction {
        snapshot_id: Uuid,
        counterpart_version_uuid: Uuid,
        captured_at: DateTime<Utc>,
    },
}

pub(super) fn apply(
    plan: &mut Plan,
    chains: &mut BTreeMap<Uuid, Vec<Version>>,
    changes: Changes,
    applied: Uuid,
    edge: &EntityEdge,
    captured_at: DateTime<Utc>,
) -> Result<(), StageError> {
    let incoming = chains
        .get(&edge.chain_id)
        .into_iter()
        .flatten()
        .find(|v| v.uuid == applied)
        .ok_or_else(|| invalid("semantic observation has no materialized version".into()))?;
    // Stale observations can identify an unchanged version without applying it.
    if incoming.latest_observation != Some(captured_at) {
        return Ok(());
    }
    let start = incoming.valid_from;
    let end = incoming.ended_at;
    if changes.older.is_empty() && changes.bound_by.is_none() {
        return Ok(());
    }
    if start != edge.valid_from || end != edge.valid_to {
        return Err(invalid(
            "semantic observation changed interval during planning".into(),
        ));
    }
    let snapshot_id = edge
        .last_seen_snapshot_id
        .ok_or_else(|| invalid("semantic closure has no evidence snapshot".into()))?;
    let audit = |counterpart_version_uuid| {
        let evidence = ClosureEvidence::SemanticContradiction {
            snapshot_id,
            counterpart_version_uuid,
            captured_at,
        };
        [(
            "closure_evidence".into(),
            serde_json::json!(evidence).to_string().into(),
        )]
        .into_iter()
        .collect()
    };
    if let Some(counterpart) = changes.bound_by {
        plan.mutations.push(GraphMutation::UpdateEdge {
            uuid: applied,
            properties: audit(counterpart),
        });
    }
    for (chain_id, previous, provisional_same_capture) in changes.older {
        let version = chains
            .get_mut(&chain_id)
            .and_then(|versions| versions.iter_mut().find(|v| v.uuid == previous.uuid))
            .ok_or_else(|| invalid("semantic candidate disappeared during planning".into()))?;
        if !overlaps(start, end, version) {
            continue;
        }
        if provisional_same_capture
            && !version.stored
            && version.latest_observation == Some(captured_at)
        {
            // Both dated observations belong to this snapshot; finish the newly planned interval.
            let mut properties = closed_edge(start);
            properties.insert("last_transition_at".into(), captured_at.to_rfc3339().into());
            plan.mutations.push(GraphMutation::UpdateEdge {
                uuid: version.uuid,
                properties,
            });
        } else {
            close(plan, version, start, captured_at)?;
        }
        plan.mutations.push(GraphMutation::UpdateEdge {
            uuid: version.uuid,
            properties: audit(applied),
        });
        version.ended_at = Some(start);
        version.latest_observation = Some(captured_at);
        version.closed = true;
        plan.counts.edges_invalidated += 1;
    }
    Ok(())
}
