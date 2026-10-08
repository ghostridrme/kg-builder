//! Freeze observation identities and context before registration and extraction.
use chrono::{DateTime, Utc};
use kg_core::{
    errors::{BackendError, PipelineError},
    models::IngestionInput,
    runtime::{
        history::{self, SnapshotEvidenceRequest},
        saga::*,
        RuntimeContext,
    },
    saga::{SagaRead, SagaReadResult, ThreadReference},
};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

fn invalid(message: impl Into<String>) -> PipelineError {
    PipelineError::StateValidation {
        stage: "observation_admission".into(),
        message: message.into(),
    }
}
fn backend(error: BackendError) -> PipelineError {
    PipelineError::StepExecution {
        stage: "observation_admission".into(),
        step: "read".into(),
        cause: "scoped observation admission read failed".into(),
        retriable: error.is_transient(),
    }
}

pub(crate) async fn prepare(
    ctx: &RuntimeContext,
    inputs: &[IngestionInput],
    captured_default: DateTime<Utc>,
    chunk_size: usize,
) -> Result<RunObservationManifest, PipelineError> {
    if inputs.len() > MAX_OBSERVATIONS || chunk_size == 0 {
        return Err(invalid("observation limit exceeded"));
    }
    for input in inputs {
        input
            .validate_request(&ctx.org_id)
            .map_err(|error| invalid(error.to_string()))?;
    }
    let mut estimated_bytes = 0usize;
    let mut estimated_refs = 0usize;
    for input in inputs {
        let association_bytes = input
            .saga()
            .map(|s| serde_json::to_vec(s).map(|v| v.len()))
            .transpose()
            .map_err(|_| invalid("cannot encode Saga association"))?
            .unwrap_or(0);
        let refs = match input {
            IngestionInput::Fresh(input)
                if ctx.policy.for_source(&input.source).extraction
                    != kg_core::policy::ExtractionMode::Heuristic =>
            {
                if input.previous_snapshot_uuids.is_empty() && input.saga.is_some() {
                    ctx.context_settings.max_records
                } else {
                    input.previous_snapshot_uuids.len()
                }
            }
            _ => 0,
        };
        estimated_refs = estimated_refs.saturating_add(refs);
        estimated_bytes = estimated_bytes
            .saturating_add(512 + input.namespace().len() + association_bytes + refs * 160);
    }
    if estimated_refs > MAX_CONTEXT_REFERENCES || estimated_bytes > MAX_MANIFEST_BYTES {
        return Err(invalid("request context manifest exceeds admission bounds"));
    }
    let _permit = ctx
        .semaphore
        .acquire()
        .await
        .map_err(|_| invalid("observation admission closed"))?;
    let mut manifest = RunObservationManifest::default();
    let mut sagas: HashMap<(String, String), (Uuid, String)> = HashMap::new();
    for input in inputs {
        let mut entry = match input {
            IngestionInput::Fresh(input) => FrozenObservation {
                snapshot_uuid: Uuid::new_v4(),
                namespace: input.namespace.clone(),
                created_at: captured_default,
                captured_at: input.captured_at.unwrap_or(captured_default),
                kind: FrozenObservationKind::Fresh,
                saga: None,
                history: vec![],
            },
            IngestionInput::Existing(input) => {
                let request = SnapshotEvidenceRequest {
                    namespace: input.namespace.clone(),
                    ids: vec![input.snapshot_uuid],
                    captured_before: DateTime::<Utc>::MAX_UTC,
                    max_bytes: ctx.context_settings.max_stored_bytes,
                };
                let mut records = ctx
                    .graph
                    .snapshot_evidence(&ctx.org_id, &request)
                    .await
                    .map_err(backend)?;
                history::validate_evidence(&ctx.org_id, &request, &mut records).map_err(backend)?;
                let record = records
                    .pop()
                    .ok_or_else(|| invalid("reused snapshot evidence unavailable"))?;
                FrozenObservation {
                    snapshot_uuid: record.uuid,
                    namespace: record.namespace.clone(),
                    created_at: record.created_at,
                    captured_at: record.captured_at,
                    kind: FrozenObservationKind::Existing {
                        evidence_digest: record.digest(),
                    },
                    saga: None,
                    history: vec![],
                }
            }
        };
        if let Some(association) = input.saga() {
            let key = (
                input.namespace().to_owned(),
                serde_json::to_string(&association.saga)
                    .map_err(|_| invalid("invalid Saga reference"))?,
            );
            let (uuid, name) = if let Some(value) = sagas.get(&key) {
                value.clone()
            } else {
                let state = ctx
                    .graph
                    .read_saga(
                        &ctx.org_id,
                        &SagaRead::State {
                            namespace: input.namespace().into(),
                            reference: association.saga.clone(),
                        },
                    )
                    .await
                    .map_err(backend)?;
                let value = match state {
                    SagaReadResult::State(Some(saga))
                        if saga.org_id == ctx.org_id.as_ref()
                            && saga.namespace == input.namespace() =>
                    {
                        (saga.uuid, saga.name)
                    }
                    SagaReadResult::State(None) => match &association.saga {
                        ThreadReference::Name { name } => (
                            kg_core::saga::saga_uuid(&ctx.org_id, input.namespace(), name),
                            name.clone(),
                        ),
                        ThreadReference::Uuid { .. } => {
                            return Err(invalid("requested Saga does not exist in this scope"));
                        }
                    },
                    _ => return Err(invalid("Saga read returned a mismatched scope or kind")),
                };
                sagas.insert(key, value.clone());
                value
            };
            entry.saga = Some(FrozenThreadAssociation {
                saga_uuid: uuid,
                name,
                previous_snapshot_uuid: association.previous_snapshot_uuid,
            });
        }
        manifest.entries.push(entry);
    }
    let mut order: Vec<usize> = (0..inputs.len()).collect();
    let mut groups: BTreeMap<(String, Uuid), Vec<usize>> = BTreeMap::new();
    for (index, entry) in manifest.entries.iter().enumerate() {
        if let Some(saga) = &entry.saga {
            groups
                .entry((entry.namespace.clone(), saga.saga_uuid))
                .or_default()
                .push(index);
        }
    }
    for indices in groups.values() {
        let mut sorted = indices.clone();
        sorted.sort_by_key(|index| (manifest.entries[*index].captured_at, *index));
        for (position, value) in indices.iter().zip(sorted) {
            order[*position] = value;
        }
    }
    // Explicit dependencies must already precede their target; reject an ambiguous bulk ordering.
    let positions: HashMap<usize, usize> = order
        .iter()
        .enumerate()
        .map(|(position, index)| (*index, position))
        .collect();
    for (index, entry) in manifest.entries.iter().enumerate() {
        if let Some(saga) = &entry.saga {
            if let Some(previous) = saga.previous_snapshot_uuid {
                for (other, candidate) in manifest.entries.iter().enumerate() {
                    if candidate.snapshot_uuid == previous
                        && candidate
                            .saga
                            .as_ref()
                            .is_some_and(|s| s.saga_uuid == saga.saga_uuid)
                        && positions[&other] >= positions[&index]
                    {
                        return Err(invalid(
                            "explicit predecessor must precede its target in Saga bulk order",
                        ));
                    }
                }
            }
        }
    }
    manifest.node_batches = order.chunks(chunk_size).map(<[usize]>::to_vec).collect();
    let mut earlier = Vec::<usize>::new();
    for index in order {
        let IngestionInput::Fresh(input) = &inputs[index] else {
            earlier.push(index);
            continue;
        };
        if ctx.policy.for_source(&input.source).extraction
            == kg_core::policy::ExtractionMode::Heuristic
        {
            earlier.push(index);
            continue;
        }
        let entry = &manifest.entries[index];
        let mut records = vec![];
        let mut local = HashMap::new();
        if !input.previous_snapshot_uuids.is_empty() {
            let request = SnapshotEvidenceRequest {
                namespace: entry.namespace.clone(),
                ids: input.previous_snapshot_uuids.clone(),
                captured_before: entry.captured_at,
                max_bytes: ctx.context_settings.max_history_bytes,
            };
            records = ctx
                .graph
                .snapshot_evidence(&ctx.org_id, &request)
                .await
                .map_err(backend)?;
            history::validate_evidence(&ctx.org_id, &request, &mut records).map_err(backend)?;
        } else if let Some(saga) = &entry.saga {
            let response = ctx
                .graph
                .read_saga(
                    &ctx.org_id,
                    &SagaRead::Context {
                        namespace: entry.namespace.clone(),
                        saga_uuid: saga.saga_uuid,
                        captured_before: entry.captured_at,
                        limit: ctx.context_settings.max_records,
                    },
                )
                .await
                .map_err(backend)?;
            let SagaReadResult::Members(page) = response else {
                return Err(invalid("Saga context returned wrong result kind"));
            };
            if page.members.len() > ctx.context_settings.max_records {
                return Err(invalid("Saga context exceeds record limit"));
            }
            let mut ranks: HashMap<Uuid, ContextOrder> = page
                .members
                .iter()
                .map(|member| (member.snapshot_uuid, ContextOrder::Stored(member.ordinal)))
                .collect();
            let ids: Vec<_> = page
                .members
                .into_iter()
                .filter(|member| member.snapshot_uuid != entry.snapshot_uuid)
                .map(|member| member.snapshot_uuid)
                .collect();
            if !ids.is_empty() {
                let request = SnapshotEvidenceRequest {
                    namespace: entry.namespace.clone(),
                    ids,
                    captured_before: entry.captured_at,
                    max_bytes: ctx.context_settings.max_history_bytes,
                };
                records = ctx
                    .graph
                    .snapshot_evidence(&ctx.org_id, &request)
                    .await
                    .map_err(backend)?;
                history::validate_evidence(&ctx.org_id, &request, &mut records).map_err(backend)?;
            }
            let mut candidates: Vec<_> = records
                .iter()
                .map(|record| ContextCandidate {
                    captured_at: record.captured_at,
                    created_at: record.created_at,
                    order: ranks[&record.uuid],
                    snapshot_uuid: record.uuid,
                })
                .collect();
            for prior in &earlier {
                let previous = &manifest.entries[*prior];
                if previous.namespace == entry.namespace
                    && previous.captured_at <= entry.captured_at
                    && previous
                        .saga
                        .as_ref()
                        .is_some_and(|s| s.saga_uuid == saga.saga_uuid)
                    && matches!(previous.kind, FrozenObservationKind::Fresh)
                {
                    let IngestionInput::Fresh(source) = &inputs[*prior] else {
                        continue;
                    };
                    let available = match &source.content {
                        Some(text) => !text.trim().is_empty(),
                        None => {
                            !source.entities.is_empty() || !source.relationship_changes.is_empty()
                        }
                    };
                    if !available {
                        continue;
                    }
                    let rank = ContextOrder::Local(*prior);
                    ranks.insert(previous.snapshot_uuid, rank);
                    local.insert(previous.snapshot_uuid, *prior);
                    candidates.push(ContextCandidate {
                        captured_at: previous.captured_at,
                        created_at: previous.created_at,
                        order: rank,
                        snapshot_uuid: previous.snapshot_uuid,
                    });
                }
            }
            let selected = select_context(candidates, ctx.context_settings.max_records);
            records.retain(|record| selected.contains(&record.uuid));
            for id in selected {
                if let Some(index) = local.get(&id) {
                    records.push(
                        manifest
                            .fresh_evidence(&ctx.org_id, inputs, *index)
                            .map_err(invalid)?,
                    );
                }
            }
        }
        if records.iter().map(|r| r.byte_len()).sum::<usize>()
            > ctx.context_settings.max_history_bytes
        {
            return Err(invalid("selected context exceeds byte limit"));
        }
        records.sort_by_key(|r| (r.captured_at, r.created_at, r.uuid));
        manifest.entries[index].history = records
            .into_iter()
            .map(|record| FrozenContextRef {
                reference: history::EvidenceRef {
                    uuid: record.uuid,
                    digest: record.digest(),
                },
                input_index: local.get(&record.uuid).copied(),
            })
            .collect();
        earlier.push(index);
    }
    manifest
        .validate_inputs(inputs, chunk_size)
        .map_err(invalid)?;
    Ok(manifest)
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ContextOrder {
    Stored(u64),
    Local(usize),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ContextCandidate {
    captured_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    order: ContextOrder,
    snapshot_uuid: Uuid,
}

// Rank membership before materializing local content; UUID only breaks a complete tie.
fn select_context(mut candidates: Vec<ContextCandidate>, limit: usize) -> Vec<Uuid> {
    candidates.sort_by_key(|candidate| std::cmp::Reverse(*candidate));
    let mut seen = std::collections::HashSet::new();
    candidates
        .into_iter()
        .filter_map(|candidate| {
            seen.insert(candidate.snapshot_uuid)
                .then_some(candidate.snapshot_uuid)
        })
        .take(limit)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn context_limit_keeps_latest_membership_and_same_run_input_before_uuid() {
        let at = Utc::now();
        let older = Uuid::from_u128(900);
        let newer = Uuid::from_u128(1);
        let local = Uuid::from_u128(2);
        let selected = select_context(
            vec![
                ContextCandidate {
                    captured_at: at,
                    created_at: at,
                    order: ContextOrder::Stored(1),
                    snapshot_uuid: older,
                },
                ContextCandidate {
                    captured_at: at,
                    created_at: at,
                    order: ContextOrder::Stored(2),
                    snapshot_uuid: newer,
                },
                ContextCandidate {
                    captured_at: at,
                    created_at: at,
                    order: ContextOrder::Local(0),
                    snapshot_uuid: local,
                },
            ],
            2,
        );
        assert_eq!(selected, vec![local, newer]);
    }
}
