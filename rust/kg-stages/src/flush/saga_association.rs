//! Plan scoped Saga membership in the snapshot's existing atomic write batch.
use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use kg_core::{
    errors::{BackendError, StageError},
    models::ThreadNode,
    runtime::{
        saga::FrozenObservationKind,
        stage_output::{BatchRecovery, PlannedBatchOutput, ThreadAssociationEffect},
        RuntimeContext, StageOutput,
    },
    saga::{SagaMember, SagaRead, SagaReadResult, ThreadAssociationWrite, ThreadReference},
    traits::{BatchKind, GraphMutation, Stage, StageCapability, StageKind},
};
use uuid::Uuid;

/// Attach successful observations to their explicit Saga before the batch is embedded.
pub struct ThreadAssociationStage;

#[async_trait]
impl Stage for ThreadAssociationStage {
    fn name(&self) -> &str {
        "saga_association"
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        &[(StageKind::PlannedBatch, StageKind::PlannedBatch)]
    }
    fn capabilities(&self) -> &'static [StageCapability] {
        &[StageCapability::ThreadAssociation]
    }
    #[tracing::instrument(name = "saga_association", skip_all)]
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::PlannedBatch(planned) = input else {
            return Err(invalid("expected planned batch"));
        };
        let deadline = ctx.identity_deadline.unwrap_or_else(|| {
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(ctx.context_settings.read_timeout_ms)
        });
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(StageError::Cancelled {stage: self.name().into()}),
            result = tokio::time::timeout_at(deadline, plan(planned, ctx)) => result
                .map_err(|_| StageError::StepFailed {stage: self.name().into(), step: "planning".into(),
                    cause: "Saga planning deadline exceeded".into(), retriable: true})?
                .map(StageOutput::PlannedBatch),
        }
    }
}

struct State {
    node: ThreadNode,
    stored: bool,
    latest: Option<SagaMember>,
    members: BTreeMap<Uuid, SagaMember>,
}

async fn plan(
    mut planned: PlannedBatchOutput,
    ctx: &RuntimeContext,
) -> Result<PlannedBatchOutput, StageError> {
    if planned.batch.org_id != ctx.org_id.as_ref() {
        return Err(invalid("batch scope mismatch"));
    }
    if planned.batch.batch.kind != BatchKind::Node {
        return Ok(planned);
    }
    let Some(manifest) = &ctx.observation_manifest else {
        return Ok(planned);
    };
    let indexes = manifest
        .node_batches
        .get(planned.batch.batch.index as usize)
        .ok_or_else(|| invalid("missing frozen node batch"))?;
    let Some(value) = planned.batch.result.get("recovery") else {
        if indexes.iter().any(|index| {
            manifest
                .entries
                .get(*index)
                .is_some_and(|entry| entry.saga.is_some())
        }) {
            return Err(invalid("Saga association requires snapshot recovery"));
        }
        return Ok(planned);
    };
    let mut recovery: BatchRecovery =
        serde_json::from_value(value.clone()).map_err(|_| invalid("invalid snapshot recovery"))?;
    if !recovery.saga_associations.is_empty() {
        return Err(invalid("Saga association already planned"));
    }
    let written: BTreeSet<_> = planned
        .batch
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            GraphMutation::UpsertSnapshot { uuid, .. } => Some(*uuid),
            _ => None,
        })
        .collect();
    let mut successful = BTreeSet::new();
    let mut seen_checkpoints = BTreeSet::new();
    for checkpoint in &recovery.nodes {
        if !seen_checkpoints.insert(checkpoint.snapshot_index) {
            return Err(invalid("duplicate snapshot checkpoint"));
        }
        let entry = manifest
            .entries
            .get(checkpoint.snapshot_index)
            .ok_or_else(|| invalid("unknown snapshot ordinal"))?;
        if !matches!(entry.kind, FrozenObservationKind::Fresh)
            || !indexes.contains(&checkpoint.snapshot_index)
        {
            return Err(invalid("snapshot recovery disagrees with frozen input"));
        }
        // Policy-skipped text has a recovery checkpoint but no stored observation.
        if checkpoint.resolution.snapshot_nodes.is_empty()
            && !written.contains(&entry.snapshot_uuid)
            && checkpoint
                .resolution
                .incomplete_extractions
                .iter()
                .any(|item| {
                    item.snapshot_id == entry.snapshot_uuid && item.namespace == entry.namespace
                })
        {
            continue;
        }
        if !written.contains(&entry.snapshot_uuid)
            || !checkpoint
                .resolution
                .snapshot_nodes
                .iter()
                .any(|node| node.uuid == entry.snapshot_uuid)
        {
            return Err(invalid("snapshot recovery disagrees with planned writes"));
        }
        if !successful.insert(checkpoint.snapshot_index) {
            return Err(invalid("duplicate successful snapshot"));
        }
    }
    for reused in &recovery.reused_snapshots {
        let entry = manifest
            .entries
            .get(reused.snapshot_index)
            .ok_or_else(|| invalid("unknown reused ordinal"))?;
        if !matches!(&entry.kind, FrozenObservationKind::Existing {evidence_digest} if evidence_digest == &reused.evidence_digest)
            || !indexes.contains(&reused.snapshot_index)
            || entry.snapshot_uuid != reused.snapshot_uuid
            || entry.namespace != reused.namespace
        {
            return Err(invalid("reused snapshot scope mismatch"));
        }
        if !successful.insert(reused.snapshot_index) {
            return Err(invalid("duplicate successful snapshot"));
        }
    }
    let mut states: BTreeMap<(String, Uuid), State> = BTreeMap::new();
    for index in indexes.iter().filter(|index| successful.contains(index)) {
        let entry = &manifest.entries[*index];
        let Some(association) = &entry.saga else {
            continue;
        };
        let key = (entry.namespace.clone(), association.saga_uuid);
        if !states.contains_key(&key) {
            let response = read(
                ctx,
                SagaRead::State {
                    namespace: entry.namespace.clone(),
                    reference: ThreadReference::Name {
                        name: association.name.clone(),
                    },
                },
            )
            .await?;
            let SagaReadResult::State(node) = response else {
                return Err(invalid("unexpected Saga state response"));
            };
            let node = match node {
                Some(node) => {
                    node.validate()
                        .map_err(|_| invalid("invalid stored Saga"))?;
                    if node.uuid != association.saga_uuid
                        || node.org_id != ctx.org_id.as_ref()
                        || node.namespace != entry.namespace
                        || node.name != association.name
                    {
                        return Err(invalid("frozen Saga identity disagrees with storage"));
                    }
                    node
                }
                None => ThreadNode {
                    summary_incomplete_reason: None,
                    summary_incomplete_from_ordinal: None,
                    uuid: association.saga_uuid,
                    org_id: ctx.org_id.to_string(),
                    namespace: entry.namespace.clone(),
                    name: association.name.clone(),
                    labels: vec![],
                    created_at: entry.captured_at,
                    summary: String::new(),
                    first_snapshot_uuid: None,
                    last_snapshot_uuid: None,
                    last_summarized_at: None,
                    last_summarized_snapshot_captured_at: None,
                    first_captured_at: None,
                    revision: 0,
                    last_membership_ordinal: 0,
                    summary_revision: None,
                    summary_cursor: 0,
                    summary_supporting_snapshot_uuids: vec![],
                },
            };
            let latest = if node.revision == 0 {
                None
            } else {
                member_result(
                    read(
                        ctx,
                        SagaRead::Latest {
                            namespace: entry.namespace.clone(),
                            saga_uuid: node.uuid,
                            excluding_snapshot_uuid: None,
                        },
                    )
                    .await?,
                )?
            };
            if latest
                .as_ref()
                .is_some_and(|member| member.ordinal > node.last_membership_ordinal)
            {
                return Err(StageError::IdentityRevisionChanged);
            }
            let stored = node.revision != 0;
            states.insert(
                key.clone(),
                State {
                    node,
                    stored,
                    latest,
                    members: BTreeMap::new(),
                },
            );
        }
        let state = states
            .get_mut(&key)
            .ok_or_else(|| invalid("missing Saga state"))?;
        let existing = lookup_member(ctx, state, entry.snapshot_uuid).await?;
        if let Some(existing) = existing {
            if association.previous_snapshot_uuid.is_some()
                && association.previous_snapshot_uuid != existing.previous_snapshot_uuid
            {
                return Err(invalid("snapshot already has a different Saga predecessor"));
            }
            recovery.saga_associations.push(ThreadAssociationEffect {
                saga_uuid: state.node.uuid,
                namespace: entry.namespace.clone(),
                membership_ordinal: existing.ordinal,
            });
            continue;
        }
        let previous = match association.previous_snapshot_uuid {
            Some(id) => {
                if lookup_member(ctx, state, id).await?.is_none() {
                    return Err(invalid("explicit predecessor is not a Saga member"));
                }
                Some(id)
            }
            None => state.latest.as_ref().map(|member| member.snapshot_uuid),
        };
        let ordinal = state
            .node
            .last_membership_ordinal
            .checked_add(1)
            .filter(|value| *value <= i64::MAX as u64)
            .ok_or_else(|| invalid("Saga membership exhausted"))?;
        let write = ThreadAssociationWrite {
            namespace: entry.namespace.clone(),
            saga_uuid: state.node.uuid,
            name: state.node.name.clone(),
            created_at: state.node.created_at,
            expected_revision: state.node.revision,
            snapshot_uuid: entry.snapshot_uuid,
            previous_snapshot_uuid: previous,
            membership_uuid: edge_id(&ctx.org_id, state.node.uuid, None, entry.snapshot_uuid),
            next_uuid: previous
                .map(|id| edge_id(&ctx.org_id, state.node.uuid, Some(id), entry.snapshot_uuid)),
        };
        write
            .validate(&ctx.org_id)
            .map_err(|_| invalid("invalid Saga association"))?;
        planned
            .batch
            .mutations
            .push(GraphMutation::AssociateSagaSnapshot {
                association: Box::new(write),
            });
        let member = SagaMember {
            snapshot_uuid: entry.snapshot_uuid,
            captured_at: entry.captured_at,
            created_at: entry.created_at,
            ordinal,
            previous_snapshot_uuid: previous,
            snapshot_name: None,
            snapshot_source: None,
        };
        if state.latest.as_ref().is_none_or(|old| {
            (member.captured_at, member.created_at, member.ordinal)
                > (old.captured_at, old.created_at, old.ordinal)
        }) {
            state.latest = Some(member.clone());
        }
        state.members.insert(entry.snapshot_uuid, member);
        state.node.revision += 1;
        state.node.last_membership_ordinal = ordinal;
        state
            .node
            .first_snapshot_uuid
            .get_or_insert(entry.snapshot_uuid);
        state.node.last_snapshot_uuid = Some(entry.snapshot_uuid);
        recovery.saga_associations.push(ThreadAssociationEffect {
            saga_uuid: state.node.uuid,
            namespace: entry.namespace.clone(),
            membership_ordinal: ordinal,
        });
    }
    tracing::debug!(
        associations = recovery.saga_associations.len(),
        "Saga associations planned"
    );
    planned.batch.result["snapshots_reused"] = serde_json::json!(recovery.reused_snapshots.len());
    planned.batch.result["recovery"] =
        serde_json::to_value(recovery).map_err(|_| invalid("invalid Saga recovery"))?;
    crate::flush::mutation_planning::preflight(&planned, ctx)?;
    Ok(planned)
}

async fn lookup_member(
    ctx: &RuntimeContext,
    state: &mut State,
    id: Uuid,
) -> Result<Option<SagaMember>, StageError> {
    if let Some(member) = state.members.get(&id) {
        return Ok(Some(member.clone()));
    }
    if !state.stored {
        return Ok(None);
    }
    let member = member_result(
        read(
            ctx,
            SagaRead::Member {
                namespace: state.node.namespace.clone(),
                saga_uuid: state.node.uuid,
                snapshot_uuid: id,
            },
        )
        .await?,
    )?;
    if let Some(member) = &member {
        if member.snapshot_uuid != id {
            return Err(invalid("unexpected Saga member"));
        }
        if member.ordinal > state.node.last_membership_ordinal {
            return Err(StageError::IdentityRevisionChanged);
        }
        state.members.insert(id, member.clone());
    }
    Ok(member)
}
fn member_result(result: SagaReadResult) -> Result<Option<SagaMember>, StageError> {
    match result {
        SagaReadResult::Member(member)
            if member
                .as_ref()
                .is_none_or(|member| !member.snapshot_uuid.is_nil() && member.ordinal > 0) =>
        {
            Ok(member)
        }
        _ => Err(invalid("invalid Saga member response")),
    }
}
async fn read(ctx: &RuntimeContext, request: SagaRead) -> Result<SagaReadResult, StageError> {
    request
        .validate(&ctx.org_id)
        .map_err(|_| invalid("invalid Saga read"))?;
    let _permit =
        kg_core::telemetry::acquire(&ctx.semaphore, kg_core::telemetry::OperationKind::GraphRead)
            .await
            .map_err(|_| StageError::Cancelled {
                stage: "saga_association".into(),
            })?;
    ctx.graph
        .read_saga(&ctx.org_id, &request)
        .await
        .map_err(|error| match error {
            BackendError::IdentityRevisionChanged => StageError::IdentityRevisionChanged,
            error => StageError::StepFailed {
                stage: "saga_association".into(),
                step: "read".into(),
                cause: "Saga state unavailable".into(),
                retriable: error.is_transient(),
            },
        })
}
fn edge_id(org: &str, saga: Uuid, previous: Option<Uuid>, target: Uuid) -> Uuid {
    let key = serde_json::json!(["saga-link-v1", org, saga, previous, target]).to_string();
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
}
fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: "saga_association".into(),
        message: message.into(),
    }
}
