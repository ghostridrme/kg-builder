//! Commit a fully prepared batch and return its durable receipt.
use async_trait::async_trait;
use kg_core::{
    errors::{BackendError, StageError},
    pipeline::CommittedCounts,
    runtime::{
        stage_output::{BatchRecovery, CommitOutput},
        RuntimeContext, StageOutput,
    },
    traits::{BatchIdentity, CommittedBatch, Stage},
};
use std::time::Instant;
const STAGE: &str = "persist";
/// Await the complete logical batch, including durable pages when required.
pub struct PersistStage;
#[async_trait]
impl Stage for PersistStage {
    fn processing_version(&self) -> String {
        "persist-durable-pages-v4".into()
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::PreparedBatch, StageKind::Committed)]
    }
    fn name(&self) -> &str {
        STAGE
    }
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::PreparedBatch(prepared) = input else {
            return Err(invalid("expected a prepared batch".into()));
        };
        let batch = prepared.batch;
        let identity = batch.batch;
        if batch.org_id != ctx.org_id.as_ref() {
            return Err(invalid("prepared batch crosses organization".into()));
        }
        super::mutation_planning::validate_commit(&batch, ctx)
            .map_err(|error| invalid(error.to_string()))?;
        let planned_result = decode_result(&batch.result).map_err(|_| {
            invalid("prepared batch holds an invalid committed result or recovery".into())
        })?;
        if ctx.cancel.is_cancelled() {
            return Err(StageError::Cancelled {
                stage: STAGE.into(),
            });
        }
        if ctx
            .identity_deadline
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            return Err(StageError::StepFailed {
                stage: STAGE.into(),
                step: "identity_budget".into(),
                cause: "identity resolution deadline exceeded before commit".into(),
                retriable: true,
            });
        }
        let started = Instant::now();
        let committed = ctx
            .graph
            .commit_batch_cancellable(&batch, &ctx.cancel)
            .await
            .map_err(|e| {
                if ctx.cancel.is_cancelled() && !matches!(&e, BackendError::UnknownCommit(_)) {
                    StageError::Cancelled {
                        stage: STAGE.into(),
                    }
                } else {
                    commit_error(identity, e)
                }
            })?;
        let (counts, recovery) = acknowledged_result(identity, &committed, planned_result)?;
        tracing::info!(
            run_id = %identity.run_id,
            batch_kind = identity.kind.label(),
            batch_index = identity.index,
            replayed = committed.replayed,
            statements = batch.mutations.len() + batch.preconditions.len(),
            commit_ms = started.elapsed().as_millis() as u64,
            "batch committed"
        );

        Ok(StageOutput::Committed(CommitOutput {
            batch: identity,
            replayed: committed.replayed,
            committed_at: committed.committed_at,
            counts,
            recovery,
        }))
    }
}

fn label(batch: BatchIdentity) -> String {
    format!("{}#{}", batch.kind.label(), batch.index)
}
pub(crate) fn invalid(message: String) -> StageError {
    StageError::StateValidation {
        stage: STAGE.into(),
        message,
    }
}
fn commit_error(batch: BatchIdentity, error: BackendError) -> StageError {
    let message = format!("batch {}: {error}", label(batch));
    match error {
        BackendError::IdentityRevisionChanged => StageError::IdentityRevisionChanged,
        BackendError::UnknownCommit(_) => StageError::CommitOutcomeUnknown {
            stage: STAGE.into(),
            message,
        },
        BackendError::Conflict(_) => StageError::CommitRejected {
            stage: STAGE.into(),
            message,
        },
        BackendError::Auth(_)
        | BackendError::CollectionOwnershipConflict(_)
        | BackendError::Query(_)
        | BackendError::NotFound(_)
        | BackendError::Serialization(_)
        | BackendError::Deserialization(_) => StageError::StepFailed {
            stage: STAGE.into(),
            step: "commit".into(),
            cause: message,
            retriable: false,
        },
        _ => StageError::StepFailed {
            stage: STAGE.into(),
            step: "commit".into(),
            cause: message,
            retriable: true,
        },
    }
}

fn decode_result(
    result: &serde_json::Value,
) -> Result<(CommittedCounts, Option<BatchRecovery>), serde_json::Error> {
    Ok((
        serde_json::from_value(result.clone())?,
        result
            .get("recovery")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?,
    ))
}

fn acknowledged_result(
    identity: BatchIdentity,
    receipt: &CommittedBatch,
    planned: (CommittedCounts, Option<BatchRecovery>),
) -> Result<(CommittedCounts, Option<BatchRecovery>), StageError> {
    let unknown = || StageError::CommitOutcomeUnknown {
        stage: STAGE.into(),
        message: format!(
            "batch {} was acknowledged but its receipt cannot restore the outcome",
            label(identity)
        ),
    };
    if receipt.batch_id != identity.batch_id()
        || receipt.run_id != identity.run_id
        || receipt.kind != identity.kind
        || receipt.index != identity.index
    {
        return Err(unknown());
    }
    if receipt.replayed {
        decode_result(&receipt.result).map_err(|_| unknown())
    } else {
        // An acknowledged fresh write committed this exact, prevalidated result.
        // Only a replay can carry a different concurrent winner's recovery data.
        Ok(planned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use kg_core::traits::BatchKind;
    use serde_json::json;
    use uuid::Uuid;

    fn identity() -> BatchIdentity {
        BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Node,
            index: 0,
        }
    }
    #[test]
    fn ownership_rejection_is_terminal_but_graph_conflicts_still_replan() {
        let ownership = BackendError::CollectionOwnershipConflict("owned by generation 5".into());
        assert!(!ownership.is_transient());
        assert!(matches!(
            kg_core::telemetry::Outcome::from(&ownership),
            kg_core::telemetry::Outcome::Conflict
        ));
        assert!(matches!(
            kg_core::search::SearchFailure::from(&ownership),
            kg_core::search::SearchFailure::Conflict
        ));
        assert!(matches!(
            commit_error(identity(), ownership),
            StageError::StepFailed {
                retriable: false,
                ..
            }
        ));
        assert!(matches!(
            commit_error(identity(), BackendError::Conflict("changed entity".into())),
            StageError::CommitRejected { .. }
        ));
        assert!(matches!(
            commit_error(
                identity(),
                BackendError::UnknownCommit("unreadable receipt".into())
            ),
            StageError::CommitOutcomeUnknown { .. }
        ));
    }
    fn receipt(
        identity: BatchIdentity,
        replayed: bool,
        result: serde_json::Value,
    ) -> CommittedBatch {
        CommittedBatch {
            batch_id: identity.batch_id(),
            run_id: identity.run_id,
            kind: identity.kind,
            index: identity.index,
            committed_at: Utc::now(),
            replayed,
            result,
        }
    }
    #[test]
    fn acknowledged_fresh_commit_uses_prevalidated_result_and_replay_uses_winner() {
        let identity = identity();
        let planned = CommittedCounts {
            entities_created: 2,
            ..Default::default()
        };
        let fresh = receipt(identity, false, json!({"invalid":true}));
        assert_eq!(
            acknowledged_result(identity, &fresh, (planned, None))
                .unwrap()
                .0,
            planned
        );
        let winner = CommittedCounts {
            entities_created: 1,
            ..Default::default()
        };
        let replay = receipt(identity, true, serde_json::to_value(winner).unwrap());
        assert_eq!(
            acknowledged_result(identity, &replay, (planned, None))
                .unwrap()
                .0,
            winner
        );
    }
    #[test]
    fn unusable_replay_or_wrong_receipt_identity_is_unknown_commit() {
        let identity = identity();
        let mut invalid_recovery = serde_json::to_value(CommittedCounts::default()).unwrap();
        invalid_recovery["recovery"] = json!({"nodes":"invalid"});
        for value in [json!({}), invalid_recovery] {
            assert!(matches!(
                acknowledged_result(
                    identity,
                    &receipt(identity, true, value),
                    (Default::default(), None)
                ),
                Err(StageError::CommitOutcomeUnknown { .. })
            ));
        }
        let mut wrong = receipt(identity, false, json!({}));
        wrong.index += 1;
        assert!(matches!(
            acknowledged_result(identity, &wrong, (Default::default(), None)),
            Err(StageError::CommitOutcomeUnknown { .. })
        ));
    }
    #[tokio::test]
    async fn malformed_planned_result_is_rejected_before_storage() {
        use kg_core::{
            runtime::{stage_output::PreparedBatchOutput, RuntimeContextBuilder},
            test_support::{MockEmbedBackend, MockLlmBackend},
            traits::{MutationBatch, RequestFingerprint},
        };
        use std::sync::Arc;
        let ctx = RuntimeContextBuilder::new("org")
            .graph(Arc::new(kg_core::test_support::UnreachableGraph))
            .embedder(Arc::new(MockEmbedBackend::default_dimension()))
            .llm_default(Arc::new(MockLlmBackend::failing()))
            .llm_extraction(Arc::new(MockLlmBackend::failing()))
            .build()
            .unwrap();
        for result in [json!({}), {
            let mut result = serde_json::to_value(CommittedCounts::default()).unwrap();
            result["recovery"] = json!({"nodes":"invalid"});
            result
        }] {
            let batch = MutationBatch {
                org_id: "org".into(),
                batch: identity(),
                fingerprint: RequestFingerprint("0".repeat(32)),
                preconditions: vec![],
                mutations: vec![],
                result,
            };
            assert!(matches!(
                PersistStage
                    .process(
                        StageOutput::PreparedBatch(PreparedBatchOutput { batch }),
                        &ctx
                    )
                    .await,
                Err(StageError::StateValidation { .. })
            ));
        }
    }
}
