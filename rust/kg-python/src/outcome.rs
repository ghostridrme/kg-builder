use kg_core::{
    errors::{BackendError, PipelineError},
    pipeline::{CommittedCounts, PipelineOutput},
};
use serde_json::{json, Value};
use uuid::Uuid;

pub fn encode(result: Result<PipelineOutput, PipelineError>, run_id: Uuid) -> Value {
    match result {
        Ok(mut output) => {
            let mut failed: Vec<_> = output.failed_indexes().into_iter().collect();
            let mut incomplete: Vec<_> = output.incomplete_indexes().into_iter().collect();
            failed.sort_unstable();
            incomplete.sort_unstable();
            // SnapshotFailure stores a rendered upstream error, not a safe
            // public category. Preserve its location/progress, never its text.
            for failure in &mut output.failed_snapshots {
                failure.error = "snapshot_failed".into();
            }
            let mut value = serde_json::to_value(&output).expect("pipeline output is serializable");
            value["complete"] = json!(output.is_complete());
            value["inputs"] = json!({"total":output.snapshots_total,"completed":output.snapshots_completed,"failed":failed,"incomplete":incomplete});
            json!({"ok":true,"result":value})
        }
        Err(error) => {
            let (committed, batches, unknown) = match &error {
                PipelineError::Aborted {
                    committed,
                    batches_committed,
                    commit_unknown,
                    ..
                } => (**committed, *batches_committed, *commit_unknown),
                _ => (CommittedCounts::default(), 0, false),
            };
            // Backend error strings may contain submitted text or connection credentials.
            let cause = match error.root_cause() {
                PipelineError::Cancelled => "cancelled",
                PipelineError::StateValidation { .. } => "input_validation",
                PipelineError::IdentityRevisionChanged => "identity_revision_changed",
                PipelineError::TaskPanic(_) => "task_panic",
                PipelineError::RetryExhausted { .. } => "retry_exhausted",
                PipelineError::StepExecution { .. } => "step_failed",
                PipelineError::StageExecution { .. } => "stage_failed",
                _ => "pipeline_failed",
            };
            let context = match error.root_cause() {
                PipelineError::StepExecution { stage, step, .. } => {
                    json!({"stage":stage,"step":step})
                }
                PipelineError::StageExecution {
                    stage, error_count, ..
                } => json!({"stage":stage,"error_count":error_count}),
                PipelineError::StateValidation { stage, .. } => json!({"stage":stage}),
                PipelineError::RetryExhausted { step, attempts, .. } => {
                    json!({"step":step,"attempts":attempts})
                }
                _ => json!({}),
            };
            json!({"ok":false,"result":{"run_id":run_id,"cause":cause,"context":context,"committed":committed,"batches_committed":batches,"commit_unknown":unknown,"retriable":error.is_retriable()}})
        }
    }
}

/// Exhaustive categories: never derive public messages from Display/Debug.
pub fn backend_kind(error: &BackendError) -> &'static str {
    match error {
        BackendError::IdentityRevisionChanged => "identity_revision_changed",
        BackendError::Connection(_) => "connection",
        BackendError::Query(_) => "query",
        BackendError::RelationshipHistoryLimit { .. } => "relationship_history_limit",
        BackendError::Transaction(_) => "transaction",
        BackendError::Timeout(_) => "timeout",
        BackendError::Auth(_) => "authentication",
        BackendError::RateLimited { .. } => "rate_limited",
        BackendError::IncompleteResponse => "incomplete_response",
        BackendError::Refused => "refused",
        BackendError::Deserialization(_) => "deserialization",
        BackendError::Serialization(_) => "serialization",
        BackendError::NotFound(_) => "not_found",
        BackendError::Unavailable(_) => "unavailable",
        BackendError::NotConfigured(_) => "not_configured",
        BackendError::AttemptBudgetExhausted => "attempt_budget_exhausted",
        BackendError::Conflict(_) => "conflict",
        BackendError::CollectionOwnershipConflict(_) => "collection_ownership_conflict",
        BackendError::UnknownCommit(_) => "unknown_commit",
        BackendError::Other(_) => "backend_failed",
    }
}

/// Bound transport encoding without losing the receipt/replay information on overflow.
pub fn wire(value: &Value, limit: usize) -> String {
    struct Bounded {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("response size limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        limit,
    };
    if serde_json::to_writer(&mut writer, value).is_ok() {
        return String::from_utf8(writer.bytes).expect("JSON is UTF-8");
    }
    let result = &value["result"];
    if result.get("run_id").is_none() {
        return json!({"ok":false,"interrupted":value["interrupted"],"result":{"cause":"response_limit","commit_unknown":result.get("commit_unknown").cloned().unwrap_or(json!(false)),"retriable":false}}).to_string();
    }
    if result.get("replay_supported") == Some(&json!(false)) {
        let mut progress = serde_json::Map::new();
        for key in [
            "scanned",
            "candidate_patterns",
            "proposed",
            "activated",
            "rejected",
            "uncertain",
            "skipped_existing",
            "model_calls",
            "truncated",
        ] {
            if let Some(value) = result.get("learning").and_then(|report| report.get(key)) {
                progress.insert(key.into(), value.clone());
            }
        }
        return json!({"ok":false,"interrupted":value["interrupted"],"result":{
            "run_id":result["run_id"],"operation":result["operation"],"cause":"response_limit",
            "operation_complete":result["complete"],"replay_supported":false,
            "learning":progress,"count":result["count"],
            "commit_unknown":result.get("commit_unknown").cloned().unwrap_or(json!(false)),"retriable":false
        }}).to_string();
    }
    json!({"ok":false,"interrupted":value["interrupted"],"result":{
        "run_id":result["run_id"],"cause":"response_limit",
        "ingestion_complete":result["complete"],"committed":result["committed"],
        "newly_committed":result["newly_committed"],
        "batches_committed":result.get("batches_committed").cloned().unwrap_or_else(|| json!(result["batches"].as_array().map_or(0,Vec::len))),
        "commit_unknown":result.get("commit_unknown").cloned().unwrap_or(json!(false)),
        "retriable":false
    }}).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_failure_and_rule_categories_never_echo_backend_text() {
        let output: PipelineOutput = serde_json::from_value(json!({
            "run_id":Uuid::new_v4(),"committed":CommittedCounts::default(),
            "newly_committed":CommittedCounts::default(),"batches":[],
            "snapshots_total":1,"snapshots_completed":0,"duration_ms":0,
            "failed_snapshots":[{"snapshot_index":0,"stage":"extract",
                "error":"sensitive-credential-and-payload","retriable":true}]
        }))
        .unwrap();
        let value = encode(Ok(output), Uuid::new_v4());
        assert!(!value.to_string().contains("sensitive-credential"));
        assert_eq!(
            value["result"]["failed_snapshots"][0]["error"],
            "snapshot_failed"
        );
        assert_eq!(value["result"]["failed_snapshots"][0]["retriable"], true);
        assert_eq!(value["result"]["inputs"]["failed"], json!([0]));
        for error in [
            BackendError::Auth("secret".into()),
            BackendError::Query("secret".into()),
            BackendError::UnknownCommit("secret".into()),
        ] {
            assert!(!backend_kind(&error).contains("secret"));
        }
    }

    #[test]
    fn response_overflow_preserves_successful_commit_counts() {
        let value = json!({"ok":true,"result":{"run_id":Uuid::new_v4(),"complete":true,"committed":{"entities_created":3},"batches":[{}],"details":"x".repeat(10000)}});
        let encoded = wire(&value, 4096);
        assert!(encoded.len() < 4096);
        let result: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(result["result"]["cause"], "response_limit");
        assert_eq!(result["result"]["ingestion_complete"], true);
        assert_eq!(result["result"]["committed"]["entities_created"], 3);
        assert_eq!(result["result"]["batches_committed"], 1);
    }

    #[test]
    fn rule_overflow_keeps_progress_and_never_claims_receipt_replay() {
        let value = json!({"ok":true,"result":{"run_id":Uuid::new_v4(),"operation":"rules_learn","replay_supported":false,"complete":true,"learning":{"proposed":3,"model_calls":0,"resume":"x".repeat(10000)}}});
        let result: Value = serde_json::from_str(&wire(&value, 4096)).unwrap();
        assert_eq!(result["result"]["replay_supported"], false);
        assert_eq!(result["result"]["learning"]["proposed"], 3);
        assert_eq!(result["result"]["operation_complete"], true);
        assert!(result["result"].get("ingestion_complete").is_none());
        assert!(wire(&value, 4096).len() < 4096);
    }

    #[test]
    fn abort_keeps_committed_progress_without_backend_text() {
        let run_id = Uuid::new_v4();
        let error = PipelineError::Aborted {
            run_id,
            committed: Box::new(CommittedCounts {
                snapshots: 1,
                entities_created: 2,
                ..Default::default()
            }),
            batches_committed: 1,
            commit_unknown: true,
            cause: Box::new(PipelineError::StepExecution {
                stage: "persist".into(),
                step: "commit".into(),
                cause: "credential-and-payload-must-not-escape".into(),
                retriable: true,
            }),
        };
        let value = encode(Err(error), run_id);
        assert_eq!(value["result"]["committed"]["entities_created"], 2);
        assert_eq!(value["result"]["batches_committed"], 1);
        assert_eq!(value["result"]["commit_unknown"], true);
        assert_eq!(value["result"]["context"]["step"], "commit");
        assert_eq!(value["result"]["run_id"], run_id.to_string());
        assert!(!value.to_string().contains("credential-and-payload"));
    }
}
