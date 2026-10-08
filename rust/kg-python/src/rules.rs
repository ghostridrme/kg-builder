//! Deterministic rule administration through the existing guarded Rust service.
use kg_core::runtime::rule_learning::{admin, service};
use kg_core::traits::rule_store::RuleStatus;
use kg_stages::Engine;
use kg_storage_neo4j::Neo4jGraphBackend;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Learn {
        source: String,
        #[serde(default)]
        auto_promote: bool,
    },
    List {
        source: String,
        status: Option<RuleStatus>,
    },
}
impl Request {
    pub fn valid(&self) -> bool {
        let source = match self {
            Self::Learn { source, .. } | Self::List { source, .. } => source,
        };
        !source.trim().is_empty()
    }
}

pub async fn run(
    graph: &Neo4jGraphBackend,
    engine: &Engine,
    request: Request,
    org: &str,
    run_id: Uuid,
    deadline: Option<Instant>,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Value {
    let writes = matches!(request, Request::Learn { .. });
    let result = match request {
        Request::Learn {
            source,
            auto_promote,
        } => {
            let mut bounds = service::LearningBounds::default();
            if let Some(deadline) = deadline {
                bounds.max_elapsed = bounds
                    .max_elapsed
                    .min(deadline.saturating_duration_since(Instant::now()));
            }
            service::learn_cancellable(
                // The production matcher validates every proposal against the
                // held-out labelled cases and repairs the graph on activation.
                service::LearningServices {
                    evidence: graph,
                    validator: engine,
                    store: graph,
                    model: None,
                    repair: Some(engine),
                },
                org,
                &source,
                &bounds,
                &service::LearningOptions {
                    auto_promote,
                    ..Default::default()
                },
                Some(cancellation),
            )
            .await
            .map(|report| {
                json!({
                    "run_id": run_id, "operation":"rules_learn", "complete": !report.truncated,
                    "learning": report, "replay_supported": false,
                })
            })
        }
        Request::List { source, status } => {
            let timeout = deadline
                .map(|at| at.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_secs(120));
            match cancellable_read(timeout, cancellation, admin::list(graph, org, &source, status)).await {
                None => return json!({"ok":false,"result":{
                    "run_id":run_id,"complete":false,"cause":"cancelled",
                    "operation":"rules_list","commit_unknown":false,
                    "retriable":false,"replay_supported":false
                }}),
                Some(result) => result.map(|rules| {
                    let views: Vec<_> = rules.iter().map(|rule| json!({
                        "id": rule.id, "revision": rule.revision, "status": rule.status,
                        "owner_slot": rule.owner_slot, "target_type": rule.mapping.target_type,
                        "relationship_name": rule.mapping.relationship_name,
                    })).collect();
                    json!({"run_id":run_id,"complete":true,"count":views.len(),"rules":views,"operation":"rules_list","replay_supported":false})
                }),
            }
        }
    };
    match result {
        Ok(value) => json!({"ok":true,"result":value}),
        Err(error) => json!({"ok":false,"result":{
            "run_id":run_id,"complete":false,"cause":"rule_operation_failed",
            "error":crate::outcome::backend_kind(&error),
            "error_kind":crate::outcome::backend_kind(&error),
            "operation":if writes {"rules_learn"} else {"rules_list"}, "commit_unknown":writes,"retriable":false,"replay_supported":false,
        }}),
    }
}

// Read-only work may be dropped on cancellation; mutations still settle.
async fn cancellable_read<T>(
    timeout: Duration,
    cancellation: &tokio_util::sync::CancellationToken,
    read: impl std::future::Future<Output = Result<T, kg_core::errors::BackendError>>,
) -> Option<Result<T, kg_core::errors::BackendError>> {
    tokio::select! { biased;
        _ = cancellation.cancelled() => None,
        result = tokio::time::timeout(timeout, read) => Some(result.unwrap_or_else(|_| {
            Err(kg_core::errors::BackendError::Timeout(timeout.as_millis().min(u64::MAX as u128) as u64))
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admitted_read_cancels_after_start_and_drops_work() {
        let token = tokio_util::sync::CancellationToken::new();
        let child = token.clone();
        let (started, entered) = tokio::sync::oneshot::channel();
        let (dropped, finished) = tokio::sync::oneshot::channel();
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let task = tokio::spawn(async move {
            cancellable_read(Duration::from_secs(120), &child, async {
                let _guard = OnDrop(Some(dropped));
                let _ = started.send(());
                std::future::pending::<Result<(), kg_core::errors::BackendError>>().await
            })
            .await
        });
        entered.await.unwrap();
        token.cancel();
        assert!(tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .is_none());
        finished.await.unwrap();
    }
}
