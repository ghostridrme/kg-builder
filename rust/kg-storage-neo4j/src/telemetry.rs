//! Payload-free operation outcomes. The application owns subscribers and exporters.
use kg_core::errors::BackendError;
use std::time::Instant;

pub(crate) fn backend_error_kind(error: &BackendError) -> &'static str {
    match error {
        BackendError::Connection(_) => "connection",
        BackendError::Query(_) => "query",
        BackendError::Transaction(_) => "transaction",
        BackendError::Timeout(_) => "timeout",
        BackendError::Auth(_) => "auth",
        BackendError::RateLimited { .. } => "rate_limited",
        BackendError::Deserialization(_) => "deserialization",
        BackendError::Serialization(_) => "serialization",
        BackendError::NotFound(_) => "not_found",
        BackendError::Unavailable(_) => "unavailable",
        BackendError::NotConfigured(_) => "not_configured",
        BackendError::AttemptBudgetExhausted => "attempt_budget_exhausted",
        BackendError::IdentityRevisionChanged => "identity_revision_changed",
        BackendError::Conflict(_) | BackendError::CollectionOwnershipConflict(_) => "conflict",
        BackendError::UnknownCommit(_) => "unknown_commit",
        BackendError::Other(_) => "other",
        BackendError::IncompleteResponse => "incomplete_response",
        BackendError::Refused => "refused",
        BackendError::RelationshipHistoryLimit { .. } => "relationship_history_limit",
    }
}

pub(crate) fn driver_error_kind(error: &neo4rs::Error) -> &'static str {
    match error {
        neo4rs::Error::IOError { .. } | neo4rs::Error::ConnectionError => "connection",
        neo4rs::Error::AuthenticationError(_) => "auth",
        neo4rs::Error::Neo4j(_) => "server",
        _ => "driver",
    }
}

/// A dropped future has an abandoned client outcome, not proof of a server rollback.
pub(crate) struct Operation<'a> {
    name: &'a str,
    started: Instant,
    outcome: &'static str,
    exported: kg_core::telemetry::OperationGuard,
}

impl<'a> Operation<'a> {
    pub(crate) fn new(name: &'a str) -> Self {
        counters()[operation_index(name)]
            .inflight
            .fetch_add(1, Ordering::Relaxed);
        Self {
            name,
            started: Instant::now(),
            outcome: "abandoned",
            exported: kg_core::telemetry::OperationGuard::new(operation_kind(name)),
        }
    }

    pub(crate) fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
        use kg_core::telemetry::Outcome;
        self.exported.finish(match outcome {
            "success" | "committed" => Outcome::Success,
            "replayed" => Outcome::Replayed,
            "timeout" => Outcome::Timeout,
            "unknown_commit" => Outcome::UnknownCommit,
            "conflict" | "identity_revision_changed" => Outcome::Conflict,
            "auth" => Outcome::Auth,
            "rate_limited" => Outcome::RateLimited,
            "abandoned" => Outcome::Abandoned,
            _ => Outcome::Failed,
        });
    }
}

impl Drop for Operation<'_> {
    fn drop(&mut self) {
        let metrics = &counters()[operation_index(self.name)];
        metrics.inflight.fetch_sub(1, Ordering::Relaxed);
        metrics.completed.fetch_add(1, Ordering::Relaxed);
        match self.outcome {
            "abandoned" => {
                metrics.abandoned.fetch_add(1, Ordering::Relaxed);
            }
            "unknown_commit" => {
                metrics.unknown_commit.fetch_add(1, Ordering::Relaxed);
                metrics.failed.fetch_add(1, Ordering::Relaxed);
            }
            "replayed" => {
                metrics.replayed.fetch_add(1, Ordering::Relaxed);
            }
            "success" | "committed" => {}
            _ => {
                metrics.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        let elapsed = self.started.elapsed();
        let millis = elapsed.as_secs_f64() * 1000.0;
        let bucket = LATENCY_MS
            .iter()
            .position(|bound| millis <= *bound as f64)
            .unwrap_or(9);
        metrics.latency[bucket].fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "kg_storage_neo4j::operation",
            operation = self.name,
            outcome = self.outcome,
            duration_ms = elapsed.as_secs_f64() * 1000.0,
            "Neo4j operation finished"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::{Arc, Mutex};
    use tracing::Instrument;

    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn outcomes_keep_parent_context_without_backend_payloads() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Buffer(Arc::clone(&bytes));
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        async {
            let mut completed = Operation::new("commit_batch");
            completed.finish(backend_error_kind(&BackendError::UnknownCommit(
                "SECRET_QUERY_DATA".into(),
            )));
            drop(completed);
            let mut future = Box::pin(async {
                let _operation = Operation::new("search_read");
                std::future::pending::<()>().await;
            });
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            drop(future);
        }
        .instrument(tracing::info_span!("test_run", run_id = "parent-run"))
        .await;
        let snapshot = storage_metrics();
        let commit = snapshot
            .iter()
            .find(|value| value.operation == "commit_batch")
            .unwrap();
        assert!(commit.unknown_commit >= 1);
        let read = snapshot
            .iter()
            .find(|value| value.operation == "search_read")
            .unwrap();
        assert!(read.abandoned >= 1);
        assert!(read.latency_counts.iter().sum::<u64>() >= 1);
        assert_eq!(snapshot.len(), 7);
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(output.contains("unknown_commit"), "{output}");
        assert!(output.contains("abandoned"), "{output}");
        assert!(output.contains("duration_ms"), "{output}");
        assert!(
            output.lines().all(|line| line.contains("parent-run")),
            "{output}"
        );
        assert!(!output.contains("SECRET_QUERY_DATA"), "{output}");
    }
}

const OPERATIONS: [&str; 7] = [
    "read",
    "search_read",
    "commit_batch",
    "health",
    "graph mutations",
    "write",
    "schema",
];
const LATENCY_MS: [u64; 10] = [1, 5, 10, 25, 50, 100, 500, 1000, 5000, u64::MAX];
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct Counters {
    inflight: AtomicU64,
    completed: AtomicU64,
    retries: AtomicU64,
    failed: AtomicU64,
    abandoned: AtomicU64,
    unknown_commit: AtomicU64,
    replayed: AtomicU64,
    latency: [AtomicU64; 10],
}

fn counters() -> &'static [Counters; 7] {
    static COUNTERS: std::sync::OnceLock<[Counters; 7]> = std::sync::OnceLock::new();
    COUNTERS.get_or_init(|| std::array::from_fn(|_| Counters::default()))
}

fn operation_index(name: &str) -> usize {
    OPERATIONS
        .iter()
        .position(|operation| *operation == name)
        .unwrap_or(6)
}

/// Process-wide adapter measurements. Values are cumulative and snapshots are
/// approximate during concurrent calls. Histogram counts are non-cumulative;
/// the last bucket includes every duration above five seconds.
#[derive(Debug, Clone)]
pub struct StorageOperationMetrics {
    pub operation: &'static str,
    pub inflight: u64,
    pub completed: u64,
    pub retries: u64,
    pub failed: u64,
    pub abandoned: u64,
    pub unknown_commit: u64,
    pub replayed: u64,
    pub latency_upper_bounds_ms: [u64; 10],
    pub latency_counts: [u64; 10],
}

/// Export through the application's metrics endpoint; no subscriber or background
/// task is installed by the storage library. Labels are fixed, never caller input.
pub fn storage_metrics() -> Vec<StorageOperationMetrics> {
    counters()
        .iter()
        .enumerate()
        .map(|(index, value)| StorageOperationMetrics {
            operation: OPERATIONS[index],
            inflight: value.inflight.load(Ordering::Relaxed),
            completed: value.completed.load(Ordering::Relaxed),
            retries: value.retries.load(Ordering::Relaxed),
            failed: value.failed.load(Ordering::Relaxed),
            abandoned: value.abandoned.load(Ordering::Relaxed),
            unknown_commit: value.unknown_commit.load(Ordering::Relaxed),
            replayed: value.replayed.load(Ordering::Relaxed),
            latency_upper_bounds_ms: LATENCY_MS,
            latency_counts: std::array::from_fn(|i| value.latency[i].load(Ordering::Relaxed)),
        })
        .collect()
}

fn operation_kind(name: &str) -> kg_core::telemetry::OperationKind {
    use kg_core::telemetry::OperationKind;
    match name {
        "read" | "search_read" => OperationKind::GraphRead,
        "commit_batch" => OperationKind::Commit,
        "health" => OperationKind::Readiness,
        "schema" | "reference_backfill" => OperationKind::Maintenance,
        _ => OperationKind::GraphWrite,
    }
}

pub(crate) fn retry(operation: &str) {
    kg_core::telemetry::retry(operation_kind(operation));
    counters()[operation_index(operation)]
        .retries
        .fetch_add(1, Ordering::Relaxed);
}
