//! Search reads carry a unique marker so a read the caller gave up on can be
//! terminated on the server instead of running until its own limits apply.
use crate::{
    driver::{build_query, row_to_json},
    Neo4jGraphBackend,
};
use kg_core::errors::BackendError;
use neo4rs::Graph;
use serde_json::{Map, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use tracing::Instrument;

/// Tracks reads before they start, including cleanup after their futures drop.
/// Retains counters and pending cleanup IDs, never completed task handles.
pub(crate) struct CleanupTracker {
    state: Mutex<CleanupState>,
    changed: Notify,
    slots: Semaphore,
}
#[derive(Default)]
struct CleanupState {
    closing: bool,
    active: usize,
    failed: usize,
    /// Reads whose server-side termination is still in flight after the caller
    /// dropped them while armed. `wait_idle` waits for the IDs present at entry;
    /// healthy reads and later cancellations cannot extend that barrier.
    pending: std::collections::BTreeSet<u64>,
    next_termination: u64,
}
impl Default for CleanupTracker {
    fn default() -> Self {
        Self {
            state: Mutex::new(CleanupState::default()),
            changed: Notify::new(),
            slots: Semaphore::new(2),
        }
    }
}
impl CleanupTracker {
    pub(crate) async fn wait_idle(&self) {
        let pending = self.state.lock().unwrap().pending.clone();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().unwrap().pending.is_disjoint(&pending) {
                return;
            }
            changed.await;
        }
    }

    /// A caller that dropped an armed read is about to terminate its server work;
    /// count it so `wait_idle` settles only once every such termination finishes.
    fn begin_termination(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.next_termination = state
            .next_termination
            .checked_add(1)
            .expect("cleanup sequence exhausted");
        let id = state.next_termination;
        state.pending.insert(id);
        id
    }

    fn register(self: &Arc<Self>) -> Result<CleanupTicket, BackendError> {
        let mut state = self.state.lock().unwrap();
        if state.closing {
            return Err(BackendError::Unavailable("Neo4j search is closing".into()));
        }
        state.active += 1;
        Ok(CleanupTicket {
            tracker: Arc::clone(self),
            success: false,
            terminating: None,
        })
    }
    pub(crate) async fn drain(&self, deadline: Duration) -> Result<(), BackendError> {
        self.state.lock().unwrap().closing = true;
        let wait = async {
            loop {
                // Register before inspecting state so completion cannot be missed.
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let state = self.state.lock().unwrap();
                    if state.active == 0 {
                        return if state.failed == 0 {
                            Ok(())
                        } else {
                            Err(BackendError::Unavailable(format!(
                                "{} Neo4j search cleanup task(s) failed",
                                state.failed
                            )))
                        };
                    }
                }
                changed.await;
            }
        };
        tokio::time::timeout(deadline, wait)
            .await
            .map_err(|_| BackendError::Timeout(deadline.as_millis() as u64))?
    }
}
struct CleanupTicket {
    tracker: Arc<CleanupTracker>,
    success: bool,
    /// Set when this ticket backs an in-flight server termination, so its drop
    /// also removes the pending termination ID awaited by `wait_idle`.
    terminating: Option<u64>,
}
impl Drop for CleanupTicket {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock().unwrap();
        state.active -= 1;
        if let Some(id) = self.terminating {
            state.pending.remove(&id);
        }
        if !self.success {
            state.failed += 1;
        }
        drop(state);
        self.tracker.changed.notify_waiters();
    }
}

/// Armed for the lifetime of one read. Dropping it while armed terminates the
/// marked server transaction through the control pool, which is never occupied
/// by the reads it must interrupt.
struct CancelRead {
    control: Arc<Graph>,
    marker: Option<String>,
    ticket: Option<CleanupTicket>,
}
impl CancelRead {
    /// The server has finished with this read: nothing is left to terminate.
    fn disarm(&mut self) {
        self.marker = None;
        if let Some(ticket) = &mut self.ticket {
            ticket.success = true;
        }
    }
}
impl Drop for CancelRead {
    fn drop(&mut self) {
        let Some(marker) = self.marker.take() else {
            return;
        };
        let control = Arc::clone(&self.control);
        let mut ticket = self.ticket.take().expect("armed read has cleanup ticket");
        // Count this termination before it is handed to the runtime (or dropped
        // here when none exists), so `wait_idle` observes it and its matching
        // decrement in `CleanupTicket::drop` stays balanced on every path.
        ticket.terminating = Some(ticket.tracker.begin_termination());
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let tracker = Arc::clone(&ticket.tracker);
                    // Queue outside the per-attempt timeout. Saturation of our own
                    // control pool must not consume the termination budget.
                    let _slot = tracker.slots.acquire().await.expect("cleanup semaphore stays open");
                        ticket.success = terminate(control, marker).await;
                        // Capture and retain the whole Drop guard through cleanup.
                        drop(ticket);
                }.in_current_span());
            }
            Err(_) => tracing::error!(
                "no async runtime to terminate a cancelled Neo4j search read; server work continues until the server's own limits apply"
            ),
        }
    }
}

/// Find the marked transaction and terminate it in one statement. The caller
/// may have given up while its RUN message was still in flight, so a miss is
/// retried briefly before concluding there is nothing to terminate.
async fn terminate(control: Arc<Graph>, marker: String) -> bool {
    let statement = kg_storage_cypher::neo4j::admin::terminate_marked(&marker);
    for attempt in 0..5 {
        let query = build_query(&statement.statement, &statement.parameters);
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            let mut stream = control.execute_once(query).await?;
            let mut terminated = 0usize;
            while let Some(row) = stream.next().await? {
                if !row_to_json(&row)
                    .is_ok_and(|row| kg_storage_cypher::neo4j::admin::termination_confirmed(&row))
                {
                    return Ok::<_, neo4rs::Error>(None);
                }
                terminated += 1;
            }
            Ok::<_, neo4rs::Error>(Some(terminated))
        })
        .await;
        match outcome {
            Ok(Ok(Some(0))) if attempt < 4 => tokio::time::sleep(Duration::from_millis(50)).await,
            Ok(Ok(Some(0))) => {
                tracing::debug!("cancelled Neo4j search read had already finished");
                return true;
            }
            Ok(Ok(Some(_))) => return true,
            Ok(Ok(None)) => {
                tracing::error!("Neo4j did not confirm cancellation of a search read");
                if attempt == 4 {
                    return false;
                }
            }
            Ok(Err(error)) => {
                tracing::error!(
                    error_kind = crate::telemetry::driver_error_kind(&error),
                    "failed to terminate a cancelled Neo4j search read"
                );
                if attempt == 4 {
                    return false;
                }
            }
            Err(_) => {
                tracing::error!("timed out terminating a cancelled Neo4j search read");
                if attempt == 4 {
                    return false;
                }
            }
        }
    }
    false
}

enum ReadFailure {
    /// The server reported the failure; it has already discarded the statement.
    Server(BackendError),
    /// The client failed while rows may still be streaming from the server.
    Client(BackendError),
}

impl Neo4jGraphBackend {
    /// Run a caller-supplied read with the adapter deadline and cancellation cleanup.
    /// The caller must enforce read-only access and bound returned rows.
    pub async fn execute_cancellable_read(
        &self,
        statement: &str,
        parameters: &Value,
    ) -> Result<Vec<Map<String, Value>>, BackendError> {
        self.execute_search_read(statement, parameters).await
    }

    /// One auto-commit read within the adapter deadline. Success and server-side
    /// failures disarm the guard; a client-side failure or an expired deadline
    /// leaves it armed so the marked server work is terminated on drop.
    #[tracing::instrument(name = "neo4j.search_read", skip_all)]
    pub(crate) async fn execute_search_read(
        &self,
        statement: &str,
        parameters: &Value,
    ) -> Result<Vec<Map<String, Value>>, BackendError> {
        let mut observation = crate::telemetry::Operation::new("search_read");
        let result = async {
            self.search_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let marker = format!("/* kg_search:{} */ ", uuid::Uuid::new_v4());
            let mut guard = CancelRead {
                control: Arc::clone(&self.cancellation_graph),
                marker: Some(marker.clone()),
                ticket: Some(self.cleanup.register()?),
            };
            let driver =
                |e| crate::driver::CallError::Driver(e).into_backend("Neo4j search read failed");
            let read = async {
                let query = build_query(&format!("{marker}{statement}"), parameters);
                let mut stream = self
                    .graph
                    .execute_once(query)
                    .await
                    .map_err(|e| server_or_client(e, driver))?;
                let mut rows = Vec::new();
                loop {
                    match stream.next().await {
                        Ok(Some(row)) => rows.push(row_to_json(&row).map_err(|e| {
                            ReadFailure::Client(e.into_backend("invalid Neo4j search row"))
                        })?),
                        Ok(None) => break,
                        Err(e) => return Err(server_or_client(e, driver)),
                    }
                }
                Ok(rows)
            };
            match tokio::time::timeout(self.options.timeout, read).await {
                Ok(Ok(rows)) => {
                    guard.disarm();
                    Ok(rows)
                }
                Ok(Err(ReadFailure::Server(error))) => {
                    guard.disarm();
                    Err(error)
                }
                Ok(Err(ReadFailure::Client(error))) => Err(error),
                Err(_) => Err(BackendError::Timeout(
                    self.options.timeout.as_millis() as u64
                )),
            }
        }
        .await;
        observation.finish(match &result {
            Ok(value) => {
                let _ = value;
                "success"
            }
            Err(error) => crate::telemetry::backend_error_kind(error),
        });
        result
    }

    /// Search reads issued so far by this backend; tests use it to pin
    /// database-call counts.
    pub fn search_reads(&self) -> u64 {
        self.search_reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn server_or_client(
    error: neo4rs::Error,
    convert: impl Fn(neo4rs::Error) -> BackendError,
) -> ReadFailure {
    if matches!(error, neo4rs::Error::Neo4j(_)) {
        ReadFailure::Server(convert(error))
    } else {
        ReadFailure::Client(convert(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "live-tests")]
    use serde_json::json;

    #[tokio::test(start_paused = true)]
    async fn close_bounds_the_wait_rejects_new_reads_and_can_be_retried() {
        let tracker = Arc::new(CleanupTracker::default());
        let mut ticket = tracker.register().unwrap();
        assert!(matches!(
            tracker.drain(Duration::from_secs(1)).await,
            Err(BackendError::Timeout(1000))
        ));
        assert!(tracker.register().is_err());
        ticket.success = true;
        drop(ticket);
        tracker.drain(Duration::from_secs(1)).await.unwrap();
        assert_eq!(tracker.state.lock().unwrap().active, 0);
    }

    #[tokio::test]
    async fn dropped_cleanup_is_reported_and_close_waits_for_registered_work() {
        let tracker = Arc::new(CleanupTracker::default());
        let ticket = tracker.register().unwrap();
        let closing = Arc::clone(&tracker);
        let close = tokio::spawn(async move { closing.drain(Duration::from_secs(1)).await });
        tokio::task::yield_now().await;
        assert!(!close.is_finished());
        drop(ticket); // Includes runtime cancellation/panic of a cleanup future.
        assert!(matches!(
            close.await.unwrap(),
            Err(BackendError::Unavailable(_))
        ));
    }

    #[cfg(feature = "live-tests")]
    #[tokio::test]
    #[ignore = "live: Neo4j"]
    async fn cancellation_burst_queues_without_spending_attempt_deadlines_and_close_drains() {
        use kg_core::traits::GraphBackend;
        let backend = backend().await;
        // Hold both cleanup execution slots longer than an attempt deadline.
        let slots = backend.cleanup.slots.acquire_many(2).await.unwrap();
        let mut tasks = Vec::new();
        let mut markers = Vec::new();
        for _ in 0..4 {
            let marker = format!("/* burst_test:{} */", uuid::Uuid::new_v4());
            let query = format!(
                "{marker} UNWIND range(1,100000000) AS i RETURN sum(sin(toFloat(i))) AS total"
            );
            let running = Arc::clone(&backend);
            tasks.push(tokio::spawn(async move {
                running.execute_search_read(&query, &json!({})).await
            }));
            markers.push(marker);
        }
        for marker in &markers {
            let mut seen = false;
            for _ in 0..100 {
                let (statement, params) = observe(marker);
                if !backend
                    .execute_read(statement, &params)
                    .await
                    .unwrap()
                    .is_empty()
                {
                    seen = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(seen, "each expensive query must start");
        }
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            assert!(task.await.unwrap_err().is_cancelled());
        }
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert_eq!(backend.cleanup.state.lock().unwrap().active, 4);
        assert_eq!(backend.cleanup.state.lock().unwrap().failed, 0);
        let closing = Arc::clone(&backend);
        let close = tokio::spawn(async move { closing.close().await });
        tokio::task::yield_now().await;
        assert!(!close.is_finished());
        drop(slots);
        close.await.unwrap().unwrap();
        for marker in &markers {
            wait_until_gone(&backend, marker, Duration::from_secs(2)).await;
        }
        assert_eq!(backend.cleanup.state.lock().unwrap().active, 0);
        assert!(backend
            .execute_search_read("RETURN 1", &json!({}))
            .await
            .is_err());
    }

    #[cfg(feature = "live-tests")]

    async fn backend() -> Arc<Neo4jGraphBackend> {
        Arc::new({
            let env = kg_neo4j_testkit::env::neo4j().unwrap_or_else(|e| panic!("{e}"));
            Neo4jGraphBackend::new(&env.uri, &env.user, &env.password)
                .await
                .unwrap()
        })
    }

    #[cfg(feature = "live-tests")]

    fn observe(marker: &str) -> (&'static str, Value) {
        (
            // The adapter prefixes its own marker, so the test marker is inside the text.
            "SHOW TRANSACTIONS YIELD transactionId,currentQuery,status WHERE currentQuery CONTAINS $marker AND NOT status STARTS WITH 'Terminated' RETURN transactionId",
            json!({"marker": marker}),
        )
    }

    #[cfg(feature = "live-tests")]

    async fn wait_until_gone(backend: &Neo4jGraphBackend, marker: &str, within: Duration) {
        let start = std::time::Instant::now();
        loop {
            let (statement, params) = observe(marker);
            if backend
                .execute_read(statement, &params)
                .await
                .unwrap()
                .is_empty()
            {
                return;
            }
            assert!(
                start.elapsed() < within,
                "server query survived cancellation"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[cfg(feature = "live-tests")]
    #[tokio::test]
    #[ignore = "live: Neo4j"]
    async fn dropped_search_terminates_server_work_and_pool_remains_usable() {
        let backend = backend().await;
        let marker = format!("/* cancellation_test:{} */", uuid::Uuid::new_v4());
        let query =
            format!("{marker} UNWIND range(1,100000000) AS i RETURN sum(sin(toFloat(i))) AS total");
        let running = Arc::clone(&backend);
        let task =
            tokio::spawn(async move { running.execute_search_read(&query, &json!({})).await });
        let mut seen = false;
        for _ in 0..100 {
            let (statement, params) = observe(&marker);
            if !backend
                .execute_read(statement, &params)
                .await
                .unwrap()
                .is_empty()
            {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            seen,
            "expensive query must actually start before cancellation"
        );
        // Keep a separate healthy query alive while cancellation settles.
        let healthy_marker = format!("/* healthy_test:{} */", uuid::Uuid::new_v4());
        let healthy_query = format!(
            "{healthy_marker} UNWIND range(1,1000000000) AS i RETURN sum(sin(toFloat(i))) AS total"
        );
        let running = Arc::clone(&backend);
        let healthy = tokio::spawn(async move {
            running
                .execute_search_read(&healthy_query, &json!({}))
                .await
        });
        let mut healthy_seen = false;
        for _ in 0..100 {
            let (statement, params) = observe(&healthy_marker);
            if !backend
                .execute_read(statement, &params)
                .await
                .unwrap()
                .is_empty()
            {
                healthy_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(healthy_seen, "unrelated healthy query actually started");
        let slots = backend.cleanup.slots.acquire_many(2).await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let settling = Arc::clone(&backend);
        let settled = tokio::spawn(async move { settling.cleanup.wait_idle().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !settled.is_finished(),
            "admission must wait for pending cleanup"
        );
        drop(slots);
        tokio::time::timeout(Duration::from_secs(5), settled)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !healthy.is_finished(),
            "healthy reads must not delay settlement"
        );
        wait_until_gone(&backend, &marker, Duration::from_secs(2)).await;
        healthy.abort();
        assert!(healthy.await.unwrap_err().is_cancelled());
        backend.cleanup.wait_idle().await;
        wait_until_gone(&backend, &healthy_marker, Duration::from_secs(2)).await;
        // Every pooled connection must still answer after the terminated read.
        for value in 42..62 {
            let rows = backend
                .execute_search_read("RETURN $value AS value", &json!({"value": value}))
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                1,
                "cancelled replies must not become empty results"
            );
            assert_eq!(rows[0]["value"], value, "reply belongs to this query");
        }
    }

    #[cfg(feature = "live-tests")]
    #[tokio::test]
    #[ignore = "live: Neo4j"]
    async fn adapter_deadline_terminates_server_work_and_reports_its_own_budget() {
        let backend = Arc::new(
            Neo4jGraphBackend::with_options(
                &kg_neo4j_testkit::env::neo4j()
                    .unwrap_or_else(|e| panic!("{e}"))
                    .uri,
                &kg_neo4j_testkit::env::neo4j()
                    .unwrap_or_else(|e| panic!("{e}"))
                    .user,
                &kg_neo4j_testkit::env::neo4j()
                    .unwrap_or_else(|e| panic!("{e}"))
                    .password,
                crate::Neo4jOptions {
                    timeout: Duration::from_millis(300),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let marker = format!("/* deadline_test:{} */", uuid::Uuid::new_v4());
        let query =
            format!("{marker} UNWIND range(1,100000000) AS i RETURN sum(sin(toFloat(i))) AS total");
        let error = backend
            .execute_search_read(&query, &json!({}))
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Timeout(300)), "{error:?}");
        wait_until_gone(&backend, &marker, Duration::from_secs(2)).await;
        assert_eq!(
            backend
                .execute_search_read("RETURN 1 AS value", &json!({}))
                .await
                .unwrap()[0]["value"],
            1
        );
    }

    #[cfg(feature = "live-tests")]
    #[tokio::test]
    #[ignore = "live: Neo4j"]
    async fn server_failures_do_not_spawn_termination_and_guards_survive_without_a_runtime() {
        let backend = backend().await;
        let before = backend.search_reads();
        let error = backend
            .execute_search_read("RETURN 1/0 AS boom", &json!({}))
            .await
            .unwrap_err();
        assert!(!matches!(error, BackendError::Timeout(_)), "{error:?}");
        assert_eq!(backend.search_reads(), before + 1);
        // A guard dropped where no runtime exists must log, not panic.
        let armed = CancelRead {
            control: Arc::clone(&backend.cancellation_graph),
            marker: Some("/* never_started */".into()),
            ticket: Some(backend.cleanup.register().unwrap()),
        };
        std::thread::spawn(move || drop(armed)).join().unwrap();
        assert_eq!(
            backend
                .execute_search_read("RETURN 2 AS value", &json!({}))
                .await
                .unwrap()[0]["value"],
            2
        );
    }
}
