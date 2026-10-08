use crate::{ConfigurationError, EngineClosedError, ForkedEngineError, InputValidationError};
use kg_core::{models::IngestionInput, saga::ThreadReference, traits::ontology_store::Ontology};
use kg_stages::{CommunityMaintenanceRequest, Engine, IngestionRequest, SagaMaintenanceRequest};
use pyo3::prelude::*;
use std::{
    sync::{mpsc, Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken as Token;
use uuid::Uuid;

#[pyclass(module = "kg_sdk._native", frozen)]
pub struct CancellationToken {
    token: Token,
    pid: u32,
}
#[pymethods]
impl CancellationToken {
    #[new]
    fn new() -> Self {
        Self {
            token: Token::new(),
            pid: std::process::id(),
        }
    }
    fn cancel(&self) -> PyResult<()> {
        check_pid(self.pid)?;
        self.token.cancel();
        Ok(())
    }
    #[getter]
    fn cancelled(&self) -> PyResult<bool> {
        check_pid(self.pid)?;
        Ok(self.token.is_cancelled())
    }
}
fn check_pid(pid: u32) -> PyResult<()> {
    if pid != std::process::id() {
        return Err(ForkedEngineError::new_err(
            "create a new engine/token after fork",
        ));
    }
    Ok(())
}
struct State {
    runtime: Option<Runtime>,
    engine: Option<Arc<Engine>>,
    graph: Option<Arc<kg_storage_neo4j::Neo4jGraphBackend>>,
    active: usize,
    closing: bool,
}
struct Shared {
    state: Mutex<State>,
    settled: Condvar,
    cancel: Token,
    permits: Arc<tokio::sync::Semaphore>,
}
struct Active(Arc<Shared>);
impl Drop for Active {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.active -= 1;
        self.0.settled.notify_all();
    }
}
#[pyclass(module = "kg_sdk._native", frozen)]
pub struct NativeEngine {
    shared: Arc<Shared>,
    pid: u32,
    ontology_org: Option<String>,
    response_limit: usize,
    request_limit: usize,
}

fn parse<T: serde::de::DeserializeOwned>(raw: &str) -> PyResult<T> {
    serde_json::from_str(raw).map_err(|e| {
        InputValidationError::new_err(format!(
            "invalid Rust input schema at line {}, column {} ({:?})",
            e.line(),
            e.column(),
            e.classify()
        ))
    })
}
#[pymethods]
impl NativeEngine {
    #[new]
    #[pyo3(signature=(configuration, ontology=None, ontology_org=None, workers=2, admitted=4, blocking=16, response_limit=67108864, request_limit=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        configuration: &str,
        ontology: Option<&str>,
        ontology_org: Option<String>,
        workers: usize,
        admitted: usize,
        blocking: usize,
        response_limit: usize,
        request_limit: usize,
    ) -> PyResult<Self> {
        if !(1..=1073741824).contains(&request_limit) {
            return Err(ConfigurationError::new_err(
                "request limit must be between 1 byte and 1 GiB",
            ));
        }
        if configuration.len() > request_limit
            || ontology.is_some_and(|value| value.len() > request_limit)
        {
            return Err(ConfigurationError::new_err(
                "configuration exceeds request byte limit",
            ));
        }
        if !(4096..=1073741824).contains(&response_limit) {
            return Err(ConfigurationError::new_err(
                "response limit must be between 4096 bytes and 1 GiB",
            ));
        }
        if workers == 0
            || workers > 64
            || admitted == 0
            || admitted > 1024
            || blocking == 0
            || blocking > 1024
        {
            return Err(ConfigurationError::new_err("invalid runtime limits"));
        }
        let config: crate::config::ApplicationConfig = parse(configuration)
            .map_err(|_| ConfigurationError::new_err("invalid engine configuration schema"))?;
        config.validate().map_err(ConfigurationError::new_err)?;
        if ontology.is_some() != ontology_org.as_ref().is_some_and(|s| !s.trim().is_empty())
            || (ontology.is_none() && ontology_org.is_some())
        {
            return Err(ConfigurationError::new_err(
                "ontology and nonblank ontology_org_id must be supplied together",
            ));
        }
        let ontology: Option<Ontology> = ontology
            .map(parse)
            .transpose()
            .map_err(|_| ConfigurationError::new_err("invalid ontology schema"))?;
        let runtime = Builder::new_multi_thread()
            .thread_name("kg-sdk")
            .worker_threads(workers)
            .max_blocking_threads(blocking)
            .enable_all()
            .build()
            .map_err(|_| ConfigurationError::new_err("could not create engine runtime"))?;
        let org = ontology_org.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        let rx = Mutex::new(rx);
        let task = runtime.spawn(async move {
            let _ = tx.send(crate::bootstrap::build(config, ontology, org).await);
        });
        let built = loop {
            match py.detach(|| rx.lock().unwrap().recv_timeout(Duration::from_millis(20))) {
                Ok(value) => break value,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break Err("bootstrap worker failed".into())
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(signal) = py.check_signals() {
                        task.abort();
                        py.detach(move || drop(runtime));
                        return Err(signal);
                    }
                }
            }
        };
        let (engine, graph) = match built {
            Ok(engine) => engine,
            Err(message) => {
                py.detach(move || drop(runtime));
                return Err(ConfigurationError::new_err(message));
            }
        };
        Ok(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    runtime: Some(runtime),
                    engine: Some(Arc::new(engine)),
                    graph: Some(graph),
                    active: 0,
                    closing: false,
                }),
                settled: Condvar::new(),
                cancel: Token::new(),
                permits: Arc::new(tokio::sync::Semaphore::new(admitted)),
            }),
            pid: std::process::id(),
            ontology_org,
            response_limit,
            request_limit,
        })
    }

    #[pyo3(signature=(operation, payload, org_id, run_id, cancellation=None, timeout=None, trace_id=None))]
    #[allow(clippy::too_many_arguments)]
    fn run(
        &self,
        py: Python<'_>,
        operation: &str,
        payload: &str,
        org_id: String,
        run_id: &str,
        cancellation: Option<&CancellationToken>,
        timeout: Option<f64>,
        trace_id: Option<String>,
    ) -> PyResult<String> {
        check_pid(self.pid)?;
        let started = Instant::now();
        let duration = timeout
            .map(|v| {
                Duration::try_from_secs_f64(v)
                    .ok()
                    .filter(|d| !d.is_zero())
                    .ok_or_else(|| {
                        InputValidationError::new_err("timeout must be positive and finite")
                    })
            })
            .transpose()?;
        let deadline = duration
            .map(|d| {
                started
                    .checked_add(d)
                    .ok_or_else(|| InputValidationError::new_err("timeout exceeds supported range"))
            })
            .transpose()?;
        let run_id = Uuid::parse_str(run_id)
            .ok()
            .filter(|id| !id.is_nil())
            .ok_or_else(|| InputValidationError::new_err("run_id must be a non-nil UUID"))?;
        if org_id.trim().is_empty() || self.ontology_org.as_ref().is_some_and(|org| org != &org_id)
        {
            return Err(InputValidationError::new_err(
                "invalid organization for engine",
            ));
        }
        let caller = match cancellation {
            Some(token) => {
                check_pid(token.pid)?;
                token.token.clone()
            }
            None => Token::new(),
        };
        if self.shared.cancel.is_cancelled() {
            return Err(EngineClosedError::new_err("engine is closed"));
        }
        if payload.len() > self.request_limit {
            return Err(InputValidationError::new_err("request byte limit exceeded"));
        }
        if caller.is_cancelled() || deadline.is_some_and(|at| Instant::now() >= at) {
            return Ok(crate::outcome::wire(
                &crate::outcome::encode(Err(kg_core::errors::PipelineError::Cancelled), run_id),
                self.response_limit,
            ));
        }
        // Python waits before encoding. Direct native callers fail fast on
        // saturation so they cannot queue an unbounded set of decoded requests.
        let permit = match self.shared.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return Ok(crate::outcome::wire(
                    &serde_json::json!({
                        "ok": false, "result": {"run_id": run_id,
                            "cause": "capacity_exceeded", "committed": {},
                            "batches_committed": 0, "commit_unknown": false, "retriable": true}
                    }),
                    self.response_limit,
                ))
            }
        };
        let request = match operation {
            "ingest" => Operation::Ingest(parse(payload)?),
            "ingest_profiles" => Operation::IngestProfiles(parse(payload)?),
            "profiles" => Operation::Profiles(parse(payload)?),
            "checkpoints" => Operation::Checkpoints(parse(payload)?),
            "decide" => Operation::Decide(parse(payload)?),
            "community" => Operation::Community(parse(payload)?),
            "saga" => Operation::Saga(parse(payload)?),
            "rules" => {
                let request: crate::rules::Request = parse(payload)?;
                if !request.valid() {
                    return Err(InputValidationError::new_err("source must be nonblank"));
                }
                Operation::Rules(request)
            }
            _ => return Err(InputValidationError::new_err("unknown engine operation")),
        };
        let cancel = self.shared.cancel.child_token();
        let signal_cancel = cancel.clone();
        let shared = self.shared.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        let rx = Mutex::new(rx);
        {
            let mut state = shared.state.lock().unwrap();
            if state.closing {
                return Err(EngineClosedError::new_err("engine is closed"));
            }
            let engine = state.engine.as_ref().unwrap().clone();
            let graph = state.graph.as_ref().unwrap().clone();
            state.active += 1;
            let active = Active(shared.clone());
            state.runtime.as_ref().unwrap().spawn(async move {
                let _active = active;
                let _permit = permit;
                let expired = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at.into()).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::pin!(expired);
                let result = if !cancel.is_cancelled()
                    && !caller.is_cancelled()
                    && deadline.is_none_or(|at| Instant::now() < at)
                {
                    let operation = async {
                        match request {
                            Operation::Checkpoints(spec)=>crate::checkpoints::run(graph.as_ref(),&org_id,spec,&cancel).await,
                            Operation::Profiles(spec)=>crate::profiles::run(graph.as_ref(),engine.as_ref(),&org_id,spec,&cancel).await,
                            Operation::Decide(spec)=> {
                                tokio::select! {
                                    _=cancel.cancelled()=>serde_json::json!({"ok":false,"result":{"cause":"cancelled","retriable":true}}),
                                    result=engine.decide(&spec.state,&spec.questions)=>match result {
                                        Ok(value)=>serde_json::json!({"ok":true,"result":value}),
                                        Err(error)=>serde_json::json!({"ok":false,"result":{"cause":crate::outcome::backend_kind(&error),"retriable":error.is_transient()}}),
                                    }
                                }
                            },
                            Operation::IngestProfiles(spec)=> {
                                let result=async {
                                    if let Some(expected)=&spec.expected {crate::profiles::verify(graph.as_ref(),engine.as_ref(),&org_id,run_id,&spec.bindings,expected,&cancel).await?;}
                                    engine.ingest_with_profiles(IngestionRequest{org_id,snapshots:spec.inputs,run_id:Some(run_id),cancel:Some(cancel.clone()),trace_id},spec.bindings).await
                                }.await;
                                crate::outcome::encode(result,run_id)
                            },
                            Operation::Rules(spec) => {
                                crate::rules::run(
                                    graph.as_ref(),
                                    engine.as_ref(),
                                    spec,
                                    &org_id,
                                    run_id,
                                    deadline,
                                    &cancel,
                                )
                                .await
                            }
                            Operation::Ingest(snapshots) => crate::outcome::encode(
                                engine
                                    .ingest(IngestionRequest {
                                        org_id,
                                        snapshots,
                                        run_id: Some(run_id),
                                        cancel: Some(cancel.clone()),
                                        trace_id,
                                    })
                                    .await,
                                run_id,
                            ),
                            Operation::Community(namespace) => crate::outcome::encode(
                                engine
                                    .rebuild_communities(CommunityMaintenanceRequest {
                                        org_id,
                                        namespace,
                                        run_id: Some(run_id),
                                        cancel: Some(cancel.clone()),
                                        trace_id,
                                    })
                                    .await,
                                run_id,
                            ),
                            Operation::Saga(spec) => crate::outcome::encode(
                                engine
                                    .summarize_saga(SagaMaintenanceRequest {
                                        org_id,
                                        namespace: spec.namespace,
                                        saga: spec.saga,
                                        run_id: Some(run_id),
                                        cancel: Some(cancel.clone()),
                                        trace_id,
                                    })
                                    .await,
                                run_id,
                            ),
                        }
                    };
                    tokio::pin!(operation);
                    tokio::select! { biased;
                        result = &mut operation => result,
                        _ = caller.cancelled() => { cancel.cancel(); operation.await },
                        _ = &mut expired => { cancel.cancel(); operation.await },
                    }
                } else {
                    crate::outcome::encode(
                        Err(kg_core::errors::PipelineError::Cancelled),
                        run_id,
                    )
                };
                // Release execution capacity before waking Python; otherwise a
                // sequential next call could spuriously observe saturation.
                drop(_permit);
                let _ = tx.send(result);
            });
        }
        let mut interrupted = false;
        let mut signal_error: Option<PyErr> = None;
        let mut value = loop {
            match py.detach(|| rx.lock().unwrap().recv_timeout(Duration::from_millis(20))) {
                Ok(value) => break value,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break serde_json::json!({"ok":false,"result":{"run_id":run_id,"cause":"worker_failed","commit_unknown":true,"retriable":false}})
                }
                Err(mpsc::RecvTimeoutError::Timeout) => (),
            }
            if let Err(error) = py.check_signals() {
                interrupted = true;
                signal_cancel.cancel();
                if signal_error.is_none() {
                    signal_error = Some(error);
                }
            }
        };
        if let Err(error) = py.check_signals() {
            interrupted = true;
            signal_cancel.cancel();
            if signal_error.is_none() {
                signal_error = Some(error);
            }
        }
        if let Some(error) = signal_error {
            if !error.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py) {
                error.value(py).setattr(
                    "outcome_json",
                    crate::outcome::wire(&value, self.response_limit),
                )?;
                return Err(error);
            }
        }
        value["interrupted"] = serde_json::json!(interrupted);
        Ok(crate::outcome::wire(&value, self.response_limit))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        check_pid(self.pid)?;
        py.detach(|| self.shutdown());
        Ok(())
    }
}
impl NativeEngine {
    fn shutdown(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closing = true;
        self.shared.cancel.cancel();
        while state.active != 0 {
            state = self.shared.settled.wait(state).unwrap();
        }
        let runtime = state.runtime.take();
        let engine = state.engine.take();
        let graph = state.graph.take();
        drop(state);
        if let Some(runtime) = runtime {
            let _entered = runtime.enter();
            drop(engine);
            drop(graph);
            drop(_entered);
            drop(runtime);
        }
    }
}
impl Drop for NativeEngine {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            self.shutdown();
        } else {
            // Inherited Tokio workers do not exist in the child; never join them.
            let shared = std::mem::replace(
                &mut self.shared,
                Arc::new(Shared {
                    state: Mutex::new(State {
                        runtime: None,
                        engine: None,
                        graph: None,
                        active: 0,
                        closing: true,
                    }),
                    settled: Condvar::new(),
                    cancel: Token::new(),
                    permits: Arc::new(tokio::sync::Semaphore::new(0)),
                }),
            );
            std::mem::forget(shared);
        }
    }
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileIngest {
    inputs: Vec<IngestionInput>,
    bindings: kg_core::profiles::ProfileBindings,
    expected: Option<kg_core::runtime::schemas::RunSchemaManifest>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DecideSpec {
    state: serde_json::Value,
    questions: std::collections::BTreeMap<String, kg_core::traits::Question>,
}
enum Operation {
    IngestProfiles(ProfileIngest),
    Profiles(crate::profiles::Request),
    Checkpoints(crate::checkpoints::Request),
    Decide(DecideSpec),
    Ingest(Vec<IngestionInput>),
    Community(String),
    Saga(SagaSpec),
    Rules(crate::rules::Request),
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SagaSpec {
    namespace: String,
    saga: ThreadReference,
}

#[pyclass(module = "kg_sdk._native", frozen)]
pub struct TelemetryHandle {
    guard: Mutex<Option<(crate::telemetry::TelemetryGuard, Runtime)>>,
    pid: u32,
}
#[pymethods]
impl TelemetryHandle {
    #[new]
    fn new(py: Python<'_>, config: &str) -> PyResult<Self> {
        let config: crate::telemetry::TelemetryConfig = parse(config)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|_| ConfigurationError::new_err("telemetry runtime failed"))?;
        let result = py.detach(|| {
            let _entered = runtime.enter();
            crate::telemetry::init(&config)
        });
        match result {
            Ok(guard) => Ok(Self {
                guard: Mutex::new(Some((guard, runtime))),
                pid: std::process::id(),
            }),
            Err(_) => {
                py.detach(move || drop(runtime));
                Err(ConfigurationError::new_err(
                    "telemetry setup failed; check settings and existing subscriber",
                ))
            }
        }
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        check_pid(self.pid)?;
        // Both mutex acquisition and shutdown happen without the GIL. Keep
        // the lock until shutdown settles so concurrent close callers wait too.
        py.detach(|| {
            if let Some((guard, runtime)) = self.guard.lock().unwrap().take() {
                runtime.block_on(guard.shutdown());
                drop(runtime);
            }
        });
        Ok(())
    }
}

impl Drop for TelemetryHandle {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            // Do not touch an inherited mutex or runtime after fork.
            let inherited = std::mem::replace(&mut self.guard, Mutex::new(None));
            std::mem::forget(inherited);
            return;
        }
        if let Some((guard, runtime)) = self.guard.get_mut().unwrap().take() {
            runtime.block_on(guard.shutdown());
        }
    }
}
