use std::collections::VecDeque;
use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tracing::Instrument;

use kg_core::errors::{PipelineError, StageError};
use kg_core::pipeline::SnapshotFailure;
use kg_core::runtime::RuntimeContext;
use kg_core::traits::{Stage, StageKind};

use crate::message::PipelineMessage;

/// Owns bounded stage futures and completed outputs. A blocked downstream
/// channel must not stop polling provider calls that may hold shared permits.
pub async fn run_stage_worker(
    stage: Arc<dyn Stage>,
    ctx: Arc<RuntimeContext>,
    mut input: mpsc::Receiver<PipelineMessage>,
    output: mpsc::Sender<PipelineMessage>,
    concurrency: usize,
) -> Result<Vec<SnapshotFailure>, PipelineError> {
    if concurrency == 0 {
        return Err(PipelineError::StateValidation {
            stage: stage.name().into(),
            message: "stage concurrency must be positive".into(),
        });
    }
    let mut tasks = FuturesUnordered::new();
    let mut pending = VecDeque::new();
    let mut failures = Vec::new();
    let mut input_open = true;

    loop {
        if ctx.cancel.is_cancelled() {
            return Err(PipelineError::Cancelled);
        }
        if !input_open && tasks.is_empty() && pending.is_empty() {
            return Ok(failures);
        }
        tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(PipelineError::Cancelled),
            msg = input.recv(), if input_open && tasks.len() + pending.len() < concurrency => {
                let Some(msg) = msg else { input_open = false; continue; };
                let stage = stage.clone();
                let mut scoped = ctx.as_ref().clone();
                scoped.snapshot_index = Some(msg.snapshot_index);
                let ctx = Arc::new(scoped);
                let stage_span = tracing::info_span!("pipeline.stage", stage = stage.name(), snapshot = msg.snapshot_index, otel.status_code = tracing::field::Empty);
                tasks.push(async move {
                    let stage_name = stage.name().to_owned();
                    let mut measurement = kg_core::telemetry::OperationGuard::stage(&stage_name);
                    let started = std::time::Instant::now();
                    let input_kind = StageKind::of(&msg.state);
                    let result = if stage.contract().iter().any(|(input, _)| *input == input_kind) {
                        stage.process(msg.state, &ctx).await.and_then(|output| {
                            if stage.contract().contains(&(input_kind, StageKind::of(&output))) {
                                Ok(output)
                            } else {
                                Err(StageError::StateValidation {
                                    stage: stage.name().into(),
                                    message: "stage returned an undeclared payload".into(),
                                })
                            }
                        })
                    } else {
                        Err(StageError::StateValidation {
                            stage: stage.name().into(),
                            message: "stage received an unsupported payload".into(),
                        })
                    };
                    measurement.finish(match &result {
                        Ok(_) => kg_core::telemetry::Outcome::Success,
                        Err(error) => kg_core::telemetry::Outcome::from(error),
                    });
                    tracing::Span::current().record("otel.status_code", if result.is_ok() { "OK" } else { "ERROR" });
                    tracing::debug!(stage = %stage_name, run_id = %msg.run_id, snapshot = msg.snapshot_index,
                        outcome = if result.is_ok() { "success" } else { "error" },
                        elapsed_ms = started.elapsed().as_millis(), "stage finished");
                    result.map(|state| PipelineMessage {
                        snapshot_index: msg.snapshot_index, run_id: msg.run_id, state,
                    }).map_err(|error| (msg.snapshot_index, stage_name, error))
                }.instrument(stage_span));
            }
            permit = output.reserve(), if !pending.is_empty() => {
                let permit = permit.map_err(|_| PipelineError::Other("downstream channel closed".into()))?;
                permit.send(pending.pop_front().expect("nonempty output queue"));
            }
            Some(result) = tasks.next(), if !tasks.is_empty() => {
                match result {
                    Ok(message) => pending.push_back(message),
                    Err((_, _, StageError::Cancelled { .. })) => return Err(PipelineError::Cancelled),
                    Err((_, _, StageError::IdentityRevisionChanged)) => return Err(PipelineError::IdentityRevisionChanged),
                    Err((snapshot_index, stage, error)) => {
                        let failure = SnapshotFailure {
                            profile: error.profile_violation(),
                            snapshot_index, stage, error: error.to_string(), retriable: error.is_retriable(),
                        };
                        if ctx.exec_config.continue_on_step_error {
                            failures.push(failure);
                        } else {
                            return Err(PipelineError::StepExecution {
                                step: failure.stage.clone(), stage: failure.stage,
                                cause: format!("snapshot {}: {}", failure.snapshot_index, failure.error),
                                retriable: failure.retriable,
                            });
                        }
                    }
                }
            }
        }
    }
}
