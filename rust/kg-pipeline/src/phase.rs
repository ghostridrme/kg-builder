use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;

use kg_core::errors::{PipelineError, StageError};
use kg_core::pipeline::SnapshotFailure;
use kg_core::runtime::RuntimeContext;
use kg_core::traits::{Stage, StageKind};

use crate::message::PipelineMessage;
use crate::worker::run_stage_worker;

/// Default bounded channel capacity between stages.
const DEFAULT_CHANNEL_CAPACITY: usize = 64;

/// Default per-stage concurrency (how many snapshots a stage processes in parallel).
const DEFAULT_STAGE_CONCURRENCY: usize = 10;

/// Runs concurrent stage segments separated by explicit chunk-wide batch barriers.
///
/// Ordinary stages are connected by bounded mpsc channels for backpressure.
/// A batch stage receives all surviving snapshots in input order and returns
/// one result per snapshot before the next concurrent segment starts.
///
/// ```text
/// inputs → [concurrent segment] → [batch barrier] → [concurrent segment] → outputs
/// ```
///
/// Returns the surviving messages plus the per-snapshot failures collected by
/// the workers (non-empty only with `continue_on_step_error = true`). On the
/// first worker error every worker is aborted and the feeder and collector
/// are joined before the error returns; no task outlives the call. Every
/// task runs inside the caller's span, so stage logs keep the run's fields.
pub async fn run_phase(
    stages: &[Arc<dyn Stage>],
    mut inputs: Vec<PipelineMessage>,
    ctx: Arc<RuntimeContext>,
    channel_capacity: Option<usize>,
    stage_concurrency: Option<usize>,
) -> Result<(Vec<PipelineMessage>, Vec<SnapshotFailure>), PipelineError> {
    let mut failures = Vec::new();
    let mut start = 0;
    for (index, stage) in stages.iter().enumerate() {
        if !stage.is_batch() {
            continue;
        }
        let (survivors, segment_failures) = run_stream_segment(
            &stages[start..index],
            inputs,
            ctx.clone(),
            channel_capacity,
            stage_concurrency,
        )
        .await?;
        failures.extend(segment_failures);
        inputs = survivors;
        inputs.sort_by_key(|message| message.snapshot_index);
        if !inputs.is_empty() {
            let metadata: Vec<_> = inputs
                .iter()
                .map(|m| (m.snapshot_index, m.run_id))
                .collect();
            let input_kinds: Vec<_> = inputs.iter().map(|m| StageKind::of(&m.state)).collect();
            if input_kinds
                .iter()
                .any(|kind| !stage.contract().iter().any(|(input, _)| input == kind))
            {
                return Err(PipelineError::StateValidation {
                    stage: stage.name().into(),
                    message: "batch stage received an unsupported payload".into(),
                });
            }
            let states = inputs.into_iter().map(|m| m.state).collect();
            let batch_stage = stage.clone();
            let batch_ctx = ctx.clone();
            let started = std::time::Instant::now();
            let mut measurement = kg_core::telemetry::OperationGuard::stage(stage.name());
            let mut handle = AbortOnDropHandle::new(tokio::spawn(
                async move { batch_stage.process_batch(states, &batch_ctx).await }
                    .in_current_span(),
            ));
            let result = tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    measurement.finish(kg_core::telemetry::Outcome::Cancelled);
                    handle.abort();
                    let _ = handle.await;
                    return Err(PipelineError::Cancelled);
                },
                result = &mut handle => result.map_err(|error| PipelineError::TaskPanic(error.to_string()))?,
            };
            measurement.finish(match &result {
                Ok(_) => kg_core::telemetry::Outcome::Failed,
                Err(error) => kg_core::telemetry::Outcome::from(error),
            });
            tracing::debug!(
                stage = stage.name(),
                snapshots = metadata.len(),
                elapsed_ms = started.elapsed().as_millis(),
                outcome = if result.is_ok() { "success" } else { "error" },
                "batch stage finished"
            );
            match result {
                Ok(outputs) => {
                    if outputs.len() != metadata.len() {
                        return Err(PipelineError::StateValidation {
                            stage: stage.name().into(),
                            message: "batch stage must return one output per input in order".into(),
                        });
                    }
                    let input_count = outputs.len();
                    inputs = Vec::with_capacity(input_count);
                    for (((snapshot_index, run_id), input_kind), outcome) in
                        metadata.into_iter().zip(input_kinds).zip(outputs)
                    {
                        match outcome {
                            Ok(state) => {
                                if !stage
                                    .contract()
                                    .contains(&(input_kind, StageKind::of(&state)))
                                {
                                    return Err(PipelineError::StateValidation {
                                        stage: stage.name().into(),
                                        message: "batch stage returned an undeclared payload"
                                            .into(),
                                    });
                                }
                                inputs.push(PipelineMessage {
                                    snapshot_index,
                                    run_id,
                                    state,
                                });
                            }
                            Err(StageError::Cancelled { .. }) => {
                                return Err(PipelineError::Cancelled)
                            }
                            Err(StageError::IdentityRevisionChanged) => {
                                return Err(PipelineError::IdentityRevisionChanged)
                            }
                            Err(error) => {
                                if !ctx.exec_config.continue_on_step_error {
                                    return Err(PipelineError::StepExecution {
                                        step: stage.name().into(),
                                        stage: stage.name().into(),
                                        cause: error.to_string(),
                                        retriable: error.is_retriable(),
                                    });
                                }
                                failures.push(SnapshotFailure {
                                    profile: error.profile_violation(),
                                    snapshot_index,
                                    stage: stage.name().into(),
                                    error: error.to_string(),
                                    retriable: error.is_retriable(),
                                });
                            }
                        }
                    }
                    measurement.finish(if inputs.len() == input_count {
                        kg_core::telemetry::Outcome::Success
                    } else {
                        kg_core::telemetry::Outcome::IncompleteResponse
                    });
                }
                Err(StageError::Cancelled { .. }) => return Err(PipelineError::Cancelled),
                Err(StageError::IdentityRevisionChanged) => {
                    return Err(PipelineError::IdentityRevisionChanged);
                }
                Err(error) => {
                    if !ctx.exec_config.continue_on_step_error {
                        return Err(PipelineError::StepExecution {
                            step: stage.name().into(),
                            stage: stage.name().into(),
                            cause: error.to_string(),
                            retriable: error.is_retriable(),
                        });
                    }
                    failures.extend(metadata.into_iter().map(|(snapshot_index, _)| {
                        SnapshotFailure {
                            profile: error.profile_violation(),
                            snapshot_index,
                            stage: stage.name().into(),
                            error: error.to_string(),
                            retriable: error.is_retriable(),
                        }
                    }));
                    inputs = Vec::new();
                }
            }
        }
        start = index + 1;
    }
    let (outputs, segment_failures) = run_stream_segment(
        &stages[start..],
        inputs,
        ctx,
        channel_capacity,
        stage_concurrency,
    )
    .await?;
    failures.extend(segment_failures);
    failures.sort_by_key(|failure| failure.snapshot_index);
    Ok((outputs, failures))
}

async fn run_stream_segment(
    stages: &[Arc<dyn Stage>],
    inputs: Vec<PipelineMessage>,
    ctx: Arc<RuntimeContext>,
    channel_capacity: Option<usize>,
    stage_concurrency: Option<usize>,
) -> Result<(Vec<PipelineMessage>, Vec<SnapshotFailure>), PipelineError> {
    let capacity = channel_capacity.unwrap_or(DEFAULT_CHANNEL_CAPACITY);
    let concurrency = stage_concurrency.unwrap_or(DEFAULT_STAGE_CONCURRENCY);
    if !(1..=Semaphore::MAX_PERMITS).contains(&capacity) || concurrency == 0 {
        return Err(PipelineError::StateValidation {
            stage: "config".into(),
            message: "channel capacity must fit Tokio's semaphore and concurrency must be positive"
                .into(),
        });
    }
    if ctx.cancel.is_cancelled() {
        return Err(PipelineError::Cancelled);
    }
    if stages.is_empty() {
        return Ok((inputs, Vec::new()));
    }

    let mut channels: Vec<(
        mpsc::Sender<PipelineMessage>,
        mpsc::Receiver<PipelineMessage>,
    )> = Vec::with_capacity(stages.len() + 1);

    for _ in 0..=stages.len() {
        channels.push(mpsc::channel(capacity));
    }

    let mut senders: Vec<mpsc::Sender<PipelineMessage>> =
        channels.iter().map(|(tx, _)| tx.clone()).collect();
    let mut receivers: Vec<mpsc::Receiver<PipelineMessage>> =
        channels.into_iter().map(|(_, rx)| rx).collect();

    let input_tx = senders.remove(0);
    let output_rx = receivers.pop().unwrap();

    let mut worker_handles: JoinSet<Result<Vec<SnapshotFailure>, PipelineError>> = JoinSet::new();

    for stage in stages.iter() {
        let stage = stage.clone();
        let ctx = ctx.clone();
        let rx = receivers.remove(0);
        let tx = senders.remove(0);

        worker_handles.spawn(
            async move { run_stage_worker(stage, ctx, rx, tx, concurrency).await }
                .in_current_span(),
        );
    }

    // Feed inputs; a closed channel means a worker stopped, so the feeder stops too.
    let feed_handle = AbortOnDropHandle::new(tokio::spawn(
        async move {
            for msg in inputs {
                if input_tx.send(msg).await.is_err() {
                    break;
                }
            }
        }
        .in_current_span(),
    ));

    let collect_handle = AbortOnDropHandle::new(tokio::spawn(
        async move {
            let mut results: Vec<PipelineMessage> = Vec::new();
            let mut output_rx = output_rx;
            while let Some(msg) = output_rx.recv().await {
                results.push(msg);
            }
            results.sort_by_key(|m| m.snapshot_index);
            results
        }
        .in_current_span(),
    ));

    let mut failures: Vec<SnapshotFailure> = Vec::new();
    let mut error: Option<PipelineError> = None;
    while let Some(result) = worker_handles.join_next().await {
        match result {
            Ok(Ok(worker_failures)) => failures.extend(worker_failures),
            Ok(Err(e)) => {
                if error.is_none() {
                    error = Some(e);
                    worker_handles.abort_all();
                }
            }
            Err(join_err) => {
                if join_err.is_panic() && error.is_none() {
                    error = Some(PipelineError::TaskPanic(format!("{join_err}")));
                    worker_handles.abort_all();
                }
            }
        }
    }

    // Every worker has stopped, so both ends of the pipe close on their own.
    if let Err(join_err) = feed_handle.await {
        if join_err.is_panic() && error.is_none() {
            error = Some(PipelineError::TaskPanic(format!("feeder: {join_err}")));
        }
    }
    let outputs = match collect_handle.await {
        Ok(outputs) => outputs,
        Err(join_err) => {
            if error.is_none() {
                error = Some(PipelineError::Other(format!(
                    "collector failed: {join_err}"
                )));
            }
            Vec::new()
        }
    };

    if let Some(error) = error {
        return Err(error);
    }
    failures.sort_by_key(|f| f.snapshot_index);
    Ok((outputs, failures))
}
