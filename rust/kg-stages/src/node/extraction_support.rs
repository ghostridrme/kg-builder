//! Shared prompt, provider and answer checks for the two text discovery passes.
use crate::model_output::ModelOutputError;
use kg_core::{
    errors::{stage::ModelFailureKind, BackendError, StageError},
    runtime::{extraction::ExtractionSettings, RuntimeContext},
    traits::llm_backend::{CompletionStatus, LlmMessage, LlmResponse},
};
use serde_json::Value;

// One typed-decision request under the engine's permit pool, the typed
// timeout and cancellation. Any failure is `None`: the caller keeps its
// language-model path, which is the fallback by design.
pub(crate) async fn typed_decide(
    ctx: &RuntimeContext,
    stage: &str,
    state: &Value,
    questions: &std::collections::BTreeMap<String, kg_core::traits::Question>,
) -> Option<kg_core::traits::Decided> {
    let backend = ctx
        .decisions
        .as_ref()
        .filter(|_| ctx.typed_decisions.enabled)?;
    if kg_core::traits::decision_backend::validate_request(state, questions, &backend.limits())
        .is_err()
    {
        tracing::debug!(
            stage,
            "typed decision skipped: request outside backend limits"
        );
        return None;
    }
    let timeout = std::time::Duration::from_millis(ctx.typed_decisions.timeout_ms);
    let result = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return None,
        result = tokio::time::timeout(timeout, async {
            let _permit = kg_core::telemetry::acquire(&ctx.decision_semaphore, kg_core::telemetry::OperationKind::Decision).await.ok()?;
            kg_core::telemetry::model_call(stage);
            backend.decide(state, questions).await.ok()
        }) => result.ok().flatten(),
    };
    if result.is_none() {
        tracing::warn!(
            stage,
            "typed decision unavailable; language-model path decides"
        );
    }
    result
}

// Append the caller's trusted domain guidance for one prompt family to a
// generic system prompt. Every model stage uses this so the base prompts stay
// domain-neutral (they apply to any data) and a connector or caller supplies
// the domain expertise through configuration, scoped per source. The generic
// rules come first and still govern; guidance never changes the output
// contract, the evidence requirements, key authority or isolation.
pub(crate) fn with_guidance(base: &str, custom: Option<&str>) -> String {
    match custom {
        Some(text) => format!(
            "{base}\nAdditional domain guidance for this data (the evidence, decision and output rules above still apply):\n{text}"
        ),
        None => base.to_owned(),
    }
}

pub(crate) fn invalid(stage: &str, message: &str) -> StageError {
    StageError::StateValidation {
        stage: stage.into(),
        message: message.into(),
    }
}

fn provider_kind(error: &BackendError) -> ModelFailureKind {
    match error {
        // A spent attempt allowance is a caller-side configuration bound, not a
        // provider outage: retrying could never help within the same request.
        BackendError::AttemptBudgetExhausted => ModelFailureKind::Configuration,
        BackendError::Timeout(_) => ModelFailureKind::Timeout,
        BackendError::RateLimited { .. } => ModelFailureKind::RateLimited,
        BackendError::Auth(_) => ModelFailureKind::Authentication,
        BackendError::Connection(_) | BackendError::Unavailable(_) => ModelFailureKind::Unavailable,
        BackendError::NotConfigured(_) | BackendError::Query(_) => ModelFailureKind::Configuration,
        BackendError::IncompleteResponse => ModelFailureKind::IncompleteResponse,
        BackendError::Refused => ModelFailureKind::Refused,
        BackendError::Deserialization(_) => ModelFailureKind::InvalidResponse,
        _ => ModelFailureKind::Other,
    }
}

pub(crate) async fn call_with_schema(
    ctx: &RuntimeContext,
    stage: &str,
    messages: &[LlmMessage],
    settings: &ExtractionSettings,
    schema: &Value,
) -> Result<LlmResponse, StageError> {
    call_provider(
        ctx,
        stage,
        messages,
        schema,
        ctx.llm_extraction.as_ref(),
        &ctx.llm_extraction_semaphore,
        settings.timeout_ms,
        settings.max_output_tokens,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn call_provider(
    ctx: &RuntimeContext,
    stage: &str,
    messages: &[LlmMessage],
    schema: &Value,
    provider: &dyn kg_core::traits::LlmBackend,
    semaphore: &tokio::sync::Semaphore,
    timeout_ms: u64,
    max_output_tokens: u32,
) -> Result<LlmResponse, StageError> {
    kg_core::runtime::history::check_prompt_budget(
        stage,
        provider,
        messages,
        schema,
        max_output_tokens,
    )?;
    let started = std::time::Instant::now();
    let result = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(StageError::Cancelled {stage:stage.into()}),
        result = tokio::time::timeout(std::time::Duration::from_millis(timeout_ms),async {
            let _permit = kg_core::telemetry::acquire(semaphore, kg_core::telemetry::OperationKind::Llm).await.map_err(|_| StageError::Cancelled {stage:stage.into()})?;
            let wait_ms = started.elapsed().as_millis() as u64;
            kg_core::telemetry::model_call(stage);
            let response = provider.complete(messages,Some(schema),Some(max_output_tokens)).await
                .map_err(|e| StageError::ModelCall {stage:stage.into(),kind:provider_kind(&e)})?;
            let kind = match response.status {CompletionStatus::Complete=>None,CompletionStatus::Truncated=>Some(ModelFailureKind::IncompleteResponse),CompletionStatus::Refused=>Some(ModelFailureKind::Refused)};
            if let Some(kind) = kind {return Err(StageError::ModelCall {stage:stage.into(),kind});}
            if response.model.trim().is_empty() || response.model.len()>256 || response.model.chars().any(char::is_control) {
                return Err(StageError::ModelCall {stage:stage.into(),kind:ModelFailureKind::InvalidResponse});
            }
            tracing::debug!(stage,wait_ms,input_tokens=response.input_tokens,output_tokens=response.output_tokens,"model call completed");
            Ok(response)
        }) => result.unwrap_or_else(|_|Err(StageError::ModelCall {stage:stage.into(),kind:ModelFailureKind::Timeout})),
    };
    tracing::debug!(
        stage,
        elapsed_ms = started.elapsed().as_millis() as u64,
        success = result.is_ok(),
        "model call finished"
    );
    result
}

// One provider completion under a request-scoped attempt allowance. The
// backend must declare bounded attempts; otherwise the call is refused before
// any dispatch, so an adapter that retries or falls back internally can never
// spend more than the caller allowed. Cancellation, timeout, permit waits and
// typed failures behave exactly as in [`call_provider`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn call_provider_bounded(
    ctx: &RuntimeContext,
    stage: &str,
    messages: &[LlmMessage],
    schema: &Value,
    provider: &dyn kg_core::traits::LlmBackend,
    semaphore: &tokio::sync::Semaphore,
    deadline: tokio::time::Instant,
    max_output_tokens: u32,
    budget: &kg_core::traits::llm_backend::CallBudget,
) -> Result<LlmResponse, StageError> {
    if !provider.supports_bounded_attempts() {
        return Err(StageError::StateValidation {
            stage: stage.into(),
            message:
                "the configured language model backend does not declare bounded provider attempts"
                    .into(),
        });
    }
    kg_core::runtime::history::check_prompt_budget(
        stage,
        provider,
        messages,
        schema,
        max_output_tokens,
    )?;
    let started = std::time::Instant::now();
    let result = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(StageError::Cancelled {stage:stage.into()}),
        result = tokio::time::timeout_at(deadline, async {
            let _permit = kg_core::telemetry::acquire(semaphore, kg_core::telemetry::OperationKind::Llm).await.map_err(|_| StageError::Cancelled {stage:stage.into()})?;
            let wait_ms = started.elapsed().as_millis() as u64;
            kg_core::telemetry::model_call(stage);
            let response = provider.complete_bounded(messages,Some(schema),Some(max_output_tokens),budget).await
                .map_err(|e| StageError::ModelCall {stage:stage.into(),kind:provider_kind(&e)})?;
            let kind = match response.status {CompletionStatus::Complete=>None,CompletionStatus::Truncated=>Some(ModelFailureKind::IncompleteResponse),CompletionStatus::Refused=>Some(ModelFailureKind::Refused)};
            if let Some(kind) = kind {return Err(StageError::ModelCall {stage:stage.into(),kind});}
            if response.model.trim().is_empty() || response.model.len()>256 || response.model.chars().any(char::is_control) {
                return Err(StageError::ModelCall {stage:stage.into(),kind:ModelFailureKind::InvalidResponse});
            }
            tracing::debug!(stage,wait_ms,input_tokens=response.input_tokens,output_tokens=response.output_tokens,attempts=budget.consumed(),"bounded model call completed");
            Ok(response)
        }) => result.unwrap_or_else(|_|Err(StageError::ModelCall {stage:stage.into(),kind:ModelFailureKind::Timeout})),
    };
    tracing::debug!(
        stage,
        elapsed_ms = started.elapsed().as_millis() as u64,
        attempts = budget.consumed(),
        success = result.is_ok(),
        "bounded model call finished"
    );
    result
}

// `name` without a trailing configured kind word, when the remaining head is
// identifier-shaped (a hyphen, underscore, dot, digit or internal capital) so
// a plain-word product name such as "App Store" is never mangled into "App".
// Case-insensitive on the kind word; the head keeps its spelling.

pub(crate) fn output_error(stage: &str, error: ModelOutputError) -> StageError {
    kg_core::telemetry::extraction_decisions(
        stage,
        kg_core::telemetry::ExtractionDecision::Rejected,
        1,
    );
    StageError::StepFailed {
        stage: stage.into(),
        step: "decode".into(),
        cause: error.to_string(),
        retriable: error.is_retriable(),
    }
}
