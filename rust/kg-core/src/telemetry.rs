//! Exporter-independent measurements. Metric labels never include caller identifiers or text.
use crate::errors::BackendError;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub enum OperationKind {
    Run,
    Stage,
    Llm,
    Decision,
    Embedding,
    Rerank,
    GraphRead,
    GraphWrite,
    Commit,
    Search,
    Maintenance,
    Readiness,
}
impl OperationKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Stage => "stage",
            Self::Llm => "llm",
            Self::Decision => "decision",
            Self::Embedding => "embedding",
            Self::Rerank => "rerank",
            Self::GraphRead => "graph_read",
            Self::GraphWrite => "graph_write",
            Self::Commit => "commit",
            Self::Search => "search",
            Self::Maintenance => "maintenance",
            Self::Readiness => "readiness",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Replayed,
    Failed,
    Timeout,
    Cancelled,
    Abandoned,
    Conflict,
    UnknownCommit,
    Auth,
    RateLimited,
    IncompleteResponse,
    InvalidResponse,
    Refused,
}
impl Outcome {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Replayed => "replayed",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Abandoned => "abandoned",
            Self::Conflict => "conflict",
            Self::UnknownCommit => "unknown_commit",
            Self::Auth => "auth",
            Self::RateLimited => "rate_limited",
            Self::IncompleteResponse => "incomplete_response",
            Self::InvalidResponse => "invalid_response",
            Self::Refused => "refused",
        }
    }
}
impl From<&BackendError> for Outcome {
    fn from(error: &BackendError) -> Self {
        match error {
            BackendError::Timeout(_) => Self::Timeout,
            BackendError::Conflict(_)
            | BackendError::CollectionOwnershipConflict(_)
            | BackendError::IdentityRevisionChanged => Self::Conflict,
            BackendError::UnknownCommit(_) => Self::UnknownCommit,
            BackendError::Auth(_) => Self::Auth,
            BackendError::RateLimited { .. } => Self::RateLimited,
            BackendError::IncompleteResponse => Self::IncompleteResponse,
            BackendError::Deserialization(_) => Self::InvalidResponse,
            BackendError::Refused => Self::Refused,
            _ => Self::Failed,
        }
    }
}

impl From<&crate::errors::StageError> for Outcome {
    fn from(error: &crate::errors::StageError) -> Self {
        use crate::errors::stage::ModelFailureKind;
        use crate::errors::StageError;
        match error {
            StageError::Cancelled { .. } => Self::Cancelled,
            StageError::IdentityRevisionChanged | StageError::CommitRejected { .. } => {
                Self::Conflict
            }
            StageError::CommitOutcomeUnknown { .. } => Self::UnknownCommit,
            StageError::ModelCall { kind, .. } => match kind {
                ModelFailureKind::Timeout => Self::Timeout,
                ModelFailureKind::RateLimited => Self::RateLimited,
                ModelFailureKind::Authentication => Self::Auth,
                ModelFailureKind::IncompleteResponse => Self::IncompleteResponse,
                ModelFailureKind::InvalidResponse => Self::InvalidResponse,
                ModelFailureKind::Refused => Self::Refused,
                _ => Self::Failed,
            },
            _ => Self::Failed,
        }
    }
}
impl From<&crate::errors::PipelineError> for Outcome {
    fn from(error: &crate::errors::PipelineError) -> Self {
        use crate::errors::PipelineError;
        match error {
            PipelineError::Cancelled => Self::Cancelled,
            PipelineError::IdentityRevisionChanged => Self::Conflict,
            PipelineError::Aborted {
                commit_unknown: true,
                ..
            } => Self::UnknownCommit,
            PipelineError::Aborted { cause, .. } => Self::from(cause.as_ref()),
            PipelineError::StageExecution { errors, .. } => {
                let outcomes: Vec<_> = errors.iter().map(Self::from).collect();
                [
                    Self::UnknownCommit,
                    Self::Conflict,
                    Self::Cancelled,
                    Self::Timeout,
                    Self::Auth,
                    Self::RateLimited,
                    Self::IncompleteResponse,
                    Self::InvalidResponse,
                    Self::Refused,
                ]
                .into_iter()
                .find(|kind| outcomes.contains(kind))
                .unwrap_or(Self::Failed)
            }
            _ => Self::Failed,
        }
    }
}

/// Reports abandonment when a future is dropped; this does not imply database rollback.
pub struct OperationGuard {
    kind: OperationKind,
    stage: &'static str,
    started: Instant,
    waiting: Option<Instant>,
    outcome: Outcome,
    span: tracing::Span,
}
impl OperationGuard {
    pub fn new(kind: OperationKind) -> Self {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO,
            { counter.kg_inflight = 1_i64, operation = kind.label() });
        Self {
            kind,
            stage: "none",
            started: Instant::now(),
            waiting: None,
            outcome: Outcome::Abandoned,
            span: tracing::Span::current(),
        }
    }
    pub fn stage(name: &str) -> Self {
        let mut guard = Self::new(OperationKind::Stage);
        guard.stage = stage_label(name);
        guard
    }
    pub fn waiting(&mut self) {
        self.waiting.get_or_insert_with(Instant::now);
    }
    pub fn acquired(&mut self) {
        self.record_wait(false);
    }
    pub fn finish(&mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
    pub fn finish_backend<T>(&mut self, result: &Result<T, BackendError>) {
        self.finish(
            result
                .as_ref()
                .map_or_else(Outcome::from, |_| Outcome::Success),
        );
    }
    fn record_wait(&mut self, interrupted: bool) {
        if let Some(start) = self.waiting.take() {
            tracing::event!(target: "kg::metrics", parent: &self.span, tracing::Level::INFO, {
                histogram.kg_queue_wait_ms = start.elapsed().as_secs_f64()*1000.0,
                operation = self.kind.label(), stage = self.stage, slot = "provider", interrupted });
        }
    }
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.record_wait(true);
        tracing::event!(target: "kg::metrics", parent: &self.span, tracing::Level::INFO,
            { counter.kg_inflight = -1_i64, operation = self.kind.label() });
        tracing::event!(target: "kg::metrics", parent: &self.span, tracing::Level::INFO, {
            monotonic_counter.kg_operations = 1_u64,
            histogram.kg_operation_duration_ms = self.started.elapsed().as_secs_f64()*1000.0,
            operation = self.kind.label(), stage = self.stage, outcome = self.outcome.label() });
    }
}
pub fn retry(kind: OperationKind) {
    tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_retries = 1_u64, operation = kind.label() });
}

/// Count dispatched provider calls, after admission; token usage remains adapter-owned.
pub fn model_call(stage: &str) {
    tracing::event!(target: "kg::metrics", tracing::Level::INFO, {
        monotonic_counter.kg_model_calls = 1_u64,
        purpose = stage_label(stage) });
}

/// Extraction decisions count work attempted, not durable graph mutations.
#[derive(Clone, Copy)]
pub enum ExtractionDecision {
    Discovered,
    Excluded,
    Rejected,
}
pub fn extraction_decisions(stage: &str, decision: ExtractionDecision, count: usize) {
    if count == 0 {
        return;
    }
    let outcome = match decision {
        ExtractionDecision::Discovered => "discovered",
        ExtractionDecision::Excluded => "excluded",
        ExtractionDecision::Rejected => "rejected",
    };
    tracing::event!(target: "kg::metrics", tracing::Level::INFO, {
        monotonic_counter.kg_extraction_decisions = count as u64,
        stage = stage_label(stage), outcome });
}

/// One candidate query returned a bounded frontier, whether or not it is widened later.
pub fn candidate_truncation(stage: &str) {
    tracing::event!(target: "kg::metrics", tracing::Level::INFO, {
        monotonic_counter.kg_candidate_truncations = 1_u64,
        stage = stage_label(stage) });
}

pub fn embedding_reuse(count: usize) {
    if count > 0 {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, {
            monotonic_counter.kg_embedding_reused_targets = count as u64,
            stage = "batch_embedding" });
    }
}

/// Bounded reference-discovery outcome counts for one stage's own contribution,
/// emitted under the current (stage) span so they correlate with run/stage
/// tracing. Labels are a fixed vocabulary — `operation="reference"`, the stage
/// name, and a decision `outcome` — never a payload, identifier, or slot text.
/// Only nonzero counts are emitted, and each stage reports only the decisions it
/// made (extraction reports its deterministic confirms and the slots it left
/// unresolved; resolution reports only the references it newly confirmed), so a
/// merged or replayed report is never double-counted. `truncated` is the number
/// of source chains whose traversal or candidate lookup was bounded this batch.
pub fn reference_decisions(
    stage: &'static str,
    confirmed: usize,
    unresolved: usize,
    excluded: usize,
    truncated: usize,
) {
    let emit = |outcome: &'static str, count: usize| {
        if count > 0 {
            tracing::event!(target: "kg::metrics", tracing::Level::INFO, {
                monotonic_counter.kg_reference_decisions = count as u64,
                operation = "reference", stage = stage, outcome = outcome });
        }
    };
    emit("confirmed", confirmed);
    emit("unresolved", unresolved);
    emit("excluded", excluded);
    if truncated > 0 {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, {
            monotonic_counter.kg_reference_truncations = truncated as u64,
            operation = "reference", stage = stage, slot = "source" });
    }
}
/// Fixed-label reference work counters. These count work attempts except the
/// two durable repair outcomes, which are emitted after receipt acknowledgement.
#[derive(Clone, Copy)]
pub enum ReferenceActivity {
    ForwardSource,
    ReverseSource,
    CompleteKeyCandidate,
    PartialKeyCandidate,
    RepairCommitted,
    RepairReplayed,
}
pub fn reference_activity(activity: ReferenceActivity, count: usize) {
    if count == 0 {
        return;
    }
    let activity = match activity {
        ReferenceActivity::ForwardSource => "forward_source",
        ReferenceActivity::ReverseSource => "reverse_source",
        ReferenceActivity::CompleteKeyCandidate => "generic_complete_key_candidate",
        ReferenceActivity::PartialKeyCandidate => "generic_partial_key_candidate",
        ReferenceActivity::RepairCommitted => "repair_committed",
        ReferenceActivity::RepairReplayed => "repair_replayed",
    };
    tracing::event!(target:"kg::metrics",tracing::Level::INFO,{
        monotonic_counter.kg_reference_work=count as u64, activity=activity
    });
}
/// Outcomes of the evidence-backed ambiguity stage. Fixed labels only: no
/// identifiers, payloads or provider error text.
#[derive(Clone, Copy)]
pub enum ReferenceResolutionOutcome {
    Attempted,
    Accepted,
    Rejected,
    Unsure,
    EvidenceBudgetRefused,
    ContentUnavailable,
    OwnIdentityRefused,
    InvalidResponse,
    AttemptBudgetExhausted,
    CacheHit,
    PersistedHit,
    PersistedUnavailable,
    HydrationChanged,
}
pub fn reference_resolution_outcome(outcome: ReferenceResolutionOutcome, count: usize) {
    if count == 0 {
        return;
    }
    let outcome = match outcome {
        ReferenceResolutionOutcome::Attempted => "attempted",
        ReferenceResolutionOutcome::Accepted => "accepted",
        ReferenceResolutionOutcome::Rejected => "rejected",
        ReferenceResolutionOutcome::Unsure => "unsure",
        ReferenceResolutionOutcome::EvidenceBudgetRefused => "evidence_budget_refused",
        ReferenceResolutionOutcome::ContentUnavailable => "content_unavailable",
        ReferenceResolutionOutcome::OwnIdentityRefused => "own_identity_refused",
        ReferenceResolutionOutcome::InvalidResponse => "invalid_response",
        ReferenceResolutionOutcome::AttemptBudgetExhausted => "attempt_budget_exhausted",
        ReferenceResolutionOutcome::CacheHit => "cache_hit",
        ReferenceResolutionOutcome::PersistedHit => "persisted_hit",
        ReferenceResolutionOutcome::PersistedUnavailable => "persisted_unavailable",
        ReferenceResolutionOutcome::HydrationChanged => "hydration_changed",
    };
    tracing::event!(target:"kg::metrics",tracing::Level::INFO,{
        monotonic_counter.kg_reference_resolution=count as u64, outcome=outcome
    });
}

/// Caller error text/paths never become metric labels.
pub fn reference_uncertainty(reason: &str, count: usize) {
    if count == 0 {
        return;
    }
    let reason = match reason {
        "partial-key" => "partial_key",
        "target-not-found" => "target_not_found",
        "lookup-truncated" => "lookup_truncated",
        "multiple-candidates" => "multiple_candidates",
        "insufficient-reference-evidence" => "insufficient_reference_evidence",
        "display-name-only" => "display_name_only",
        "ambiguous-correspondence" => "ambiguous_correspondence",
        _ => "other",
    };
    tracing::event!(target:"kg::metrics",tracing::Level::INFO,{
        monotonic_counter.kg_reference_uncertainty=count as u64, reason=reason
    });
}

/// A semaphore wait measured separately from the operation that owns the permit.
pub struct QueueGuard {
    kind: OperationKind,
    slot: &'static str,
    started: Instant,
    acquired: bool,
    span: tracing::Span,
}
impl QueueGuard {
    pub fn new(kind: OperationKind) -> Self {
        Self {
            kind,
            slot: "search",
            started: Instant::now(),
            acquired: false,
            span: tracing::Span::current(),
        }
    }
    pub fn acquired(&mut self) {
        self.acquired = true;
    }
}
impl Drop for QueueGuard {
    fn drop(&mut self) {
        tracing::event!(target: "kg::metrics", parent: &self.span, tracing::Level::INFO, {
            histogram.kg_queue_wait_ms = self.started.elapsed().as_secs_f64()*1000.0,
            operation = self.kind.label(), stage = "none", slot = self.slot, interrupted = !self.acquired });
    }
}

/// Measures only permit waiting; callers retain ownership of deadlines and cancellation.
pub async fn acquire(
    semaphore: &tokio::sync::Semaphore,
    kind: OperationKind,
) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
    let mut waiting = QueueGuard::new(kind);
    waiting.slot = "engine";
    let permit = semaphore.acquire().await?;
    waiting.acquired();
    Ok(permit)
}

/// Only provider-reported usage belongs here. Missing usage is not a zero-cost call.
pub fn token_usage(input: Option<u64>, output: Option<u64>) {
    if let Some(value) = input {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_input_tokens = value, operation = "llm" });
    }
    if let Some(value) = output {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_output_tokens = value, operation = "llm" });
    }
    if input.is_none() || output.is_none() {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_missing_usage = 1_u64, operation = "llm" });
    }
}
/// Used when an adapter cannot expose billed provider usage; never infer tokens from text length.
pub fn unknown_usage(kind: OperationKind) {
    tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_missing_usage = 1_u64, operation = kind.label() });
    tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_unknown_cost_calls = 1_u64, operation = kind.label() });
}

/// Explicit prices are estimates in USD per million tokens, never fetched during ingestion.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenPrices {
    pub input_per_million_usd: f64,
    pub output_per_million_usd: f64,
    /// Rate for input tokens the provider served from its prompt cache. When
    /// absent, cached tokens are priced at the uncached input rate, so an
    /// estimate is never lower than the provider could charge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_per_million_usd: Option<f64>,
}
impl TokenPrices {
    pub fn validate(&self) -> Result<(), BackendError> {
        if [self.input_per_million_usd, self.output_per_million_usd]
            .iter()
            .chain(self.cached_input_per_million_usd.iter())
            .any(|rate| !rate.is_finite() || *rate < 0.0)
        {
            return Err(BackendError::NotConfigured(
                "token prices must be finite and nonnegative".into(),
            ));
        }
        if self
            .cached_input_per_million_usd
            .is_some_and(|rate| rate > self.input_per_million_usd)
        {
            return Err(BackendError::NotConfigured(
                "cached input rate cannot exceed the uncached input rate".into(),
            ));
        }
        Ok(())
    }
    pub fn estimate(&self, input: u64, output: u64) -> Option<f64> {
        self.estimate_with_cached(input, 0, output)
    }
    /// `cached` input tokens (never more than `input`) are priced at the cached
    /// rate when one is configured, the remaining input at the uncached rate.
    pub fn estimate_with_cached(&self, input: u64, cached: u64, output: u64) -> Option<f64> {
        self.validate().ok()?;
        let cached = cached.min(input);
        let cached_rate = self
            .cached_input_per_million_usd
            .unwrap_or(self.input_per_million_usd);
        let cost = ((input - cached) as f64 / 1_000_000.0) * self.input_per_million_usd
            + (cached as f64 / 1_000_000.0) * cached_rate
            + (output as f64 / 1_000_000.0) * self.output_per_million_usd;
        cost.is_finite().then_some(cost)
    }
}
/// Billed usage of one completion. `cached` is the provider's count of input
/// tokens served from its prompt cache (a subset of `input`); it is recorded on
/// its own counter and priced at the cached rate when the price table has one.
pub fn usage_with_prices(
    input: Option<u64>,
    output: Option<u64>,
    cached: Option<u64>,
    prices: Option<&TokenPrices>,
) {
    token_usage(input, output);
    if let Some(value) = cached {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_cached_input_tokens = value, operation = "llm" });
    }
    let cost = input
        .zip(output)
        .zip(prices)
        .and_then(|((i, o), p)| p.estimate_with_cached(i, cached.unwrap_or(0), o));
    if let Some(cost) = cost {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_estimated_cost_usd = cost, operation = "llm" });
    } else {
        tracing::event!(target: "kg::metrics", tracing::Level::INFO, { monotonic_counter.kg_unknown_cost_calls = 1_u64, operation = "llm" });
    }
}

fn stage_label(name: &str) -> &'static str {
    const STAGES: &[&str] = &[
        "input_validation",
        "snapshot_preparation",
        "context_retrieval",
        "saga_association",
        "direct_extraction",
        "entity_extraction",
        "llm_extraction",
        "omission_check",
        "schema_inference",
        "resolve_nodes",
        "fuzzy_match",
        "entity_attribute_enrichment",
        "entity_versioning",
        "reference_extraction",
        "reference_resolution",
        "declared_relationship_extraction",
        "llm_relationship_extraction",
        "edge_resolution",
        "relationship_resolution",
        "relationship_endpoint_resolution",
        "relationship_naming",
        "relationship_attribute_enrichment",
        "relationship_timestamp_extraction",
        "relationship_contradiction_resolution",
        "mutation_planning",
        "batch_embedding",
        "persist",
        "entity_summary",
        "saga_summary",
        "community_detection",
        "community_summary",
        "community_embedding",
    ];
    STAGES
        .iter()
        .copied()
        .find(|known| *known == name)
        .unwrap_or("custom")
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn labels_cannot_include_customer_data() {
        assert_eq!(stage_label("tenant-secret-stage"), "custom");
        assert_eq!(stage_label("community_summary"), "community_summary");
        assert_eq!(
            Outcome::from(&BackendError::UnknownCommit("secret".into())),
            Outcome::UnknownCommit
        );
    }
    #[test]
    fn model_purpose_and_extraction_metrics_never_label_caller_text() {
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct Writer(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        model_call("llm_relationship_extraction");
        model_call("SECRET_CUSTOM_PURPOSE");
        extraction_decisions("SECRET_CUSTOM_STAGE", ExtractionDecision::Rejected, 1);
        extraction_decisions("llm_extraction", ExtractionDecision::Discovered, 0);
        embedding_reuse(2);
        candidate_truncation("SECRET_CUSTOM_STAGE");
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert_eq!(output.lines().count(), 5, "{output}");
        assert!(
            output.contains("purpose=\"llm_relationship_extraction\""),
            "{output}"
        );
        assert!(output.contains("purpose=\"custom\""), "{output}");
        assert!(output.contains("outcome=\"rejected\""), "{output}");
        assert!(!output.contains("SECRET"), "{output}");
    }
    #[test]
    fn prices_never_turn_missing_or_invalid_usage_into_free_calls() {
        let prices = TokenPrices {
            input_per_million_usd: 2.0,
            output_per_million_usd: 8.0,
            cached_input_per_million_usd: None,
        };
        assert_eq!(prices.estimate(1_000_000, 500_000), Some(6.0));
        // Without a cached rate, cached tokens cost the uncached rate; never less.
        assert_eq!(
            prices.estimate_with_cached(1_000_000, 500_000, 500_000),
            Some(6.0)
        );
        let discounted = TokenPrices {
            cached_input_per_million_usd: Some(0.5),
            ..prices
        };
        assert_eq!(
            discounted.estimate_with_cached(1_000_000, 500_000, 0),
            Some(1.25)
        );
        assert_eq!(
            discounted.estimate_with_cached(1_000_000, 5_000_000, 0),
            Some(0.5),
            "cached tokens are capped at the input count"
        );
        assert!(TokenPrices {
            cached_input_per_million_usd: Some(3.0),
            ..prices
        }
        .validate()
        .is_err());
        assert_eq!(
            serde_json::from_value::<TokenPrices>(
                serde_json::json!({"input_per_million_usd":2.0,"output_per_million_usd":8.0})
            )
            .unwrap()
            .cached_input_per_million_usd,
            None
        );
        assert!(TokenPrices {
            input_per_million_usd: f64::NAN,
            ..prices
        }
        .validate()
        .is_err());
        assert!(TokenPrices {
            input_per_million_usd: -1.0,
            ..prices
        }
        .validate()
        .is_err());
        assert_eq!(
            TokenPrices {
                input_per_million_usd: f64::MAX,
                ..prices
            }
            .estimate(u64::MAX, 0),
            None
        );
    }

    #[test]
    fn dropped_operations_keep_their_parent_and_report_interrupted_waits() {
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct Writer(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let mut abandoned = {
            let parent = tracing::info_span!("caller", request = "parent-only");
            let _entered = parent.enter();
            OperationGuard::stage("CUSTOM_SECRET_STAGE")
        };
        abandoned.waiting();
        drop(abandoned);
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(output.contains("abandoned"), "{output}");
        assert!(output.contains("interrupted=true"), "{output}");
        assert!(output.contains("stage=\"custom\""), "{output}");
        assert!(!output.contains("CUSTOM_SECRET_STAGE"));
        assert!(
            output.lines().all(|line| line.contains("parent-only")),
            "{output}"
        );
    }
    #[test]
    fn reference_decisions_emit_bounded_labels_and_skip_zero_counts() {
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct Writer(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let output = tracing::subscriber::with_default(subscriber, || {
            // Two confirmed, three unresolved, zero excluded, one truncated source.
            reference_decisions("reference_extraction", 2, 3, 0, 1);
            // A stage with nothing to report emits nothing at all.
            reference_decisions("reference_resolution", 0, 0, 0, 0);
            String::from_utf8(bytes.lock().unwrap().clone()).unwrap()
        });
        // Only the nonzero decisions and the truncation are emitted.
        assert_eq!(
            output.lines().count(),
            3,
            "confirmed, unresolved, truncation only: {output}"
        );
        assert!(output.contains("kg_reference_decisions=2"), "{output}");
        assert!(output.contains("outcome=\"confirmed\""), "{output}");
        assert!(output.contains("kg_reference_decisions=3"), "{output}");
        assert!(output.contains("outcome=\"unresolved\""), "{output}");
        assert!(!output.contains("outcome=\"excluded\""), "{output}");
        assert!(
            output.contains("kg_reference_truncations=1") && output.contains("slot=\"source\""),
            "{output}"
        );
        // Labels are the fixed vocabulary only — never an org, run, or slot text.
        for line in output.lines() {
            assert!(line.contains("operation=\"reference\""), "{line}");
            assert!(line.contains("stage=\"reference_extraction\""), "{line}");
        }
    }

    #[test]
    fn reference_work_and_uncertainty_never_export_caller_strings() {
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct Writer(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let output = tracing::subscriber::with_default(subscriber, || {
            reference_activity(ReferenceActivity::ReverseSource, 2);
            reference_activity(ReferenceActivity::RepairCommitted, 0);
            reference_uncertainty("SECRET_org_chain_payload", 3);
            reference_uncertainty("multiple-candidates", 1);
            reference_uncertainty("ambiguous-correspondence", 1);
            String::from_utf8(bytes.lock().unwrap().clone()).unwrap()
        });
        assert_eq!(output.lines().count(), 4);
        assert!(output.contains("multiple_candidates"));
        assert!(output.contains("ambiguous_correspondence"));
        assert!(output.contains("reverse_source"));
        assert!(output.contains("reason=\"other\""));
        assert!(!output.contains("SECRET"));
        assert!(!output.contains("repair_committed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_engine_wait_is_measured_without_acquiring_capacity() {
        use std::future::Future;
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct Writer(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let semaphore = tokio::sync::Semaphore::new(0);
        let mut future = Box::pin(acquire(&semaphore, OperationKind::Llm));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        drop(future);
        assert_eq!(semaphore.available_permits(), 0);
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(output.contains("slot=\"engine\""), "{output}");
        assert!(output.contains("interrupted=true"), "{output}");
        assert!(
            !output.contains("kg_operations"),
            "wait is not a completed provider call: {output}"
        );
    }
}
