//! Shared mutation-plan data and property encoding helpers.
use chrono::{DateTime, Utc};
use kg_core::{
    errors::StageError,
    pipeline::CommittedCounts,
    runtime::stage_output::PlannedEmbedding,
    traits::{GraphMutation, GraphProperties, Precondition},
};
use uuid::Uuid;
pub(crate) const STAGE: &str = "mutation_planning";
#[derive(Debug, Default)]
pub(crate) struct Plan {
    pub(crate) preconditions: Vec<Precondition>,
    pub(crate) mutations: Vec<GraphMutation>,
    pub(crate) counts: CommittedCounts,
    pub(crate) embeddings: Vec<PlannedEmbedding>,
}
impl Plan {
    pub(crate) fn require(&mut self, precondition: Precondition) {
        if !self.preconditions.contains(&precondition) {
            self.preconditions.push(precondition);
        }
    }
}
pub(crate) fn set_string(props: &mut GraphProperties, key: &str, value: &str) {
    if !value.is_empty() {
        props.insert(key.into(), value.into());
    }
}
pub(crate) fn set_uuid(props: &mut GraphProperties, key: &str, value: Option<Uuid>) {
    if let Some(value) = value {
        props.insert(key.into(), value.to_string().into());
    }
}
pub(crate) fn closed_edge(at: DateTime<Utc>) -> GraphProperties {
    let mut props = GraphProperties::new();
    props.insert("invalid_at".into(), at.to_rfc3339().into());
    props.insert("last_transition_at".into(), at.to_rfc3339().into());
    props.insert("is_latest".into(), false.into());
    props
}
#[track_caller]
pub(crate) fn invalid(message: String) -> StageError {
    let location = std::panic::Location::caller();
    tracing::warn!(
        validation_file = location.file(),
        validation_line = location.line(),
        "mutation plan validation rejected"
    );
    StageError::StateValidation {
        stage: STAGE.into(),
        message,
    }
}
pub(crate) fn identity_budget_exhausted() -> StageError {
    StageError::StepFailed {
        stage: STAGE.into(),
        step: "identity_budget".into(),
        cause: "identity resolution deadline exceeded before commit".into(),
        retriable: true,
    }
}
