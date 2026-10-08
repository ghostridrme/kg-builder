//! Bounded registry operations and prepared-contract verification. No Python callbacks.
use kg_core::{
    errors::BackendError,
    profiles::{Profile, ProfileBindings, ProfileRef},
    runtime::schemas::RunSchemaManifest,
    traits::{GraphBackend, ProfileRegistry},
};
use kg_stages::Engine;
use kg_storage_neo4j::Neo4jGraphBackend;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Put {
        document: Profile,
    },
    Get {
        reference: ProfileRef,
    },
    List {
        after: Option<ProfileRef>,
        limit: usize,
    },
    Prepare {
        bindings: ProfileBindings,
    },
}
fn failure(error: BackendError) -> Value {
    json!({"ok":false,"result":{"cause":crate::outcome::backend_kind(&error),"retriable":error.is_transient(),"commit_unknown":matches!(error,BackendError::UnknownCommit(_))}})
}
pub async fn run(
    graph: &Neo4jGraphBackend,
    engine: &Engine,
    org: &str,
    request: Request,
    cancel: &CancellationToken,
) -> Value {
    if cancel.is_cancelled() {
        return failure(BackendError::Unavailable("cancelled".into()));
    }
    let result = match request {
        // The storage implementation bounds the transaction and awaits any submitted commit.
        Request::Put { document } => graph.put(org, document).await.map(|p| json!({"profile":p})),
        Request::Get { reference } => graph
            .get(org, &reference)
            .await
            .map(|p| json!({"profile":p})),
        Request::List { after, limit } => graph
            .list(org, after.as_ref(), limit)
            .await
            .map(|p| json!({"page":p})),
        Request::Prepare { bindings } => {
            return match engine
                .prepare_profiles(org, bindings, Some(cancel.clone()))
                .await
            {
                Ok(manifest) => json!({"ok":true,"result":{"manifest":manifest}}),
                Err(e) => {
                    json!({"ok":false,"result":{"cause":"profile_admission","retriable":e.is_retriable()}})
                }
            }
        }
    };
    match result {
        Ok(value) => json!({"ok":true,"result":value}),
        Err(e) => failure(e),
    }
}

/// A prepared journal is evidence, never an authority. Recover the durable header
/// if present; otherwise resolve exact immutable revisions against current host policy.
pub async fn verify(
    graph: &Neo4jGraphBackend,
    engine: &Engine,
    org: &str,
    run_id: Uuid,
    bindings: &ProfileBindings,
    expected: &RunSchemaManifest,
    cancel: &CancellationToken,
) -> Result<(), kg_core::errors::PipelineError> {
    use kg_core::errors::PipelineError;
    let invalid = || PipelineError::StateValidation {
        stage: "profile_journal".into(),
        message: "prepared profile contract differs from the authorized contract".into(),
    };
    expected.validate(org).map_err(|_| invalid())?;
    let references = expected
        .profiles
        .iter()
        .map(|(s, p)| (s.clone(), p.document.reference()))
        .collect::<ProfileBindings>();
    if &references != bindings || expected.sources.len() != bindings.len() {
        return Err(invalid());
    }
    let header = tokio::select! {
        _=cancel.cancelled()=>return Err(PipelineError::Cancelled),
        result=tokio::time::timeout(std::time::Duration::from_millis(engine.settings().context.read_timeout_ms),graph.read_run(org,run_id))=>result.map_err(|_|PipelineError::StepExecution{stage:"profile_journal".into(),step:"read".into(),cause:"read timed out".into(),retriable:true})?.map_err(|e|PipelineError::StepExecution{stage:"profile_journal".into(),step:"read".into(),cause:"read failed".into(),retriable:e.is_transient()})?,
    };
    let actual = match header {
        Some(h) => h.schema_manifest,
        None => {
            engine
                .prepare_profiles(org, bindings.clone(), Some(cancel.clone()))
                .await?
        }
    };
    for source in bindings.keys() {
        if actual.profiles.get(source) != expected.profiles.get(source)
            || actual.sources.get(source) != expected.sources.get(source)
        {
            return Err(invalid());
        }
    }
    Ok(())
}
