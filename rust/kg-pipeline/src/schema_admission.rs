//! Freeze custom schemas before new runs; recover the stored contract on retries.

use kg_core::{
    errors::{BackendError, PipelineError},
    models::{IngestionInput, SnapshotInput},
    runtime::{
        schemas::{self, RunSchemaManifest},
        RuntimeContext,
    },
    traits::{PlannedBatch, RequestFingerprint, RunHeader},
};
use std::{future::Future, time::Duration};
use uuid::Uuid;

async fn bounded<T>(
    ctx: &RuntimeContext,
    operation: impl Future<Output = Result<T, BackendError>>,
) -> Result<T, PipelineError> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(PipelineError::Cancelled),
        result = tokio::time::timeout(Duration::from_millis(ctx.context_settings.read_timeout_ms), async {
            let _permit = ctx.semaphore.acquire().await.map_err(|_| BackendError::Unavailable("schema admission closed".into()))?;
            operation.await
        }) => result.map_err(|_| PipelineError::StepExecution {
            stage:"schema_admission".into(), step:"read".into(), cause:"schema admission timed out".into(), retriable:true,
        })?.map_err(|e| PipelineError::StepExecution {
            stage:"schema_admission".into(), step:"read".into(), cause:"schema admission backend failed".into(), retriable:e.is_transient(),
        }),
    }
}

fn invalid(message: String) -> PipelineError {
    PipelineError::StateValidation {
        stage: "schema_admission".into(),
        message,
    }
}

async fn prepare_header(
    ctx: &RuntimeContext,
    inputs: &[IngestionInput],
    run_id: Uuid,
    fingerprint: RequestFingerprint,
    batch_plan: Vec<PlannedBatch>,
    version: &str,
    chunk_size: usize,
) -> Result<RunHeader, PipelineError> {
    let fresh: Vec<SnapshotInput> = inputs
        .iter()
        .filter_map(|input| match input {
            IngestionInput::Fresh(input) => Some(input.as_ref().clone()),
            _ => None,
        })
        .collect();
    let sources = schemas::sources(&fresh);
    if sources.len() > 256 {
        return Err(invalid("too many schema sources".into()));
    }
    kg_core::profiles::validate_bindings(&ctx.profile_bindings)
        .map_err(|e| invalid(e.to_string()))?;
    for input in &fresh {
        if ctx.profile_bindings.contains_key(&input.source)
            && input
                .entities
                .iter()
                .any(|e| !ctx.profile_bindings.contains_key(&e.source))
        {
            return Err(invalid(
                "a profile-bound snapshot needs a binding for every supplied entity source".into(),
            ));
        }
    }
    if ctx
        .profile_bindings
        .keys()
        .any(|source| !sources.contains(source))
    {
        return Err(invalid("profile binding has no input source".into()));
    }
    for source in ctx.profile_bindings.keys() {
        if ctx
            .extraction_settings
            .source_guidance
            .get(source)
            .is_some_and(|g| {
                g.shared_instructions.is_some()
                    || g.instructions.is_some()
                    || g.relationship_instructions.is_some()
                    || g.identity_instructions.is_some()
            })
        {
            return Err(invalid(
                "profile conflicts with legacy source guidance".into(),
            ));
        }
    }
    if let Some(header) = bounded(ctx, ctx.graph.read_run(&ctx.org_id, run_id)).await? {
        if header.org_id != ctx.org_id.as_ref()
            || header.run_id != run_id
            || header.fingerprint != fingerprint
            || header.settings_version != version
        {
            return Err(PipelineError::StateValidation {
                stage: "run".into(),
                message: "run id belongs to a different request or settings".into(),
            });
        }
        let recorded = header
            .schema_manifest
            .profiles
            .iter()
            .map(|(source, profile)| (source.clone(), profile.document.reference()))
            .collect::<kg_core::profiles::ProfileBindings>();
        if recorded != ctx.profile_bindings {
            return Err(invalid(
                "profile bindings differ from registered run".into(),
            ));
        }
        header
            .schema_manifest
            .validate_inputs(&ctx.org_id, &fresh)
            .map_err(invalid)?;
        tracing::debug!(
            sources = header.schema_manifest.sources.len(),
            "recovered frozen schema definitions"
        );
        header
            .observation_manifest
            .validate_inputs(inputs, chunk_size)
            .map_err(invalid)?;
        ctx.with_run_schemas(header.schema_manifest.clone())
            .extraction_settings
            .validate()
            .map_err(|e| invalid(e.to_string()))?;
        return Ok(header);
    }
    let manifest = resolve_manifest(ctx, sources).await?;
    manifest
        .validate_inputs(&ctx.org_id, &fresh)
        .map_err(invalid)?;
    tracing::debug!(
        sources = manifest.sources.len(),
        "prepared schema definitions"
    );
    ctx.with_run_schemas(manifest.clone())
        .extraction_settings
        .validate()
        .map_err(|e| invalid(e.to_string()))?;
    let capture_default = chrono::Utc::now();
    let observation_manifest =
        crate::observation_admission::prepare(ctx, inputs, capture_default, chunk_size).await?;
    let mut batch_plan = batch_plan;
    batch_plan.retain(|batch| {
        !matches!(
            batch.kind,
            kg_core::traits::BatchKind::Node | kg_core::traits::BatchKind::Relationship
        )
    });
    for (index, indices) in observation_manifest.node_batches.iter().enumerate() {
        for kind in [
            kg_core::traits::BatchKind::Node,
            kg_core::traits::BatchKind::Relationship,
        ] {
            batch_plan.push(PlannedBatch {
                kind,
                index: index as u32,
                items: indices.len() as u32,
            });
        }
    }
    Ok(RunHeader {
        observation_manifest,
        rule_freezes: ctx.rule_freezes.as_ref().clone(),
        org_id: ctx.org_id.to_string(),
        run_id,
        fingerprint,
        settings_version: version.into(),
        capture_default,
        batch_plan,
        schema_manifest: manifest,
    })
}

#[tracing::instrument(name = "schema_admission", skip_all, fields(run_id = %run_id))]
pub(crate) async fn header(
    ctx: &RuntimeContext,
    inputs: &[IngestionInput],
    run_id: Uuid,
    fingerprint: RequestFingerprint,
    batch_plan: Vec<PlannedBatch>,
    version: &str,
    chunk_size: usize,
) -> Result<RunHeader, PipelineError> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(PipelineError::Cancelled),
        result = tokio::time::timeout(
            Duration::from_millis(ctx.context_settings.read_timeout_ms),
            prepare_header(ctx, inputs, run_id, fingerprint, batch_plan, version, chunk_size),
        ) => result.map_err(|_| PipelineError::StepExecution {
            stage: "schema_admission".into(),
            step: "prepare".into(),
            cause: "schema admission timed out".into(),
            retriable: true,
        })?,
    }
}

/// Resolve exact contracts before external collection, without registering an ingestion run.
pub async fn resolve_profile_manifest(
    ctx: &RuntimeContext,
) -> Result<RunSchemaManifest, PipelineError> {
    kg_core::profiles::validate_bindings(&ctx.profile_bindings)
        .map_err(|e| invalid(e.to_string()))?;
    for source in ctx.profile_bindings.keys() {
        if ctx
            .extraction_settings
            .source_guidance
            .get(source)
            .is_some_and(|g| {
                g.shared_instructions.is_some()
                    || g.instructions.is_some()
                    || g.relationship_instructions.is_some()
                    || g.identity_instructions.is_some()
            })
        {
            return Err(invalid(
                "profile conflicts with legacy source guidance".into(),
            ));
        }
    }
    let manifest = resolve_manifest(ctx, ctx.profile_bindings.keys().cloned().collect()).await?;
    ctx.with_run_schemas(manifest.clone())
        .extraction_settings
        .validate()
        .map_err(|e| invalid(e.to_string()))?;
    Ok(manifest)
}

async fn resolve_manifest(
    ctx: &RuntimeContext,
    sources: std::collections::BTreeSet<String>,
) -> Result<RunSchemaManifest, PipelineError> {
    let base = match &ctx.ontology_store {
        Some(store) if !sources.is_empty() => bounded(ctx, store.get(&ctx.org_id, None))
            .await?
            .unwrap_or_default(),
        _ => Default::default(),
    };
    schemas::validate_definitions(&base).map_err(invalid)?;
    let mut manifest = RunSchemaManifest {
        profiles: Default::default(),
        org_id: ctx.org_id.to_string(),
        sources: Default::default(),
    };
    for source in sources {
        let over = match &ctx.ontology_store {
            Some(store) => bounded(ctx, store.get(&ctx.org_id, Some(&source)))
                .await?
                .unwrap_or_default(),
            None => Default::default(),
        };
        schemas::validate_definitions(&over).map_err(invalid)?;
        let mut effective = base.overlaid_with(&over);
        if let Some(reference) = ctx.profile_bindings.get(&source) {
            let registry = ctx
                .profile_registry
                .as_ref()
                .ok_or_else(|| invalid("profile registry is not configured".into()))?;
            let profile = bounded(ctx, registry.get(&ctx.org_id, reference))
                .await?
                .ok_or_else(|| invalid("profile revision not found in organization".into()))?;
            profile.validate().map_err(|e| invalid(e.to_string()))?;
            if profile.document.reference() != *reference {
                return Err(invalid("registry returned wrong profile revision".into()));
            }
            effective = profile
                .document
                .compose(&effective)
                .map_err(|e| invalid(e.to_string()))?;
            manifest.profiles.insert(source.clone(), profile);
        }
        manifest.sources.insert(source, effective);
        manifest.validate(&ctx.org_id).map_err(invalid)?;
    }
    Ok(manifest)
}
