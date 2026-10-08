//! Opt-in enforcement. With no frozen profile these checks perform no reads or mutations.
use kg_core::{
    errors::StageError,
    models::EntityNode,
    runtime::{stage_output::EdgeExtractionOutput, RuntimeContext},
    traits::EntityLookup,
};
use std::collections::{BTreeMap, BTreeSet};
fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: "profile_validation".into(),
        message: message.into(),
    }
}
pub(crate) fn entity(
    ctx: &RuntimeContext,
    source: &str,
    entity: &EntityNode,
) -> Result<(), StageError> {
    entity_type(ctx, source, &entity.entity_type)
}
pub(crate) fn entity_type(
    ctx: &RuntimeContext,
    source: &str,
    kind: &str,
) -> Result<(), StageError> {
    if let Some(profile) = ctx
        .run_schemas
        .as_ref()
        .and_then(|m| m.profiles.get(source))
    {
        if !profile.document.permits_entity(kind) {
            return Err(violation(
                source,
                profile,
                "undeclared_entity",
                "entity_type",
                kind,
            ));
        }
    }
    Ok(())
}
pub(crate) async fn edges(
    ctx: &RuntimeContext,
    output: &EdgeExtractionOutput,
) -> Result<(), StageError> {
    if ctx
        .run_schemas
        .as_ref()
        .is_none_or(|m| m.profiles.is_empty())
    {
        return Ok(());
    }
    let signatures = output
        .edges
        .iter()
        .map(|e| {
            (
                e.producer_source.as_str(),
                e.source_chain_id,
                e.target_chain_id,
                e.name.as_str(),
            )
        })
        .collect::<Vec<_>>();
    relationships(ctx, &signatures, &output.resolved_nodes).await
}

pub(crate) async fn relationships(
    ctx: &RuntimeContext,
    signatures: &[(&str, uuid::Uuid, uuid::Uuid, &str)],
    nodes: &[EntityNode],
) -> Result<(), StageError> {
    let Some(manifest) = ctx.run_schemas.as_ref().filter(|m| !m.profiles.is_empty()) else {
        return Ok(());
    };
    let profiled: Vec<_> = signatures
        .iter()
        .filter(|e| manifest.profiles.contains_key(e.0))
        .collect();
    if profiled.is_empty() {
        return Ok(());
    }
    let ids: BTreeSet<_> = profiled.iter().flat_map(|e| [e.1, e.2]).collect();
    let mut types: BTreeMap<_, _> = nodes
        .iter()
        .filter(|e| e.org_id == ctx.org_id.as_ref())
        .map(|e| (e.chain_id, e.entity_type.clone()))
        .collect();
    let missing: Vec<_> = ids
        .into_iter()
        .filter(|id| !types.contains_key(id))
        .collect();
    for chunk in missing.chunks(kg_core::traits::graph_reads::MAX_LOOKUP_KEYS) {
        let query = EntityLookup::LatestByChain {
            chain_ids: chunk.to_vec(),
        };
        let rows=tokio::select!{
            biased;
            _=ctx.cancel.cancelled()=>return Err(StageError::Cancelled{stage:"profile_validation".into()}),
            result=tokio::time::timeout(std::time::Duration::from_millis(ctx.context_settings.read_timeout_ms),async {
                let _permit=ctx.semaphore.acquire().await.map_err(|_|kg_core::errors::BackendError::Unavailable("profile lookup closed".into()))?;
                ctx.graph.find_entities(&ctx.org_id,&query).await
            })=>result.map_err(|_|StageError::StepFailed{stage:"profile_validation".into(),step:"endpoints".into(),cause:"profile endpoint lookup timed out".into(),retriable:true})?
        }.map_err(|e|StageError::StepFailed{stage:"profile_validation".into(),step:"endpoints".into(),cause:e.to_string(),retriable:e.is_transient()})?;
        for row in rows {
            if !chunk.contains(&row.chain_id)
                || row.stored.get("org_id").and_then(serde_json::Value::as_str)
                    != Some(ctx.org_id.as_ref())
            {
                return Err(invalid("profile endpoint scope mismatch"));
            }
            types.insert(row.chain_id, row.entity_type);
        }
    }
    for edge in profiled {
        let profile = &manifest.profiles[edge.0];
        let ontology = &manifest.sources[edge.0];
        let source = types
            .get(&edge.1)
            .ok_or_else(|| invalid("profile source endpoint unavailable"))?;
        let target = types
            .get(&edge.2)
            .ok_or_else(|| invalid("profile target endpoint unavailable"))?;
        let name = ontology.canonical_relationship(edge.3).ok_or_else(|| {
            violation(
                edge.0,
                profile,
                "undeclared_relationship",
                "relationship.name",
                edge.3,
            )
        })?;
        if !profile.document.permits_entity(source)
            || !profile.document.permits_entity(target)
            || !ontology.permits_signature(source, target, &name)
        {
            return Err(violation(
                edge.0,
                profile,
                "invalid_relationship_signature",
                "relationship.endpoints",
                &name,
            ));
        }
        if let Some(def) = ontology.edge_types.iter().find(|d| d.name == name) {
            if def
                .source_type
                .as_deref()
                .is_some_and(|t| t != "Entity" && t != source)
                || def
                    .target_type
                    .as_deref()
                    .is_some_and(|t| t != "Entity" && t != target)
            {
                return Err(violation(
                    edge.0,
                    profile,
                    "invalid_relationship_endpoints",
                    "relationship.endpoints",
                    &name,
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

fn violation(
    source: &str,
    profile: &kg_core::profiles::FrozenProfile,
    reason: &str,
    path: &str,
    kind: &str,
) -> StageError {
    StageError::ProfileViolation(Box::new(kg_core::profiles::ProfileViolation {
        source: source.chars().take(4096).collect(),
        profile: profile.document.reference(),
        reason: reason.into(),
        property_path: path.into(),
        type_name: kind.chars().take(256).collect(),
    }))
}

/// Add safe profile context to attribute validation, preserving transient model/storage errors.
pub(crate) fn attribute_error(
    ctx: &RuntimeContext,
    source: &str,
    kind: &str,
    error: StageError,
) -> StageError {
    if matches!(error, StageError::StateValidation { .. }) {
        if let Some(profile) = ctx
            .run_schemas
            .as_ref()
            .and_then(|m| m.profiles.get(source))
        {
            return violation(source, profile, "attribute_validation", "attributes", kind);
        }
    }
    error
}
