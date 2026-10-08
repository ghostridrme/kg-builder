use std::sync::Arc;

use async_trait::async_trait;
use kg_core::{
    errors::StageError,
    runtime::{stage_output::EdgeExtractionOutput, RuntimeContext, StageOutput},
    traits::Stage,
};

/// Preserve configured child relationships whose endpoints survived node persistence.
pub struct DeclaredRelationshipExtractionStage;

#[async_trait]
impl Stage for DeclaredRelationshipExtractionStage {
    fn processing_version(&self) -> String {
        "declared-relationship-decisions-v4".into()
    }

    fn capabilities(&self) -> &'static [kg_core::traits::StageCapability] {
        &[kg_core::traits::StageCapability::DeclaredRelationships]
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::NodeResolution, StageKind::EdgeExtraction)]
    }

    fn name(&self) -> &str {
        "declared_relationship_extraction"
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::NodeResolution(resolution) = input else {
            return Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "expected node resolution".into(),
            });
        };
        if resolution
            .snapshot_nodes
            .iter()
            .any(|snapshot| snapshot.org_id != ctx.org_id.as_ref())
            || resolution
                .sub_edges
                .iter()
                .any(|edge| edge.org_id != ctx.org_id.as_ref())
        {
            return Err(StageError::StateValidation {
                stage: self.name().into(),
                message: "declared relationship scope mismatch".into(),
            });
        }
        let mut edges: Vec<_> = resolution
            .sub_edges
            .iter()
            .filter(|edge| {
                resolution.chunk_entities.as_ref().is_none_or(|targets| {
                    [edge.source_chain_id, edge.target_chain_id]
                        .iter()
                        .all(|chain| {
                            targets
                                .binary_search_by_key(chain, |record| record.chain_id)
                                .is_ok()
                        })
                })
            })
            .cloned()
            .collect();
        let (explicit, directives, declines) = explicit_changes(&resolution, ctx).await?;
        edges.extend(explicit);
        tracing::debug!(
            declared = resolution.sub_edges.len(),
            retained = edges.len(),
            "declared relationships prepared"
        );
        Ok(StageOutput::EdgeExtraction(EdgeExtractionOutput {
            relationship_times: Default::default(),
            relationship_directives: Arc::new(directives),
            reference_report: kg_core::runtime::stage_output::ReferenceReport {
                relationship_declines: declines,
                ..Default::default()
            },
            snapshot_nodes: resolution.snapshot_nodes.clone(),
            resolved_nodes: resolution.live_entities(),
            resolution: Arc::new(resolution),
            edges: Arc::new(edges),
            pending_references: Arc::new(vec![]),
        }))
    }
}

fn invalid(message: impl Into<String>) -> StageError {
    StageError::StateValidation {
        stage: "declared_relationship_extraction".into(),
        message: message.into(),
    }
}

type EndpointRecords =
    std::collections::HashMap<EndpointKey, Vec<kg_core::traits::EntityVersionRecord>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EndpointKey {
    Chain(uuid::Uuid),
    Identity(String),
}

fn endpoint_key(
    endpoint: &kg_core::models::RelationshipEndpoint,
    org: &str,
) -> Result<EndpointKey, StageError> {
    use kg_core::models::RelationshipEndpoint;
    match endpoint {
        RelationshipEndpoint::Chain { chain_id } => Ok(EndpointKey::Chain(*chain_id)),
        RelationshipEndpoint::Identity {
            namespace,
            entity_type,
            key_values,
        } => Ok(EndpointKey::Identity(
            kg_core::identity::IdentityHash::compute_values(
                org,
                namespace,
                entity_type,
                &key_values
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>(),
            )
            .map_err(invalid)?
            .to_hex(),
        )),
    }
}

fn endpoint_lookups(
    changes: impl Iterator<Item = impl AsRef<[kg_core::models::RelationshipChange]>>,
    org: &str,
) -> Result<Vec<kg_core::traits::EntityLookup>, StageError> {
    use kg_core::{
        models::RelationshipChange,
        traits::{graph_reads::MAX_LOOKUP_KEYS, EntityLookup, VersionState},
    };
    let mut chains = std::collections::BTreeSet::new();
    let mut hashes = std::collections::BTreeSet::new();
    for changes in changes {
        for change in changes.as_ref() {
            let observation = match change {
                RelationshipChange::Observe { relationship } => relationship,
                RelationshipChange::Replace { replacement, .. } => replacement,
                RelationshipChange::Cancel { .. } => continue,
            };
            for endpoint in [&observation.source, &observation.target] {
                match endpoint_key(endpoint, org)? {
                    EndpointKey::Chain(chain) => {
                        chains.insert(chain);
                    }
                    EndpointKey::Identity(hash) => {
                        hashes.insert(hash);
                    }
                }
            }
        }
    }
    let chains = chains.into_iter().collect::<Vec<_>>();
    let hashes = hashes.into_iter().collect::<Vec<_>>();
    Ok(chains
        .chunks(MAX_LOOKUP_KEYS)
        .map(|chunk| EntityLookup::LatestByChain {
            chain_ids: chunk.to_vec(),
        })
        .chain(
            hashes
                .chunks(MAX_LOOKUP_KEYS)
                .map(|chunk| EntityLookup::ByIdentity {
                    hashes: chunk.to_vec(),
                    state: VersionState::Live,
                }),
        )
        .collect())
}

async fn read_endpoints<F, Fut>(
    lookups: Vec<kg_core::traits::EntityLookup>,
    mut read: F,
) -> Result<EndpointRecords, StageError>
where
    F: FnMut(kg_core::traits::EntityLookup) -> Fut,
    Fut: std::future::Future<
        Output = Result<Vec<kg_core::traits::EntityVersionRecord>, kg_core::errors::BackendError>,
    >,
{
    let mut out: EndpointRecords = Default::default();
    for lookup in lookups {
        let rows = read(lookup.clone())
            .await
            .map_err(|error| StageError::StepFailed {
                stage: "declared_relationship_extraction".into(),
                step: "endpoint_lookup".into(),
                retriable: error.is_transient(),
                cause: error.to_string(),
            })?;
        for record in rows {
            match &lookup {
                kg_core::traits::EntityLookup::LatestByChain { chain_ids } => {
                    if chain_ids.binary_search(&record.chain_id).is_ok() {
                        out.entry(EndpointKey::Chain(record.chain_id))
                            .or_default()
                            .push(record);
                    }
                }
                kg_core::traits::EntityLookup::ByIdentity { hashes, .. } => {
                    let matched: std::collections::HashSet<_> = record
                        .answering_hashes()
                        .filter(|hash| {
                            hashes
                                .binary_search_by(|candidate| candidate.as_str().cmp(hash))
                                .is_ok()
                        })
                        .map(str::to_owned)
                        .collect();
                    for hash in matched {
                        out.entry(EndpointKey::Identity(hash))
                            .or_default()
                            .push(record.clone());
                    }
                }
                _ => return Err(invalid("unsupported explicit endpoint lookup")),
            }
        }
    }
    Ok(out)
}

fn endpoint(
    endpoint: &kg_core::models::RelationshipEndpoint,
    snapshot: &kg_core::models::SnapshotNode,
    ctx: &RuntimeContext,
    records: &EndpointRecords,
) -> Result<kg_core::traits::EntityVersionRecord, StageError> {
    use kg_core::models::RelationshipEndpoint;
    let key = endpoint_key(endpoint, &ctx.org_id)?;
    let records = records.get(&key).map(Vec::as_slice).unwrap_or_default();
    let mut matches = Vec::new();
    for record in records {
        if !record.is_latest || record.deleted_at.is_some() || record.merged_into.is_some() {
            continue;
        }
        let matching = match endpoint {
            RelationshipEndpoint::Chain { chain_id } => {
                record.chain_id == *chain_id && record.namespace == snapshot.namespace
            }
            RelationshipEndpoint::Identity {
                namespace,
                entity_type,
                key_values,
            } => {
                let properties = kg_core::traits::property_codec::read_properties(&record.stored)
                    .map_err(invalid)?;
                record.namespace == *namespace
                    && record.entity_type == *entity_type
                    && key_values.iter().all(|(key, value)| {
                        if key == "name" {
                            value == &kg_core::models::PropertyValue::String(record.name.clone())
                        } else {
                            properties.get(key) == Some(value)
                        }
                    })
            }
        };
        if matching {
            if record
                .last_transition_at
                .is_some_and(|at| snapshot.captured_at < at)
            {
                return Err(invalid(
                    "explicit relationship capture precedes an endpoint lifecycle transition",
                ));
            }
            matches.push(record);
        }
    }
    if matches.len() != 1 {
        return Err(invalid(
            "explicit relationship endpoint is missing or ambiguous",
        ));
    }
    Ok(matches.remove(0).clone())
}

async fn explicit_changes(
    resolution: &kg_core::runtime::stage_output::NodeResolutionOutput,
    ctx: &RuntimeContext,
) -> Result<
    (
        Vec<kg_core::models::EntityEdge>,
        Vec<kg_core::runtime::stage_output::PendingRelationshipDirective>,
        Vec<kg_core::runtime::stage_output::RelationshipDecline>,
    ),
    StageError,
> {
    use kg_core::{
        models::{EntityEdge, RelationshipChange, RelationshipOrigin},
        runtime::stage_output::{
            ConnectorScope, PendingRelationshipDirective, RelationshipDecline,
            RelationshipDeclineReason, RelationshipDirectiveAction,
        },
    };
    let mut edges = Vec::new();
    let mut directives = Vec::new();
    let mut declines = Vec::new();
    let lookups = endpoint_lookups(
        resolution
            .relationship_changes
            .iter()
            .filter(|(id, _)| {
                resolution
                    .snapshot_nodes
                    .iter()
                    .find(|s| s.uuid == **id)
                    .is_some_and(|s| ctx.policy.for_source(&s.source).declared_relationships)
            })
            .map(|(_, changes)| changes),
        &ctx.org_id,
    )?;
    let endpoint_records = read_endpoints(lookups, |lookup| async move {
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(kg_core::errors::BackendError::Unavailable("declared endpoint lookup cancelled".into())),
            result = tokio::time::timeout(
                std::time::Duration::from_millis(ctx.context_settings.read_timeout_ms),
                ctx.graph.find_entities(&ctx.org_id, &lookup),
            ) => result.unwrap_or_else(|_| Err(kg_core::errors::BackendError::Unavailable("declared endpoint lookup timed out".into()))),
        }
    })
    .await
    .map_err(|error| if ctx.cancel.is_cancelled() {
        StageError::Cancelled { stage: "declared_relationship_extraction".into() }
    } else { error })?;
    for (snapshot_id, changes) in resolution.relationship_changes.iter() {
        if changes.is_empty() {
            continue;
        }
        let snapshot = resolution
            .snapshot_nodes
            .iter()
            .find(|snapshot| snapshot.uuid == *snapshot_id)
            .ok_or_else(|| invalid("relationship commands have no observing snapshot"))?;
        let schemas = resolution
            .schemas
            .get(snapshot_id)
            .ok_or_else(|| invalid("relationship commands have no schema handoff"))?;
        let ontology = schemas
            .for_snapshot(snapshot, &ctx.org_id)
            .map_err(invalid)?;
        for change in changes {
            let operation = match change {
                RelationshipChange::Observe { .. } => "observe",
                RelationshipChange::Replace { .. } => "replace",
                RelationshipChange::Cancel { .. } => "cancel",
            };
            if !ctx
                .policy
                .for_source(&snapshot.source)
                .declared_relationships
            {
                declines.push(RelationshipDecline {
                    snapshot_id: *snapshot_id,
                    source_chain_id: None,
                    target_chain_id: None,
                    name: match change {
                        RelationshipChange::Observe { relationship } => {
                            Some(relationship.name.clone())
                        }
                        RelationshipChange::Replace { replacement, .. } => {
                            Some(replacement.name.clone())
                        }
                        _ => None,
                    },
                    operation: operation.into(),
                    reason: RelationshipDeclineReason::DeclaredDisabled,
                });
                continue;
            }
            let observation = match change {
                RelationshipChange::Observe { relationship } => Some(relationship),
                RelationshipChange::Replace { replacement, .. } => Some(replacement),
                RelationshipChange::Cancel { .. } => None,
            };
            let edge_uuid = uuid::Uuid::new_v4();
            if let Some(observation) = observation {
                let canonical_name = if ctx
                    .run_schemas
                    .as_ref()
                    .is_some_and(|m| m.profiles.contains_key(&snapshot.source))
                {
                    ontology
                        .canonical_relationship(&observation.name)
                        .ok_or_else(|| invalid("profile rejects declared relationship name"))?
                } else {
                    observation.name.clone()
                };
                let source = endpoint(&observation.source, snapshot, ctx, &endpoint_records)?;
                let target = endpoint(&observation.target, snapshot, ctx, &endpoint_records)?;
                if !ctx
                    .namespace_policy
                    .allows(&source.namespace, &target.namespace)
                {
                    declines.push(RelationshipDecline {
                        snapshot_id: *snapshot_id,
                        source_chain_id: Some(source.chain_id),
                        target_chain_id: Some(target.chain_id),
                        name: Some(observation.name.clone()),
                        operation: operation.into(),
                        reason: RelationshipDeclineReason::NamespacePolicy,
                    });
                    continue;
                }
                if !ontology.permits_signature(
                    &source.entity_type,
                    &target.entity_type,
                    &canonical_name,
                ) {
                    return Err(invalid("explicit relationship violates endpoint schema"));
                }
                super::relationship_schema::validate(
                    &canonical_name,
                    &observation.properties,
                    ontology,
                )
                .map_err(invalid)?;
                edges.push(EntityEdge {
                    time_evidence: Some(kg_core::models::RelationshipTimeEvidence::explicit(
                        snapshot.uuid,
                        snapshot.captured_at,
                        observation.valid_from,
                        observation.valid_to,
                    )),
                    uuid: edge_uuid,
                    chain_id: uuid::Uuid::new_v4(),
                    identity_hash: None,
                    cardinality_key: None,
                    org_id: ctx.org_id.to_string(),
                    producer_source: snapshot.source.clone(),
                    origin: RelationshipOrigin::Declared,
                    source_chain_id: source.chain_id,
                    target_chain_id: target.chain_id,
                    name: canonical_name,
                    identity_name: None,
                    description: observation.description.clone(),
                    all_properties: observation.properties.clone(),
                    discovered_by: Some("source_declaration".into()),
                    resolved_by: Some("exact_endpoint".into()),
                    source_property: None,
                    target_identity_field: None,
                    reference_evidence: None,
                    confidence: kg_core::models::edges::CONFIDENCE_DECLARED,
                    justification: None,
                    first_seen_snapshot_id: Some(*snapshot_id),
                    last_seen_snapshot_id: Some(*snapshot_id),
                    last_seen_at: Some(snapshot.captured_at),
                    sync_generation: snapshot.sync_generation,
                    valid_from: observation.valid_from,
                    valid_to: observation.valid_to,
                    version: 1,
                    is_latest: observation.valid_to.is_none(),
                    previous_version_uuid: None,
                    deleted_at: None,
                    deleted_by: None,
                    deletion_reason: None,
                    cancelled_at: None,
                    cancellation_snapshot_id: None,
                    cancellation_context: None,
                    created_at: snapshot.created_at,
                });
            }
            let directive = match change {
                RelationshipChange::Observe { .. } => None,
                RelationshipChange::Replace {
                    target,
                    effective_at,
                    ..
                } => Some((
                    target,
                    effective_at,
                    RelationshipDirectiveAction::Replace {
                        replacement_edge_uuid: edge_uuid,
                    },
                )),
                RelationshipChange::Cancel {
                    target,
                    effective_at,
                } => Some((target, effective_at, RelationshipDirectiveAction::Cancel)),
            };
            if let Some((target, effective_at, action)) = directive {
                directives.push(PendingRelationshipDirective {
                    target: target.clone(),
                    effective_at: *effective_at,
                    action,
                    snapshot_id: *snapshot_id,
                    captured_at: snapshot.captured_at,
                    scope: ConnectorScope::of(snapshot),
                });
            }
        }
    }
    Ok((edges, directives, declines))
}

#[cfg(test)]
mod tests;
