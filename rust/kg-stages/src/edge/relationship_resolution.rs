use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use kg_core::errors::StageError;
use kg_core::models::{EntityEdge, SnapshotNode};
use kg_core::runtime::stage_output::{
    ConnectorScope, EdgeResolutionOutput, PairBaseline, PendingRelationshipDirective,
    ReferenceOwnerBaseline, ReferenceOwnerSelector, RelationBaseline, RelationshipBaseline,
    RelationshipDirectiveAction, StoredRelationship,
};
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::graph_reads::MAX_LOOKUP_KEYS;
use kg_core::traits::{EdgeLookup, EdgeRecord, EntityLookup, Stage};

/// Resolve relationship identities across all observations in the current chunk.
pub struct EdgeResolutionStage;

#[async_trait]
impl Stage for EdgeResolutionStage {
    fn processing_version(&self) -> String {
        kg_core::traits::stage::processing_version(
            "relationship-resolution-v5-inherited-labels",
            &[
                super::relationship_matching::SYSTEM_PROMPT,
                super::relationship_termination::SYSTEM_PROMPT,
            ],
        )
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::EdgeExtraction, StageKind::EdgeResolution)]
    }

    fn name(&self) -> &str {
        "edge_resolution"
    }
    fn is_batch(&self) -> bool {
        true
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        self.process_batch(vec![input], ctx)
            .await?
            .pop()
            .ok_or_else(|| invalid("missing relationship output"))?
    }

    async fn process_batch(
        &self,
        inputs: Vec<StageOutput>,
        ctx: &RuntimeContext,
    ) -> Result<Vec<Result<StageOutput, kg_core::errors::StageError>>, StageError> {
        let mut extractions = Vec::with_capacity(inputs.len());
        let mut observations = Vec::new();
        let mut snapshots = Vec::new();
        for input in inputs {
            let StageOutput::EdgeExtraction(extraction) = input else {
                return Err(invalid("expected extracted relationships"));
            };
            if !extraction.pending_references.is_empty() {
                return Err(invalid("reference resolution must precede edge resolution"));
            }
            for edge in extraction.edges.iter() {
                let mut edge = edge.clone();
                if edge.org_id != ctx.org_id.as_ref()
                    || edge.chain_id.is_nil()
                    || edge.uuid.is_nil()
                {
                    return Err(invalid("invalid relationship identity scope"));
                }
                let snapshot = extraction
                    .snapshot_nodes
                    .iter()
                    .find(|snapshot| {
                        Some(snapshot.uuid) == edge.last_seen_snapshot_id
                            && snapshot.org_id == edge.org_id
                    })
                    .ok_or_else(|| invalid("relationship has no producing observation"))?;
                edge.last_seen_at = Some(snapshot.captured_at);
                if let Some(time) = &edge.time_evidence {
                    time.validate()
                        .map_err(|_| invalid("invalid relationship time evidence"))?;
                    if time.snapshot_id != snapshot.uuid
                        || time.captured_at != snapshot.captured_at
                        || time.resolved_target.is_some()
                        || extraction
                            .relationship_times
                            .get(&edge.uuid)
                            .is_some_and(|frozen| frozen != time)
                    {
                        return Err(invalid(
                            "relationship time evidence disagrees with its observation",
                        ));
                    }
                } else if extraction.relationship_times.contains_key(&edge.uuid) {
                    return Err(invalid("relationship lost its frozen time evidence"));
                }
                if edge.valid_to.is_some_and(|end| end < edge.valid_from) {
                    return Err(invalid("relationship validity ends before it begins"));
                }
                if edge.producer_source.trim().is_empty() {
                    return Err(invalid("relationship has no producer source"));
                }
                let onto =
                    if let Some(schemas) = extraction.resolution.schemas.get(&snapshot.uuid) {
                        schemas
                            .for_snapshot(snapshot, &ctx.org_id)
                            .map_err(|_| invalid("invalid relationship schema scope"))?;
                        let definition = schemas
                            .definitions
                            .get(&edge.producer_source)
                            .ok_or_else(|| {
                                invalid("relationship producer missing from observation schemas")
                            })?;
                        kg_core::runtime::schemas::validate_effective(definition)
                            .map_err(|_| invalid("invalid relationship producer schema"))?;
                        definition.clone()
                    } else if ctx.run_schemas.is_some() || ctx.ontology_store.is_some() {
                        return Err(invalid(
                            "relationship requires prepared observation schemas",
                        ));
                    } else {
                        kg_core::traits::Ontology::default()
                    };
                edge.name = onto
                    .canonical_relationship(&edge.name)
                    .ok_or_else(|| invalid("relationship name violates its producer schema"))?;
                // Identifying properties and cardinality belong to the name the
                // edge was discovered with. Optional naming changes only the
                // label, so a named edge and its generic re-observation must
                // resolve the same definition and therefore the same identity.
                if let Some(identity) = edge.identity_name.take() {
                    edge.identity_name =
                        Some(onto.canonical_relationship(&identity).unwrap_or(identity));
                }
                let identity_name = edge.identity_name.as_deref().unwrap_or(&edge.name);
                let definition = onto
                    .edge_types
                    .iter()
                    .find(|definition| definition.name == identity_name);
                let keys = definition
                    .map(|definition| definition.identifying_properties.as_slice())
                    .unwrap_or(&[]);
                super::relationship_matching::prepare_identity(
                    &mut edge,
                    keys,
                    definition.is_some_and(|definition| definition.single_target),
                    &snapshot.namespace,
                )?;
                super::relationship_schema::validate(&edge.name, &edge.all_properties, &onto)
                    .map_err(|_| {
                        invalid("relationship attributes violate current producer schema")
                    })?;
                observations.push((extractions.len(), edge, onto));
            }
            snapshots.extend(extraction.snapshot_nodes.iter().cloned());
            extractions.push(extraction);
        }
        observations.sort_by(|(ai, a, _), (bi, b, _)| {
            a.last_seen_at
                .cmp(&b.last_seen_at)
                .then_with(|| b.confidence.total_cmp(&a.confidence))
                .then_with(|| ai.cmp(bi))
                .then_with(|| a.uuid.cmp(&b.uuid))
        });
        let raw: Vec<_> = observations
            .iter()
            .map(|(_, edge, _)| edge.clone())
            .collect();
        let directives: Vec<_> = extractions
            .iter()
            .flat_map(|extraction| extraction.relationship_directives.iter().cloned())
            .collect();
        let mut owner_refresh: Vec<_> = extractions
            .iter()
            .flat_map(|extraction| {
                extraction
                    .resolution
                    .reference_owner_refresh
                    .iter()
                    .cloned()
            })
            .collect();
        // Discover previous slots even when an explicit null/empty array produces no edge.
        // Fence the complete incident read so concurrent new slots cannot escape retirement.
        let coverage_chains: BTreeSet<_> = extractions
            .iter()
            .flat_map(|e| &e.reference_report.source_coverage)
            .map(|c| c.chain_id)
            .collect();
        let mut histories: BTreeMap<
            Uuid,
            Vec<kg_core::traits::relationship_timeline::IncidentVersionState>,
        > = coverage_chains.iter().map(|id| (*id, Vec::new())).collect();
        let chains: Vec<_> = coverage_chains.iter().copied().collect();
        for page in chains.chunks(MAX_LOOKUP_KEYS) {
            for record in find_edges(
                ctx,
                &EdgeLookup::VersionsByEndpointChains {
                    chain_ids: page.to_vec(),
                },
                "reference_source_history",
            )
            .await?
            {
                for chain in [record.source_chain_id, record.target_chain_id]
                    .into_iter()
                    .collect::<BTreeSet<_>>()
                {
                    if page.contains(&chain) {
                        histories.get_mut(&chain).expect("requested source").push(
                            kg_core::traits::relationship_timeline::IncidentVersionState {
                                source_chain_id: record.source_chain_id,
                                target_chain_id: record.target_chain_id,
                                properties: kg_core::traits::relationship_timeline::state(
                                    &record.stored,
                                ),
                            },
                        );
                    }
                }
                // Unrelated facts contribute to the incident fence, but are not
                // reference owners and need not satisfy a reference's typed schema.
                if record
                    .stored
                    .get("reference_owner_chain_id")
                    .is_some_and(|value| !value.is_null())
                {
                    if let Some(evidence) = stored_relationship(&record, None)?.reference_evidence {
                        if coverage_chains.contains(&evidence.observing_chain_id) {
                            owner_refresh.push(ReferenceOwnerSelector {
                                chain_id: evidence.observing_chain_id,
                                namespace: evidence.observing_namespace,
                                slot: evidence.slot,
                            });
                        }
                    }
                }
            }
        }
        for (chain, versions) in &mut histories {
            versions.sort_by(|a, b| {
                a.properties["uuid"]
                    .as_str()
                    .cmp(&b.properties["uuid"].as_str())
            });
            kg_core::traits::relationship_timeline::validate_incident(*chain, versions)
                .map_err(|error| invalid(&error.to_string()))?;
        }
        for extraction in &mut extractions {
            extraction.reference_report.source_histories = extraction
                .reference_report
                .source_coverage
                .iter()
                .map(
                    |coverage| kg_core::runtime::stage_output::ReferenceSourceHistory {
                        chain_id: coverage.chain_id,
                        versions: histories[&coverage.chain_id].clone(),
                    },
                )
                .collect();
        }
        let baseline = read_baseline(ctx, &snapshots, &raw, &directives, &owner_refresh).await?;
        validate_directives(&directives, &snapshots, &raw, &baseline)?;
        tracing::debug!(
            commands = directives.len(),
            pairs = baseline.pairs.len(),
            "relationship commands validated"
        );
        let timelines: HashMap<_, _> = baseline
            .pairs
            .iter()
            .map(|pair| {
                super::relationship_timeline::RelationshipTimeline::from_pair(pair)
                    .map(|timeline| ((pair.source_chain_id, pair.target_chain_id), timeline))
            })
            .collect::<Result<_, _>>()?;
        // Each observed candidate and prior observation carries the position that
        // produced it, so a snapshot that later fails cannot leave accepted
        // evidence influencing a survivor at another position.
        let mut observed_candidates: HashMap<(Uuid, Uuid), Vec<(usize, StoredRelationship)>> =
            HashMap::new();
        let mut outputs = vec![Vec::new(); extractions.len()];
        let mut prior_observations: Vec<(usize, EntityEdge, ConnectorScope)> = Vec::new();
        // A model or schema failure resolving one observation must fail only the
        // snapshot that owns it, not every snapshot in the chunk; entity identity
        // already isolates per input. Cancellation and identity-revision changes
        // stay batch-fatal because they invalidate the whole run.
        let mut failed_positions: HashMap<usize, StageError> = HashMap::new();
        // Which other positions' accepted evidence each position's survivors were
        // resolved against. If any of those positions is rejected, the dependent
        // position is rejected too, so no committed edge rests on evidence that
        // will not commit.
        let mut position_deps: HashMap<usize, BTreeSet<usize>> = HashMap::new();
        for (position, mut edge, onto) in observations {
            if ctx.cancel.is_cancelled() {
                return Err(StageError::Cancelled {
                    stage: self.name().into(),
                });
            }
            if failed_positions.contains_key(&position) {
                continue;
            }
            let key = (edge.source_chain_id, edge.target_chain_id);
            let timeline = timelines
                .get(&key)
                .ok_or_else(|| invalid("missing relationship pair timeline"))?;
            if edge
                .time_evidence
                .as_ref()
                .is_some_and(|time| time.end_only())
            {
                let snapshot = extractions[position]
                    .snapshot_nodes
                    .iter()
                    .find(|snapshot| Some(snapshot.uuid) == edge.last_seen_snapshot_id)
                    .ok_or_else(|| invalid("termination has no producing snapshot"))?;
                let scope = ConnectorScope {
                    namespace: snapshot.namespace.clone(),
                    source: edge.producer_source.clone(),
                };
                // Only this directed pair and producer scope can contribute to
                // the termination. Unrelated observations are not dependencies.
                let relevant: Vec<_> = prior_observations
                    .iter()
                    .filter(|(_, prior, owner)| {
                        prior.source_chain_id == edge.source_chain_id
                            && prior.target_chain_id == edge.target_chain_id
                            && owner == &scope
                    })
                    .collect();
                let prior_edges: Vec<(EntityEdge, ConnectorScope)> = relevant
                    .iter()
                    .map(|(_, prior, owner)| (prior.clone(), owner.clone()))
                    .collect();
                if let Err(error) = super::relationship_termination::resolve_with_prior(
                    &mut edge,
                    timeline,
                    &prior_edges,
                    &scope,
                    ctx,
                    &onto,
                )
                .await
                {
                    if is_batch_fatal(&error) {
                        return Err(error);
                    }
                    failed_positions.insert(position, error);
                    continue;
                }
                let deps = position_deps.entry(position).or_default();
                deps.extend(relevant.iter().map(|(origin, _, _)| *origin));
                prior_observations.push((position, edge.clone(), scope));
                outputs[position].push(edge);
                continue;
            }
            let mut pool = timeline.candidates_at(edge.valid_from);
            // Positions whose accepted edges actually enter the pool this match is
            // resolved against; recorded as dependencies once the edge is accepted.
            let mut edge_deps: BTreeSet<usize> = BTreeSet::new();
            let updates = observed_candidates.entry(key).or_default();
            for (origin, update) in updates.iter() {
                if let Some(prior) = pool
                    .iter_mut()
                    .find(|prior| prior.chain_id == update.chain_id)
                {
                    // A repeated current interval cannot replace a pending successor.
                    if update.valid_from <= edge.valid_from
                        && (prior.valid_from <= update.valid_from || prior.uuid == update.uuid)
                    {
                        *prior = update.clone();
                        edge_deps.insert(*origin);
                    }
                } else {
                    pool.push(update.clone());
                    edge_deps.insert(*origin);
                }
            }
            // Endpoint types come from this observation's resolved entities or
            // the run's committed target index; no further reads.
            let extraction = &extractions[position];
            let endpoint_type = |chain_id: Uuid| -> Option<String> {
                extraction
                    .resolved_nodes
                    .iter()
                    .find(|node| node.chain_id == chain_id)
                    .map(|node| node.entity_type.clone())
                    .or_else(|| {
                        let targets = extraction.resolution.chunk_entities.as_ref()?;
                        let position = targets
                            .binary_search_by_key(&chain_id, |target| target.chain_id)
                            .ok()?;
                        Some(targets[position].entity_type.clone())
                    })
            };
            let endpoint_types =
                endpoint_type(edge.source_chain_id).zip(endpoint_type(edge.target_chain_id));
            if let Err(error) = super::relationship_matching::resolve_identity(
                &mut edge,
                &pool,
                ctx,
                &onto,
                endpoint_types
                    .as_ref()
                    .map(|(source, target)| (source.as_str(), target.as_str())),
            )
            .await
            {
                if is_batch_fatal(&error) {
                    return Err(error);
                }
                failed_positions.insert(position, error);
                continue;
            }
            if super::relationship_schema::validate(&edge.name, &edge.all_properties, &onto)
                .is_err()
            {
                failed_positions.insert(
                    position,
                    invalid("resolved relationship violates current producer schema"),
                );
                continue;
            }
            let mut candidate = super::relationship_matching::observed_relationship(&edge);
            let prior = pool.iter().find(|prior| prior.chain_id == edge.chain_id);
            let current_repeat = prior.is_some_and(|prior| {
                prior.valid_from <= edge.valid_from
                    && prior.ended_at.is_some_and(|end| edge.valid_from < end)
                    && edge.valid_to.is_none()
                    && edge.name == prior.name
                    && edge.description == prior.description
                    && edge.all_properties == prior.all_properties
                    && (edge.confidence - prior.confidence).abs() < 1e-6
                    && prior
                        .latest_observation
                        .is_none_or(|at| edge.last_seen_at.is_some_and(|observed| observed >= at))
            });
            if current_repeat {
                candidate = prior
                    .cloned()
                    .ok_or_else(|| invalid("missing repeated relationship"))?;
                candidate.latest_observation = edge.last_seen_at;
            }
            if current_repeat || prior.is_none_or(|prior| can_replace_candidate(prior, &edge)) {
                // The candidate now reflects this position's observation; the
                // transitive dependency on the evidence it rested on travels
                // through this position's own dependency set below.
                if let Some((origin, update)) = updates
                    .iter_mut()
                    .find(|(_, prior)| prior.chain_id == edge.chain_id)
                {
                    *origin = position;
                    *update = candidate;
                } else {
                    updates.push((position, candidate));
                }
            }
            position_deps
                .entry(position)
                .or_default()
                .extend(edge_deps.iter().copied());
            let snapshot = extractions[position]
                .snapshot_nodes
                .iter()
                .find(|s| Some(s.uuid) == edge.last_seen_snapshot_id)
                .ok_or_else(|| invalid("missing relationship snapshot"))?;
            prior_observations.push((
                position,
                edge.clone(),
                ConnectorScope {
                    namespace: snapshot.namespace.clone(),
                    source: edge.producer_source.clone(),
                },
            ));
            outputs[position].push(edge);
        }
        // Matching and terminations may adopt a stored name. Check the final
        // signatures, including commands that do not create a new edge.
        if ctx
            .run_schemas
            .as_ref()
            .is_some_and(|m| !m.profiles.is_empty())
        {
            for (position, (extraction, observed)) in extractions.iter().zip(&outputs).enumerate() {
                if failed_positions.contains_key(&position) {
                    continue;
                }
                let mut signatures = observed
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
                for directive in extraction.relationship_directives.iter() {
                    let target = &directive.target;
                    let stored = baseline
                        .pairs
                        .iter()
                        .find(|p| {
                            p.source_chain_id == target.source_chain_id
                                && p.target_chain_id == target.target_chain_id
                        })
                        .and_then(|p| {
                            p.versions.iter().find(|v| {
                                v.get("uuid").and_then(serde_json::Value::as_str)
                                    == Some(target.version_uuid.to_string().as_str())
                            })
                        })
                        .ok_or_else(|| invalid("profile command target missing"))?;
                    let name = stored
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| invalid("profile command name missing"))?;
                    signatures.push((
                        &directive.scope.source,
                        target.source_chain_id,
                        target.target_chain_id,
                        name,
                    ));
                }
                crate::profiles::relationships(ctx, &signatures, &extraction.resolved_nodes)
                    .await?;
            }
        }
        // Propagate rejection: any position whose survivors were resolved against
        // a rejected position's evidence is itself rejected, transitively, so no
        // committed edge rests on evidence that will not commit. Dependencies
        // point only at other positions' accepted evidence; iterate to a fixpoint.
        let mut rejected: BTreeSet<usize> = failed_positions.keys().copied().collect();
        loop {
            let mut changed = false;
            for (position, deps) in &position_deps {
                if !rejected.contains(position) && deps.iter().any(|dep| rejected.contains(dep)) {
                    rejected.insert(*position);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        let baseline = Arc::new(baseline);
        Ok(extractions
            .into_iter()
            .zip(outputs)
            .enumerate()
            .map(|(position, (extraction, observed))| {
                if let Some(error) = failed_positions.remove(&position) {
                    return Err(error);
                }
                if rejected.contains(&position) {
                    return Err(invalid(
                        "relationship observation depended on a rejected snapshot",
                    ));
                }
                Ok(StageOutput::EdgeResolution(EdgeResolutionOutput {
                    relationship_assessments: Default::default(),
                    contradiction_timelines: Default::default(),
                    relationship_directives: extraction.relationship_directives,
                    reference_report: extraction.reference_report.clone(),
                    snapshot_nodes: extraction.snapshot_nodes,
                    resolution: extraction.resolution,
                    resolved_nodes: extraction.resolved_nodes,
                    observed: Arc::new(observed),
                    baseline: baseline.clone(),
                }))
            })
            .collect())
    }
}

/// Resolution failures that invalidate the whole chunk rather than one snapshot:
/// a cancellation or an identity-revision change. Everything else (a model
/// timeout, a malformed answer, a schema violation) fails only its own input.
fn is_batch_fatal(error: &StageError) -> bool {
    matches!(
        error,
        StageError::Cancelled { .. } | StageError::IdentityRevisionChanged
    )
}

fn validate_directives(
    directives: &[PendingRelationshipDirective],
    snapshots: &[SnapshotNode],
    observations: &[EntityEdge],
    baseline: &RelationshipBaseline,
) -> Result<(), StageError> {
    for directive in directives {
        let snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.uuid == directive.snapshot_id)
            .ok_or_else(|| invalid("relationship command has no producing snapshot"))?;
        if directive.captured_at != snapshot.captured_at
            || directive.scope != ConnectorScope::of(snapshot)
        {
            return Err(invalid("relationship command scope mismatch"));
        }
        let target = &directive.target;
        let stored = baseline
            .pairs
            .iter()
            .find(|pair| {
                pair.source_chain_id == target.source_chain_id
                    && pair.target_chain_id == target.target_chain_id
            })
            .and_then(|pair| {
                pair.versions.iter().find(|version| {
                    version
                        .get("uuid")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|value| Uuid::parse_str(value).ok())
                        == Some(target.version_uuid)
                        && version
                            .get("chain_id")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| Uuid::parse_str(value).ok())
                            == Some(target.chain_id)
                })
            })
            .ok_or_else(|| invalid("relationship command target is unavailable"))?;
        if stored
            .get("producer_source")
            .and_then(serde_json::Value::as_str)
            != Some(directive.scope.source.as_str())
            || stored
                .get("producer_namespace")
                .and_then(serde_json::Value::as_str)
                != Some(directive.scope.namespace.as_str())
        {
            return Err(invalid("relationship command target is unavailable"));
        }
        if let RelationshipDirectiveAction::Replace {
            replacement_edge_uuid,
        } = directive.action
        {
            let replacement = observations
                .iter()
                .find(|edge| edge.uuid == replacement_edge_uuid)
                .ok_or_else(|| invalid("relationship replacement observation is missing"))?;
            if replacement.last_seen_snapshot_id != Some(directive.snapshot_id)
                || replacement.producer_source != directive.scope.source
                || replacement.origin != kg_core::models::RelationshipOrigin::Declared
            {
                return Err(invalid("relationship replacement scope mismatch"));
            }
        }
    }
    Ok(())
}

fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: "edge_resolution".into(),
        message: message.into(),
    }
}

/// Capture complete timelines, then select lineage heads for identity decisions.
async fn read_baseline(
    ctx: &RuntimeContext,
    snapshot_nodes: &[SnapshotNode],
    observed: &[EntityEdge],
    directives: &[PendingRelationshipDirective],
    owner_refresh: &[ReferenceOwnerSelector],
) -> Result<RelationshipBaseline, StageError> {
    let mut pairs: Vec<(Uuid, Uuid)> = observed
        .iter()
        .map(|e| (e.source_chain_id, e.target_chain_id))
        .collect();
    pairs.extend(directives.iter().map(|directive| {
        (
            directive.target.source_chain_id,
            directive.target.target_chain_id,
        )
    }));
    pairs.sort_unstable();
    pairs.dedup();
    let mut stored_pairs: HashMap<(Uuid, Uuid), Vec<StoredRelationship>> = HashMap::new();
    let mut ended = Vec::new();
    let mut timelines: HashMap<(Uuid, Uuid), Vec<kg_core::traits::GraphProperties>> =
        HashMap::new();
    for chunk in pairs.chunks(MAX_LOOKUP_KEYS) {
        let records = find_edges(
            ctx,
            &EdgeLookup::VersionsByChainPairs {
                pairs: chunk.to_vec(),
            },
            "pair_lookup",
        )
        .await?;
        let mut newest: HashMap<(Uuid, Uuid, String), u32> = HashMap::new();
        for record in &records {
            let pair = (record.source_chain_id, record.target_chain_id);
            if chunk.binary_search(&pair).is_err() {
                return Err(invalid("timeline lookup returned an unexpected pair"));
            }
            let state = kg_core::traits::relationship_timeline::state(&record.stored);
            kg_core::traits::relationship_timeline::validate(
                pair.0,
                pair.1,
                std::slice::from_ref(&state),
            )
            .map_err(|_| invalid("invalid stored relationship timeline"))?;
            let chain = state["chain_id"]
                .as_str()
                .ok_or_else(|| invalid("missing relationship chain"))?;
            newest
                .entry((pair.0, pair.1, chain.to_owned()))
                .and_modify(|version| *version = (*version).max(record.version))
                .or_insert(record.version);
            timelines.entry(pair).or_default().push(state);
        }
        for record in records {
            let chain = record.stored["chain_id"]
                .as_str()
                .ok_or_else(|| invalid("missing relationship chain"))?;
            let open = record.is_latest
                && ["valid_to", "invalid_at", "deleted_at"].iter().all(|key| {
                    record
                        .stored
                        .get(*key)
                        .is_none_or(serde_json::Value::is_null)
                });
            if newest.get(&(
                record.source_chain_id,
                record.target_chain_id,
                chain.to_owned(),
            )) != Some(&record.version)
                && !open
            {
                continue;
            }
            let stored = stored_relationship(&record, None)?;
            if stored.ended_at.is_some() || stored.cancelled_at.is_some() {
                ended.push(stored);
            } else {
                stored_pairs
                    .entry((record.source_chain_id, record.target_chain_id))
                    .or_default()
                    .push(stored);
            }
        }
    }

    let mut relations: BTreeSet<(Uuid, String)> = observed
        .iter()
        .filter(|e| e.cardinality_key.is_some())
        .map(|e| (e.source_chain_id, e.name.clone()))
        .collect();
    // A keyed lineage can be stored under a label optional naming gave it. A
    // generic re-observation inherits that label during matching, so the
    // relation it will be planned under needs its live-set baseline as well.
    for edge in observed {
        let (Some(_), Some(hash)) = (&edge.cardinality_key, &edge.identity_hash) else {
            continue;
        };
        relations.extend(
            stored_pairs
                .get(&(edge.source_chain_id, edge.target_chain_id))
                .into_iter()
                .flatten()
                .chain(&ended)
                .filter(|stored| {
                    stored.identity_hash.as_ref() == Some(hash) && stored.name != edge.name
                })
                .map(|stored| (edge.source_chain_id, stored.name.clone())),
        );
    }
    let mut reference_owners: BTreeSet<ReferenceOwnerSelector> = observed
        .iter()
        .filter_map(|edge| edge.reference_evidence.as_ref())
        .map(|evidence| ReferenceOwnerSelector {
            chain_id: evidence.observing_chain_id,
            namespace: evidence.observing_namespace.clone(),
            slot: evidence.slot.clone(),
        })
        .collect();
    reference_owners.extend(owner_refresh.iter().cloned());
    let mut owner_timelines: BTreeMap<
        ReferenceOwnerSelector,
        Vec<kg_core::traits::relationship_timeline::IncidentVersionState>,
    > = reference_owners
        .iter()
        .cloned()
        .map(|owner| (owner, Vec::new()))
        .collect();
    let mut owner_live: BTreeMap<ReferenceOwnerSelector, Vec<StoredRelationship>> =
        reference_owners
            .iter()
            .cloned()
            .map(|owner| (owner, Vec::new()))
            .collect();
    let owner_selectors: Vec<_> = reference_owners.iter().cloned().collect();
    // The adapter's history lookahead is global to one query. Read one owner at
    // a time so that limit remains a per-owner completeness guard.
    for chunk in owner_selectors.chunks(1) {
        let records = find_edges(
            ctx,
            &EdgeLookup::VersionsByReferenceOwners {
                owners: chunk.to_vec(),
            },
            "reference_owner_timeline_lookup",
        )
        .await?;
        for record in records {
            let stored = stored_relationship(&record, None)?;
            let evidence = stored
                .reference_evidence
                .as_ref()
                .ok_or_else(|| invalid("reference owner lookup returned unowned edge"))?;
            let selector = ReferenceOwnerSelector {
                chain_id: evidence.observing_chain_id,
                namespace: evidence.observing_namespace.clone(),
                slot: evidence.slot.clone(),
            };
            if !reference_owners.contains(&selector) {
                return Err(invalid(
                    "reference owner lookup returned an unexpected owner",
                ));
            }
            owner_timelines
                .get_mut(&selector)
                .expect("validated selector")
                .push(
                    kg_core::traits::relationship_timeline::IncidentVersionState {
                        source_chain_id: record.source_chain_id,
                        target_chain_id: record.target_chain_id,
                        properties: kg_core::traits::relationship_timeline::state(&record.stored),
                    },
                );
            if record.is_latest
                && stored.ended_at.is_none()
                && stored.cancelled_at.is_none()
                && record.invalid_at.is_none()
            {
                owner_live
                    .get_mut(&selector)
                    .expect("validated selector")
                    .push(stored);
            }
        }
    }
    for (selector, versions) in &mut owner_timelines {
        versions.sort_by(|a, b| {
            a.properties["uuid"]
                .as_str()
                .cmp(&b.properties["uuid"].as_str())
        });
        kg_core::traits::relationship_timeline::validate_incident(selector.chain_id, versions)
            .map_err(|_| invalid("invalid reference owner timeline"))?;
    }
    let mut relation_timelines: BTreeMap<
        _,
        Vec<kg_core::traits::relationship_timeline::VersionState>,
    > = BTreeMap::new();
    let selectors: Vec<_> = relations.iter().cloned().collect();
    for chunk in selectors.chunks(MAX_LOOKUP_KEYS) {
        let records = find_edges(
            ctx,
            &EdgeLookup::VersionsByRelations {
                relations: chunk.to_vec(),
            },
            "relation_timeline_lookup",
        )
        .await?;
        for record in records {
            let key = (record.source_chain_id, record.name.clone());
            if chunk.binary_search(&key).is_err() {
                return Err(invalid("timeline lookup returned an unexpected relation"));
            }
            relation_timelines.entry(key).or_default().push(
                kg_core::traits::relationship_timeline::VersionState {
                    target_chain_id: record.target_chain_id,
                    properties: kg_core::traits::relationship_timeline::state(&record.stored),
                },
            );
        }
    }
    for ((source, name), versions) in &mut relation_timelines {
        versions.sort_by(|a, b| {
            a.properties["uuid"]
                .as_str()
                .cmp(&b.properties["uuid"].as_str())
        });
        kg_core::traits::relationship_timeline::validate_relation(*source, name, versions)
            .map_err(|_| invalid("invalid stored relation timeline"))?;
    }
    let mut live: BTreeMap<(Uuid, String), Vec<StoredRelationship>> = relations
        .iter()
        .map(|key| (key.clone(), Vec::new()))
        .collect();
    if !relations.is_empty() {
        // Which of this snapshot's connector scopes each live relationship
        // belongs to, if any.
        let scopes: BTreeSet<ConnectorScope> = observed
            .iter()
            .filter_map(|edge| {
                snapshot_nodes
                    .iter()
                    .find(|snapshot| Some(snapshot.uuid) == edge.last_seen_snapshot_id)
                    .map(|snapshot| ConnectorScope {
                        namespace: snapshot.namespace.clone(),
                        source: edge.producer_source.clone(),
                    })
            })
            .collect();
        let sources: Vec<Uuid> = relations
            .iter()
            .map(|(source, _)| *source)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for chain_ids in sources.chunks(MAX_LOOKUP_KEYS) {
            let records = find_edges(
                ctx,
                &EdgeLookup::LiveByEndpointChains {
                    chain_ids: chain_ids.to_vec(),
                },
                "relation_lookup",
            )
            .await?;
            for record in records {
                if let Some(set) = live.get_mut(&(record.source_chain_id, record.name.clone())) {
                    set.push(stored_relationship(
                        &record,
                        producer_scope(&record, &scopes)?,
                    )?);
                }
            }
        }
    }
    // Pair baselines and relation members describe the same stored records;
    // keep their scopes consistent.
    for set in live.values() {
        for stored in set {
            if let Some(pair) =
                stored_pairs.get_mut(&(stored.source_chain_id, stored.target_chain_id))
            {
                for member in pair.iter_mut().filter(|member| member.uuid == stored.uuid) {
                    member.scope = stored.scope.clone();
                }
            }
        }
    }

    // A relation member whose target no longer has a live entity head is a
    // legacy orphan: the target was deleted or merged away without closing the
    // edge. Report those targets so single-target supersession leaves the
    // orphan edge open (reconciliation closes it later) instead of retiring it
    // as if the retarget had superseded a live relationship.
    let mut orphan_targets = Vec::new();
    if !relation_timelines.is_empty() {
        let relation_targets: BTreeSet<Uuid> = relation_timelines
            .values()
            .flatten()
            .map(|version| version.target_chain_id)
            .collect();
        let candidates: Vec<Uuid> = relation_targets.iter().copied().collect();
        let mut live_targets = BTreeSet::new();
        for chunk in candidates.chunks(MAX_LOOKUP_KEYS) {
            let heads = ctx
                .graph
                .find_entities(
                    ctx.org_id.as_ref(),
                    &EntityLookup::LatestByChain {
                        chain_ids: chunk.to_vec(),
                    },
                )
                .await
                .map_err(|e| StageError::StepFailed {
                    stage: "edge_resolution".into(),
                    step: "orphan_target_lookup".into(),
                    cause: e.to_string(),
                    retriable: e.is_transient(),
                })?;
            for head in heads {
                live_targets.insert(head.chain_id);
            }
        }
        orphan_targets = relation_targets
            .into_iter()
            .filter(|target| !live_targets.contains(target))
            .collect();
    }

    let mut chain_pairs = BTreeMap::new();
    for (pair, versions) in &timelines {
        for properties in versions {
            let chain = properties["chain_id"]
                .as_str()
                .ok_or_else(|| invalid("missing relationship chain"))?;
            if chain_pairs
                .insert(chain, pair)
                .is_some_and(|previous| previous != pair)
            {
                return Err(invalid("relationship lineage changes endpoint pair"));
            }
        }
    }
    ended.sort_by_key(|head| head.chain_id);
    Ok(RelationshipBaseline {
        ended,
        pairs: pairs
            .iter()
            .map(|(source, target)| PairBaseline {
                versions: timelines.remove(&(*source, *target)).unwrap_or_default(),
                source_chain_id: *source,
                target_chain_id: *target,
                live: {
                    let mut records = stored_pairs
                        .get(&(*source, *target))
                        .cloned()
                        .unwrap_or_default();
                    records.sort_by_key(|record| record.uuid);
                    records
                },
            })
            .collect(),
        relations: live
            .into_iter()
            .map(|((source_chain_id, name), mut live)| {
                live.sort_by_key(|r| r.uuid);
                RelationBaseline {
                    versions: relation_timelines
                        .remove(&(source_chain_id, name.clone()))
                        .unwrap_or_default(),
                    source_chain_id,
                    name,
                    live,
                }
            })
            .collect(),
        reference_owners: owner_live
            .into_iter()
            .map(|(selector, mut live)| {
                live.sort_by_key(|edge| edge.uuid);
                ReferenceOwnerBaseline {
                    versions: owner_timelines.remove(&selector).unwrap_or_default(),
                    selector,
                    live,
                }
            })
            .collect(),
        orphan_targets,
    })
}

fn producer_scope(
    record: &EdgeRecord,
    observed: &BTreeSet<ConnectorScope>,
) -> Result<Option<ConnectorScope>, StageError> {
    let text = |key| {
        record
            .stored
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| invalid("stored relationship has invalid producer scope"))
    };
    let scope = ConnectorScope {
        namespace: text("producer_namespace")?.to_owned(),
        source: text("producer_source")?.to_owned(),
    };
    Ok(observed.contains(&scope).then_some(scope))
}

async fn find_edges(
    ctx: &RuntimeContext,
    lookup: &EdgeLookup,
    step: &str,
) -> Result<Vec<EdgeRecord>, StageError> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(StageError::Cancelled { stage: "edge_resolution".into() }),
        result = tokio::time::timeout(
            std::time::Duration::from_millis(ctx.context_settings.read_timeout_ms),
            ctx.graph.find_edges(ctx.org_id.as_ref(), lookup),
        ) => result.map_err(|_| StageError::StepFailed {
            stage: "edge_resolution".into(), step: step.into(),
            cause: "relationship lookup timed out".into(), retriable: true,
        })?.map_err(|e| StageError::StepFailed {
            stage: "edge_resolution".into(), step: step.into(),
            cause: e.to_string(), retriable: e.is_transient(),
        }),
    }
}

fn stored_relationship(
    record: &EdgeRecord,
    scope: Option<ConnectorScope>,
) -> Result<StoredRelationship, StageError> {
    super::relationship_timeline::decode(
        record.source_chain_id,
        record.target_chain_id,
        &record.stored,
        scope,
    )
}

fn can_replace_candidate(prior: &StoredRelationship, edge: &EntityEdge) -> bool {
    prior.ended_at.is_none_or(|end| {
        edge.valid_from > end
            || (edge.valid_from == prior.valid_from
                && edge.valid_to == prior.ended_at
                && edge.name == prior.name
                && edge.description == prior.description
                && edge.all_properties == prior.all_properties
                && (edge.confidence - prior.confidence).abs() < 1e-6)
    }) && prior
        .latest_observation
        .is_none_or(|at| edge.last_seen_at.is_some_and(|observed| observed >= at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record() -> EdgeRecord {
        EdgeRecord {
            uuid: Uuid::from_u128(1),
            source_chain_id: Uuid::from_u128(2),
            target_chain_id: Uuid::from_u128(3),
            name: "USES".into(),
            version: 1,
            is_latest: true,
            confidence: 1.0,
            valid_from: None,
            invalid_at: None,
            sync_generation: None,
            source_collections: vec![],
            stored: Default::default(),
        }
    }

    #[test]
    fn producer_scope_comes_from_relationship_and_requires_both_fields() {
        let mut record = record();
        let scope = ConnectorScope {
            source: "github".into(),
            namespace: "prod".into(),
        };
        let observed = BTreeSet::from([scope.clone()]);
        assert!(producer_scope(&record, &observed).is_err());
        record
            .stored
            .insert("producer_source".into(), json!("github"));
        record
            .stored
            .insert("producer_namespace".into(), json!("prod"));
        assert_eq!(producer_scope(&record, &observed).unwrap(), Some(scope));
        record
            .stored
            .insert("producer_namespace".into(), json!("dev"));
        assert_eq!(producer_scope(&record, &observed).unwrap(), None);
    }
    #[test]
    fn closed_candidate_pool_tracks_only_observations_the_planner_accepts() {
        let source = crate::node::entity_versioning::tests::test_entity("api");
        let mut edge = crate::edge::reference_extraction::build_fk_edge(
            &source,
            Uuid::new_v4(),
            "Service",
            "target",
            "dependency",
            "name",
            false,
        )
        .unwrap();
        let at = edge.valid_from;
        edge.last_seen_at = Some(at + chrono::Duration::days(3));
        edge.valid_to = Some(at + chrono::Duration::days(1));
        let prior = super::super::relationship_matching::observed_relationship(&edge);
        edge.last_seen_at = Some(at + chrono::Duration::days(4));
        assert!(can_replace_candidate(&prior, &edge));
        edge.description = "changed historical assertion".into();
        assert!(!can_replace_candidate(&prior, &edge));
        edge.valid_to = None;
        assert!(!can_replace_candidate(&prior, &edge));
        edge.valid_from = at + chrono::Duration::days(2);
        assert!(can_replace_candidate(&prior, &edge));
        edge.last_seen_at = Some(at + chrono::Duration::days(2));
        assert!(!can_replace_candidate(&prior, &edge));
    }

    // ---- merged from `mod processing_tests`

    use crate::composition::{ingestion_pipeline_with_recipe, PipelineRecipe};
    use kg_core::traits::stage::processing_version;

    #[test]
    fn engine_relationship_recipes_fingerprint_reference_and_termination_prompts() {
        {
            let recipe = PipelineRecipe::DeclaredAndReferences;
            let runner = ingestion_pipeline_with_recipe(Default::default(), recipe).unwrap();
            let reference = runner
                .edge_stages
                .iter()
                .find(|stage| stage.name() == "reference_resolution")
                .unwrap();
            assert_eq!(
                reference.processing_version(),
                super::super::reference_resolution::processing_version()
            );
            assert!(reference
                .processing_version()
                .starts_with("reference-resolution-v8-restricted-key-evidence:"));
            let resolution = runner
                .relationship_resolution_stages
                .iter()
                .find(|stage| stage.name() == "edge_resolution")
                .unwrap();
            let duplication = super::super::relationship_matching::SYSTEM_PROMPT;
            let termination = super::super::relationship_termination::SYSTEM_PROMPT;
            assert_eq!(
                resolution.processing_version(),
                processing_version(
                    "relationship-resolution-v5-inherited-labels",
                    &[duplication, termination]
                )
            );
            assert_ne!(
                resolution.processing_version(),
                processing_version(
                    "relationship-resolution-v5-inherited-labels",
                    &[duplication, "changed termination prompt"]
                )
            );
        }
    }

    // ---- merged from `mod read_cancellation_tests`

    #[tokio::test]
    async fn cancelled_baseline_read_does_not_touch_storage() {
        use kg_core::{
            runtime::RuntimeContextBuilder,
            test_support::{MockEmbedBackend, MockLlmBackend, UnreachableGraph},
        };
        let ctx = RuntimeContextBuilder::new("org")
            .graph(std::sync::Arc::new(UnreachableGraph))
            .llm_extraction(std::sync::Arc::new(MockLlmBackend::empty()))
            .llm_default(std::sync::Arc::new(MockLlmBackend::empty()))
            .embedder(std::sync::Arc::new(MockEmbedBackend::default_dimension()))
            .build()
            .unwrap();
        ctx.cancel.cancel();
        let result = find_edges(
            &ctx,
            &EdgeLookup::VersionsByEndpointChains {
                chain_ids: vec![Uuid::new_v4()],
            },
            "cancel_test",
        )
        .await;
        assert!(matches!(result, Err(StageError::Cancelled { .. })));
    }

    /// Gate: (1) a relationship optional naming labelled keeps the trusted
    /// identity of the generic relationship it was discovered as; (2) resolving
    /// identifying properties by the label instead of the discovery name gives
    /// the labelled edge another hash, so a later generic re-observation forks
    /// a second lineage; (3) `relationship_matching` tests start after the
    /// definition is chosen, which only this stage does; (4) no seam.
    #[tokio::test]
    async fn a_named_relationship_keeps_the_identity_of_its_discovery_name() {
        use kg_core::{
            errors::BackendError,
            models::edges::GENERIC_RELATIONSHIP_NAME,
            runtime::{
                schemas::ObservationSchemas,
                stage_output::{EdgeExtractionOutput, NodeResolutionOutput},
                RuntimeContextBuilder,
            },
            test_support::{MockEmbedBackend, MockLlmBackend},
            traits::*,
        };
        struct Empty;
        #[async_trait]
        impl SearchBackend for Empty {}
        #[async_trait]
        impl GraphBackend for Empty {
            async fn apply_mutations(
                &self,
                _: &str,
                _: &[GraphMutation],
            ) -> Result<(), BackendError> {
                unreachable!()
            }
            async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
                unreachable!()
            }
            async fn commit_batch(
                &self,
                _: &MutationBatch,
            ) -> Result<CommittedBatch, BackendError> {
                unreachable!()
            }
            async fn committed_batches(
                &self,
                _: &str,
                _: Uuid,
            ) -> Result<Vec<CommittedBatch>, BackendError> {
                unreachable!()
            }
            async fn find_entities(
                &self,
                _: &str,
                _: &EntityLookup,
            ) -> Result<Vec<EntityVersionRecord>, BackendError> {
                Ok(vec![])
            }
            async fn find_edges(
                &self,
                _: &str,
                _: &EdgeLookup,
            ) -> Result<Vec<EdgeRecord>, BackendError> {
                Ok(vec![])
            }
            async fn health(&self) -> Result<(), BackendError> {
                Ok(())
            }
            async fn connect(&self) -> Result<(), BackendError> {
                Ok(())
            }
            async fn close(&self) -> Result<(), BackendError> {
                Ok(())
            }
        }
        let ctx = RuntimeContextBuilder::new("org")
            .graph(Arc::new(Empty))
            .llm_extraction(Arc::new(MockLlmBackend::empty()))
            .llm_default(Arc::new(MockLlmBackend::empty()))
            .embedder(Arc::new(MockEmbedBackend::default_dimension()))
            .build()
            .unwrap();
        let snapshot: SnapshotNode = serde_json::from_value(json!({
            "uuid": Uuid::from_u128(0xa1), "org_id": "org", "namespace": "ns", "name": "report",
            "source": "test", "content": "", "data_type": "text",
            "snapshot_kind": "incremental", "complete": false,
            "captured_at": "2026-09-19T12:00:00Z", "created_at": "2026-09-19T12:00:00Z",
            "entities": [], "entity_edges": [], "labels": [], "tags": {}
        }))
        .unwrap();
        // The discovery name declares an identifying property; the label does not.
        let ontology: kg_core::traits::Ontology = serde_json::from_value(json!({
            "edge_types": [{"name": GENERIC_RELATIONSHIP_NAME, "identifying_properties": ["port"]}]
        }))
        .unwrap();
        let resolution = NodeResolutionOutput {
            schemas: Arc::new(HashMap::from([(
                snapshot.uuid,
                ObservationSchemas {
                    org_id: "org".into(),
                    source: "test".into(),
                    definitions: BTreeMap::from([("test".to_string(), ontology)]),
                },
            )])),
            ..Default::default()
        };
        let resolution = Arc::new(resolution);
        let source = crate::node::entity_versioning::tests::test_entity("api");
        let mut generic = crate::edge::reference_extraction::build_fk_edge(
            &source,
            Uuid::new_v4(),
            "Service",
            "target",
            "dependency",
            "name",
            false,
        )
        .unwrap();
        generic.name = GENERIC_RELATIONSHIP_NAME.into();
        generic.valid_from = snapshot.captured_at;
        generic.last_seen_snapshot_id = Some(snapshot.uuid);
        generic.first_seen_snapshot_id = Some(snapshot.uuid);
        generic
            .all_properties
            .insert("port".into(), kg_core::models::PropertyValue::Integer(5432));
        let mut named = generic.clone();
        named.identity_name = Some(std::mem::replace(&mut named.name, "STORES_IN".into()));

        let mut hashes = Vec::new();
        for edge in [generic, named] {
            let output = EdgeResolutionStage
                .process(
                    StageOutput::EdgeExtraction(EdgeExtractionOutput {
                        relationship_times: Default::default(),
                        relationship_directives: Default::default(),
                        reference_report: Default::default(),
                        snapshot_nodes: Arc::new(vec![snapshot.clone()]),
                        resolution: resolution.clone(),
                        resolved_nodes: Default::default(),
                        edges: Arc::new(vec![edge]),
                        pending_references: Default::default(),
                    }),
                    &ctx,
                )
                .await
                .unwrap();
            let StageOutput::EdgeResolution(output) = output else {
                panic!("expected resolved relationships")
            };
            hashes.push((
                output.observed[0].name.clone(),
                output.observed[0].identity_hash.clone().unwrap(),
            ));
        }
        assert_eq!(hashes[0].0, GENERIC_RELATIONSHIP_NAME);
        assert_eq!(hashes[1].0, "STORES_IN");
        assert_eq!(hashes[0].1, hashes[1].1);
    }
}
