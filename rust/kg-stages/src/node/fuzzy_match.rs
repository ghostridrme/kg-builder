//! Semantic identity decisions for candidates unresolved by authoritative keys.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use kg_core::runtime::matching_cache::CacheWrite;
use serde_json::Value;
use uuid::Uuid;

use super::matching_candidates::{conflicting_key, FuzzyCandidate};
use kg_core::embedding::{self, ComputedEmbedding};
use kg_core::errors::{BackendError, StageError};
use kg_core::models::EntityNode;
use kg_core::policy::EntityMatching;
use kg_core::runtime::stage_output::{
    ChainsMerged, IdentityMatch, IdentityOutcome, NodeIdentityOutput,
};
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::Stage;

/// Match whole authoritative components without deciding version history.
pub struct FuzzyMatchStage;

pub(super) const STAGE: &str = "fuzzy_match";

type ComponentDecision = (Vec<(usize, EntityNode)>, Option<Adoption>);

/// An existing chain the incoming entity continues, with the audit trail.
struct Adoption {
    candidate: FuzzyCandidate,
    resolved_by: String,
    reason: String,
}

#[async_trait]
impl Stage for FuzzyMatchStage {
    fn processing_version(&self) -> String {
        kg_core::traits::stage::processing_version(
            "entity-matching-complete-evidence-v24",
            &[
                super::matching_decision::SYSTEM_PROMPT,
                super::matching_batch::BATCH_PROMPT,
                super::matching_decision::SHARED_EVIDENCE_LAYOUT,
                super::matching_decision::CYCLE_CLARIFICATION,
            ],
        )
    }

    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::NodeIdentity, StageKind::NodeIdentity)]
    }

    fn name(&self) -> &str {
        STAGE
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
            .ok_or_else(|| invalid("missing identity output"))?
    }

    async fn process_batch(
        &self,
        inputs: Vec<StageOutput>,
        ctx: &RuntimeContext,
    ) -> Result<Vec<Result<StageOutput, kg_core::errors::StageError>>, StageError> {
        let mut outputs: Vec<NodeIdentityOutput> = inputs
            .into_iter()
            .map(|input| match input {
                StageOutput::NodeIdentity(identity) => Ok(identity),
                _ => Err(invalid("expected identity decisions")),
            })
            .collect::<Result<_, _>>()?;
        let mut groups = std::collections::BTreeMap::<Uuid, Vec<(usize, EntityNode)>>::new();
        for (index, identity) in outputs.iter().enumerate() {
            for (_, nodes) in identity.extraction.entities_by_snapshot.iter() {
                for entity in nodes {
                    let matched = identity
                        .matches
                        .get(&entity.uuid)
                        .ok_or_else(|| invalid("missing identity decision"))?;
                    groups
                        .entry(matched.chain_id)
                        .or_default()
                        .push((index, entity.clone()));
                }
            }
        }
        ctx.matching_settings
            .validate()
            .map_err(|_| invalid("invalid matching settings"))?;
        for members in groups.values_mut() {
            members.sort_by_key(|(_, e)| (e.valid_from, e.uuid));
        }
        let mut declined = HashMap::<usize, String>::new();
        let mut semantic_groups = Vec::new();
        for (&component_id, members) in &groups {
            let mut members = members.clone();
            if outputs[members[0].0].matches[&members[0].1.uuid]
                .existing
                .is_some()
            {
                continue;
            }
            members.sort_by_key(|(_, e)| (e.valid_from, e.uuid));
            let semantic = members.iter().all(|(index, entity)| {
                let output = &outputs[*index];
                output
                    .observations
                    .get(&entity.uuid)
                    .and_then(|observation| {
                        output
                            .extraction
                            .snapshot_nodes
                            .iter()
                            .find(|snapshot| snapshot.uuid == observation.snapshot_uuid)
                    })
                    .is_some_and(|snapshot| {
                        ctx.policy.for_source(&snapshot.source).matching == EntityMatching::Semantic
                    })
            });
            if !semantic {
                if members.iter().any(|(_, e)| !e.has_authoritative_keys()) {
                    for (index, _) in &members {
                        declined.insert(
                            *index,
                            "keyless observation requires semantic identity resolution".into(),
                        );
                    }
                }
                continue;
            }
            if let Err(error) = super::matching_candidates::validate_component(
                &members.iter().map(|(_, e)| e).collect::<Vec<_>>(),
                ctx,
            ) {
                for (index, _) in &members {
                    declined.insert(*index, error.to_string());
                }
                continue;
            }
            semantic_groups.push((component_id, members));
        }
        // Large mixed chunks need the same ranked retrieval used for stored candidates.
        // These vectors share the incoming cache with persistence; no extra model decisions.
        let ranking_locals =
            !semantic_groups.is_empty() && groups.len() > ctx.matching_settings.candidate_limit;
        let retrieval_members: Vec<_> = if ranking_locals {
            groups.values().collect()
        } else {
            semantic_groups.iter().map(|(_, members)| members).collect()
        };
        let mut retrieval_entities: Vec<_> = retrieval_members
            .into_iter()
            .flat_map(|members| members.iter())
            .map(|(_, entity)| entity.clone())
            .collect();
        bounded(
            ctx,
            "embed_incoming",
            embed_incoming(&mut retrieval_entities, ctx),
        )
        .await?;
        let embedded: HashMap<_, _> = retrieval_entities
            .into_iter()
            .map(|e| (e.uuid, e))
            .collect();
        let local_frontiers =
            local_candidates(&groups, &semantic_groups, &outputs, &embedded, ctx)?;
        let (decisions, local_targets, cache_writes, declined, decision_failures) =
            resolve_components(
                &groups,
                semantic_groups,
                local_frontiers,
                &embedded,
                &outputs,
                ctx,
                declined,
            )
            .await?;
        if !ctx.exec_config.continue_on_step_error {
            if let Some((_, reason)) = declined.iter().min_by_key(|(index, _)| *index) {
                return Err(invalid(reason));
            }
        }
        let survivors: Vec<_> = outputs
            .iter()
            .enumerate()
            .filter(|(index, _)| !declined.contains_key(index))
            .map(|(_, output)| output.clone())
            .collect();
        validate_shared_targets(&decisions, &survivors)?;
        for (members, adoption) in decisions {
            for (index, original) in members {
                let identity = &mut outputs[index];
                let groups = Arc::make_mut(&mut identity.extraction.entities_by_snapshot);
                let entity = groups
                    .iter_mut()
                    .flat_map(|(_, nodes)| nodes.iter_mut())
                    .find(|e| e.uuid == original.uuid)
                    .ok_or_else(|| invalid("missing observation"))?;
                entity.embedding = embedded[&entity.uuid].embedding.clone();
                let Some(adoption) = &adoption else {
                    if let Some((anchor, method)) = local_targets.get(&original.uuid) {
                        if !entity.has_authoritative_keys() {
                            if entity.entity_type != anchor.entity_type {
                                identity
                                    .observations
                                    .get_mut(&original.uuid)
                                    .ok_or_else(|| invalid("missing original observation"))?
                                    .resolved_entity_type = Some(anchor.entity_type.clone());
                            }
                            entity.entity_type = anchor.entity_type.clone();
                            // A keyless mention may use an alias. The anchor owns the
                            // canonical display name after the identities converge.
                            entity.name = anchor.name.clone();
                            entity.primary_key_properties = anchor.primary_key_properties.clone();
                            entity.additional_key_properties =
                                anchor.additional_key_properties.clone();
                            entity.identity_hash = anchor.identity_hash;
                            for key in entity
                                .primary_key_properties
                                .iter()
                                .chain(entity.additional_key_properties.iter().flatten())
                            {
                                if key == "name" {
                                    entity.name = anchor.name.clone();
                                } else if let Some(value) = anchor.all_properties.get(key) {
                                    entity.all_properties.insert(key.clone(), value.clone());
                                }
                            }
                            entity.structural_hash = super::entity_versioning::structural_hash_of(
                                ctx,
                                &entity.entity_type,
                                &entity.all_properties,
                                identity
                                    .extraction
                                    .version_exclusions
                                    .get(&original.uuid)
                                    .map(Vec::as_slice)
                                    .unwrap_or(&[]),
                            );
                        } else if original.chain_id != anchor.chain_id {
                            let mut hashes = vec![
                                original.identity_hash.to_string(),
                                anchor.identity_hash.to_string(),
                            ];
                            hashes.sort();
                            hashes.dedup();
                            identity.chains_merged.push(ChainsMerged {
                                effective_at: original.valid_from,
                                winner_chain_id: anchor.chain_id,
                                loser_chain_id: original.chain_id,
                                merged_identity_hashes: hashes,
                                merged_by: method.clone(),
                                reason: Some("contextual match to incoming entity".into()),
                            });
                        }
                        identity.matches.insert(
                            entity.uuid,
                            IdentityMatch {
                                outcome: IdentityOutcome::New,
                                chain_id: anchor.chain_id,
                                existing: None,
                            },
                        );
                        identity.methods.insert(entity.uuid, method.clone());
                    }
                    continue;
                };
                let record = &adoption.candidate.record;
                if !entity.has_authoritative_keys() {
                    if entity.entity_type != record.entity_type {
                        identity
                            .observations
                            .get_mut(&original.uuid)
                            .ok_or_else(|| invalid("missing original observation"))?
                            .resolved_entity_type = Some(record.entity_type.clone());
                    }
                    entity.entity_type = record.entity_type.clone();
                    // A keyless observation can name the same resource differently;
                    // matching it does not declare a resource rename.
                    entity.name = record.name.clone();
                    entity.primary_key_properties = adoption.candidate.primary_keys.clone();
                    entity.additional_key_properties = adoption.candidate.additional_keys.clone();
                    entity.identity_hash = record
                        .identity_hash
                        .as_ref()
                        .and_then(|hash| serde_json::from_value(Value::String(hash.clone())).ok())
                        .ok_or_else(|| invalid("matched entity has no identity token"))?;
                    let stored = record
                        .typed_source_properties()
                        .map_err(|_| invalid("invalid stored properties"))?;
                    for key in entity
                        .primary_key_properties
                        .iter()
                        .chain(entity.additional_key_properties.iter().flatten())
                    {
                        if key == "name" {
                            entity.name = record.name.clone();
                        } else if let Some(value) = stored.get(key) {
                            entity.all_properties.insert(key.clone(), value.clone());
                        }
                    }
                    entity.structural_hash = super::entity_versioning::structural_hash_of(
                        ctx,
                        &entity.entity_type,
                        &entity.all_properties,
                        identity
                            .extraction
                            .version_exclusions
                            .get(&original.uuid)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                    );
                } else {
                    let mut winner = entity.clone();
                    winner.chain_id = record.chain_id;
                    identity.chains_merged.push(chains_merged_event(
                        &winner,
                        &original,
                        &adoption.candidate,
                        &adoption.resolved_by,
                        adoption.reason.clone(),
                    ));
                }
                identity.matches.insert(
                    entity.uuid,
                    IdentityMatch {
                        outcome: IdentityOutcome::Matched,
                        chain_id: record.chain_id,
                        existing: Some(record.clone()),
                    },
                );
                identity
                    .methods
                    .insert(entity.uuid, adoption.resolved_by.clone());
            }
        }
        for write in cache_writes {
            ctx.matching_cache.record(write);
        }
        Ok(outputs
            .into_iter()
            .enumerate()
            .map(|(index, output)| match declined.get(&index) {
                Some(reason) => Err(match decision_failures.get(&index) {
                    Some(error) => error.clone(),
                    None => invalid(reason),
                }),
                None => Ok(StageOutput::NodeIdentity(output)),
            })
            .collect())
    }
}

/// Dropping a failed wave cancels its remaining futures before callers can apply decisions.
async fn concurrent_ordered<T, F>(jobs: Vec<F>, limit: usize) -> Result<Vec<T>, StageError>
where
    F: std::future::Future<Output = Result<T, StageError>>,
{
    async fn indexed<T>(
        index: usize,
        job: impl std::future::Future<Output = Result<T, StageError>>,
    ) -> Result<(usize, T), StageError> {
        job.await.map(|value| (index, value))
    }
    let mut remaining = jobs.into_iter().enumerate();
    let mut pending = futures::stream::FuturesUnordered::new();
    for (index, job) in remaining.by_ref().take(limit) {
        pending.push(indexed(index, job));
    }
    let mut results = Vec::new();
    while let Some(result) = pending.next().await {
        results.push(result?);
        if let Some((index, job)) = remaining.next() {
            pending.push(indexed(index, job));
        }
    }
    results.sort_unstable_by_key(|(index, _)| *index);
    Ok(results.into_iter().map(|(_, value)| value).collect())
}

async fn bounded<T>(
    ctx: &RuntimeContext,
    step: &str,
    work: impl std::future::Future<Output = Result<T, StageError>>,
) -> Result<T, StageError> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(StageError::Cancelled {stage:STAGE.into()}),
        result = tokio::time::timeout(std::time::Duration::from_millis(ctx.matching_settings.timeout_ms), work) =>
            result.unwrap_or_else(|_| Err(StageError::StepFailed {stage:STAGE.into(),step:step.into(),cause:"matching operation timed out".into(),retriable:true})),
    }
}

pub(super) fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: STAGE.into(),
        message: message.into(),
    }
}

pub(super) fn step_failed(step: &str, error: BackendError) -> StageError {
    StageError::StepFailed {
        stage: STAGE.into(),
        step: step.into(),
        cause: error.to_string(),
        retriable: error.is_transient(),
    }
}

/// Deduplicate texts and respect provider batch limits. Retain vectors for persistence.
async fn embed_incoming(
    entities: &mut [EntityNode],
    ctx: &RuntimeContext,
) -> Result<(), StageError> {
    if entities.is_empty() {
        return Ok(());
    }
    use kg_core::runtime::embedding_cache::EmbeddingCacheKey;

    let batch_size = ctx.embedder.max_batch_size();
    if batch_size == 0 {
        return Err(invalid("embedding provider has zero batch capacity"));
    }
    let texts: Vec<String> = entities
        .iter()
        .map(|entity| embedding::entity_text(entity, &ctx.embedding))
        .collect();
    let mut unique = Vec::new();
    let mut index = HashMap::new();
    for (entity, text) in entities.iter().zip(&texts) {
        let key = EmbeddingCacheKey::new(&ctx.org_id, &entity.namespace, &ctx.embedding, text);
        index.entry(key).or_insert_with(|| {
            unique.push((key, text.as_str()));
            unique.len() - 1
        });
    }
    let mut vectors: Vec<_> = unique
        .iter()
        .map(|(key, _)| ctx.incoming_embeddings.get(key))
        .collect();
    let missing: Vec<_> = vectors
        .iter()
        .enumerate()
        .filter_map(|(i, vector)| vector.is_none().then_some(i))
        .collect();
    tracing::debug!(
        input_count = entities.len(),
        unique_count = unique.len(),
        cache_hits = unique.len() - missing.len(),
        provider_inputs = missing.len(),
        "incoming embedding reuse"
    );
    for batch in missing.chunks(batch_size) {
        let batch_texts: Vec<_> = batch.iter().map(|i| unique[*i].1).collect();
        let completed = embedding::embed(ctx, &batch_texts)
            .await
            .map_err(|e| step_failed("embed_incoming", e))?;
        // embed validates the entire response before any value becomes reusable.
        for (i, vector) in batch.iter().zip(completed) {
            ctx.incoming_embeddings
                .record(unique[*i].0, &ctx.embedding, vector.clone());
            vectors[*i] = Some(vector);
        }
    }
    for (entity, text) in entities.iter_mut().zip(&texts) {
        let key = EmbeddingCacheKey::new(&ctx.org_id, &entity.namespace, &ctx.embedding, text);
        let vector = vectors[index[&key]]
            .as_ref()
            .ok_or_else(|| invalid("incoming embedding is missing"))?;
        entity.embedding = Some(Arc::new(ComputedEmbedding::new(
            &ctx.embedding,
            embedding::content_hash(text),
            vector.clone(),
        )));
    }
    Ok(())
}

type Members = Vec<(usize, EntityNode)>;
type LocalTargets = HashMap<Uuid, (EntityNode, String)>;

fn local_covered_by_stored(
    local: &super::matching_decision::LocalCandidate,
    stored: &FuzzyCandidate,
) -> bool {
    local.stored.is_none()
        && !local.members.is_empty()
        && local.members.iter().all(|(_, entity)| {
            entity.entity_type == stored.entity_type
                && entity.name == stored.name
                && entity
                    .all_properties
                    .iter()
                    .any(|(key, value)| key != "name" && stored.properties.get(key) == Some(value))
                && entity
                    .all_properties
                    .iter()
                    .all(|(key, value)| stored.properties.get(key) == Some(value))
        })
}

fn local_candidates(
    groups: &std::collections::BTreeMap<Uuid, Members>,
    subjects: &[(Uuid, Members)],
    outputs: &[NodeIdentityOutput],
    embedded: &HashMap<Uuid, EntityNode>,
    ctx: &RuntimeContext,
) -> Result<HashMap<Uuid, Result<Vec<super::matching_decision::LocalCandidate>, String>>, StageError>
{
    use super::{matching_decision::LocalCandidate, matching_graph};
    let mut scopes = HashMap::<&str, Vec<Uuid>>::new();
    for (&id, members) in groups {
        let first = &members[0].1;
        scopes.entry(&first.namespace).or_default().push(id);
    }
    let mut frontiers = HashMap::new();
    for (id, members) in subjects {
        let first = &members[0].1;
        let mut locals = Vec::new();
        let inferred = members.iter().all(|(index, e)| {
            super::matching_candidates::inferred_type(&outputs[*index].extraction, e)
        });
        for target in &scopes[first.namespace.as_str()] {
            if target == id {
                continue;
            }
            let target_members = &groups[target];
            if !inferred && target_members[0].1.entity_type != first.entity_type {
                continue;
            }
            if !members.iter().all(|(_, a)| {
                target_members
                    .iter()
                    .all(|(_, b)| matching_graph::compatible(a, b))
            }) {
                continue;
            }
            let (index, entity) = &target_members[0];
            let stored = outputs[*index].matches[&entity.uuid]
                .existing
                .as_ref()
                .map(super::matching_candidates::candidate_from_record)
                .transpose()?
                .flatten();
            if stored.as_ref().is_some_and(|c| {
                !c.record.is_latest
                    || c.record.deleted_at.is_some()
                    || c.record.valid_to.is_some()
                    || c.record.merged_into.is_some()
                    || members.iter().any(|(_, e)| conflicting_key(e, c).is_some())
            }) {
                continue;
            }
            locals.push(LocalCandidate {
                component_id: *target,
                members: target_members.clone(),
                stored,
            });
        }
        frontiers.insert(
            *id,
            super::matching_local::select(members, locals, embedded, &ctx.matching_settings),
        );
    }
    Ok(frontiers)
}

async fn resolve_components(
    groups: &std::collections::BTreeMap<Uuid, Members>,
    subjects: Vec<(Uuid, Members)>,
    mut local_frontiers: HashMap<
        Uuid,
        Result<Vec<super::matching_decision::LocalCandidate>, String>,
    >,
    embedded: &HashMap<Uuid, EntityNode>,
    outputs: &[NodeIdentityOutput],
    ctx: &RuntimeContext,
    mut declined: HashMap<usize, String>,
) -> Result<
    (
        Vec<ComponentDecision>,
        LocalTargets,
        Vec<CacheWrite>,
        HashMap<usize, String>,
        HashMap<usize, StageError>,
    ),
    StageError,
> {
    use super::matching_decision::Decision;
    use super::matching_graph::{self, Anchor, Component, Decision as Link};
    let mut decision_failures = HashMap::new();
    let mut graph = std::collections::BTreeMap::new();
    let mut records = HashMap::<Uuid, FuzzyCandidate>::new();
    for (&id, members) in groups {
        let first = &members[0].1;
        let existing = outputs[members[0].0].matches[&first.uuid].existing.as_ref();
        let decision = if let Some(record) = existing {
            let candidate = super::matching_candidates::candidate_from_record(record)?
                .ok_or_else(|| invalid("invalid stored identity anchor"))?;
            records.insert(record.chain_id, candidate);
            Link::Stored(record.chain_id)
        } else {
            Link::New
        };
        graph.insert(
            id,
            Component {
                id,
                members: members.iter().map(|(_, e)| e.clone()).collect(),
                inferred_type: members.iter().all(|(index, e)| {
                    super::matching_candidates::inferred_type(&outputs[*index].extraction, e)
                }),
                decision,
            },
        );
    }
    // Freeze all offered frontiers before asking the model or applying any identity.
    let jobs = subjects.into_iter().map(|(id, members)| {
        let locals = local_frontiers.remove(&id);
        async move {
            let indices: Vec<_> = members.iter().map(|(index, _)| *index).collect();
            let result = async {
                let first = &members[0].1;
                let inferred = members.iter().all(|(index, e)| {
                    super::matching_candidates::inferred_type(&outputs[*index].extraction, e)
                });
                let revision_type = if inferred {
                    "*"
                } else {
                    first.entity_type.as_str()
                };
                let revision = outputs[members[0].0]
                    .identity_revisions
                    .iter()
                    .find(|r| {
                        r.scope.namespace == first.namespace && r.scope.entity_type == revision_type
                    })
                    .ok_or_else(|| invalid("missing identity revision for semantic decision"))?
                    .clone();
                let locals = locals
                    .ok_or_else(|| invalid("missing incoming candidate frontier"))?
                    .map_err(|reason| invalid(&reason))?;
                let entities: Vec<_> = members.iter().map(|(_, e)| &embedded[&e.uuid]).collect();
                let mut candidates = bounded(
                    ctx,
                    "identity_candidates",
                    super::matching_candidates::retrieve(&entities, &revision.scope, ctx),
                )
                .await?;
                candidates.retain(|c| members.iter().all(|(_, e)| conflicting_key(e, c).is_none()));
                Ok((id, members, revision, candidates, locals))
            }
            .await;
            match result {
                Err(error @ StageError::StateValidation { .. }) => Ok((indices, Err(error))),
                Err(error) => Err(error),
                Ok(frontier) => Ok((indices, Ok(frontier))),
            }
        }
    });
    let retrieved = concurrent_ordered(
        jobs.collect(),
        ctx.matching_settings.max_concurrent_components,
    )
    .await?;
    let mut frontiers = Vec::new();
    for (indices, result) in retrieved {
        match result {
            Ok(frontier) => frontiers.push(frontier),
            Err(error) => {
                for index in indices {
                    declined.insert(index, error.to_string());
                }
            }
        }
    }
    for (_, members, _, candidates, locals) in &mut frontiers {
        // An incoming duplicate adds no identity evidence when a stored record
        // already contains its name, type, and observed properties.
        locals.retain(|local| {
            !candidates
                .iter()
                .any(|stored| local_covered_by_stored(local, stored))
        });
        for candidate in candidates.iter() {
            if let Some(previous) = records.insert(candidate.record.chain_id, candidate.clone()) {
                if previous.record.uuid != candidate.record.uuid
                    || previous.record.stored != candidate.record.stored
                {
                    return Err(StageError::IdentityRevisionChanged);
                }
            }
        }
        candidates.retain(|c| {
            !locals.iter().any(|local| {
                local
                    .stored
                    .as_ref()
                    .is_some_and(|stored| stored.record.chain_id == c.record.chain_id)
            })
        });
        if candidates.len() + locals.len() > ctx.matching_settings.max_candidate_limit {
            for (index, _) in members.iter() {
                declined.insert(
                    *index,
                    "combined candidate frontier exceeds budget; evidence is incomplete".into(),
                );
            }
        }
    }
    frontiers.retain(|(_, members, _, _, _)| {
        !members
            .iter()
            .any(|(index, _)| declined.contains_key(index))
    });
    let mut requests = Vec::new();
    let mut model_choices = HashMap::new();
    for (id, members, revision, candidates, locals) in &frontiers {
        // An empty frontier is a new chain without a model call: there is nothing the
        // model could match or abstain against, and asking it for a novelty
        // quote only adds cost and a failure mode. Components with stored or
        // incoming candidates keep the grounded decision.
        if candidates.is_empty() && locals.is_empty() {
            let decision = if members.iter().any(|(_, e)| e.has_authoritative_keys()) {
                Decision::New
            } else {
                // The empty frontier is the anchor: no quote is needed to
                // establish novelty against nothing.
                Decision::GroundedNew {
                    resolved_by: "new:no_candidates".into(),
                }
            };
            model_choices.insert(*id, decision);
        } else {
            match super::matching_batch::Request::new(
                *id, members, candidates, locals, revision, outputs, ctx,
            ) {
                Ok(request) => requests.push(request),
                // Complete evidence that does not fit the prompt budget is
                // explicit uncertainty for this component, never a truncated
                // prompt, a new entity or a failed chunk.
                Err(error) if super::matching_decision::exceeds_budget(&error) => {
                    tracing::warn!(
                        observations = members.len(),
                        candidates = candidates.len() + locals.len(),
                        "complete identity evidence exceeds the prompt budget; abstaining"
                    );
                    model_choices.insert(*id, Decision::InsufficientEvidence);
                }
                Err(error)
                    if ctx.exec_config.continue_on_step_error
                        && super::matching_decision::is_partial_failure(&error) =>
                {
                    for (index, _) in members {
                        declined.entry(*index).or_insert_with(|| error.to_string());
                        decision_failures
                            .entry(*index)
                            .or_insert_with(|| error.clone());
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
    requests.retain(|request| {
        !request
            .members
            .iter()
            .any(|(index, _)| declined.contains_key(index))
    });
    // Typed decisions first, when enabled: a confident answer settles the
    // component; everything else continues into the language-model path.
    if ctx.typed_decisions.enabled && ctx.decisions.is_some() {
        let answered = futures::future::join_all(requests.into_iter().map(|request| async {
            let decision = super::matching_decision::typed_decision(&request, ctx).await;
            (request, decision)
        }))
        .await;
        requests = Vec::new();
        for (request, decision) in answered {
            match decision {
                Some(decision) => {
                    model_choices.insert(request.id, decision);
                }
                None => requests.push(request),
            }
        }
    }
    let batches = super::matching_batch::pack(requests, ctx)?;
    let jobs = batches.into_iter().map(|batch| async move {
        let ids: Vec<_> = batch.iter().map(|request| request.id).collect();
        let result = bounded(
            ctx,
            "identity_decision",
            super::matching_batch::decide(batch, outputs, ctx),
        )
        .await;
        match result {
            Err(error)
                if ctx.exec_config.continue_on_step_error
                    && super::matching_decision::is_partial_failure(&error) =>
            {
                Ok(ids
                    .into_iter()
                    .map(|id| (id, Decision::Failed(error.clone())))
                    .collect())
            }
            other => other,
        }
    });
    for decisions in concurrent_ordered(
        jobs.collect(),
        ctx.matching_settings.max_concurrent_components,
    )
    .await?
    {
        for (id, decision) in decisions {
            if model_choices.insert(id, decision).is_some() {
                return Err(invalid("duplicate identity decision"));
            }
        }
    }
    let mut choices = Vec::new();
    for (id, members, _, candidates, locals) in &frontiers {
        if members
            .iter()
            .any(|(index, _)| declined.contains_key(index))
        {
            continue;
        }
        let mut method = None;
        let mut cache_write = None;
        let decision = match model_choices
            .remove(id)
            .ok_or_else(|| invalid("missing identity decision"))?
        {
            Decision::Match {
                candidate_id,
                resolved_by,
                cache_write: pending_write,
            } => {
                method = Some(resolved_by);
                cache_write = pending_write;
                if let Some(candidate) = candidates.get(candidate_id) {
                    Link::Stored(candidate.record.chain_id)
                } else {
                    let local = locals
                        .get(candidate_id - candidates.len())
                        .ok_or_else(|| invalid("unknown local candidate"))?;
                    Link::MatchLocal(local.component_id)
                }
            }
            Decision::New => Link::New,
            Decision::GroundedNew { resolved_by } => {
                method = Some(resolved_by);
                Link::GroundedNew
            }
            Decision::InsufficientEvidence => Link::Insufficient,
            Decision::Failed(error) => {
                for (index, _) in &groups[id] {
                    decision_failures
                        .entry(*index)
                        .or_insert_with(|| error.clone());
                }
                Link::Insufficient
            }
        };
        choices.push((*id, decision, method, cache_write));
    }
    let mut methods = HashMap::new();
    let mut cache_writes = Vec::new();
    for (id, decision, method, cache_write) in choices {
        if let Some(method) = method {
            methods.insert(id, method);
        }
        if let Some(write) = cache_write {
            cache_writes.push((id, write));
        }
        graph
            .get_mut(&id)
            .ok_or_else(|| invalid("unknown identity component"))?
            .decision = decision;
    }
    // One bounded clarification per cycle. No cycle itself authorizes a new identity.
    let cycles = matching_graph::cycles(&graph);
    if !cycles.is_empty() {
        tracing::warn!(
            cycles = cycles.len(),
            components = cycles.iter().map(Vec::len).sum::<usize>(),
            "clarifying circular identity decisions"
        );
    }
    let unanchored = matching_graph::unanchored_components(&graph);
    let repairs = cycles
        .iter()
        .map(|cycle| {
            let id = *cycle
                .iter()
                .min_by_key(|id| {
                    (
                        !graph[id]
                            .members
                            .iter()
                            .any(EntityNode::has_authoritative_keys),
                        **id,
                    )
                })
                .expect("cycle is nonempty");
            let frontier = frontiers.iter().find(|(candidate, ..)| *candidate == id);
            let blocked = &unanchored;
            async move {
                let decision = if let Some((_, members, revision, candidates, locals)) = frontier {
                    super::matching_decision::partial_decision(
                        bounded(
                            ctx,
                            "identity_cycle_clarification",
                            super::matching_decision::clarify_cycle(
                                members, outputs, candidates, locals, revision, blocked, ctx,
                            ),
                        )
                        .await,
                        ctx.exec_config.continue_on_step_error,
                    )?
                } else {
                    Decision::InsufficientEvidence
                };
                Ok((id, decision))
            }
        })
        .collect();
    for (id, decision) in
        concurrent_ordered(repairs, ctx.matching_settings.max_concurrent_components).await?
    {
        let link = match decision {
            Decision::GroundedNew { resolved_by } => {
                methods.insert(id, resolved_by);
                Link::GroundedNew
            }
            Decision::New => Link::New,
            Decision::InsufficientEvidence => Link::Insufficient,
            Decision::Failed(error) => {
                for (index, _) in &groups[&id] {
                    decision_failures
                        .entry(*index)
                        .or_insert_with(|| error.clone());
                }
                Link::Insufficient
            }
            Decision::Match {
                candidate_id,
                resolved_by,
                cache_write,
            } => {
                let (_, _, _, candidates, locals) = frontiers
                    .iter()
                    .find(|(candidate, ..)| *candidate == id)
                    .ok_or_else(|| invalid("missing cycle clarification frontier"))?;
                methods.insert(id, resolved_by);
                if let Some(write) = cache_write {
                    cache_writes.push((id, write));
                }
                if let Some(candidate) = candidates.get(candidate_id) {
                    Link::Stored(candidate.record.chain_id)
                } else {
                    let local = locals
                        .get(candidate_id - candidates.len())
                        .ok_or_else(|| invalid("unknown clarification candidate"))?;
                    Link::MatchLocal(local.component_id)
                }
            }
        };
        graph
            .get_mut(&id)
            .ok_or_else(|| invalid("missing cycle component"))?
            .decision = link;
    }
    // A repair can point into a different unresolved cycle. Never loop or auto-merge.
    for cycle in matching_graph::cycles(&graph) {
        for id in cycle {
            graph.get_mut(&id).expect("cycle member exists").decision = Link::Insufficient;
        }
    }
    // A snapshot is atomic. Decline its siblings and every incoming local link,
    // repeating until the remaining identity graph is closed.
    loop {
        let before = declined.len();
        let rejected: std::collections::HashSet<_> = graph
            .iter()
            .filter_map(|(id, component)| {
                (component.decision == Link::Insufficient
                    || groups[id]
                        .iter()
                        .any(|(index, _)| declined.contains_key(index)))
                .then_some(*id)
            })
            .collect();
        for (id, component) in &graph {
            if rejected.contains(id)
                || matches!(component.decision, Link::MatchLocal(target) if rejected.contains(&target))
            {
                for (index, _) in &groups[id] {
                    declined.entry(*index).or_insert_with(|| {
                        "insufficient evidence for entity identity or dependent identity".into()
                    });
                }
            }
        }
        if before == declined.len() {
            break;
        }
    }
    graph.retain(|id, _| {
        !groups[id]
            .iter()
            .any(|(index, _)| declined.contains_key(index))
    });
    frontiers.retain(|(id, _, _, _, _)| graph.contains_key(id));
    let cache_writes = cache_writes
        .into_iter()
        .filter_map(|(id, write)| graph.contains_key(&id).then_some(write))
        .collect();
    let anchors = matching_graph::validate(&graph.into_values().collect::<Vec<_>>())?;
    tracing::debug!(
        components = anchors.len(),
        semantic_components = frontiers.len(),
        max_concurrent_components = ctx.matching_settings.max_concurrent_components,
        stored_targets = anchors
            .values()
            .filter(|anchor| matches!(anchor, Anchor::Stored(_)))
            .count(),
        "identity component mappings validated"
    );
    let mut decisions = Vec::new();
    let mut local_targets = HashMap::new();
    for (id, members, _, _, _) in frontiers {
        let method = methods.remove(&id).unwrap_or_else(|| "new".into());
        let adoption = match anchors[&id] {
            Anchor::Stored(chain) => {
                let candidate = records
                    .get(&chain)
                    .ok_or_else(|| invalid("missing terminal stored candidate"))?
                    .clone();
                if members.iter().any(|(index, e)| {
                    conflicting_key(e, &candidate).is_some()
                        || (e.entity_type != candidate.entity_type
                            && !super::matching_candidates::inferred_type(
                                &outputs[*index].extraction,
                                e,
                            ))
                }) {
                    return Err(invalid(
                        "transitive match conflicts with stored identifying keys",
                    ));
                }
                Some(Adoption {
                    candidate,
                    resolved_by: method,
                    reason: "contextual identity decision".into(),
                })
            }
            Anchor::New(target) => {
                if target != id || members.iter().any(|(_, e)| !e.has_authoritative_keys()) {
                    let mut anchor = groups[&target]
                        .iter()
                        .map(|(_, e)| e)
                        .min_by_key(|entity| (!entity.has_authoritative_keys(), entity.uuid))
                        .ok_or_else(|| invalid("new component has no observation"))?
                        .clone();
                    anchor.chain_id = target;
                    for (_, entity) in &members {
                        local_targets.insert(entity.uuid, (anchor.clone(), method.clone()));
                    }
                }
                None
            }
        };
        decisions.push((members, adoption));
    }
    Ok((
        decisions,
        local_targets,
        cache_writes,
        declined,
        decision_failures,
    ))
}

/// Separate components cannot bypass conflicting keys by selecting the same chain.
fn validate_shared_targets(
    decisions: &[ComponentDecision],
    outputs: &[NodeIdentityOutput],
) -> Result<(), StageError> {
    let mut targets = HashMap::<Uuid, Vec<&EntityNode>>::new();
    let mut records = HashMap::new();
    for output in outputs {
        for (_, entities) in output.extraction.entities_by_snapshot.iter() {
            for entity in entities {
                if let Some(record) = output
                    .matches
                    .get(&entity.uuid)
                    .and_then(|m| m.existing.as_ref())
                {
                    if let Some(previous) = records.insert(record.chain_id, record) {
                        if previous.uuid != record.uuid || previous.stored != record.stored {
                            return Err(StageError::IdentityRevisionChanged);
                        }
                    }
                    targets.entry(record.chain_id).or_default().push(entity);
                }
            }
        }
    }
    for (members, adoption) in decisions {
        let Some(adoption) = adoption else { continue };
        let record = &adoption.candidate.record;
        if let Some(previous) = records.insert(record.chain_id, record) {
            if previous.uuid != record.uuid || previous.stored != record.stored {
                return Err(StageError::IdentityRevisionChanged);
            }
        }
        let previous = targets.entry(record.chain_id).or_default();
        for (_, entity) in members {
            for other in previous.iter() {
                let keys = entity
                    .primary_key_properties
                    .iter()
                    .chain(entity.additional_key_properties.iter().flatten())
                    .chain(other.primary_key_properties.iter())
                    .chain(other.additional_key_properties.iter().flatten());
                for key in keys {
                    let conflict = if key == "name" {
                        entity
                            .primary_key_properties
                            .iter()
                            .chain(entity.additional_key_properties.iter().flatten())
                            .any(|k| k == "name")
                            && other
                                .primary_key_properties
                                .iter()
                                .chain(other.additional_key_properties.iter().flatten())
                                .any(|k| k == "name")
                            && entity.name != other.name
                    } else {
                        matches!((entity.all_properties.get(key), other.all_properties.get(key)), (Some(a), Some(b)) if a != b)
                    };
                    if conflict {
                        return Err(invalid(
                            "components selected the same chain with conflicting identifying keys",
                        ));
                    }
                }
            }
        }
        previous.extend(members.iter().map(|(_, entity)| entity));
    }
    Ok(())
}

/// The `ChainsMerged` record for an adoption: the winner must answer to the
/// adopted entity's hash and to its own displaced primary. The loser chain
/// id is the chain the adopted entity would have minted; no node carries it.
fn chains_merged_event(
    matched: &EntityNode,
    original: &EntityNode,
    candidate: &FuzzyCandidate,
    merged_by: &str,
    justification: String,
) -> ChainsMerged {
    let mut hashes: Vec<String> = vec![original.identity_hash.to_string()];
    if let Some(primary) = &candidate.identity_hash {
        hashes.push(primary.clone());
    }
    hashes.sort();
    hashes.dedup();
    ChainsMerged {
        effective_at: original.valid_from,
        winner_chain_id: matched.chain_id,
        loser_chain_id: original.chain_id,
        merged_identity_hashes: hashes,
        merged_by: merged_by.to_string(),
        reason: Some(justification),
    }
}

#[cfg(test)]
mod tests {
    use super::super::matching_candidates::candidate_from_record;
    use super::super::matching_decision::summarize_candidate;
    use super::*;
    use crate::node::entity_versioning::tests::test_entity;
    use kg_core::models::PropertyValue;
    use kg_core::traits::EntityVersionRecord;

    struct RetryEmbedder {
        calls: std::sync::atomic::AtomicUsize,
        fail_second: bool,
        capacity: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl kg_core::traits::EmbedBackend for RetryEmbedder {
        async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail_second && call == 1 {
                return Ok(vec![vec![f32::NAN, 1.0]]);
            }
            Ok(texts
                .iter()
                .map(|text| vec![text.bytes().map(u32::from).sum::<u32>() as f32, 1.0])
                .collect())
        }
        fn dimension(&self) -> usize {
            2
        }
        fn max_batch_size(&self) -> usize {
            self.capacity.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn model_id(&self) -> &str {
            "retry-test"
        }
    }

    #[tokio::test]
    async fn incoming_retries_reuse_only_completed_valid_batches_and_preserve_embeddings() {
        use kg_core::{runtime::RuntimeContextBuilder, test_support::MockLlmBackend};
        use std::sync::atomic::Ordering;
        let embedder = Arc::new(RetryEmbedder {
            calls: Default::default(),
            fail_second: true,
            capacity: std::sync::atomic::AtomicUsize::new(1),
        });
        let llm = Arc::new(MockLlmBackend::empty());
        let ctx = RuntimeContextBuilder::new("org")
            .graph(Arc::new(kg_core::test_support::UnreachableGraph))
            .llm_extraction(llm.clone())
            .llm_default(llm)
            .embedder(embedder.clone())
            .build()
            .unwrap();
        let original = vec![test_entity("api"), test_entity("worker")];
        assert!(embed_incoming(&mut original.clone(), &ctx).await.is_err());
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 2);
        let mut retried = original.clone();
        embed_incoming(&mut retried, &ctx).await.unwrap();
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 3);
        for entity in &retried {
            assert!(entity.embedding.as_ref().unwrap().matches(
                &ctx.embedding,
                &embedding::content_hash(&embedding::entity_text(entity, &ctx.embedding))
            ));
        }
        embed_incoming(&mut original.clone(), &ctx).await.unwrap();
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 3);
        let mut changed = original.clone();
        changed[1].summary = Some("changed evidence".into());
        embed_incoming(&mut changed, &ctx).await.unwrap();
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 4);
        let mut interleaved = vec![
            original[1].clone(),
            test_entity("new"),
            original[0].clone(),
            original[1].clone(),
        ];
        embed_incoming(&mut interleaved, &ctx).await.unwrap();
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 5);
        for entity in interleaved {
            let text = embedding::entity_text(&entity, &ctx.embedding);
            assert_eq!(
                entity.embedding.unwrap().values,
                vec![text.bytes().map(u32::from).sum::<u32>() as f32, 1.0]
            );
        }
        let mut fresh_chunk = ctx.clone();
        fresh_chunk.incoming_embeddings = Arc::new(Default::default());
        embed_incoming(&mut original.clone(), &fresh_chunk)
            .await
            .unwrap();
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 7);
        embedder.capacity.store(0, Ordering::SeqCst);
        assert!(embed_incoming(&mut original.clone(), &ctx).await.is_err());
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 7);
        embedder.capacity.store(1, Ordering::SeqCst);
        ctx.cancel.cancel();
        assert!(matches!(
            bounded(
                &ctx,
                "embed_incoming",
                embed_incoming(&mut original.clone(), &ctx)
            )
            .await,
            Err(StageError::Cancelled { .. })
        ));
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 7);
    }

    #[tokio::test]
    async fn component_calls_overlap_with_a_bound_and_restore_input_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let gates: Vec<_> = (0..5).map(|_| tokio::sync::Semaphore::new(0)).collect();
        let jobs = (0..5).map(|id| {
            let (active, peak, gates, started_tx) = (&active, &peak, &gates, &started_tx);
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                started_tx.send(id).unwrap();
                gates[id].acquire().await.unwrap().forget();
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(id)
            }
        });
        let controller = async {
            let mut first = [
                started_rx.recv().await.unwrap(),
                started_rx.recv().await.unwrap(),
            ];
            first.sort_unstable();
            assert_eq!(first, [0, 1]);
            assert!(started_rx.try_recv().is_err());
            gates[1].add_permits(1);
            assert_eq!(started_rx.recv().await, Some(2));
            gates[2].add_permits(1);
            assert_eq!(started_rx.recv().await, Some(3));
            gates[3].add_permits(1);
            assert_eq!(started_rx.recv().await, Some(4));
            gates[4].add_permits(1);
            gates[0].add_permits(1);
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(concurrent_ordered(jobs.collect(), 2), controller)
        })
        .await
        .expect("component work stalled");
        assert_eq!(result.unwrap(), vec![0, 1, 2, 3, 4]);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn component_failure_drops_pending_calls_without_starting_more_work() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct InFlight<'a>(&'a AtomicUsize);
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let active = AtomicUsize::new(0);
        let started = AtomicUsize::new(0);
        let jobs = (0..5).map(|id| {
            let (active, started) = (&active, &started);
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                active.fetch_add(1, Ordering::SeqCst);
                let _call = InFlight(active);
                if id == 1 {
                    return Err::<(), _>(StageError::IdentityRevisionChanged);
                }
                std::future::pending().await
            }
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            concurrent_ordered(jobs.collect(), 2),
        )
        .await
        .expect("later failure waited for an earlier pending call");
        assert!(matches!(result, Err(StageError::IdentityRevisionChanged)));
        assert_eq!(started.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    fn candidate(keys: &[&str], values: &[(&str, &str)]) -> FuzzyCandidate {
        FuzzyCandidate {
            record: EntityVersionRecord {
                uuid: Uuid::new_v4(),
                chain_id: Uuid::new_v4(),
                version: 1,
                is_latest: true,
                entity_type: "Type".into(),
                name: "api".into(),
                namespace: "ns".into(),
                source: None,
                identity_hash: None,
                identity_hashes: vec![],
                structural_hash: None,
                valid_from: None,
                valid_to: None,
                deleted_at: None,
                last_seen_at: None,
                last_transition_at: None,
                sync_generation: None,
                collections: vec![],
                merged_into: None,
                embedding: None,
                stored: Default::default(),
            },
            identity_hash: None,
            name: "api".into(),
            entity_type: "Type".into(),
            namespace: "ns".into(),
            primary_keys: keys.iter().map(|k| k.to_string()).collect(),
            additional_keys: vec![],
            key_values: values
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        serde_json::to_value(PropertyValue::String(v.to_string())).unwrap(),
                    )
                })
                .collect(),
            properties: Default::default(),
        }
    }

    #[test]
    fn stored_record_covers_only_equivalent_incoming_candidates() {
        let mut stored = candidate(&[], &[]);
        stored.properties.insert(
            "workflow_path".into(),
            PropertyValue::String(".github/workflows/deploy.yml".into()),
        );
        let mut entity = test_entity("api");
        entity.entity_type = "Type".into();
        entity.all_properties.insert(
            "workflow_path".into(),
            PropertyValue::String(".github/workflows/deploy.yml".into()),
        );
        let mut local = super::super::matching_decision::LocalCandidate {
            component_id: Uuid::new_v4(),
            members: vec![(0, entity.clone())],
            stored: None,
        };
        assert!(local_covered_by_stored(&local, &stored));
        local.members[0].1.all_properties.insert(
            "workflow_path".into(),
            PropertyValue::String(".github/workflows/other.yml".into()),
        );
        assert!(!local_covered_by_stored(&local, &stored));
        local.members[0].1 = entity;
        local.members[0].1.all_properties.clear();
        assert!(!local_covered_by_stored(&local, &stored));
    }

    #[test]
    fn authoritative_keys_conflict_only_when_both_sides_carry_different_values() {
        let mut entity = test_entity("api");
        entity.primary_key_properties = vec!["name".into(), "arn".into()];
        entity
            .all_properties
            .insert("arn".into(), PropertyValue::String("arn:a".into()));
        let same_arn = candidate(&["arn"], &[("arn", "arn:a")]);
        assert_eq!(conflicting_key(&entity, &same_arn), None);
        let other_arn = candidate(&["arn"], &[("arn", "arn:b")]);
        assert_eq!(conflicting_key(&entity, &other_arn).as_deref(), Some("arn"));
        // The candidate declares a key the incoming entity does not carry.
        let candidate_key = candidate(&["resource_id"], &[("resource_id", "r-1")]);
        assert_eq!(conflicting_key(&entity, &candidate_key), None);
        entity
            .all_properties
            .insert("resource_id".into(), PropertyValue::String("r-2".into()));
        assert_eq!(
            conflicting_key(&entity, &candidate_key).as_deref(),
            Some("resource_id")
        );
        let mut by_name = candidate(&["name"], &[("name", "other")]);
        by_name.name = "other".into();
        assert_eq!(conflicting_key(&entity, &by_name).as_deref(), Some("name"));
        entity.primary_key_properties.clear();
        assert_eq!(conflicting_key(&entity, &by_name), None);
    }

    #[test]
    fn malformed_alternative_keys_cannot_disappear_during_candidate_decode() {
        for value in [
            serde_json::json!([["arn"]]),
            serde_json::json!(42),
            serde_json::Value::Null,
        ] {
            let mut record = candidate(&[], &[]).record;
            record
                .stored
                .insert("additional_key_properties".into(), value);
            assert!(candidate_from_record(&record).is_err());
        }
    }

    #[test]
    fn candidate_comparison_preserves_null_presence_and_property_types() {
        let base = candidate(&[], &[]).record;
        let absent = summarize_candidate(&candidate_from_record(&base).unwrap().unwrap());
        let mut record = base.clone();
        record
            .stored
            .insert("property_type_details".into(), serde_json::json!("n"));
        let null = summarize_candidate(&candidate_from_record(&record).unwrap().unwrap());
        assert_ne!(absent, null);
        record
            .stored
            .insert("prop_details".into(), serde_json::json!("{}"));
        record
            .stored
            .insert("property_type_details".into(), serde_json::json!("s"));
        let string = summarize_candidate(&candidate_from_record(&record).unwrap().unwrap());
        record
            .stored
            .insert("property_type_details".into(), serde_json::json!("j"));
        let json = summarize_candidate(&candidate_from_record(&record).unwrap().unwrap());
        assert_ne!(string, json);
        let payload: serde_json::Value = serde_json::from_str(&string).unwrap();
        assert_eq!(
            payload["properties"]["details"],
            serde_json::json!({"t":"s","v":"{}"})
        );
    }

    #[test]
    fn malformed_stored_key_declarations_are_not_silently_dropped() {
        let mut record = candidate(&[], &[]).record;
        record.stored.insert(
            "primary_key_properties".into(),
            serde_json::json!(["id", 3]),
        );
        assert!(candidate_from_record(&record).is_err());
    }

    fn catalog_observation(value: &str) -> EntityNode {
        let mut entity = test_entity("api");
        entity.primary_key_properties = vec!["catalog_id".into()];
        entity
            .all_properties
            .insert("catalog_id".into(), PropertyValue::String(value.into()));
        entity
    }

    fn exact_output(entity: EntityNode, record: EntityVersionRecord) -> NodeIdentityOutput {
        use kg_core::runtime::stage_output::{
            IdentityMatch, IdentityOutcome, NodeExtractionOutput,
        };
        let matches = HashMap::from([(
            entity.uuid,
            IdentityMatch {
                outcome: IdentityOutcome::Matched,
                chain_id: record.chain_id,
                existing: Some(record),
            },
        )]);
        NodeIdentityOutput {
            identity_revisions: Default::default(),
            extraction: NodeExtractionOutput {
                raw_text_drafts: Default::default(),
                relationship_changes: Default::default(),
                version_exclusions: Default::default(),
                text_observation_ids: Default::default(),
                fk_exclusions: Default::default(),
                schemas: Default::default(),
                history: Default::default(),
                snapshot_nodes: Default::default(),
                entities_by_snapshot: Arc::new(vec![(Uuid::new_v4(), vec![entity])]),
                source_deleted: Default::default(),
                sub_edges: Default::default(),
                incomplete_extractions: Default::default(),
            },
            observations: Default::default(),
            matches,
            methods: Default::default(),
            chains_merged: Default::default(),
        }
    }

    fn adoption(candidate: FuzzyCandidate) -> Option<Adoption> {
        Some(Adoption {
            candidate,
            resolved_by: "test".into(),
            reason: "test".into(),
        })
    }

    #[test]
    fn exact_anchor_observation_keys_constrain_semantic_adoption() {
        let stored = candidate(&["name"], &[]);
        let mut anchor = test_entity("api");
        anchor.additional_key_properties = vec![vec!["catalog_id".into()]];
        anchor
            .all_properties
            .insert("catalog_id".into(), PropertyValue::String("A".into()));
        let incoming = catalog_observation("B");
        // Neither observation conflicts with the older stored record alone.
        assert_eq!(conflicting_key(&anchor, &stored), None);
        assert_eq!(conflicting_key(&incoming, &stored), None);
        let output = exact_output(anchor, stored.record.clone());
        let decisions = vec![(vec![(1, incoming)], adoption(stored.clone()))];
        let error = validate_shared_targets(&decisions, std::slice::from_ref(&output)).unwrap_err();
        assert!(matches!(error, StageError::StateValidation { message, .. }
            if message.contains("conflicting identifying keys")));

        let compatible = vec![(vec![(1, catalog_observation("A"))], adoption(stored))];
        assert!(validate_shared_targets(&compatible, &[output]).is_ok());
    }

    #[test]
    fn independent_semantic_groups_cannot_merge_conflicting_keys_through_one_target() {
        let stored = candidate(&["name"], &[]);
        for reverse in [false, true] {
            let mut decisions = vec![
                (
                    vec![(0, catalog_observation("A"))],
                    adoption(stored.clone()),
                ),
                (
                    vec![(1, catalog_observation("B"))],
                    adoption(stored.clone()),
                ),
            ];
            if reverse {
                decisions.reverse();
            }
            let error = validate_shared_targets(&decisions, &[]).unwrap_err();
            assert!(matches!(error, StageError::StateValidation { message, .. }
                if message.contains("conflicting identifying keys")));
        }
    }

    #[test]
    fn changed_target_between_exact_and_semantic_reads_requires_revision_retry() {
        let stored = candidate(&["name"], &[]);
        let output = exact_output(test_entity("api"), stored.record.clone());
        for change_uuid in [false, true] {
            let mut changed = stored.clone();
            if change_uuid {
                changed.record.uuid = Uuid::new_v4();
            } else {
                changed
                    .record
                    .stored
                    .insert("summary".into(), serde_json::json!("changed"));
            }
            let decisions = vec![(vec![(1, catalog_observation("A"))], adoption(changed))];
            assert!(matches!(
                validate_shared_targets(&decisions, std::slice::from_ref(&output)),
                Err(StageError::IdentityRevisionChanged)
            ));
        }
    }
}
