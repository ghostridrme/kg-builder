//! Build a complete guarded write plan and reject oversized work before paid calls.

use std::collections::HashSet;

use async_trait::async_trait;
use kg_core::{
    embedding::{self, EmbeddingSettings},
    embedding_rebuild::EmbeddingKind,
    errors::StageError,
    runtime::{
        stage_output::{FlushWork, PlannedBatchOutput, PlannedEmbedding},
        RuntimeContext, StageOutput,
    },
    traits::{
        graph_commit::{MAX_EMBEDDING_BYTES_PER_BATCH, MAX_STATEMENTS_PER_BATCH},
        BatchKind, GraphMutation, MutationBatch, Precondition, Stage,
    },
};

use super::{
    mutation_plan::{identity_budget_exhausted, invalid, STAGE},
    node_mutation_planning::plan_nodes,
    reconciliation_planning::plan_reconciliation,
    relationship_embedding_planning::plan_relationship_embeddings,
    relationship_mutation_planning::plan_relationships_for_rule_maintenance,
};

/// Plan guarded writes and reserve their full cost before embedding calls.
pub struct MutationPlanningStage;

#[async_trait]
impl Stage for MutationPlanningStage {
    fn processing_version(&self) -> String {
        "mutation-planning-reference-coverage-v6".into()
    }
    fn name(&self) -> &str {
        STAGE
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::FlushBatch, StageKind::PlannedBatch)]
    }
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        let StageOutput::FlushBatch(flush) = input else {
            return Err(invalid("expected a flush batch".into()));
        };
        let planning = async {
            let mut plan = match (&flush.work, flush.batch.kind) {
                (FlushWork::Nodes(batch), BatchKind::Node) => plan_nodes(batch, ctx).await?,
                (FlushWork::Relationships(batch), BatchKind::Relationship) => {
                    let retire_at = ctx
                        .rule_maintenance_guard
                        .filter(|(_, _, status)| {
                            matches!(
                                status,
                                kg_core::traits::RuleStatus::Stale
                                    | kg_core::traits::RuleStatus::Revoked
                            )
                        })
                        .and(ctx.rule_maintenance_effective_at);
                    let (mut plan, targets) =
                        plan_relationships_for_rule_maintenance(batch, retire_at)?;
                    plan_relationship_embeddings(&targets, ctx, &mut plan).await?;
                    plan
                }
                (FlushWork::Reconciliation(batch), BatchKind::Reconciliation) => {
                    plan_reconciliation(batch, flush.batch)?
                }
                _ => return Err(invalid("batch identity and work kind disagree".into())),
            };
            plan.preconditions.splice(
                0..0,
                flush.scans.iter().map(|scan| Precondition::OwnsCollection {
                    collection: scan.collection.clone(),
                    generation: scan.generation,
                    run_id: flush.batch.run_id,
                }),
            );
            if let Some((id, revision, status)) = ctx.rule_maintenance_guard {
                plan.require(Precondition::RuleRevisionIs {
                    id,
                    revision,
                    status,
                });
            }
            plan.counts.embeddings += plan.embeddings.len();
            let mut recovery = flush.recovery.clone().unwrap_or_default();
            let derived_followup =
                ctx.entity_summary_settings.enabled || ctx.community_settings.incremental_enabled;
            if derived_followup {
                recovery.summary_affected_chains = super::summary_targets::collect(
                    &flush.work,
                    &mut plan,
                    ctx,
                    &mut recovery.skipped_summaries,
                )
                .await?;
            }
            plan.counts.summaries_skipped += recovery.skipped_summaries.len();
            let mut result =
                serde_json::to_value(plan.counts).map_err(|error| invalid(error.to_string()))?;
            if flush.recovery.is_some() || derived_followup {
                result["recovery"] =
                    serde_json::to_value(recovery).map_err(|error| invalid(error.to_string()))?;
            }
            let output = PlannedBatchOutput {
                batch: MutationBatch {
                    org_id: ctx.org_id.to_string(),
                    batch: flush.batch,
                    fingerprint: flush.fingerprint,
                    preconditions: plan.preconditions,
                    mutations: plan.mutations,
                    result,
                },
                embeddings: plan.embeddings,
            };
            preflight(&output, ctx)?;
            tracing::debug!(
                batch_kind = output.batch.batch.kind.label(),
                batch_index = output.batch.batch.index,
                mutations = output.batch.mutations.len(),
                preconditions = output.batch.preconditions.len(),
                embeddings = output.embeddings.len(),
                "batch mutations planned"
            );
            Ok(StageOutput::PlannedBatch(output))
        };
        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(StageError::Cancelled { stage: STAGE.into() }),
            result = async {
                if let Some(deadline) = ctx.identity_deadline {
                    tokio::time::timeout_at(deadline, planning).await.map_err(|_| identity_budget_exhausted())?
                } else { planning.await }
            } => result,
        }
    }
}

pub(crate) fn target_settings(
    target: &PlannedEmbedding,
    ctx: &RuntimeContext,
) -> EmbeddingSettings {
    let mut settings = ctx.embedding.clone();
    if target.kind() == EmbeddingKind::Relationship {
        settings.text_version = embedding::RELATIONSHIP_TEXT_VERSION.into();
    }
    if target.kind() == EmbeddingKind::DerivedSummary {
        settings.text_version = kg_core::entity_summary::SUMMARY_TEXT_VERSION.into();
    }
    settings
}

pub(crate) fn preflight(
    planned: &PlannedBatchOutput,
    ctx: &RuntimeContext,
) -> Result<(), StageError> {
    if planned.batch.org_id != ctx.org_id.as_ref() {
        return Err(invalid("embedding plan crosses organization".into()));
    }
    validate_commit(&planned.batch, ctx).map_err(|error| invalid(error.to_string()))?;
    let mut statements = planned.batch.estimated_statement_count();
    let mut entity_group = false;
    for target in &planned.embeddings {
        use kg_core::runtime::stage_output::PlannedEmbeddingWrite;
        let is_entity = matches!(&target.write, PlannedEmbeddingWrite::EntityVersion { .. });
        // The compiler groups consecutive entity embedding writes. Duplicate targets
        // are rejected below, so each such run costs one statement. Other write kinds
        // break the group and retain their individual guard/write costs.
        let additional = match &target.write {
            PlannedEmbeddingWrite::EntityVersion { .. } => usize::from(!entity_group),
            PlannedEmbeddingWrite::DerivedSummary { guard, .. } => {
                guard.entity_versions.len().saturating_add(2)
            }
            PlannedEmbeddingWrite::RelationshipVersion => 1,
        };
        statements = statements.saturating_add(additional);
        entity_group = is_entity;
    }
    if statements > MAX_STATEMENTS_PER_BATCH && !ctx.graph.supports_paged_commits() {
        return Err(invalid(
            "planned batch exceeds statement budget before embeddings".into(),
        ));
    }
    if planned.batch.mutations.iter().any(|mutation| {
        matches!(
            mutation,
            GraphMutation::SetEmbedding { .. }
                | GraphMutation::SetRelationshipEmbedding { .. }
                | GraphMutation::SetEntityVersionEmbedding { .. }
                | GraphMutation::SetDerivedSummary { .. }
        )
    }) {
        return Err(invalid(
            "embedding mutations must be prepared by the embedding stage".into(),
        ));
    }
    let mut bytes = 0usize;
    let mut versions = HashSet::new();
    for target in &planned.embeddings {
        let settings = target_settings(target, ctx);
        if target.uuid.is_nil()
            || target.namespace.trim().is_empty()
            || target.text.trim().is_empty()
            || target.text.chars().count() > embedding::MAX_TEXT_CHARS
            || target.text_version != settings.text_version
            || target.content_hash != embedding::content_hash(&target.text)
            || !versions.insert((target.kind() as u8, target.uuid))
        {
            return Err(invalid("invalid or duplicate planned embedding".into()));
        }
        bytes = bytes.saturating_add(
            settings
                .dimension
                .saturating_mul(std::mem::size_of::<f32>()),
        );
        match &target.write {
            kg_core::runtime::stage_output::PlannedEmbeddingWrite::EntityVersion {
                expected_properties: properties,
                labels,
                primary_key_properties,
                additional_key_properties,
            } => {
                kg_core::traits::graph_mutation::validate_entity_embedding_state(properties)
                    .map_err(|error| invalid(error.to_string()))?;
                if properties
                    .get("namespace")
                    .and_then(serde_json::Value::as_str)
                    != Some(target.namespace.as_str())
                {
                    return Err(invalid(
                        "entity embedding namespace disagrees with guarded state".into(),
                    ));
                }
                // The guarded content plus the text inputs storage does not guard.
                let mut rendered = properties.clone();
                rendered.insert("labels".into(), serde_json::json!(labels));
                rendered.insert(
                    "primary_key_properties".into(),
                    serde_json::json!(primary_key_properties),
                );
                rendered.insert(
                    "additional_key_properties".into(),
                    serde_json::json!(serde_json::to_string(additional_key_properties)
                        .map_err(|error| invalid(error.to_string()))?),
                );
                let guarded_text = kg_core::embedding_rebuild::EmbeddingRecord {
                    uuid: target.uuid,
                    properties: rendered,
                }
                .text(EmbeddingKind::Entity, &settings.entity_fields)
                .map_err(|error| invalid(error.to_string()))?;
                if guarded_text != target.text {
                    return Err(invalid(
                        "entity embedding text disagrees with guarded state".into(),
                    ));
                }
                bytes = bytes.saturating_add(
                    serde_json::to_vec(properties)
                        .map_err(|error| invalid(error.to_string()))?
                        .len(),
                );
            }
            kg_core::runtime::stage_output::PlannedEmbeddingWrite::RelationshipVersion => {}
            kg_core::runtime::stage_output::PlannedEmbeddingWrite::DerivedSummary {
                summary,
                guard,
            } => {
                summary
                    .validate()
                    .map_err(|error| invalid(error.to_string()))?;
                guard
                    .validate()
                    .map_err(|error| invalid(error.to_string()))?;
                let expected_target = guard.entity_versions.values().flatten().find(|properties| {
                    properties.get("uuid").and_then(serde_json::Value::as_str)
                        == Some(target.uuid.to_string().as_str())
                });
                if expected_target
                    .and_then(|properties| properties.get("namespace"))
                    .and_then(serde_json::Value::as_str)
                    != Some(target.namespace.as_str())
                    || guard.entity_versions.values().flatten().any(|properties| {
                        properties
                            .get("org_id")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|org| org != ctx.org_id.as_ref())
                    })
                {
                    return Err(invalid(
                        "summary embedding scope disagrees with evidence".into(),
                    ));
                }
                if guard.target_uuid != target.uuid
                    || kg_core::entity_summary::embedding_text(&summary.text) != target.text
                {
                    return Err(invalid(
                        "derived summary disagrees with embedding target".into(),
                    ));
                }
                bytes = bytes.saturating_add(
                    serde_json::to_vec(&(guard, summary))
                        .map_err(|error| invalid(error.to_string()))?
                        .len(),
                );
            }
        }
    }
    if bytes > MAX_EMBEDDING_BYTES_PER_BATCH && !ctx.graph.supports_paged_commits() {
        return Err(invalid(
            "planned batch exceeds embedding byte budget before provider calls".into(),
        ));
    }
    if ctx.graph.supports_paged_commits() && !planned.embeddings.is_empty() {
        // Reserve the actual target dimensions before provider work. f32 values
        // use at most 24 JSON bytes each. Probe-only model padding reserves
        // 20 bytes per component plus the serialized 1.0 and comma; these
        // placeholders are discarded and never sent to storage or a provider.
        let mut probe = planned.batch.clone();
        let mut reserve = 0usize;
        for target in &planned.embeddings {
            let settings = target_settings(target, ctx);
            reserve = reserve.saturating_add(settings.dimension.saturating_mul(24));
            if reserve > 64 * 1024 * 1024 {
                return Err(invalid(
                    "embedding reservation exceeds frozen plan byte budget".into(),
                ));
            }
            let mut model = settings.model;
            model.extend(std::iter::repeat_n(
                'x',
                settings.dimension.saturating_mul(20),
            ));
            let embedding = kg_core::traits::graph_backend::GraphEmbedding {
                model,
                values: vec![1.0; settings.dimension],
            };
            probe.mutations.push(embedding_mutation(target, embedding));
        }
        validate_commit(&probe, ctx).map_err(|e| invalid(e.to_string()))?;
    }
    Ok(())
}

/// Preserve the adapter's atomic budget when it has no durable page protocol.
pub(crate) fn validate_commit(
    batch: &kg_core::traits::MutationBatch,
    ctx: &RuntimeContext,
) -> Result<(), kg_core::errors::BackendError> {
    match batch.validate() {
        Ok(()) => Ok(()),
        Err(_) if ctx.graph.supports_paged_commits() => {
            kg_core::traits::commit_pages::partition(batch).map(|_| ())
        }
        Err(error) => Err(error),
    }
}
pub(crate) fn embedding_mutation(
    target: &kg_core::runtime::stage_output::PlannedEmbedding,
    embedding: kg_core::traits::graph_backend::GraphEmbedding,
) -> GraphMutation {
    use kg_core::runtime::stage_output::PlannedEmbeddingWrite;
    match &target.write {
        PlannedEmbeddingWrite::EntityVersion {
            expected_properties,
            ..
        } => GraphMutation::SetEntityVersionEmbedding {
            uuid: target.uuid,
            expected_properties: expected_properties.clone(),
            embedding,
            text_version: target.text_version.clone(),
            content_hash: target.content_hash.clone(),
        },
        PlannedEmbeddingWrite::RelationshipVersion => GraphMutation::SetRelationshipEmbedding {
            uuid: target.uuid,
            embedding,
            text_version: target.text_version.clone(),
            content_hash: target.content_hash.clone(),
        },
        PlannedEmbeddingWrite::DerivedSummary { summary, guard } => {
            GraphMutation::SetDerivedSummary {
                summary: summary.clone(),
                guard: guard.clone(),
                embedding,
            }
        }
    }
}
