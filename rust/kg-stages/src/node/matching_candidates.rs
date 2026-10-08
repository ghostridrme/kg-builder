//! Scoped, bounded candidate frontiers shared by every observation in a key component.
use super::fuzzy_match::{invalid, step_failed};
use kg_core::{
    errors::StageError,
    models::EntityNode,
    runtime::RuntimeContext,
    traits::{
        graph_backend::GraphEmbedding, identity_candidates::MAX_IDENTITY_QUERY_NAMES,
        EntityVersionRecord, IdentityCandidateQuery, IdentityCandidateRequest, IdentityScope,
    },
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;
/// A candidate resolved from the graph: the latest version of its chain.
#[derive(Clone)]
pub(super) struct FuzzyCandidate {
    pub(super) record: EntityVersionRecord,
    /// The candidate's current primary identity hash, kept as an alias after adoption.
    pub(super) identity_hash: Option<String>,
    pub(super) name: String,
    pub(super) entity_type: String,
    pub(super) namespace: String,
    /// Declared identity keys of the stored version.
    pub(super) primary_keys: Vec<String>,
    pub(super) additional_keys: Vec<Vec<String>>,
    /// Source properties as stored, untruncated, for key comparison.
    pub(super) key_values: HashMap<String, Value>,
    /// Full source properties, sorted for stable comparison.
    pub(super) properties: BTreeMap<String, kg_core::models::PropertyValue>,
}

/// Declared keys are authoritative; an undeclared display name is only a clue.
pub(super) fn conflicting_key(entity: &EntityNode, candidate: &FuzzyCandidate) -> Option<String> {
    let declared = entity
        .primary_key_properties
        .iter()
        .chain(entity.additional_key_properties.iter().flatten());
    if declared.clone().any(|key| key == "name") && entity.name != candidate.name {
        return Some("name".into());
    }
    declared
        .chain(candidate.primary_keys.iter())
        .chain(candidate.additional_keys.iter().flatten())
        .filter(|key| key.as_str() != "name")
        .find(|key| {
            let incoming = entity
                .all_properties
                .get(key.as_str())
                .map(|value| serde_json::to_value(value).expect("validated property serializes"));
            let stored = candidate.key_values.get(key.as_str());
            matches!((incoming, stored), (Some(a), Some(b)) if &a != b)
        })
        .cloned()
}

pub(super) fn candidate_from_record(
    record: &EntityVersionRecord,
) -> Result<Option<FuzzyCandidate>, StageError> {
    if record.name.is_empty() {
        return Ok(None);
    }
    let typed =
        record
            .typed_source_properties()
            .map_err(|message| StageError::StateValidation {
                stage: "fuzzy_match".into(),
                message,
            })?;
    let properties = typed
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let key_values = typed
        .iter()
        .filter(|(_, value)| value.as_identity_key().is_some())
        .map(|(key, value)| {
            (
                key.clone(),
                serde_json::to_value(value).expect("scalar identity serializes"),
            )
        })
        .collect();
    let primary_keys = record
        .stored
        .get("primary_key_properties")
        .map(|keys| serde_json::from_value::<Vec<String>>(keys.clone()))
        .transpose()
        .map_err(|_| invalid("invalid stored primary keys"))?
        .unwrap_or_default();

    Ok(Some(FuzzyCandidate {
        record: record.clone(),
        identity_hash: record.identity_hash.clone(),
        name: record.name.clone(),
        entity_type: record.entity_type.clone(),
        namespace: record.namespace.clone(),
        primary_keys,
        additional_keys: record
            .stored
            .get("additional_key_properties")
            .map(|value| {
                let encoded = value
                    .as_str()
                    .ok_or_else(|| invalid("invalid stored identifying keys"))?;
                serde_json::from_str(encoded)
                    .map_err(|_| invalid("invalid stored identifying keys"))
            })
            .transpose()
            .map_err(|_| invalid("invalid stored identifying keys"))?
            .unwrap_or_default(),
        key_values,
        properties,
    }))
}

/// Reject oversized components before generating billable embedding requests.
pub(super) fn validate_component(
    entities: &[&EntityNode],
    ctx: &RuntimeContext,
) -> Result<(), StageError> {
    let first = entities
        .first()
        .ok_or_else(|| invalid("empty identity component"))?;
    let names: BTreeSet<_> = entities.iter().map(|e| e.name.clone()).collect();
    let chains: BTreeSet<_> = entities.iter().map(|e| e.chain_id).collect();
    let request = IdentityCandidateRequest {
        scope: IdentityScope {
            namespace: first.namespace.clone(),
            entity_type: first.entity_type.clone(),
        },
        query: IdentityCandidateQuery::Names(names.into_iter().collect()),
        exclude_chains: chains.into_iter().collect(),
        limit: ctx.matching_settings.candidate_limit,
    };
    request
        .validate(&ctx.org_id)
        .map_err(|e| step_failed("candidate_budget", e))?;
    let properties = property_pairs(entities)?;
    if !properties.is_empty() {
        let mut property_request = request.clone();
        property_request.query = IdentityCandidateQuery::PropertyOverlap(properties);
        property_request
            .validate(&ctx.org_id)
            .map_err(|e| step_failed("candidate_budget", e))?;
    }
    let mut texts = BTreeSet::new();
    for entity in entities {
        if entity.org_id != ctx.org_id.as_ref()
            || entity.namespace != first.namespace
            || entity.entity_type != first.entity_type
        {
            return Err(invalid("identity component crosses scope"));
        }
        // One similarity query per distinct name: observations of one entity
        // differ in properties (and so in text) without being different candidates.
        texts.insert((entity.entity_type.clone(), entity.name.clone()));
        if texts.len() > MAX_IDENTITY_QUERY_NAMES {
            return Err(invalid("identity component exceeds candidate query budget"));
        }
    }
    Ok(())
}

/// Keyless text mentions have an inferred type unless that type is explicitly declared.
pub(super) fn inferred_type(
    extraction: &kg_core::runtime::stage_output::NodeExtractionOutput,
    entity: &EntityNode,
) -> bool {
    if entity.has_authoritative_keys() {
        return false;
    }
    extraction.raw_text_drafts.iter().any(|draft| {
        draft.org_id == entity.org_id
            && draft.namespace == entity.namespace
            && draft
                .mentions
                .iter()
                .any(|mention| mention.observation_uuid == entity.uuid)
            && extraction
                .snapshot_nodes
                .iter()
                .find(|s| s.uuid == draft.snapshot_uuid)
                .and_then(|snapshot| {
                    extraction
                        .schemas
                        .get(&snapshot.uuid)
                        .and_then(|schemas| schemas.for_snapshot(snapshot, &entity.org_id).ok())
                })
                .is_some_and(|schema| {
                    !schema
                        .entity_types
                        .iter()
                        .any(|declared| declared.name == entity.entity_type)
                })
    })
}

/// Names must fit completely; vector results are bounded nearest-neighbor windows.
pub(super) async fn retrieve(
    entities: &[&EntityNode],
    scope: &IdentityScope,
    ctx: &RuntimeContext,
) -> Result<Vec<FuzzyCandidate>, StageError> {
    let first = entities
        .first()
        .ok_or_else(|| invalid("empty identity component"))?;
    let mut names = BTreeSet::new();
    let mut vectors = BTreeMap::new();
    let mut excluded = BTreeSet::new();
    for entity in entities {
        if entity.org_id != ctx.org_id.as_ref()
            || entity.namespace != scope.namespace
            || entity.entity_type != first.entity_type
            || (scope.entity_type != "*" && entity.entity_type != scope.entity_type)
        {
            return Err(invalid("identity component crosses scope"));
        }
        names.insert(entity.name.clone());
        excluded.insert(entity.chain_id);
        let embedding = entity
            .embedding
            .as_ref()
            .ok_or_else(|| invalid("missing incoming embedding"))?;
        // One vector per name: later observations of the same entity replace
        // earlier ones, so a batch of many observations stays within budget.
        vectors.insert(entity.name.clone(), embedding.clone());
    }
    if names.len() > MAX_IDENTITY_QUERY_NAMES || vectors.len() > MAX_IDENTITY_QUERY_NAMES {
        return Err(invalid("identity component exceeds candidate query budget"));
    }
    let names: Vec<_> = names.into_iter().collect();
    let mut queries = vec![
        (0, IdentityCandidateQuery::ExactNames(names.clone())),
        (3, IdentityCandidateQuery::Names(names)),
    ];
    let properties = property_pairs(entities)?;
    if !properties.is_empty() {
        queries.push((2, IdentityCandidateQuery::PropertyOverlap(properties)));
    }
    queries.extend(vectors.into_values().map(|v| {
        (
            1,
            IdentityCandidateQuery::Similarity {
                embedding: GraphEmbedding {
                    model: ctx.embedding.model.clone(),
                    values: v.values.clone(),
                },
                text_version: ctx.embedding.text_version.clone(),
                // For text mentions, dissimilar stored entities are not
                // candidates on vector evidence (names and properties still
                // reach them); keyed components keep the ranked window.
                min_score: if entities.iter().all(|e| !e.has_authoritative_keys()) {
                    ctx.matching_settings.candidate_min_similarity
                } else {
                    -1.0
                },
            },
        )
    }));
    let mut merged = BTreeMap::<Uuid, (EntityVersionRecord, [f64; 4])>::new();
    for (source, query) in queries {
        let mut request = IdentityCandidateRequest {
            scope: scope.clone(),
            query,
            exclude_chains: excluded.iter().copied().collect(),
            limit: ctx.matching_settings.candidate_limit,
        };
        let mut page = ctx
            .graph
            .identity_candidates(&ctx.org_id, &request)
            .await
            .map_err(|e| step_failed("identity_candidates", e))?;
        page.validate(&ctx.org_id, &request)
            .map_err(|e| step_failed("identity_candidates", e))?;
        if page.truncated {
            kg_core::telemetry::candidate_truncation("fuzzy_match");
        }
        let ranked_window = source != 0;
        if page.truncated
            && !ranked_window
            && request.limit < ctx.matching_settings.max_candidate_limit
        {
            request.limit = ctx.matching_settings.max_candidate_limit;
            page = ctx
                .graph
                .identity_candidates(&ctx.org_id, &request)
                .await
                .map_err(|e| step_failed("identity_candidates", e))?;
            page.validate(&ctx.org_id, &request)
                .map_err(|e| step_failed("identity_candidates", e))?;
            if page.truncated {
                kg_core::telemetry::candidate_truncation("fuzzy_match");
            }
        }
        if page.truncated && !ranked_window {
            return Err(invalid(
                "identity candidate frontier exceeds budget for names; evidence is incomplete",
            ));
        }
        let mut items = compatible_items(entities, page.items)?;
        if ranked_window
            && page.truncated
            && items.len() < ctx.matching_settings.candidate_limit
            && request.limit < ctx.matching_settings.max_candidate_limit
        {
            request.limit = ctx.matching_settings.max_candidate_limit;
            page = ctx
                .graph
                .identity_candidates(&ctx.org_id, &request)
                .await
                .map_err(|e| step_failed("identity_candidates", e))?;
            page.validate(&ctx.org_id, &request)
                .map_err(|e| step_failed("identity_candidates", e))?;
            if page.truncated {
                kg_core::telemetry::candidate_truncation("fuzzy_match");
            }
            items = compatible_items(entities, page.items)?;
        }
        if ranked_window && page.truncated && items.len() < ctx.matching_settings.candidate_limit {
            return Err(invalid(
                "compatible identity candidates exceed retrieval budget",
            ));
        }
        let more_ranked_candidates = ranked_window
            && (page.truncated || items.len() > ctx.matching_settings.candidate_limit);
        if ranked_window {
            items.truncate(ctx.matching_settings.candidate_limit);
        }
        tracing::debug!(
            stage = "fuzzy_match",
            source = match source {
                0 => "name",
                1 => "vector",
                2 => "property",
                _ => "name_fulltext",
            },
            coverage = if ranked_window {
                "ranked_window"
            } else {
                "exhaustive_name_matches"
            },
            candidates = items.len(),
            more_ranked_candidates,
            "identity candidate frontier"
        );
        for (rank, item) in items.into_iter().enumerate() {
            let score = 1.0 / (60.0 + rank as f64 + 1.0);
            let entry = merged
                .entry(item.record.chain_id)
                .or_insert_with(|| (item.record.clone(), [0.0; 4]));
            if entry.0.uuid != item.record.uuid || entry.0.stored != item.record.stored {
                return Err(StageError::IdentityRevisionChanged);
            }
            entry.1[source] = entry.1[source].max(score);
        }
        if merged.len() > ctx.matching_settings.max_candidate_limit {
            return Err(invalid(
                "combined identity candidates exceed budget; evidence is incomplete",
            ));
        }
    }
    let mut records: Vec<_> = merged.into_values().collect();
    records.sort_by(|a, b| {
        b.1.iter()
            .sum::<f64>()
            .total_cmp(&a.1.iter().sum::<f64>())
            .then(a.0.chain_id.cmp(&b.0.chain_id))
    });
    records
        .into_iter()
        .map(|(record, _)| {
            candidate_from_record(&record)?.ok_or_else(|| invalid("candidate has no name"))
        })
        .collect()
}

fn compatible_items(
    entities: &[&EntityNode],
    items: Vec<kg_core::traits::IdentityCandidate>,
) -> Result<Vec<kg_core::traits::IdentityCandidate>, StageError> {
    let mut compatible = Vec::new();
    for item in items {
        let candidate =
            candidate_from_record(&item.record)?.ok_or_else(|| invalid("candidate has no name"))?;
        if entities
            .iter()
            .all(|entity| conflicting_key(entity, &candidate).is_none())
        {
            compatible.push(item);
        }
    }
    Ok(compatible)
}

// Oversized values remain in source evidence; they are not auxiliary retrieval clues.
fn property_pairs(
    entities: &[&EntityNode],
) -> Result<Vec<(String, kg_core::models::PropertyValue)>, StageError> {
    use kg_core::models::PropertyValue;
    let mut pairs = BTreeMap::new();
    for entity in entities {
        for (key, value) in &entity.all_properties {
            let eligible = match value {
                PropertyValue::String(value) => !value.trim().is_empty() && value.len() <= 4096,
                PropertyValue::Integer(_) | PropertyValue::Bool(_) => true,
                PropertyValue::Float(value) => value.is_finite(),
                _ => false,
            };
            if eligible && !key.trim().is_empty() && key.len() <= 1024 {
                let encoded = serde_json::to_string(value)
                    .map_err(|_| invalid("invalid property evidence"))?;
                pairs
                    .entry((key.clone(), encoded))
                    .or_insert_with(|| value.clone());
            }
        }
    }
    Ok(pairs
        .into_iter()
        .map(|((key, _), value)| (key, value))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::{
        models::{PropertyValue, SnapshotNode},
        runtime::{
            entity_drafts::{EntityMention, RawMention, RawTextDraft},
            schemas::ObservationSchemas,
            stage_output::NodeExtractionOutput,
        },
        traits::Ontology,
    };
    use serde_json::json;
    use std::sync::Arc;

    fn observation(declared: &[&str]) -> (NodeExtractionOutput, EntityNode) {
        let mut entity = crate::node::entity_versioning::tests::test_entity("orders");
        entity.entity_type = "CloudSqlInstance".into();
        entity.primary_key_properties.clear();
        entity.extracted_by = "llm:test".into();
        let snapshot: SnapshotNode = serde_json::from_value(json!({
            "uuid":Uuid::new_v4(), "org_id":entity.org_id, "namespace":entity.namespace,
            "name":"incident", "data_type":"text", "snapshot_kind":"incremental",
            "complete":false, "source":"slack", "content":"Source evidence",
            "captured_at":"2026-09-01T12:00:00Z", "created_at":"2026-09-01T12:01:00Z",
            "entities":[], "entity_edges":[], "labels":[], "tags":{}
        }))
        .unwrap();
        let ontology: Ontology = serde_json::from_value(json!({
            "entity_types":declared.iter().map(|name| json!({"name":name})).collect::<Vec<_>>()
        }))
        .unwrap();
        let schemas = ObservationSchemas {
            org_id: entity.org_id.clone(),
            source: snapshot.source.clone(),
            definitions: BTreeMap::from([(snapshot.source.clone(), ontology)]),
        };
        let evidence = EntityMention {
            entity_type: entity.entity_type.clone(),
            name: entity.name.clone(),
            properties: Default::default(),
            extracted_by: entity.extracted_by.clone(),
        };
        let mention = RawMention {
            id: kg_core::runtime::entity_drafts::MentionId::for_mention(snapshot.uuid, &evidence)
                .unwrap(),
            observation_uuid: entity.uuid,
            evidence,
        };
        let draft =
            RawTextDraft::new(&snapshot, &schemas, &Default::default(), vec![mention]).unwrap();
        let extraction = NodeExtractionOutput {
            raw_text_drafts: Arc::new(vec![draft]),
            schemas: Arc::new(HashMap::from([(snapshot.uuid, schemas)])),
            snapshot_nodes: Arc::new(vec![snapshot]),
            entities_by_snapshot: Default::default(),
            relationship_changes: Default::default(),
            version_exclusions: Default::default(),
            text_observation_ids: Default::default(),
            fk_exclusions: Default::default(),
            history: Default::default(),
            source_deleted: Default::default(),
            sub_edges: Default::default(),
            incomplete_extractions: Default::default(),
        };
        (extraction, entity)
    }

    fn resolved_observation(
        declared: &[&str],
    ) -> kg_core::runtime::stage_output::NodeResolutionOutput {
        use kg_core::runtime::stage_output::{
            NodeResolutionOutput, Observed, ObservedEntityProperties,
        };
        let (extraction, mut entity) = observation(declared);
        let original_uuid = entity.uuid;
        let mut original =
            ObservedEntityProperties::from_entity(&entity, extraction.snapshot_nodes[0].uuid);
        original.raw_mention_id = Some(extraction.raw_text_drafts[0].mentions[0].id);
        original.resolved_entity_type = Some("GCP::SQL::Instance".into());
        entity.entity_type = "GCP::SQL::Instance".into();
        entity.uuid = Uuid::new_v4();
        entity.chain_id = Uuid::new_v4();
        // The stored canonical entity can have authoritative keys: the original mention did not.
        entity.primary_key_properties = vec!["selfLink".into()];
        entity.all_properties.insert(
            "selfLink".into(),
            PropertyValue::String("projects/orders/instances/orders".into()),
        );
        NodeResolutionOutput {
            raw_text_drafts: extraction.raw_text_drafts,
            schemas: extraction.schemas,
            snapshot_nodes: extraction.snapshot_nodes,
            observed_properties: Arc::new(vec![original]),
            nodes_unchanged: Arc::new(vec![Observed::new(original_uuid, entity)]),
            ..Default::default()
        }
    }

    #[test]
    fn open_ontology_adoption_retains_raw_mention_mapping_through_checkpoint() {
        for declared in [
            vec![],
            vec!["Application"],
            vec!["Application", "GCP::SQL::Instance"],
        ] {
            let output = resolved_observation(&declared);
            output
                .validate_raw_text_drafts(&Default::default())
                .unwrap();
            let mappings = output.raw_mention_mappings().unwrap();
            assert_eq!(mappings.len(), 1);
            assert_eq!(mappings[0].chain_id, output.nodes_unchanged[0].chain_id);
            assert_eq!(mappings[0].version_uuid, output.nodes_unchanged[0].uuid);
            assert_eq!(
                output.raw_text_drafts[0].mentions[0].evidence.entity_type,
                "CloudSqlInstance"
            );
            let restored: kg_core::runtime::stage_output::NodeResolutionOutput =
                serde_json::from_value(serde_json::to_value(&output).unwrap()).unwrap();
            restored
                .validate_raw_text_drafts(&Default::default())
                .unwrap();
            assert_eq!(restored.raw_mention_mappings().unwrap(), mappings);
        }
    }

    #[test]
    fn raw_mapping_rejects_declared_original_reclassification_and_corrupt_identity() {
        assert!(resolved_observation(&["Application", "CloudSqlInstance"])
            .raw_mention_mappings()
            .is_err());
        for mismatch in [
            "organization",
            "namespace",
            "canonical_type",
            "mention_key",
            "snapshot_key",
        ] {
            let mut output = resolved_observation(&["Application"]);
            match mismatch {
                "organization" => {
                    Arc::make_mut(&mut output.nodes_unchanged)[0].org_id = "other".into()
                }
                "namespace" => {
                    Arc::make_mut(&mut output.nodes_unchanged)[0].namespace = "other".into()
                }
                "canonical_type" => {
                    Arc::make_mut(&mut output.nodes_unchanged)[0].entity_type = "Application".into()
                }
                "mention_key" => {
                    Arc::make_mut(&mut output.observed_properties)[0].raw_mention_id =
                        Some(kg_core::runtime::entity_drafts::MentionId(Uuid::new_v4()))
                }
                "snapshot_key" => {
                    Arc::make_mut(&mut output.observed_properties)[0].snapshot_uuid = Uuid::new_v4()
                }
                _ => unreachable!(),
            }
            assert!(output.raw_mention_mappings().is_err(), "{mismatch}");
        }
    }

    #[test]
    fn candidate_semantics_change_the_owning_stage_descriptor() {
        use kg_core::traits::Stage;
        let prompts = [
            super::super::matching_decision::SYSTEM_PROMPT,
            super::super::matching_batch::BATCH_PROMPT,
            super::super::matching_decision::SHARED_EVIDENCE_LAYOUT,
            super::super::matching_decision::CYCLE_CLARIFICATION,
        ];
        let current = super::super::fuzzy_match::FuzzyMatchStage.processing_version();
        assert_eq!(
            current,
            kg_core::traits::stage::processing_version(
                "entity-matching-complete-evidence-v24",
                &prompts
            )
        );
        assert_ne!(
            current,
            kg_core::traits::stage::processing_version("entity-matching-declines-v5", &prompts)
        );
    }

    #[test]
    fn open_ontology_keeps_unlisted_text_type_inferred() {
        for declared in [vec![], vec!["Application"], vec!["Application", "Database"]] {
            let (extraction, entity) = observation(&declared);
            assert!(
                inferred_type(&extraction, &entity),
                "unrelated declarations must not authorize this classification"
            );
        }
    }

    #[test]
    fn actual_declared_type_is_not_inferred() {
        let (extraction, entity) = observation(&["Application", "CloudSqlInstance"]);
        assert!(!inferred_type(&extraction, &entity));
    }

    #[test]
    fn authoritative_keys_prevent_cross_classification_even_with_open_ontology() {
        for declared in [vec![], vec!["Application"]] {
            let (extraction, mut entity) = observation(&declared);
            entity.primary_key_properties = vec!["instance_id".into()];
            entity.all_properties.insert(
                "instance_id".into(),
                PropertyValue::String("orders-prod".into()),
            );
            assert!(!inferred_type(&extraction, &entity));
            entity.primary_key_properties = vec!["name".into()];
            assert!(!inferred_type(&extraction, &entity));
        }
    }

    #[test]
    fn inferred_classification_requires_matching_observation_and_scope() {
        for mismatch in [
            "organization",
            "namespace",
            "observation",
            "schema_organization",
            "snapshot_organization",
        ] {
            let (mut extraction, entity) = observation(&["Application"]);
            match mismatch {
                "organization" => {
                    Arc::make_mut(&mut extraction.raw_text_drafts)[0].org_id = "other".into()
                }
                "namespace" => {
                    Arc::make_mut(&mut extraction.raw_text_drafts)[0].namespace = "other".into()
                }
                "observation" => {
                    Arc::make_mut(&mut extraction.raw_text_drafts)[0].mentions[0].observation_uuid =
                        Uuid::new_v4()
                }
                "schema_organization" => {
                    Arc::make_mut(&mut extraction.schemas)
                        .values_mut()
                        .next()
                        .unwrap()
                        .org_id = "other".into()
                }
                "snapshot_organization" => {
                    Arc::make_mut(&mut extraction.snapshot_nodes)[0].org_id = "other".into()
                }
                _ => unreachable!(),
            }
            assert!(!inferred_type(&extraction, &entity), "{mismatch}");
        }
    }
}
