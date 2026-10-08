use std::sync::Arc;

use kg_core::errors::StageError;
use kg_core::runtime::stage_output::{NodeExtractionOutput, StructuredEntityDrafts};
use kg_core::runtime::{RuntimeContext, StageOutput};

use chrono::{DateTime, Utc};
use kg_core::entity_type_config::EntityTypeConfig;
use kg_core::identity::{structural_hash::compute_structural_hash, IdentityHash};
use kg_core::models::{ConnectorEntity, EntityNode, PropertyValue};
use uuid::Uuid;

// Consume declared drafts using the current schema and property rules.
#[tracing::instrument(name = "structured_preparation", skip_all)]
pub(crate) async fn prepare_structured(
    input: StructuredEntityDrafts,
    ctx: &RuntimeContext,
) -> Result<StageOutput, StageError> {
    let attached = input.schemas().cloned();
    let history = input.history().clone();
    let (snapshot_input, snapshot_node, drafts) =
        input
            .into_parts(&ctx.org_id)
            .map_err(|error| StageError::StateValidation {
                stage: "structured_preparation".into(),
                message: error.to_string(),
            })?;
    let schemas = crate::schemas::observation(ctx, &snapshot_input, attached)?;
    let org_id = ctx.org_id.as_ref();

    let namespace = &snapshot_input.namespace;

    let snapshot_uuid = snapshot_node.uuid;
    let captured_at = snapshot_node.captured_at;

    for entity in &snapshot_input.entities {
        crate::profiles::entity_type(ctx, &entity.source, &entity.entity_type)?;
    }
    let mut inferred = std::collections::HashMap::new();
    let mut groups: std::collections::BTreeMap<(&str, &str), Vec<&ConnectorEntity>> =
        Default::default();
    for entity in &snapshot_input.entities {
        if entity.primary_key_properties.is_empty()
            && entity.additional_key_properties.is_empty()
            && ctx.policy.for_source(&entity.source).schema
                == kg_core::policy::SchemaMode::InferOnce
        {
            groups
                .entry((&entity.source, &entity.entity_type))
                .or_default()
                .push(entity);
        }
    }
    for ((source, entity_type), samples) in groups {
        let schema = super::schema_inference::resolve_schema(
            ctx,
            org_id,
            source,
            entity_type,
            namespace,
            &samples,
        )
        .await?;
        inferred.insert((source.to_string(), entity_type.to_string()), schema);
    }

    let mut entities = Vec::new();
    let mut version_exclusions = std::collections::HashMap::new();
    let mut source_deleted = Vec::new();
    let mut sub_edges = Vec::new();
    for draft in &drafts {
        let connector_entity = draft.entity();
        let resolved_entity;
        let mut hint_exclusions: Option<Vec<String>> = None;
        let connector_entity = if connector_entity.primary_key_properties.is_empty()
            && !connector_entity.additional_key_properties.is_empty()
        {
            let mut entity = connector_entity.clone();
            entity.primary_key_properties = entity.additional_key_properties.remove(0);
            resolved_entity = entity;
            &resolved_entity
        } else if connector_entity.primary_key_properties.is_empty() {
            match inferred.get(&(
                connector_entity.source.clone(),
                connector_entity.entity_type.clone(),
            )) {
                Some(schema) => {
                    let mut e = connector_entity.clone();
                    e.primary_key_properties = schema.primary_key_properties.clone();
                    // Inferred volatility contributes to the incoming change fingerprint.
                    if !schema.volatile_property_hints.is_empty() {
                        let mut merged = snapshot_input.ignore_change_properties.clone();
                        for hint in &schema.volatile_property_hints {
                            if !merged.contains(hint) {
                                merged.push(hint.clone());
                            }
                        }
                        hint_exclusions = Some(merged);
                    }
                    resolved_entity = e;
                    &resolved_entity
                }
                None => connector_entity, // rejected below
            }
        } else {
            connector_entity
        };
        let default_config = EntityTypeConfig::default();
        let type_config = Some(
            ctx.entity_type_configs
                .get(&connector_entity.entity_type)
                .unwrap_or(&default_config),
        );
        match connector_entity_to_node(
            connector_entity,
            namespace,
            org_id,
            captured_at,
            type_config,
            hint_exclusions
                .as_deref()
                .unwrap_or(&snapshot_input.ignore_change_properties),
        ) {
            Ok(mut node) => {
                let mut effective = type_config.map(|c| c.hash_exclusions()).unwrap_or_default();
                effective.extend(
                    hint_exclusions
                        .as_deref()
                        .unwrap_or(&snapshot_input.ignore_change_properties)
                        .iter()
                        .cloned(),
                );
                version_exclusions.insert(node.uuid, effective);
                node.first_seen_snapshot_id = Some(draft.snapshot_id());
                node.last_seen_snapshot_id = Some(draft.snapshot_id());
                node.last_seen_at = Some(draft.captured_at());
                node.sync_generation = draft.sync_generation();

                // Source says it's gone — route to deletion, never create.
                if draft.is_deleted() {
                    source_deleted.push(node);
                    continue;
                }

                if let Some(type_config) = type_config {
                    let (sub_entities, edges) = super::child_extract::extract_children(
                        &mut node,
                        type_config,
                        org_id,
                        &connector_entity.raw_properties,
                    )
                    .map_err(|e| StageError::StepFailed {
                        stage: "structured_preparation".into(),
                        step: "child_extraction".into(),
                        cause: e.to_string(),
                        retriable: false,
                    })?;
                    super::property_normalizer::normalize_entity(
                        &mut node,
                        type_config,
                        hint_exclusions
                            .as_deref()
                            .unwrap_or(&snapshot_input.ignore_change_properties),
                    )
                    .map_err(|e| {
                        super::extraction_support::invalid("property_normalization", &e.to_string())
                    })?;
                    for mut sub in sub_entities {
                        {
                            let child_config = ctx
                                .entity_type_configs
                                .get(&sub.entity_type)
                                .unwrap_or(&default_config);
                            super::property_normalizer::normalize_entity(
                                &mut sub,
                                child_config,
                                &snapshot_input.ignore_change_properties,
                            )
                            .map_err(|e| {
                                super::extraction_support::invalid(
                                    "property_normalization",
                                    &e.to_string(),
                                )
                            })?;
                        }

                        sub.first_seen_snapshot_id = Some(snapshot_uuid);
                        sub.last_seen_snapshot_id = Some(snapshot_uuid);
                        sub.last_seen_at = Some(captured_at);
                        sub.sync_generation = snapshot_input.sync_generation;
                        let mut effective = ctx
                            .entity_type_configs
                            .get(&sub.entity_type)
                            .map(|c| c.hash_exclusions())
                            .unwrap_or_default();
                        effective.extend(snapshot_input.ignore_change_properties.iter().cloned());
                        version_exclusions.insert(sub.uuid, effective);
                        entities.push(sub);
                    }
                    // Sub-entity edges are carried into the edge phase.
                    sub_edges.extend(edges);
                }

                entities.push(node);
            }
            // An invalid entity fails the whole snapshot; the runner decides
            // whether other snapshots continue. Dropping it here would report
            // a complete observation that omits a member.
            Err(e) => return Err(e),
        }
    }

    tracing::debug!(
        entities = entities.len(),
        source_deleted = source_deleted.len(),
        sub_edges = sub_edges.len(),
        snapshot = %snapshot_uuid,
        "extracted entities"
    );

    for entity in &entities {
        crate::profiles::entity(ctx, &entity.source, entity)?;
    }
    for entity in &source_deleted {
        crate::profiles::entity(ctx, &entity.source, entity)?;
    }
    Ok(StageOutput::NodeExtraction(NodeExtractionOutput {
        raw_text_drafts: Default::default(),
        relationship_changes: Arc::new(std::collections::HashMap::from([(
            snapshot_uuid,
            snapshot_input.relationship_changes.clone(),
        )])),
        version_exclusions: Arc::new(version_exclusions),
        text_observation_ids: Default::default(),
        fk_exclusions: Arc::new(std::collections::HashMap::from([(
            snapshot_uuid,
            snapshot_input.exclude_fk_properties.clone(),
        )])),
        schemas: Arc::new(std::collections::HashMap::from([(snapshot_uuid, schemas)])),
        history: Arc::new(std::collections::HashMap::from([(snapshot_uuid, history)])),
        snapshot_nodes: Arc::new(vec![snapshot_node]),
        entities_by_snapshot: Arc::new(vec![(snapshot_uuid, entities)]),
        source_deleted: Arc::new(source_deleted),
        sub_edges: Arc::new(sub_edges),
        // Structured extraction either succeeds for every entity or fails
        // the snapshot; it is never partially complete.
        incomplete_extractions: Arc::new(vec![]),
    }))
}

/// Build a resolution candidate from a declared entity with effective keys.
pub fn connector_entity_to_node(
    entity: &ConnectorEntity,
    namespace: &str,
    org_id: &str,
    captured_at: DateTime<Utc>,
    type_config: Option<&EntityTypeConfig>,
    snapshot_ignore_changes: &[String],
) -> Result<EntityNode, StageError> {
    let validation_err = |cause: String| StageError::StepFailed {
        stage: "validate".into(),
        step: "validate_entity".into(),
        cause: format!(
            "entity `{}` of type `{}`: {cause}",
            entity.name, entity.entity_type
        ),
        retriable: false,
    };

    if entity.primary_key_properties.is_empty() {
        return Err(validation_err(
            "structured observations require resolved identifying keys".into(),
        ));
    }

    // Flatten raw_properties into PropertyValue map, normalizing floats
    let all_properties = PropertyValue::flatten_source(
        &entity.raw_properties,
        type_config
            .map(|c| c.force_json_properties.as_slice())
            .unwrap_or(&[]),
    )
    .map_err(&validation_err)?;

    // Every declared key must be present: hashing a partial key merges unrelated entities.
    let mut pk_values: Vec<(String, PropertyValue)> =
        Vec::with_capacity(entity.primary_key_properties.len());
    for pk in &entity.primary_key_properties {
        let rendered = if pk == "name" {
            PropertyValue::String(entity.name.clone())
        } else {
            let value = all_properties.get(pk).ok_or_else(|| {
                validation_err(format!(
                    "primary key property '{pk}' is declared but absent from the entity's \
                     properties; identity cannot be computed over a partial key"
                ))
            })?;
            value.as_identity_key().ok_or_else(|| {
                validation_err(format!(
                    "primary key property '{pk}' is not a scalar identity value ({value:?}); \
                     null, lists, json, and blobs cannot serve as identity keys"
                ))
            })?;
            value.clone()
        };
        pk_values.push((pk.clone(), rendered));
    }

    for group in &entity.additional_key_properties {
        if group.is_empty() {
            return Err(validation_err("empty additional key group".into()));
        }
        let mut seen = std::collections::HashSet::new();
        for key in group {
            if key.trim().is_empty()
                || !seen.insert(key)
                || (key != "name"
                    && all_properties
                        .get(key)
                        .and_then(PropertyValue::as_identity_key)
                        .is_none())
            {
                return Err(validation_err(
                    "additional keys must be complete, distinct scalar properties".into(),
                ));
            }
        }
    }
    if entity
        .primary_key_properties
        .iter()
        .chain(entity.additional_key_properties.iter().flatten())
        .any(|key| key == "name")
        && all_properties
            .get("name")
            .is_some_and(|v| v != &PropertyValue::String(entity.name.clone()))
    {
        return Err(validation_err(
            "name property conflicts with the declared name key".into(),
        ));
    }
    let ns = entity.namespace.as_deref().unwrap_or(namespace);
    let identity_hash = IdentityHash::compute_values(org_id, ns, &entity.entity_type, &pk_values)
        .map_err(validation_err)?;

    // Structural hash: volatile properties are auto-excluded, with
    // always_version_on_change precedence, plus per-snapshot
    // ignore_change_properties (input ∪ type-config).
    let mut exclusions = type_config.map(|c| c.hash_exclusions()).unwrap_or_default();
    for p in snapshot_ignore_changes {
        if !exclusions.contains(p) {
            exclusions.push(p.clone());
        }
    }
    let structural_hash = compute_structural_hash(&all_properties, &exclusions);

    let uuid = Uuid::new_v4();
    let chain_id = Uuid::new_v4();

    let node = EntityNode {
        labels: entity.labels.clone(),
        inherited_labels: Vec::new(),
        uuid,
        chain_id,
        org_id: org_id.to_string(),
        namespace: ns.to_string(),
        entity_type: entity.entity_type.clone(),
        name: entity.name.clone(),
        all_properties,
        primary_key_properties: entity.primary_key_properties.clone(),
        additional_key_properties: entity.additional_key_properties.clone(),
        identity_hash,
        lifecycle: entity.lifecycle,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        embedding: None,
        valid_from: captured_at,
        valid_to: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        source: entity.source.clone(),
        extracted_by: "direct".to_string(),
        resolved_by: None,
        first_seen_snapshot_id: None,
        last_seen_snapshot_id: None,
        last_seen_at: None,
        sync_generation: None,
        tags: entity.tags.clone(),
        summary: None,
        structural_hash,
        needs_llm_review: false,
        collections: Vec::new(),
    };

    // Core invariants: non-empty org/name/type, valid PK list.
    node.validate().map_err(|e| validation_err(e.to_string()))?;

    Ok(node)
}

pub(crate) fn processing_version() -> String {
    kg_core::traits::stage::processing_version(
        "structured-preparation-observation-clock-v3",
        &[super::schema_inference::SYSTEM_PROMPT],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::enums::EntityLifecycle;

    fn make_connector_entity() -> ConnectorEntity {
        ConnectorEntity {
            additional_key_properties: vec![],
            labels: Vec::new(),
            entity_type: "Deployment".into(),
            name: "payment-api".into(),
            primary_key_properties: vec!["name".into(), "namespace".into()],
            raw_properties: serde_json::json!({
                "namespace": "default",
                "replicas": 3,
                "image": "payment-api:1.4.2",
                "ready": true
            }),
            namespace: Some("production".into()),
            lifecycle: EntityLifecycle::Active,
            tags: indexmap::IndexMap::new(),
            source: "k8s-connector".into(),
            org_id: "org-1".into(),
        }
    }

    fn convert(ce: &ConnectorEntity) -> Result<EntityNode, StageError> {
        connector_entity_to_node(ce, "production", "org-1", Utc::now(), None, &[])
    }

    #[test]
    fn converts_connector_entity_to_node() {
        let ce = make_connector_entity();
        let node = convert(&ce).unwrap();

        assert_eq!(node.entity_type, "Deployment");
        assert_eq!(node.name, "payment-api");
        assert_eq!(node.namespace, "production");
        assert_eq!(node.org_id, "org-1");

        assert_eq!(node.version, 1);
        assert!(node.is_latest);
        assert!(node.deleted_at.is_none());

        // Properties flattened
        assert!(node.all_properties.contains_key("replicas"));
        assert!(node.all_properties.contains_key("image"));
    }

    #[test]
    fn source_is_provenance_and_namespace_still_scopes_identity() {
        let first = make_connector_entity();
        let mut second = first.clone();
        second.source = "another-connector".into();
        let first_node = convert(&first).unwrap();
        let second_node = convert(&second).unwrap();
        assert_eq!(first_node.identity_hash, second_node.identity_hash);
        assert_eq!(first_node.source, "k8s-connector");
        assert_eq!(second_node.source, "another-connector");

        second.namespace = Some("staging".into());
        assert_ne!(
            first_node.identity_hash,
            convert(&second).unwrap().identity_hash
        );
    }

    #[test]
    fn rejects_empty_primary_keys() {
        let mut ce = make_connector_entity();
        ce.primary_key_properties = vec![];
        assert!(convert(&ce).is_err());
    }

    #[test]
    fn rejects_primary_key_absent_from_properties() {
        // `pk = [name, namespace]` but the payload omits `namespace`: hashing
        // over `name` alone would collide with any other name-only entity, so
        // this must be a loud rejection, not a silent partial-key hash.
        let mut ce = make_connector_entity();
        ce.primary_key_properties = vec!["name".into(), "namespace".into()];
        ce.raw_properties = serde_json::json!({ "replicas": 3 }); // no `namespace`
        let err = convert(&ce).expect_err("absent PK must be rejected");
        assert!(
            format!("{err}").contains("namespace"),
            "error names the missing PK: {err}"
        );
    }

    #[test]
    fn rejects_non_scalar_primary_key() {
        // A PK pointing at a list/json/blob/null has a lossy or colliding
        // Display form (`<blob>`, `null`, debug text) — reject it.
        let mut ce = make_connector_entity();
        ce.primary_key_properties = vec!["name".into(), "tags".into()];
        ce.raw_properties = serde_json::json!({ "tags": ["a", "b"], "namespace": "default" });
        assert!(
            convert(&ce).is_err(),
            "a list-valued primary key must be rejected as a non-scalar identity"
        );
    }

    #[test]
    fn rejects_duplicate_and_blank_primary_keys() {
        let mut ce = make_connector_entity();
        ce.primary_key_properties = vec!["name".into(), "name".into()];
        assert!(convert(&ce).is_err());

        ce.primary_key_properties = vec!["  ".into()];
        assert!(convert(&ce).is_err());
    }

    #[test]
    fn valid_from_is_snapshot_captured_at_not_now() {
        let ce = make_connector_entity();
        let captured = Utc::now() - chrono::Duration::days(3);
        let node = connector_entity_to_node(&ce, "prod", "org-1", captured, None, &[]).unwrap();
        assert_eq!(node.valid_from, captured, "validity starts at source time");
    }

    #[test]
    fn nan_and_negative_zero_normalized() {
        // NaN → Null, -0.0 → 0.0, so floats can't cause perpetual diffs
        let mut ce = make_connector_entity();
        ce.raw_properties = serde_json::json!({ "namespace": "default", "zero": -0.0 });
        let node = convert(&ce).unwrap();
        assert_eq!(
            node.all_properties.get("zero"),
            Some(&PropertyValue::Float(0.0))
        );
    }

    #[test]
    fn volatile_properties_excluded_from_structural_hash() {
        // Volatile churn must never change the structural hash
        let config = EntityTypeConfig {
            volatile_properties: vec!["ready".into()],
            ..Default::default()
        };
        let mut ce1 = make_connector_entity();
        ce1.raw_properties =
            serde_json::json!({ "namespace": "default", "replicas": 3, "ready": true });
        let mut ce2 = make_connector_entity();
        ce2.raw_properties =
            serde_json::json!({ "namespace": "default", "replicas": 3, "ready": false });

        let now = Utc::now();
        let n1 = connector_entity_to_node(&ce1, "prod", "org-1", now, Some(&config), &[]).unwrap();
        let n2 = connector_entity_to_node(&ce2, "prod", "org-1", now, Some(&config), &[]).unwrap();
        assert_eq!(
            n1.structural_hash, n2.structural_hash,
            "volatile-only differences must not change the hash"
        );
    }

    #[test]
    fn always_version_beats_explicit_and_volatile_exclusions() {
        // A property in both lists IS versioned (participates in hash)
        let config = EntityTypeConfig {
            volatile_properties: vec!["ready".into()],
            always_version_on_change: vec!["ready".into()],
            exclude_from_hash: vec!["ready".into()],
            ..Default::default()
        };
        let mut ce1 = make_connector_entity();
        ce1.raw_properties = serde_json::json!({ "namespace": "default", "ready": true });
        let mut ce2 = make_connector_entity();
        ce2.raw_properties = serde_json::json!({ "namespace": "default", "ready": false });

        let now = Utc::now();
        let n1 = connector_entity_to_node(&ce1, "prod", "org-1", now, Some(&config), &[]).unwrap();
        let n2 = connector_entity_to_node(&ce2, "prod", "org-1", now, Some(&config), &[]).unwrap();
        assert_ne!(
            n1.structural_hash, n2.structural_hash,
            "forced-version properties must affect the hash"
        );
    }

    #[test]
    fn normalized_hash_respects_type_rules_and_snapshot_exclusions() {
        let config = EntityTypeConfig {
            volatile_properties: vec!["status".into(), "heartbeat".into()],
            exclude_from_hash: vec!["status".into(), "noise".into()],
            always_version_on_change: vec!["status".into(), "discarded".into()],
            drop_properties: vec!["discarded".into()],
            force_json_properties: vec!["status".into()],
            ..Default::default()
        };
        let normalized =
            |status: &str, heartbeat: i64, noise: i64, discarded: i64, ignored: &[String]| {
                let mut input = make_connector_entity();
                input.raw_properties = serde_json::json!({
                    "namespace": "default", "status": status, "heartbeat": heartbeat,
                    "noise": noise, "discarded": discarded
                });
                let mut node = connector_entity_to_node(
                    &input,
                    "prod",
                    "org-1",
                    Utc::now(),
                    Some(&config),
                    ignored,
                )
                .unwrap();
                super::super::property_normalizer::normalize_entity(&mut node, &config, ignored)
                    .unwrap();
                node
            };
        let original = normalized("pending", 1, 1, 1, &[]);
        let forced = normalized("ready", 1, 1, 1, &[]);
        assert_ne!(original.structural_hash, forced.structural_hash);
        assert_eq!(original.identity_hash, forced.identity_hash);
        assert!(matches!(
            forced.all_properties["status"],
            PropertyValue::Json(_)
        ));
        assert!(!forced.all_properties.contains_key("discarded"));
        assert_eq!(
            original.structural_hash,
            normalized("pending", 2, 2, 2, &[]).structural_hash
        );

        let ignored = vec!["status".into()];
        assert_eq!(
            normalized("pending", 1, 1, 1, &ignored).structural_hash,
            normalized("ready", 1, 1, 1, &ignored).structural_hash,
            "snapshot exclusions apply after type-level rules",
        );
    }

    #[test]
    fn identity_hash_is_deterministic() {
        let ce = make_connector_entity();
        let n1 = convert(&ce).unwrap();
        let n2 = convert(&ce).unwrap();

        assert_eq!(n1.identity_hash, n2.identity_hash);
        // But UUIDs differ (new_v4 each time)
        assert_ne!(n1.uuid, n2.uuid);
    }

    #[test]
    fn flatten_json_properties() {
        let json = serde_json::json!({
            "name": "test",
            "replicas": 3,
            "ready": true,
            "labels": {"app": "api", "env": "prod"},
            "containers": [{"image": "nginx"}]
        });

        let props = PropertyValue::flatten_source(&json, &[]).unwrap();

        assert_eq!(props.get("name").unwrap().to_string(), "test");
        assert_eq!(props.get("replicas").unwrap().to_string(), "3");
        assert_eq!(props.get("ready").unwrap().to_string(), "true");
        // Flat nested object
        assert_eq!(props.get("labels.app").unwrap().to_string(), "api");
        // Array stored as Json
        assert!(matches!(
            props.get("containers"),
            Some(PropertyValue::Json(_))
        ));
    }

    #[test]
    fn creates_snapshot_node() {
        let input = kg_core::models::SnapshotInput {
            relationship_changes: Default::default(),
            saga: None,
            previous_snapshot_uuids: vec![],
            labels: Vec::new(),
            tags: Default::default(),
            namespace: "prod".into(),
            name: "k8s-sync-001".into(),
            source_description: None,
            data_type: kg_core::models::SnapshotDataType::Entities,
            snapshot_kind: kg_core::models::SnapshotKind::Full,
            sync_generation: Some(7),
            complete: true,
            org_id: None,
            source: "k8s-connector".into(),
            entities: vec![],
            content: None,
            entity_types: None,
            edge_types: None,
            edge_type_map: None,
            exclude_fk_properties: vec![],
            ignore_change_properties: vec![],
            captured_at: None,
            collection: None,
        };

        let prepared = kg_core::runtime::stage_output::PreparedSnapshotInput::new(
            kg_core::models::ValidatedSnapshotInput::new(input, "org-1").unwrap(),
            "org-1",
        )
        .unwrap();
        let node = prepared.snapshot();
        assert_eq!(node.namespace, "prod");
        assert_eq!(node.name, "k8s-sync-001");
        assert_eq!(node.org_id, "org-1");
        assert_eq!(node.sync_generation, Some(7));
        assert!(node.complete);
    }
    #[test]
    fn typed_keys_are_distinct_and_oversized_integers_are_not_rounded() {
        let mut input = make_connector_entity();
        input.primary_key_properties = vec!["id".into()];
        input.additional_key_properties = vec![vec!["external".into()]];
        input.raw_properties =
            serde_json::json!({"id":123,"external":"stable","large":18446744073709551615u64});
        let numeric =
            connector_entity_to_node(&input, "prod", "org", Utc::now(), None, &[]).unwrap();
        input.raw_properties["id"] = serde_json::json!("123");
        let text = connector_entity_to_node(&input, "prod", "org", Utc::now(), None, &[]).unwrap();
        assert_ne!(numeric.identity_hash, text.identity_hash);
        assert_eq!(
            numeric.additional_identity_hashes().unwrap(),
            text.additional_identity_hashes().unwrap()
        );
        assert_eq!(
            numeric.all_properties["large"],
            PropertyValue::Json("18446744073709551615".into())
        );
        input.primary_key_properties = vec!["large".into()];
        assert!(connector_entity_to_node(&input, "prod", "org", Utc::now(), None, &[]).is_err());
    }
}
