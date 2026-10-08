use std::fmt;

use uuid::Uuid;

#[cfg(test)]
use chrono::Utc;
use kg_core::entity_type_config::{EntityTypeConfig, SubEntityRule};
use kg_core::enums::{EdgeDirection, EntityLifecycle};
use kg_core::identity::structural_hash::compute_structural_hash;
use kg_core::identity::IdentityHash;
use kg_core::models::edges::EntityEdge;
use kg_core::models::EntityNode;
use kg_core::models::PropertyValue;

/// A configured child collection that cannot be extracted. The parent's own
/// facts are intact, but its children are unknown, so the snapshot must
/// fail rather than commit the parent without them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildExtractionError {
    /// Name of the entity whose collection failed.
    pub parent: String,
    /// The configured property holding the collection.
    pub source_path: String,
    /// What was wrong with it.
    pub reason: String,
}

impl fmt::Display for ChildExtractionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "child collection `{}` of `{}`: {}",
            self.source_path, self.parent, self.reason
        )
    }
}

impl std::error::Error for ChildExtractionError {}

/// Read child collections before any parent cleanup. Rules all see the same input.
#[tracing::instrument(name = "child_extraction", skip_all)]
pub fn extract_children(
    entity: &mut EntityNode,
    config: &EntityTypeConfig,
    org_id: &str,
    raw: &serde_json::Value,
) -> Result<(Vec<EntityNode>, Vec<EntityEdge>), ChildExtractionError> {
    super::property_normalizer::validate_rules(entity, config)?;
    let mut original = entity.clone();
    original.all_properties = PropertyValue::flatten_source(
        raw,
        &config
            .sub_entity_rules
            .iter()
            .map(|r| r.source_path.clone())
            .collect::<Vec<_>>(),
    )
    .map_err(|reason| ChildExtractionError {
        parent: entity.name.clone(),
        source_path: String::new(),
        reason,
    })?;
    let mut children = Vec::new();
    let mut edges = Vec::new();
    let mut promotions = indexmap::IndexMap::new();
    for rule in &config.sub_entity_rules {
        let mut working = original.clone();
        let (nodes, links) = extract_sub_entities(&mut working, rule, org_id)?;
        for key in rule
            .promote_to_parent
            .iter()
            .map(|p| &p.target_field)
            .chain(rule.raw_blob_field.iter().filter(|_| rule.keep_raw_blob))
        {
            if let Some(value) = working.all_properties.get(key) {
                if promotions.insert(key.clone(), value.clone()).is_some() {
                    return Err(ChildExtractionError {
                        parent: entity.name.clone(),
                        source_path: key.clone(),
                        reason: "multiple rules write the same property".into(),
                    });
                }
            }
        }
        children.extend(nodes);
        edges.extend(links);
    }
    entity.all_properties.extend(promotions);
    tracing::debug!(
        children = children.len(),
        edges = edges.len(),
        "child extraction complete"
    );
    Ok((children, edges))
}

/// Extract sub-entities from a nested array property.
///
/// Example: Pod `spec.containers` → separate ContainerImage nodes + USES_IMAGE edges.
/// An absent collection has no children; a present one must be a JSON array
/// of objects that each carry the rule's full identity.
fn extract_sub_entities(
    parent: &mut EntityNode,
    rule: &SubEntityRule,
    org_id: &str,
) -> Result<(Vec<EntityNode>, Vec<EntityEdge>), ChildExtractionError> {
    // Children live in the parent's namespace: a child edge never crosses one.
    let namespace = parent.namespace.as_str();
    let mut sub_entities = Vec::new();
    let mut edges = Vec::new();
    let fail = |reason: String| ChildExtractionError {
        parent: parent.name.clone(),
        source_path: rule.source_path.clone(),
        reason,
    };
    if rule.pk_properties.is_empty() {
        return Err(fail("the rule declares no identity properties".into()));
    }

    let items = match parent.all_properties.get(&rule.source_path) {
        None | Some(PropertyValue::Null) => return Ok((sub_entities, edges)),
        Some(PropertyValue::Json(raw)) => match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(serde_json::Value::Array(items)) => items,
            Ok(_) => return Err(fail("holds JSON that is not an array".into())),
            Err(error) => return Err(fail(format!("holds invalid JSON: {error}"))),
        },
        Some(other) => {
            return Err(fail(format!(
                "is a {} value instead of a JSON array",
                property_kind(other)
            )))
        }
    };

    let mut promoted_lists: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for (index, item) in items.iter().enumerate() {
        let Some(_) = item.as_object() else {
            return Err(fail(format!("item {index} is not an object")));
        };

        let decoded = PropertyValue::flatten_source(item, &rule.blob_properties).map_err(&fail)?;
        let mut props = indexmap::IndexMap::new();
        for key in &rule.flat_properties {
            if let Some(value) = decoded.get(key) {
                props.insert(key.clone(), value.clone());
            }
        }
        for key in &rule.blob_properties {
            if let Some(value) = decoded.get(key) {
                let json = value.to_source().map_err(&fail)?;
                props.insert(key.clone(), PropertyValue::Json(json.to_string()));
            }
        }

        // The full declared identity, or nothing: a partial key would
        // collide distinct children into one chain.
        let mut pk_values: Vec<(String, PropertyValue)> =
            Vec::with_capacity(rule.pk_properties.len());
        for pk in &rule.pk_properties {
            match props.get(pk).filter(|v| v.as_identity_key().is_some()) {
                Some(v) => pk_values.push((pk.clone(), v.clone())),
                None => {
                    return Err(fail(format!(
                        "item {index} has no scalar identity value for `{pk}`"
                    )))
                }
            }
        }

        let name = if rule.pk_properties.iter().any(|key| key == "name") {
            match props.get("name") {
                Some(PropertyValue::String(value)) => value.clone(),
                _ => return Err(fail("a declared name key must be a string".into())),
            }
        } else {
            pk_values
                .first()
                .map(|(_, value)| value.to_string())
                .unwrap_or_default()
        };
        let identity_hash =
            IdentityHash::compute_values(org_id, namespace, &rule.target_entity_type, &pk_values)
                .map_err(fail)?;
        let structural_hash = compute_structural_hash(&props, &[]);

        let sub_uuid = Uuid::new_v4();
        let sub_chain = Uuid::new_v4();

        let sub_entity = EntityNode {
            labels: Vec::new(),
            inherited_labels: Vec::new(),
            uuid: sub_uuid,
            chain_id: sub_chain,
            org_id: org_id.to_string(),
            namespace: namespace.to_string(),
            entity_type: rule.target_entity_type.clone(),
            name,
            all_properties: props,
            primary_key_properties: rule.pk_properties.clone(),
            additional_key_properties: vec![],
            identity_hash,
            lifecycle: EntityLifecycle::Active,
            version: 1,
            is_latest: true,
            previous_version_uuid: None,
            embedding: None,
            valid_from: parent.valid_from,
            valid_to: None,
            deleted_at: None,
            deleted_by: None,
            deletion_reason: None,
            source: parent.source.clone(),
            extracted_by: "sub_entity_rule".to_string(),
            resolved_by: None,
            first_seen_snapshot_id: parent.first_seen_snapshot_id,
            last_seen_snapshot_id: parent.last_seen_snapshot_id,
            last_seen_at: parent.last_seen_at,
            sync_generation: parent.sync_generation,
            tags: indexmap::IndexMap::new(),
            summary: None,
            structural_hash,
            needs_llm_review: false,
            collections: Vec::new(),
        };

        sub_entity
            .validate()
            .map_err(|error| fail(error.to_string()))?;

        for promoted in &rule.promote_to_parent {
            if let Some(val) = sub_entity.all_properties.get(&promoted.source_field) {
                promoted_lists
                    .entry(promoted.target_field.clone())
                    .or_default()
                    .push(val.to_string());
            }
        }

        // The configured direction: outgoing is parent → child, incoming is
        // child → parent, both is one edge each way.
        let directions: &[(Uuid, Uuid)] = match rule.edge_direction {
            EdgeDirection::Outgoing => &[(parent.chain_id, sub_chain)],
            EdgeDirection::Incoming => &[(sub_chain, parent.chain_id)],
            EdgeDirection::Both => &[(parent.chain_id, sub_chain), (sub_chain, parent.chain_id)],
        };
        for (source_chain_id, target_chain_id) in directions {
            let (from, to) = if *source_chain_id == parent.chain_id {
                (
                    format!("{} {}", parent.entity_type, parent.name),
                    format!("{} {}", sub_entity.entity_type, sub_entity.name),
                )
            } else {
                (
                    format!("{} {}", sub_entity.entity_type, sub_entity.name),
                    format!("{} {}", parent.entity_type, parent.name),
                )
            };
            edges.push(EntityEdge {
                time_evidence: None,
                chain_id: Uuid::new_v4(),
                identity_hash: None,
                cardinality_key: None,
                producer_source: parent.source.clone(),
                origin: kg_core::models::edges::RelationshipOrigin::Declared,
                uuid: Uuid::new_v4(),
                org_id: org_id.to_string(),
                source_chain_id: *source_chain_id,
                target_chain_id: *target_chain_id,
                name: rule.edge_name.clone(),
                identity_name: None,
                description: format!("{from} {} {to}", rule.edge_name),
                all_properties: indexmap::IndexMap::from([
                    ("child_owned".into(), PropertyValue::Bool(rule.owned)),
                    (
                        "child_parent_endpoint".into(),
                        PropertyValue::String(
                            if *source_chain_id == parent.chain_id {
                                "source"
                            } else {
                                "target"
                            }
                            .into(),
                        ),
                    ),
                ]),
                discovered_by: Some("sub_entity_rule".into()),
                resolved_by: None,
                source_property: Some(rule.source_path.clone()),
                target_identity_field: None,
                reference_evidence: None,
                // Declared in the source payload itself: top trust tier.
                confidence: kg_core::models::edges::CONFIDENCE_DECLARED,
                justification: None,
                first_seen_snapshot_id: parent.first_seen_snapshot_id,
                last_seen_snapshot_id: parent.last_seen_snapshot_id,
                last_seen_at: parent.last_seen_at,
                sync_generation: parent.sync_generation,
                valid_from: parent.valid_from,
                cancelled_at: None,
                cancellation_snapshot_id: None,
                cancellation_context: None,
                valid_to: None,
                version: 1,
                is_latest: true,
                previous_version_uuid: None,
                deleted_at: None,
                deleted_by: None,
                deletion_reason: None,
                created_at: parent.last_seen_at.unwrap_or(parent.valid_from),
            });
        }

        sub_entities.push(sub_entity);
    }

    for (field, values) in promoted_lists {
        parent
            .all_properties
            .insert(field, PropertyValue::StringList(values));
    }

    if rule.keep_raw_blob {
        if let Some(field) = &rule.raw_blob_field {
            if let Some(raw) = parent.all_properties.get(&rule.source_path) {
                parent.all_properties.insert(field.clone(), raw.clone());
            }
        }
    }

    Ok((sub_entities, edges))
}

fn property_kind(value: &PropertyValue) -> &'static str {
    match value {
        PropertyValue::String(_) => "string",
        PropertyValue::Integer(_) => "integer",
        PropertyValue::Float(_) => "float",
        PropertyValue::Bool(_) => "boolean",
        PropertyValue::Timestamp(_) => "timestamp",
        PropertyValue::Duration(_) => "duration",
        PropertyValue::UuidRef(_) => "uuid",
        PropertyValue::StringList(_) => "string list",
        PropertyValue::IntegerList(_) => "integer list",
        PropertyValue::FloatList(_) => "float list",
        PropertyValue::Json(_) => "json",
        PropertyValue::Blob(_) => "blob",
        PropertyValue::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::entity_type_config::{PromotedList, SubEntityRule};
    use kg_core::enums::EdgeDirection;

    fn extract_children(
        entity: &mut EntityNode,
        config: &EntityTypeConfig,
        org: &str,
    ) -> Result<(Vec<EntityNode>, Vec<EntityEdge>), ChildExtractionError> {
        let raw = entity
            .all_properties
            .iter()
            .map(|(key, value)| value.to_source().map(|v| (key.clone(), v)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map_err(|reason| ChildExtractionError {
                parent: entity.name.clone(),
                source_path: String::new(),
                reason,
            })?;
        super::extract_children(entity, config, org, &serde_json::Value::Object(raw))
    }

    fn make_pod_entity() -> EntityNode {
        let mut props = indexmap::IndexMap::new();
        props.insert("namespace".into(), PropertyValue::String("default".into()));
        props.insert(
            "containers".into(),
            PropertyValue::Json(
                serde_json::json!([
                    {"name": "app", "image": "payment-api:1.4"},
                    {"name": "sidecar", "image": "envoy:1.2"}
                ])
                .to_string(),
            ),
        );

        EntityNode {
            labels: Vec::new(),
            inherited_labels: Vec::new(),
            uuid: Uuid::new_v4(),
            chain_id: Uuid::new_v4(),
            org_id: "org".into(),
            namespace: "prod".into(),
            entity_type: "Pod".into(),
            name: "payment-pod-abc".into(),
            all_properties: props,
            primary_key_properties: vec!["name".into()],
            additional_key_properties: vec![],
            identity_hash: IdentityHash::compute(
                "org",
                "prod",
                "Pod",
                &[("name", "payment-pod-abc")],
            ),
            lifecycle: EntityLifecycle::Active,
            version: 1,
            is_latest: true,
            previous_version_uuid: None,
            embedding: None,
            valid_from: Utc::now(),
            valid_to: None,
            deleted_at: None,
            source: "k8s".into(),
            extracted_by: "direct".into(),
            resolved_by: None,
            deleted_by: None,
            deletion_reason: None,
            first_seen_snapshot_id: None,
            last_seen_snapshot_id: None,
            last_seen_at: None,
            sync_generation: None,
            tags: indexmap::IndexMap::new(),
            summary: None,
            structural_hash: 0,
            needs_llm_review: false,
            collections: Vec::new(),
        }
    }

    fn container_config() -> EntityTypeConfig {
        EntityTypeConfig {
            sub_entity_rules: vec![SubEntityRule {
                owned: false,
                source_path: "containers".into(),
                target_entity_type: "ContainerImage".into(),
                pk_properties: vec!["image".into()],
                flat_properties: vec!["name".into(), "image".into()],
                blob_properties: vec![],
                edge_name: "USES_IMAGE".into(),
                edge_direction: EdgeDirection::Outgoing,
                promote_to_parent: vec![PromotedList {
                    source_field: "image".into(),
                    target_field: "container_images".into(),
                }],
                keep_raw_blob: false,
                raw_blob_field: None,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn child_identity_obeys_the_same_name_convention_as_structured_input() {
        let mut pod = make_pod_entity();
        let mut config = container_config();
        config.sub_entity_rules[0].pk_properties = vec!["image".into(), "name".into()];
        let (children, _) = extract_children(&mut pod, &config, "org").unwrap();
        assert_eq!(children[0].name, "app");
        let input = kg_core::models::ConnectorEntity {
            org_id: "org".into(),
            entity_type: children[0].entity_type.clone(),
            name: "app".into(),
            primary_key_properties: vec!["image".into(), "name".into()],
            additional_key_properties: vec![],
            raw_properties: serde_json::json!({"name":"app","image":"payment-api:1.4"}),
            namespace: None,
            source: "k8s".into(),
            lifecycle: EntityLifecycle::Active,
            labels: vec![],
            tags: Default::default(),
        };
        let prepared = super::super::structured_prepare::connector_entity_to_node(
            &input,
            "prod",
            "org",
            pod.valid_from,
            None,
            &[],
        )
        .unwrap();
        assert_eq!(children[0].identity_hash, prepared.identity_hash);
        config.sub_entity_rules[0]
            .pk_properties
            .push("image".into());
        assert!(extract_children(&mut pod, &config, "org").is_err());
        config.sub_entity_rules[0].pk_properties = vec!["image".into()];
        config.sub_entity_rules[0].target_entity_type.clear();
        assert!(extract_children(&mut pod, &config, "org").is_err());
    }

    #[test]
    fn children_are_read_before_parent_cleanup() {
        let mut pod = make_pod_entity();
        let mut config = container_config();
        config.drop_properties.push("containers".into());
        let (children, edges) = extract_children(&mut pod, &config, "org").unwrap();
        super::super::property_normalizer::normalize_entity(&mut pod, &config, &[]).unwrap();
        assert_eq!((children.len(), edges.len()), (2, 2));
        assert!(!pod.all_properties.contains_key("containers"));
    }

    #[test]
    fn child_null_and_nested_paths_use_shared_decoding() {
        let mut pod = make_pod_entity();
        pod.all_properties.insert(
            "containers".into(),
            PropertyValue::Json(
                serde_json::json!([{"spec":{"id":1,"optional":null,"empty":{}},"name":"child"}])
                    .to_string(),
            ),
        );
        let mut config = container_config();
        config.sub_entity_rules[0].pk_properties = vec!["spec.id".into()];
        config.sub_entity_rules[0].flat_properties = vec![
            "spec.id".into(),
            "spec.optional".into(),
            "spec.empty".into(),
        ];
        let (children, _) = extract_children(&mut pod, &config, "org").unwrap();
        assert_eq!(
            children[0].all_properties["spec.optional"],
            PropertyValue::Null
        );
        assert_eq!(
            children[0].all_properties["spec.empty"],
            PropertyValue::Json("{}".into())
        );
    }

    #[test]
    fn extracts_container_sub_entities() {
        let mut pod = make_pod_entity();
        let config = container_config();

        let (subs, edges) = extract_children(&mut pod, &config, "org").unwrap();

        assert_eq!(subs.len(), 2, "Should extract 2 ContainerImage entities");
        assert_eq!(edges.len(), 2, "Should create 2 USES_IMAGE edges");
        assert_eq!(subs[0].entity_type, "ContainerImage");
        assert_eq!(edges[0].name, "USES_IMAGE");
        assert!(edges.iter().all(|edge| edge.created_at == pod.valid_from));
        assert_eq!(subs[0].extracted_by, "sub_entity_rule");
        assert_eq!(edges[0].discovered_by.as_deref(), Some("sub_entity_rule"));

        // Promoted list on parent
        if let Some(PropertyValue::StringList(images)) = pod.all_properties.get("container_images")
        {
            assert_eq!(images.len(), 2);
        } else {
            panic!("Expected container_images StringList on parent");
        }
    }

    #[test]
    fn absent_collection_has_no_children_but_malformed_collections_fail() {
        let config = container_config();
        let mut absent = make_pod_entity();
        absent.all_properties.shift_remove("containers");
        let (subs, edges) = extract_children(&mut absent, &config, "org").unwrap();
        assert!(subs.is_empty() && edges.is_empty());

        let mut null = make_pod_entity();
        null.all_properties
            .insert("containers".into(), PropertyValue::Null);
        assert!(extract_children(&mut null, &config, "org").is_ok());

        let mut scalar = make_pod_entity();
        scalar
            .all_properties
            .insert("containers".into(), PropertyValue::String("oops".into()));
        let error = extract_children(&mut scalar, &config, "org").unwrap_err();
        assert_eq!(error.parent, "payment-pod-abc");
        assert!(error.reason.contains("string value"), "{error}");

        let mut object = make_pod_entity();
        object
            .all_properties
            .insert("containers".into(), PropertyValue::Json("{}".into()));
        assert!(extract_children(&mut object, &config, "org").is_err());

        let mut nonempty_object = make_pod_entity();
        nonempty_object.all_properties.insert(
            "containers".into(),
            PropertyValue::Json(r#"{"name":"app"}"#.into()),
        );
        assert!(extract_children(&mut nonempty_object, &config, "org").is_err());

        let mut broken = make_pod_entity();
        broken
            .all_properties
            .insert("containers".into(), PropertyValue::Json("[{".into()));
        assert!(extract_children(&mut broken, &config, "org").is_err());
    }

    #[test]
    fn children_follow_the_configured_direction_and_the_parents_namespace() {
        for (direction, expected) in [
            (EdgeDirection::Outgoing, vec![("Pod", "ContainerImage")]),
            (EdgeDirection::Incoming, vec![("ContainerImage", "Pod")]),
            (
                EdgeDirection::Both,
                vec![("Pod", "ContainerImage"), ("ContainerImage", "Pod")],
            ),
        ] {
            let mut config = container_config();
            config.sub_entity_rules[0].edge_direction = direction;
            let mut pod = make_pod_entity();
            pod.namespace = "team-a".into();
            pod.all_properties.insert(
                "containers".into(),
                PropertyValue::Json(
                    serde_json::json!([{"name": "app", "image": "nginx:1"}]).to_string(),
                ),
            );
            let (subs, edges) = extract_children(&mut pod, &config, "org").unwrap();
            assert_eq!(subs.len(), 1);
            assert_eq!(
                subs[0].namespace, "team-a",
                "the child takes the parent's namespace"
            );
            assert_eq!(subs[0].source, pod.source);
            let types: Vec<(&str, &str)> = edges
                .iter()
                .map(|e| {
                    let which = |chain: Uuid| {
                        if chain == pod.chain_id {
                            "Pod"
                        } else {
                            "ContainerImage"
                        }
                    };
                    (which(e.source_chain_id), which(e.target_chain_id))
                })
                .collect();
            assert_eq!(types, expected, "{direction:?}");
            for edge in &edges {
                assert_eq!(edge.valid_from, pod.valid_from);
                assert_eq!(edge.name, "USES_IMAGE");
            }
        }
    }

    #[test]
    fn child_without_its_identity_fails_instead_of_vanishing() {
        let config = container_config();
        let mut pod = make_pod_entity();
        pod.all_properties.insert(
            "containers".into(),
            PropertyValue::Json(
                serde_json::json!([{"name": "app", "image": "payment-api:1.4"}, {"name": "sidecar"}])
                    .to_string(),
            ),
        );
        let error = extract_children(&mut pod, &config, "org").unwrap_err();
        assert!(error.reason.contains("item 1"), "{error}");
        assert!(error.reason.contains("`image`"), "{error}");

        let mut scalar_items = make_pod_entity();
        scalar_items
            .all_properties
            .insert("containers".into(), PropertyValue::Json("[1]".into()));
        let error = extract_children(&mut scalar_items, &config, "org").unwrap_err();
        assert!(error.reason.contains("not an object"), "{error}");
    }

    // ---- Keyed-array chain stability (R1–R3 node-versioning behavior) ----
    //
    // A keyed child's `chain_id` is minted fresh each extraction; cross-run chain
    // stability is instead carried by `identity_hash`, a pure function of the
    // child's declared identity (org, namespace, type, pk values) — independent of
    // the item's array position, its siblings, or duplicate entries. `structural_hash`
    // tracks content, so a non-identity edit moves only that one child's structural
    // hash and never disturbs a sibling. These tests pin that contract so identity
    // resolution keeps mapping the same item to the same chain across runs.

    fn pod_with_containers(items: serde_json::Value) -> EntityNode {
        let mut pod = make_pod_entity();
        pod.all_properties
            .insert("containers".into(), PropertyValue::Json(items.to_string()));
        pod
    }

    /// The child's `(identity_hash, structural_hash)` keyed by its image pk value,
    /// so a run's children can be compared identity-by-identity regardless of order.
    fn child_hashes_by_image(
        children: &[EntityNode],
    ) -> std::collections::HashMap<String, (IdentityHash, u64)> {
        children
            .iter()
            .map(|child| {
                let image = child.all_properties["image"].to_string();
                (image, (child.identity_hash, child.structural_hash))
            })
            .collect()
    }

    #[test]
    fn keyed_array_permutation_preserves_each_child_identity() {
        let config = container_config();
        let mut ordered = pod_with_containers(serde_json::json!([
            {"name": "app", "image": "payment-api:1.4"},
            {"name": "sidecar", "image": "envoy:1.2"},
        ]));
        let mut reversed = pod_with_containers(serde_json::json!([
            {"name": "sidecar", "image": "envoy:1.2"},
            {"name": "app", "image": "payment-api:1.4"},
        ]));
        let (a, _) = extract_children(&mut ordered, &config, "org").unwrap();
        let (b, _) = extract_children(&mut reversed, &config, "org").unwrap();
        // Reordering the array changes neither identity nor content of any child.
        assert_eq!(child_hashes_by_image(&a), child_hashes_by_image(&b));
    }

    #[test]
    fn keyed_array_prepend_leaves_existing_children_untouched() {
        let config = container_config();
        let mut before = pod_with_containers(serde_json::json!([
            {"name": "app", "image": "payment-api:1.4"},
            {"name": "sidecar", "image": "envoy:1.2"},
        ]));
        let mut after = pod_with_containers(serde_json::json!([
            {"name": "init", "image": "busybox:1.36"},
            {"name": "app", "image": "payment-api:1.4"},
            {"name": "sidecar", "image": "envoy:1.2"},
        ]));
        let (before, _) = extract_children(&mut before, &config, "org").unwrap();
        let (after, _) = extract_children(&mut after, &config, "org").unwrap();
        let before = child_hashes_by_image(&before);
        let after = child_hashes_by_image(&after);
        // The prepended child is new; every pre-existing child keeps both hashes.
        assert!(after.contains_key("busybox:1.36"));
        assert_eq!(after.len(), before.len() + 1);
        for (image, hashes) in &before {
            assert_eq!(after.get(image), Some(hashes), "child `{image}` drifted");
        }
    }

    #[test]
    fn keyed_array_duplicate_items_share_one_identity() {
        let config = container_config();
        let mut single = pod_with_containers(serde_json::json!([
            {"name": "app", "image": "payment-api:1.4"},
        ]));
        let mut doubled = pod_with_containers(serde_json::json!([
            {"name": "app", "image": "payment-api:1.4"},
            {"name": "app", "image": "payment-api:1.4"},
        ]));
        let (single, _) = extract_children(&mut single, &config, "org").unwrap();
        let (doubled, _) = extract_children(&mut doubled, &config, "org").unwrap();
        // A duplicate array entry never forks a second chain: both extracted children
        // carry the same identity as the lone item, so resolution collapses them.
        assert_eq!(doubled.len(), 2);
        assert_eq!(doubled[0].identity_hash, doubled[1].identity_hash);
        assert_eq!(doubled[0].identity_hash, single[0].identity_hash);
    }

    #[test]
    fn keyed_array_one_item_update_moves_only_that_child() {
        let config = container_config();
        let mut before = pod_with_containers(serde_json::json!([
            {"name": "app", "image": "payment-api:1.4"},
            {"name": "sidecar", "image": "envoy:1.2"},
        ]));
        // Only `app`'s non-identity `name` changes; `image` (the pk) is untouched.
        let mut after = pod_with_containers(serde_json::json!([
            {"name": "app-renamed", "image": "payment-api:1.4"},
            {"name": "sidecar", "image": "envoy:1.2"},
        ]));
        let (before, _) = extract_children(&mut before, &config, "org").unwrap();
        let (after, _) = extract_children(&mut after, &config, "org").unwrap();
        let before = child_hashes_by_image(&before);
        let after = child_hashes_by_image(&after);
        let (app_before_id, app_before_struct) = before["payment-api:1.4"];
        let (app_after_id, app_after_struct) = after["payment-api:1.4"];
        // The edited child keeps its identity (same chain) but gains new content.
        assert_eq!(app_after_id, app_before_id);
        assert_ne!(app_after_struct, app_before_struct);
        // The untouched sibling is unchanged in both identity and content.
        assert_eq!(after["envoy:1.2"], before["envoy:1.2"]);
    }
}
