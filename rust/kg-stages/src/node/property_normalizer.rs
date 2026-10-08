//! Apply property rules without changing authoritative identity.
use super::child_extract::ChildExtractionError;
use kg_core::{
    entity_type_config::EntityTypeConfig,
    identity::structural_hash::compute_structural_hash,
    models::{EntityNode, PropertyValue},
};

pub(crate) fn validate_rules(
    entity: &EntityNode,
    config: &EntityTypeConfig,
) -> Result<(), ChildExtractionError> {
    let keys: std::collections::HashSet<_> = entity
        .primary_key_properties
        .iter()
        .chain(entity.additional_key_properties.iter().flatten())
        .collect();
    let changes = config
        .drop_properties
        .iter()
        .chain(&config.force_json_properties)
        .chain(config.sub_entity_rules.iter().flat_map(|r| {
            r.promote_to_parent
                .iter()
                .map(|p| &p.target_field)
                .chain(r.raw_blob_field.iter().filter(|_| r.keep_raw_blob))
        }));
    for path in changes {
        if keys
            .iter()
            .any(|key| *key == path || key.starts_with(&format!("{path}.")))
        {
            return Err(ChildExtractionError {
                parent: entity.name.clone(),
                source_path: path.clone(),
                reason: "property rule changes an authoritative key".into(),
            });
        }
    }
    Ok(())
}

/// Normalize a candidate after child extraction, preserving its declared identity.
#[tracing::instrument(name = "property_normalization", skip_all)]
pub fn normalize_entity(
    entity: &mut EntityNode,
    config: &EntityTypeConfig,
    extra_hash_exclusions: &[String],
) -> Result<(), ChildExtractionError> {
    validate_rules(entity, config)?;
    for path in &config.force_json_properties {
        if !entity.all_properties.contains_key(path)
            && entity
                .all_properties
                .keys()
                .any(|key| key.starts_with(&format!("{path}.")))
        {
            return Err(ChildExtractionError {
                parent: entity.name.clone(),
                source_path: path.clone(),
                reason:
                    "forced JSON subtree was already projected; select it as a child blob property"
                        .into(),
            });
        }
    }
    let mut properties = entity.all_properties.clone();
    for path in &config.drop_properties {
        properties.retain(|key, _| key != path && !key.starts_with(&format!("{path}.")));
    }
    for path in &config.force_json_properties {
        if let Some(value) = properties.get(path) {
            let source = value.to_source().map_err(|reason| ChildExtractionError {
                parent: entity.name.clone(),
                source_path: path.clone(),
                reason,
            })?;
            properties.insert(path.clone(), PropertyValue::Json(source.to_string()));
        }
    }
    // A port is a number even when a text source writes it as quoted JSON.
    // Preserve declared key types; only descriptive port fields are normalized.
    let identity_keys: std::collections::HashSet<&str> = entity
        .primary_key_properties
        .iter()
        .chain(entity.additional_key_properties.iter().flatten())
        .map(String::as_str)
        .collect();
    for (path, value) in &mut properties {
        if path.rsplit('.').next() != Some("port") || identity_keys.contains(path.as_str()) {
            continue;
        }
        if let PropertyValue::String(text) = value {
            if let Ok(port) = text.parse::<u16>() {
                if port != 0 && port.to_string() == *text {
                    *value = PropertyValue::Integer(i64::from(port));
                }
            }
        }
    }
    let mut exclusions = config.hash_exclusions();
    exclusions.extend(extra_hash_exclusions.iter().cloned());
    PropertyValue::validate_flat_paths(&properties).map_err(|reason| ChildExtractionError {
        parent: entity.name.clone(),
        source_path: String::new(),
        reason,
    })?;
    entity.structural_hash = compute_structural_hash(&properties, &exclusions);
    entity.all_properties = properties;
    tracing::debug!(
        properties = entity.all_properties.len(),
        "property normalization complete"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::{enums::EntityLifecycle, models::ConnectorEntity};
    fn node() -> EntityNode {
        super::super::structured_prepare::connector_entity_to_node(
            &ConnectorEntity {
                org_id: "org".into(),
                entity_type: "Service".into(),
                name: "service".into(),
                primary_key_properties: vec!["id".into()],
                additional_key_properties: vec![vec!["alias".into()]],
                raw_properties: serde_json::json!({"id":1,"alias":"a","value":[null, {"n":2}]}),
                labels: vec![],
                tags: Default::default(),
                namespace: None,
                source: "test".into(),
                lifecycle: EntityLifecycle::Active,
            },
            "prod",
            "org",
            chrono::Utc::now(),
            None,
            &[],
        )
        .unwrap()
    }
    #[test]
    fn force_json_is_source_json_and_idempotent() {
        let mut node = node();
        node.all_properties
            .insert("text".into(), PropertyValue::String("hello".into()));
        let config = EntityTypeConfig {
            force_json_properties: vec!["value".into(), "text".into()],
            ..Default::default()
        };
        normalize_entity(&mut node, &config, &[]).unwrap();
        assert_eq!(
            node.all_properties["text"],
            PropertyValue::Json("\"hello\"".into())
        );
        let before = node.all_properties.clone();
        let hash = node.structural_hash;
        normalize_entity(&mut node, &config, &[]).unwrap();
        assert_eq!(node.all_properties, before);
        assert_eq!(node.structural_hash, hash);
    }
    #[test]
    fn primary_and_additional_keys_cannot_be_rewritten() {
        for key in ["id", "alias"] {
            for config in [
                EntityTypeConfig {
                    drop_properties: vec![key.into()],
                    ..Default::default()
                },
                EntityTypeConfig {
                    force_json_properties: vec![key.into()],
                    ..Default::default()
                },
            ] {
                let mut node = node();
                let before = node.all_properties.clone();
                assert!(normalize_entity(&mut node, &config, &[]).is_err());
                assert_eq!(node.all_properties, before);
            }
        }
    }
    #[test]
    fn descriptive_port_text_is_canonicalized_without_changing_identity_keys() {
        let mut entity = node();
        entity
            .all_properties
            .insert("port".into(), PropertyValue::String("5432".into()));
        entity
            .all_properties
            .insert("listener.port".into(), PropertyValue::String("443".into()));
        entity
            .all_properties
            .insert("other".into(), PropertyValue::String("5432".into()));
        normalize_entity(&mut entity, &EntityTypeConfig::default(), &[]).unwrap();
        assert_eq!(entity.all_properties["port"], PropertyValue::Integer(5432));
        assert_eq!(
            entity.all_properties["listener.port"],
            PropertyValue::Integer(443)
        );
        assert_eq!(
            entity.all_properties["other"],
            PropertyValue::String("5432".into())
        );
        let mut keyed = node();
        keyed.primary_key_properties = vec!["port".into()];
        keyed
            .all_properties
            .insert("port".into(), PropertyValue::String("5432".into()));
        normalize_entity(&mut keyed, &EntityTypeConfig::default(), &[]).unwrap();
        assert_eq!(
            keyed.all_properties["port"],
            PropertyValue::String("5432".into())
        );
    }
}
