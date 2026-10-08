use super::*;
use chrono::Duration;

fn entity() -> EntityNode {
    let now = Utc::now();
    EntityNode {
        labels: Vec::new(),
        inherited_labels: Vec::new(),
        uuid: Uuid::new_v4(),
        chain_id: Uuid::new_v4(),
        org_id: "org".into(),
        namespace: "prod".into(),
        entity_type: "Service".into(),
        name: "payment-api".into(),
        all_properties: IndexMap::new(),
        primary_key_properties: vec!["name".into()],
        additional_key_properties: vec![],
        identity_hash: IdentityHash::compute("org", "prod", "Service", &[("name", "payment-api")]),
        lifecycle: EntityLifecycle::Active,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        embedding: None,
        valid_from: now - Duration::days(10),
        valid_to: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        source: "test".into(),
        extracted_by: "test".into(),
        resolved_by: None,
        first_seen_snapshot_id: None,
        last_seen_snapshot_id: None,
        last_seen_at: None,
        sync_generation: None,
        tags: IndexMap::new(),
        summary: None,
        structural_hash: 0,
        needs_llm_review: false,
        collections: Vec::new(),
    }
}

#[test]
fn validate_rejects_invalid_names_and_primary_keys() {
    assert!(entity().validate().is_ok());

    let mut e = entity();
    e.org_id = "  ".into();
    assert!(e.validate().is_err(), "blank org_id");

    let mut e = entity();
    e.namespace = " \t".into();
    assert!(e.validate().is_err(), "blank namespace");

    let mut e = entity();
    e.name = String::new();
    assert!(e.validate().is_err(), "empty name");

    let mut e = entity();
    e.entity_type = " ".into();
    assert!(e.validate().is_err(), "blank entity_type");

    let mut e = entity();
    e.primary_key_properties.clear();
    assert!(
        e.validate().is_ok(),
        "keyless observations have no declared source key"
    );
    assert!(!e.has_authoritative_keys());

    let mut e = entity();
    e.primary_key_properties = vec!["name".into(), " ".into()];
    assert!(e.validate().is_err(), "blank PK name");

    let mut e = entity();
    e.primary_key_properties = vec!["name".into(), "name".into()];
    assert!(e.validate().is_err(), "duplicate PK name");
}

#[test]
fn is_valid_at_bitemporal_matrix() {
    let now = Utc::now();
    let mut e = entity();
    e.valid_from = now - Duration::days(10);
    e.valid_to = Some(now - Duration::days(2));
    e.deleted_at = Some(now - Duration::days(2));

    assert!(
        !e.is_valid_at(now - Duration::days(11)),
        "before valid_from"
    );
    assert!(e.is_valid_at(now - Duration::days(5)), "inside the window");
    assert!(
        !e.is_valid_at(now - Duration::days(1)),
        "after valid_to/deletion"
    );
    assert!(!e.is_valid_at(now), "now");

    let mut e = entity();
    e.deleted_at = Some(now - Duration::days(2));
    assert!(e.is_valid_at(now - Duration::days(3)));
    assert!(!e.is_valid_at(now - Duration::days(1)));
    assert!(!e.is_valid(), "deleted entities are not currently valid");

    let e = entity();
    assert!(e.is_valid());
    assert!(e.is_valid_at(now));
    assert!(!e.is_valid_at(now - Duration::days(11)));
}

#[test]
fn extraction_provenance_uses_the_entity_field_name() {
    let mut node = entity();
    node.extracted_by = "llm:provider/model".into();
    let json = serde_json::to_value(&node).unwrap();
    assert_eq!(json["extracted_by"], "llm:provider/model");
    assert!(json.get("discovered_by").is_none());
    let decoded: EntityNode = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(decoded.extracted_by, node.extracted_by);
    let mut legacy = json;
    legacy.as_object_mut().unwrap().remove("extracted_by");
    legacy["discovered_by"] = "direct".into();
    assert!(serde_json::from_value::<EntityNode>(legacy).is_err());
}
