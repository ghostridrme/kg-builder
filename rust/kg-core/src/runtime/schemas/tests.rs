use super::*;
use crate::models::AttributeSchema;
use serde_json::json;

fn input(value: serde_json::Value) -> SnapshotInput {
    let mut base = json!({"namespace":"prod","name":"observation","source":"github","data_type":"text","entities":[],"content":"service calls database"});
    base.as_object_mut()
        .unwrap()
        .extend(value.as_object().unwrap().clone());
    serde_json::from_value(base).unwrap()
}

fn ontology(value: serde_json::Value) -> Ontology {
    serde_json::from_value(value).unwrap()
}

#[test]
fn typed_attributes_preserve_missing_null_and_numeric_values() {
    let schema = AttributeSchema(json!({"type":"object","properties":{
        "port":{"type":"integer","minimum":1,"maximum":65535},
        "owner":{"type":["string","null"]},
        "states":{"type":"array","items":{"type":"string","enum":["up","down"]},"maxItems":2},
        "details":{"type":"object","properties":{"active":{"type":"boolean"}},"additionalProperties":false}
    },"required":["port"],"additionalProperties":false}));
    for value in [
        json!({"port":5432}),
        json!({"port":5432,"owner":null,"states":["up"],"details":{"active":true}}),
    ] {
        let original = value.clone();
        schema.validate_attributes(&value).unwrap();
        assert_eq!(value, original);
    }
    for value in [
        json!({}),
        json!({"port":"5432"}),
        json!({"port":null}),
        json!({"port":70000}),
        json!({"port":1,"states":["unknown"]}),
        json!({"port":1,"details":{"extra":true}}),
    ] {
        assert!(schema.validate_attributes(&value).is_err());
    }
}

#[test]
fn malformed_or_unsupported_schemas_are_rejected() {
    for schema in [
        json!({"type":"object","$ref":"https://example.invalid/schema"}),
        json!({"type":"object","default":{}}),
        json!({"type":"object","required":["undeclared"]}),
        json!({"type":"object","properties":{"x":{"type":"array"}}}),
        json!({"type":"object","properties":{"x":{"type":"string","minimum":1}}}),
        json!({"type":"object","properties":{"x":{"type":"string","minLength":5,"maxLength":2}}}),
        json!({"type":"object","properties":{"x":{"type":["string","string"]}}}),
    ] {
        assert!(AttributeSchema(schema).validate().is_err());
    }
    let mut deep = json!({"type":"string"});
    for _ in 0..18 {
        deep = json!({"type":"object","properties":{"child":deep}});
    }
    assert!(AttributeSchema(deep).validate().is_err());
    assert!(
        AttributeSchema(json!({"type":"object","description":"x".repeat(65536)}))
            .validate()
            .is_err()
    );
}

#[test]
fn named_overrides_replace_whole_definitions_and_inherited_references_work() {
    let base = ontology(
        json!({"entity_types":[{"name":"Service","description":"original","properties":[{"name":"old","required":true}]},{"name":"Database"}],"edge_types":[{"name":"USES"}]}),
    );
    let manifest = RunSchemaManifest {
        profiles: Default::default(),
        org_id: "org".into(),
        sources: BTreeMap::from([("github".into(), base)]),
    };
    let input = input(
        json!({"entity_types":[{"name":"Service","description":"request"}],"edge_type_map":[{"source_type":"Service","target_type":"Database","edge_name":"USES"}]}),
    );
    validate_input_schemas(&input).unwrap();
    let effective = manifest.effective(&input, "github").unwrap();
    assert_eq!(
        effective.entity_types[0].description.as_deref(),
        Some("request")
    );
    assert!(effective.entity_types[0].properties.is_empty());
    assert_eq!(effective.entity_types.len(), 2);
    assert_eq!(effective.edge_type_map.len(), 1);
    assert!(manifest.effective(&input, "aws").is_err());
    assert!(manifest.validate_inputs("other", &[input]).is_err());
}

#[test]
fn explicit_empty_mapping_clears_guidance_without_widening_vocabulary() {
    let base = ontology(
        json!({"entity_types":[{"name":"Service"}],"edge_types":[{"name":"USES"}],"edge_type_map":[{"source_type":"Service","target_type":"Entity","edge_name":"USES"}],"relationship_vocabulary":["USES"]}),
    );
    let manifest = RunSchemaManifest {
        profiles: Default::default(),
        org_id: "org".into(),
        sources: BTreeMap::from([("github".into(), base)]),
    };
    let inherited = manifest.effective(&input(json!({})), "github").unwrap();
    assert_eq!(inherited.edge_type_map.len(), 1);
    let cleared = manifest
        .effective(&input(json!({"edge_type_map":[]})), "github")
        .unwrap();
    assert!(cleared.edge_type_map.is_empty());
    assert_eq!(cleared.canonical_relationship("OTHER"), None);
}

#[test]
fn mappings_require_known_types_and_consistent_endpoints() {
    for value in [
        json!({"edge_type_map":[{"source_type":"Missing","target_type":"Entity","edge_name":"USES"}]}),
        json!({"edge_type_map":[{"source_type":"Entity","target_type":"Entity","edge_name":"Missing"}]}),
        json!({"entity_types":[{"name":"A"},{"name":"B"}],"edge_types":[{"name":"USES","source_type":"A"}],"edge_type_map":[{"source_type":"B","target_type":"Entity","edge_name":"USES"}]}),
    ] {
        assert!(validate_effective(&ontology(value)).is_err());
    }
}

#[test]
fn normalized_vocabulary_intersection_preserves_restriction() {
    let base = ontology(json!({"relationship_vocabulary":["DEPENDS_ON"]}));
    let source = ontology(json!({"relationship_vocabulary":["depends-on"]}));
    assert_eq!(
        base.overlaid_with(&source)
            .canonical_relationship("depends on")
            .as_deref(),
        Some("DEPENDS_ON")
    );
    let disjoint = ontology(json!({"relationship_vocabulary":["USES"]}));
    let denied = base
        .overlaid_with(&disjoint)
        .overlaid_with(&Ontology::default());
    assert!(denied.restricted_relationships);
    assert_eq!(denied.canonical_relationship("DEPENDS_ON"), None);
    assert!(validate_definitions(&ontology(
        json!({"relationship_vocabulary":["USES","uses"]})
    ))
    .is_err());
    assert!(validate_definitions(&ontology(
        json!({"relation_aliases":{"used-by":"A","USED BY":"B"}})
    ))
    .is_err());
}

#[test]
fn observation_schema_survives_checkpoint_serialization() {
    let input = input(
        json!({"entity_types":[{"name":"Service","attributes":{"type":"object","properties":{"owner":{"type":"string"}}}}]}),
    );
    let manifest = RunSchemaManifest {
        profiles: Default::default(),
        org_id: "org".into(),
        sources: BTreeMap::from([("github".into(), Ontology::default())]),
    };
    let prepared = crate::runtime::stage_output::PreparedSnapshotInput::new(
        crate::models::ValidatedSnapshotInput::new(input, "org").unwrap(),
        "org",
    )
    .unwrap();
    let schema = manifest.observation("org", prepared.input()).unwrap();
    let snapshot = prepared.snapshot().clone();
    let resolution = crate::runtime::stage_output::NodeResolutionOutput {
        relationship_changes: Default::default(),
        schemas: std::sync::Arc::new(std::collections::HashMap::from([(
            snapshot.uuid,
            schema.clone(),
        )])),
        snapshot_nodes: std::sync::Arc::new(vec![snapshot.clone()]),
        ..Default::default()
    };
    let recovered: crate::runtime::stage_output::NodeResolutionOutput =
        serde_json::from_str(&serde_json::to_string(&resolution).unwrap()).unwrap();
    assert_eq!(recovered.schemas[&snapshot.uuid], schema);
    assert!(recovered.schemas[&snapshot.uuid]
        .for_snapshot(&snapshot, "other")
        .is_err());
}

#[test]
fn signature_restrictions_intersect_wildcards_and_requests_cannot_widen_them() {
    let base = ontology(
        json!({"entity_types":[{"name":"Service"},{"name":"Database"}],"edge_types":[{"name":"USES"}],"allowed_relationships":[{"source_type":"Entity","target_type":"Database","edge_name":"USES"}]}),
    );
    let over = ontology(
        json!({"allowed_relationships":[{"source_type":"Service","target_type":"Entity","edge_name":"USES"}]}),
    );
    let merged = base.overlaid_with(&over);
    validate_effective(&merged).unwrap();
    assert!(merged.permits_signature("Service", "Database", "USES"));
    assert!(!merged.permits_signature("Database", "Service", "USES"));
    let manifest = RunSchemaManifest {
        profiles: Default::default(),
        org_id: "org".into(),
        sources: BTreeMap::from([("github".into(), merged)]),
    };
    let wider = manifest.effective(&input(json!({"edge_type_map":[{"source_type":"Entity","target_type":"Entity","edge_name":"USES"}]})),"github").unwrap();
    assert!(wider.permits_signature("Service", "Database", "USES"));
    assert!(!wider.permits_signature("Database", "Service", "USES"));
    let empty = manifest
        .effective(&input(json!({"edge_type_map":[]})), "github")
        .unwrap();
    assert!(!empty.permits_signature("Service", "Database", "USES"));
    assert_eq!(empty.allowed_relationships, Some(vec![]));
}

#[test]
fn relationship_identity_keys_are_explicit_and_unambiguous() {
    let mut schema = crate::traits::Ontology::default();
    let definition = serde_json::json!({"name":"CONNECTS","identifying_properties":["port"]});
    schema.edge_types = vec![serde_json::from_value(definition).unwrap()];
    super::validate_definitions(&schema).unwrap();
    schema.edge_types[0].identifying_properties = vec!["port".into(), "port".into()];
    assert!(super::validate_definitions(&schema).is_err());
    schema.edge_types[0].identifying_properties = vec!["spec..port".into()];
    assert!(super::validate_definitions(&schema).is_err());
}

#[test]
fn profile_manifest_legacy_wire_and_override_protection() {
    let legacy = serde_json::json!({"org_id":"org","sources":{"inventory":{}}});
    let decoded: super::RunSchemaManifest = serde_json::from_value(legacy).unwrap();
    assert!(decoded.profiles.is_empty());
    assert!(serde_json::to_value(&decoded)
        .unwrap()
        .get("profiles")
        .is_none());
    let profile:crate::profiles::Profile=serde_json::from_value(serde_json::json!({"format_version":1,"profile_id":"p","revision":1,"mode":"strict","ontology":{"entity_types":[{"name":"Service"}]}})).unwrap();
    let mut manifest = decoded;
    manifest.sources.insert(
        "inventory".into(),
        profile.compose(&Default::default()).unwrap(),
    );
    manifest
        .profiles
        .insert("inventory".into(), profile.freeze().unwrap());
    let mut input:crate::models::SnapshotInput=serde_json::from_value(serde_json::json!({"namespace":"prod","name":"scan","source":"inventory","data_type":"text","content":"A service","entities":[]})).unwrap();
    manifest.validate_inputs("org", &[input.clone()]).unwrap();
    input.entity_types = Some(vec![]);
    assert!(manifest.validate_inputs("org", &[input]).is_err());
}
