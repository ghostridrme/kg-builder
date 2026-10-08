use super::*;
use crate::models::{CollectionMembership, CollectionRef};

fn full_scan(generation: Option<u64>, complete: bool, key: &str) -> SnapshotInput {
    SnapshotInput {
        relationship_changes: Default::default(),
        saga: None,
        previous_snapshot_uuids: vec![],
        labels: Vec::new(),
        tags: Default::default(),
        namespace: "prod".into(),
        name: "scan".into(),
        source_description: None,
        data_type: SnapshotDataType::Entities,
        snapshot_kind: SnapshotKind::Full,
        sync_generation: generation,
        complete,
        org_id: None,
        source: "aws".into(),
        entities: vec![],
        content: None,
        entity_types: None,
        edge_types: None,
        edge_type_map: None,
        exclude_fk_properties: vec![],
        ignore_change_properties: vec![],
        captured_at: None,
        collection: Some(CollectionScope {
            key: key.into(),
            relationships_complete: false,
        }),
    }
}

#[test]
fn collection_requires_full_complete_and_generation() {
    assert!(full_scan(Some(1), true, "aws:us-east-1")
        .validate_scope()
        .is_ok());
    assert!(full_scan(None, true, "k").validate_scope().is_err());
    assert!(full_scan(Some(u64::MAX), true, "k")
        .validate_scope()
        .is_err());
    assert!(full_scan(Some(1), false, "k").validate_scope().is_err());
    assert!(full_scan(Some(1), true, " \t").validate_scope().is_err());
    let mut incremental = full_scan(Some(1), true, "k");
    incremental.snapshot_kind = SnapshotKind::Incremental;
    assert!(incremental.validate_scope().is_err());
    let mut plain = full_scan(Some(1), true, "k");
    plain.collection = None;
    plain.snapshot_kind = SnapshotKind::Incremental;
    plain.complete = false;
    assert!(
        plain.validate_scope().is_ok(),
        "no collection, nothing to check"
    );
}

#[test]
fn pages_of_one_collection_must_agree() {
    let same = [full_scan(Some(3), true, "k"), full_scan(Some(3), true, "k")];
    assert!(validate_collection_scopes(&same).is_ok());
    let other_key = [full_scan(Some(3), true, "k"), full_scan(Some(4), true, "j")];
    assert!(validate_collection_scopes(&other_key).is_ok());
    let generations = [full_scan(Some(3), true, "k"), full_scan(Some(4), true, "k")];
    let error = validate_collection_scopes(&generations).unwrap_err();
    assert!(error.contains("snapshot 1"), "{error}");
    let mut coverage = [full_scan(Some(3), true, "k"), full_scan(Some(3), true, "k")];
    coverage[1]
        .collection
        .as_mut()
        .unwrap()
        .relationships_complete = true;
    assert!(validate_collection_scopes(&coverage).is_err());
    let mut different_source = [full_scan(Some(3), true, "k"), full_scan(Some(4), true, "k")];
    different_source[1].source = "github".into();
    assert!(validate_collection_scopes(&different_source).is_ok());
}

#[test]
fn membership_ids_round_trip_and_merge_never_regresses() {
    let scan = full_scan(Some(4), true, "acct/us-east-1/ec2");
    let collection = CollectionRef::of(&scan).unwrap();
    assert_eq!(
        CollectionRef::parse_member_id(&collection.member_id()),
        Some(collection.clone())
    );
    assert!(CollectionRef::parse_member_id("aws/prod").is_none());
    assert_ne!(
        collection.scope_id("org-a"),
        collection.scope_id("org-b"),
        "scan records are organization scoped"
    );
    let observed = CollectionMembership::of(&scan).unwrap();
    assert_eq!(observed.generation, 4);
    let older = CollectionMembership {
        generation: 2,
        ..observed.clone()
    };
    let merged = CollectionMembership::merge(std::slice::from_ref(&observed), Some(&older));
    assert_eq!(
        merged,
        vec![observed.clone()],
        "an older scan cannot lower the generation"
    );
    let other = CollectionMembership {
        collection: CollectionRef {
            key: "other".into(),
            ..collection.clone()
        },
        generation: 9,
    };
    let merged = CollectionMembership::merge(std::slice::from_ref(&observed), Some(&other));
    assert_eq!(merged, vec![observed.clone(), other]);
    assert_eq!(CollectionMembership::merge(&[], None), vec![]);
    let mut plain = full_scan(None, false, "k");
    plain.collection = None;
    assert!(CollectionMembership::of(&plain).is_none());
}

#[test]
fn request_validation_requires_explicit_scope_and_matching_org() {
    let scan = full_scan(Some(1), true, "k");
    assert!(validate_request("org", std::slice::from_ref(&scan)).is_ok());
    assert!(validate_request(" ", std::slice::from_ref(&scan)).is_err());
    let mut blank = scan.clone();
    blank.namespace = " ".into();
    assert!(blank.validate_request("org").is_err());
    let mut other_org = scan.clone();
    other_org.org_id = Some("else".into());
    assert!(other_org.validate_request("org").is_err());
    let mut invalid_scope = scan.clone();
    invalid_scope.complete = false;
    assert!(validate_request("org", &[scan.clone(), invalid_scope.clone()]).is_ok());
    let error = validate_collection_scopes(&[scan, invalid_scope]).unwrap_err();
    assert!(error.starts_with("snapshot 1"), "{error}");
}

#[test]
fn collection_is_optional_and_strict_in_json() {
    let json = serde_json::json!({
        "namespace": "prod", "name": "scan", "data_type": "entities", "source": "aws",
        "entities": [], "content": null, "entity_types": null, "edge_types": null,
        "edge_type_map": null, "captured_at": null
    });
    let parsed: SnapshotInput = serde_json::from_value(json.clone()).unwrap();
    assert!(parsed.collection.is_none());
    let mut with_scope = json;
    with_scope["collection"] = serde_json::json!({"key": "aws:us-east-1", "pages": 2});
    assert!(serde_json::from_value::<SnapshotInput>(with_scope).is_err());
}

fn generic_input() -> SnapshotInput {
    let mut input = full_scan(None, false, "irrelevant");
    input.collection = None;
    input.entities.push(ConnectorEntity {
        additional_key_properties: vec![],
        entity_type: "Repository".into(),
        name: "repo".into(),
        primary_key_properties: vec![],
        raw_properties: serde_json::json!({}),
        namespace: Some("engineering".into()),
        lifecycle: EntityLifecycle::Active,
        labels: vec!["source".into(), "source".into()],
        tags: IndexMap::from([("owner".into(), "".into())]),
        source: "github".into(),
        org_id: "org".into(),
    });
    input
}

#[test]
fn generic_validation_preserves_input_and_does_not_interpret_collection_fields() {
    let mut input = generic_input();
    input.content = Some(String::new());
    input.complete = false;
    input.collection = Some(CollectionScope {
        key: "".into(),
        relationships_complete: true,
    });
    input.sync_generation = Some(u64::MAX);
    let expected = serde_json::to_value(&input).unwrap();
    let validated = ValidatedSnapshotInput::new(input, "org").unwrap();
    assert_eq!(serde_json::to_value(validated.input()).unwrap(), expected);
    assert!(validated.clone().into_input("other").is_err());
    assert_eq!(
        serde_json::to_value(validated.into_input("org").unwrap()).unwrap(),
        expected
    );
    assert!(validate_request("org", &[]).is_ok());
}

#[test]
fn malformed_fields_report_locations_without_values() {
    let good = generic_input();
    let mut bad = good.clone();
    bad.entities[0].org_id = "sensitive-other-org".into();
    let error = validate_request("org", &[good.clone(), bad]).unwrap_err();
    assert_eq!(error.field, "snapshots[1].entities[0].org_id");
    assert_eq!(error.reason, InputValidationReason::ScopeMismatch);
    assert!(!error.to_string().contains("sensitive-other-org"));
    for value in [
        serde_json::Value::Null,
        serde_json::json!([]),
        serde_json::json!("secret"),
        serde_json::json!(5),
    ] {
        let mut bad = good.clone();
        bad.entities[0].raw_properties = value;
        let error = bad.validate_request("org").unwrap_err();
        assert_eq!(error.field, "entities[0].raw_properties");
        assert_eq!(error.reason, InputValidationReason::WrongShape);
    }
    for field in ["name", "entity_type", "source", "namespace"] {
        let mut bad = good.clone();
        let entity = &mut bad.entities[0];
        match field {
            "name" => entity.name = " ".into(),
            "entity_type" => entity.entity_type = "\t".into(),
            "source" => entity.source = "".into(),
            _ => entity.namespace = Some(" ".into()),
        }
        assert_eq!(
            bad.validate_request("org").unwrap_err().field,
            format!("entities[0].{field}")
        );
    }
    let mut bad = good.clone();
    bad.entities[0].primary_key_properties = vec!["id".into(), "id".into()];
    assert_eq!(
        bad.validate_request("org").unwrap_err().reason,
        InputValidationReason::Duplicate
    );
    bad.entities[0].primary_key_properties = vec![" ".into()];
    assert_eq!(
        bad.validate_request("org").unwrap_err().reason,
        InputValidationReason::Blank
    );
    let mut bad = good.clone();
    bad.tags.insert(" ".into(), "secret-value".into());
    assert_eq!(
        validate_request("org", &[bad]).unwrap_err().field,
        "snapshots[0].tags[0].key"
    );
    let mut bad = good;
    bad.entities[0].labels.push("\n".into());
    assert_eq!(
        bad.validate_request("org").unwrap_err().field,
        "entities[0].labels[2]"
    );
}

#[test]
fn transient_validated_input_cannot_be_deserialized_unchecked() {
    use crate::runtime::StageOutput;
    let input = generic_input();
    let malicious = serde_json::json!({"ValidatedInput": {"input": input, "org_id": "org"}});
    assert!(serde_json::from_value::<StageOutput>(malicious).is_err());
    let state = StageOutput::ValidatedInput(Box::new(
        ValidatedSnapshotInput::new(generic_input(), "org").unwrap(),
    ));
    assert!(serde_json::to_value(state).is_err());
}

#[test]
fn control_envelopes_reject_unknown_fields_but_source_data_stays_open() {
    let mut input = generic_input();
    input.ignore_change_properties = vec!["status".into()];
    input.entities[0].raw_properties = serde_json::json!({
        "saga": "source-value", "community": {"arbitrary": [1, 2]},
        "ignore_change_propertes": "ordinary resource property"
    });
    input.tags.insert("saga".into(), "source-tag".into());
    let value = serde_json::to_value(&input).unwrap();
    let decoded: SnapshotInput = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    for key in ["update_communities", "ignore_change_propertes"] {
        let mut invalid = value.clone();
        invalid[key] = serde_json::json!("SECRET_VALUE");
        let error = serde_json::from_value::<SnapshotInput>(invalid).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
        assert!(error.to_string().contains(key));
        assert!(!error.to_string().contains("SECRET_VALUE"));
    }
    let mut invalid = value;
    invalid["entities"][0]["saga"] = serde_json::json!("SECRET_VALUE");
    assert!(serde_json::from_value::<SnapshotInput>(invalid).is_err());
}

#[test]
fn nested_extraction_hints_reject_misspellings() {
    let mut value = serde_json::to_value(generic_input()).unwrap();
    value["entity_types"] = serde_json::json!([{
        "name": "Service", "description": null,
        "properties": [{"name": "status", "description": null, "required": false}]
    }]);
    value["edge_types"] = serde_json::json!([{
        "name": "USES", "source_type": "Service", "target_type": "Service", "description": null
    }]);
    value["edge_type_map"] = serde_json::json!([{
        "source_type": "Service", "target_type": "Service", "edge_name": "USES", "description": null
    }]);
    assert!(serde_json::from_value::<SnapshotInput>(value.clone()).is_ok());
    for path in [
        "/entity_types/0",
        "/entity_types/0/properties/0",
        "/edge_types/0",
        "/edge_type_map/0",
    ] {
        let mut invalid = value.clone();
        invalid.pointer_mut(path).unwrap()["descripton"] = "ignored-before".into();
        assert!(
            serde_json::from_value::<SnapshotInput>(invalid)
                .unwrap_err()
                .to_string()
                .contains("unknown field"),
            "{path}"
        );
    }
}

#[test]
fn message_source_description_is_strict_and_fingerprinted() {
    let mut input = generic_input();
    input.data_type = SnapshotDataType::Message;
    input.source_description = Some("Discussion from payments/api".into());
    let value = serde_json::to_value(&input).unwrap();
    assert_eq!(value["data_type"], "message");
    assert_eq!(input.data_type.to_string(), "message");
    assert!(serde_json::from_value::<SnapshotInput>(value)
        .unwrap()
        .validate_request("org")
        .is_ok());
    let fingerprint =
        crate::traits::RequestFingerprint::compute("org", &[input.clone()], &serde_json::json!({}))
            .unwrap();
    input.source_description = Some("A different context".into());
    assert_ne!(
        fingerprint,
        crate::traits::RequestFingerprint::compute("org", &[input.clone()], &serde_json::json!({}))
            .unwrap()
    );
    input.source_description = Some(" ".into());
    assert_eq!(
        input.validate_request("org").unwrap_err().field,
        "source_description"
    );
}

#[test]
fn additional_key_groups_are_explicit_complete_definitions() {
    let mut input: SnapshotInput = serde_json::from_value(serde_json::json!({"namespace":"prod","name":"inventory","source":"aws","data_type":"entities","entities":[{"name":"api","entity_type":"Service","primary_key_properties":["id"],"additional_key_properties":[["account","arn"]],"raw_properties":{"id":"a","account":"1","arn":"x"},"lifecycle":"active","tags":{},"source":"aws","org_id":"org"}]})).unwrap();
    assert!(input.validate_request("org").is_ok());
    let roundtrip: SnapshotInput =
        serde_json::from_value(serde_json::to_value(&input).unwrap()).unwrap();
    assert_eq!(
        roundtrip.entities[0].additional_key_properties,
        vec![vec!["account", "arn"]]
    );
    for groups in [
        vec![vec![]],
        vec![vec![" "]],
        vec![vec!["arn", "arn"]],
        vec![vec!["id"]],
        vec![vec!["account", "arn"], vec!["arn", "account"]],
    ] {
        input.entities[0].additional_key_properties = groups
            .into_iter()
            .map(|g| g.into_iter().map(str::to_string).collect())
            .collect();
        assert!(input.validate_request("org").is_err());
    }
}

#[test]
fn explicit_relationship_commands_validate_typed_keys_intervals_and_targets() {
    let mut input = full_scan(Some(1), true, "k");
    let at = Utc::now();
    let relationship = RelationshipObservation {
        source: RelationshipEndpoint::Identity {
            namespace: "prod".into(),
            entity_type: "Service".into(),
            key_values: IndexMap::from([(
                "port".into(),
                crate::models::PropertyValue::Integer(443),
            )]),
        },
        target: RelationshipEndpoint::Chain {
            chain_id: uuid::Uuid::new_v4(),
        },
        name: "CALLS".into(),
        description: "source-declared call".into(),
        properties: Default::default(),
        valid_from: at,
        valid_to: None,
    };
    input.relationship_changes = vec![RelationshipChange::Observe {
        relationship: relationship.clone(),
    }];
    assert!(input.validate_request("org").is_ok());
    let roundtrip: SnapshotInput =
        serde_json::from_value(serde_json::to_value(&input).unwrap()).unwrap();
    assert_eq!(roundtrip.relationship_changes, input.relationship_changes);
    let mut empty = relationship.clone();
    empty.valid_to = Some(at);
    input.relationship_changes = vec![RelationshipChange::Observe {
        relationship: empty,
    }];
    assert!(
        input.validate_request("org").is_ok(),
        "zero-length versions preserve immediate invalidation evidence"
    );
    let mut invalid = relationship.clone();
    invalid.valid_to = Some(at - chrono::Duration::seconds(1));
    input.relationship_changes = vec![RelationshipChange::Observe {
        relationship: invalid,
    }];
    assert!(input.validate_request("org").is_err());
    let mut invalid = relationship.clone();
    let RelationshipEndpoint::Identity { key_values, .. } = &mut invalid.source else {
        unreachable!()
    };
    key_values.insert(
        "port".into(),
        crate::models::PropertyValue::StringList(vec!["443".into()]),
    );
    input.relationship_changes = vec![RelationshipChange::Observe {
        relationship: invalid,
    }];
    assert!(input.validate_request("org").is_err());
    let target = RelationshipVersionRef {
        source_chain_id: uuid::Uuid::new_v4(),
        target_chain_id: uuid::Uuid::new_v4(),
        chain_id: uuid::Uuid::new_v4(),
        version_uuid: uuid::Uuid::new_v4(),
    };
    input.relationship_changes = vec![RelationshipChange::Cancel {
        target: target.clone(),
        effective_at: at,
    }];
    assert!(input.validate_request("org").is_ok());
    input
        .relationship_changes
        .push(RelationshipChange::Replace {
            target,
            replacement: relationship,
            effective_at: at,
        });
    assert!(
        input.validate_request("org").is_err(),
        "one lineage cannot receive conflicting commands in a snapshot"
    );
}

#[test]
fn relationship_commands_survive_node_checkpoint_serialization() {
    use crate::runtime::stage_output::{NodeCheckpoint, NodeResolutionOutput};
    let snapshot = uuid::Uuid::new_v4();
    let command = RelationshipChange::Cancel {
        target: RelationshipVersionRef {
            source_chain_id: uuid::Uuid::new_v4(),
            target_chain_id: uuid::Uuid::new_v4(),
            chain_id: uuid::Uuid::new_v4(),
            version_uuid: uuid::Uuid::new_v4(),
        },
        effective_at: Utc::now(),
    };
    let checkpoint = NodeCheckpoint {
        snapshot_index: 0,
        resolution: NodeResolutionOutput {
            relationship_changes: std::sync::Arc::new(std::collections::HashMap::from([(
                snapshot,
                vec![command.clone()],
            )])),
            ..Default::default()
        },
    };
    let recovered: NodeCheckpoint =
        serde_json::from_slice(&serde_json::to_vec(&checkpoint).unwrap()).unwrap();
    assert_eq!(
        recovered.resolution.relationship_changes[&snapshot],
        vec![command]
    );
}

#[test]
fn replacement_start_is_independent_of_when_the_pending_version_is_cancelled() {
    let cancelled_at = Utc::now();
    let scheduled_start = cancelled_at + chrono::Duration::days(1);
    let source = uuid::Uuid::new_v4();
    let target = uuid::Uuid::new_v4();
    let mut input = full_scan(Some(1), true, "k");
    input.relationship_changes = vec![RelationshipChange::Replace {
        target: RelationshipVersionRef {
            source_chain_id: source,
            target_chain_id: target,
            chain_id: uuid::Uuid::new_v4(),
            version_uuid: uuid::Uuid::new_v4(),
        },
        effective_at: cancelled_at,
        replacement: RelationshipObservation {
            source: RelationshipEndpoint::Chain { chain_id: source },
            target: RelationshipEndpoint::Chain { chain_id: target },
            name: "CALLS".into(),
            description: "revised scheduled call".into(),
            properties: Default::default(),
            valid_from: scheduled_start,
            valid_to: None,
        },
    }];
    assert!(
        input.validate_request("org").is_ok(),
        "cancelling the old Friday schedule on Thursday must allow its replacement to still start Friday"
    );
}

#[test]
fn existing_snapshot_input_has_no_fresh_extraction_controls() {
    let uuid = uuid::Uuid::new_v4();
    let mut value = serde_json::json!({"kind":"existing","input":{"namespace":"prod","snapshot_uuid":uuid,"saga":{"saga":{"kind":"name","name":"deployment"},"previous_snapshot_uuid":null}}});
    let input: IngestionInput = serde_json::from_value(value.clone()).unwrap();
    input.validate_request("org").unwrap();
    value["input"]["entities"] = serde_json::json!([]);
    assert!(serde_json::from_value::<IngestionInput>(value).is_err());
}

#[test]
fn saga_controls_reject_blank_reference_and_self_predecessor() {
    let mut input = generic_input();
    input.saga = Some(ThreadAssociation {
        saga: crate::saga::ThreadReference::Name { name: " ".into() },
        previous_snapshot_uuid: None,
    });
    assert!(input.validate_request("org").is_err());
    let id = uuid::Uuid::new_v4();
    let reused = ExistingSnapshotInput {
        namespace: "prod".into(),
        snapshot_uuid: id,
        saga: ThreadAssociation {
            saga: crate::saga::ThreadReference::Uuid {
                uuid: uuid::Uuid::new_v4(),
            },
            previous_snapshot_uuid: Some(id),
        },
    };
    assert!(reused.validate_request("org").is_err());
}

// Public input compatibility: both spellings must produce identical canonical
// evidence/fingerprints. Mixed references must fail instead of selecting one.
#[test]
fn thread_input_preserves_legacy_canonical_evidence_and_rejects_conflicts() {
    let baseline = serde_json::to_value(full_scan(Some(1), true, "inventory")).unwrap();
    for reference in [
        serde_json::json!({"kind":"name","name":"incident-1"}),
        serde_json::json!({"kind":"uuid","uuid":"40000000-0000-4000-8000-000000000001"}),
    ] {
        let mut legacy = baseline.clone();
        legacy["saga"] = serde_json::json!({"saga":reference,"previous_snapshot_uuid":null});
        let expected: SnapshotInput = serde_json::from_value(legacy.clone()).unwrap();
        let mut renamed = legacy.clone();
        renamed.as_object_mut().unwrap().remove("saga");
        renamed["thread"] = reference.clone();
        renamed["thread"]["predecessor_snapshot_uuid"] = serde_json::Value::Null;
        let actual: SnapshotInput = serde_json::from_value(renamed.clone()).unwrap();
        actual.validate_request("acme").unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        renamed["saga"] = legacy["saga"].clone();
        assert!(serde_json::from_value::<SnapshotInput>(renamed).is_err());
    }
    for invalid in [
        serde_json::json!({"kind":"name","name":"x","uuid":"40000000-0000-4000-8000-000000000001"}),
        serde_json::json!({"kind":"name","name":"x","unexpected":true}),
        serde_json::json!({"kind":"name","name":"x","saga":{"kind":"name","name":"y"}}),
        serde_json::json!({"kind":"name","name":"x","previous_snapshot_uuid":null,"predecessor_snapshot_uuid":null}),
    ] {
        assert!(serde_json::from_value::<ThreadAssociation>(invalid).is_err());
    }
}
