use chrono::Utc;
use kg_core::traits::{
    BatchIdentity, BatchKind, GraphMutation, MutationBatch, PlannedBatch, Precondition,
    RequestFingerprint, RunHeader,
};
use serde_json::json;
use uuid::Uuid;

#[test]
fn malformed_run_identities_and_duplicate_receipt_keys_are_rejected() {
    let mut header = RunHeader {
        observation_manifest: Default::default(),
        rule_freezes: vec![],
        schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
            profiles: Default::default(),
            org_id: "org".into(),
            ..Default::default()
        },
        org_id: "org".into(),
        run_id: Uuid::new_v4(),
        fingerprint: RequestFingerprint("0".repeat(32)),
        settings_version: "1".into(),
        capture_default: Utc::now(),
        batch_plan: vec![PlannedBatch {
            kind: BatchKind::Node,
            index: 0,
            items: 1,
        }],
    };
    header.validate().unwrap();
    header.batch_plan.push(header.batch_plan[0].clone());
    assert!(header.validate().is_err());
    header.batch_plan.pop();
    header.batch_plan.push(PlannedBatch {
        kind: BatchKind::Relationship,
        index: 0,
        items: 1,
    });
    header.validate().unwrap();
    header.run_id = Uuid::nil();
    assert!(header.validate().is_err());
    let batch = MutationBatch {
        org_id: "org".into(),
        batch: BatchIdentity {
            run_id: Uuid::nil(),
            kind: BatchKind::Node,
            index: 0,
        },
        fingerprint: header.fingerprint,
        preconditions: vec![],
        mutations: vec![],
        result: json!({}),
    };
    assert!(batch.validate().is_err());
}

#[test]
fn mutations_reject_nil_identity_and_provenance() {
    let id = Uuid::new_v4();
    for mutation in [
        GraphMutation::UpsertSnapshot {
            uuid: Uuid::nil(),
            properties: Default::default(),
        },
        GraphMutation::UpsertEntity {
            uuid: id,
            properties:
                json!({"namespace":"prod", "entity_type":"Service", "chain_id":Uuid::nil()})
                    .as_object()
                    .unwrap()
                    .clone(),
        },
        GraphMutation::UpsertEdge {
            uuid: id,
            source_chain_id: id,
            target_chain_id: Uuid::nil(),
            properties: Default::default(),
        },
        GraphMutation::DeleteEntity {
            chain_id: Uuid::nil(),
            deleted_at: Utc::now(),
            deleted_by: None,
            reason: None,
        },
        GraphMutation::ObserveEntity {
            chain_id: id,
            observed_at: Utc::now(),
            sync_generation: None,
            snapshot_id: Some(Uuid::nil()),
            collection: None,
        },
        GraphMutation::RepointEntity {
            previous_uuid: id,
            new_uuid: Uuid::nil(),
            chain_id: id,
        },
        GraphMutation::MergeChains {
            loser_chain_id: Uuid::nil(),
            winner_chain_id: id,
            effective_at: Utc::now(),
            identity_hashes: vec![],
        },
        GraphMutation::RecordObservation {
            uuid: id,
            snapshot_uuid: id,
            entity_uuid: Uuid::nil(),
            entity_chain_id: id,
            observed_at: Utc::now(),
            reconciliations: Vec::new(),
        },
    ] {
        assert!(mutation.validate("org").is_err(), "{mutation:?}");
    }
}

#[test]
fn relationship_set_guards_reject_duplicate_and_nil_ids() {
    let id = Uuid::new_v4();
    for uuids in [vec![id, id], vec![Uuid::nil()]] {
        for guard in [
            Precondition::LiveIncidentEdgesAre {
                chain_id: id,
                uuids: uuids.clone(),
            },
            Precondition::LiveEdgesForRelationAre {
                source_chain_id: id,
                name: "USES".into(),
                uuids,
            },
        ] {
            assert!(guard.validate().is_err(), "{guard:?}");
        }
    }
    assert!(Precondition::LatestVersionIs {
        chain_id: Uuid::nil(),
        uuid: id,
        version: 1
    }
    .validate()
    .is_err());
    Precondition::LiveIncidentEdgesAre {
        chain_id: id,
        uuids: vec![],
    }
    .validate()
    .unwrap();
}

#[test]
fn unresolved_slot_budget_counts_compiled_groups_and_bounds_the_whole_payload() {
    let source = Uuid::new_v4();
    let at = Utc::now();
    let mut batch = MutationBatch {
        org_id: "org".into(),
        batch: BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Relationship,
            index: 0,
        },
        fingerprint: RequestFingerprint("0".repeat(32)),
        preconditions: vec![],
        result: json!({}),
        mutations: (0..6400)
            .map(|i| GraphMutation::RecordUnresolvedReferences {
                source_chain_id: source,
                slot: format!("field-{i}"),
                decided_at: at,
                decision_id: Uuid::new_v4(),
                entries: vec![],
            })
            .collect(),
    };
    batch.validate().unwrap();
    batch.mutations = vec![batch.mutations[0].clone(); 5001];
    assert!(batch
        .validate()
        .unwrap_err()
        .to_string()
        .contains("statements"));
    batch.mutations = vec![GraphMutation::UpdateEntity {
        uuid: Uuid::new_v4(),
        properties: json!({"description":"x".repeat(17 * 1024 * 1024)})
            .as_object()
            .unwrap()
            .clone(),
    }];
    assert!(batch.validate().unwrap_err().to_string().contains("16 MiB"));
}
