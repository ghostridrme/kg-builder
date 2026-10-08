use kg_core::traits::{BatchIdentity, BatchKind, GraphMutation, MutationBatch, RequestFingerprint};
use serde_json::json;
use uuid::Uuid;

// Admission used to reject these independent writes even though storage compiled
// them into one bulk statement. The compiler is the owner boundary for this cost
// contract; repeated keys must still retain their ordered individual writes.
#[test]
fn bulk_admission_preserves_grouping_and_repeated_key_boundaries() {
    let mut batch = MutationBatch {
        org_id: "org".into(),
        batch: BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Node,
            index: 0,
        },
        fingerprint: RequestFingerprint("a".repeat(32)),
        preconditions: vec![],
        mutations: (1..=6_000)
            .map(|id| GraphMutation::UpsertSnapshot {
                uuid: Uuid::from_u128(id),
                properties: json!({"namespace":"prod"}).as_object().unwrap().clone(),
            })
            .collect(),
        result: json!({}),
    };
    batch
        .validate()
        .expect("independent bulk writes fit the statement budget");
    assert_eq!(batch.estimated_statement_count(), 1);
    let compiled = kg_storage_cypher::mutations("org", &batch.mutations).unwrap();
    assert_eq!(compiled.len(), 1);
    assert_eq!(compiled[0].expected_rows, 6_000);

    batch.mutations = vec![batch.mutations[0].clone(); 5_001];
    assert!(
        batch.validate().is_err(),
        "repeated keys cannot bypass the cap"
    );
    assert_eq!(
        kg_storage_cypher::mutations("org", &batch.mutations)
            .unwrap()
            .len(),
        5_001
    );

    batch.mutations = (1..=6_000)
        .map(|id| GraphMutation::RecordObservation {
            uuid: Uuid::from_u128(id),
            snapshot_uuid: Uuid::from_u128(10_000),
            entity_uuid: Uuid::from_u128(id + 20_000),
            entity_chain_id: Uuid::from_u128(id + 30_000),
            observed_at: chrono::Utc::now(),
            reconciliations: vec![],
        })
        .collect();
    batch.validate().unwrap();
    assert_eq!(batch.estimated_statement_count(), 2);
    assert_eq!(
        kg_storage_cypher::mutations("org", &batch.mutations)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn expanded_mutations_are_rejected_at_admission() {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    for (mutation, cost) in [
        (
            GraphMutation::RepointEntity {
                previous_uuid: a,
                new_uuid: b,
                chain_id: a,
            },
            5,
        ),
        (
            GraphMutation::MergeChains {
                loser_chain_id: a,
                winner_chain_id: b,
                identity_hashes: vec![],
                effective_at: chrono::Utc::now(),
            },
            8,
        ),
        (
            GraphMutation::SplitChain {
                split_chain_id: a,
                from_chain_id: b,
                identity_hashes: vec![],
                effective_at: chrono::Utc::now(),
            },
            8,
        ),
        (
            GraphMutation::UpsertEdge {
                uuid: a,
                source_chain_id: a,
                target_chain_id: b,
                properties: json!({"namespace":"prod"}).as_object().unwrap().clone(),
            },
            2,
        ),
    ] {
        let count = 5000 / cost;
        let mut batch = MutationBatch {
            org_id: "org".into(),
            batch: BatchIdentity {
                run_id: Uuid::new_v4(),
                kind: BatchKind::Node,
                index: 0,
            },
            fingerprint: RequestFingerprint("a".repeat(32)),
            preconditions: vec![],
            mutations: vec![mutation.clone(); count],
            result: json!({}),
        };
        batch.validate().unwrap();
        assert_eq!(
            kg_storage_cypher::mutations("org", &batch.mutations)
                .unwrap()
                .len(),
            count * cost
        );
        batch.mutations.push(mutation);
        assert!(batch.validate().is_err());
    }
}

// The compiler boundary owns admission cost and ordered atomic-component packing;
// live adapter tests separately own persistence, conflicts and replay.
#[test]
fn paged_admission_retains_guards_and_whole_version_transitions() {
    use kg_core::traits::{commit_pages, Precondition};
    let mut batch = MutationBatch {
        org_id: "org".into(),
        batch: BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Node,
            index: 0,
        },
        fingerprint: RequestFingerprint("a".repeat(32)),
        result: json!({}),
        preconditions: (1..=5_001)
            .map(|id| Precondition::NotObservedAfter {
                uuid: Uuid::from_u128(id),
                observed_at: chrono::Utc::now(),
            })
            .collect(),
        mutations: vec![],
    };
    for id in 1..=1_001 {
        batch.mutations.push(GraphMutation::UpsertEntity {uuid:Uuid::from_u128(id+10_000),properties:json!({"chain_id":Uuid::from_u128(id),"previous_version_uuid":Uuid::from_u128(id),"namespace":"prod","entity_type":"Service","version":2,"is_latest":true}).as_object().unwrap().clone()});
    }
    for id in 1..=1_001 {
        batch.mutations.push(GraphMutation::RepointEntity {
            previous_uuid: Uuid::from_u128(id),
            new_uuid: Uuid::from_u128(id + 10_000),
            chain_id: Uuid::from_u128(id),
        });
    }
    assert!(batch.validate().is_err());
    let pages = commit_pages::partition(&batch).unwrap();
    assert!(pages.len() > 1);
    assert_eq!(
        pages.iter().map(|p| p.preconditions.len()).sum::<usize>(),
        5_001
    );
    let mut seen_data = false;
    let mut created = std::collections::HashMap::new();
    let mut transitions = 0;
    for (page_index, page) in pages.iter().enumerate() {
        page.validate().unwrap();
        assert!(
            page.preconditions.len()
                + kg_storage_cypher::mutations("org", &page.mutations)
                    .unwrap()
                    .len()
                <= 5_000
        );
        if !page.preconditions.is_empty() {
            assert!(!seen_data, "all original guards precede data");
        }
        seen_data |= !page.mutations.is_empty();
        for mutation in &page.mutations {
            match mutation {
                GraphMutation::UpsertEntity { uuid, .. } => {
                    assert!(created.insert(*uuid, page_index).is_none());
                }
                GraphMutation::RepointEntity { new_uuid, .. } => {
                    assert_eq!(created.get(new_uuid), Some(&page_index));
                    transitions += 1;
                }
                _ => panic!("unexpected mutation"),
            }
        }
    }
    assert_eq!(created.len(), 1_001);
    assert_eq!(transitions, 1_001);
}
