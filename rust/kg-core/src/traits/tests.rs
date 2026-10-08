use super::*;
use crate::models::{CollectionMembership, CollectionRef};
use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

fn collection() -> CollectionRef {
    CollectionRef {
        namespace: "prod".into(),
        source: "aws".into(),
        key: "account".into(),
    }
}

#[test]
fn generation_boundaries_are_consistent_for_reads_mutations_and_preconditions() {
    for generation in [0, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
        let valid = generation <= i64::MAX as u64;
        let membership = CollectionMembership {
            collection: collection(),
            generation,
        };
        for (sync_generation, member) in
            [(Some(generation), None), (None, Some(membership.clone()))]
        {
            let mutation = GraphMutation::ObserveEntity {
                chain_id: Uuid::new_v4(),
                observed_at: Utc::now(),
                sync_generation,
                snapshot_id: None,
                collection: member,
            };
            assert_eq!(mutation.validate("org").is_ok(), valid);
        }
        let preconditions = [
            Precondition::OwnsCollection {
                collection: collection(),
                generation,
                run_id: Uuid::new_v4(),
            },
            Precondition::CollectionMembershipsAre {
                uuid: Uuid::new_v4(),
                memberships: vec![membership],
            },
        ];
        for precondition in preconditions {
            assert_eq!(precondition.validate().is_ok(), valid);
        }
        assert_eq!(
            EntityLookup::StaleInCollection {
                collection: collection(),
                before_generation: generation
            }
            .validate("org")
            .is_ok(),
            valid
        );
        assert_eq!(
            EdgeLookup::StaleInCollection {
                collection: collection(),
                before_generation: generation
            }
            .validate("org")
            .is_ok(),
            valid
        );
    }
}

#[test]
fn a_batch_cannot_claim_a_collection_for_another_run() {
    let run = Uuid::new_v4();
    let mut batch = MutationBatch {
        org_id: "org".into(),
        batch: BatchIdentity {
            run_id: run,
            kind: BatchKind::Node,
            index: 0,
        },
        fingerprint: RequestFingerprint("a".repeat(32)),
        preconditions: vec![Precondition::OwnsCollection {
            collection: collection(),
            generation: 1,
            run_id: Uuid::new_v4(),
        }],
        mutations: vec![],
        result: json!({}),
    };
    assert!(batch.validate().is_err());
    batch.preconditions = vec![Precondition::OwnsCollection {
        collection: collection(),
        generation: 1,
        run_id: run,
    }];
    batch.validate().unwrap();
}

#[test]
fn request_fingerprints_have_one_canonical_spelling() {
    for value in ["abcdef0123456789abcdef0123456789", "0".repeat(32).as_str()] {
        RequestFingerprint(value.into()).validate().unwrap();
    }
    for value in [
        "ABCDEF0123456789ABCDEF0123456789",
        "g1234567890123456789012345678901",
        "short",
    ] {
        assert!(RequestFingerprint(value.into()).validate().is_err());
    }
}

#[test]
fn ontology_typos_cannot_silently_disable_restrictions() {
    assert!(
        serde_json::from_value::<Ontology>(json!({"relationship_vocabulry":["USES"]})).is_err()
    );
    let ontology: Ontology =
        serde_json::from_value(json!({"relationship_vocabulary":["USES"]})).unwrap();
    assert_eq!(ontology.canonical_relationship("OTHER"), None);
    assert!(serde_json::from_value::<Ontology>(json!({}))
        .unwrap()
        .is_empty());
}
