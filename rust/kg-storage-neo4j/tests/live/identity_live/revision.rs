//! Concurrent identity decisions and revision-aware writer contracts.
use chrono::Utc;
use kg_core::{
    errors::BackendError,
    traits::{
        BatchIdentity, BatchKind, GraphBackend, GraphMutation as M, IdentityRevision,
        IdentityScope, MutationBatch, Precondition, RequestFingerprint, RunHeader,
    },
};
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::json;
use uuid::Uuid;

use kg_neo4j_testkit::indexed_graph as graph;
fn scope() -> IdentityScope {
    IdentityScope {
        namespace: "prod".into(),
        entity_type: "Service".into(),
    }
}
fn entity(id: Uuid) -> M {
    M::UpsertEntity {uuid:id,properties:json!({"chain_id":id,"namespace":"prod","entity_type":"Service","name":"api","version":1,"is_latest":true,"valid_from":"2026-01-01T00:00:00Z"}).as_object().unwrap().clone()}
}
async fn revision(graph: &Neo4jGraphBackend, org: &str) -> IdentityRevision {
    graph
        .identity_revisions(org, &[scope()])
        .await
        .unwrap()
        .remove(0)
}
async fn batch(
    graph: &Neo4jGraphBackend,
    org: &str,
    expected: IdentityRevision,
    mutations: Vec<M>,
) -> MutationBatch {
    let run = Uuid::new_v4();
    let fingerprint = RequestFingerprint("0".repeat(32));
    graph
        .register_run(&RunHeader {
            observation_manifest: Default::default(),
            rule_freezes: vec![],
            org_id: org.into(),
            run_id: run,
            fingerprint: fingerprint.clone(),
            settings_version: "revision-test".into(),
            capture_default: Utc::now(),
            batch_plan: vec![],
            schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
                profiles: Default::default(),
                org_id: org.into(),
                sources: Default::default(),
            },
        })
        .await
        .unwrap();
    MutationBatch {
        org_id: org.into(),
        batch: BatchIdentity {
            run_id: run,
            kind: BatchKind::Node,
            index: 0,
        },
        fingerprint,
        preconditions: vec![Precondition::IdentityRevisionIs(expected)],
        mutations,
        result: json!({"test":true}),
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn concurrent_creations_have_one_winner_and_replay_does_not_advance() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let initial = revision(&graph, &org).await;
    assert_eq!(initial.revision, 0);
    let a = batch(&graph, &org, initial.clone(), vec![entity(Uuid::new_v4())]).await;
    let b = batch(&graph, &org, initial, vec![entity(Uuid::new_v4())]).await;
    let (ra, rb) = tokio::join!(graph.commit_batch(&a), graph.commit_batch(&b));
    let winner = match (&ra, &rb) {
        (Ok(_), Err(BackendError::IdentityRevisionChanged)) => &a,
        (Err(BackendError::IdentityRevisionChanged), Ok(_)) => &b,
        _ => panic!("expected one revision rejection: {ra:?}, {rb:?}"),
    };
    assert_eq!(revision(&graph, &org).await.revision, 1);
    assert!(graph.commit_batch(winner).await.unwrap().replayed);
    assert_eq!(revision(&graph, &org).await.revision, 1);
    let counts = graph
        .execute_read(
            "MATCH (n:Entity {org_id:$org}) RETURN count(n) AS count",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    assert_eq!(counts[0]["count"], 1);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn reads_are_nonmutating_and_scopes_are_isolated() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    revision(&graph, &org).await;
    let rows = graph
        .execute_read(
            "MATCH (r:IdentityRevision {org_id:$org}) RETURN count(r) AS count",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    assert_eq!(rows[0]["count"], 0);
    let expected = revision(&graph, &org).await;
    let mut elsewhere = entity(Uuid::new_v4());
    if let M::UpsertEntity { properties, .. } = &mut elsewhere {
        properties.insert("namespace".into(), json!("dev"));
    }
    graph.apply_mutations(&org, &[elsewhere]).await.unwrap();
    graph
        .apply_mutations("another-org", &[entity(Uuid::new_v4())])
        .await
        .unwrap();
    assert_eq!(revision(&graph, &org).await, expected);
    let plan = batch(&graph, &org, expected, vec![entity(Uuid::new_v4())]).await;
    graph.commit_batch(&plan).await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn administrative_writes_invalidate_decisions_and_failed_batches_roll_back() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    graph.apply_mutations(&org, &[entity(id)]).await.unwrap();
    let old = revision(&graph, &org).await;
    graph
        .apply_mutations(
            &org,
            &[M::UpdateEntity {
                uuid: id,
                properties: json!({"name":"renamed"}).as_object().unwrap().clone(),
            }],
        )
        .await
        .unwrap();
    let plan = batch(&graph, &org, old, vec![]).await;
    assert!(matches!(
        graph.commit_batch(&plan).await,
        Err(BackendError::IdentityRevisionChanged)
    ));
    let current = revision(&graph, &org).await;
    let id2 = Uuid::new_v4();
    let failure = graph
        .apply_mutations(
            &org,
            &[
                entity(id2),
                M::UpdateEntity {
                    uuid: Uuid::new_v4(),
                    properties: json!({"name":"missing"}).as_object().unwrap().clone(),
                },
            ],
        )
        .await;
    assert!(failure.is_err());
    assert_eq!(revision(&graph, &org).await, current);
    let counts = graph
        .execute_read(
            "MATCH (n:Entity {uuid:$id}) RETURN count(n) AS count",
            &json!({"id":id2}),
        )
        .await
        .unwrap();
    assert_eq!(counts[0]["count"], 0);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn existing_versions_cannot_move_identity_scope() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    graph.apply_mutations(&org, &[entity(id)]).await.unwrap();
    let before = revision(&graph, &org).await;
    let mut upsert = entity(id);
    if let M::UpsertEntity { properties, .. } = &mut upsert {
        properties.insert("namespace".into(), json!("other"));
    }
    assert!(graph.apply_mutations(&org, &[upsert]).await.is_err());
    assert!(graph
        .apply_mutations(
            &org,
            &[M::UpdateEntity {
                uuid: id,
                properties: json!({"entity_type":"Other"}).as_object().unwrap().clone()
            }]
        )
        .await
        .is_err());
    assert_eq!(revision(&graph, &org).await, before);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn entity_embedding_refresh_invalidates_identity_evidence() {
    use kg_core::{
        embedding::{content_hash, ComputedEmbedding, EmbeddingSettings},
        embedding_rebuild::{EmbeddingKind, EmbeddingRefresh},
    };
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    graph
        .apply_mutations(&org, &[entity(Uuid::new_v4())])
        .await
        .unwrap();
    let expected = revision(&graph, &org).await;
    let record = graph
        .embedding_records(&org, EmbeddingKind::Entity, None, 1)
        .await
        .unwrap()
        .remove(0);
    let text = record
        .text(
            EmbeddingKind::Entity,
            &kg_core::embedding::EntityEmbeddingFields::default(),
        )
        .unwrap();
    let settings = EmbeddingSettings {
        entity_fields: Default::default(),
        model: "test".into(),
        dimension: 2,
        text_version: kg_core::embedding::TEXT_VERSION.into(),
    };
    let update = EmbeddingRefresh {
        entity_fields: Default::default(),
        record,
        embedding: ComputedEmbedding::new(&settings, content_hash(&text), vec![1., 0.]),
    };
    assert_eq!(
        graph
            .refresh_embeddings(&org, EmbeddingKind::Entity, &[update])
            .await
            .unwrap(),
        1
    );
    assert_eq!(revision(&graph, &org).await.revision, expected.revision + 1);
    let stale = batch(&graph, &org, expected, vec![]).await;
    assert!(matches!(
        graph.commit_batch(&stale).await,
        Err(BackendError::IdentityRevisionChanged)
    ));
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn same_receipt_race_replays_and_read_only_scope_does_not_advance() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let plan = batch(
        &graph,
        &org,
        revision(&graph, &org).await,
        vec![entity(Uuid::new_v4())],
    )
    .await;
    let (a, b) = tokio::join!(graph.commit_batch(&plan), graph.commit_batch(&plan));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a.replayed, b.replayed);
    assert_eq!(revision(&graph, &org).await.revision, 1);
    let read_only = batch(&graph, &org, revision(&graph, &org).await, vec![]).await;
    graph.commit_batch(&read_only).await.unwrap();
    assert_eq!(revision(&graph, &org).await.revision, 1);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn wildcard_scope_lock_serializes_a_new_target_with_an_inflight_mutation() {
    let graph = std::sync::Arc::new(graph().await);
    let org = Uuid::new_v4().to_string();
    let known = Uuid::new_v4();
    let phantom = Uuid::new_v4();
    graph.apply_mutations(&org, &[entity(known)]).await.unwrap();
    let before = revision(&graph, &org).await;
    let control = neo4rs::Graph::new(
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .uri,
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .user,
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .password,
    )
    .await
    .unwrap();
    let mut gate = control.start_txn().await.unwrap();
    gate.run(
        neo4rs::query("MATCH (r:IdentityRevision {scope_id:$scope}) SET r.revision=r.revision")
            .param("scope", scope().key(&org).unwrap()),
    )
    .await
    .unwrap();
    let writer = graph.clone();
    let writer_org = org.clone();
    let task = tokio::spawn(async move {
        writer
            .apply_mutations(
                &writer_org,
                &[known, phantom].map(|uuid| M::UpdateEntity {
                    uuid,
                    properties: json!({"name":"must roll back"})
                        .as_object()
                        .unwrap()
                        .clone(),
                }),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let blocked=graph.execute_read("SHOW TRANSACTIONS YIELD currentQuery,status,parameters WHERE status STARTS WITH 'Blocked' AND currentQuery CONTAINS 'MERGE (r:IdentityRevision' AND parameters.org=$org RETURN status",&json!({"org":org})).await.unwrap();
            if !blocked.is_empty() {break;}
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }).await.expect("writer must block after initial scope discovery");
    let mut new_target = entity(phantom);
    if let M::UpsertEntity { properties, .. } = &mut new_target {
        properties.insert("namespace".into(), json!("dev"));
    }
    let target_graph = graph.clone();
    let target_org = org.clone();
    let target_task = tokio::spawn(async move {
        target_graph
            .apply_mutations(&target_org, &[new_target])
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !target_task.is_finished(),
        "the wildcard identity scope must serialize the new target"
    );
    gate.rollback().await.unwrap();
    let writer_result = task.await.unwrap();
    target_task.await.unwrap().unwrap();
    assert!(
        matches!(writer_result, Err(BackendError::NotFound(_))),
        "the new target cannot enter the writer's locked identity frontier: {writer_result:?}"
    );
    assert_eq!(revision(&graph, &org).await, before);
    let wildcard = IdentityScope {
        namespace: "*".into(),
        entity_type: "*".into(),
    };
    assert!(
        graph.identity_revisions(&org, &[wildcard]).await.unwrap()[0].revision > 0,
        "the serialized target advances the organization-wide identity frontier"
    );
    let rows = graph
        .execute_read(
            "MATCH (n:Entity {org_id:$org}) RETURN n.name AS name",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r["name"] == "api"));
    let other = IdentityScope {
        namespace: "dev".into(),
        ..scope()
    };
    assert_eq!(
        graph.identity_revisions(&org, &[other]).await.unwrap()[0].revision,
        1
    );
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn opposite_scope_order_commits_without_lock_order_deadlock() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let operations = |reverse| {
        let mut a = entity(Uuid::new_v4());
        if let M::UpsertEntity { properties, .. } = &mut a {
            properties.insert("namespace".into(), json!("dev"));
        }
        let mut ops = vec![a, entity(Uuid::new_v4())];
        if reverse {
            ops.reverse();
        }
        ops
    };
    let a = operations(false);
    let b = operations(true);
    let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            graph.apply_mutations(&org, &a),
            graph.apply_mutations(&org, &b)
        )
    })
    .await
    .unwrap();
    left.unwrap();
    right.unwrap();
    let revisions = graph
        .identity_revisions(
            &org,
            &[
                scope(),
                IdentityScope {
                    namespace: "dev".into(),
                    ..scope()
                },
            ],
        )
        .await
        .unwrap();
    assert!(revisions.iter().all(|r| r.revision == 2));
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn metadata_membership_and_chain_transitions_advance_revisions() {
    use kg_core::models::{CollectionMembership, CollectionRef};
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    graph
        .apply_mutations(&org, &[entity(a), entity(b)])
        .await
        .unwrap();
    let at = |day: u32| format!("2026-01-{day:02}T00:00:00Z").parse().unwrap();
    let collection = CollectionRef {
        namespace: "prod".into(),
        source: "inventory".into(),
        key: "services".into(),
    };
    let writes = vec![
        M::ApplyEntityMetadata {
            profile_contract: None,
            reference_exclusions: vec![],
            uuid: a,
            previous_uuid: None,
            tags: Default::default(),
            labels: vec!["compute".into()],
            replace: false,
            observed_at: at(2),
        },
        M::ObserveEntity {
            chain_id: a,
            observed_at: at(3),
            sync_generation: Some(1),
            snapshot_id: None,
            collection: Some(CollectionMembership {
                collection: collection.clone(),
                generation: 1,
            }),
        },
        M::ReleaseMembership {
            uuid: a,
            collection,
        },
        M::MergeChains {
            effective_at: at(4),
            loser_chain_id: a,
            winner_chain_id: b,
            identity_hashes: vec![],
        },
        M::SplitChain {
            effective_at: at(5),
            split_chain_id: a,
            from_chain_id: b,
            identity_hashes: vec![],
        },
        M::DeleteEntity {
            chain_id: a,
            deleted_at: at(6),
            deleted_by: None,
            reason: None,
        },
    ];
    for write in writes {
        let before = revision(&graph, &org).await;
        graph.apply_mutations(&org, &[write]).await.unwrap();
        assert_eq!(revision(&graph, &org).await.revision, before.revision + 1);
    }
    let before = revision(&graph, &org).await;
    let next = Uuid::new_v4();
    let mut restored = entity(next);
    if let M::UpsertEntity { properties, .. } = &mut restored {
        properties.insert("chain_id".into(), json!(a));
        properties.insert("valid_from".into(), json!(at(7)));
        properties.insert("last_seen_at".into(), json!(at(7)));
        properties.insert("previous_version_uuid".into(), json!(a));
        properties.insert("version".into(), json!(2));
    }
    graph
        .apply_mutations(
            &org,
            &[
                restored,
                M::RepointEntity {
                    previous_uuid: a,
                    new_uuid: next,
                    chain_id: a,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(revision(&graph, &org).await.revision, before.revision + 1);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn namespace_revision_fences_concurrent_inferred_types() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let scope = IdentityScope {
        namespace: "prod".into(),
        entity_type: "*".into(),
    };
    let expected = graph
        .identity_revisions(&org, std::slice::from_ref(&scope))
        .await
        .unwrap()
        .remove(0);
    let left = entity(Uuid::new_v4());
    let mut right = entity(Uuid::new_v4());
    if let M::UpsertEntity { properties, .. } = &mut right {
        properties.insert("entity_type".into(), json!("Application"));
    }
    let a = batch(&graph, &org, expected.clone(), vec![left]).await;
    let b = batch(&graph, &org, expected, vec![right]).await;
    let (a, b) = tokio::join!(graph.commit_batch(&a), graph.commit_batch(&b));
    assert!(
        matches!(
            (&a, &b),
            (Ok(_), Err(BackendError::IdentityRevisionChanged))
                | (Err(BackendError::IdentityRevisionChanged), Ok(_))
        ),
        "{a:?} {b:?}"
    );
    assert_eq!(
        graph.identity_revisions(&org, &[scope]).await.unwrap()[0].revision,
        1
    );
}
