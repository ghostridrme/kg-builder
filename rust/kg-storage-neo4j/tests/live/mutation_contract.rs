//! Storage contracts of the Neo4j adapter: atomic mutations, typed reads,
//! and receipted commits with preconditions. Every test needs an isolated
//! database and is opt-in.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use kg_core::{
    errors::BackendError,
    traits::{
        graph_backend::GraphEmbedding, BatchIdentity, BatchKind, CommittedBatch, EdgeLookup,
        EntityLookup, EntityVersionRecord, GraphBackend, GraphMutation as M, MutationBatch,
        Precondition, RequestFingerprint, RunHeader, RunRegistration, VersionState,
    },
};
use kg_storage_neo4j::{FaultInjection, Neo4jGraphBackend, Neo4jOptions};
use serde_json::{json, Value};
use uuid::Uuid;

fn at() -> DateTime<Utc> {
    "2026-09-15T12:00:00Z".parse().unwrap()
}
fn entity(uuid: Uuid, chain: Uuid) -> M {
    M::UpsertEntity { uuid, properties: json!({"chain_id":chain,"namespace":"prod","entity_type":"Service","name":"api","is_latest":true,"version":1,"valid_from":at(),"last_seen_at":at(),"embedding":[0.1,0.2,0.3],"tags":["a","b"],"replicas":3,"cpu":0.5}).as_object().unwrap().clone() }
}
fn successor_entity(uuid: Uuid, chain: Uuid, previous: Uuid) -> M {
    let mut mutation = entity(uuid, chain);
    let M::UpsertEntity { properties, .. } = &mut mutation else {
        unreachable!()
    };
    properties.insert("version".into(), json!(2));
    properties.insert("previous_version_uuid".into(), json!(previous));
    mutation
}
fn edge(uuid: Uuid, source: Uuid, target: Uuid) -> M {
    M::UpsertEdge {
        uuid,
        source_chain_id: source,
        target_chain_id: target,
        properties: json!({"name":"DEPENDS_ON","is_latest":true})
            .as_object()
            .unwrap()
            .clone(),
    }
}
/// The stored version with this uuid, found through its chain history.
async fn version(
    graph: &dyn GraphBackend,
    org: &str,
    chain: Uuid,
    uuid: Uuid,
) -> Option<EntityVersionRecord> {
    graph
        .find_entities(
            org,
            &EntityLookup::VersionsByChain {
                chain_ids: vec![chain],
            },
        )
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.uuid == uuid)
}
async fn node(graph: &dyn GraphBackend, org: &str, chain: Uuid, uuid: Uuid) -> Value {
    version(graph, org, chain, uuid)
        .await
        .map(|v| Value::Object(v.stored))
        .unwrap_or(Value::Null)
}
async fn live_pairs(graph: &dyn GraphBackend, org: &str, a: Uuid, b: Uuid) -> usize {
    graph
        .find_edges(
            org,
            &EdgeLookup::LiveByChainPairs {
                pairs: vec![(a, b)],
            },
        )
        .await
        .unwrap()
        .len()
}

/// Two organizations with one entity pair, a snapshot and a relationship each;
/// `setup` is applied twice, so idempotent replays are part of every seed.
struct Seed {
    org: String,
    foreign: String,
    a: Uuid,
    b: Uuid,
    f: Uuid,
    new: Uuid,
    snap: Uuid,
    foreign_snap: Uuid,
    rel: Uuid,
    foreign_rel: Uuid,
}

async fn seed(graph: &dyn GraphBackend) -> Seed {
    let org = format!("mutation-{}", Uuid::new_v4());
    let foreign = format!("foreign-{org}");
    let (a, b, f, new, snap, foreign_snap, rel, foreign_rel) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let setup = [
        entity(a, a),
        entity(b, b),
        M::UpsertSnapshot {
            uuid: snap,
            properties: json!({"name":"sync"}).as_object().unwrap().clone(),
        },
        edge(rel, a, b),
    ];
    graph.apply_mutations(&org, &setup).await.unwrap();
    graph.apply_mutations(&org, &setup).await.unwrap();
    graph
        .apply_mutations(
            &foreign,
            &[
                entity(f, f),
                M::UpsertSnapshot {
                    uuid: foreign_snap,
                    properties: Default::default(),
                },
                edge(foreign_rel, f, f),
            ],
        )
        .await
        .unwrap();
    Seed {
        org,
        foreign,
        a,
        b,
        f,
        new,
        snap,
        foreign_snap,
        rel,
        foreign_rel,
    }
}

/// `a` is superseded by `new` in its chain; applied twice for idempotence.
async fn supersede(graph: &dyn GraphBackend, s: &Seed) {
    let (org, a, new) = (&s.org, s.a, s.new);
    let successor = [
        M::SupersedeEntity {
            uuid: a,
            chain_id: a,
            valid_to: at(),
        },
        successor_entity(new, a, a),
        M::RepointEntity {
            previous_uuid: a,
            new_uuid: new,
            chain_id: a,
        },
    ];
    graph.apply_mutations(org, &successor).await.unwrap();
    graph.apply_mutations(org, &successor).await.unwrap();
}

/// An entity and a snapshot racing for one uuid: exactly one organization wins.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn concurrent_node_kind_collision_has_one_winner() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let org = format!("mutation-{}", Uuid::new_v4());
    let foreign = format!("foreign-{org}");
    for _ in 0..8 {
        let uuid = Uuid::new_v4();
        let entity_ops = [entity(uuid, uuid)];
        let snapshot_ops = [M::UpsertSnapshot {
            uuid,
            properties: Default::default(),
        }];
        let (entity_result, snapshot_result) = tokio::join!(
            graph.apply_mutations(&org, &entity_ops),
            graph.apply_mutations(&foreign, &snapshot_ops)
        );
        assert_ne!(
            entity_result.is_ok(),
            snapshot_result.is_ok(),
            "concurrent node kind collision must have exactly one winner"
        );
    }
}

/// A relationship vector lands on its own organization's edge and never on a foreign or unknown one.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn relationship_vectors_are_scoped_to_their_organization() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let s = seed(graph).await;
    let Seed {
        org,
        a,
        b,
        rel,
        foreign_rel,
        ..
    } = &s;
    let org = org.clone();
    let (a, b, rel, foreign_rel) = (*a, *b, *rel, *foreign_rel);
    let fact_vector = |uuid| M::SetRelationshipEmbedding {
        uuid,
        embedding: GraphEmbedding {
            model: "fact-model".into(),
            values: vec![1.0, 0.0],
        },
        text_version: kg_core::embedding::RELATIONSHIP_TEXT_VERSION.into(),
        content_hash: "fact-content".into(),
    };
    graph
        .apply_mutations(&org, &[fact_vector(rel)])
        .await
        .unwrap();
    let facts = graph
        .find_edges(
            &org,
            &kg_core::traits::EdgeLookup::LiveByChainPairs {
                pairs: vec![(a, b)],
            },
        )
        .await
        .unwrap();
    let fact = facts.iter().find(|r| r.uuid == rel).unwrap();
    assert_eq!(fact.stored["embedding"], json!([1.0, 0.0]));
    assert_eq!(fact.stored["embedding_content_hash"], "fact-content");
    assert!(graph
        .apply_mutations(&org, &[fact_vector(foreign_rel)])
        .await
        .is_err());
    assert!(graph
        .apply_mutations(&org, &[fact_vector(Uuid::new_v4())])
        .await
        .is_err());
}

/// A second live version for a chain that already has one is rejected without a write.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn one_chain_has_one_live_version() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let s = seed(graph).await;
    let Seed { org, .. } = &s;
    let org = org.clone();
    let unique = Uuid::new_v4();
    let duplicate = Uuid::new_v4();
    let version_of = |uuid| {
        M::UpsertEntity {
        uuid,
        properties:
            json!({"namespace":"prod","entity_type":"Service","chain_id":unique,"hash_version":format!("{unique}:1"),"is_latest":true})
                .as_object()
                .unwrap()
                .clone(),
    }
    };
    graph
        .apply_mutations(&org, &[version_of(unique)])
        .await
        .unwrap();
    assert!(graph
        .apply_mutations(&org, &[version_of(duplicate)])
        .await
        .is_err());
    assert!(node(graph, &org, unique, duplicate).await.is_null());
}

/// Once a chain has two live versions, no observation, edge, merge or split may touch it.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn ambiguous_live_chains_reject_every_mutation() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let s = seed(graph).await;
    let Seed { org, a, .. } = &s;
    let org = org.clone();
    let a = *a;
    let duplicate_chain = Uuid::new_v4();
    let split_loser = Uuid::new_v4();
    graph
        .apply_mutations(
            &org,
            &[
                entity(Uuid::new_v4(), duplicate_chain),
                entity(split_loser, split_loser),
                M::MergeChains {
                    effective_at: at(),
                    loser_chain_id: split_loser,
                    winner_chain_id: duplicate_chain,
                    identity_hashes: vec![],
                },
                entity(Uuid::new_v4(), duplicate_chain),
            ],
        )
        .await
        .unwrap();
    for mutation in [
        M::ObserveEntity {
            chain_id: duplicate_chain,
            observed_at: at(),
            sync_generation: None,
            snapshot_id: None,
            collection: None,
        },
        edge(Uuid::new_v4(), duplicate_chain, a),
        M::MergeChains {
            effective_at: at(),
            loser_chain_id: Uuid::new_v4(),
            winner_chain_id: duplicate_chain,
            identity_hashes: vec![],
        },
        M::SplitChain {
            effective_at: at() + Duration::seconds(1),
            split_chain_id: split_loser,
            from_chain_id: duplicate_chain,
            identity_hashes: vec![],
        },
    ] {
        assert!(
            graph
                .apply_mutations(&org, std::slice::from_ref(&mutation))
                .await
                .is_err(),
            "ambiguous live chain accepted: {mutation:?}"
        );
    }
}

/// Every invalid mutation paired with a valid one leaves both organizations untouched.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn invalid_mutations_roll_back_the_whole_batch() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let s = seed(graph).await;
    let Seed {
        org,
        foreign,
        a,
        b,
        f,
        snap,
        foreign_snap,
        foreign_rel,
        ..
    } = &s;
    let org = org.clone();
    let foreign = foreign.clone();
    let (a, b, f, snap, foreign_snap, foreign_rel) =
        (*a, *b, *f, *snap, *foreign_snap, *foreign_rel);
    let before = node(graph, &org, a, a).await;
    assert_eq!(before["embedding"], json!([0.1, 0.2, 0.3]));
    assert_eq!(before["replicas"], 3);
    assert_eq!(before["cpu"], 0.5);
    assert_eq!(before["tags"], json!(["a", "b"]));
    let foreign_before = node(graph, &foreign, f, f).await;
    let patch = || json!({"name":"changed"}).as_object().unwrap().clone();
    let mut invalid = vec![
        entity(f, f),
        entity(a, b),
        M::UpsertSnapshot {
            uuid: a,
            properties: Default::default(),
        },
        edge(foreign_rel, a, b),
        M::RepointEntity {
            previous_uuid: a,
            new_uuid: b,
            chain_id: a,
        },
        M::MergeChains {
            effective_at: at(),
            loser_chain_id: f,
            winner_chain_id: a,
            identity_hashes: vec!["bad".into()],
        },
        M::SplitChain {
            effective_at: at() + Duration::seconds(1),
            split_chain_id: b,
            from_chain_id: a,
            identity_hashes: vec![],
        },
        M::SetEmbedding {
            uuid: f,
            embedding: GraphEmbedding {
                model: "m".into(),
                values: vec![1.0],
            },
            text_version: kg_core::embedding::TEXT_VERSION.into(),
            content_hash: "h".into(),
        },
    ];
    for bad in [f, Uuid::new_v4()] {
        invalid.extend([
            M::SupersedeEntity {
                uuid: bad,
                chain_id: bad,
                valid_to: at(),
            },
            M::UpdateEntity {
                uuid: bad,
                properties: patch(),
            },
            M::DeleteEntity {
                chain_id: bad,
                deleted_at: at(),
                deleted_by: None,
                reason: None,
            },
            M::ObserveEntity {
                chain_id: bad,
                observed_at: at(),
                sync_generation: Some(1),
                snapshot_id: None,
                collection: None,
            },
            M::UpdateEdge {
                uuid: bad,
                properties: patch(),
            },
            M::RepointEntity {
                previous_uuid: bad,
                new_uuid: a,
                chain_id: a,
            },
            M::MergeChains {
                effective_at: at(),
                loser_chain_id: a,
                winner_chain_id: bad,
                identity_hashes: vec![],
            },
            M::SplitChain {
                effective_at: at() + Duration::seconds(1),
                split_chain_id: bad,
                from_chain_id: a,
                identity_hashes: vec![],
            },
            edge(Uuid::new_v4(), a, bad),
            M::RecordObservation {
                uuid: Uuid::new_v4(),
                snapshot_uuid: snap,
                entity_uuid: bad,
                entity_chain_id: bad,
                observed_at: at(),
                reconciliations: Vec::new(),
            },
            M::SetEmbedding {
                uuid: bad,
                embedding: GraphEmbedding {
                    model: "m".into(),
                    values: vec![1.0],
                },
                text_version: kg_core::embedding::TEXT_VERSION.into(),
                content_hash: "h".into(),
            },
        ]);
    }
    invalid.push(M::UpdateEdge {
        uuid: foreign_rel,
        properties: patch(),
    });
    for bad in [foreign_snap, Uuid::new_v4(), a] {
        invalid.push(M::RecordObservation {
            uuid: Uuid::new_v4(),
            snapshot_uuid: bad,
            entity_uuid: a,
            entity_chain_id: a,
            observed_at: at(),
            reconciliations: Vec::new(),
        });
    }
    invalid.push(M::RecordObservation {
        uuid: Uuid::new_v4(),
        snapshot_uuid: snap,
        entity_uuid: a,
        entity_chain_id: b,
        observed_at: at(),
        reconciliations: Vec::new(),
    });
    for properties in [
        json!({"uuid":a}),
        json!({"org_id":foreign}),
        json!({"chain_id":b}),
        json!({"prop_nested":{"x":1}}),
        json!({"prop_mixed":[1,"x"]}),
        json!({"prop_large":u64::MAX}),
    ] {
        invalid.push(M::UpdateEntity {
            uuid: a,
            properties: properties.as_object().unwrap().clone(),
        });
    }
    for bad in invalid {
        let result = graph
            .apply_mutations(
                &org,
                &[
                    M::UpdateEntity {
                        uuid: a,
                        properties: patch(),
                    },
                    bad.clone(),
                ],
            )
            .await;
        assert!(result.is_err(), "accepted invalid mutation: {bad:?}");
        assert_eq!(
            node(graph, &org, a, a).await,
            before,
            "partial write from {bad:?}"
        );
        assert_eq!(node(graph, &foreign, f, f).await, foreign_before);
    }
}

/// Replaying observations, embeddings, supersession and edge closure writes once and keeps live pairs consistent.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn observations_embeddings_and_supersession_are_idempotent() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let s = seed(graph).await;
    let Seed {
        org,
        a,
        b,
        new,
        snap,
        rel,
        ..
    } = &s;
    let org = org.clone();
    let (a, b, new, snap, rel) = (*a, *b, *new, *snap, *rel);
    let observation = M::RecordObservation {
        uuid: rel,
        snapshot_uuid: snap,
        entity_uuid: a,
        entity_chain_id: a,
        observed_at: at(),
        reconciliations: Vec::new(),
    };
    graph
        .apply_mutations(&org, &[observation.clone(), observation])
        .await
        .unwrap();
    let embedding = M::SetEmbedding {
        uuid: a,
        embedding: GraphEmbedding {
            model: "text-v1".into(),
            values: vec![0.5, 0.5],
        },
        text_version: kg_core::embedding::TEXT_VERSION.into(),
        content_hash: "h".into(),
    };
    graph
        .apply_mutations(&org, &[embedding.clone(), embedding])
        .await
        .unwrap();
    assert_eq!(
        graph.get_entity_embedding(&org, a).await.unwrap(),
        Some(GraphEmbedding {
            model: "text-v1".into(),
            values: vec![0.5, 0.5],
        })
    );
    let successor = [
        M::SupersedeEntity {
            uuid: a,
            chain_id: a,
            valid_to: at(),
        },
        successor_entity(new, a, a),
        M::RepointEntity {
            previous_uuid: a,
            new_uuid: new,
            chain_id: a,
        },
    ];
    graph.apply_mutations(&org, &successor).await.unwrap();
    graph.apply_mutations(&org, &successor).await.unwrap();
    assert_eq!(node(graph, &org, a, a).await["is_latest"], false);
    assert_eq!(node(graph, &org, a, new).await["is_latest"], true);
    assert_eq!(live_pairs(graph, &org, a, b).await, 1);
    let updated = M::UpdateEdge {
        uuid: rel,
        properties: json!({"is_latest":false,"invalid_at":at()})
            .as_object()
            .unwrap()
            .clone(),
    };
    assert!(graph
        .apply_mutations(
            &org,
            &[updated.clone(), edge(Uuid::new_v4(), a, Uuid::new_v4())]
        )
        .await
        .is_err());
    assert_eq!(live_pairs(graph, &org, a, b).await, 1);
    graph
        .apply_mutations(&org, &[updated.clone(), updated])
        .await
        .unwrap();
    assert_eq!(live_pairs(graph, &org, a, b).await, 0);
}

/// Aliases union across merges, survive supersession, resolve to the newest version and stay on tombstones.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn merges_and_splits_carry_aliases_across_versions_and_tombstones() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let s = seed(graph).await;
    let Seed { org, a, b, new, .. } = &s;
    let org = org.clone();
    let (a, b, new) = (*a, *b, *new);
    supersede(graph, &s).await;
    let merge = M::MergeChains {
        effective_at: at(),
        loser_chain_id: b,
        winner_chain_id: a,
        identity_hashes: vec!["alias".into()],
    };
    graph
        .apply_mutations(&org, &[merge.clone(), merge])
        .await
        .unwrap();
    let split = M::SplitChain {
        effective_at: at() + Duration::seconds(1),
        split_chain_id: b,
        from_chain_id: a,
        identity_hashes: vec!["alias".into()],
    };
    graph
        .apply_mutations(&org, &[split.clone(), split])
        .await
        .unwrap();
    assert_eq!(node(graph, &org, b, b).await["is_latest"], true);
    let absent = M::MergeChains {
        effective_at: at() + Duration::seconds(2),
        loser_chain_id: Uuid::new_v4(),
        winner_chain_id: a,
        identity_hashes: vec!["new-alias".into()],
    };
    graph
        .apply_mutations(&org, &[absent.clone(), absent])
        .await
        .unwrap();
    assert_eq!(
        node(graph, &org, a, new).await["identity_hashes"],
        json!(["new-alias"])
    );
    let by_alias = graph
        .find_entities(
            &org,
            &EntityLookup::ByIdentity {
                hashes: vec!["new-alias".into()],
                state: VersionState::Live,
            },
        )
        .await
        .unwrap();
    assert_eq!(by_alias.len(), 1);
    assert_eq!(by_alias[0].uuid, new);
    // Aliases belong to the chain: every later version carries the union of
    // every merge so far and answers to each alias.
    let successor = Uuid::new_v4();
    graph
        .apply_mutations(
            &org,
            &[
                M::SupersedeEntity {
                    uuid: new,
                    chain_id: a,
                    valid_to: at() + Duration::seconds(3),
                },
                M::UpsertEntity {
                    uuid: successor,
                    properties: json!({"namespace":"prod","entity_type":"Service","chain_id":a,"name":"api","is_latest":true,"version":3,"previous_version_uuid":new,"valid_from":at()+Duration::seconds(3),"last_seen_at":at()+Duration::seconds(3)})
                        .as_object()
                        .unwrap()
                        .clone(),
                },
                M::RepointEntity {
                    previous_uuid: new,
                    new_uuid: successor,
                    chain_id: a,
                },
                M::MergeChains {
                    effective_at: at() + Duration::seconds(3),
                    loser_chain_id: Uuid::new_v4(),
                    winner_chain_id: a,
                    identity_hashes: vec!["second-alias".into()],
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        node(graph, &org, a, successor).await["identity_hashes"],
        json!(["new-alias", "second-alias"]),
        "aliases union across merges and survive supersession"
    );
    for alias in ["new-alias", "second-alias"] {
        let hits = graph
            .find_entities(
                &org,
                &EntityLookup::ByIdentity {
                    hashes: vec![alias.into()],
                    state: VersionState::Live,
                },
            )
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "{alias}");
        assert_eq!(
            hits[0].uuid, successor,
            "{alias} resolves to the latest version"
        );
    }
    let new = successor;
    let delete = M::DeleteEntity {
        chain_id: a,
        deleted_at: at() + Duration::seconds(4),
        deleted_by: Some("sweep".into()),
        reason: Some("absent".into()),
    };
    graph
        .apply_mutations(&org, &[delete.clone(), delete])
        .await
        .unwrap();
    assert_eq!(node(graph, &org, a, new).await["deleted_by"], "sweep");
    let tombstones = graph
        .find_entities(
            &org,
            &EntityLookup::ByIdentity {
                hashes: vec!["new-alias".into()],
                state: VersionState::Deleted,
            },
        )
        .await
        .unwrap();
    assert_eq!(tombstones.len(), 1, "the deleted version is a tombstone");
    assert!(tombstones[0].deleted_at.is_some());
}

fn header(org: &str, run_id: Uuid, fingerprint: &RequestFingerprint) -> RunHeader {
    RunHeader {
        observation_manifest: Default::default(),
        rule_freezes: vec![],
        schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
            profiles: Default::default(),
            org_id: org.into(),
            sources: Default::default(),
        },
        org_id: org.into(),
        run_id,
        fingerprint: fingerprint.clone(),
        settings_version: "v1".into(),
        capture_default: at(),
        batch_plan: vec![],
    }
}

fn batch(
    org: &str,
    run_id: Uuid,
    fingerprint: &RequestFingerprint,
    kind: BatchKind,
    index: u32,
    preconditions: Vec<Precondition>,
    mutations: Vec<M>,
) -> MutationBatch {
    MutationBatch {
        org_id: org.into(),
        batch: BatchIdentity {
            run_id,
            kind,
            index,
        },
        fingerprint: fingerprint.clone(),
        preconditions,
        mutations,
        result: json!({"kind": kind.label(), "index": index}),
    }
}

fn identity_entity(org: &str, uuid: Uuid, chain: Uuid, hash: &str, version: u32) -> M {
    M::UpsertEntity {
        uuid,
        properties: json!({
            "chain_id": chain, "name": "api", "namespace": "prod", "entity_type": "Service",
            "identity_hash": hash, "hash_version": format!("{org}:{hash}:{version}"),
            "is_latest": true, "version": version, "valid_from": at().to_rfc3339(), "last_seen_at": at().to_rfc3339(),
            "previous_version_uuid": (version>1).then_some(chain),
        })
        .as_object()
        .unwrap()
        .clone(),
    }
}

/// One registered run with its first node batch committed: entity `a` (chain `a`,
/// identity `hash`, version 1), snapshot `snap`, an observation and an embedding.
struct Run {
    org: String,
    run: Uuid,
    fingerprint: RequestFingerprint,
    other_fingerprint: RequestFingerprint,
    a: Uuid,
    a2: Uuid,
    b: Uuid,
    snap: Uuid,
    rel: Uuid,
    hash: String,
}

fn fresh_run() -> Run {
    let a = Uuid::new_v4();
    Run {
        org: format!("commit-{}", Uuid::new_v4()),
        run: Uuid::new_v4(),
        fingerprint: RequestFingerprint(format!("{:032x}", 1u128)),
        other_fingerprint: RequestFingerprint(format!("{:032x}", 2u128)),
        a,
        a2: Uuid::new_v4(),
        b: Uuid::new_v4(),
        snap: Uuid::new_v4(),
        rel: Uuid::new_v4(),
        hash: format!("hash-{a}"),
    }
}

fn first_batch(r: &Run) -> MutationBatch {
    let (org, run, fingerprint, a, snap, hash) =
        (&r.org, r.run, &r.fingerprint, r.a, r.snap, &r.hash);
    batch(
        org,
        run,
        fingerprint,
        BatchKind::Node,
        0,
        vec![Precondition::NoLiveVersionFor {
            hashes: vec![hash.clone()],
        }],
        vec![
            identity_entity(org, a, a, hash, 1),
            M::UpsertSnapshot {
                uuid: snap,
                properties: json!({"name": "sync"}).as_object().unwrap().clone(),
            },
            M::RecordObservation {
                uuid: Uuid::new_v5(&snap, a.as_bytes()),
                snapshot_uuid: snap,
                entity_uuid: a,
                entity_chain_id: a,
                observed_at: at(),
                reconciliations: Vec::new(),
            },
            M::SetEmbedding {
                uuid: a,
                embedding: GraphEmbedding {
                    model: "text-v1".into(),
                    values: vec![0.25, 0.75],
                },
                text_version: kg_core::embedding::TEXT_VERSION.into(),
                content_hash: "h".into(),
            },
        ],
    )
}

/// Register the run and commit its first batch.
async fn registered(graph: &dyn GraphBackend) -> (Run, CommittedBatch) {
    let r = fresh_run();
    assert_eq!(
        graph
            .register_run(&header(&r.org, r.run, &r.fingerprint))
            .await
            .unwrap(),
        RunRegistration::Registered
    );
    let committed = graph.commit_batch(&first_batch(&r)).await.unwrap();
    (r, committed)
}

/// Supersede `a` with `a2` (version 2) in a receipted node batch.
async fn with_successor(graph: &dyn GraphBackend, r: &Run) {
    let (org, run, fingerprint, a, a2, hash) = (&r.org, r.run, &r.fingerprint, r.a, r.a2, &r.hash);
    let successor = batch(
        org,
        run,
        fingerprint,
        BatchKind::Node,
        1,
        vec![Precondition::LatestVersionIs {
            chain_id: a,
            uuid: a,
            version: 1,
        }],
        vec![
            M::SupersedeEntity {
                uuid: a,
                chain_id: a,
                valid_to: at(),
            },
            identity_entity(org, a2, a, hash, 2),
            M::RepointEntity {
                previous_uuid: a,
                new_uuid: a2,
                chain_id: a,
            },
        ],
    );
    assert!(!graph.commit_batch(&successor).await.unwrap().replayed);
    assert_eq!(node(graph, org, a, a2).await["is_latest"], true);
}

/// A batch cannot commit before its run is registered; registration is idempotent for the same fingerprint and organization and a conflict otherwise.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn commits_need_a_registered_run_with_the_same_fingerprint() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let r = fresh_run();
    let first = first_batch(&r);
    let Run {
        org,
        run,
        fingerprint,
        other_fingerprint,
        a,
        ..
    } = r;
    assert!(matches!(
        graph.commit_batch(&first).await,
        Err(BackendError::Conflict(_))
    ));
    assert!(node(graph, &org, a, a).await.is_null(), "nothing written");

    assert_eq!(
        graph
            .register_run(&header(&org, run, &fingerprint))
            .await
            .unwrap(),
        RunRegistration::Registered
    );
    assert_eq!(
        graph
            .register_run(&header(&org, run, &fingerprint))
            .await
            .unwrap(),
        RunRegistration::Resumed {
            observation_manifest: Default::default(),
            schema_manifest: header(&org, run, &fingerprint).schema_manifest,
            capture_default: at(),
            committed: vec![]
        }
    );
    assert!(matches!(
        graph
            .register_run(&header(&org, run, &other_fingerprint))
            .await,
        Err(BackendError::Conflict(_))
    ));
    assert!(matches!(
        graph
            .register_run(&header("other-org", run, &fingerprint))
            .await,
        Err(BackendError::Conflict(_))
    ));

    let committed = graph.commit_batch(&first).await.unwrap();
    assert!(!committed.replayed);
    assert_eq!(committed.batch_id, first.batch_id());
    assert_eq!(committed.result, json!({"kind": "node", "index": 0}));
    assert_eq!(node(graph, &org, a, a).await["embedding_model"], "text-v1");
}

/// The same batch commits once; a replay returns the receipt and a different request under the same identity is rejected.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn replay_returns_the_stored_result_and_writes_nothing() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, committed) = registered(graph).await;
    let first = first_batch(&r);
    let Run {
        other_fingerprint, ..
    } = r;
    // Replay returns the stored result and writes nothing.
    let replayed = graph.commit_batch(&first).await.unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.result, committed.result);
    assert_eq!(replayed.batch_id, committed.batch_id);
    let mut different = first.clone();
    different.fingerprint = other_fingerprint.clone();
    assert!(
        matches!(
            graph.commit_batch(&different).await,
            Err(BackendError::Conflict(_))
        ),
        "same identity with a different request is rejected"
    );
}

/// A failed precondition or a failing mutation writes nothing and leaves no receipt.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn failed_preconditions_and_mutations_leave_no_receipt_and_no_partial_write() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    let Run {
        org,
        run,
        fingerprint,
        b,
        hash,
        ..
    } = r;
    // A failed precondition writes nothing and leaves no receipt.
    let stale = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        1,
        vec![Precondition::NoLiveVersionFor {
            hashes: vec![hash.clone()],
        }],
        vec![identity_entity(&org, b, b, "hash-b", 1)],
    );
    assert!(matches!(
        graph.commit_batch(&stale).await,
        Err(BackendError::Conflict(_))
    ));
    assert!(node(graph, &org, b, b).await.is_null());
    assert_eq!(
        graph.committed_batches(&org, run).await.unwrap().len(),
        1,
        "no receipt for a rejected batch"
    );

    // A failed mutation rolls back everything before it in the batch.
    let partial = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        1,
        vec![],
        vec![
            identity_entity(&org, b, b, "hash-b", 1),
            edge(Uuid::new_v4(), b, Uuid::new_v4()),
        ],
    );
    assert!(matches!(
        graph.commit_batch(&partial).await,
        Err(BackendError::NotFound(_))
    ));
    assert!(node(graph, &org, b, b).await.is_null(), "rolled back");
}

/// Supersession commits against the planned version and a plan built on a superseded version is stale.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn version_transitions_require_the_planned_current_version() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    let Run {
        org,
        run,
        fingerprint,
        a,
        a2,
        hash,
        ..
    } = r;
    // Version transitions require the planned current version.
    let successor = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        1,
        vec![Precondition::LatestVersionIs {
            chain_id: a,
            uuid: a,
            version: 1,
        }],
        vec![
            M::SupersedeEntity {
                uuid: a,
                chain_id: a,
                valid_to: at(),
            },
            identity_entity(&org, a2, a, &hash, 2),
            M::RepointEntity {
                previous_uuid: a,
                new_uuid: a2,
                chain_id: a,
            },
        ],
    );
    assert!(!graph.commit_batch(&successor).await.unwrap().replayed);
    assert_eq!(node(graph, &org, a, a2).await["is_latest"], true);
    let outdated = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        2,
        vec![Precondition::LatestVersionIs {
            chain_id: a,
            uuid: a,
            version: 1,
        }],
        vec![],
    );
    assert!(
        matches!(
            graph.commit_batch(&outdated).await,
            Err(BackendError::Conflict(_))
        ),
        "a plan built on a superseded version is stale"
    );
}

/// An observation newer than the fence commits; an older one is a conflict.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn observation_freshness_fences_older_observations() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    with_successor(graph, &r).await;
    let Run {
        org,
        run,
        fingerprint,
        a,
        a2,
        ..
    } = r;
    // Observation freshness.
    let observe = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        2,
        vec![Precondition::NotObservedAfter {
            uuid: a2,
            observed_at: at() + Duration::hours(1),
        }],
        vec![M::ObserveEntity {
            chain_id: a,
            observed_at: at() + Duration::hours(1),
            sync_generation: Some(2),
            snapshot_id: None,
            collection: None,
        }],
    );
    assert!(!graph.commit_batch(&observe).await.unwrap().replayed);
    let older = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        3,
        vec![Precondition::NotObservedAfter {
            uuid: a2,
            observed_at: at(),
        }],
        vec![],
    );
    assert!(matches!(
        graph.commit_batch(&older).await,
        Err(BackendError::Conflict(_))
    ));
}

/// Live-set, version and observation fences on relationships accept the planned state and reject every other.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn relationship_preconditions_fence_live_sets_versions_and_observations() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    let Run {
        org,
        run,
        fingerprint,
        a,
        b,
        rel,
        ..
    } = r;
    // Relationship preconditions.
    graph
        .apply_mutations(&org, &[identity_entity(&org, b, b, "hash-b", 1)])
        .await
        .unwrap();
    let link = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        0,
        vec![Precondition::LiveEdgesForPairAre {
            source_chain_id: a,
            target_chain_id: b,
            uuids: vec![],
        }],
        vec![edge(rel, a, b)],
    );
    assert!(!graph.commit_batch(&link).await.unwrap().replayed);
    let duplicate_link = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        1,
        vec![Precondition::LiveEdgesForPairAre {
            source_chain_id: a,
            target_chain_id: b,
            uuids: vec![],
        }],
        vec![],
    );
    assert!(matches!(
        graph.commit_batch(&duplicate_link).await,
        Err(BackendError::Conflict(_))
    ));
    let current_edge = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        1,
        vec![
            Precondition::LiveEdgesForPairAre {
                source_chain_id: a,
                target_chain_id: b,
                uuids: vec![rel],
            },
            Precondition::EdgeIsLatest {
                uuid: rel,
                version: 1,
            },
            Precondition::EdgeNotObservedAfter {
                uuid: rel,
                observed_at: at(),
            },
        ],
        vec![M::UpdateEdge {
            uuid: rel,
            properties: json!({"last_seen_at": at().to_rfc3339()})
                .as_object()
                .unwrap()
                .clone(),
        }],
    );
    assert!(!graph.commit_batch(&current_edge).await.unwrap().replayed);
    let stale_edge_observation = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        2,
        vec![Precondition::EdgeNotObservedAfter {
            uuid: rel,
            observed_at: at() - Duration::hours(1),
        }],
        vec![M::UpdateEdge {
            uuid: rel,
            properties: json!({"last_seen_at":(at() - Duration::hours(1)).to_rfc3339()})
                .as_object()
                .unwrap()
                .clone(),
        }],
    );
    assert!(matches!(
        graph.commit_batch(&stale_edge_observation).await,
        Err(BackendError::Conflict(_))
    ));
    // Closing a current target: an observation at the closing time is a
    // contradiction, so the strict fence rejects it; strictly older passes.
    let closed_at_observation = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        2,
        vec![Precondition::EdgeObservedBefore {
            uuid: rel,
            observed_at: at(),
        }],
        vec![],
    );
    assert!(matches!(
        graph.commit_batch(&closed_at_observation).await,
        Err(BackendError::Conflict(_))
    ));
    let closed_after_observation = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        3,
        vec![Precondition::EdgeObservedBefore {
            uuid: rel,
            observed_at: at() + Duration::hours(1),
        }],
        vec![],
    );
    assert!(
        !graph
            .commit_batch(&closed_after_observation)
            .await
            .unwrap()
            .replayed
    );
    let wrong_edge_version = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        2,
        vec![Precondition::EdgeIsLatest {
            uuid: rel,
            version: 2,
        }],
        vec![],
    );
    assert!(matches!(
        graph.commit_batch(&wrong_edge_version).await,
        Err(BackendError::Conflict(_))
    ));
}

/// A concurrent target for a single-target relation rejects the commit; another relation name and a missing source behave as planned.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn single_target_relations_reject_concurrent_targets_only() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    let Run {
        org,
        run,
        fingerprint,
        a,
        b,
        ..
    } = r;
    graph
        .apply_mutations(&org, &[identity_entity(&org, b, b, "hash-b", 1)])
        .await
        .unwrap();
    // A single-target relation's live set must be exactly what was planned:
    // a concurrent target for the same relation rejects the commit, another
    // relation name is unaffected.
    let (placement, placed) = (Uuid::new_v4(), Uuid::new_v4());
    graph
        .apply_mutations(
            &org,
            &[identity_entity(
                &org,
                placement,
                placement,
                "hash-placement",
                1,
            )],
        )
        .await
        .unwrap();
    let deployed = |uuid: Uuid, target: Uuid| M::UpsertEdge {
        uuid,
        source_chain_id: a,
        target_chain_id: target,
        properties: json!({"name":"DEPLOYED_IN","is_latest":true})
            .as_object()
            .unwrap()
            .clone(),
    };
    let relation = |uuids: Vec<Uuid>| Precondition::LiveEdgesForRelationAre {
        source_chain_id: a,
        name: "DEPLOYED_IN".into(),
        uuids,
    };
    let absent = Uuid::new_v4();
    for (index, check) in [
        Precondition::LiveEdgesForRelationAre {
            source_chain_id: absent,
            name: "DEPLOYED_IN".into(),
            uuids: vec![],
        },
        Precondition::LiveIncidentEdgesAre {
            chain_id: absent,
            uuids: vec![],
        },
    ]
    .into_iter()
    .enumerate()
    {
        let missing_source = batch(
            &org,
            run,
            &fingerprint,
            BatchKind::Relationship,
            90 + index as u32,
            vec![check],
            vec![],
        );
        assert!(
            matches!(
                graph.commit_batch(&missing_source).await,
                Err(BackendError::Conflict(_))
            ),
            "an empty relationship set must not make a missing source valid"
        );
    }
    let first_target = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        30,
        vec![relation(vec![])],
        vec![deployed(placed, b)],
    );
    assert!(!graph.commit_batch(&first_target).await.unwrap().replayed);
    let unseen_target = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        31,
        vec![relation(vec![])],
        vec![deployed(Uuid::new_v4(), placement)],
    );
    assert!(matches!(
        graph.commit_batch(&unseen_target).await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(
        graph
            .find_edges(
                &org,
                &EdgeLookup::LiveByEndpointChains {
                    chain_ids: vec![placement]
                }
            )
            .await
            .unwrap()
            .len(),
        0,
        "the rejected target was not written"
    );
    let seen_target = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        31,
        vec![relation(vec![placed])],
        vec![],
    );
    assert!(!graph.commit_batch(&seen_target).await.unwrap().replayed);
    let other_relation = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        32,
        vec![Precondition::LiveEdgesForRelationAre {
            source_chain_id: a,
            name: "RUNS_ON".into(),
            uuids: vec![],
        }],
        vec![],
    );
    assert!(!graph.commit_batch(&other_relation).await.unwrap().replayed);
}

/// Restoration commits only against the newest tombstone and only when captured after the deletion.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn restoration_requires_the_newest_tombstone() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    with_successor(graph, &r).await;
    let Run {
        org,
        run,
        fingerprint,
        a,
        a2,
        ..
    } = r;
    // Restoration requires the newest tombstone.
    let delete = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Reconciliation,
        0,
        vec![Precondition::LatestVersionIs {
            chain_id: a,
            uuid: a2,
            version: 2,
        }],
        vec![M::DeleteEntity {
            chain_id: a,
            deleted_at: at() + Duration::hours(2),
            deleted_by: Some("sweep".into()),
            reason: None,
        }],
    );
    assert!(!graph.commit_batch(&delete).await.unwrap().replayed);
    let restore_old = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        3,
        vec![Precondition::LatestDeletedVersionIs {
            chain_id: a,
            uuid: a,
            restored_at: at() + Duration::hours(3),
        }],
        vec![],
    );
    assert!(matches!(
        graph.commit_batch(&restore_old).await,
        Err(BackendError::Conflict(_))
    ));
    // A restoration captured at the deletion time or before it loses.
    for restored_at in [at() + Duration::hours(2), at() + Duration::hours(1)] {
        let restore_stale = batch(
            &org,
            run,
            &fingerprint,
            BatchKind::Node,
            3,
            vec![Precondition::LatestDeletedVersionIs {
                chain_id: a,
                uuid: a2,
                restored_at,
            }],
            vec![],
        );
        assert!(
            matches!(
                graph.commit_batch(&restore_stale).await,
                Err(BackendError::Conflict(_))
            ),
            "restoration at {restored_at} against a deletion at {}",
            at() + Duration::hours(2)
        );
    }
    let restore = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        3,
        vec![Precondition::LatestDeletedVersionIs {
            chain_id: a,
            uuid: a2,
            restored_at: at() + Duration::hours(3),
        }],
        vec![],
    );
    assert!(!graph.commit_batch(&restore).await.unwrap().replayed);
}

/// Receipts come back in kind then index order, registration resumes with them,
/// and another organization sees none.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn receipts_recover_committed_progress_in_kind_then_index_order() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    with_successor(graph, &r).await;
    let Run {
        org,
        run,
        fingerprint,
        a,
        a2,
        b,
        rel,
        ..
    } = r;
    graph
        .apply_mutations(&org, &[identity_entity(&org, b, b, "hash-b", 1)])
        .await
        .unwrap();
    for planned in [
        batch(
            &org,
            run,
            &fingerprint,
            BatchKind::Relationship,
            0,
            vec![],
            vec![edge(rel, a, b)],
        ),
        batch(
            &org,
            run,
            &fingerprint,
            BatchKind::Relationship,
            3,
            vec![],
            vec![],
        ),
        batch(
            &org,
            run,
            &fingerprint,
            BatchKind::Reconciliation,
            0,
            vec![Precondition::LatestVersionIs {
                chain_id: a,
                uuid: a2,
                version: 2,
            }],
            vec![],
        ),
    ] {
        assert!(!graph.commit_batch(&planned).await.unwrap().replayed);
    }
    let committed = graph.committed_batches(&org, run).await.unwrap();
    let order: Vec<(BatchKind, u32)> = committed.iter().map(|b| (b.kind, b.index)).collect();
    assert_eq!(
        order,
        vec![
            (BatchKind::Node, 0),
            (BatchKind::Node, 1),
            (BatchKind::Relationship, 0),
            (BatchKind::Relationship, 3),
            (BatchKind::Reconciliation, 0),
        ]
    );
    assert!(committed.iter().all(|b| b.replayed && b.run_id == run));
    assert_eq!(
        graph
            .register_run(&header(&org, run, &fingerprint))
            .await
            .unwrap(),
        RunRegistration::Resumed {
            observation_manifest: Default::default(),
            schema_manifest: header(&org, run, &fingerprint).schema_manifest,
            capture_default: at(),
            committed
        }
    );
    assert!(graph
        .committed_batches("other-org", run)
        .await
        .unwrap()
        .is_empty());
}

/// Two concurrent executions of one batch: one writes, the other replays.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn duplicate_execution_of_one_batch_writes_once() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    let Run {
        org, fingerprint, ..
    } = r;
    // Duplicate execution of one batch: exactly one attempt writes.
    let run2 = Uuid::new_v4();
    graph
        .register_run(&header(&org, run2, &fingerprint))
        .await
        .unwrap();
    let c = Uuid::new_v4();
    let twice = batch(
        &org,
        run2,
        &fingerprint,
        BatchKind::Node,
        0,
        vec![Precondition::NoLiveVersionFor {
            hashes: vec!["hash-c".into()],
        }],
        vec![identity_entity(&org, c, c, "hash-c", 1)],
    );
    let (left, right) = tokio::join!(graph.commit_batch(&twice), graph.commit_batch(&twice));
    let fresh = [&left, &right]
        .iter()
        .filter(|r| matches!(r, Ok(b) if !b.replayed))
        .count();
    let other = [&left, &right]
        .iter()
        .filter(|r| matches!(r, Ok(b) if b.replayed))
        .count();
    assert_eq!((fresh, other), (1, 1), "{left:?} / {right:?}");
    assert_eq!(graph.committed_batches(&org, run2).await.unwrap().len(), 1);
}

/// Two runs creating the same identity at once: exactly one commits, the loser is a replannable conflict.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn concurrent_creation_of_one_identity_has_one_winner() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    let Run {
        org, fingerprint, ..
    } = r;
    // Concurrent creation of one identity from two runs: at most one wins.
    let run3 = Uuid::new_v4();
    let run4 = Uuid::new_v4();
    for run in [run3, run4] {
        graph
            .register_run(&header(&org, run, &fingerprint))
            .await
            .unwrap();
    }
    let contested = "hash-contested".to_string();
    let (d, e) = (Uuid::new_v4(), Uuid::new_v4());
    let creation = |run, uuid| {
        batch(
            &org,
            run,
            &fingerprint,
            BatchKind::Node,
            0,
            vec![Precondition::NoLiveVersionFor {
                hashes: vec![contested.clone()],
            }],
            vec![identity_entity(&org, uuid, uuid, &contested, 1)],
        )
    };
    let (left_batch, right_batch) = (creation(run3, d), creation(run4, e));
    let (left, right) = tokio::join!(
        graph.commit_batch(&left_batch),
        graph.commit_batch(&right_batch)
    );
    assert!(
        left.is_ok() != right.is_ok(),
        "exactly one creation may commit: {left:?} / {right:?}"
    );
    let loser = if let Err(error) = &left {
        error
    } else {
        right.as_ref().unwrap_err()
    };
    assert!(
        matches!(loser, BackendError::Conflict(_)),
        "identity race must be replannable: {loser:?}"
    );
    let live = graph
        .find_entities(
            &org,
            &EntityLookup::ByIdentity {
                hashes: vec![contested.clone()],
                state: VersionState::Live,
            },
        )
        .await
        .unwrap();
    assert_eq!(live.len(), 1, "one live version for the contested identity");
}

/// An oversized batch is rejected before any statement runs.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn budgets_are_checked_before_any_write() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let graph: &dyn GraphBackend = &graph;
    let (r, _) = registered(graph).await;
    let Run {
        org,
        run,
        fingerprint,
        ..
    } = r;
    // Budgets are checked before any write.
    let mut oversized = batch(&org, run, &fingerprint, BatchKind::Node, 9, vec![], vec![]);
    oversized.mutations = vec![
        M::ObserveEntity {
            chain_id: Uuid::new_v4(),
            observed_at: at(),
            sync_generation: None,
            snapshot_id: None,
            collection: None,
        };
        kg_core::traits::graph_commit::MAX_STATEMENTS_PER_BATCH + 1
    ];
    assert!(matches!(
        graph.commit_batch(&oversized).await,
        Err(BackendError::Query(_))
    ));
    assert_eq!(
        graph.committed_batches(&org, run).await.unwrap().len(),
        1,
        "only the seed batch"
    );
}

async fn neo4j(options: Neo4jOptions) -> Neo4jGraphBackend {
    let uri = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .uri;
    let password = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .password;
    let graph = Neo4jGraphBackend::with_options(&uri, "neo4j", &password, options)
        .await
        .unwrap();
    graph.ensure_indexes().await.unwrap();
    graph
}

/// Observations stay on the version that saw them; relationships follow the chain to its newest version.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn supersession_repoints_observations_and_relationships() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let org = format!("provenance-{}", Uuid::new_v4());
    let (old, new, snap, rel) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    graph
        .apply_mutations(
            &org,
            &[
                entity(old, old),
                M::UpsertSnapshot {
                    uuid: snap,
                    properties: Default::default(),
                },
                M::RecordObservation {
                    uuid: Uuid::new_v5(&snap, old.as_bytes()),
                    snapshot_uuid: snap,
                    entity_uuid: old,
                    entity_chain_id: old,
                    observed_at: at(),
                    reconciliations: Vec::new(),
                },
                edge(rel, old, old),
                M::SupersedeEntity {
                    uuid: old,
                    chain_id: old,
                    valid_to: at(),
                },
                successor_entity(new, old, old),
                M::RepointEntity {
                    previous_uuid: old,
                    new_uuid: new,
                    chain_id: old,
                },
            ],
        )
        .await
        .unwrap();
    let observed=graph.execute_read("MATCH (s:Snapshot {org_id:$org})-[r:MENTIONS]->(n) RETURN s.uuid AS source,n.uuid AS target",&json!({"org":org})).await.unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0]["source"], json!(snap));
    assert_eq!(observed[0]["target"], json!(old));
    let domain = graph
        .execute_read(
            "MATCH (s)-[r:RELATES_TO {uuid:$uuid}]->(t) RETURN s.uuid AS source,t.uuid AS target",
            &json!({"uuid":rel}),
        )
        .await
        .unwrap();
    assert_eq!(domain.len(), 1);
    assert_eq!(domain[0]["source"], json!(new));
    assert_eq!(domain[0]["target"], json!(new));
}

/// Commit acknowledgement loss and pre-commit connection loss are simulated by
/// injected faults in the adapter; the server, transaction, and receipt are real.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn neo4j_commit_outcome_recovery() {
    let faults = Arc::new(FaultInjection::default());
    let graph = neo4j(Neo4jOptions {
        max_retries: 2,
        faults: Some(Arc::clone(&faults)),
        ..Neo4jOptions::default()
    })
    .await;
    let org = format!("recovery-{}", Uuid::new_v4());
    let run = Uuid::new_v4();
    let fingerprint = RequestFingerprint(format!("{:032x}", 7u128));
    graph
        .register_run(&header(&org, run, &fingerprint))
        .await
        .unwrap();
    let a = Uuid::new_v4();
    let first = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        0,
        vec![],
        vec![identity_entity(&org, a, a, "hash-recovery-a", 1)],
    );

    // The server commits, the acknowledgement is lost: the receipt proves the outcome.
    faults
        .lose_commit_ack
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let committed = graph.commit_batch(&first).await.unwrap();
    assert!(!committed.replayed, "recovered as this attempt's commit");
    assert_eq!(graph.committed_batches(&org, run).await.unwrap().len(), 1);
    assert_eq!(node(&graph, &org, a, a).await["is_latest"], true);

    // The connection drops before commit: no receipt, so the retry commits.
    let b = Uuid::new_v4();
    let second = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        1,
        vec![],
        vec![identity_entity(&org, b, b, "hash-recovery-b", 1)],
    );
    faults
        .drop_before_commit
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(!graph.commit_batch(&second).await.unwrap().replayed);
    assert_eq!(graph.committed_batches(&org, run).await.unwrap().len(), 2);

    // With retries exhausted the failure is reported and nothing is committed.
    let c = Uuid::new_v4();
    let third = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        2,
        vec![],
        vec![identity_entity(&org, c, c, "hash-recovery-c", 1)],
    );
    faults
        .drop_before_commit
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        graph.commit_batch(&third).await,
        Err(BackendError::Connection(_))
    ));
    assert!(node(&graph, &org, c, c).await.is_null());
    assert_eq!(graph.committed_batches(&org, run).await.unwrap().len(), 2);
    assert!(!graph.commit_batch(&third).await.unwrap().replayed);
    assert_eq!(graph.committed_batches(&org, run).await.unwrap().len(), 3);
}

/// Once the commit request is sent, an absent receipt proves nothing. The
/// injected fault commits on the server, loses the acknowledgement, and
/// holds (or fails) the receipt checks: a check that eventually sees the
/// receipt recovers the commit; checks exhausted report an unknown outcome
/// without repeating the write, and the receipt later proves the commit.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn neo4j_uncertain_commit_is_reported_not_repeated() {
    use std::sync::atomic::Ordering::SeqCst;
    let faults = Arc::new(FaultInjection::default());
    let graph = neo4j(Neo4jOptions {
        max_retries: 2,
        commit_verification_attempts: 3,
        base_backoff: Duration::milliseconds(20).to_std().unwrap(),
        faults: Some(Arc::clone(&faults)),
        ..Neo4jOptions::default()
    })
    .await;
    let org = format!("uncertain-{}", Uuid::new_v4());
    let run = Uuid::new_v4();
    let fingerprint = RequestFingerprint(format!("{:032x}", 11u128));
    graph
        .register_run(&header(&org, run, &fingerprint))
        .await
        .unwrap();
    let versions = |chain: Uuid| {
        let graph = &graph;
        let org = org.clone();
        async move {
            graph
                .execute_read(
                    "MATCH (n:Entity {org_id:$org, chain_id:$chain}) RETURN count(n) AS n",
                    &json!({ "org": org, "chain": chain }),
                )
                .await
                .unwrap()[0]["n"]
                .as_u64()
                .unwrap()
        }
    };

    // Fault: server commits, acknowledgement lost, receipt held for two of
    // three checks. The third check finds it: this attempt's commit.
    let a = Uuid::new_v4();
    let first = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        0,
        vec![],
        vec![identity_entity(&org, a, a, "hash-uncertain-a", 1)],
    );
    faults.lose_commit_ack.store(1, SeqCst);
    faults.hold_receipt.store(2, SeqCst);
    let committed = graph.commit_batch(&first).await.unwrap();
    assert!(!committed.replayed);
    assert_eq!(versions(a).await, 1);
    assert_eq!(faults.hold_receipt.load(SeqCst), 0);

    // Fault: receipt held for every check. The outcome is unknown, nothing
    // is repeated, the receipt proves the commit afterwards, and a retry
    // replays it instead of writing again.
    let b = Uuid::new_v4();
    let second = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        1,
        vec![],
        vec![identity_entity(&org, b, b, "hash-uncertain-b", 1)],
    );
    faults.lose_commit_ack.store(1, SeqCst);
    faults.hold_receipt.store(3, SeqCst);
    let error = graph.commit_batch(&second).await.unwrap_err();
    let BackendError::UnknownCommit(message) = &error else {
        panic!("expected UnknownCommit, got {error:?}");
    };
    assert!(message.contains("no receipt after 3 check(s)"), "{message}");
    assert!(error.is_transient());
    assert_eq!(
        graph.committed_batches(&org, run).await.unwrap().len(),
        2,
        "the commit landed"
    );
    assert_eq!(versions(b).await, 1);
    assert!(graph.commit_batch(&second).await.unwrap().replayed);
    assert_eq!(versions(b).await, 1, "the retry wrote nothing");

    // Fault: every check fails to read. Still unknown, still not repeated.
    let c = Uuid::new_v4();
    let third = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        2,
        vec![],
        vec![identity_entity(&org, c, c, "hash-uncertain-c", 1)],
    );
    faults.lose_commit_ack.store(1, SeqCst);
    faults.fail_receipt.store(3, SeqCst);
    let error = graph.commit_batch(&third).await.unwrap_err();
    let BackendError::UnknownCommit(message) = &error else {
        panic!("expected UnknownCommit, got {error:?}");
    };
    assert!(
        message.contains("receipt check failed 3 time(s)"),
        "{message}"
    );
    assert!(graph.commit_batch(&third).await.unwrap().replayed);
    assert_eq!(versions(c).await, 1);

    // A single check that sees nothing is not proof of rollback either.
    let single = neo4j(Neo4jOptions {
        max_retries: 1,
        commit_verification_attempts: 1,
        faults: Some(Arc::clone(&faults)),
        ..Neo4jOptions::default()
    })
    .await;
    let d = Uuid::new_v4();
    let fourth = batch(
        &org,
        run,
        &fingerprint,
        BatchKind::Node,
        3,
        vec![],
        vec![identity_entity(&org, d, d, "hash-uncertain-d", 1)],
    );
    faults.lose_commit_ack.store(1, SeqCst);
    faults.hold_receipt.store(1, SeqCst);
    assert!(matches!(
        single.commit_batch(&fourth).await,
        Err(BackendError::UnknownCommit(_))
    ));
    assert!(single.commit_batch(&fourth).await.unwrap().replayed);
    assert_eq!(graph.committed_batches(&org, run).await.unwrap().len(), 4);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn relationship_creation_rechecks_target_after_waiting_for_its_lock() {
    let uri = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .uri;
    let password = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .password;
    let graph = Arc::new(
        Neo4jGraphBackend::new(&uri, "neo4j", &password)
            .await
            .unwrap(),
    );
    graph.ensure_indexes().await.unwrap();
    for deleting_source in [false, true] {
        let org = format!("endpoint-race-{}", Uuid::new_v4());
        let source = Uuid::new_v4();
        let target = Uuid::new_v4();
        let edge_id = Uuid::new_v4();
        let locked = if deleting_source { source } else { target };
        graph
            .apply_mutations(&org, &[entity(source, source), entity(target, target)])
            .await
            .unwrap();
        let control = neo4rs::Graph::new(&uri, "neo4j", &password).await.unwrap();
        let mut deleting = control.start_txn().await.unwrap();
        deleting
            .run(
                neo4rs::query("MATCH (n:Entity {uuid:$uuid}) SET n.uuid=n.uuid")
                    .param("uuid", locked.to_string()),
            )
            .await
            .unwrap();
        let writer = graph.clone();
        let writer_org = org.clone();
        let creating = tokio::spawn(async move {
            writer
                .apply_mutations(&writer_org, &[edge(edge_id, source, target)])
                .await
        });
        // Endpoint locks can be acquired by summary invalidation before the
        // relationship statement. Identify this writer by its unique scope,
        // rather than by whichever statement currently holds up the transaction.
        let blocked = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if creating.is_finished() {
                    return Err("relationship writer finished before blocking".to_owned());
                }
                let blocked = graph.execute_read(
                    "SHOW TRANSACTIONS YIELD status,parameters WHERE status STARTS WITH 'Blocked' AND parameters.org=$org RETURN status",
                    &json!({"org":org}),
                ).await.map_err(|error| format!("blocked-writer probe failed: {error}"))?;
                if !blocked.is_empty() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }).await;
        if !matches!(&blocked, Ok(Ok(()))) {
            let rollback = deleting.rollback().await;
            creating.abort();
            let writer = creating.await;
            panic!(
                "relationship writer must block on the endpoint lock: {blocked:?}; rollback: {rollback:?}; writer: {writer:?}"
            );
        }
        deleting
            .run(
                neo4rs::query(
                    "MATCH (n:Entity {uuid:$uuid}) SET n.is_latest=false,n.deleted_at=$at",
                )
                .param("uuid", locked.to_string())
                .param("at", at().to_rfc3339()),
            )
            .await
            .unwrap();
        deleting.commit().await.unwrap();
        let outcome = creating.await.unwrap();
        let rows = graph
            .execute_read(
                "MATCH ()-[r:RELATES_TO {uuid:$uuid}]->() RETURN count(r) AS count",
                &json!({"uuid": edge_id}),
            )
            .await
            .unwrap();
        assert_eq!(
            rows[0]["count"], 0,
            "a relationship must not attach to the deleted target; write result: {outcome:?}"
        );
        assert!(outcome.is_err());
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn unreceipted_mutations_report_lost_acknowledgement_without_replay() {
    let faults = Arc::new(FaultInjection::default());
    let graph = Neo4jGraphBackend::with_options(
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .uri,
        "neo4j",
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .password,
        Neo4jOptions {
            faults: Some(Arc::clone(&faults)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    graph.ensure_indexes().await.unwrap();
    let org = format!("unreceipted-{}", Uuid::new_v4());
    let id = Uuid::new_v4();
    faults
        .lose_commit_ack
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let result = graph.apply_mutations(&org, &[entity(id, id)]).await;
    assert!(
        matches!(result, Err(BackendError::UnknownCommit(_))),
        "{result:?}"
    );
    assert!(version(&graph, &org, id, id).await.is_some());
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn partial_observations_preserve_generation_and_malformed_keys_do_not_break_reads() {
    let graph = Neo4jGraphBackend::new(
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .uri,
        "neo4j",
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .password,
    )
    .await
    .unwrap();
    graph.ensure_indexes().await.unwrap();
    let org = format!("partial-{}", Uuid::new_v4());
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    for id in ids {
        graph.apply_mutations(&org, &[entity(id, id), M::UpdateEntity {
            uuid:id, properties: json!({"sync_generation":42,"primary_key_properties":["bad"],"prop_bad":["a","b"],"property_type_bad":"sl"}).as_object().unwrap().clone(),
        }]).await.unwrap();
    }
    let observations: Vec<_> = ids
        .iter()
        .map(|id| M::ObserveEntity {
            chain_id: *id,
            observed_at: at(),
            sync_generation: None,
            snapshot_id: None,
            collection: None,
        })
        .collect();
    graph.apply_mutations(&org, &observations).await.unwrap();
    // One observation exercises the ungrouped statement too.
    graph
        .apply_mutations(&org, &observations[..1])
        .await
        .unwrap();
    for id in ids {
        assert_eq!(node(&graph, &org, id, id).await["sync_generation"], 42);
    }
    for malformed in [json!(["bad"]), json!("x"), json!(true), json!([1])] {
        for id in ids {
            graph
                .apply_mutations(
                    &org,
                    &[M::UpdateEntity {
                        uuid: id,
                        properties: json!({"primary_key_properties":malformed})
                            .as_object()
                            .unwrap()
                            .clone(),
                    }],
                )
                .await
                .unwrap();
        }
        let result = graph
            .find_entities(
                &org,
                &EntityLookup::LiveByIdentifyingValue {
                    values: vec!["api".into()],
                },
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 2, "malformed key shape: {malformed}");
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn source_scope_and_endpoint_reads_exclude_foreign_and_dead_endpoints() {
    let graph = Neo4jGraphBackend::new(
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .uri,
        "neo4j",
        &kg_neo4j_testkit::env::neo4j()
            .unwrap_or_else(|e| panic!("{e}"))
            .password,
    )
    .await
    .unwrap();
    graph.ensure_indexes().await.unwrap();
    let org = format!("scope-{}", Uuid::new_v4());
    let mut chains = Vec::new();
    let mut expected_scope = Vec::new();
    let mut expected_endpoints = Vec::new();
    for case in [
        "live",
        "namespace",
        "source",
        "missing_producer",
        "foreign_source",
        "foreign_target",
        "foreign_edge",
        "deleted_source",
        "deleted_target",
        "old_source",
        "old_target",
        "ended_edge",
    ] {
        let s = Uuid::new_v4();
        let t = Uuid::new_v4();
        let r = Uuid::new_v4();
        chains.extend([s, t]);
        // Deliberately create inconsistent fixtures to test the read boundary,
        // independently of the mutation API's endpoint validation.
        graph.execute_write("CREATE (s:Entity {uuid:$s,chain_id:$s,org_id:$so,namespace:'endpoint_namespace',source:'endpoint_source',is_latest:$sl,deleted_at:$sd}) CREATE (t:Entity {uuid:$t,chain_id:$t,org_id:$to,is_latest:$tl,deleted_at:$td}) CREATE (s)-[:RELATES_TO {uuid:$r,org_id:$ro,name:'DEPENDS_ON',producer_namespace:$ns,producer_source:$source,is_latest:true,valid_to:$end}]->(t)", &json!({
            "s":s,"t":t,"r":r,
            "so":if case=="foreign_source" { "foreign" } else { &org },
            "to":if case=="foreign_target" { "foreign" } else { &org },
            "ro":if case=="foreign_edge" { "foreign" } else { &org },
            "ns":if case=="namespace" { "dev" } else { "prod" },
            "source":match case { "source" => Some("other"), "missing_producer" => None, _ => Some("connector") },
            "sl":case!="old_source","tl":case!="old_target",
            "sd":if case=="deleted_source" {Some(at())} else {None},
            "td":if case=="deleted_target" {Some(at())} else {None},
            "end":if case=="ended_edge" {Some(at())} else {None},
        })).await.unwrap();
        if case == "live" {
            expected_scope.push(r);
        }
        if matches!(case, "live" | "namespace" | "source" | "missing_producer") {
            expected_endpoints.push(r);
        }
    }
    let mut scoped: Vec<_> = graph
        .find_edges(
            &org,
            &EdgeLookup::LiveBySourceScope {
                namespace: "prod".into(),
                source: "connector".into(),
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.uuid)
        .collect();
    let mut endpoints: Vec<_> = graph
        .find_edges(
            &org,
            &EdgeLookup::LiveByEndpointChains { chain_ids: chains },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.uuid)
        .collect();
    scoped.sort();
    endpoints.sort();
    expected_scope.sort();
    expected_endpoints.sort();
    assert_eq!(scoped, expected_scope);
    assert_eq!(endpoints, expected_endpoints);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn concurrent_registration_selects_one_durable_schema_manifest() {
    let graph = neo4j(Neo4jOptions::default()).await;
    let org = format!("schema-race-{}", Uuid::new_v4());
    let run = Uuid::new_v4();
    let fingerprint = RequestFingerprint::compute(&org, &[], &json!({})).unwrap();
    let mut left = header(&org, run, &fingerprint);
    left.schema_manifest.sources.insert(
        "source".into(),
        serde_json::from_value(json!({"relationship_vocabulary":["USES"]})).unwrap(),
    );
    let mut right = left.clone();
    right.schema_manifest.sources.insert(
        "source".into(),
        serde_json::from_value(json!({"relationship_vocabulary":["OWNS"]})).unwrap(),
    );
    assert!(graph.read_run(&org, run).await.unwrap().is_none());
    let (a, b) = tokio::join!(graph.register_run(&left), graph.register_run(&right));
    let (a, b) = (a.unwrap(), b.unwrap());
    let (winner, resumed) = match (a, b) {
        (
            RunRegistration::Registered,
            RunRegistration::Resumed {
                schema_manifest, ..
            },
        ) => (left, schema_manifest),
        (
            RunRegistration::Resumed {
                schema_manifest, ..
            },
            RunRegistration::Registered,
        ) => (right, schema_manifest),
        outcomes => panic!("exactly one initializer must win: {outcomes:?}"),
    };
    assert_eq!(winner.schema_manifest, resumed);
    let stored = graph.read_run(&org, run).await.unwrap().unwrap();
    assert_eq!(stored.schema_manifest, winner.schema_manifest);
    assert_eq!(stored.fingerprint, winner.fingerprint);
    assert!(graph.read_run("other-org", run).await.unwrap().is_none());
}

/// The refresh marker covers snapshot-only and edge-only commits, but never
/// replays, failed transactions, or another tenant's work.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn graph_refresh_markers_follow_atomic_commits() {
    use kg_core::traits::graph_explorer::{ExplorerQuery, ExplorerRequest, GraphExplorerBackend};
    let live = kg_neo4j_testkit::LiveGraph::open_named("graph-refresh")
        .await
        .unwrap();
    let graph = live.backend();
    let org = live.org();
    let request = |org: &str| ExplorerRequest {
        org_id: org.into(),
        namespace: None,
        as_of: None,
        limit: 64,
        offset: 0,
        query: ExplorerQuery::GraphRevision,
    };
    let run = Uuid::new_v4();
    let fingerprint = RequestFingerprint(format!("{:032x}", 1u128));
    graph
        .register_run(&header(org, run, &fingerprint))
        .await
        .unwrap();
    assert!(graph.explore(&request(org)).await.unwrap().items.is_empty());
    let snap = batch(
        org,
        run,
        &fingerprint,
        BatchKind::Node,
        0,
        vec![],
        vec![M::UpsertSnapshot {
            uuid: Uuid::new_v4(),
            properties: Default::default(),
        }],
    );
    graph.commit_batch(&snap).await.unwrap();
    let first = graph.explore(&request(org)).await.unwrap().items;
    assert_eq!(first.len(), 1);
    assert!(graph.commit_batch(&snap).await.unwrap().replayed);
    assert_eq!(graph.explore(&request(org)).await.unwrap().items, first);
    assert!(graph
        .explore(&request(&format!("{org}-foreign")))
        .await
        .unwrap()
        .items
        .is_empty());
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    graph
        .apply_mutations(org, &[entity(a, a), entity(b, b)])
        .await
        .unwrap();
    let edge_batch = batch(
        org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        0,
        vec![],
        vec![edge(Uuid::new_v4(), a, b)],
    );
    graph.commit_batch(&edge_batch).await.unwrap();
    let second = graph.explore(&request(org)).await.unwrap().items;
    assert_ne!(second, first);
    let broken = batch(
        org,
        run,
        &fingerprint,
        BatchKind::Relationship,
        1,
        vec![],
        vec![edge(Uuid::new_v4(), a, Uuid::new_v4())],
    );
    assert!(graph.commit_batch(&broken).await.is_err());
    assert_eq!(graph.explore(&request(org)).await.unwrap().items, second);
    live.cleanup().await.unwrap();
}

/// Owner-boundary regression: oversized independent writes survive interruption,
/// while a stale frozen plan and an indivisible write never bypass the budgets.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn durable_commit_pages_resume_and_fence_external_changes() {
    use std::sync::atomic::Ordering;
    use tokio_util::sync::CancellationToken;
    let live = kg_neo4j_testkit::LiveGraph::open_named("commit-pages")
        .await
        .unwrap();
    let org = live.org();
    let faults = Arc::new(FaultInjection::default());
    let graph = neo4j(Neo4jOptions {
        faults: Some(faults.clone()),
        ..Default::default()
    })
    .await;
    for interference in ["none", "mutation", "embedding"] {
        let run = Uuid::new_v4();
        let fingerprint = RequestFingerprint(format!("{:032x}", 31u128));
        graph
            .register_run(&header(org, run, &fingerprint))
            .await
            .unwrap();
        let ids: Vec<_> = (0..18).map(|_| Uuid::new_v4()).collect();
        let mutations = ids
            .iter()
            .map(|id| {
                let mut value = entity(*id, *id);
                if let M::UpsertEntity { properties, .. } = &mut value {
                    properties.insert("large_payload".into(), json!("x".repeat(1024 * 1024)));
                }
                value
            })
            .collect();
        let large = batch(
            org,
            run,
            &fingerprint,
            BatchKind::Node,
            0,
            vec![],
            mutations,
        );
        assert!(
            large.validate().is_err(),
            "fixture exceeds the existing atomic payload budget"
        );
        faults.lose_commit_ack.store(1, Ordering::SeqCst);
        let outcome = graph.commit_batch(&large).await;
        assert!(
            matches!(&outcome, Err(BackendError::UnknownCommit(_))),
            "{outcome:?}"
        );
        assert!(
            graph.committed_batches(org, run).await.unwrap().is_empty(),
            "a guard page is not a completed ingestion batch"
        );
        let stopped = CancellationToken::new();
        stopped.cancel();
        assert!(graph
            .resume_commit(org, large.batch, &fingerprint, &stopped)
            .await
            .is_err());
        assert!(graph.committed_batches(org, run).await.unwrap().is_empty());
        faults.lose_commit_ack.store(1, Ordering::SeqCst);
        assert!(matches!(
            graph
                .resume_commit(org, large.batch, &fingerprint, &CancellationToken::new())
                .await,
            Err(BackendError::UnknownCommit(_))
        ));
        let partial = graph
            .execute_read(
                "MATCH (n:Entity {org_id:$org}) WHERE n.uuid IN $ids RETURN count(n) AS n",
                &json!({"org":org,"ids":ids}),
            )
            .await
            .unwrap()[0]["n"]
            .as_u64()
            .unwrap();
        assert!(
            partial > 0 && partial < ids.len() as u64,
            "interruption follows a real data page"
        );
        assert!(graph.committed_batches(org, run).await.unwrap().is_empty());
        if interference != "none" {
            if interference == "mutation" {
                let extra = Uuid::new_v4();
                live.backend()
                    .apply_mutations(org, &[entity(extra, extra)])
                    .await
                    .unwrap();
            } else {
                // Maintenance changes candidate evidence and must invalidate a
                // frozen plan just like ordinary ingestion does.
                live.backend()
                    .set_entity_embedding(
                        org,
                        ids[0],
                        &GraphEmbedding {
                            model: "refreshed".into(),
                            values: vec![1.0, 0.0],
                        },
                        kg_core::embedding::TEXT_VERSION,
                        &kg_core::embedding::content_hash("type: Service\nname: api"),
                        &kg_core::embedding::EntityEmbeddingFields::default(),
                    )
                    .await
                    .unwrap();
            }
            let rejected = graph
                .resume_commit(org, large.batch, &fingerprint, &CancellationToken::new())
                .await;
            assert!(
                matches!(&rejected, Err(BackendError::Query(_))),
                "{rejected:?}"
            );
            assert!(graph.committed_batches(org, run).await.unwrap().is_empty());
            assert!(
                version(&graph, org, *ids.last().unwrap(), *ids.last().unwrap())
                    .await
                    .is_none()
            );
        } else {
            // Two independent adapters resume the same frozen pages. Both must
            // succeed even when one advances the fence before the other locks it.
            let token_a = CancellationToken::new();
            let token_b = CancellationToken::new();
            let other = live.backend();
            let (a, b) = tokio::join!(
                other.resume_commit(org, large.batch, &fingerprint, &token_a),
                graph.resume_commit(org, large.batch, &fingerprint, &token_b),
            );
            let a = a.unwrap().unwrap();
            let b = b.unwrap().unwrap();
            assert_eq!(a.result, large.result);
            assert_eq!(b.result, large.result);
            assert_ne!(a.replayed, b.replayed, "exactly one parent receipt commits");
            let parts = graph
                .execute_read(
                    "MATCH (p:CommitPlanPart {org_id:$org,batch_id:$id}) RETURN count(p) AS n",
                    &json!({"org":org,"id":large.batch_id()}),
                )
                .await
                .unwrap();
            assert_eq!(parts[0]["n"], 0, "completed payload copies are released");
            assert_eq!(graph.committed_batches(org, run).await.unwrap().len(), 1);
            for id in &ids {
                assert_eq!(node(&graph, org, *id, *id).await["version"], 1);
            }
            assert!(graph.commit_batch(&large).await.unwrap().replayed);
            let count = graph
                .execute_read(
                    "MATCH (n:Entity {org_id:$org}) WHERE n.uuid IN $ids RETURN count(n) AS n",
                    &json!({"org":org,"ids":ids}),
                )
                .await
                .unwrap();
            assert_eq!(count[0]["n"], 18);
        }
    }
    let run = Uuid::new_v4();
    let fingerprint = RequestFingerprint(format!("{:032x}", 32u128));
    graph
        .register_run(&header(org, run, &fingerprint))
        .await
        .unwrap();
    let id = Uuid::new_v4();
    let mut mutation = entity(id, id);
    if let M::UpsertEntity { properties, .. } = &mut mutation {
        properties.insert("large_payload".into(), json!("x".repeat(17 * 1024 * 1024)));
    }
    let oversized = batch(
        org,
        run,
        &fingerprint,
        BatchKind::Node,
        0,
        vec![],
        vec![mutation],
    );
    assert!(matches!(
        graph.commit_batch(&oversized).await,
        Err(BackendError::Query(_))
    ));
    assert!(graph.committed_batches(org, run).await.unwrap().is_empty());
    assert!(version(&graph, org, id, id).await.is_none());
    live.cleanup().await.unwrap();
}
