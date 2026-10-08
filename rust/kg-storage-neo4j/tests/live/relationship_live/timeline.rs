//! Timeline commits must detect changes to history, not only the newest UUID.
use kg_core::{
    errors::BackendError,
    traits::{
        relationship_timeline::state, BatchIdentity, BatchKind, EdgeLookup, GraphBackend,
        GraphMutation as M, GraphProperties, MutationBatch, Precondition, RequestFingerprint,
        RunHeader,
    },
};
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::json;
use uuid::Uuid;

use kg_neo4j_testkit::indexed_graph as graph;
fn entity(id: Uuid, chain: Uuid, latest: bool) -> M {
    M::UpsertEntity {
        uuid: id,
        properties: json!({"chain_id":chain,"name":"api","namespace":"prod",
        "entity_type":"Service","version":1,"is_latest":latest,"valid_from":"2026-01-01T00:00:00Z"})
        .as_object()
        .unwrap()
        .clone(),
    }
}
fn edge(id: Uuid, chain: Uuid, source: Uuid, target: Uuid, version: u32) -> M {
    M::UpsertEdge {
        uuid: id,
        source_chain_id: source,
        target_chain_id: target,
        properties: json!({"chain_id":chain,"version":version,"name":"USES","is_latest":true,
            "valid_from":"2026-01-01T00:00:00Z","last_seen_at":"2026-02-01T00:00:00Z"})
        .as_object()
        .unwrap()
        .clone(),
    }
}
fn scheduled_edge(id: Uuid, chain: Uuid, source: Uuid, target: Uuid, version: u32) -> M {
    let mut mutation = edge(id, chain, source, target, version);
    let M::UpsertEdge { properties, .. } = &mut mutation else {
        unreachable!()
    };
    properties.insert("valid_from".into(), json!("2100-01-01T00:00:00Z"));
    mutation
}
async fn timeline(
    graph: &Neo4jGraphBackend,
    org: &str,
    source: Uuid,
    target: Uuid,
) -> Vec<GraphProperties> {
    graph
        .find_edges(
            org,
            &EdgeLookup::VersionsByChainPairs {
                pairs: vec![(source, target)],
            },
        )
        .await
        .unwrap()
        .iter()
        .map(|record| state(&record.stored))
        .collect()
}
async fn batch(
    graph: &Neo4jGraphBackend,
    org: &str,
    source: Uuid,
    target: Uuid,
    versions: Vec<GraphProperties>,
    mutations: Vec<M>,
) -> MutationBatch {
    let run_id = Uuid::new_v4();
    let fingerprint = RequestFingerprint(format!("{:032x}", run_id.as_u128()));
    graph
        .register_run(&RunHeader {
            observation_manifest: Default::default(),
            rule_freezes: vec![],
            org_id: org.into(),
            run_id,
            fingerprint: fingerprint.clone(),
            settings_version: "timeline-test".into(),
            capture_default: "2026-02-01T00:00:00Z".parse().unwrap(),
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
            run_id,
            kind: BatchKind::Relationship,
            index: 0,
        },
        fingerprint,
        preconditions: vec![Precondition::RelationshipTimelineIs {
            source_chain_id: source,
            target_chain_id: target,
            versions,
        }],
        mutations,
        result: json!({}),
    }
}
fn conflict(result: Result<kg_core::traits::CommittedBatch, BackendError>) {
    assert!(
        matches!(result, Err(BackendError::Conflict(_))),
        "{result:?}"
    );
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn timeline_fences_history_clocks_content_and_membership() {
    let graph = graph().await;
    let org = format!("timeline-{}", Uuid::new_v4());
    let (source, target, old_source, first, second, chain) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    graph
        .apply_mutations(
            &org,
            &[
                entity(source, source, true),
                entity(target, target, true),
                entity(old_source, source, false),
                edge(first, chain, source, target, 1),
                scheduled_edge(second, chain, source, target, 2),
            ],
        )
        .await
        .unwrap();
    graph.execute_write("MATCH (s:Entity {uuid:$old}) MATCH (current)-[r:RELATES_TO {uuid:$first}]->(t) WITH s,t,r,properties(r) AS props DELETE r CREATE (s)-[copy:RELATES_TO]->(t) SET copy=props,copy.is_latest=false,copy.valid_to='2100-01-01T00:00:00Z'",
        &json!({"old":old_source,"first":first})).await.unwrap();
    let initial = timeline(&graph, &org, source, target).await;
    assert_eq!(
        initial.len(),
        2,
        "historical endpoints and scheduled successor"
    );
    assert!(timeline(&graph, "different-org", source, target)
        .await
        .is_empty());
    let unchanged = batch(&graph, &org, source, target, initial.clone(), vec![]).await;
    graph.commit_batch(&unchanged).await.unwrap();
    assert!(graph.commit_batch(&unchanged).await.unwrap().replayed);
    for (key, value) in [
        ("valid_to", json!("2099-01-01T00:00:00Z")),
        ("last_seen_at", json!("2026-04-01T00:00:00Z")),
        ("last_transition_at", json!("2026-03-01T00:00:00Z")),
        ("prop_region", json!("east")),
        ("prop_region", serde_json::Value::Null),
        ("invalid_at", json!("2098-01-01T00:00:00Z")),
        ("deleted_at", json!("2097-01-01T00:00:00Z")),
    ] {
        let before = timeline(&graph, &org, source, target).await;
        let plan = batch(&graph, &org, source, target, before.clone(), vec![]).await;
        let mut properties: GraphProperties = [(key.into(), value.clone())].into_iter().collect();
        if key == "prop_region" {
            properties.insert(
                "property_type_region".into(),
                if value.is_null() {
                    serde_json::Value::Null
                } else {
                    json!("s")
                },
            );
        }
        let mutation = M::UpdateEdge {
            uuid: first,
            properties,
        };
        if key == "deleted_at" {
            mutation.validate(&org).unwrap();
            assert!(matches!(
                graph.apply_mutations(&org, &[mutation]).await,
                Err(BackendError::NotFound(_))
            ));
            assert_eq!(before, timeline(&graph, &org, source, target).await);
            // Typed patches cannot rewrite deletion history. Simulate an explicit
            // out-of-band administrative edit to verify the timeline fence also
            // detects this corruption, as it detects raw rewiring below.
            graph
                .execute_write(
                    "MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$id}]->() SET r.deleted_at=$at",
                    &json!({"org":org,"id":first,"at":value}),
                )
                .await
                .unwrap();
            conflict(graph.commit_batch(&plan).await);
            continue;
        }
        graph.apply_mutations(&org, &[mutation]).await.unwrap();
        conflict(graph.commit_batch(&plan).await);
    }
    let before = timeline(&graph, &org, source, target).await;
    graph.execute_write("MATCH ()-[r:RELATES_TO {uuid:$id}]->() SET r.embedding=[1.0,0.0],r.embedding_model='test',r.embedding_text_version=1,r.embedding_content_hash='text'",&json!({"id":first})).await.unwrap();
    assert_eq!(before, timeline(&graph, &org, source, target).await);
    graph
        .commit_batch(&batch(&graph, &org, source, target, before, vec![]).await)
        .await
        .unwrap();
    let before = timeline(&graph, &org, source, target).await;
    let third = Uuid::new_v4();
    graph
        .apply_mutations(&org, &[edge(third, chain, source, target, 3)])
        .await
        .unwrap();
    conflict(
        graph
            .commit_batch(&batch(&graph, &org, source, target, before, vec![]).await)
            .await,
    );
    let before = timeline(&graph, &org, source, target).await;
    graph
        .execute_write(
            "MATCH ()-[r:RELATES_TO {uuid:$id}]->() DELETE r",
            &json!({"id":third}),
        )
        .await
        .unwrap();
    conflict(
        graph
            .commit_batch(&batch(&graph, &org, source, target, before, vec![]).await)
            .await,
    );
    let missing = Uuid::new_v4();
    conflict(
        graph
            .commit_batch(&batch(&graph, &org, source, missing, vec![], vec![]).await)
            .await,
    );
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn competing_timeline_insertions_and_amendments_have_one_winner() {
    let graph = graph().await;
    let org = format!("timeline-race-{}", Uuid::new_v4());
    let (source, target, first, second) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    graph
        .apply_mutations(
            &org,
            &[entity(source, source, true), entity(target, target, true)],
        )
        .await
        .unwrap();
    let left = batch(
        &graph,
        &org,
        source,
        target,
        vec![],
        vec![edge(first, first, source, target, 1)],
    )
    .await;
    let right = batch(
        &graph,
        &org,
        source,
        target,
        vec![],
        vec![edge(second, second, source, target, 1)],
    )
    .await;
    let (a, b) = tokio::join!(graph.commit_batch(&left), graph.commit_batch(&right));
    assert_ne!(a.is_ok(), b.is_ok(), "{a:?} / {b:?}");
    if a.is_err() {
        conflict(a)
    } else {
        conflict(b)
    };
    let versions = timeline(&graph, &org, source, target).await;
    assert_eq!(versions.len(), 1);
    let id = Uuid::parse_str(versions[0]["uuid"].as_str().unwrap()).unwrap();
    let change = |end| M::UpdateEdge {
        uuid: id,
        properties: json!({"valid_to":end,"is_latest":false})
            .as_object()
            .unwrap()
            .clone(),
    };
    let left = batch(
        &graph,
        &org,
        source,
        target,
        versions.clone(),
        vec![change("2027-01-01T00:00:00Z")],
    )
    .await;
    let right = batch(
        &graph,
        &org,
        source,
        target,
        versions,
        vec![change("2028-01-01T00:00:00Z")],
    )
    .await;
    let (a, b) = tokio::join!(graph.commit_batch(&left), graph.commit_batch(&right));
    assert_ne!(a.is_ok(), b.is_ok(), "{a:?} / {b:?}");
    if a.is_err() {
        conflict(a)
    } else {
        conflict(b)
    };
    // Self-loops acquire one endpoint lock and appear once in the timeline.
    let id = Uuid::new_v4();
    graph
        .commit_batch(
            &batch(
                &graph,
                &org,
                source,
                source,
                vec![],
                vec![edge(id, id, source, source, 1)],
            )
            .await,
        )
        .await
        .unwrap();
    let self_loop = timeline(&graph, &org, source, source).await;
    assert_eq!(self_loop.len(), 1);
    graph
        .commit_batch(&batch(&graph, &org, source, source, self_loop, vec![]).await)
        .await
        .unwrap();
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}

async fn relation_timeline(
    graph: &Neo4jGraphBackend,
    org: &str,
    source: Uuid,
) -> Vec<kg_core::traits::relationship_timeline::VersionState> {
    graph
        .find_edges(
            org,
            &EdgeLookup::VersionsByRelations {
                relations: vec![(source, "USES".into())],
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(
            |record| kg_core::traits::relationship_timeline::VersionState {
                target_chain_id: record.target_chain_id,
                properties: state(&record.stored),
            },
        )
        .collect()
}

async fn relation_batch(
    graph: &Neo4jGraphBackend,
    org: &str,
    source: Uuid,
    versions: Vec<kg_core::traits::relationship_timeline::VersionState>,
    mutations: Vec<M>,
) -> MutationBatch {
    let mut batch = batch(graph, org, source, source, vec![], mutations).await;
    batch.preconditions = vec![Precondition::RelationTimelineIs {
        source_chain_id: source,
        name: "USES".into(),
        versions,
    }];
    batch
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn relation_timeline_fences_other_targets_and_historical_amendments() {
    let graph = graph().await;
    let org = format!("relation-timeline-{}", Uuid::new_v4());
    let (source, target, other, id) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    graph
        .apply_mutations(
            &org,
            &[
                entity(source, source, true),
                entity(target, target, true),
                entity(other, other, true),
                edge(id, id, source, target, 1),
                M::UpdateEdge {
                    uuid: id,
                    properties: json!({"is_latest":false,"valid_to":"2100-01-01T00:00:00Z"})
                        .as_object()
                        .unwrap()
                        .clone(),
                },
            ],
        )
        .await
        .unwrap();
    let original = relation_timeline(&graph, &org, source).await;
    assert_eq!(original.len(), 1);
    assert_eq!(original[0].target_chain_id, target);
    assert!(relation_timeline(&graph, "another-org", source)
        .await
        .is_empty());
    let unchanged = relation_batch(&graph, &org, source, original.clone(), vec![]).await;
    graph.commit_batch(&unchanged).await.unwrap();
    graph.commit_batch(&unchanged).await.unwrap();

    // A finite historical interval remains protected even when its UUID is unchanged.
    graph
        .apply_mutations(
            &org,
            &[M::UpdateEdge {
                uuid: id,
                properties: json!({"valid_to":"2099-01-01T00:00:00Z"})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        )
        .await
        .unwrap();
    conflict(
        graph
            .commit_batch(&relation_batch(&graph, &org, source, original, vec![]).await)
            .await,
    );
    let before_insert = relation_timeline(&graph, &org, source).await;
    let future = Uuid::new_v4();
    graph
        .apply_mutations(&org, &[scheduled_edge(future, future, source, other, 1)])
        .await
        .unwrap();
    conflict(
        graph
            .commit_batch(&relation_batch(&graph, &org, source, before_insert, vec![]).await)
            .await,
    );

    // Rewiring the same UUID to another chain must not hide behind equal properties.
    let before_rewire = relation_timeline(&graph, &org, source).await;
    graph.execute_write("MATCH (s:Entity {org_id:$org,chain_id:$source})-[r:RELATES_TO {uuid:$id}]->(), (t:Entity {org_id:$org,chain_id:$target}) WITH s,t,r,properties(r) AS props DELETE r CREATE (s)-[copy:RELATES_TO]->(t) SET copy=props", &json!({"org":org,"source":source,"id":id,"target":other})).await.unwrap();
    conflict(
        graph
            .commit_batch(&relation_batch(&graph, &org, source, before_rewire, vec![]).await)
            .await,
    );
    let current = relation_timeline(&graph, &org, source).await;
    graph.execute_write("MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$id}]->() SET r.embedding=[0.1,0.2],r.embedding_model='refreshed'",&json!({"org":org,"id":id})).await.unwrap();
    graph
        .commit_batch(&relation_batch(&graph, &org, source, current, vec![]).await)
        .await
        .unwrap();
    conflict(
        graph
            .commit_batch(&relation_batch(&graph, &org, Uuid::new_v4(), vec![], vec![]).await)
            .await,
    );
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn competing_relation_targets_have_one_winner() {
    let graph = graph().await;
    let org = format!("relation-race-{}", Uuid::new_v4());
    let (source, target, other) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    graph
        .apply_mutations(
            &org,
            &[
                entity(source, source, true),
                entity(target, target, true),
                entity(other, other, true),
            ],
        )
        .await
        .unwrap();
    let left_id = Uuid::new_v4();
    let right_id = Uuid::new_v4();
    let left = relation_batch(
        &graph,
        &org,
        source,
        vec![],
        vec![edge(left_id, left_id, source, target, 1)],
    )
    .await;
    let right = relation_batch(
        &graph,
        &org,
        source,
        vec![],
        vec![edge(right_id, right_id, source, other, 1)],
    )
    .await;
    let (a, b) = tokio::join!(graph.commit_batch(&left), graph.commit_batch(&right));
    assert_ne!(a.is_ok(), b.is_ok(), "{a:?} / {b:?}");
    if a.is_err() {
        conflict(a)
    } else {
        conflict(b)
    };
    assert_eq!(relation_timeline(&graph, &org, source).await.len(), 1);
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}

async fn incident_timeline(
    graph: &Neo4jGraphBackend,
    org: &str,
    chains: Vec<Uuid>,
) -> Vec<kg_core::traits::relationship_timeline::IncidentVersionState> {
    graph
        .find_edges(
            org,
            &EdgeLookup::VersionsByEndpointChains { chain_ids: chains },
        )
        .await
        .unwrap()
        .into_iter()
        .map(
            |record| kg_core::traits::relationship_timeline::IncidentVersionState {
                source_chain_id: record.source_chain_id,
                target_chain_id: record.target_chain_id,
                properties: state(&record.stored),
            },
        )
        .collect()
}

async fn incident_batch(
    graph: &Neo4jGraphBackend,
    org: &str,
    anchor: Uuid,
    versions: Vec<kg_core::traits::relationship_timeline::IncidentVersionState>,
    mutations: Vec<M>,
) -> MutationBatch {
    let mut plan = batch(graph, org, anchor, anchor, vec![], mutations).await;
    plan.preconditions = vec![Precondition::IncidentTimelineIs {
        chain_id: anchor,
        versions,
    }];
    plan
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn incident_timeline_covers_both_directions_history_and_self_loops() {
    let graph = graph().await;
    let org = format!("incident-{}", Uuid::new_v4());
    let (anchor, old_anchor, other, third, incoming, outgoing, self_loop) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    graph
        .apply_mutations(
            &org,
            &[
                entity(anchor, anchor, true),
                entity(old_anchor, anchor, false),
                entity(other, other, true),
                entity(third, third, true),
                edge(incoming, incoming, other, anchor, 1),
                edge(outgoing, outgoing, anchor, other, 1),
                edge(self_loop, self_loop, anchor, anchor, 1),
            ],
        )
        .await
        .unwrap();
    graph.execute_write("MATCH (s)-[r:RELATES_TO {org_id:$org,uuid:$id}]->(), (t:Entity {org_id:$org,uuid:$old}) WITH s,t,r,properties(r) AS props DELETE r CREATE (s)-[copy:RELATES_TO]->(t) SET copy=props,copy.is_latest=false,copy.valid_to='2026-02-01T00:00:00Z'", &json!({"org":org,"id":incoming,"old":old_anchor})).await.unwrap();
    graph.execute_write("MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$id}]->() SET r.valid_from='2100-01-01T00:00:00Z'", &json!({"org":org,"id":outgoing})).await.unwrap();
    let initial = incident_timeline(&graph, &org, vec![anchor]).await;
    assert_eq!(initial.len(), 3);
    assert_eq!(
        initial,
        incident_timeline(&graph, &org, vec![anchor, other]).await
    );
    assert!(incident_timeline(&graph, "foreign-org", vec![anchor])
        .await
        .is_empty());
    let unchanged = incident_batch(&graph, &org, anchor, initial.clone(), vec![]).await;
    graph.commit_batch(&unchanged).await.unwrap();
    assert!(graph.commit_batch(&unchanged).await.unwrap().replayed);

    graph.execute_write("MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$id}]->() SET r.valid_to='2026-03-01T00:00:00Z'", &json!({"org":org,"id":incoming})).await.unwrap();
    conflict(
        graph
            .commit_batch(&incident_batch(&graph, &org, anchor, initial, vec![]).await)
            .await,
    );
    let before_rewire = incident_timeline(&graph, &org, vec![anchor]).await;
    graph.execute_write("MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$id}]->(t), (s:Entity {org_id:$org,uuid:$source}) WITH s,t,r,properties(r) AS props DELETE r CREATE (s)-[copy:RELATES_TO]->(t) SET copy=props", &json!({"org":org,"id":incoming,"source":third})).await.unwrap();
    conflict(
        graph
            .commit_batch(&incident_batch(&graph, &org, anchor, before_rewire, vec![]).await)
            .await,
    );
    let current = incident_timeline(&graph, &org, vec![anchor]).await;
    graph.execute_write("MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$id}]->() SET r.embedding=[0.1,0.2],r.embedding_model='refresh'", &json!({"org":org,"id":incoming})).await.unwrap();
    graph
        .commit_batch(&incident_batch(&graph, &org, anchor, current, vec![]).await)
        .await
        .unwrap();
    conflict(
        graph
            .commit_batch(&incident_batch(&graph, &org, Uuid::new_v4(), vec![], vec![]).await)
            .await,
    );
    let deleted = Uuid::new_v4();
    graph
        .apply_mutations(&org, &[entity(deleted, deleted, true)])
        .await
        .unwrap();
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org,uuid:$id}) SET n.deleted_at='2026-09-01T00:00:00Z'",
            &json!({"org":org,"id":deleted}),
        )
        .await
        .unwrap();
    conflict(
        graph
            .commit_batch(&incident_batch(&graph, &org, deleted, vec![], vec![]).await)
            .await,
    );
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn competing_incident_insertions_have_one_winner() {
    let graph = graph().await;
    let org = format!("incident-race-{}", Uuid::new_v4());
    let (anchor, other, first, second) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    graph
        .apply_mutations(
            &org,
            &[entity(anchor, anchor, true), entity(other, other, true)],
        )
        .await
        .unwrap();
    let left = incident_batch(
        &graph,
        &org,
        anchor,
        vec![],
        vec![edge(first, first, anchor, other, 1)],
    )
    .await;
    let right = incident_batch(
        &graph,
        &org,
        anchor,
        vec![],
        vec![edge(second, second, other, anchor, 1)],
    )
    .await;
    let (a, b) = tokio::join!(graph.commit_batch(&left), graph.commit_batch(&right));
    assert_ne!(a.is_ok(), b.is_ok(), "{a:?} / {b:?}");
    if a.is_err() {
        conflict(a);
    } else {
        conflict(b);
    }
    assert_eq!(incident_timeline(&graph, &org, vec![anchor]).await.len(), 1);
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn incident_timeline_refuses_a_truncated_history() {
    let graph = graph().await;
    let org = format!("incident-budget-{}", Uuid::new_v4());
    let anchor = Uuid::new_v4();
    graph
        .apply_mutations(&org, &[entity(anchor, anchor, true)])
        .await
        .unwrap();
    graph.execute_write("MATCH (n:Entity {org_id:$org,uuid:$id}) UNWIND range(1,$count) AS i CREATE (n)-[r:RELATES_TO]->(n) SET r.org_id=$org,r.uuid=randomUUID(),r.chain_id=r.uuid,r.version=1,r.name='USES',r.is_latest=false,r.valid_from='2026-01-01T00:00:00Z',r.valid_to='2026-02-01T00:00:00Z'", &json!({"org":org,"id":anchor,"count":kg_core::traits::relationship_timeline::MAX_VERSIONS + 1})).await.unwrap();
    let result = graph
        .find_edges(
            &org,
            &EdgeLookup::VersionsByEndpointChains {
                chain_ids: vec![anchor],
            },
        )
        .await;
    assert!(
        matches!(result, Err(BackendError::RelationshipHistoryLimit { limit }) if limit == kg_core::traits::relationship_timeline::MAX_VERSIONS)
    );
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}
