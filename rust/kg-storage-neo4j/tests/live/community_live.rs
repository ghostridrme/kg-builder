//! Complete generation staging, source fencing and scoped Community projections.
use chrono::{DateTime, Utc};
use kg_core::{
    community::*,
    errors::BackendError,
    models::CommunityNode,
    traits::{graph_backend::GraphEmbedding, GraphBackend, GraphMutation as M},
};
use kg_neo4j_testkit::indexed_graph as graph;
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::json;
use uuid::Uuid;
fn at() -> DateTime<Utc> {
    "2026-01-20T00:00:00Z".parse().unwrap()
}
fn entity(uuid: Uuid, name: &str) -> M {
    M::UpsertEntity{uuid,properties:json!({"chain_id":uuid,"namespace":"prod","entity_type":"Service","name":name,"summary":"Handles checkout requests","is_latest":true,"version":1,"valid_from":"2026-01-01T00:00:00+00:00"}).as_object().unwrap().clone()}
}
async fn state(g: &Neo4jGraphBackend, org: &str) -> CommunityState {
    match g
        .read_community(
            org,
            &CommunityRead::State {
                namespace: "prod".into(),
            },
        )
        .await
        .unwrap()
    {
        CommunityReadResult::State(s) => s,
        _ => panic!("state"),
    }
}
fn definition(org: &str, count: u64) -> CommunityDefinition {
    CommunityDefinition {
        node: CommunityNode {
            uuid: Uuid::new_v4(),
            org_id: org.into(),
            namespace: "prod".into(),
            name: "Checkout services".into(),
            labels: vec![],
            created_at: at(),
            summary: "Services handle checkout requests.".into(),
            name_embedding: Some(GraphEmbedding {
                model: "test".into(),
                values: vec![1., 0., 0.],
            }),
        },
        revision: Uuid::new_v4(),
        expected_member_count: count,
        source_hash: text_hash("checkout"),
        projected_at: at(),
        valid_until: None,
    }
}
async fn staged(
    g: &Neo4jGraphBackend,
    org: &str,
    s: CommunityState,
    definition: CommunityDefinition,
    members: &[Uuid],
) -> Uuid {
    let generation = Uuid::new_v4();
    let mut partitions = vec![CommunityPartition {
        definitions: vec![definition.clone()],
        memberships: vec![],
    }];
    for chunk in members.chunks(1) {
        partitions.push(CommunityPartition {
            definitions: vec![],
            memberships: vec![CommunityMembershipChunk {
                community_uuid: definition.node.uuid,
                members: chunk
                    .iter()
                    .map(|id| CommunityMember {
                        entity_uuid: *id,
                        chain_id: *id,
                    })
                    .collect(),
            }],
        });
    }
    g.apply_mutations(
        org,
        &[M::BeginCommunityGeneration {
            generation: Box::new(BeginCommunityGeneration {
                generation,
                expected_state: s,
                projected_at: definition.projected_at,
                valid_until: definition.valid_until,
                partition_hashes: partitions
                    .iter()
                    .map(|p| partition_hash(p).unwrap())
                    .collect(),
                community_count: 1,
                member_count: members.len() as u64,
            }),
        }],
    )
    .await
    .unwrap();
    for (index, partition) in partitions.into_iter().enumerate() {
        g.apply_mutations(
            org,
            &[M::StageCommunityPartition {
                partition: Box::new(StageCommunityPartition {
                    namespace: "prod".into(),
                    generation,
                    index,
                    partition,
                }),
            }],
        )
        .await
        .unwrap();
    }
    generation
}
fn publish(generation: Uuid, s: CommunityState) -> M {
    M::PublishCommunityGeneration {
        publication: Box::new(PublishCommunityGeneration {
            generation,
            expected_state: s,
            publication_revision: Uuid::new_v4(),
        }),
    }
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn generations_are_fragmented_guarded_and_dirty_atomically() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let zero = state(&g, &org).await;
    assert_eq!(zero.source_revision, CommunityRevision::default());
    g.apply_mutations(&org, &[entity(a, "api"), entity(b, "worker")])
        .await
        .unwrap();
    let s = state(&g, &org).await;
    assert_eq!(s.source_revision.0.iter().sum::<u64>(), 1);
    let d = definition(&org, 2);
    let generation = staged(&g, &org, s.clone(), d.clone(), &[a, b]).await;
    assert_eq!(state(&g, &org).await.active_generation, None);
    g.apply_mutations(&org, &[publish(generation, s.clone())])
        .await
        .unwrap();
    let active = state(&g, &org).await;
    assert_eq!(active.source_revision, s.source_revision);
    assert_eq!(active.active_generation, Some(generation));
    let request = CommunityRead::Community {
        namespace: "prod".into(),
        generation,
        community_uuid: d.node.uuid,
    };
    match g.read_community(&org, &request).await.unwrap() {
        CommunityReadResult::Community(Some(c)) => assert!(!c.dirty),
        _ => panic!("missing"),
    }
    g.apply_mutations(
        &org,
        &[M::UpdateEntity {
            uuid: a,
            properties: json!({"summary":"Changed behavior"})
                .as_object()
                .unwrap()
                .clone(),
        }],
    )
    .await
    .unwrap();
    match g.read_community(&org, &request).await.unwrap() {
        CommunityReadResult::Community(Some(c)) => assert!(c.dirty),
        _ => panic!("missing"),
    }
    let mut next = d.clone();
    next.revision = Uuid::new_v4();
    next.projected_at = at();
    let update = |expected_state| M::UpdateCommunities {
        update: Box::new(UpdateCommunities {
            publication_revision: Uuid::new_v4(),
            generation,
            expected_state,
            communities: vec![GuardedCommunityWrite {
                expected_revision: d.revision,
                definition: next.clone(),
                members: vec![
                    CommunityMember {
                        entity_uuid: a,
                        chain_id: a,
                    },
                    CommunityMember {
                        entity_uuid: b,
                        chain_id: b,
                    },
                ],
            }],
        }),
    };
    assert!(matches!(
        g.apply_mutations(&org, &[update(active)]).await,
        Err(BackendError::Conflict(_))
    ));
    g.apply_mutations(&org, &[update(state(&g, &org).await)])
        .await
        .unwrap();
    match g.read_community(&org, &request).await.unwrap() {
        CommunityReadResult::Community(Some(c)) => assert!(!c.dirty),
        _ => panic!("missing"),
    }
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn paged_projection_scopes_histories_and_rejects_stale_publication() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(a, "api"), entity(b, "worker")])
        .await
        .unwrap();
    let mut found = Vec::new();
    let mut cursor = None;
    loop {
        let p = match g
            .read_community(
                &org,
                &CommunityRead::EntityPage {
                    namespace: "prod".into(),
                    at: at(),
                    after_uuid: cursor,
                    limit: 1,
                },
            )
            .await
            .unwrap()
        {
            CommunityReadResult::Entities(p) => p,
            _ => panic!("page"),
        };
        assert_eq!(p.scanned_rows, 1);
        for e in p.records {
            assert_eq!(e.text_hash, text_hash(&e.text));
            found.push(e.uuid);
        }
        if p.exhausted {
            break;
        }
        cursor = p.next;
    }
    found.sort();
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(found, expected);
    match g
        .read_community(
            &org,
            &CommunityRead::EntitiesByChains {
                after_uuid: None,
                namespace: "prod".into(),
                at: at(),
                chain_ids: vec![a],
                limit: 10,
            },
        )
        .await
        .unwrap()
    {
        CommunityReadResult::Entities(p) => assert_eq!(p.records.len(), 1),
        _ => panic!("chains"),
    }
    let s = state(&g, &org).await;
    let d = definition(&org, 2);
    let generation = staged(&g, &org, s.clone(), d, &[a, b]).await;
    g.apply_mutations(&org, &[entity(Uuid::new_v4(), "new")])
        .await
        .unwrap();
    assert!(matches!(
        g.apply_mutations(&org, &[publish(generation, s)]).await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(state(&g, &org).await.active_generation, None);
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn relationship_and_neighbor_pages_include_temporal_expiry_without_future_edges() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    g.apply_mutations(
        &org,
        &ids.iter().map(|id| entity(*id, "svc")).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let live = Uuid::new_v4();
    let future = Uuid::new_v4();
    let edge = |uuid, source, target, start: &str| M::UpsertEdge {
        uuid,
        source_chain_id: source,
        target_chain_id: target,
        properties: json!({"name":"CALLS","valid_from":start,"is_latest":true})
            .as_object()
            .unwrap()
            .clone(),
    };
    g.apply_mutations(
        &org,
        &[
            edge(live, ids[0], ids[1], "2026-01-01T00:00:00+00:00"),
            edge(future, ids[0], ids[2], "2026-01-25T00:00:00+00:00"),
        ],
    )
    .await
    .unwrap();
    let mut after = None;
    let mut edges = Vec::new();
    let mut scanned = 0;
    let mut boundary = None;
    loop {
        let page = match g
            .read_community(
                &org,
                &CommunityRead::RelationshipPage {
                    namespace: "prod".into(),
                    at: at(),
                    after,
                    limit: 1,
                },
            )
            .await
            .unwrap()
        {
            CommunityReadResult::Relationships(p) => p,
            _ => panic!("relationships"),
        };
        scanned += page.scanned_rows;
        edges.extend(page.records.iter().map(|r| r.uuid));
        boundary = boundary.or(page.next_temporal_boundary);
        if page.exhausted {
            break;
        }
        after = page.next;
    }
    assert_eq!(edges, vec![live]);
    assert_eq!(scanned, 5);
    assert_eq!(boundary, Some("2026-01-25T00:00:00Z".parse().unwrap()));
    let mut after_edge_uuid = None;
    let mut neighbors = Vec::new();
    let mut total = 0;
    loop {
        let p = match g
            .read_community(
                &org,
                &CommunityRead::Neighbors {
                    namespace: "prod".into(),
                    at: at(),
                    chain_ids: vec![ids[0]],
                    after_edge_uuid,
                    limit: 1,
                },
            )
            .await
            .unwrap()
        {
            CommunityReadResult::Neighbors(p) => p,
            _ => panic!("neighbors"),
        };
        total += p.scanned_rows;
        neighbors.extend(p.relationships.iter().map(|r| r.uuid));
        if p.exhausted {
            break;
        }
        after_edge_uuid = p.next;
    }
    assert_eq!(neighbors, vec![live]);
    assert_eq!(total, 2);
    match g
        .read_community(
            &org,
            &CommunityRead::EntitiesByChains {
                after_uuid: None,
                namespace: "prod".into(),
                at: at(),
                chain_ids: vec![ids[0]],
                limit: 100,
            },
        )
        .await
        .unwrap()
    {
        CommunityReadResult::Entities(p) => assert_eq!(p.records[0].valid_until, boundary),
        _ => panic!("entities"),
    }
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn a_generation_cannot_assign_one_chain_to_two_communities() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(id, "api")]).await.unwrap();
    let first = definition(&org, 1);
    let second = definition(&org, 1);
    let generation = Uuid::new_v4();
    let s = state(&g, &org).await;
    let partition = CommunityPartition {
        definitions: vec![first.clone(), second.clone()],
        memberships: vec![
            CommunityMembershipChunk {
                community_uuid: first.node.uuid,
                members: vec![CommunityMember {
                    entity_uuid: id,
                    chain_id: id,
                }],
            },
            CommunityMembershipChunk {
                community_uuid: second.node.uuid,
                members: vec![CommunityMember {
                    entity_uuid: id,
                    chain_id: id,
                }],
            },
        ],
    };
    g.apply_mutations(
        &org,
        &[M::BeginCommunityGeneration {
            generation: Box::new(BeginCommunityGeneration {
                generation,
                expected_state: s.clone(),
                projected_at: at(),
                valid_until: None,
                partition_hashes: vec![partition_hash(&partition).unwrap()],
                community_count: 2,
                member_count: 2,
            }),
        }],
    )
    .await
    .unwrap();
    assert!(g
        .apply_mutations(
            &org,
            &[M::StageCommunityPartition {
                partition: Box::new(StageCommunityPartition {
                    namespace: "prod".into(),
                    generation,
                    index: 0,
                    partition
                })
            }]
        )
        .await
        .is_err());
    assert!(g
        .apply_mutations(&org, &[publish(generation, s)])
        .await
        .is_err());
    match g
        .read_community(
            &org,
            &CommunityRead::Community {
                namespace: "prod".into(),
                generation,
                community_uuid: first.node.uuid,
            },
        )
        .await
        .unwrap()
    {
        CommunityReadResult::Community(None) => {}
        _ => panic!("failed partition leaked"),
    }
    assert_eq!(state(&g, &org).await.active_generation, None);
    match g
        .read_community(
            "different-org",
            &CommunityRead::Scopes {
                chain_ids: vec![id],
            },
        )
        .await
        .unwrap()
    {
        CommunityReadResult::Scopes(rows) => assert!(rows.is_empty()),
        _ => panic!("scopes"),
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn source_publication_race_never_exposes_untracked_current_community() {
    let g = graph().await;
    for _ in 0..4 {
        let org = Uuid::new_v4().to_string();
        let entity_id = Uuid::new_v4();
        g.apply_mutations(&org, &[entity(entity_id, "api")])
            .await
            .unwrap();
        let expected = state(&g, &org).await;
        let definition = definition(&org, 1);
        let generation = staged(&g, &org, expected.clone(), definition.clone(), &[entity_id]).await;
        let publication = [publish(generation, expected)];
        let mutation = [M::UpdateEntity {
            uuid: entity_id,
            properties: json!({"summary":"New observed behavior"})
                .as_object()
                .unwrap()
                .clone(),
        }];
        let (published, changed) = tokio::join!(
            g.apply_mutations(&org, &publication),
            g.apply_mutations(&org, &mutation)
        );
        changed.unwrap();
        match published {
            Ok(()) => match g
                .read_community(
                    &org,
                    &CommunityRead::Community {
                        namespace: "prod".into(),
                        generation,
                        community_uuid: definition.node.uuid,
                    },
                )
                .await
                .unwrap()
            {
                CommunityReadResult::Community(Some(stored)) => assert!(stored.dirty),
                _ => panic!("published community disappeared"),
            },
            Err(BackendError::Conflict(_)) => {
                assert_eq!(state(&g, &org).await.active_generation, None)
            }
            Err(error) => panic!("unexpected publication outcome: {error:?}"),
        }
    }
}

fn refresh(
    record: kg_core::embedding_rebuild::EmbeddingRecord,
) -> kg_core::embedding_rebuild::EmbeddingRefresh {
    use kg_core::{
        embedding::{content_hash, ComputedEmbedding},
        embedding_rebuild::EmbeddingKind,
    };
    let text = record
        .text(EmbeddingKind::CommunityName, &Default::default())
        .unwrap();
    kg_core::embedding_rebuild::EmbeddingRefresh {
        entity_fields: Default::default(),
        record,
        embedding: ComputedEmbedding {
            model: "replacement".into(),
            text_version: NAME_TEXT_VERSION.into(),
            content_hash: content_hash(&text),
            values: vec![0., 1., 0.],
        },
    }
}
async fn raw_community(
    g: &Neo4jGraphBackend,
    org: &str,
    id: Uuid,
) -> kg_core::embedding_rebuild::EmbeddingRecord {
    let rows = g
        .execute_read(
            "MATCH (n:Community {org_id:$org,uuid:$uuid}) RETURN properties(n) AS record",
            &json!({"org":org,"uuid":id}),
        )
        .await
        .unwrap();
    kg_core::embedding_rebuild::EmbeddingRecord {
        uuid: id,
        properties: rows[0]["record"].as_object().unwrap().clone(),
    }
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn name_refresh_fences_renames_generation_changes_dirty_evidence_and_expiry() {
    use kg_core::embedding_rebuild::EmbeddingKind;
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(id, "api")]).await.unwrap();
    let mut d = definition(&org, 1);
    let baseline = state(&g, &org).await;
    let generation = staged(&g, &org, baseline.clone(), d.clone(), &[id]).await;
    g.apply_mutations(&org, &[publish(generation, baseline)])
        .await
        .unwrap();
    let original = g
        .embedding_records(&org, EmbeddingKind::CommunityName, None, 1)
        .await
        .unwrap()
        .remove(0);
    let stale = refresh(original.clone());
    let previous = d.revision;
    d.revision = Uuid::new_v4();
    d.node.name = "Renamed checkout service".into();
    g.apply_mutations(
        &org,
        &[M::UpdateCommunities {
            update: Box::new(UpdateCommunities {
                publication_revision: Uuid::new_v4(),
                generation,
                expected_state: state(&g, &org).await,
                communities: vec![GuardedCommunityWrite {
                    expected_revision: previous,
                    definition: d.clone(),
                    members: vec![CommunityMember {
                        entity_uuid: id,
                        chain_id: id,
                    }],
                }],
            }),
        }],
    )
    .await
    .unwrap();
    assert_eq!(
        g.refresh_embeddings(&org, EmbeddingKind::CommunityName, &[stale])
            .await
            .unwrap(),
        0
    );
    let current = g
        .embedding_records(&org, EmbeddingKind::CommunityName, None, 1)
        .await
        .unwrap()
        .remove(0);
    let before = state(&g, &org).await;
    assert_eq!(
        g.refresh_embeddings(&org, EmbeddingKind::CommunityName, &[refresh(current)])
            .await
            .unwrap(),
        1
    );
    assert_eq!(before, state(&g, &org).await);
    let old = raw_community(&g, &org, d.node.uuid).await;
    let replacement = definition(&org, 1);
    let baseline = state(&g, &org).await;
    let second = staged(&g, &org, baseline.clone(), replacement.clone(), &[id]).await;
    g.apply_mutations(&org, &[publish(second, baseline)])
        .await
        .unwrap();
    assert_eq!(
        g.refresh_embeddings(&org, EmbeddingKind::CommunityName, &[refresh(old)])
            .await
            .unwrap(),
        0
    );
    let clean = g
        .embedding_records(&org, EmbeddingKind::CommunityName, None, 1)
        .await
        .unwrap()
        .remove(0);
    g.apply_mutations(
        &org,
        &[M::UpdateEntity {
            uuid: id,
            properties: json!({"summary":"source changed"})
                .as_object()
                .unwrap()
                .clone(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(
        g.refresh_embeddings(&org, EmbeddingKind::CommunityName, &[refresh(clean)])
            .await
            .unwrap(),
        0
    );
    assert!(g
        .embedding_records(&org, EmbeddingKind::CommunityName, None, 10)
        .await
        .unwrap()
        .is_empty());
    let mut expired = replacement.clone();
    expired.revision = Uuid::new_v4();
    let expires_at = Utc::now() + chrono::Duration::seconds(3);
    expired.valid_until = Some(expires_at);
    g.apply_mutations(
        &org,
        &[M::UpdateCommunities {
            update: Box::new(UpdateCommunities {
                publication_revision: Uuid::new_v4(),
                generation: second,
                expected_state: state(&g, &org).await,
                communities: vec![GuardedCommunityWrite {
                    expected_revision: replacement.revision,
                    definition: expired,
                    members: vec![CommunityMember {
                        entity_uuid: id,
                        chain_id: id,
                    }],
                }],
            }),
        }],
    )
    .await
    .unwrap();
    // Let a legitimately published projection cross its scheduled boundary.
    let remaining = (expires_at - Utc::now()).to_std().unwrap_or_default();
    tokio::time::sleep(remaining + std::time::Duration::from_millis(50)).await;
    let exact_expired = raw_community(&g, &org, replacement.node.uuid).await;
    assert_eq!(
        g.refresh_embeddings(
            &org,
            EmbeddingKind::CommunityName,
            &[refresh(exact_expired)]
        )
        .await
        .unwrap(),
        0
    );
    assert!(g
        .embedding_records(&org, EmbeddingKind::CommunityName, None, 10)
        .await
        .unwrap()
        .is_empty());
}

struct NameProvider(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl kg_core::traits::EmbedBackend for NameProvider {
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
        assert!(texts.len() <= 2);
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(texts.iter().map(|_| vec![1., 0., 0.]).collect())
    }
    fn model_id(&self) -> &str {
        "maintenance-model"
    }
    fn dimension(&self) -> usize {
        3
    }
    fn max_batch_size(&self) -> usize {
        2
    }
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn standard_embedding_rebuild_refreshes_community_names_then_reuses_them() {
    use kg_core::embedding_rebuild::{rebuild_embeddings, EmbeddingKind, RebuildOptions};
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(id, "api")]).await.unwrap();
    let baseline = state(&g, &org).await;
    let generation = staged(&g, &org, baseline.clone(), definition(&org, 1), &[id]).await;
    g.apply_mutations(&org, &[publish(generation, baseline)])
        .await
        .unwrap();
    let provider = NameProvider(Default::default());
    let report = rebuild_embeddings(&g, &provider, &org, &RebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(report.updated, 2);
    let record = g
        .embedding_records(&org, EmbeddingKind::CommunityName, None, 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        record.properties["name_embedding_model"],
        json!("maintenance-model")
    );
    assert_eq!(
        record.properties["name_embedding_text_version"],
        json!(NAME_TEXT_VERSION)
    );
    let calls = provider.0.load(std::sync::atomic::Ordering::SeqCst);
    let reused = rebuild_embeddings(&g, &provider, &org, &RebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(reused.updated, 0);
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), calls);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn chain_entity_pages_walk_history_before_returning_the_current_version() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let chain = Uuid::new_v4();
    let mut ids = (0..4).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
    ids.sort();
    let mutations=ids.iter().enumerate().map(|(index,id)| {
        let start=format!("2026-01-{:02}T00:00:00Z",index+1);let end=(index<3).then(||format!("2026-01-{:02}T00:00:00Z",index+2));
        M::UpsertEntity{uuid:*id,properties:json!({"chain_id":chain,"namespace":"prod","entity_type":"Service","name":"Versioned API","version":index+1,"is_latest":index==3,"valid_from":start,"valid_to":end}).as_object().unwrap().clone()}
    }).collect::<Vec<_>>();
    g.apply_mutations(&org, &mutations).await.unwrap();
    let mut cursor = None;
    let mut scanned = 0;
    let mut visible = Vec::new();
    loop {
        let page = match g
            .read_community(
                &org,
                &CommunityRead::EntitiesByChains {
                    namespace: "prod".into(),
                    at: at(),
                    chain_ids: vec![chain],
                    after_uuid: cursor,
                    limit: 1,
                },
            )
            .await
            .unwrap()
        {
            CommunityReadResult::Entities(page) => page,
            _ => panic!("entities"),
        };
        assert_eq!(page.scanned_rows, 1);
        assert!(page.next > cursor);
        scanned += page.scanned_rows;
        if scanned < 4 {
            assert!(page.records.is_empty());
            assert!(!page.exhausted);
        }
        visible.extend(page.records.into_iter().map(|entity| entity.uuid));
        if page.exhausted {
            break;
        }
        cursor = page.next;
    }
    assert_eq!(scanned, 4);
    assert_eq!(visible, vec![ids[3]]);
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn complete_typed_source_properties_change_factual_hash_and_match_neighbor_reads() {
    use kg_core::{models::PropertyValue, traits::property_codec::write_property};
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let mut first = entity(a, "database");
    if let M::UpsertEntity { properties, .. } = &mut first {
        properties.remove("summary");
        write_property(properties, "summary", Some(&PropertyValue::Integer(3)));
        write_property(
            properties,
            "region",
            Some(&PropertyValue::String("us-east-1".into())),
        );
        write_property(
            properties,
            "engine",
            Some(&PropertyValue::String("postgres".into())),
        );
        write_property(properties, "port", Some(&PropertyValue::Integer(5432)));
        write_property(properties, "weight", Some(&PropertyValue::Float(1.0)));
        write_property(
            properties,
            "zones",
            Some(&PropertyValue::StringList(vec!["a".into(), "b".into()])),
        );
        write_property(properties, "optional", Some(&PropertyValue::Null));
    }
    g.apply_mutations(
        &org,
        &[
            first,
            entity(b, "api"),
            M::UpsertEdge {
                uuid: Uuid::new_v4(),
                source_chain_id: b,
                target_chain_id: a,
                properties: json!({"name":"USES","valid_from":"2026-01-01T00:00:00Z"})
                    .as_object()
                    .unwrap()
                    .clone(),
            },
        ],
    )
    .await
    .unwrap();
    let request = CommunityRead::EntitiesByChains {
        namespace: "prod".into(),
        at: at(),
        chain_ids: vec![a],
        after_uuid: None,
        limit: 10,
    };
    let before = match g.read_community(&org, &request).await.unwrap() {
        CommunityReadResult::Entities(mut page) => page.records.remove(0),
        _ => panic!("entity"),
    };
    assert!(before.text.contains("us-east-1"));
    assert!(before.text.contains("postgres"));
    assert!(before.text.contains("\"port\":{\"t\":\"i\",\"v\":5432}"));
    assert!(before.text.contains("\"weight\":{\"t\":\"f\",\"v\":1.0}"));
    assert!(before.text.contains("\"optional\":{\"t\":\"n\"}"));
    let mut patch = serde_json::Map::new();
    write_property(
        &mut patch,
        "region",
        Some(&PropertyValue::String("eu-west-1".into())),
    );
    g.apply_mutations(
        &org,
        &[M::UpdateEntity {
            uuid: a,
            properties: patch,
        }],
    )
    .await
    .unwrap();
    let after = match g.read_community(&org, &request).await.unwrap() {
        CommunityReadResult::Entities(mut page) => page.records.remove(0),
        _ => panic!("entity"),
    };
    assert_ne!(before.text_hash, after.text_hash);
    assert!(after.text.contains("eu-west-1"));
    let neighbors = match g
        .read_community(
            &org,
            &CommunityRead::Neighbors {
                namespace: "prod".into(),
                at: at(),
                chain_ids: vec![b],
                after_edge_uuid: None,
                limit: 10,
            },
        )
        .await
        .unwrap()
    {
        CommunityReadResult::Neighbors(page) => page,
        _ => panic!("neighbors"),
    };
    let neighbor = neighbors
        .entities
        .iter()
        .find(|entity| entity.uuid == a)
        .unwrap();
    assert_eq!(neighbor.text, after.text);
    assert_eq!(neighbor.text_hash, after.text_hash);
    assert_eq!(neighbor.valid_until, after.valid_until);
    let mut oversized = serde_json::Map::new();
    write_property(
        &mut oversized,
        "oversized",
        Some(&PropertyValue::String(
            "x".repeat(MAX_ENTITY_TEXT_BYTES * 2),
        )),
    );
    g.apply_mutations(
        &org,
        &[M::UpdateEntity {
            uuid: a,
            properties: oversized,
        }],
    )
    .await
    .unwrap();
    assert!(g.read_community(&org, &request).await.is_err());
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn expired_publication_and_incremental_update_preserve_the_active_community() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(a, "api"), entity(b, "worker")])
        .await
        .unwrap();
    let baseline = state(&g, &org).await;
    let original = definition(&org, 1);
    let generation = staged(&g, &org, baseline.clone(), original.clone(), &[a]).await;
    g.apply_mutations(&org, &[publish(generation, baseline)])
        .await
        .unwrap();
    let active = state(&g, &org).await;
    let before = raw_community(&g, &org, original.node.uuid).await;

    let mut expired = definition(&org, 1);
    expired.valid_until = Some(Utc::now() - chrono::Duration::seconds(1));
    let staged_generation = staged(&g, &org, active.clone(), expired.clone(), &[b]).await;
    assert!(matches!(
        g.apply_mutations(&org, &[publish(staged_generation, active.clone())])
            .await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(state(&g, &org).await, active);
    let rows = g
        .execute_read(
            "MATCH (g:CommunityGeneration {org_id:$org,uuid:$generation}) RETURN g.status AS status",
            &json!({"org":org,"generation":staged_generation}),
        )
        .await
        .unwrap();
    assert_eq!(rows[0]["status"], "staging");

    expired.node.uuid = original.node.uuid;
    expired.node.name = "Rejected expired replacement".into();
    let update = M::UpdateCommunities {
        update: Box::new(UpdateCommunities {
            generation,
            expected_state: active.clone(),
            publication_revision: Uuid::new_v4(),
            communities: vec![GuardedCommunityWrite {
                expected_revision: original.revision,
                definition: expired,
                members: vec![CommunityMember {
                    entity_uuid: b,
                    chain_id: b,
                }],
            }],
        }),
    };
    assert!(matches!(
        g.apply_mutations(&org, &[update]).await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(state(&g, &org).await, active);
    assert_eq!(
        raw_community(&g, &org, original.node.uuid).await.properties,
        before.properties
    );
    let CommunityReadResult::Members(page) = g
        .read_community(
            &org,
            &CommunityRead::Members {
                namespace: "prod".into(),
                generation,
                community_uuid: original.node.uuid,
                after_chain_id: None,
                limit: 10,
            },
        )
        .await
        .unwrap()
    else {
        panic!("missing Community members")
    };
    assert!(page.exhausted);
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].entity_uuid, a);
    assert_eq!(page.records[0].chain_id, a);
}
