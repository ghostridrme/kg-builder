//! Live merge/split history and alias ownership contracts on a disposable database.
use chrono::{DateTime, Duration, Utc};
use kg_core::{
    search::{EvidenceSearch, NodeProjection, NodeQuery, NodeSearch, NodeSignals, SearchFilter},
    traits::{GraphBackend, GraphMutation as M, SearchBackend},
};
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::{json, Value};
use uuid::Uuid;

fn at(second: i64) -> DateTime<Utc> {
    "2026-09-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap() + Duration::seconds(second)
}
use kg_neo4j_testkit::indexed_graph as graph;
fn entity(id: Uuid) -> M {
    M::UpsertEntity { uuid: id, properties: json!({
        "chain_id":id,"namespace":"prod","entity_type":"Service","name":"service",
        "version":1,"is_latest":true,"valid_from":at(0),"last_seen_at":at(0),"identity_hash":id.to_string()
    }).as_object().unwrap().clone() }
}
fn merge(loser: Uuid, winner: Uuid, second: i64, hashes: &[&str]) -> M {
    M::MergeChains {
        loser_chain_id: loser,
        winner_chain_id: winner,
        effective_at: at(second),
        identity_hashes: hashes.iter().map(|h| h.to_string()).collect(),
    }
}
fn split(loser: Uuid, winner: Uuid, second: i64, hashes: &[&str]) -> M {
    M::SplitChain {
        split_chain_id: loser,
        from_chain_id: winner,
        effective_at: at(second),
        identity_hashes: hashes.iter().map(|h| h.to_string()).collect(),
    }
}
fn filter(org: &str, second: Option<i64>) -> SearchFilter {
    SearchFilter {
        org_id: org.into(),
        namespaces: vec!["prod".into()],
        as_of: second.map(at),
        ..Default::default()
    }
}
async fn visible(g: &Neo4jGraphBackend, org: &str, chain: Uuid, second: Option<i64>) -> bool {
    !g.search_nodes(&NodeSearch {
        embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
        filter: filter(org, second),
        query: NodeQuery::ByChain,
        chain_ids: Some(vec![chain]),
        limit: 10,
        min_score: 0.,
        projection: NodeProjection::Full,
        signals: NodeSignals::default(),
    })
    .await
    .unwrap()
    .items
    .is_empty()
}
async fn facts(g: &Neo4jGraphBackend, org: &str, chain: Uuid, second: i64) -> usize {
    g.search_relationships(&EvidenceSearch {
        filter: filter(org, Some(second)),
        query: None,
        passage_query: None,
        chain_ids: Some(vec![chain]),
        limit: 10,
    })
    .await
    .unwrap()
    .items
    .len()
}
async fn aliases(g: &Neo4jGraphBackend, org: &str, winner: Uuid) -> Vec<String> {
    let rows=g.execute_read("MATCH (n:Entity {org_id:$org,chain_id:$chain,is_latest:true}) RETURN coalesce(n.identity_hashes,[]) AS hashes",
        &json!({"org":org,"chain":winner})).await.unwrap();
    serde_json::from_value(rows[0]["hashes"].clone()).unwrap()
}
async fn edge(g: &Neo4jGraphBackend, org: &str, source: Uuid, target: Uuid, second: i64) -> Uuid {
    let id = Uuid::new_v4();
    g.apply_mutations(
        org,
        &[M::UpsertEdge {
            uuid: id,
            source_chain_id: source,
            target_chain_id: target,
            properties: json!({"name":"DEPENDS_ON","description":"dependency","is_latest":true,
            "valid_from":at(second),"last_seen_at":at(second)})
            .as_object()
            .unwrap()
            .clone(),
        }],
    )
    .await
    .unwrap();
    id
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn history_preserves_boundaries_and_split_never_reopens_facts() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (loser, winner, other) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(&org, &[entity(loser), entity(winner), entity(other)])
        .await
        .unwrap();
    edge(&g, &org, loser, other, 0).await;
    edge(&g, &org, loser, loser, 0).await;
    let first = merge(loser, winner, 10, &["shared"]);
    g.apply_mutations(&org, &[first.clone(), first.clone()])
        .await
        .unwrap();
    assert!(visible(&g, &org, loser, Some(9)).await);
    assert!(!visible(&g, &org, loser, Some(10)).await);
    assert!(!visible(&g, &org, loser, None).await);
    assert_eq!(facts(&g, &org, loser, 9).await, 2);
    assert_eq!(facts(&g, &org, loser, 10).await, 0);
    let restore = split(loser, winner, 20, &["shared"]);
    g.apply_mutations(&org, &[restore.clone(), restore])
        .await
        .unwrap();
    assert!(!visible(&g, &org, loser, Some(19)).await);
    assert!(visible(&g, &org, loser, Some(20)).await);
    assert!(visible(&g, &org, loser, None).await);
    assert_eq!(facts(&g, &org, loser, 20).await, 0);
    assert!(
        g.apply_mutations(&org, &[first]).await.is_err(),
        "old replay must not reopen a closed period"
    );
    edge(&g, &org, other, loser, 21).await;
    g.apply_mutations(
        &org,
        &[merge(loser, winner, 30, &[]), split(loser, winner, 40, &[])],
    )
    .await
    .unwrap();
    for (second, expected) in [
        (0, true),
        (9, true),
        (10, false),
        (19, false),
        (20, true),
        (29, true),
        (30, false),
        (39, false),
        (40, true),
    ] {
        assert_eq!(
            visible(&g, &org, loser, Some(second)).await,
            expected,
            "at {second}"
        );
    }
    assert_eq!(facts(&g, &org, loser, 29).await, 1);
    assert_eq!(facts(&g, &org, loser, 40).await, 0);
    assert_eq!(
        facts(&g, &org, loser, 9).await,
        2,
        "closed facts retain earlier history"
    );
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn overlapping_grants_preserve_native_aliases_and_release_the_last_claim() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (a, b, winner) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(
        &org,
        &[
            entity(a),
            entity(b),
            entity(winner),
            M::UpdateEntity {
                uuid: winner,
                properties: json!({"identity_hashes":["native"]})
                    .as_object()
                    .unwrap()
                    .clone(),
            },
        ],
    )
    .await
    .unwrap();
    let primary = winner.to_string();
    let grants = ["shared", "native", primary.as_str()];
    let ma = [merge(a, winner, 10, &grants)];
    let mb = [merge(b, winner, 10, &grants)];
    let (ra, rb) = tokio::join!(g.apply_mutations(&org, &ma), g.apply_mutations(&org, &mb));
    ra.unwrap();
    rb.unwrap();
    g.apply_mutations(&org, &[split(a, winner, 20, &grants)])
        .await
        .unwrap();
    assert!(aliases(&g, &org, winner)
        .await
        .contains(&"shared".to_string()));
    g.apply_mutations(&org, &[split(b, winner, 20, &grants)])
        .await
        .unwrap();
    let hashes = aliases(&g, &org, winner).await;
    assert!(!hashes.contains(&"shared".to_string()));
    assert!(hashes.contains(&"native".to_string()));
    assert!(hashes.contains(&primary));
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn newer_observations_block_merge_atomically_and_orphan_edges_are_closed() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (loser, winner, other) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(&org, &[entity(loser), entity(winner), entity(other)])
        .await
        .unwrap();
    let edge_id = edge(&g, &org, loser, other, 20).await;
    assert!(g
        .apply_mutations(&org, &[merge(loser, winner, 10, &["alias"])])
        .await
        .is_err());
    assert!(visible(&g, &org, loser, None).await);
    assert!(aliases(&g, &org, winner).await.is_empty());
    g.apply_mutations(
        &org,
        &[M::DeleteEntity {
            chain_id: other,
            deleted_at: at(21),
            deleted_by: None,
            reason: None,
        }],
    )
    .await
    .unwrap();
    g.apply_mutations(
        &org,
        &[
            merge(loser, winner, 30, &["alias"]),
            split(loser, winner, 40, &["alias"]),
        ],
    )
    .await
    .unwrap();
    let rows=g.execute_read("MATCH ()-[r:RELATES_TO {uuid:$id}]->() RETURN r.is_latest AS live,r.invalid_at AS ended",&json!({"id":edge_id})).await.unwrap();
    assert_eq!(rows[0]["live"], false);
    assert_eq!(rows[0]["ended"], json!(at(30).to_rfc3339()));
    // A split cannot revive a previously tombstoned source version.
    g.apply_mutations(
        &org,
        &[merge(other, winner, 45, &[]), split(other, winner, 50, &[])],
    )
    .await
    .unwrap();
    assert!(!visible(&g, &org, other, None).await);
    assert!(visible(&g, &org, other, Some(20)).await);
    assert!(!visible(&g, &org, other, Some(40)).await);
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn absent_loser_is_serialized_and_nested_merges_are_rejected() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (loser, a, b) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(&org, &[entity(a), entity(b)])
        .await
        .unwrap();
    let ma = [merge(loser, a, 10, &["alias"])];
    let mb = [merge(loser, b, 10, &["alias"])];
    let (ra, rb) = tokio::join!(g.apply_mutations(&org, &ma), g.apply_mutations(&org, &mb));
    assert_ne!(ra.is_ok(), rb.is_ok());
    let (winner, other) = if ra.is_ok() { (a, b) } else { (b, a) };
    assert!(g
        .apply_mutations(&org, &[split(loser, winner, 20, &["alias"])])
        .await
        .is_err());
    assert!(g
        .apply_mutations(&org, &[merge(winner, other, 20, &[])])
        .await
        .is_err());
    assert!(
        g.apply_mutations(&org, &[entity(loser)]).await.is_err(),
        "active absent loser cannot be materialized"
    );
    assert!(
        g.apply_mutations(&org, &[entity(Uuid::new_v4()), entity(loser)])
            .await
            .is_err(),
        "grouped writes obey the same merge fence"
    );
    assert!(!visible(&g, &org, loser, None).await);
    let rows = g
        .execute_read(
            "MATCH (m:ChainMerge {org_id:$org}) RETURN count(m) AS count",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    assert_eq!(rows[0]["count"], Value::from(1));
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn winner_must_exist_at_merge_time_but_its_current_version_may_be_newer() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (loser, winner, new) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let mut late = entity(winner);
    if let M::UpsertEntity { properties, .. } = &mut late {
        properties.insert("valid_from".into(), json!(at(10)));
    }
    g.apply_mutations(&org, &[entity(loser), late])
        .await
        .unwrap();
    assert!(g
        .apply_mutations(&org, &[merge(loser, winner, 5, &[])])
        .await
        .is_err());
    let mut successor = entity(new);
    if let M::UpsertEntity { properties, .. } = &mut successor {
        properties.insert("chain_id".into(), json!(winner));
        properties.insert("valid_from".into(), json!(at(20)));
        properties.insert("version".into(), json!(2));
        properties.insert("previous_version_uuid".into(), json!(winner));
    }
    g.apply_mutations(
        &org,
        &[
            M::SupersedeEntity {
                uuid: winner,
                chain_id: winner,
                valid_to: at(20),
            },
            successor,
            M::RepointEntity {
                previous_uuid: winner,
                new_uuid: new,
                chain_id: winner,
            },
            merge(loser, winner, 15, &[]),
        ],
    )
    .await
    .unwrap();
    assert!(visible(&g, &org, winner, Some(15)).await);
    assert!(!visible(&g, &org, loser, Some(15)).await);
    assert!(g
        .apply_mutations(
            &org,
            &[M::UpdateEntity {
                uuid: loser,
                properties: json!({"is_latest":true,"merged_into":null})
                    .as_object()
                    .unwrap()
                    .clone()
            }]
        )
        .await
        .is_err());
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn merge_shortens_an_active_finite_fact_before_its_scheduled_end() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (loser, winner, other) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(&org, &[entity(loser), entity(winner), entity(other)])
        .await
        .unwrap();
    let id = edge(&g, &org, loser, other, 0).await;
    g.apply_mutations(
        &org,
        &[M::UpdateEdge {
            uuid: id,
            properties: json!({
                "is_latest": false, "invalid_at": at(40)
            })
            .as_object()
            .unwrap()
            .clone(),
        }],
    )
    .await
    .unwrap();
    g.apply_mutations(
        &org,
        &[merge(loser, winner, 30, &[]), split(loser, winner, 35, &[])],
    )
    .await
    .unwrap();
    assert!(visible(&g, &org, loser, None).await);
    assert_eq!(facts(&g, &org, loser, 29).await, 1);
    assert_eq!(facts(&g, &org, loser, 35).await, 0);
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn split_fences_older_source_writes_without_changing_observation_time() {
    use kg_core::traits::{EntityLookup, Precondition};
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (loser, winner) = (Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(
        &org,
        &[
            entity(loser),
            entity(winner),
            merge(loser, winner, 10, &[]),
            split(loser, winner, 20, &[]),
        ],
    )
    .await
    .unwrap();
    let mut undated = entity(loser);
    if let M::UpsertEntity { properties, .. } = &mut undated {
        properties.remove("valid_from");
        properties.remove("last_seen_at");
    }
    assert!(g.apply_mutations(&org, &[undated]).await.is_err());
    let observed = |chain, second| M::ObserveEntity {
        chain_id: chain,
        observed_at: at(second),
        sync_generation: None,
        snapshot_id: None,
        collection: None,
    };
    let mut successor = entity(Uuid::new_v4());
    if let M::UpsertEntity { properties, .. } = &mut successor {
        properties.insert("chain_id".into(), json!(loser));
        properties.insert("valid_from".into(), json!(at(15)));
        properties.insert("last_seen_at".into(), json!(at(15)));
        properties.insert("version".into(), json!(2));
    }
    for mutations in [
        vec![observed(loser, 15)],
        vec![observed(winner, 15), observed(loser, 15)],
        vec![successor.clone()],
        vec![entity(Uuid::new_v4()), successor],
        vec![M::DeleteEntity {
            chain_id: loser,
            deleted_at: at(15),
            deleted_by: None,
            reason: None,
        }],
        vec![M::SupersedeEntity {
            uuid: loser,
            chain_id: loser,
            valid_to: at(15),
        }],
        vec![M::UpsertEdge {
            uuid: Uuid::new_v4(),
            source_chain_id: loser,
            target_chain_id: winner,
            properties: json!({"name":"DEPENDS_ON","is_latest":true,"valid_from":at(15)})
                .as_object()
                .unwrap()
                .clone(),
        }],
    ] {
        assert!(
            g.apply_mutations(&org, &mutations).await.is_err(),
            "accepted stale write: {mutations:?}"
        );
    }
    let statement = kg_storage_cypher::precondition(
        &org,
        &Precondition::NotObservedAfter {
            uuid: loser,
            observed_at: at(15),
        },
    )
    .unwrap();
    assert!(g
        .execute_read(&statement.statement, &statement.parameters)
        .await
        .unwrap()
        .is_empty());
    let rows = g
        .find_entities(
            &org,
            &EntityLookup::LatestByChain {
                chain_ids: vec![loser],
            },
        )
        .await
        .unwrap();
    assert_eq!(rows[0].last_seen_at, Some(at(0)));
    assert_eq!(rows[0].last_transition_at, Some(at(20)));
    g.apply_mutations(&org, &[observed(loser, 21)])
        .await
        .unwrap();
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn split_rejects_identity_reclaimed_while_loser_was_hidden() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let loser = Uuid::new_v4();
    let winner = Uuid::new_v4();
    let claimant = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(loser), entity(winner)])
        .await
        .unwrap();
    g.apply_mutations(&org, &[merge(loser, winner, 10, &[])])
        .await
        .unwrap();
    let mut claim = entity(claimant);
    if let M::UpsertEntity { properties, .. } = &mut claim {
        properties.insert("identity_hash".into(), json!(loser.to_string()));
    }
    g.apply_mutations(&org, &[claim]).await.unwrap();
    assert!(matches!(
        g.apply_mutations(&org, &[split(loser, winner, 20, &[])])
            .await,
        Err(kg_core::errors::BackendError::Conflict(_))
    ));
    assert!(!visible(&g, &org, loser, None).await);
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn merge_cancels_pending_intervals_preserves_bounds_and_split_never_revives_them() {
    use kg_core::models::CancellationContext;
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (loser, winner, other) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(&org, &[entity(loser), entity(winner), entity(other)])
        .await
        .unwrap();
    let mut ids = Vec::new();
    for (start, end) in [(0, Some(20)), (20, Some(40)), (40, None), (50, Some(50))] {
        let id = Uuid::new_v4();
        ids.push(id);
        g.apply_mutations(
            &org,
            &[M::UpsertEdge {
                uuid: id,
                source_chain_id: loser,
                target_chain_id: other,
                properties: json!({"name":"DEPENDS_ON", "description":"scheduled dependency",
                "is_latest":end.is_none(), "valid_from":at(start), "valid_to":end.map(at),
                "last_seen_at":at(if start == 50 { 99 } else { 0 }) })
                .as_object()
                .unwrap()
                .clone(),
            }],
        )
        .await
        .unwrap();
    }
    // A newer transition must reject the entire merge without alias or cancellation writes.
    g.apply_mutations(
        &org,
        &[M::UpdateEdge {
            uuid: ids[2],
            properties: json!({"last_transition_at":at(11)})
                .as_object()
                .unwrap()
                .clone(),
        }],
    )
    .await
    .unwrap();
    assert!(g
        .apply_mutations(&org, &[merge(loser, winner, 10, &["alias"])])
        .await
        .is_err());
    assert!(visible(&g, &org, loser, None).await);
    assert!(aliases(&g, &org, winner).await.is_empty());
    let untouched = g.execute_read("MATCH ()-[r:RELATES_TO {org_id:$org}]->() RETURN count(r.cancelled_at) AS cancelled,count(r.invalid_at) AS ended", &json!({"org":org})).await.unwrap();
    assert_eq!(untouched[0]["cancelled"], 0);
    assert_eq!(untouched[0]["ended"], 0);
    g.apply_mutations(
        &org,
        &[
            merge(loser, winner, 12, &["alias"]),
            split(loser, winner, 15, &["alias"]),
        ],
    )
    .await
    .unwrap();
    let rows = g.execute_read("MATCH ()-[r:RELATES_TO {org_id:$org}]->() RETURN properties(r) AS properties ORDER BY r.valid_from", &json!({"org":org})).await.unwrap();
    assert_eq!(
        rows[0]["properties"]["invalid_at"],
        json!(at(12).to_rfc3339())
    );
    assert_eq!(rows[0]["properties"]["valid_to"], json!(at(20)));
    for (row, start, end) in [(&rows[1], 20, Some(40)), (&rows[2], 40, None)] {
        let p = &row["properties"];
        assert_eq!(p["valid_from"], json!(at(start)));
        assert_eq!(p["valid_to"], json!(end.map(at)));
        assert_eq!(p["cancelled_at"], json!(at(12).to_rfc3339()));
        assert_eq!(p["is_latest"], false);
        assert!(p.get("cancellation_snapshot_id").is_none());
        let context: CancellationContext =
            serde_json::from_str(p["cancellation_context"].as_str().unwrap()).unwrap();
        assert_eq!(
            context,
            CancellationContext::Merge {
                loser_chain_id: loser,
                winner_chain_id: winner,
                effective_at: at(12)
            }
        );
    }
    let empty = &rows[3]["properties"];
    assert_eq!(empty["valid_from"], json!(at(50)));
    assert_eq!(empty["valid_to"], json!(at(50)));
    assert!(empty.get("cancelled_at").is_none());
    assert!(empty.get("invalid_at").is_none());
    assert_eq!(facts(&g, &org, loser, 11).await, 1);
    for second in [12, 15, 20, 40, 50] {
        assert_eq!(facts(&g, &org, loser, second).await, 0);
    }
    let periods = g.execute_read("MATCH (m:ChainMerge {org_id:$org,loser_chain_id:$loser,winner_chain_id:$winner}) RETURN m.valid_from AS start,m.valid_to AS end", &json!({"org":org,"loser":loser,"winner":winner})).await.unwrap();
    assert_eq!(periods.len(), 1);
    assert_eq!(periods[0]["start"], json!(at(12).to_rfc3339()));
    assert_eq!(periods[0]["end"], json!(at(15).to_rfc3339()));
    g.close().await.unwrap();
}
