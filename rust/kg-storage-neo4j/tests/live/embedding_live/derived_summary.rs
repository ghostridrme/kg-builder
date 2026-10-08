//! Real fulltext, indexed/exact vector and hydration validity with independent base fallback.
use chrono::{Duration, Utc};
use kg_core::{
    search::*,
    traits::{graph_backend::GraphEmbedding, GraphBackend, SearchBackend},
};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn derived_summary_boundaries_scope_indexes_and_base_fallback() {
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.unwrap();
    let org = Uuid::new_v4().to_string();
    let other = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    let at = Utc::now();
    let end = at + Duration::hours(1);
    let row = |id: Uuid, org: &str, text: &str, vector: Vec<f32>| json!({"org_id":org,"uuid":id,"chain_id":id,"entity_type":"Service","namespace":"prod","name":"baseanchor","summary":"source text","version":1,"is_latest":true,"valid_from":(at-Duration::days(1)).to_rfc3339(),"derived_summary":text,"summary_revision":Uuid::new_v4(),"summary_as_of":(at-Duration::hours(1)).to_rfc3339(),"summary_valid_until":end.to_rfc3339(),"summary_policy_version":kg_core::entity_summary::POLICY_VERSION,"summary_evidence_hash":"hash","summary_evidence_ids":[],"summary_total_evidence":0,"summary_embedding":vector,"summary_embedding_model":"m","summary_embedding_text_version":kg_core::entity_summary::SUMMARY_TEXT_VERSION,"summary_embedding_content_hash":"hash","embedding":[1.0,0.0],"embedding_model":"m","embedding_text_version":kg_core::embedding::TEXT_VERSION,"embedding_content_hash":"base"});
    let mut rows = vec![
        row(id, &org, "violet deployment", vec![1.0, 0.0]),
        row(Uuid::new_v4(), &other, "violet deployment", vec![1.0, 0.0]),
    ];
    rows.extend((0..520).map(|_| row(Uuid::new_v4(), &org, "background", vec![0.0, 1.0])));
    graph
        .execute_write(
            "UNWIND $rows AS row CREATE (n:Entity) SET n=row",
            &json!({"rows":rows}),
        )
        .await
        .unwrap();
    let request = |time, query| SummarySearch {
        node: NodeSearch {
            embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: SearchFilter {
                org_id: org.clone(),
                namespaces: vec!["prod".into()],
                as_of: Some(time),
                relationship_now: Some(at),
                ..Default::default()
            },
            query,
            chain_ids: None,
            limit: 10,
            min_score: 0.95,
            signals: Default::default(),
            projection: NodeProjection::Full,
        },
    };
    for (time, count) in [
        (at - Duration::hours(2), 0),
        (at, 1),
        (end - Duration::milliseconds(1), 1),
        (end, 0),
        (end + Duration::seconds(1), 0),
    ] {
        let page = graph
            .search_entity_summaries(&request(time, NodeQuery::Fulltext("violet".into())), false)
            .await
            .unwrap();
        assert_eq!(page.items.len(), count);
        for indexed in [false, true] {
            let page = graph
                .search_entity_summaries(
                    &request(
                        time,
                        NodeQuery::Similarity(GraphEmbedding {
                            model: "m".into(),
                            values: vec![1.0, 0.0],
                        }),
                    ),
                    indexed,
                )
                .await
                .unwrap();
            assert_eq!(page.items.len(), count);
            if count == 1 {
                assert_eq!(page.items[0].uuid, id);
                assert!(page.items[0].derived_summary.as_ref().unwrap().contributed);
                if indexed {
                    assert!(page.approximate);
                }
            }
        }
        let mut base = request(time, NodeQuery::ByChain).node;
        base.chain_ids = Some(vec![id]);
        let page = graph.search_nodes(&base).await.unwrap();
        assert_eq!(page.items.len(), 1);
        let hit = &page.items[0];
        assert_eq!(hit.derived_summary.is_some(), count == 1);
        assert_eq!(hit.properties["summary"], "source text");
        assert!(hit.embedding.is_some());
        assert!(hit.properties.get("summary_embedding").is_none());
        assert!(hit.properties.get("derived_summary").is_none());
    }
    let ready = graph
        .summary_readiness(&EmbeddingReadinessRequest {
            entity_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: request(at, NodeQuery::Fulltext("x".into())).node.filter,
            scope: SearchScope::Nodes,
            model: "m".into(),
            dimensions: 2,
        })
        .await
        .unwrap();
    assert_eq!(ready.valid, 521);
    assert_eq!(ready.compatible, 521);
    assert_eq!(ready.missing, 0);
    for (vector, missing, incompatible) in [(json!([0.0, 0.0]), 0, 1), (json!(null), 1, 0)] {
        graph
            .execute_write(
                "MATCH (n:Entity {org_id:$org,uuid:$id}) SET n.summary_embedding=$vector",
                &json!({"org":org,"id":id,"vector":vector}),
            )
            .await
            .unwrap();
        let ready = graph
            .summary_readiness(&EmbeddingReadinessRequest {
                entity_text_version: kg_core::embedding::TEXT_VERSION.into(),
                filter: request(at, NodeQuery::Fulltext("x".into())).node.filter,
                scope: SearchScope::Nodes,
                model: "m".into(),
                dimensions: 2,
            })
            .await
            .unwrap();
        assert_eq!(ready.valid, 521);
        assert_eq!(ready.compatible, 520);
        assert_eq!(ready.missing, missing);
        assert_eq!(ready.incompatible, incompatible);
    }
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org,uuid:$id}) REMOVE n.summary_revision",
            &json!({"org":org,"id":id}),
        )
        .await
        .unwrap();
    assert!(graph
        .search_entity_summaries(&request(at, NodeQuery::Fulltext("violet".into())), false)
        .await
        .unwrap()
        .items
        .is_empty());
    let base = request(at, NodeQuery::Fulltext("baseanchor".into())).node;
    assert!(!graph.search_nodes(&base).await.unwrap().items.is_empty());
    graph
        .execute_write(
            "MATCH (n:Entity) WHERE n.org_id IN $orgs DETACH DELETE n",
            &json!({"orgs":[org,other]}),
        )
        .await
        .unwrap();
    graph.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn explorer_withholds_summary_and_evidence_outside_coverage() {
    use kg_core::traits::graph_explorer::{
        ExplorerDirection, ExplorerQuery, ExplorerRequest, GraphExplorerBackend,
    };
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let org = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    let other = Uuid::new_v4();
    let start = Utc::now() - Duration::days(2);
    let end = start + Duration::days(1);
    graph.execute_write(
        "CREATE (n:Entity), (m:Entity) SET n=$props, m=$props SET n.uuid=$id,n.chain_id=$id,m.uuid=$other,m.chain_id=$other CREATE (n)-[r:RELATES_TO]->(m) SET r.org_id=$org,r.uuid=$edge,r.name='LINK',r.source_chain_id=$id,r.target_chain_id=$other,r.valid_from=$valid_from",
        &json!({"id":id,"other":other,"org":org,"edge":Uuid::new_v4(),"valid_from":(start-Duration::days(1)).to_rfc3339(),"props":{
            "org_id":org,"namespace":"prod","entity_type":"Service","name":"test","version":1,"is_latest":true,
            "valid_from":(start-Duration::days(1)).to_rfc3339(),"summary":"source summary",
            "derived_summary":"future evidence","summary_revision":Uuid::new_v4(),"summary_as_of":start.to_rfc3339(),"summary_valid_until":end.to_rfc3339(),
            "summary_policy_version":kg_core::entity_summary::POLICY_VERSION,"summary_evidence_hash":"hash","summary_evidence_ids":["evidence"],"summary_total_evidence":1
        }}),
    ).await.unwrap();
    for (time, visible) in [
        (Some(start - Duration::seconds(1)), false),
        (Some(start), true),
        (Some(end - Duration::seconds(1)), true),
        (Some(end), false),
        (None, false),
    ] {
        for query in [
            ExplorerQuery::Entity {
                entity_type: "Service".into(),
                chain_id: id,
            },
            ExplorerQuery::Versions {
                entity_type: "Service".into(),
                chain_id: id,
            },
            ExplorerQuery::Neighbors {
                entity_type: "Service".into(),
                chain_id: id,
                direction: ExplorerDirection::Out,
                entity_types: Vec::new(),
            },
        ] {
            let neighbors = matches!(query, ExplorerQuery::Neighbors { .. });
            let page = graph
                .explore(&ExplorerRequest {
                    org_id: org.clone(),
                    namespace: Some("prod".into()),
                    as_of: time,
                    limit: 10,
                    offset: 0,
                    query,
                })
                .await
                .unwrap();
            assert_eq!(page.items.len(), 1);
            let value = if neighbors {
                &page.items[0]["entity"]
            } else {
                &page.items[0]
            };
            assert_eq!(value["summary"], "source summary");
            for field in [
                "derived_summary",
                "summary_revision",
                "summary_as_of",
                "summary_valid_until",
                "summary_policy_version",
                "summary_evidence_hash",
                "summary_evidence_ids",
                "summary_total_evidence",
            ] {
                assert_eq!(!value[field].is_null(), visible, "{field} at {time:?}");
            }
        }
    }
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
}
