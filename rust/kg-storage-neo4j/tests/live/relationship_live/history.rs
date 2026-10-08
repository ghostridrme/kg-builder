//! Historical facts can precede recorded endpoint versions without inventing entity history.
use kg_core::{
    search::*,
    traits::{graph_backend::GraphEmbedding, GraphBackend, GraphMutation, SearchBackend},
};
use kg_storage_cypher as cypher;
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::json;
use uuid::Uuid;

struct Fixture {
    org: String,
    source: Uuid,
    target: Uuid,
    physical: Uuid,
    edge: Uuid,
    snapshot: Uuid,
}

async fn assert_retrieval(
    graph: &Neo4jGraphBackend,
    fixture: &Fixture,
    filter: SearchFilter,
    expected: bool,
) {
    let evidence = EvidenceSearch {
        filter: filter.clone(),
        query: Some("historical checkout dependency".into()),
        passage_query: None,
        chain_ids: None,
        limit: 10,
    };
    let page = graph.search_relationships(&evidence).await.unwrap();
    assert_eq!(page.items.len(), usize::from(expected), "fulltext");
    if expected {
        assert_eq!(page.items[0].uuid, fixture.edge);
        assert_eq!(page.items[0].source_chain_id, fixture.source);
        assert_eq!(page.items[0].target_chain_id, fixture.target);
        assert_eq!(page.items[0].snapshot_id, Some(fixture.snapshot));
    }
    let similarity = RelationshipSimilarity {
        filter: filter.clone(),
        embedding: GraphEmbedding {
            model: "history-test".into(),
            values: vec![1.0, 0.0],
        },
        limit: 10,
        min_score: 0.5,
        anchor_chains: None,
    };
    // Execute both prepared paths directly: a small fixture otherwise selects exact scoring.
    for query in [
        cypher::relationship_similarity(&similarity).unwrap(),
        cypher::indexed_relationships(&similarity, 100).unwrap(),
    ] {
        let rows = graph
            .execute_read(&query.statement, &query.parameters)
            .await
            .unwrap();
        let ids: Vec<_> = rows
            .iter()
            .filter_map(|row| row.get("uuid").and_then(serde_json::Value::as_str))
            .collect();
        assert_eq!(ids.len(), usize::from(expected), "vector retrieval");
        if expected {
            assert_eq!(ids[0], fixture.edge.to_string());
        }
    }
    let population = cypher::vector_population(
        &filter,
        "history-test",
        true,
        kg_core::embedding::TEXT_VERSION,
        &None,
    );
    let rows = graph
        .execute_read(&population.statement, &population.parameters)
        .await
        .unwrap();
    assert_eq!(rows[0]["count"], usize::from(expected));
    let coverage = graph
        .embedding_readiness(&EmbeddingReadinessRequest {
            entity_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: filter.clone(),
            scope: SearchScope::Relationships,
            model: "history-test".into(),
            dimensions: 2,
        })
        .await
        .unwrap();
    assert_eq!(coverage.eligible, usize::from(expected));
    assert_eq!(coverage.compatible, usize::from(expected));
    let attached = graph
        .attached_relationships(&AttachedEvidence {
            filter,
            anchors: vec![fixture.source],
            per_anchor: 10,
            passage_query: None,
        })
        .await
        .unwrap();
    assert_eq!(attached.items.len(), usize::from(expected), "attached");
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn historical_fact_uses_earliest_known_classification_without_backfilling_entity_history() {
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.unwrap();
    let fixture = Fixture {
        org: format!("relationship-history-{}", Uuid::new_v4()),
        source: Uuid::new_v4(),
        target: Uuid::new_v4(),
        physical: Uuid::new_v4(),
        edge: Uuid::new_v4(),
        snapshot: Uuid::new_v4(),
    };
    for (uuid, chain, namespace, kind, from, to, latest) in [
        (
            fixture.source,
            fixture.source,
            "prod",
            "Service",
            "2026-03-01T00:00:00Z",
            Some("2026-04-01T00:00:00Z"),
            false,
        ),
        (
            fixture.physical,
            fixture.source,
            "changed",
            "RenamedType",
            "2026-04-01T00:00:00Z",
            None,
            true,
        ),
        (
            fixture.target,
            fixture.target,
            "prod",
            "Database",
            "2026-03-01T00:00:00Z",
            None,
            true,
        ),
    ] {
        graph.execute_write("CREATE (n:Entity) SET n=$props", &json!({"props":{
            "uuid":uuid,"chain_id":chain,"org_id":fixture.org,"namespace":namespace,
            "entity_type":kind,"name":"checkout","valid_from":from,"valid_to":to,"is_latest":latest
        }})).await.unwrap();
    }
    graph.execute_write("MATCH (s:Entity {uuid:$source}),(t:Entity {uuid:$target}) CREATE (s)-[r:RELATES_TO]->(t) SET r=$props CREATE (:Snapshot {uuid:$snapshot,org_id:$org,namespace:'prod',name:'later report',source:'logs',content:'historical checkout dependency',captured_at:'2026-05-01T00:00:00Z'})", &json!({
        "source":fixture.physical,"target":fixture.target,"snapshot":fixture.snapshot,"org":fixture.org,
        "props":{"uuid":fixture.edge,"org_id":fixture.org,"source_chain_id":fixture.source,"target_chain_id":fixture.target,"name":"DEPENDS_ON","description":"historical checkout dependency","valid_from":"2026-01-01T00:00:00Z","valid_to":"2026-02-01T00:00:00Z","is_latest":false,"first_seen_snapshot_id":fixture.snapshot}
    })).await.unwrap();
    // Exercise the supported historical embedding write, not a fixture-only raw vector setter.
    graph
        .apply_mutations(
            &fixture.org,
            &[GraphMutation::SetRelationshipEmbedding {
                uuid: fixture.edge,
                embedding: GraphEmbedding {
                    model: "history-test".into(),
                    values: vec![1.0, 0.0],
                },
                text_version: kg_core::embedding::RELATIONSHIP_TEXT_VERSION.into(),
                content_hash: kg_core::embedding::content_hash("historical checkout dependency"),
            }],
        )
        .await
        .unwrap();
    graph
        .execute_read("CALL db.awaitIndexes(60)", &json!({}))
        .await
        .unwrap();
    let filter = SearchFilter {
        org_id: fixture.org.clone(),
        namespaces: vec!["prod".into()],
        entity_types: vec!["Service".into()],
        as_of: Some("2026-01-15T00:00:00Z".parse().unwrap()),
        ..Default::default()
    };
    assert_retrieval(&graph, &fixture, filter.clone(), true).await;
    let nodes = graph
        .search_nodes(&NodeSearch {
            embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: filter.clone(),
            query: NodeQuery::ByChain,
            chain_ids: Some(vec![fixture.source, fixture.target]),
            limit: 10,
            min_score: 0.0,
            projection: NodeProjection::Full,
            signals: NodeSignals::default(),
        })
        .await
        .unwrap();
    assert!(
        nodes.items.is_empty(),
        "fact identity must not fabricate historical entity state"
    );
    let snapshots = graph
        .search_snapshots(&EvidenceSearch {
            filter: filter.clone(),
            query: Some("historical checkout dependency".into()),
            passage_query: None,
            chain_ids: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(
        snapshots.items.is_empty(),
        "standalone snapshot time semantics are unchanged"
    );
    for alternate in [
        SearchFilter {
            org_id: format!("foreign-history-org-{}", Uuid::new_v4()),
            ..filter.clone()
        },
        SearchFilter {
            namespaces: vec!["changed".into()],
            ..filter.clone()
        },
        SearchFilter {
            entity_types: vec!["RenamedType".into()],
            ..filter.clone()
        },
        SearchFilter {
            as_of: None,
            ..filter.clone()
        },
        SearchFilter {
            as_of: Some("2026-02-01T00:00:00Z".parse().unwrap()),
            ..filter.clone()
        },
    ] {
        assert_retrieval(&graph, &fixture, alternate, false).await;
    }
    // Once a chain has known history, a later gap cannot use the prehistory fallback.
    let prior = Uuid::new_v4();
    graph.execute_write("CREATE (:Entity {uuid:$uuid,chain_id:$chain,org_id:$org,namespace:'prod',entity_type:'Service',name:'checkout',is_latest:false,valid_from:'2025-01-01T00:00:00Z',valid_to:'2026-02-01T00:00:00Z'})", &json!({"uuid":prior,"chain":fixture.source,"org":fixture.org})).await.unwrap();
    assert_retrieval(&graph, &fixture, filter.clone(), true).await;
    graph
        .execute_write(
            "MATCH (n:Entity {uuid:$uuid}) SET n.valid_to='2025-12-01T00:00:00Z'",
            &json!({"uuid":prior}),
        )
        .await
        .unwrap();
    assert_retrieval(&graph, &fixture, filter.clone(), false).await;
    graph
        .execute_write(
            "MATCH (n:Entity {uuid:$uuid}) DELETE n",
            &json!({"uuid":prior}),
        )
        .await
        .unwrap();
    graph
        .execute_write(
            "MATCH (n:Entity {uuid:$uuid}) SET n.deleted_at='2026-01-10T00:00:00Z'",
            &json!({"uuid":fixture.source}),
        )
        .await
        .unwrap();
    assert_retrieval(&graph, &fixture, filter.clone(), false).await;
    graph
        .execute_write(
            "MATCH (n:Entity {uuid:$uuid}) REMOVE n.deleted_at",
            &json!({"uuid":fixture.source}),
        )
        .await
        .unwrap();
    graph.execute_write("CREATE (:ChainMerge {org_id:$org,loser_chain_id:$chain,winner_chain_id:$winner,valid_from:'2026-01-10T00:00:00Z'})", &json!({"org":fixture.org,"chain":fixture.source,"winner":fixture.target})).await.unwrap();
    assert_retrieval(&graph, &fixture, filter, false).await;
    graph
        .execute_write(
            "MATCH (n {org_id:$org}) DETACH DELETE n",
            &json!({"org":fixture.org}),
        )
        .await
        .unwrap();
}
