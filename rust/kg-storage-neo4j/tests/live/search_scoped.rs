//! Storage search contracts that must be verified by Neo4j rather than a fake:
//! signed cosine scores, exact/indexed parity, temporal and namespace scoping of
//! similarity reads, tenant fallback read budgets and keyword index scoping.
use kg_core::{search::*, traits::SearchBackend};
use kg_neo4j_testkit::Fixture;
use serde_json::json;

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn cosine_preserves_signed_scores_and_skips_invalid_vectors() {
    use kg_core::{embedding, traits::graph_backend::GraphEmbedding};
    let f = Fixture::new().await;
    f.entity(1000, json!({})).await;
    let vectors = [
        vec![1.0, 0.0],
        vec![0.0, 1.0],
        vec![-1.0, 0.0],
        vec![3e38, 3e38],
        vec![1e-40, 1e-40],
        vec![0.0, 0.0],
        vec![1.0, 0.0, 0.0],
    ];
    for (index, vector) in vectors.iter().enumerate() {
        let n = index as u128 + 1;
        f.entity(n,json!({"embedding":vector,"embedding_model":"test","embedding_text_version":embedding::TEXT_VERSION})).await;
        f.fact(n+100,n,1000,json!({"embedding":vector,"embedding_model":"test","embedding_text_version":embedding::RELATIONSHIP_TEXT_VERSION})).await;
    }
    f.entity(
        8,
        json!({"embedding_model":"test","embedding_text_version":embedding::TEXT_VERSION}),
    )
    .await;
    f.fact(108,8,1000,json!({"embedding_model":"test","embedding_text_version":embedding::RELATIONSHIP_TEXT_VERSION})).await;
    f.graph.execute_write("MATCH (n:Entity {uuid:$node})-[r:RELATES_TO {uuid:$edge}]->() SET n.embedding=[toFloat('NaN'),1.0],r.embedding=[toFloat('NaN'),1.0]",&json!({"node":f.id(8),"edge":f.id(108)})).await.unwrap();
    let embedding = GraphEmbedding {
        model: "test".into(),
        values: vec![1.0, 0.0],
    };
    for cutoff in [-1.0, 0.5] {
        let nodes = f
            .graph
            .search_nodes(&NodeSearch {
                embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
                filter: f.filter(),
                query: NodeQuery::Similarity(embedding.clone()),
                chain_ids: None,
                limit: 20,
                min_score: cutoff,
                signals: NodeSignals::default(),
                projection: NodeProjection::Full,
            })
            .await
            .unwrap()
            .items;
        let facts = f
            .graph
            .search_relationship_similarity(&RelationshipSimilarity {
                filter: f.filter(),
                embedding: embedding.clone(),
                limit: 20,
                min_score: cutoff,
                anchor_chains: None,
            })
            .await
            .unwrap()
            .items;
        for (n, score) in [
            (1, 1.0),
            (2, 0.0),
            (3, -1.0),
            (4, std::f32::consts::FRAC_1_SQRT_2),
            (5, std::f32::consts::FRAC_1_SQRT_2),
        ] {
            let node = nodes.iter().find(|hit| hit.uuid == f.id(n));
            let fact = facts.iter().find(|hit| hit.uuid == f.id(n + 100));
            if score < cutoff {
                assert!(node.is_none() && fact.is_none());
            } else {
                assert!((node.unwrap().score - score).abs() < 1e-6);
                assert!((fact.unwrap().score - score).abs() < 1e-6);
            }
        }
        assert_eq!(nodes.len(), if cutoff < 0.0 { 5 } else { 3 });
        assert_eq!(facts.len(), nodes.len());
    }
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn indexed_semantic_reads_preserve_history_and_namespace_boundaries() {
    use kg_core::{embedding, traits::graph_backend::GraphEmbedding};
    let f = Fixture::new().await;
    f.entity(1, json!({"is_latest":false,"valid_to":"2026-07-01T00:00:00Z",
        "embedding":[1.0,0.0],"embedding_model":"test","embedding_text_version":embedding::TEXT_VERSION})).await;
    f.entity(2, json!({"chain_id":f.id(1),"valid_from":"2026-07-01T00:00:00Z",
        "embedding":[1.0,0.0],"embedding_model":"test","embedding_text_version":embedding::TEXT_VERSION})).await;
    f.entity(3, json!({"namespace":"staging"})).await;
    f.fact(101,1,3,json!({"is_latest":false,"valid_to":"2026-07-01T00:00:00Z",
        "embedding":[1.0,0.0],"embedding_model":"test","embedding_text_version":embedding::RELATIONSHIP_TEXT_VERSION})).await;
    f.fact(102,2,3,json!({"source_chain_id":f.id(1),"valid_from":"2026-07-01T00:00:00Z",
        "embedding":[1.0,0.0],"embedding_model":"test","embedding_text_version":embedding::RELATIONSHIP_TEXT_VERSION})).await;
    for (at, entity, fact) in [
        (None, 2, 102),
        (Some("2026-06-01T00:00:00Z"), 1, 101),
        (Some("2026-07-01T00:00:00Z"), 2, 102),
    ] {
        for namespaces in [
            vec![],
            vec!["prod".to_string()],
            vec!["prod".to_string(), "staging".to_string()],
        ] {
            let only_prod = namespaces.len() == 1;
            let filter = SearchFilter {
                org_id: f.org.clone(),
                namespaces,
                as_of: at.map(|v| v.parse().unwrap()),
                ..Default::default()
            };
            let embedding = GraphEmbedding {
                model: "test".into(),
                values: vec![1.0, 0.0],
            };
            let nodes = f
                .graph
                .search_nodes(&NodeSearch {
                    embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
                    filter: filter.clone(),
                    query: NodeQuery::Similarity(embedding.clone()),
                    chain_ids: None,
                    limit: 10,
                    min_score: 0.5,
                    signals: NodeSignals::default(),
                    projection: NodeProjection::Candidate,
                })
                .await
                .unwrap();
            assert_eq!(
                nodes.items.iter().map(|n| n.uuid).collect::<Vec<_>>(),
                vec![f.id(entity)]
            );
            let facts = f
                .graph
                .search_relationship_similarity(&RelationshipSimilarity {
                    filter,
                    embedding,
                    limit: 10,
                    min_score: 0.5,
                    anchor_chains: None,
                })
                .await
                .unwrap();
            assert_eq!(
                facts.items.iter().map(|r| r.uuid).collect::<Vec<_>>(),
                if only_prod { vec![] } else { vec![f.id(fact)] }
            );
        }
    }
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn indexed_recall_matches_exact_after_foreign_and_historical_candidates() {
    use kg_core::{embedding, traits::graph_backend::GraphEmbedding};
    let f = Fixture::new().await;
    f.entity(100000, json!({})).await;
    for start in (0..4200u128).step_by(100) {
        let rows: Vec<_>=(start..start+100).map(|n| {
            let foreign=n<3000;
            let old=n>=3600;
            let org=if foreign{format!("{}-foreign",f.org)}else{f.org.clone()};
            let values=if foreign{vec![1.0,0.2,0.3]}else{vec![1.0,if old{0.2}else{0.25}+(n%600)as f64*0.001,0.3]};
            json!({"uuid":f.id(n),"chain_id":f.id(if old{n-600}else{n}),"org_id":org,"namespace":"prod","name":"service","entity_type":"Service","is_latest":!old,"valid_from":if old||foreign{"2026-01-01T00:00:00Z"}else{"2026-07-01T00:00:00Z"},"valid_to":if old{Some("2026-07-01T00:00:00Z")}else{None},"embedding":values,"embedding_model":"test","embedding_text_version":embedding::TEXT_VERSION,"test_fixture":f.org})
        }).collect();
        f.graph
            .execute_write(
                "UNWIND $rows AS props CREATE (n:Entity) SET n=props",
                &json!({"rows":rows}),
            )
            .await
            .unwrap();
    }
    f.graph.execute_write("MATCH (n:Entity {test_fixture:$org}),(t:Entity {uuid:$target}) WHERE n.embedding IS NOT NULL CREATE (n)-[r:RELATES_TO]->(t) SET r.uuid=n.uuid,r.org_id=n.org_id,r.source_chain_id=n.chain_id,r.target_chain_id=t.chain_id,r.name='USES',r.description='dependency',r.is_latest=n.is_latest,r.valid_from=n.valid_from,r.valid_to=n.valid_to,r.embedding=n.embedding,r.embedding_model=n.embedding_model,r.embedding_text_version=$version",&json!({"org":f.org,"target":f.id(100000),"version":embedding::RELATIONSHIP_TEXT_VERSION})).await.unwrap();
    for at in [None, Some("2026-06-01T00:00:00Z")] {
        let filter = SearchFilter {
            as_of: at.map(|s| s.parse().unwrap()),
            ..f.filter()
        };
        let embedding = GraphEmbedding {
            model: "test".into(),
            values: vec![1.0, 0.2, 0.3],
        };
        let request = NodeSearch {
            embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: filter.clone(),
            query: NodeQuery::Similarity(embedding.clone()),
            chain_ids: None,
            limit: 10,
            min_score: 0.0,
            signals: NodeSignals::default(),
            projection: NodeProjection::Candidate,
        };
        let exact = f.graph.search_nodes(&request).await.unwrap();
        let indexed = f.graph.search_nodes_indexed(&request).await.unwrap();
        assert_eq!(exact.items.len(), 10);
        assert_eq!(
            indexed.items.iter().map(|h| h.uuid).collect::<Vec<_>>(),
            exact.items.iter().map(|h| h.uuid).collect::<Vec<_>>()
        );
        for (a, b) in indexed.items.iter().zip(&exact.items) {
            assert!((a.score - b.score).abs() < 1e-6);
        }
        let request = RelationshipSimilarity {
            filter,
            embedding,
            limit: 10,
            min_score: 0.0,
            anchor_chains: None,
        };
        let exact = f
            .graph
            .search_relationship_similarity(&request)
            .await
            .unwrap();
        let indexed = f
            .graph
            .search_relationships_indexed(&request)
            .await
            .unwrap();
        assert_eq!(exact.items.len(), 10);
        assert_eq!(
            indexed.items.iter().map(|h| h.uuid).collect::<Vec<_>>(),
            exact.items.iter().map(|h| h.uuid).collect::<Vec<_>>()
        );
    }
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn small_tenant_among_foreign_neighbors_falls_back_to_exact_with_bounded_reads() {
    use kg_core::{embedding, traits::graph_backend::GraphEmbedding};
    let f = Fixture::new().await;
    let foreign = format!("{}-foreign", f.org);
    // 20,000 foreign vectors sit closest to the query; 600 own vectors are
    // further away, so the index alone never surfaces the tenant.
    let mut rows = Vec::new();
    for n in 0..20_600u128 {
        let own = n >= 20_000;
        let namespace = if own && n % 30 == 0 {
            "staging"
        } else {
            "prod"
        };
        let values = if own {
            vec![0.6, 0.8 - (n % 600) as f64 * 0.0001, 0.0]
        } else {
            vec![1.0, (n % 100) as f64 * 0.00001, 0.0]
        };
        rows.push(json!({"uuid": f.id(n), "chain_id": f.id(n), "org_id": if own { f.org.clone() } else { foreign.clone() },
            "namespace": namespace, "name": "service", "entity_type": "Service", "is_latest": true,
            "valid_from": "2026-01-01T00:00:00Z", "embedding": values, "embedding_model": "test",
            "embedding_text_version": embedding::TEXT_VERSION, "test_fixture": f.org}));
        if rows.len() == 1000 || n == 20_599 {
            f.graph
                .execute_write(
                    "UNWIND $rows AS props CREATE (n:Entity) SET n=props",
                    &json!({"rows": std::mem::take(&mut rows)}),
                )
                .await
                .unwrap();
        }
    }
    f.graph
        .execute_read("CALL db.awaitIndexes(120)", &json!({}))
        .await
        .unwrap();
    let request = |namespaces: Vec<&str>, min_score: f32| NodeSearch {
        embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
        filter: SearchFilter {
            org_id: f.org.clone(),
            namespaces: namespaces.into_iter().map(String::from).collect(),
            ..Default::default()
        },
        query: NodeQuery::Similarity(GraphEmbedding {
            model: "test".into(),
            values: vec![1.0, 0.0, 0.0],
        }),
        chain_ids: None,
        limit: 10,
        min_score,
        signals: NodeSignals::default(),
        projection: NodeProjection::Candidate,
    };

    // Whole tenant: population above the exact threshold, zero index survivors,
    // no widening, exact fallback.
    let before = f.graph.search_reads();
    let exact = f
        .graph
        .search_nodes(&request(vec!["prod"], 0.0))
        .await
        .unwrap();
    let indexed = f
        .graph
        .search_nodes_indexed(&request(vec!["prod"], 0.0))
        .await
        .unwrap();
    assert_eq!(
        f.graph.search_reads(),
        before + 1 + 3,
        "population, one index call, exact scan"
    );
    assert!(!indexed.approximate && indexed.truncated);
    assert_eq!(exact.items.len(), 10);
    assert_eq!(
        indexed.items.iter().map(|h| h.uuid).collect::<Vec<_>>(),
        exact.items.iter().map(|h| h.uuid).collect::<Vec<_>>()
    );
    let own: std::collections::HashSet<_> = (20_000..20_600u128).map(|n| f.id(n)).collect();
    assert!(indexed
        .items
        .iter()
        .all(|h| h.namespace == "prod" && own.contains(&h.uuid)));

    // Selective filter: the staging slice is small enough for an exact scan directly.
    let before = f.graph.search_reads();
    let staging = f
        .graph
        .search_nodes_indexed(&request(vec!["staging"], 0.0))
        .await
        .unwrap();
    assert_eq!(f.graph.search_reads(), before + 2, "population then exact");
    assert_eq!(staging.items.len(), 10);
    assert!(!staging.approximate && staging.truncated);
    assert!(staging.items.iter().all(|h| h.namespace == "staging"));

    // Insufficient qualifying results: the foreign frontier stays above the
    // cutoff, so the index cannot prove exhaustion and the exact scan confirms
    // nothing qualifies.
    let before = f.graph.search_reads();
    let none = f
        .graph
        .search_nodes_indexed(&request(vec!["prod"], 0.9))
        .await
        .unwrap();
    assert_eq!(f.graph.search_reads(), before + 3);
    assert!(none.items.is_empty() && !none.truncated && !none.approximate);

    // A cutoff every own vector clears but the frontier sits below: exhaustion
    // ends the search after one index call, and the page is marked approximate.
    let mut close = request(vec!["prod"], 0.999);
    close.query = NodeQuery::Similarity(GraphEmbedding {
        model: "test".into(),
        values: vec![0.6, 0.8, 0.0],
    });
    close.filter.org_id = foreign.clone();
    let before = f.graph.search_reads();
    let foreign_page = f.graph.search_nodes_indexed(&close).await.unwrap();
    assert_eq!(
        f.graph.search_reads(),
        before + 2,
        "population then one index call"
    );
    assert!(foreign_page.items.is_empty() && foreign_page.approximate && !foreign_page.truncated);
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn keyword_scope_is_applied_inside_the_index_and_survives_punctuation_identities() {
    let f = Fixture::new().await;
    let own = format!("{}.corp-1_x", f.org);
    let big = format!("{}-big", f.org);
    let symbols = "***".to_string();
    let seed = |org: &str, count: usize, name: &str| json!({"count": count, "org": org, "name": name, "fixture": f.org});
    for (org, count, name) in [
        (own.as_str(), 5, "checkout gateway"),
        (big.as_str(), 3000, "checkout gateway"),
        (symbols.as_str(), 1, "checkout symbols"),
    ] {
        f.graph
            .execute_write(
                "UNWIND range(1,$count) AS i CREATE (n:Entity {uuid: randomUUID(), chain_id: randomUUID(), org_id: $org, namespace: 'prod', name: $name, entity_type: 'Service', is_latest: true, valid_from: '2026-01-01T00:00:00Z', prop_summary: 'Service in prod', test_fixture: $fixture})",
                &seed(org, count, name),
            )
            .await
            .unwrap();
    }
    f.graph
        .execute_read("CALL db.awaitIndexes(120)", &json!({}))
        .await
        .unwrap();
    let request = |org: &str, query: &str| NodeSearch {
        embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
        filter: SearchFilter {
            org_id: org.into(),
            ..Default::default()
        },
        query: NodeQuery::Fulltext(query.into()),
        chain_ids: None,
        limit: 10,
        min_score: 0.0,
        signals: NodeSignals::default(),
        projection: NodeProjection::Candidate,
    };
    let page = f
        .graph
        .search_nodes(&request(&own, "checkout"))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 5);
    assert!(!page.truncated);
    assert!(page.items.iter().all(|h| h.name == "checkout gateway"));
    let big_page = f
        .graph
        .search_nodes(&request(&big, "gateway"))
        .await
        .unwrap();
    assert_eq!(big_page.items.len(), 10);
    assert!(big_page.truncated);
    // Namespace and type words are filters, not searchable text; prop_summary is.
    assert!(!f
        .graph
        .search_nodes(&request(&own, "Service"))
        .await
        .unwrap()
        .items
        .is_empty());
    assert!(f
        .graph
        .search_nodes(&request(&own, "staging"))
        .await
        .unwrap()
        .items
        .is_empty());
    let scoped = f.graph.search_nodes(&request(&own, "prod")).await.unwrap();
    assert_eq!(
        scoped.items.len(),
        5,
        "'prod' matches the summary text, never the namespace"
    );
    // A tenant whose identity has no analyzer tokens is still found through the exact filter.
    let page = f
        .graph
        .search_nodes(&request(&symbols, "symbols"))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name, "checkout symbols");
    assert!(f
        .graph
        .search_nodes(&request(&own, "symbols"))
        .await
        .unwrap()
        .items
        .is_empty());
    f.cleanup().await;
}
