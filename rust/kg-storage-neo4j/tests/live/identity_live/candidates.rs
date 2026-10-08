use kg_core::traits::{graph_backend::GraphEmbedding, *};
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::{json, Value};
use uuid::Uuid;

use kg_neo4j_testkit::indexed_graph as graph;
fn request(query: IdentityCandidateQuery, limit: usize) -> IdentityCandidateRequest {
    IdentityCandidateRequest {
        scope: IdentityScope {
            namespace: "prod".into(),
            entity_type: "Service".into(),
        },
        query,
        exclude_chains: vec![],
        limit,
    }
}
fn names(values: &[&str]) -> IdentityCandidateQuery {
    IdentityCandidateQuery::Names(values.iter().map(|v| v.to_string()).collect())
}
fn vector() -> IdentityCandidateQuery {
    IdentityCandidateQuery::Similarity {
        embedding: GraphEmbedding {
            model: "m".into(),
            values: vec![1.0, 0.0],
        },
        text_version: "test-v2".into(),
        min_score: 0.8,
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn exact_name_coverage_is_independent_of_a_large_keyword_frontier() {
    let live = kg_neo4j_testkit::LiveGraph::open_named("identity-name-frontier")
        .await
        .unwrap();
    let graph = live.backend();
    graph.ensure_indexes().await.unwrap();
    let expected = seed(&graph, live.org(), "orders", json!({})).await;
    let mutations: Vec<_> = (0..120)
        .map(|index| {
            let id = Uuid::new_v4();
            GraphMutation::UpsertEntity {
                uuid: id,
                properties: json!({
                    "chain_id":id,"namespace":"prod","entity_type":"Service",
                    "name":format!("orders worker {index}"),"version":1,"is_latest":true,
                    "valid_from":"2026-01-01T00:00:00Z"
                })
                .as_object()
                .unwrap()
                .clone(),
            }
        })
        .collect();
    graph.apply_mutations(live.org(), &mutations).await.unwrap();
    let exact = graph
        .identity_candidates(
            live.org(),
            &request(
                IdentityCandidateQuery::ExactNames(vec!["orders".into()]),
                15,
            ),
        )
        .await
        .unwrap();
    assert!(!exact.truncated);
    assert_eq!(exact.items.len(), 1);
    assert_eq!(exact.items[0].record.uuid, expected);
    let ranked = graph
        .identity_candidates(live.org(), &request(names(&["orders"]), 15))
        .await
        .unwrap();
    assert!(ranked.truncated);
    assert_eq!(ranked.items.len(), 15);
    assert_eq!(ranked.items[0].record.uuid, expected);
    live.cleanup().await.unwrap();
}
async fn seed(graph: &Neo4jGraphBackend, org: &str, name: &str, extra: Value) -> Uuid {
    let id = Uuid::new_v4();
    let mut props = json!({"chain_id":id,"namespace":"prod","entity_type":"Service","name":name,"version":1,"is_latest":true,"valid_from":"2026-01-01T00:00:00Z"}).as_object().unwrap().clone();
    props.extend(extra.as_object().unwrap().clone());
    graph
        .apply_mutations(
            org,
            &[GraphMutation::UpsertEntity {
                uuid: id,
                properties: props,
            }],
        )
        .await
        .unwrap();
    id
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn names_are_scoped_and_literal_punctuation_needs_no_vector() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    for name in ["🚀", "api\" OR *", "日本語サービス"] {
        let expected = seed(&graph, &org, name, json!({})).await;
        seed(&graph, "foreign", name, json!({})).await;
        seed(&graph, &org, name, json!({"namespace":"dev"})).await;
        seed(&graph, &org, name, json!({"entity_type":"Repository"})).await;
        let page = graph
            .identity_candidates(&org, &request(names(&[name]), 100))
            .await
            .unwrap();
        assert!(!page.truncated);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].record.uuid, expected);
        assert!(page.items[0].record.embedding.is_none());
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn eligibility_precedes_limit_and_sentinel_distinguishes_a_full_page() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let compatible =
        json!({"embedding":[1.0,0.0],"embedding_model":"m","embedding_text_version":"test-v2"});
    let mut excluded = Vec::new();
    for extra in [
        json!({"deleted_at":"2026-02-01T00:00:00Z"}),
        json!({"valid_to":"2026-02-01T00:00:00Z"}),
        json!({"is_latest":false}),
        json!({"merged_into":Uuid::new_v4()}),
        json!({"namespace":"dev"}),
        json!({"entity_type":"Repository"}),
    ] {
        let mut props = compatible.as_object().unwrap().clone();
        props.extend(extra.as_object().unwrap().clone());
        seed(&graph, &org, "api", Value::Object(props)).await;
    }
    excluded.push(seed(&graph, &org, "api", compatible.clone()).await);
    let valid = seed(&graph, &org, "api", compatible.clone()).await;
    for query in [names(&["api"]), vector()] {
        let mut req = request(query, 1);
        req.exclude_chains = excluded.clone();
        let page = graph.identity_candidates(&org, &req).await.unwrap();
        assert!(!page.truncated);
        assert_eq!(page.items[0].record.uuid, valid);
    }
    seed(&graph, &org, "api", compatible).await;
    for query in [names(&["api"]), vector()] {
        let mut req = request(query, 1);
        req.exclude_chains = excluded.clone();
        let page = graph.identity_candidates(&org, &req).await.unwrap();
        assert!(page.truncated);
        assert_eq!(page.items.len(), 1);
        req.limit = 2;
        assert!(
            !graph
                .identity_candidates(&org, &req)
                .await
                .unwrap()
                .truncated
        );
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn vector_compatibility_is_filtered_before_limit() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    for props in [
        json!({"embedding":[1.0,0.0],"embedding_model":"wrong","embedding_text_version":"test-v2"}),
        json!({"embedding":[1.0,0.0],"embedding_model":"m","embedding_text_version":"old"}),
        json!({"embedding":[1.0,0.0,0.0],"embedding_model":"m","embedding_text_version":"test-v2"}),
    ] {
        seed(&graph, &org, "api", props).await;
    }
    let expected = seed(
        &graph,
        &org,
        "api",
        json!({"embedding":[0.99,0.1],"embedding_model":"m","embedding_text_version":"test-v2"}),
    )
    .await;
    let page = graph
        .identity_candidates(&org, &request(vector(), 1))
        .await
        .unwrap();
    assert!(!page.truncated);
    assert_eq!(page.items[0].record.uuid, expected);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn duplicate_live_heads_fail_even_when_only_one_head_matches() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let chain = seed(
        &graph,
        &org,
        "api",
        json!({"embedding":[1.0,0.0],"embedding_model":"m","embedding_text_version":"test-v2"}),
    )
    .await;
    seed(
        &graph,
        &org,
        "worker",
        json!({"chain_id":chain,"embedding_model":"different"}),
    )
    .await;
    for query in [names(&["api"]), vector()] {
        assert!(graph
            .identity_candidates(&org, &request(query, 1))
            .await
            .is_err());
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn all_component_names_are_searched_and_channels_deduplicate_before_limit() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let expected = seed(&graph, &org, "actual service", json!({})).await;
    let page = graph
        .identity_candidates(
            &org,
            &request(
                names(&["not-present", "actual service", "actual service"]),
                1,
            ),
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].record.uuid, expected);
    assert!(!page.truncated);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn property_agreements_preserve_types_scope_and_recover_unembedded_nodes() {
    use kg_core::models::PropertyValue;
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let target = seed(&graph,&org,"opaque",json!({"prop_id":42,"property_type_id":"i","prop_cluster":"blue","property_type_cluster":"s","prop_status":"changed","property_type_status":"s"})).await;
    seed(
        &graph,
        &org,
        "string-id",
        json!({"prop_id":"42","property_type_id":"s"}),
    )
    .await;
    seed(
        &graph,
        &org,
        "float-id",
        json!({"prop_id":42.0,"property_type_id":"f"}),
    )
    .await;
    seed(
        &graph,
        "foreign",
        "opaque",
        json!({"prop_id":42,"property_type_id":"i"}),
    )
    .await;
    let page = graph
        .identity_candidates(
            &org,
            &request(
                IdentityCandidateQuery::PropertyOverlap(vec![
                    ("id".into(), PropertyValue::Integer(42)),
                    // Alternative values still count as one property, with type preserved.
                    ("id".into(), PropertyValue::Integer(43)),
                    ("cluster".into(), PropertyValue::String("blue".into())),
                    ("status".into(), PropertyValue::String("previous".into())),
                ]),
                15,
            ),
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].record.uuid, target);
    assert!(page.items[0].record.embedding.is_none());
    assert!((page.items[0].score - 2.0 / 3.0).abs() < 1e-9);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn property_ranking_counts_a_changed_key_once_and_reports_bounded_window() {
    use kg_core::models::PropertyValue;
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    for index in 0..102 {
        seed(
            &graph,
            &org,
            &format!("opaque{index}"),
            json!({"prop_cluster":"blue","property_type_cluster":"s"}),
        )
        .await;
    }
    let page = graph
        .identity_candidates(
            &org,
            &request(
                IdentityCandidateQuery::PropertyOverlap(vec![
                    ("cluster".into(), PropertyValue::String("blue".into())),
                    ("cluster".into(), PropertyValue::String("green".into())),
                ]),
                15,
            ),
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 15);
    assert!(page.truncated);
    assert!(page.items.iter().all(|item| item.score == 1.0));
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn inferred_type_frontier_preserves_namespace_org_liveness_and_page_bounds() {
    let graph = graph().await;
    let org = Uuid::new_v4().to_string();
    let evidence = json!({"embedding":[1.0,0.0],"embedding_model":"m","embedding_text_version":"test-v2",
        "prop_cluster":"blue","property_type_cluster":"s"});
    let first = seed(&graph, &org, "api", evidence.clone()).await;
    let mut other_type = evidence.clone();
    other_type["entity_type"] = json!("Application");
    let second = seed(&graph, &org, "api", other_type.clone()).await;
    for extra in [
        json!({"namespace":"elsewhere"}),
        json!({"deleted_at":"2026-02-01T00:00:00Z"}),
        json!({"valid_to":"2026-02-01T00:00:00Z"}),
        json!({"is_latest":false}),
    ] {
        let mut props = other_type.clone();
        props
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        seed(&graph, &org, "api", props).await;
    }
    seed(&graph, &Uuid::new_v4().to_string(), "api", other_type).await;
    for query in [
        names(&["api"]),
        vector(),
        IdentityCandidateQuery::PropertyOverlap(vec![(
            "cluster".into(),
            kg_core::models::PropertyValue::String("blue".into()),
        )]),
    ] {
        let mut req = request(query, 1);
        req.scope.entity_type = "*".into();
        assert!(
            graph
                .identity_candidates(&org, &req)
                .await
                .unwrap()
                .truncated
        );
        req.limit = 10;
        let page = graph.identity_candidates(&org, &req).await.unwrap();
        assert!(!page.truncated);
        let ids: std::collections::HashSet<_> = page.items.iter().map(|c| c.record.uuid).collect();
        assert_eq!(ids, std::collections::HashSet::from([first, second]));
        req.scope.entity_type = "Service".into();
        let page = graph.identity_candidates(&org, &req).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].record.uuid, first);
    }
}
