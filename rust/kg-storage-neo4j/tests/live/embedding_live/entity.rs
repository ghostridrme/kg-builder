//! Opt-in contract against an isolated Neo4j test database.
use kg_core::traits::graph_backend::{GraphBackend, GraphEmbedding};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn entity_embeddings_are_graph_properties_and_search_is_scoped() {
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let org = format!("embedding-test-{}", Uuid::new_v4());
    let uuid = Uuid::new_v4();
    let embedding = GraphEmbedding {
        model: "test-model".into(),
        values: vec![0.8, 0.6],
    };
    assert!(graph
        .set_entity_embedding(
            &org,
            uuid,
            &embedding,
            kg_core::embedding::TEXT_VERSION,
            &kg_core::embedding::content_hash("type: Service\nname: test"),
            &kg_core::embedding::EntityEmbeddingFields::default()
        )
        .await
        .is_err());
    graph.execute_write(
        "CREATE (:Entity {org_id:$org_id, uuid:$uuid, chain_id:$chain_id, namespace:'prod', entity_type:'Service', name:'test', is_latest:true})",
        &json!({"org_id":org,"uuid":uuid.to_string(),"chain_id":Uuid::new_v4().to_string()}),
    ).await.unwrap();
    graph
        .set_entity_embedding(
            &org,
            uuid,
            &embedding,
            kg_core::embedding::TEXT_VERSION,
            &kg_core::embedding::content_hash("type: Service\nname: test"),
            &kg_core::embedding::EntityEmbeddingFields::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        graph.get_entity_embedding(&org, uuid).await.unwrap(),
        Some(embedding)
    );
    assert!(graph
        .get_entity_embedding("other-org", uuid)
        .await
        .unwrap()
        .is_none());
    let hits = graph
        .search_entity_embeddings(
            &[1.0, 0.0],
            "test-model",
            &org,
            Some(&["prod"]),
            Some(&["Service"]),
            1,
            0.0,
            kg_core::embedding::TEXT_VERSION,
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].uuid, uuid);
    assert!((hits[0].score - 0.8).abs() < 1e-6);
    for (scope, model, namespace) in [
        (&org[..], "other-model", "prod"),
        ("other-org", "test-model", "prod"),
        (&org[..], "test-model", "dev"),
    ] {
        assert!(graph
            .search_entity_embeddings(
                &[1.0, 0.0],
                model,
                scope,
                Some(&[namespace]),
                None,
                10,
                -1.0,
                kg_core::embedding::TEXT_VERSION
            )
            .await
            .unwrap()
            .is_empty());
    }
    assert!(graph
        .search_entity_embeddings(
            &[1.0, 0.0, 0.0],
            "test-model",
            &org,
            None,
            None,
            10,
            -1.0,
            kg_core::embedding::TEXT_VERSION
        )
        .await
        .unwrap()
        .is_empty());
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org_id, uuid:$uuid}) SET n.is_latest = false",
            &json!({"org_id":org,"uuid":uuid.to_string()}),
        )
        .await
        .unwrap();
    assert!(graph
        .search_entity_embeddings(
            &[1.0, 0.0],
            "test-model",
            &org,
            None,
            None,
            10,
            -1.0,
            kg_core::embedding::TEXT_VERSION
        )
        .await
        .unwrap()
        .is_empty());
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org_id}) DETACH DELETE n",
            &json!({"org_id":org}),
        )
        .await
        .unwrap();
}
