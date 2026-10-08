use kg_core::traits::GraphBackend;
use serde_json::json;

#[tokio::test]
#[ignore = "schema: exclusive destructive Neo4j"]
async fn startup_rejects_a_constraint_with_the_right_name_but_wrong_property() {
    kg_neo4j_testkit::env::exclusive_neo4j().unwrap_or_else(|e| panic!("{e}"));
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.unwrap();
    graph
        .execute_write("DROP CONSTRAINT operation_receipt_batch_unique", &json!({}))
        .await
        .unwrap();
    graph.execute_write("CREATE CONSTRAINT operation_receipt_batch_unique FOR (n:OperationReceipt) REQUIRE n.wrong_property IS UNIQUE", &json!({})).await.unwrap();
    let result = graph.ensure_indexes().await;
    graph
        .execute_write("DROP CONSTRAINT operation_receipt_batch_unique", &json!({}))
        .await
        .unwrap();
    graph.ensure_indexes().await.unwrap();
    let error = result.unwrap_err().to_string();
    assert!(error.contains("operation_receipt_batch_unique"), "{error}");
    assert!(error.contains("incompatible"), "{error}");
}

#[tokio::test]
#[ignore = "schema: exclusive destructive Neo4j"]
async fn startup_rejects_wrong_range_definition_and_unindexed_identity() {
    kg_neo4j_testkit::env::exclusive_neo4j().unwrap_or_else(|e| panic!("{e}"));
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.unwrap();
    graph
        .execute_write("DROP INDEX entity_chain_id_idx", &json!({}))
        .await
        .unwrap();
    graph
        .execute_write(
            "CREATE INDEX entity_chain_id_idx FOR (n:Entity) ON (n.wrong_property)",
            &json!({}),
        )
        .await
        .unwrap();
    let result = graph.ensure_indexes().await;
    graph
        .execute_write("DROP INDEX entity_chain_id_idx", &json!({}))
        .await
        .unwrap();
    graph.ensure_indexes().await.unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("entity_chain_id_idx"));
    let org = uuid::Uuid::new_v4().to_string();
    graph
        .execute_write(
            "CREATE (:Entity {org_id:$org,uuid:randomUUID(),identity_hash:'unindexed'})",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    let result = graph.ensure_indexes().await;
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    assert!(result.unwrap_err().to_string().contains("reimport"));
    graph.ensure_indexes().await.unwrap();
    graph.close().await.unwrap();
}

#[tokio::test]
#[ignore = "schema: exclusive destructive Neo4j"]
async fn startup_rejects_legacy_evidence_and_its_constraint() {
    kg_neo4j_testkit::env::exclusive_neo4j().unwrap_or_else(|e| panic!("{e}"));
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.unwrap();
    let org = uuid::Uuid::new_v4().to_string();
    graph.execute_write(
        "CREATE (s:Snapshot {org_id:$org}), (n:Entity {org_id:$org}) CREATE (s)-[:OBSERVED_IN]->(n)",
        &json!({"org":org}),
    ).await.unwrap();
    let legacy = graph.ensure_indexes().await;
    graph
        .execute_write(
            "MATCH (n {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    let error = legacy.unwrap_err().to_string();
    assert!(
        error.contains("OBSERVED_IN") && error.contains("re-ingest"),
        "{error}"
    );
    graph.ensure_indexes().await.unwrap();

    graph
        .execute_write("DROP CONSTRAINT observation_uuid_unique", &json!({}))
        .await
        .unwrap();
    graph.execute_write("CREATE CONSTRAINT observation_uuid_unique FOR ()-[r:OBSERVED_IN]-() REQUIRE r.uuid IS UNIQUE", &json!({})).await.unwrap();
    let legacy_constraint = graph.ensure_indexes().await;
    graph
        .execute_write("DROP CONSTRAINT observation_uuid_unique", &json!({}))
        .await
        .unwrap();
    graph.ensure_indexes().await.unwrap();
    let error = legacy_constraint.unwrap_err().to_string();
    assert!(
        error.contains("observation_uuid_unique") && error.contains("incompatible"),
        "{error}"
    );
}
