//! Exact-content embedding writes include historical and deleted entity versions.
use kg_core::{
    errors::BackendError,
    traits::{
        graph_backend::GraphEmbedding, graph_mutation::entity_embedding_state, GraphBackend,
        GraphMutation,
    },
};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn historical_embedding_guards_complete_content_and_rolls_back_grouped_mismatch() {
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let org = Uuid::new_v4().to_string();
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    let baseline = json!({"name":"api","entity_type":"Service","namespace":"prod","version":1,
        "prop_port":5432,"property_type_port":"i","property_type_nullable":"n"})
    .as_object()
    .unwrap()
    .clone();
    for (index, id) in ids.iter().enumerate() {
        graph.execute_write("CREATE (n:Entity) SET n=$props,n.uuid=$uuid,n.chain_id=$uuid,n.org_id=$org,n.is_latest=false,n.valid_to='2026-01-01T00:00:00Z',n.deleted_at=$deleted", &json!({"props":baseline,"uuid":id,"org":org,"deleted":if index==1 {Some("2026-01-01T00:00:00Z")}else{None}})).await.unwrap();
    }
    let mutation = |id, expected| GraphMutation::SetEntityVersionEmbedding {
        uuid: id,
        expected_properties: expected,
        embedding: GraphEmbedding {
            model: "m".into(),
            values: vec![1.0, 0.0],
        },
        text_version: kg_core::embedding::TEXT_VERSION.into(),
        content_hash: "hash".into(),
    };
    let expected = entity_embedding_state(&baseline);
    graph
        .apply_mutations(
            &org,
            &ids.iter()
                .map(|id| mutation(*id, expected.clone()))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let rows = graph
        .execute_read(
            "MATCH (n:Entity {org_id:$org}) RETURN count(n.embedding) AS vectors",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    assert_eq!(rows[0]["vectors"], 2);
    // The original setter stays live-only.
    let strict = GraphMutation::SetEmbedding {
        uuid: ids[0],
        embedding: GraphEmbedding {
            model: "m".into(),
            values: vec![1.0, 0.0],
        },
        text_version: kg_core::embedding::TEXT_VERSION.into(),
        content_hash: "hash".into(),
    };
    let strict_result = graph.apply_mutations(&org, &[strict]).await;
    assert!(
        matches!(strict_result, Err(BackendError::NotFound(_))),
        "{strict_result:?}"
    );
    assert!(matches!(
        graph
            .apply_mutations("other-org", &[mutation(ids[0], expected.clone())])
            .await,
        Err(BackendError::NotFound(_))
    ));
    // Presence, removal, and value changes all invalidate the complete projection.
    for patch in [
        json!({"summary":"new"}),
        json!({"prop_port":5433}),
        json!({"prop_port":null,"property_type_port":null}),
        json!({"prop_extra":"v","property_type_extra":"s"}),
        json!({"namespace":"dev"}),
    ] {
        graph
            .execute_write(
                "MATCH (n:Entity {org_id:$org,uuid:$uuid}) SET n += $patch",
                &json!({"org":org,"uuid":ids[1],"patch":patch}),
            )
            .await
            .unwrap();
        graph
            .execute_write(
                "MATCH (n:Entity {org_id:$org,uuid:$uuid}) REMOVE n.embedding",
                &json!({"org":org,"uuid":ids[0]}),
            )
            .await
            .unwrap();
        assert!(matches!(
            graph
                .apply_mutations(
                    &org,
                    &[
                        mutation(ids[0], expected.clone()),
                        mutation(ids[1], expected.clone())
                    ]
                )
                .await,
            Err(BackendError::Conflict(_))
        ));
        let rows = graph
            .execute_read(
                "MATCH (n:Entity {org_id:$org,uuid:$uuid}) RETURN n.embedding IS NULL AS absent",
                &json!({"org":org,"uuid":ids[0]}),
            )
            .await
            .unwrap();
        assert_eq!(
            rows[0]["absent"], true,
            "partial grouped write must roll back"
        );
        graph.execute_write("MATCH (n:Entity {org_id:$org,uuid:$uuid}) SET n=$props,n.uuid=$uuid,n.chain_id=$uuid,n.org_id=$org,n.is_latest=false,n.deleted_at='2026-01-01T00:00:00Z'",&json!({"org":org,"uuid":ids[1],"props":baseline})).await.unwrap();
    }
    graph
        .execute_write(
            "MATCH (n:Entity {org_id:$org}) DETACH DELETE n",
            &json!({"org":org}),
        )
        .await
        .unwrap();
    graph.close().await.unwrap();
}
