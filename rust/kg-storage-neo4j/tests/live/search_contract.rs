//! Live search contract. Uses an isolated organization and removes its fixtures.
use kg_core::{
    search::*,
    traits::{graph_backend::GraphEmbedding, GraphBackend, SearchBackend},
};
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::json;
use uuid::Uuid;

/// The content hash the driver guards against: rendered from the stored row, so the
/// test follows the embedding text representation instead of pinning it.
async fn stored_text_hash(g: &Neo4jGraphBackend, org: &str, uuid: Uuid) -> String {
    use kg_core::embedding::{content_hash, EntityEmbeddingFields};
    use kg_core::embedding_rebuild::EmbeddingKind;
    let record = g
        .embedding_records(org, EmbeddingKind::Entity, None, 100)
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.uuid == uuid)
        .expect("stored entity row");
    content_hash(
        &record
            .text(EmbeddingKind::Entity, &EntityEmbeddingFields::default())
            .unwrap(),
    )
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn scoped_temporal_search_and_evidence() {
    let g = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    g.ensure_indexes().await.unwrap();
    g.execute_read("CALL db.awaitIndexes(60)", &json!({}))
        .await
        .unwrap();
    let org = format!("search-contract-{}", Uuid::new_v4());
    let (a, b, old, snapshot, edge) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    for (uuid, chain, name, ns, kind, latest, from, to) in [
        (
            old,
            a,
            "historic checkout",
            "prod",
            "Service",
            false,
            "2026-01-01T00:00:00Z",
            Some("2026-02-01T00:00:00Z"),
        ),
        (
            a,
            a,
            "current checkout",
            "prod",
            "Service",
            true,
            "2026-02-01T00:00:00Z",
            None,
        ),
        (
            b,
            b,
            "orders",
            "prod",
            "Database",
            true,
            "2026-01-01T00:00:00Z",
            None,
        ),
    ] {
        g.execute_write("CREATE (n:Entity) SET n=$props",&json!({"props":{"uuid":uuid,"chain_id":chain,"org_id":org,"namespace":ns,"entity_type":kind,"name":name,"is_latest":latest,"valid_from":from,"valid_to":to,"prop_owner":"payments","property_type_owner":"s","embedding_text_version":kg_core::embedding::TEXT_VERSION}})).await.unwrap();
    }
    g.set_entity_embedding(
        &org,
        a,
        &GraphEmbedding {
            model: "test".into(),
            values: vec![1., 0.],
        },
        kg_core::embedding::TEXT_VERSION,
        &stored_text_hash(&g, &org, a).await,
        &kg_core::embedding::EntityEmbeddingFields::default(),
    )
    .await
    .unwrap();
    g.set_entity_embedding(
        &org,
        b,
        &GraphEmbedding {
            model: "test".into(),
            values: vec![0.5, 0.5],
        },
        kg_core::embedding::TEXT_VERSION,
        &stored_text_hash(&g, &org, b).await,
        &kg_core::embedding::EntityEmbeddingFields::default(),
    )
    .await
    .unwrap();
    g.execute_write("MATCH (s:Entity {uuid:$a}),(t:Entity {uuid:$b}) CREATE (s)-[r:RELATES_TO]->(t) SET r=$props",&json!({"a":a,"b":b,"props":{"uuid":edge,"org_id":org,"source_chain_id":a,"target_chain_id":b,"name":"READS_FROM","description":"checkout reads orders","is_latest":true,"valid_from":"2026-01-01T00:00:00Z","first_seen_snapshot_id":snapshot}})).await.unwrap();
    g.execute_write("MATCH (n:Entity {uuid:$old}) CREATE (s:Snapshot {uuid:$snapshot,org_id:$org,namespace:'prod',name:'runbook',source:'docs',content:'checkout reads orders',captured_at:'2026-01-05T00:00:00Z'}) CREATE (s)-[:MENTIONS {org_id:$org,observed_at:'2026-01-05T00:00:00Z'}]->(n)",&json!({"old":old,"snapshot":snapshot,"org":org})).await.unwrap();
    let filter = SearchFilter {
        org_id: org.clone(),
        namespaces: vec!["prod".into()],
        ..Default::default()
    };
    let mut request = NodeSearch {
        embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
        filter: filter.clone(),
        query: NodeQuery::Fulltext("checkout".into()),
        chain_ids: None,
        limit: 10,
        min_score: 0.0,
        projection: NodeProjection::Full,
        signals: NodeSignals {
            observations: true,
            dependents: true,
        },
    };
    let page = g.search_nodes(&request).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].uuid, a);
    let serialized = serde_json::to_value(&page.items[0]).unwrap();
    assert!(serialized["properties"].get("n").is_none());
    assert!(serialized["properties"].get("embedding").is_none());
    assert_eq!(page.items[0].observation_count, Some(1));
    assert_eq!(page.items[0].owner.as_deref(), Some("payments"));
    request.projection = NodeProjection::Candidate;
    let candidate = g.search_nodes(&request).await.unwrap().items.remove(0);
    assert_eq!(candidate.uuid, a);
    assert_eq!(candidate.name, page.items[0].name);
    assert!(candidate.embedding.is_none());
    assert!(candidate.properties.get("prop_owner").is_none());
    request.projection = NodeProjection::Full;
    request.filter.as_of = Some("2026-01-15T00:00:00Z".parse().unwrap());
    let page = g.search_nodes(&request).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].uuid, old);
    request.filter.as_of = Some("2026-02-01T00:00:00Z".parse().unwrap());
    assert_eq!(g.search_nodes(&request).await.unwrap().items[0].uuid, a);
    request.filter.as_of = None;
    request.filter.entity_types = vec!["Database".into()];
    request.query = NodeQuery::Similarity(GraphEmbedding {
        model: "test".into(),
        values: vec![1., 0.],
    });
    request.limit = 1;
    let page = g.search_nodes(&request).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].uuid, b);
    assert_eq!(page.items[0].dependent_count, Some(1));
    assert!((page.items[0].score - std::f32::consts::FRAC_1_SQRT_2).abs() < 0.0001);
    request.filter.org_id = "foreign".into();
    assert!(g.search_nodes(&request).await.unwrap().items.is_empty());
    g.apply_mutations(
        &org,
        &[kg_core::traits::GraphMutation::SetRelationshipEmbedding {
            uuid: edge,
            embedding: GraphEmbedding {
                model: "test".into(),
                values: vec![1.0, 0.0],
            },
            text_version: kg_core::embedding::RELATIONSHIP_TEXT_VERSION.into(),
            content_hash: "fact".into(),
        }],
    )
    .await
    .unwrap();
    let semantic_fact = RelationshipSimilarity {
        filter: filter.clone(),
        embedding: GraphEmbedding {
            model: "test".into(),
            values: vec![1.0, 0.0],
        },
        limit: 1,
        min_score: 0.5,
        anchor_chains: None,
    };
    let facts = g
        .search_relationship_similarity(&semantic_fact)
        .await
        .unwrap();
    assert_eq!(facts.items[0].uuid, edge);
    assert_eq!(facts.items[0].snapshot_id, Some(snapshot));
    let readiness = EmbeddingReadinessRequest {
        entity_text_version: kg_core::embedding::TEXT_VERSION.into(),
        filter: filter.clone(),
        scope: SearchScope::Relationships,
        model: "test".into(),
        dimensions: 2,
    };
    assert_eq!(
        g.embedding_readiness(&readiness).await.unwrap().compatible,
        1
    );
    g.execute_write(
        "MATCH ()-[r:RELATES_TO {uuid:$uuid}]->() SET r.embedding_text_version='obsolete'",
        &json!({"uuid":edge}),
    )
    .await
    .unwrap();
    assert!(g
        .search_relationship_similarity(&semantic_fact)
        .await
        .unwrap()
        .items
        .is_empty());
    assert_eq!(
        g.embedding_readiness(&readiness)
            .await
            .unwrap()
            .incompatible,
        1
    );
    let mut evidence = EvidenceSearch {
        passage_query: None,
        filter,
        query: None,
        chain_ids: Some(vec![a]),
        limit: 10,
    };
    evidence.filter.as_of = Some("2026-01-15T00:00:00Z".parse().unwrap());
    let edges = g.search_relationships(&evidence).await.unwrap();
    assert_eq!(edges.items.len(), 1);
    assert_eq!(edges.items[0].source_chain_id, a);
    assert_eq!(edges.items[0].snapshot_id, Some(snapshot));
    let snapshots = g.search_snapshots(&evidence).await.unwrap();
    assert_eq!(snapshots.items.len(), 1);
    assert_eq!(snapshots.items[0].source, "docs");
    let content = format!("{}İ 🌍 Checkout retry", "前".repeat(5000));
    g.execute_write(
        "MATCH (s:Snapshot {uuid:$uuid}) SET s.content=$content",
        &json!({"uuid": snapshot, "content": content}),
    )
    .await
    .unwrap();
    evidence.passage_query = Some("checkout".into());
    for query in [None, Some("checkout".to_owned())] {
        evidence.query = query;
        let page = g.search_snapshots(&evidence).await.unwrap();
        let hit = &page.items[0];
        assert_eq!(hit.uuid, snapshot);
        assert_eq!(hit.selection_kind, ExcerptSelection::Matched);
        assert!(hit.content_start > 4096);
        assert_eq!(
            hit.content,
            content
                .chars()
                .skip(hit.content_start)
                .take(hit.content_end - hit.content_start)
                .collect::<String>()
        );
        assert!(hit.content_truncated);
        assert!(!hit.selection_limited);
    }
    evidence.query = None;
    g.execute_write("MATCH ()-[r:RELATES_TO {uuid:$uuid}]->() SET r.valid_to='2026-04-01T00:00:00Z',r.invalid_at='2026-03-01T00:00:00Z'", &json!({"uuid":edge})).await.unwrap();
    assert_eq!(
        g.search_relationships(&evidence).await.unwrap().items[0]
            .valid_to
            .as_deref(),
        Some("2026-03-01T00:00:00Z")
    );
    evidence.filter.relationship_types = vec!["WRITES_TO".into()];
    assert!(g
        .search_relationships(&evidence)
        .await
        .unwrap()
        .items
        .is_empty());
    g.execute_write(
        "MATCH (n {org_id:$org}) DETACH DELETE n",
        &json!({"org":org}),
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn indexed_evidence_filters_before_limit_and_preserves_literal_passages() {
    let g = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    g.ensure_indexes().await.unwrap();
    let org = format!("indexed-evidence-{}", Uuid::new_v4());
    let foreign = format!("{org}-foreign");
    // Short foreign and expired documents score above the longer eligible documents.
    for (scope, ns, latest, count) in [
        (foreign.as_str(), "prod", true, 30),
        (org.as_str(), "dev", true, 30),
        (org.as_str(), "prod", false, 30),
        (org.as_str(), "prod", true, 2),
    ] {
        g.execute_write(
            r#"UNWIND range(1,$count) AS i
            CREATE (s:Entity {uuid:randomUUID(),chain_id:randomUUID(),org_id:$org,
                namespace:$ns,name:'weak endpoint',entity_type:'Service',is_latest:$latest,
                valid_from:'2026-01-01T00:00:00Z'})
            CREATE (t:Entity {uuid:randomUUID(),chain_id:randomUUID(),org_id:$org,
                namespace:$ns,name:'other endpoint',entity_type:'Service',is_latest:$latest,
                valid_from:'2026-01-01T00:00:00Z'})
            CREATE (s)-[r:RELATES_TO {uuid:randomUUID(),org_id:$org,name:'CALLS',
                description:$text,source_chain_id:s.chain_id,target_chain_id:t.chain_id,
                is_latest:$latest,valid_from:'2026-01-01T00:00:00Z'}]->(t)
            CREATE (snap:Snapshot {uuid:randomUUID(),org_id:$org,namespace:$ns,
                name:'source',source:'docs',content:$text,captured_at:'2026-01-01T00:00:00Z'})
            CREATE (snap)-[:MENTIONS {org_id:$org,observed_at:'2026-01-01T00:00:00Z'}]->(s)"#,
            &json!({
                "count": count, "org": scope, "ns": ns, "latest": latest,
                "text": if count == 2 {
                    "searchcontractneedle relevant source with additional context src/main.rs arn:aws:lambda foo-bar"
                } else { "searchcontractneedle" }
            }),
        ).await.unwrap();
    }
    let mut request = EvidenceSearch {
        filter: SearchFilter {
            org_id: org.clone(),
            namespaces: vec!["prod".into()],
            entity_types: vec!["Service".into()],
            ..Default::default()
        },
        query: Some("searchcontractneedle".into()),
        passage_query: None,
        chain_ids: None,
        limit: 1,
    };
    let facts = g.search_relationships(&request).await.unwrap();
    let sources = g.search_snapshots(&request).await.unwrap();
    assert_eq!(facts.items.len(), 1);
    assert_eq!(sources.items.len(), 1);
    assert!(facts.truncated && sources.truncated);
    assert!(facts.items[0].description.contains("additional context"));
    assert!(sources.items[0].content.contains("additional context"));

    for literal in ["src/main.rs", "arn:aws:lambda"] {
        request.query = Some(literal.into());
        assert_eq!(
            g.search_relationships(&request).await.unwrap().items.len(),
            1
        );
        assert_eq!(g.search_snapshots(&request).await.unwrap().items.len(), 1);
    }
    request.query = Some("foo/bar".into());
    let page = g.search_snapshots(&request).await.unwrap();
    assert_eq!(page.items.len(), 1);
    // The analyzer finds the same tokens, but the original source contains foo-bar.
    assert_eq!(page.items[0].selection_kind, ExcerptSelection::Fallback);
    request.query = Some("unfindablecontracttoken".into());
    assert!(g.search_snapshots(&request).await.unwrap().items.is_empty());
    assert!(g
        .search_relationships(&request)
        .await
        .unwrap()
        .items
        .is_empty());
    request.query = Some("*".into());
    assert!(g.search_snapshots(&request).await.unwrap().items.is_empty());
    request.query = Some("searchcontractneedle".into());
    request.filter.as_of = Some("2025-12-31T00:00:00Z".parse().unwrap());
    assert!(g.search_snapshots(&request).await.unwrap().items.is_empty());
    assert!(g
        .search_relationships(&request)
        .await
        .unwrap()
        .items
        .is_empty());
    g.execute_write(
        "MATCH (n) WHERE n.org_id IN $orgs DETACH DELETE n",
        &json!({"orgs": [org, foreign]}),
    )
    .await
    .unwrap();
}
