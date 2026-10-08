//! Shared fixtures for mutation-planning and embedding-stage tests.
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use kg_core::{
    enums::EntityLifecycle,
    identity::IdentityHash,
    models::{EntityNode, PropertyValue, SnapshotDataType, SnapshotKind, SnapshotNode},
    runtime::{RuntimeContext, RuntimeContextBuilder},
    test_support::{MockEmbedBackend, MockLlmBackend},
    traits::GraphBackend,
};
#[cfg(feature = "live-tests")]
use kg_core::{models::EntityEdge, traits::GraphMutation};
#[cfg(feature = "live-tests")]
use kg_storage_neo4j::Neo4jGraphBackend;
#[cfg(feature = "live-tests")]
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) const ORG: &str = "org";

/// The disposable Neo4j the stage suites use. The graph test below seeds
/// fresh uuids only, so the database is neither emptied nor leased.
#[cfg(feature = "live-tests")]
pub(crate) async fn live_graph() -> Arc<Neo4jGraphBackend> {
    let uri = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .uri;
    let password = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .password;
    let user = kg_neo4j_testkit::env::neo4j()
        .unwrap_or_else(|e| panic!("{e}"))
        .user;
    let graph = Neo4jGraphBackend::new(&uri, &user, &password)
        .await
        .expect("connect to the disposable Neo4j");
    graph.ensure_indexes().await.expect("indexes");
    Arc::new(graph)
}

#[cfg(feature = "live-tests")]
pub(crate) async fn seed_node(graph: &Neo4jGraphBackend, props: Value) {
    let uuid = Uuid::parse_str(props["uuid"].as_str().expect("node UUID")).unwrap();
    let mut properties = props.as_object().expect("node properties").clone();
    properties
        .entry("namespace")
        .or_insert_with(|| json!("prod"));
    properties
        .entry("entity_type")
        .or_insert_with(|| json!("Service"));
    graph
        .apply_mutations(ORG, &[GraphMutation::UpsertEntity { uuid, properties }])
        .await
        .expect("seed node and identity index");
}

#[cfg(feature = "live-tests")]
pub(crate) async fn seed_edge(
    graph: &Neo4jGraphBackend,
    edge: &EntityEdge,
    props: &serde_json::Map<String, Value>,
) {
    graph
        .execute_write(
            "MATCH (s:GraphNode {uuid:$source}), (t:GraphNode {uuid:$target})
                 MERGE (s)-[r:RELATES_TO {uuid:$uuid}]->(t) SET r = $props, r.uuid = $uuid",
            &json!({
                "uuid": edge.uuid.to_string(),
                "source": edge.source_chain_id.to_string(),
                "target": edge.target_chain_id.to_string(),
                "props": props,
            }),
        )
        .await
        .expect("seed relationship");
}

pub(crate) fn ctx(graph: Arc<dyn GraphBackend>) -> RuntimeContext {
    let llm = Arc::new(MockLlmBackend::empty());
    RuntimeContextBuilder::new(ORG)
        .graph(graph)
        .llm_extraction(llm.clone())
        .llm_default(llm)
        .embedder(Arc::new(MockEmbedBackend::default_dimension()))
        .build()
        .unwrap()
}

pub(crate) fn snapshot(captured_at: DateTime<Utc>) -> SnapshotNode {
    SnapshotNode {
        uuid: Uuid::new_v4(),
        org_id: ORG.into(),
        namespace: "prod".into(),
        name: "scan".into(),
        source_description: None,
        data_type: SnapshotDataType::Entities,
        snapshot_kind: SnapshotKind::Full,
        sync_generation: Some(1),
        complete: true,
        collection: None,
        source: "aws".into(),
        content: None,
        captured_at,
        entities: vec![],
        entity_edges: vec![],
        labels: vec![],
        tags: IndexMap::new(),
        created_at: captured_at,
    }
}

pub(crate) fn entity(name: &str, replicas: i64, snapshot: &SnapshotNode) -> EntityNode {
    let mut all_properties = IndexMap::new();
    all_properties.insert("replicas".to_string(), PropertyValue::Integer(replicas));
    EntityNode {
        labels: Vec::new(),
        inherited_labels: Vec::new(),
        uuid: Uuid::new_v4(),
        chain_id: Uuid::new_v4(),
        org_id: ORG.into(),
        namespace: "prod".into(),
        entity_type: "Service".into(),
        name: name.into(),
        all_properties,
        primary_key_properties: vec!["name".into()],
        additional_key_properties: vec![],
        identity_hash: IdentityHash::compute(ORG, "prod", "Service", &[("name", name)]),
        lifecycle: EntityLifecycle::Active,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        embedding: None,
        valid_from: snapshot.captured_at,
        valid_to: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        source: "aws".into(),
        extracted_by: "direct".into(),
        resolved_by: None,
        first_seen_snapshot_id: Some(snapshot.uuid),
        last_seen_snapshot_id: Some(snapshot.uuid),
        last_seen_at: None,
        sync_generation: Some(1),
        tags: IndexMap::new(),
        summary: None,
        structural_hash: 1000 + replicas as u64,
        needs_llm_review: false,
        collections: Vec::new(),
    }
}
