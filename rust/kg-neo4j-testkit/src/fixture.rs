//! A live-Neo4j fixture for storage and search suites: a connected adapter, one
//! organization per test, and org-scoped seed/cleanup helpers.
use kg_core::search::SearchFilter;
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub struct Fixture {
    pub graph: Arc<Neo4jGraphBackend>,
    pub org: String,
    pub seed: u128,
}
impl Fixture {
    pub async fn new() -> Self {
        let graph = Arc::new(crate::connect().await.unwrap_or_else(|e| panic!("{e}")));
        graph.ensure_indexes().await.unwrap();
        let seed = Uuid::new_v4();
        Self {
            graph,
            org: format!("search-storage-{seed}"),
            seed: seed.as_u128(),
        }
    }
    pub fn id(&self, n: u128) -> Uuid {
        Uuid::from_u128(self.seed ^ n)
    }
    pub async fn entity(&self, n: u128, patch: Value) {
        let mut props = json!({"uuid":self.id(n),"chain_id":self.id(n),"org_id":self.org,"namespace":"prod","name":"checkout","entity_type":"Service","is_latest":true,"valid_from":"2026-01-01T00:00:00Z","test_fixture":self.org});
        props
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        self.graph
            .execute_write("CREATE (n:Entity) SET n=$props", &json!({"props":props}))
            .await
            .unwrap();
    }
    pub async fn fact(&self, n: u128, s: u128, t: u128, patch: Value) {
        let mut props = json!({"uuid":self.id(n),"org_id":self.org,"source_chain_id":self.id(s),"target_chain_id":self.id(t),"name":"USES","description":"checkout uses orders","is_latest":true,"valid_from":"2026-01-01T00:00:00Z"});
        props
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        self.graph.execute_write("MATCH (s:Entity {uuid:$s}),(t:Entity {uuid:$t}) CREATE (s)-[r:RELATES_TO]->(t) SET r=$props",&json!({"s":self.id(s),"t":self.id(t),"props":props})).await.unwrap();
    }
    pub async fn snapshot(&self, n: u128, org: &str, at: &str, content: &str) {
        self.graph.execute_write("MATCH (n:Entity {uuid:$entity}) CREATE (s:Snapshot) SET s=$props CREATE (s)-[:MENTIONS {org_id:$org,observed_at:$at}]->(n)",&json!({"entity":self.id(1),"org":org,"at":at,"props":{"uuid":self.id(n),"org_id":org,"namespace":"prod","name":"runbook","source":"docs","captured_at":at,"content":content,"test_fixture":self.org}})).await.unwrap();
    }
    pub async fn cleanup(&self) {
        self.graph
            .execute_write(
                "MATCH (n {test_fixture:$org}) DETACH DELETE n",
                &json!({"org":self.org}),
            )
            .await
            .unwrap();
    }
    pub fn filter(&self) -> SearchFilter {
        SearchFilter {
            org_id: self.org.clone(),
            namespaces: vec!["prod".into()],
            ..Default::default()
        }
    }
}
