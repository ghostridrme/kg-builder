//! `GraphExplorerBackend` contract over live Neo4j: catalog counts, the single
//! visible entity, ordered version history, directed neighbors, and scope
//! isolation across organizations and namespaces.
use kg_core::traits::graph_explorer::{
    ExplorerDirection, ExplorerQuery, ExplorerRequest, GraphExplorerBackend,
};
use kg_neo4j_testkit::LiveGraph;
use serde_json::{json, Value};
use uuid::Uuid;

struct Seed {
    live: LiveGraph,
    a: Uuid,
    b: Uuid,
    c: Uuid,
}

impl Seed {
    async fn plant() -> Self {
        let live = LiveGraph::open_named("explorer")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let org = live.org().to_string();
        let entity = |chain: Uuid, namespace: &str, name: &str, version: u32, latest: bool| {
            json!({"uuid":Uuid::new_v5(&chain, &[version as u8]),"chain_id":chain,"org_id":org,"namespace":namespace,
                "name":name,"entity_type":"Service","is_latest":latest,"version":version,
                "valid_from":format!("2026-01-0{version}T00:00:00Z"),
                "valid_to":if latest { Value::Null } else { json!(format!("2026-01-0{}T00:00:00Z", version + 1)) },
                "embedding":[0.5,0.5]})
        };
        let rows = vec![
            entity(a, "prod", "checkout", 1, false),
            entity(a, "prod", "checkout", 2, true),
            entity(b, "prod", "orders", 1, true),
            entity(c, "dev", "sandbox", 1, true),
        ];
        live.backend()
            .execute_write(
                "UNWIND $rows AS props CREATE (n:Entity) SET n = props",
                &json!({"rows":rows}),
            )
            .await
            .unwrap();
        // A foreign organization with the same chain ids must stay invisible.
        let foreign = json!({"uuid":Uuid::new_v4(),"chain_id":a,"org_id":format!("{org}-foreign"),"namespace":"prod",
            "name":"impostor","entity_type":"Service","is_latest":true,"version":1,"valid_from":"2026-01-01T00:00:00Z"});
        live.backend()
            .execute_write(
                "CREATE (n:Entity) SET n = $props",
                &json!({"props":foreign}),
            )
            .await
            .unwrap();
        let edge = |source: Uuid, target: Uuid, name: &str| {
            json!({"s":source,"t":target,"props":{"uuid":Uuid::new_v4(),"org_id":org,"source_chain_id":source,
                "target_chain_id":target,"name":name,"description":"seeded","is_latest":true,
                "valid_from":"2026-01-01T00:00:00Z"}})
        };
        live.backend()
            .execute_write(
                "UNWIND $rows AS row MATCH (s:Entity {chain_id:row.s, org_id:$org, is_latest:true}), (t:Entity {chain_id:row.t, org_id:$org, is_latest:true}) \
                 CREATE (s)-[r:RELATES_TO]->(t) SET r = row.props",
                &json!({"rows":[edge(a, b, "USES"), edge(c, a, "DEPENDS_ON")],"org":org}),
            )
            .await
            .unwrap();
        Self { live, a, b, c }
    }

    fn request(&self, namespace: Option<&str>, query: ExplorerQuery) -> ExplorerRequest {
        ExplorerRequest {
            org_id: self.live.org().to_string(),
            namespace: namespace.map(str::to_string),
            as_of: None,
            limit: 50,
            offset: 0,
            query,
        }
    }

    async fn explore(&self, request: ExplorerRequest) -> Vec<Value> {
        self.live.backend().explore(&request).await.unwrap().items
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn explorer_contract_catalog_entity_versions_and_neighbors_stay_in_scope() {
    let seed = Seed::plant().await;
    let backend = seed.live.backend();

    // Catalog: one row per (namespace, type) with live-version counts, namespace-scoped.
    let catalog = seed
        .explore(seed.request(Some("prod"), ExplorerQuery::Catalog))
        .await;
    assert_eq!(catalog.len(), 1, "{catalog:?}");
    assert_eq!(catalog[0]["entity_type"], "Service");
    assert_eq!(catalog[0]["count"], 2, "two live prod chains");
    let all = seed
        .explore(seed.request(None, ExplorerQuery::Catalog))
        .await;
    assert_eq!(
        all.iter()
            .map(|r| r["count"].as_u64().unwrap())
            .sum::<u64>(),
        3
    );

    // Entity: the current version only, embeddings stripped, foreign orgs invisible.
    let entity = |chain| ExplorerQuery::Entity {
        entity_type: "Service".into(),
        chain_id: chain,
    };
    let items = seed.explore(seed.request(None, entity(seed.a))).await;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["version"], 2);
    assert_eq!(items[0]["name"], "checkout");
    assert_eq!(
        items[0]["embedding"],
        Value::Null,
        "embedding arrays never leave the adapter"
    );
    let mut foreign = seed.request(None, entity(seed.a));
    foreign.org_id = format!("{}-other", seed.live.org());
    assert!(backend.explore(&foreign).await.unwrap().items.is_empty());
    let wrong_namespace = seed
        .explore(seed.request(Some("dev"), entity(seed.a)))
        .await;
    assert!(
        wrong_namespace.is_empty(),
        "a prod entity is not visible under dev"
    );

    // Versions: every version of the chain in version order.
    let history = seed
        .explore(seed.request(
            None,
            ExplorerQuery::Versions {
                entity_type: "Service".into(),
                chain_id: seed.a,
            },
        ))
        .await;
    assert_eq!(
        history
            .iter()
            .map(|v| v["version"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    // Neighbors: direction filters follow the edge, both sides stay in the organization.
    let neighbors = |direction| ExplorerQuery::Neighbors {
        entity_type: "Service".into(),
        chain_id: seed.a,
        direction,
        entity_types: Vec::new(),
    };
    let out = seed
        .explore(seed.request(None, neighbors(ExplorerDirection::Out)))
        .await;
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["via"], "USES");
    assert_eq!(out[0]["entity"]["chain_id"], json!(seed.b));
    let incoming = seed
        .explore(seed.request(None, neighbors(ExplorerDirection::In)))
        .await;
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0]["via"], "DEPENDS_ON");
    assert_eq!(incoming[0]["entity"]["chain_id"], json!(seed.c));
    let both = seed
        .explore(seed.request(None, neighbors(ExplorerDirection::Both)))
        .await;
    assert_eq!(both.len(), 2);
    // Namespace scope applies to the neighbor as well as the anchor.
    let prod_only = seed
        .explore(seed.request(Some("prod"), neighbors(ExplorerDirection::Both)))
        .await;
    assert_eq!(
        prod_only.len(),
        1,
        "the dev neighbor is filtered out: {prod_only:?}"
    );

    // A request the trait rejects never reaches the database.
    let mut invalid = seed.request(None, ExplorerQuery::Catalog);
    invalid.limit = 0;
    assert!(backend.explore(&invalid).await.is_err());

    let foreign_org = format!("{}-foreign", seed.live.org());
    seed.live.cleanup_org(&foreign_org).await.unwrap();
    seed.live.cleanup().await.unwrap();
}
