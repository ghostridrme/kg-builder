//! Effective relationship intervals must not inherit entity head visibility.
use std::collections::BTreeSet;

use kg_core::{
    search::*,
    traits::{graph_backend::GraphEmbedding, GraphBackend, GraphMutation, SearchBackend},
};
use kg_storage_cypher as cypher;
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::{json, Map, Value};
use uuid::Uuid;

const START: &str = "2000-01-01T00:00:00Z";
const SWITCH: &str = "2100-01-01T00:00:00Z";
const END: &str = "2101-01-01T00:00:00Z";
const MODEL: &str = "schedule-test";

struct Fixture {
    org: String,
    target: Uuid,
    predecessor: Uuid,
    successor: Uuid,
    finite: Uuid,
}

async fn read_at(
    graph: &Neo4jGraphBackend,
    mut query: cypher::PreparedQuery,
    now: &str,
) -> Vec<Map<String, Value>> {
    // Override only the relationship clock: as_of=None must retain current entity semantics.
    query.parameters["relationship_now"] = json!(now);
    graph
        .execute_read(&query.statement, &query.parameters)
        .await
        .unwrap()
}

async fn assert_paths(
    graph: &Neo4jGraphBackend,
    fixture: &Fixture,
    filter: SearchFilter,
    now: &str,
    expected: &[Uuid],
    check_dependents: bool,
) {
    let evidence = EvidenceSearch {
        filter: filter.clone(),
        query: Some("scheduled checkout dependency".into()),
        passage_query: None,
        chain_ids: None,
        limit: 10,
    };
    let similarity = RelationshipSimilarity {
        filter: filter.clone(),
        embedding: GraphEmbedding {
            model: MODEL.into(),
            values: vec![1.0, 0.0],
        },
        limit: 10,
        min_score: 0.5,
        anchor_chains: None,
    };
    let attached = AttachedEvidence {
        filter: filter.clone(),
        anchors: vec![fixture.target],
        per_anchor: 10,
        passage_query: None,
    };
    let expected_ids: BTreeSet<_> = expected.iter().map(Uuid::to_string).collect();
    for (label, query) in [
        ("fulltext", cypher::relationships(&evidence).unwrap()),
        (
            "exact",
            cypher::relationship_similarity(&similarity).unwrap(),
        ),
        (
            "indexed",
            cypher::indexed_relationships(&similarity, 100).unwrap(),
        ),
        (
            "attached",
            cypher::attached_relationships(&attached).unwrap(),
        ),
    ] {
        let rows = read_at(graph, query, now).await;
        let ids: BTreeSet<_> = rows
            .iter()
            .filter_map(|row| row.get("uuid").and_then(Value::as_str).map(str::to_owned))
            .collect();
        assert_eq!(
            ids, expected_ids,
            "{label}, now={now}, as_of={:?}",
            filter.as_of
        );
    }
    let rows = read_at(
        graph,
        cypher::vector_population(
            &filter,
            MODEL,
            true,
            kg_core::embedding::TEXT_VERSION,
            &None,
        ),
        now,
    )
    .await;
    assert_eq!(rows[0]["count"], expected.len(), "population");
    let rows = read_at(
        graph,
        cypher::embedding_readiness(&EmbeddingReadinessRequest {
            entity_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: filter.clone(),
            scope: SearchScope::Relationships,
            model: MODEL.into(),
            dimensions: 2,
        })
        .unwrap(),
        now,
    )
    .await;
    assert_eq!(rows[0]["eligible"], expected.len(), "eligible");
    assert_eq!(rows[0]["compatible"], expected.len(), "compatible");
    if check_dependents {
        let rows = read_at(
            graph,
            cypher::nodes(&NodeSearch {
                embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
                filter,
                query: NodeQuery::ByChain,
                chain_ids: Some(vec![fixture.target]),
                limit: 10,
                min_score: 0.0,
                projection: NodeProjection::Full,
                signals: NodeSignals {
                    observations: false,
                    dependents: true,
                },
            })
            .unwrap(),
            now,
        )
        .await;
        assert_eq!(rows.len(), 1, "unchanged entity visibility");
        assert_eq!(rows[0]["dependents"], expected.len(), "dependent sources");
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn scheduled_relationship_reads_use_effective_intervals_without_changing_entity_heads() {
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.unwrap();
    let fixture = Fixture {
        org: format!("relationship-schedule-{}", Uuid::new_v4()),
        target: Uuid::new_v4(),
        predecessor: Uuid::new_v4(),
        successor: Uuid::new_v4(),
        finite: Uuid::new_v4(),
    };
    let source = Uuid::new_v4();
    let second_source = Uuid::new_v4();
    let lineage = Uuid::new_v4();
    for uuid in [source, second_source, fixture.target] {
        graph.execute_write("CREATE (n:Entity) SET n=$props", &json!({"props": {
            "uuid": uuid, "chain_id": uuid, "org_id": fixture.org, "namespace": "prod",
            "entity_type": "Service", "name": "checkout", "valid_from": "1990-01-01T00:00:00Z", "is_latest": true
        }})).await.unwrap();
    }
    for (uuid, chain, from, to, latest, version, source) in [
        (
            fixture.predecessor,
            lineage,
            START,
            Some(SWITCH),
            false,
            1,
            source,
        ),
        (fixture.successor, lineage, SWITCH, None, true, 2, source),
        (
            fixture.finite,
            Uuid::new_v4(),
            START,
            Some(END),
            false,
            1,
            second_source,
        ),
    ] {
        graph.execute_write("MATCH (s:Entity {uuid:$source}),(t:Entity {uuid:$target}) CREATE (s)-[r:RELATES_TO]->(t) SET r=$props", &json!({
            "source": source, "target": fixture.target, "props": {
                "uuid": uuid, "chain_id": chain, "org_id": fixture.org, "source_chain_id": source,
                "target_chain_id": fixture.target, "name": "DEPENDS_ON", "description": "scheduled checkout dependency",
                "valid_from": from, "valid_to": to, "is_latest": latest, "version": version
            }
        })).await.unwrap();
        graph
            .apply_mutations(
                &fixture.org,
                &[GraphMutation::SetRelationshipEmbedding {
                    uuid,
                    embedding: GraphEmbedding {
                        model: MODEL.into(),
                        values: vec![1.0, 0.0],
                    },
                    text_version: kg_core::embedding::RELATIONSHIP_TEXT_VERSION.into(),
                    content_hash: kg_core::embedding::content_hash("scheduled checkout dependency"),
                }],
            )
            .await
            .unwrap();
    }
    graph
        .execute_read("CALL db.awaitIndexes(60)", &json!({}))
        .await
        .unwrap();
    let filter = SearchFilter {
        org_id: fixture.org.clone(),
        namespaces: vec!["prod".into()],
        entity_types: vec!["Service".into()],
        ..Default::default()
    };
    for (now, expected) in [
        ("1999-12-31T23:59:59Z", vec![]),
        (START, vec![fixture.predecessor, fixture.finite]),
        (
            "2099-12-31T23:59:59Z",
            vec![fixture.predecessor, fixture.finite],
        ),
        (SWITCH, vec![fixture.successor, fixture.finite]),
        (END, vec![fixture.successor]),
    ] {
        assert_paths(&graph, &fixture, filter.clone(), now, &expected, true).await;
        // An explicit time must override the independently bound current clock.
        let at = SearchFilter {
            as_of: Some(now.parse().unwrap()),
            ..filter.clone()
        };
        assert_paths(
            &graph,
            &fixture,
            at,
            "2200-01-01T00:00:00Z",
            &expected,
            true,
        )
        .await;
    }
    // Exercise normal adapter-generated current parameters as well as controlled boundaries.
    let current = graph
        .search_relationships(&EvidenceSearch {
            filter: filter.clone(),
            query: Some("scheduled checkout dependency".into()),
            passage_query: None,
            chain_ids: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        current
            .items
            .iter()
            .map(|item| item.uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([fixture.predecessor, fixture.finite])
    );
    for other in [
        SearchFilter {
            org_id: format!("foreign-schedule-org-{}", Uuid::new_v4()),
            ..filter.clone()
        },
        SearchFilter {
            namespaces: vec!["dev".into()],
            ..filter.clone()
        },
        SearchFilter {
            entity_types: vec!["Database".into()],
            ..filter.clone()
        },
        SearchFilter {
            relationship_types: vec!["OWNS".into()],
            ..filter.clone()
        },
    ] {
        assert_paths(&graph, &fixture, other, START, &[], false).await;
    }
    // Every end boundary remains exclusive, even if another end lies later.
    for field in ["invalid_at", "deleted_at"] {
        graph
            .execute_write(
                "MATCH ()-[r:RELATES_TO {uuid:$uuid}]->() SET r += $props",
                &json!({"uuid": fixture.finite, "props": {field: SWITCH}}),
            )
            .await
            .unwrap();
        assert_paths(
            &graph,
            &fixture,
            filter.clone(),
            SWITCH,
            &[fixture.successor],
            true,
        )
        .await;
        graph
            .execute_write(
                "MATCH ()-[r:RELATES_TO {uuid:$uuid}]->() SET r += $props",
                &json!({"uuid": fixture.finite, "props": {field: null}}),
            )
            .await
            .unwrap();
    }
    graph.execute_write(
        "MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->() SET r.valid_to=$end,r.invalid_at=$end,r.is_latest=false,r.last_seen_at=$seen",
        &json!({"org":fixture.org,"uuid":fixture.successor,"end":END,"seen":"2098-01-01T00:00:00Z"}),
    ).await.unwrap();
    assert!(graph
        .apply_mutations(
            &fixture.org,
            &[GraphMutation::CancelEdge {
                uuid: fixture.successor,
                cancelled_at: "2097-01-01T00:00:00Z".parse().unwrap(),
                cancellation_snapshot_id: Some(Uuid::new_v4()),
                cancellation_context: None,
                observed_at: "2097-01-01T00:00:00Z".parse().unwrap(),
            }]
        )
        .await
        .is_err());
    let cancellation = GraphMutation::CancelEdge {
        uuid: fixture.successor,
        cancelled_at: "2099-01-01T00:00:00Z".parse().unwrap(),
        cancellation_snapshot_id: Some(Uuid::new_v4()),
        cancellation_context: None,
        observed_at: "2099-01-01T00:00:00Z".parse().unwrap(),
    };
    assert!(graph
        .apply_mutations("another-org", std::slice::from_ref(&cancellation))
        .await
        .is_err());
    for invalid in [
        GraphMutation::CancelEdge {
            uuid: Uuid::new_v4(),
            cancelled_at: START.parse().unwrap(),
            cancellation_snapshot_id: Some(Uuid::new_v4()),
            cancellation_context: None,
            observed_at: START.parse().unwrap(),
        },
        GraphMutation::CancelEdge {
            uuid: fixture.predecessor,
            cancelled_at: SWITCH.parse().unwrap(),
            cancellation_snapshot_id: Some(Uuid::new_v4()),
            cancellation_context: None,
            observed_at: SWITCH.parse().unwrap(),
        },
    ] {
        assert!(graph
            .apply_mutations(&fixture.org, &[invalid])
            .await
            .is_err());
    }
    graph
        .apply_mutations(&fixture.org, std::slice::from_ref(&cancellation))
        .await
        .unwrap();
    assert!(graph
        .apply_mutations(&fixture.org, &[cancellation])
        .await
        .is_err());
    for (at, expected) in [
        (START, vec![fixture.predecessor, fixture.finite]),
        (SWITCH, vec![fixture.finite]),
        (END, vec![]),
    ] {
        assert_paths(&graph, &fixture, filter.clone(), at, &expected, true).await;
        assert_paths(
            &graph,
            &fixture,
            SearchFilter {
                as_of: Some(at.parse().unwrap()),
                ..filter.clone()
            },
            END,
            &expected,
            true,
        )
        .await;
    }
    let history = graph
        .execute_read(
            "MATCH ()-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->() RETURN properties(r) AS stored",
            &json!({"org":fixture.org,"uuid":fixture.successor}),
        )
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    let stored = &history[0]["stored"];
    assert_eq!(stored["valid_from"], SWITCH);
    assert_eq!(stored["valid_to"], END);
    assert_eq!(stored["invalid_at"], END);
    assert_eq!(stored["is_latest"], false);
    assert_eq!(stored["cancelled_at"], "2099-01-01T00:00:00+00:00");
    assert_eq!(stored["last_transition_at"], "2099-01-01T00:00:00+00:00");
    graph
        .execute_write(
            "MATCH (n {org_id:$org}) DETACH DELETE n",
            &json!({"org": fixture.org}),
        )
        .await
        .unwrap();
}
