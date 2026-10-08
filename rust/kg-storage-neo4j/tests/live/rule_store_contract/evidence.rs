//! Live end-to-end learning: seed committed reference edges and
//! unresolved decisions in a disposable Neo4j, collect them as evidence, and run
//! the whole learn() pass (propose -> independent held-out validation ->
//! persist) against the Neo4j rule store.
//!
//! Gated by `NEO4J_TEST_URI`; run via `task test:live:storage`. Uses a fresh org
//! per test so the shared disposable database stays isolated.
use kg_core::runtime::rule_learning::service::{
    learn, LearningBounds, LearningOptions, LearningServices, ReferenceEvidenceSource,
};
use kg_core::runtime::rule_learning::validation::NoAdjudicatedValidation;
use kg_core::traits::rule_store::{RuleStatus, RuleStore};
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::json;
use uuid::Uuid;

async fn backend() -> Neo4jGraphBackend {
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.install_schema().await.unwrap();
    graph
}

/// Seed `positives` CmdbChange -> CmdbGroup reference edges and `negatives`
/// unresolved decisions on the same slot, all tagged with `marker` so this
/// test's data is isolated within the shared database.
async fn seed(
    graph: &Neo4jGraphBackend,
    org: &str,
    marker: &str,
    positives: usize,
    negatives: usize,
) {
    let edges: Vec<_> = (0..positives)
        .map(|_| json!({"s": Uuid::new_v4().to_string(), "t": Uuid::new_v4().to_string()}))
        .collect();
    graph
        .execute_write(
            "UNWIND $rows AS row \
             CREATE (s:Entity {org_id:$org, uuid:row.s, chain_id: row.s, entity_type:'CmdbChange', namespace:'prod', source:$source, is_latest:true, marker:$marker}) \
             CREATE (t:Entity {org_id:$org, uuid:row.t, chain_id: row.t, entity_type:'CmdbGroup', namespace:'prod', source:$source, is_latest:true, marker:$marker}) \
             CREATE (s)-[:RELATES_TO {uuid:randomUUID(),org_id:$org, producer_source:$source, producer_namespace:'prod', reference_owner_chain_id:row.s, reference_owner_namespace:'prod', is_latest:true, \
                 reference_slot:'CmdbChange.owning_group', reference_tokens:['s:' + row.t], target_key_group:['group_id'], \
                 evidence_location:'owning_group', marker:$marker}]->(t)",
            &json!({"rows": edges, "org": org, "source": marker, "marker": marker}),
        )
        .await
        .unwrap();

    // This fixture writes raw relationships, so explicitly build the derived
    // dependency index just as pre-index data maintenance does.
    while graph
        .backfill_reference_dependencies(org, 500)
        .await
        .unwrap()
        != 0
    {}

    let negs: Vec<_> = (0..negatives)
        .map(|i| json!({"s": Uuid::new_v4().to_string(), "tok": format!("s:missing{i}")}))
        .collect();
    graph
        .execute_write(
            "UNWIND $rows AS row \
             CREATE (:Entity {org_id:$org, chain_id:row.s, entity_type:'CmdbChange', namespace:'prod', source:$source, is_latest:true, marker:$marker}) \
             CREATE (:UnresolvedReference {org_id:$org, source_chain_id: row.s, \
                 slot:'CmdbChange.owning_group', token: row.tok, reason:'target-not-found', \
                 recorded_at:'2026-09-21T00:00:00+00:00', marker:$marker})",
            &json!({"rows": negs, "org": org, "source": marker, "marker": marker}),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn collects_evidence_from_committed_edges_and_unresolved_decisions() {
    let graph = backend().await;
    let org = format!("ev-{}", Uuid::new_v4());
    let source = format!("cmdb-{}", Uuid::new_v4());
    seed(&graph, &org, &source, 40, 10).await;
    let other_org = format!("ev-{}", Uuid::new_v4());
    seed(&graph, &other_org, &source, 7, 3).await;

    let batch = graph
        .collect(&org, &source, &LearningBounds::default(), 300)
        .await
        .unwrap();
    // Storage supplies observations, not ground truth. Unresolved values remain
    // unlabeled because absence of a target does not prove a negative example.
    assert!(!batch.examples.is_empty(), "training examples present");
    assert!(batch.labeled.is_empty(), "storage cannot adjudicate labels");
    assert!(batch.schema_fingerprint.starts_with("refshape-"));
    assert_eq!(batch.examples.len(), 40);
    assert!(batch
        .examples
        .iter()
        .chain(batch.labeled.iter().map(|case| &case.example))
        .all(|example| example.source_namespace == "prod"));
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn a_full_learn_pass_refuses_auto_promotion_without_independent_negatives() {
    let graph = backend().await;
    let org = format!("ev-{}", Uuid::new_v4());
    let source = format!("cmdb-{}", Uuid::new_v4());
    // Enough positives that the ~30% holdout comfortably clears the 20-positive
    // gate despite split variance, and enough negatives to clear the 20-negative
    // gate.
    seed(&graph, &org, &source, 120, 25).await;

    let options = LearningOptions {
        auto_promote: true,
        ..Default::default()
    };
    let report = learn(
        LearningServices {
            evidence: &graph,
            validator: &NoAdjudicatedValidation,
            store: &graph,
            model: None,
            repair: None,
        },
        &org,
        &source,
        &LearningBounds::default(),
        &options,
    )
    .await
    .unwrap();
    assert_eq!(report.activated, 0);
    assert_eq!(
        report.uncertain, 1,
        "missing adjudicated negatives fail closed"
    );

    let active = graph.list_active(&org, &source).await.unwrap();
    assert!(active.is_empty());
    let rules = graph.list_all(&org, &source).await.unwrap();
    assert_eq!(rules.len(), 1);
    let rule = &rules[0];
    assert_eq!(rule.status, RuleStatus::Uncertain);
    assert_eq!(rule.mapping.source_entity_type, "CmdbChange");
    assert_eq!(rule.mapping.target_type, "CmdbGroup");
    assert_eq!(rule.owner_slot, "CmdbChange.owning_group");
    let validation = rule.validation.as_ref().unwrap();
    assert!(validation.independent);
    assert_eq!(validation.positives, 0);
    assert_eq!(validation.negatives, 0);

    // A second run reuses the active rule and does no new work.
    let second = learn(
        LearningServices {
            evidence: &graph,
            validator: &NoAdjudicatedValidation,
            store: &graph,
            model: None,
            repair: None,
        },
        &org,
        &source,
        &LearningBounds::default(),
        &options,
    )
    .await
    .unwrap();
    assert_eq!(second.activated, 0);
    assert_eq!(
        second.skipped_existing, 1,
        "the same insufficient evidence does not create a duplicate rule"
    );
}
