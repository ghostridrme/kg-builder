//! Live contract for the versioned learned-rule store:
//! optimistic-revision writes, the state machine, and idempotent schema.
//!
//! Gated by `NEO4J_TEST_URI`; run via `task test:live:storage`. Each test uses a
//! fresh org id so the shared disposable database stays isolated.
use chrono::Utc;
use kg_core::errors::BackendError;
use kg_core::runtime::extraction::ReferenceMapping;
use kg_core::traits::rule_store::{
    LearnedRule, RuleDecision, RuleOrigin, RuleStatus, RuleStore, RuleTransition, RuleValidation,
    MIN_PROMOTION_NEGATIVES, MIN_PROMOTION_POSITIVES, MIN_PROMOTION_PRECISION,
    MIN_PROMOTION_RECALL,
};
use kg_storage_neo4j::Neo4jGraphBackend;
use uuid::Uuid;

async fn store() -> Neo4jGraphBackend {
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    // Idempotent: installing twice must not error.
    graph.install_schema().await.unwrap();
    graph.install_schema().await.unwrap();
    graph
}

fn mapping() -> ReferenceMapping {
    ReferenceMapping {
        source_namespace: None,
        source_entity_type: "CmdbChange".into(),
        reference_path: "owning_group".into(),
        context_paths: Default::default(),
        target_type: "CmdbGroup".into(),
        target_key_group: vec!["group_id".into()],
        shape: Default::default(),
        direction: Default::default(),
        relationship_name: "REFERENCES_CMDBGROUP".into(),
        qualifiers: None,
        cardinality: Default::default(),
        case_insensitive_types: Vec::new(),
    }
}

fn proposed(org: &str, id: Uuid) -> LearnedRule {
    LearnedRule {
        id,
        revision: 1,
        org_id: org.into(),
        source: "cmdb".into(),
        namespace: Some("prod".into()),
        schema_fingerprint: "fp-1".into(),
        mapping: mapping(),
        owner_slot: "CmdbChange.owning_group".into(),
        origin: RuleOrigin::Model {
            model: "gpt-5.4-mini".into(),
            prompt_version: "v1".into(),
        },
        evidence_refs: vec!["ev-1".into(), "ev-2".into()],
        validation: None,
        decisions: vec![],
        status: RuleStatus::Proposed,
        effective_from: None,
        revoked_at: None,
    }
}

fn gate() -> RuleValidation {
    RuleValidation {
        positives: MIN_PROMOTION_POSITIVES,
        negatives: MIN_PROMOTION_NEGATIVES,
        precision: MIN_PROMOTION_PRECISION,
        recall: MIN_PROMOTION_RECALL,
        conflicting_failures: 0,
        independent: true,
    }
}

fn decision(actor: &str) -> RuleDecision {
    RuleDecision {
        origin: RuleOrigin::Human {
            actor: actor.into(),
        },
        at: Utc::now(),
        note: Some(format!("{actor} decision")),
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn propose_then_get_round_trips_and_duplicate_id_conflicts() {
    let store = store().await;
    let org = format!("rules-{}", Uuid::new_v4());
    let id = Uuid::new_v4();

    assert!(store.get(&org, id).await.unwrap().is_none());

    let rule = proposed(&org, id);
    let stored = store.propose(rule.clone()).await.unwrap();
    assert_eq!(stored, rule);

    let fetched = store.get(&org, id).await.unwrap().unwrap();
    assert_eq!(fetched, rule);

    // A second proposal at the same id is a conflict, never an overwrite.
    match store.propose(proposed(&org, id)).await {
        Err(BackendError::Conflict(_)) => {}
        other => panic!("expected conflict, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn activation_appears_in_list_active_and_freezes_the_gate() {
    let store = store().await;
    let org = format!("rules-{}", Uuid::new_v4());
    let id = Uuid::new_v4();
    store.propose(proposed(&org, id)).await.unwrap();

    assert!(store.list_active(&org, "cmdb").await.unwrap().is_empty());

    // Activation without a passing gate is rejected.
    let no_gate = RuleTransition {
        to: RuleStatus::Active,
        decision: decision("sre"),
        validation: None,
    };
    assert!(matches!(
        store.transition(&org, id, 1, no_gate).await,
        Err(BackendError::Query(_))
    ));

    let activate = RuleTransition {
        to: RuleStatus::Active,
        decision: decision("sre"),
        validation: Some(gate()),
    };
    let active = store.transition(&org, id, 1, activate).await.unwrap();
    assert_eq!(active.revision, 2);
    assert_eq!(active.status, RuleStatus::Active);
    assert!(active.effective_from.is_some());
    assert_eq!(active.decisions.len(), 1);
    // Provenance is preserved: the model proposed, a human activated.
    assert!(matches!(active.origin, RuleOrigin::Model { .. }));
    assert!(matches!(
        active.decisions[0].origin,
        RuleOrigin::Human { .. }
    ));

    let listed = store.list_active(&org, "cmdb").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].revision, 2);
    // Scoped by source: another source sees nothing.
    assert!(store.list_active(&org, "aws").await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn a_stale_expected_revision_conflicts_and_does_not_write() {
    let store = store().await;
    let org = format!("rules-{}", Uuid::new_v4());
    let id = Uuid::new_v4();
    store.propose(proposed(&org, id)).await.unwrap();
    store
        .transition(
            &org,
            id,
            1,
            RuleTransition {
                to: RuleStatus::Active,
                decision: decision("sre"),
                validation: Some(gate()),
            },
        )
        .await
        .unwrap();

    // The rule is now at revision 2; a transition expecting revision 1 conflicts.
    match store
        .transition(
            &org,
            id,
            1,
            RuleTransition {
                to: RuleStatus::Stale,
                decision: decision("sre"),
                validation: None,
            },
        )
        .await
    {
        Err(BackendError::Conflict(_)) => {}
        other => panic!("expected conflict, got {other:?}"),
    }
    // The losing transition left no trace.
    assert_eq!(store.get(&org, id).await.unwrap().unwrap().revision, 2);
    assert_eq!(
        store.get(&org, id).await.unwrap().unwrap().status,
        RuleStatus::Active
    );
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn revocation_is_terminal_and_records_its_time() {
    let store = store().await;
    let org = format!("rules-{}", Uuid::new_v4());
    let id = Uuid::new_v4();
    store.propose(proposed(&org, id)).await.unwrap();
    store
        .transition(
            &org,
            id,
            1,
            RuleTransition {
                to: RuleStatus::Active,
                decision: decision("sre"),
                validation: Some(gate()),
            },
        )
        .await
        .unwrap();
    let revoked = store
        .transition(
            &org,
            id,
            2,
            RuleTransition {
                to: RuleStatus::Revoked,
                decision: decision("sre"),
                validation: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(revoked.revision, 3);
    assert!(revoked.revoked_at.is_some());
    assert_eq!(revoked.decisions.len(), 2);
    // No longer active for a run.
    assert!(store.list_active(&org, "cmdb").await.unwrap().is_empty());

    // Revoked is terminal: any further transition is rejected.
    match store
        .transition(
            &org,
            id,
            3,
            RuleTransition {
                to: RuleStatus::Active,
                decision: decision("sre"),
                validation: Some(gate()),
            },
        )
        .await
    {
        Err(BackendError::Query(_)) => {}
        other => panic!("expected illegal-transition error, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn supersede_refreshes_the_mapping_under_a_revision_guard() {
    let store = store().await;
    let org = format!("rules-{}", Uuid::new_v4());
    let id = Uuid::new_v4();
    store.propose(proposed(&org, id)).await.unwrap();

    // Build a superseding revision with a refreshed target type and fingerprint.
    let mut updated = proposed(&org, id);
    updated.revision = 2;
    updated.schema_fingerprint = "fp-2".into();
    updated.mapping.target_type = "CmdbTeam".into();
    updated.status = RuleStatus::Active;
    updated.validation = Some(gate());
    updated.effective_from = Some(Utc::now());
    updated.decisions.push(decision("sre"));

    let stored = store.supersede(&org, id, 1, updated).await.unwrap();
    assert_eq!(stored.revision, 2);
    assert_eq!(stored.mapping.target_type, "CmdbTeam");
    let fetched = store.get(&org, id).await.unwrap().unwrap();
    assert_eq!(fetched.schema_fingerprint, "fp-2");
    assert_eq!(fetched.status, RuleStatus::Active);

    // A stale expected revision conflicts and writes nothing: the stored rule
    // is at revision 2, so a supersede expecting revision 1 (bumping to 2) finds
    // no matching row.
    let mut stale = fetched.clone();
    stale.revision = 2;
    stale.mapping.target_type = "CmdbOther".into();
    match store.supersede(&org, id, 1, stale).await {
        Err(BackendError::Conflict(_)) => {}
        other => panic!("expected conflict, got {other:?}"),
    }
    let after = store.get(&org, id).await.unwrap().unwrap();
    assert_eq!(after.revision, 2);
    assert_eq!(
        after.mapping.target_type, "CmdbTeam",
        "no write on conflict"
    );
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn transition_on_a_missing_rule_is_not_found() {
    let store = store().await;
    let org = format!("rules-{}", Uuid::new_v4());
    match store
        .transition(
            &org,
            Uuid::new_v4(),
            1,
            RuleTransition {
                to: RuleStatus::Active,
                decision: decision("sre"),
                validation: Some(gate()),
            },
        )
        .await
    {
        Err(BackendError::NotFound(_)) => {}
        other => panic!("expected not-found, got {other:?}"),
    }
}
