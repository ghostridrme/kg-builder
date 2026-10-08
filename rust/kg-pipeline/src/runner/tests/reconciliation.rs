use super::*;

fn entity(chain_id: Uuid) -> StaleEntity {
    StaleEntity {
        chain_id,
        uuid: Uuid::new_v4(),
        version: 1,
        entity_type: "Service".into(),
        name: "service".into(),
        collections: vec![],
    }
}

fn edge(source_chain_id: Uuid, target_chain_id: Uuid) -> StaleEdge {
    StaleEdge {
        uuid: Uuid::new_v4(),
        version: 1,
        source_chain_id,
        target_chain_id,
        name: "USES".into(),
        properties: Default::default(),
    }
}

fn scan() -> CollectionScan {
    CollectionScan {
        collection: CollectionRef {
            namespace: "prod".into(),
            source: "test".into(),
            key: "services".into(),
        },
        generation: 2,
    }
}

#[test]
fn shared_and_self_loop_edges_are_budgeted_once() {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let stale = StaleMembers {
        entities: vec![entity(a), entity(b)],
        incident_timelines: [(a, vec![]), (b, vec![])].into_iter().collect(),
        live_incident: [(a, vec![]), (b, vec![])].into_iter().collect(),
        edges: vec![edge(a, a), edge(a, b)],
        ..Default::default()
    };
    let batch = stale.within_budget(20, scan(), Utc::now()).unwrap();
    assert_eq!(batch.entities.len(), 2);
    assert_eq!(batch.edges.len(), 2);
    assert_eq!(
        batch
            .edges
            .iter()
            .map(|e| e.uuid)
            .collect::<HashSet<_>>()
            .len(),
        2
    );
}

#[test]
fn large_stale_population_keeps_each_selected_deletion_atomic() {
    let chains: Vec<_> = (0..20_000).map(|_| Uuid::new_v4()).collect();
    let stale = StaleMembers {
        entities: chains.iter().copied().map(entity).collect(),
        incident_timelines: chains.iter().map(|chain| (*chain, vec![])).collect(),
        live_incident: chains.iter().map(|chain| (*chain, vec![])).collect(),
        edges: chains
            .windows(2)
            .map(|pair| edge(pair[0], pair[1]))
            .collect(),
        ..Default::default()
    };
    let batch = stale.within_budget(14, scan(), Utc::now()).unwrap();
    assert_eq!(batch.entities.len(), 1);
    assert_eq!(batch.entities[0].chain_id, chains[0]);
    assert_eq!(batch.edges.len(), 1);
    assert_eq!(batch.edges[0].source_chain_id, chains[0]);
    assert_eq!(batch.edges[0].target_chain_id, chains[1]);
}

fn stored_relationship(chain: Uuid, source: Uuid, target: Uuid) -> kg_core::traits::EdgeRecord {
    kg_core::traits::EdgeRecord {
        uuid: Uuid::new_v4(),
        source_chain_id: source,
        target_chain_id: target,
        name: "USES".into(),
        version: 1,
        is_latest: true,
        confidence: 1.0,
        valid_from: None,
        invalid_at: None,
        sync_generation: Some(1),
        source_collections: vec![],
        stored: serde_json::json!({"chain_id":chain.to_string()})
            .as_object()
            .unwrap()
            .clone(),
    }
}

#[test]
fn recovered_observations_protect_only_the_observed_relationship_chain() {
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let seen = stored_relationship(Uuid::new_v4(), source, target);
    let stale = stored_relationship(Uuid::new_v4(), source, target);
    let recovery = BatchRecovery {
        observed_relationships: vec![relationship_chain_id(&seen).unwrap()],
        ..Default::default()
    };
    let restored: BatchRecovery =
        serde_json::from_value(serde_json::to_value(recovery).unwrap()).unwrap();
    let observed = ObservedSet {
        relationship_chains: restored.observed_relationships.into_iter().collect(),
        ..Default::default()
    };
    assert!(observed
        .relationship_chains
        .contains(&relationship_chain_id(&seen).unwrap()));
    assert!(!observed
        .relationship_chains
        .contains(&relationship_chain_id(&stale).unwrap()));
    // A new version retains the protected chain even when its record UUID changes.
    let new_version = stored_relationship(relationship_chain_id(&seen).unwrap(), source, target);
    assert!(observed
        .relationship_chains
        .contains(&relationship_chain_id(&new_version).unwrap()));
}

#[test]
fn reconciliation_rejects_missing_or_invalid_relationship_identity() {
    let mut record = stored_relationship(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    for value in [
        serde_json::Value::Null,
        serde_json::json!("invalid"),
        serde_json::json!(Uuid::nil().to_string()),
    ] {
        record.stored.insert("chain_id".into(), value);
        assert!(relationship_chain_id(&record).is_err());
    }
    record.stored.remove("chain_id");
    assert!(relationship_chain_id(&record).is_err());
    assert!(serde_json::from_value::<BatchRecovery>(serde_json::json!({
        "nodes":[],"failures":[],"observed_relationships":[[Uuid::new_v4(),Uuid::new_v4()]]
    }))
    .is_err());
}

#[test]
fn deletion_requires_history_and_budget_includes_its_guard() {
    let chain = Uuid::new_v4();
    let missing = StaleMembers {
        entities: vec![entity(chain)],
        ..Default::default()
    };
    assert!(missing.within_budget(6, scan(), Utc::now()).is_err());
    let with_history = || StaleMembers {
        entities: vec![entity(chain)],
        incident_timelines: [(chain, vec![])].into_iter().collect(),
        live_incident: [(chain, vec![])].into_iter().collect(),
        ..Default::default()
    };
    assert!(with_history().within_budget(5, scan(), Utc::now()).is_err());
    let batch = with_history().within_budget(6, scan(), Utc::now()).unwrap();
    assert_eq!(batch.incident_timelines.len(), 1);
    assert!(batch.incident_timelines[&chain].is_empty());
}

#[test]
fn reconciliation_carries_history_only_for_selected_deletions() {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let stale = StaleMembers {
        entities: vec![entity(a), entity(b)],
        incident_timelines: [(a, vec![]), (b, vec![])].into_iter().collect(),
        live_incident: [(a, vec![]), (b, vec![])].into_iter().collect(),
        ..Default::default()
    };
    let batch = stale.within_budget(6, scan(), Utc::now()).unwrap();
    assert_eq!(batch.entities.len(), 1);
    assert_eq!(
        batch.incident_timelines.keys().copied().collect::<Vec<_>>(),
        vec![a]
    );
}

#[test]
fn finite_and_pending_intervals_retire_but_ended_and_cancelled_do_not() {
    let at = DateTime::parse_from_rfc3339("2026-09-19T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let interval = |start: &str, end: Option<&str>| {
        let mut properties = kg_core::traits::GraphProperties::new();
        properties.insert("valid_from".into(), serde_json::json!(start));
        if let Some(end) = end {
            properties.insert("valid_to".into(), serde_json::json!(end));
        }
        properties
    };
    for (start, end, expected) in [
        ("2026-09-19T11:00:00Z", None, true),
        ("2026-09-19T11:00:00Z", Some("2026-09-19T13:00:00Z"), true),
        ("2026-09-19T13:00:00Z", None, true),
        ("2026-09-19T13:00:00Z", Some("2026-09-19T14:00:00Z"), true),
        ("2026-09-19T11:00:00Z", Some("2026-09-19T12:00:00Z"), false),
        ("2026-09-19T13:00:00Z", Some("2026-09-19T13:00:00Z"), false),
    ] {
        assert_eq!(
            relationship_needs_retirement(&interval(start, end), at).unwrap(),
            expected
        );
    }
    for field in ["cancelled_at", "deleted_at"] {
        let mut properties = interval("2026-09-19T13:00:00Z", None);
        properties.insert(field.into(), serde_json::json!("2026-09-19T11:00:00Z"));
        assert!(!relationship_needs_retirement(&properties, at).unwrap());
    }
    let mut properties = interval("2026-09-19T11:00:00Z", Some("2026-09-19T14:00:00Z"));
    properties.insert(
        "invalid_at".into(),
        serde_json::json!("2026-09-19T12:00:00Z"),
    );
    assert!(!relationship_needs_retirement(&properties, at).unwrap());
    properties.insert("invalid_at".into(), serde_json::json!("bad time"));
    assert!(relationship_needs_retirement(&properties, at).is_err());
}

#[test]
fn standalone_relationships_budget_and_carry_a_shared_history_guard() {
    let source = Uuid::new_v4();
    let make = || StaleMembers {
        edges: vec![edge(source, Uuid::new_v4()), edge(source, Uuid::new_v4())],
        incident_timelines: [(source, vec![])].into_iter().collect(),
        relationship_owners: [(source, entity(source))].into_iter().collect(),
        ..Default::default()
    };
    assert!(make().within_budget(6, scan(), Utc::now()).is_err());
    let one = make().within_budget(7, scan(), Utc::now()).unwrap();
    assert_eq!(one.edges.len(), 1);
    assert!(one.incident_timelines.contains_key(&source));
    let two = make().within_budget(11, scan(), Utc::now()).unwrap();
    assert_eq!(two.edges.len(), 2);
    assert_eq!(two.incident_timelines.len(), 1);
}
