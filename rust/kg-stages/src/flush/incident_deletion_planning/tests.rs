use super::*;
use crate::flush::reconciliation_planning::plan_reconciliation;
#[cfg(feature = "live-tests")]
use crate::flush::{node_mutation_planning::entity_properties, test_support::*};
#[cfg(feature = "live-tests")]
use kg_core::{
    models::CollectionMembership,
    traits::{GraphBackend, MutationBatch},
};
use kg_core::{
    runtime::stage_output::ReconciliationBatch,
    traits::{BatchIdentity, BatchKind},
};
#[cfg(feature = "live-tests")]
use serde_json::json;

fn interval_properties(start: DateTime<Utc>, end: Option<DateTime<Utc>>) -> GraphProperties {
    let mut properties = GraphProperties::new();
    properties.insert("uuid".into(), Uuid::new_v4().to_string().into());
    properties.insert("valid_from".into(), start.to_rfc3339().into());
    properties.insert("is_latest".into(), end.is_none().into());
    if let Some(end) = end {
        properties.insert("invalid_at".into(), end.to_rfc3339().into());
    }
    properties
}

#[test]
fn deletion_shortens_finite_current_and_cancels_pending_without_rewriting_bounds() {
    let at = Utc::now();
    let hour = chrono::Duration::hours(1);
    let snapshot = Uuid::new_v4();
    let mut plan = Plan::default();
    let current = interval_properties(at - hour, Some(at + hour));
    let pending = interval_properties(at + hour, Some(at + hour * 2));
    let open_pending = interval_properties(at + hour * 2, None);
    let ended = interval_properties(at - hour * 2, Some(at));
    let empty_future = interval_properties(at + hour, Some(at + hour));
    for properties in [&current, &pending, &open_pending, &ended, &empty_future] {
        plan_interval_deletion(&mut plan, properties, at, Some(snapshot), None).unwrap();
    }
    assert_eq!(plan.counts.edges_invalidated, 3);
    assert!(
        matches!(&plan.mutations[0], GraphMutation::UpdateEdge { properties, .. }
            if properties["invalid_at"] == at.to_rfc3339() && properties["is_latest"] == false)
    );
    for mutation in &plan.mutations[1..] {
        assert!(matches!(mutation, GraphMutation::CancelEdge {
                cancelled_at, cancellation_snapshot_id: Some(id), cancellation_context: None, ..
            } if *cancelled_at == at && *id == snapshot));
    }
    assert_eq!(pending["invalid_at"], (at + hour * 2).to_rfc3339());
}

#[test]
fn deletion_rejects_newer_observation_or_transition_and_skips_cancelled_history() {
    let at = Utc::now();
    let hour = chrono::Duration::hours(1);
    for field in ["last_seen_at", "last_transition_at"] {
        let mut properties = interval_properties(at + hour, None);
        properties.insert(field.into(), (at + hour).to_rfc3339().into());
        let mut plan = Plan::default();
        assert!(matches!(
            plan_interval_deletion(&mut plan, &properties, at, Some(Uuid::new_v4()), None),
            Err(StageError::CommitRejected { .. })
        ));
        assert!(plan.mutations.is_empty());
    }
    let mut cancelled = interval_properties(at + hour, None);
    cancelled.insert("cancelled_at".into(), (at - hour).to_rfc3339().into());
    let mut plan = Plan::default();
    plan_interval_deletion(&mut plan, &cancelled, at, Some(Uuid::new_v4()), None).unwrap();
    assert!(plan.mutations.is_empty());
}

#[test]
fn reconciliation_requires_complete_incident_history_before_deletion() {
    use kg_core::runtime::stage_output::{CollectionScan, StaleEntity};
    let chain = Uuid::new_v4();
    let mut batch = ReconciliationBatch {
        scan: CollectionScan {
            collection: kg_core::models::CollectionRef {
                namespace: "prod".into(),
                source: "aws".into(),
                key: "services".into(),
            },
            generation: 2,
        },
        captured_at: Utc::now(),
        entities: vec![StaleEntity {
            chain_id: chain,
            uuid: Uuid::new_v4(),
            version: 1,
            entity_type: "Service".into(),
            name: "api".into(),
            collections: vec![],
        }],
        released: vec![],
        edges: vec![],
        incident_timelines: Default::default(),
        live_incident: Default::default(),
        relationship_owners: Default::default(),
    };
    assert!(plan_reconciliation(
        &batch,
        BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Reconciliation,
            index: 0
        }
    )
    .unwrap_err()
    .to_string()
    .contains("missing an incident timeline"));
    batch.incident_timelines.insert(chain, vec![]);
    batch.live_incident.insert(chain, vec![]);
    let plan = plan_reconciliation(
        &batch,
        BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Reconciliation,
            index: 0,
        },
    )
    .unwrap();
    assert!(plan.preconditions.iter().any(|condition| matches!(condition,
            Precondition::IncidentTimelineIs { chain_id, versions } if *chain_id == chain && versions.is_empty()
        )));
    assert!(plan.preconditions.iter().any(|condition| matches!(condition,
            Precondition::LiveIncidentEdgesAre { chain_id, uuids } if *chain_id == chain && uuids.is_empty()
        )));
    let mut properties = interval_properties(batch.captured_at + chrono::Duration::hours(1), None);
    properties.insert("chain_id".into(), Uuid::new_v4().to_string().into());
    properties.insert("version".into(), 1.into());
    batch.incident_timelines.insert(
        chain,
        vec![IncidentVersionState {
            source_chain_id: chain,
            target_chain_id: chain,
            properties,
        }],
    );
    let identity = BatchIdentity {
        run_id: Uuid::new_v4(),
        kind: BatchKind::Reconciliation,
        index: 7,
    };
    let plan = plan_reconciliation(&batch, identity).unwrap();
    assert_eq!(
        plan.counts.edges_invalidated, 1,
        "full history supplies omitted pending self-loop action"
    );
    assert!(plan.mutations.iter().any(|mutation| matches!(mutation,
            GraphMutation::CancelEdge { cancellation_snapshot_id: None,
                cancellation_context: Some(kg_core::models::CancellationContext::Batch { batch }), .. }
                if *batch == identity)));
    let owner = batch.entities.pop().unwrap();
    let properties = batch.incident_timelines[&chain][0].properties.clone();
    batch.edges.push(kg_core::runtime::stage_output::StaleEdge {
        uuid: Uuid::parse_str(properties["uuid"].as_str().unwrap()).unwrap(),
        version: 1,
        source_chain_id: chain,
        target_chain_id: chain,
        name: "DEPENDS_ON".into(),
        properties,
    });
    assert!(plan_reconciliation(&batch, identity)
        .unwrap_err()
        .to_string()
        .contains("source ownership baseline"));
    batch.relationship_owners.insert(chain, owner.clone());
    let plan = plan_reconciliation(&batch, identity).unwrap();
    assert!(plan
        .preconditions
        .iter()
        .any(|condition| matches!(condition,
            Precondition::SoleCollectionOwnerIs { uuid, .. } if *uuid == owner.uuid)));
    assert!(plan
        .preconditions
        .iter()
        .any(|condition| matches!(condition,
            Precondition::LatestVersionIs { uuid, .. } if *uuid == owner.uuid)));
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn source_deletion_fences_ended_shared_and_self_loop_history() {
    let graph = live_graph().await;
    let context = ctx(graph.clone());
    let snap = snapshot(Utc::now());
    let mut source = entity(&format!("incident-source-{}", Uuid::new_v4()), 1, &snap);
    let mut target = entity(&format!("incident-target-{}", Uuid::new_v4()), 1, &snap);
    source.uuid = source.chain_id;
    target.uuid = target.chain_id;
    for node in [&source, &target] {
        seed_node(
            &graph,
            json!(entity_properties(
                node,
                ORG,
                1,
                None,
                &node.all_properties,
                node.structural_hash,
                &node.collections
            )),
        )
        .await;
    }
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    for (uuid, end) in [(ids[0], target.chain_id), (ids[1], source.chain_id)] {
        graph.execute_write(
                "MATCH (s:Entity {uuid:$source}), (t:Entity {uuid:$target}) CREATE (s)-[r:RELATES_TO]->(t) SET r=$props",
                &json!({"source":source.chain_id,"target":end,"props":{
                    "uuid":uuid,"chain_id":Uuid::new_v4(),"org_id":ORG,"name":"DEPENDS_ON",
                    "version":1,"is_latest":false,"valid_from":(snap.captured_at-chrono::Duration::hours(2)).to_rfc3339(),
                    "valid_to":(snap.captured_at-chrono::Duration::hours(1)).to_rfc3339()
                }}),
            ).await.unwrap();
    }
    let mut plan = Plan::default();
    for chain_id in [source.chain_id, target.chain_id] {
        plan.mutations.push(GraphMutation::DeleteEntity {
            chain_id,
            deleted_at: snap.captured_at,
            deleted_by: None,
            reason: None,
        });
    }
    plan_source_deletions(
        &mut plan,
        &HashMap::from([
            ((source.chain_id, snap.captured_at), snap.uuid),
            ((target.chain_id, snap.captured_at), snap.uuid),
        ]),
        &context,
    )
    .await
    .unwrap();
    for (chain, expected) in [(source.chain_id, 2), (target.chain_id, 1)] {
        let versions = plan
            .preconditions
            .iter()
            .find_map(|condition| match condition {
                Precondition::IncidentTimelineIs { chain_id, versions } if *chain_id == chain => {
                    Some(versions)
                }
                _ => None,
            })
            .expect("complete incident guard");
        assert_eq!(versions.len(), expected);
    }
    assert_eq!(
        plan.counts.edges_invalidated, 0,
        "ended versions are guarded, not reopened or closed again"
    );
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn source_deletion_plans_all_intervals_once_with_real_capture_evidence() {
    let graph = live_graph().await;
    let context = ctx(graph.clone());
    let snap = snapshot(Utc::now());
    let hour = chrono::Duration::hours(1);
    let mut source = entity(&format!("scheduled-source-{}", Uuid::new_v4()), 1, &snap);
    let mut target = entity(&format!("scheduled-target-{}", Uuid::new_v4()), 1, &snap);
    source.uuid = source.chain_id;
    target.uuid = target.chain_id;
    for node in [&source, &target] {
        seed_node(
            &graph,
            json!(entity_properties(
                node,
                ORG,
                1,
                None,
                &node.all_properties,
                node.structural_hash,
                &node.collections
            )),
        )
        .await;
    }
    let mut ids = Vec::new();
    for (start, end, target_chain) in [
        (
            snap.captured_at - hour,
            Some(snap.captured_at + hour),
            target.chain_id,
        ),
        (
            snap.captured_at + hour,
            Some(snap.captured_at + hour * 2),
            target.chain_id,
        ),
        (snap.captured_at + hour * 2, None, source.chain_id),
    ] {
        let mut props = interval_properties(start, end);
        let id = Uuid::parse_str(props["uuid"].as_str().unwrap()).unwrap();
        ids.push(id);
        props.insert("chain_id".into(), Uuid::new_v4().to_string().into());
        props.insert("org_id".into(), ORG.into());
        props.insert("name".into(), "DEPENDS_ON".into());
        props.insert("version".into(), 1.into());
        graph.execute_write(
                "MATCH (s:Entity {uuid:$source}), (t:Entity {uuid:$target}) CREATE (s)-[r:RELATES_TO]->(t) SET r=$props",
                &json!({"source":source.chain_id,"target":target_chain,"props":props}),
            ).await.unwrap();
    }
    let mut plan = Plan::default();
    for chain_id in [source.chain_id, target.chain_id] {
        plan.mutations.push(GraphMutation::DeleteEntity {
            chain_id,
            deleted_at: snap.captured_at,
            deleted_by: None,
            reason: None,
        });
    }
    let evidence = HashMap::from([
        ((source.chain_id, snap.captured_at), snap.uuid),
        ((target.chain_id, snap.captured_at), snap.uuid),
    ]);
    plan_source_deletions(&mut plan, &evidence, &context)
        .await
        .unwrap();
    assert_eq!(
        plan.counts.edges_invalidated, 3,
        "shared and self-loop versions count once"
    );
    assert!(plan.mutations.iter().any(|mutation| matches!(mutation,
            GraphMutation::UpdateEdge { uuid, properties } if *uuid == ids[0]
                && properties["invalid_at"] == snap.captured_at.to_rfc3339())));
    for id in &ids[1..] {
        assert!(plan.mutations.iter().any(|mutation| matches!(mutation,
                GraphMutation::CancelEdge { uuid, cancellation_snapshot_id: Some(snapshot),
                    cancellation_context: None, .. } if uuid == id && *snapshot == snap.uuid)));
    }
    assert_eq!(
        plan.preconditions
            .iter()
            .filter(|condition| matches!(condition, Precondition::IncidentTimelineIs { .. }))
            .count(),
        2
    );
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn reconciliation_rejects_source_ownership_changed_after_planning() {
    use kg_core::runtime::stage_output::{CollectionScan, StaleEdge, StaleEntity};
    use kg_core::traits::{RequestFingerprint, RunHeader};
    let graph = live_graph().await;
    let snap = snapshot(Utc::now());
    let collection = kg_core::models::CollectionRef {
        namespace: "prod".into(),
        source: "aws".into(),
        key: Uuid::new_v4().to_string(),
    };
    let mut source = entity(&format!("ownership-source-{}", Uuid::new_v4()), 1, &snap);
    source.uuid = source.chain_id;
    source.collections = vec![CollectionMembership {
        collection: collection.clone(),
        generation: 1,
    }];
    seed_node(
        &graph,
        json!(entity_properties(
            &source,
            ORG,
            1,
            None,
            &source.all_properties,
            source.structural_hash,
            &source.collections
        )),
    )
    .await;
    let edge_id = Uuid::new_v4();
    let mut properties = interval_properties(snap.captured_at + chrono::Duration::hours(1), None);
    properties.insert("uuid".into(), edge_id.to_string().into());
    properties.insert("chain_id".into(), Uuid::new_v4().to_string().into());
    properties.insert("org_id".into(), ORG.into());
    properties.insert("name".into(), "DEPENDS_ON".into());
    properties.insert("version".into(), 1.into());
    properties.insert(
        "producer_namespace".into(),
        collection.namespace.clone().into(),
    );
    properties.insert("producer_source".into(), collection.source.clone().into());
    properties.insert("sync_generation".into(), 1.into());
    graph
        .execute_write(
            "MATCH (s:Entity {uuid:$source}) CREATE (s)-[r:RELATES_TO]->(s) SET r=$props",
            &json!({"source":source.uuid,"props":properties}),
        )
        .await
        .unwrap();
    let identity = BatchIdentity {
        run_id: Uuid::new_v4(),
        kind: BatchKind::Reconciliation,
        index: 0,
    };
    let fingerprint = RequestFingerprint(format!("{:032x}", identity.run_id.as_u128()));
    graph
        .register_run(&RunHeader {
            observation_manifest: Default::default(),
            rule_freezes: vec![],
            schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
                profiles: Default::default(),
                org_id: ORG.into(),
                sources: Default::default(),
            },
            org_id: ORG.into(),
            run_id: identity.run_id,
            fingerprint: fingerprint.clone(),
            settings_version: "ownership-race".into(),
            capture_default: snap.captured_at,
            batch_plan: vec![],
        })
        .await
        .unwrap();
    let batch = ReconciliationBatch {
        scan: CollectionScan {
            collection: collection.clone(),
            generation: 2,
        },
        captured_at: snap.captured_at,
        entities: vec![],
        released: vec![],
        live_incident: Default::default(),
        edges: vec![StaleEdge {
            uuid: edge_id,
            version: 1,
            source_chain_id: source.chain_id,
            target_chain_id: source.chain_id,
            name: "DEPENDS_ON".into(),
            properties: properties.clone(),
        }],
        relationship_owners: [(
            source.chain_id,
            StaleEntity {
                chain_id: source.chain_id,
                uuid: source.uuid,
                version: 1,
                entity_type: source.entity_type.clone(),
                name: source.name.clone(),
                collections: source.collections.clone(),
            },
        )]
        .into_iter()
        .collect(),
        incident_timelines: [(
            source.chain_id,
            vec![IncidentVersionState {
                source_chain_id: source.chain_id,
                target_chain_id: source.chain_id,
                properties,
            }],
        )]
        .into_iter()
        .collect(),
    };
    let plan = plan_reconciliation(&batch, identity).unwrap();
    let initially_stale = graph
        .find_edges(
            ORG,
            &EdgeLookup::ScheduledStaleInCollection {
                collection: collection.clone(),
                before_generation: 2,
                effective_at: snap.captured_at,
            },
        )
        .await
        .unwrap();
    assert!(initially_stale.iter().any(|edge| edge.uuid == edge_id));
    let another = kg_core::models::CollectionRef {
        key: Uuid::new_v4().to_string(),
        ..collection.clone()
    };
    graph
        .apply_mutations(
            ORG,
            &[GraphMutation::ObserveEntity {
                chain_id: source.chain_id,
                observed_at: snap.captured_at,
                sync_generation: None,
                snapshot_id: None,
                collection: Some(CollectionMembership {
                    collection: another,
                    generation: 1,
                }),
            }],
        )
        .await
        .unwrap();
    let mutation = MutationBatch {
        org_id: ORG.into(),
        batch: identity,
        fingerprint,
        preconditions: plan.preconditions,
        mutations: plan.mutations,
        result: json!({}),
    };
    assert!(matches!(
        graph.commit_batch(&mutation).await,
        Err(BackendError::Conflict(_))
    ));
    let versions = graph
        .find_edges(
            ORG,
            &EdgeLookup::VersionsByEndpointChains {
                chain_ids: vec![source.chain_id],
            },
        )
        .await
        .unwrap();
    let edge = versions.iter().find(|edge| edge.uuid == edge_id).unwrap();
    assert!(!edge.stored.contains_key("cancelled_at"));
    let stale = graph
        .find_edges(
            ORG,
            &EdgeLookup::ScheduledStaleInCollection {
                collection,
                before_generation: 2,
                effective_at: snap.captured_at,
            },
        )
        .await
        .unwrap();
    let fresh = stale.iter().find(|edge| edge.uuid == edge_id).unwrap();
    assert_eq!(
        fresh.source_collections.len(),
        2,
        "fresh read exposes shared ownership"
    );
    assert_ne!(
        fresh.source_collections,
        vec![batch.scan.collection.clone()],
        "shared source cannot satisfy reconciliation's sole-owner selection"
    );
}
