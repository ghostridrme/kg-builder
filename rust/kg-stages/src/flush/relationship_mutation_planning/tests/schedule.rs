use super::*;

pub(super) fn cancel_command(
    version: &StoredRelationship,
    snapshot: &SnapshotNode,
) -> PendingRelationshipDirective {
    PendingRelationshipDirective {
        target: kg_core::models::RelationshipVersionRef {
            source_chain_id: version.source_chain_id,
            target_chain_id: version.target_chain_id,
            chain_id: version.chain_id,
            version_uuid: version.uuid,
        },
        action: RelationshipDirectiveAction::Cancel,
        effective_at: snapshot.captured_at,
        snapshot_id: snapshot.uuid,
        captured_at: snapshot.captured_at,
        scope: ConnectorScope::of(snapshot),
    }
}

#[test]
fn cancelling_pending_version_keeps_original_bounds_and_predecessor() {
    let (current, future, base, _, snapshot) = scheduled_fixture();
    let mut work = batch(vec![], vec![snapshot.clone()], base);
    work.relationship_directives = Arc::new(vec![cancel_command(&future, &snapshot)]);
    let (plan, embeddings) = plan_relationships(&work).unwrap();
    assert!(embeddings.is_empty());
    assert_eq!(plan.mutations.len(), 1);
    assert!(
        matches!(&plan.mutations[0], GraphMutation::CancelEdge { uuid, cancellation_snapshot_id, .. } if *uuid == future.uuid && *cancellation_snapshot_id == Some(snapshot.uuid))
    );
    assert!(!plan
        .mutations
        .iter()
        .any(|m| matches!(m, GraphMutation::UpdateEdge { uuid, .. } if *uuid == current.uuid)));
}

#[test]
fn replacement_is_atomic_and_allocates_after_cancelled_revision() {
    let (_, future, base, mut incoming, snapshot) = scheduled_fixture();
    incoming.valid_from = future.valid_from;
    incoming.description = "replacement".into();
    let mut command = cancel_command(&future, &snapshot);
    command.action = RelationshipDirectiveAction::Replace {
        replacement_edge_uuid: incoming.uuid,
    };
    let mut work = batch(vec![incoming.clone()], vec![snapshot], base);
    work.relationship_directives = Arc::new(vec![command]);
    let (plan, _) = plan_relationships(&work).unwrap();
    assert!(
        matches!(plan.mutations[0], GraphMutation::CancelEdge { uuid, .. } if uuid == future.uuid)
    );
    let properties = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge {
                uuid, properties, ..
            } if *uuid == incoming.uuid => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["version"], 3);
    assert_eq!(properties["previous_version_uuid"], future.uuid.to_string());
    assert_eq!(properties["description"], "replacement");
}

#[test]
fn cancellation_rejects_wrong_owner_stale_capture_and_active_target() {
    let (_, future, base, _, snapshot) = scheduled_fixture();
    for mode in 0..4 {
        let mut capture = snapshot.clone();
        if mode == 0 {
            capture.source = "someone-else".into();
        }
        if mode == 1 {
            capture.captured_at = future.latest_observation.unwrap();
        }
        if mode == 2 {
            capture.captured_at = future.valid_from;
        }
        let mut command = cancel_command(&future, &capture);
        if mode == 2 {
            command.effective_at = future.valid_from;
        }
        if mode == 3 {
            command.target.version_uuid = Uuid::new_v4();
        }
        let mut work = batch(vec![], vec![capture], base.clone());
        work.relationship_directives = Arc::new(vec![command]);
        assert!(plan_relationships(&work).is_err(), "mode {mode}");
    }
}

#[test]
fn scheduled_observation_can_have_finite_bounds_before_capture() {
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let capture = snapshot(Uuid::new_v4(), at);
    let mut incoming = edge(
        source,
        target,
        "CALLS",
        "scheduled",
        capture.uuid,
        at + Duration::days(2),
    );
    incoming.valid_to = Some(at + Duration::days(3));
    let (plan, embeddings) = plan_relationships(&batch(
        vec![incoming],
        vec![capture],
        baseline(&[(source, target)], vec![], &[]),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_created, 1);
    assert_eq!(embeddings.len(), 1);
    let props = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(props["is_latest"], false);
    assert_eq!(props["valid_to"], (at + Duration::days(3)).to_rfc3339());
}

#[test]
fn disjoint_same_capture_cardinality_intervals_coexist() {
    let at = Utc::now();
    let (source, first, second) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let capture = snapshot(Uuid::new_v4(), at);
    let mut a = edge(
        source,
        first,
        "RUNS_ON",
        "a",
        capture.uuid,
        at + Duration::days(1),
    );
    a.valid_to = Some(at + Duration::days(2));
    let b = edge(
        source,
        second,
        "RUNS_ON",
        "b",
        capture.uuid,
        at + Duration::days(2),
    );
    let (plan, _) = plan_relationships(&batch(
        vec![a, b],
        vec![capture],
        baseline(
            &[(source, first), (source, second)],
            vec![],
            &[(source, "RUNS_ON")],
        ),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_created, 2);
    assert_eq!(plan.counts.edges_invalidated, 0);
}

#[test]
fn current_cardinality_change_stops_before_later_scheduled_target() {
    let at = Utc::now();
    let (source, now_target, later_target) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let capture = snapshot(Uuid::new_v4(), at);
    let mut future = stored(
        source,
        later_target,
        "RUNS_ON",
        "future",
        at - Duration::days(1),
        Some(scope()),
    );
    future.valid_from = at + Duration::days(1);
    let incoming = edge(source, now_target, "RUNS_ON", "now", capture.uuid, at);
    let (plan, _) = plan_relationships(&batch(
        vec![incoming],
        vec![capture],
        baseline(
            &[(source, now_target)],
            vec![future.clone()],
            &[(source, "RUNS_ON")],
        ),
    ))
    .unwrap();
    assert!(closures(&plan).is_empty());
    let props = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(props["valid_to"], future.valid_from.to_rfc3339());
}

#[test]
fn cancellation_effective_time_is_independent_of_when_it_is_learned() {
    let (_, future, base, _, snapshot) = scheduled_fixture();
    for captured_at in [snapshot.captured_at, future.valid_from + Duration::days(1)] {
        let mut capture = snapshot.clone();
        capture.captured_at = captured_at;
        let mut command = cancel_command(&future, &capture);
        command.effective_at = future.valid_from - Duration::hours(1);
        let mut work = batch(vec![], vec![capture.clone()], base.clone());
        work.relationship_directives = Arc::new(vec![command.clone()]);
        let (plan, _) = plan_relationships(&work).unwrap();
        assert!(
            matches!(&plan.mutations[0], GraphMutation::CancelEdge { cancelled_at, observed_at, .. } if *cancelled_at == command.effective_at && *observed_at == captured_at)
        );
    }
}

#[test]
fn pending_same_start_changes_require_explicit_replacement() {
    let (_, future, base, mut incoming, snapshot) = scheduled_fixture();
    incoming.valid_from = future.valid_from;
    incoming.description = "silently replace future".into();
    assert!(plan_relationships(&batch(
        vec![incoming.clone()],
        vec![snapshot.clone()],
        base.clone()
    ))
    .unwrap_err()
    .to_string()
    .contains("explicit replacement"));
    incoming.description = future.description;
    incoming.valid_to = Some(future.valid_from + Duration::days(1));
    assert!(
        plan_relationships(&batch(vec![incoming], vec![snapshot], base))
            .unwrap_err()
            .to_string()
            .contains("explicit replacement")
    );
}

#[test]
fn later_future_successor_shortens_pending_interval_without_replacing_its_identity() {
    let (_, future, base, mut incoming, snapshot) = scheduled_fixture();
    incoming.valid_from = future.valid_from + Duration::days(2);
    incoming.description = "next schedule".into();
    let (plan, _) =
        plan_relationships(&batch(vec![incoming.clone()], vec![snapshot], base)).unwrap();
    let closing = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpdateEdge { uuid, properties } if *uuid == future.uuid => {
                Some(properties)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(closing["invalid_at"], incoming.valid_from.to_rfc3339());
    assert!(!closing.contains_key("description"));
    let next = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(next["version"], 3);
    assert_eq!(next["previous_version_uuid"], future.uuid.to_string());
}

pub(super) fn apply_plan_to_baseline(
    mut baseline: RelationshipBaseline,
    plan: &Plan,
) -> RelationshipBaseline {
    for mutation in &plan.mutations {
        match mutation {
            GraphMutation::UpdateEdge { uuid, properties } => {
                for pair in &mut baseline.pairs {
                    if let Some(value) = pair
                        .versions
                        .iter_mut()
                        .find(|value| value["uuid"] == uuid.to_string())
                    {
                        value.extend(properties.clone());
                    }
                }
            }
            GraphMutation::UpsertEdge {
                uuid,
                source_chain_id,
                target_chain_id,
                properties,
            } => {
                let pair = baseline
                    .pairs
                    .iter_mut()
                    .find(|pair| {
                        pair.source_chain_id == *source_chain_id
                            && pair.target_chain_id == *target_chain_id
                    })
                    .unwrap();
                let mut properties = properties.clone();
                properties.insert("uuid".into(), uuid.to_string().into());
                pair.versions
                    .push(kg_core::traits::relationship_timeline::state(&properties));
            }
            GraphMutation::CancelEdge {
                uuid,
                cancelled_at,
                cancellation_snapshot_id,
                observed_at,
                ..
            } => {
                let properties = baseline
                    .pairs
                    .iter_mut()
                    .flat_map(|pair| &mut pair.versions)
                    .find(|properties| properties["uuid"] == uuid.to_string())
                    .unwrap();
                properties.insert("cancelled_at".into(), cancelled_at.to_rfc3339().into());
                properties.insert(
                    "cancellation_snapshot_id".into(),
                    cancellation_snapshot_id.unwrap().to_string().into(),
                );
                properties.insert("last_transition_at".into(), observed_at.to_rfc3339().into());
                properties.insert("is_latest".into(), false.into());
            }
            _ => panic!("unexpected relationship mutation"),
        }
    }
    for pair in &mut baseline.pairs {
        pair.live.clear();
        let timeline = RelationshipTimeline::from_pair(pair).unwrap();
        pair.live = timeline
            .chains
            .values()
            .flat_map(|chain| &chain.versions)
            .filter(|version| {
                version.relationship.ended_at.is_none()
                    && version.relationship.cancelled_at.is_none()
            })
            .map(|version| {
                let mut value = version.relationship.clone();
                value.scope = Some(scope());
                value
            })
            .collect();
        pair.versions
            .sort_by_key(|properties| properties["uuid"].as_str().unwrap().to_owned());
    }
    baseline
}

#[test]
fn repeated_insertions_have_identical_history_in_one_batch_or_separate_batches() {
    let (_, future, base, mut first, capture) = scheduled_fixture();
    first.description = "middle one".into();
    let next_capture = snapshot(Uuid::new_v4(), capture.captured_at + Duration::hours(1));
    let second = edge(
        first.source_chain_id,
        first.target_chain_id,
        "CALLS",
        "middle two",
        next_capture.uuid,
        next_capture.captured_at,
    );
    let (combined, embeddings) = plan_relationships(&batch(
        vec![second.clone(), first.clone()],
        vec![next_capture.clone(), capture.clone()],
        base.clone(),
    ))
    .unwrap();
    assert_eq!(embeddings.len(), 2);
    let original_future = base.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == future.uuid.to_string())
        .unwrap()
        .clone();
    let combined = apply_plan_to_baseline(base.clone(), &combined);
    let (one, _) = plan_relationships(&batch(vec![first], vec![capture], base.clone())).unwrap();
    let after_one = apply_plan_to_baseline(base, &one);
    let (two, _) =
        plan_relationships(&batch(vec![second], vec![next_capture], after_one.clone())).unwrap();
    let separate = apply_plan_to_baseline(after_one, &two);
    assert_eq!(combined.pairs[0].versions, separate.pairs[0].versions);
    let future_record = combined.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == future.uuid.to_string())
        .unwrap();
    assert_eq!(future_record, &original_future);
}

#[test]
fn cancelled_schedule_keeps_revision_allocation_and_does_not_extend_predecessor() {
    let (current, future, base, mut incoming, capture) = scheduled_fixture();
    let original_current = base.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == current.uuid.to_string())
        .unwrap()
        .clone();
    let mut cancel = batch(vec![], vec![capture.clone()], base);
    cancel.relationship_directives = Arc::new(vec![cancel_command(&future, &capture)]);
    let (plan, _) = plan_relationships(&cancel).unwrap();
    let after = apply_plan_to_baseline((*cancel.baseline).clone(), &plan);
    let new_capture = snapshot(Uuid::new_v4(), capture.captured_at + Duration::hours(1));
    incoming.last_seen_snapshot_id = Some(new_capture.uuid);
    incoming.valid_from = future.valid_from + Duration::days(1);
    incoming.description = "new schedule".into();
    let (next, _) =
        plan_relationships(&batch(vec![incoming], vec![new_capture], after.clone())).unwrap();
    let props = next
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(props["version"], 3);
    assert_eq!(props["previous_version_uuid"], future.uuid.to_string());
    assert_eq!(
        after.pairs[0]
            .versions
            .iter()
            .find(|props| props["uuid"] == current.uuid.to_string())
            .unwrap(),
        &original_current
    );
}

#[test]
fn ordinary_same_capture_observation_cannot_undo_cancellation_in_any_chunk() {
    let (_, future, base, mut incoming, capture) = scheduled_fixture();
    incoming.valid_from = future.valid_from;
    incoming.description = future.description.clone();
    let command = cancel_command(&future, &capture);
    let mut combined = batch(vec![incoming.clone()], vec![capture.clone()], base.clone());
    combined.relationship_directives = Arc::new(vec![command.clone()]);
    assert!(plan_relationships(&combined)
        .unwrap_err()
        .to_string()
        .contains("cancellation at the same capture"));
    let mut cancellation = batch(vec![], vec![capture.clone()], base);
    cancellation.relationship_directives = Arc::new(vec![command]);
    let (plan, _) = plan_relationships(&cancellation).unwrap();
    let after = apply_plan_to_baseline((*cancellation.baseline).clone(), &plan);
    assert!(
        plan_relationships(&batch(vec![incoming], vec![capture], after))
            .unwrap_err()
            .to_string()
            .contains("cancellation at the same capture")
    );
}

#[test]
fn cancellation_and_predecessor_provenance_can_share_capture() {
    let (current, future, base, incoming, capture) = scheduled_fixture();
    let mut work = batch(vec![incoming], vec![capture.clone()], base);
    work.relationship_directives = Arc::new(vec![cancel_command(&future, &capture)]);
    let (plan, _) = plan_relationships(&work).unwrap();
    assert!(plan.mutations.iter().any(|m| matches!(m, GraphMutation::UpdateEdge { uuid, properties } if *uuid == current.uuid && properties.len() == 2)));
    assert_eq!(plan.counts.edges_unchanged, 1);
    assert_eq!(plan.counts.edges_invalidated, 1);
}

#[test]
fn empty_intervals_neither_close_other_targets_nor_bound_same_lineage() {
    let at = Utc::now();
    let (source, first, second) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let capture = snapshot(Uuid::new_v4(), at);
    let current = stored(
        source,
        first,
        "RUNS_ON",
        "current",
        at - Duration::days(1),
        Some(scope()),
    );
    let mut empty = edge(source, second, "RUNS_ON", "empty", capture.uuid, at);
    empty.valid_to = Some(at);
    let (plan, _) = plan_relationships(&batch(
        vec![empty],
        vec![capture.clone()],
        baseline(&[(source, second)], vec![current], &[(source, "RUNS_ON")]),
    ))
    .unwrap();
    assert!(closures(&plan).is_empty());
    let mut empty = stored(
        source,
        second,
        "CALLS",
        "empty",
        at - Duration::days(1),
        Some(scope()),
    );
    empty.valid_from = at + Duration::days(1);
    empty.ended_at = Some(empty.valid_from);
    let mut base = baseline(&[(source, second)], vec![], &[]);
    base.pairs[0].versions.push(raw(&empty));
    let incoming = edge(source, second, "CALLS", "real", capture.uuid, at);
    let (plan, _) = plan_relationships(&batch(vec![incoming], vec![capture], base)).unwrap();
    let props = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert!(!props.contains_key("valid_to"));
}

#[test]
fn same_chain_empty_revision_keeps_effective_predecessor_open() {
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let capture = snapshot(Uuid::new_v4(), at);
    let current = stored(
        source,
        target,
        "CALLS",
        "current",
        at - Duration::days(1),
        Some(scope()),
    );
    let mut empty = edge(source, target, "CALLS", "empty", capture.uuid, at);
    empty.valid_to = Some(at);
    let (plan, _) = plan_relationships(&batch(
        vec![empty],
        vec![capture],
        baseline(&[(source, target)], vec![current], &[]),
    ))
    .unwrap();
    assert!(closures(&plan).is_empty());
    assert_eq!(plan.counts.edges_updated, 1);
}

#[test]
fn later_disjoint_schedules_do_not_suppress_current_cardinality_interval() {
    let at = Utc::now();
    let (source, now_target, tomorrow_target, later_target) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let capture = snapshot(Uuid::new_v4(), at);
    let mut tomorrow = stored(
        source,
        tomorrow_target,
        "RUNS_ON",
        "tomorrow",
        at - Duration::hours(1),
        Some(scope()),
    );
    tomorrow.valid_from = at + Duration::days(1);
    tomorrow.ended_at = Some(at + Duration::days(2));
    let mut later = stored(source, later_target, "RUNS_ON", "later", at, Some(scope()));
    later.valid_from = at + Duration::days(2);
    let mut base = baseline(
        &[(source, now_target)],
        vec![later.clone()],
        &[(source, "RUNS_ON")],
    );
    base.relations[0]
        .versions
        .push(kg_core::traits::relationship_timeline::VersionState {
            target_chain_id: tomorrow_target,
            properties: raw(&tomorrow),
        });
    let incoming = edge(source, now_target, "RUNS_ON", "now", capture.uuid, at);
    let (plan, _) = plan_relationships(&batch(vec![incoming], vec![capture], base)).unwrap();
    assert_eq!(plan.counts.edges_created, 1);
    assert!(closures(&plan).is_empty());
    let props = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(props["valid_to"], tomorrow.valid_from.to_rfc3339());
}

#[test]
fn finite_cardinality_intervals_use_persisted_scope_without_live_projection() {
    let at = Utc::now();
    let (source, old_target, new_target) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let capture = snapshot(Uuid::new_v4(), at);
    let mut finite = stored(
        source,
        old_target,
        "RUNS_ON",
        "old",
        at - Duration::days(1),
        None,
    );
    finite.ended_at = Some(at + Duration::days(1));
    let mut base = baseline(&[(source, new_target)], vec![], &[(source, "RUNS_ON")]);
    base.relations[0]
        .versions
        .push(kg_core::traits::relationship_timeline::VersionState {
            target_chain_id: old_target,
            properties: raw(&finite),
        });
    let incoming = edge(source, new_target, "RUNS_ON", "new", capture.uuid, at);
    let (plan, _) = plan_relationships(&batch(vec![incoming], vec![capture], base)).unwrap();
    assert_eq!(closures(&plan), vec![finite.uuid]);
}

#[cfg(feature = "live-tests")]
pub(super) async fn live_schedule_baseline(
    graph: &kg_storage_neo4j::Neo4jGraphBackend,
    org: &str,
    source: Uuid,
    target: Uuid,
) -> RelationshipBaseline {
    use kg_core::traits::{EdgeLookup, GraphBackend};
    let records = graph
        .find_edges(
            org,
            &EdgeLookup::VersionsByChainPairs {
                pairs: vec![(source, target)],
            },
        )
        .await
        .unwrap();
    let mut pair = PairBaseline {
        source_chain_id: source,
        target_chain_id: target,
        versions: records
            .iter()
            .map(|record| kg_core::traits::relationship_timeline::state(&record.stored))
            .collect(),
        live: vec![],
    };
    let timeline = RelationshipTimeline::from_pair(&pair).unwrap();
    pair.live = timeline
        .chains
        .values()
        .flat_map(|chain| &chain.versions)
        .filter(|version| {
            version.relationship.ended_at.is_none() && version.relationship.cancelled_at.is_none()
        })
        .map(|version| version.relationship.clone())
        .collect();
    RelationshipBaseline {
        pairs: vec![pair],
        ..Default::default()
    }
}

#[cfg(feature = "live-tests")]
pub(super) async fn commit_schedule_plan(
    graph: &kg_storage_neo4j::Neo4jGraphBackend,
    org: &str,
    plan: Plan,
    captured_at: DateTime<Utc>,
) -> kg_core::traits::MutationBatch {
    use kg_core::traits::{
        BatchIdentity, BatchKind, GraphBackend, MutationBatch, RequestFingerprint, RunHeader,
    };
    let run_id = Uuid::new_v4();
    let fingerprint = RequestFingerprint(format!("{:032x}", run_id.as_u128()));
    graph
        .register_run(&RunHeader {
            observation_manifest: Default::default(),
            rule_freezes: vec![],
            org_id: org.into(),
            run_id,
            fingerprint: fingerprint.clone(),
            settings_version: "schedule-planner-test".into(),
            capture_default: captured_at,
            batch_plan: vec![],
            schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
                profiles: Default::default(),
                org_id: org.into(),
                sources: Default::default(),
            },
        })
        .await
        .unwrap();
    let batch = MutationBatch {
        org_id: org.into(),
        batch: BatchIdentity {
            run_id,
            kind: BatchKind::Relationship,
            index: 0,
        },
        fingerprint,
        preconditions: plan.preconditions,
        mutations: plan.mutations,
        result: serde_json::json!({"scheduled":true}),
    };
    let first = graph.commit_batch(&batch).await.unwrap();
    assert!(!first.replayed);
    let replay = graph.commit_batch(&batch).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(first.result, replay.result);
    batch
}

#[cfg(feature = "live-tests")]
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn scheduled_insert_replace_cancel_commit_with_real_timeline_fences_and_replay() {
    use kg_core::{errors::BackendError, traits::GraphBackend};
    let graph = kg_neo4j_testkit::connect()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.unwrap();
    let org = format!("schedule-planner-{}", Uuid::new_v4());
    let (current, future, base, mut incoming, capture) = scheduled_fixture();
    let source = current.source_chain_id;
    let target = current.target_chain_id;
    let mut seed: Vec<_> = [source, target].into_iter().map(|uuid| GraphMutation::UpsertEntity {
        uuid,
        properties: serde_json::json!({"chain_id":uuid,"name":"scheduled endpoint","namespace":"prod","entity_type":"Service","version":1,"is_latest":true,"valid_from":"2026-01-01T00:00:00Z"}).as_object().unwrap().clone()
    }).collect();
    seed.extend(
        base.pairs[0]
            .versions
            .iter()
            .map(|properties| GraphMutation::UpsertEdge {
                uuid: properties["uuid"].as_str().unwrap().parse().unwrap(),
                source_chain_id: source,
                target_chain_id: target,
                properties: properties.clone(),
            }),
    );
    graph.apply_mutations(&org, &seed).await.unwrap();
    let actual = live_schedule_baseline(&graph, &org, source, target).await;
    let original_future = actual.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == future.uuid.to_string())
        .unwrap()
        .clone();
    incoming.description = "inserted middle interval".into();
    let middle_uuid = incoming.uuid;
    let (plan, _) = plan_relationships(&batch(
        vec![incoming.clone()],
        vec![capture.clone()],
        actual,
    ))
    .unwrap();
    let mut stale = commit_schedule_plan(&graph, &org, plan, capture.captured_at).await;
    stale.batch.index = 1;
    assert!(matches!(
        graph.commit_batch(&stale).await,
        Err(BackendError::Conflict(_))
    ));
    let after_insert = live_schedule_baseline(&graph, &org, source, target).await;
    let middle = after_insert.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == middle_uuid.to_string())
        .unwrap();
    assert_eq!(middle["version"], 3);
    assert_eq!(middle["previous_version_uuid"], future.uuid.to_string());
    assert_eq!(middle["valid_to"], future.valid_from.to_rfc3339());
    assert_eq!(
        after_insert.pairs[0]
            .versions
            .iter()
            .find(|props| props["uuid"] == future.uuid.to_string())
            .unwrap(),
        &original_future
    );

    let replacement_capture = snapshot(Uuid::new_v4(), capture.captured_at + Duration::hours(1));
    let mut replacement = edge(
        source,
        target,
        "CALLS",
        "replacement schedule",
        replacement_capture.uuid,
        future.valid_from,
    );
    replacement.chain_id = future.chain_id;
    let replacement_uuid = replacement.uuid;
    let mut command = cancel_command(&future, &replacement_capture);
    command.action = RelationshipDirectiveAction::Replace {
        replacement_edge_uuid: replacement.uuid,
    };
    let mut work = batch(
        vec![replacement],
        vec![replacement_capture.clone()],
        after_insert,
    );
    work.relationship_directives = Arc::new(vec![command]);
    let (plan, _) = plan_relationships(&work).unwrap();
    commit_schedule_plan(&graph, &org, plan, replacement_capture.captured_at).await;
    let after_replace = live_schedule_baseline(&graph, &org, source, target).await;
    let cancelled = after_replace.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == future.uuid.to_string())
        .unwrap();
    assert_eq!(
        cancelled["cancelled_at"],
        replacement_capture.captured_at.to_rfc3339()
    );
    assert_eq!(cancelled["valid_from"], original_future["valid_from"]);
    let next = after_replace.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == replacement_uuid.to_string())
        .unwrap();
    assert_eq!(next["version"], 4);
    assert_eq!(next["previous_version_uuid"], middle_uuid.to_string());
    let next = RelationshipTimeline::from_pair(&after_replace.pairs[0])
        .unwrap()
        .chains[&future.chain_id]
        .versions
        .iter()
        .find(|version| version.relationship.uuid == replacement_uuid)
        .unwrap()
        .relationship
        .clone();
    let cancellation_capture = snapshot(Uuid::new_v4(), capture.captured_at + Duration::hours(2));
    let mut work = batch(vec![], vec![cancellation_capture.clone()], after_replace);
    work.relationship_directives = Arc::new(vec![cancel_command(&next, &cancellation_capture)]);
    let (plan, _) = plan_relationships(&work).unwrap();
    commit_schedule_plan(&graph, &org, plan, cancellation_capture.captured_at).await;
    let final_state = live_schedule_baseline(&graph, &org, source, target).await;
    assert_eq!(final_state.pairs[0].versions.len(), 4);
    assert!(final_state.pairs[0].live.is_empty());
    let middle = final_state.pairs[0]
        .versions
        .iter()
        .find(|props| props["uuid"] == middle_uuid.to_string())
        .unwrap();
    assert_eq!(
        middle["valid_to"],
        future.valid_from.to_rfc3339(),
        "cancellation never extends its predecessor"
    );
}

#[test]
fn earlier_start_observation_cannot_cover_cancelled_schedule_at_same_capture() {
    let (_, future, mut base, mut incoming, capture) = scheduled_fixture();
    base.pairs[0]
        .versions
        .retain(|properties| properties["uuid"] == future.uuid.to_string());
    incoming.description = future.description.clone();
    let mut combined = batch(vec![incoming], vec![capture.clone()], base);
    combined.relationship_directives = Arc::new(vec![cancel_command(&future, &capture)]);
    assert!(plan_relationships(&combined)
        .unwrap_err()
        .to_string()
        .contains("cancellation at the same capture"));
}

#[test]
fn adjacent_interval_can_follow_finite_revision_regardless_of_capture_time() {
    let at = Utc::now();
    let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
    let capture = snapshot(Uuid::new_v4(), at);
    let mut finite = stored(
        source,
        target,
        "CALLS",
        "first schedule",
        at - Duration::hours(1),
        Some(scope()),
    );
    finite.valid_from = at + Duration::days(1);
    finite.ended_at = Some(at + Duration::days(2));
    let mut base = baseline(&[(source, target)], vec![], &[]);
    base.pairs[0].versions.push(raw(&finite));
    let incoming = edge(
        source,
        target,
        "CALLS",
        "next schedule",
        capture.uuid,
        finite.ended_at.unwrap(),
    );
    for captured_at in [at, finite.ended_at.unwrap() + Duration::days(1)] {
        let mut capture = capture.clone();
        capture.captured_at = captured_at;
        let (plan, _) =
            plan_relationships(&batch(vec![incoming.clone()], vec![capture], base.clone()))
                .unwrap();
        assert!(closures(&plan).is_empty());
        let properties = plan
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                GraphMutation::UpsertEdge { properties, .. } => Some(properties),
                _ => None,
            })
            .unwrap();
        assert_eq!(properties["version"], finite.version + 1);
        assert_eq!(properties["previous_version_uuid"], finite.uuid.to_string());
    }
}

#[test]
fn replacement_of_older_pending_revision_allocates_after_inserted_finite_head() {
    let (_, future, base, mut middle, capture) = scheduled_fixture();
    middle.description = "middle".into();
    let middle_uuid = middle.uuid;
    let (plan, _) =
        plan_relationships(&batch(vec![middle], vec![capture.clone()], base.clone())).unwrap();
    let after_insert = apply_plan_to_baseline(base, &plan);
    let later_capture = snapshot(Uuid::new_v4(), capture.captured_at + Duration::hours(1));
    let replacement = edge(
        future.source_chain_id,
        future.target_chain_id,
        "CALLS",
        "replacement",
        later_capture.uuid,
        future.valid_from,
    );
    let mut command = cancel_command(&future, &later_capture);
    command.action = RelationshipDirectiveAction::Replace {
        replacement_edge_uuid: replacement.uuid,
    };
    let mut work = batch(vec![replacement], vec![later_capture], after_insert);
    work.relationship_directives = Arc::new(vec![command]);
    let (plan, _) = plan_relationships(&work).unwrap();
    let properties = plan
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["version"], 4);
    assert_eq!(properties["previous_version_uuid"], middle_uuid.to_string());
}

#[test]
fn explicit_finite_backfill_fills_historical_gap_without_touching_surrounding_versions() {
    let (mut previous, future, mut base, mut incoming, mut capture) = scheduled_fixture();
    previous.ended_at = Some(previous.valid_from + Duration::hours(1));
    base.pairs[0].versions[1] = raw(&previous);
    capture.captured_at = future.valid_from + Duration::days(1);
    incoming.valid_from = previous.ended_at.unwrap() + Duration::hours(1);
    incoming.valid_to = Some(incoming.valid_from + Duration::hours(1));
    incoming.description = "historical gap".into();
    let (plan, _) =
        plan_relationships(&batch(vec![incoming.clone()], vec![capture], base)).unwrap();
    assert!(closures(&plan).is_empty());
    assert_eq!(plan.mutations.len(), 1);
    let properties = plan
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["version"], 3);
    assert_eq!(properties["valid_from"], incoming.valid_from.to_rfc3339());
    assert_eq!(
        properties["valid_to"],
        incoming.valid_to.unwrap().to_rfc3339()
    );
    assert_eq!(properties["previous_version_uuid"], future.uuid.to_string());
}

#[test]
fn resolved_end_only_closes_exact_version_including_zero_length_without_new_revision() {
    use kg_core::models::relationship_time::*;
    for zero_length in [false, true] {
        let (current, _, base, mut incoming, mut capture) = scheduled_fixture();
        capture.captured_at += Duration::days(10);
        incoming.valid_from = current.valid_from;
        incoming.valid_to = Some(if zero_length {
            current.valid_from
        } else {
            current.valid_from + Duration::days(1)
        });
        incoming.last_seen_at = Some(capture.captured_at);
        incoming.confidence = current.confidence;
        incoming.time_evidence = Some(RelationshipTimeEvidence {
            resolved_target: Some(kg_core::models::RelationshipTarget::StoredVersion {
                uuid: current.uuid,
            }),
            snapshot_id: capture.uuid,
            captured_at: capture.captured_at,
            outcome: RelationshipTimeOutcome::Inferred,
            start: None,
            end: Some(RelationshipTimeBound {
                at: incoming.valid_to.unwrap(),
                precision: TimePrecision::Instant,
                basis: TimeBasis::Absolute,
                quote: Some("terminated at this time".into()),
            }),
        });
        let (plan, _) =
            plan_relationships(&batch(vec![incoming.clone()], vec![capture], base)).unwrap();
        assert_eq!(plan.counts.edges_invalidated, 1);
        assert!(!plan
            .mutations
            .iter()
            .any(|mutation| matches!(mutation, GraphMutation::UpsertEdge { .. })));
        assert!(plan.mutations.iter().any(|mutation| matches!(mutation,
            GraphMutation::UpdateEdge { uuid, properties } if *uuid == current.uuid
                && properties.get("valid_to") == Some(&serde_json::json!(incoming.valid_to.unwrap().to_rfc3339()))
                && properties.contains_key("time_evidence"))));
    }
}

#[test]
fn end_only_closure_does_not_reconcile_other_cardinality_targets() {
    use kg_core::models::relationship_time::*;
    let at = Utc::now();
    let (source, target, other_target) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let current = stored(
        source,
        target,
        "RUNS_ON",
        "first",
        at - Duration::days(2),
        Some(scope()),
    );
    let other = stored(
        source,
        other_target,
        "RUNS_ON",
        "other",
        at - Duration::days(2),
        Some(scope()),
    );
    let capture = snapshot(Uuid::new_v4(), at);
    let mut incoming = edge(
        source,
        target,
        "RUNS_ON",
        "first",
        capture.uuid,
        current.valid_from,
    );
    incoming.confidence = current.confidence;
    incoming.valid_to = Some(at - Duration::days(1));
    incoming.time_evidence = Some(RelationshipTimeEvidence {
        resolved_target: Some(kg_core::models::RelationshipTarget::StoredVersion {
            uuid: current.uuid,
        }),
        snapshot_id: capture.uuid,
        captured_at: capture.captured_at,
        outcome: RelationshipTimeOutcome::Inferred,
        start: None,
        end: Some(RelationshipTimeBound {
            at: incoming.valid_to.unwrap(),
            precision: TimePrecision::Instant,
            basis: TimeBasis::Absolute,
            quote: Some("stopped running on first".into()),
        }),
    });
    let base = baseline(
        &[(source, target)],
        vec![current.clone(), other.clone()],
        &[(source, "RUNS_ON")],
    );
    let (plan, _) = plan_relationships(&batch(vec![incoming], vec![capture], base)).unwrap();
    assert_eq!(closures(&plan), vec![current.uuid]);
    assert!(!plan.mutations.iter().any(
        |mutation| matches!(mutation, GraphMutation::UpdateEdge { uuid, .. } if *uuid == other.uuid)
    ));
}

pub(super) fn termination_evidence(
    target: kg_core::models::RelationshipTarget,
    capture: &SnapshotNode,
    end: DateTime<Utc>,
) -> kg_core::models::RelationshipTimeEvidence {
    use kg_core::models::relationship_time::*;
    RelationshipTimeEvidence {
        resolved_target: Some(target),
        snapshot_id: capture.uuid,
        captured_at: capture.captured_at,
        outcome: RelationshipTimeOutcome::Inferred,
        start: None,
        end: Some(RelationshipTimeBound {
            at: end,
            precision: TimePrecision::Instant,
            basis: TimeBasis::Absolute,
            quote: Some("explicit ending".into()),
        }),
    }
}

#[test]
fn prior_observation_termination_uses_created_reused_or_versioned_actual_uuid() {
    use kg_core::models::RelationshipTarget;
    for mode in 0..3 {
        let at = Utc::now();
        let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
        let prior = stored(
            source,
            target,
            "CALLS",
            "calls",
            at - Duration::days(2),
            Some(scope()),
        );
        let first_capture = snapshot(Uuid::new_v4(), at);
        let second_capture = snapshot(Uuid::new_v4(), at + Duration::days(2));
        let mut first = edge(
            source,
            target,
            "CALLS",
            if mode == 2 { "updated calls" } else { "calls" },
            first_capture.uuid,
            at,
        );
        first.confidence = prior.confidence;
        let mut ending = first.clone();
        ending.uuid = Uuid::new_v4();
        ending.last_seen_snapshot_id = Some(second_capture.uuid);
        ending.last_seen_at = Some(second_capture.captured_at);
        ending.valid_to = Some(at + Duration::days(1));
        ending.time_evidence = Some(termination_evidence(
            RelationshipTarget::PriorObservation {
                observation_uuid: first.uuid,
            },
            &second_capture,
            ending.valid_to.unwrap(),
        ));
        let base = baseline(
            &[(source, target)],
            if mode == 0 {
                vec![]
            } else {
                vec![prior.clone()]
            },
            &[],
        );
        let (plan, _) = plan_relationships(&batch(
            vec![ending, first.clone()],
            vec![second_capture, first_capture],
            base,
        ))
        .unwrap();
        let actual = if mode == 1 { prior.uuid } else { first.uuid };
        assert!(plan.mutations.iter().any(|mutation| matches!(mutation,
            GraphMutation::UpdateEdge { uuid, properties } if *uuid == actual && properties.get("valid_to") == Some(&serde_json::json!((at + Duration::days(1)).to_rfc3339())))));
        let upserts = plan
            .mutations
            .iter()
            .filter(|m| matches!(m, GraphMutation::UpsertEdge { .. }))
            .count();
        assert_eq!(upserts, usize::from(mode != 1));
    }
}

#[test]
fn exact_pending_termination_may_set_end_without_replacing_start_or_meaning() {
    use kg_core::models::RelationshipTarget;
    let (_, future, base, mut incoming, capture) = scheduled_fixture();
    incoming.valid_from = future.valid_from;
    incoming.valid_to = Some(future.valid_from + Duration::days(1));
    incoming.description = future.description.clone();
    incoming.confidence = future.confidence;
    incoming.time_evidence = Some(termination_evidence(
        RelationshipTarget::StoredVersion { uuid: future.uuid },
        &capture,
        incoming.valid_to.unwrap(),
    ));
    let (plan, _) = plan_relationships(&batch(vec![incoming], vec![capture], base)).unwrap();
    assert_eq!(closures(&plan), vec![future.uuid]);
    assert!(!plan
        .mutations
        .iter()
        .any(|m| matches!(m, GraphMutation::UpsertEdge { .. })));
}

#[test]
fn ignored_historical_repeat_can_anchor_later_supported_termination() {
    use kg_core::models::RelationshipTarget;
    let at = Utc::now() - Duration::days(15);
    let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
    let mut old = stored(source, target, "CALLS", "calls", at, Some(scope()));
    old.ended_at = Some(at + Duration::days(10));
    old.latest_observation = Some(at + Duration::days(11));
    let first_capture = snapshot(Uuid::new_v4(), at + Duration::days(12));
    let end_capture = snapshot(Uuid::new_v4(), at + Duration::days(13));
    let mut repeat = edge(
        source,
        target,
        "CALLS",
        "calls",
        first_capture.uuid,
        at + Duration::days(5),
    );
    repeat.confidence = old.confidence;
    let mut ending = repeat.clone();
    ending.uuid = Uuid::new_v4();
    ending.last_seen_snapshot_id = Some(end_capture.uuid);
    ending.valid_from = at;
    ending.valid_to = Some(at + Duration::days(8));
    ending.time_evidence = Some(termination_evidence(
        RelationshipTarget::PriorObservation {
            observation_uuid: repeat.uuid,
        },
        &end_capture,
        ending.valid_to.unwrap(),
    ));
    let base = baseline(&[(source, target)], vec![old.clone()], &[]);
    let (plan, _) = plan_relationships(&batch(
        vec![repeat, ending],
        vec![first_capture, end_capture],
        base,
    ))
    .unwrap();
    assert_eq!(closures(&plan), vec![old.uuid]);
    assert!(!plan
        .mutations
        .iter()
        .any(|m| matches!(m, GraphMutation::UpsertEdge { .. })));
}
