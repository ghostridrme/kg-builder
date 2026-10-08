use super::*;

pub(super) fn semantic_batch(
    candidate: &StoredRelationship,
    incoming: EntityEdge,
    captured: DateTime<Utc>,
) -> RelationshipBatch {
    use kg_core::runtime::stage_output::{RelationshipAssessment, RelationshipAssessmentDecision};
    let source = incoming.source_chain_id;
    let snapshot_id = incoming.last_seen_snapshot_id.unwrap();
    let observation_uuid = incoming.uuid;
    let properties = raw(candidate);
    let mut batch = batch(
        vec![incoming.clone()],
        vec![snapshot(snapshot_id, captured)],
        baseline(&[(source, incoming.target_chain_id)], vec![], &[]),
    );
    // Cross-target candidate history is independent of the incoming pair baseline.
    if incoming.source_chain_id == candidate.source_chain_id
        && incoming.target_chain_id == candidate.target_chain_id
    {
        Arc::make_mut(&mut batch.baseline).pairs[0]
            .versions
            .push(properties.clone());
    }
    batch.contradiction_timelines.insert(
        if source == candidate.source_chain_id {
            source
        } else {
            incoming.target_chain_id
        },
        vec![
            kg_core::traits::relationship_timeline::IncidentVersionState {
                source_chain_id: candidate.source_chain_id,
                target_chain_id: candidate.target_chain_id,
                properties,
            },
        ],
    );
    batch.relationship_assessments = Arc::new(vec![RelationshipAssessment {
        observation_uuid,
        candidate: kg_core::models::RelationshipTarget::StoredVersion {
            uuid: candidate.uuid,
        },
        protected_properties: vec![],
        decision: RelationshipAssessmentDecision::Contradiction,
    }]);
    batch
}

#[test]
fn semantic_contradiction_closes_older_fact_with_snapshot_and_phantom_fence() {
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let old = stored(
        source,
        target,
        "ALLOWS",
        "allows plaintext",
        at - Duration::hours(2),
        Some(scope()),
    );
    let incoming = edge(
        source,
        target,
        "DENIES",
        "denies plaintext",
        Uuid::new_v4(),
        at,
    );
    let id = incoming.uuid;
    let snapshot = incoming.last_seen_snapshot_id.unwrap();
    let (plan, _) = plan_relationships(&semantic_batch(&old, incoming, at)).unwrap();
    assert_eq!(closures(&plan), vec![old.uuid]);
    assert!(plan.preconditions.iter().any(
        |p| matches!(p, Precondition::IncidentTimelineIs { chain_id, .. } if *chain_id == source)
    ));
    let evidence = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpdateEdge { uuid, properties } if *uuid == old.uuid => {
                properties.get("closure_evidence")
            }
            _ => None,
        })
        .unwrap();
    let evidence: serde_json::Value = serde_json::from_str(evidence.as_str().unwrap()).unwrap();
    assert_eq!(evidence["snapshot_id"], snapshot.to_string());
    assert_eq!(evidence["counterpart_version_uuid"], id.to_string());
}

#[test]
fn semantic_historical_fact_is_bounded_by_later_fact_without_closing_it() {
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let later = stored(
        source,
        target,
        "DENIES",
        "denies plaintext",
        at - Duration::hours(1),
        Some(scope()),
    );
    let mut incoming = edge(
        source,
        target,
        "ALLOWS",
        "allowed plaintext",
        Uuid::new_v4(),
        at,
    );
    incoming.valid_from = at - Duration::hours(3);
    let id = incoming.uuid;
    let (plan, _) = plan_relationships(&semantic_batch(&later, incoming, at)).unwrap();
    assert!(closures(&plan).is_empty());
    let properties = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge {
                uuid, properties, ..
            } if *uuid == id => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["valid_to"], later.valid_from.to_rfc3339());
}

#[test]
fn semantic_compatible_unsure_and_disjoint_decisions_never_close_facts() {
    use kg_core::runtime::stage_output::RelationshipAssessmentDecision;
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let old = stored(
        source,
        target,
        "ALLOWS",
        "allows plaintext",
        at - Duration::hours(2),
        Some(scope()),
    );
    for decision in [
        RelationshipAssessmentDecision::Compatible,
        RelationshipAssessmentDecision::Unsure,
    ] {
        let incoming = edge(
            source,
            target,
            "DENIES",
            "denies plaintext",
            Uuid::new_v4(),
            at,
        );
        let mut batch = semantic_batch(&old, incoming, at);
        Arc::make_mut(&mut batch.relationship_assessments)[0].decision = decision;
        assert!(closures(&plan_relationships(&batch).unwrap().0).is_empty());
    }
    let mut ended = old;
    ended.ended_at = Some(at - Duration::hours(1));
    let incoming = edge(
        source,
        target,
        "DENIES",
        "denies plaintext",
        Uuid::new_v4(),
        at,
    );
    assert!(closures(
        &plan_relationships(&semantic_batch(&ended, incoming, at))
            .unwrap()
            .0
    )
    .is_empty());
}

#[test]
fn semantic_same_time_and_stale_closures_fail_without_a_plan() {
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    for start in [at, at - Duration::hours(1)] {
        let mut old = stored(
            source,
            target,
            "ALLOWS",
            "allows plaintext",
            start,
            Some(scope()),
        );
        old.latest_observation = Some(at);
        let incoming = edge(
            source,
            target,
            "DENIES",
            "denies plaintext",
            Uuid::new_v4(),
            at,
        );
        assert!(plan_relationships(&semantic_batch(&old, incoming, at)).is_err());
    }
}

#[test]
fn semantic_model_decisions_cannot_override_authority_or_identifying_properties() {
    use kg_core::models::{PropertyValue, RelationshipOrigin};
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    for violation in 0..4 {
        let mut old = stored(
            source,
            target,
            "ALLOWS",
            "allows plaintext",
            at - Duration::hours(2),
            Some(scope()),
        );
        let mut incoming = edge(
            source,
            target,
            "DENIES",
            "denies plaintext",
            Uuid::new_v4(),
            at,
        );
        match violation {
            0 => old.scope = Some(other_scope()),
            1 => old.origin = RelationshipOrigin::Declared,
            2 => incoming.origin = RelationshipOrigin::Reference,
            _ => {
                old.all_properties
                    .insert("port".into(), PropertyValue::Integer(80));
                incoming
                    .all_properties
                    .insert("port".into(), PropertyValue::Integer(443));
            }
        }
        let mut batch = semantic_batch(&old, incoming, at);
        if violation == 3 {
            Arc::make_mut(&mut batch.relationship_assessments)[0].protected_properties =
                vec!["port".into()];
        }
        assert!(plan_relationships(&batch).is_err(), "violation {violation}");
    }
}

#[test]
fn semantic_prior_observation_uses_actual_reused_version_for_audit() {
    use kg_core::models::RelationshipTarget;
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let old = stored(
        source,
        target,
        "ALLOWS",
        "allows plaintext",
        at - Duration::hours(3),
        Some(scope()),
    );
    let mut prior = edge(
        source,
        target,
        "ALLOWS",
        "allows plaintext",
        Uuid::new_v4(),
        at - Duration::hours(1),
    );
    prior.valid_from = old.valid_from;
    let incoming = edge(
        source,
        target,
        "DENIES",
        "denies plaintext",
        Uuid::new_v4(),
        at,
    );
    let mut batch = semantic_batch(&old, incoming, at);
    Arc::make_mut(&mut batch.relationship_assessments)[0].candidate =
        RelationshipTarget::PriorObservation {
            observation_uuid: prior.uuid,
        };
    Arc::make_mut(&mut batch.snapshot_nodes).push(snapshot(
        prior.last_seen_snapshot_id.unwrap(),
        prior.last_seen_at.unwrap(),
    ));
    Arc::make_mut(&mut batch.observed).push(prior);
    let (plan, _) = plan_relationships(&batch).unwrap();
    assert_eq!(closures(&plan), vec![old.uuid]);
}

#[test]
fn semantic_incoming_reobservation_keeps_canonical_start_and_bounds_its_history() {
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let canonical = stored(
        source,
        target,
        "ALLOWS",
        "allows plaintext",
        at - Duration::hours(3),
        Some(scope()),
    );
    let later = stored(
        source,
        target,
        "DENIES",
        "denies plaintext",
        at - Duration::hours(1),
        Some(scope()),
    );
    let incoming = edge(
        source,
        target,
        "ALLOWS",
        "allows plaintext",
        Uuid::new_v4(),
        at,
    );
    let mut batch = semantic_batch(&later, incoming, at);
    let raw = raw(&canonical);
    Arc::make_mut(&mut batch.baseline).pairs[0]
        .versions
        .push(raw.clone());
    batch
        .contradiction_timelines
        .get_mut(&source)
        .unwrap()
        .push(
            kg_core::traits::relationship_timeline::IncidentVersionState {
                source_chain_id: source,
                target_chain_id: target,
                properties: raw,
            },
        );
    let (plan, _) = plan_relationships(&batch).unwrap();
    assert_eq!(closures(&plan), vec![canonical.uuid]);
    assert!(plan.mutations.iter().any(|mutation| matches!(mutation,
        GraphMutation::UpdateEdge { uuid, properties } if *uuid == canonical.uuid && properties.get("valid_to") == Some(&serde_json::json!(later.valid_from.to_rfc3339()))
    )));
    assert!(upserts(&plan).is_empty());
}

#[test]
fn semantic_same_snapshot_dated_history_is_independent_of_observation_order() {
    use kg_core::models::{RelationshipTarget, RelationshipTimeEvidence};
    use kg_core::runtime::stage_output::{RelationshipAssessment, RelationshipAssessmentDecision};
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    for later_first in [false, true] {
        for evidence_case in 0..3 {
            let snapshot_id = Uuid::new_v4();
            let mut old = edge(
                source,
                target,
                "ALLOWS",
                "allows plaintext",
                snapshot_id,
                at,
            );
            old.valid_from = at - Duration::hours(3);
            let mut new = edge(
                source,
                target,
                "DENIES",
                "denies plaintext",
                snapshot_id,
                at,
            );
            new.valid_from = at - Duration::hours(1);
            if evidence_case == 2 {
                new.last_seen_snapshot_id = Some(Uuid::new_v4());
            }
            if evidence_case != 0 {
                old.time_evidence = Some(RelationshipTimeEvidence::explicit(
                    snapshot_id,
                    at,
                    old.valid_from,
                    None,
                ));
                new.time_evidence = Some(RelationshipTimeEvidence::explicit(
                    new.last_seen_snapshot_id.unwrap(),
                    at,
                    new.valid_from,
                    None,
                ));
            }
            if later_first {
                new.confidence = 0.95;
            } else {
                old.confidence = 0.95;
            }
            let (incoming, prior) = if later_first {
                (&old, &new)
            } else {
                (&new, &old)
            };
            let assessment = RelationshipAssessment {
                observation_uuid: incoming.uuid,
                candidate: RelationshipTarget::PriorObservation {
                    observation_uuid: prior.uuid,
                },
                protected_properties: vec![],
                decision: RelationshipAssessmentDecision::Contradiction,
            };
            let mut snapshots = vec![snapshot(snapshot_id, at)];
            if evidence_case == 2 {
                snapshots.push(snapshot(new.last_seen_snapshot_id.unwrap(), at));
            }
            let old_id = old.uuid;
            let new_id = new.uuid;
            let mut batch = batch(
                vec![old, new],
                snapshots,
                baseline(&[(source, target)], vec![], &[]),
            );
            batch.contradiction_timelines.insert(source, vec![]);
            batch.relationship_assessments = Arc::new(vec![assessment]);
            let result = plan_relationships(&batch);
            if evidence_case != 1 {
                assert!(result.is_err());
                continue;
            }
            let (plan, _) = result.unwrap();
            assert!(!closures(&plan).contains(&new_id));
            assert!(
                plan.mutations.iter().any(|mutation| match mutation {
                    GraphMutation::UpdateEdge { uuid, properties }
                    | GraphMutation::UpsertEdge {
                        uuid, properties, ..
                    } =>
                        *uuid == old_id
                            && (properties.get("valid_to")
                                == Some(&serde_json::json!((at - Duration::hours(1)).to_rfc3339()))
                                || properties.get("invalid_at")
                                    == Some(&serde_json::json!(
                                        (at - Duration::hours(1)).to_rfc3339()
                                    ))),
                    _ => false,
                })
            );
        }
    }
}

#[test]
fn shared_target_contradiction_requires_the_target_incident_fence() {
    let at = Utc::now();
    let target = Uuid::new_v4();
    let old = stored(
        Uuid::new_v4(),
        target,
        "OWNS",
        "Alice exclusively owns resource",
        at - Duration::days(1),
        Some(scope()),
    );
    let incoming = edge(
        Uuid::new_v4(),
        target,
        "OWNS",
        "Bob exclusively owns resource",
        Uuid::new_v4(),
        at,
    );
    let source = incoming.source_chain_id;
    let mut batch = semantic_batch(&old, incoming, at);
    let (plan, _) = plan_relationships(&batch).unwrap();
    assert_eq!(closures(&plan), vec![old.uuid]);
    assert!(plan.preconditions.iter().any(
        |p| matches!(p, Precondition::IncidentTimelineIs { chain_id, .. } if *chain_id == target)
    ));
    batch.contradiction_timelines.remove(&target);
    batch.contradiction_timelines.insert(source, vec![]);
    assert!(plan_relationships(&batch).is_err());
}
