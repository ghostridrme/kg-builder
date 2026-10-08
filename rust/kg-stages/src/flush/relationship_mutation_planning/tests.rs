//! Relationship planning tests; `contradictions` and `schedule` hold the
//! semantic-contradiction and scheduled-interval scenarios.
use std::sync::Arc;

use chrono::Duration;
use kg_core::models::SnapshotNode;
use kg_core::models::{SnapshotDataType, SnapshotKind};
use kg_core::runtime::stage_output::{PairBaseline, RelationBaseline, RelationshipBaseline};

use super::*;

mod contradictions;

fn scope() -> ConnectorScope {
    ConnectorScope {
        namespace: "prod".into(),
        source: "aws".into(),
    }
}

fn other_scope() -> ConnectorScope {
    ConnectorScope {
        namespace: "prod".into(),
        source: "cmdb".into(),
    }
}

fn snapshot(uuid: Uuid, captured_at: DateTime<Utc>) -> SnapshotNode {
    SnapshotNode {
        uuid,
        org_id: "org".into(),
        namespace: "prod".into(),
        name: "s".into(),
        source_description: None,
        data_type: SnapshotDataType::Entities,
        snapshot_kind: SnapshotKind::Incremental,
        sync_generation: None,
        complete: false,
        collection: None,
        source: "aws".into(),
        content: None,
        captured_at,
        entities: vec![],
        entity_edges: vec![],
        labels: vec![],
        tags: Default::default(),
        created_at: captured_at,
    }
}

// Fixtures represent identities already selected by resolution, independently of version UUIDs.
fn test_lineage(source: Uuid, target: Uuid, name: &str) -> Uuid {
    Uuid::new_v5(&source, format!("{target}/{name}").as_bytes())
}

fn test_cardinality(source: Uuid, name: &str) -> Option<String> {
    // These fixtures explicitly configure placement slots; production never infers cardinality from verbs.
    matches!(
        name,
        "RUNS_ON" | "DEPLOYED_IN" | "HOSTED_IN" | "LOCATED_IN" | "SCHEDULED_ON"
    )
    .then(|| format!("{source}/{name}"))
}

fn edge(
    source: Uuid,
    target: Uuid,
    name: &str,
    description: &str,
    snapshot: Uuid,
    at: DateTime<Utc>,
) -> EntityEdge {
    EntityEdge {
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        time_evidence: None,
        producer_source: "aws".into(),
        chain_id: test_lineage(source, target, name),
        identity_hash: None,
        cardinality_key: test_cardinality(source, name),
        origin: kg_core::models::RelationshipOrigin::Fact,
        uuid: Uuid::new_v4(),
        org_id: "org".into(),
        source_chain_id: source,
        target_chain_id: target,
        name: name.into(),
        identity_name: None,
        description: description.into(),
        all_properties: Default::default(),
        discovered_by: Some("test".into()),
        resolved_by: None,
        source_property: None,
        target_identity_field: None,
        reference_evidence: None,
        confidence: 0.9,
        justification: None,
        first_seen_snapshot_id: Some(snapshot),
        last_seen_snapshot_id: Some(snapshot),
        last_seen_at: Some(at),
        sync_generation: None,
        valid_from: at,
        valid_to: None,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        created_at: at,
    }
}

fn stored(
    source: Uuid,
    target: Uuid,
    name: &str,
    description: &str,
    latest: DateTime<Utc>,
    scope: Option<ConnectorScope>,
) -> StoredRelationship {
    StoredRelationship {
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        time_evidence: None,
        valid_from: latest,
        chain_id: test_lineage(source, target, name),
        identity_hash: None,
        cardinality_key: test_cardinality(source, name),
        reference_evidence: None,
        origin: kg_core::models::RelationshipOrigin::Fact,
        all_properties: Default::default(),
        first_seen_snapshot_id: None,
        uuid: Uuid::new_v4(),
        source_chain_id: source,
        target_chain_id: target,
        name: name.into(),
        version: 2,
        confidence: 0.9,
        description: description.into(),
        latest_observation: Some(latest),
        ended_at: None,
        scope,
    }
}

fn batch(
    observed: Vec<EntityEdge>,
    snapshots: Vec<SnapshotNode>,
    baseline: RelationshipBaseline,
) -> RelationshipBatch {
    RelationshipBatch {
        relationship_assessments: Default::default(),
        contradiction_timelines: Default::default(),
        relationship_directives: Default::default(),
        reference_report: Default::default(),
        snapshot_nodes: Arc::new(snapshots),
        observed: Arc::new(observed),
        baseline: Arc::new(baseline),
    }
}

fn raw(stored: &StoredRelationship) -> GraphProperties {
    let scope = stored.scope.clone().unwrap_or_else(scope);
    let snapshot = stored.first_seen_snapshot_id.unwrap_or_else(Uuid::new_v4);
    let mut edge = edge(
        stored.source_chain_id,
        stored.target_chain_id,
        &stored.name,
        &stored.description,
        snapshot,
        stored.valid_from,
    );
    edge.uuid = stored.uuid;
    edge.chain_id = stored.chain_id;
    edge.identity_hash = stored.identity_hash.clone();
    edge.cardinality_key = stored.cardinality_key.clone();
    edge.reference_evidence = stored.reference_evidence.clone();
    edge.origin = stored.origin;
    edge.confidence = stored.confidence;
    edge.all_properties = stored.all_properties.clone();
    edge.valid_to = stored.ended_at;
    edge.cancelled_at = stored.cancelled_at;
    edge.cancellation_snapshot_id = stored.cancellation_snapshot_id;
    edge.cancellation_context = stored.cancellation_context.clone();
    edge.first_seen_snapshot_id = stored.first_seen_snapshot_id;
    edge.producer_source = scope.source.clone();
    let mut props = edge_properties(&edge, stored.version, None, &scope);
    props.insert("uuid".into(), stored.uuid.to_string().into());
    if let Some(at) = stored.latest_observation {
        props.insert("last_seen_at".into(), at.to_rfc3339().into());
    }
    kg_core::traits::relationship_timeline::state(&props)
}

/// A baseline for `pairs` with stored relationships, plus the live
/// set of every `relations` entry (its stored relationships of that name).
fn baseline(
    pairs: &[(Uuid, Uuid)],
    stored: Vec<StoredRelationship>,
    relations: &[(Uuid, &str)],
) -> RelationshipBaseline {
    RelationshipBaseline {
        ended: vec![],
        pairs: pairs
            .iter()
            .map(|(s, t)| PairBaseline {
                versions: stored
                    .iter()
                    .filter(|r| r.source_chain_id == *s && r.target_chain_id == *t)
                    .map(raw)
                    .collect(),
                source_chain_id: *s,
                target_chain_id: *t,
                live: stored
                    .iter()
                    .filter(|r| r.source_chain_id == *s && r.target_chain_id == *t)
                    .cloned()
                    .collect(),
            })
            .collect(),
        relations: relations
            .iter()
            .map(|(source, name)| RelationBaseline {
                versions: stored
                    .iter()
                    .filter(|r| r.source_chain_id == *source && r.name == *name)
                    .map(|r| kg_core::traits::relationship_timeline::VersionState {
                        target_chain_id: r.target_chain_id,
                        properties: raw(r),
                    })
                    .collect(),
                source_chain_id: *source,
                name: (*name).into(),
                live: stored
                    .iter()
                    .filter(|r| r.source_chain_id == *source && r.name == *name)
                    .cloned()
                    .collect(),
            })
            .collect(),
        reference_owners: vec![],
        orphan_targets: vec![],
    }
}

fn closures(plan: &Plan) -> Vec<Uuid> {
    plan.mutations
        .iter()
        .filter_map(|m| match m {
            GraphMutation::UpdateEdge { uuid, properties }
                if properties.get("invalid_at").is_some() =>
            {
                Some(*uuid)
            }
            _ => None,
        })
        .collect()
}

fn upserts(plan: &Plan) -> Vec<(Uuid, Uuid, u32)> {
    plan.mutations
        .iter()
        .filter_map(|m| match m {
            GraphMutation::UpsertEdge {
                source_chain_id,
                target_chain_id,
                properties,
                ..
            } => Some((
                *source_chain_id,
                *target_chain_id,
                properties["version"].as_u64().unwrap() as u32,
            )),
            _ => None,
        })
        .collect()
}

fn scheduled_fixture() -> (
    StoredRelationship,
    StoredRelationship,
    RelationshipBaseline,
    EntityEdge,
    SnapshotNode,
) {
    let at: DateTime<Utc> = "2026-09-19T00:00:00Z".parse().unwrap();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let mut current = stored(
        source,
        target,
        "CALLS",
        "current",
        at - Duration::days(2),
        Some(scope()),
    );
    current.version = 1;
    current.ended_at = Some(at + Duration::days(2));
    let mut future = current.clone();
    future.uuid = Uuid::new_v4();
    future.version = 2;
    future.valid_from = current.ended_at.unwrap();
    future.ended_at = None;
    future.description = "future".into();
    future.latest_observation = Some(at - Duration::days(1));
    let mut base = baseline(&[(source, target)], vec![future.clone()], &[]);
    base.pairs[0].versions.push(raw(&current));
    let snapshot = snapshot(Uuid::new_v4(), at);
    let observation = edge(source, target, "CALLS", "current", snapshot.uuid, at);
    (current, future, base, observation, snapshot)
}

#[test]
fn successor_from_another_producer_preserves_lineage_ownership() {
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let prior = stored(
        source,
        target,
        "CALLS",
        "calls",
        at - Duration::days(1),
        Some(scope()),
    );
    let mut snapshot = snapshot(Uuid::new_v4(), at);
    snapshot.source = "logs".into();
    snapshot.namespace = "observations".into();
    let mut incoming = edge(source, target, "CALLS", "calls", snapshot.uuid, at);
    incoming.producer_source = "logs".into();
    incoming.confidence = 0.8;
    let base = baseline(&[(source, target)], vec![prior.clone()], &[]);
    let (plan, _) =
        plan_relationships(&batch(vec![incoming], vec![snapshot], base.clone())).unwrap();
    assert_eq!(plan.counts.edges_updated, 1);
    let mut pair = base.pairs[0].clone();
    pair.live.clear();
    for mutation in plan.mutations {
        match mutation {
            GraphMutation::UpdateEdge { uuid, properties } if uuid == prior.uuid => {
                pair.versions[0].extend(properties);
            }
            GraphMutation::UpsertEdge {
                uuid,
                mut properties,
                ..
            } => {
                assert_eq!(properties["producer_source"], scope().source);
                assert_eq!(properties["producer_namespace"], scope().namespace);
                assert_eq!(properties["confidence"], serde_json::json!(0.8_f32));
                properties.insert("uuid".into(), uuid.to_string().into());
                pair.versions
                    .push(kg_core::traits::relationship_timeline::state(&properties));
            }
            _ => {}
        }
    }
    let timeline = RelationshipTimeline::from_pair(&pair).unwrap();
    assert_eq!(
        timeline.chains[&prior.chain_id]
            .revision_head()
            .relationship
            .version,
        prior.version + 1
    );
}

#[test]
fn a_reference_edge_fences_every_read_version_it_was_confirmed_against() {
    // R1: the read set (selected target first, then rejected competitors) must
    // become `LatestVersionIs` preconditions, so a target or competitor that
    // changed after the read rejects the commit and the runner refreshes the
    // decision instead of persisting stale evidence.
    let at = Utc::now();
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let competitor = Uuid::new_v4();
    let target_version = Uuid::new_v4();
    let competitor_version = Uuid::new_v4();
    let snapshot = snapshot(Uuid::new_v4(), at);
    let mut incoming = edge(source, target, "REFERENCES", "ref", snapshot.uuid, at);
    incoming.reference_evidence = Some(kg_core::models::edges::ReferenceEvidence {
        component_paths: None,
        observing_chain_id: source,
        observing_namespace: "observations".into(),
        slot: "Server.subnet_id".into(),
        location: "Server.subnet_id".into(),
        target_key_group: vec!["subnet_id".into()],
        reference_tokens: vec!["s:subnet-1".into()],
        read_set: vec![
            kg_core::models::edges::ReadVersion {
                chain_id: target,
                version_uuid: target_version,
                version: 1,
                observed_at: None,
            },
            kg_core::models::edges::ReadVersion {
                chain_id: competitor,
                version_uuid: competitor_version,
                version: 3,
                observed_at: None,
            },
        ],
        decision: None,
    });
    let mut base = baseline(&[(source, target)], vec![], &[]);
    base.reference_owners
        .push(kg_core::runtime::stage_output::ReferenceOwnerBaseline {
            selector: kg_core::runtime::stage_output::ReferenceOwnerSelector {
                chain_id: source,
                namespace: "observations".into(),
                slot: "Server.subnet_id".into(),
            },
            versions: vec![],
            live: vec![],
        });
    let (plan, _) = plan_relationships(&batch(vec![incoming], vec![snapshot], base)).unwrap();
    let fenced: HashSet<(Uuid, Uuid, u32)> = plan
        .preconditions
        .iter()
        .filter_map(|precondition| match precondition {
            Precondition::LatestVersionIs {
                chain_id,
                uuid,
                version,
            } => Some((*chain_id, *uuid, *version)),
            _ => None,
        })
        .collect();
    assert!(
        fenced.contains(&(target, target_version, 1)),
        "the selected target's read version must be fenced: {fenced:?}"
    );
    assert!(
        fenced.contains(&(competitor, competitor_version, 3)),
        "a rejected competitor's read version must be fenced too: {fenced:?}"
    );
}

#[test]
fn scheduled_predecessor_repeat_only_updates_its_observation() {
    let (current, future, base, observation, snapshot) = scheduled_fixture();
    let (plan, embeddings) =
        plan_relationships(&batch(vec![observation], vec![snapshot], base)).unwrap();
    assert_eq!(plan.counts.edges_unchanged, 1);
    assert_eq!(plan.mutations.len(), 1);
    assert!(
        matches!(&plan.mutations[0], GraphMutation::UpdateEdge { uuid, properties }
        if *uuid == current.uuid && !properties.contains_key("valid_to") && !properties.contains_key("is_latest"))
    );
    assert_eq!(embeddings.len(), 1);
    assert_eq!(embeddings[0].uuid, current.uuid);
    assert_ne!(embeddings[0].uuid, future.uuid);
    assert!(plan.preconditions.iter().any(|p| matches!(p, Precondition::RelationshipTimelineIs { versions, .. } if versions.len() == 2)));
    assert!(!plan.preconditions.iter().any(|p| matches!(
        p,
        Precondition::EdgeIsLatest { .. }
            | Precondition::EdgeHeadIs { .. }
            | Precondition::EdgeNotObservedAfter { .. }
    )));
}

#[test]
fn creation_head_after_open_tail_does_not_reuse_its_revision() {
    let (mut current, future, mut base, _, _) = scheduled_fixture();
    current.version = 3;
    base.pairs[0].versions[1] = raw(&current);
    base.ended.push(current);
    let at = future.valid_from + Duration::days(1);
    let snapshot = snapshot(Uuid::new_v4(), at);
    let mut observation = edge(
        future.source_chain_id,
        future.target_chain_id,
        "CALLS",
        "future",
        snapshot.uuid,
        at,
    );
    let (plan, _) = plan_relationships(&batch(
        vec![observation.clone()],
        vec![snapshot.clone()],
        base.clone(),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_unchanged, 1);
    assert!(
        matches!(&plan.mutations[0], GraphMutation::UpdateEdge { uuid, .. } if *uuid == future.uuid)
    );
    observation.description = "changed".into();
    let (plan, _) = plan_relationships(&batch(vec![observation], vec![snapshot], base)).unwrap();
    let props = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(props["version"], 4);
}

#[test]
fn historical_repeat_after_successor_activation_updates_original_interval() {
    let (current, future, base, mut observation, mut snapshot) = scheduled_fixture();
    snapshot.captured_at = future.valid_from + Duration::days(1);
    observation.valid_from = current.valid_from;
    let (plan, _) = plan_relationships(&batch(vec![observation], vec![snapshot], base)).unwrap();
    assert_eq!(plan.counts.edges_unchanged, 1);
    assert!(
        matches!(&plan.mutations[0], GraphMutation::UpdateEdge { uuid, .. } if *uuid == current.uuid)
    );
}

#[test]
fn scheduled_predecessor_amendment_preserves_future_successor() {
    let (current, future, base, mut observation, snapshot) = scheduled_fixture();
    observation.description = "changed".into();
    let (plan, _) = plan_relationships(&batch(vec![observation], vec![snapshot], base)).unwrap();
    let properties = plan
        .mutations
        .iter()
        .find_map(|m| match m {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["version"], 3);
    assert_eq!(properties["previous_version_uuid"], future.uuid.to_string());
    assert_eq!(properties["valid_to"], future.valid_from.to_rfc3339());
    assert_eq!(closures(&plan), vec![current.uuid]);
    assert!(!plan
        .mutations
        .iter()
        .any(|m| matches!(m, GraphMutation::UpdateEdge { uuid, .. } if *uuid == future.uuid)));
}

#[test]
fn stale_scheduled_predecessor_observation_does_not_change_provenance() {
    let (mut current, _, mut base, observation, snapshot) = scheduled_fixture();
    current.latest_observation = Some(snapshot.captured_at + Duration::hours(1));
    base.pairs[0].versions[1] = raw(&current);
    let (plan, embeddings) =
        plan_relationships(&batch(vec![observation], vec![snapshot], base)).unwrap();
    assert!(plan.mutations.is_empty());
    assert!(embeddings.is_empty());
}

#[test]
fn repeated_current_observations_preserve_pending_successor_in_one_batch() {
    let (current, _, base, observation, snapshot) = scheduled_fixture();
    let second = self::snapshot(Uuid::new_v4(), snapshot.captured_at + Duration::hours(1));
    let repeated = edge(
        current.source_chain_id,
        current.target_chain_id,
        "CALLS",
        "current",
        second.uuid,
        second.captured_at,
    );
    let (plan, _) = plan_relationships(&batch(
        vec![observation, repeated],
        vec![snapshot, second],
        base,
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_unchanged, 2);
    assert!(plan
        .mutations
        .iter()
        .all(|m| matches!(m, GraphMutation::UpdateEdge { uuid, .. } if *uuid == current.uuid)));
}

#[test]
fn relationship_properties_preserve_the_observation_producer_source() {
    let mut declared = edge(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "CALLS",
        "calls",
        Uuid::new_v4(),
        Utc::now(),
    );
    declared.producer_source = "github-actions".into();
    let properties = edge_properties(&declared, 1, None, &scope());
    assert_eq!(properties["producer_source"], "github-actions");
    assert_eq!(
        properties["last_seen_snapshot_id"],
        declared.last_seen_snapshot_id.unwrap().to_string()
    );
}

#[test]
fn cardinality_ownership_uses_entity_producer_and_snapshot_namespace() {
    let (source, old_target, new_target, snapshot_id) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let at = Utc::now();
    let prior = stored(
        source,
        old_target,
        "RUNS_ON",
        "old",
        at - Duration::seconds(1),
        Some(ConnectorScope {
            namespace: "observations".into(),
            source: "entity-producer".into(),
        }),
    );
    let mut incoming = edge(source, new_target, "RUNS_ON", "new", snapshot_id, at);
    incoming.producer_source = "entity-producer".into();
    let mut snapshot = snapshot(snapshot_id, at);
    snapshot.source = "outer-connector".into();
    snapshot.namespace = "observations".into();
    let (plan, _) = plan_relationships(&batch(
        vec![incoming],
        vec![snapshot],
        baseline(
            &[(source, new_target)],
            vec![prior.clone()],
            &[(source, "RUNS_ON")],
        ),
    ))
    .unwrap();
    assert_eq!(closures(&plan), vec![prior.uuid]);
    let properties = plan
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["producer_source"], "entity-producer");
    assert_eq!(properties["producer_namespace"], "observations");
}

#[test]
fn unchanged_observation_does_not_transfer_producer_ownership() {
    let (source, target, snapshot_id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let prior = stored(
        source,
        target,
        "CALLS",
        "calls",
        at - Duration::seconds(1),
        Some(other_scope()),
    );
    let mut incoming = edge(source, target, "CALLS", "calls", snapshot_id, at);
    incoming.producer_source = "another-producer".into();
    let (plan, _) = plan_relationships(&batch(
        vec![incoming],
        vec![snapshot(snapshot_id, at)],
        baseline(&[(source, target)], vec![prior], &[]),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_unchanged, 1);
    for mutation in &plan.mutations {
        if let GraphMutation::UpdateEdge { properties, .. } = mutation {
            assert!(!properties.contains_key("producer_source"));
            assert!(!properties.contains_key("producer_namespace"));
        }
    }
}

#[test]
fn distinct_relationship_identities_on_one_pair_are_all_preserved_and_fenced() {
    let (source, target, snapshot_id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let owns = edge(source, target, "OWNS", "owns", snapshot_id, at);
    let mut first_call = edge(source, target, "CALLS", "calls", snapshot_id, at);
    first_call.chain_id = Uuid::new_v4();
    first_call
        .all_properties
        .insert("port".into(), kg_core::models::PropertyValue::Integer(443));
    let mut second_call = first_call.clone();
    second_call.uuid = Uuid::new_v4();
    second_call.chain_id = Uuid::new_v4();
    second_call
        .all_properties
        .insert("port".into(), kg_core::models::PropertyValue::Integer(8443));
    let (plan, live) = plan_relationships(&batch(
        vec![owns, first_call, second_call],
        vec![snapshot(snapshot_id, at)],
        baseline(&[(source, target)], vec![], &[]),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_created, 3);
    assert_eq!(live.len(), 3);
    assert!(plan
        .preconditions
        .contains(&Precondition::LiveEdgesForPairAre {
            source_chain_id: source,
            target_chain_id: target,
            uuids: vec![]
        }));
    let ids: HashSet<_> = plan
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            GraphMutation::UpsertEdge { properties, .. } => {
                properties.get("chain_id").and_then(|value| value.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 3);
}

#[test]
fn attribute_changes_version_only_the_resolved_lineage_and_preserve_first_evidence() {
    let (source, target, first_snapshot, later_snapshot) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let at = Utc::now();
    let mut prior = stored(source, target, "CALLS", "calls", at, Some(scope()));
    prior.first_seen_snapshot_id = Some(first_snapshot);
    prior.all_properties.insert(
        "timeout".into(),
        kg_core::models::PropertyValue::Integer(10),
    );
    let other = stored(source, target, "OWNS", "owns", at, Some(scope()));
    let mut incoming = edge(
        source,
        target,
        "CALLS",
        "calls",
        later_snapshot,
        at + Duration::seconds(1),
    );
    incoming.chain_id = prior.chain_id;
    incoming.all_properties.insert(
        "timeout".into(),
        kg_core::models::PropertyValue::Integer(20),
    );
    let (plan, live) = plan_relationships(&batch(
        vec![incoming],
        vec![snapshot(later_snapshot, at + Duration::seconds(1))],
        baseline(&[(source, target)], vec![prior.clone(), other.clone()], &[]),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_updated, 1);
    assert_eq!(closures(&plan), vec![prior.uuid]);
    assert_eq!(live.len(), 1);
    let mut expected = vec![prior.uuid, other.uuid];
    expected.sort();
    assert!(plan
        .preconditions
        .contains(&Precondition::LiveEdgesForPairAre {
            source_chain_id: source,
            target_chain_id: target,
            uuids: expected
        }));
    let properties = plan
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["chain_id"], prior.chain_id.to_string());
    assert_eq!(
        properties["first_seen_snapshot_id"],
        first_snapshot.to_string()
    );
    assert_eq!(properties["prop_timeout"], 20);
}

#[test]
fn same_lineage_conflicting_attributes_at_one_capture_fail() {
    let (source, target, snapshot_id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let first = edge(source, target, "CALLS", "calls", snapshot_id, at);
    let mut second = first.clone();
    second.uuid = Uuid::new_v4();
    second
        .all_properties
        .insert("port".into(), kg_core::models::PropertyValue::Integer(443));
    assert!(plan_relationships(&batch(
        vec![first, second],
        vec![snapshot(snapshot_id, at)],
        baseline(&[(source, target)], vec![], &[])
    ))
    .is_err());
}

#[test]
fn ended_lineage_reopens_at_half_open_boundary_with_timeline_fence() {
    let (source, target, first_snapshot, next_snapshot) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let ended_at = Utc::now();
    let mut previous = stored(
        source,
        target,
        "CALLS",
        "calls",
        ended_at - Duration::seconds(1),
        Some(scope()),
    );
    previous.ended_at = Some(ended_at);
    previous.first_seen_snapshot_id = Some(first_snapshot);
    let mut baseline = baseline(&[(source, target)], vec![], &[]);
    baseline.pairs[0].versions.push(raw(&previous));
    baseline.ended.push(previous.clone());
    for captured_at in [ended_at - Duration::seconds(1), ended_at] {
        let incoming = edge(source, target, "CALLS", "calls", next_snapshot, captured_at);
        let (plan, live) = plan_relationships(&batch(
            vec![incoming],
            vec![snapshot(next_snapshot, captured_at)],
            baseline.clone(),
        ))
        .unwrap();
        if captured_at < ended_at {
            assert_eq!(plan.counts.edges_unchanged, 1);
            assert_eq!(live[0].uuid, previous.uuid);
        } else {
            assert_eq!(plan.counts.edges_updated, 1);
            assert_eq!(live.len(), 1);
        }
    }
    let captured_at = ended_at + Duration::seconds(1);
    let incoming = edge(source, target, "CALLS", "calls", next_snapshot, captured_at);
    let (plan, live) = plan_relationships(&batch(
        vec![incoming],
        vec![snapshot(next_snapshot, captured_at)],
        baseline,
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_created, 0);
    assert_eq!(plan.counts.edges_updated, 1);
    assert!(plan.preconditions.iter().any(|guard| matches!(guard, Precondition::RelationshipTimelineIs { versions, .. } if versions.len() == 1)));
    let properties = plan
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            GraphMutation::UpsertEdge { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(properties["version"], 3);
    assert_eq!(
        properties["previous_version_uuid"],
        previous.uuid.to_string()
    );
    assert_eq!(
        properties["first_seen_snapshot_id"],
        first_snapshot.to_string()
    );
    assert_eq!(live.len(), 1);
}

#[test]
fn inferred_fact_cannot_close_a_declared_cardinality_slot() {
    use kg_core::models::RelationshipOrigin;
    let (source, declared_target, inferred_target, snapshot_id) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let at = Utc::now();
    let mut declared = stored(
        source,
        declared_target,
        "RUNS_ON",
        "source placement",
        at,
        Some(scope()),
    );
    declared.origin = RelationshipOrigin::Declared;
    let mut inferred = edge(
        source,
        inferred_target,
        "RUNS_ON",
        "inferred placement",
        snapshot_id,
        at + Duration::seconds(1),
    );
    inferred.origin = RelationshipOrigin::Fact;
    assert_eq!(inferred.cardinality_key, declared.cardinality_key);
    let (plan, live) = plan_relationships(&batch(
        vec![inferred],
        vec![snapshot(snapshot_id, at + Duration::seconds(1))],
        baseline(
            &[(source, inferred_target)],
            vec![declared.clone()],
            &[(source, "RUNS_ON")],
        ),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_created, 1);
    assert_eq!(plan.counts.edges_invalidated, 0);
    assert!(closures(&plan).is_empty());
    assert_eq!(live.len(), 1);
    assert!(!plan.mutations.iter().any(|mutation| matches!(mutation, GraphMutation::UpdateEdge { uuid, .. } if *uuid == declared.uuid)));
}

#[test]
fn incomplete_reference_discovery_never_retires_stored_relations() {
    let (source, target, snapshot_id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let live = stored(
        source,
        target,
        "REFERENCES_HOST",
        "stored reference",
        at,
        Some(scope()),
    );
    let mut batch = batch(
        vec![],
        vec![snapshot(snapshot_id, at + Duration::seconds(1))],
        baseline(&[(source, target)], vec![live.clone()], &[]),
    );
    batch.reference_report.incomplete_sources.push(source);
    batch.reference_report.attempted = 3;
    let (plan, _) = plan_relationships(&batch).unwrap();
    assert_eq!(
        plan.counts.edges_invalidated, 0,
        "absence in an incomplete scan is not evidence"
    );
    assert!(closures(&plan).is_empty());
    assert!(!plan
        .mutations
        .iter()
        .any(|m| matches!(m, GraphMutation::UpdateEdge { uuid, .. } if *uuid == live.uuid)));
    assert_eq!(
        plan.counts.references_incomplete, 1,
        "incompleteness is durable in the receipt"
    );
    assert_eq!(plan.counts.references_attempted, 3);
    assert!(plan.counts.reference_discovery_incomplete());
}

#[test]
fn separate_cardinality_slots_and_unrestricted_placement_names_coexist() {
    let (source, a, b, snapshot_id) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let at = Utc::now();
    for keyed in [false, true] {
        let mut first = edge(source, a, "RUNS_ON", "release a", snapshot_id, at);
        let mut second = edge(source, b, "RUNS_ON", "release b", snapshot_id, at);
        first.cardinality_key = keyed.then(|| "deployment-a".into());
        second.cardinality_key = keyed.then(|| "deployment-b".into());
        let relations = if keyed {
            vec![(source, "RUNS_ON")]
        } else {
            vec![]
        };
        let (plan, live) = plan_relationships(&batch(
            vec![first, second],
            vec![snapshot(snapshot_id, at)],
            baseline(&[(source, a), (source, b)], vec![], &relations),
        ))
        .unwrap();
        assert_eq!(plan.counts.edges_created, 2);
        assert_eq!(plan.counts.edges_invalidated, 0);
        assert_eq!(live.len(), 2);
    }
}

#[test]
fn later_target_closes_the_earlier_one_whatever_the_input_order() {
    let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let t0 = Utc::now();
    let (s1, s2) = (Uuid::new_v4(), Uuid::new_v4());
    let first = edge(a, b, "RUNS_ON", "b", s1, t0);
    let second = edge(a, c, "RUNS_ON", "c", s2, t0 + Duration::minutes(1));
    let baseline = baseline(&[(a, b), (a, c)], vec![], &[(a, "RUNS_ON")]);
    for (observed, snapshots) in [
        (
            vec![first.clone(), second.clone()],
            vec![snapshot(s1, t0), snapshot(s2, t0 + Duration::minutes(1))],
        ),
        (
            vec![second.clone(), first.clone()],
            vec![snapshot(s2, t0 + Duration::minutes(1)), snapshot(s1, t0)],
        ),
    ] {
        let (plan, live) =
            plan_relationships(&batch(observed, snapshots, baseline.clone())).unwrap();
        assert_eq!(plan.counts.edges_created, 2);
        assert_eq!(plan.counts.edges_invalidated, 1);
        assert_eq!(
            closures(&plan),
            vec![first.uuid],
            "b closes when c is observed"
        );
        assert_eq!(live.len(), 2, "both persisted versions need embeddings");
        assert!(live.iter().any(|target| target.uuid == first.uuid));
        assert!(live.iter().any(|target| target.uuid == second.uuid));
        assert!(plan
            .preconditions
            .contains(&Precondition::LiveEdgesForRelationAre {
                source_chain_id: a,
                name: "RUNS_ON".into(),
                uuids: vec![],
            }));
    }
}

/// Two targets at one capture time fail whether the other target was
/// observed by this batch or is stored with that observation time; a
/// stored target observed earlier closes with a strict fence.
#[test]
fn same_time_targets_are_contradictions_and_earlier_ones_close_strictly() {
    let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let t0 = Utc::now();
    let (s1, s2) = (Uuid::new_v4(), Uuid::new_v4());
    let error = plan_relationships(&batch(
        vec![
            edge(a, b, "RUNS_ON", "b", s1, t0),
            edge(a, c, "RUNS_ON", "c", s2, t0),
        ],
        vec![snapshot(s1, t0), snapshot(s2, t0)],
        baseline(&[(a, b), (a, c)], vec![], &[(a, "RUNS_ON")]),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("two targets captured at"),
        "{error}"
    );

    let same_time = stored(a, b, "RUNS_ON", "b", t0, Some(scope()));
    let error = plan_relationships(&batch(
        vec![edge(a, c, "RUNS_ON", "c", s1, t0)],
        vec![snapshot(s1, t0)],
        baseline(&[(a, c)], vec![same_time.clone()], &[(a, "RUNS_ON")]),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("two targets captured at"),
        "a stored target observed at the same time is a contradiction: {error}"
    );

    let earlier = stored(
        a,
        b,
        "RUNS_ON",
        "b",
        t0 - Duration::minutes(1),
        Some(scope()),
    );
    let (plan, live) = plan_relationships(&batch(
        vec![edge(a, c, "RUNS_ON", "c", s1, t0)],
        vec![snapshot(s1, t0)],
        baseline(&[(a, c)], vec![earlier.clone()], &[(a, "RUNS_ON")]),
    ))
    .unwrap();
    assert_eq!(closures(&plan), vec![earlier.uuid]);
    assert!(plan.preconditions.iter().any(|guard| matches!(guard, Precondition::RelationTimelineIs { versions, .. } if versions.iter().any(|v| v.properties["uuid"] == earlier.uuid.to_string()))));
    assert_eq!(live.len(), 1);
}

/// One pair at one capture time with two wordings or confidences is not
/// a contradiction: the most confident observation is applied, the
/// other is a re-observation of it.
#[test]
fn same_time_wordings_of_one_resolved_lineage_pick_the_most_confident() {
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let t0 = Utc::now();
    let (s1, s2) = (Uuid::new_v4(), Uuid::new_v4());
    let mut weak = edge(a, b, "DEPENDS_ON", "via property x", s1, t0);
    weak.confidence = 0.7;
    let strong = edge(a, b, "DEPENDS_ON", "via property y", s2, t0);
    let (plan, live) = plan_relationships(&batch(
        vec![weak.clone(), strong.clone()],
        vec![snapshot(s1, t0), snapshot(s2, t0)],
        baseline(&[(a, b)], vec![], &[]),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_created, 1);
    assert_eq!(plan.counts.edges_unchanged, 1, "the weaker one re-observes");
    assert_eq!(upserts(&plan), vec![(a, b, 1)]);
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].uuid, strong.uuid);
    assert_eq!(live[0].description, "via property y");

    // A stored version observed at that time stands over new wording.
    let existing = stored(a, b, "DEPENDS_ON", "stored", t0, Some(scope()));
    let (plan, live) = plan_relationships(&batch(
        vec![strong.clone()],
        vec![snapshot(s2, t0)],
        baseline(&[(a, b)], vec![existing.clone()], &[]),
    ))
    .unwrap();
    assert_eq!(plan.counts.edges_unchanged, 1);
    assert_eq!(plan.counts.edges_updated, 0);
    assert!(upserts(&plan).is_empty());
    assert_eq!(live[0].uuid, existing.uuid);
}

#[test]
fn repeated_observations_of_one_resolved_lineage_preserve_history() {
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let t0 = Utc::now();
    let at = |m: i64| t0 + Duration::minutes(m);
    let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    let existing = stored(a, b, "DEPENDS_ON", "stored", at(0), Some(scope()));
    let baseline = baseline(&[(a, b)], vec![existing.clone()], &[]);
    // Older than stored, unchanged, changed, unchanged again; input order scrambled.
    let observed = vec![
        edge(a, b, "DEPENDS_ON", "changed", ids[2], at(2)),
        edge(a, b, "DEPENDS_ON", "stored", ids[1], at(1)),
        edge(a, b, "DEPENDS_ON", "late", ids[0], at(-1)),
        edge(a, b, "DEPENDS_ON", "changed", ids[3], at(3)),
    ];
    let snapshots = vec![
        snapshot(ids[2], at(2)),
        snapshot(ids[1], at(1)),
        snapshot(ids[0], at(-1)),
        snapshot(ids[3], at(3)),
    ];
    let (plan, live) = plan_relationships(&batch(observed.clone(), snapshots, baseline)).unwrap();
    assert_eq!(
        plan.counts.edges_unchanged, 2,
        "stored re-observed, then v3 re-observed"
    );
    assert_eq!(plan.counts.edges_updated, 1);
    assert_eq!(plan.counts.edges_invalidated, 0);
    assert_eq!(upserts(&plan), vec![(a, b, 3)]);
    assert_eq!(
        closures(&plan),
        vec![existing.uuid],
        "the stored version closes at the change"
    );
    assert!(plan.preconditions.iter().any(|guard| matches!(guard, Precondition::RelationshipTimelineIs { versions, .. } if versions[0]["uuid"] == existing.uuid.to_string())));
    assert_eq!(
        live.len(),
        2,
        "observed history and its successor both retain compatible vectors"
    );
    let successor = live
        .iter()
        .find(|target| target.uuid != existing.uuid)
        .unwrap();
    assert_eq!(successor.description, "changed");
    assert!(successor.stored_pair.is_none());
    let last_seen: Vec<String> = plan
        .mutations
        .iter()
        .filter_map(|m| match m {
            GraphMutation::UpdateEdge { properties, .. } => properties
                .get("last_seen_at")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        last_seen,
        vec![at(1).to_rfc3339(), at(3).to_rfc3339()],
        "each unchanged re-observation advances last_seen; the late one changes nothing"
    );
}

#[test]
fn a_target_seen_again_reopens_its_resolved_lineage_as_a_successor() {
    let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let t0 = Utc::now();
    let at = |m: i64| t0 + Duration::minutes(m);
    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let b_stored = stored(a, b, "RUNS_ON", "b", at(0), Some(scope()));
    let baseline = baseline(&[(a, c), (a, b)], vec![b_stored.clone()], &[(a, "RUNS_ON")]);
    let observed = vec![
        edge(a, c, "RUNS_ON", "c", ids[0], at(1)),
        edge(a, b, "RUNS_ON", "b", ids[1], at(2)),
    ];
    let snapshots = vec![
        snapshot(ids[0], at(1)),
        snapshot(ids[1], at(2)),
        snapshot(ids[2], at(3)),
    ];
    let (plan, live) = plan_relationships(&batch(observed.clone(), snapshots, baseline)).unwrap();
    assert_eq!(plan.counts.edges_created, 1, "c");
    assert_eq!(plan.counts.edges_invalidated, 2, "b at 1, then c at 2");
    assert_eq!(plan.counts.edges_updated, 1, "b reopened as v3");
    assert_eq!(upserts(&plan), vec![(a, c, 1), (a, b, 3)]);
    assert_eq!(closures(&plan), vec![b_stored.uuid, observed[0].uuid]);
    assert_eq!(live.len(), 2);
    assert_eq!(
        live.iter()
            .map(|target| target.uuid)
            .collect::<BTreeSet<_>>(),
        observed
            .iter()
            .map(|edge| edge.uuid)
            .collect::<BTreeSet<_>>()
    );
}

/// A newer current target makes the observation stale; a target in
/// another connector's scope, known or not, is left alone and only
/// fenced.
#[test]
fn stale_or_foreign_current_targets_are_left_alone() {
    let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let t0 = Utc::now();
    let s = Uuid::new_v4();
    let newer = stored(
        a,
        b,
        "RUNS_ON",
        "b",
        t0 + Duration::minutes(5),
        Some(scope()),
    );
    let (plan, live) = plan_relationships(&batch(
        vec![edge(a, c, "RUNS_ON", "c", s, t0 + Duration::minutes(1))],
        vec![snapshot(s, t0 + Duration::minutes(1))],
        baseline(&[(a, c)], vec![newer.clone()], &[(a, "RUNS_ON")]),
    ))
    .unwrap();
    assert_eq!(
        plan.counts.edges_created, 1,
        "an earlier disjoint interval does not overwrite the future target"
    );
    assert_eq!(plan.counts.edges_invalidated, 0);
    assert_eq!(live.len(), 1);
    assert!(plan.mutations.iter().any(|m| matches!(m, GraphMutation::UpsertEdge { properties, .. } if properties["valid_to"] == newer.valid_from.to_rfc3339())));

    {
        let foreign_scope = Some(other_scope());
        let foreign = stored(a, b, "RUNS_ON", "b", t0, foreign_scope);
        let (plan, _) = plan_relationships(&batch(
            vec![edge(a, c, "RUNS_ON", "c", s, t0 + Duration::minutes(1))],
            vec![snapshot(s, t0 + Duration::minutes(1))],
            baseline(&[(a, c)], vec![foreign.clone()], &[(a, "RUNS_ON")]),
        ))
        .unwrap();
        assert_eq!(plan.counts.edges_created, 1);
        assert_eq!(
            plan.counts.edges_invalidated, 0,
            "another connector's placement is not this batch's to close"
        );
        assert!(plan
            .preconditions
            .contains(&Precondition::LiveEdgesForRelationAre {
                source_chain_id: a,
                name: "RUNS_ON".into(),
                uuids: vec![foreign.uuid],
            }));
    }
}

/// A disjoint future target does not hide contradictory current observations.
#[test]
fn disjoint_future_target_does_not_hide_same_time_contradictions() {
    let (a, b, c, d) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let t0 = Utc::now();
    let (s1, s2) = (Uuid::new_v4(), Uuid::new_v4());
    let newer = stored(
        a,
        b,
        "RUNS_ON",
        "b",
        t0 + Duration::minutes(5),
        Some(scope()),
    );
    let error = plan_relationships(&batch(
        vec![
            edge(a, c, "RUNS_ON", "c", s1, t0),
            edge(a, d, "RUNS_ON", "d", s2, t0),
        ],
        vec![snapshot(s1, t0), snapshot(s2, t0)],
        baseline(&[(a, c), (a, d)], vec![newer], &[(a, "RUNS_ON")]),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("two targets captured at"));
}

/// An observed single-target relation without its live set in the
/// baseline cannot be fenced and is rejected as a planning error.
#[test]
fn an_observed_single_target_relation_needs_its_live_set() {
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let t0 = Utc::now();
    let s = Uuid::new_v4();
    let error = plan_relationships(&batch(
        vec![edge(a, b, "RUNS_ON", "b", s, t0)],
        vec![snapshot(s, t0)],
        baseline(&[(a, b)], vec![], &[]),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("without live-set baseline"),
        "{error}"
    );
}
#[test]
fn observation_clock_drives_freshness_and_effective_clock_closes_versions() {
    let (source, target, id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let mut prior = stored(source, target, "CALLS", "old", at - Duration::days(1), None);
    prior.valid_from = at - Duration::days(10);
    let mut incoming = edge(source, target, "CALLS", "new", id, at);
    incoming.valid_from = at - Duration::days(3);
    incoming.last_seen_at = Some(at + Duration::days(99));
    let (plan, _) = plan_relationships(&batch(
        vec![incoming.clone()],
        vec![snapshot(id, at)],
        baseline(&[(source, target)], vec![prior.clone()], &[]),
    ))
    .unwrap();
    assert!(plan.preconditions.iter().any(|guard| matches!(guard, Precondition::RelationshipTimelineIs { versions, .. } if versions[0]["uuid"] == prior.uuid.to_string())));
    assert!(plan.mutations.iter().any(|m| matches!(m, GraphMutation::UpsertEdge { properties, .. } if properties["last_seen_at"] == serde_json::json!(at.to_rfc3339()) && properties["valid_from"] == serde_json::json!(incoming.valid_from.to_rfc3339()))));
}

#[test]
fn backwards_historical_update_and_invalid_windows_are_rejected() {
    let (source, target, id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let prior = stored(source, target, "CALLS", "old", at - Duration::days(1), None);
    let mut incoming = edge(source, target, "CALLS", "new", id, at);
    incoming.valid_from = at - Duration::days(2);
    assert!(plan_relationships(&batch(
        vec![incoming.clone()],
        vec![snapshot(id, at)],
        baseline(&[(source, target)], vec![prior], &[])
    ))
    .is_err());
    {
        let (start, end) = (at, Some(at - Duration::seconds(1)));
        incoming.valid_from = start;
        incoming.valid_to = end;
        assert!(plan_relationships(&batch(
            vec![incoming.clone()],
            vec![snapshot(id, at)],
            baseline(&[(source, target)], vec![], &[])
        ))
        .is_err());
    }
}

#[test]
fn embeddings_include_intermediate_versions_and_new_ended_facts() {
    let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let snapshots: Vec<_> = (0..3)
        .map(|i| snapshot(Uuid::new_v4(), at + Duration::minutes(i)))
        .collect();
    let observations: Vec<_> = snapshots
        .iter()
        .enumerate()
        .map(|(i, snapshot)| {
            let mut observed = edge(
                source,
                target,
                "CALLS",
                &format!("revision {i}"),
                snapshot.uuid,
                snapshot.captured_at,
            );
            if i == 2 {
                observed.valid_to = Some(observed.valid_from);
            }
            observed
        })
        .collect();
    let ids: BTreeSet<_> = observations.iter().map(|edge| edge.uuid).collect();
    let (plan, targets) = plan_relationships(&batch(
        observations,
        snapshots,
        baseline(&[(source, target)], vec![], &[]),
    ))
    .unwrap();
    assert_eq!(upserts(&plan).len(), 3);
    assert_eq!(
        targets
            .iter()
            .map(|target| target.uuid)
            .collect::<BTreeSet<_>>(),
        ids
    );
    assert!(targets.iter().all(|target| target.stored_pair.is_none()));
}

#[test]
fn ended_repeat_updates_capture_without_reopening_or_new_version() {
    let (source, target, id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let mut prior = stored(
        source,
        target,
        "CALLS",
        "calls",
        at - Duration::days(1),
        None,
    );
    prior.valid_from = at - Duration::days(3);
    prior.ended_at = Some(at - Duration::days(2));
    let mut incoming = edge(source, target, "CALLS", "calls", id, at);
    incoming.valid_from = prior.valid_from;
    incoming.valid_to = prior.ended_at;
    let mut base = baseline(&[(source, target)], vec![], &[]);
    base.pairs[0].versions.push(raw(&prior));
    base.ended.push(prior.clone());
    let (plan, live) =
        plan_relationships(&batch(vec![incoming], vec![snapshot(id, at)], base)).unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].uuid, prior.uuid);
    assert_eq!(live[0].stored_pair, Some((source, target)));
    assert_eq!(plan.counts.edges_unchanged, 1);
    assert!(!plan
        .mutations
        .iter()
        .any(|m| matches!(m, GraphMutation::UpsertEdge { .. })));
    assert!(plan.preconditions.iter().any(|guard| matches!(guard, Precondition::RelationshipTimelineIs { versions, .. } if versions[0]["uuid"] == prior.uuid.to_string())));
    assert!(plan.mutations.iter().any(|m| matches!(m, GraphMutation::UpdateEdge { properties, .. } if properties["last_seen_at"] == serde_json::json!(at.to_rfc3339()))));
}

#[test]
fn capture_after_closed_end_does_not_reopen_an_earlier_effective_fact() {
    let (source, target, id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let at = Utc::now();
    let mut prior = stored(
        source,
        target,
        "CALLS",
        "calls",
        at - Duration::days(1),
        None,
    );
    prior.valid_from = at - Duration::days(3);
    prior.ended_at = Some(at - Duration::days(2));
    let mut incoming = edge(source, target, "CALLS", "calls", id, at);
    incoming.valid_from = prior.valid_from;
    let mut base = baseline(&[(source, target)], vec![], &[]);
    base.pairs[0].versions.push(raw(&prior));
    base.ended.push(prior);
    let (plan, live) =
        plan_relationships(&batch(vec![incoming], vec![snapshot(id, at)], base)).unwrap();
    assert!(plan.mutations.is_empty());
    assert!(live.is_empty());
}
mod schedule;

mod slot_properties {
    use super::*;
    use kg_core::runtime::stage_output::{ReferenceReport, UnresolvedSlot};
    use proptest::prelude::*;

    fn slots() -> impl Strategy<Value = Vec<UnresolvedSlot>> {
        prop::collection::vec(
            (0u8..4, "[A-Z][a-z]{1,4}\\.[a-z]{1,6}").prop_map(|(source, slot)| UnresolvedSlot {
                source_chain_id: Uuid::from_u128(1 + source as u128),
                slot,
                decided_at: Utc::now(),
                decision_id: Uuid::new_v4(),
                entries: vec![],
            }),
            0..8,
        )
    }

    proptest! {
        /// Every unresolved slot the report carries becomes exactly one durable
        /// record in the same batch, in report order, and the reference counts
        /// are copied verbatim; nothing is invented or dropped.
        #[test]
        fn every_reported_slot_is_recorded_once_in_order(
            slots in slots(), attempted in 0usize..50, confirmed in 0usize..50, unresolved in 0usize..50
        ) {
            let mut input = batch(vec![], vec![], baseline(&[], vec![], &[]));
            input.reference_report = ReferenceReport {
                retirement_decisions: Vec::new(),
                source_coverage: Vec::new(),
                source_histories: Vec::new(),
                source_reads: Vec::new(),
                relationship_declines: vec![],
                attempted,
                confirmed,
                unresolved,
                excluded: 0,
                incomplete_sources: vec![],
                candidate_revisions: vec![],
                unresolved_slots: slots.clone(),
                decisions: vec![],
                decision_contexts: vec![],
            };
            let (plan, targets) = plan_relationships(&input).unwrap();
            prop_assert!(targets.is_empty());
            let recorded: Vec<(Uuid, String)> = plan
                .mutations
                .iter()
                .filter_map(|m| match m {
                    GraphMutation::RecordUnresolvedReferences { source_chain_id, slot, entries, .. } => {
                        prop_assert_eq_ok(entries.is_empty());
                        Some((*source_chain_id, slot.clone()))
                    }
                    _ => None,
                })
                .collect();
            let expected: Vec<(Uuid, String)> =
                slots.iter().map(|s| (s.source_chain_id, s.slot.clone())).collect();
            prop_assert_eq!(recorded, expected);
            prop_assert_eq!(plan.mutations.len(), slots.len(), "no other mutation without edges");
            prop_assert_eq!(
                (plan.counts.references_attempted, plan.counts.references_confirmed, plan.counts.references_unresolved),
                (attempted, confirmed, unresolved)
            );
        }
    }

    fn prop_assert_eq_ok(condition: bool) {
        assert!(condition, "empty entries stay empty");
    }
}

#[test]
fn reference_retirement_preserves_exclusions_partial_omissions_and_same_time_evidence() {
    use kg_core::{
        models::edges::ReferenceEvidence,
        runtime::stage_output::{ReferenceSourceCoverage, UnresolvedSlot},
        traits::UnresolvedReferenceEntry,
    };
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let at = Utc::now();
    let capture = at + Duration::seconds(10);
    let mut prior = stored(source, target, "RELATES_TO", "ref", at, Some(scope()));
    prior.reference_evidence = Some(ReferenceEvidence {
        component_paths: None,
        observing_chain_id: source,
        observing_namespace: "prod".into(),
        slot: "Resource.targets".into(),
        location: "targets[0].id".into(),
        target_key_group: vec!["id".into()],
        reference_tokens: vec!["s:target".into()],
        read_set: vec![],
        decision: None,
    });
    let base_coverage = ReferenceSourceCoverage {
        chain_id: source,
        namespace: "prod".into(),
        captured_at: capture,
        complete: true,
        paths: vec![],
        excluded_paths: vec![],
    };
    for case in [
        "excluded",
        "partial_omission",
        "same_time_uncertainty",
        "same_time_positive",
        "older_uncertainty",
        "unrelated_lookup_overflow",
        "same_slot_lookup_overflow",
    ] {
        let mut coverage = base_coverage.clone();
        let mut input = batch(vec![], vec![], RelationshipBaseline::default());
        match case {
            "excluded" => coverage.excluded_paths.push("targets".into()),
            "partial_omission" => coverage.complete = false,
            "same_time_uncertainty"
            | "older_uncertainty"
            | "unrelated_lookup_overflow"
            | "same_slot_lookup_overflow" => {
                input
                    .reference_report
                    .retirement_decisions
                    .push(UnresolvedSlot {
                        source_chain_id: source,
                        slot: if case == "unrelated_lookup_overflow" {
                            "Resource.account"
                        } else {
                            "Resource.targets"
                        }
                        .into(),
                        decided_at: if case == "older_uncertainty" {
                            at
                        } else {
                            capture
                        },
                        decision_id: Uuid::new_v4(),
                        entries: vec![UnresolvedReferenceEntry {
                            token: "s:target".into(),
                            reason: if case.ends_with("lookup_overflow") {
                                "lookup-truncated"
                            } else {
                                "multiple-candidates"
                            }
                            .into(),
                            snapshot_id: None,
                            recorded_at: capture,
                        }],
                    });
            }
            "same_time_positive" => {
                let mut positive =
                    edge(source, target, "RELATES_TO", "ref", Uuid::new_v4(), capture);
                positive.reference_evidence = prior.reference_evidence.clone();
                input.observed = Arc::new(vec![positive]);
            }
            _ => unreachable!(),
        }
        let mut chains = BTreeMap::from([(prior.chain_id, vec![Version::stored(&prior)])]);
        let mut plan = Plan::default();
        reference_retirement::apply(&mut plan, &mut chains, &input, &coverage).unwrap();
        let closes = matches!(case, "older_uncertainty" | "unrelated_lookup_overflow");
        assert_eq!(chains[&prior.chain_id][0].closed, closes, "{case}");
        assert_eq!(plan.counts.edges_invalidated, usize::from(closes), "{case}");
    }
}

mod decision_audits {
    use super::*;
    use kg_core::runtime::reference_resolution::{
        DecisionOutcome, DecisionReason, EvidenceOrigin, ReferenceDecisionAudit,
    };

    /// Original model decisions with context become packed persisted records,
    /// rebindings become reuse rows, host refusals are skipped, and pages split
    /// at the per-statement cap.
    #[test]
    fn original_decisions_are_persisted_packed_and_reuses_only_note_their_original() {
        use kg_core::runtime::reference_resolution::{
            DecisionContext, PersistedCandidate, MAX_DECISIONS_PER_STATEMENT,
        };
        let context = |id: Uuid| DecisionContext {
            decision_id: id,
            producer_source: "cmdb".into(),
            observing_namespace: "prod".into(),
            source_entity_type: "Server".into(),
            target_type: Some("Server".into()),
            components: vec![("peer".into(), "s:x".into())],
            reference_tokens: vec!["s:x".into()],
            candidates: vec![PersistedCandidate {
                chain_id: Uuid::from_u128(4),
                entity_type: "Server".into(),
                key_groups: vec![vec!["id".into()]],
            }],
        };
        let mut input = batch(vec![], vec![], baseline(&[], vec![], &[]));
        let original = rejected(Uuid::from_u128(100));
        let mut reused = rejected(Uuid::from_u128(101));
        reused.reused = true;
        reused.reused_from = Some(Uuid::from_u128(100));
        let mut refusal = rejected(Uuid::from_u128(102));
        refusal.outcome = DecisionOutcome::Unsure;
        refusal.reason = DecisionReason::OwnIdentityValue;
        refusal.model_served = None;
        refusal.provider_attempts = 0;
        input.reference_report.decisions = vec![original.clone(), reused, refusal];
        input.reference_report.decision_contexts = vec![context(Uuid::from_u128(100))];
        let (plan, _) = plan_relationships(&input).unwrap();
        let records: Vec<_> = plan
            .mutations
            .iter()
            .filter_map(|m| match m {
                GraphMutation::RecordReferenceDecisions { decisions, reuses } => {
                    Some((decisions.clone(), reuses.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(records.len(), 2, "{:?}", plan.mutations.len());
        let (decisions, none) = &records[0];
        assert!(
            none.is_empty(),
            "records are written before any reuse is noted"
        );
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].audit.decision_id, Uuid::from_u128(100));
        assert_eq!(decisions[0].producer_source, "cmdb");
        let (none, reuses) = &records[1];
        assert!(none.is_empty());
        assert_eq!(reuses.len(), 1);
        assert_eq!(
            (reuses[0].original, reuses[0].decision_id),
            (Uuid::from_u128(100), Uuid::from_u128(101))
        );

        // More originals than one statement holds: several mutations, none over the cap.
        let mut many = batch(vec![], vec![], baseline(&[], vec![], &[]));
        let mut decisions = Vec::new();
        let mut contexts = Vec::new();
        for i in 0..(MAX_DECISIONS_PER_STATEMENT + 3) {
            let id = Uuid::from_u128(1000 + i as u128);
            decisions.push(rejected(id));
            contexts.push(context(id));
        }
        // A reuse of the very last original: noted only after every record page.
        let mut reuse_of_last = rejected(Uuid::from_u128(5000));
        reuse_of_last.reused = true;
        reuse_of_last.reused_from = Some(Uuid::from_u128(
            1000 + MAX_DECISIONS_PER_STATEMENT as u128 + 2,
        ));
        decisions.push(reuse_of_last);
        many.reference_report.decisions = decisions;
        many.reference_report.decision_contexts = contexts;
        let (plan, _) = plan_relationships(&many).unwrap();
        let pages: Vec<(usize, usize)> = plan
            .mutations
            .iter()
            .filter_map(|m| match m {
                GraphMutation::RecordReferenceDecisions { decisions, reuses } => {
                    Some((decisions.len(), reuses.len()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            pages,
            vec![(MAX_DECISIONS_PER_STATEMENT, 0), (3, 0), (0, 1)]
        );
        for mutation in &plan.mutations {
            mutation.validate("org").unwrap();
        }
    }

    fn rejected(id: Uuid) -> ReferenceDecisionAudit {
        ReferenceDecisionAudit {
            decision_id: id,
            source_chain_id: Uuid::from_u128(1),
            source_version_uuid: Uuid::from_u128(2),
            source_snapshot_id: Uuid::from_u128(3),
            source_captured_at: Utc::now(),
            slot: "Server.peer".into(),
            location: "peer".into(),
            value: "x".into(),
            evidence_origin: EvidenceOrigin::Structured,
            outcome: DecisionOutcome::Rejected,
            reason: DecisionReason::ModelRejected,
            target_chain_id: None,
            fact: None,
            supporting_evidence: vec![],
            candidate_read_set: vec![kg_core::models::edges::ReadVersion {
                chain_id: Uuid::from_u128(4),
                version_uuid: Uuid::from_u128(5),
                version: 1,
                observed_at: None,
            }],
            evidence_fingerprint: "a".repeat(64),
            evidence_complete: true,
            model_configured: "m".into(),
            model_served: Some("m".into()),
            provider_attempts: 1,
            input_tokens: None,
            output_tokens: None,
            processing_version: "v".into(),
            decided_at: Utc::now(),
            reused: false,
            reused_from: None,
            reuse_fingerprint: "c".repeat(64),
            cited_value_hashes: Vec::new(),
        }
    }

    #[test]
    fn a_changed_audit_alone_never_creates_a_version_and_evidence_clocks_are_fenced() {
        use kg_core::models::edges::{ReadVersion, ReferenceEvidence};
        let at = Utc::now();
        let capture = at + Duration::seconds(30);
        let source = Uuid::new_v4();
        let target = Uuid::new_v4();
        let competitor = Uuid::new_v4();
        let mut first = rejected(Uuid::from_u128(21));
        first.source_chain_id = source;
        first.outcome = DecisionOutcome::Accepted;
        first.reason = DecisionReason::ModelAccepted;
        first.target_chain_id = Some(target);
        first.fact = Some("first wording".into());
        first.candidate_read_set = vec![
            ReadVersion {
                chain_id: target,
                version_uuid: Uuid::from_u128(31),
                version: 1,
                observed_at: Some(at),
            },
            ReadVersion {
                chain_id: competitor,
                version_uuid: Uuid::from_u128(32),
                version: 2,
                observed_at: Some(at),
            },
        ];
        first.supporting_evidence =
            vec![kg_core::runtime::reference_resolution::EvidenceCitation {
                id: "src:occ".into(),
                owner_chain_id: source,
                kind: kg_core::runtime::reference_resolution::EvidenceKind::StructuredProperty,
                path: Some("peer".into()),
                snapshot_uuid: None,
                start_char: None,
                end_char: None,
            }];
        let evidence = |decision: ReferenceDecisionAudit| ReferenceEvidence {
            component_paths: None,
            observing_chain_id: source,
            observing_namespace: "prod".into(),
            slot: "Server.peer".into(),
            location: "peer".into(),
            target_key_group: vec!["id".into()],
            reference_tokens: vec!["s:x".into()],
            read_set: decision.candidate_read_set.clone(),
            decision: Some(decision),
        };
        let mut prior = stored(source, target, "RELATES_TO", "ref", at, Some(scope()));
        prior.version = 1;
        prior.origin = kg_core::models::RelationshipOrigin::Reference;
        prior.reference_evidence = Some(evidence(first.clone()));
        let snapshot = snapshot(Uuid::new_v4(), capture);
        let mut incoming = edge(source, target, "RELATES_TO", "ref", snapshot.uuid, capture);
        incoming.chain_id = prior.chain_id;
        incoming.origin = prior.origin;
        incoming.valid_from = at;
        // Same relationship content; only the audit differs (new capture, new wording).
        let mut second = first.clone();
        second.decision_id = Uuid::from_u128(22);
        second.fact = Some("second wording".into());
        second.source_snapshot_id = snapshot.uuid;
        second.reused = true;
        second.candidate_read_set[0].observed_at = Some(capture);
        incoming.reference_evidence = Some(evidence(second.clone()));
        let mut base = baseline(&[(source, target)], vec![prior.clone()], &[]);
        base.reference_owners
            .push(kg_core::runtime::stage_output::ReferenceOwnerBaseline {
                selector: kg_core::runtime::stage_output::ReferenceOwnerSelector {
                    chain_id: source,
                    namespace: "prod".into(),
                    slot: "Server.peer".into(),
                },
                versions: vec![],
                live: vec![],
            });
        let mut input = batch(vec![incoming], vec![snapshot], base);
        input.reference_report.decisions = vec![second.clone()];
        let (plan, targets) = plan_relationships(&input).unwrap();
        assert_eq!(
            plan.counts.edges_unchanged, 1,
            "audit provenance is not relationship content"
        );
        assert_eq!(plan.counts.edges_created + plan.counts.edges_updated, 0);
        assert!(
            !plan
                .mutations
                .iter()
                .any(|m| matches!(m, GraphMutation::UpsertEdge { .. })),
            "no new version for a changed audit"
        );
        let refreshed = plan
            .mutations
            .iter()
            .find_map(|m| match m {
                GraphMutation::UpdateEdge { uuid, properties } if *uuid == prior.uuid => {
                    Some(properties.clone())
                }
                _ => None,
            })
            .expect("the re-observation refreshes the stored version's bookkeeping");
        let stored_audit: ReferenceDecisionAudit =
            serde_json::from_str(refreshed["reference_decision"].as_str().unwrap()).unwrap();
        assert_eq!(
            stored_audit, second,
            "the current audit rides along as bookkeeping"
        );
        assert!(!refreshed.contains_key("description") && !refreshed.contains_key("name"));
        assert_eq!(targets.len(), 1);
        assert_eq!(
            targets[0].uuid, prior.uuid,
            "embedding target is the unchanged version"
        );
        assert_eq!(targets[0].description, prior.description);
        // Every version the decision read is fenced on both identity and observation clock.
        for read in &second.candidate_read_set {
            assert!(plan.preconditions.iter().any(|p| matches!(
                p,
                Precondition::LatestVersionIs { uuid, .. } if *uuid == read.version_uuid
            )));
            assert!(plan.preconditions.iter().any(|p| matches!(
                p,
                Precondition::NotObservedAfter { uuid, observed_at }
                    if *uuid == read.version_uuid && Some(*observed_at) == read.observed_at
            )));
        }
        // A fresh edge write carries the audit as a property outside the typed source properties.
        let props = edge_properties(&input.observed[0], 1, None, &scope());
        assert!(props["reference_decision"].is_string());
        assert!(!props.contains_key("prop_reference_decision"));
    }

    #[test]
    fn identical_audits_dedupe_but_conflicting_audits_fail_the_batch() {
        let mut input = batch(vec![], vec![], baseline(&[], vec![], &[]));
        let first = rejected(Uuid::from_u128(9));
        input.reference_report.decisions = vec![first.clone(), first.clone()];
        plan_relationships(&input).expect("identical repeats are fine");
        let mut other = first.clone();
        other.outcome = DecisionOutcome::Unsure;
        other.reason = DecisionReason::ModelUnsure;
        input.reference_report.decisions = vec![first, other];
        assert!(
            plan_relationships(&input).is_err(),
            "one decision id with two different records is never last-writer-wins"
        );
    }
}
