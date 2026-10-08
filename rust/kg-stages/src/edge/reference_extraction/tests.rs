use kg_core::models::CollectionRef;
use kg_core::runtime::stage_output::{NodeResolutionOutput, TargetKeyComponent, TargetKeyGroup};

use super::*;

fn reference_input(resolution: NodeResolutionOutput) -> StageOutput {
    StageOutput::EdgeExtraction(EdgeExtractionOutput {
        relationship_times: Default::default(),
        relationship_directives: Default::default(),
        reference_report: Default::default(),
        pending_references: Default::default(),
        snapshot_nodes: resolution.snapshot_nodes.clone(),
        resolved_nodes: resolution.live_entities(),
        edges: resolution.sub_edges.clone(),
        resolution: Arc::new(resolution),
    })
}

fn context() -> RuntimeContext {
    let llm = Arc::new(kg_core::test_support::MockLlmBackend::with_responses(
        vec![],
    ));
    kg_core::runtime::RuntimeContextBuilder::new("org")
        .graph(Arc::new(EmptyReferenceGraph))
        .llm_extraction(llm.clone())
        .llm_disambiguation(llm.clone())
        .llm_default(llm)
        .embedder(Arc::new(kg_core::test_support::MockEmbedBackend::new(4)))
        .build()
        .unwrap()
}

#[test]
fn default_reference_discovery_stays_within_a_namespace() {
    let ctx = context();
    assert!(ctx.namespace_policy.allows("prod", "prod"));
    assert!(!ctx.namespace_policy.allows("prod", "staging"));
}

#[test]
fn scalar_reference_has_a_single_target_slot_but_list_reference_does_not() {
    let mut source = crate::node::entity_versioning::tests::test_entity("source");
    source.all_properties.insert(
        "SubnetId".into(),
        PropertyValue::String("subnet-one".into()),
    );
    source.all_properties.insert(
        "SubnetIds".into(),
        PropertyValue::StringList(vec!["subnet-one".into()]),
    );
    let mut scalar = build_fk_edge(
        &source,
        Uuid::new_v4(),
        "Subnet",
        "subnet-one",
        "SubnetId",
        "SubnetId",
        false,
    )
    .unwrap();
    let mut list = build_fk_edge(
        &source,
        Uuid::new_v4(),
        "Subnet",
        "subnet-one",
        "SubnetIds",
        "SubnetId",
        false,
    )
    .unwrap();
    assert!(scalar.cardinality_key.is_some());
    assert!(list.cardinality_key.is_none());
    super::super::relationship_matching::prepare_identity(&mut scalar, &[], false, "test-ns")
        .unwrap();
    super::super::relationship_matching::prepare_identity(&mut list, &[], false, "test-ns")
        .unwrap();
    assert!(scalar.cardinality_key.is_some());
    assert!(list.cardinality_key.is_none());
}

fn two_observations() -> (NodeResolutionOutput, Uuid, Uuid) {
    use kg_core::runtime::stage_output::{Observed, ObservedEntityProperties};
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let mut entity = crate::node::entity_versioning::tests::test_entity("source");
    entity.last_seen_snapshot_id = Some(Uuid::new_v4());
    entity.all_properties.insert(
        "cluster_name".into(),
        PropertyValue::String("cluster-target".into()),
    );
    let observations = [Uuid::new_v4(), Uuid::new_v4()];
    let originals = observations
        .into_iter()
        .zip([first, second])
        .map(|(observation, snapshot)| {
            let mut original = ObservedEntityProperties::from_entity(&entity, snapshot);
            original.observation_uuid = observation;
            original
        })
        .collect();
    let mut targets = vec![
        observation_target(&entity),
        RelationshipTarget {
            chain_id: Uuid::new_v4(),
            name: "cluster-target".into(),
            entity_type: "Cluster".into(),
            namespace: entity.namespace.clone(),
            version_uuid: Uuid::new_v4(),
            version: 1,
            // Exact source/target key association supplies structural evidence.
            key_groups: vec![kg_core::runtime::stage_output::TargetKeyGroup {
                components: vec![kg_core::runtime::stage_output::TargetKeyComponent {
                    property: "cluster_name".into(),
                    type_tag: "s".into(),
                    value: "cluster-target".into(),
                }],
            }],
        },
    ];
    targets.sort_by_key(|t| t.chain_id);
    (
        NodeResolutionOutput {
            relationship_changes: Default::default(),
            snapshot_nodes: Arc::new(
                [first, second]
                    .into_iter()
                    .map(|uuid| kg_core::models::SnapshotNode {
                        uuid,
                        org_id: entity.org_id.clone(),
                        namespace: entity.namespace.clone(),
                        name: "observation".into(),
                        source_description: None,
                        data_type: kg_core::models::SnapshotDataType::Entities,
                        snapshot_kind: kg_core::models::SnapshotKind::Incremental,
                        sync_generation: None,
                        complete: false,
                        collection: None,
                        source: entity.source.clone(),
                        content: None,
                        captured_at: entity.valid_from,
                        entities: vec![],
                        entity_edges: vec![],
                        labels: vec![],
                        tags: Default::default(),
                        created_at: entity.valid_from,
                    })
                    .collect(),
            ),
            fk_exclusions: Arc::new(HashMap::from([
                (first, vec!["cluster_name".into()]),
                (second, vec![]),
            ])),
            observed_properties: Arc::new(originals),
            nodes_unchanged: Arc::new(
                observations
                    .into_iter()
                    .map(|id| Observed::new(id, entity.clone()))
                    .collect(),
            ),
            chunk_entities: Some(Arc::new(targets)),
            ..Default::default()
        },
        first,
        second,
    )
}

#[tokio::test]
async fn exclusions_follow_original_observations_and_survive_checkpoint_recovery() {
    use kg_core::runtime::stage_output::NodeCheckpoint;
    let (resolution, first, second) = two_observations();
    let targets = resolution.chunk_entities.clone();
    let encoded = serde_json::to_vec(&NodeCheckpoint {
        snapshot_index: 0,
        resolution,
    })
    .unwrap();
    let mut recovered: NodeCheckpoint = serde_json::from_slice(&encoded).unwrap();
    recovered.resolution.chunk_entities = targets;
    assert_eq!(
        super::super::observation_evidence::current_observations(
            &recovered.resolution,
            "org",
            "test"
        )
        .unwrap()
        .into_iter()
        .map(|observation| observation.snapshot_uuid)
        .collect::<Vec<_>>(),
        [first, second]
    );
    let output = ReferenceExtractionStage
        .process(reference_input(recovered.resolution), &context())
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = output else {
        panic!("edge output required")
    };
    assert_eq!(
        output.edges.len(),
        1,
        "only the second source allows the reference"
    );
    assert_eq!(
        output.resolved_nodes[0].all_properties["cluster_name"],
        PropertyValue::String("cluster-target".into())
    );
}

#[tokio::test]
async fn excluded_properties_never_reach_stored_target_lookup() {
    let (mut resolution, first, _) = two_observations();
    Arc::make_mut(&mut resolution.nodes_unchanged).truncate(1);
    let entities = resolution.live_entities();
    let ctx = context();
    // The only scalar (cluster_name) is excluded, so no wanted token is produced.
    let excluded = excluded_properties(&ctx, &entities[0], Some(first), &resolution.fk_exclusions);
    let (observations, truncated) = source_observations(&entities[0], &excluded);
    assert!(!truncated);
    assert!(
        observations.is_empty(),
        "excluded properties must not become lookup tokens"
    );
    // With no tokens, the stored lookup returns without touching the graph, so an
    // unreachable backend proves no read was attempted.
    let mut ctx = context();
    ctx.graph = Arc::new(kg_core::test_support::UnreachableGraph);
    let (stored, lookup_truncated) = stored_candidates(&ctx, &[]).await.unwrap();
    assert!(stored.is_empty() && lookup_truncated.is_empty());
}

#[test]
fn configured_exclusions_require_source_provenance() {
    let (mut resolution, _, _) = two_observations();
    resolution.observed_properties = Default::default();
    assert!(
        super::super::observation_evidence::current_observations(&resolution, "org", "test")
            .is_err()
    );
}

#[test]
fn value_token_is_typed_and_case_preserving() {
    // Case is preserved and the type marker distinguishes 443 from "443".
    assert_eq!(
        value_token(&PropertyValue::String("VPC-1".into())).as_deref(),
        Some("s:VPC-1")
    );
    assert_eq!(
        value_token(&PropertyValue::Integer(443)).as_deref(),
        Some("i:443")
    );
    // Blank strings, lists, and JSON blobs are not identities.
    assert_eq!(value_token(&PropertyValue::String("   ".into())), None);
    assert_eq!(
        value_token(&PropertyValue::StringList(vec!["a".into()])),
        None
    );
    assert_eq!(value_token(&PropertyValue::Json(r#"["a"]"#.into())), None);
}

#[test]
fn every_scalar_is_a_typed_observation_without_a_field_allowlist() {
    let mut source = crate::node::entity_versioning::tests::test_entity("source");
    source
        .all_properties
        .insert("VpcId".into(), PropertyValue::String("vpc-1".into()));
    source
        .all_properties
        .insert("Count".into(), PropertyValue::Integer(443));
    source.all_properties.insert(
        "SecurityGroupIds".into(),
        PropertyValue::StringList(vec!["sg-1".into(), "sg-2".into()]),
    );
    source.all_properties.insert(
        "SecurityGroups".into(),
        PropertyValue::Json(r#"[{"GroupId":"sg-3","GroupName":"demo-web"}]"#.into()),
    );
    source.all_properties.insert(
        "Tags".into(),
        PropertyValue::Json(r#"[{"Key":"Name","Value":"demo"}]"#.into()),
    );
    let (observations, truncated) = source_observations(&source, &[]);
    assert!(!truncated);
    let tokens: std::collections::BTreeSet<(String, String, bool)> = observations
        .iter()
        .map(|o| (o.location.clone(), o.token.clone(), o.from_collection))
        .collect();
    // Scalars keep their exact typed value; a list element shares one location
    // and is a collection observation.
    assert!(tokens.contains(&("VpcId".into(), "s:vpc-1".into(), false)));
    assert!(tokens.contains(&("Count".into(), "i:443".into(), false)));
    assert!(tokens.contains(&("SecurityGroupIds[0]".into(), "s:sg-1".into(), true)));
    assert!(tokens.contains(&("SecurityGroupIds[1]".into(), "s:sg-2".into(), true)));
    // Nested object leaves keep their full path; there is no id/arn allowlist,
    // and naming is not evidence, so even a Tags value is a candidate.
    assert!(tokens.contains(&("SecurityGroups[0].GroupId".into(), "s:sg-3".into(), true)));
    assert!(tokens.contains(&(
        "SecurityGroups[0].GroupName".into(),
        "s:demo-web".into(),
        true
    )));
    assert!(tokens.contains(&("Tags[0].Value".into(), "s:demo".into(), true)));
}

#[test]
fn traversal_truncation_is_reported_not_dropped() {
    let mut source = crate::node::entity_versioning::tests::test_entity("source");
    let attachments: Vec<serde_json::Value> = (0..MAX_TRAVERSAL_VALUES + 12)
        .map(|i| serde_json::json!({ "ref_id": format!("x-{i}") }))
        .collect();
    source.all_properties.insert(
        "attachments".into(),
        PropertyValue::Json(serde_json::Value::Array(attachments).to_string()),
    );
    let (observations, truncated) = source_observations(&source, &[]);
    assert!(truncated, "hitting the value bound must be reported");
    assert!(observations.len() <= MAX_TRAVERSAL_VALUES);
}

#[test]
fn only_a_single_string_key_is_a_bridge_reference_target() {
    let target = |groups: Vec<Vec<(&str, &str, &str)>>| RelationshipTarget {
        chain_id: Uuid::new_v4(),
        name: "n".into(),
        entity_type: "T".into(),
        namespace: "prod".into(),
        version_uuid: Uuid::new_v4(),
        version: 1,
        key_groups: groups
            .into_iter()
            .map(|group| TargetKeyGroup {
                components: group
                    .into_iter()
                    .map(|(property, type_tag, value)| TargetKeyComponent {
                        property: property.into(),
                        type_tag: type_tag.into(),
                        value: value.into(),
                    })
                    .collect(),
            })
            .collect(),
    };
    assert_eq!(
        target(vec![vec![("vpc_id", "s", "vpc-0a1b")]]).single_string_identity(),
        Some(("vpc_id", "vpc-0a1b"))
    );
    assert_eq!(
        target(vec![vec![("name", "s", "x"), ("region", "s", "us-east-1")]])
            .single_string_identity(),
        None,
        "a composite key is not matched by one of its parts"
    );
    assert_eq!(
        target(vec![vec![("name", "s", "x")]]).single_string_identity(),
        None
    );
    assert_eq!(
        target(vec![vec![("id", "i", "443")]]).single_string_identity(),
        None,
        "typed non-string keys wait for whole-group matching"
    );
    assert_eq!(
        target(vec![vec![("id", "s", "ab")]]).single_string_identity(),
        Some(("id", "ab")),
        "length is enforced at the index, not on the target"
    );
    assert_eq!(target(vec![]).single_string_identity(), None);
}

#[test]
fn reference_edges_are_owned_by_the_source_slot_not_the_target() {
    let mut source = crate::node::entity_versioning::tests::test_entity("source");
    source.all_properties.insert(
        "SubnetId".into(),
        PropertyValue::String("subnet-one".into()),
    );
    source.all_properties.insert(
        "SubnetIds".into(),
        PropertyValue::StringList(vec!["subnet-one".into(), "subnet-two".into()]),
    );
    let to_a = build_fk_edge(
        &source,
        Uuid::new_v4(),
        "Subnet",
        "subnet-one",
        "SubnetId",
        "subnet_id",
        false,
    )
    .unwrap();
    let to_b = build_fk_edge(
        &source,
        Uuid::new_v4(),
        "Subnet",
        "subnet-one",
        "SubnetId",
        "subnet_id",
        false,
    )
    .unwrap();
    // R2: the single-target slot belongs to the observing source; retargeting the
    // edge (a different endpoint) keeps the same owner key, so retirement and
    // retarget planning key on the slot even when graph endpoints differ.
    assert!(to_a.cardinality_key.is_some());
    assert_eq!(to_a.cardinality_key, to_b.cardinality_key);
    let evidence = to_a.reference_evidence.as_ref().unwrap();
    assert_eq!(evidence.slot, format!("{}.SubnetId", source.entity_type));
    assert_eq!(evidence.location, "SubnetId");
    assert_eq!(evidence.target_key_group, vec!["subnet_id".to_string()]);
    // R3: list elements share one slot with collection semantics (no single-target key).
    let list = build_fk_edge(
        &source,
        Uuid::new_v4(),
        "Subnet",
        "subnet-two",
        "SubnetIds",
        "subnet_id",
        false,
    )
    .unwrap();
    assert!(list.cardinality_key.is_none());
    assert_eq!(
        list.reference_evidence.as_ref().unwrap().slot,
        format!("{}.SubnetIds", source.entity_type)
    );
    // Array indexes locate evidence only; they never enter the slot.
    assert_eq!(
        reference_slot("AwsInstance", "network_interfaces[1].subnetwork"),
        "AwsInstance.network_interfaces.subnetwork"
    );
}

#[test]
fn a_sole_member_of_the_observing_scan_is_not_linked() {
    let scan = CollectionRef {
        namespace: "prod".into(),
        source: "aws".into(),
        key: "us-east-1".into(),
    };
    let other = CollectionRef {
        key: "eu-west-1".into(),
        ..scan.clone()
    };
    let target = |collections: Vec<(CollectionRef, u64)>| StoredTarget {
        target: RelationshipTarget {
            chain_id: Uuid::new_v4(),
            name: "target".into(),
            entity_type: "Vpc".into(),
            namespace: "prod".into(),
            version_uuid: Uuid::new_v4(),
            version: 1,
            key_groups: Vec::new(),
        },
        collections: collections
            .into_iter()
            .map(|(collection, generation)| CollectionMembership {
                collection,
                generation,
            })
            .collect(),
    };
    let observing = CollectionMembership {
        collection: scan.clone(),
        generation: 2,
    };
    assert!(target(vec![(scan.clone(), 1)]).swept_by(Some(&observing)));
    assert!(
        !target(vec![(scan.clone(), 2)]).swept_by(Some(&observing)),
        "already re-observed at this generation"
    );
    assert!(!target(vec![(scan.clone(), 1), (other.clone(), 1)]).swept_by(Some(&observing)));
    assert!(!target(vec![(other, 1)]).swept_by(Some(&observing)));
    assert!(!target(vec![(scan.clone(), 1)]).swept_by(None));
}
#[tokio::test]
async fn inherited_references_are_not_fresh_in_batch_or_cross_run_evidence() {
    for in_batch in [true, false] {
        let (mut resolution, _, _) = two_observations();
        for original in Arc::make_mut(&mut resolution.observed_properties) {
            original.properties.clear();
        }
        if !in_batch {
            let source = observation_target(&resolution.live_entities()[0]);
            resolution.chunk_entities = Some(Arc::new(vec![source]));
        }
        let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
            .process(reference_input(resolution), &context())
            .await
            .unwrap()
        else {
            panic!("expected edges");
        };
        assert!(output.edges.is_empty());
        assert!(
            output
                .resolved_nodes
                .iter()
                .all(|node| node.all_properties.contains_key("cluster_name")),
            "resolved state stays complete"
        );
    }
}

#[tokio::test]
async fn fresh_reference_uses_original_snapshot_provenance() {
    let (resolution, _, second) = two_observations();
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(resolution), &context())
        .await
        .unwrap()
    else {
        panic!("expected edges");
    };
    assert_eq!(output.edges.len(), 1);
    assert_eq!(output.edges[0].last_seen_snapshot_id, Some(second));
    assert_eq!(output.edges[0].first_seen_snapshot_id, Some(second));
}

#[tokio::test]
async fn missing_duplicate_or_unbound_evidence_fails_before_discovery() {
    for case in 0..4 {
        let (mut resolution, _, _) = two_observations();
        match case {
            0 => resolution.observed_properties = Default::default(),
            1 => {
                let duplicate = resolution.observed_properties[0].clone();
                Arc::make_mut(&mut resolution.observed_properties).push(duplicate);
            }
            2 => {
                Arc::make_mut(&mut resolution.observed_properties)[0].snapshot_uuid = Uuid::new_v4()
            }
            _ => {
                let duplicate = resolution.nodes_unchanged[0].clone();
                Arc::make_mut(&mut resolution.nodes_unchanged).push(duplicate);
            }
        }
        assert!(
            matches!(
                ReferenceExtractionStage
                    .process(reference_input(resolution), &context())
                    .await,
                Err(StageError::StateValidation { .. })
            ),
            "case {case}"
        );
    }
}

fn ambiguous_resolution() -> NodeResolutionOutput {
    let (mut resolution, _, _) = two_observations();
    let targets = Arc::make_mut(resolution.chunk_entities.as_mut().unwrap());
    let mut other = targets
        .iter()
        .find(|target| target.name == "cluster-target")
        .unwrap()
        .clone();
    other.chain_id = Uuid::new_v4();
    targets.push(other);
    targets.sort_by_key(|target| target.chain_id);
    resolution
}

#[tokio::test]
async fn ambiguous_references_are_resolved_only_by_the_resolution_stage() {
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    // 0: accept an offered candidate; 1: reject; 2: accept a target that was never offered.
    for case in 0..3 {
        let resolution = ambiguous_resolution();
        let mut ctx = context();
        ctx.graph = targets_graph(resolution.chunk_entities.as_ref().unwrap());
        ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
            edge_ambiguity: EdgeAmbiguityMode::Llm,
            ..Default::default()
        }));
        let extracted = ReferenceExtractionStage
            .process(reference_input(resolution), &ctx)
            .await
            .unwrap();
        let StageOutput::EdgeExtraction(ref output) = extracted else {
            panic!()
        };
        assert_eq!(output.pending_references.len(), 1);
        assert!(output.edges.is_empty());
        // Extraction counts the ambiguous reference as unresolved until the
        // resolution stage decides it.
        assert_eq!(output.reference_report.unresolved, 1);
        assert_eq!(output.reference_report.confirmed, 0);
        let chosen = output.pending_references[0].candidates[1].target.chain_id;
        let response = match case {
            0 => accept_answer(chosen),
            1 => REJECT_ANSWER.into(),
            _ => accept_answer(Uuid::new_v4()),
        };
        let backend = Arc::new(kg_core::test_support::MockLlmBackend::with_responses(vec![
            response,
        ]));
        ctx.llm_disambiguation = backend.clone();
        let result = super::super::ReferenceResolutionStage
            .process(extracted, &ctx)
            .await;
        assert_eq!(backend.call_count(), 1, "exactly one decision call");
        if case == 2 {
            assert!(result.is_err(), "an unoffered target is never accepted");
            continue;
        }
        let StageOutput::EdgeExtraction(output) = result.unwrap() else {
            panic!()
        };
        assert!(output.pending_references.is_empty());
        let confirmed = usize::from(case == 0);
        assert_eq!(output.edges.len(), confirmed);
        // A reference the resolution stage confirms moves from unresolved to
        // confirmed in the receipt counts; a rejection stays unresolved.
        assert_eq!(output.reference_report.confirmed, confirmed);
        assert_eq!(output.reference_report.unresolved, 1 - confirmed);
        assert_eq!(
            output.reference_report.decisions.len(),
            1,
            "every occurrence leaves an audit"
        );
        if confirmed == 1 {
            assert_eq!(output.edges[0].target_chain_id, chosen);
            let evidence = output.edges[0].reference_evidence.as_ref().unwrap();
            assert_eq!(
                evidence.decision.as_ref().unwrap().decision_id,
                output.reference_report.decisions[0].decision_id
            );
            assert!(evidence
                .read_set
                .iter()
                .all(|read| read.observed_at.is_some()));
        }
    }
}

#[tokio::test]
async fn unconfirmed_references_are_recorded_as_durable_unresolved_decisions() {
    // R4: a scalar reference whose value matches no target is recorded as a
    // durable `target-not-found` decision so a later target can find the waiting
    // source; a confirmed reference is not recorded as unresolved.
    let (mut resolution, _first, _second) = two_observations();
    // The source's current properties come from the original observations, so the
    // unresolved reference must be injected there (not only on the resolved node).
    for original in Arc::make_mut(&mut resolution.observed_properties) {
        original.properties.insert(
            "missing_ref".into(),
            PropertyValue::String("nonexistent-xyz".into()),
        );
    }
    let output = ReferenceExtractionStage
        .process(reference_input(resolution), &context())
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = output else {
        panic!("edge output required")
    };
    let slots = &output.reference_report.unresolved_slots;
    let missing = slots
        .iter()
        .find(|slot| slot.slot.ends_with(".missing_ref"))
        .expect("target-not-found reference is recorded");
    assert_eq!(missing.entries.len(), 1);
    assert_eq!(missing.entries[0].token, "s:nonexistent-xyz");
    assert_eq!(missing.entries[0].reason, "target-not-found");
    // The confirmed cluster_name reference emits an empty-entry clear
    // (clear-on-confirm), retracting any prior run's durable unresolved record.
    let confirmed = slots
        .iter()
        .find(|slot| slot.slot.ends_with(".cluster_name"))
        .expect("a confirmed reference emits an empty-entry clear");
    assert!(
        confirmed.entries.is_empty(),
        "a confirmed slot carries no unresolved entries"
    );
}

#[tokio::test]
async fn a_keyed_array_slot_keeps_its_unmatched_elements_when_others_confirm() {
    // Clear-on-confirm must not over-clear: in one list slot where one element
    // confirms and another matches no target, the slot stays recorded carrying
    // only the still-unresolved token, so a target appearing later still finds the
    // source waiting on it. A confirming element never erases an unresolved sibling.
    let mut source = typed_entity(
        "Server",
        "server-1",
        "prod",
        vec!["id"],
        vec![],
        &[("id", "server-1")],
    );
    source.all_properties.insert(
        "subnet_id".into(),
        PropertyValue::StringList(vec!["subnet-a".into(), "subnet-missing".into()]),
    );
    let subnet = typed_entity(
        "Subnet",
        "subnet-a",
        "prod",
        vec!["subnet_id"],
        vec![],
        &[("subnet_id", "subnet-a")],
    );
    let subnet_chain = subnet.chain_id;
    let targets = vec![observation_target(&source), observation_target(&subnet)];
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(
            reference_input(guided_resolution(source, targets)),
            &context(),
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    // The present element confirmed; the missing one did not.
    assert_eq!(output.edges.len(), 1, "only the present element confirms");
    assert_eq!(output.edges[0].target_chain_id, subnet_chain);
    let slot = output
        .reference_report
        .unresolved_slots
        .iter()
        .find(|slot| slot.slot == "Server.subnet_id")
        .expect("the slot stays recorded for the missing element");
    assert_eq!(slot.entries.len(), 1, "only the unresolved element remains");
    assert_eq!(slot.entries[0].token, "s:subnet-missing");
    assert_eq!(slot.entries[0].reason, "target-not-found");
}

#[tokio::test]
async fn a_deterministic_edge_does_not_suppress_an_ambiguous_reference_on_the_same_source() {
    // A source can carry two distinct references, one matching
    // exactly one target (a deterministic edge, built in extraction) and one
    // matching two targets (ambiguous). The confirmed deterministic edge must not
    // swallow the ambiguous reference; it still reaches the resolution stage as a
    // pending reference and is decided there, independently.
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    let source = typed_entity(
        "Server",
        "server-1",
        "prod",
        vec!["id"],
        vec![],
        &[
            ("id", "server-1"),
            ("subnet_id", "subnet-a"),
            ("peer_id", "peer-x"),
        ],
    );
    // Deterministic target: the sole Subnet whose key equals `subnet-a`.
    let subnet = typed_entity(
        "Subnet",
        "subnet-a",
        "prod",
        vec!["subnet_id"],
        vec![],
        &[("subnet_id", "subnet-a")],
    );
    let subnet_chain = subnet.chain_id;
    // Ambiguous targets: two distinct Peer chains both keyed on `peer-x`.
    let peer = typed_entity(
        "Peer",
        "peer-x",
        "prod",
        vec!["peer_id"],
        vec![],
        &[("peer_id", "peer-x")],
    );
    let mut peer_twin = observation_target(&peer);
    peer_twin.chain_id = Uuid::new_v4();
    peer_twin.version_uuid = Uuid::new_v4();
    let targets = vec![
        observation_target(&source),
        observation_target(&subnet),
        observation_target(&peer),
        peer_twin,
    ];
    let mut ctx = context();
    ctx.graph = targets_graph(&targets);
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    let extracted = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(ref output) = extracted else {
        panic!()
    };
    let backend = Arc::new(kg_core::test_support::MockLlmBackend::with_responses(vec![
        accept_answer(output.pending_references[0].candidates[0].target.chain_id),
    ]));
    ctx.llm_disambiguation = backend.clone();
    // The deterministic Subnet edge is built now; the ambiguous Peer reference is
    // not suppressed by it — it is queued for the resolution stage, no model call.
    assert_eq!(
        output.edges.len(),
        1,
        "one deterministic edge at extraction"
    );
    assert_eq!(output.edges[0].target_chain_id, subnet_chain);
    assert_eq!(
        output.pending_references.len(),
        1,
        "the ambiguous peer reference survives as pending"
    );
    assert_eq!(backend.call_count(), 0, "extraction makes no model call");
    // The resolution stage decides the surviving ambiguous reference on its own,
    // leaving the deterministic edge intact.
    let StageOutput::EdgeExtraction(output) = super::super::ReferenceResolutionStage
        .process(extracted, &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        backend.call_count(),
        1,
        "one decision for the ambiguous slot"
    );
    assert!(output.pending_references.is_empty());
    assert_eq!(
        output.edges.len(),
        2,
        "the deterministic edge plus the model-resolved peer"
    );
    assert!(
        output
            .edges
            .iter()
            .any(|edge| edge.target_chain_id == subnet_chain),
        "the deterministic edge is preserved through resolution"
    );
}

#[tokio::test]
async fn cross_type_targets_sharing_a_key_value_are_one_ambiguous_reference() {
    // Two live targets of DIFFERENT types whose single-field keys hold the same
    // value (a CmdbGroup keyed by group_id and a CmdbPerson keyed by user_id, both
    // "atlas") each complete their key group for a source reference of that value.
    // Because key hashes are type-scoped, identity resolution keeps them distinct,
    // so the reference is genuinely ambiguous (two candidates) and routes to the
    // model rather than being guessed — the shape the real-model eval relies on.
    let source = typed_entity(
        "CmdbChange",
        "CHG0100",
        "acme",
        vec!["number"],
        vec![],
        &[("number", "CHG0100"), ("owning_group", "atlas")],
    );
    let group = typed_entity(
        "CmdbGroup",
        "Atlas Platform",
        "acme",
        vec!["group_id"],
        vec![],
        &[("group_id", "atlas")],
    );
    let person = typed_entity(
        "CmdbPerson",
        "Atlas Rivera",
        "acme",
        vec!["user_id"],
        vec![],
        &[("user_id", "atlas")],
    );
    let targets = vec![
        observation_target(&source),
        observation_target(&group),
        observation_target(&person),
    ];
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    let mut ctx = context();
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(
        output.edges.is_empty(),
        "an ambiguous cross-type reference is never a deterministic edge"
    );
    assert_eq!(
        output.pending_references.len(),
        1,
        "one pending reference for the owning_group slot"
    );
    let types: BTreeSet<&str> = output.pending_references[0]
        .candidates
        .iter()
        .map(|c| c.target.entity_type.as_str())
        .collect();
    assert_eq!(
        types,
        BTreeSet::from(["CmdbGroup", "CmdbPerson"]),
        "both same-valued cross-type targets are candidates for the model"
    );
}

#[tokio::test]
async fn pending_reference_cannot_bypass_disabled_policy() {
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    let backend = Arc::new(kg_core::test_support::MockLlmBackend::with_responses(
        vec![],
    ));
    let mut ctx = context();
    ctx.llm_disambiguation = backend.clone();
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    let input = ReferenceExtractionStage
        .process(reference_input(ambiguous_resolution()), &ctx)
        .await
        .unwrap();
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy::default()));
    assert!(super::super::ReferenceResolutionStage
        .process(input, &ctx)
        .await
        .is_err());
    assert_eq!(backend.call_count(), 0);
}

#[tokio::test]
async fn namespace_filter_and_duplicate_chains_do_not_create_false_ambiguity() {
    let mut ctx = context();
    ctx.namespace_policy = Arc::new(kg_core::tenant::NamespacePolicy {
        open_policy: false,
        environment_tiers: vec![],
        cross_namespace_rules: vec![],
    });
    let mut resolution = ambiguous_resolution();
    let targets = Arc::make_mut(resolution.chunk_entities.as_mut().unwrap());
    let target = targets
        .iter_mut()
        .find(|target| target.name == "cluster-target")
        .unwrap();
    target.namespace = "forbidden".into();
    let duplicate = targets
        .iter()
        .find(|target| target.name == "cluster-target" && target.namespace != "forbidden")
        .unwrap()
        .clone();
    targets.push(duplicate);
    targets.sort_by_key(|target| target.chain_id);
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(resolution), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.edges.len(), 1);
    assert!(output.pending_references.is_empty());
}

#[tokio::test]
async fn declared_edges_survive_reference_stages_without_model_work() {
    let (mut resolution, _, _) = two_observations();
    for original in Arc::make_mut(&mut resolution.observed_properties) {
        original.properties.clear();
    }
    let source = &resolution.live_entities()[0];
    let mut declared = build_fk_edge(
        source,
        Uuid::new_v4(),
        "Cluster",
        "cluster-target",
        "cluster_name",
        "name",
        false,
    )
    .unwrap();
    // A declared relationship carries no reference evidence and is never rebuilt.
    declared.origin = kg_core::models::edges::RelationshipOrigin::Declared;
    declared.discovered_by = None;
    declared.reference_evidence = None;
    let edge_id = declared.uuid;
    resolution.sub_edges = Arc::new(vec![declared]);
    let extracted = ReferenceExtractionStage
        .process(reference_input(resolution), &context())
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = super::super::ReferenceResolutionStage
        .process(extracted, &context())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.edges.len(), 1);
    assert_eq!(output.edges[0].uuid, edge_id);
    assert!(output.pending_references.is_empty());
}

#[tokio::test]
async fn source_display_name_collision_is_never_an_unambiguous_foreign_reference() {
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    for policy in [EdgeAmbiguityMode::Skip, EdgeAmbiguityMode::Llm] {
        let (mut resolution, _, _) = two_observations();
        Arc::make_mut(resolution.chunk_entities.as_mut().unwrap())
            .iter_mut()
            .find(|target| target.entity_type == "Cluster")
            .unwrap()
            .key_groups
            .clear();
        for observation in Arc::make_mut(&mut resolution.nodes_unchanged) {
            observation.value.name = "cluster-target".into();
            observation.value.primary_key_properties.clear();
        }
        let backend = Arc::new(kg_core::test_support::MockLlmBackend::empty());
        let mut ctx = context();
        ctx.llm_disambiguation = backend.clone();
        ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
            edge_ambiguity: policy,
            ..Default::default()
        }));
        let extracted = ReferenceExtractionStage
            .process(reference_input(resolution), &ctx)
            .await
            .unwrap();
        let StageOutput::EdgeExtraction(output) = &extracted else {
            panic!()
        };
        assert!(output.edges.is_empty());
        assert_eq!(backend.call_count(), 0);
        assert!(output.pending_references.is_empty());
        let StageOutput::EdgeExtraction(output) = super::super::ReferenceResolutionStage
            .process(extracted, &ctx)
            .await
            .unwrap()
        else {
            panic!()
        };
        assert!(output.edges.is_empty());
        assert_eq!(backend.call_count(), 0);
    }
}

#[test]
fn source_identity_properties_are_available_for_join_entity_references() {
    let mut source = crate::node::entity_versioning::tests::test_entity("display-name");
    source.primary_key_properties = vec!["id".into()];
    source.additional_key_properties =
        vec![vec!["alias".into()], vec!["region".into(), "name".into()]];
    for (key, value) in [
        ("id", "resource-id"),
        ("alias", "alias-id"),
        ("region", "us-east-1"),
    ] {
        source
            .all_properties
            .insert(key.into(), PropertyValue::String(value.into()));
    }
    let ctx = context();
    let exclusions = HashMap::new();
    let excluded = excluded_properties(&ctx, &source, None, &exclusions);
    assert!(!excluded.contains(&"region"));
    assert!(!excluded.contains(&"id"));
    assert!(!excluded.contains(&"alias"));
}

#[tokio::test]
async fn composite_identity_components_still_discover_unique_foreign_targets() {
    for primary in [true, false] {
        let (mut resolution, _, _) = two_observations();
        for observation in Arc::make_mut(&mut resolution.nodes_unchanged) {
            let group = vec!["cluster_name".into(), "name".into()];
            if primary {
                observation.value.primary_key_properties = group;
            } else {
                observation.value.additional_key_properties = vec![group];
            }
        }
        let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
            .process(reference_input(resolution), &context())
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(output.edges.len(), 1);
        assert_eq!(
            output.edges[0].source_property.as_deref(),
            Some("cluster_name")
        );
        assert!(output.pending_references.is_empty());
    }
}

/// Build a single-source, in-run-targets resolution for guided-matching tests.
fn guided_resolution(source: EntityNode, targets: Vec<RelationshipTarget>) -> NodeResolutionOutput {
    use kg_core::runtime::stage_output::{Observed, ObservedEntityProperties};
    let snapshot = Uuid::new_v4();
    let observation = Uuid::new_v4();
    let mut source = source;
    source.last_seen_snapshot_id = Some(snapshot);
    let mut original = ObservedEntityProperties::from_entity(&source, snapshot);
    original.observation_uuid = observation;
    // `live_observations` keeps a source only if its chain is found in
    // `chunk_entities` by binary search, so the run targets must be sorted.
    let mut targets = targets;
    targets.sort_by_key(|target| target.chain_id);
    NodeResolutionOutput {
        relationship_changes: Default::default(),
        snapshot_nodes: Arc::new(vec![kg_core::models::SnapshotNode {
            uuid: snapshot,
            org_id: source.org_id.clone(),
            namespace: source.namespace.clone(),
            name: "observation".into(),
            source_description: None,
            data_type: kg_core::models::SnapshotDataType::Entities,
            snapshot_kind: kg_core::models::SnapshotKind::Incremental,
            sync_generation: None,
            complete: false,
            collection: None,
            source: source.source.clone(),
            content: None,
            captured_at: source.valid_from,
            entities: vec![],
            entity_edges: vec![],
            labels: vec![],
            tags: Default::default(),
            created_at: source.valid_from,
        }]),
        fk_exclusions: Arc::new(HashMap::new()),
        observed_properties: Arc::new(vec![original]),
        nodes_unchanged: Arc::new(vec![Observed::new(observation, source)]),
        chunk_entities: Some(Arc::new(targets)),
        ..Default::default()
    }
}

fn typed_entity(
    entity_type: &str,
    name: &str,
    namespace: &str,
    primary: Vec<&str>,
    additional: Vec<Vec<&str>>,
    properties: &[(&str, &str)],
) -> EntityNode {
    let mut node = crate::node::entity_versioning::tests::test_entity(name);
    node.entity_type = entity_type.into();
    node.name = name.into();
    node.namespace = namespace.into();
    node.source = "cloud".into();
    node.primary_key_properties = primary.into_iter().map(String::from).collect();
    node.additional_key_properties = additional
        .into_iter()
        .map(|group| group.into_iter().map(String::from).collect())
        .collect();
    node.all_properties.clear();
    for (key, value) in properties {
        node.all_properties
            .insert((*key).into(), PropertyValue::String((*value).into()));
    }
    node
}

#[tokio::test]
async fn nested_aws_reference_uses_scan_scope_and_links_context_nodes() {
    let mut nat = typed_entity(
        "AWS::EC2::NatGateway",
        "nat-1",
        "default",
        vec![
            "_astrolabe_scope.account_id",
            "_astrolabe_scope.region",
            "NatGatewayId",
        ],
        vec![],
        &[
            ("NatGatewayId", "nat-1"),
            ("_astrolabe_scope.account_id", "111122223333"),
            ("_astrolabe_scope.region", "us-east-1"),
        ],
    );
    nat.source = "aws".into();
    nat.all_properties.insert(
        "NatGatewayAddresses".into(),
        PropertyValue::Json(serde_json::json!([{"AllocationId":"eipalloc-1"}]).to_string()),
    );
    let mut eip = typed_entity(
        "AWS::EC2::EIP",
        "eipalloc-1",
        "default",
        vec![
            "_astrolabe_scope.account_id",
            "_astrolabe_scope.region",
            "AllocationId",
        ],
        vec![],
        &[
            ("AllocationId", "eipalloc-1"),
            ("_astrolabe_scope.account_id", "111122223333"),
            ("_astrolabe_scope.region", "us-east-1"),
        ],
    );
    eip.source = "aws".into();
    let account = typed_entity(
        "AWS::Account",
        "111122223333",
        "default",
        vec!["account_id"],
        vec![],
        &[("account_id", "111122223333")],
    );
    let region = typed_entity(
        "AWS::Region",
        "us-east-1",
        "default",
        vec!["region"],
        vec![],
        &[("region", "us-east-1")],
    );
    let namespace = typed_entity(
        "Astrolabe::Namespace",
        "default",
        "default",
        vec!["name"],
        vec![],
        &[],
    );
    let targets = vec![&nat, &eip, &account, &region, &namespace]
        .into_iter()
        .map(RelationshipTarget::from_node)
        .collect();
    let resolution = guided_resolution(nat, targets);
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(resolution), &context())
        .await
        .unwrap()
    else {
        panic!()
    };
    let linked_types: std::collections::HashSet<_> = output
        .edges
        .iter()
        .map(|edge| edge.target_chain_id)
        .collect();
    assert_eq!(linked_types.len(), 4);
    assert!(output.edges.iter().all(|edge| edge.name == "RELATES_TO"));
    assert!(
        output
            .edges
            .iter()
            .any(|edge| edge.source_property.as_deref()
                == Some("NatGatewayAddresses[0].AllocationId"))
    );
}

#[test]
fn explicit_foreign_account_does_not_borrow_the_scan_account() {
    let mut source = typed_entity(
        "AWS::EC2::NatGateway",
        "nat-1",
        "default",
        vec!["NatGatewayId"],
        vec![],
        &[
            ("_astrolabe_scope.account_id", "111122223333"),
            ("_astrolabe_scope.region", "us-east-1"),
        ],
    );
    source.source = "aws".into();
    let target = typed_entity(
        "AWS::EC2::EIP",
        "eipalloc-1",
        "default",
        vec![
            "_astrolabe_scope.account_id",
            "_astrolabe_scope.region",
            "AllocationId",
        ],
        vec![],
        &[
            ("_astrolabe_scope.account_id", "111122223333"),
            ("_astrolabe_scope.region", "us-east-1"),
            ("AllocationId", "eipalloc-1"),
        ],
    );
    let observation = Observation {
        location: "NatGatewayAddresses[0].AllocationId".into(),
        token: "s:eipalloc-1".into(),
        canonical: "eipalloc-1".into(),
        component: "AllocationId".into(),
        from_collection: true,
        context: Arc::new(vec![("AccountId".into(), "s:999900001111".into())]),
    };
    assert!(complete_group_match(
        &source,
        &RelationshipTarget::from_node(&target),
        &observation
    )
    .is_none());
}

#[tokio::test]
async fn account_and_region_link_to_their_namespace() {
    let namespace = typed_entity(
        "Astrolabe::Namespace",
        "default",
        "default",
        vec!["name"],
        vec![],
        &[],
    );
    for (entity_type, field, value) in [
        ("AWS::Account", "account_id", "111122223333"),
        ("AWS::Region", "region", "us-east-1"),
    ] {
        let mut source = typed_entity(
            entity_type,
            value,
            "default",
            vec![field],
            vec![],
            &[(field, value)],
        );
        source.source = "aws".into();
        source.labels.push("astrolabe:scope".into());
        let targets = vec![&source, &namespace]
            .into_iter()
            .map(RelationshipTarget::from_node)
            .collect();
        let resolution = guided_resolution(source, targets);
        let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
            .process(reference_input(resolution), &context())
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(output.edges.len(), 1);
        assert_eq!(output.edges[0].name, "RELATES_TO");
        assert_eq!(
            output.edges[0].source_property.as_deref(),
            Some("_astrolabe_namespace")
        );
    }
}

#[tokio::test]
async fn guided_case_insensitive_composite_confirms_via_the_guided_channel() {
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    // A per-provider case-insensitive contract (Azure resource groups) is the ONLY
    // way case-folding enters: the source's `myrg` folds to the subnet's `MyRG`
    // while the other three components match exactly, completing the alt key group.
    let source = typed_entity(
        "AzureWebApp",
        "web-app-2",
        "sub-1111",
        vec!["id"],
        vec![],
        &[
            ("id", "/subscriptions/sub-1111/sites/web-app-2"),
            ("subnet_ref.subscription_id", "sub-1111"),
            ("subnet_ref.resource_group", "myrg"),
            ("subnet_ref.vnet", "vnet-a"),
            ("subnet_ref.name", "app-subnet"),
        ],
    );
    let subnet = typed_entity(
        "AzureSubnet",
        "app-subnet",
        "sub-1111",
        vec!["id"],
        vec![vec!["subscription_id", "resource_group", "vnet", "name"]],
        &[
            ("id", "/subscriptions/sub-1111/subnets/app-subnet"),
            ("subscription_id", "sub-1111"),
            ("resource_group", "MyRG"),
            ("vnet", "vnet-a"),
            ("name", "app-subnet"),
        ],
    );
    let source_chain = source.chain_id;
    let subnet_chain = subnet.chain_id;
    let targets = vec![observation_target(&source), observation_target(&subnet)];

    let mut ctx = context();
    ctx.extraction_settings.reference_guidance.insert(
        "cloud".into(),
        vec![ReferenceMapping {
            source_namespace: None,
            source_entity_type: "AzureWebApp".into(),
            reference_path: "subnet_ref".into(),
            context_paths: BTreeMap::new(),
            target_type: "AzureSubnet".into(),
            target_key_group: vec![
                "subscription_id".into(),
                "resource_group".into(),
                "vnet".into(),
                "name".into(),
            ],
            shape: ReferenceShape::Object,
            direction: ReferenceDirection::SourceToTarget,
            relationship_name: "IN_SUBNET".into(),
            qualifiers: None,
            cardinality: ReferenceCardinality::One,
            case_insensitive_types: vec!["AzureSubnet".into()],
        }],
    );

    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.edges.len(), 1, "one guided edge");
    let edge = &output.edges[0];
    assert_eq!(edge.name, "IN_SUBNET");
    assert_eq!(edge.source_chain_id, source_chain);
    assert_eq!(edge.target_chain_id, subnet_chain);
    assert_eq!(edge.discovered_by.as_deref(), Some("guided_fk"));
    assert_eq!(
        edge.reference_evidence.as_ref().unwrap().slot,
        "AzureWebApp.subnet_ref"
    );
    assert!(
        edge.cardinality_key.is_some(),
        "cardinality one has a slot key"
    );
    assert!(output.pending_references.is_empty());
    // A confirmed guided reference emits an empty-entry clear (clear-on-confirm),
    // never a lingering unresolved decision.
    let confirmed = output
        .reference_report
        .unresolved_slots
        .iter()
        .find(|slot| slot.slot == "AzureWebApp.subnet_ref")
        .expect("a confirmed guided reference emits an empty-entry clear");
    assert!(
        confirmed.entries.is_empty(),
        "a confirmed slot carries no unresolved entries"
    );
}

#[tokio::test]
async fn guided_context_paths_complete_a_composite_from_declared_source_paths() {
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    // The reference object supplies `code`; guidance supplies `region` from the
    // source's own scope (`raw:namespace`), completing the target composite. This
    // is provider-neutral: only declared source paths, no packed-string parsing.
    let source = typed_entity(
        "Widget",
        "widget-1",
        "tenant-9",
        vec!["id"],
        vec![],
        &[("id", "widget-1"), ("link.code", "xyz")],
    );
    let gadget = typed_entity(
        "Gadget",
        "gadget-a",
        "tenant-9",
        vec!["id"],
        vec![vec!["region", "code"]],
        &[("id", "gadget-a"), ("region", "tenant-9"), ("code", "xyz")],
    );
    let source_chain = source.chain_id;
    let gadget_chain = gadget.chain_id;
    let targets = vec![observation_target(&source), observation_target(&gadget)];

    let mut ctx = context();
    ctx.extraction_settings.reference_guidance.insert(
        "cloud".into(),
        vec![ReferenceMapping {
            source_namespace: None,
            source_entity_type: "Widget".into(),
            reference_path: "link".into(),
            context_paths: BTreeMap::from([("region".into(), "raw:namespace".into())]),
            target_type: "Gadget".into(),
            target_key_group: vec!["region".into(), "code".into()],
            shape: ReferenceShape::Object,
            direction: ReferenceDirection::SourceToTarget,
            relationship_name: "USES".into(),
            qualifiers: None,
            cardinality: ReferenceCardinality::One,
            case_insensitive_types: vec![],
        }],
    );

    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        output.edges.len(),
        1,
        "context_paths completed the composite"
    );
    let edge = &output.edges[0];
    assert_eq!(edge.name, "USES");
    assert_eq!(edge.source_chain_id, source_chain);
    assert_eq!(edge.target_chain_id, gadget_chain);
    assert_eq!(
        edge.reference_evidence.as_ref().unwrap().slot,
        "Widget.link"
    );
}

#[tokio::test]
async fn guided_list_keeps_element_locations_and_one_owner_slot() {
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    let mut source = typed_entity(
        "Server",
        "server-1",
        "prod",
        vec!["id"],
        vec![],
        &[("id", "server-1")],
    );
    source.all_properties.insert(
        "subnets".into(),
        PropertyValue::StringList(vec!["subnet-a".into(), "subnet-b".into()]),
    );
    let a = typed_entity(
        "Subnet",
        "a",
        "prod",
        vec!["subnet_id"],
        vec![],
        &[("subnet_id", "subnet-a")],
    );
    let b = typed_entity(
        "Subnet",
        "b",
        "prod",
        vec!["subnet_id"],
        vec![],
        &[("subnet_id", "subnet-b")],
    );
    let targets = vec![
        observation_target(&source),
        observation_target(&a),
        observation_target(&b),
    ];
    let ctx = one_guidance(ReferenceMapping {
        source_namespace: None,
        source_entity_type: "Server".into(),
        reference_path: "subnets[]".into(),
        context_paths: BTreeMap::new(),
        target_type: "Subnet".into(),
        target_key_group: vec!["subnet_id".into()],
        shape: ReferenceShape::List,
        direction: ReferenceDirection::SourceToTarget,
        relationship_name: "IN_SUBNET".into(),
        qualifiers: None,
        cardinality: ReferenceCardinality::Many,
        case_insensitive_types: vec![],
    });
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.edges.len(), 2);
    let mut locations: Vec<_> = output
        .edges
        .iter()
        .map(|edge| {
            let evidence = edge.reference_evidence.as_ref().unwrap();
            assert_eq!(evidence.slot, "Server.subnets");
            assert!(edge.cardinality_key.is_none());
            evidence.location.as_str()
        })
        .collect();
    locations.sort();
    assert_eq!(locations, ["subnets[0]", "subnets[1]"]);
}

#[tokio::test]
async fn guided_object_array_never_combines_composite_components_between_elements() {
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    let mut source = typed_entity(
        "Deployment",
        "api",
        "prod",
        vec!["id"],
        vec![],
        &[("id", "api")],
    );
    source.all_properties.insert(
        "targets".into(),
        PropertyValue::Json(
            r#"[{"region":"east","code":"a"},{"region":"west","code":"b"}]"#.into(),
        ),
    );
    let target = typed_entity(
        "Endpoint",
        "east-a",
        "prod",
        vec!["region", "code"],
        vec![],
        &[("region", "east"), ("code", "a")],
    );
    let targets = vec![observation_target(&source), observation_target(&target)];
    let ctx = one_guidance(ReferenceMapping {
        source_namespace: None,
        source_entity_type: "Deployment".into(),
        reference_path: "targets[]".into(),
        context_paths: BTreeMap::new(),
        target_type: "Endpoint".into(),
        target_key_group: vec!["region".into(), "code".into()],
        shape: ReferenceShape::Object,
        direction: ReferenceDirection::SourceToTarget,
        relationship_name: "DEPLOYS_TO".into(),
        qualifiers: None,
        cardinality: ReferenceCardinality::Many,
        case_insensitive_types: vec![],
    });
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.edges.len(), 1);
    assert_eq!(
        output.edges[0]
            .reference_evidence
            .as_ref()
            .unwrap()
            .location,
        "targets[0]"
    );
    assert!(output.reference_report.unresolved > 0);
}

#[tokio::test]
async fn model_confirmation_preserves_guided_name_direction_and_cardinality() {
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    let source = typed_entity(
        "Workload",
        "api",
        "prod",
        vec!["id"],
        vec![],
        &[("id", "api"), ("owner", "team-a")],
    );
    let a = typed_entity(
        "Team",
        "alpha",
        "prod",
        vec!["slug"],
        vec![],
        &[("slug", "team-a")],
    );
    let b = typed_entity(
        "Team",
        "also-alpha",
        "prod",
        vec!["slug"],
        vec![],
        &[("slug", "team-a")],
    );
    let source_chain = source.chain_id;
    let selected_chain = [a.chain_id, b.chain_id].into_iter().min().unwrap();
    let targets = vec![
        observation_target(&source),
        observation_target(&a),
        observation_target(&b),
    ];
    let mut ctx = one_guidance(ReferenceMapping {
        source_namespace: None,
        source_entity_type: "Workload".into(),
        reference_path: "owner".into(),
        context_paths: BTreeMap::new(),
        target_type: "Team".into(),
        target_key_group: vec!["slug".into()],
        shape: ReferenceShape::Scalar,
        direction: ReferenceDirection::Inverse,
        relationship_name: "OWNS".into(),
        qualifiers: None,
        cardinality: ReferenceCardinality::One,
        case_insensitive_types: vec![],
    });
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    ctx.graph = targets_graph(&targets);
    ctx.llm_disambiguation = Arc::new(kg_core::test_support::MockLlmBackend::with_responses(vec![
        accept_answer(selected_chain),
    ]));
    let extracted = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = super::super::ReferenceResolutionStage
        .process(extracted, &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.edges.len(), 1);
    let edge = &output.edges[0];
    assert_eq!(edge.name, "OWNS");
    assert_eq!(edge.source_chain_id, selected_chain);
    assert_eq!(edge.target_chain_id, source_chain);
    assert!(edge.cardinality_key.is_some());
    assert_eq!(
        edge.reference_evidence.as_ref().unwrap().slot,
        "Workload.owner"
    );
}

#[test]
fn two_children_referencing_one_parent_own_independent_slots() {
    // R2: each child owns its own single-target slot keyed by (source chain,
    // relationship, location). Two children pointing at the same parent never
    // collide, and retargeting one child to a different parent keeps its slot so
    // retirement/retarget planning still keys on the source, not the endpoint.
    let parent = Uuid::new_v4();
    let child_a = typed_entity(
        "Server",
        "srv-a",
        "prod",
        vec!["id"],
        vec![],
        &[("id", "srv-a"), ("rack_id", "rack-1")],
    );
    let child_b = typed_entity(
        "Server",
        "srv-b",
        "prod",
        vec!["id"],
        vec![],
        &[("id", "srv-b"), ("rack_id", "rack-1")],
    );
    let edge_a =
        build_fk_edge(&child_a, parent, "Rack", "rack-1", "rack_id", "name", false).unwrap();
    let edge_b =
        build_fk_edge(&child_b, parent, "Rack", "rack-1", "rack_id", "name", false).unwrap();
    assert_eq!(edge_a.target_chain_id, parent);
    assert_eq!(edge_b.target_chain_id, parent);
    assert!(edge_a.cardinality_key.is_some() && edge_b.cardinality_key.is_some());
    assert_ne!(
        edge_a.cardinality_key, edge_b.cardinality_key,
        "each child owns its own slot"
    );
    // Independent retargeting: same child, same location, different parent → same
    // owner slot, different endpoint.
    let other_parent = Uuid::new_v4();
    let edge_a2 = build_fk_edge(
        &child_a,
        other_parent,
        "Rack",
        "rack-9",
        "rack_id",
        "name",
        false,
    )
    .unwrap();
    assert_eq!(
        edge_a.cardinality_key, edge_a2.cardinality_key,
        "retarget keeps the source owner slot"
    );
    assert_ne!(edge_a.target_chain_id, edge_a2.target_chain_id);
}

#[test]
fn literal_dotted_and_slashed_reference_keys_are_addressable() {
    use kg_core::runtime::extraction::ReferencePath;
    // Segments split only on an unescaped '.', so a literal dot inside a key is
    // backslash-escaped; a '/' is an ordinary key character needing no escape.
    let dotted = ReferencePath::parse(r"labels.app\.kubernetes\.io/name").unwrap();
    assert_eq!(
        dotted
            .segments
            .iter()
            .map(|s| s.key.as_str())
            .collect::<Vec<_>>(),
        vec!["labels", "app.kubernetes.io/name"]
    );
    let slashed = ReferencePath::parse("spec/selector").unwrap();
    assert_eq!(slashed.segments.len(), 1);
    assert_eq!(slashed.segments[0].key, "spec/selector");
    // Array indexes never enter the durable slot; an escaped dot is preserved.
    assert_eq!(
        reference_slot("Widget", r"items[3].id\.raw"),
        r"Widget.items.id\.raw"
    );
}

/// One guided mapping for the two provider-neutral guided tests below.
fn one_guidance(mapping: kg_core::runtime::extraction::ReferenceMapping) -> RuntimeContext {
    let mut ctx = context();
    ctx.extraction_settings
        .reference_guidance
        .insert("cloud".into(), vec![mapping]);
    ctx
}

#[tokio::test]
async fn guided_cross_namespace_ownership_respects_policy() {
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    // The source (prod) references a target that lives in another namespace. The
    // guided pass applies the run's namespace policy exactly like the generic pass:
    // forbidden by default (no edge, a durable `target-not-found`), permitted only
    // when the policy allows the cross-namespace link.
    let mapping = || ReferenceMapping {
        source_namespace: None,
        source_entity_type: "WebApp".into(),
        reference_path: "subnet".into(),
        context_paths: BTreeMap::new(),
        target_type: "Subnet".into(),
        target_key_group: vec!["subnet_id".into()],
        shape: ReferenceShape::Scalar,
        direction: ReferenceDirection::SourceToTarget,
        relationship_name: "IN_SUBNET".into(),
        qualifiers: None,
        cardinality: ReferenceCardinality::One,
        case_insensitive_types: vec![],
    };
    let build = || {
        let source = typed_entity(
            "WebApp",
            "app-1",
            "prod",
            vec!["id"],
            vec![],
            &[("id", "app-1"), ("subnet", "subnet-xyz")],
        );
        let subnet = typed_entity(
            "Subnet",
            "subnet-a",
            "staging",
            vec!["id"],
            vec![vec!["subnet_id"]],
            &[("id", "subnet-a"), ("subnet_id", "subnet-xyz")],
        );
        let targets = vec![observation_target(&source), observation_target(&subnet)];
        guided_resolution(source, targets)
    };

    // Guided failures remain durable even when generic bookkeeping is disabled.
    let mut forbidden_ctx = one_guidance(mapping());
    forbidden_ctx.policy = Arc::new(kg_core::policy::PolicyResolver::new(
        kg_core::policy::PipelinePolicy {
            record_generic_unresolved: false,
            ..Default::default()
        },
    ));
    let StageOutput::EdgeExtraction(forbidden) = ReferenceExtractionStage
        .process(reference_input(build()), &forbidden_ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(forbidden.edges.is_empty(), "cross-namespace link is denied");
    assert!(forbidden
        .reference_report
        .unresolved_slots
        .iter()
        .any(|slot| slot.slot == "WebApp.subnet"
            && !slot.entries.is_empty()
            && slot.entries.iter().all(|e| e.reason == "target-not-found")));

    // Permitted: an open policy allows the cross-namespace link.
    let mut ctx = one_guidance(mapping());
    ctx.namespace_policy = Arc::new(kg_core::tenant::NamespacePolicy {
        open_policy: true,
        environment_tiers: vec![],
        cross_namespace_rules: vec![],
    });
    let StageOutput::EdgeExtraction(permitted) = ReferenceExtractionStage
        .process(reference_input(build()), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(permitted.edges.len(), 1, "open policy permits the link");
    assert_eq!(permitted.edges[0].name, "IN_SUBNET");
}

#[tokio::test]
async fn gcp_connector_normalized_reference_resolves_provider_neutrally() {
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    // GCP packs a subnetwork reference as a relative path
    // (`projects/p/regions/us-central1/subnetworks/app-subnet`). Per the frozen
    // architecture the CONNECTOR decomposes that packed string into clean typed
    // fields in the ONE canonical pipeline-input schema; the engine never parses a
    // provider string. Given that clean shape, the same provider-neutral guided
    // matcher confirms the composite — no GCP branch anywhere in the engine.
    let instance = typed_entity(
        "GceInstance",
        "vm-1",
        "proj-1",
        vec!["id"],
        vec![],
        &[
            ("id", "vm-1"),
            // Connector-normalized fields (not a packed path):
            ("network_interface.subnetwork.project", "proj-1"),
            ("network_interface.subnetwork.region", "us-central1"),
            ("network_interface.subnetwork.name", "app-subnet"),
        ],
    );
    let subnetwork = typed_entity(
        "GceSubnetwork",
        "app-subnet",
        "proj-1",
        vec!["id"],
        vec![vec!["project", "region", "name"]],
        &[
            ("id", "app-subnet"),
            ("project", "proj-1"),
            ("region", "us-central1"),
            ("name", "app-subnet"),
        ],
    );
    let instance_chain = instance.chain_id;
    let subnetwork_chain = subnetwork.chain_id;
    let targets = vec![
        observation_target(&instance),
        observation_target(&subnetwork),
    ];
    let ctx = one_guidance(ReferenceMapping {
        source_namespace: None,
        source_entity_type: "GceInstance".into(),
        reference_path: "network_interface.subnetwork".into(),
        context_paths: BTreeMap::new(),
        target_type: "GceSubnetwork".into(),
        target_key_group: vec!["project".into(), "region".into(), "name".into()],
        shape: ReferenceShape::Object,
        direction: ReferenceDirection::SourceToTarget,
        relationship_name: "IN_SUBNETWORK".into(),
        qualifiers: None,
        cardinality: ReferenceCardinality::One,
        case_insensitive_types: vec![],
    });

    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(guided_resolution(instance, targets)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.edges.len(), 1, "one guided edge from clean fields");
    let edge = &output.edges[0];
    assert_eq!(edge.name, "IN_SUBNETWORK");
    assert_eq!(edge.source_chain_id, instance_chain);
    assert_eq!(edge.target_chain_id, subnetwork_chain);
    assert_eq!(edge.discovered_by.as_deref(), Some("guided_fk"));
    assert_eq!(
        edge.reference_evidence.as_ref().unwrap().slot,
        "GceInstance.network_interface.subnetwork"
    );
}

async fn reference_edges(
    resolution: NodeResolutionOutput,
    ctx: &RuntimeContext,
) -> Vec<kg_core::models::edges::EntityEdge> {
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(resolution), ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    output
        .edges
        .iter()
        .filter(|edge| edge.reference_evidence.is_some())
        .cloned()
        .collect()
}

#[tokio::test]
async fn confirmed_selection_is_independent_of_target_position() {
    // `chunk_entities` is the authoritative live set, canonically sorted by chain
    // id (`live_observations` binary-searches it) — that canonicalization is what
    // makes extraction chunk-order invariant. Interleaving unrelated decoy targets
    // around the real one must not change the single confirmed edge.
    let (mut resolution, _, _) = two_observations();
    let namespace = resolution.chunk_entities.as_ref().unwrap()[0]
        .namespace
        .clone();
    let decoys = (0..5).map(|i| RelationshipTarget {
        chain_id: Uuid::new_v4(),
        name: format!("decoy-{i}"),
        entity_type: "Unrelated".into(),
        namespace: namespace.clone(),
        version_uuid: Uuid::new_v4(),
        version: 1,
        key_groups: Vec::new(),
    });
    let mut targets: Vec<_> = resolution
        .chunk_entities
        .as_ref()
        .unwrap()
        .iter()
        .cloned()
        .chain(decoys)
        .collect();
    targets.sort_by_key(|target| target.chain_id);
    resolution.chunk_entities = Some(Arc::new(targets));
    let edges = reference_edges(resolution, &context()).await;
    assert_eq!(edges.len(), 1, "decoys do not add or drop edges");
    assert_eq!(
        edges[0].target_identity_field.as_deref(),
        Some("cluster_name")
    );
}

#[tokio::test]
async fn ambiguous_candidate_order_is_deterministic() {
    // Regardless of the order targets were inserted into the run index, ambiguous
    // candidates are returned in a stable chain-id order (a chain-keyed map), so
    // the resolution stage and prompts see a deterministic candidate list.
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    let mut ctx = context();
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(ambiguous_resolution()), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.pending_references.len(), 1);
    let candidates: Vec<Uuid> = output.pending_references[0]
        .candidates
        .iter()
        .map(|candidate| candidate.target.chain_id)
        .collect();
    assert_eq!(candidates.len(), 2);
    let mut sorted = candidates.clone();
    sorted.sort();
    assert_eq!(
        candidates, sorted,
        "candidates in deterministic chain-id order"
    );
}

#[tokio::test]
async fn stored_reference_lookup_failure_is_surfaced_not_silently_dropped() {
    // A failed candidate read must never look like "no competitors". The stage
    // errors so the reference is retried, never confirmed against a partial universe.
    let (resolution, _, _) = two_observations();
    let mut ctx = context();
    ctx.graph = Arc::new(LookupFailsGraph);
    assert!(matches!(
        ReferenceExtractionStage
            .process(reference_input(resolution), &ctx)
            .await,
        Err(StageError::StepFailed { .. })
    ));
}

use kg_core::{errors::BackendError, traits::*};
struct EmptyReferenceGraph;

/// The stored version hydration would read for one frozen target: its complete
/// key groups as typed properties, in declared group order.
fn record_for(target: &RelationshipTarget) -> EntityVersionRecord {
    let mut stored = serde_json::Map::new();
    for group in &target.key_groups {
        for component in &group.components {
            let value = match component.type_tag.as_str() {
                "i" => PropertyValue::Integer(component.value.parse().unwrap()),
                _ => PropertyValue::String(component.value.clone()),
            };
            kg_core::traits::property_codec::write_property(
                &mut stored,
                &component.property,
                Some(&value),
            );
        }
    }
    let groups: Vec<Vec<String>> = target
        .key_groups
        .iter()
        .map(|g| g.components.iter().map(|c| c.property.clone()).collect())
        .collect();
    stored.insert(
        "primary_key_properties".into(),
        serde_json::json!(groups.first().cloned().unwrap_or_default()),
    );
    stored.insert(
        "additional_key_properties".into(),
        serde_json::json!(serde_json::to_string(&groups.get(1..).unwrap_or(&[])).unwrap()),
    );
    stored.insert("labels".into(), serde_json::json!([]));
    EntityVersionRecord {
        uuid: target.version_uuid,
        chain_id: target.chain_id,
        version: target.version,
        is_latest: true,
        entity_type: target.entity_type.clone(),
        name: target.name.clone(),
        namespace: target.namespace.clone(),
        source: Some("cloud".into()),
        identity_hash: None,
        identity_hashes: vec![],
        structural_hash: None,
        valid_from: None,
        valid_to: None,
        deleted_at: None,
        last_seen_at: Some(Utc::now()),
        last_transition_at: None,
        sync_generation: None,
        collections: vec![],
        merged_into: None,
        embedding: None,
        stored,
    }
}

/// A graph that hydrates the run's targets and finds no stored candidates.
struct TargetsGraph(Vec<EntityVersionRecord>, std::sync::atomic::AtomicUsize);

fn targets_graph(targets: &[RelationshipTarget]) -> Arc<TargetsGraph> {
    Arc::new(TargetsGraph(
        targets.iter().map(record_for).collect(),
        std::sync::atomic::AtomicUsize::new(0),
    ))
}

fn accept_answer(target: Uuid) -> String {
    format!(
        r#"{{"decision":"accept","target_id":"{target}","fact":"the source field names the target","supporting_evidence_ids":["src:occ"]}}"#
    )
}
const REJECT_ANSWER: &str =
    r#"{"decision":"reject","target_id":null,"fact":null,"supporting_evidence_ids":[]}"#;

#[async_trait]
impl SearchBackend for TargetsGraph {}

#[async_trait]
impl GraphBackend for TargetsGraph {
    async fn identity_revisions(
        &self,
        org: &str,
        scopes: &[IdentityScope],
    ) -> Result<Vec<IdentityRevision>, BackendError> {
        EmptyReferenceGraph.identity_revisions(org, scopes).await
    }
    async fn apply_mutations(&self, org: &str, m: &[GraphMutation]) -> Result<(), BackendError> {
        EmptyReferenceGraph.apply_mutations(org, m).await
    }
    async fn register_run(&self, h: &RunHeader) -> Result<RunRegistration, BackendError> {
        EmptyReferenceGraph.register_run(h).await
    }
    async fn commit_batch(&self, b: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        EmptyReferenceGraph.commit_batch(b).await
    }
    async fn committed_batches(
        &self,
        org: &str,
        run: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        EmptyReferenceGraph.committed_batches(org, run).await
    }
    async fn find_entities(
        &self,
        _: &str,
        lookup: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        match lookup {
            EntityLookup::LatestByChain { chain_ids } => {
                self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(self
                    .0
                    .iter()
                    .filter(|record| chain_ids.contains(&record.chain_id))
                    .cloned()
                    .collect())
            }
            EntityLookup::LiveByKeyValue { .. } => Ok(vec![]),
            other => panic!("unexpected lookup {other:?}"),
        }
    }
    async fn find_edges(&self, org: &str, l: &EdgeLookup) -> Result<Vec<EdgeRecord>, BackendError> {
        EmptyReferenceGraph.find_edges(org, l).await
    }
    async fn health(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn connect(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
}

/// A backend whose reference-candidate read fails, to prove a lookup error is
/// surfaced rather than mistaken for an empty candidate universe.
struct LookupFailsGraph;

#[async_trait]
impl SearchBackend for LookupFailsGraph {}

#[async_trait]
impl GraphBackend for LookupFailsGraph {
    async fn identity_revisions(
        &self,
        org: &str,
        scopes: &[IdentityScope],
    ) -> Result<Vec<IdentityRevision>, BackendError> {
        EmptyReferenceGraph.identity_revisions(org, scopes).await
    }
    async fn find_reference_candidates(
        &self,
        _: &str,
        _: Vec<kg_core::traits::graph_reads::WantedKeyValue>,
    ) -> Result<kg_core::traits::graph_reads::ReferenceCandidates, BackendError> {
        Err(BackendError::Unavailable(
            "simulated reference lookup failure".into(),
        ))
    }
    async fn apply_mutations(&self, org: &str, m: &[GraphMutation]) -> Result<(), BackendError> {
        EmptyReferenceGraph.apply_mutations(org, m).await
    }
    async fn register_run(&self, h: &RunHeader) -> Result<RunRegistration, BackendError> {
        EmptyReferenceGraph.register_run(h).await
    }
    async fn commit_batch(&self, b: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        EmptyReferenceGraph.commit_batch(b).await
    }
    async fn committed_batches(
        &self,
        org: &str,
        run: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        EmptyReferenceGraph.committed_batches(org, run).await
    }
    async fn find_entities(
        &self,
        org: &str,
        lookup: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        EmptyReferenceGraph.find_entities(org, lookup).await
    }
    async fn find_edges(
        &self,
        org: &str,
        lookup: &EdgeLookup,
    ) -> Result<Vec<EdgeRecord>, BackendError> {
        EmptyReferenceGraph.find_edges(org, lookup).await
    }
    async fn health(&self) -> Result<(), BackendError> {
        EmptyReferenceGraph.health().await
    }
    async fn connect(&self) -> Result<(), BackendError> {
        EmptyReferenceGraph.connect().await
    }
    async fn close(&self) -> Result<(), BackendError> {
        EmptyReferenceGraph.close().await
    }
}

fn no_graph<T>() -> Result<T, BackendError> {
    panic!("validation must not call storage")
}

#[async_trait]
impl SearchBackend for EmptyReferenceGraph {}

#[async_trait]
impl GraphBackend for EmptyReferenceGraph {
    async fn identity_revisions(
        &self,
        _: &str,
        scopes: &[IdentityScope],
    ) -> Result<Vec<IdentityRevision>, BackendError> {
        Ok(scopes
            .iter()
            .cloned()
            .map(|scope| IdentityRevision { scope, revision: 0 })
            .collect())
    }
    async fn apply_mutations(&self, _: &str, _: &[GraphMutation]) -> Result<(), BackendError> {
        no_graph()
    }

    async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
        no_graph()
    }

    async fn commit_batch(&self, _: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        no_graph()
    }

    async fn committed_batches(
        &self,
        _: &str,
        _: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        no_graph()
    }

    async fn find_entities(
        &self,
        _: &str,
        lookup: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        assert!(matches!(
            lookup,
            EntityLookup::LiveByKeyValue { .. } | EntityLookup::LatestByChain { .. }
        ));
        Ok(vec![])
    }

    async fn find_edges(&self, _: &str, _: &EdgeLookup) -> Result<Vec<EdgeRecord>, BackendError> {
        no_graph()
    }

    async fn health(&self) -> Result<(), BackendError> {
        no_graph()
    }

    async fn connect(&self) -> Result<(), BackendError> {
        no_graph()
    }

    async fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
}

#[test]
fn structural_references_require_word_boundaries_and_generic_key_context() {
    assert!(has_structural_reference(
        "metadata.ownerReferences[].uid",
        "ReplicaSet",
        &["metadata.uid".into()]
    ));
    assert!(!has_structural_reference(
        "description",
        "ReplicaSet",
        &["metadata.uid".into()]
    ));
    for path in ["grid", "pid", "uuid", "processId", "process_id"] {
        assert!(
            !has_structural_reference(path, "Service", &["id".into()]),
            "{path}"
        );
    }
    for path in [
        "serviceId",
        "service_id",
        "service.id",
        "services[].id",
        "id",
    ] {
        assert!(
            has_structural_reference(path, "Service", &["id".into()]),
            "{path}"
        );
    }
    assert!(has_structural_reference(
        "networkSubnetId",
        "AWS::EC2::Subnet",
        &["SubnetId".into()]
    ));
    assert!(!has_structural_reference(
        "notasubnetid",
        "AWS::EC2::Subnet",
        &["SubnetId".into()]
    ));
    assert!(has_structural_reference(
        "SecurityGroupIds",
        "AWS::EC2::SecurityGroup",
        &["GroupId".into()]
    ));
}

#[test]
fn configured_scan_bounds_can_cover_large_generic_payloads_without_hiding_truncation() {
    let mut source = crate::node::entity_versioning::tests::test_entity("source");
    source.all_properties.clear();
    source.all_properties.insert(
        "items".into(),
        PropertyValue::Json(
            serde_json::json!((0..256)
                .map(|i| serde_json::json!({"reference":i}))
                .collect::<Vec<_>>())
            .to_string(),
        ),
    );
    assert!(source_observations(&source, &[]).1);
    let (observations, truncated) = source_observations_with_limits(
        &source,
        &[],
        ScanLimits {
            values: 512,
            depth: 8,
        },
    );
    assert!(!truncated);
    assert_eq!(observations.len(), 256);
    assert_eq!(observations.last().unwrap().token, "i:255");
    assert!(
        source_observations_with_limits(
            &source,
            &[],
            ScanLimits {
                values: 512,
                depth: 1
            }
        )
        .1
    );
}

#[tokio::test]
async fn guided_composite_lists_require_complete_context_and_preserve_scope_and_types() {
    use kg_core::runtime::extraction::{
        ReferenceCardinality, ReferenceDirection, ReferenceMapping, ReferenceShape,
    };
    for mode in [
        "complete",
        "projected",
        "ambiguous",
        "missing",
        "multiple-varying",
        "typed",
        "truncated",
    ] {
        let mut source = typed_entity(
            "Server",
            "source",
            "prod",
            vec!["id"],
            vec![],
            &[
                ("id", "source"),
                ("scope.account", "tenant-a"),
                ("scope.region", "east"),
            ],
        );
        let array = if mode == "projected" {
            PropertyValue::Json(r#"[{"ForeignCode":"a"},{"ForeignCode":"b"}]"#.into())
        } else if mode == "typed" {
            PropertyValue::IntegerList(vec![1])
        } else {
            PropertyValue::StringList(vec!["a".into(), "b".into()])
        };
        source.all_properties.insert("config.refs".into(), array);
        if mode == "missing" {
            source.all_properties.shift_remove("scope.region");
        }
        let mut targets = vec![observation_target(&source)];
        let mut expected = Vec::new();
        for (name, account, region, code) in [
            ("a", "tenant-a", "east", "a"),
            ("b", "tenant-a", "east", "b"),
            ("foreign-account", "tenant-b", "east", "a"),
            ("foreign-region", "tenant-a", "west", "a"),
            ("string-one", "tenant-a", "east", "1"),
        ] {
            let target = typed_entity(
                "Target",
                name,
                "prod",
                vec!["id"],
                vec![vec!["account", "region", "code"]],
                &[
                    ("id", name),
                    ("account", account),
                    ("region", region),
                    ("code", code),
                ],
            );
            if name == "a" || name == "b" {
                expected.push(target.chain_id);
            }
            targets.push(observation_target(&target));
        }
        if mode == "ambiguous" {
            let duplicate = typed_entity(
                "Target",
                "duplicate",
                "prod",
                vec!["id"],
                vec![vec!["account", "region", "code"]],
                &[
                    ("id", "duplicate"),
                    ("account", "tenant-a"),
                    ("region", "east"),
                    ("code", "a"),
                ],
            );
            targets.push(observation_target(&duplicate));
        }
        let mut paths = BTreeMap::from([
            ("account".into(), "scope.account".into()),
            ("region".into(), "scope.region".into()),
        ]);
        if mode == "multiple-varying" {
            paths.remove("region");
        }
        let mapping = ReferenceMapping {
            source_namespace: None,
            source_entity_type: "Server".into(),
            reference_path: if mode == "projected" {
                "config.refs[].ForeignCode"
            } else {
                "config.refs[]"
            }
            .into(),
            context_paths: paths,
            target_type: "Target".into(),
            target_key_group: vec!["account".into(), "region".into(), "code".into()],
            shape: ReferenceShape::List,
            direction: ReferenceDirection::SourceToTarget,
            relationship_name: "USES".into(),
            qualifiers: None,
            cardinality: ReferenceCardinality::Many,
            case_insensitive_types: vec![],
        };
        mapping.validate().unwrap();
        let mut ctx = one_guidance(mapping);
        if mode == "truncated" {
            ctx.extraction_settings.reference_max_values = 1;
        }
        let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
            .process(reference_input(guided_resolution(source, targets)), &ctx)
            .await
            .unwrap()
        else {
            panic!()
        };
        let edges: Vec<_> = output
            .edges
            .iter()
            .filter(|edge| edge.discovered_by.as_deref() == Some("guided_fk"))
            .collect();
        let expected_count = match mode {
            "complete" | "projected" => 2,
            "ambiguous" | "truncated" => 1,
            _ => 0,
        };
        assert_eq!(edges.len(), expected_count, "{mode}");
        assert!(
            edges
                .iter()
                .all(|edge| expected.contains(&edge.target_chain_id)),
            "{mode}"
        );
        if mode == "ambiguous" {
            // Default ambiguity policy abstains without queueing a model call.
            assert!(output.pending_references.is_empty());
            assert!(output
                .reference_report
                .unresolved_slots
                .iter()
                .any(|slot| slot
                    .entries
                    .iter()
                    .any(|entry| entry.reason == "multiple-candidates")));
        }
        if mode == "truncated" {
            assert!(output.reference_report.incomplete());
        }
        if mode == "projected" {
            let mut locations: Vec<_> = edges
                .iter()
                .map(|edge| edge.reference_evidence.as_ref().unwrap().location.as_str())
                .collect();
            locations.sort();
            assert_eq!(
                locations,
                ["config.refs[0].ForeignCode", "config.refs[1].ForeignCode"]
            );
        }
    }
}

#[tokio::test]
async fn renamed_composite_keys_reach_model_and_persist_correspondence() {
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    let source = typed_entity(
        "Instance",
        "instance",
        "prod",
        vec!["id"],
        vec![],
        &[
            ("id", "i-1"),
            ("attachment.OwnerId", "B"),
            ("attachment.Region", "west"),
            ("attachment.VolumeId", "vol-1"),
        ],
    );
    let disk = typed_entity(
        "Disk",
        "disk",
        "prod",
        vec!["account", "region", "disk_id"],
        vec![],
        &[("account", "B"), ("region", "west"), ("disk_id", "vol-1")],
    );
    let targets = vec![observation_target(&source), observation_target(&disk)];
    let mut ctx = context();
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    let output = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = output else {
        panic!()
    };
    let pending = output
        .pending_references
        .iter()
        .find(|p| p.intent.location == "attachment.VolumeId")
        .unwrap();
    assert_eq!(pending.candidates.len(), 1);
    let paths = reference_correspondence(&pending.candidates[0].target, &pending.intent).unwrap();
    assert_eq!(paths["account"], "attachment.OwnerId");
    assert_eq!(paths["region"], "attachment.Region");
    assert_eq!(paths["disk_id"], "attachment.VolumeId");
    let edge = build_reference_edge(
        &pending.source,
        &pending.candidates[0].target,
        "Disk",
        "vol-1",
        &pending.intent,
        true,
    )
    .unwrap();
    // The fact names both endpoints by type and name (the source with its labels
    // when it has any); the matched value follows.
    assert!(
        edge.description.contains(&format!(
            "references Disk {} via property",
            pending.candidates[0].target.name
        )) && edge.description.ends_with("(vol-1)")
            && (pending.source.labels.is_empty()
                || edge
                    .description
                    .contains(&format!("[labels: {}]", pending.source.labels.join(", ")))),
        "{}",
        edge.description
    );
    assert_eq!(
        edge.reference_evidence.unwrap().component_paths,
        Some(paths)
    );
}

#[test]
fn excluded_nested_sibling_cannot_complete_a_reference_key() {
    let mut source = crate::node::entity_versioning::tests::test_entity("source");
    source.all_properties.insert(
        "disks".into(),
        PropertyValue::Json(r#"[{"OwnerId":"B","VolumeId":"vol-1"}]"#.into()),
    );
    let (observations, _) = source_observations(&source, &["disks.OwnerId"]);
    let disk = observations
        .iter()
        .find(|o| o.location == "disks[0].VolumeId")
        .unwrap();
    assert!(!disk.context.iter().any(|(field, _)| field == "OwnerId"));
}

struct TruncatedSubnetGraph;
#[async_trait]
impl SearchBackend for TruncatedSubnetGraph {}
#[async_trait]
impl GraphBackend for TruncatedSubnetGraph {
    async fn identity_revisions(
        &self,
        org: &str,
        scopes: &[IdentityScope],
    ) -> Result<Vec<IdentityRevision>, BackendError> {
        EmptyReferenceGraph.identity_revisions(org, scopes).await
    }
    async fn find_reference_candidates(
        &self,
        _: &str,
        wanted: Vec<kg_core::traits::graph_reads::WantedKeyValue>,
    ) -> Result<kg_core::traits::graph_reads::ReferenceCandidates, BackendError> {
        let requests: Vec<_> = wanted.iter().filter(|w| w.token == "s:subnet-1").collect();
        assert!(
            !requests.is_empty(),
            "guidance suppressed generic storage lookup"
        );
        Ok(kg_core::traits::graph_reads::ReferenceCandidates {
            records: Vec::new(),
            truncated_requests: requests.into_iter().map(|w| w.request_id()).collect(),
        })
    }
    async fn apply_mutations(&self, org: &str, m: &[GraphMutation]) -> Result<(), BackendError> {
        EmptyReferenceGraph.apply_mutations(org, m).await
    }
    async fn register_run(&self, h: &RunHeader) -> Result<RunRegistration, BackendError> {
        EmptyReferenceGraph.register_run(h).await
    }
    async fn commit_batch(&self, b: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        EmptyReferenceGraph.commit_batch(b).await
    }
    async fn committed_batches(
        &self,
        org: &str,
        run: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        EmptyReferenceGraph.committed_batches(org, run).await
    }
    async fn find_entities(
        &self,
        org: &str,
        lookup: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        EmptyReferenceGraph.find_entities(org, lookup).await
    }
    async fn find_edges(
        &self,
        org: &str,
        lookup: &EdgeLookup,
    ) -> Result<Vec<EdgeRecord>, BackendError> {
        EmptyReferenceGraph.find_edges(org, lookup).await
    }
    async fn health(&self) -> Result<(), BackendError> {
        EmptyReferenceGraph.health().await
    }
    async fn connect(&self) -> Result<(), BackendError> {
        EmptyReferenceGraph.connect().await
    }
    async fn close(&self) -> Result<(), BackendError> {
        EmptyReferenceGraph.close().await
    }
}

#[tokio::test]
async fn declined_guidance_never_skips_the_generic_candidate_universe() {
    use kg_core::runtime::extraction::{ReferenceMapping, ReferenceShape};
    for case in ["namespace", "excluded-context", "missing-context", "active"] {
        let source = typed_entity(
            "Server",
            "server",
            "prod",
            vec!["id"],
            vec![],
            &[("id", "server-1"), ("subnet", "subnet-1")],
        );
        let subnet = typed_entity(
            "Subnet",
            "subnet",
            "prod",
            vec!["id"],
            vec![],
            &[("id", "subnet-1")],
        );
        let targets = vec![observation_target(&source), observation_target(&subnet)];
        let mut mapping = ReferenceMapping {
            source_entity_type: "Server".into(),
            source_namespace: None,
            reference_path: "subnet".into(),
            context_paths: BTreeMap::new(),
            target_type: "Subnet".into(),
            target_key_group: vec!["id".into()],
            shape: ReferenceShape::Scalar,
            direction: Default::default(),
            relationship_name: "RELATES_TO".into(),
            qualifiers: None,
            cardinality: Default::default(),
            case_insensitive_types: vec![],
        };
        if case == "namespace" {
            mapping.source_namespace = Some("staging".into());
        }
        if matches!(case, "excluded-context" | "missing-context") {
            mapping.target_key_group.push("region".into());
            mapping
                .context_paths
                .insert("region".into(), "missing".into());
        }
        let mut ctx = one_guidance(mapping);
        ctx.graph = Arc::new(TruncatedSubnetGraph);
        let mut resolution = guided_resolution(source, targets);
        if case == "excluded-context" {
            // The mapping needs this field, but this observation excludes it.
            let snapshot = resolution.snapshot_nodes[0].uuid;
            Arc::make_mut(&mut resolution.fk_exclusions).insert(snapshot, vec!["missing".into()]);
        }
        let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
            .process(reference_input(resolution), &ctx)
            .await
            .unwrap()
        else {
            panic!()
        };
        assert!(
            output.edges.is_empty(),
            "{case}: local target is not a complete universe"
        );
        assert_eq!(
            output.reference_report.incomplete(),
            case == "missing-context",
            "{case}: lookup uncertainty is slot-local, incomplete traversal remains source-wide"
        );
        if case != "missing-context" {
            assert!(
                output.reference_report.unresolved_slots.iter().any(|s| s
                    .entries
                    .iter()
                    .any(|e| e.token == "s:subnet-1" && e.reason == "lookup-truncated")),
                "{case}"
            );
        }
        if case == "missing-context" {
            eprintln!(
                "SLOTS {:?}",
                output
                    .reference_report
                    .unresolved_slots
                    .iter()
                    .map(|s| (
                        s.slot.clone(),
                        s.entries
                            .iter()
                            .map(|e| (e.token.clone(), e.reason.clone()))
                            .collect::<Vec<_>>()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn identity_tokens_preserve_unicode_punctuation_and_numeric_types() {
    assert_eq!(
        value_token(&PropertyValue::String("é/资源:01".into())).as_deref(),
        Some("s:é/资源:01")
    );
    assert_ne!(
        value_token(&PropertyValue::Float(1.25)),
        value_token(&PropertyValue::String("1.25".into()))
    );
    assert_ne!(
        value_token(&PropertyValue::Integer(1)),
        value_token(&PropertyValue::String("1".into()))
    );
    assert_ne!(
        value_token(&PropertyValue::Bool(true)),
        value_token(&PropertyValue::String("true".into()))
    );
}

#[tokio::test]
async fn uncertainty_keeps_each_capture_and_only_latest_slot_is_durable() {
    let (mut resolution, first, second) = two_observations();
    resolution.fk_exclusions = Arc::new(HashMap::new());
    let snapshots = Arc::make_mut(&mut resolution.snapshot_nodes);
    let later = snapshots[0].captured_at + chrono::Duration::seconds(30);
    snapshots[1].captured_at = later;
    Arc::make_mut(&mut resolution.nodes_unchanged)[1]
        .value
        .last_seen_at = Some(later);
    // The later property has no target. It must not inherit the first capture's clock.
    Arc::make_mut(&mut resolution.observed_properties)[1]
        .properties
        .insert(
            "cluster_name".into(),
            PropertyValue::String("missing-later-target".into()),
        );
    let StageOutput::EdgeExtraction(output) = ReferenceExtractionStage
        .process(reference_input(resolution), &context())
        .await
        .unwrap()
    else {
        panic!()
    };
    let decisions: Vec<_> = output
        .reference_report
        .retirement_decisions
        .iter()
        .filter(|decision| decision.slot.ends_with(".cluster_name"))
        .collect();
    assert_eq!(decisions.len(), 2);
    assert!(decisions
        .iter()
        .any(|decision| decision.decision_id == first && decision.entries.is_empty()));
    assert!(decisions
        .iter()
        .any(|decision| decision.decision_id == second
            && decision.decided_at == later
            && decision
                .entries
                .iter()
                .any(|entry| entry.token == "s:missing-later-target")));
    let durable = output
        .reference_report
        .unresolved_slots
        .iter()
        .find(|decision| decision.slot.ends_with(".cluster_name"))
        .unwrap();
    assert_eq!((durable.decided_at, durable.decision_id), (later, second));
}

/// Two array members with the same value, the same candidates and no
/// distinguishing context are one decision: the model is asked once and the
/// second member is rebound to that verdict (decision memory keys are
/// index-free), so a confirmation resolves both and a rejection leaves both
/// unresolved. The second scripted answer is never consumed.
#[tokio::test]
async fn same_token_array_members_share_one_model_verdict() {
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    for first_accepts in [false, true] {
        let mut resolution = ambiguous_resolution();
        for original in Arc::make_mut(&mut resolution.observed_properties) {
            original.properties.insert(
                "cluster_name".into(),
                PropertyValue::StringList(vec!["cluster-target".into(), "cluster-target".into()]),
            );
        }
        let mut ctx = context();
        ctx.graph = targets_graph(resolution.chunk_entities.as_ref().unwrap());
        ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
            edge_ambiguity: EdgeAmbiguityMode::Llm,
            ..Default::default()
        }));
        let extracted = ReferenceExtractionStage
            .process(reference_input(resolution), &ctx)
            .await
            .unwrap();
        let StageOutput::EdgeExtraction(ref before) = extracted else {
            panic!()
        };
        assert_eq!(before.pending_references.len(), 2);
        let chosen = before.pending_references[0].candidates[1].target.chain_id;
        let llm = Arc::new(kg_core::test_support::MockLlmBackend::with_responses(vec![
            if first_accepts {
                accept_answer(chosen)
            } else {
                REJECT_ANSWER.into()
            },
            REJECT_ANSWER.into(),
        ]));
        ctx.llm_disambiguation = llm.clone();
        let StageOutput::EdgeExtraction(output) = super::super::ReferenceResolutionStage
            .process(extracted, &ctx)
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(llm.call_count(), 1, "identical members are one decision");
        let audits = &output.reference_report.decisions;
        assert_eq!(audits.len(), 2);
        assert_eq!(audits.iter().filter(|a| a.reused).count(), 1);
        let unresolved = !first_accepts;
        assert_eq!(
            output.reference_report.unresolved,
            2 * usize::from(unresolved)
        );
        for decisions in [
            &output.reference_report.unresolved_slots,
            &output.reference_report.retirement_decisions,
        ] {
            let decision = decisions
                .iter()
                .find(|d| d.slot.ends_with(".cluster_name"))
                .unwrap();
            assert_eq!(
                decision
                    .entries
                    .iter()
                    .any(|entry| entry.token == "s:cluster-target"),
                unresolved
            );
        }
    }
}

#[test]
fn a_non_model_uncertainty_survives_same_token_model_ambiguity_in_either_order() {
    for reasons in [
        ["partial-key", "multiple-candidates"],
        ["multiple-candidates", "partial-key"],
        ["target-not-found", "partial-key"],
        ["partial-key", "target-not-found"],
    ] {
        let mut entries = Vec::new();
        for reason in reasons {
            record_entry(
                &mut entries,
                kg_core::traits::UnresolvedReferenceEntry {
                    token: "s:target".into(),
                    reason: reason.into(),
                    snapshot_id: None,
                    recorded_at: Utc::now(),
                },
            );
        }
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].reason, "partial-key");
    }
}

#[test]
fn display_only_uncertainty_cannot_be_cleared_by_a_model_eligible_sibling() {
    for reasons in [
        ["display-name-only", "insufficient-reference-evidence"],
        ["insufficient-reference-evidence", "display-name-only"],
    ] {
        let mut entries = Vec::new();
        for reason in reasons {
            record_entry(
                &mut entries,
                kg_core::traits::UnresolvedReferenceEntry {
                    token: "s:target".into(),
                    reason: reason.into(),
                    snapshot_id: None,
                    recorded_at: Utc::now(),
                },
            );
        }
        assert_eq!(entries[0].reason, "display-name-only");
        assert!(!model_resolvable_reason(&entries[0].reason));
    }
}

#[test]
fn qualified_types_and_identifier_spelling_keep_exact_key_reference_checks() {
    for (path, typ, key) in [
        ("SecurityGroups[0]", "AWS::EC2::SecurityGroup", "GroupId"),
        ("roleArn", "AWS::IAM::Role", "Arn"),
        (
            "DBSubnetGroup.Subnets[0].SubnetIdentifier",
            "AWS::EC2::Subnet",
            "SubnetId",
        ),
        ("serviceId", "acme.platform.Service", "id"),
        ("project_identifier", "GCP::Project", "projectId"),
        (
            "namespace_identifier",
            "Kubernetes::Namespace",
            "namespaceId",
        ),
    ] {
        assert!(
            has_structural_reference(path, typ, &[key.into()]),
            "{path}: {typ}"
        );
    }
    for (path, typ, key) in [
        ("description", "AWS::IAM::Role", "Arn"),
        ("ownerArn", "AWS::IAM::Role", "Arn"),
        ("processId", "acme.Service", "id"),
        ("grid", "AWS::EC2::Subnet", "id"),
        ("notasubnetidentifier", "AWS::EC2::Subnet", "SubnetId"),
        ("namespace_identifier", "GCP::Project", "projectId"),
    ] {
        assert!(
            !has_structural_reference(path, typ, &[key.into()]),
            "{path}: {typ}"
        );
    }
}

// ---------------------------------------------------------------------------
// Genericity fixtures (plan Phase 3): own-identity coincidence versus legitimate
// foreign keys, realistic provider payload shapes, and bounded resource use.
// A scripted model answers by candidate id after reading the fenced packet;
// nothing in the engine knows any of these field or type names.

/// Accepts the offered candidate of `want_type` (citing the occurrence) and
/// rejects everything else; records every packet it saw.
struct Choosing {
    want_type: &'static str,
    packets: std::sync::Mutex<Vec<serde_json::Value>>,
    calls: std::sync::atomic::AtomicUsize,
}

impl Choosing {
    fn new(want_type: &'static str) -> Arc<Self> {
        Arc::new(Self {
            want_type,
            packets: Default::default(),
            calls: Default::default(),
        })
    }
    fn packet(messages: &[kg_core::traits::llm_backend::LlmMessage]) -> serde_json::Value {
        let user = &messages.last().unwrap().content;
        kg_core::test_support::source_data_json(user)
    }
}

#[async_trait]
impl LlmBackend for Choosing {
    async fn complete(
        &self,
        messages: &[kg_core::traits::llm_backend::LlmMessage],
        _: Option<&serde_json::Value>,
        _: Option<u32>,
    ) -> Result<kg_core::traits::llm_backend::LlmResponse, BackendError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let packet = Self::packet(messages);
        let answer = match packet["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["entity"]["entity_type"] == self.want_type)
        {
            Some(candidate) => {
                accept_answer(candidate["candidate_id"].as_str().unwrap().parse().unwrap())
            }
            None => REJECT_ANSWER.into(),
        };
        self.packets.lock().unwrap().push(packet);
        Ok(kg_core::traits::llm_backend::LlmResponse {
            status: kg_core::traits::llm_backend::CompletionStatus::Complete,
            content: answer,
            model: "choosing".into(),
            input_tokens: Some(1),
            output_tokens: Some(1),
        })
    }
    async fn complete_bounded(
        &self,
        messages: &[kg_core::traits::llm_backend::LlmMessage],
        schema: Option<&serde_json::Value>,
        max_tokens: Option<u32>,
        budget: &kg_core::traits::llm_backend::CallBudget,
    ) -> Result<kg_core::traits::llm_backend::LlmResponse, BackendError> {
        budget.consume()?;
        self.complete(messages, schema, max_tokens).await
    }
    fn supports_bounded_attempts(&self) -> bool {
        true
    }
    fn model_id(&self) -> &str {
        "choosing"
    }
    fn context_window(&self) -> usize {
        1_000_000
    }
}

/// The stored record of a run target with its complete properties, so
/// hydration returns the record the packet must disclose.
fn full_record(node: &EntityNode) -> EntityVersionRecord {
    let mut record = record_for(&observation_target(node));
    for (path, value) in &node.all_properties {
        kg_core::traits::property_codec::write_property(&mut record.stored, path, Some(value));
    }
    record
}

fn nodes_graph(nodes: &[&EntityNode]) -> Arc<TargetsGraph> {
    Arc::new(TargetsGraph(
        nodes.iter().map(|node| full_record(node)).collect(),
        std::sync::atomic::AtomicUsize::new(0),
    ))
}

/// Extraction then resolution over in-run targets with `model` deciding.
async fn resolve_with(
    source: EntityNode,
    targets: Vec<&EntityNode>,
    model: Arc<dyn LlmBackend>,
) -> kg_core::runtime::stage_output::EdgeExtractionOutput {
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    let mut ctx = context();
    ctx.graph = nodes_graph(&targets);
    // `live_observations` keeps the source only when its chain is a run target.
    let mut targets: Vec<RelationshipTarget> =
        targets.iter().map(|n| observation_target(n)).collect();
    targets.push(observation_target(&source));
    ctx.llm_disambiguation = model;
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    let extracted = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap();
    match super::super::ReferenceResolutionStage
        .process(extracted, &ctx)
        .await
        .unwrap()
    {
        StageOutput::EdgeExtraction(output) => output,
        _ => panic!(),
    }
}

fn nested_entity(
    entity_type: &str,
    name: &str,
    keys: &[&str],
    additional: &[&[&str]],
    raw: serde_json::Value,
) -> EntityNode {
    let mut node = typed_entity(
        entity_type,
        name,
        "prod",
        keys.to_vec(),
        additional.iter().map(|g| g.to_vec()).collect(),
        &[],
    );
    node.all_properties = PropertyValue::flatten_source(&raw, &[]).unwrap();
    node
}

#[tokio::test]
async fn join_keys_link_deterministically_while_own_identity_coincidence_needs_a_decision() {
    // A membership row references the User and the Group its own key parts name:
    // both are complete declared keys named by the field, so no model is asked.
    let membership = nested_entity(
        "Membership",
        "u1/g1",
        &["user_id", "group_id"],
        &[],
        serde_json::json!({"user_id": "u1", "group_id": "g1", "role": "admin"}),
    );
    let user = nested_entity(
        "User",
        "u1",
        &["user_id"],
        &[],
        serde_json::json!({"user_id": "u1"}),
    );
    let group = nested_entity(
        "Group",
        "g1",
        &["group_id"],
        &[],
        serde_json::json!({"group_id": "g1"}),
    );
    let (user_chain, group_chain) = (user.chain_id, group.chain_id);
    let model = Choosing::new("never");
    let output = resolve_with(membership, vec![&user, &group], model.clone()).await;
    let targets: BTreeSet<_> = output.edges.iter().map(|e| e.target_chain_id).collect();
    assert_eq!(
        targets,
        BTreeSet::from([user_chain, group_chain]),
        "source identity fields are not blanket exclusions"
    );
    assert_eq!(
        model.calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "complete structural keys never reach the model"
    );
    assert!(output.reference_report.decisions.is_empty());

    // A bucket's own primary Name equals a role's additional RoleName: nothing in
    // the bucket record says it refers to the role, so the occurrence is decided,
    // and a reject leaves no edge while keeping the occurrence unresolved.
    let bucket = nested_entity(
        "AWS::S3::Bucket",
        "archive",
        &["Name"],
        &[],
        serde_json::json!({"Name": "archive", "BucketRegion": "us-east-1"}),
    );
    let role = nested_entity(
        "AWS::IAM::Role",
        "archive-role",
        &["Arn"],
        &[&["RoleName"]],
        serde_json::json!({"Arn": "arn:aws:iam::1:role/archive", "RoleName": "archive"}),
    );
    let rejecting = Choosing::new("never");
    let output = resolve_with(bucket, vec![&role], rejecting.clone()).await;
    assert!(output.edges.is_empty());
    assert_eq!(
        rejecting.calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "an occurrence in the source's own single-field key is refused by the host"
    );
    assert_eq!(output.reference_report.decisions.len(), 1);
    assert_eq!(
        (
            output.reference_report.decisions[0].outcome,
            output.reference_report.decisions[0].reason
        ),
        (
            kg_core::runtime::reference_resolution::DecisionOutcome::Unsure,
            kg_core::runtime::reference_resolution::DecisionReason::OwnIdentityValue
        )
    );
    // Two durable unresolved entries: the rejected occurrence itself
    // (`insufficient-reference-evidence`) and the region value that matched
    // nothing (`target-not-found`).
    assert_eq!(output.reference_report.unresolved, 2);
    let slots: BTreeMap<_, _> = output
        .reference_report
        .unresolved_slots
        .iter()
        .map(|s| {
            (
                s.slot.clone(),
                s.entries
                    .iter()
                    .map(|e| (e.token.clone(), e.reason.clone()))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    assert_eq!(
        slots.get("AWS::S3::Bucket.Name").map(Vec::as_slice),
        Some(
            &[(
                "s:archive".to_string(),
                "insufficient-reference-evidence".to_string()
            )][..]
        )
    );
    let audit = &output.reference_report.decisions[0];
    assert_eq!(audit.location, "Name");
    assert_eq!(audit.provider_attempts, 0);
    assert_eq!(
        audit.candidate_read_set.len(),
        1,
        "the role was the frozen candidate"
    );

    // A declared `name` key is a real key: the team is offered as a candidate and
    // an accepted decision links it; an alternative key named by the field links
    // deterministically.
    let workload = nested_entity(
        "Workload",
        "api",
        &["id"],
        &[],
        serde_json::json!({"id": "api", "owner": "alpha", "team_slug": "beta"}),
    );
    let team = nested_entity(
        "Team",
        "alpha",
        &["name"],
        &[],
        serde_json::json!({"name": "alpha"}),
    );
    let other = nested_entity(
        "Team",
        "team-beta",
        &["id"],
        &[&["slug"]],
        serde_json::json!({"id": "t2", "slug": "beta"}),
    );
    let (team_chain, other_chain) = (team.chain_id, other.chain_id);
    let choosing = Choosing::new("Team");
    let output = resolve_with(workload, vec![&team, &other], choosing.clone()).await;
    let by_target: BTreeMap<_, _> = output
        .edges
        .iter()
        .map(|e| (e.target_chain_id, e.discovered_by.clone().unwrap()))
        .collect();
    assert_eq!(
        by_target.get(&other_chain).map(String::as_str),
        Some("heuristic_fk"),
        "alternative key named by the field"
    );
    assert_eq!(
        by_target.get(&team_chain).map(String::as_str),
        Some("llm_fk_disambiguation"),
        "declared name key decided on evidence"
    );
    assert_eq!(choosing.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn provider_shapes_are_handled_by_one_generic_path() {
    use std::sync::atomic::Ordering;
    // GCP: a full resource name in a field that does not name the key → decided
    // with the network's complete record in the packet.
    let instance = nested_entity(
        "GceInstance",
        "vm-1",
        &["id"],
        &[],
        serde_json::json!({"id": "vm-1", "network": "projects/p/global/networks/vpc-1", "zone": "us-central1-a"}),
    );
    let network = nested_entity(
        "GceNetwork",
        "vpc-1",
        &["self_link"],
        &[],
        serde_json::json!({"self_link": "projects/p/global/networks/vpc-1", "auto_create_subnetworks": false, "mtu": 1460}),
    );
    let model = Choosing::new("GceNetwork");
    let output = resolve_with(instance, vec![&network], model.clone()).await;
    assert_eq!(output.edges.len(), 1);
    assert_eq!(output.edges[0].target_chain_id, network.chain_id);
    let packet = model.packets.lock().unwrap().remove(0);
    assert_eq!(
        packet["candidates"][0]["entity"]["properties"]["mtu"], 1460,
        "complete candidate record upfront"
    );
    assert_eq!(packet["reference"]["location"], "network");

    // Azure: an ARM id under a nested object whose enclosing field names the
    // target type (`subnet.id`) is structural evidence: linked deterministically
    // with no model call, exactly like any other provider's nested shape.
    let nic = nested_entity(
        "AzureNetworkInterface",
        "nic-1",
        &["id"],
        &[],
        serde_json::json!({"id": "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Network/networkInterfaces/nic-1", "properties": {"ipConfigurations": [{"name": "ipconfig1", "properties": {"subnet": {"id": "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/vnet1/subnets/default"}}}]}}),
    );
    let subnet = nested_entity(
        "AzureSubnet",
        "default",
        &["id"],
        &[],
        serde_json::json!({"id": "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/vnet1/subnets/default", "addressPrefix": "10.0.0.0/24"}),
    );
    let model = Choosing::new("AzureSubnet");
    let output = resolve_with(nic, vec![&subnet], model.clone()).await;
    let edges: Vec<_> = output
        .edges
        .iter()
        .filter(|e| e.target_chain_id == subnet.chain_id)
        .collect();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].discovered_by.as_deref(), Some("heuristic_fk"));
    assert_eq!(
        edges[0].reference_evidence.as_ref().unwrap().location,
        "properties.ipConfigurations[0].properties.subnet.id"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);

    // Kubernetes: identical node names in two clusters keyed [cluster, name]; a
    // nested nodeName cannot borrow the pod's top-level cluster to complete the
    // composite, so nothing is decided and no model is asked.
    let pod = nested_entity(
        "Pod",
        "web",
        &["cluster", "namespace", "name"],
        &[],
        serde_json::json!({"cluster": "c1", "namespace": "default", "name": "web", "spec": {"nodeName": "node-a"}}),
    );
    let node_a = nested_entity(
        "Node",
        "node-a",
        &["cluster", "name"],
        &[],
        serde_json::json!({"cluster": "c1", "name": "node-a"}),
    );
    let node_b = nested_entity(
        "Node",
        "node-a",
        &["cluster", "name"],
        &[],
        serde_json::json!({"cluster": "c2", "name": "node-a"}),
    );
    let model = Choosing::new("Node");
    let output = resolve_with(pod, vec![&node_a, &node_b], model.clone()).await;
    assert!(
        output
            .edges
            .iter()
            .all(|e| e.target_chain_id != node_a.chain_id && e.target_chain_id != node_b.chain_id),
        "no partial composite ever links"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    // The nested name is durably unresolved (it only matches display names and a
    // partial composite); it never becomes a model question.
    assert!(output
        .reference_report
        .unresolved_slots
        .iter()
        .any(|s| s.slot == "Pod.spec.nodeName"
            && s.entries.iter().any(|e| e.token == "s:node-a"
                && matches!(e.reason.as_str(), "display-name-only" | "partial-key"))));

    // GitHub: a workflow names its repository by full name in a field the key
    // does not name → decided on the packet.
    let workflow = nested_entity(
        "GitHubWorkflow",
        "ci",
        &["id"],
        &[],
        serde_json::json!({"id": 7, "name": "ci", "repository": "acme/platform", "path": ".github/workflows/ci.yml"}),
    );
    let repo = nested_entity(
        "GitHubRepository",
        "platform",
        &["full_name"],
        &[],
        serde_json::json!({"full_name": "acme/platform", "default_branch": "main"}),
    );
    let model = Choosing::new("GitHubRepository");
    let output = resolve_with(workflow, vec![&repo], model.clone()).await;
    assert_eq!(
        output
            .edges
            .iter()
            .filter(|e| e.target_chain_id == repo.chain_id)
            .count(),
        1
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);

    // CMDB: one value completes two candidates of different types; the packet
    // offers both and the decision names one.
    let change = nested_entity(
        "CmdbChange",
        "CHG1",
        &["number"],
        &[],
        serde_json::json!({"number": "CHG1", "owning_group": "atlas"}),
    );
    let group = nested_entity(
        "CmdbGroup",
        "Atlas",
        &["group_id"],
        &[],
        serde_json::json!({"group_id": "atlas"}),
    );
    let person = nested_entity(
        "CmdbPerson",
        "Atlas Rivera",
        &["user_id"],
        &[],
        serde_json::json!({"user_id": "atlas"}),
    );
    let model = Choosing::new("CmdbGroup");
    let output = resolve_with(change, vec![&group, &person], model.clone()).await;
    assert_eq!(output.edges.len(), 1);
    assert_eq!(output.edges[0].target_chain_id, group.chain_id);
    let packet = model.packets.lock().unwrap().remove(0);
    assert_eq!(packet["candidates"].as_array().unwrap().len(), 2);
    assert_eq!(
        output.reference_report.decisions[0]
            .candidate_read_set
            .len(),
        2
    );
}

#[tokio::test]
async fn many_pending_occurrences_hydrate_once_and_stay_within_audit_bounds() {
    use std::sync::atomic::Ordering;
    let tags: Vec<_> = (0..30)
        .map(|i| serde_json::json!({"Key": format!("Ref{i}"), "Value": "shared"}))
        .collect();
    let source = nested_entity(
        "Instance",
        "i-1",
        &["InstanceId"],
        &[],
        serde_json::json!({"InstanceId": "i-1", "Tags": tags}),
    );
    let a = nested_entity(
        "Bucket",
        "shared-bucket",
        &["Name"],
        &[],
        serde_json::json!({"Name": "shared"}),
    );
    let b = nested_entity(
        "Role",
        "shared-role",
        &["Arn"],
        &[&["RoleName"]],
        serde_json::json!({"Arn": "arn:x", "RoleName": "shared"}),
    );
    let graph = nodes_graph(&[&a, &b, &source]);
    let targets = vec![
        observation_target(&a),
        observation_target(&b),
        observation_target(&source),
    ];
    use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
    let model = Choosing::new("Bucket");
    let mut ctx = context();
    ctx.graph = graph.clone();
    ctx.llm_disambiguation = model.clone();
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
        edge_ambiguity: EdgeAmbiguityMode::Llm,
        ..Default::default()
    }));
    let extracted = ReferenceExtractionStage
        .process(reference_input(guided_resolution(source, targets)), &ctx)
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = super::super::ReferenceResolutionStage
        .process(extracted, &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        output.reference_report.decisions.len(),
        30,
        "one audit per occurrence"
    );
    assert_eq!(
        graph.1.load(Ordering::SeqCst),
        1,
        "candidates are hydrated in one bounded read per snapshot"
    );
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        30,
        "distinct occurrences are distinct evidence: one attempt each"
    );
    assert_eq!(output.edges.len(), 30);
    let bound = ctx.reference_resolution_settings.max_audit_bytes;
    assert!(output
        .reference_report
        .decisions
        .iter()
        .all(|d| serde_json::to_vec(d).unwrap().len() <= bound));
    let ids: BTreeSet<_> = output
        .reference_report
        .decisions
        .iter()
        .map(|d| d.decision_id)
        .collect();
    assert_eq!(ids.len(), 30);
}

/// A declared additional key whose value equals the entity's display name is a
/// key, not a display name: a field that names that key links deterministically
/// (RDS `ReadReplicaSourceDBInstanceIdentifier` → the primary's `DBInstanceIdentifier`).
#[tokio::test]
async fn a_declared_key_equal_to_the_display_name_still_links_deterministically() {
    let primary = nested_entity(
        "AWS::RDS::DBInstance",
        "orders-db",
        &["DBInstanceArn"],
        &[&["DbiResourceId"], &["DBInstanceIdentifier"]],
        serde_json::json!({"DBInstanceIdentifier": "orders-db", "DBInstanceArn": "arn:aws:rds:us-east-1:1:db:orders-db", "DbiResourceId": "db-AAA", "ReadReplicaDBInstanceIdentifiers": ["orders-db-replica"]}),
    );
    let replica = nested_entity(
        "AWS::RDS::DBInstance",
        "orders-db-replica",
        &["DBInstanceArn"],
        &[&["DbiResourceId"], &["DBInstanceIdentifier"]],
        serde_json::json!({"DBInstanceIdentifier": "orders-db-replica", "DBInstanceArn": "arn:aws:rds:us-east-1:1:db:orders-db-replica", "DbiResourceId": "db-BBB", "ReadReplicaSourceDBInstanceIdentifier": "orders-db"}),
    );
    let (primary_chain, replica_chain) = (primary.chain_id, replica.chain_id);
    let model = Choosing::new("never");
    let output = resolve_with(replica.clone(), vec![&primary], model.clone()).await;
    assert!(
        output
            .edges
            .iter()
            .any(|e| e.target_chain_id == primary_chain
                && e.discovered_by.as_deref() == Some("heuristic_fk")),
        "replica → primary via the identifier key: {:?}",
        output.reference_report.unresolved_slots
    );
    let output = resolve_with(primary, vec![&replica], model.clone()).await;
    assert!(
        output
            .edges
            .iter()
            .any(|e| e.target_chain_id == replica_chain
                && e.discovered_by.as_deref() == Some("heuristic_fk")),
        "primary → replica via the identifiers list: {:?}",
        output.reference_report.unresolved_slots
    );
    assert_eq!(model.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn sibling_observations_share_their_context_allocation() {
    let source = typed_entity(
        "Service",
        "a",
        "prod",
        vec!["id"],
        vec![],
        &[("id", "one"), ("owner", "two"), ("network", "three")],
    );
    let (observations, truncated) = source_observations(&source, &[]);
    assert!(!truncated);
    assert!(observations.len() >= 3);
    for observation in &observations[1..] {
        assert!(Arc::ptr_eq(&observations[0].context, &observation.context));
    }
}
