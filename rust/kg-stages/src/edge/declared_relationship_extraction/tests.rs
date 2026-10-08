use super::*;
use chrono::Utc;
use kg_core::{
    models::{EntityEdge, PropertyValue},
    runtime::{
        stage_output::{NodeResolutionOutput, RelationshipTarget},
        RuntimeContextBuilder,
    },
    test_support::{MockEmbedBackend, MockLlmBackend},
};
use uuid::Uuid;

fn context() -> (RuntimeContext, Arc<MockLlmBackend>) {
    let llm = Arc::new(MockLlmBackend::empty());
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(kg_core::test_support::UnreachableGraph))
        .llm_default(llm.clone())
        .llm_extraction(llm.clone())
        .embedder(Arc::new(MockEmbedBackend::default_dimension()))
        .build()
        .unwrap();
    (ctx, llm)
}

fn edge(source: Uuid, target: Uuid) -> EntityEdge {
    let at = Utc::now();
    EntityEdge {
        time_evidence: None,
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        chain_id: Uuid::new_v4(),
        identity_hash: None,
        cardinality_key: None,
        producer_source: "test".into(),
        origin: kg_core::models::edges::RelationshipOrigin::Declared,
        uuid: Uuid::new_v4(),
        org_id: "org".into(),
        source_chain_id: source,
        target_chain_id: target,
        name: "CALLS".into(),
        identity_name: None,
        description: "declared call".into(),
        all_properties: indexmap::IndexMap::from([("port".into(), PropertyValue::Integer(443))]),
        discovered_by: None,
        resolved_by: Some("declared".into()),
        source_property: Some("callee".into()),
        target_identity_field: Some("id".into()),
        reference_evidence: None,
        confidence: 1.0,
        justification: Some("source declaration".into()),
        first_seen_snapshot_id: Some(Uuid::new_v4()),
        last_seen_snapshot_id: None,
        last_seen_at: Some(at),
        sync_generation: Some(5),
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

fn input(edges: Vec<EntityEdge>, targets: Option<Vec<Uuid>>) -> StageOutput {
    StageOutput::NodeResolution(NodeResolutionOutput {
        relationship_changes: Default::default(),
        sub_edges: Arc::new(edges),
        chunk_entities: targets.map(|mut ids| {
            ids.sort();
            Arc::new(
                ids.into_iter()
                    .map(|chain_id| RelationshipTarget {
                        chain_id,
                        name: "endpoint".into(),
                        entity_type: "Service".into(),
                        namespace: "prod".into(),
                        version_uuid: Uuid::new_v4(),
                        version: 1,
                        key_groups: Vec::new(),
                    })
                    .collect(),
            )
        }),
        ..Default::default()
    })
}

#[tokio::test]
async fn declared_facts_preserve_fields_direction_and_self_links_without_model_calls() {
    let (ctx, llm) = context();
    let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
    let edges = vec![
        edge(source, target),
        edge(target, source),
        edge(source, source),
    ];
    let expected = serde_json::to_value(&edges).unwrap();
    for targets in [None, Some(vec![source, target])] {
        let output = DeclaredRelationshipExtractionStage
            .process(input(edges.clone(), targets), &ctx)
            .await
            .unwrap();
        let StageOutput::EdgeExtraction(output) = output else {
            panic!("wrong handoff")
        };
        assert_eq!(
            serde_json::to_value(output.edges.as_ref()).unwrap(),
            expected
        );
        assert!(output.pending_references.is_empty());
    }
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn authoritative_targets_remove_only_edges_with_missing_endpoints() {
    let (ctx, llm) = context();
    let (source, target, missing) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let retained = edge(source, target);
    let edges = vec![
        retained.clone(),
        edge(source, missing),
        edge(missing, target),
    ];
    let output = DeclaredRelationshipExtractionStage
        .process(input(edges.clone(), Some(vec![source, target])), &ctx)
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = output else {
        panic!("wrong handoff")
    };
    assert_eq!(output.edges.len(), 1);
    assert_eq!(output.edges[0].uuid, retained.uuid);
    let output = DeclaredRelationshipExtractionStage
        .process(input(edges, Some(vec![])), &ctx)
        .await
        .unwrap();
    let StageOutput::EdgeExtraction(output) = output else {
        panic!("wrong handoff")
    };
    assert!(output.edges.is_empty());
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn foreign_scope_is_rejected_before_endpoint_filtering() {
    let (ctx, llm) = context();
    let mut foreign = edge(Uuid::new_v4(), Uuid::new_v4());
    foreign.org_id = "foreign".into();
    let result = DeclaredRelationshipExtractionStage
        .process(input(vec![foreign], Some(vec![])), &ctx)
        .await;
    assert!(matches!(result, Err(StageError::StateValidation { .. })));
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn unrelated_handoffs_are_rejected_without_provider_calls() {
    let (ctx, llm) = context();
    let result = DeclaredRelationshipExtractionStage
        .process(StageOutput::Empty, &ctx)
        .await;
    assert!(matches!(result, Err(StageError::StateValidation { .. })));
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn a_thousand_explicit_commands_use_one_read_per_endpoint_kind() {
    use kg_core::models::{RelationshipChange, RelationshipEndpoint, RelationshipObservation};
    let at = Utc::now();
    let changes: Vec<_> = (0..1_000)
        .map(|index| RelationshipChange::Observe {
            relationship: RelationshipObservation {
                source: RelationshipEndpoint::Identity {
                    namespace: "prod".into(),
                    entity_type: "Service".into(),
                    key_values: indexmap::IndexMap::from([(
                        "id".into(),
                        PropertyValue::Integer(index),
                    )]),
                },
                target: RelationshipEndpoint::Chain {
                    chain_id: Uuid::new_v4(),
                },
                name: "CALLS".into(),
                description: "declared".into(),
                properties: Default::default(),
                valid_from: at,
                valid_to: None,
            },
        })
        .collect();
    // A repeated observation adds no lookup keys or database reads.
    let lookups = endpoint_lookups([&changes, &changes].into_iter(), "org").unwrap();
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let keys = std::sync::atomic::AtomicUsize::new(0);
    read_endpoints(lookups, |lookup| {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        keys.fetch_add(
            match lookup {
                kg_core::traits::EntityLookup::ByIdentity { hashes, .. } => hashes.len(),
                kg_core::traits::EntityLookup::LatestByChain { chain_ids } => chain_ids.len(),
                _ => panic!("unexpected endpoint lookup"),
            },
            std::sync::atomic::Ordering::SeqCst,
        );
        std::future::ready(Ok(Vec::new()))
    })
    .await
    .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(keys.load(std::sync::atomic::Ordering::SeqCst), 2_000);
}

#[test]
fn explicit_namespace_policy_honors_directional_grants() {
    use kg_core::tenant::namespace::{CrossNamespaceRule, EnvironmentTier, NamespacePolicy};
    let (mut ctx, _) = context();
    ctx.namespace_policy = Arc::new(NamespacePolicy {
        environment_tiers: vec![
            EnvironmentTier {
                name: "production".into(),
                namespaces: vec!["prod".into()],
            },
            EnvironmentTier {
                name: "shared".into(),
                namespaces: vec!["shared".into()],
            },
        ],
        cross_namespace_rules: vec![CrossNamespaceRule {
            source_tier: "production".into(),
            target_tiers: vec!["shared".into()],
        }],
        open_policy: false,
    });
    assert!(ctx.namespace_policy.allows("prod", "prod"));
    assert!(ctx.namespace_policy.allows("prod", "shared"));
    assert!(!ctx.namespace_policy.allows("shared", "prod"));
    assert!(!ctx.namespace_policy.allows("prod", "staging"));
}
