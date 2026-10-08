use super::*;
use kg_core::{
    models::{SnapshotDataType, SnapshotKind},
    runtime::RuntimeContextBuilder,
    test_support::{MockEmbedBackend, MockLlmBackend},
};

fn props(value: Value) -> Properties {
    PropertyValue::flatten_source(&value, &[]).unwrap()
}
fn schema(value: Value) -> AttributeSchema {
    AttributeSchema(value)
}
fn snapshot() -> SnapshotNode {
    SnapshotNode {
        uuid: Uuid::new_v4(),
        org_id: "org".into(),
        namespace: "ns".into(),
        name: "observation".into(),
        source_description: None,
        data_type: SnapshotDataType::Text,
        snapshot_kind: SnapshotKind::Incremental,
        sync_generation: None,
        complete: false,
        collection: None,
        source: "test".into(),
        content: Some("api owner is platform and replicas is 3".into()),
        captured_at: chrono::Utc::now(),
        entities: vec![],
        entity_edges: vec![],
        labels: vec![],
        tags: Default::default(),
        created_at: chrono::Utc::now(),
    }
}
fn context(responses: Vec<Value>) -> (RuntimeContext, Arc<MockLlmBackend>) {
    let llm = Arc::new(MockLlmBackend::with_responses(
        responses.into_iter().map(|v| v.to_string()).collect(),
    ));
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(kg_core::test_support::UnreachableGraph))
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .llm_extraction(llm.clone())
        .llm_default(llm.clone())
        .build()
        .unwrap();
    (ctx, llm)
}
fn owner_schema() -> AttributeSchema {
    schema(
        json!({"type":"object","properties":{"owner":{"type":"string"}},"required":["owner"],"additionalProperties":false}),
    )
}

#[test]
fn projection_preserves_nested_constraints_and_ignores_unrelated_source_fields() {
    let schema = schema(
        json!({"type":"object","properties":{"spec":{"type":"object","properties":{"port":{"type":"integer"}},"additionalProperties":false}},"additionalProperties":false}),
    );
    let values = props(json!({"id":"source-id","spec":{"port":80,"unexpected":1}}));
    let value = reconstruct(&schema.0, "", &values).unwrap().unwrap();
    assert!(schema
        .validate_attributes(&project(&schema.0, &value))
        .is_err());
    let values = props(json!({"id":"source-id","spec":{"port":80}}));
    let value = reconstruct(&schema.0, "", &values).unwrap().unwrap();
    assert!(schema
        .validate_attributes(&project(&schema.0, &value))
        .is_ok());
}
#[test]
fn arrays_null_and_opaque_objects_keep_types_and_fields() {
    let schema = schema(
        json!({"type":"object","properties":{"spec":{"type":"object","properties":{"port":{"type":"integer"}}},"items":{"type":"array","items":{"type":"object","properties":{"x":{"type":"integer"}},"required":["x"],"additionalProperties":false}},"optional":{"type":["string","null"]}}}),
    );
    let source = json!({"spec":{"port":80,"retained":true},"items":[{"x":1}],"optional":null});
    for opaque in [vec![], vec!["spec".into()]] {
        let values = PropertyValue::flatten_source(&source, &opaque).unwrap();
        let reconstructed = reconstruct(&schema.0, "", &values).unwrap().unwrap();
        assert_eq!(reconstructed, source);
        assert!(validate_present(&schema.0, &reconstructed).is_ok());
    }
    assert!(validate_present(&schema.0, &json!({"items":[{}]})).is_err());
}
#[test]
fn ambiguous_schema_and_storage_paths_are_rejected() {
    let s = json!({"type":"object","properties":{"a":{"type":"object","properties":{"b":{"type":"integer"}}},"a.b":{"type":"integer"}}});
    assert!(check_paths(&s, "", &mut HashSet::new()).is_err());
    let p = IndexMap::from([
        ("a".into(), PropertyValue::Json("{}".into())),
        ("a.b".into(), PropertyValue::Integer(1)),
    ]);
    assert!(reconstruct(&s, "", &p).is_err());
}
#[test]
fn null_is_present_and_missing_nullable_object_can_be_returned_whole() {
    let s = json!({"type":"object","properties":{"owner":{"type":["string","null"]},"spec":{"type":["object","null"],"properties":{"port":{"type":"integer"}}}}});
    let mut missing = Vec::new();
    missing_fields(&s, &json!({"owner":null}), &mut vec![], &mut missing);
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].0, vec!["spec"]);
    let mut current = json!({"owner":null});
    apply_updates(
        &mut current,
        &json!({"updates":[{"path":"spec","value":null,"quote":"spec absent"}]}),
        &missing,
        "spec absent",
    )
    .unwrap();
    assert_eq!(current, json!({"owner":null,"spec":null}));
}
#[test]
fn updates_cannot_overwrite_or_invent_paths_or_unsupported_quotes() {
    let missing = vec![(vec!["owner".into()], json!({"type":"string"}))];
    for update in [
        json!({"path":"id","value":"x","quote":"api"}),
        json!({"path":"owner","value":"x","quote":"not in content"}),
        json!({"path":"owner","value":3,"quote":"api"}),
    ] {
        assert!(apply_updates(
            &mut json!({}),
            &json!({"updates":[update]}),
            &missing,
            "api"
        )
        .is_err());
    }
    let update = json!({"path":"owner","value":"platform","quote":"api"});
    assert!(apply_updates(
        &mut json!({}),
        &json!({"updates":[update.clone(),update]}),
        &missing,
        "api"
    )
    .is_err());
    assert!(overlaps("spec.id", "spec"));
    assert!(overlaps("spec", "spec.id"));
    assert!(!overlaps("spec.other", "spec.id"));
}
#[tokio::test]
async fn structured_values_are_validated_without_model_calls() {
    let (ctx, llm) = context(vec![]);
    let mut node = super::super::entity_versioning::tests::test_entity("api");
    node.all_properties = props(json!({"owner":"platform","id":"source-id"}));
    enrich(
        &mut node,
        &snapshot(),
        &owner_schema(),
        false,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(llm.call_count(), 0);
    assert!(node.all_properties.contains_key("id"));
    node.all_properties = props(json!({"owner":7}));
    assert!(enrich(
        &mut node,
        &snapshot(),
        &owner_schema(),
        false,
        None,
        &Properties::new(),
        None,
        &ctx
    )
    .await
    .is_err());
    assert_eq!(llm.call_count(), 0);
}
#[tokio::test]
async fn text_fills_missing_only_and_keeps_explicit_null() {
    let (mut ctx, llm) = context(vec![
        json!({"updates":[{"path":"owner","value":"platform","quote":"owner is platform"}]}),
    ]);
    crate::tests::prompt_contract::configure(&mut ctx);
    let recorder = crate::tests::prompt_contract::RecordingModel::new(ctx.llm_extraction.clone());
    ctx.llm_extraction = recorder.clone();
    let mut node = super::super::entity_versioning::tests::test_entity("api");
    node.all_properties = props(json!({"replicas":3,"retained":null}));
    enrich(
        &mut node,
        &snapshot(),
        &owner_schema(),
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(llm.call_count(), 1);
    assert_eq!(
        node.all_properties["owner"],
        PropertyValue::String("platform".into())
    );
    assert_eq!(node.all_properties["replicas"], PropertyValue::Integer(3));
    assert_eq!(node.all_properties["retained"], PropertyValue::Null);
    enrich(
        &mut node,
        &snapshot(),
        &owner_schema(),
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(llm.call_count(), 1);
    recorder.assert_guidance(
        &["ENTITY_TASK_MARKER"],
        &[
            "SHARED_CONTEXT_MARKER",
            "RELATION_TASK_MARKER",
            "MATCH_TASK_MARKER",
            "SUMMARY_TASK_MARKER",
        ],
    );
}
#[tokio::test]
async fn prior_attributes_satisfy_partial_validation_without_becoming_observations() {
    let (ctx, llm) = context(vec![json!({"updates":[]}), json!({"updates":[]})]);
    let mut node = super::super::entity_versioning::tests::test_entity("api");
    let prior = props(json!({"owner":"platform"}));
    enrich(
        &mut node,
        &snapshot(),
        &owner_schema(),
        true,
        Some(&prior),
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert!(!node.all_properties.contains_key("owner"));
    let mut full = snapshot();
    full.snapshot_kind = SnapshotKind::Full;
    assert!(enrich(
        &mut node,
        &full,
        &owner_schema(),
        true,
        Some(&prior),
        &Properties::new(),
        None,
        &ctx
    )
    .await
    .is_err());
    assert_eq!(llm.call_count(), 2);
}
#[tokio::test]
async fn supplied_identity_and_name_convention_never_become_model_updates() {
    let (ctx, llm) = context(vec![]);
    let mut node = super::super::entity_versioning::tests::test_entity("api");
    let s = schema(
        json!({"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}),
    );
    enrich(
        &mut node,
        &snapshot(),
        &s,
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(llm.call_count(), 0);
    assert!(!node.all_properties.contains_key("name"));
    node.primary_key_properties = vec!["owner".into()];
    assert!(enrich(
        &mut node,
        &snapshot(),
        &owner_schema(),
        true,
        None,
        &Properties::new(),
        None,
        &ctx
    )
    .await
    .is_err());
    assert_eq!(llm.call_count(), 0);
}
#[tokio::test]
async fn unsupported_required_attributes_fail_without_defaults() {
    let (ctx, _) = context(vec![json!({"updates":[]})]);
    let mut node = super::super::entity_versioning::tests::test_entity("api");
    let error = enrich(
        &mut node,
        &snapshot(),
        &owner_schema(),
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error,StageError::StateValidation{message,..} if message=="entity attributes do not satisfy declared schema")
    );
}
fn handoff(
    mut entities: Vec<EntityNode>,
    snap: SnapshotNode,
    attributes: Option<AttributeSchema>,
    text: bool,
) -> StageOutput {
    use kg_core::{
        models::EntityTypeSchema,
        runtime::{
            schemas::ObservationSchemas,
            stage_output::{
                IdentityMatch, IdentityOutcome, NodeExtractionOutput, ObservedEntityProperties,
            },
        },
        traits::Ontology,
    };
    use std::collections::HashMap;
    for e in &mut entities {
        e.first_seen_snapshot_id = Some(snap.uuid);
        e.last_seen_snapshot_id = Some(snap.uuid);
    }
    let observations = entities
        .iter()
        .map(|e| (e.uuid, ObservedEntityProperties::from_entity(e, snap.uuid)))
        .collect();
    let matches = entities
        .iter()
        .map(|e| {
            (
                e.uuid,
                IdentityMatch {
                    chain_id: e.chain_id,
                    outcome: IdentityOutcome::New,
                    existing: None,
                },
            )
        })
        .collect();
    let schemas = ObservationSchemas {
        org_id: "org".into(),
        source: "test".into(),
        definitions: BTreeMap::from([(
            "test".into(),
            Ontology {
                entity_types: vec![EntityTypeSchema {
                    name: "Type".into(),
                    description: None,
                    properties: vec![],
                    attributes,
                    identity_properties: vec![],
                }],
                ..Default::default()
            },
        )]),
    };
    StageOutput::NodeIdentity(NodeIdentityOutput {
        identity_revisions: Default::default(),
        observations,
        matches,
        methods: Default::default(),
        chains_merged: vec![],
        extraction: NodeExtractionOutput {
            raw_text_drafts: Default::default(),
            relationship_changes: Default::default(),
            version_exclusions: Arc::new(entities.iter().map(|e| (e.uuid, vec![])).collect()),
            text_observation_ids: Arc::new(if text {
                entities.iter().map(|e| e.uuid).collect()
            } else {
                HashSet::new()
            }),
            fk_exclusions: Default::default(),
            schemas: Arc::new(HashMap::from([(snap.uuid, schemas)])),
            history: Default::default(),
            snapshot_nodes: Arc::new(vec![snap.clone()]),
            entities_by_snapshot: Arc::new(vec![(snap.uuid, entities)]),
            source_deleted: Default::default(),
            sub_edges: Default::default(),
            incomplete_extractions: Default::default(),
        },
    })
}
#[tokio::test]
async fn stage_preserves_adopted_keys_without_fabricating_observation_evidence() {
    let (ctx, llm) = context(vec![
        json!({"updates":[{"path":"owner","value":"platform","quote":"owner is platform"}]}),
    ]);
    for attributes in [None, Some(owner_schema())] {
        let mut entity = super::super::entity_versioning::tests::test_entity("api");
        entity.primary_key_properties = vec!["id".into()];
        let mut input = handoff(vec![entity], snapshot(), attributes, true);
        if let StageOutput::NodeIdentity(identity) = &mut input {
            Arc::make_mut(&mut identity.extraction.entities_by_snapshot)[0].1[0]
                .all_properties
                .insert("id".into(), PropertyValue::String("adopted".into()));
        }
        let StageOutput::NodeIdentity(output) = EntityAttributeEnrichmentStage
            .process(input, &ctx)
            .await
            .unwrap()
        else {
            panic!()
        };
        let entity = &output.extraction.entities_by_snapshot[0].1[0];
        assert_eq!(
            entity.all_properties["id"],
            PropertyValue::String("adopted".into())
        );
        assert!(!output.observations[&entity.uuid]
            .properties
            .contains_key("id"));
    }
    assert_eq!(llm.call_count(), 1);
}
#[tokio::test]
async fn simultaneous_partial_mentions_validate_together_and_reject_conflicts() {
    let (ctx, llm) = context(vec![]);
    let mut a = super::super::entity_versioning::tests::test_entity("api");
    a.all_properties = props(json!({"owner":"platform"}));
    let mut b = a.clone();
    b.uuid = Uuid::new_v4();
    b.all_properties = props(json!({"replicas":3}));
    let s = schema(
        json!({"type":"object","properties":{"owner":{"type":"string"},"replicas":{"type":"integer"}},"required":["owner","replicas"]}),
    );
    let input = handoff(
        vec![a.clone(), b.clone()],
        snapshot(),
        Some(s.clone()),
        true,
    );
    let StageOutput::NodeIdentity(output) = EntityAttributeEnrichmentStage
        .process(input, &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.observations[&a.uuid].properties.len(), 1);
    assert_eq!(output.observations[&b.uuid].properties.len(), 1);
    b.all_properties
        .insert("owner".into(), PropertyValue::String("other".into()));
    assert!(EntityAttributeEnrichmentStage
        .process(handoff(vec![a, b], snapshot(), Some(s), false), &ctx)
        .await
        .is_err());
    assert_eq!(llm.call_count(), 0);
}
#[tokio::test]
async fn cancellation_and_schema_scope_fail_before_model_work() {
    let (ctx, llm) = context(vec![]);
    let e = super::super::entity_versioning::tests::test_entity("api");
    let mut snap = snapshot();
    snap.org_id = "other".into();
    assert!(EntityAttributeEnrichmentStage
        .process(
            handoff(vec![e.clone()], snap, Some(owner_schema()), true),
            &ctx
        )
        .await
        .is_err());
    ctx.cancel.cancel();
    assert!(matches!(
        EntityAttributeEnrichmentStage
            .process(
                handoff(vec![e], snapshot(), Some(owner_schema()), true),
                &ctx
            )
            .await,
        Err(StageError::Cancelled { .. })
    ));
    assert_eq!(llm.call_count(), 0);
}
#[tokio::test]
async fn opaque_unknown_fields_and_drop_rules_survive_enrichment() {
    let (mut ctx, llm) = context(vec![
        json!({"updates":[{"path":"spec.owner","value":"platform","quote":"owner is platform"}]}),
    ]);
    ctx.entity_type_configs = Arc::new(std::collections::HashMap::from([(
        "Type".into(),
        kg_core::entity_type_config::EntityTypeConfig {
            force_json_properties: vec!["spec".into()],
            drop_properties: vec!["discard".into()],
            ..Default::default()
        },
    )]));
    let mut node = super::super::entity_versioning::tests::test_entity("api");
    node.all_properties = PropertyValue::flatten_source(
        &json!({"spec":{"unknown":7},"unrelated":"retain"}),
        &["spec".into()],
    )
    .unwrap();
    let s = schema(
        json!({"type":"object","properties":{"spec":{"type":"object","properties":{"owner":{"type":"string"}},"required":["owner"]},"discard":{"type":"string"}}}),
    );
    enrich(
        &mut node,
        &snapshot(),
        &s,
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(
        node.all_properties["spec"].to_source().unwrap(),
        json!({"unknown":7,"owner":"platform"})
    );
    assert!(node.all_properties.contains_key("unrelated"));
    assert!(!node.all_properties.contains_key("discard"));
    assert_eq!(llm.call_count(), 1);
}

#[tokio::test]
async fn restoration_cannot_satisfy_required_attributes_from_a_tombstone() {
    let (ctx, llm) = context(vec![json!({"updates":[]})]);
    let entity = super::super::entity_versioning::tests::test_entity("api");
    let mut input = handoff(vec![entity.clone()], snapshot(), Some(owner_schema()), true);
    if let StageOutput::NodeIdentity(identity) = &mut input {
        let matched = identity.matches.get_mut(&entity.uuid).unwrap();
        matched.outcome = kg_core::runtime::stage_output::IdentityOutcome::Matched;
        matched.existing = Some(kg_core::traits::EntityVersionRecord {
            uuid: Uuid::new_v4(),
            chain_id: entity.chain_id,
            version: 1,
            is_latest: false,
            entity_type: entity.entity_type.clone(),
            name: entity.name.clone(),
            namespace: entity.namespace.clone(),
            source: Some("test".into()),
            identity_hash: None,
            identity_hashes: vec![],
            structural_hash: None,
            valid_from: None,
            valid_to: None,
            deleted_at: Some(entity.valid_from - chrono::Duration::seconds(1)),
            last_seen_at: None,
            last_transition_at: None,
            sync_generation: None,
            collections: vec![],
            merged_into: None,
            embedding: None,
            stored: json!({"prop_owner":"platform","property_type_owner":"s"})
                .as_object()
                .unwrap()
                .clone(),
        });
        assert_eq!(
            matched
                .existing
                .as_ref()
                .unwrap()
                .typed_source_properties()
                .unwrap()["owner"],
            PropertyValue::String("platform".into())
        );
    }
    assert!(
        matches!(EntityAttributeEnrichmentStage.process(input,&ctx).await,Err(StageError::StateValidation{message,..}) if message=="entity attributes do not satisfy declared schema")
    );
    assert_eq!(llm.call_count(), 1);
}

#[tokio::test]
async fn one_failed_group_cancels_another_pending_call() {
    use kg_core::{
        errors::BackendError,
        traits::{
            llm_backend::{LlmMessage, LlmResponse},
            LlmBackend,
        },
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Backend {
        calls: AtomicUsize,
        dropped: Arc<AtomicBool>,
    }
    struct Guard(Arc<AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait]
    impl LlmBackend for Backend {
        async fn complete(
            &self,
            _: &[LlmMessage],
            _: Option<&Value>,
            _: Option<u32>,
        ) -> Result<LlmResponse, BackendError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let _guard = Guard(self.dropped.clone());
                std::future::pending().await
            } else {
                Err(BackendError::Unavailable("test failure".into()))
            }
        }
        fn model_id(&self) -> &str {
            "test"
        }
        fn context_window(&self) -> usize {
            128_000
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let backend = Arc::new(Backend {
        calls: AtomicUsize::new(0),
        dropped: dropped.clone(),
    });
    let (mut ctx, _) = context(vec![]);
    ctx.llm_extraction = backend;
    let a = super::super::entity_versioning::tests::test_entity("api");
    let b = super::super::entity_versioning::tests::test_entity("worker");
    let input = handoff(vec![a, b], snapshot(), Some(owner_schema()), true);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        EntityAttributeEnrichmentStage.process(input, &ctx),
    )
    .await
    .expect("a later error must not wait for the first call timeout");
    assert!(matches!(result, Err(StageError::ModelCall { .. })));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn only_an_authorized_current_deletion_resets_prior_attributes() {
    let (ctx, llm) = context(vec![]);
    for (authorized, stale) in [(true, false), (false, false), (true, true)] {
        let entity = super::super::entity_versioning::tests::test_entity("api");
        let mut input = handoff(
            vec![entity.clone()],
            snapshot(),
            Some(owner_schema()),
            false,
        );
        if let StageOutput::NodeIdentity(identity) = &mut input {
            let record = kg_core::traits::EntityVersionRecord {
                uuid: Uuid::new_v4(),
                chain_id: entity.chain_id,
                version: 1,
                is_latest: true,
                entity_type: entity.entity_type.clone(),
                name: entity.name.clone(),
                namespace: entity.namespace.clone(),
                source: Some("test".into()),
                identity_hash: None,
                identity_hashes: vec![],
                structural_hash: None,
                valid_from: Some(entity.valid_from - chrono::Duration::seconds(5)),
                valid_to: None,
                deleted_at: None,
                last_seen_at: Some(entity.valid_from - chrono::Duration::seconds(3)),
                last_transition_at: None,
                sync_generation: None,
                collections: vec![],
                merged_into: None,
                embedding: None,
                stored: json!({"prop_owner":"platform","property_type_owner":"s"})
                    .as_object()
                    .unwrap()
                    .clone(),
            };
            assert_eq!(
                record.typed_source_properties().unwrap()["owner"],
                PropertyValue::String("platform".into())
            );
            let matched = identity.matches.get_mut(&entity.uuid).unwrap();
            matched.outcome = kg_core::runtime::stage_output::IdentityOutcome::Matched;
            matched.existing = Some(record.clone());
            let mut deletion = entity.clone();
            deletion.uuid = Uuid::new_v4();
            deletion.valid_from =
                entity.valid_from - chrono::Duration::seconds(if stale { 4 } else { 1 });
            if !authorized {
                deletion.source = "other".into();
            }
            identity.matches.insert(
                deletion.uuid,
                kg_core::runtime::stage_output::IdentityMatch {
                    chain_id: entity.chain_id,
                    outcome: kg_core::runtime::stage_output::IdentityOutcome::Matched,
                    existing: Some(record),
                },
            );
            identity.extraction.source_deleted = Arc::new(vec![deletion]);
        }
        let result = EntityAttributeEnrichmentStage.process(input, &ctx).await;
        assert_eq!(
            result.is_err(),
            authorized && !stale,
            "authorized={authorized}, stale={stale}: {result:?}"
        );
    }
    assert_eq!(llm.call_count(), 0);
}
#[tokio::test]
async fn heuristic_policy_and_empty_content_do_not_call_the_model() {
    use kg_core::policy::{
        ExtractionMode, PipelinePolicy, PolicyOverride, PolicyResolver, TenantPipelinePolicy,
    };
    let (mut ctx, llm) = context(vec![]);
    let mut tenant = TenantPipelinePolicy::default();
    tenant.per_source.insert(
        "test".into(),
        PolicyOverride {
            extraction: Some(ExtractionMode::Heuristic),
            ..Default::default()
        },
    );
    ctx.policy = Arc::new(PolicyResolver::with_tenant(
        PipelinePolicy::default(),
        tenant,
    ));
    let mut entity = super::super::entity_versioning::tests::test_entity("api");
    let optional = schema(json!({"type":"object","properties":{"owner":{"type":"string"}}}));
    enrich(
        &mut entity,
        &snapshot(),
        &optional,
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy::default()));
    let mut snap = snapshot();
    snap.content = None;
    enrich(
        &mut entity,
        &snap,
        &optional,
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert!(enrich(
        &mut entity,
        &snap,
        &owner_schema(),
        true,
        None,
        &Properties::new(),
        None,
        &ctx
    )
    .await
    .is_err());
    assert_eq!(llm.call_count(), 0);
}
#[tokio::test]
async fn nullable_peer_replaces_stored_descendants_before_validation() {
    let (ctx, llm) = context(vec![]);
    let a = super::super::entity_versioning::tests::test_entity("api");
    let mut b = a.clone();
    b.uuid = Uuid::new_v4();
    b.all_properties = props(json!({"spec":null}));
    let attributes = schema(
        json!({"type":"object","properties":{"spec":{"type":["object","null"],"properties":{"region":{"type":"string"}},"required":["region"]}},"required":["spec"]}),
    );
    let mut input = handoff(vec![a.clone(), b], snapshot(), Some(attributes), true);
    let record = kg_core::traits::EntityVersionRecord {
        uuid: Uuid::new_v4(),
        chain_id: a.chain_id,
        version: 1,
        is_latest: true,
        entity_type: a.entity_type.clone(),
        name: a.name.clone(),
        namespace: a.namespace.clone(),
        source: Some("test".into()),
        identity_hash: None,
        identity_hashes: vec![],
        structural_hash: None,
        valid_from: Some(a.valid_from - chrono::Duration::seconds(5)),
        valid_to: None,
        deleted_at: None,
        last_seen_at: None,
        last_transition_at: None,
        sync_generation: None,
        collections: vec![],
        merged_into: None,
        embedding: None,
        stored: json!({"prop_spec.region":"east","property_type_spec.region":"s"})
            .as_object()
            .unwrap()
            .clone(),
    };
    assert_eq!(
        record.typed_source_properties().unwrap()["spec.region"],
        PropertyValue::String("east".into())
    );
    if let StageOutput::NodeIdentity(identity) = &mut input {
        for matched in identity.matches.values_mut() {
            matched.outcome = kg_core::runtime::stage_output::IdentityOutcome::Matched;
            matched.existing = Some(record.clone());
        }
    }
    let StageOutput::NodeIdentity(output) = EntityAttributeEnrichmentStage
        .process(input, &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(output.observations[&a.uuid].properties.is_empty());
    assert_eq!(llm.call_count(), 0);
}
// ---- G1/G2: simultaneous descriptive attribute reconciliation and isolation ----

fn snapshot_with(
    content: Option<&str>,
    at: chrono::DateTime<chrono::Utc>,
    kind: SnapshotKind,
) -> SnapshotNode {
    let mut snap = snapshot();
    snap.content = content.map(str::to_owned);
    snap.captured_at = at;
    snap.snapshot_kind = kind;
    snap
}
fn observation(
    name: &str,
    chain: Uuid,
    at: chrono::DateTime<chrono::Utc>,
    properties: Value,
) -> EntityNode {
    let mut entity = super::super::entity_versioning::tests::test_entity(name);
    entity.chain_id = chain;
    entity.valid_from = at;
    entity.all_properties = props(properties);
    entity
}
fn continuation(ctx: &mut RuntimeContext) {
    ctx.exec_config = Arc::new(kg_core::runtime::ExecutionConfig {
        continue_on_step_error: true,
    });
}
fn equivalent(path: &str, value: Value, evidence: &[(Uuid, &str)]) -> Value {
    let evidence: Vec<_> = evidence
        .iter()
        .map(|(snapshot, quote)| json!({"snapshot_id":snapshot,"quote":quote}))
        .collect();
    json!({"decisions":[{"path":path,"outcome":"equivalent","value":value,"evidence":evidence}]})
}
/// A live stored version of `chain` from an hour before `at`, owner `original`.
fn stored_version(
    template: &EntityNode,
    at: chrono::DateTime<chrono::Utc>,
) -> kg_core::traits::EntityVersionRecord {
    kg_core::traits::EntityVersionRecord {
        uuid: Uuid::new_v4(),
        chain_id: template.chain_id,
        version: 1,
        is_latest: true,
        entity_type: template.entity_type.clone(),
        name: template.name.clone(),
        namespace: template.namespace.clone(),
        source: Some("test".into()),
        identity_hash: None,
        identity_hashes: vec![],
        structural_hash: None,
        valid_from: Some(at - chrono::Duration::hours(1)),
        valid_to: None,
        deleted_at: None,
        last_seen_at: None,
        last_transition_at: None,
        sync_generation: None,
        collections: vec![],
        merged_into: None,
        embedding: None,
        stored: json!({"prop_owner":"original","property_type_owner":"s"})
            .as_object()
            .unwrap()
            .clone(),
    }
}
/// A committed version of `template`'s chain starting exactly at `at`, with
/// the given owner wording and extraction provenance.
fn committed_version(
    template: &EntityNode,
    at: chrono::DateTime<chrono::Utc>,
    owner: &str,
    extracted_by: &str,
) -> kg_core::traits::EntityVersionRecord {
    let mut record = stored_version(template, at);
    record.valid_from = Some(at);
    record.stored = json!({
        "prop_owner": owner,
        "property_type_owner": "s",
        "extracted_by": extracted_by,
    })
    .as_object()
    .unwrap()
    .clone();
    record
}
/// Mark every observation of `chain` in `inputs` as matched to `record`.
fn match_existing(
    inputs: &mut [StageOutput],
    chain: Uuid,
    record: &kg_core::traits::EntityVersionRecord,
) {
    for input in inputs {
        let StageOutput::NodeIdentity(identity) = input else {
            panic!()
        };
        for matched in identity.matches.values_mut() {
            if matched.chain_id == chain {
                matched.outcome = kg_core::runtime::stage_output::IdentityOutcome::Matched;
                matched.existing = Some(record.clone());
            }
        }
    }
}
fn identity_of(output: &Result<StageOutput, StageError>) -> &NodeIdentityOutput {
    match output {
        Ok(StageOutput::NodeIdentity(identity)) => identity,
        other => panic!("expected an accepted identity output: {other:?}"),
    }
}
fn message_of(error: &StageError) -> String {
    match error {
        StageError::StateValidation { message, .. } => message.clone(),
        other => panic!("expected a state validation error: {other:?}"),
    }
}
/// The recorded atlas-orders disagreement: two text observations of one
/// chain at one time, one in each partial snapshot.
fn owner_pair(
    at: chrono::DateTime<chrono::Utc>,
) -> ((EntityNode, SnapshotNode), (EntityNode, SnapshotNode)) {
    let chain = Uuid::new_v4();
    let first = snapshot_with(
        Some("atlas-orders is owned by the Commerce platform team"),
        at,
        SnapshotKind::Incremental,
    );
    let second = snapshot_with(
        Some("owner: commerce-platform (atlas-orders)"),
        at,
        SnapshotKind::Incremental,
    );
    (
        (
            observation(
                "atlas-orders",
                chain,
                at,
                json!({"owner":"Commerce platform"}),
            ),
            first,
        ),
        (
            observation(
                "atlas-orders",
                chain,
                at,
                json!({"owner":"commerce-platform"}),
            ),
            second,
        ),
    )
}

#[tokio::test]
async fn equivalent_descriptive_wording_reconciles_across_snapshots_in_any_order() {
    for reversed in [false, true] {
        let at = chrono::Utc::now();
        let ((a, first), (b, second)) = owner_pair(at);
        // The decision names a listed value, so the same choice needs another
        // index when the inputs arrive in the other order.
        let response = equivalent(
            "owner",
            json!(usize::from(reversed)),
            &[
                (first.uuid, "owned by the Commerce platform team"),
                (second.uuid, "owner: commerce-platform"),
            ],
        );
        let (mut ctx, llm) = context(vec![response]);
        crate::tests::prompt_contract::configure(&mut ctx);
        let recorder =
            crate::tests::prompt_contract::RecordingModel::new(ctx.llm_extraction.clone());
        ctx.llm_extraction = recorder.clone();
        let mut inputs = vec![
            handoff(vec![a.clone()], first.clone(), None, true),
            handoff(vec![b.clone()], second.clone(), None, true),
        ];
        if reversed {
            inputs.reverse();
        }
        let outputs = EntityAttributeEnrichmentStage
            .process_batch(inputs, &ctx)
            .await
            .unwrap();
        assert_eq!(outputs.len(), 2);
        for output in &outputs {
            let identity = identity_of(output);
            for (uuid, observed) in &identity.observations {
                assert_eq!(
                    observed.properties["owner"],
                    PropertyValue::String("Commerce platform".into()),
                    "reversed={reversed}"
                );
                assert!(identity.methods[uuid].ends_with("+reconciled:owner"));
            }
            let entity = &identity.extraction.entities_by_snapshot[0].1[0];
            assert_eq!(
                entity.all_properties["owner"],
                PropertyValue::String("Commerce platform".into())
            );
        }
        assert_eq!(llm.call_count(), 1);
        let requests = recorder.requests();
        let data = kg_core::test_support::source_data_json(&requests[0].0[1].content).to_string();
        assert!(data.contains(r#""values":["Commerce platform","commerce-platform"]"#) ^ reversed);
        assert!(data.contains("owned by the Commerce platform team"));
        assert!(data.contains("owner: commerce-platform (atlas-orders)"));
        assert!(!requests[0].0[0].content.contains("atlas"));
        recorder.assert_guidance(
            &["ENTITY_TASK_MARKER"],
            &[
                "SHARED_CONTEXT_MARKER",
                "RELATION_TASK_MARKER",
                "MATCH_TASK_MARKER",
                "SUMMARY_TASK_MARKER",
            ],
        );
        // Versioning's simultaneous-state check now sees one value.
        let versioned = super::super::entity_versioning::EntityVersioningStage
            .process_batch(outputs.into_iter().map(Result::unwrap).collect(), &ctx)
            .await
            .unwrap();
        assert!(versioned.iter().all(Result::is_ok), "{versioned:?}");
    }
}

#[tokio::test]
async fn same_snapshot_variants_reconcile_and_the_result_survives_a_chunk_boundary() {
    let at = chrono::Utc::now();
    let chain = Uuid::new_v4();
    let snap = snapshot_with(
        Some("atlas-orders (Commerce platform). Later: owner commerce-platform."),
        at,
        SnapshotKind::Incremental,
    );
    let a = observation(
        "atlas-orders",
        chain,
        at,
        json!({"owner":"Commerce platform"}),
    );
    let b = observation(
        "atlas-orders",
        chain,
        at,
        json!({"owner":"commerce-platform"}),
    );
    let response = equivalent("owner", json!(0), &[(snap.uuid, "owner commerce-platform")]);
    let (ctx, llm) = context(vec![response]);
    let output = EntityAttributeEnrichmentStage
        .process(handoff(vec![a.clone(), b.clone()], snap, None, true), &ctx)
        .await
        .unwrap();
    let StageOutput::NodeIdentity(identity) = &output else {
        panic!()
    };
    for uuid in [a.uuid, b.uuid] {
        assert_eq!(
            identity.observations[&uuid].properties["owner"],
            PropertyValue::String("Commerce platform".into())
        );
    }
    assert_eq!(llm.call_count(), 1);
    // The reconciled output is what a later chunk boundary would carry on.
    let versioned = super::super::entity_versioning::EntityVersioningStage
        .process(output, &ctx)
        .await;
    assert!(versioned.is_ok(), "{versioned:?}");
}

#[tokio::test]
async fn protected_conflicts_never_reach_the_model_and_zero_call_modes_stay_explicit() {
    use kg_core::policy::{
        ExtractionMode, PipelinePolicy, PolicyOverride, PolicyResolver, TenantPipelinePolicy,
    };
    let at = chrono::Utc::now();
    type Arrange = Box<
        dyn Fn(&mut EntityNode, &mut EntityNode, &mut RuntimeContext, &mut bool, &mut SnapshotNode),
    >;
    let cases: Vec<(&str, Arrange)> = vec![
        ("authoritative", Box::new(|_, _, _, text, _| *text = false)),
        (
            "identity key",
            Box::new(|a, b, _, _, _| {
                a.primary_key_properties = vec!["owner".into()];
                b.primary_key_properties = vec!["owner".into()];
            }),
        ),
        (
            "typed",
            Box::new(|_, b, _, _, _| {
                b.all_properties.insert("owner".into(), PropertyValue::Null);
            }),
        ),
        (
            "typed",
            Box::new(|a, b, _, _, _| {
                a.all_properties
                    .insert("owner".into(), PropertyValue::Integer(3));
                b.all_properties
                    .insert("owner".into(), PropertyValue::Integer(4));
            }),
        ),
        (
            "no model is configured",
            Box::new(|_, _, ctx, _, _| {
                ctx.llm_extraction = Arc::new(kg_core::traits::llm_backend::LlmDisabled);
            }),
        ),
        (
            "policy disables",
            Box::new(|_, _, ctx, _, _| {
                let mut tenant = TenantPipelinePolicy::default();
                tenant.per_source.insert(
                    "test".into(),
                    PolicyOverride {
                        extraction: Some(ExtractionMode::Heuristic),
                        ..Default::default()
                    },
                );
                ctx.policy = Arc::new(PolicyResolver::with_tenant(
                    PipelinePolicy::default(),
                    tenant,
                ));
            }),
        ),
        (
            "lacks current observation evidence",
            Box::new(|_, _, _, _, second| {
                second.content = None;
            }),
        ),
    ];
    for (expected, arrange) in cases {
        let ((mut a, first), (mut b, mut second)) = owner_pair(at);
        let (mut ctx, llm) = context(vec![]);
        let mut text = true;
        arrange(&mut a, &mut b, &mut ctx, &mut text, &mut second);
        let inputs = vec![
            handoff(vec![a], first, None, text),
            handoff(vec![b], second, None, text),
        ];
        let error = EntityAttributeEnrichmentStage
            .process_batch(inputs, &ctx)
            .await
            .unwrap_err();
        assert!(
            message_of(&error).contains(expected),
            "{expected}: {error:?}"
        );
        assert_eq!(llm.call_count(), 0, "{expected}");
    }
}

#[tokio::test]
async fn model_answers_cannot_default_invent_or_skip_evidence() {
    let at = chrono::Utc::now();
    let ((a, first), (b, second)) = owner_pair(at);
    let grounded = [
        (first.uuid, "owned by the Commerce platform team"),
        (second.uuid, "owner: commerce-platform"),
    ];
    let cases = vec![
        (
            json!({"decisions":[{"path":"owner","outcome":"conflict","value":null,"evidence":[]}]}),
            Some("genuine simultaneous attribute conflict"),
        ),
        (
            json!({"decisions":[{"path":"owner","outcome":"insufficient","value":null,"evidence":[]}]}),
            Some("insufficient evidence"),
        ),
        (
            equivalent(
                "owner",
                json!(0),
                &[(first.uuid, "not in the content"), grounded[1]],
            ),
            Some("lacks grounded observation evidence"),
        ),
        (
            equivalent("owner", json!(0), &grounded[..1]),
            Some("must cite every contributing observation"),
        ),
        (equivalent("owner", json!(5), &grounded), None),
        (equivalent("owner", Value::Null, &grounded), None),
        (equivalent("other", json!(0), &grounded), None),
        (json!({"decisions":[]}), None),
    ];
    for (response, expected) in cases {
        let (ctx, llm) = context(vec![response.clone()]);
        let inputs = vec![
            handoff(vec![a.clone()], first.clone(), None, true),
            handoff(vec![b.clone()], second.clone(), None, true),
        ];
        let error = EntityAttributeEnrichmentStage
            .process_batch(inputs, &ctx)
            .await
            .unwrap_err();
        match expected {
            Some(expected) => assert!(
                message_of(&error).contains(expected),
                "{response}: {error:?}"
            ),
            None => assert!(
                matches!(error, StageError::StepFailed { ref step, .. } if step == "decode"),
                "{response}: {error:?}"
            ),
        }
        assert_eq!(llm.call_count(), 1, "{response}");
    }
}

#[tokio::test]
async fn continuation_rejects_conflicting_and_dependent_snapshots_only() {
    for existing in [false, true] {
        let at = chrono::Utc::now();
        let ((a, first), (b, second)) = owner_pair(at);
        let later = at + chrono::Duration::seconds(1);
        let independent = snapshot_with(Some("worker runs alone"), at, SnapshotKind::Incremental);
        let c = observation("worker", Uuid::new_v4(), at, json!({"owner":"ops"}));
        let follow_up = snapshot_with(
            Some("atlas-orders still owned by Commerce platform"),
            later,
            SnapshotKind::Incremental,
        );
        let d = observation(
            "atlas-orders",
            a.chain_id,
            later,
            json!({"owner":"Commerce platform"}),
        );
        let (mut ctx, llm) = context(vec![
            json!({"decisions":[{"path":"owner","outcome":"conflict","value":null,"evidence":[]}]}),
        ]);
        let mut inputs = vec![
            handoff(vec![a.clone()], first, None, true),
            handoff(vec![b.clone()], second, None, true),
            handoff(vec![c.clone()], independent, None, true),
            handoff(vec![d.clone()], follow_up, None, true),
        ];
        if existing {
            match_existing(&mut inputs, a.chain_id, &stored_version(&a, at));
        }
        // Fail-fast: the first failure is the batch's failure.
        let error = EntityAttributeEnrichmentStage
            .process_batch(inputs.clone(), &ctx)
            .await
            .unwrap_err();
        assert!(message_of(&error).contains("genuine simultaneous attribute conflict"));
        assert_eq!(llm.call_count(), 1);

        continuation(&mut ctx);
        let outputs = EntityAttributeEnrichmentStage
            .process_batch(inputs, &ctx)
            .await
            .unwrap();
        assert!(message_of(outputs[0].as_ref().unwrap_err())
            .contains("genuine simultaneous attribute conflict"));
        assert!(message_of(outputs[1].as_ref().unwrap_err())
            .contains("genuine simultaneous attribute conflict"));
        let worker = identity_of(&outputs[2]);
        assert_eq!(
            worker.observations[&c.uuid].properties["owner"],
            PropertyValue::String("ops".into())
        );
        if existing {
            // A later observation of a stored chain does not depend on the rejected state.
            let later = identity_of(&outputs[3]);
            assert_eq!(
                later.observations[&d.uuid].properties["owner"],
                PropertyValue::String("Commerce platform".into())
            );
            assert!(!later.methods.contains_key(&d.uuid));
        } else {
            assert!(message_of(outputs[3].as_ref().unwrap_err())
                .contains("dependent new identity anchored in a rejected snapshot"));
        }
        assert_eq!(llm.call_count(), 2);
    }
}

#[tokio::test]
async fn rejected_snapshot_evidence_is_withdrawn_from_surviving_chains() {
    let at = chrono::Utc::now();
    let ((a, first), (b, second)) = owner_pair(at);
    // The first snapshot also carries an invalid structured attribute of
    // another chain, which rejects that snapshot without any model call.
    let mut broken = observation("broken", Uuid::new_v4(), at, json!({"owner":7}));
    broken.primary_key_properties = vec!["name".into()];
    let response = equivalent(
        "owner",
        json!(0),
        &[
            (first.uuid, "owned by the Commerce platform team"),
            (second.uuid, "owner: commerce-platform"),
        ],
    );
    for existing in [true, false] {
        let (mut ctx, llm) = context(vec![response.clone()]);
        continuation(&mut ctx);
        let mut inputs = vec![
            handoff(
                vec![a.clone(), broken.clone()],
                first.clone(),
                Some(owner_schema()),
                true,
            ),
            handoff(vec![b.clone()], second.clone(), Some(owner_schema()), true),
        ];
        if existing {
            match_existing(&mut inputs, a.chain_id, &stored_version(&a, at));
        }
        let outputs = EntityAttributeEnrichmentStage
            .process_batch(inputs, &ctx)
            .await
            .unwrap();
        assert!(message_of(outputs[0].as_ref().unwrap_err())
            .contains("supplied attributes violate declared schema"));
        if existing {
            // The survivor keeps its own wording: the rejected observation no
            // longer takes part in its simultaneous state, so nothing was
            // reconciled, and the accepted first-pass decision is withdrawn.
            let survivor = identity_of(&outputs[1]);
            assert_eq!(
                survivor.observations[&b.uuid].properties["owner"],
                PropertyValue::String("commerce-platform".into())
            );
            assert!(!survivor.methods.contains_key(&b.uuid));
        } else {
            // A new chain anchored in the rejected snapshot cannot survive on
            // the other snapshot alone.
            assert!(message_of(outputs[1].as_ref().unwrap_err())
                .contains("dependent new identity anchored in a rejected snapshot"));
        }
        // The first pass adjudicated once; the recomputation needed no model.
        assert_eq!(llm.call_count(), 1, "existing={existing}");
    }
}

/// A variant already committed by another chunk or run at the same instant
/// takes part in the reconciliation; equivalence keeps the committed value so
/// the persisted history is not rewritten, and the decision is recorded.
#[tokio::test]
async fn committed_same_time_variant_reconciles_to_the_committed_value() {
    let at = chrono::Utc::now();
    let ((_, first), (b, second)) = owner_pair(at);
    let mut committed = committed_version(&b, at, "Commerce platform", "llm:test");
    committed
        .stored
        .insert("first_seen_snapshot_id".into(), json!(first.uuid));
    // Listed values: the incoming wording first, then the committed one.
    let response = equivalent(
        "owner",
        json!(1),
        &[
            (second.uuid, "owner: commerce-platform"),
            (first.uuid, "Commerce platform team"),
        ],
    );
    let (mut ctx, llm) = context(vec![response]);
    ctx.graph = Arc::new(EvidenceGraph(first.clone()));
    let mut inputs = vec![handoff(vec![b.clone()], second.clone(), None, true)];
    match_existing(&mut inputs, b.chain_id, &committed);
    let outputs = EntityAttributeEnrichmentStage
        .process_batch(inputs, &ctx)
        .await
        .unwrap();
    let identity = identity_of(&outputs[0]);
    let observed = &identity.observations[&b.uuid];
    assert_eq!(
        observed.properties["owner"],
        PropertyValue::String("Commerce platform".into())
    );
    assert_eq!(observed.reconciliations.len(), 1);
    let record = &observed.reconciliations[0];
    assert_eq!(record.path, "owner");
    assert_eq!(
        record.accepted,
        PropertyValue::String("Commerce platform".into())
    );
    assert_eq!(record.alternatives.len(), 2);
    assert!(record.alternatives.iter().any(|alternative| {
        alternative.committed
            && alternative.source_uuid == committed.uuid
            && alternative.value == PropertyValue::String("Commerce platform".into())
    }));
    assert!(record.alternatives.iter().any(|alternative| {
        !alternative.committed
            && alternative.observation_uuid == Some(b.uuid)
            && alternative.source_uuid == second.uuid
    }));
    assert_eq!(record.evidence.len(), 2);
    assert!(record
        .evidence
        .iter()
        .any(|e| e.snapshot_uuid == first.uuid));
    assert!(record
        .evidence
        .iter()
        .any(|e| e.snapshot_uuid == second.uuid));
    assert!(record.model.starts_with("llm:"));
    assert!(identity.methods[&b.uuid].ends_with("+reconciled:owner"));
    assert_eq!(llm.call_count(), 1);
    // Versioning sees the committed value: a re-observation, not a new version.
    let StageOutput::NodeResolution(resolution) =
        super::super::entity_versioning::EntityVersioningStage
            .process(outputs.into_iter().next().unwrap().unwrap(), &ctx)
            .await
            .unwrap()
    else {
        panic!()
    };
    assert_eq!(resolution.nodes_unchanged.len(), 1);
    assert!(resolution.nodes_new_version.is_empty());
    assert_eq!(
        resolution.observed_properties[0].reconciliations.len(),
        1,
        "the record travels with the observation into persistence planning"
    );
}

#[tokio::test]
async fn committed_variants_keep_history_authority_and_temporal_order() {
    let at = chrono::Utc::now();
    // Choosing the incoming wording over the committed one is refused.
    let ((_, first), (b, second)) = owner_pair(at);
    let (mut ctx, llm) = context(vec![equivalent(
        "owner",
        json!(0),
        &[
            (second.uuid, "owner: commerce-platform"),
            (first.uuid, "Commerce platform team"),
        ],
    )]);
    ctx.graph = Arc::new(EvidenceGraph(first.clone()));
    let mut inputs = vec![handoff(vec![b.clone()], second.clone(), None, true)];
    let mut record = committed_version(&b, at, "Commerce platform", "llm:test");
    record
        .stored
        .insert("first_seen_snapshot_id".into(), json!(first.uuid));
    match_existing(&mut inputs, b.chain_id, &record);
    let error = EntityAttributeEnrichmentStage
        .process_batch(inputs, &ctx)
        .await
        .unwrap_err();
    assert!(matches!(error, StageError::StepFailed { ref step, .. } if step == "decode"));
    assert_eq!(llm.call_count(), 1);

    // A committed structured value is authoritative: no adjudication.
    let (ctx, llm) = context(vec![]);
    let mut inputs = vec![handoff(vec![b.clone()], second.clone(), None, true)];
    match_existing(
        &mut inputs,
        b.chain_id,
        &committed_version(&b, at, "Commerce platform", "connector"),
    );
    let error = EntityAttributeEnrichmentStage
        .process_batch(inputs, &ctx)
        .await
        .unwrap_err();
    assert!(message_of(&error).contains("authoritative"));
    assert_eq!(llm.call_count(), 0);

    // A version that started earlier is prior state, not a same-time variant:
    // the later wording is a change and is kept as observed, without a call.
    for deleted in [false, true] {
        let (ctx, llm) = context(vec![]);
        let mut inputs = vec![handoff(vec![b.clone()], second.clone(), None, true)];
        let mut earlier = committed_version(&b, at, "Commerce platform", "llm:test");
        earlier.valid_from = Some(at - chrono::Duration::hours(1));
        if deleted {
            earlier.valid_from = Some(at);
            earlier.deleted_at = Some(at - chrono::Duration::minutes(30));
        }
        match_existing(&mut inputs, b.chain_id, &earlier);
        let outputs = EntityAttributeEnrichmentStage
            .process_batch(inputs, &ctx)
            .await
            .unwrap();
        let identity = identity_of(&outputs[0]);
        assert_eq!(
            identity.observations[&b.uuid].properties["owner"],
            PropertyValue::String("commerce-platform".into())
        );
        assert!(identity.observations[&b.uuid].reconciliations.is_empty());
        assert_eq!(llm.call_count(), 0, "deleted={deleted}");
    }
}

/// A deletion reported by a snapshot that is rejected for another reason must
/// not clear the prior state a surviving observation validates against.
#[tokio::test]
async fn rejected_deletion_owner_does_not_reset_surviving_prior_state() {
    for broken in [true, false] {
        let at = chrono::Utc::now();
        let chain = Uuid::new_v4();
        let stored_template = observation("atlas-orders", chain, at, json!({}));
        // The committed version carries the required attribute.
        let record = stored_version(&stored_template, at);
        let first = snapshot_with(
            Some("atlas-orders was decommissioned; broken has owner 7"),
            at - chrono::Duration::minutes(30),
            SnapshotKind::Incremental,
        );
        let mut deletion = observation(
            "atlas-orders",
            chain,
            at - chrono::Duration::minutes(30),
            json!({}),
        );
        deletion.last_seen_snapshot_id = Some(first.uuid);
        let mut first_entities = Vec::new();
        if broken {
            first_entities.push(observation(
                "broken",
                Uuid::new_v4(),
                first.captured_at,
                json!({"owner":7}),
            ));
        } else {
            first_entities.push(observation(
                "worker",
                Uuid::new_v4(),
                first.captured_at,
                json!({"owner":"ops"}),
            ));
        }
        let mut first_input = handoff(first_entities, first.clone(), Some(owner_schema()), false);
        if let StageOutput::NodeIdentity(identity) = &mut first_input {
            identity.matches.insert(
                deletion.uuid,
                kg_core::runtime::stage_output::IdentityMatch {
                    chain_id: chain,
                    outcome: kg_core::runtime::stage_output::IdentityOutcome::Matched,
                    existing: Some(record.clone()),
                },
            );
            identity.observations.insert(
                deletion.uuid,
                kg_core::runtime::stage_output::ObservedEntityProperties::from_entity(
                    &deletion, first.uuid,
                ),
            );
            identity.extraction.source_deleted = Arc::new(vec![deletion.clone()]);
        }
        // The survivor mentions the chain without restating its owner.
        let second = snapshot_with(
            Some("atlas-orders handled the release"),
            at,
            SnapshotKind::Incremental,
        );
        let survivor = observation("atlas-orders", chain, at, json!({}));
        let mut second_input = handoff(vec![survivor.clone()], second, Some(owner_schema()), false);
        match_existing(std::slice::from_mut(&mut second_input), chain, &record);
        let (mut ctx, llm) = context(vec![]);
        continuation(&mut ctx);
        let outputs = EntityAttributeEnrichmentStage
            .process_batch(vec![first_input, second_input], &ctx)
            .await
            .unwrap();
        if broken {
            assert!(message_of(outputs[0].as_ref().unwrap_err())
                .contains("supplied attributes violate declared schema"));
            // The rejected snapshot's deletion no longer applies: the stored
            // owner satisfies the schema without any model work.
            let identity = identity_of(&outputs[1]);
            assert!(identity.observations[&survivor.uuid].properties.is_empty());
        } else {
            // An accepted authorized deletion still resets the prior state,
            // so the survivor must supply the required attribute itself.
            assert!(outputs[0].is_ok());
            assert!(message_of(outputs[1].as_ref().unwrap_err())
                .contains("entity attributes do not satisfy declared schema"));
        }
        assert_eq!(llm.call_count(), 0, "broken={broken}");
    }
}

#[tokio::test]
async fn continuation_still_aborts_on_cancellation_and_bounds_adjudication() {
    let at = chrono::Utc::now();
    let ((a, first), (b, second)) = owner_pair(at);
    let (mut ctx, llm) = context(vec![]);
    continuation(&mut ctx);
    ctx.cancel.cancel();
    let result = EntityAttributeEnrichmentStage
        .process_batch(
            vec![
                handoff(vec![a.clone()], first.clone(), None, true),
                handoff(vec![b.clone()], second.clone(), None, true),
            ],
            &ctx,
        )
        .await;
    assert!(matches!(result, Err(StageError::Cancelled { .. })));
    assert_eq!(llm.call_count(), 0);

    // More distinct values than one request may weigh is an explicit failure.
    let (mut ctx, llm) = context(vec![]);
    continuation(&mut ctx);
    let chain = a.chain_id;
    let content = (0..=MAX_ADJUDICATED_VALUES)
        .map(|i| format!("owner variant {i}"))
        .collect::<Vec<_>>()
        .join("; ");
    let snap = snapshot_with(Some(&content), at, SnapshotKind::Incremental);
    let many: Vec<_> = (0..=MAX_ADJUDICATED_VALUES)
        .map(|i| {
            observation(
                "atlas-orders",
                chain,
                at,
                json!({"owner":format!("variant {i}")}),
            )
        })
        .collect();
    let outputs = EntityAttributeEnrichmentStage
        .process_batch(vec![handoff(many, snap, None, true)], &ctx)
        .await
        .unwrap();
    assert!(message_of(outputs[0].as_ref().unwrap_err()).contains("too many distinct"));
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn protected_nested_field_does_not_block_missing_siblings() {
    let (mut ctx, llm) = context(vec![
        json!({"updates":[{"path":"spec.replicas","value":3,"quote":"replicas is 3"}]}),
    ]);
    ctx.entity_type_configs = Arc::new(std::collections::HashMap::from([(
        "Type".into(),
        kg_core::entity_type_config::EntityTypeConfig {
            drop_properties: vec!["spec.secret".into()],
            ..Default::default()
        },
    )]));
    let spec = json!({"type":["object","null"],"properties":{"secret":{"type":"string"},"replicas":{"type":"integer"}},"required":["replicas"]});
    let blocked = "spec.secret".to_string();
    let mut candidates = vec![];
    eligible_missing(
        vec!["spec".into()],
        spec.clone(),
        &[&blocked],
        &mut candidates,
    );
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].0, vec!["spec", "replicas"]);
    let attributes =
        schema(json!({"type":"object","properties":{"spec":spec},"required":["spec"]}));
    let mut entity = super::super::entity_versioning::tests::test_entity("api");
    enrich(
        &mut entity,
        &snapshot(),
        &attributes,
        true,
        None,
        &Properties::new(),
        None,
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(
        entity.all_properties["spec.replicas"],
        PropertyValue::Integer(3)
    );
    assert!(!entity.all_properties.contains_key("spec.secret"));
    assert_eq!(llm.call_count(), 1);
}

mod evidence_backend {
    use super::*;
    use async_trait::async_trait;
    use kg_core::{errors::BackendError, traits::*};
    pub(super) struct EvidenceGraph(pub SnapshotNode);
    fn unreachable_evidence_operation<T>(operation: &str) -> Result<T, BackendError> {
        panic!("unexpected {operation}")
    }
    #[async_trait]
    impl SearchBackend for EvidenceGraph {}
    #[async_trait]
    impl GraphBackend for EvidenceGraph {
        async fn snapshot_evidence(
            &self,
            _: &str,
            _: &kg_core::runtime::history::SnapshotEvidenceRequest,
        ) -> Result<Vec<kg_core::runtime::history::SnapshotEvidence>, BackendError> {
            let s = &self.0;
            Ok(vec![kg_core::runtime::history::SnapshotEvidence {
                uuid: s.uuid,
                org_id: s.org_id.clone(),
                namespace: s.namespace.clone(),
                source: s.source.clone(),
                data_type: s.data_type,
                source_description: s.source_description.clone(),
                captured_at: s.captured_at,
                created_at: s.created_at,
                content: s.content.clone().unwrap(),
            }])
        }

        async fn apply_mutations(&self, _: &str, _: &[GraphMutation]) -> Result<(), BackendError> {
            unreachable_evidence_operation("apply_mutations")
        }

        async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
            unreachable_evidence_operation("register_run")
        }

        async fn commit_batch(&self, _: &MutationBatch) -> Result<CommittedBatch, BackendError> {
            unreachable_evidence_operation("commit_batch")
        }

        async fn committed_batches(
            &self,
            _: &str,
            _: Uuid,
        ) -> Result<Vec<CommittedBatch>, BackendError> {
            unreachable_evidence_operation("committed_batches")
        }

        async fn find_entities(
            &self,
            _: &str,
            _: &EntityLookup,
        ) -> Result<Vec<EntityVersionRecord>, BackendError> {
            unreachable_evidence_operation("find_entities")
        }

        async fn find_edges(
            &self,
            _: &str,
            _: &EdgeLookup,
        ) -> Result<Vec<EdgeRecord>, BackendError> {
            unreachable_evidence_operation("find_edges")
        }

        async fn health(&self) -> Result<(), BackendError> {
            unreachable_evidence_operation("health")
        }

        async fn connect(&self) -> Result<(), BackendError> {
            unreachable_evidence_operation("connect")
        }

        async fn close(&self) -> Result<(), BackendError> {
            Ok(())
        }
    }
}
use evidence_backend::EvidenceGraph;

#[tokio::test]
async fn committed_reconciliation_requires_scoped_original_evidence_before_model_calls() {
    for invalid_evidence in [
        "missing_pointer",
        "wrong_org",
        "wrong_time",
        "empty_content",
    ] {
        let at = chrono::Utc::now();
        let ((_, mut first), (b, second)) = owner_pair(at);
        let mut record = committed_version(&b, at, "Commerce platform", "llm:test");
        if invalid_evidence != "missing_pointer" {
            record
                .stored
                .insert("first_seen_snapshot_id".into(), json!(first.uuid));
        }
        match invalid_evidence {
            "wrong_org" => first.org_id = "another-org".into(),
            "wrong_time" => first.captured_at = at - chrono::Duration::seconds(1),
            "empty_content" => first.content = Some(String::new()),
            _ => {}
        }
        let (mut ctx, llm) = context(vec![]);
        ctx.graph = Arc::new(EvidenceGraph(first));
        let mut inputs = vec![handoff(vec![b.clone()], second, None, true)];
        match_existing(&mut inputs, b.chain_id, &record);
        assert!(
            EntityAttributeEnrichmentStage
                .process_batch(inputs, &ctx)
                .await
                .is_err(),
            "{invalid_evidence}"
        );
        assert_eq!(llm.call_count(), 0, "{invalid_evidence}");
    }
}
