use super::*;
use kg_core::{
    runtime::{
        schemas::ObservationSchemas, stage_output::NodeResolutionOutput, RuntimeContextBuilder,
    },
    test_support::{MockEmbedBackend, MockLlmBackend},
};
use uuid::Uuid;

fn fixture(
    origin: RelationshipOrigin,
    properties: Value,
    required: bool,
) -> (EdgeExtractionOutput, RuntimeContext, Arc<MockLlmBackend>) {
    let now = chrono::Utc::now();
    let snapshot = serde_json::from_value(json!({"uuid":Uuid::new_v4(),"org_id":"org","namespace":"ns","name":"log","source":"logs","content":"api calls database on port 443","data_type":"text","snapshot_kind":"incremental","complete":false,"captured_at":now,"created_at":now,"entities":[],"entity_edges":[],"labels":[],"tags":{}})).unwrap();
    let mut output = EdgeExtractionOutput {
        relationship_times: Default::default(),
        relationship_directives: Default::default(),
        reference_report: Default::default(),
        snapshot_nodes: Arc::new(vec![snapshot]),
        resolution: Arc::new(NodeResolutionOutput::default()),
        resolved_nodes: Arc::new(vec![]),
        edges: Arc::new(vec![]),
        pending_references: Default::default(),
    };
    let snapshot = &output.snapshot_nodes[0];
    let mut attribute_schema = json!({"type":"object","additionalProperties":false,"properties":{"port":{"type":"integer"},"protocol":{"type":"string"}}});
    if required {
        attribute_schema["required"] = json!(["port"]);
    }
    let ontology = serde_json::from_value(
        json!({"edge_types":[{"name":"CALLS","attributes":attribute_schema}]}),
    )
    .unwrap();
    Arc::make_mut(&mut output.resolution).schemas = Arc::new(std::collections::HashMap::from([(
        snapshot.uuid,
        ObservationSchemas {
            org_id: "org".into(),
            source: "logs".into(),
            definitions: std::collections::BTreeMap::from([("logs".into(), ontology)]),
        },
    )]));
    let edge = serde_json::from_value(json!({"uuid":Uuid::new_v4(),"chain_id":Uuid::new_v4(),"identity_hash":null,"cardinality_key":null,"origin":origin,"producer_source":"logs","org_id":"org","source_chain_id":Uuid::new_v4(),"target_chain_id":Uuid::new_v4(),"name":"CALLS","description":"api calls database","all_properties":{},"confidence":0.7,"first_seen_snapshot_id":snapshot.uuid,"last_seen_snapshot_id":snapshot.uuid,"valid_from":now,"version":1,"is_latest":true,"created_at":now})).unwrap();
    output.edges = Arc::new(vec![edge]);
    Arc::make_mut(&mut output.edges)[0].all_properties =
        PropertyValue::flatten_source(&properties, &[]).unwrap();
    let llm = Arc::new(MockLlmBackend::with_responses(vec![
        json!({"updates":[{"path":"port","value":443,"quote":"port 443"}]}).to_string(),
    ]));
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(kg_core::test_support::UnreachableGraph))
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .llm_extraction(llm.clone())
        .llm_default(llm.clone())
        .build()
        .unwrap();
    (output, ctx, llm)
}

#[tokio::test]
async fn missing_fact_attribute_is_filled_without_changing_identity_or_supplied_values() {
    let (input, mut ctx, llm) =
        fixture(RelationshipOrigin::Fact, json!({"protocol":"https"}), true);
    crate::tests::prompt_contract::configure(&mut ctx);
    let recorder =
        crate::tests::prompt_contract::RecordingModel::new(ctx.llm_edge_discovery.clone());
    ctx.llm_edge_discovery = recorder.clone();
    let before = input.edges[0].clone();
    let result = enrich(before.clone(), &input, &ctx).await.unwrap();
    assert_eq!(result.uuid, before.uuid);
    assert_eq!(result.source_chain_id, before.source_chain_id);
    assert_eq!(result.valid_from, before.valid_from);
    assert_eq!(
        result.all_properties["protocol"],
        before.all_properties["protocol"]
    );
    assert_eq!(result.all_properties["port"], PropertyValue::Integer(443));
    assert_eq!(llm.call_count(), 1);
    recorder.assert_guidance(
        &["RELATION_TASK_MARKER"],
        &[
            "SHARED_CONTEXT_MARKER",
            "ENTITY_TASK_MARKER",
            "MATCH_TASK_MARKER",
            "SUMMARY_TASK_MARKER",
        ],
    );
}

#[tokio::test]
async fn deterministic_edges_never_call_model_and_required_absence_fails() {
    for origin in [RelationshipOrigin::Declared, RelationshipOrigin::Reference] {
        let (input, ctx, llm) = fixture(origin, json!({}), false);
        assert!(enrich(input.edges[0].clone(), &input, &ctx).await.is_ok());
        assert_eq!(llm.call_count(), 0);
        let (input, ctx, llm) = fixture(origin, json!({}), true);
        assert!(enrich(input.edges[0].clone(), &input, &ctx).await.is_err());
        assert_eq!(llm.call_count(), 0);
    }
}

#[tokio::test]
async fn supplied_invalid_values_are_rejected_without_model_repair() {
    let (input, ctx, llm) = fixture(RelationshipOrigin::Fact, json!({"port":"443"}), true);
    assert!(enrich(input.edges[0].clone(), &input, &ctx).await.is_err());
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn cancellation_is_observed_before_any_provider_call() {
    let (input, ctx, llm) = fixture(RelationshipOrigin::Fact, json!({}), true);
    ctx.cancel.cancel();
    assert!(matches!(
        enrich(input.edges[0].clone(), &input, &ctx).await,
        Err(StageError::Cancelled { .. })
    ));
    assert_eq!(llm.call_count(), 0);
}

#[test]
fn supplied_nulls_and_keys_are_not_eligible_but_nested_siblings_are() {
    let schema = json!({"properties":{"transport":{"properties":{"port":{"type":"integer"},"protocol":{"type":"string"}}},"nullable":{"type":["null","string"]}}});
    let mut missing = vec![];
    missing_fields(
        &schema,
        &json!({"transport":{"port":443},"nullable":null}),
        &mut vec![],
        &["transport.port".into()],
        &mut missing,
    );
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].0, vec!["transport", "protocol"]);
}

#[test]
fn updates_reject_invented_evidence_types_duplicates_and_supplied_fields() {
    let missing = vec![(vec!["port".into()], json!({"type":"integer"}))];
    for updates in [
        json!([{"path":"port","value":443,"quote":"not present"}]),
        json!([{"path":"port","value":"443","quote":"port 443"}]),
        json!([{"path":"protocol","value":"tcp","quote":"port 443"}]),
        json!([{"path":"port","value":443,"quote":"port 443"},{"path":"port","value":443,"quote":"port 443"}]),
    ] {
        assert!(apply_updates(
            &mut json!({}),
            &json!({"updates":updates}),
            &missing,
            "port 443"
        )
        .is_err());
    }
}

#[test]
fn present_validation_keeps_array_item_requirements() {
    let schema = AttributeSchema(
        json!({"type":"object","required":["port"],"properties":{"port":{"type":"integer"},"items":{"type":"array","items":{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}}}}),
    );
    assert!(super::super::relationship_schema::validate_present(&schema, &json!({})).is_ok());
    assert!(
        super::super::relationship_schema::validate_present(&schema, &json!({"items":[{}]}))
            .is_err()
    );
}

#[test]
fn model_objects_cannot_introduce_undeclared_children() {
    let schema = json!({"type":"object","properties":{"port":{"type":"integer"}}});
    assert!(declared_values(&schema, &json!({"port":443})).is_ok());
    assert!(declared_values(&schema, &json!({"port":443,"invented":"x"})).is_err());
    assert!(declared_values(
        &json!({"type":"array","items":schema}),
        &json!([{"invented":"x"}])
    )
    .is_err());
}

#[test]
fn ambiguous_dotted_schema_paths_are_rejected() {
    let schema =
        json!({"properties":{"a.b":{"type":"string"},"a":{"properties":{"b":{"type":"string"}}}}});
    assert!(check_schema_paths(&schema, "", &mut HashSet::new()).is_err());
}

#[tokio::test]
async fn producer_scope_and_missing_identity_keys_fail_without_model_calls() {
    let (input, ctx, llm) = fixture(RelationshipOrigin::Fact, json!({}), true);
    let mut edge = input.edges[0].clone();
    edge.producer_source = "other-source".into();
    assert!(enrich(edge, &input, &ctx).await.is_err());
    assert_eq!(llm.call_count(), 0);
    let mut input = input;
    let schemas = Arc::make_mut(&mut Arc::make_mut(&mut input.resolution).schemas);
    schemas
        .values_mut()
        .next()
        .unwrap()
        .definitions
        .get_mut("logs")
        .unwrap()
        .edge_types[0]
        .identifying_properties = vec!["port".into()];
    assert!(enrich(input.edges[0].clone(), &input, &ctx).await.is_err());
    assert_eq!(llm.call_count(), 0);
}

struct PromptCheck {
    timestamp: String,
    inner: MockLlmBackend,
}
#[async_trait]
impl kg_core::traits::LlmBackend for PromptCheck {
    async fn complete(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        max_tokens: Option<u32>,
    ) -> Result<kg_core::traits::llm_backend::LlmResponse, kg_core::errors::BackendError> {
        assert!(messages[0].content.contains("reference_time"));
        assert!(messages[0].content.contains("never ingestion time"));
        assert!(messages[1].content.contains(&self.timestamp));
        assert!(messages[1]
            .content
            .contains("source_description_context_only"));
        self.inner.complete(messages, schema, max_tokens).await
    }
    fn model_id(&self) -> &str {
        "mock"
    }
    fn context_window(&self) -> usize {
        128000
    }
}

#[tokio::test]
async fn prompt_anchors_dates_to_capture_time_without_changing_edge_times() {
    let (input, mut ctx, _) = fixture(RelationshipOrigin::Fact, json!({}), false);
    let timestamp = serde_json::to_value(input.snapshot_nodes[0].captured_at)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    ctx.llm_edge_discovery = Arc::new(PromptCheck {
        timestamp,
        inner: MockLlmBackend::with_responses(vec![json!({"updates":[]}).to_string()]),
    });
    let result = enrich(input.edges[0].clone(), &input, &ctx).await.unwrap();
    assert_eq!(result.valid_from, input.edges[0].valid_from);
    assert_eq!(result.created_at, input.edges[0].created_at);
}

#[test]
fn same_name_endpoints_retain_distinct_keys_without_stored_descriptive_evidence() {
    let mut a = crate::node::entity_versioning::tests::test_entity("api");
    a.primary_key_properties = vec!["repo".into()];
    a.all_properties
        .insert("repo".into(), PropertyValue::String("org/first".into()));
    a.all_properties
        .insert("old_port".into(), PropertyValue::Integer(80));
    let mut b = a.clone();
    b.chain_id = Uuid::new_v4();
    b.all_properties
        .insert("repo".into(), PropertyValue::String("org/second".into()));
    let first = endpoint_context(&a).unwrap();
    let second = endpoint_context(&b).unwrap();
    assert_eq!(first["name"], second["name"]);
    assert_ne!(
        first["identity_properties_context_only"],
        second["identity_properties_context_only"]
    );
    assert!(first["identity_properties_context_only"]
        .get("old_port")
        .is_none());
    assert_eq!(first["namespace"], json!(a.namespace));
}

#[test]
fn explicit_maps_allow_dynamic_keys_but_implicit_permission_does_not() {
    let map = json!({"type":"object","additionalProperties":{"type":"string"}});
    assert!(declared_values(&map, &json!({"region":"east"})).is_ok());
    assert!(declared_values(
        &json!({"type":"array","items":map}),
        &json!([{"region":"east"}])
    )
    .is_ok());
    assert!(declared_values(
        &json!({"type":"object","additionalProperties":true}),
        &json!({"region":"east"})
    )
    .is_ok());
    assert!(declared_values(&json!({"type":"object"}), &json!({"region":"east"})).is_err());
    let missing = vec![(vec!["tags".into()], map)];
    assert!(apply_updates(
        &mut json!({}),
        &json!({"updates":[{"path":"tags","value":{"region":1},"quote":"east"}]}),
        &missing,
        "east"
    )
    .is_err());
}
