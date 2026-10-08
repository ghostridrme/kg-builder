use super::*;
use chrono::Duration;
use kg_core::{
    runtime::{
        stage_output::{EdgeResolutionOutput, NodeResolutionOutput},
        RuntimeContextBuilder,
    },
    test_support::{MockEmbedBackend, MockLlmBackend},
    traits::LlmBackend,
};
use serde_json::Value;

fn observation() -> Observation {
    let snapshot:SnapshotNode=serde_json::from_value(json!({"uuid":Uuid::from_u128(10),"org_id":"org","namespace":"ns","name":"report","source":"logs","content":"api denies plaintext HTTP to database.","data_type":"text","snapshot_kind":"incremental","complete":false,"captured_at":"2026-09-19T12:00:00Z","created_at":"2026-09-19T12:00:00Z","entities":[],"entity_edges":[],"labels":[],"tags":{}})).unwrap();
    let mut edge = crate::edge::reference_extraction::build_fk_edge(
        &crate::node::entity_versioning::tests::test_entity("api"),
        Uuid::from_u128(4),
        "Service",
        "database",
        "name",
        "name",
        false,
    )
    .unwrap();
    edge.origin = RelationshipOrigin::Fact;
    edge.identity_hash = None;
    edge.cardinality_key = None;
    edge.uuid = Uuid::from_u128(1);
    edge.chain_id = Uuid::from_u128(2);
    edge.org_id = "org".into();
    edge.source_chain_id = Uuid::from_u128(3);
    edge.name = "DENIES".into();
    edge.description = snapshot.content.clone().unwrap();
    edge.producer_source = "logs".into();
    edge.valid_from = snapshot.captured_at;
    edge.last_seen_at = Some(snapshot.captured_at);
    edge.last_seen_snapshot_id = Some(snapshot.uuid);
    Observation {
        position: 0,
        edge,
        snapshot,
        ontology: Ontology::default(),
    }
}
fn candidate(o: &Observation) -> StoredRelationship {
    let mut candidate = super::super::relationship_matching::observed_relationship(&o.edge);
    candidate.uuid = Uuid::from_u128(20);
    candidate.chain_id = Uuid::from_u128(21);
    candidate.name = "ALLOWS".into();
    candidate.description = "api allows plaintext HTTP to database.".into();
    candidate.valid_from -= Duration::days(1);
    candidate.latest_observation = Some(candidate.valid_from);
    candidate.scope = Some(o.scope());
    candidate
}
fn context(llm: Arc<dyn LlmBackend>) -> RuntimeContext {
    RuntimeContextBuilder::new("org")
        .graph(Arc::new(kg_core::test_support::UnreachableGraph))
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .llm_extraction(llm.clone())
        .llm_default(llm.clone())
        .llm_disambiguation(llm)
        .build()
        .unwrap()
}
#[test]
fn exact_response_coverage_is_required_before_publishing_decisions() {
    assert_eq!(
        parse_response(
            r#"{"results":[{"id":1,"decision":"unsure"},{"id":0,"decision":"contradiction"}]}"#,
            2,
            4096
        )
        .unwrap(),
        vec![
            RelationshipAssessmentDecision::Contradiction,
            RelationshipAssessmentDecision::Unsure
        ]
    );
    for content in [
        r#"{"results":[]}"#,
        r#"{"results":[{"id":0,"decision":"compatible"},{"id":0,"decision":"contradiction"}]}"#,
        r#"{"results":[{"id":2,"decision":"compatible"},{"id":1,"decision":"unsure"}]}"#,
        r#"{"results":[{"id":0,"decision":"same"},{"id":1,"decision":"unsure"}]}"#,
        r#"{"results":[{"id":0,"decision":"compatible","reason":"x"},{"id":1,"decision":"unsure"}]}"#,
    ] {
        assert!(parse_response(content, 2, 4096).is_err());
    }
}
#[test]
fn authority_direction_identity_and_intervals_precede_semantic_calls() {
    let o = observation();
    let base = candidate(&o);
    assert!(eligible(&o, &base).is_some());
    let mut c = base.clone();
    c.origin = RelationshipOrigin::Declared;
    assert!(eligible(&o, &c).is_none());
    let mut c = base.clone();
    c.scope.as_mut().unwrap().source = "aws".into();
    assert!(eligible(&o, &c).is_none());
    let mut c = base.clone();
    c.scope.as_mut().unwrap().namespace = "dev".into();
    assert!(eligible(&o, &c).is_none());
    let mut c = base.clone();
    c.source_chain_id = o.edge.target_chain_id;
    c.target_chain_id = o.edge.source_chain_id;
    assert!(eligible(&o, &c).is_none());
    let mut c = base.clone();
    c.chain_id = o.edge.chain_id;
    assert!(eligible(&o, &c).is_none());
    let mut c = base.clone();
    c.ended_at = Some(o.edge.valid_from);
    assert!(eligible(&o, &c).is_none());
    let mut c = base.clone();
    c.cancelled_at = Some(c.valid_from - Duration::days(1));
    assert!(eligible(&o, &c).is_none());
    let mut c = base.clone();
    c.ended_at = Some(c.valid_from);
    assert!(eligible(&o, &c).is_none());
    let mut o = o.clone();
    o.edge.valid_to = Some(o.edge.valid_from);
    assert!(eligible(&o, &base).is_none());
}
#[tokio::test]
async fn disabled_and_deterministic_inputs_do_not_read_or_call_models() {
    let llm = Arc::new(MockLlmBackend::failing());
    let mut ctx = context(llm.clone());
    for origin in [
        RelationshipOrigin::Declared,
        RelationshipOrigin::Reference,
        RelationshipOrigin::Fact,
    ] {
        ctx.extraction_settings.relationship_contradictions.enabled =
            origin != RelationshipOrigin::Fact;
        let mut o = observation();
        o.edge.origin = origin;
        let output = EdgeResolutionOutput {
            relationship_assessments: Default::default(),
            contradiction_timelines: Default::default(),
            relationship_directives: Default::default(),
            reference_report: Default::default(),
            snapshot_nodes: Arc::new(vec![o.snapshot]),
            resolution: Arc::new(NodeResolutionOutput::default()),
            resolved_nodes: Default::default(),
            observed: Arc::new(vec![o.edge]),
            baseline: Default::default(),
        };
        let StageOutput::EdgeResolution(output) = RelationshipContradictionResolutionStage
            .process(StageOutput::EdgeResolution(output), &ctx)
            .await
            .unwrap()
        else {
            panic!()
        };
        assert!(output.relationship_assessments.is_empty());
    }
    assert_eq!(llm.call_count(), 0);
}
#[tokio::test]
async fn model_failure_publishes_no_partial_assessment() {
    let llm = Arc::new(MockLlmBackend::failing());
    let ctx = context(llm);
    let o = observation();
    let c = Comparison {
        candidate: candidate(&o),
        observation: Arc::new(o),
        target: RelationshipTarget::StoredVersion {
            uuid: Uuid::from_u128(20),
        },
        protected_properties: vec![],
    };
    assert!(compare_batch(&[c], &ctx, &ctx.extraction_settings)
        .await
        .is_err());
}

#[cfg(feature = "paid-tests")]
#[derive(Default)]
struct Usage {
    calls: usize,
    input: u64,
    output: u64,
    unknown: bool,
}
#[cfg(feature = "paid-tests")]
struct RecordedModel {
    inner: Arc<dyn LlmBackend>,
    usage: Arc<std::sync::Mutex<Usage>>,
}
#[cfg(feature = "paid-tests")]
#[async_trait]
impl LlmBackend for RecordedModel {
    async fn complete(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        max_tokens: Option<u32>,
    ) -> Result<kg_core::traits::llm_backend::LlmResponse, kg_core::errors::BackendError> {
        self.usage.lock().unwrap().calls += 1;
        let result = self.inner.complete(messages, schema, max_tokens).await;
        let mut usage = self.usage.lock().unwrap();
        match &result {
            Ok(r) => {
                usage.input += r.input_tokens.unwrap_or(0) as u64;
                usage.output += r.output_tokens.unwrap_or(0) as u64;
                usage.unknown |= r.input_tokens.is_none() || r.output_tokens.is_none();
            }
            Err(_) => usage.unknown = true,
        };
        result
    }
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn context_window(&self) -> usize {
        self.inner.context_window()
    }
}
#[tokio::test]
#[cfg(feature = "paid-tests")]
#[ignore = "paid: provider spend"]
async fn real_openai_contradiction_accuracy_and_abstention() {
    use kg_core::test_support::env as gate;
    use sha2::Digest;
    use std::io::Write;
    let key = gate::paid().unwrap_or_else(|e| panic!("{e}")).openai_key;
    let path = gate::required("RELATIONSHIP_CONTRADICTION_EVALUATION_OUTPUT")
        .unwrap_or_else(|e| panic!("{e} (a new retained report path)"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    let model = std::env::var("RELATIONSHIP_CONTRADICTION_EVALUATION_MODEL")
        .unwrap_or_else(|_| "gpt-5.4-2026-03-05".into());
    let provider = kg_rig::openai_llm(&key, &model, 128_000)
        .unwrap()
        .with_options(kg_rig::CallOptions {
            max_attempts: 1,
            max_concurrent: 1,
            ..Default::default()
        });
    let usage = Arc::new(std::sync::Mutex::new(Usage::default()));
    let ctx = context(Arc::new(RecordedModel {
        inner: Arc::new(provider),
        usage: usage.clone(),
    }));
    let fixture = include_str!(
        "../../../../../rust/kg-stages/tests/fixtures/relationship-contradiction-fixtures-v2.json"
    );
    let corpus: Value = serde_json::from_str(fixture).unwrap();
    let mut rows = Vec::new();
    let (mut tp, mut fp, mut fn_, mut correct) = (0, 0, 0, 0);
    for case in corpus["cases"].as_array().unwrap() {
        let comparison = fixture_comparison(case);
        let actual = compare_batch(&[comparison], &ctx, &ctx.extraction_settings).await;

        let decision = actual
            .as_ref()
            .ok()
            .and_then(|v| v.first())
            .map(|(_, assessment)| json!(assessment.decision));
        let expected = case["expected"].clone();
        let predicted = decision.as_ref() == Some(&json!("contradiction"));
        let positive = expected == json!("contradiction");
        tp += usize::from(positive && predicted);
        fp += usize::from(!positive && predicted);
        fn_ += usize::from(positive && !predicted);
        correct += usize::from(decision.as_ref() == Some(&expected));
        rows.push(json!({"id":case["id"],"expected":expected,"actual":decision,"stage_failed":actual.is_err()}));
    }
    let usage = usage.lock().unwrap();
    let precision = tp as f64 / (tp + fp).max(1) as f64;
    let recall = tp as f64 / (tp + fn_).max(1) as f64;
    let report = json!({"fixture_version":corpus["version"],"fixture_sha256":format!("{:x}",sha2::Sha256::digest(fixture.as_bytes())),"prompt_sha256":format!("{:x}",sha2::Sha256::digest(INSTRUCTIONS.as_bytes())),"model":model,"settings_version":kg_pipeline::SETTINGS_VERSION,"cases":rows,"correct_cases":correct,"total_cases":rows.len(),"precision":precision,"recall":recall,"f1":if precision+recall==0.0 {0.0}else{2.0*precision*recall/(precision+recall)},"false_closures":fp,"calls":usage.calls,"input_tokens":if usage.unknown {None}else{Some(usage.input)},"output_tokens":if usage.unknown {None}else{Some(usage.output)}});
    file.write_all(serde_json::to_string_pretty(&report).unwrap().as_bytes())
        .unwrap();
    file.sync_all().unwrap();
    assert_eq!(fp, 0);
    assert_eq!(correct, rows.len());
}

struct HistoryGraph(Vec<kg_core::traits::EdgeRecord>);
#[async_trait]
impl kg_core::traits::SearchBackend for HistoryGraph {}
#[async_trait]
impl kg_core::traits::GraphBackend for HistoryGraph {
    async fn apply_mutations(
        &self,
        _: &str,
        _: &[kg_core::traits::GraphMutation],
    ) -> Result<(), kg_core::errors::BackendError> {
        panic!("assessment must not write")
    }
    async fn register_run(
        &self,
        _: &kg_core::traits::RunHeader,
    ) -> Result<kg_core::traits::RunRegistration, kg_core::errors::BackendError> {
        unreachable!()
    }
    async fn commit_batch(
        &self,
        _: &kg_core::traits::MutationBatch,
    ) -> Result<kg_core::traits::CommittedBatch, kg_core::errors::BackendError> {
        panic!("assessment must not commit")
    }
    async fn committed_batches(
        &self,
        _: &str,
        _: Uuid,
    ) -> Result<Vec<kg_core::traits::CommittedBatch>, kg_core::errors::BackendError> {
        unreachable!()
    }
    async fn find_entities(
        &self,
        _: &str,
        _: &kg_core::traits::EntityLookup,
    ) -> Result<Vec<kg_core::traits::EntityVersionRecord>, kg_core::errors::BackendError> {
        unreachable!()
    }
    async fn find_edges(
        &self,
        org: &str,
        lookup: &EdgeLookup,
    ) -> Result<Vec<kg_core::traits::EdgeRecord>, kg_core::errors::BackendError> {
        assert_eq!(org, "org");
        let EdgeLookup::VersionsByEndpointChains { chain_ids } = lookup else {
            panic!("expected complete history")
        };
        Ok(self
            .0
            .iter()
            .filter(|r| {
                chain_ids.contains(&r.source_chain_id) || chain_ids.contains(&r.target_chain_id)
            })
            .cloned()
            .collect())
    }
    async fn health(&self) -> Result<(), kg_core::errors::BackendError> {
        unreachable!()
    }
    async fn connect(&self) -> Result<(), kg_core::errors::BackendError> {
        unreachable!()
    }
    async fn close(&self) -> Result<(), kg_core::errors::BackendError> {
        Ok(())
    }
}
fn history_record(o: &Observation, id: u128) -> kg_core::traits::EdgeRecord {
    let mut edge = o.edge.clone();
    edge.uuid = Uuid::from_u128(id);
    edge.chain_id = Uuid::from_u128(id + 1000);
    edge.name = "ALLOWS".into();
    edge.description = "api allows plaintext HTTP to database.".into();
    edge.valid_from -= Duration::days(1);
    edge.last_seen_at = Some(edge.valid_from);
    let mut stored =
        crate::flush::relationship_mutation_planning::edge_properties(&edge, 1, None, &o.scope());
    stored.insert("uuid".into(), json!(edge.uuid));
    kg_core::traits::EdgeRecord {
        uuid: edge.uuid,
        source_chain_id: edge.source_chain_id,
        target_chain_id: edge.target_chain_id,
        name: edge.name,
        version: 1,
        is_latest: true,
        confidence: edge.confidence,
        valid_from: Some(edge.valid_from),
        invalid_at: None,
        sync_generation: None,
        source_collections: vec![],
        stored: relationship_timeline::state(&stored),
    }
}
fn resolved_output(o: &Observation) -> EdgeResolutionOutput {
    EdgeResolutionOutput {
        relationship_assessments: Default::default(),
        contradiction_timelines: Default::default(),
        relationship_directives: Default::default(),
        reference_report: Default::default(),
        snapshot_nodes: Arc::new(vec![o.snapshot.clone()]),
        resolution: Arc::new(NodeResolutionOutput::default()),
        resolved_nodes: Default::default(),
        observed: Arc::new(vec![o.edge.clone()]),
        baseline: Default::default(),
    }
}
#[tokio::test]
async fn full_history_is_fenced_and_all_model_decisions_are_published_together() {
    let o = observation();
    let record = history_record(&o, 20);
    let llm = Arc::new(MockLlmBackend::with_responses(vec![
        r#"{"results":[{"id":0,"decision":"contradiction"}]}"#.into(),
    ]));
    let mut ctx = context(llm.clone());
    crate::tests::prompt_contract::configure(&mut ctx);
    let recorder =
        crate::tests::prompt_contract::RecordingModel::new(ctx.llm_disambiguation.clone());
    ctx.llm_disambiguation = recorder.clone();
    ctx.graph = Arc::new(HistoryGraph(vec![record.clone()]));
    ctx.extraction_settings.relationship_contradictions.enabled = true;
    let StageOutput::EdgeResolution(result) = RelationshipContradictionResolutionStage
        .process(StageOutput::EdgeResolution(resolved_output(&o)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.relationship_assessments.len(), 1);
    assert_eq!(
        result.relationship_assessments[0].candidate,
        RelationshipTarget::StoredVersion { uuid: record.uuid }
    );
    assert_eq!(
        result.contradiction_timelines[&o.edge.source_chain_id][0].properties,
        record.stored
    );
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
async fn candidate_overflow_fails_before_model_work_and_empty_reads_still_have_a_fence() {
    let o = observation();
    let llm = Arc::new(MockLlmBackend::failing());
    let mut ctx = context(llm.clone());
    ctx.extraction_settings.relationship_contradictions.enabled = true;
    ctx.extraction_settings
        .relationship_contradictions
        .max_candidates = 1;
    ctx.graph = Arc::new(HistoryGraph(vec![
        history_record(&o, 20),
        history_record(&o, 21),
    ]));
    assert!(RelationshipContradictionResolutionStage
        .process(StageOutput::EdgeResolution(resolved_output(&o)), &ctx)
        .await
        .is_err());
    assert_eq!(llm.call_count(), 0);
    ctx.graph = Arc::new(HistoryGraph(vec![]));
    let StageOutput::EdgeResolution(result) = RelationshipContradictionResolutionStage
        .process(StageOutput::EdgeResolution(resolved_output(&o)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(result
        .contradiction_timelines
        .contains_key(&o.edge.source_chain_id));
    assert!(result.relationship_assessments.is_empty());
}
#[test]
fn opposite_names_keep_identifying_qualifiers_and_do_not_block_on_different_hashes() {
    let mut o = observation();
    let mut c = candidate(&o);
    o.ontology.edge_types.push(kg_core::models::EdgeTypeSchema {
        name: "DENIES".into(),
        identifying_properties: vec!["port".into()],
        single_target: false,
        attributes: None,
        description: None,
        source_type: None,
        target_type: None,
    });
    o.edge.identity_hash = Some("deny-port".into());
    c.identity_hash = Some("allow-port".into());
    o.edge
        .all_properties
        .insert("port".into(), kg_core::models::PropertyValue::Integer(443));
    c.all_properties
        .insert("port".into(), kg_core::models::PropertyValue::Integer(443));
    assert_eq!(eligible(&o, &c), Some(vec!["port".into()]));
    c.all_properties
        .insert("port".into(), kg_core::models::PropertyValue::Integer(80));
    assert!(eligible(&o, &c).is_none());
}

fn fixture_comparison(case: &Value) -> Comparison {
    use kg_core::models::{
        PropertyValue, RelationshipTimeBound, RelationshipTimeEvidence, RelationshipTimeOutcome,
        TimeBasis, TimePrecision,
    };
    let mut o = observation();
    o.snapshot.captured_at = case["captured_at"].as_str().unwrap().parse().unwrap();
    o.snapshot.content = Some(case["current_content"].as_str().unwrap().into());
    let set = |edge: &mut EntityEdge,
               field: &Value,
               snapshot: Uuid,
               capture: chrono::DateTime<chrono::Utc>| {
        edge.source_chain_id = field["source_chain_id"].as_str().unwrap().parse().unwrap();
        edge.target_chain_id = field["target_chain_id"].as_str().unwrap().parse().unwrap();
        edge.name = field["name"].as_str().unwrap().into();
        edge.description = field["statement"].as_str().unwrap().into();
        edge.valid_from = field["effective_start"].as_str().unwrap().parse().unwrap();
        edge.valid_to = field["effective_end"].as_str().map(|s| s.parse().unwrap());
        edge.all_properties = field["properties"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    match value {
                        Value::Bool(v) => PropertyValue::Bool(*v),
                        Value::Number(v) => PropertyValue::Integer(v.as_i64().unwrap()),
                        Value::String(v) => PropertyValue::String(v.clone()),
                        _ => panic!("unsupported fixture property"),
                    },
                )
            })
            .collect();
        edge.last_seen_at = Some(capture);
        edge.last_seen_snapshot_id = Some(snapshot);
        let inferred = field["time_outcome"] == "inferred";
        let bound = |at| RelationshipTimeBound {
            at,
            precision: TimePrecision::Date,
            basis: TimeBasis::Absolute,
            quote: Some(edge.description.clone()),
        };
        edge.time_evidence = Some(RelationshipTimeEvidence {
            resolved_target: None,
            snapshot_id: snapshot,
            captured_at: capture,
            outcome: if inferred {
                RelationshipTimeOutcome::Inferred
            } else {
                RelationshipTimeOutcome::Unknown
            },
            start: inferred.then(|| bound(edge.valid_from)),
            end: if inferred {
                edge.valid_to.map(bound)
            } else {
                None
            },
        });
        edge.time_evidence.as_ref().unwrap().validate().unwrap();
    };
    set(
        &mut o.edge,
        &case["incoming"],
        o.snapshot.uuid,
        o.snapshot.captured_at,
    );
    let mut prior = o.edge.clone();
    prior.uuid = Uuid::from_u128(20);
    prior.chain_id = Uuid::from_u128(21);
    set(
        &mut prior,
        &case["candidate"],
        Uuid::from_u128(11),
        o.snapshot.captured_at - Duration::days(1),
    );
    let mut candidate = super::super::relationship_matching::observed_relationship(&prior);
    candidate.scope = Some(o.scope());
    Comparison {
        observation: Arc::new(o),
        candidate,
        target: RelationshipTarget::StoredVersion { uuid: prior.uuid },
        protected_properties: vec![],
    }
}
#[test]
fn retained_fixture_envelope_preserves_endpoints_properties_and_temporal_evidence() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../../../rust/kg-stages/tests/fixtures/relationship-contradiction-fixtures-v2.json"
    ))
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let comparison = fixture_comparison(case);
        assert_eq!(
            comparison.observation.edge.source_chain_id,
            comparison.candidate.source_chain_id
        );
        assert_eq!(
            comparison.observation.edge.target_chain_id,
            comparison.candidate.target_chain_id
        );
        assert_eq!(
            comparison.observation.edge.description,
            case["incoming"]["statement"]
        );
        comparison
            .observation
            .edge
            .time_evidence
            .as_ref()
            .unwrap()
            .validate()
            .unwrap();
    }
}
#[test]
fn canonical_reobservation_uses_existing_interval_without_replacing_the_capture_clock() {
    let mut o = observation();
    let mut canonical = candidate(&o);
    canonical.chain_id = o.edge.chain_id;
    canonical.name = o.edge.name.clone();
    canonical.description = o.edge.description.clone();
    let capture = o.snapshot.captured_at;
    normalize_reobservation(&mut o, &[canonical.clone()]);
    assert_eq!(o.edge.valid_from, canonical.valid_from);
    assert_eq!(o.snapshot.captured_at, capture);
    assert_eq!(o.edge.last_seen_at, Some(capture));
    let mut changed = observation();
    canonical.scope.as_mut().unwrap().source = "other".into();
    normalize_reobservation(&mut changed, &[canonical]);
    assert_eq!(changed.edge.valid_from, capture);
}

#[tokio::test]
async fn batch_publishes_large_timeline_once_and_call_budget_fails_before_models() {
    let mut o = observation();
    let llm = Arc::new(MockLlmBackend::failing());
    let mut ctx = context(llm.clone());
    ctx.extraction_settings.relationship_contradictions.enabled = true;
    ctx.graph = Arc::new(HistoryGraph(vec![]));
    let first = resolved_output(&o);
    o.edge.uuid = Uuid::from_u128(101);
    o.snapshot.uuid = Uuid::from_u128(11);
    o.edge.last_seen_snapshot_id = Some(o.snapshot.uuid);
    let second = resolved_output(&o);
    let output = RelationshipContradictionResolutionStage
        .process_batch(
            vec![
                StageOutput::EdgeResolution(first),
                StageOutput::EdgeResolution(second),
            ],
            &ctx,
        )
        .await
        .unwrap();
    let maps: Vec<_> = output
        .iter()
        .map(|o| match o.as_ref().unwrap() {
            StageOutput::EdgeResolution(o) => o.contradiction_timelines.len(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(maps, vec![2, 0]);
    ctx.graph = Arc::new(HistoryGraph(vec![
        history_record(&o, 20),
        history_record(&o, 21),
    ]));
    ctx.extraction_settings
        .relationship_contradictions
        .batch_size = 1;
    ctx.extraction_settings
        .relationship_contradictions
        .max_batches = 1;
    assert!(RelationshipContradictionResolutionStage
        .process(StageOutput::EdgeResolution(resolved_output(&o)), &ctx)
        .await
        .is_err());
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn malformed_history_fails_before_sorting_or_model_work() {
    let observation = observation();
    let mut record = history_record(&observation, 20);
    record.stored.remove("uuid");
    let model = Arc::new(MockLlmBackend::failing());
    let mut context = context(model.clone());
    context.graph = Arc::new(HistoryGraph(vec![record]));
    context
        .extraction_settings
        .relationship_contradictions
        .enabled = true;
    assert!(RelationshipContradictionResolutionStage
        .process(
            StageOutput::EdgeResolution(resolved_output(&observation)),
            &context,
        )
        .await
        .is_err());
    assert_eq!(model.call_count(), 0);
}

#[tokio::test]
async fn different_subject_same_target_is_assessed_with_a_target_history_fence() {
    let o = observation();
    let mut record = history_record(&o, 20);
    record.source_chain_id = Uuid::from_u128(99);
    record
        .stored
        .insert("source_chain_id".into(), json!(record.source_chain_id));
    let llm = Arc::new(MockLlmBackend::with_responses(vec![
        r#"{"results":[{"id":0,"decision":"contradiction"}]}"#.into(),
    ]));
    let mut ctx = context(llm.clone());
    ctx.graph = Arc::new(HistoryGraph(vec![record.clone()]));
    ctx.extraction_settings.relationship_contradictions.enabled = true;
    let StageOutput::EdgeResolution(result) = RelationshipContradictionResolutionStage
        .process(StageOutput::EdgeResolution(resolved_output(&o)), &ctx)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.relationship_assessments.len(), 1);
    assert_eq!(
        result.relationship_assessments[0].candidate,
        RelationshipTarget::StoredVersion { uuid: record.uuid }
    );
    assert_eq!(
        result.contradiction_timelines[&o.edge.target_chain_id][0].properties,
        record.stored
    );
    assert!(result.contradiction_timelines[&o.edge.source_chain_id].is_empty());
    assert_eq!(llm.call_count(), 1);
}

#[tokio::test]
async fn typed_assessment_decides_every_pair_or_none() {
    use kg_core::test_support::MockDecisionBackend;
    use kg_core::traits::{Answer, Question};
    let o = observation();
    let pairs = vec![
        json!({"id":0,"incoming":"a","candidate":"b"}),
        json!({"id":1,"incoming":"c","candidate":"d"}),
    ];
    let answer = |value: &str, confidence: f32| Answer {
        value: json!(value),
        confidence,
        probabilities: std::collections::BTreeMap::new(),
    };
    let mut ctx = context(Arc::new(MockLlmBackend::failing()));
    let mock = Arc::new(MockDecisionBackend::with_answers(
        std::collections::BTreeMap::from([
            ("c0".to_string(), answer("compatible", 0.95)),
            ("c1".to_string(), answer("contradiction", 0.93)),
        ]),
    ));
    ctx.decisions = Some(mock.clone());
    ctx.typed_decisions.enabled = true;
    let settings = ExtractionSettings {
        relationship_instructions: Some("DOMAIN_REL".into()),
        ..Default::default()
    };
    let decisions = typed_assess(&pairs, &o.snapshot, &ctx, &settings)
        .await
        .unwrap();
    assert_eq!(
        decisions,
        vec![
            RelationshipAssessmentDecision::Compatible,
            RelationshipAssessmentDecision::Contradiction
        ]
    );
    let (state, questions) = &mock.requests()[0];
    assert_eq!(state["comparisons"], json!(pairs));
    assert_eq!(questions.len(), 2);
    let Question::Choice {
        instructions,
        options,
    } = &questions["c1"]
    else {
        panic!("choice")
    };
    assert!(
        instructions.contains("DOMAIN_REL"),
        "caller guidance reaches the question"
    );
    assert_eq!(
        options.keys().collect::<Vec<_>>(),
        ["compatible", "contradiction", "unsure"]
    );

    // One answer below the floor: the whole batch goes to the language model.
    mock.set_answers(std::collections::BTreeMap::from([
        ("c0".to_string(), answer("compatible", 0.95)),
        ("c1".to_string(), answer("unsure", 0.55)),
    ]));
    assert!(typed_assess(&pairs, &o.snapshot, &ctx, &settings)
        .await
        .is_none());
    // Switch off: no call.
    ctx.typed_decisions.enabled = false;
    assert!(typed_assess(&pairs, &o.snapshot, &ctx, &settings)
        .await
        .is_none());
    assert_eq!(mock.call_count(), 2);
}
