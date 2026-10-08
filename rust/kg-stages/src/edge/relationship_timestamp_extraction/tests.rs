use super::*;
use kg_core::{
    models::EntityEdge,
    runtime::{
        extraction::RelationshipTimestampSettings, stage_output::NodeResolutionOutput,
        RuntimeContextBuilder,
    },
    test_support::{MockEmbedBackend, MockLlmBackend},
    traits::{llm_backend::LlmResponse, LlmBackend},
};
use std::sync::Mutex;

fn fixture(origin: RelationshipOrigin, content: &str) -> EdgeExtractionOutput {
    let captured = "2026-09-19T12:00:00Z";
    let snapshot: SnapshotNode = serde_json::from_value(json!({"uuid":Uuid::from_u128(10),"org_id":"org","namespace":"ns","name":"report","source":"logs","content":content,"data_type":"text","snapshot_kind":"incremental","complete":false,"captured_at":captured,"created_at":captured,"entities":[],"entity_edges":[],"labels":[],"tags":{}})).unwrap();
    let mut edge: EntityEdge = serde_json::from_value(json!({"uuid":Uuid::from_u128(100),"chain_id":Uuid::from_u128(200),"identity_hash":null,"cardinality_key":null,"origin":origin,"producer_source":"logs","org_id":"org","source_chain_id":Uuid::from_u128(300),"target_chain_id":Uuid::from_u128(400),"name":"CONNECTS_TO","description":"checkout connects to orders-rds","all_properties":{},"confidence":0.7,"first_seen_snapshot_id":snapshot.uuid,"last_seen_snapshot_id":snapshot.uuid,"valid_from":captured,"version":1,"is_latest":true,"created_at":captured})).unwrap();
    if origin == RelationshipOrigin::Declared {
        edge.time_evidence = Some(RelationshipTimeEvidence::explicit(
            snapshot.uuid,
            snapshot.captured_at,
            edge.valid_from,
            edge.valid_to,
        ));
    }
    EdgeExtractionOutput {
        relationship_times: Default::default(),
        relationship_directives: Default::default(),
        reference_report: Default::default(),
        snapshot_nodes: Arc::new(vec![snapshot]),
        resolution: Arc::new(NodeResolutionOutput::default()),
        resolved_nodes: Default::default(),
        edges: Arc::new(vec![edge]),
        pending_references: Default::default(),
    }
}
fn context(llm: Arc<dyn LlmBackend>, enabled: bool) -> RuntimeContext {
    RuntimeContextBuilder::new("org")
        .graph(Arc::new(kg_core::test_support::UnreachableGraph))
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .llm_extraction(llm.clone())
        .llm_default(llm)
        .extraction_settings(ExtractionSettings {
            relationship_timestamps: RelationshipTimestampSettings {
                enabled,
                ..Default::default()
            },
            ..Default::default()
        })
        .build()
        .unwrap()
}
fn response(start: Value, end: Value) -> Value {
    json!({"results":[{"id":Uuid::from_u128(100),"start":start,"end":end}]})
}
fn instant(value: &str, quote: &str) -> Value {
    json!({"value":value,"precision":"instant","basis":"absolute","quote":quote})
}
async fn run(
    input: EdgeExtractionOutput,
    ctx: &RuntimeContext,
) -> Result<EdgeExtractionOutput, StageError> {
    let StageOutput::EdgeExtraction(output) = RelationshipTimestampExtractionStage
        .process(StageOutput::EdgeExtraction(input), ctx)
        .await?
    else {
        panic!()
    };
    Ok(output)
}

#[tokio::test]
async fn explicit_reference_disabled_and_policy_bypass_never_call_a_model() {
    for origin in [
        RelationshipOrigin::Declared,
        RelationshipOrigin::Reference,
        RelationshipOrigin::Fact,
    ] {
        let llm = Arc::new(MockLlmBackend::failing());
        let ctx = context(llm.clone(), origin != RelationshipOrigin::Fact);
        let input = fixture(origin, "");
        let start = input.edges[0].valid_from;
        let output = run(input, &ctx).await.unwrap();
        assert_eq!(output.edges[0].valid_from, start);
        let evidence = output.edges[0].time_evidence.as_ref().unwrap();
        assert_eq!(
            evidence.outcome,
            match origin {
                RelationshipOrigin::Declared => RelationshipTimeOutcome::Explicit,
                RelationshipOrigin::Reference => RelationshipTimeOutcome::Ongoing,
                RelationshipOrigin::Fact => RelationshipTimeOutcome::Disabled,
            }
        );
        assert_eq!(llm.call_count(), 0);
    }
    let llm = Arc::new(MockLlmBackend::failing());
    let mut child = fixture(RelationshipOrigin::Declared, "");
    Arc::make_mut(&mut child.edges)[0].time_evidence = None;
    let child = run(child, &context(llm.clone(), true)).await.unwrap();
    assert_eq!(
        child.edges[0].time_evidence.as_ref().unwrap().outcome,
        RelationshipTimeOutcome::Ongoing
    );
    assert_eq!(llm.call_count(), 0);
    let mut ctx = context(llm.clone(), true);
    ctx.policy = kg_core::policy::PolicyResolver::new(kg_core::policy::PipelinePolicy {
        edge_discovery: EdgeDiscoveryMode::Heuristic,
        ..Default::default()
    })
    .into();
    assert!(run(fixture(RelationshipOrigin::Fact, ""), &ctx)
        .await
        .is_ok());
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn inferred_bounds_preserve_identity_capture_precision_and_frozen_evidence() {
    let content = "checkout connects to orders-rds since 2026-09-02";
    let answer = response(
        json!({"value":"2026-09-02","precision":"date","basis":"absolute","quote":"since 2026-09-02"}),
        Value::Null,
    );
    let llm = Arc::new(MockLlmBackend::with_responses(vec![answer.to_string()]));
    let mut ctx = context(llm.clone(), true);
    crate::tests::prompt_contract::configure(&mut ctx);
    let recorder =
        crate::tests::prompt_contract::RecordingModel::new(ctx.llm_edge_discovery.clone());
    ctx.llm_edge_discovery = recorder.clone();
    let input = fixture(RelationshipOrigin::Fact, content);
    let before = input.edges[0].clone();
    let output = run(input, &ctx).await.unwrap();
    let edge = &output.edges[0];
    assert_eq!(edge.uuid, before.uuid);
    assert_eq!(edge.chain_id, before.chain_id);
    assert_eq!(edge.source_chain_id, before.source_chain_id);
    assert_eq!(edge.all_properties, before.all_properties);
    assert_eq!(edge.last_seen_at, before.last_seen_at);
    assert_eq!(
        edge.valid_from,
        "2026-09-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert_eq!(
        edge.time_evidence
            .as_ref()
            .unwrap()
            .start
            .as_ref()
            .unwrap()
            .precision,
        TimePrecision::Date
    );
    assert_eq!(
        edge.time_evidence.as_ref().unwrap().captured_at,
        output.snapshot_nodes[0].captured_at
    );
    let again = run(output, &ctx).await.unwrap();
    assert_eq!(again.relationship_times.len(), 1);
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
async fn end_only_stays_unresolved_and_does_not_invent_an_inverted_interval() {
    let content = "stopped at 2026-09-18T23:00:00Z";
    let llm = Arc::new(MockLlmBackend::with_responses(vec![response(
        Value::Null,
        instant("2026-09-18T23:00:00Z", content),
    )
    .to_string()]));
    let ctx = context(llm, true);
    let input = fixture(RelationshipOrigin::Fact, content);
    let start = input.edges[0].valid_from;
    let output = run(input, &ctx).await.unwrap();
    let time = &output.relationship_times[&output.edges[0].uuid];
    assert!(time.end_only());
    assert!(time.resolved_target.is_none());
    assert_eq!(output.edges[0].valid_from, start);
    assert_eq!(output.edges[0].valid_to, None);
}

#[test]
fn strict_answers_reject_missing_foreign_duplicate_ids_bad_dates_and_false_evidence() {
    let snapshot = fixture(RelationshipOrigin::Fact, "2026-09-18T23:00:00Z yesterday")
        .snapshot_nodes[0]
        .clone();
    let valid = response(
        instant("2026-09-18T23:00:00Z", "2026-09-18T23:00:00Z"),
        Value::Null,
    );
    let mut cases = Vec::new();
    for field in ["id", "start", "end"] {
        let mut bad = valid.clone();
        bad["results"][0].as_object_mut().unwrap().remove(field);
        cases.push(bad);
    }
    let mut bad = valid.clone();
    bad["results"][0]["id"] = json!(Uuid::new_v4());
    cases.push(bad);
    let mut bad = valid.clone();
    bad["results"]
        .as_array_mut()
        .unwrap()
        .push(valid["results"][0].clone());
    cases.push(bad);
    let mut bad = valid.clone();
    bad["results"][0]["start"]["quote"] = json!("invented");
    cases.push(bad);
    for date in ["2026-09-18T23:00:00", "2026-02-30T00:00:00Z", "bad"] {
        let mut bad = valid.clone();
        bad["results"][0]["start"]["value"] = json!(date);
        cases.push(bad);
    }
    let mut bad = valid.clone();
    bad["results"][0]["start"]["basis"] = json!("explicit");
    cases.push(bad);
    let mut bad = valid.clone();
    bad["results"][0]["outcome"] = json!("explicit");
    cases.push(bad);
    let mut bad = valid.clone();
    bad["results"][0]["outcome"] = json!("unknown");
    cases.push(bad);
    let mut bad = valid.clone();
    bad["results"][0]["injected"] = json!(true);
    cases.push(bad);
    for bad in cases {
        assert!(parse_results(bad, &[Uuid::from_u128(100)], &snapshot).is_err());
    }
    assert!(parse_results(valid, &[Uuid::from_u128(100)], &snapshot).is_ok());
}

#[test]
fn abstention_relative_day_and_offset_dates_preserve_supported_precision() {
    let snapshot = fixture(
        RelationshipOrigin::Fact,
        "checkout connects now; started yesterday; 2026-09-18T09:15:00-04:00",
    )
    .snapshot_nodes[0]
        .clone();
    let answer = response(Value::Null, Value::Null);
    let parsed = parse_results(answer, &[Uuid::from_u128(100)], &snapshot).unwrap();
    assert_eq!(
        parsed[&Uuid::from_u128(100)].outcome,
        RelationshipTimeOutcome::Unknown
    );
    assert!(parsed[&Uuid::from_u128(100)].start.is_none());
    let answer = response(
        json!({"value":"2026-09-18","precision":"date","basis":"relative","quote":"started yesterday"}),
        Value::Null,
    );
    assert_eq!(
        parse_results(answer, &[Uuid::from_u128(100)], &snapshot).unwrap()[&Uuid::from_u128(100)]
            .start
            .as_ref()
            .unwrap()
            .at,
        "2026-09-18T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    let answer = response(
        instant("2026-09-18T09:15:00-04:00", "2026-09-18T09:15:00-04:00"),
        Value::Null,
    );
    assert_eq!(
        parse_results(answer, &[Uuid::from_u128(100)], &snapshot).unwrap()[&Uuid::from_u128(100)]
            .start
            .as_ref()
            .unwrap()
            .at,
        "2026-09-18T13:15:00Z".parse::<DateTime<Utc>>().unwrap()
    );
}

#[tokio::test]
async fn cancellation_provider_failure_invalid_scope_and_call_budget_fail_explicitly() {
    let llm = Arc::new(MockLlmBackend::failing());
    let mut ctx = context(llm.clone(), true);
    ctx.cancel.cancel();
    assert!(matches!(
        run(fixture(RelationshipOrigin::Fact, "content"), &ctx).await,
        Err(StageError::Cancelled { .. })
    ));
    assert_eq!(llm.call_count(), 0);
    ctx.cancel = tokio_util::sync::CancellationToken::new();
    assert!(matches!(
        run(fixture(RelationshipOrigin::Fact, "content"), &ctx).await,
        Err(StageError::ModelCall { .. })
    ));
    assert_eq!(llm.call_count(), 1);
    let mut input = fixture(RelationshipOrigin::Fact, "content");
    Arc::make_mut(&mut input.edges)[0].last_seen_snapshot_id = Some(Uuid::new_v4());
    assert!(run(input, &ctx).await.is_err());
    ctx.extraction_settings.relationship_timestamps.batch_size = 1;
    ctx.extraction_settings.relationship_timestamps.max_batches = 1;
    let mut input = fixture(RelationshipOrigin::Fact, "content");
    let mut second = input.edges[0].clone();
    second.uuid = Uuid::new_v4();
    Arc::make_mut(&mut input.edges).push(second);
    assert!(run(input, &ctx).await.is_err());
    assert_eq!(llm.call_count(), 1);
}

#[derive(Default)]
struct Usage {
    calls: usize,
    input: u64,
    output: u64,
    unknown: bool,
    prompts: Vec<String>,
}
struct RecordedModel {
    inner: Arc<dyn LlmBackend>,
    usage: Arc<Mutex<Usage>>,
}
#[async_trait]
impl LlmBackend for RecordedModel {
    async fn complete(
        &self,
        messages: &[LlmMessage],
        schema: Option<&Value>,
        max_tokens: Option<u32>,
    ) -> Result<LlmResponse, kg_core::errors::BackendError> {
        {
            let mut usage = self.usage.lock().unwrap();
            usage.calls += 1;
            usage.prompts.push(messages.last().unwrap().content.clone());
        }
        let result = match self.inner.complete(messages, schema, max_tokens).await {
            Ok(response) => response,
            Err(error) => {
                self.usage.lock().unwrap().unknown = true;
                return Err(error);
            }
        };
        let mut usage = self.usage.lock().unwrap();
        usage.input += result.input_tokens.unwrap_or(0) as u64;
        usage.output += result.output_tokens.unwrap_or(0) as u64;
        usage.unknown |= result.input_tokens.is_none() || result.output_tokens.is_none();
        Ok(result)
    }
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn context_window(&self) -> usize {
        self.inner.context_window()
    }
}

#[tokio::test]
async fn snapshot_batches_keep_reference_clocks_and_explicit_output_ids() {
    let answer1 = response(Value::Null, Value::Null);
    let mut answer2 = answer1.clone();
    answer2["results"][0]["id"] = json!(Uuid::from_u128(101));
    let usage = Arc::new(Mutex::new(Usage::default()));
    let llm = Arc::new(RecordedModel {
        inner: Arc::new(MockLlmBackend::with_responses(vec![
            answer1.to_string(),
            answer2.to_string(),
        ])),
        usage: usage.clone(),
    });
    let ctx = context(llm, true);
    let mut input = fixture(RelationshipOrigin::Fact, "current facts");
    let mut second = input.snapshot_nodes[0].clone();
    second.uuid = Uuid::from_u128(11);
    second.captured_at = "2026-09-16T12:00:00Z".parse().unwrap();
    Arc::make_mut(&mut input.snapshot_nodes).push(second.clone());
    let mut edge = input.edges[0].clone();
    edge.uuid = Uuid::from_u128(101);
    edge.last_seen_snapshot_id = Some(second.uuid);
    Arc::make_mut(&mut input.edges).push(edge);
    let output = run(input, &ctx).await.unwrap();
    assert_eq!(
        output.relationship_times[&Uuid::from_u128(100)].captured_at,
        "2026-09-19T12:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert_eq!(
        output.relationship_times[&Uuid::from_u128(101)].captured_at,
        second.captured_at
    );
    let usage = usage.lock().unwrap();
    assert_eq!(usage.calls, 2);
    assert!(usage.prompts[0].contains("2026-09-19T12:00:00Z"));
    assert!(!usage.prompts[0].contains("2026-09-16T12:00:00Z"));
    assert!(usage.prompts[1].contains("2026-09-16T12:00:00Z"));
}

#[tokio::test]
#[cfg(feature = "paid-tests")]
#[ignore = "paid: provider spend"]
async fn real_openai_timestamp_accuracy_and_abstention() {
    use kg_core::test_support::env as gate;
    use std::io::Write;
    let key = gate::paid().unwrap_or_else(|e| panic!("{e}")).openai_key;
    let path = gate::required("RELATIONSHIP_TIME_EVALUATION_OUTPUT")
        .unwrap_or_else(|e| panic!("{e} (a new retained report path)"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    let model = std::env::var("RELATIONSHIP_TIME_EVALUATION_MODEL")
        .unwrap_or_else(|_| "gpt-5.4-2026-03-05".into());
    let provider = kg_rig::openai_llm(&key, &model, 128_000)
        .unwrap()
        .with_options(kg_rig::CallOptions {
            max_attempts: 1,
            max_concurrent: 1,
            ..Default::default()
        });
    let usage = Arc::new(Mutex::new(Usage::default()));
    let llm = Arc::new(RecordedModel {
        inner: Arc::new(provider),
        usage: usage.clone(),
    });
    let mut ctx = context(llm, true);
    ctx.extraction_settings.max_output_tokens = 2048;
    use sha2::Digest;
    let fixture_text = include_str!(
        "../../../../../rust/kg-stages/tests/fixtures/relationship-time-fixtures-v2.json"
    );
    let fixture_hash = format!("{:x}", sha2::Sha256::digest(fixture_text.as_bytes()));
    let prompt_hash = format!("{:x}", sha2::Sha256::digest(INSTRUCTIONS.as_bytes()));
    let corpus: Value = serde_json::from_str(fixture_text).unwrap();
    let mut rows = Vec::new();
    let mut correct_cases = 0;
    let mut expected_bounds = 0;
    let mut correct_bounds = 0;
    let mut false_bounds = 0;
    let mut expected_abstentions = 0;
    let mut correct_abstentions = 0;
    let mut end_only_expected = 0;
    let mut end_only_correct = 0;
    for case in corpus["cases"].as_array().unwrap() {
        let mut input = fixture(RelationshipOrigin::Fact, case["content"].as_str().unwrap());
        let capture = case["captured_at"].as_str().unwrap().parse().unwrap();
        Arc::make_mut(&mut input.snapshot_nodes)[0].captured_at = capture;
        let edge = &mut Arc::make_mut(&mut input.edges)[0];
        edge.description = case["relationship"].as_str().unwrap().into();
        edge.name = edge.description.split_whitespace().nth(1).unwrap().into();
        edge.valid_from = capture;
        let actual = run(input, &ctx)
            .await
            .map(|o| o.relationship_times[&Uuid::from_u128(100)].clone());
        let mut correct = actual.is_ok();
        let abstain_expected = case["expected_start"].is_null() && case["expected_end"].is_null();
        expected_abstentions += usize::from(abstain_expected);
        let end_only = case["expected_start"].is_null() && !case["expected_end"].is_null();
        end_only_expected += usize::from(end_only);
        for (field, bound) in [
            (
                "expected_start",
                actual.as_ref().ok().and_then(|e| e.start.as_ref()),
            ),
            (
                "expected_end",
                actual.as_ref().ok().and_then(|e| e.end.as_ref()),
            ),
        ] {
            let expected = case[field]
                .as_str()
                .map(|s| s.parse::<DateTime<Utc>>().unwrap());
            expected_bounds += usize::from(expected.is_some());
            let equal = expected == bound.map(|b| b.at);
            let precision_ok =
                bound.is_none_or(|b| json!(b.precision) == case["expected_precision"]);
            correct_bounds += usize::from(expected.is_some() && equal && precision_ok);
            false_bounds += usize::from(bound.is_some() && (!equal || !precision_ok));
            correct &= equal && precision_ok;
        }
        if let Ok(evidence) = &actual {
            correct &= json!(evidence.outcome) == case["expected_outcome"];
            correct_abstentions +=
                usize::from(abstain_expected && evidence.start.is_none() && evidence.end.is_none());
            end_only_correct += usize::from(end_only && correct && evidence.end_only());
        }
        correct_cases += usize::from(correct);
        rows.push(json!({"id":case["id"],"expected":case,"actual":actual.as_ref().ok(),"stage_failed":actual.is_err(),"correct":correct}));
    }
    let usage = usage.lock().unwrap();
    let report = json!({"fixture_version":corpus["version"],"fixture_sha256":fixture_hash,"prompt_sha256":prompt_hash,"model":model,"settings_version":kg_pipeline::SETTINGS_VERSION,
        "cases":rows,"correct_cases":correct_cases,"total_cases":rows.len(),"expected_bounds":expected_bounds,"correct_bounds":correct_bounds,
        "date_accuracy":correct_bounds as f64 / expected_bounds.max(1) as f64,"false_inferred_bounds":false_bounds,
        "expected_abstentions":expected_abstentions,"correct_abstentions":correct_abstentions,
        "end_only_expected":end_only_expected,"end_only_correct":end_only_correct,
        "calls":usage.calls,"input_tokens":if usage.unknown {None} else {Some(usage.input)},"output_tokens":if usage.unknown {None} else {Some(usage.output)}});
    file.write_all(serde_json::to_string_pretty(&report).unwrap().as_bytes())
        .unwrap();
    file.sync_all().unwrap();
    assert_eq!(
        correct_cases,
        rows.len(),
        "inspect retained timestamp evaluation report"
    );
    assert_eq!(false_bounds, 0);
    assert_eq!(usage.calls, rows.len());
}

#[tokio::test]
async fn discovery_supplied_end_is_not_reextracted_when_time_inference_is_enabled() {
    let mut input = fixture(RelationshipOrigin::Fact, "stopped at 2026-09-18T12:00:00Z");
    let snapshot = &input.snapshot_nodes[0];
    let end = parse_model_bound(
        &instant("2026-09-18T12:00:00Z", "stopped at 2026-09-18T12:00:00Z"),
        snapshot.content.as_deref().unwrap(),
    )
    .unwrap();
    let evidence = RelationshipTimeEvidence {
        resolved_target: None,
        snapshot_id: snapshot.uuid,
        captured_at: snapshot.captured_at,
        outcome: RelationshipTimeOutcome::Inferred,
        start: None,
        end: Some(end),
    };
    Arc::make_mut(&mut input.edges)[0].time_evidence = Some(evidence.clone());
    let llm = Arc::new(MockLlmBackend::failing());
    let output = run(input, &context(llm.clone(), true)).await.unwrap();
    assert_eq!(output.edges[0].time_evidence.as_ref(), Some(&evidence));
    assert_eq!(output.relationship_times[&output.edges[0].uuid], evidence);
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn invalid_quote_gets_one_bounded_correction_without_weakening_validation() {
    let content = "2026-09-18T23:00:00Z checkout connected to orders-rds. A later sentence.";
    let invalid = response(
        instant(
            "2026-09-18T23:00:00Z",
            "2026-09-18T23:00:00Z A later sentence.",
        ),
        Value::Null,
    );
    let valid = response(
        instant(
            "2026-09-18T23:00:00Z",
            "2026-09-18T23:00:00Z checkout connected to orders-rds.",
        ),
        Value::Null,
    );
    for (budget, answers, succeeds, calls) in [
        (2, vec![invalid.clone(), valid], true, 2),
        (2, vec![invalid.clone()], false, 2),
        (1, vec![invalid], false, 1),
    ] {
        let llm = Arc::new(MockLlmBackend::with_responses(
            answers.into_iter().map(|v| v.to_string()).collect(),
        ));
        let mut ctx = context(llm.clone(), true);
        ctx.extraction_settings.relationship_timestamps.max_batches = budget;
        let output = run(fixture(RelationshipOrigin::Fact, content), &ctx).await;
        assert_eq!(output.is_ok(), succeeds);
        assert_eq!(llm.call_count(), calls);
        if let Ok(output) = output {
            assert_eq!(
                output.edges[0].valid_from,
                "2026-09-18T23:00:00Z".parse::<DateTime<Utc>>().unwrap()
            );
        }
    }
}

#[tokio::test]
async fn continuation_preserves_valid_rows_and_repairs_only_declined_observations() {
    for continue_on_error in [false, true] {
        let mut input = fixture(RelationshipOrigin::Fact, "checkout connects to orders-rds");
        let mut second = input.edges[0].clone();
        second.uuid = Uuid::from_u128(101);
        second.target_chain_id = Uuid::from_u128(401);
        Arc::make_mut(&mut input.edges).push(second);
        let bad = json!({"id":Uuid::from_u128(101),"start":instant("nonsense", "absent quote"),"end":null});
        let answer = json!({"results":[{"id":Uuid::from_u128(100),"start":null,"end":null},bad]});
        let repair = if continue_on_error {
            json!({"results":[bad]})
        } else {
            answer.clone()
        };
        let llm = Arc::new(MockLlmBackend::with_responses(vec![
            answer.to_string(),
            repair.to_string(),
        ]));
        let recorded = crate::tests::prompt_contract::RecordingModel::new(llm.clone());
        let mut ctx = context(recorded.clone(), true);
        Arc::make_mut(&mut ctx.exec_config).continue_on_step_error = continue_on_error;
        let result = run(input, &ctx).await;
        assert_eq!(llm.call_count(), 2);
        let requests = recorded.requests();
        assert!(requests[1].0[1].content.contains("previous_invalid_answer"));
        assert!(requests[1].0[1].content.contains("absent quote"));
        assert!(requests[1].0[1].content.contains("validation_error"));
        if !continue_on_error {
            assert!(result.is_err());
            continue;
        }
        let output = result.unwrap();
        assert_eq!(output.edges.len(), 1);
        assert_eq!(output.edges[0].uuid, Uuid::from_u128(100));
        assert_eq!(output.relationship_times.len(), 1);
        let declines = &output.reference_report.relationship_declines;
        assert_eq!(declines.len(), 1);
        assert_eq!(declines[0].target_chain_id, Some(Uuid::from_u128(401)));
        assert_eq!(
            declines[0].reason,
            kg_core::runtime::stage_output::RelationshipDeclineReason::InvalidTimestampEvidence
        );
    }
}
