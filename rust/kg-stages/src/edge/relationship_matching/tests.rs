use super::*;
use kg_core::{
    models::PropertyValue,
    runtime::RuntimeContextBuilder,
    test_support::{MockEmbedBackend, MockLlmBackend},
};
use std::sync::Arc;
use uuid::Uuid;

fn edge() -> EntityEdge {
    let source = crate::node::entity_versioning::tests::test_entity("api");
    let mut edge = crate::edge::reference_extraction::build_fk_edge(
        &source,
        Uuid::new_v4(),
        "Service",
        "target",
        "dependency",
        "name",
        false,
    )
    .unwrap();
    edge.origin = RelationshipOrigin::Fact;
    edge.name = "USES".into();
    edge.description = "api uses target".into();
    edge
}
fn context(responses: Vec<Value>) -> (RuntimeContext, Arc<MockLlmBackend>) {
    let llm = Arc::new(MockLlmBackend::with_responses(
        responses
            .into_iter()
            .map(|value| value.to_string())
            .collect(),
    ));
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(kg_core::test_support::UnreachableGraph))
        .llm_extraction(llm.clone())
        .llm_disambiguation(llm.clone())
        .llm_default(llm.clone())
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .build()
        .unwrap();
    (ctx, llm)
}

#[tokio::test]
async fn trusted_keys_reuse_ended_lineage_but_keep_current_mutable_values() {
    let (ctx, llm) = context(vec![]);
    let mut prior = edge();
    prior.origin = RelationshipOrigin::Declared;
    prior
        .all_properties
        .insert("port".into(), PropertyValue::Integer(443));
    prior
        .all_properties
        .insert("timeout".into(), PropertyValue::Integer(10));
    prepare_identity(&mut prior, &["port".into()], true, "test-ns").unwrap();
    let mut candidate = observed_relationship(&prior);
    candidate.ended_at = Some(prior.valid_from);
    let mut incoming = prior.clone();
    incoming.uuid = Uuid::new_v4();
    incoming.chain_id = Uuid::new_v4();
    incoming
        .all_properties
        .insert("timeout".into(), PropertyValue::Integer(20));
    prepare_identity(&mut incoming, &["port".into()], true, "test-ns").unwrap();
    resolve_identity(&mut incoming, &[candidate], &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, prior.chain_id);
    assert_eq!(
        incoming.all_properties["timeout"],
        PropertyValue::Integer(20)
    );
    assert_eq!(llm.call_count(), 0);
}

#[test]
fn explicit_cardinality_slot_excludes_target_but_retains_qualifiers() {
    let mut first = edge();
    first.origin = RelationshipOrigin::Declared;
    first
        .all_properties
        .insert("env".into(), PropertyValue::String("prod".into()));
    prepare_identity(&mut first, &["env".into()], true, "test-ns").unwrap();
    let mut other = first.clone();
    other.target_chain_id = Uuid::new_v4();
    prepare_identity(&mut other, &["env".into()], true, "test-ns").unwrap();
    assert_ne!(first.identity_hash, other.identity_hash);
    assert_eq!(first.cardinality_key, other.cardinality_key);
    other
        .all_properties
        .insert("env".into(), PropertyValue::String("test".into()));
    prepare_identity(&mut other, &["env".into()], true, "test-ns").unwrap();
    assert_ne!(first.cardinality_key, other.cardinality_key);
    prepare_identity(&mut other, &["env".into()], false, "test-ns").unwrap();
    assert!(other.cardinality_key.is_none());
}

#[test]
fn reference_role_is_separate_from_user_key_names() {
    let mut reference = edge();
    reference.origin = RelationshipOrigin::Reference;
    reference.source_property = Some("dependency".into());
    reference.all_properties.insert(
        "$reference_property".into(),
        PropertyValue::String("user-value".into()),
    );
    prepare_identity(
        &mut reference,
        &["$reference_property".into()],
        false,
        "test-ns",
    )
    .unwrap();
    let mut declared = reference.clone();
    declared.origin = RelationshipOrigin::Declared;
    prepare_identity(
        &mut declared,
        &["$reference_property".into()],
        false,
        "test-ns",
    )
    .unwrap();
    assert_ne!(reference.identity_hash, declared.identity_hash);
}

#[tokio::test]
async fn semantic_same_keeps_canonical_name_wording_and_attributes() {
    let (mut ctx, llm) = context(vec![json!({"decision":"same","candidate":0})]);
    crate::tests::prompt_contract::configure(&mut ctx);
    let recorder =
        crate::tests::prompt_contract::RecordingModel::new(ctx.llm_disambiguation.clone());
    ctx.llm_disambiguation = recorder.clone();
    let mut prior = edge();
    prior.name = "DEPENDS_ON".into();
    prior
        .all_properties
        .insert("release_id".into(), PropertyValue::String("r1".into()));
    let mut incoming = prior.clone();
    incoming.chain_id = Uuid::new_v4();
    incoming.name = "USES".into();
    incoming.description = "target is required by api".into();
    incoming.all_properties.clear();
    incoming
        .all_properties
        .insert("release".into(), PropertyValue::String("r1".into()));
    resolve_identity(
        &mut incoming,
        &[observed_relationship(&prior)],
        &ctx,
        &Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(incoming.chain_id, prior.chain_id);
    assert_eq!(incoming.name, prior.name);
    assert_eq!(incoming.description, prior.description);
    assert_eq!(incoming.all_properties, prior.all_properties);
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
async fn enriched_schema_qualifiers_cannot_be_erased_by_semantic_adoption() {
    let (ctx, llm) = context(vec![]);
    let mut incoming = edge();
    let prior = observed_relationship(&incoming);
    incoming.chain_id = Uuid::new_v4();
    let expected_chain = incoming.chain_id;
    incoming
        .all_properties
        .insert("port".into(), PropertyValue::Integer(443));
    let ontology = serde_json::from_value(json!({"edge_types":[{
        "name":"USES", "attributes":{"type":"object", "properties":{
            "port":{"type":"integer"}
        }, "additionalProperties":false}
    }]}))
    .unwrap();
    resolve_identity(&mut incoming, &[prior], &ctx, &ontology, None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, expected_chain);
    assert_eq!(incoming.all_properties["port"], PropertyValue::Integer(443));
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn conflicting_typed_qualifiers_and_ended_facts_skip_the_model() {
    let (ctx, llm) = context(vec![]);
    let mut incoming = edge();
    incoming
        .all_properties
        .insert("port".into(), PropertyValue::Integer(443));
    let original = incoming.chain_id;
    let mut prior = observed_relationship(&incoming);
    prior.chain_id = Uuid::new_v4();
    prior
        .all_properties
        .insert("port".into(), PropertyValue::String("443".into()));
    resolve_identity(&mut incoming, &[prior], &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, original);
    let mut ended = observed_relationship(&incoming);
    ended.chain_id = Uuid::new_v4();
    ended.ended_at = Some(incoming.valid_from);
    resolve_identity(&mut incoming, &[ended], &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, original);
    assert_eq!(llm.call_count(), 0);
}

fn candidates(edge: &EntityEdge, count: usize) -> Vec<StoredRelationship> {
    (0..count)
        .map(|index| {
            let mut candidate = observed_relationship(edge);
            candidate.chain_id = Uuid::new_v4();
            candidate.uuid = Uuid::new_v4();
            candidate.description = format!("candidate-{index}");
            candidate
        })
        .collect()
}

#[tokio::test]
async fn more_than_sixty_four_facts_are_processed_in_bounded_batches() {
    let (ctx, llm) = context(vec![json!({"decision":"distinct","candidate":null})]);
    let mut incoming = edge();
    let original = incoming.chain_id;
    let pool = candidates(&incoming, 130);
    resolve_identity(&mut incoming, &pool, &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, original);
    assert_eq!(llm.call_count(), 3);
}

#[tokio::test]
async fn separate_batches_cannot_choose_two_lineages_or_hide_uncertainty() {
    for responses in [
        vec![
            json!({"decision":"same","candidate":0}),
            json!({"decision":"same","candidate":0}),
        ],
        vec![
            json!({"decision":"same","candidate":0}),
            json!({"decision":"unresolved","candidate":null}),
        ],
    ] {
        let uncertain = responses[1]["decision"] == "unresolved";
        let (ctx, llm) = context(responses);
        let mut incoming = edge();
        let original = incoming.chain_id;
        let pool = candidates(&incoming, 65);
        let result = resolve_identity(&mut incoming, &pool, &ctx, &Default::default(), None).await;
        assert_eq!(result.is_ok(), uncertain);
        assert_eq!(incoming.chain_id, original);
        assert_eq!(llm.call_count(), 2);
    }
}

#[test]
fn malformed_decisions_do_not_silently_mean_distinct() {
    assert_eq!(
        parse_decision(&json!({"decision":"same","candidate":0}), 1).unwrap(),
        Decision::Same(0)
    );
    for value in [
        json!({}),
        json!({"decision":"same","candidate":1}),
        json!({"decision":"same","candidate":-1}),
        json!({"decision":"same","candidate":null}),
        json!({"decision":"distinct","candidate":0}),
        json!({"decision":"unresolved","candidate":null,"extra":true}),
    ] {
        assert!(parse_decision(&value, 1).is_err());
    }
}

#[tokio::test]
async fn expired_aggregate_deadline_and_cancellation_prevent_adoption() {
    let (mut ctx, llm) = context(vec![]);
    ctx.identity_deadline = Some(tokio::time::Instant::now() - std::time::Duration::from_millis(1));
    let mut incoming = edge();
    let original = incoming.chain_id;
    let pool = candidates(&incoming, 65);
    let result = resolve_identity(&mut incoming, &pool, &ctx, &Default::default(), None).await;
    assert!(result.is_err());
    assert_eq!(incoming.chain_id, original);
    ctx.cancel.cancel();
    let result = resolve_identity(&mut incoming, &pool, &ctx, &Default::default(), None).await;
    assert!(matches!(result, Err(StageError::Cancelled { .. })));
    assert_eq!(incoming.chain_id, original);
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn configured_meanings_and_endpoint_rules_cannot_be_overridden_by_semantic_adoption() {
    for ontology in [
        serde_json::from_value(json!({"edge_types":[{"name":"USES"},{"name":"CALLS"}]})).unwrap(),
        serde_json::from_value(json!({"allowed_relationships":[{"edge_name":"USES","source_type":"Service","target_type":"Database"},{"edge_name":"CALLS","source_type":"Service","target_type":"Service"}]})).unwrap(),
    ] {
        let (ctx,llm)=context(vec![]);
        let mut incoming=edge();
        let original=incoming.chain_id;
        let mut prior=observed_relationship(&incoming);
        prior.chain_id=Uuid::new_v4();
        prior.name="CALLS".into();
        resolve_identity(&mut incoming, &[prior], &ctx, &ontology, None).await.unwrap();
        assert_eq!(incoming.chain_id,original);
        assert_eq!(incoming.name,"USES");
        assert_eq!(llm.call_count(),0);
    }
}

#[tokio::test]
async fn historical_fact_cannot_adopt_a_live_fact_with_the_same_words() {
    let (ctx, llm) = context(vec![]);
    let prior = edge();
    let mut incoming = prior.clone();
    incoming.chain_id = Uuid::new_v4();
    incoming.uuid = Uuid::new_v4();
    incoming.valid_from = prior.valid_from - chrono::Duration::days(3);
    incoming.valid_to = Some(prior.valid_from - chrono::Duration::days(2));
    let identity = incoming.chain_id;
    resolve_identity(
        &mut incoming,
        &[observed_relationship(&prior)],
        &ctx,
        &Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(incoming.chain_id, identity);
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn finite_current_fact_matches_without_erasing_its_future_boundary() {
    let (ctx, llm) = context(vec![]);
    let prior = edge();
    let mut current = observed_relationship(&prior);
    current.ended_at = Some(prior.valid_from + chrono::Duration::days(2));
    let mut incoming = prior.clone();
    incoming.valid_from += chrono::Duration::days(1);
    incoming.chain_id = Uuid::new_v4();
    incoming.uuid = Uuid::new_v4();
    resolve_identity(
        &mut incoming,
        std::slice::from_ref(&current),
        &ctx,
        &Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(incoming.chain_id, current.chain_id);
    assert_eq!(
        current.ended_at,
        Some(prior.valid_from + chrono::Duration::days(2))
    );
    assert_eq!(llm.call_count(), 0);

    incoming.chain_id = Uuid::new_v4();
    incoming.valid_from = current.ended_at.unwrap();
    let unmatched = incoming.chain_id;
    resolve_identity(&mut incoming, &[current], &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, unmatched);
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn finite_current_fact_with_conflicting_qualifier_is_not_a_match() {
    let (ctx, llm) = context(vec![]);
    let mut prior = edge();
    prior
        .all_properties
        .insert("environment".into(), PropertyValue::String("prod".into()));
    let mut current = observed_relationship(&prior);
    current.ended_at = Some(prior.valid_from + chrono::Duration::days(2));
    let mut incoming = prior.clone();
    incoming.valid_from += chrono::Duration::days(1);
    incoming.chain_id = Uuid::new_v4();
    incoming
        .all_properties
        .insert("environment".into(), PropertyValue::String("dev".into()));
    let unmatched = incoming.chain_id;
    resolve_identity(&mut incoming, &[current], &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, unmatched);
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn stale_exact_fact_keeps_identity_but_future_ambiguity_and_fresh_backdating_do_not() {
    let (ctx, llm) = context(vec![]);
    let prior = edge();
    let mut candidate = observed_relationship(&prior);
    candidate.latest_observation = Some(prior.valid_from + chrono::Duration::days(2));
    let mut incoming = prior.clone();
    incoming.valid_from -= chrono::Duration::days(1);
    incoming.last_seen_at = Some(incoming.valid_from);
    incoming.chain_id = Uuid::new_v4();
    let untouched = candidate.clone();
    resolve_identity(
        &mut incoming,
        std::slice::from_ref(&candidate),
        &ctx,
        &Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(incoming.chain_id, candidate.chain_id);
    assert_eq!(candidate, untouched);

    for (description, capture) in [
        ("api depends on target", incoming.valid_from),
        (
            prior.description.as_str(),
            prior.valid_from + chrono::Duration::days(3),
        ),
    ] {
        incoming.chain_id = Uuid::new_v4();
        let distinct = incoming.chain_id;
        incoming.description = description.into();
        incoming.last_seen_at = Some(capture);
        resolve_identity(
            &mut incoming,
            std::slice::from_ref(&candidate),
            &ctx,
            &Default::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(incoming.chain_id, distinct);
    }
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn historical_paraphrase_reuses_only_the_same_complete_interval() {
    for shift in [0, 1, 2] {
        let prior = edge();
        let end = prior.valid_from + chrono::Duration::days(3);
        let mut candidate = observed_relationship(&prior);
        candidate.ended_at = Some(end);
        let mut incoming = prior.clone();
        incoming.chain_id = Uuid::new_v4();
        incoming.description = "target was required by api".into();
        incoming.valid_to = Some(end);
        if shift == 1 {
            incoming.valid_from += chrono::Duration::hours(1);
        }
        if shift == 2 {
            incoming.valid_to = Some(end + chrono::Duration::hours(1));
        }
        let original_chain = incoming.chain_id;
        let bounds = (incoming.valid_from, incoming.valid_to);
        let (ctx, llm) = context(if shift == 0 {
            vec![json!({"decision":"same","candidate":0})]
        } else {
            vec![]
        });
        resolve_identity(&mut incoming, &[candidate], &ctx, &Default::default(), None)
            .await
            .unwrap();
        assert_eq!((incoming.valid_from, incoming.valid_to), bounds);
        assert_eq!(llm.call_count(), usize::from(shift == 0));
        assert_eq!(
            incoming.chain_id,
            if shift == 0 {
                prior.chain_id
            } else {
                original_chain
            }
        );
        if shift == 0 {
            assert_eq!(incoming.description, prior.description);
        }
    }
}

/// G3 reproduction: an ongoing unkeyed fact re-observed with an equivalent
/// descriptive wording of one property. Without reconciliation the stored
/// fact never reaches the model and the observation fragments into a new
/// chain.
#[tokio::test]
async fn equivalent_descriptive_wording_reaches_the_model_and_reuses_the_fact() {
    let (mut ctx, llm) = context(vec![json!({"decision":"same","candidate":0})]);
    let recorder =
        crate::tests::prompt_contract::RecordingModel::new(ctx.llm_disambiguation.clone());
    ctx.llm_disambiguation = recorder.clone();
    let mut prior = edge();
    prior.all_properties.insert(
        "owner".into(),
        PropertyValue::String("Commerce platform".into()),
    );
    let candidate = observed_relationship(&prior);
    let mut incoming = prior.clone();
    incoming.uuid = Uuid::new_v4();
    incoming.chain_id = Uuid::new_v4();
    incoming.valid_from += chrono::Duration::days(1);
    incoming.description = "api relies on target".into();
    incoming.all_properties.insert(
        "owner".into(),
        PropertyValue::String("commerce-platform".into()),
    );
    resolve_identity(&mut incoming, &[candidate], &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(
        llm.call_count(),
        1,
        "the candidate must be offered to the model"
    );
    assert_eq!(incoming.chain_id, prior.chain_id);
    assert_eq!(
        incoming.all_properties["owner"],
        PropertyValue::String("Commerce platform".into())
    );
    let requests = recorder.requests();
    assert_eq!(
        kg_core::test_support::source_data_json(&requests[0].0[1].content)["candidates"][0]
            ["differing_properties"],
        serde_json::json!(["owner"])
    );
}

/// G3 guard rails: only case/spacing/punctuation variants of a textual
/// property are put to the model; changed ports, releases, referents, typed
/// values and declared attributes keep the facts apart without a model call,
/// and the model may still keep an offered pair distinct.
#[tokio::test]
async fn descriptive_variants_keep_specific_facts_distinct() {
    type Case = (
        &'static str,
        PropertyValue,
        PropertyValue,
        Vec<Value>,
        bool,
        usize,
    );
    let cases: Vec<Case> = vec![
        (
            "port",
            PropertyValue::Integer(443),
            PropertyValue::Integer(8443),
            vec![],
            false,
            0,
        ),
        (
            "release",
            PropertyValue::String("r1".into()),
            PropertyValue::String("r2".into()),
            vec![],
            false,
            0,
        ),
        (
            "owner",
            PropertyValue::String("Commerce platform".into()),
            PropertyValue::String("Payments".into()),
            vec![],
            false,
            0,
        ),
        // Punctuation that carries meaning: the digit runs split differently.
        (
            "release",
            PropertyValue::String("1.23".into()),
            PropertyValue::String("12.3".into()),
            vec![json!({"decision":"same","candidate":0})],
            false,
            0,
        ),
        (
            "release",
            PropertyValue::String("v1.2".into()),
            PropertyValue::String("v12".into()),
            vec![json!({"decision":"same","candidate":0})],
            false,
            0,
        ),
        // Values without any letters or digits are never offered.
        (
            "marker",
            PropertyValue::String("!!!".into()),
            PropertyValue::String("???".into()),
            vec![json!({"decision":"same","candidate":0})],
            false,
            0,
        ),
        // A case-only identifier variant is offered, and the model keeps it apart.
        (
            "account",
            PropertyValue::String("ABC".into()),
            PropertyValue::String("abc".into()),
            vec![json!({"decision":"distinct","candidate":null})],
            false,
            1,
        ),
        (
            "owner",
            PropertyValue::String("Commerce platform".into()),
            PropertyValue::String("commerce-platform".into()),
            vec![json!({"decision":"distinct","candidate":null})],
            false,
            1,
        ),
        (
            "owner",
            PropertyValue::String("Commerce platform".into()),
            PropertyValue::String("COMMERCE_PLATFORM".into()),
            vec![json!({"decision":"same","candidate":0})],
            true,
            1,
        ),
    ];
    for (key, stored, incoming_value, responses, same, calls) in cases {
        let (ctx, llm) = context(responses);
        let mut prior = edge();
        prior.all_properties.insert(key.into(), stored.clone());
        let candidate = observed_relationship(&prior);
        let mut incoming = prior.clone();
        incoming.uuid = Uuid::new_v4();
        incoming.chain_id = Uuid::new_v4();
        incoming.valid_from += chrono::Duration::days(1);
        incoming
            .all_properties
            .insert(key.into(), incoming_value.clone());
        let original = incoming.chain_id;
        resolve_identity(&mut incoming, &[candidate], &ctx, &Default::default(), None)
            .await
            .unwrap();
        assert_eq!(incoming.chain_id == prior.chain_id, same, "{key}");
        assert_eq!(
            incoming.all_properties[key],
            if same { stored } else { incoming_value },
            "{key}"
        );
        if !same {
            assert_eq!(incoming.chain_id, original);
        }
        assert_eq!(llm.call_count(), calls, "{key}");
    }

    // A declared attribute that differs only in case still adds specificity.
    let (ctx, llm) = context(vec![json!({"decision":"same","candidate":0})]);
    let mut prior = edge();
    prior
        .all_properties
        .insert("tier".into(), PropertyValue::String("Gold".into()));
    let candidate = observed_relationship(&prior);
    let mut incoming = prior.clone();
    incoming.chain_id = Uuid::new_v4();
    incoming
        .all_properties
        .insert("tier".into(), PropertyValue::String("gold".into()));
    let ontology = serde_json::from_value(json!({"edge_types":[{
        "name":"USES","attributes":{"type":"object","properties":{"tier":{"type":"string"}}}
    }]}))
    .unwrap();
    let original = incoming.chain_id;
    resolve_identity(&mut incoming, &[candidate], &ctx, &ontology, None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, original);
    assert_eq!(llm.call_count(), 0);

    // More differing textual properties than one request weighs stay apart.
    let (ctx, llm) = context(vec![json!({"decision":"same","candidate":0})]);
    let mut prior = edge();
    let mut incoming = prior.clone();
    incoming.chain_id = Uuid::new_v4();
    for index in 0..=MAX_DIFFERING_PROPERTIES {
        prior.all_properties.insert(
            format!("p{index}"),
            PropertyValue::String(format!("Value {index}")),
        );
        incoming.all_properties.insert(
            format!("p{index}"),
            PropertyValue::String(format!("value-{index}")),
        );
    }
    let candidate = observed_relationship(&prior);
    let original = incoming.chain_id;
    resolve_identity(&mut incoming, &[candidate], &ctx, &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(incoming.chain_id, original);
    assert_eq!(llm.call_count(), 0);
}

/// An ended fact restated with a case variant of a property reuses only the
/// same complete interval, as an exact paraphrase would.
#[tokio::test]
async fn descriptive_variant_of_an_ended_fact_keeps_its_interval_rules() {
    for shifted in [false, true] {
        let (ctx, llm) = context(vec![json!({"decision":"same","candidate":0})]);
        let mut prior = edge();
        prior.all_properties.insert(
            "owner".into(),
            PropertyValue::String("Commerce platform".into()),
        );
        let end = prior.valid_from + chrono::Duration::days(3);
        let mut candidate = observed_relationship(&prior);
        candidate.ended_at = Some(end);
        let mut incoming = prior.clone();
        incoming.chain_id = Uuid::new_v4();
        incoming.valid_to = Some(if shifted {
            end + chrono::Duration::hours(1)
        } else {
            end
        });
        incoming.all_properties.insert(
            "owner".into(),
            PropertyValue::String("commerce-platform".into()),
        );
        let original = incoming.chain_id;
        resolve_identity(&mut incoming, &[candidate], &ctx, &Default::default(), None)
            .await
            .unwrap();
        assert_eq!(incoming.chain_id == prior.chain_id, !shifted);
        assert_eq!(incoming.chain_id == original, shifted);
        assert_eq!(llm.call_count(), usize::from(!shifted));
    }
}

/// Optional semantic naming support: a generic re-observation of a lineage that
/// was already given a stable semantic name inherits that name (no revert, no
/// duplicate), while a freshly chosen non-generic label still supersedes it.
/// Both keep the same chain because the identity hash is computed from the
/// generic `identity_name`, never from the display label.
#[tokio::test]
async fn generic_reobservation_inherits_a_stored_name_but_a_new_label_supersedes() {
    use kg_core::models::edges::GENERIC_RELATIONSHIP_NAME;
    // A keyed reference lineage stored with a semantic name but the generic
    // identity hash (exactly what naming produces: identity_name = generic).
    let mut generic = edge();
    generic.origin = RelationshipOrigin::Reference;
    generic.source_property = Some("dependency".into());
    generic.name = GENERIC_RELATIONSHIP_NAME.into();
    prepare_identity(&mut generic, &[], false, "ns").unwrap();
    let mut stored = observed_relationship(&generic);
    stored.chain_id = Uuid::new_v4();
    stored.name = "DEPENDS_ON".into();

    // Naming disabled / model unavailable this run: the edge is still generic.
    let (ctx, llm) = context(vec![]);
    let mut incoming = generic.clone();
    incoming.chain_id = Uuid::new_v4();
    prepare_identity(&mut incoming, &[], false, "ns").unwrap();
    resolve_identity(
        &mut incoming,
        &[stored.clone()],
        &ctx,
        &Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        incoming.chain_id, stored.chain_id,
        "same lineage, no duplicate"
    );
    assert_eq!(incoming.name, "DEPENDS_ON", "stored name is not reverted");

    // A schema that no longer accepts the stored label must not be violated by
    // inheriting it: the same lineage is re-observed under its discovery name.
    let restricted = kg_core::traits::Ontology {
        relationship_vocabulary: vec![GENERIC_RELATIONSHIP_NAME.into()],
        ..Default::default()
    };
    let mut narrowed = generic.clone();
    narrowed.chain_id = Uuid::new_v4();
    resolve_identity(&mut narrowed, &[stored.clone()], &ctx, &restricted, None)
        .await
        .unwrap();
    assert_eq!(narrowed.chain_id, stored.chain_id);
    assert_eq!(narrowed.name, GENERIC_RELATIONSHIP_NAME);

    // A profile that still declares the label but forbids it for this endpoint
    // pair: same lineage, discovery name kept, nothing asked of a model.
    let forbidden: kg_core::traits::Ontology = serde_json::from_value(json!({
        "entity_types": [{"name": "Service"}, {"name": "Database"}, {"name": "Queue"}],
        "edge_types": [{"name": "DEPENDS_ON"}, {"name": GENERIC_RELATIONSHIP_NAME}],
        "allowed_relationships": [
            {"source_type": "Service", "target_type": "Queue", "edge_name": "DEPENDS_ON"},
            {"source_type": "Service", "target_type": "Database",
             "edge_name": GENERIC_RELATIONSHIP_NAME}
        ]
    }))
    .unwrap();
    let mut elsewhere = generic.clone();
    elsewhere.chain_id = Uuid::new_v4();
    resolve_identity(
        &mut elsewhere,
        &[stored.clone()],
        &ctx,
        &forbidden,
        Some(("Service", "Database")),
    )
    .await
    .unwrap();
    assert_eq!(elsewhere.chain_id, stored.chain_id);
    assert_eq!(elsewhere.name, GENERIC_RELATIONSHIP_NAME);
    let mut permitted = generic.clone();
    permitted.chain_id = Uuid::new_v4();
    resolve_identity(
        &mut permitted,
        &[stored.clone()],
        &ctx,
        &forbidden,
        Some(("Service", "Queue")),
    )
    .await
    .unwrap();
    assert_eq!(permitted.name, "DEPENDS_ON");

    // A fresh, different label (identity_name carries the generic name so the
    // hash still matches) supersedes on the same chain rather than forking one.
    let mut renamed = generic.clone();
    renamed.chain_id = Uuid::new_v4();
    renamed.name = "USES".into();
    renamed.identity_name = Some(GENERIC_RELATIONSHIP_NAME.into());
    prepare_identity(&mut renamed, &[], false, "ns").unwrap();
    assert_eq!(
        renamed.identity_hash, incoming.identity_hash,
        "renaming never moves the trusted identity hash"
    );
    resolve_identity(
        &mut renamed,
        &[stored.clone()],
        &ctx,
        &Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        renamed.chain_id, stored.chain_id,
        "same lineage, no duplicate"
    );
    assert_eq!(renamed.name, "USES", "a new decision supersedes");
    assert_eq!(llm.call_count(), 0, "keyed carry-forward needs no model");
}

#[tokio::test]
async fn semantic_comparison_receives_capture_fallback_provenance() {
    use kg_core::models::{RelationshipTimeEvidence, RelationshipTimeOutcome};
    let (mut ctx, _) = context(vec![json!({"decision":"same","candidate":0})]);
    let recorder =
        crate::tests::prompt_contract::RecordingModel::new(ctx.llm_disambiguation.clone());
    ctx.llm_disambiguation = recorder.clone();
    let mut prior = edge();
    prior.time_evidence = Some(RelationshipTimeEvidence {
        resolved_target: None,
        snapshot_id: Uuid::new_v4(),
        captured_at: prior.valid_from,
        outcome: RelationshipTimeOutcome::Unknown,
        start: None,
        end: None,
    });
    let mut incoming = prior.clone();
    incoming.chain_id = Uuid::new_v4();
    incoming.valid_from += chrono::Duration::days(1);
    incoming.description = "api (also called application) uses target".into();
    incoming.time_evidence.as_mut().unwrap().captured_at = incoming.valid_from;
    incoming.time_evidence.as_mut().unwrap().snapshot_id = Uuid::new_v4();
    resolve_identity(
        &mut incoming,
        &[observed_relationship(&prior)],
        &ctx,
        &Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(incoming.chain_id, prior.chain_id);
    let requests = recorder.requests();
    let system = &requests[0].0[0].content;
    let data = kg_core::test_support::source_data_json(&requests[0].0[1].content).to_string();
    assert!(system.contains("capture fallback"));
    assert!(system.contains("source-supported dates"));
    assert_eq!(data.matches("\"time_evidence\":").count(), 2);
    assert_eq!(data.matches("\"outcome\":\"unknown\"").count(), 2);
    assert!(data.contains(&serde_json::to_string(&incoming.valid_from).unwrap()));
}
