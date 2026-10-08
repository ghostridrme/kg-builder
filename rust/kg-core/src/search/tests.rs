use super::*;
use uuid::Uuid;

#[test]
fn supported_recipes_validate() {
    for config in [
        SearchConfig::hybrid_rrf(),
        SearchConfig::relationship_hybrid(),
        SearchConfig::keyword_only(),
        SearchConfig::semantic_only(),
        SearchConfig::hybrid_mmr(),
        SearchConfig::hybrid_model(),
        SearchConfig::graph_traversal(Uuid::new_v4(), 5),
        SearchConfig::graph_anchored(Uuid::new_v4()),
    ] {
        config.validate().unwrap();
    }
}

#[test]
fn required_evidence_floor_cannot_be_silently_unused() {
    let mut config = SearchConfig {
        include_evidence: false,
        evidence_reranker: EvidenceReranker::Model,
        evidence_model_min_score: Some(0.5),
        ..Default::default()
    };
    assert!(config.validate().is_err());
    config.include_evidence = true;
    config.validate().unwrap();
    config.include_evidence = false;
    config.scopes = vec![SearchScope::Snapshots];
    config.validate().unwrap();
}

#[test]
fn readiness_rejects_unrepresentable_dimensions_and_snapshot_scope() {
    let mut request = EmbeddingReadinessRequest {
        entity_text_version: crate::embedding::TEXT_VERSION.into(),
        filter: SearchFilter {
            org_id: "org".into(),
            ..Default::default()
        },
        scope: SearchScope::Nodes,
        model: "test".into(),
        dimensions: 1536,
    };
    request.validate().unwrap();
    for dimensions in std::iter::once(0).chain(usize::try_from(i64::MAX as u64 + 1).ok()) {
        request.dimensions = dimensions;
        assert!(request.validate().is_err());
    }
    request.dimensions = 1536;
    request.scope = SearchScope::Snapshots;
    assert!(request.validate().is_err());
}

#[test]
fn page_limits_and_approximation_are_independent() {
    let exact = SearchPage::bounded(vec![1, 2], 2);
    assert!(!exact.truncated && !exact.approximate);
    let indexed = exact.approximate();
    assert!(!indexed.truncated && indexed.approximate);
    let truncated = SearchPage::bounded(vec![1, 2, 3], 2);
    assert_eq!(truncated.items, [1, 2]);
    assert!(truncated.truncated && !truncated.approximate);
}

#[test]
fn filter_typos_cannot_silently_change_temporal_scope() {
    let mut value = serde_json::to_value(SearchFilter {
        org_id: "org".into(),
        ..Default::default()
    })
    .unwrap();
    value["as_off"] = serde_json::json!("2026-01-01T00:00:00Z");
    assert!(serde_json::from_value::<SearchFilter>(value).is_err());
}

#[test]
fn saga_filter_requires_the_snapshot_scope_alone_and_one_namespace() {
    let valid = SearchConfig {
        scopes: vec![SearchScope::Snapshots],
        namespaces: vec!["prod".into()],
        saga_uuid: Some(uuid::Uuid::from_u128(1)),
        include_evidence: false,
        ..SearchConfig::keyword_only()
    };
    valid.validate().unwrap();
    let broken: [&dyn Fn(&mut SearchConfig); 6] = [
        &|c| c.scopes.push(SearchScope::Nodes),
        &|c| c.scopes = vec![SearchScope::Relationships],
        &|c| c.scopes = vec![SearchScope::Nodes],
        &|c| c.namespaces.clear(),
        &|c| c.namespaces.push("dev".into()),
        &|c| c.saga_uuid = Some(uuid::Uuid::nil()),
    ];
    for (index, change) in broken.iter().enumerate() {
        let mut config = valid.clone();
        change(&mut config);
        assert!(config.validate().is_err(), "variant {index} accepted");
    }
}
