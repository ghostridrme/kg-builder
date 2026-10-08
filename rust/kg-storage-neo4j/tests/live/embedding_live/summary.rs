//! Derived text/vector publication fences and supporting-write invalidation on real transactions.
use kg_core::{
    embedding::{ComputedEmbedding, EmbeddingSettings},
    embedding_rebuild::{EmbeddingKind, EmbeddingRecord, EmbeddingRefresh},
    entity_summary::*,
    traits::{
        graph_backend::GraphEmbedding,
        relationship_timeline::{self, IncidentVersionState},
        EdgeLookup, EntityLookup, GraphBackend, GraphMutation as M,
    },
};
use kg_neo4j_testkit::indexed_graph as graph;
use serde_json::json;
use std::collections::BTreeMap;
use uuid::Uuid;
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn guarded_summary_preserves_base_vectors_and_never_resurrects_after_support_changes() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let edge = Uuid::new_v4();
    let node = |id, name| {
        M::UpsertEntity {uuid:id,properties:json!({"chain_id":id,"namespace":"prod","entity_type":"Service","name":name,"version":1,"is_latest":true,"valid_from":"2026-01-01T00:00:00Z","embedding":[1.0,0.0],"embedding_model":"base","embedding_text_version":kg_core::embedding::TEXT_VERSION,"embedding_content_hash":"base-hash","summary":"source summary"}).as_object().unwrap().clone()}
    };
    g.apply_mutations(&org,&[node(s,"api"),node(t,"db"),M::UpsertEdge {uuid:edge,source_chain_id:s,target_chain_id:t,properties:json!({"chain_id":edge,"name":"DEPENDS_ON","description":"API depends on DB","version":1,"is_latest":true,"valid_from":"2026-01-01T00:00:00Z","producer_namespace":"prod","producer_source":"test"}).as_object().unwrap().clone()}]).await.unwrap();
    let mut timelines = BTreeMap::new();
    for id in [s, t] {
        let records = g
            .find_entities(
                &org,
                &EntityLookup::VersionsByChain {
                    chain_ids: vec![id],
                },
            )
            .await
            .unwrap();
        timelines.insert(
            id,
            records
                .iter()
                .map(|record| entity_state(&record.stored))
                .collect(),
        );
    }
    let edges = g
        .find_edges(
            &org,
            &EdgeLookup::VersionsByEndpointChains { chain_ids: vec![s] },
        )
        .await
        .unwrap();
    let guard = SummaryEvidenceGuard {
        target_uuid: s,
        target_chain_id: s,
        expected_revision: None,
        entity_versions: timelines,
        incident_versions: edges
            .iter()
            .map(|r| IncidentVersionState {
                source_chain_id: r.source_chain_id,
                target_chain_id: r.target_chain_id,
                properties: relationship_timeline::state(&r.stored),
            })
            .collect(),
    };
    let as_of = "2026-02-01T00:00:00Z".parse().unwrap();
    let evidence = SummaryEvidence {
        guard: guard.clone(),
        namespace: "prod".into(),
        as_of,
        valid_until: None,
        facts: vec![SummaryFact {
            id: edge,
            source_uuid: s,
            target_uuid: t,
            line: "API depends on DB".into(),
            valid_from: "2026-01-01T00:00:00Z".parse().unwrap(),
            valid_until: None,
            snapshot_ids: vec![],
        }],
    };
    let summary = DerivedSummary {
        revision: Uuid::new_v4(),
        text: "API depends on DB".into(),
        as_of,
        valid_until: None,
        evidence_hash: evidence_hash(&evidence).unwrap(),
        policy_version: POLICY_VERSION.into(),
        evidence_ids: vec![edge],
        total_evidence: 1,
    };
    let write = M::SetDerivedSummary {
        guard: guard.clone(),
        summary: Box::new(summary.clone()),
        embedding: GraphEmbedding {
            model: "derived".into(),
            values: vec![0.0, 1.0],
        },
    };
    g.apply_mutations(&org, std::slice::from_ref(&write))
        .await
        .unwrap();
    assert!(
        matches!(
            g.apply_mutations(&org, std::slice::from_ref(&write)).await,
            Err(kg_core::errors::BackendError::Conflict(_))
        ),
        "an old refresh cannot overwrite the accepted revision"
    );
    let mut stale_empty_guard = guard.clone();
    stale_empty_guard.incident_versions.clear();
    assert!(
        matches!(
            g.apply_mutations(
                &org,
                &[M::ClearDerivedSummary {
                    guard: stale_empty_guard
                }]
            )
            .await,
            Err(kg_core::errors::BackendError::Conflict(_))
        ),
        "a stale empty-evidence clear cannot erase a concurrent accepted summary"
    );
    let stored = g
        .find_entities(&org, &EntityLookup::VersionsByChain { chain_ids: vec![s] })
        .await
        .unwrap()
        .remove(0)
        .stored;
    assert_eq!(stored["summary"], json!("source summary"));
    assert_eq!(stored["embedding_model"], json!("base"));
    assert_eq!(stored["derived_summary"], json!(summary.text));
    assert_eq!(stored["summary_revision"], json!(summary.revision));
    let settings = EmbeddingSettings {
        entity_fields: Default::default(),
        model: "replacement".into(),
        dimension: 2,
        text_version: SUMMARY_TEXT_VERSION.into(),
    };
    let refresh = EmbeddingRefresh {
        entity_fields: Default::default(),
        record: EmbeddingRecord {
            uuid: s,
            properties: stored,
        },
        embedding: ComputedEmbedding::new(
            &settings,
            kg_core::embedding::content_hash(&embedding_text(&summary.text)),
            vec![1.0, 1.0],
        ),
    };
    g.apply_mutations(
        &org,
        &[M::UpdateEdge {
            uuid: edge,
            properties: json!({"description":"API no longer depends on DB"})
                .as_object()
                .unwrap()
                .clone(),
        }],
    )
    .await
    .unwrap();
    let now = g
        .find_entities(&org, &EntityLookup::VersionsByChain { chain_ids: vec![s] })
        .await
        .unwrap()
        .remove(0)
        .stored;
    assert!(!now.contains_key("derived_summary"));
    assert!(!now.contains_key("summary_embedding"));
    assert_eq!(now["embedding_model"], json!("base"));
    assert_eq!(
        g.refresh_embeddings(&org, EmbeddingKind::DerivedSummary, &[refresh])
            .await
            .unwrap(),
        0
    );
    assert!(
        matches!(
            g.apply_mutations(&org, std::slice::from_ref(&write)).await,
            Err(kg_core::errors::BackendError::Conflict(_))
        ),
        "changed incident evidence rejects stale summary publication"
    );
    // The receipted pipeline path must expose the same re-plannable conflict.
    use kg_core::traits::{BatchIdentity, BatchKind, MutationBatch, RequestFingerprint, RunHeader};
    let run_id = Uuid::new_v4();
    let fingerprint = RequestFingerprint::compute(&org, &[], &json!({})).unwrap();
    g.register_run(&RunHeader {
        observation_manifest: Default::default(),
        rule_freezes: vec![],
        schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
            profiles: Default::default(),
            org_id: org.clone(),
            sources: Default::default(),
        },
        org_id: org.clone(),
        run_id,
        fingerprint: fingerprint.clone(),
        settings_version: "summary-test".into(),
        capture_default: as_of,
        batch_plan: vec![],
    })
    .await
    .unwrap();
    let stale_batch = MutationBatch {
        org_id: org.clone(),
        batch: BatchIdentity {
            run_id,
            kind: BatchKind::Summary,
            index: 1,
        },
        fingerprint,
        preconditions: vec![],
        mutations: vec![write],
        result: json!({}),
    };
    assert!(matches!(
        g.commit_batch(&stale_batch).await,
        Err(kg_core::errors::BackendError::Conflict(_))
    ));
    assert!(g.committed_batches(&org, run_id).await.unwrap().is_empty());
    g.close().await.unwrap();
}
