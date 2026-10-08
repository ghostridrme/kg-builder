//! Live model migration, retained-history coverage, and concurrent-write fences.
use async_trait::async_trait;
use kg_core::{
    embedding::{content_hash, ComputedEmbedding, EmbeddingSettings, TEXT_VERSION},
    embedding_rebuild::{rebuild_embeddings, EmbeddingKind, EmbeddingRefresh, RebuildOptions},
    errors::BackendError,
    traits::{EmbedBackend, GraphBackend, GraphMutation as M},
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

struct Provider {
    calls: AtomicUsize,
}
#[async_trait]
impl EmbedBackend for Provider {
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
        assert!(texts.len() <= 2);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(texts.iter().map(|_| vec![1., 0., 0.]).collect())
    }
    fn dimension(&self) -> usize {
        3
    }
    fn max_batch_size(&self) -> usize {
        2
    }
    fn model_id(&self) -> &str {
        "rebuilt"
    }
}
use kg_neo4j_testkit::indexed_graph as graph;
fn entity(id: Uuid, live: bool) -> M {
    M::UpsertEntity{uuid:id,properties:json!({"chain_id":id,"name":"service","namespace":"prod","entity_type":"Service","version":1,"is_latest":live,"valid_from":"2026-01-01T00:00:00Z","embedding":[1.,0.],"embedding_model":"previous","embedding_text_version":"old","embedding_content_hash":"old"}).as_object().unwrap().clone()}
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn rebuild_refreshes_both_kinds_and_history_then_reuses_without_provider_calls() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let ids: Vec<_> = (0..10).map(|_| Uuid::new_v4()).collect();
    g.apply_mutations(
        &org,
        &ids.iter()
            .enumerate()
            .map(|(i, id)| entity(*id, i < 8))
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    for live in [true, false] {
        g.apply_mutations(&org,&[M::UpsertEdge{uuid:Uuid::new_v4(),source_chain_id:ids[0],target_chain_id:ids[1],properties:json!({"name":"DEPENDS_ON","description":"service depends on another service","is_latest":live,"valid_from":"2026-01-01T00:00:00Z","embedding":[1.,0.],"embedding_model":"previous","embedding_text_version":"old"}).as_object().unwrap().clone()}]).await.unwrap();
    }
    let foreign = Uuid::new_v4().to_string();
    g.apply_mutations(&foreign, &[entity(Uuid::new_v4(), true)])
        .await
        .unwrap();
    // A dimensionless Neo4j vector index accepts two dimensions concurrently.
    g.set_entity_embedding(
        &org,
        ids[0],
        &kg_core::traits::graph_backend::GraphEmbedding {
            model: "rebuilt".into(),
            values: vec![0., 0., 1.],
        },
        TEXT_VERSION,
        &content_hash("type: Service\nname: service"),
        &kg_core::embedding::EntityEmbeddingFields::default(),
    )
    .await
    .unwrap();
    let mixed=g.execute_read("CALL db.index.vector.queryNodes('search_entity_vectors',1000,[0.0,0.0,1.0]) YIELD node,score RETURN node.uuid AS uuid",&json!({})).await.unwrap();
    assert!(mixed.iter().any(|r| r["uuid"] == json!(ids[0])));
    let provider = Provider {
        calls: AtomicUsize::new(0),
    };
    let report = rebuild_embeddings(&g, &provider, &org, &RebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(report.updated, 11);
    assert_eq!(report.conflicts, 0);
    assert!(provider.calls.load(Ordering::SeqCst) >= 6);
    let settings = EmbeddingSettings::of(&provider).unwrap();
    assert_eq!(g.incompatible_embeddings(&org, &settings).await.unwrap(), 0);
    assert_eq!(
        g.incompatible_embeddings(&foreign, &settings)
            .await
            .unwrap(),
        1
    );
    let before = provider.calls.load(Ordering::SeqCst);
    let second = rebuild_embeddings(&g, &provider, &org, &RebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(second.updated, 0);
    assert_eq!(second.reused, 12);
    assert_eq!(before, provider.calls.load(Ordering::SeqCst));
    // Superseded entities and invalidated facts kept compatible vectors.
    for kind in [EmbeddingKind::Entity, EmbeddingKind::Relationship] {
        let records = g.embedding_records(&org, kind, None, 100).await.unwrap();
        assert!(records.iter().any(|r| r.properties["is_latest"] == false));
        assert!(records
            .iter()
            .all(|r| r.properties["embedding_model"] == "rebuilt"));
    }
    g.close().await.unwrap();
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn refresh_refuses_stale_content_and_fuzzy_search_excludes_old_text_versions() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let id = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(id, true)]).await.unwrap();
    assert!(g
        .search_entity_embeddings(
            &[1., 0.],
            "previous",
            &org,
            None,
            None,
            10,
            -1.,
            kg_core::embedding::TEXT_VERSION
        )
        .await
        .unwrap()
        .is_empty());
    let record = g
        .embedding_records(&org, EmbeddingKind::Entity, None, 10)
        .await
        .unwrap()
        .remove(0);
    let text = record
        .text(
            EmbeddingKind::Entity,
            &kg_core::embedding::EntityEmbeddingFields::default(),
        )
        .unwrap();
    let update = EmbeddingRefresh {
        entity_fields: Default::default(),
        record,
        embedding: ComputedEmbedding {
            model: "rebuilt".into(),
            values: vec![1., 0., 0.],
            text_version: TEXT_VERSION.into(),
            content_hash: content_hash(&text),
        },
    };
    g.apply_mutations(
        &org,
        &[M::UpdateEntity {
            uuid: id,
            properties: json!({"name":"changed"}).as_object().unwrap().clone(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(
        g.refresh_embeddings(&org, EmbeddingKind::Entity, &[update])
            .await
            .unwrap(),
        0
    );
    let current = g
        .embedding_records(&org, EmbeddingKind::Entity, None, 10)
        .await
        .unwrap()
        .remove(0);
    assert!(!current.properties.contains_key("embedding_model"));
    assert!(matches!(
        g.set_entity_embedding(
            &org,
            id,
            &kg_core::traits::graph_backend::GraphEmbedding {
                model: "rebuilt".into(),
                values: vec![1., 0., 0.]
            },
            TEXT_VERSION,
            &content_hash(&text),
            &kg_core::embedding::EntityEmbeddingFields::default()
        )
        .await,
        Err(kg_core::errors::BackendError::Conflict(_))
    ));
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn repeated_upserts_clear_vectors_when_source_text_changes() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    g.apply_mutations(&org, &[entity(a, true), entity(b, true), entity(c, true)])
        .await
        .unwrap();
    let changed = |uuid| {
        M::UpsertEntity {
        uuid,
        properties: json!({"chain_id":uuid,"namespace":"prod","entity_type":"Service","name":"changed","is_latest":true})
            .as_object()
            .unwrap()
            .clone(),
    }
    };
    g.apply_mutations(&org, &[changed(a)]).await.unwrap();
    g.apply_mutations(&org, &[changed(b), changed(c)])
        .await
        .unwrap();
    let records = g
        .embedding_records(&org, EmbeddingKind::Entity, None, 10)
        .await
        .unwrap();
    assert_eq!(records.len(), 3);
    assert!(records
        .iter()
        .all(|r| !r.properties.contains_key("embedding")));
    let edge = Uuid::new_v4();
    let rel = |properties: serde_json::Value| M::UpsertEdge {
        uuid: edge,
        source_chain_id: a,
        target_chain_id: b,
        properties: properties.as_object().unwrap().clone(),
    };
    g.apply_mutations(&org,&[rel(json!({"name":"USES","description":"before","is_latest":true,"embedding":[1.,0.],"embedding_model":"previous"}))]).await.unwrap();
    g.apply_mutations(&org, &[rel(json!({"description":"after"}))])
        .await
        .unwrap();
    assert!(!g
        .embedding_records(&org, EmbeddingKind::Relationship, None, 10)
        .await
        .unwrap()[0]
        .properties
        .contains_key("embedding"));
    g.close().await.unwrap();
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn field_policy_rebuilds_current_and_historical_versions_and_search_uses_it() {
    use kg_core::{
        embedding::EntityEmbeddingFields,
        search::*,
        traits::{graph_backend::GraphEmbedding, SearchBackend},
    };
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    let mut mutations = Vec::new();
    for (index, id) in ids.into_iter().enumerate() {
        let M::UpsertEntity { mut properties, .. } = entity(id, index == 0) else {
            unreachable!()
        };
        kg_core::traits::property_codec::write_property(
            &mut properties,
            "resource_namespace",
            Some(&kg_core::models::PropertyValue::String("payments".into())),
        );
        kg_core::traits::property_codec::write_property(
            &mut properties,
            "replicas",
            Some(&kg_core::models::PropertyValue::Integer(3)),
        );
        mutations.push(M::UpsertEntity {
            uuid: id,
            properties,
        });
    }
    g.apply_mutations(&org, &mutations).await.unwrap();
    let provider = Provider {
        calls: AtomicUsize::new(0),
    };
    assert_eq!(
        rebuild_embeddings(&g, &provider, &org, &RebuildOptions::default())
            .await
            .unwrap()
            .updated,
        2
    );
    let mut fields = EntityEmbeddingFields::default();
    fields.by_entity_type.insert(
        "Service".into(),
        vec!["resource_namespace".into(), "replicas".into()],
    );
    let settings = EmbeddingSettings::of(&provider)
        .unwrap()
        .with_entity_fields(fields.clone())
        .unwrap();
    assert_eq!(g.incompatible_embeddings(&org, &settings).await.unwrap(), 2);
    let options = RebuildOptions {
        entity_fields: fields.clone(),
        ..Default::default()
    };
    assert_eq!(
        rebuild_embeddings(&g, &provider, &org, &options)
            .await
            .unwrap()
            .updated,
        2
    );
    let records = g
        .embedding_records(&org, EmbeddingKind::Entity, None, 10)
        .await
        .unwrap();
    for record in &records {
        assert_eq!(
            record.properties["embedding_text_version"],
            json!(settings.text_version)
        );
        let text = record.text(EmbeddingKind::Entity, &fields).unwrap();
        assert!(text.contains("resource_namespace: payments\nreplicas: 3"));
        assert_eq!(
            record.properties["embedding_content_hash"],
            json!(content_hash(&text))
        );
    }
    let filter = SearchFilter {
        org_id: org.clone(),
        ..Default::default()
    };
    let readiness = EmbeddingReadinessRequest {
        entity_text_version: fields.text_version(),
        filter: filter.clone(),
        scope: SearchScope::Nodes,
        model: provider.model_id().into(),
        dimensions: provider.dimension(),
    };
    assert_eq!(
        g.embedding_readiness(&readiness).await.unwrap().compatible,
        1
    );
    let mut query = NodeSearch {
        embedding_text_version: fields.text_version(),
        filter,
        query: NodeQuery::Similarity(GraphEmbedding {
            model: provider.model_id().into(),
            values: vec![1., 0., 0.],
        }),
        chain_ids: None,
        limit: 10,
        min_score: 0.9,
        signals: NodeSignals::default(),
        projection: NodeProjection::Full,
    };
    let hits = g.search_nodes(&query).await.unwrap();
    assert_eq!(hits.items.len(), 1);
    assert!(hits.items[0].embedding.is_some());
    query.embedding_text_version = TEXT_VERSION.into();
    assert!(g.search_nodes(&query).await.unwrap().items.is_empty());
    let calls = provider.calls.load(Ordering::SeqCst);
    assert_eq!(
        rebuild_embeddings(&g, &provider, &org, &options)
            .await
            .unwrap()
            .updated,
        0
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
}

/// Derived summaries are the fourth embedding kind: a summary published with a stale
/// vector is rebuilt together with the entity's own vector and reused on the next pass.
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn rebuild_refreshes_derived_summaries_then_reuses_them() {
    use kg_core::{
        entity_summary::{
            entity_state, evidence_hash, DerivedSummary, SummaryEvidence, SummaryEvidenceGuard,
            SummaryFact, POLICY_VERSION, SUMMARY_TEXT_VERSION,
        },
        traits::{
            graph_backend::GraphEmbedding,
            relationship_timeline::{self, IncidentVersionState},
            EdgeLookup, EntityLookup,
        },
    };
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let (s, t, edge) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    g.apply_mutations(
        &org,
        &[
            entity(s, true),
            entity(t, true),
            M::UpsertEdge {
                uuid: edge,
                source_chain_id: s,
                target_chain_id: t,
                properties: json!({"chain_id":edge,"name":"DEPENDS_ON","description":"service depends on another service","version":1,"is_latest":true,"valid_from":"2026-01-01T00:00:00Z","producer_namespace":"prod","producer_source":"test","embedding":[1.,0.],"embedding_model":"previous","embedding_text_version":"old"}).as_object().unwrap().clone(),
            },
        ],
    )
    .await
    .unwrap();
    let mut entity_versions = std::collections::BTreeMap::new();
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
        entity_versions.insert(
            id,
            records
                .iter()
                .map(|record| entity_state(&record.stored))
                .collect(),
        );
    }
    let incidents = g
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
        entity_versions,
        incident_versions: incidents
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
            line: "service depends on another service".into(),
            valid_from: "2026-01-01T00:00:00Z".parse().unwrap(),
            valid_until: None,
            snapshot_ids: vec![],
        }],
    };
    let summary = DerivedSummary {
        revision: Uuid::new_v4(),
        text: "service depends on another service".into(),
        as_of,
        valid_until: None,
        evidence_hash: evidence_hash(&evidence).unwrap(),
        policy_version: POLICY_VERSION.into(),
        evidence_ids: vec![edge],
        total_evidence: 1,
    };
    g.apply_mutations(
        &org,
        &[M::SetDerivedSummary {
            guard,
            summary: Box::new(summary),
            embedding: GraphEmbedding {
                model: "previous".into(),
                values: vec![0., 1.],
            },
        }],
    )
    .await
    .unwrap();

    let provider = Provider {
        calls: AtomicUsize::new(0),
    };
    let report = rebuild_embeddings(&g, &provider, &org, &RebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(
        report.updated, 4,
        "two entity vectors, one relationship vector, one derived summary vector"
    );
    assert_eq!(report.conflicts, 0);
    let summaries = g
        .embedding_records(&org, EmbeddingKind::DerivedSummary, None, 10)
        .await
        .unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries[0].properties["summary_embedding_model"],
        "rebuilt"
    );
    assert_eq!(
        summaries[0].properties["summary_embedding_text_version"],
        SUMMARY_TEXT_VERSION
    );
    let calls = provider.calls.load(Ordering::SeqCst);
    let second = rebuild_embeddings(&g, &provider, &org, &RebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(second.updated, 0);
    assert_eq!(second.reused, 4);
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    g.close().await.unwrap();
}
