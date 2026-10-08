use super::*;
use kg_core::traits::GraphMutation;
use kg_core::{
    errors::BackendError,
    models::PropertyValue,
    runtime::{
        stage_output::{PlannedBatchOutput, PlannedEmbedding},
        RuntimeContextBuilder,
    },
    test_support::MockLlmBackend,
    traits::{
        graph_commit::MAX_STATEMENTS_PER_BATCH, graph_mutation::entity_embedding_state,
        BatchIdentity, BatchKind, EmbedBackend, MutationBatch, RequestFingerprint,
    },
};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use uuid::Uuid;

struct Provider {
    calls: AtomicUsize,
    fail_second: bool,
}
#[async_trait]
impl EmbedBackend for Provider {
    fn model_id(&self) -> &str {
        "test-embedding"
    }
    fn dimension(&self) -> usize {
        2
    }
    fn max_batch_size(&self) -> usize {
        2
    }
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
        assert!(texts.len() <= 2);
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_second && call == 1 {
            return Ok(vec![vec![f32::NAN, 1.0]; texts.len()]);
        }
        Ok(texts
            .iter()
            .map(|text| vec![1.0, text.len() as f32])
            .collect())
    }
}
fn context(provider: Arc<Provider>) -> RuntimeContext {
    RuntimeContextBuilder::new("org")
        .graph(Arc::new(kg_core::test_support::UnreachableGraph))
        .embedder(provider)
        .llm_default(Arc::new(MockLlmBackend::failing()))
        .llm_extraction(Arc::new(MockLlmBackend::failing()))
        .build()
        .unwrap()
}
fn plan(ctx: &RuntimeContext, count: usize) -> PlannedBatchOutput {
    let embeddings = (0..count)
        .map(|i| {
            let source: indexmap::IndexMap<String, PropertyValue> = indexmap::IndexMap::from([(
                "description".to_string(),
                PropertyValue::String(format!("service purpose {i}")),
            )]);
            let text = embedding::representation(
                &embedding::EntityText {
                    entity_type: "Service",
                    name: "api",
                    summary: None,
                    properties: &source,
                    key_properties: &[],
                    labels: &[],
                },
                &ctx.embedding.entity_fields,
            );
            let mut properties =
                json!({"name":"api","entity_type":"Service","namespace":"prod","version":1})
                    .as_object()
                    .unwrap()
                    .clone();
            for (key, value) in &source {
                kg_core::traits::property_codec::write_property(&mut properties, key, Some(value));
            }
            PlannedEmbedding {
                uuid: Uuid::new_v4(),
                namespace: "prod".into(),
                content_hash: embedding::content_hash(&text),
                text,
                text_version: ctx.embedding.text_version.clone(),
                reuse: None,
                write: kg_core::runtime::stage_output::PlannedEmbeddingWrite::EntityVersion {
                    expected_properties: entity_embedding_state(&properties),
                    labels: Vec::new(),
                    primary_key_properties: Vec::new(),
                    additional_key_properties: Vec::new(),
                },
            }
        })
        .collect();
    PlannedBatchOutput {
        batch: MutationBatch {
            org_id: "org".into(),
            batch: BatchIdentity {
                run_id: Uuid::new_v4(),
                kind: BatchKind::Node,
                index: 0,
            },
            fingerprint: RequestFingerprint("0".repeat(32)),
            preconditions: vec![],
            mutations: vec![],
            result: json!({}),
        },
        embeddings,
    }
}
#[tokio::test]
async fn capacity_and_retry_reuse_cover_all_versions_without_a_graph_call() {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        fail_second: false,
    });
    let ctx = context(provider.clone());
    let planned = plan(&ctx, 4);
    for _ in 0..2 {
        let output = BatchEmbeddingStage
            .process(StageOutput::PlannedBatch(planned.clone()), &ctx)
            .await
            .unwrap();
        let StageOutput::PreparedBatch(output) = output else {
            panic!()
        };
        assert_eq!(output.batch.mutations.len(), 4);
        assert!(output
            .batch
            .mutations
            .iter()
            .all(|m| matches!(m, GraphMutation::SetEntityVersionEmbedding { .. })));
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn an_invalid_later_response_caches_none_of_the_new_vectors() {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        fail_second: true,
    });
    let ctx = context(provider.clone());
    let planned = plan(&ctx, 4);
    assert!(BatchEmbeddingStage
        .process(StageOutput::PlannedBatch(planned.clone()), &ctx)
        .await
        .is_err());
    for target in planned.embeddings {
        assert!(ctx
            .incoming_embeddings
            .get(&EmbeddingCacheKey::new(
                "org",
                "prod",
                &ctx.embedding,
                &target.text
            ))
            .is_none());
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn budgets_and_invalid_metadata_fail_before_provider_work() {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        fail_second: false,
    });
    let mut ctx = context(provider.clone());
    let mut oversized = plan(&ctx, 1);
    oversized.batch.mutations = (0..MAX_STATEMENTS_PER_BATCH)
        .map(|_| GraphMutation::UpdateEntity {
            uuid: Uuid::new_v4(),
            properties: json!({"summary":"context"}).as_object().unwrap().clone(),
        })
        .collect();
    assert!(BatchEmbeddingStage
        .process(StageOutput::PlannedBatch(oversized), &ctx)
        .await
        .is_err());
    let oversized = plan(&ctx, 1);
    ctx.embedding.dimension = 3_000_000;
    assert!(BatchEmbeddingStage
        .process(StageOutput::PlannedBatch(oversized), &ctx)
        .await
        .is_err());
    ctx.embedding.dimension = 2;
    let mut invalid = plan(&ctx, 1);
    invalid.embeddings[0].content_hash = "wrong".into();
    assert!(BatchEmbeddingStage
        .process(StageOutput::PlannedBatch(invalid), &ctx)
        .await
        .is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

    // The embedding stage owns reservation before provider work. Compiler tests
    // cannot detect its separate overcount: four independent entity embeddings
    // are one statement, so this plan fits exactly without raising the cap.
    let mut boundary = plan(&ctx, 4);
    boundary.batch.mutations = (0..MAX_STATEMENTS_PER_BATCH - 1)
        .map(|_| GraphMutation::UpdateEntity {
            uuid: Uuid::new_v4(),
            properties: json!({"summary":"context"}).as_object().unwrap().clone(),
        })
        .collect();
    let StageOutput::PreparedBatch(output) = BatchEmbeddingStage
        .process(StageOutput::PlannedBatch(boundary), &ctx)
        .await
        .unwrap()
    else {
        panic!("expected prepared batch");
    };
    assert_eq!(
        output.batch.estimated_statement_count(),
        MAX_STATEMENTS_PER_BATCH
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn mismatched_guarded_content_is_rejected_before_embedding() {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        fail_second: false,
    });
    let ctx = context(provider.clone());
    for key in ["name", "summary", "prop_description"] {
        let mut planned = plan(&ctx, 1);
        let kg_core::runtime::stage_output::PlannedEmbeddingWrite::EntityVersion {
            expected_properties,
            ..
        } = &mut planned.embeddings[0].write
        else {
            panic!("expected entity write")
        };
        expected_properties.insert(key.into(), json!("different content"));
        assert!(BatchEmbeddingStage
            .process(StageOutput::PlannedBatch(planned), &ctx)
            .await
            .is_err());
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}
