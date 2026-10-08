use super::super::*;
use async_trait::async_trait;
use kg_core::{
    errors::BackendError,
    runtime::RuntimeContextBuilder,
    test_support::{MockEmbedBackend, MockLlmBackend},
    traits::*,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use uuid::Uuid;
pub struct UnreachableGraph;

fn no_graph<T>() -> Result<T, BackendError> {
    panic!("validation must not call storage")
}

#[async_trait]
impl SearchBackend for UnreachableGraph {}

#[async_trait]
impl GraphBackend for UnreachableGraph {
    async fn apply_mutations(&self, _: &str, _: &[GraphMutation]) -> Result<(), BackendError> {
        no_graph()
    }

    async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
        no_graph()
    }

    async fn commit_batch(&self, _: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        no_graph()
    }

    async fn committed_batches(
        &self,
        _: &str,
        _: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        no_graph()
    }

    async fn find_entities(
        &self,
        _: &str,
        _: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        no_graph()
    }

    async fn find_edges(&self, _: &str, _: &EdgeLookup) -> Result<Vec<EdgeRecord>, BackendError> {
        no_graph()
    }

    async fn health(&self) -> Result<(), BackendError> {
        no_graph()
    }

    async fn connect(&self) -> Result<(), BackendError> {
        no_graph()
    }

    async fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
}

struct Store {
    error: Option<&'static str>,
    winner: Mutex<Option<InferredSchema>>,
    barrier: Option<tokio::sync::Barrier>,
    adopts: AtomicUsize,
}
#[async_trait]
impl SchemaStore for Store {
    async fn get(&self, _: &str, _: &str, _: &str) -> Result<Option<InferredSchema>, BackendError> {
        match self.error {
            Some("get_connection") => return Err(BackendError::Connection("test".into())),
            Some("get_auth") => return Err(BackendError::Auth("test".into())),
            Some("cached") => return Ok(self.winner.lock().unwrap().clone()),
            _ => {}
        }
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        Ok(None)
    }
    async fn adopt(&self, schema: InferredSchema) -> Result<InferredSchema, BackendError> {
        self.adopts.fetch_add(1, Ordering::SeqCst);
        if self.error == Some("adopt") {
            return Err(BackendError::Connection("test".into()));
        }
        Ok(self.winner.lock().unwrap().get_or_insert(schema).clone())
    }
}
fn context(store: Arc<Store>, key: &str) -> (RuntimeContext, Arc<MockLlmBackend>) {
    let model = Arc::new(MockLlmBackend::with_responses(vec![
        serde_json::json!({"primary_key_properties":[key]}).to_string(),
    ]));
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(UnreachableGraph))
        .schema_store(store)
        .llm_default(model.clone())
        .llm_extraction(model.clone())
        .embedder(Arc::new(MockEmbedBackend::new(8)))
        .build()
        .unwrap();
    (ctx, model)
}
fn sample() -> ConnectorEntity {
    serde_json::from_value(serde_json::json!({"org_id":"org","source":"source","entity_type":"Service","name":"a",
        "primary_key_properties":[],"raw_properties":{"id":"1","arn":"arn:1"},"lifecycle":"active","tags":{}})).unwrap()
}
fn winner() -> InferredSchema {
    InferredSchema {
        org_id: "org".into(),
        source: "source".into(),
        entity_type: "Service".into(),
        primary_key_properties: vec!["arn".into()],
        fk_property_hints: vec![],
        volatile_property_hints: vec![],
        inferred_by: "test".into(),
        degraded: false,
    }
}
#[tokio::test]
async fn storage_errors_are_classified_without_fallback() {
    for (error, calls, retriable) in [
        ("get_connection", 0, true),
        ("get_auth", 0, false),
        ("adopt", 1, true),
    ] {
        let store = Arc::new(Store {
            error: Some(error),
            winner: Mutex::new(None),
            barrier: None,
            adopts: AtomicUsize::new(0),
        });
        let (ctx, model) = context(store.clone(), "id");
        let entity = sample();
        let result = resolve_schema(&ctx, "org", "source", "Service", "prod", &[&entity]).await;
        assert!(
            matches!(result, Err(StageError::StepFailed {retriable:r,..}) if r == retriable),
            "{result:?}"
        );
        assert_eq!(model.call_count(), calls);
        assert_eq!(store.adopts.load(Ordering::SeqCst), calls);
    }
}
#[tokio::test]
async fn adopted_and_cached_winners_are_validated() {
    for cached in [false, true] {
        let store = Arc::new(Store {
            error: cached.then_some("cached"),
            winner: Mutex::new(Some(winner())),
            barrier: None,
            adopts: AtomicUsize::new(0),
        });
        let (ctx, model) = context(store, "id");
        let entity = sample();
        let result = resolve_schema(&ctx, "org", "source", "Service", "prod", &[&entity])
            .await
            .unwrap();
        assert_eq!(result.primary_key_properties, ["arn"]);
        assert_eq!(model.call_count(), usize::from(!cached));
    }
    for case in 0..6 {
        let mut schema = winner();
        match case {
            0 => schema.org_id = "wrong".into(),
            1 => schema.source = "wrong".into(),
            2 => schema.entity_type = "wrong".into(),
            3 => schema.degraded = true,
            4 => schema.inferred_by.clear(),
            _ => {}
        }
        let store = Arc::new(Store {
            error: None,
            winner: Mutex::new(Some(schema)),
            barrier: None,
            adopts: AtomicUsize::new(0),
        });
        let (ctx, _) = context(store, "id");
        let mut entities = vec![sample(); 4];
        for (i, entity) in entities.iter_mut().enumerate() {
            entity.name = i.to_string();
            entity.raw_properties["id"] = i.into();
        }
        if case == 5 {
            entities[3]
                .raw_properties
                .as_object_mut()
                .unwrap()
                .remove("arn");
        }
        let refs: Vec<_> = entities.iter().collect();
        assert!(matches!(
            resolve_schema(&ctx, "org", "source", "Service", "prod", &refs).await,
            Err(StageError::StateValidation { .. })
        ));
    }
}
#[tokio::test]
async fn concurrent_proposals_use_the_same_adopted_schema() {
    let store = Arc::new(Store {
        error: None,
        winner: Mutex::new(None),
        barrier: Some(tokio::sync::Barrier::new(2)),
        adopts: AtomicUsize::new(0),
    });
    let (first, _) = context(store.clone(), "id");
    let (second, _) = context(store.clone(), "arn");
    let entity = sample();
    let samples = [&entity];
    let (a, b) = tokio::join!(
        resolve_schema(&first, "org", "source", "Service", "prod", &samples),
        resolve_schema(&second, "org", "source", "Service", "prod", &samples)
    );
    assert_eq!(
        a.unwrap().primary_key_properties,
        b.unwrap().primary_key_properties
    );
    assert_eq!(store.adopts.load(Ordering::SeqCst), 2);
}
