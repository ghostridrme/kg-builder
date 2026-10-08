use super::*;
use kg_core::{
    errors::{BackendError, StageError},
    runtime::{RuntimeContextBuilder, StageOutput},
    test_support::{MockEmbedBackend, MockLlmBackend},
    traits::{EmbedBackend, LlmBackend, LlmMessage, LlmResponse, StageContract, StageKind},
};

struct VersionedStage(&'static str);
#[async_trait::async_trait]
impl Stage for VersionedStage {
    fn name(&self) -> &str {
        "same-name"
    }
    fn processing_version(&self) -> String {
        self.0.into()
    }
    fn contract(&self) -> StageContract {
        StageKind::IDENTITY
    }
    async fn process(&self, _: StageOutput, _: &RuntimeContext) -> Result<StageOutput, StageError> {
        panic!("fingerprinting cannot execute stages")
    }
}
struct Model {
    route: &'static str,
    context: usize,
    output: u64,
}
#[async_trait::async_trait]
impl LlmBackend for Model {
    fn model_id(&self) -> &str {
        "same-model"
    }
    fn context_window(&self) -> usize {
        self.context
    }
    fn processing_descriptor(&self) -> serde_json::Value {
        serde_json::json!({"route":self.route,"context":self.context,"output":self.output})
    }
    async fn complete(
        &self,
        _: &[LlmMessage],
        _: Option<&serde_json::Value>,
        _: Option<u32>,
    ) -> Result<LlmResponse, BackendError> {
        panic!("fingerprinting cannot call providers")
    }
}
struct EmbeddingRoute(&'static str);
#[async_trait::async_trait]
impl EmbedBackend for EmbeddingRoute {
    fn model_id(&self) -> &str {
        "same-embedding-model"
    }
    fn dimension(&self) -> usize {
        4
    }
    fn max_batch_size(&self) -> usize {
        10
    }
    fn processing_descriptor(&self) -> serde_json::Value {
        serde_json::json!({"route":self.0})
    }
    async fn embed_batch(&self, _: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
        panic!("fingerprinting cannot call providers")
    }
}
fn context() -> RuntimeContext {
    RuntimeContextBuilder::new("org")
        .graph(Arc::new(crate::test_support::UnreachableGraph))
        .llm_extraction(Arc::new(MockLlmBackend::empty()))
        .llm_default(Arc::new(MockLlmBackend::empty()))
        .embedder(Arc::new(MockEmbedBackend::default_dimension()))
        .build()
        .unwrap()
}
#[test]
fn every_phase_processing_version_participates_before_receipt_recovery() {
    for phase in 0..8 {
        let mut runner = PipelineRunner::new();
        let ctx = context();
        let stages = match phase {
            0 => &mut runner.node_stages,
            1 => &mut runner.resolution_stages,
            2 => &mut runner.edge_stages,
            3 => &mut runner.relationship_resolution_stages,
            4 => &mut runner.flush_stages,
            5 => &mut runner.summary_stages,
            6 => &mut runner.saga_summary_stages,
            _ => &mut runner.community_stages,
        };
        stages.push(Arc::new(VersionedStage("prompt-v1")));
        let first = runner.settings(&ctx);
        let stages = match phase {
            0 => &mut runner.node_stages,
            1 => &mut runner.resolution_stages,
            2 => &mut runner.edge_stages,
            3 => &mut runner.relationship_resolution_stages,
            4 => &mut runner.flush_stages,
            5 => &mut runner.summary_stages,
            6 => &mut runner.saga_summary_stages,
            _ => &mut runner.community_stages,
        };
        stages[0] = Arc::new(VersionedStage("prompt-v2"));
        assert_ne!(first, runner.settings(&ctx), "phase {phase}");
    }
}
#[test]
fn effective_provider_changes_affect_all_model_roles_and_embeddings() {
    let runner = PipelineRunner::new();
    for role in 0..4 {
        let mut ctx = context();
        let assign = |ctx: &mut RuntimeContext, model: Arc<dyn LlmBackend>| match role {
            0 => ctx.llm_extraction = model,
            1 => ctx.llm_disambiguation = model,
            2 => ctx.llm_edge_discovery = model,
            _ => ctx.llm_default = model,
        };
        assign(
            &mut ctx,
            Arc::new(Model {
                route: "a",
                context: 4096,
                output: 100,
            }),
        );
        let original = runner.settings(&ctx);
        for model in [
            Model {
                route: "b",
                context: 4096,
                output: 100,
            },
            Model {
                route: "a",
                context: 8192,
                output: 100,
            },
            Model {
                route: "a",
                context: 4096,
                output: 200,
            },
        ] {
            assign(&mut ctx, Arc::new(model));
            assert_ne!(original, runner.settings(&ctx));
        }
    }
    let mut ctx = context();
    ctx.embedder = Arc::new(EmbeddingRoute("a"));
    let original = runner.settings(&ctx);
    ctx.embedder = Arc::new(EmbeddingRoute("b"));
    assert_ne!(original, runner.settings(&ctx));
}

#[test]
fn reference_resolution_settings_are_part_of_the_run_identity() {
    let runner = PipelineRunner::new();
    let mut ctx = context();
    let original = runner.settings(&ctx);
    ctx.reference_resolution_settings
        .disclosure_restricted_paths = vec!["Secret".into()];
    let restricted = runner.settings(&ctx);
    assert_ne!(original, restricted, "disclosure policy changes the run");
    ctx.reference_resolution_settings
        .disclosure_restricted_paths
        .clear();
    ctx.reference_resolution_settings.max_supporting_items = 2;
    assert_ne!(
        original,
        runner.settings(&ctx),
        "answer bounds change the run"
    );
}
