use kg_core::{
    profiles::Profile,
    runtime::{schemas::RunSchemaManifest, RuntimeContextBuilder, StageOutput},
    test_support::{MockEmbedBackend, MockLlmBackend, UnreachableGraph},
    traits::{LlmBackend, Stage},
};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn profile_structured_membership_is_opt_in_and_no_model_is_called() {
    let model = Arc::new(MockLlmBackend::empty());
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(UnreachableGraph))
        .llm_extraction(model.clone())
        .llm_default(model.clone())
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .build()
        .unwrap();
    let profile:Profile=serde_json::from_value(json!({"format_version":1,"profile_id":"test","revision":1,"mode":"strict","ontology":{"entity_types":[{"name":"Service"}]}})).unwrap();
    let legacy = ctx.clone();
    let ctx = ctx.with_run_schemas(RunSchemaManifest {
        org_id: "org".into(),
        profiles: BTreeMap::from([("inventory".into(), profile.freeze().unwrap())]),
        sources: BTreeMap::from([(
            "inventory".into(),
            profile.compose(&Default::default()).unwrap(),
        )]),
    });
    // Exercise real validation/preparation and supplied extraction, not just a type predicate.
    let input:Box<kg_core::models::SnapshotInput>=serde_json::from_value(json!({"name":"scan","source":"inventory","namespace":"prod","data_type":"entities","entities":[{"name":"a","entity_type":"Unexpected","source":"inventory","org_id":"org","primary_key_properties":["name"],"raw_properties":{},"lifecycle":"active","tags":{}}]})).unwrap();
    for (context, rejects) in [(&ctx, true), (&legacy, false)] {
        let validated = crate::InputValidationStage
            .process(StageOutput::Input(input.clone()), context)
            .await
            .unwrap();
        let prepared = crate::SnapshotPreparationStage
            .process(validated, context)
            .await
            .unwrap();
        let result = crate::EntityExtractionStage
            .process(prepared, context)
            .await;
        assert_eq!(result.is_err(), rejects, "{result:?}");
        assert_eq!(model.call_count(), 0);
    }
}

#[tokio::test]
async fn profile_guidance_is_frozen_but_text_extraction_is_rejected() {
    let model = Arc::new(CaptureModel {
        inner: MockLlmBackend::with_responses(vec![
            json!({"entities":[{"name":"api","entity_type":"Unknown","properties":{}}]})
                .to_string(),
        ]),
        requests: Default::default(),
    });
    let mut extraction = kg_core::runtime::extraction::ExtractionSettings {
        omission_check: false,
        ..Default::default()
    };
    extraction.source_guidance.insert(
        "messages".into(),
        kg_core::runtime::extraction::SourceExtractionGuidance {
            excluded_entity_types: vec!["Secret".into()],
            ..Default::default()
        },
    );
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(UnreachableGraph))
        .llm_extraction(model.clone())
        .llm_default(model.clone())
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .extraction_settings(extraction)
        .build()
        .unwrap();
    let profile:Profile=serde_json::from_value(json!({"format_version":1,"profile_id":"text","revision":1,"mode":"strict","ontology":{"entity_types":[{"name":"Service"}]},"source_guidance":{"messages":{"instructions":"Only identified operational services."}}})).unwrap();
    let ctx = ctx.with_run_schemas(RunSchemaManifest {
        org_id: "org".into(),
        profiles: BTreeMap::from([("messages".into(), profile.freeze().unwrap())]),
        sources: BTreeMap::from([(
            "messages".into(),
            profile.compose(&Default::default()).unwrap(),
        )]),
    });
    assert_eq!(
        ctx.extraction_settings
            .for_source("messages")
            .instructions
            .as_deref(),
        Some("Only identified operational services.")
    );
    assert!(ctx
        .extraction_settings
        .for_source("messages")
        .excluded_entity_types
        .contains(&"Secret".into()));
    let mut next = ctx.run_schemas.as_ref().unwrap().as_ref().clone();
    let mut no_guidance = profile.clone();
    no_guidance.revision = 2;
    no_guidance.source_guidance.clear();
    next.profiles
        .insert("messages".into(), no_guidance.freeze().unwrap());
    let next_ctx = ctx.with_run_schemas(next);
    assert!(next_ctx
        .extraction_settings
        .for_source("messages")
        .instructions
        .is_none());
    assert!(next_ctx
        .extraction_settings
        .for_source("messages")
        .excluded_entity_types
        .contains(&"Secret".into()));
    let input=serde_json::from_value(json!({"name":"message","namespace":"prod","source":"messages","data_type":"text","content":"api is an operational service","entities":[]})).unwrap();
    let validated = crate::InputValidationStage
        .process(StageOutput::Input(input), &ctx)
        .await
        .unwrap();
    let prepared = crate::SnapshotPreparationStage
        .process(validated, &ctx)
        .await
        .unwrap();
    assert!(crate::EntityExtractionStage
        .process(prepared, &ctx)
        .await
        .is_err());
    assert_eq!(model.inner.call_count(), 0);
    assert!(model.requests.lock().unwrap().is_empty());
}

struct CaptureModel {
    inner: MockLlmBackend,
    requests: std::sync::Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl LlmBackend for CaptureModel {
    async fn complete(
        &self,
        messages: &[kg_core::traits::LlmMessage],
        schema: Option<&serde_json::Value>,
        max_tokens: Option<u32>,
    ) -> Result<kg_core::traits::LlmResponse, kg_core::errors::BackendError> {
        self.requests
            .lock()
            .unwrap()
            .extend(messages.iter().map(|m| m.content.clone()));
        self.inner.complete(messages, schema, max_tokens).await
    }
    async fn complete_bounded(
        &self,
        messages: &[kg_core::traits::LlmMessage],
        schema: Option<&serde_json::Value>,
        max_tokens: Option<u32>,
        budget: &kg_core::traits::llm_backend::CallBudget,
    ) -> Result<kg_core::traits::LlmResponse, kg_core::errors::BackendError> {
        budget.consume()?;
        self.complete(messages, schema, max_tokens).await
    }
    fn supports_bounded_attempts(&self) -> bool {
        true
    }
    fn model_id(&self) -> &str {
        "captured-fake"
    }
    fn context_window(&self) -> usize {
        128_000
    }
}

#[tokio::test]
async fn profile_endpoint_reads_obey_cancellation_before_accessing_storage() {
    let profile:Profile=serde_json::from_value(json!({"format_version":1,"profile_id":"cancel","revision":1,"mode":"strict","ontology":{"entity_types":[{"name":"Service"}]}})).unwrap();
    let ctx = RuntimeContextBuilder::new("org")
        .graph(Arc::new(UnreachableGraph))
        .llm_extraction(Arc::new(MockLlmBackend::empty()))
        .llm_default(Arc::new(MockLlmBackend::empty()))
        .embedder(Arc::new(MockEmbedBackend::new(4)))
        .build()
        .unwrap()
        .with_run_schemas(RunSchemaManifest {
            org_id: "org".into(),
            profiles: BTreeMap::from([("inventory".into(), profile.freeze().unwrap())]),
            sources: BTreeMap::from([(
                "inventory".into(),
                profile.compose(&Default::default()).unwrap(),
            )]),
        });
    ctx.cancel.cancel();
    let result = super::relationships(
        &ctx,
        &[(
            "inventory",
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "RELATES_TO",
        )],
        &[],
    )
    .await;
    assert!(matches!(
        result,
        Err(kg_core::errors::StageError::Cancelled { .. })
    ));
}
