//! Byte limits of reference repair owner and selector identifiers.
use super::*;
#[test]
fn owner_and_selector_bytes_have_independent_limits() {
    let mut work = BTreeMap::new();
    for id in 1..=10_000 {
        work.insert(Uuid::from_u128(id), (None, Utc::now(), BTreeSet::new()));
    }
    assert!(validate_reference_work(&work).is_ok());
    work.insert(Uuid::from_u128(10_001), (None, Utc::now(), BTreeSet::new()));
    assert!(validate_reference_work(&work).is_err());
    work.clear();
    work.insert(
        Uuid::from_u128(1),
        (
            None,
            Utc::now(),
            [ReferenceOwnerSelector {
                chain_id: Uuid::from_u128(1),
                namespace: "prod".into(),
                slot: "x".repeat(16 * 1024 * 1024),
            }]
            .into(),
        ),
    );
    assert!(validate_reference_work(&work).is_err());
}

#[test]
fn cursor_progress_detects_cycles_without_assuming_unicode_collation() {
    use kg_core::traits::UnresolvedReferenceCursor;
    let mut cursors = ReferenceCursorProgress::default();
    let a = UnresolvedReferenceCursor {
        source_chain_id: Uuid::from_u128(1),
        slot: "Service.database".into(),
        token: "s:😀".into(),
    };
    let mut b = a.clone();
    b.token = "s:\u{e000}".into();
    cursors.advance(&a).unwrap();
    cursors.advance(&b).unwrap();
    assert!(cursors.advance(&a).is_err());
}

#[tokio::test(start_paused = true)]
async fn reference_reads_honor_cancellation_and_deadlines_for_custom_backends() {
    use kg_core::test_support::{MockEmbedBackend, MockLlmBackend};
    let mut ctx = kg_core::runtime::RuntimeContextBuilder::new("org")
        .graph(Arc::new(crate::test_support::UnreachableGraph))
        .llm_default(Arc::new(MockLlmBackend::empty()))
        .llm_extraction(Arc::new(MockLlmBackend::empty()))
        .embedder(Arc::new(MockEmbedBackend::default_dimension()))
        .build()
        .unwrap();
    ctx.context_settings.read_timeout_ms = 10;
    let result = reference_read(
        &ctx,
        "pending",
        std::future::pending::<Result<(), BackendError>>(),
    )
    .await;
    assert!(matches!(
        result,
        Err(PipelineError::StepExecution {
            retriable: true,
            ..
        })
    ));
    ctx.cancel.cancel();
    assert!(matches!(
        reference_read(
            &ctx,
            "cancelled",
            std::future::pending::<Result<(), BackendError>>()
        )
        .await,
        Err(PipelineError::Cancelled)
    ));
}
