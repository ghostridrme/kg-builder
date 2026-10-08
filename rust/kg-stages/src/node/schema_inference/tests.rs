//! Schema inference tests; `store` covers the inferred-schema store.
mod store;

use super::*;
use kg_core::enums::EntityLifecycle;

fn proposal_passes_guardrails(
    samples: &[&ConnectorEntity],
    pks: &[String],
    volatile: &[String],
) -> bool {
    proposal_passes_guardrails_in_namespace(samples, pks, volatile, "prod")
}
fn sample(name: &str, props: serde_json::Value) -> ConnectorEntity {
    ConnectorEntity {
        additional_key_properties: vec![],
        labels: Vec::new(),
        entity_type: "NatGateway".into(),
        name: name.into(),
        primary_key_properties: vec![],
        raw_properties: props,
        namespace: Some("prod".into()),
        lifecycle: EntityLifecycle::Active,
        tags: indexmap::IndexMap::new(),
        source: "aws".into(),
        org_id: "org-1".into(),
    }
}

#[test]
fn guardrails_accept_stable_unique_pks() {
    let a = sample("nat-a", serde_json::json!({"arn": "arn:a", "state": "up"}));
    let b = sample("nat-b", serde_json::json!({"arn": "arn:b", "state": "up"}));
    assert!(proposal_passes_guardrails(
        &[&a, &b],
        &["arn".into()],
        &["state".into()]
    ));
}

#[test]
fn guardrails_reject_missing_field_duplicates_volatile_and_oversize() {
    let a = sample("nat-a", serde_json::json!({"arn": "arn:a"}));
    let b = sample("nat-b", serde_json::json!({"state": "up"}));
    // Field missing in one sample.
    assert!(!proposal_passes_guardrails(&[&a, &b], &["arn".into()], &[]));
    // Duplicate identity across samples.
    let c = sample("nat-c", serde_json::json!({"region": "us-east-1"}));
    let d = sample("nat-d", serde_json::json!({"region": "us-east-1"}));
    assert!(!proposal_passes_guardrails(
        &[&c, &d],
        &["region".into()],
        &[]
    ));
    // PK flagged volatile by the model itself.
    assert!(!proposal_passes_guardrails(
        &[&a],
        &["arn".into()],
        &["arn".into()]
    ));
    // Empty and oversized proposals.
    assert!(!proposal_passes_guardrails(&[&a], &[], &[]));
    let many: Vec<String> = (0..5).map(|i| format!("f{i}")).collect();
    assert!(!proposal_passes_guardrails(&[&a], &many, &[]));
}
#[test]
fn adopted_schema_validates_scope_all_values_and_rejects_degraded_identity() {
    let a = sample("a", serde_json::json!({"id":"a"}));
    let missing = sample("b", serde_json::json!({"other":"b"}));
    let mut schema = InferredSchema {
        org_id: "org-1".into(),
        source: "aws".into(),
        entity_type: "NatGateway".into(),
        primary_key_properties: vec!["id".into()],
        fk_property_hints: vec![],
        volatile_property_hints: vec![],
        inferred_by: "model".into(),
        degraded: false,
    };
    assert!(validate_schema(
        &schema,
        "org-1",
        "aws",
        "NatGateway",
        &[&a, &a],
        false,
        "prod"
    )
    .is_ok());
    assert!(validate_schema(
        &schema,
        "org-1",
        "aws",
        "NatGateway",
        &[&a, &missing],
        false,
        "prod"
    )
    .is_err());
    assert!(validate_schema(
        &schema,
        "org-1",
        "github",
        "NatGateway",
        &[&a],
        false,
        "prod"
    )
    .is_err());
    schema.primary_key_properties = vec!["name".into()];
    assert!(validate_schema(&schema, "org-1", "aws", "NatGateway", &[&a], false, "prod").is_err());
    schema.primary_key_properties = vec!["id".into()];
    schema.degraded = true;
    assert!(validate_schema(&schema, "org-1", "aws", "NatGateway", &[&a], false, "prod").is_err());
    assert!(proposal_passes_guardrails(&[&a, &a], &["id".into()], &[]));
    assert!(!proposal_passes_guardrails(
        &[&a],
        &["id".into(), "id".into()],
        &[]
    ));
}
