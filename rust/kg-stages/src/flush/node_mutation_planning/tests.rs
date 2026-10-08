use super::*;
use crate::flush::{relationship_mutation_planning::plan_relationships, test_support::*};
use kg_core::{models::EntityEdge, runtime::stage_output::RelationshipBatch};
use serde_json::json;
use std::sync::Arc;

#[test]
fn entity_provenance_is_written_under_extracted_by() {
    let snapshot = snapshot(Utc::now());
    let mut node = entity("api", 1, &snapshot);
    for method in [
        "direct",
        "sub_entity_rule",
        "llm:provider/model",
        "custom-parser",
    ] {
        node.extracted_by = method.into();
        let properties = entity_properties(
            &node,
            ORG,
            node.version,
            None,
            &node.all_properties,
            node.structural_hash,
            &node.collections,
        );
        assert_eq!(properties.get("extracted_by"), Some(&json!(method)));
        assert!(!properties.contains_key("discovered_by"));
    }
}

#[test]
fn volatile_removal_writes_null_and_relationship_observations_need_their_snapshot() {
    let removed = PropertyChange {
        property: "status".into(),
        old_value: Some(serde_json::json!({"s": "running"})),
        new_value: None,
    };
    let props = change_properties(&[removed]).unwrap();
    assert_eq!(props.get("prop_status"), Some(&Value::Null));

    let snap = snapshot(Utc::now());
    let source = entity("api", 1, &snap);
    let edge = EntityEdge {
        producer_source: "test".into(),
        chain_id: Uuid::new_v4(),
        identity_hash: None,
        cardinality_key: None,
        origin: kg_core::models::RelationshipOrigin::Fact,
        uuid: Uuid::new_v4(),
        org_id: ORG.into(),
        source_chain_id: source.chain_id,
        target_chain_id: Uuid::new_v4(),
        name: "DEPENDS_ON".into(),
        identity_name: None,
        description: String::new(),
        all_properties: IndexMap::new(),
        discovered_by: None,
        resolved_by: None,
        source_property: None,
        target_identity_field: None,
        reference_evidence: None,
        confidence: 1.0,
        justification: None,
        first_seen_snapshot_id: None,
        last_seen_snapshot_id: None,
        last_seen_at: None,
        sync_generation: None,
        valid_from: snap.captured_at,
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        time_evidence: None,
        valid_to: None,
        version: 2,
        is_latest: true,
        previous_version_uuid: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        created_at: snap.captured_at,
    };
    let batch = RelationshipBatch {
        relationship_assessments: Default::default(),
        contradiction_timelines: Default::default(),
        relationship_directives: Default::default(),
        reference_report: Default::default(),
        snapshot_nodes: Arc::new(vec![snap]),
        observed: Arc::new(vec![edge]),
        ..Default::default()
    };
    let error = plan_relationships(&batch).unwrap_err();
    assert!(
        error.to_string().contains("no observing snapshot"),
        "{error}"
    );
}

#[tokio::test]
async fn volatile_writes_restore_reusable_vectors_even_when_rendered_text_is_unchanged() {
    use kg_core::runtime::stage_output::{Observed, ObservedEntityProperties, ResolvedChange};
    for excluded in [false, true] {
        let mut context = ctx(Arc::new(kg_core::test_support::UnreachableGraph));
        if excluded {
            context.embedding = context
                .embedding
                .clone()
                .with_entity_fields(embedding::EntityEmbeddingFields {
                    default: vec![],
                    ..Default::default()
                })
                .unwrap();
        }
        let snap = snapshot(Utc::now());
        let mut node = entity("api", 1, &snap);
        let prefix = "x".repeat(embedding::MAX_FIELD_CHARS);
        let old_description = format!("{prefix} old");
        let new_description = format!("{prefix} new");
        node.all_properties.insert(
            "description".into(),
            PropertyValue::String(old_description.clone()),
        );
        let original_text = embedding::entity_text(&node, &context.embedding);
        let vector = embedding::ComputedEmbedding::new(
            &context.embedding,
            embedding::content_hash(&original_text),
            vec![1.0; context.embedding.dimension],
        );
        node.embedding = Some(Arc::new(vector.clone()));
        node.all_properties.insert(
            "description".into(),
            PropertyValue::String(new_description.clone()),
        );
        assert_eq!(
            embedding::entity_text(&node, &context.embedding),
            original_text
        );
        let observed = ObservedEntityProperties::from_entity(&node, snap.uuid);
        let batch = NodeBatch {
            snapshot_nodes: Arc::new(vec![snap]),
            observed_properties: Arc::new(vec![observed.clone()]),
            nodes_volatile: Arc::new(vec![Observed::new(
                observed.observation_uuid,
                ResolvedChange {
                    entity: node,
                    changes: vec![PropertyChange {
                        property: "description".into(),
                        old_value: Some(
                            serde_json::to_value(PropertyValue::String(old_description)).unwrap(),
                        ),
                        new_value: Some(
                            serde_json::to_value(PropertyValue::String(new_description)).unwrap(),
                        ),
                    }],
                },
            )]),
            ..Default::default()
        };
        let plan = plan_nodes(&batch, &context).await.unwrap();
        assert!(plan.mutations.iter().any(|mutation| matches!(mutation, GraphMutation::UpdateEntity { properties, .. } if properties.contains_key("prop_description"))));
        assert_eq!(
            plan.embeddings.len(),
            1,
            "a content write must restore its vector"
        );
        assert_eq!(plan.embeddings[0].reuse.as_ref(), Some(&vector));
        assert_eq!(plan.embeddings[0].text, original_text);
    }
}

#[tokio::test]
async fn same_chunk_reobservation_does_not_embed_unwritten_native_fields() {
    use kg_core::runtime::stage_output::{Observed, ObservedEntityProperties};
    let context = ctx(Arc::new(kg_core::test_support::UnreachableGraph));
    let first = snapshot(Utc::now());
    let later = snapshot(first.captured_at + chrono::Duration::seconds(1));
    let mut created = entity("original name", 1, &first);
    created.primary_key_properties = vec!["replicas".into()];
    created.summary = Some("original summary".into());
    let first_observed = ObservedEntityProperties::from_entity(&created, first.uuid);
    let mut again = created.clone();
    again.uuid = Uuid::new_v4();
    again.name = "later display name".into();
    again.summary = Some("later summary".into());
    again.valid_from = later.captured_at;
    again.last_seen_snapshot_id = Some(later.uuid);
    let later_observed = ObservedEntityProperties::from_entity(&again, later.uuid);
    let batch = NodeBatch {
        snapshot_nodes: Arc::new(vec![first, later]),
        observed_properties: Arc::new(vec![first_observed.clone(), later_observed.clone()]),
        nodes_to_create: Arc::new(vec![Observed::new(
            first_observed.observation_uuid,
            created,
        )]),
        nodes_unchanged: Arc::new(vec![Observed::new(later_observed.observation_uuid, again)]),
        ..Default::default()
    };
    let plan = plan_nodes(&batch, &context).await.unwrap();
    assert_eq!(plan.embeddings.len(), 1);
    let target = &plan.embeddings[0];
    assert!(target
        .text
        .contains("name: original name\nsummary: original summary"));
    let kg_core::runtime::stage_output::PlannedEmbeddingWrite::EntityVersion {
        expected_properties: guarded,
        ..
    } = &target.write
    else {
        panic!("expected entity write")
    };
    assert_eq!(guarded["name"], json!("original name"));
    assert_eq!(guarded["summary"], json!("original summary"));
}

#[tokio::test]
async fn simultaneous_complementary_incremental_observations_share_one_version() {
    use kg_core::runtime::stage_output::{Observed, ObservedEntityProperties};
    let context = ctx(Arc::new(kg_core::test_support::UnreachableGraph));
    let mut first = snapshot(Utc::now());
    first.snapshot_kind = SnapshotKind::Incremental;
    let mut second = snapshot(first.captured_at);
    second.snapshot_kind = SnapshotKind::Incremental;
    let mut left = entity("service", 1, &first);
    left.all_properties
        .insert("left_fact".into(), PropertyValue::String("left".into()));
    let mut right = left.clone();
    right.uuid = Uuid::new_v4();
    right.last_seen_snapshot_id = Some(second.uuid);
    right.all_properties.shift_remove("left_fact");
    right
        .all_properties
        .insert("right_fact".into(), PropertyValue::String("right".into()));
    let a = ObservedEntityProperties::from_entity(&left, first.uuid);
    let b = ObservedEntityProperties::from_entity(&right, second.uuid);
    let batch = NodeBatch {
        snapshot_nodes: Arc::new(vec![first, second]),
        observed_properties: Arc::new(vec![a.clone(), b.clone()]),
        nodes_to_create: Arc::new(vec![Observed::new(a.observation_uuid, left)]),
        nodes_unchanged: Arc::new(vec![Observed::new(b.observation_uuid, right)]),
        ..Default::default()
    };
    let plan = plan_nodes(&batch, &context).await.unwrap();
    assert_eq!(plan.counts.entities_created, 1);
    assert_eq!(plan.counts.entities_updated, 0);
    assert_eq!(plan.counts.observations, 2);
    let creates: Vec<_> = plan
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            GraphMutation::UpsertEntity { properties, .. } => Some(properties),
            _ => None,
        })
        .collect();
    assert_eq!(creates.len(), 1);
    assert_eq!(creates[0]["prop_left_fact"], json!("left"));
    assert_eq!(creates[0]["prop_right_fact"], json!("right"));
    assert_eq!(
        batch.observed_properties[0].properties.get("right_fact"),
        None
    );
    assert_eq!(
        batch.observed_properties[1].properties.get("left_fact"),
        None
    );
    let mut reversed = batch.clone();
    Arc::make_mut(&mut reversed.snapshot_nodes).reverse();
    let reversed_plan = plan_nodes(&reversed, &context).await.unwrap();
    let reversed_properties = reversed_plan
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            GraphMutation::UpsertEntity { properties, .. } => Some(properties),
            _ => None,
        })
        .unwrap();
    assert_eq!(reversed_properties["prop_left_fact"], json!("left"));
    assert_eq!(reversed_properties["prop_right_fact"], json!("right"));
    assert_eq!(reversed_plan.counts.entities_updated, 0);

    let mut conflicting = batch.clone();
    Arc::make_mut(&mut conflicting.observed_properties)[1]
        .properties
        .insert(
            "left_fact".into(),
            PropertyValue::String("contradiction".into()),
        );
    assert!(plan_nodes(&conflicting, &context)
        .await
        .unwrap_err()
        .to_string()
        .contains("conflicting partial source properties"));
    let mut full = batch.clone();
    Arc::make_mut(&mut full.snapshot_nodes)[1].snapshot_kind = SnapshotKind::Full;
    assert!(plan_nodes(&full, &context)
        .await
        .unwrap_err()
        .to_string()
        .contains("two different contents captured at the same time"));
}
