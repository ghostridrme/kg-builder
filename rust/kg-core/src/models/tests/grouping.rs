use super::*;
use chrono::{Duration, Utc};
use uuid::Uuid;

fn saga() -> ThreadNode {
    ThreadNode {
        summary_incomplete_reason: None,
        summary_incomplete_from_ordinal: None,
        revision: 0,
        last_membership_ordinal: 0,
        summary_revision: None,
        summary_cursor: 0,
        summary_supporting_snapshot_uuids: vec![],
        uuid: Uuid::new_v4(),
        org_id: "org".into(),
        namespace: "prod".into(),
        name: "incident".into(),
        labels: vec!["operations".into()],
        created_at: Utc::now(),
        summary: String::new(),
        first_snapshot_uuid: None,
        last_snapshot_uuid: None,
        last_summarized_at: None,
        last_summarized_snapshot_captured_at: None,
        first_captured_at: None,
    }
}

#[test]
fn saga_requires_scope_and_paired_valid_endpoints_without_ordering_source_clocks() {
    let mut node = saga();
    node.validate().unwrap();
    node.first_snapshot_uuid = Some(Uuid::new_v4());
    assert!(node.validate().is_err());
    node.last_snapshot_uuid = node.first_snapshot_uuid;
    node.last_summarized_at = Some(node.created_at);
    node.last_summarized_snapshot_captured_at = Some(node.created_at + Duration::days(1));
    node.validate().unwrap();
    let restored: ThreadNode =
        serde_json::from_value(serde_json::to_value(&node).unwrap()).unwrap();
    restored.validate().unwrap();
    node.last_snapshot_uuid = Some(Uuid::nil());
    assert!(node.validate().is_err());
    node.last_snapshot_uuid = node.first_snapshot_uuid;
    node.namespace.clear();
    assert!(node.validate().is_err());
}

#[test]
fn community_names_have_model_tagged_valid_embeddings() {
    let mut node = CommunityNode {
        uuid: Uuid::new_v4(),
        org_id: "org".into(),
        namespace: "prod".into(),
        name: "payments".into(),
        labels: vec![],
        created_at: Utc::now(),
        summary: "Payment services".into(),
        name_embedding: None,
    };
    node.validate().unwrap();
    node.name_embedding = Some(crate::traits::graph_backend::GraphEmbedding {
        model: "model".into(),
        values: vec![0.0],
    });
    assert!(node.validate().is_err());
    node.name_embedding.as_mut().unwrap().values = vec![1.0];
    node.validate().unwrap();
    let restored: CommunityNode =
        serde_json::from_value(serde_json::to_value(&node).unwrap()).unwrap();
    restored.validate().unwrap();
    node.labels.push(" ".into());
    assert!(node.validate().is_err());
}

#[test]
fn structural_edges_preserve_exact_endpoints_and_reject_self_links() {
    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    let mut evidence = SnapshotEdge {
        uuid: Uuid::new_v4(),
        org_id: "org".into(),
        snapshot_uuid: source,
        entity_uuid: target,
        entity_chain_id: Uuid::new_v4(),
        observed_at: Utc::now(),
    };
    evidence.validate().unwrap();
    evidence.entity_chain_id = source;
    assert!(evidence.validate().is_err());
    for endpoint in [source, Uuid::nil()] {
        assert!(CommunityEdge {
            uuid: Uuid::new_v4(),
            org_id: "org".into(),
            community_uuid: source,
            member_uuid: endpoint,
            created_at: Utc::now()
        }
        .validate()
        .is_err());
        assert!(HasSnapshotEdge {
            ordinal: 1,
            uuid: Uuid::new_v4(),
            org_id: "org".into(),
            saga_uuid: source,
            snapshot_uuid: endpoint,
            created_at: Utc::now()
        }
        .validate()
        .is_err());
        assert!(NextSnapshotEdge {
            saga_uuid: Uuid::new_v4(),
            uuid: Uuid::new_v4(),
            org_id: "org".into(),
            source_snapshot_uuid: source,
            target_snapshot_uuid: endpoint,
            created_at: Utc::now()
        }
        .validate()
        .is_err());
    }
    CommunityEdge {
        uuid: Uuid::new_v4(),
        org_id: "org".into(),
        community_uuid: source,
        member_uuid: target,
        created_at: Utc::now(),
    }
    .validate()
    .unwrap();
}
