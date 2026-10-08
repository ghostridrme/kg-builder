use super::*;
use crate::flush::relationship_mutation_planning::{edge_properties, RelationshipEmbeddingTarget};
use crate::flush::{batch_embedding::BatchEmbeddingStage, test_support::*};
use chrono::Utc;
use indexmap::IndexMap;
use kg_core::{
    models::EntityEdge,
    traits::{EdgeLookup, GraphBackend, GraphMutation},
};
use kg_core::{
    runtime::{stage_output::PlannedBatchOutput, StageOutput},
    traits::{BatchIdentity, BatchKind, MutationBatch, RequestFingerprint, Stage},
};
use serde_json::json;

async fn prepare_embeddings(plan: Plan, context: &RuntimeContext) -> MutationBatch {
    let run_id = Uuid::new_v4();
    let planned = PlannedBatchOutput {
        batch: MutationBatch {
            org_id: ORG.into(),
            batch: BatchIdentity {
                run_id,
                kind: BatchKind::Relationship,
                index: 0,
            },
            fingerprint: RequestFingerprint(format!("{:032x}", run_id.as_u128())),
            preconditions: plan.preconditions,
            mutations: plan.mutations,
            result: json!({}),
        },
        embeddings: plan.embeddings,
    };
    let StageOutput::PreparedBatch(prepared) = BatchEmbeddingStage
        .process(StageOutput::PlannedBatch(planned), context)
        .await
        .unwrap()
    else {
        panic!("prepared embedding batch expected")
    };
    prepared.batch
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn relationship_vectors_are_planned_before_commit_and_reused_only_when_compatible() {
    let graph = live_graph().await;
    let context = ctx(graph.clone());
    let snap = snapshot(Utc::now());
    let edge = EntityEdge {
        producer_source: "test".into(),
        chain_id: Uuid::new_v4(),
        identity_hash: None,
        cardinality_key: None,
        origin: kg_core::models::RelationshipOrigin::Fact,
        uuid: Uuid::new_v4(),
        org_id: ORG.into(),
        source_chain_id: Uuid::new_v4(),
        target_chain_id: Uuid::new_v4(),
        name: "STOPPED".into(),
        identity_name: None,
        description: "worker retries exhausted".into(),
        all_properties: IndexMap::new(),
        discovered_by: None,
        resolved_by: None,
        source_property: None,
        target_identity_field: None,
        reference_evidence: None,
        confidence: 1.0,
        justification: None,
        first_seen_snapshot_id: Some(snap.uuid),
        last_seen_snapshot_id: Some(snap.uuid),
        last_seen_at: None,
        sync_generation: None,
        valid_from: snap.captured_at,
        cancelled_at: None,
        cancellation_snapshot_id: None,
        cancellation_context: None,
        time_evidence: None,
        valid_to: None,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        deleted_at: None,
        deleted_by: None,
        deletion_reason: None,
        created_at: snap.captured_at,
    };
    let live = |stored_pair| RelationshipEmbeddingTarget {
        uuid: edge.uuid,
        namespace: "prod".into(),
        name: edge.name.clone(),
        description: edge.description.clone(),
        stored_pair,
    };
    let mut plan = Plan::default();
    plan_relationship_embeddings(&[live(None)], &context, &mut plan)
        .await
        .unwrap();
    assert_eq!(plan.embeddings.len(), 1);
    assert!(
        plan.mutations.is_empty(),
        "planning must not prepare vectors"
    );
    let prepared = prepare_embeddings(plan, &context).await;
    let GraphMutation::SetRelationshipEmbedding {
        embedding: vector,
        text_version,
        content_hash,
        ..
    } = &prepared.mutations[0]
    else {
        panic!("relationship vector mutation expected")
    };
    assert_eq!(text_version, embedding::RELATIONSHIP_TEXT_VERSION);
    assert_eq!(
        content_hash,
        &embedding::content_hash(&embedding::relationship_representation(
            &edge.name,
            &edge.description
        ))
    );
    assert_eq!(vector.values.len(), context.embedding.dimension);
    for chain in [edge.source_chain_id, edge.target_chain_id] {
        seed_node(
            &graph,
            serde_json::json!({"uuid":chain,"chain_id":chain,"org_id":ORG,"is_latest":true}),
        )
        .await;
    }
    let mut props = edge_properties(
        &edge,
        1,
        None,
        &kg_core::runtime::stage_output::ConnectorScope {
            namespace: "prod".into(),
            source: edge.producer_source.clone(),
        },
    );
    props.insert("org_id".into(), ORG.into());
    props.insert("embedding".into(), serde_json::json!(vector.values));
    props.insert("embedding_model".into(), serde_json::json!(vector.model));
    props.insert(
        "embedding_text_version".into(),
        serde_json::json!(text_version),
    );
    props.insert(
        "embedding_content_hash".into(),
        serde_json::json!(content_hash),
    );
    for (ended, compatible) in [(false, true), (true, true), (true, false)] {
        if ended {
            props.insert("is_latest".into(), false.into());
            props.insert("valid_to".into(), snap.captured_at.to_rfc3339().into());
        }
        if !compatible {
            props.insert("embedding_text_version".into(), "obsolete".into());
        }
        seed_edge(&graph, &edge, &props).await;
        let mut plan = Plan::default();
        plan_relationship_embeddings(
            &[live(Some((edge.source_chain_id, edge.target_chain_id)))],
            &context,
            &mut plan,
        )
        .await
        .unwrap();
        assert_eq!(plan.embeddings.len(), usize::from(!compatible));
        assert!(plan.mutations.is_empty());
        if !compatible {
            let prepared = prepare_embeddings(plan, &context).await;
            graph
                .apply_mutations(ORG, &prepared.mutations)
                .await
                .unwrap();
            let records = graph
                .find_edges(
                    ORG,
                    &EdgeLookup::HeadsByChainPairs {
                        pairs: vec![(edge.source_chain_id, edge.target_chain_id)],
                    },
                )
                .await
                .unwrap();
            let stored = records
                .iter()
                .find(|record| record.uuid == edge.uuid)
                .unwrap();
            assert_eq!(
                stored.stored["embedding_text_version"],
                embedding::RELATIONSHIP_TEXT_VERSION
            );
            assert_eq!(stored.stored["is_latest"], false);
            assert!(graph
                .find_edges(
                    ORG,
                    &EdgeLookup::LiveByChainPairs {
                        pairs: vec![(edge.source_chain_id, edge.target_chain_id)],
                    }
                )
                .await
                .unwrap()
                .is_empty());
        }
    }
}
