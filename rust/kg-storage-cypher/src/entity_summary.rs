//! Guarded derived-summary publication and atomic invalidation of supporting changes.
use crate::PreparedWrite;
use kg_core::{
    entity_summary::{self, DERIVED_PROPERTIES},
    errors::BackendError,
    traits::GraphMutation,
};
use serde_json::json;
use std::collections::BTreeSet;
use uuid::Uuid;

pub fn incident_history(
    org: &str,
    chain: Uuid,
    versions: &[kg_core::traits::relationship_timeline::IncidentVersionState],
) -> Result<PreparedWrite, BackendError> {
    kg_core::traits::relationship_timeline::validate_incident(chain, versions)?;
    let (keys, rows) = encode_incident(versions);
    Ok(PreparedWrite { statement:"OPTIONAL MATCH (anchor:Entity {org_id:$org,chain_id:$chain})-[r:RELATES_TO {org_id:$org}]-(other:Entity {org_id:$org})
        WITH DISTINCT r ORDER BY r.uuid
        FOREACH (edge IN CASE WHEN r IS NULL THEN [] ELSE [r] END | SET edge.uuid=edge.uuid)
        WITH collect(r) AS actual WHERE size(actual)=size($rows)
          AND all(i IN range(0,size(actual)-1) WHERE
            startNode(actual[i]).chain_id=$rows[i][0] AND endNode(actual[i]).chain_id=$rows[i][1]
            AND size([key IN keys(actual[i]) WHERE NOT key IN $ignored])=size($rows[i][2])
            AND all(j IN range(0,size($rows[i][2])-1) WHERE actual[i][$keys[$rows[i][2][j]]]=$rows[i][3][j])) RETURN true AS ok".into(),parameters:json!({"org":org,"chain":chain,"keys":keys,"rows":rows,"ignored":kg_core::traits::relationship_timeline::EMBEDDING_PROPERTIES}),expected_rows:1 })
}

fn encode_incident(
    versions: &[kg_core::traits::relationship_timeline::IncidentVersionState],
) -> (Vec<String>, Vec<serde_json::Value>) {
    let mut versions = versions.to_vec();
    versions.sort_by_key(|v| v.properties["uuid"].as_str().unwrap_or_default().to_owned());
    // Dictionary encoding removes repeated property names without hashing away state.
    let keys: Vec<_> = versions
        .iter()
        .flat_map(|v| v.properties.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let rows: Vec<_> = versions
        .iter()
        .map(|v| {
            let indices: Vec<_> = v
                .properties
                .keys()
                .map(|k| keys.binary_search(k).expect("collected key"))
                .collect();
            let values: Vec<_> = v.properties.values().collect();
            json!([v.source_chain_id, v.target_chain_id, indices, values])
        })
        .collect();
    (keys, rows)
}

fn ignored_entity_properties() -> Vec<&'static str> {
    DERIVED_PROPERTIES
        .iter()
        .copied()
        .chain([
            "embedding",
            "embedding_model",
            "embedding_text_version",
            "embedding_content_hash",
        ])
        .collect()
}
const ENTITY_TIMELINE:&str="OPTIONAL MATCH (n:Entity {org_id:$org,chain_id:$chain}) WITH n ORDER BY n.uuid
    FOREACH (node IN CASE WHEN n IS NULL THEN [] ELSE [n] END | SET node.uuid=node.uuid)
    WITH collect(n) AS actual WHERE size(actual)=size($versions)
      AND all(i IN range(0,size(actual)-1) WHERE actual[i].uuid=$versions[i].uuid
        AND size([key IN keys(actual[i]) WHERE NOT key IN $ignored])=size(keys($versions[i]))
        AND all(key IN keys($versions[i]) WHERE actual[i][key]=$versions[i][key])) RETURN true AS ok";

pub fn publication(org: &str, mutation: &GraphMutation) -> Vec<PreparedWrite> {
    let GraphMutation::SetDerivedSummary {
        guard,
        summary,
        embedding,
    } = mutation
    else {
        return vec![];
    };
    let mut checks = guarded_checks(org, guard);
    checks.push(PreparedWrite {statement:WRITE.into(),parameters:json!({"org":org,"uuid":guard.target_uuid,"revision":summary.revision,"text":summary.text,"as_of":summary.as_of.to_rfc3339(),"until":summary.valid_until.map(|v|v.to_rfc3339()),"evidence_hash":summary.evidence_hash,"policy":summary.policy_version,"ids":summary.evidence_ids,"total":summary.total_evidence,"values":embedding.values,"model":embedding.model,"text_version":entity_summary::SUMMARY_TEXT_VERSION,"content_hash":kg_core::embedding::content_hash(&entity_summary::embedding_text(&summary.text))}),expected_rows:1});
    checks
}
fn guarded_checks(org: &str, guard: &entity_summary::SummaryEvidenceGuard) -> Vec<PreparedWrite> {
    let mut checks = Vec::new();
    for (chain, versions) in &guard.entity_versions {
        // GraphMutation validation already checked each complete projection.
        let mut versions = versions.clone();
        versions.sort_by_key(|p| p["uuid"].as_str().unwrap_or_default().to_owned());
        checks.push(PreparedWrite {statement:ENTITY_TIMELINE.into(),parameters:json!({"org":org,"chain":chain,"versions":versions,"ignored":ignored_entity_properties()}),expected_rows:1});
    }
    let (keys, rows) = encode_incident(&guard.incident_versions);
    checks.push(PreparedWrite {statement:INCIDENT_GUARD.into(),parameters:json!({"org":org,"uuid":guard.target_uuid,"chain":guard.target_chain_id,"keys":keys,"rows":rows,"ignored":kg_core::traits::relationship_timeline::EMBEDDING_PROPERTIES,"revision":guard.expected_revision}),expected_rows:1});
    for check in &mut checks {
        check.statement = format!(
            "CALL {{ {} }} RETURN count(*)=1 AS ok,count(*)<>1 AS summary_conflict",
            check.statement
        );
    }
    checks
}
const INCIDENT_GUARD:&str="MATCH (n:Entity {org_id:$org,uuid:$uuid,chain_id:$chain})
    WHERE (n.summary_revision IS NULL AND $revision IS NULL) OR n.summary_revision=$revision
    SET n.uuid=n.uuid
    WITH n OPTIONAL MATCH (anchor:Entity {org_id:$org,chain_id:$chain})-[r:RELATES_TO {org_id:$org}]-(other:Entity {org_id:$org})
    WITH DISTINCT n,r ORDER BY r.uuid
    FOREACH (edge IN CASE WHEN r IS NULL THEN [] ELSE [r] END | SET edge.uuid=edge.uuid)
    WITH n,collect(r) AS actual WHERE size(actual)=size($rows)
      AND all(i IN range(0,size(actual)-1) WHERE
        startNode(actual[i]).chain_id=$rows[i][0] AND endNode(actual[i]).chain_id=$rows[i][1]
        AND size([key IN keys(actual[i]) WHERE NOT key IN $ignored])=size($rows[i][2])
        AND all(j IN range(0,size($rows[i][2])-1) WHERE actual[i][$keys[$rows[i][2][j]]]=$rows[i][3][j])) RETURN true AS ok";
const WRITE:&str="MATCH (n:Entity {org_id:$org,uuid:$uuid})
    SET n.derived_summary=$text,n.summary_revision=$revision,n.summary_as_of=$as_of,n.summary_valid_until=$until,
        n.summary_evidence_hash=$evidence_hash,n.summary_policy_version=$policy,n.summary_evidence_ids=$ids,n.summary_total_evidence=$total,
        n.summary_embedding=$values,n.summary_embedding_model=$model,n.summary_embedding_text_version=$text_version,n.summary_embedding_content_hash=$content_hash
    RETURN true AS ok";

pub fn clear(org: &str, guard: &entity_summary::SummaryEvidenceGuard) -> Vec<PreparedWrite> {
    let mut checks = guarded_checks(org, guard);
    checks.push(PreparedWrite {statement:format!("MATCH (n:Entity {{org_id:$org,uuid:$uuid}}) SET n.uuid=n.uuid REMOVE {} RETURN true AS ok",DERIVED_PROPERTIES.iter().map(|key|format!("n.{key}")).collect::<Vec<_>>().join(",")),parameters:json!({"org":org,"uuid":guard.target_uuid}),expected_rows:1});
    checks
}

/// Resolve old and new endpoints before any mutation changes the graph, then lock in UUID order.
pub fn invalidate(org: &str, mutations: &[GraphMutation]) -> Option<PreparedWrite> {
    use GraphMutation::*;
    let (mut entities, mut chains, mut edges, mut lock_chains) = (
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    for mutation in mutations {
        match mutation {
            // Unresolved-reference records never change summarized content.
            RecordUnresolvedReferences { .. } => {}
            UpsertEntity { uuid, properties } => {
                entities.insert(*uuid);
                if let Some(chain) = properties
                    .get("chain_id")
                    .and_then(|v| v.as_str())
                    .and_then(|v| v.parse::<Uuid>().ok())
                {
                    chains.insert(chain);
                }
            }
            UpdateEntity { uuid, .. }
            | ApplyEntityMetadata { uuid, .. }
            | SupersedeEntity { uuid, .. }
            | ReleaseMembership { uuid, .. } => {
                entities.insert(*uuid);
            }
            ObserveEntity { chain_id, .. } | DeleteEntity { chain_id, .. } => {
                chains.insert(*chain_id);
            }
            RepointEntity {
                previous_uuid,
                new_uuid,
                chain_id,
            } => {
                entities.extend([*previous_uuid, *new_uuid]);
                chains.insert(*chain_id);
            }
            MergeChains {
                loser_chain_id,
                winner_chain_id,
                ..
            } => {
                chains.extend([*loser_chain_id, *winner_chain_id]);
            }
            SplitChain {
                from_chain_id,
                split_chain_id,
                ..
            } => {
                chains.extend([*from_chain_id, *split_chain_id]);
            }
            UpsertEdge {
                uuid,
                source_chain_id,
                target_chain_id,
                ..
            } => {
                edges.insert(*uuid);
                chains.extend([*source_chain_id, *target_chain_id]);
            }
            UpdateEdge { uuid, .. } | CancelEdge { uuid, .. } => {
                edges.insert(*uuid);
            }
            RecordObservation { entity_uuid, .. } => {
                entities.insert(*entity_uuid);
            }
            SetDerivedSummary { guard, .. } | ClearDerivedSummary { guard } => {
                lock_chains.extend(guard.entity_versions.keys().copied());
            }
            AssertCommunityState { .. }
            | BeginCommunityGeneration { .. }
            | StageCommunityPartition { .. }
            | PublishCommunityGeneration { .. }
            | UpdateCommunities { .. }
            | AssociateSagaSnapshot { .. }
            | SetSagaSummary { .. }
            | UpsertSnapshot { .. }
            | SetEmbedding { .. }
            | SetEntityVersionEmbedding { .. }
            | SetRelationshipEmbedding { .. }
            | RecordReferenceDecisions { .. } => {}
        }
    }
    if entities.is_empty() && chains.is_empty() && edges.is_empty() && lock_chains.is_empty() {
        return None;
    }
    let remove = DERIVED_PROPERTIES
        .iter()
        .map(|key| format!("n.{key}"))
        .collect::<Vec<_>>()
        .join(",");
    Some(PreparedWrite {statement:format!("CALL {{ UNWIND $entities AS id MATCH (n:Entity {{org_id:$org,uuid:id}}) RETURN n.chain_id AS chain
        UNION UNWIND $chains AS chain RETURN chain
        UNION UNWIND $edges AS id MATCH (s:Entity {{org_id:$org}})-[r:RELATES_TO {{org_id:$org,uuid:id}}]->(t:Entity {{org_id:$org}}) UNWIND [s.chain_id,t.chain_id] AS chain RETURN chain }}
        WITH collect(DISTINCT chain) AS direct
        CALL (direct) {{ UNWIND direct AS chain OPTIONAL MATCH (n:Entity {{org_id:$org,chain_id:chain}})-[:RELATES_TO {{org_id:$org}}]-(other:Entity {{org_id:$org}}) RETURN collect(DISTINCT other.chain_id) AS neighbors }}
        WITH direct+neighbors AS affected
        UNWIND affected+$lock_chains AS chain MATCH (n:Entity {{org_id:$org,chain_id:chain}})
        WITH DISTINCT n,affected ORDER BY n.uuid
        SET n.uuid=n.uuid
        FOREACH (_ IN CASE WHEN n.chain_id IN affected THEN [1] ELSE [] END | REMOVE {remove})
        WITH count(n) AS locked RETURN true AS ok"),parameters:json!({"org":org,"entities":entities,"chains":chains,"edges":edges,"lock_chains":lock_chains}),expected_rows:1})
}

#[cfg(test)]
mod encoding_tests {
    use super::*;
    use kg_core::traits::relationship_timeline::IncidentVersionState;

    #[test]
    fn incident_encoding_preserves_every_value_and_reduces_repeated_keys() {
        let source = Uuid::new_v4();
        let target = Uuid::new_v4();
        let versions: Vec<_> = (0..100)
            .map(|i| IncidentVersionState {
                source_chain_id: source,
                target_chain_id: target,
                properties: serde_json::from_value(json!({
                    "uuid": Uuid::from_u128(i + 1), "valid_from": "2026-01-01T00:00:00Z",
                    "last_observed_at": "2026-01-02T00:00:00Z", "version": i,
                    "prop_evidence": ["one", "two"], "prop_optional": i % 2 == 0,
                }))
                .unwrap(),
            })
            .collect();
        let (keys, rows) = encode_incident(&versions);
        for (version, row) in versions.iter().zip(&rows) {
            assert_eq!(row[0], json!(version.source_chain_id));
            assert_eq!(row[1], json!(version.target_chain_id));
            let decoded: serde_json::Map<String, serde_json::Value> = row[2]
                .as_array()
                .unwrap()
                .iter()
                .zip(row[3].as_array().unwrap())
                .map(|(index, value)| {
                    (
                        keys[index.as_u64().unwrap() as usize].clone(),
                        value.clone(),
                    )
                })
                .collect();
            assert_eq!(decoded, version.properties);
        }
        assert!(
            serde_json::to_vec(&json!({"keys":keys,"rows":rows}))
                .unwrap()
                .len()
                < serde_json::to_vec(&versions).unwrap().len()
        );
    }
}
