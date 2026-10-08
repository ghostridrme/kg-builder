//! Transaction stripes fence complete Community projections without a tenant mutex.
use crate::{PreparedQuery, PreparedWrite};
use kg_core::traits::GraphMutation;
use serde_json::json;
use std::collections::BTreeSet;
use uuid::Uuid;

#[derive(Default)]
pub struct Targets {
    pub namespaces: BTreeSet<String>,
    pub entities: BTreeSet<Uuid>,
    pub chains: BTreeSet<Uuid>,
    pub edges: BTreeSet<Uuid>,
    pub publications: BTreeSet<String>,
}
impl Targets {
    pub fn has_source(&self) -> bool {
        !(self.namespaces.is_empty()
            && self.entities.is_empty()
            && self.chains.is_empty()
            && self.edges.is_empty())
    }

    pub fn from_mutations(mutations: &[GraphMutation]) -> Self {
        use GraphMutation::*;
        let mut t = Self::default();
        for mutation in mutations {
            match mutation {
                // Unresolved-reference records never touch entity versions or communities.
                RecordUnresolvedReferences { .. } => {}
                UpsertEntity { uuid, properties } | UpdateEntity { uuid, properties } => {
                    t.entities.insert(*uuid);
                    if let Some(ns) = properties.get("namespace").and_then(|v| v.as_str()) {
                        t.namespaces.insert(ns.into());
                    }
                    if let Some(chain) = properties
                        .get("chain_id")
                        .and_then(|v| v.as_str())
                        .and_then(|v| v.parse().ok())
                    {
                        t.chains.insert(chain);
                    }
                }
                ApplyEntityMetadata { uuid, .. }
                | SupersedeEntity { uuid, .. }
                | ReleaseMembership { uuid, .. } => {
                    t.entities.insert(*uuid);
                }
                DeleteEntity { chain_id, .. } | ObserveEntity { chain_id, .. } => {
                    t.chains.insert(*chain_id);
                }
                RecordObservation { entity_uuid, .. } => {
                    t.entities.insert(*entity_uuid);
                }
                RepointEntity {
                    previous_uuid,
                    new_uuid,
                    chain_id,
                } => {
                    t.entities.extend([*previous_uuid, *new_uuid]);
                    t.chains.insert(*chain_id);
                }
                MergeChains {
                    loser_chain_id,
                    winner_chain_id,
                    ..
                } => {
                    t.chains.extend([*loser_chain_id, *winner_chain_id]);
                }
                SplitChain {
                    split_chain_id,
                    from_chain_id,
                    ..
                } => {
                    t.chains.extend([*split_chain_id, *from_chain_id]);
                }
                UpsertEdge {
                    uuid,
                    source_chain_id,
                    target_chain_id,
                    ..
                } => {
                    t.edges.insert(*uuid);
                    t.chains.extend([*source_chain_id, *target_chain_id]);
                }
                UpdateEdge { uuid, .. } | CancelEdge { uuid, .. } => {
                    t.edges.insert(*uuid);
                }
                SetDerivedSummary { guard, .. } | ClearDerivedSummary { guard } => {
                    t.entities.insert(guard.target_uuid);
                }
                AssertCommunityState { state } => {
                    t.publications.insert(state.namespace.clone());
                }
                BeginCommunityGeneration { generation } => {
                    t.publications
                        .insert(generation.expected_state.namespace.clone());
                }
                PublishCommunityGeneration { publication } => {
                    t.publications
                        .insert(publication.expected_state.namespace.clone());
                }
                UpdateCommunities { update } => {
                    t.publications
                        .insert(update.expected_state.namespace.clone());
                }
                StageCommunityPartition { .. }
                | AssociateSagaSnapshot { .. }
                | SetSagaSummary { .. }
                | UpsertSnapshot { .. }
                | SetEmbedding { .. }
                | SetEntityVersionEmbedding { .. }
                | SetRelationshipEmbedding { .. }
                | RecordReferenceDecisions { .. } => {}
            }
        }
        t
    }
    fn params(&self, org: &str) -> serde_json::Value {
        json!({"org":org,"entities":self.entities,"chains":self.chains,"edges":self.edges})
    }
}
// Mirrors the actual derived-summary invalidation neighborhood, including old endpoints.
const AFFECTED:&str="CALL { UNWIND $entities AS id MATCH (n:Entity {org_id:$org,uuid:id}) RETURN n.chain_id AS chain
 UNION UNWIND $chains AS chain RETURN chain
 UNION UNWIND $edges AS id MATCH (s:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,uuid:id}]->(t:Entity {org_id:$org}) UNWIND [s.chain_id,t.chain_id] AS chain RETURN chain }
 WITH collect(DISTINCT chain) AS direct
 CALL (direct) { UNWIND direct AS chain OPTIONAL MATCH (n:Entity {org_id:$org,chain_id:chain})-[:RELATES_TO {org_id:$org}]-(other:Entity {org_id:$org}) RETURN collect(DISTINCT other.chain_id) AS neighbors }
 UNWIND direct+neighbors AS chain MATCH (n:Entity {org_id:$org,chain_id:chain}) WITH DISTINCT n";

pub fn discover(org: &str, targets: &Targets) -> PreparedQuery {
    PreparedQuery {
        statement: format!(
            "{AFFECTED} RETURN DISTINCT n.namespace AS namespace ORDER BY namespace"
        ),
        parameters: targets.params(org),
    }
}
pub fn dirty(org: &str, targets: &Targets) -> PreparedWrite {
    PreparedWrite{statement:format!("{AFFECTED} MATCH (c:Community {{org_id:$org}})-[:HAS_MEMBER]->(n) MATCH (s:CommunityScope {{org_id:$org,namespace:c.namespace}}) WHERE s.active_generation=c.generation_uuid WITH DISTINCT c ORDER BY c.uuid SET c.uuid=c.uuid,c.dirty=true WITH count(c) AS affected RETURN true AS ok"),parameters:targets.params(org),expected_rows:1}
}
pub fn locks(org: &str, locks: &[(String, usize)]) -> PreparedWrite {
    PreparedWrite{statement:"UNWIND $locks AS item WITH item ORDER BY item.namespace,item.stripe MERGE (r:CommunityRevision {org_id:$org,namespace:item.namespace,stripe:item.stripe}) ON CREATE SET r.revision=0 SET r.stripe=r.stripe WITH count(r) AS locked WHERE locked=size($locks) RETURN true AS ok".into(),parameters:json!({"org":org,"locks":locks.iter().map(|(namespace,stripe)|json!({"namespace":namespace,"stripe":stripe})).collect::<Vec<_>>()}),expected_rows:1}
}
pub fn advance(org: &str, namespace: &str, stripe: usize) -> PreparedWrite {
    PreparedWrite{statement:"MATCH (r:CommunityRevision {org_id:$org,namespace:$namespace,stripe:$stripe}) WHERE r.revision<9223372036854775807 SET r.revision=r.revision+1 RETURN true AS ok".into(),parameters:json!({"org":org,"namespace":namespace,"stripe":stripe}),expected_rows:1}
}
pub fn stripe(transaction_key: &[u8]) -> usize {
    (xxhash_rust::xxh3::xxh3_64(transaction_key) % kg_core::community::REVISION_STRIPES as u64)
        as usize
}
