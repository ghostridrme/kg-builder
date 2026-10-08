//! Sorted revision locks protect atomically published entity changes.

use crate::{PreparedQuery, PreparedWrite};
use kg_core::{
    errors::BackendError,
    traits::{GraphMutation, IdentityRevision, IdentityScope},
};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use uuid::Uuid;

pub fn read(org: &str, scopes: &[IdentityScope]) -> Result<PreparedQuery, BackendError> {
    if org.trim().is_empty() || scopes.len() > kg_core::traits::graph_reads::MAX_LOOKUP_KEYS {
        return Err(BackendError::Query(
            "invalid identity revision request".into(),
        ));
    }
    let rows = scopes.iter().map(|scope| Ok(json!({"key":scope.key(org)?,"namespace":scope.namespace,"entity_type":scope.entity_type}))).collect::<Result<Vec<_>,BackendError>>()?;
    Ok(PreparedQuery {
        statement: "UNWIND $scopes AS scope OPTIONAL MATCH (r:IdentityRevision {scope_id:scope.key}) RETURN scope.namespace AS namespace,scope.entity_type AS entity_type,coalesce(r.revision,0) AS revision".into(),
        parameters: json!({"scopes":rows}),
    })
}

pub fn decode(row: &Map<String, Value>) -> Result<IdentityRevision, BackendError> {
    let value = IdentityRevision {
        scope: decode_scope(row)?,
        revision: row
            .get("revision")
            .and_then(Value::as_u64)
            .ok_or_else(|| BackendError::Deserialization("invalid identity revision".into()))?,
    };
    value.validate()?;
    Ok(value)
}

pub fn decode_scope(row: &Map<String, Value>) -> Result<IdentityScope, BackendError> {
    let text = |key| {
        row.get(key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| BackendError::Deserialization("invalid stored identity scope".into()))
    };
    let scope = IdentityScope {
        namespace: text("namespace")?,
        entity_type: text("entity_type")?,
    };
    scope.validate()?;
    Ok(scope)
}

pub fn lock(org: &str, scope: &IdentityScope) -> Result<PreparedWrite, BackendError> {
    Ok(PreparedWrite {
        statement: "MERGE (r:IdentityRevision {scope_id:$key}) ON CREATE SET r.org_id=$org,r.namespace=$namespace,r.entity_type=$entity_type,r.revision=0 SET r.revision=r.revision RETURN true AS ok".into(),
        parameters:json!({"key":scope.key(org)?,"org":org,"namespace":scope.namespace,"entity_type":scope.entity_type}), expected_rows:1,
    })
}

pub fn advance(org: &str, scope: &IdentityScope) -> Result<PreparedWrite, BackendError> {
    Ok(PreparedWrite { statement:"MATCH (r:IdentityRevision {scope_id:$key}) WHERE r.revision < 9223372036854775806 SET r.revision=r.revision+1 RETURN true AS ok".into(), parameters:json!({"key":scope.key(org)?}), expected_rows:1 })
}

pub fn check(org: &str, revision: &IdentityRevision) -> Result<PreparedWrite, BackendError> {
    revision.validate()?;
    Ok(PreparedWrite {statement:"MATCH (r:IdentityRevision {scope_id:$key}) WHERE r.revision=$revision RETURN true AS ok".into(),parameters:json!({"key":revision.scope.key(org)?,"revision":revision.revision}),expected_rows:1})
}

/// UUID and chain seeks also include historical versions affected by merge/split.
pub fn existing_scopes(org: &str, uuids: &[Uuid], chains: &[Uuid]) -> PreparedQuery {
    PreparedQuery {statement:"CALL { UNWIND $uuids AS id MATCH (n:Entity {org_id:$org,uuid:id}) RETURN n.namespace AS namespace,n.entity_type AS entity_type UNION UNWIND $chains AS id MATCH (n:Entity {org_id:$org,chain_id:id}) RETURN n.namespace AS namespace,n.entity_type AS entity_type } RETURN DISTINCT namespace,entity_type".into(),parameters:json!({"org":org,"uuids":uuids,"chains":chains})}
}

pub struct MutationScopes {
    pub declared: BTreeSet<IdentityScope>,
    pub uuids: Vec<Uuid>,
    pub chains: Vec<Uuid>,
}

pub fn add_revision_scopes(scopes: &mut BTreeSet<IdentityScope>, scope: IdentityScope) {
    scopes.insert(IdentityScope {
        namespace: scope.namespace.clone(),
        entity_type: "*".into(),
    });
    scopes.insert(IdentityScope {
        namespace: "*".into(),
        entity_type: scope.entity_type.clone(),
    });
    scopes.insert(IdentityScope {
        namespace: "*".into(),
        entity_type: "*".into(),
    });
    scopes.insert(scope);
}

pub fn mutation_scopes(
    org: &str,
    mutations: &[GraphMutation],
) -> Result<MutationScopes, BackendError> {
    use GraphMutation::*;
    let mut scopes = BTreeSet::new();
    let mut uuids = BTreeSet::new();
    let mut chains = BTreeSet::new();
    for mutation in mutations {
        mutation.validate(org)?;
        match mutation {
            // Unresolved-reference records are keyed by source chain and take no identity lock.
            RecordUnresolvedReferences { .. } => {}
            SetDerivedSummary { guard, .. } | ClearDerivedSummary { guard } => {
                chains.extend(guard.entity_versions.keys().copied());
                uuids.insert(guard.target_uuid);
            }
            UpsertEntity { uuid, properties } => {
                add_revision_scopes(&mut scopes, decode_scope(properties)?);
                uuids.insert(*uuid);
            }
            UpdateEntity { uuid, .. }
            | ApplyEntityMetadata { uuid, .. }
            | ReleaseMembership { uuid, .. }
            | SetEmbedding { uuid, .. }
            | SetEntityVersionEmbedding { uuid, .. }
            | SupersedeEntity { uuid, .. } => {
                uuids.insert(*uuid);
            }
            DeleteEntity { chain_id, .. } | ObserveEntity { chain_id, .. } => {
                chains.insert(*chain_id);
            }
            RepointEntity {
                previous_uuid,
                new_uuid,
                ..
            } => {
                uuids.extend([*previous_uuid, *new_uuid]);
            }
            MergeChains {
                loser_chain_id,
                winner_chain_id,
                ..
            } => {
                chains.extend([*loser_chain_id, *winner_chain_id]);
            }
            SplitChain {
                split_chain_id,
                from_chain_id,
                ..
            } => {
                chains.extend([*split_chain_id, *from_chain_id]);
            }
            AssertCommunityState { .. }
            | BeginCommunityGeneration { .. }
            | StageCommunityPartition { .. }
            | PublishCommunityGeneration { .. }
            | UpdateCommunities { .. }
            | AssociateSagaSnapshot { .. }
            | SetSagaSummary { .. }
            | UpsertSnapshot { .. }
            | UpsertEdge { .. }
            | UpdateEdge { .. }
            | CancelEdge { .. }
            | RecordObservation { .. }
            | SetRelationshipEmbedding { .. }
            | RecordReferenceDecisions { .. } => {}
        }
    }
    Ok(MutationScopes {
        declared: scopes,
        uuids: uuids.into_iter().collect(),
        chains: chains.into_iter().collect(),
    })
}
