//! Typed ingestion reads: parameterized statements and record decoding.
//! Rows may carry the node or relationship under a column (`n`, `r`) and its
//! properties flattened at the top level; decoding accepts both shapes.

use crate::PreparedQuery;
use chrono::{DateTime, Utc};
use kg_core::{
    errors::BackendError,
    models::{CollectionMembership, CollectionRef, GENERATIONS_PROPERTY, MEMBERS_PROPERTY},
    traits::{
        graph_backend::GraphEmbedding, graph_reads::MAX_CARRIERS_PER_IDENTIFYING_VALUE, EdgeLookup,
        EdgeRecord, EntityLookup, EntityVersionRecord, VersionState,
    },
};
use serde_json::{json, Map, Value};
use uuid::Uuid;

/// Bolt integers are signed; larger generations compare as the ceiling.
pub fn bolt_generation(generation: u64) -> i64 {
    i64::try_from(generation).unwrap_or(i64::MAX)
}

/// Prepare an organization-scoped entity read.
pub fn entities(org: &str, lookup: &EntityLookup) -> Result<PreparedQuery, BackendError> {
    lookup.validate(org)?;
    let (statement, parameters) = match lookup {
        EntityLookup::ByIdentity {
            hashes,
            state: VersionState::Live,
        } => (LIVE_BY_IDENTITY, json!({"org_id": org, "hashes": hashes})),
        EntityLookup::ByIdentity {
            hashes,
            state: VersionState::Deleted,
        } => (
            DELETED_BY_IDENTITY,
            json!({"org_id": org, "hashes": hashes}),
        ),
        EntityLookup::LatestByChain { chain_ids } => (
            LATEST_BY_CHAIN,
            json!({"org_id": org, "chain_ids": uuids(chain_ids)}),
        ),
        EntityLookup::LiveByName { names } => {
            (LIVE_BY_NAME, json!({"org_id": org, "values": names}))
        }
        EntityLookup::LiveByIdentifyingValue { values } => (
            LIVE_BY_IDENTIFYING_VALUE,
            json!({"org_id": org, "values": values, "carriers": MAX_CARRIERS_PER_IDENTIFYING_VALUE}),
        ),
        EntityLookup::LiveByKeyValue { wanted } => (
            LIVE_BY_KEY_VALUE,
            json!({"org_id": org, "wanted": wanted, "carriers": MAX_CARRIERS_PER_IDENTIFYING_VALUE + 1}),
        ),
        EntityLookup::LiveReferenceSources { after_chain, limit } => (
            "MATCH (n:Entity {org_id:$org_id,is_latest:true}) WHERE n.deleted_at IS NULL AND n.merged_into IS NULL AND ($after IS NULL OR n.chain_id>$after) RETURN n ORDER BY n.chain_id LIMIT $limit",
            json!({"org_id":org,"after":after_chain.map(|id| id.to_string()),"limit":limit}),
        ),
        EntityLookup::LiveMissingKeyValues { after_chain, limit } => (
            LIVE_MISSING_KEY_VALUES,
            json!({"org_id": org, "after": after_chain.map(|c| c.to_string()), "limit": limit}),
        ),
        EntityLookup::VersionsByChain { chain_ids } => (
            VERSIONS_BY_CHAIN,
            json!({"org_id": org, "chain_ids": uuids(chain_ids)}),
        ),
        EntityLookup::StaleInCollection {
            collection,
            before_generation,
        } => (
            STALE_ENTITIES_IN_COLLECTION,
            json!({
                "org_id": org,
                "member": collection.member_id(),
                "sync_generation": bolt_generation(*before_generation),
            }),
        ),
    };
    Ok(PreparedQuery {
        statement: statement.into(),
        parameters,
    })
}

/// Prepare an organization-scoped relationship read.
pub fn edges(org: &str, lookup: &EdgeLookup) -> Result<PreparedQuery, BackendError> {
    lookup.validate(org)?;
    let (statement, parameters) = match lookup {
        EdgeLookup::LiveByChainPairs { pairs } | EdgeLookup::HeadsByChainPairs { pairs } => (
            if matches!(lookup, EdgeLookup::HeadsByChainPairs { .. }) {
                EDGE_HEADS_BY_PAIRS
            } else {
                LIVE_EDGES_BY_PAIRS
            },
            json!({
                "org_id": org,
                "pairs": pairs
                    .iter()
                    .map(|(s, t)| json!({"source": s.to_string(), "target": t.to_string()}))
                    .collect::<Vec<_>>(),
            }),
        ),
        EdgeLookup::VersionsByEndpointChains { chain_ids } => (
            RELATIONSHIP_VERSIONS_BY_ENDPOINT_CHAINS,
            json!({"org_id":org,"chain_ids":uuids(chain_ids),"limit":kg_core::traits::relationship_timeline::MAX_VERSIONS + 1}),
        ),
        EdgeLookup::VersionsByRelations { relations } => (
            RELATIONSHIP_VERSIONS_BY_RELATIONS,
            json!({"org_id":org,"relations":relations.iter().map(|(source,name)|json!({"source":source,"name":name})).collect::<Vec<_>>(), "limit": kg_core::traits::relationship_timeline::MAX_VERSIONS + 1}),
        ),
        EdgeLookup::VersionsByReferenceOwners { owners } => (
            RELATIONSHIP_VERSIONS_BY_REFERENCE_OWNERS,
            json!({"org_id":org,"owners":owners.iter().map(|owner|json!({"chain":owner.chain_id,"namespace":owner.namespace,"slot":owner.slot})).collect::<Vec<_>>(), "limit": kg_core::traits::relationship_timeline::MAX_VERSIONS + 1}),
        ),
        EdgeLookup::VersionsByChainPairs { pairs } => (
            RELATIONSHIP_VERSIONS_BY_PAIRS,
            json!({"org_id":org,"pairs":pairs.iter().map(|(s,t)|json!({"source":s,"target":t})).collect::<Vec<_>>(), "limit": kg_core::traits::relationship_timeline::MAX_VERSIONS + 1}),
        ),
        EdgeLookup::LiveByEndpointChains { chain_ids } => (
            LIVE_EDGES_BY_ENDPOINT_CHAINS,
            json!({"org_id": org, "chain_ids": uuids(chain_ids)}),
        ),
        EdgeLookup::LiveBySourceScope { namespace, source } => (
            LIVE_EDGES_BY_SOURCE_SCOPE,
            json!({
                "org_id": org,
                "namespace": namespace,
                "source": source,
            }),
        ),
        EdgeLookup::ScheduledStaleInCollection {
            collection,
            before_generation,
            effective_at,
        } => (
            SCHEDULED_STALE_EDGES_IN_COLLECTION,
            json!({"org_id":org,"namespace":collection.namespace,"source":collection.source,
                "member":collection.member_id(),"sync_generation":bolt_generation(*before_generation),
                "effective_at":effective_at.to_rfc3339(),"limit":kg_core::traits::relationship_timeline::MAX_VERSIONS+1}),
        ),
        EdgeLookup::StaleInCollection {
            collection,
            before_generation,
        } => (
            STALE_EDGES_IN_COLLECTION,
            json!({
                "org_id": org,
                "namespace": collection.namespace,
                "source": collection.source,
                "member": collection.member_id(),
                "sync_generation": bolt_generation(*before_generation),
            }),
        ),
    };
    Ok(PreparedQuery {
        statement: statement.into(),
        parameters,
    })
}

fn uuids(ids: &[Uuid]) -> Vec<String> {
    ids.iter().map(Uuid::to_string).collect()
}

/// Matches primary identities and accumulated aliases on live versions.
const LIVE_BY_IDENTITY: &str = "UNWIND $hashes AS hash MATCH (:IdentityKey {org_id:$org_id,hash:hash})-[:IDENTIFIES]->(n:Entity {org_id:$org_id}) WHERE n.is_latest = true AND n.deleted_at IS NULL RETURN DISTINCT n";

/// Newest deleted version first; callers choose one candidate per identity.
const DELETED_BY_IDENTITY: &str = "UNWIND $hashes AS hash MATCH (:IdentityKey {org_id:$org_id,hash:hash})-[:IDENTIFIES]->(n:Entity {org_id:$org_id}) WHERE n.deleted_at IS NOT NULL RETURN DISTINCT n ORDER BY n.version DESC";

const LATEST_BY_CHAIN: &str = "UNWIND $chain_ids AS chain_id MATCH (n:Entity {chain_id: chain_id, org_id: $org_id, is_latest: true}) WHERE n.deleted_at IS NULL RETURN n";

/// Matches names only; callers enforce namespace policy and reject ambiguous targets.
const LIVE_BY_NAME: &str = "MATCH (n:Entity {org_id: $org_id, is_latest: true}) WHERE n.deleted_at IS NULL AND toLower(toStringOrNull(n.name)) IN $values RETURN n";

/// Matches a name or the value of a single-property primary key. The scan is
/// bounded to the organization's live latest versions through the
/// `(org_id, is_latest)` index; the case-insensitive comparison itself
/// cannot use an index, so one read per snapshot carries every wanted value.
/// The result is bounded to `$carriers` entities per matched value, in
/// chain order: a widely shared value is ambiguous after two carriers.
const LIVE_BY_IDENTIFYING_VALUE: &str = "MATCH (n:Entity {org_id: $org_id, is_latest: true}) WHERE n.deleted_at IS NULL WITH n, CASE WHEN n.primary_key_properties IS :: LIST<STRING NOT NULL> THEN n.primary_key_properties ELSE [] END AS primary_keys WITH n, [v IN [toLower(toStringOrNull(n.name)), CASE WHEN size(primary_keys) = 1 AND primary_keys[0] <> 'name' AND n['property_type_' + primary_keys[0]] = 's' THEN toLower(toStringOrNull(n['prop_' + primary_keys[0]])) ELSE null END] WHERE v IS NOT NULL AND v IN $values] AS matched WHERE size(matched) > 0 UNWIND matched AS value WITH value, n ORDER BY n.chain_id WITH value, collect(n)[0..$carriers] AS carriers UNWIND carriers AS n RETURN DISTINCT n";

/// Exact typed key-value tokens over every key component (`key_values`, written
/// by the node planner from the same canonicalization as the target index), so
/// composite parts and integer keys are found and nothing is lowercased. Nodes
/// written before `key_values` existed match nothing here until backfilled.
/// Bounded to `$carriers` per matched value in chain order; the caller detects
/// overflow from the extra carrier.
const LIVE_BY_KEY_VALUE: &str = "UNWIND $wanted AS w
    CALL {
        WITH w
        CALL {
            WITH w
            MATCH (value:IdentityValue {org_id:$org_id,token:w.token})
            RETURN value
            UNION
            WITH w
            WITH w WHERE w.case_insensitive_string = true AND w.token STARTS WITH 's:'
            MATCH (value:IdentityValue {org_id:$org_id,folded_token:toLower(w.token)})
            RETURN value
        }
        MATCH (value)-[:KEY_COMPONENT]->(n:Entity {org_id:$org_id,is_latest:true})
        WHERE n.deleted_at IS NULL AND n.merged_into IS NULL
            AND (w.namespaces IS NULL OR n.namespace IN w.namespaces)
            AND (w.target_types IS NULL OR n.entity_type IN w.target_types)
        RETURN DISTINCT n ORDER BY n.chain_id LIMIT $carriers
    }
    RETURN DISTINCT n";

/// Live latest versions still lacking `key_values`, paged in chain order for the
/// awaited backfill. Chain ids are stored as strings, so string order is stable.
const LIVE_MISSING_KEY_VALUES: &str = "MATCH (n:Entity {org_id: $org_id, is_latest: true}) WHERE n.deleted_at IS NULL AND (n.key_values IS NULL OR any(token IN n.key_values WHERE NOT EXISTS { MATCH (:IdentityValue {org_id:n.org_id,token:token})-[:KEY_COMPONENT]->(n) })) AND ($after IS NULL OR n.chain_id > $after) RETURN n ORDER BY n.chain_id LIMIT $limit";

/// Includes superseded and deleted versions, oldest first within a chain.
const VERSIONS_BY_CHAIN: &str = "UNWIND $chain_ids AS chain_id MATCH (n:Entity {chain_id: chain_id, org_id: $org_id}) RETURN n ORDER BY n.chain_id, n.version";

/// Members of the collection whose membership generation is older than the
/// scan; `collection_members` and `collection_generations` are index aligned.
/// Stable chain order keeps reconciliation deterministic across retries.
const STALE_ENTITIES_IN_COLLECTION: &str = "MATCH (n:Entity {org_id: $org_id}) WHERE n.is_latest = true AND n.deleted_at IS NULL AND $member IN coalesce(n.collection_members, []) AND any(i IN range(0, size(n.collection_members) - 1) WHERE n.collection_members[i] = $member AND coalesce(n.collection_generations[i], -1) < $sync_generation) RETURN n ORDER BY n.chain_id";

// Historical versions may remain attached to superseded endpoints.
const RELATIONSHIP_VERSIONS_BY_ENDPOINT_CHAINS: &str = "CALL {
    UNWIND $chain_ids AS chain
    MATCH (s:Entity {org_id:$org_id,chain_id:chain})-[r:RELATES_TO {org_id:$org_id}]->(t:Entity {org_id:$org_id})
    RETURN s,r,t
    UNION
    UNWIND $chain_ids AS chain
    MATCH (s:Entity {org_id:$org_id})-[r:RELATES_TO {org_id:$org_id}]->(t:Entity {org_id:$org_id,chain_id:chain})
    RETURN s,r,t
    }
    RETURN r{.*,embedding:null,embedding_model:null,embedding_text_version:null,embedding_content_hash:null} AS r,
        s.chain_id AS source_chain_id,t.chain_id AS target_chain_id
    ORDER BY r.uuid LIMIT $limit";

const RELATIONSHIP_VERSIONS_BY_RELATIONS: &str = "UNWIND $relations AS relation
    MATCH (s:Entity {org_id:$org_id,chain_id:relation.source})-[r:RELATES_TO {org_id:$org_id,name:relation.name}]->(t:Entity {org_id:$org_id})
    RETURN r{.*,embedding:null,embedding_model:null,embedding_text_version:null,embedding_content_hash:null} AS r,
        s.chain_id AS source_chain_id,t.chain_id AS target_chain_id
    ORDER BY r.uuid LIMIT $limit";

const RELATIONSHIP_VERSIONS_BY_REFERENCE_OWNERS: &str = "UNWIND $owners AS owner
    MATCH (s:Entity {org_id:$org_id})-[r:RELATES_TO {org_id:$org_id,reference_owner_chain_id:owner.chain,reference_owner_namespace:owner.namespace,reference_slot:owner.slot}]->(t:Entity {org_id:$org_id})
    RETURN r{.*,embedding:null,embedding_model:null,embedding_text_version:null,embedding_content_hash:null} AS r,
        s.chain_id AS source_chain_id,t.chain_id AS target_chain_id
    ORDER BY r.uuid LIMIT $limit";

const RELATIONSHIP_VERSIONS_BY_PAIRS: &str = "UNWIND $pairs AS pair
    MATCH (s:Entity {org_id:$org_id,chain_id:pair.source})-[r:RELATES_TO {org_id:$org_id}]->(t:Entity {org_id:$org_id,chain_id:pair.target})
    RETURN r{.*,embedding:null,embedding_model:null,embedding_text_version:null,embedding_content_hash:null} AS r,
        s.chain_id AS source_chain_id,t.chain_id AS target_chain_id
    ORDER BY r.uuid LIMIT $limit";

const LIVE_EDGES_BY_PAIRS: &str = "UNWIND $pairs AS pair MATCH (s:Entity {org_id: $org_id, is_latest: true})-[r:RELATES_TO]->(t:Entity {org_id: $org_id, is_latest: true}) WHERE s.chain_id = pair.source AND t.chain_id = pair.target AND s.deleted_at IS NULL AND t.deleted_at IS NULL AND r.org_id=$org_id AND r.is_latest = true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL RETURN r, s.chain_id AS source_chain_id, t.chain_id AS target_chain_id";

// Historical relationship versions may still attach to superseded endpoint versions.
const EDGE_HEADS_BY_PAIRS: &str = "UNWIND $pairs AS pair
    MATCH (s:Entity {org_id:$org_id,chain_id:pair.source,is_latest:true}), (t:Entity {org_id:$org_id,chain_id:pair.target,is_latest:true})
    WHERE s.deleted_at IS NULL AND t.deleted_at IS NULL
    MATCH (old_s:Entity {org_id:$org_id,chain_id:pair.source})-[r:RELATES_TO {org_id:$org_id}]->(old_t:Entity {org_id:$org_id,chain_id:pair.target})
    WITH s,t,r.chain_id AS chain,collect(r) AS versions,max(r.version) AS newest
    UNWIND versions AS r
    WITH s,t,r,newest WHERE r.version=newest OR (r.is_latest=true AND r.cancelled_at IS NULL AND r.valid_to IS NULL AND r.invalid_at IS NULL AND r.deleted_at IS NULL)
    RETURN r,s.chain_id AS source_chain_id,t.chain_id AS target_chain_id,s.collection_members AS source_collections ORDER BY r.uuid";

const LIVE_EDGES_BY_ENDPOINT_CHAINS: &str = "CALL {
    UNWIND $chain_ids AS chain
    MATCH (s:Entity {org_id:$org_id,chain_id:chain,is_latest:true})-[r:RELATES_TO {org_id:$org_id}]->(t:Entity {org_id:$org_id,is_latest:true})
    WHERE s.deleted_at IS NULL AND t.deleted_at IS NULL AND r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
    RETURN s,r,t
    UNION
    UNWIND $chain_ids AS chain
    MATCH (s:Entity {org_id:$org_id,is_latest:true})-[r:RELATES_TO {org_id:$org_id}]->(t:Entity {org_id:$org_id,chain_id:chain,is_latest:true})
    WHERE s.deleted_at IS NULL AND t.deleted_at IS NULL AND r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
    RETURN s,r,t
    }
    RETURN r,s.chain_id AS source_chain_id,t.chain_id AS target_chain_id,s.collection_members AS source_collections ORDER BY r.uuid";

/// Every live relationship of a connector scope, for supersession.
const LIVE_EDGES_BY_SOURCE_SCOPE: &str = "MATCH (s:Entity {org_id: $org_id, is_latest: true})-[r:RELATES_TO {producer_namespace:$namespace,producer_source:$source}]->(t:Entity {org_id:$org_id,is_latest:true}) WHERE s.deleted_at IS NULL AND t.deleted_at IS NULL AND r.org_id=$org_id AND r.is_latest = true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL RETURN r, s.chain_id AS source_chain_id, t.chain_id AS target_chain_id, s.collection_members AS source_collections ORDER BY r.uuid";

/// Live relationships whose source entity belongs to the collection and whose
/// generation predates the scan; stable uuid order preserves retry identifiers.
const SCHEDULED_STALE_EDGES_IN_COLLECTION: &str = "MATCH (s:Entity {org_id:$org_id,is_latest:true}) WHERE s.deleted_at IS NULL AND $member IN coalesce(s.collection_members,[]) MATCH (old_s:Entity {org_id:$org_id,chain_id:s.chain_id})-[r:RELATES_TO {org_id:$org_id,producer_namespace:$namespace,producer_source:$source}]->(old_t:Entity {org_id:$org_id}) MATCH (t:Entity {org_id:$org_id,chain_id:old_t.chain_id,is_latest:true}) WHERE t.deleted_at IS NULL AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND (r.valid_to IS NULL OR datetime(r.valid_to)>datetime($effective_at)) AND (r.invalid_at IS NULL OR datetime(r.invalid_at)>datetime($effective_at)) AND (r.sync_generation IS NULL OR r.sync_generation<$sync_generation) RETURN r,s.chain_id AS source_chain_id,t.chain_id AS target_chain_id,s.collection_members AS source_collections ORDER BY r.uuid LIMIT $limit";

const STALE_EDGES_IN_COLLECTION: &str = "MATCH (s:Entity {org_id: $org_id, is_latest: true})-[r:RELATES_TO {producer_namespace:$namespace,producer_source:$source}]->(t:Entity {org_id:$org_id,is_latest:true}) WHERE s.deleted_at IS NULL AND t.deleted_at IS NULL AND r.org_id=$org_id AND $member IN coalesce(s.collection_members, []) AND r.is_latest = true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL AND (r.sync_generation IS NULL OR r.sync_generation < $sync_generation) RETURN r, s.chain_id AS source_chain_id, t.chain_id AS target_chain_id, s.collection_members AS source_collections ORDER BY r.uuid";

fn text<'a>(p: &'a Map<String, Value>, k: &str) -> &'a str {
    p.get(k).and_then(Value::as_str).unwrap_or("")
}

fn optional(p: &Map<String, Value>, k: &str) -> Option<String> {
    p.get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn id(p: &Map<String, Value>, k: &str) -> Result<Uuid, BackendError> {
    Uuid::parse_str(text(p, k))
        .map_err(|_| BackendError::Deserialization(format!("invalid {k} in graph record")))
}

fn optional_id(p: &Map<String, Value>, k: &str) -> Option<Uuid> {
    p.get(k)
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
}

fn time(p: &Map<String, Value>, k: &str) -> Result<Option<DateTime<Utc>>, BackendError> {
    match p.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => DateTime::parse_from_rfc3339(s)
            .map(|t| Some(t.with_timezone(&Utc)))
            .map_err(|_| BackendError::Deserialization(format!("invalid {k} in graph record"))),
        Some(_) => Err(BackendError::Deserialization(format!(
            "invalid {k} in graph record"
        ))),
    }
}

fn version(p: &Map<String, Value>) -> u32 {
    p.get("version")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(1)
}

fn generation(p: &Map<String, Value>) -> Option<u64> {
    p.get("sync_generation").and_then(Value::as_u64)
}

fn member_ids(value: Option<&Value>) -> Vec<CollectionRef> {
    value
        .and_then(Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .filter_map(CollectionRef::parse_member_id)
                .collect()
        })
        .unwrap_or_default()
}

/// Memberships stored on a node, in the shared aligned-list encoding.
pub fn memberships(p: &Map<String, Value>) -> Vec<CollectionMembership> {
    CollectionMembership::from_properties(p.get(MEMBERS_PROPERTY), p.get(GENERATIONS_PROPERTY))
}

/// The aligned list properties that store a membership set.
pub fn membership_properties(memberships: &[CollectionMembership]) -> (Value, Value) {
    CollectionMembership::to_properties(memberships)
}

/// Prefer the record column when present; a flattened row is the fallback.
fn record(mut row: Map<String, Value>, column: &str) -> Map<String, Value> {
    match row.remove(column) {
        Some(Value::Object(props)) => props,
        _ => row,
    }
}

/// Decode one entity version row. Missing bookkeeping defaults to version 1,
/// not latest, and no source properties; identifiers must be valid.
pub fn decode_entity_version(row: Map<String, Value>) -> Result<EntityVersionRecord, BackendError> {
    let stored = record(row, "n");
    let embedding = match (
        stored.get("embedding").and_then(Value::as_array),
        optional(&stored, "embedding_model"),
    ) {
        (Some(values), Some(model)) => {
            let values: Vec<f32> = values
                .iter()
                .map(|v| v.as_f64().map(|f| f as f32))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default();
            let candidate = GraphEmbedding { model, values };
            candidate.validate().ok().map(|_| candidate)
        }
        _ => None,
    };
    Ok(EntityVersionRecord {
        uuid: id(&stored, "uuid")?,
        chain_id: id(&stored, "chain_id")?,
        version: version(&stored),
        is_latest: stored
            .get("is_latest")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        entity_type: text(&stored, "entity_type").into(),
        name: text(&stored, "name").into(),
        namespace: text(&stored, "namespace").into(),
        source: optional(&stored, "source"),
        identity_hash: optional(&stored, "identity_hash"),
        identity_hashes: stored
            .get("identity_hashes")
            .and_then(Value::as_array)
            .map(|aliases| {
                aliases
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        structural_hash: stored.get("structural_hash").and_then(|v| match v {
            Value::String(s) if !s.is_empty() => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }),
        valid_from: time(&stored, "valid_from")?,
        valid_to: time(&stored, "valid_to")?,
        deleted_at: time(&stored, "deleted_at")?,
        last_seen_at: time(&stored, "last_seen_at")?,
        last_transition_at: time(&stored, "last_transition_at")?,
        sync_generation: generation(&stored),
        collections: memberships(&stored),
        merged_into: optional_id(&stored, "merged_into"),
        embedding,
        stored,
    })
}

/// Decode one relationship row. Endpoint chains come from explicit columns
/// when present, otherwise from the relationship's own stored chain properties.
pub fn decode_edge(row: Map<String, Value>) -> Result<EdgeRecord, BackendError> {
    let columns = (
        optional_id(&row, "source_chain_id"),
        optional_id(&row, "target_chain_id"),
    );
    let source_collections = member_ids(row.get("source_collections"));
    let stored = record(row, "r");
    let source_chain_id = columns
        .0
        .or_else(|| optional_id(&stored, "source_chain_id"))
        .ok_or_else(|| BackendError::Deserialization("edge record lacks a source chain".into()))?;
    let target_chain_id = columns
        .1
        .or_else(|| optional_id(&stored, "target_chain_id"))
        .ok_or_else(|| BackendError::Deserialization("edge record lacks a target chain".into()))?;
    Ok(EdgeRecord {
        uuid: id(&stored, "uuid")?,
        source_chain_id,
        target_chain_id,
        name: text(&stored, "name").into(),
        version: version(&stored),
        is_latest: stored
            .get("is_latest")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        confidence: stored
            .get("confidence")
            .and_then(Value::as_f64)
            .unwrap_or(0.0) as f32,
        valid_from: time(&stored, "valid_from")?,
        invalid_at: time(&stored, "invalid_at")?,
        sync_generation: generation(&stored),
        source_collections,
        stored,
    })
}

/// Return bounded evidence only; oversized/missing records fail the complete lookup.
pub fn snapshot_evidence(
    org: &str,
    request: &kg_core::runtime::history::SnapshotEvidenceRequest,
) -> Result<PreparedQuery, BackendError> {
    request.validate(org)?;
    Ok(PreparedQuery {
        statement: "UNWIND $ids AS id
            MATCH (s:Snapshot {uuid: id})
            USING INDEX s:Snapshot(uuid)
            WHERE s.org_id = $org_id AND s.namespace = $namespace
              AND datetime(s.captured_at) <= datetime($cutoff) AND s.content IS NOT NULL
            WITH s, 4 * (size(s.content) + size(coalesce(s.source_description, ''))
                + size(s.source) + size(s.org_id) + size(s.namespace) + size(s.data_type)
                + size(s.captured_at) + size(s.created_at)) + 256 AS bytes_upper_bound
            WITH collect(s) AS nodes, sum(bytes_upper_bound) AS bytes_upper_bound
            WHERE bytes_upper_bound <= $max_bytes
            UNWIND nodes AS s
            RETURN s.uuid AS uuid, s.org_id AS org_id, s.namespace AS namespace,
                s.source AS source, s.data_type AS data_type,
                s.source_description AS source_description, s.captured_at AS captured_at,
                s.created_at AS created_at, s.content AS content"
            .into(),
        parameters: json!({"org_id": org, "namespace": request.namespace, "ids": uuids(&request.ids), "cutoff": request.captured_before.to_rfc3339(), "max_bytes": request.max_bytes}),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::traits::WantedKeyValue;

    #[test]
    fn historical_heads_require_live_endpoint_chains_and_keep_ended_versions() {
        let q = edges(
            "org",
            &EdgeLookup::HeadsByChainPairs {
                pairs: vec![(Uuid::from_u128(1), Uuid::from_u128(2))],
            },
        )
        .unwrap();
        assert!(q.statement.contains("max(r.version)"));
        assert!(q
            .statement
            .contains("s.deleted_at IS NULL AND t.deleted_at IS NULL"));
        assert!(q
            .statement
            .contains("r.version=newest OR (r.is_latest=true"));
        assert_eq!(q.parameters["pairs"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn scheduled_collection_lookup_keeps_finite_history_and_binds_capture() {
        let collection = CollectionRef {
            namespace: "prod".into(),
            source: "aws".into(),
            key: "account".into(),
        };
        let at = chrono::DateTime::parse_from_rfc3339("2026-09-19T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let q = edges(
            "org",
            &EdgeLookup::ScheduledStaleInCollection {
                collection: collection.clone(),
                before_generation: 3,
                effective_at: at,
            },
        )
        .unwrap();
        assert_eq!(q.parameters["effective_at"], json!(at.to_rfc3339()));
        assert_eq!(q.parameters["sync_generation"], json!(3));
        assert_eq!(q.parameters["member"], json!(collection.member_id()));
        assert!(!q.statement.contains("r.is_latest"));
        assert!(q.statement.contains("old_s:Entity"));
        assert!(q.statement.contains("r.cancelled_at IS NULL"));
        assert!(q
            .statement
            .contains("datetime(r.valid_to)>datetime($effective_at)"));
        assert!(q
            .statement
            .contains("datetime(r.invalid_at)>datetime($effective_at)"));
        assert!(edges(
            "org",
            &EdgeLookup::ScheduledStaleInCollection {
                collection,
                before_generation: i64::MAX as u64 + 1,
                effective_at: at
            }
        )
        .is_err());
    }

    #[test]
    fn statements_bind_every_lookup_value() {
        let collection = CollectionRef {
            namespace: "prod".into(),
            source: "aws".into(),
            key: "acct/us-east-1/ec2".into(),
        };
        let q = entities(
            "org",
            &EntityLookup::StaleInCollection {
                collection: collection.clone(),
                before_generation: i64::MAX as u64,
            },
        )
        .unwrap();
        assert_eq!(q.parameters["sync_generation"], json!(i64::MAX));
        assert!(entities(
            "org",
            &EntityLookup::StaleInCollection {
                collection: collection.clone(),
                before_generation: i64::MAX as u64 + 1,
            }
        )
        .is_err());
        assert!(edges(
            "org",
            &EdgeLookup::StaleInCollection {
                collection: collection.clone(),
                before_generation: i64::MAX as u64 + 1,
            }
        )
        .is_err());
        assert_eq!(q.parameters["member"], json!(collection.member_id()));
        assert!(q.statement.contains("$sync_generation") && q.statement.contains("$member"));
        let q = edges(
            "org",
            &EdgeLookup::StaleInCollection {
                collection: collection.clone(),
                before_generation: 3,
            },
        )
        .unwrap();
        assert_eq!(q.parameters["sync_generation"], json!(3));
        assert_eq!(q.parameters["namespace"], json!(collection.namespace));
        assert_eq!(q.parameters["source"], json!(collection.source));
        assert!(q
            .statement
            .contains("producer_namespace:$namespace,producer_source:$source"));
        assert!(q.statement.contains("source_collections"));
        let q = edges(
            "org",
            &EdgeLookup::LiveBySourceScope {
                namespace: "prod".into(),
                source: "aws".into(),
            },
        )
        .unwrap();
        assert_eq!(q.parameters["source"], json!("aws"));
        assert!(q
            .statement
            .contains("producer_namespace:$namespace,producer_source:$source"));
        assert!(!q
            .statement
            .contains("namespace: $namespace, source: $source"));
        assert!(!q.statement.contains("$sync_generation"));
        let pair = (Uuid::new_v4(), Uuid::new_v4());
        let q = edges("org", &EdgeLookup::LiveByChainPairs { pairs: vec![pair] }).unwrap();
        assert_eq!(q.parameters["pairs"][0]["source"], json!(pair.0));
        assert!(entities(" ", &EntityLookup::LiveByName { names: vec![] }).is_err());
        let by_value = entities(
            "org",
            &EntityLookup::LiveByIdentifyingValue {
                values: vec!["vpc-0a1b".into()],
            },
        )
        .unwrap();
        assert_eq!(by_value.parameters["values"], json!(["vpc-0a1b"]));
        assert!(by_value.statement.contains("primary_key_properties"));
        let by_token = entities(
            "org",
            &EntityLookup::LiveByKeyValue {
                wanted: vec![WantedKeyValue::exact(
                    "i:443".into(),
                    Some(vec!["prod".into()]),
                )],
            },
        )
        .unwrap();
        assert_eq!(by_token.parameters["wanted"][0]["token"], json!("i:443"));
        assert_eq!(
            by_token.parameters["wanted"][0]["namespaces"],
            json!(["prod"])
        );
        assert!(by_token
            .statement
            .contains("w.namespaces IS NULL OR n.namespace IN w.namespaces"));
        assert!(by_token
            .statement
            .contains("w.target_types IS NULL OR n.entity_type IN w.target_types"));
        assert!(by_token
            .statement
            .contains("w.case_insensitive_string = true"));
        let missing = entities(
            "org",
            &EntityLookup::LiveMissingKeyValues {
                after_chain: None,
                limit: 500,
            },
        )
        .unwrap();
        assert!(missing.statement.contains("key_values IS NULL"));
        assert!(missing.statement.contains("LIMIT $limit"));
        assert_eq!(missing.parameters["after"], json!(null));
        assert_eq!(missing.parameters["limit"], json!(500));
        assert_eq!(
            by_token.parameters["carriers"],
            json!(MAX_CARRIERS_PER_IDENTIFYING_VALUE + 1)
        );
        assert!(by_token.statement.contains("IdentityValue"));
        assert!(by_token.statement.contains("KEY_COMPONENT"));
        assert_eq!(
            by_token.parameters["wanted"][0]["case_insensitive_string"],
            json!(false)
        );
        assert!(by_token.statement.contains("is_latest:true"));
        assert!(by_token.statement.contains("deleted_at IS NULL"));
    }

    #[test]
    fn decodes_nested_and_flattened_rows() {
        let uuid = Uuid::new_v4();
        let chain = Uuid::new_v4();
        let props = json!({
            "uuid": uuid, "chain_id": chain, "version": 3, "is_latest": true,
            "entity_type": "Service", "name": "api", "namespace": "prod",
            "identity_hash": "h1", "identity_hashes": ["h0"], "structural_hash": "42",
            "valid_from": "2026-01-01T00:00:00+00:00", "sync_generation": 7,
            "collection_members": [r#"["prod","aws","k"]"#, "not-a-member"],
            "collection_generations": [7],
            "embedding": [0.1, 0.2], "embedding_model": "m", "prop_owner": "payments"
        });
        let mut flattened = props.as_object().unwrap().clone();
        flattened.insert("n".into(), props.clone());
        let nested = decode_entity_version(flattened).unwrap();
        let direct = decode_entity_version(props.as_object().unwrap().clone()).unwrap();
        assert_eq!(nested, direct);
        assert_eq!(nested.version, 3);
        assert_eq!(nested.answering_hashes().collect::<Vec<_>>(), ["h1", "h0"]);
        assert_eq!(nested.structural_hash.as_deref(), Some("42"));
        assert_eq!(nested.sync_generation, Some(7));
        assert_eq!(
            nested.collections,
            vec![CollectionMembership {
                collection: CollectionRef {
                    namespace: "prod".into(),
                    source: "aws".into(),
                    key: "k".into(),
                },
                generation: 7,
            }],
            "unreadable member ids are skipped"
        );
        let (members, generations) = membership_properties(&nested.collections);
        assert_eq!(members, json!([r#"["prod","aws","k"]"#]));
        assert_eq!(generations, json!([7]));
        assert_eq!(nested.embedding.as_ref().map(|e| e.values.len()), Some(2));
        assert_eq!(
            nested.source_properties().collect::<Vec<_>>(),
            vec![("owner", &json!("payments"))]
        );
        assert!(!nested.stored.contains_key("n"));

        let bad = json!({"uuid": "nope", "chain_id": chain});
        assert!(decode_entity_version(bad.as_object().unwrap().clone()).is_err());
        let bad_time = json!({"uuid": uuid, "chain_id": chain, "valid_from": "yesterday"});
        assert!(decode_entity_version(bad_time.as_object().unwrap().clone()).is_err());
    }

    #[test]
    fn edge_endpoints_come_from_columns_or_properties() {
        let (uuid, s, t) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let rel = json!({"uuid": uuid, "name": "DEPENDS_ON", "confidence": 0.9, "is_latest": true});
        let mut row = Map::new();
        row.insert("r".into(), rel.clone());
        row.insert("source_chain_id".into(), json!(s));
        row.insert("target_chain_id".into(), json!(t));
        row.insert(
            "source_collections".into(),
            json!([r#"["prod","aws","k"]"#]),
        );
        let from_columns = decode_edge(row).unwrap();
        assert_eq!(from_columns.source_chain_id, s);
        assert_eq!(from_columns.source_collections.len(), 1);
        assert_eq!(from_columns.source_collections[0].key, "k");
        assert_eq!(from_columns.version, 1);
        assert!((from_columns.confidence - 0.9).abs() < 1e-6);

        let mut props = rel.as_object().unwrap().clone();
        props.insert("source_chain_id".into(), json!(s));
        props.insert("target_chain_id".into(), json!(t));
        let from_props = decode_edge(props).unwrap();
        assert_eq!(from_props.target_chain_id, t);
        assert!(decode_edge(rel.as_object().unwrap().clone()).is_err());
    }
}
