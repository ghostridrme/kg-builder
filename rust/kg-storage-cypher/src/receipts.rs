//! Run headers, operation receipts, and transactional preconditions.
//!
//! Preconditions lock the checked record with a self-assignment before
//! evaluating it, so the check sees the state that the commit will see.
//! Every precondition statement must return exactly one `ok = true` row.

use crate::bolt_generation;
use crate::{PreparedQuery, PreparedWrite};
use chrono::{DateTime, Utc};
use kg_core::{
    errors::BackendError,
    traits::{BatchKind, CommittedBatch, MutationBatch, Precondition, RunHeader},
};
use serde_json::{json, Map, Value};
use uuid::Uuid;

/// Run header fields read back after registration.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredRunHeader {
    pub observation_manifest: kg_core::runtime::saga::RunObservationManifest,
    pub schema_manifest: kg_core::runtime::schemas::RunSchemaManifest,
    pub rule_freezes: Vec<kg_core::runtime::rule_learning::materialize::RuleFreeze>,
    pub batch_plan: Vec<kg_core::traits::PlannedBatch>,
    pub org_id: String,
    pub fingerprint: String,
    pub settings_version: String,
    pub capture_default: DateTime<Utc>,
    /// True when this registration created the header.
    pub created: bool,
}

/// Create the run header if absent and return the stored header either way.
pub fn register_run(header: &RunHeader) -> Result<PreparedQuery, BackendError> {
    header.validate()?;
    let batch_plan = serde_json::to_string(&header.batch_plan)
        .map_err(|e| BackendError::Serialization(e.to_string()))?;
    Ok(PreparedQuery {
        statement: REGISTER_RUN.into(),
        parameters: json!({
            "run_id": header.run_id,
            "org": header.org_id,
            "fingerprint": header.fingerprint.0,
            "settings_version": header.settings_version,
            "capture_default": header.capture_default.to_rfc3339(),
            "batch_plan": batch_plan,
            "observation_manifest": serde_json::to_string(&header.observation_manifest).map_err(|_| BackendError::Serialization("observation manifest".into()))?,
            "schema_manifest": serde_json::to_string(&header.schema_manifest).map_err(|_| BackendError::Serialization("schema manifest".into()))?,
            "rule_freezes": serde_json::to_string(&header.rule_freezes).map_err(|_| BackendError::Serialization("rule freezes".into()))?,
            "now": Utc::now().to_rfc3339(),
            "registration_token": Uuid::new_v4(),
        }),
    })
}

pub fn decode_run_header(row: &Map<String, Value>) -> Result<StoredRunHeader, BackendError> {
    let text = |key: &str| {
        row.get(key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| BackendError::Deserialization(format!("run header lacks {key}")))
    };
    Ok(StoredRunHeader {
        observation_manifest: serde_json::from_str(&text("observation_manifest")?)
            .map_err(|_| BackendError::Deserialization("invalid observation manifest".into()))?,
        schema_manifest: serde_json::from_str(&text("schema_manifest")?)
            .map_err(|_| BackendError::Deserialization("invalid run schema manifest".into()))?,
        rule_freezes: serde_json::from_str(&text("rule_freezes")?)
            .map_err(|_| BackendError::Deserialization("invalid rule freezes".into()))?,
        batch_plan: serde_json::from_str(&text("batch_plan")?)
            .map_err(|_| BackendError::Deserialization("invalid batch plan".into()))?,
        org_id: text("org_id")?,
        fingerprint: text("fingerprint")?,
        settings_version: text("settings_version")?,
        capture_default: DateTime::parse_from_rfc3339(&text("capture_default")?)
            .map(|t| t.with_timezone(&Utc))
            .map_err(|_| {
                BackendError::Deserialization("run header has an invalid capture default".into())
            })?,
        created: row
            .get("created")
            .and_then(Value::as_bool)
            .ok_or_else(|| BackendError::Deserialization("run header lacks created".into()))?,
    })
}

/// Read a scoped run before deciding whether mutable ontology is needed.
pub fn read_run(org: &str, run_id: Uuid) -> PreparedQuery {
    PreparedQuery {
        statement: "MATCH (run:IngestionRun {org_id:$org,run_id:$run_id}) RETURN run.org_id AS org_id,run.fingerprint AS fingerprint,run.settings_version AS settings_version,run.capture_default AS capture_default,false AS created,run.observation_manifest AS observation_manifest,run.schema_manifest AS schema_manifest,run.rule_freezes AS rule_freezes,run.batch_plan AS batch_plan".into(),
        parameters: json!({"org":org,"run_id":run_id}),
    }
}

/// Inside the commit transaction: the run header must exist with this fingerprint.
pub fn check_run(org: &str, run_id: Uuid, fingerprint: &str) -> PreparedWrite {
    PreparedWrite {
        statement: CHECK_RUN.into(),
        parameters: json!({"run_id": run_id, "org": org, "fingerprint": fingerprint}),
        expected_rows: 1,
    }
}

pub fn read_receipt(batch_id: Uuid) -> PreparedQuery {
    PreparedQuery {
        statement: format!(
            "MATCH (r:OperationReceipt {{batch_id:$batch_id}}) RETURN {RECEIPT_COLUMNS}"
        ),
        parameters: json!({"batch_id": batch_id}),
    }
}

pub fn run_receipts(org: &str, run_id: Uuid) -> PreparedQuery {
    PreparedQuery {
        statement: format!(
            "MATCH (r:OperationReceipt {{org_id:$org,run_id:$run_id}}) RETURN {RECEIPT_COLUMNS} ORDER BY r.committed_at,r.index"
        ),
        parameters: json!({"org": org, "run_id": run_id}),
    }
}

/// Write the receipt; the uniqueness constraint on `batch_id` arbitrates
/// concurrent commits of the same batch.
pub fn create_receipt(
    batch: &MutationBatch,
    committed_at: DateTime<Utc>,
) -> Result<PreparedWrite, BackendError> {
    let result = serde_json::to_string(&batch.result)
        .map_err(|e| BackendError::Serialization(e.to_string()))?;
    Ok(PreparedWrite {
        statement: CREATE_RECEIPT.into(),
        parameters: json!({
            "batch_id": batch.batch_id(),
            "graph_stripe": batch.batch_id().as_bytes()[0] % 64,
            "org": batch.org_id,
            "run_id": batch.batch.run_id,
            "kind": batch.batch.kind.label(),
            "index": batch.batch.index,
            "fingerprint": batch.fingerprint.0,
            "result": result,
            "at": committed_at.to_rfc3339(),
        }),
        expected_rows: 1,
    })
}

/// A stored receipt; `fingerprint` lets callers reject reuse with different content.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredReceipt {
    pub org_id: String,
    pub fingerprint: String,
    pub batch: CommittedBatch,
}

pub fn decode_receipt(row: &Map<String, Value>) -> Result<StoredReceipt, BackendError> {
    let text = |key: &str| {
        row.get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| BackendError::Deserialization(format!("receipt lacks {key}")))
    };
    let id = |key: &str| {
        Uuid::parse_str(text(key)?)
            .map_err(|_| BackendError::Deserialization(format!("receipt has an invalid {key}")))
    };
    let kind = BatchKind::parse(text("kind")?)
        .ok_or_else(|| BackendError::Deserialization("receipt has an unknown kind".into()))?;
    let index = row
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|i| u32::try_from(i).ok())
        .ok_or_else(|| BackendError::Deserialization("receipt lacks index".into()))?;
    let committed_at = DateTime::parse_from_rfc3339(text("committed_at")?)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| BackendError::Deserialization("receipt has an invalid time".into()))?;
    let result: Value = serde_json::from_str(text("result")?)
        .map_err(|e| BackendError::Deserialization(format!("receipt result: {e}")))?;
    Ok(StoredReceipt {
        org_id: text("org_id")?.to_owned(),
        fingerprint: text("fingerprint")?.to_owned(),
        batch: CommittedBatch {
            batch_id: id("batch_id")?,
            run_id: id("run_id")?,
            kind,
            index,
            committed_at,
            result,
            replayed: true,
        },
    })
}

/// Prepare one precondition check.
pub fn precondition(org: &str, precondition: &Precondition) -> Result<PreparedWrite, BackendError> {
    precondition.validate()?;
    if let Precondition::IdentityRevisionIs(expected) = precondition {
        return crate::identity_revision::check(org, expected);
    }
    let (statement, mut parameters) = match precondition {
        Precondition::RuleRevisionIs {
            id,
            revision,
            status,
        } => (
            RULE_REVISION_IS,
            json!({
                "id": id,
                "revision": revision,
                "status": serde_json::to_value(status)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .ok_or_else(|| BackendError::Serialization("rule status".into()))?,
            }),
        ),
        Precondition::IdentityRevisionIs(_) => unreachable!("handled above"),
        Precondition::LatestVersionIs {
            chain_id,
            uuid,
            version,
        } => (
            LATEST_VERSION_IS,
            json!({"chain": chain_id, "uuid": uuid, "version": version}),
        ),
        Precondition::NoLiveVersionFor { hashes } => {
            (NO_LIVE_VERSION_FOR, json!({"hashes": hashes}))
        }
        Precondition::LatestDeletedVersionIs {
            chain_id,
            uuid,
            restored_at,
        } => (
            LATEST_DELETED_VERSION_IS,
            json!({"chain": chain_id, "uuid": uuid, "restored_at": restored_at.to_rfc3339()}),
        ),
        Precondition::NotObservedAfter { uuid, observed_at } => (
            NOT_OBSERVED_AFTER,
            json!({"uuid": uuid, "at": observed_at.to_rfc3339()}),
        ),
        Precondition::EdgeHeadIs {
            source_chain_id,
            target_chain_id,
            chain_id,
            uuid,
            version,
            observed_at,
        } => (
            EDGE_HEAD_IS,
            json!({"source":source_chain_id,"target":target_chain_id,"chain":chain_id,"uuid":uuid,"version":version,"at":observed_at.to_rfc3339()}),
        ),
        Precondition::EdgeIsLatest { uuid, version } => {
            (EDGE_IS_LATEST, json!({"uuid": uuid, "version": version}))
        }
        Precondition::EdgeStartsNoLaterThan {
            uuid,
            effective_end,
        } => (
            EDGE_STARTS_NO_LATER_THAN,
            json!({"uuid": uuid, "at": effective_end.to_rfc3339()}),
        ),
        Precondition::EdgeNotObservedAfter { uuid, observed_at } => (
            EDGE_NOT_OBSERVED_AFTER,
            json!({"uuid":uuid,"at":observed_at.to_rfc3339()}),
        ),
        Precondition::EdgeObservedBefore { uuid, observed_at } => (
            EDGE_OBSERVED_BEFORE,
            json!({"uuid":uuid,"at":observed_at.to_rfc3339()}),
        ),
        Precondition::IncidentHistoryIs { chain_id, versions } => {
            return crate::entity_summary::incident_history(org, *chain_id, versions);
        }
        Precondition::IncidentTimelineIs { chain_id, versions } => {
            let mut ordered: Vec<_> = versions.iter().collect();
            ordered.sort_by_key(|version| version.properties["uuid"].as_str().unwrap_or_default());
            (
                INCIDENT_TIMELINE_IS,
                json!({"chain":chain_id,"versions":ordered,"ignored":kg_core::traits::relationship_timeline::EMBEDDING_PROPERTIES}),
            )
        }
        Precondition::RelationTimelineIs {
            source_chain_id,
            name,
            versions,
        } => {
            let mut ordered: Vec<_> = versions.iter().collect();
            ordered.sort_by_key(|version| version.properties["uuid"].as_str().unwrap_or_default());
            (
                RELATION_TIMELINE_IS,
                json!({"source":source_chain_id,"name":name,"versions":ordered,"ignored":kg_core::traits::relationship_timeline::EMBEDDING_PROPERTIES}),
            )
        }
        Precondition::ReferenceOwnerTimelineIs { owner, versions } => {
            let mut ordered: Vec<_> = versions.iter().collect();
            ordered.sort_by_key(|version| version.properties["uuid"].as_str().unwrap_or_default());
            (
                REFERENCE_OWNER_TIMELINE_IS,
                json!({"owner":owner.chain_id,"namespace":owner.namespace,"slot":owner.slot,"versions":ordered,"ignored":kg_core::traits::relationship_timeline::EMBEDDING_PROPERTIES}),
            )
        }
        Precondition::RelationshipTimelineIs {
            source_chain_id,
            target_chain_id,
            versions,
        } => {
            let mut ordered: Vec<_> = versions.iter().collect();
            ordered.sort_by_key(|version| version["uuid"].as_str().unwrap_or_default());
            (
                RELATIONSHIP_TIMELINE_IS,
                json!({"source":source_chain_id,"target":target_chain_id,"versions":ordered,"ignored":kg_core::traits::relationship_timeline::EMBEDDING_PROPERTIES}),
            )
        }
        Precondition::LiveEdgesForPairAre {
            source_chain_id,
            target_chain_id,
            uuids,
        } => (
            LIVE_EDGES_FOR_PAIR_ARE,
            json!({"source": source_chain_id, "target": target_chain_id,"uuids":uuids}),
        ),
        Precondition::CollectionMembershipsAre { uuid, memberships } => {
            let (members, generations) =
                kg_core::models::CollectionMembership::to_properties(memberships);
            (
                COLLECTION_MEMBERSHIPS_ARE,
                json!({"uuid": uuid, "members": members, "generations": generations}),
            )
        }
        Precondition::SoleCollectionOwnerIs { uuid, collection } => (
            SOLE_COLLECTION_OWNER_IS,
            json!({"uuid": uuid, "member": collection.member_id()}),
        ),
        Precondition::LiveIncidentEdgesAre { chain_id, uuids } => (
            LIVE_INCIDENT_EDGES_ARE,
            json!({"chain": chain_id, "uuids": uuids}),
        ),
        Precondition::LiveEdgesForRelationAre {
            source_chain_id,
            name,
            uuids,
        } => (
            LIVE_EDGES_FOR_RELATION_ARE,
            json!({"source": source_chain_id, "name": name, "uuids": uuids}),
        ),
        Precondition::LiveEdgesForReferenceOwnerAre { owner, uuids } => (
            LIVE_EDGES_FOR_REFERENCE_OWNER_ARE,
            json!({"owner":owner.chain_id,"namespace":owner.namespace,"slot":owner.slot,"uuids":uuids}),
        ),
        Precondition::OwnsCollection {
            collection,
            generation,
            run_id,
        } => (
            OWNS_COLLECTION,
            json!({
                "scope_id": collection.scope_id(org),
                "namespace": collection.namespace,
                "source": collection.source,
                "key": collection.key,
                "generation": bolt_generation(*generation),
                "run_id": run_id,
                "at": Utc::now().to_rfc3339(),
            }),
        ),
    };
    parameters["org"] = json!(org);
    Ok(PreparedWrite {
        statement: statement.into(),
        parameters,
        expected_rows: 1,
    })
}

const REGISTER_RUN: &str = "MERGE (run:IngestionRun {run_id:$run_id})
    ON CREATE SET run.org_id=$org,run.fingerprint=$fingerprint,run.settings_version=$settings_version,run.capture_default=$capture_default,run.batch_plan=$batch_plan,run.schema_manifest=$schema_manifest,run.observation_manifest=$observation_manifest,run.rule_freezes=$rule_freezes,run.created_at=$now,run.registration_token=$registration_token
    RETURN run.org_id AS org_id,run.fingerprint AS fingerprint,run.settings_version AS settings_version,run.capture_default AS capture_default,run.registration_token=$registration_token AS created,run.observation_manifest AS observation_manifest,run.schema_manifest AS schema_manifest,run.rule_freezes AS rule_freezes,run.batch_plan AS batch_plan";

const CHECK_RUN: &str =
    "MATCH (run:IngestionRun {run_id:$run_id,org_id:$org,fingerprint:$fingerprint})
    RETURN true AS ok";

const RULE_REVISION_IS: &str = "MATCH (r:LearnedRule {org_id:$org,id:$id})
    SET r.id=r.id
    WITH r WHERE r.revision=$revision AND r.status=$status
    RETURN true AS ok";

const RECEIPT_COLUMNS: &str = "r.batch_id AS batch_id,r.org_id AS org_id,r.run_id AS run_id,r.kind AS kind,r.index AS index,r.fingerprint AS fingerprint,r.result AS result,r.committed_at AS committed_at";

// Commit markers share the receipt transaction: rollback and replay cannot publish
// a false change. Stripes avoid serializing all ingestion on one organization lock.
const CREATE_RECEIPT: &str = "CREATE (r:OperationReceipt {batch_id:$batch_id,org_id:$org,run_id:$run_id,kind:$kind,index:$index,fingerprint:$fingerprint,result:$result,committed_at:$at})
    WITH r
    MERGE (revision:GraphRevision {org_id:$org,stripe:$graph_stripe})
    SET revision.token=$batch_id
    RETURN true AS ok";

const LATEST_VERSION_IS: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid,chain_id:$chain})
    SET n.uuid=n.uuid
    WITH n
    WHERE n.is_latest=true AND n.deleted_at IS NULL AND coalesce(n.version,1)=$version
    AND NOT EXISTS { MATCH (o:Entity {org_id:$org,chain_id:$chain,is_latest:true}) WHERE o.uuid<>$uuid }
    RETURN true AS ok";

const NO_LIVE_VERSION_FOR: &str = "UNWIND $hashes AS hash
    WITH DISTINCT hash ORDER BY hash
    MERGE (key:IdentityKey {org_id:$org,hash:hash})
    SET key.hash=key.hash
    WITH key
    OPTIONAL MATCH (key)-[:IDENTIFIES]->(n:Entity {org_id:$org})
    WHERE n.is_latest=true AND n.deleted_at IS NULL
    WITH count(n) AS live WHERE live=0
    RETURN true AS ok";

const LATEST_DELETED_VERSION_IS: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid,chain_id:$chain})
    SET n.uuid=n.uuid
    WITH n
    WHERE n.deleted_at IS NOT NULL AND datetime(n.deleted_at)<datetime($restored_at)
    AND (n.last_transition_at IS NULL OR datetime(n.last_transition_at)<datetime($restored_at))
    AND NOT EXISTS { MATCH (o:Entity {org_id:$org,chain_id:$chain}) WHERE o.is_latest=true OR coalesce(o.version,1)>coalesce(n.version,1) }
    RETURN true AS ok";

const NOT_OBSERVED_AFTER: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid})
    SET n.uuid=n.uuid
    WITH n
    WHERE n.is_latest=true AND n.deleted_at IS NULL AND (n.last_seen_at IS NULL OR datetime(n.last_seen_at)<=datetime($at))
    AND (n.last_transition_at IS NULL OR datetime(n.last_transition_at)<=datetime($at))
    RETURN true AS ok";

const EDGE_IS_LATEST: &str = "MATCH (s:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->(t:Entity {org_id:$org})
    SET r.uuid=r.uuid
    WITH r
    WHERE r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL AND coalesce(r.version,1)=$version
    RETURN true AS ok";

const EDGE_STARTS_NO_LATER_THAN: &str =
    "MATCH (:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->(:Entity {org_id:$org})
    SET r.uuid=r.uuid
    WITH r WHERE r.valid_from IS NOT NULL AND datetime(r.valid_from)<=datetime($at)
    RETURN true AS ok";

const EDGE_NOT_OBSERVED_AFTER: &str =
    "MATCH (:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->(:Entity {org_id:$org})
    SET r.uuid=r.uuid
    WITH r
    WHERE r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
      AND (r.last_seen_at IS NULL OR datetime(r.last_seen_at)<=datetime($at))
      AND (r.last_transition_at IS NULL OR datetime(r.last_transition_at)<=datetime($at))
    RETURN true AS ok";

// Closing a current target at `$at`: an observation at that very time is
// a contradiction, so every recorded time must be strictly older.
const EDGE_OBSERVED_BEFORE: &str =
    "MATCH (:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->(:Entity {org_id:$org})
    SET r.uuid=r.uuid
    WITH r
    WHERE r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
      AND (r.last_seen_at IS NULL OR datetime(r.last_seen_at)<datetime($at))
      AND (r.last_transition_at IS NULL OR datetime(r.last_transition_at)<datetime($at))
    RETURN true AS ok";

// MERGE locks the record; an older generation is superseded in place, the
// same generation passes only for the run that claimed it.
const OWNS_COLLECTION: &str = "MERGE (c:CollectionScan {scope_id:$scope_id})
    ON CREATE SET c.org_id=$org,c.namespace=$namespace,c.source=$source,c.key=$key,c.generation=$generation,c.run_id=$run_id,c.claimed_at=$at
    SET c.generation=c.generation
    WITH c
    WHERE c.org_id=$org AND (c.generation<$generation OR (c.generation=$generation AND c.run_id=$run_id))
    SET c.generation=$generation,c.run_id=$run_id,c.claimed_at=$at
    RETURN true AS ok";

// The source node is locked; the live relationships of one name must be
// exactly the planned set, so a concurrent target for the same relation
// rejects the commit.
const LIVE_EDGES_FOR_RELATION_ARE: &str =
    "MATCH (s:Entity {org_id:$org,chain_id:$source,is_latest:true})
    WHERE s.deleted_at IS NULL
    SET s.uuid=s.uuid
    WITH s
    WHERE s.is_latest=true AND s.deleted_at IS NULL
    OPTIONAL MATCH (s)-[r:RELATES_TO {org_id:$org,name:$name}]->(t:Entity {org_id:$org,is_latest:true})
    WHERE t.deleted_at IS NULL AND r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
    WITH s,collect(r.uuid) AS live
    WHERE size(live)=size($uuids) AND all(u IN $uuids WHERE u IN live)
    RETURN true AS ok";

const LIVE_EDGES_FOR_REFERENCE_OWNER_ARE: &str =
    "MATCH (owner:Entity {org_id:$org,chain_id:$owner,is_latest:true})
    WHERE owner.deleted_at IS NULL
    SET owner.uuid=owner.uuid
    WITH owner
    OPTIONAL MATCH (s:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,reference_owner_chain_id:$owner,reference_owner_namespace:$namespace,reference_slot:$slot}]->(t:Entity {org_id:$org})
    WHERE r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
    WITH owner,collect(r.uuid) AS live
    WHERE size(live)=size($uuids) AND all(u IN $uuids WHERE u IN live)
    RETURN true AS ok";

const EDGE_HEAD_IS: &str = "MATCH (s:Entity {org_id:$org,chain_id:$source,is_latest:true})
    WHERE s.deleted_at IS NULL
    SET s.uuid=s.uuid
    WITH s WHERE s.is_latest=true AND s.deleted_at IS NULL
    MATCH (t:Entity {org_id:$org,chain_id:$target,is_latest:true})
    WHERE t.deleted_at IS NULL
    MATCH (old_s:Entity {org_id:$org,chain_id:$source})-[r:RELATES_TO {org_id:$org,chain_id:$chain}]->(old_t:Entity {org_id:$org,chain_id:$target})
    WITH collect(r) AS versions,max(r.version) AS newest
    WITH [r IN versions WHERE r.version=newest] AS heads
    WHERE size(heads)=1 AND heads[0].uuid=$uuid AND heads[0].version=$version
      AND (heads[0].last_seen_at IS NULL OR datetime(heads[0].last_seen_at)<=datetime($at))
      AND (heads[0].last_transition_at IS NULL OR datetime(heads[0].last_transition_at)<=datetime($at))
    RETURN true AS ok";

// Endpoint locks protect the set; relationship locks protect in-place amendments.
const INCIDENT_TIMELINE_IS: &str = "MATCH (head:Entity {org_id:$org,chain_id:$chain,is_latest:true})
    WHERE head.deleted_at IS NULL
    SET head.uuid=head.uuid
    WITH collect(head) AS heads
    WHERE size(heads)=1 AND all(head IN heads WHERE head.is_latest=true AND head.deleted_at IS NULL)
    OPTIONAL MATCH (anchor:Entity {org_id:$org,chain_id:$chain})-[r:RELATES_TO {org_id:$org}]-(other:Entity {org_id:$org})
    WITH DISTINCT heads,r ORDER BY r.uuid
    FOREACH (edge IN CASE WHEN r IS NULL THEN [] ELSE [r] END | SET edge.uuid=edge.uuid)
    WITH heads,collect(r) AS actual
    WHERE size(actual)=size($versions)
      AND all(i IN range(0,size(actual)-1) WHERE actual[i].uuid=$versions[i].properties.uuid
        AND startNode(actual[i]).chain_id=$versions[i].source_chain_id
        AND endNode(actual[i]).chain_id=$versions[i].target_chain_id
        AND size([key IN keys(actual[i]) WHERE NOT key IN $ignored])=size(keys($versions[i].properties))
        AND all(key IN keys($versions[i].properties) WHERE actual[i][key]=$versions[i].properties[key]))
    RETURN true AS ok";

const RELATION_TIMELINE_IS: &str = "MATCH (head:Entity {org_id:$org,chain_id:$source,is_latest:true})
    WHERE head.deleted_at IS NULL
    SET head.uuid=head.uuid
    WITH collect(head) AS heads
    WHERE size(heads)=1 AND all(head IN heads WHERE head.is_latest=true AND head.deleted_at IS NULL)
    OPTIONAL MATCH (s:Entity {org_id:$org,chain_id:$source})-[r:RELATES_TO {org_id:$org,name:$name}]->(t:Entity {org_id:$org})
    WITH heads,r,t.chain_id AS target ORDER BY r.uuid
    FOREACH (edge IN CASE WHEN r IS NULL THEN [] ELSE [r] END | SET edge.uuid=edge.uuid)
    WITH heads,collect(CASE WHEN r IS NULL THEN null ELSE {edge:r,target:target} END) AS actual
    WHERE size(actual)=size($versions)
      AND all(i IN range(0,size(actual)-1) WHERE actual[i].edge.uuid=$versions[i].properties.uuid
        AND actual[i].target=$versions[i].target_chain_id
        AND size([key IN keys(actual[i].edge) WHERE NOT key IN $ignored])=size(keys($versions[i].properties))
        AND all(key IN keys($versions[i].properties) WHERE actual[i].edge[key]=$versions[i].properties[key]))
    RETURN true AS ok";

const REFERENCE_OWNER_TIMELINE_IS: &str = "MATCH (head:Entity {org_id:$org,chain_id:$owner,is_latest:true})
    WHERE head.deleted_at IS NULL
    SET head.uuid=head.uuid
    WITH collect(head) AS heads
    WHERE size(heads)=1
    OPTIONAL MATCH (s:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,reference_owner_chain_id:$owner,reference_owner_namespace:$namespace,reference_slot:$slot}]->(t:Entity {org_id:$org})
    WITH heads,r,s.chain_id AS source,t.chain_id AS target ORDER BY r.uuid
    FOREACH (edge IN CASE WHEN r IS NULL THEN [] ELSE [r] END | SET edge.uuid=edge.uuid)
    WITH heads,collect(CASE WHEN r IS NULL THEN null ELSE {properties:r,source_chain_id:source,target_chain_id:target} END) AS actual
    WHERE size(actual)=size($versions)
      AND all(i IN range(0,size(actual)-1) WHERE actual[i].properties.uuid=$versions[i].properties.uuid
        AND actual[i].source_chain_id=$versions[i].source_chain_id
        AND actual[i].target_chain_id=$versions[i].target_chain_id
        AND size([key IN keys(actual[i].properties) WHERE NOT key IN $ignored])=size(keys($versions[i].properties))
        AND all(key IN keys($versions[i].properties) WHERE actual[i].properties[key]=$versions[i].properties[key]))
    RETURN true AS ok";

const RELATIONSHIP_TIMELINE_IS: &str = "UNWIND CASE WHEN $source=$target THEN [$source] ELSE [$source,$target] END AS chain
    WITH chain ORDER BY chain
    MATCH (head:Entity {org_id:$org,chain_id:chain,is_latest:true})
    WHERE head.deleted_at IS NULL
    SET head.uuid=head.uuid
    WITH collect(head) AS heads
    WHERE size(heads)=CASE WHEN $source=$target THEN 1 ELSE 2 END
      AND all(head IN heads WHERE head.is_latest=true AND head.deleted_at IS NULL)
    OPTIONAL MATCH (s:Entity {org_id:$org,chain_id:$source})-[r:RELATES_TO {org_id:$org}]->(t:Entity {org_id:$org,chain_id:$target})
    WITH DISTINCT heads,r ORDER BY r.uuid
    FOREACH (edge IN CASE WHEN r IS NULL THEN [] ELSE [r] END | SET edge.uuid=edge.uuid)
    WITH heads,collect(r) AS actual
    WHERE size(actual)=size($versions)
      AND all(i IN range(0,size(actual)-1) WHERE actual[i].uuid=$versions[i].uuid
        AND size([key IN keys(actual[i]) WHERE NOT key IN $ignored])=size(keys($versions[i]))
        AND all(key IN keys($versions[i]) WHERE actual[i][key]=$versions[i][key]))
    RETURN true AS ok";

const LIVE_EDGES_FOR_PAIR_ARE: &str = "MATCH (s:Entity {org_id:$org,chain_id:$source,is_latest:true})
    WHERE s.deleted_at IS NULL
    SET s.uuid=s.uuid
    WITH s
    WHERE s.is_latest=true AND s.deleted_at IS NULL
    OPTIONAL MATCH (s)-[r:RELATES_TO {org_id:$org}]->(t:Entity {org_id:$org,chain_id:$target,is_latest:true})
    WHERE t.deleted_at IS NULL AND r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
    WITH s,collect(r.uuid) AS live
    WHERE size(live)=size($uuids) AND all(u IN $uuids WHERE u IN live)
    RETURN true AS ok";

const LIVE_INCIDENT_EDGES_ARE: &str =
    "MATCH (n:Entity {org_id:$org,chain_id:$chain,is_latest:true})
    WHERE n.deleted_at IS NULL
    SET n.uuid=n.uuid
    WITH n
    WHERE n.is_latest=true AND n.deleted_at IS NULL
    OPTIONAL MATCH (n)-[r:RELATES_TO {org_id:$org}]-(other:Entity {org_id:$org,is_latest:true})
    WHERE other.deleted_at IS NULL AND r.is_latest=true AND r.cancelled_at IS NULL AND r.deleted_at IS NULL AND r.invalid_at IS NULL AND r.valid_to IS NULL
    WITH n,collect(DISTINCT r.uuid) AS live
    WHERE size(live)=size($uuids) AND all(u IN $uuids WHERE u IN live)
    RETURN true AS ok";

const SOLE_COLLECTION_OWNER_IS: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid})
    SET n.uuid=n.uuid
    WITH n
    WHERE n.is_latest=true AND n.deleted_at IS NULL
      AND coalesce(n.collection_members,[]) = [$member]
    RETURN true AS ok";

const COLLECTION_MEMBERSHIPS_ARE: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid})
    SET n.uuid=n.uuid
    WITH n
    WHERE n.is_latest=true AND n.deleted_at IS NULL
      AND coalesce(n.collection_members,[])=$members
      AND coalesce(n.collection_generations,[])=$generations
    RETURN true AS ok";

const COLLECTION_OWNER: &str = "MATCH (c:CollectionScan {scope_id:$scope_id, org_id:$org})
                         RETURN c.generation AS generation, c.run_id AS run_id";

pub fn collection_owner(org: &str, scope_id: &uuid::Uuid) -> crate::PreparedQuery {
    crate::PreparedQuery {
        statement: COLLECTION_OWNER.into(),
        parameters: serde_json::json!({"org": org, "scope_id": scope_id}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::traits::{BatchIdentity, RequestFingerprint};

    #[test]
    fn receipt_round_trips_through_its_row_shape() {
        let batch = MutationBatch {
            org_id: "org".into(),
            batch: BatchIdentity {
                run_id: Uuid::new_v4(),
                kind: BatchKind::Relationship,
                index: 4,
            },
            fingerprint: RequestFingerprint("a".repeat(32)),
            preconditions: vec![],
            mutations: vec![],
            result: json!({"edges_created": 2}),
        };
        let at: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().unwrap();
        let write = create_receipt(&batch, at).unwrap();
        assert_eq!(write.expected_rows, 1);
        let mut row = Map::new();
        for key in [
            "batch_id",
            "org",
            "run_id",
            "kind",
            "index",
            "fingerprint",
            "result",
        ] {
            let column = if key == "org" { "org_id" } else { key };
            row.insert(column.into(), write.parameters[key].clone());
        }
        row.insert("committed_at".into(), write.parameters["at"].clone());
        let stored = decode_receipt(&row).unwrap();
        assert_eq!(stored.org_id, "org");
        assert_eq!(stored.fingerprint, "a".repeat(32));
        assert_eq!(stored.batch.batch_id, batch.batch_id());
        assert_eq!(stored.batch.kind, BatchKind::Relationship);
        assert_eq!(stored.batch.index, 4);
        assert_eq!(stored.batch.result, json!({"edges_created": 2}));
        assert_eq!(stored.batch.committed_at, at);
        assert!(stored.batch.replayed);
        assert_eq!(RECEIPT_COLUMNS.matches(" AS ").count(), 8);
        assert!(read_receipt(Uuid::nil())
            .statement
            .contains(RECEIPT_COLUMNS));
        assert!(run_receipts("org", Uuid::nil())
            .statement
            .contains("ORDER BY"));
    }

    #[test]
    fn preconditions_bind_the_organization() {
        for precondition in [
            Precondition::RuleRevisionIs {
                id: Uuid::from_u128(9),
                revision: 3,
                status: kg_core::traits::RuleStatus::Active,
            },
            Precondition::LatestVersionIs {
                chain_id: Uuid::from_u128(1),
                uuid: Uuid::from_u128(1),
                version: 2,
            },
            Precondition::NoLiveVersionFor {
                hashes: vec!["h".into()],
            },
            Precondition::LatestDeletedVersionIs {
                chain_id: Uuid::from_u128(1),
                uuid: Uuid::from_u128(1),
                restored_at: Utc::now(),
            },
            Precondition::NotObservedAfter {
                uuid: Uuid::from_u128(1),
                observed_at: Utc::now(),
            },
            Precondition::EdgeHeadIs {
                observed_at: Utc::now(),
                source_chain_id: Uuid::from_u128(1),
                target_chain_id: Uuid::from_u128(2),
                chain_id: Uuid::from_u128(3),
                uuid: Uuid::from_u128(4),
                version: 1,
            },
            Precondition::EdgeStartsNoLaterThan {
                uuid: Uuid::from_u128(4),
                effective_end: Utc::now(),
            },
            Precondition::EdgeIsLatest {
                uuid: Uuid::from_u128(1),
                version: 1,
            },
            Precondition::LiveEdgesForPairAre {
                source_chain_id: Uuid::from_u128(1),
                target_chain_id: Uuid::from_u128(2),
                uuids: vec![],
            },
            Precondition::LiveEdgesForRelationAre {
                source_chain_id: Uuid::from_u128(1),
                name: "DEPLOYED_IN".into(),
                uuids: vec![Uuid::from_u128(1)],
            },
            Precondition::OwnsCollection {
                collection: kg_core::models::CollectionRef {
                    namespace: "prod".into(),
                    source: "aws".into(),
                    key: "k".into(),
                },
                generation: 3,
                run_id: Uuid::from_u128(1),
            },
        ] {
            let write = super::precondition("org", &precondition).unwrap();
            assert_eq!(write.parameters["org"], json!("org"));
            assert_eq!(write.expected_rows, 1);
            assert!(write.statement.contains("RETURN true AS ok"));
        }
        assert!(
            super::precondition("org", &Precondition::NoLiveVersionFor { hashes: vec![] }).is_err()
        );
    }
}
