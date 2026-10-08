//! Interactive reads share search's scope and half-open temporal visibility.
use crate::{filters, PreparedQuery};
use kg_core::{
    errors::BackendError,
    search::SearchFilter,
    traits::graph_explorer::{ExplorerDirection, ExplorerQuery, ExplorerRequest},
};
use serde_json::json;

pub fn prepare(request: &ExplorerRequest) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    let filter = SearchFilter {
        org_id: request.org_id.clone(),
        namespaces: request.namespace.iter().cloned().collect(),
        as_of: request.as_of,
        entity_types: match &request.query {
            ExplorerQuery::CanvasNeighbors { entity_types, .. }
            | ExplorerQuery::Neighbors { entity_types, .. }
            | ExplorerQuery::Entities { entity_types } => entity_types.clone(),
            _ => Vec::new(),
        },
        ..Default::default()
    };
    let scoped = |alias: &str| {
        format!(
            "{} AND {}",
            filters::scope(alias, &filter),
            filters::entity_visible(alias, &filter)
        )
    };
    let project = |alias: &str| {
        let visible = crate::summary_search::visible(alias);
        let guarded = kg_core::entity_summary::DERIVED_PROPERTIES
            .iter()
            .filter(|field| **field != "summary_embedding")
            .map(|field| format!("{field}: CASE WHEN {visible} THEN {alias}.{field} ELSE null END"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{alias} {{ .*, embedding: null, summary_embedding: null, {guarded} }}")
    };
    let mut parameters = filters::params(&filter, &None, request.limit);
    parameters["offset"] = json!(request.offset);
    parameters["summary_policy"] = json!(kg_core::entity_summary::POLICY_VERSION);
    let identity = match &request.query {
        ExplorerQuery::Changes { .. }
        | ExplorerQuery::Community { .. }
        | ExplorerQuery::GraphRevision
        | ExplorerQuery::Catalog
        | ExplorerQuery::Filters { .. }
        | ExplorerQuery::Entities { .. }
        | ExplorerQuery::EntitiesByChains { .. }
        | ExplorerQuery::SnapshotObservations { .. }
        | ExplorerQuery::EntityVersion { .. }
        | ExplorerQuery::Snapshot { .. }
        | ExplorerQuery::NamespaceRelationships { .. }
        | ExplorerQuery::CanvasRelationship { .. }
        | ExplorerQuery::Relationship { .. } => None,
        ExplorerQuery::VersionHeaders {
            entity_type,
            chain_id,
        }
        | ExplorerQuery::CanvasEntity {
            entity_type,
            chain_id,
        }
        | ExplorerQuery::CanvasNeighbors {
            entity_type,
            chain_id,
            ..
        }
        | ExplorerQuery::Entity {
            entity_type,
            chain_id,
        }
        | ExplorerQuery::VersionHistory {
            entity_type,
            chain_id,
            ..
        }
        | ExplorerQuery::Versions {
            entity_type,
            chain_id,
        }
        | ExplorerQuery::Neighbors {
            entity_type,
            chain_id,
            ..
        } => Some((entity_type, chain_id)),
    };
    if let Some((entity_type, chain_id)) = identity {
        parameters["entity_type"] = json!(entity_type);
        parameters["chain_id"] = json!(chain_id);
    }
    let anchor = format!("MATCH (n:Entity {{org_id:$org_id,chain_id:$chain_id}}) WHERE n.entity_type=$entity_type AND {}", scoped("n"));
    let statement = match &request.query {
        ExplorerQuery::GraphRevision => "UNWIND range(0,63) AS stripe MATCH (r:GraphRevision {org_id:$org_id,stripe:stripe}) USING INDEX r:GraphRevision(org_id,stripe) RETURN {stripe:stripe,token:r.token} AS item ORDER BY stripe LIMIT 64".into(),
        ExplorerQuery::Community { uuid, member_offset, member_limit } => {
            parameters["community_uuid"] = json!(uuid);
            parameters["member_offset"] = json!(member_offset);
            parameters["member_limit"] = json!(member_limit + 1);
            parameters["at"] = json!(request.as_of.unwrap_or_else(chrono::Utc::now).to_rfc3339());
            format!(r#"MATCH (c:Community {{org_id:$org_id,uuid:$community_uuid}})
                WHERE {scope} AND coalesce(c.dirty,true)=false
                  AND datetime(c.projected_at)<=datetime($at)
                  AND (c.valid_until IS NULL OR datetime($at)<datetime(c.valid_until))
                  AND EXISTS {{ MATCH (publication:CommunityScope {{org_id:$org_id,namespace:c.namespace}})
                    WHERE publication.active_generation=c.generation_uuid }}
                CALL (c) {{
                    MATCH (c)-[membership:HAS_MEMBER]->(n:Entity {{org_id:$org_id,namespace:c.namespace}})
                    WHERE membership.generation_uuid=c.generation_uuid
                    WITH DISTINCT n ORDER BY n.chain_id,n.uuid SKIP $member_offset LIMIT $member_limit
                    RETURN collect(n{{entity_uuid:n.uuid,chain_id:n.chain_id,name:n.name,entity_type:n.entity_type}}) AS members
                }}
                RETURN c{{.uuid,.namespace,.name,.summary,.revision,.source_hash,.projected_at,.valid_until,
                    generation:c.generation_uuid,member_count:c.expected_member_count,members:members}} AS item"#,
                scope=filters::scope("c", &filter))
        }
        ExplorerQuery::Changes { from, to, chains, event_kinds } => {
            parameters["from"] = json!(from);
            parameters["to"] = json!(to);
            parameters["event_chains"] = json!(chains);
            parameters["event_kinds"] = json!(event_kinds);
            // Event endpoints are authorized by chain, even when currently tombstoned.
            // Physical repoints retain the edge UUID and do not create another event.
            format!(r#"CALL {{
                MATCH (n:Entity {{org_id:$org_id}}) WHERE {node_scope}
                  AND (size($event_chains)=0 OR n.chain_id IN $event_chains)
                UNWIND [{{kind:'entity_version',at:n.valid_from}},{{kind:'entity_deleted',at:n.deleted_at}}] AS event
                WITH n,event WHERE event.at IS NOT NULL AND datetime(event.at)>datetime($from) AND datetime(event.at)<=datetime($to)
                OPTIONAL MATCH (previous:Entity {{org_id:$org_id,chain_id:n.chain_id}})
                  WHERE previous.version=n.version-1 AND {previous_scope}
                RETURN {{kind:event.kind,effective_at:event.at,chain_id:n.chain_id,version_id:n.uuid,
                  before_version_id:CASE WHEN event.kind='entity_deleted' THEN n.uuid ELSE previous.uuid END,
                  after_version_id:CASE WHEN event.kind='entity_version' THEN n.uuid ELSE null END,
                  changed_properties:CASE WHEN event.kind='entity_version' THEN
                    [key IN keys(n) WHERE key STARTS WITH 'prop_' AND (previous IS NULL OR previous[key] IS NULL OR previous[key]<>n[key]) | substring(key,5)] +
                    [key IN coalesce(keys(previous),[]) WHERE key STARTS WITH 'prop_' AND n[key] IS NULL | substring(key,5)] ELSE [] END,
                  version:n.version,entity_type:n.entity_type,name:n.name,namespace:n.namespace,
                  observed_at:n.observed_at,created_at:n.created_at}} AS item
                UNION ALL
                MATCH (a:Entity {{org_id:$org_id}})-[r:RELATES_TO]->(b:Entity)
                WHERE r.org_id=$org_id AND {a_scope} AND {b_scope}
                  AND (size($event_chains)=0 OR r.source_chain_id IN $event_chains OR r.target_chain_id IN $event_chains)
                WITH DISTINCT r
                UNWIND [{{kind:'relationship_opened',at:r.valid_from}},{{kind:'relationship_closed',at:r.valid_to}},
                        {{kind:'relationship_deleted',at:r.deleted_at}},{{kind:'relationship_cancelled',at:r.cancelled_at}}] AS event
                WITH r,event WHERE event.at IS NOT NULL AND datetime(event.at)>datetime($from) AND datetime(event.at)<=datetime($to)
                RETURN {{kind:CASE WHEN event.kind='relationship_opened' AND r.previous_version_uuid IS NOT NULL THEN 'relationship_version' ELSE event.kind END,
                    effective_at:event.at,edge_id:r.uuid,name:r.name,
                    relationship_chain_id:r.chain_id,before_edge_id:r.previous_version_uuid,
                    after_edge_id:CASE WHEN event.kind='relationship_opened' THEN r.uuid ELSE null END,
                    time_basis:CASE WHEN event.kind='relationship_cancelled' THEN 'recorded_correction' ELSE 'effective_time' END,
                    source_chain_id:r.source_chain_id,target_chain_id:r.target_chain_id,
                    observed_at:r.observed_at,created_at:r.created_at}} AS item
              }} WITH DISTINCT item WHERE size($event_kinds)=0 OR item.kind IN $event_kinds RETURN item ORDER BY item.effective_at,item.kind,item.chain_id,item.version_id,item.edge_id
              SKIP $offset LIMIT $limit"#,
              node_scope=filters::scope("n",&filter),previous_scope=filters::scope("previous",&filter),a_scope=filters::scope("a",&filter),b_scope=filters::scope("b",&filter))
        },
        ExplorerQuery::Catalog => format!("MATCH (n:Entity {{org_id:$org_id}}) WHERE {} WITH n.namespace AS namespace,n.entity_type AS entity_type,count(*) AS count RETURN {{namespace:namespace,entity_type:entity_type,count:count}} AS item ORDER BY namespace,entity_type SKIP $offset LIMIT $limit",scoped("n")),
        ExplorerQuery::EntitiesByChains {chains} => {
            parameters["chains"] = json!(chains);
            format!("UNWIND $chains AS chain MATCH (n:Entity {{org_id:$org_id,chain_id:chain}}) WHERE {} RETURN n {{.chain_id,.uuid,.entity_type,.name,.namespace}} AS item LIMIT $limit",scoped("n"))
        }
        ExplorerQuery::EntityVersion { uuid } => {
            parameters["version_uuid"]=json!(uuid);
            format!("MATCH (n:Entity {{org_id:$org_id,uuid:$version_uuid}}) WHERE {} AND ($as_of IS NULL OR datetime(n.valid_from)<=datetime($as_of)) RETURN {} AS item LIMIT 1",filters::scope("n",&filter),project("n"))
        }
        ExplorerQuery::VersionHeaders {..} => format!("MATCH (n:Entity {{org_id:$org_id,chain_id:$chain_id}}) WHERE n.entity_type=$entity_type AND {} AND ($as_of IS NULL OR datetime(n.valid_from)<=datetime($as_of)) RETURN n {{.chain_id,.uuid,.entity_type,.name,.namespace,.version,.is_latest,.deleted_at,.valid_from,.valid_to}} AS item ORDER BY n.version,n.uuid SKIP $offset LIMIT $limit",filters::scope("n",&filter)),
        ExplorerQuery::Snapshot { uuid } => {
            parameters["snapshot_uuid"] = json!(uuid);
            format!("MATCH (snap:Snapshot {{org_id:$org_id,uuid:$snapshot_uuid}}) WHERE {} AND ($as_of IS NULL OR datetime(snap.captured_at)<=datetime($as_of)) RETURN snap {{.*,embedding:null,summary_embedding:null}} AS item LIMIT 1", filters::scope("snap",&filter))
        }
        ExplorerQuery::SnapshotObservations { versions } => {
            parameters["versions"] = json!(versions);
            format!("UNWIND $versions AS version MATCH (n:Entity {{org_id:$org_id,uuid:version}}) WHERE {}
                MATCH (snap:Snapshot)-[o:MENTIONS]->(n) WHERE o.org_id=$org_id AND {} AND ($as_of IS NULL OR (datetime(snap.captured_at)<=datetime($as_of) AND datetime(o.observed_at)<=datetime($as_of)))
                RETURN {{snapshot:snap {{.uuid,.name,.source,.namespace,.captured_at,.data_type}},observation:{{uuid:o.uuid,snapshot_uuid:snap.uuid,entity_uuid:n.uuid,chain_id:n.chain_id,entity_version:n.version,observed_at:o.observed_at}}}} AS item
                ORDER BY snap.captured_at DESC,snap.uuid,o.uuid SKIP $offset LIMIT $limit", scoped("n"),filters::scope("snap",&filter))
        }
        ExplorerQuery::CanvasRelationship {edge_id} | ExplorerQuery::Relationship {edge_id} => {
            let projection = if matches!(request.query, ExplorerQuery::CanvasRelationship {..}) { "r {.uuid,.name,.source_chain_id,.target_chain_id,.valid_from,.invalid_at,.origin,.source_property,.target_identity_field}" } else {"r { .*,embedding:null }"};
            parameters["edge_id"] = json!(edge_id);
            format!("MATCH ()-[r:RELATES_TO {{org_id:$org_id,uuid:$edge_id}}]->() WHERE {}
              MATCH (n:Entity {{org_id:$org_id,chain_id:r.source_chain_id}}) WHERE {}
              MATCH (m:Entity {{org_id:$org_id,chain_id:r.target_chain_id}}) WHERE {}
              RETURN DISTINCT {projection} AS item LIMIT 1", filters::relationship_visible("r",&filter),scoped("n"),scoped("m"))
        }
        ExplorerQuery::NamespaceRelationships {chains} => {
            parameters["visible_chains"] = json!(chains);
            format!("MATCH (n:Entity {{org_id:$org_id}}) WHERE n.chain_id IN $visible_chains AND {}
                MATCH (head:Entity {{org_id:$org_id,chain_id:n.chain_id}})-[r:RELATES_TO]->(other:Entity)
                WHERE r.org_id=$org_id AND other.org_id=$org_id AND other.chain_id IN $visible_chains AND {}
                WITH DISTINCT r,other.chain_id AS other_chain
                MATCH (m:Entity {{org_id:$org_id,chain_id:other_chain}}) WHERE {}
                RETURN {{entity:m {{.chain_id,.entity_type,.name,.namespace,.uuid,.version}},via:r.name,edge_id:r.uuid,src_chain:r.source_chain_id,dst_chain:r.target_chain_id,relationship:{{}} }} AS item ORDER BY r.uuid SKIP $offset LIMIT $limit",scoped("n"),filters::relationship_visible("r",&filter),scoped("m"))
        }
        ExplorerQuery::Entities {..} => format!("MATCH (n:Entity {{org_id:$org_id}}) WHERE {} AND {} RETURN n {{ .chain_id,.uuid,.entity_type,.name,.namespace,.version,.is_latest,.deleted_at }} AS item ORDER BY n.name,n.chain_id SKIP $offset LIMIT $limit",scoped("n"),filters::types("n",&filter)),
        ExplorerQuery::Filters {namespace_dimension, search} => {
            parameters["option_search"] = json!(search.to_lowercase());
            let field = if *namespace_dimension { "namespace" } else { "entity_type" };
            format!("MATCH (n:Entity {{org_id:$org_id}}) WHERE {} AND toLower(n.{field}) CONTAINS $option_search WITH n.{field} AS value,count(*) AS count RETURN {{value:value,count:count}} AS item ORDER BY value SKIP $offset LIMIT $limit", scoped("n"))
        }
        ExplorerQuery::CanvasEntity {..} => format!("{anchor} RETURN n {{.chain_id,.uuid,.entity_type,.name,.namespace,.version,.is_latest,.deleted_at,.valid_from,.valid_to,.source}} AS item ORDER BY n.version DESC,n.uuid LIMIT 1"),
        ExplorerQuery::Entity {..} => format!("{anchor} RETURN {} AS item ORDER BY n.version DESC,n.uuid LIMIT 1",project("n")),
        ExplorerQuery::VersionHistory {from,to,newest_first,..} => {
            parameters["history_from"]=json!(from);parameters["history_to"]=json!(to);
            let order=if *newest_first {"DESC"} else {"ASC"};
            format!("MATCH (n:Entity {{org_id:$org_id,chain_id:$chain_id}}) WHERE n.entity_type=$entity_type AND {} AND ($as_of IS NULL OR datetime(n.valid_from)<=datetime($as_of)) AND ($history_from IS NULL OR datetime(n.valid_from)>=datetime($history_from)) AND ($history_to IS NULL OR datetime(n.valid_from)<=datetime($history_to)) RETURN {} AS item ORDER BY n.version {order},n.uuid {order} SKIP $offset LIMIT $limit",filters::scope("n",&filter),project("n"))
        }
        ExplorerQuery::Versions {..} => format!("MATCH (n:Entity {{org_id:$org_id,chain_id:$chain_id}}) WHERE n.entity_type=$entity_type AND {} AND ($as_of IS NULL OR datetime(n.valid_from)<=datetime($as_of)) RETURN {} AS item ORDER BY n.version,n.uuid SKIP $offset LIMIT $limit",filters::scope("n",&filter),project("n")),
        ExplorerQuery::CanvasNeighbors {direction,..} | ExplorerQuery::Neighbors {direction,..} => {
            let compact = matches!(request.query, ExplorerQuery::CanvasNeighbors { .. });
            let entity_projection = if compact { "m {.chain_id,.uuid,.entity_type,.name,.namespace,.version}".into() } else { project("m") };
            let relationship_projection = if compact { "r {.uuid,.name,.source_chain_id,.target_chain_id,.valid_from,.valid_to}" } else { "r {.*,embedding:null}" };
            let direction = match direction {
                ExplorerDirection::Both => "true",
                ExplorerDirection::Out => "r.source_chain_id=n.chain_id",
                ExplorerDirection::In => "r.target_chain_id=n.chain_id",
            };
            // Physical endpoints follow current heads; hydrate the visible version by chain.
            format!("{anchor}
                MATCH (head:Entity {{org_id:$org_id,chain_id:n.chain_id}})-[r:RELATES_TO]-(other:Entity)
                WHERE r.org_id=$org_id AND other.org_id=$org_id AND {direction} AND {}
                WITH DISTINCT n,r,other.chain_id AS other_chain
                MATCH (m:Entity {{org_id:$org_id,chain_id:other_chain}}) WHERE {} AND {}
                RETURN {{entity:{},via:r.name,edge_id:r.uuid,src_chain:r.source_chain_id,dst_chain:r.target_chain_id,
                    relationship:{relationship_projection} }} AS item
                ORDER BY m.name,m.chain_id,r.uuid SKIP $offset LIMIT $limit",
                filters::relationship_visible("r",&filter),scoped("m"),filters::types("m",&filter),entity_projection)
        }
    };
    Ok(PreparedQuery {
        statement,
        parameters,
    })
}

/// Combine homogeneous entity/neighbor reads using one parameterized frontier query.
/// Each anchor gets the shared page limit and offset; pagination never crosses anchors.
pub fn prepare_batch(requests: &[ExplorerRequest]) -> Result<PreparedQuery, BackendError> {
    if requests.is_empty() || requests.len() > 32 {
        return Err(BackendError::Query("invalid explorer batch".into()));
    }
    let mut first = prepare(&requests[0])?;
    let inner = first
        .statement
        .replace("$chain_id", "anchor.chain_id")
        .replace("$entity_type AND", "anchor.entity_type AND");
    let mut anchors = Vec::new();
    for request in requests {
        let (entity_type, chain_id) = match &request.query {
            ExplorerQuery::VersionHeaders {
                entity_type,
                chain_id,
            }
            | ExplorerQuery::CanvasEntity {
                entity_type,
                chain_id,
            }
            | ExplorerQuery::CanvasNeighbors {
                entity_type,
                chain_id,
                ..
            }
            | ExplorerQuery::Entity {
                entity_type,
                chain_id,
            }
            | ExplorerQuery::Neighbors {
                entity_type,
                chain_id,
                ..
            } => (entity_type, chain_id),
            _ => {
                return Err(BackendError::Query(
                    "unsupported explorer batch query".into(),
                ))
            }
        };
        let mut query = prepare(request)?;
        // A batch is one read instant; params() otherwise supplies a fresh now per anchor.
        query.parameters["relationship_now"] = first.parameters["relationship_now"].clone();
        let mut expected = first.parameters.clone();
        for key in ["chain_id", "entity_type"] {
            query.parameters.as_object_mut().unwrap().remove(key);
            expected.as_object_mut().unwrap().remove(key);
        }
        if query.statement != first.statement || query.parameters != expected {
            return Err(BackendError::Query(
                "explorer batch must share scope and limits".into(),
            ));
        }
        anchors
            .push(json!({"chain_id":chain_id,"entity_type":entity_type,"offset":request.offset}));
    }
    first.parameters["anchors"] = json!(anchors);
    first.statement = format!("UNWIND $anchors AS anchor CALL (anchor) {{ CALL (anchor) {{ {inner} }} RETURN collect(item) AS items }} RETURN anchor.chain_id AS anchor,items");
    Ok(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canvas_projects_in_storage_without_changing_endpoint_visibility() {
        let request = ExplorerRequest {
            org_id: "org".into(),
            namespace: Some("scope".into()),
            as_of: Some(chrono::Utc::now()),
            limit: 50,
            offset: 0,
            query: ExplorerQuery::CanvasNeighbors {
                entity_type: "Service".into(),
                chain_id: uuid::Uuid::new_v4(),
                direction: ExplorerDirection::Both,
                entity_types: vec![],
            },
        };
        let query = prepare(&request).unwrap();
        assert!(!query.statement.contains(".*"));
        assert!(query.statement.contains("source_chain_id"));
        assert!(query.statement.contains("$as_of"));
        assert!(query.statement.contains("m.org_id"));
        let mut detail = request.clone();
        if let ExplorerQuery::CanvasNeighbors {
            entity_type,
            chain_id,
            direction,
            entity_types,
        } = request.query
        {
            detail.query = ExplorerQuery::Neighbors {
                entity_type,
                chain_id,
                direction,
                entity_types,
            };
        }
        assert!(prepare(&detail).unwrap().statement.contains(".*"));
    }
    #[test]
    fn homogeneous_batch_keeps_shared_parameters() {
        let r = ExplorerRequest {
            org_id: "graph-demo".into(),
            namespace: Some("demo".into()),
            as_of: Some(chrono::Utc::now()),
            limit: 60,
            offset: 0,
            query: ExplorerQuery::Entity {
                entity_type: "AWS::EC2::Instance".into(),
                chain_id: uuid::Uuid::new_v4(),
            },
        };
        let q = prepare_batch(std::slice::from_ref(&r)).unwrap();
        assert!(q.statement.contains("anchor.entity_type"));
        let mut r = r;
        r.query = ExplorerQuery::Neighbors {
            entity_type: "AWS::EC2::Instance".into(),
            chain_id: uuid::Uuid::new_v4(),
            direction: ExplorerDirection::Both,
            entity_types: vec!["AWS::EC2::Subnet".into()],
        };
        let q = prepare_batch(&[r]).unwrap();
        assert!(q.statement.contains("$types"));
    }
    #[test]
    fn caller_text_is_bound_and_history_uses_visible_chain_versions() {
        let request = ExplorerRequest {
            org_id: "org' OR true".into(),
            namespace: Some("prod".into()),
            as_of: Some(chrono::Utc::now()),
            limit: 20,
            offset: 0,
            query: ExplorerQuery::Neighbors {
                entity_type: "AWS::EC2::Instance".into(),
                chain_id: uuid::Uuid::new_v4(),
                direction: ExplorerDirection::Both,
                entity_types: Vec::new(),
            },
        };
        let query = prepare(&request).unwrap();
        assert!(!query.statement.contains(&request.org_id));
        assert!(query.statement.contains("r.cancelled_at IS NULL"));
        assert!(query.statement.contains("ChainMerge"));
        assert!(query.statement.contains("chain_id:other_chain"));
        assert_eq!(query.parameters["limit"], 21);
        assert!(
            !query.statement.contains("m.entity_type IN $types"),
            "no type filter without types"
        );
        // Neighbour types filter the neighbour, never the anchor.
        let mut typed = request.clone();
        typed.query = ExplorerQuery::Neighbors {
            entity_type: "AWS::EC2::Instance".into(),
            chain_id: uuid::Uuid::new_v4(),
            direction: ExplorerDirection::Both,
            entity_types: vec!["AWS::EC2::Volume".into(), "AWS::S3::Bucket".into()],
        };
        let query = prepare(&typed).unwrap();
        assert!(query.statement.contains("m.entity_type IN $types"));
        assert_eq!(
            query.parameters["types"],
            serde_json::json!(["AWS::EC2::Volume", "AWS::S3::Bucket"])
        );
        assert!(!query.statement.contains("n.entity_type IN $types"));
        let mut too_many = request.clone();
        too_many.query = ExplorerQuery::Neighbors {
            entity_type: "AWS::EC2::Instance".into(),
            chain_id: uuid::Uuid::new_v4(),
            direction: ExplorerDirection::Both,
            entity_types: (0..33).map(|i| format!("T{i}")).collect(),
        };
        assert!(prepare(&too_many).is_err());
    }
}
