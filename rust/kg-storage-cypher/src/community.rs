//! Scoped Community projections, invisible generation staging and guarded publication.
use crate::{PreparedQuery, PreparedWrite};
use chrono::{DateTime, Utc};
use kg_core::{community::*, errors::BackendError, traits::GraphProperties};
use serde_json::{json, Value};
use uuid::Uuid;

pub fn read(org: &str, request: &CommunityRead) -> Result<PreparedQuery, BackendError> {
    request.validate(org)?;
    let temporal = entity_boundaries();
    let properties = entity_properties();
    let summary = entity_summary_expression();
    let mut p = json!({"org":org,"namespace":request.namespace(),"summary_policy":kg_core::entity_summary::POLICY_VERSION});
    let statement = match request {
        CommunityRead::Scopes { chain_ids } => {
            p["chains"] = json!(chain_ids);
            "UNWIND $chains AS chain CALL (chain) { MATCH (n:Entity {org_id:$org,chain_id:chain}) RETURN n.namespace AS namespace ORDER BY n.version DESC,n.uuid DESC LIMIT 1 } RETURN chain AS chain_id,namespace ORDER BY chain_id".into()
        }
        CommunityRead::State { .. } => STATE.into(),
        CommunityRead::EntityPage {
            at,
            after_uuid,
            limit,
            ..
        } => {
            p["at"] = json!(at.to_rfc3339());
            p["after"] = json!(after_uuid.map(|v| v.to_string()).unwrap_or_default());
            p["limit"] = json!(limit + 1);
            p["text_limit"] = json!(MAX_ENTITY_TEXT_BYTES + 1);
            let filter = kg_core::search::SearchFilter {
                as_of: Some(*at),
                ..Default::default()
            };
            p["as_of"] = p["at"].clone();
            p["org_id"] = json!(org);
            format!("MATCH (n:Entity {{org_id:$org,namespace:$namespace}}) USING INDEX n:Entity(org_id,namespace,uuid) WHERE n.uuid>$after WITH n ORDER BY n.org_id,n.namespace,n.uuid LIMIT $limit
                {temporal}
                {properties}
                RETURN source_properties,properties_oversized,n.uuid AS cursor,merge_scanned+incident_scanned AS auxiliary_scanned_rows, {} AS visible, n.uuid AS uuid,n.chain_id AS chain_id,substring(n.name,0,$text_limit) AS name,substring(n.entity_type,0,$text_limit) AS entity_type,
                substring({summary},0,$text_limit) AS summary,
                [n.valid_from,n.valid_to,n.invalid_at,n.deleted_at,n.summary_valid_until,incident_boundary]+merge_boundaries AS boundaries ORDER BY cursor",crate::filters::entity_visible("n",&filter))
        }
        CommunityRead::RelationshipPage {
            at, after, limit, ..
        } => {
            p["at"] = json!(at.to_rfc3339());
            p["after_source"] = json!(after
                .as_ref()
                .map(|c| c.source_uuid.to_string())
                .unwrap_or_default());
            p["after_edge"] = json!(after
                .as_ref()
                .map(|c| c.edge_uuid.to_string())
                .unwrap_or_default());
            p["limit"] = json!(limit + 1);
            p["source_limit"] = json!((limit + 1).min(32));
            p["source_end"] = json!(Uuid::from_u128(u128::MAX));
            let filter = kg_core::search::SearchFilter {
                as_of: Some(*at),
                ..Default::default()
            };
            p["as_of"] = p["at"].clone();
            p["org_id"] = json!(org);
            format!("MATCH (s:Entity {{org_id:$org,namespace:$namespace}}) USING INDEX s:Entity(org_id,namespace,uuid)
                WHERE s.uuid>=$after_source AND (s.uuid>$after_source OR $after_edge<>$source_end)
                WITH s ORDER BY s.org_id,s.namespace,s.uuid LIMIT $source_limit
                CALL (s) {{ OPTIONAL MATCH (owner:Entity)-[r:RELATES_TO]->(t:Entity)
                    USING INDEX r:RELATES_TO(org_id,source_chain_id,uuid)
                    WHERE r.org_id=$org AND r.source_chain_id=s.chain_id AND r.uuid>CASE WHEN s.uuid=$after_source THEN $after_edge ELSE '' END AND owner.uuid=s.uuid AND owner.org_id=$org AND t.org_id=$org AND t.namespace=$namespace
                    RETURN r,t ORDER BY r.org_id,r.source_chain_id,r.uuid LIMIT $limit }}
                WITH s,r,t ORDER BY s.uuid,r.uuid LIMIT $limit
                RETURN s.uuid AS source_uuid,r.uuid AS uuid,coalesce(r.uuid,$source_end) AS cursor_edge,s.chain_id AS source_chain_id,t.chain_id AS target_chain_id,
                r IS NOT NULL AND {} AND {} AND {} AS visible,[r.valid_from,r.valid_to,r.invalid_at,r.deleted_at,r.cancelled_at] AS boundaries ORDER BY source_uuid,cursor_edge",crate::filters::entity_visible("s",&filter),crate::filters::entity_visible("t",&filter),crate::filters::relationship_visible("r",&filter))
        }
        CommunityRead::EntitiesByChains {
            at,
            chain_ids,
            after_uuid,
            limit,
            ..
        } => {
            let mut query = read(
                org,
                &CommunityRead::EntityPage {
                    namespace: request.namespace().into(),
                    at: *at,
                    after_uuid: *after_uuid,
                    limit: *limit,
                },
            )?;
            query.parameters["chains"] = json!(chain_ids);
            query.statement=query.statement.replace("MATCH (n:Entity {org_id:$org,namespace:$namespace}) USING INDEX n:Entity(org_id,namespace,uuid) WHERE n.uuid>$after", "UNWIND $chains AS chain MATCH (n:Entity {org_id:$org,chain_id:chain}) WHERE n.namespace=$namespace AND n.uuid>$after");
            return Ok(query);
        }
        CommunityRead::Neighbors {
            at,
            chain_ids,
            after_edge_uuid,
            limit,
            ..
        } => {
            p["at"] = json!(at.to_rfc3339());
            p["as_of"] = p["at"].clone();
            p["org_id"] = json!(org);
            p["chains"] = json!(chain_ids);
            p["after"] = json!(after_edge_uuid.map(|id| id.to_string()).unwrap_or_default());
            p["limit"] = json!(limit + 1);
            p["text_limit"] = json!(MAX_ENTITY_TEXT_BYTES + 1);
            let filter = kg_core::search::SearchFilter {
                as_of: Some(*at),
                ..Default::default()
            };
            format!("CALL {{
 UNWIND $chains AS chain CALL (chain) {{ MATCH (s:Entity)-[r:RELATES_TO]->(t:Entity) USING INDEX r:RELATES_TO(org_id,source_chain_id,uuid) WHERE r.org_id=$org AND r.source_chain_id=chain AND r.uuid>$after AND s.org_id=$org AND t.org_id=$org AND s.namespace=$namespace AND t.namespace=$namespace RETURN r ORDER BY r.org_id,r.source_chain_id,r.uuid LIMIT $limit }} RETURN r
 UNION UNWIND $chains AS chain CALL (chain) {{ MATCH (s:Entity)-[r:RELATES_TO]->(t:Entity) USING INDEX r:RELATES_TO(org_id,target_chain_id,uuid) WHERE r.org_id=$org AND r.target_chain_id=chain AND r.uuid>$after AND s.org_id=$org AND t.org_id=$org AND s.namespace=$namespace AND t.namespace=$namespace RETURN r ORDER BY r.org_id,r.target_chain_id,r.uuid LIMIT $limit }} RETURN r }} WITH r ORDER BY r.uuid LIMIT $limit WITH collect(r) AS edges
                CALL (edges) {{ UNWIND edges AS edge UNWIND [startNode(edge),endNode(edge)] AS n WITH DISTINCT n {temporal} {properties}
                    RETURN collect({{source_properties:source_properties,properties_oversized:properties_oversized,uuid:n.uuid,chain_id:n.chain_id,name:substring(n.name,0,$text_limit),entity_type:substring(n.entity_type,0,$text_limit),summary:substring({summary},0,$text_limit),boundaries:[n.valid_from,n.valid_to,n.invalid_at,n.deleted_at,n.summary_valid_until,incident_boundary]+merge_boundaries,auxiliary_scanned_rows:merge_scanned+incident_scanned}}) AS projected_entities }}
                UNWIND edges AS r WITH r,startNode(r) AS s,endNode(r) AS t,projected_entities
                WITH r,s,t,[node IN projected_entities WHERE node.uuid=s.uuid OR node.uuid=t.uuid] AS endpoints
                RETURN r.uuid AS uuid,s.chain_id AS source_chain_id,t.chain_id AS target_chain_id,
                {} AND {} AND {} AS visible,
                [r.valid_from,r.valid_to,r.invalid_at,r.deleted_at,r.cancelled_at,s.valid_from,s.valid_to,s.invalid_at,s.deleted_at,t.valid_from,t.valid_to,t.invalid_at,t.deleted_at] AS boundaries,
                endpoints ORDER BY uuid",crate::filters::entity_visible("s",&filter),crate::filters::entity_visible("t",&filter),crate::filters::relationship_visible("r",&filter))
        }
        CommunityRead::Memberships {
            generation,
            chain_ids,
            limit,
            ..
        } => {
            p["generation"] = json!(generation);
            p["chains"] = json!(chain_ids);
            p["limit"] = json!(limit + 1);
            "UNWIND $chains AS chain MATCH (c:Community {org_id:$org,namespace:$namespace,generation_uuid:$generation})-[m:HAS_MEMBER]->(n:Entity {org_id:$org}) USING INDEX m:HAS_MEMBER(generation_uuid,chain_id) WHERE m.generation_uuid=$generation AND m.chain_id=chain RETURN c.uuid AS community_uuid,c.revision AS community_revision,n.uuid AS entity_uuid,m.chain_id AS chain_id ORDER BY chain_id LIMIT $limit".into()
        }
        CommunityRead::Members {
            generation,
            community_uuid,
            after_chain_id,
            limit,
            ..
        } => {
            p["generation"] = json!(generation);
            p["community"] = json!(community_uuid);
            p["after"] = json!(after_chain_id.map(|v| v.to_string()).unwrap_or_default());
            p["limit"] = json!(limit + 1);
            "MATCH (c:Community {org_id:$org,namespace:$namespace,generation_uuid:$generation,uuid:$community})-[m:HAS_MEMBER]->(n:Entity {org_id:$org,namespace:$namespace}) USING INDEX m:HAS_MEMBER(community_uuid,chain_id) WHERE m.community_uuid=$community AND m.chain_id>$after RETURN n.uuid AS entity_uuid,m.chain_id AS chain_id ORDER BY m.community_uuid,m.chain_id LIMIT $limit".into()
        }
        CommunityRead::Community {
            generation,
            community_uuid,
            ..
        } => {
            p["generation"] = json!(generation);
            p["community"] = json!(community_uuid);
            "MATCH (c:Community {org_id:$org,namespace:$namespace,generation_uuid:$generation,uuid:$community}) RETURN c{.*} AS community LIMIT 2".into()
        }
    };
    Ok(PreparedQuery {
        statement,
        parameters: p,
    })
}
// Bound native payloads before returning them; rendering enforces the UTF-8 byte limit.
fn entity_properties() -> &'static str {
    r#"CALL (n) {
        WITH n,[key IN keys(n) WHERE key STARTS WITH 'prop_' OR key STARTS WITH 'property_type_'] AS source_keys
        WITH n,source_keys,reduce(total=0,key IN source_keys | CASE WHEN total>$text_limit THEN total ELSE total+size(key)+8+
            CASE WHEN n[key] IS :: LIST<ANY> THEN reduce(items=2,value IN n[key] | CASE WHEN items>$text_limit THEN items ELSE items+size(coalesce(toStringOrNull(value),''))+4 END)
            ELSE size(coalesce(toStringOrNull(n[key]),''))+4 END END) AS source_size
        RETURN source_size>$text_limit AS properties_oversized,
            CASE WHEN source_size>$text_limit THEN [] ELSE [key IN source_keys | {key:key,value:n[key]}] END AS source_properties
    }"#
}
fn source_properties(row: &GraphProperties) -> Result<GraphProperties, BackendError> {
    if row.get("properties_oversized").and_then(Value::as_bool) != Some(false) {
        return Err(bad(
            "Community source property payload exceeds its read bound",
        ));
    }
    let values = row
        .get("source_properties")
        .and_then(Value::as_array)
        .ok_or_else(|| bad("missing Community source properties"))?;
    let mut result = GraphProperties::new();
    for field in values {
        let key = field
            .get("key")
            .and_then(Value::as_str)
            .filter(|key| {
                key.starts_with(kg_core::traits::graph_mutation::USER_PROPERTY_PREFIX)
                    || key.starts_with(kg_core::traits::property_codec::TYPE_PREFIX)
            })
            .ok_or_else(|| bad("invalid Community source property name"))?;
        let value = field
            .get("value")
            .ok_or_else(|| bad("missing Community source property value"))?;
        if result.insert(key.to_owned(), value.clone()).is_some() {
            return Err(bad("duplicate Community source property"));
        }
    }
    Ok(result)
}
fn entity_boundaries() -> &'static str {
    r#"                CALL (n) { OPTIONAL MATCH (merge:ChainMerge {org_id:$org,loser_chain_id:n.chain_id}) RETURN collect(merge.valid_from)+collect(merge.valid_to) AS merge_boundaries,count(merge) AS merge_scanned }
                CALL (n) { OPTIONAL MATCH (n)-[incident:RELATES_TO {org_id:$org}]-(peer:Entity {org_id:$org,namespace:$namespace})
                    WITH incident,[value IN [incident.valid_from,incident.valid_to,incident.invalid_at,incident.deleted_at,peer.valid_from,peer.valid_to,peer.invalid_at,peer.deleted_at] WHERE value IS NOT NULL AND datetime(value)>datetime($at)] AS future
                    RETURN toString(min(datetime(reduce(first=null,value IN future | CASE WHEN first IS NULL OR datetime(value)<datetime(first) THEN value ELSE first END)))) AS incident_boundary,count(incident) AS incident_scanned }
"#
}
fn entity_summary_expression() -> &'static str {
    "CASE WHEN n.derived_summary IS NOT NULL AND n.summary_policy_version=$summary_policy AND datetime(n.summary_as_of)<=datetime($at) AND (n.summary_valid_until IS NULL OR datetime($at)<datetime(n.summary_valid_until)) THEN n.derived_summary ELSE CASE WHEN n.summary IS :: STRING NOT NULL THEN n.summary ELSE '' END END"
}
const STATE:&str="UNWIND range(0,31) AS stripe OPTIONAL MATCH (r:CommunityRevision {org_id:$org,namespace:$namespace,stripe:stripe}) WITH stripe,coalesce(r.revision,0) AS revision ORDER BY stripe WITH collect(revision) AS revisions OPTIONAL MATCH (s:CommunityScope {org_id:$org,namespace:$namespace}) RETURN revisions,s.active_generation AS generation,s.publication_revision AS publication";
fn parse<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, BackendError> {
    serde_json::from_value(value).map_err(|_| bad("invalid stored Community record"))
}
fn text(row: &GraphProperties, key: &str) -> Result<String, BackendError> {
    row.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| bad("missing Community text"))
}
fn uid(row: &GraphProperties, key: &str) -> Result<Uuid, BackendError> {
    text(row, key)?
        .parse()
        .map_err(|_| bad("invalid Community UUID"))
}
fn boundary(
    rows: &[GraphProperties],
    at: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, BackendError> {
    let mut next = None;
    for row in rows {
        for endpoint in row
            .get("endpoints")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(endpoint) = endpoint.as_object() {
                if let Some(time) = boundary(std::slice::from_ref(endpoint), at)? {
                    if next.is_none_or(|old| time < old) {
                        next = Some(time);
                    }
                }
            }
        }
        for value in row
            .get("boundaries")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|v| !v.is_null())
        {
            let time: DateTime<Utc> = parse(value.clone())?;
            if time > at && next.is_none_or(|old| time < old) {
                next = Some(time);
            }
        }
    }
    Ok(next)
}
pub fn decode(
    request: &CommunityRead,
    mut rows: Vec<GraphProperties>,
) -> Result<CommunityReadResult, BackendError> {
    let limit = match request {
        CommunityRead::EntitiesByChains { limit, .. }
        | CommunityRead::Neighbors { limit, .. }
        | CommunityRead::EntityPage { limit, .. }
        | CommunityRead::RelationshipPage { limit, .. }
        | CommunityRead::Memberships { limit, .. }
        | CommunityRead::Members { limit, .. } => *limit,
        CommunityRead::Scopes { chain_ids } => chain_ids.len(),
        _ => 1,
    };
    let exhausted = rows.len() <= limit;
    if !exhausted {
        rows.truncate(limit);
    }
    let mut auxiliary_scanned_rows = 0usize;
    let mut counted_endpoints = std::collections::BTreeSet::new();
    for row in &rows {
        auxiliary_scanned_rows = auxiliary_scanned_rows
            .checked_add(
                row.get("auxiliary_scanned_rows")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as usize,
            )
            .ok_or_else(|| bad("Community scan count overflow"))?;
        for endpoint in row
            .get("endpoints")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if counted_endpoints.insert(
                endpoint
                    .get("uuid")
                    .cloned()
                    .unwrap_or(Value::Null)
                    .to_string(),
            ) {
                auxiliary_scanned_rows = auxiliary_scanned_rows
                    .checked_add(
                        endpoint
                            .get("auxiliary_scanned_rows")
                            .and_then(Value::as_u64)
                            .unwrap_or(0) as usize,
                    )
                    .ok_or_else(|| bad("Community scan count overflow"))?;
            }
        }
    }
    match request {
        CommunityRead::Scopes { .. } => Ok(CommunityReadResult::Scopes(
            rows.into_iter()
                .map(|r| {
                    Ok(CommunityScope {
                        chain_id: uid(&r, "chain_id")?,
                        namespace: text(&r, "namespace")?,
                    })
                })
                .collect::<Result<Vec<_>, BackendError>>()?,
        )),
        CommunityRead::State { namespace } => {
            if rows.len() != 1 || !exhausted {
                return Err(bad("Community state missing"));
            }
            let row = &rows[0];
            let state = CommunityState {
                namespace: namespace.clone(),
                source_revision: CommunityRevision(parse(
                    row.get("revisions").cloned().unwrap_or(Value::Null),
                )?),
                active_generation: parse(row.get("generation").cloned().unwrap_or(Value::Null))?,
                publication_revision: parse(
                    row.get("publication").cloned().unwrap_or(Value::Null),
                )?,
            };
            Ok(CommunityReadResult::State(state))
        }
        CommunityRead::EntityPage { at, .. } | CommunityRead::EntitiesByChains { at, .. } => {
            let next = rows.last().map(|r| uid(r, "uuid")).transpose()?;
            let scanned_rows = rows.len();
            let next_temporal_boundary = boundary(&rows, *at)?;
            let mut records = Vec::new();
            for row in &rows {
                if row.get("visible") != Some(&json!(true)) {
                    continue;
                }
                let name = text(row, "name")?;
                let entity_type = text(row, "entity_type")?;
                let summary = text(row, "summary")?;
                let text = entity_text(&name, &entity_type, &summary, &source_properties(row)?)?;
                records.push(CommunityEntity {
                    uuid: uid(row, "uuid")?,
                    chain_id: uid(row, "chain_id")?,
                    name,
                    entity_type,
                    text_hash: text_hash(&text),
                    text,
                    valid_until: boundary(std::slice::from_ref(row), *at)?,
                });
            }
            Ok(CommunityReadResult::Entities(CommunityPage {
                records,
                next,
                exhausted,
                scanned_rows,
                auxiliary_scanned_rows,
                next_temporal_boundary,
            }))
        }
        CommunityRead::RelationshipPage { at, .. } => {
            let next = rows
                .last()
                .map(|r| {
                    Ok(RelationshipCursor {
                        source_uuid: uid(r, "source_uuid")?,
                        edge_uuid: uid(r, "cursor_edge")?,
                    })
                })
                .transpose()?;
            let scanned_rows = rows.len();
            let next_temporal_boundary = boundary(&rows, *at)?;
            let records = rows
                .iter()
                .filter(|r| r.get("visible") == Some(&json!(true)))
                .map(|r| {
                    Ok(CommunityRelationship {
                        uuid: uid(r, "uuid")?,
                        source_chain_id: uid(r, "source_chain_id")?,
                        target_chain_id: uid(r, "target_chain_id")?,
                        valid_until: boundary(std::slice::from_ref(r), *at)?,
                    })
                })
                .collect::<Result<Vec<_>, BackendError>>()?;
            Ok(CommunityReadResult::Relationships(CommunityPage {
                records,
                next,
                exhausted: rows.is_empty(),
                scanned_rows,
                auxiliary_scanned_rows,
                next_temporal_boundary,
            }))
        }
        CommunityRead::Neighbors { at, .. } => {
            let scanned_rows = rows.len();
            let next_temporal_boundary = boundary(&rows, *at)?;
            let mut entities = std::collections::BTreeMap::new();
            let mut relationships = Vec::new();
            for row in &rows {
                let valid_until = boundary(std::slice::from_ref(row), *at)?;
                if row.get("visible") != Some(&json!(true)) {
                    continue;
                }
                relationships.push(CommunityRelationship {
                    uuid: uid(row, "uuid")?,
                    source_chain_id: uid(row, "source_chain_id")?,
                    target_chain_id: uid(row, "target_chain_id")?,
                    valid_until,
                });
                for endpoint in row
                    .get("endpoints")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let e = endpoint
                        .as_object()
                        .ok_or_else(|| bad("invalid Community endpoint"))?;
                    let name = text(e, "name")?;
                    let entity_type = text(e, "entity_type")?;
                    let text = entity_text(
                        &name,
                        &entity_type,
                        &text(e, "summary")?,
                        &source_properties(e)?,
                    )?;
                    let uuid = uid(e, "uuid")?;
                    entities.insert(
                        uuid,
                        CommunityEntity {
                            uuid,
                            chain_id: uid(e, "chain_id")?,
                            name,
                            entity_type,
                            text_hash: text_hash(&text),
                            text,
                            valid_until: boundary(std::slice::from_ref(e), *at)?,
                        },
                    );
                }
            }
            Ok(CommunityReadResult::Neighbors(CommunityNeighborhood {
                next: rows.last().map(|row| uid(row, "uuid")).transpose()?,
                entities: entities.into_values().collect(),
                relationships,
                scanned_rows,
                auxiliary_scanned_rows,
                exhausted,
                next_temporal_boundary,
            }))
        }
        CommunityRead::Memberships { .. } => {
            let next = rows.last().map(|r| uid(r, "chain_id")).transpose()?;
            let scanned_rows = rows.len();
            let records = rows
                .into_iter()
                .map(|r| {
                    Ok(CommunityMembership {
                        community_uuid: uid(&r, "community_uuid")?,
                        community_revision: uid(&r, "community_revision")?,
                        member: CommunityMember {
                            entity_uuid: uid(&r, "entity_uuid")?,
                            chain_id: uid(&r, "chain_id")?,
                        },
                    })
                })
                .collect::<Result<Vec<_>, BackendError>>()?;
            Ok(CommunityReadResult::Memberships(CommunityPage {
                records,
                next,
                exhausted,
                scanned_rows,
                auxiliary_scanned_rows,
                next_temporal_boundary: None,
            }))
        }
        CommunityRead::Members { .. } => {
            let next = rows.last().map(|r| uid(r, "chain_id")).transpose()?;
            let scanned_rows = rows.len();
            let records = rows
                .into_iter()
                .map(|r| {
                    Ok(CommunityMember {
                        entity_uuid: uid(&r, "entity_uuid")?,
                        chain_id: uid(&r, "chain_id")?,
                    })
                })
                .collect::<Result<Vec<_>, BackendError>>()?;
            Ok(CommunityReadResult::Members(CommunityPage {
                records,
                next,
                exhausted,
                scanned_rows,
                auxiliary_scanned_rows,
                next_temporal_boundary: None,
            }))
        }
        CommunityRead::Community { generation, .. } => {
            if !exhausted {
                return Err(bad("duplicate Community UUID"));
            }
            let record = rows
                .first()
                .map(|r| {
                    decode_stored(
                        *generation,
                        r.get("community").cloned().unwrap_or(Value::Null),
                    )
                })
                .transpose()?;
            Ok(CommunityReadResult::Community(record))
        }
    }
}
fn decode_stored(generation: Uuid, value: Value) -> Result<StoredCommunity, BackendError> {
    let mut node = value.clone();
    node["name_embedding"] = if value.get("name_embedding").is_some()
        && value
            .get("name_embedding_text_version")
            .and_then(Value::as_str)
            == Some(NAME_TEXT_VERSION)
        && value
            .get("name_embedding_content_hash")
            .and_then(Value::as_str)
            == value
                .get("name")
                .and_then(Value::as_str)
                .map(kg_core::embedding::content_hash)
                .as_deref()
    {
        json!({"model":value["name_embedding_model"],"values":value["name_embedding"]})
    } else {
        Value::Null
    };
    Ok(StoredCommunity {
        generation,
        dirty: value["dirty"].as_bool().unwrap_or(true),
        definition: CommunityDefinition {
            node: parse(node)?,
            revision: parse(value["revision"].clone())?,
            expected_member_count: parse(value["expected_member_count"].clone())?,
            source_hash: parse(value["source_hash"].clone())?,
            projected_at: parse(value["projected_at"].clone())?,
            valid_until: parse(value["valid_until"].clone())?,
        },
    })
}
fn bad(message: &str) -> BackendError {
    BackendError::Deserialization(message.into())
}

fn write(statement: String, parameters: Value) -> PreparedWrite {
    PreparedWrite {
        statement: format!(
            "CALL {{ {statement} }} RETURN count(*)=1 AS ok,count(*)<>1 AS community_conflict"
        ),
        parameters,
        expected_rows: 1,
    }
}
pub fn guard(org: &str, state: &CommunityState) -> PreparedWrite {
    let statement=format!("CALL {{ {STATE} }} WITH revisions,generation,publication WHERE revisions=$revisions AND (generation=$generation OR (generation IS NULL AND $generation IS NULL)) AND (publication=$publication OR (publication IS NULL AND $publication IS NULL)) RETURN true AS checked");
    write(
        statement,
        json!({"org":org,"namespace":state.namespace,"revisions":state.source_revision.0,"generation":state.active_generation,"publication":state.publication_revision}),
    )
}
pub fn begin(org: &str, g: &BeginCommunityGeneration) -> Vec<PreparedWrite> {
    let mut result = vec![guard(org, &g.expected_state)];
    result.push(write("MERGE (g:CommunityGeneration {uuid:$generation}) ON CREATE SET g.org_id=$org,g.namespace=$namespace,g.projected_at=$at,g.valid_until=$until,g.partition_hashes=$hashes,g.community_count=$communities,g.member_count=$members,g.source_revision=$source_revision,g.status='staging'
        WITH g WHERE g.org_id=$org AND g.namespace=$namespace AND g.projected_at=$at AND (g.valid_until=$until OR (g.valid_until IS NULL AND $until IS NULL)) AND g.partition_hashes=$hashes AND g.community_count=$communities AND g.member_count=$members AND g.source_revision=$source_revision AND g.status='staging' RETURN true AS checked".into(),json!({"org":org,"namespace":g.expected_state.namespace,"generation":g.generation,"at":g.projected_at.to_rfc3339(),"until":g.valid_until.map(|v|v.to_rfc3339()),"hashes":g.partition_hashes,"communities":g.community_count,"members":g.member_count,"source_revision":g.expected_state.source_revision.0})));
    result
}
fn definition_properties(org: &str, generation: Uuid, d: &CommunityDefinition) -> Value {
    let embedding = d
        .node
        .name_embedding
        .as_ref()
        .expect("validated community vector");
    json!({"uuid":d.node.uuid,"org_id":org,"namespace":d.node.namespace,"generation_uuid":generation,"revision":d.revision,"name":d.node.name,"labels":d.node.labels,"created_at":d.node.created_at.to_rfc3339(),"summary":d.node.summary,"name_embedding":embedding.values,"name_embedding_model":embedding.model,"name_embedding_text_version":NAME_TEXT_VERSION,"name_embedding_content_hash":kg_core::embedding::content_hash(&d.node.name),"expected_member_count":d.expected_member_count,"source_hash":d.source_hash,"projected_at":d.projected_at.to_rfc3339(),"valid_until":d.valid_until.map(|v|v.to_rfc3339()),"dirty":false})
}
pub fn stage(org: &str, p: &StageCommunityPartition) -> Vec<PreparedWrite> {
    let hash = partition_hash(&p.partition).expect("validated partition serializes");
    let definitions=p.partition.definitions.iter().map(|d|json!({"properties":definition_properties(org,p.generation,d),"serialized":serde_json::to_string(d).expect("validated definition serializes")})).collect::<Vec<_>>();
    let members=p.partition.memberships.iter().flat_map(|chunk|chunk.members.iter().map(move|m|json!({"community":chunk.community_uuid,"uuid":format!("{}:{}",p.generation,m.chain_id),"entity":m.entity_uuid,"chain":m.chain_id}))).collect::<Vec<_>>();
    let parameters = json!({"org":org,"namespace":p.namespace,"generation":p.generation,"index":p.index,"hash":hash,"definitions":definitions,"members":members});
    vec![write("MATCH (g:CommunityGeneration {uuid:$generation,org_id:$org,namespace:$namespace}) SET g.uuid=g.uuid WITH g WHERE g.status='staging' AND g.partition_hashes[$index]=$hash AND all(definition IN $definitions WHERE definition.properties.projected_at=g.projected_at)
        MERGE (p:CommunityPartition {generation_uuid:$generation,partition_index:$index}) ON CREATE SET p.hash=$hash,p.definition_count=size($definitions),p.member_count=size($members)
        WITH g,p WHERE p.hash=$hash AND p.definition_count=size($definitions) AND p.member_count=size($members)
        CALL { WITH g UNWIND $definitions AS definition MERGE (c:GraphNode:Community {uuid:definition.properties.uuid}) ON CREATE SET c+=definition.properties,c.definition_json=definition.serialized WITH c,definition WHERE c.generation_uuid=$generation AND c.org_id=$org AND c.namespace=$namespace AND c.definition_json=definition.serialized RETURN count(c) AS written_definitions }
        WITH g,p,written_definitions WHERE written_definitions=size($definitions)
        CALL { WITH g UNWIND $members AS member MATCH (c:Community {uuid:member.community,generation_uuid:$generation,org_id:$org,namespace:$namespace}),(n:Entity {uuid:member.entity,chain_id:member.chain,org_id:$org,namespace:$namespace})
            MERGE (c)-[m:HAS_MEMBER {membership_key:member.uuid}]->(n) ON CREATE SET m.uuid=member.uuid,m.org_id=$org,m.namespace=$namespace,m.generation_uuid=$generation,m.community_uuid=member.community,m.chain_id=member.chain
            WITH m,member WHERE m.community_uuid=member.community AND endNode(m).uuid=member.entity RETURN count(m) AS written_members }
        WITH p,written_members WHERE written_members=size($members) SET p.complete=true RETURN true AS checked".into(),parameters)]
}
// Source revisions do not advance when a scheduled temporal boundary passes.
// Check server time at the publication write after taking its locks.
pub fn publish(org: &str, p: &PublishCommunityGeneration) -> Vec<PreparedWrite> {
    let mut result = vec![guard(org, &p.expected_state)];
    result.push(write("MATCH (g:CommunityGeneration {uuid:$generation,org_id:$org,namespace:$namespace}) SET g.uuid=g.uuid WITH g WHERE g.status='staging' AND g.source_revision=$source_revision
        CALL (g) { OPTIONAL MATCH (p:CommunityPartition {generation_uuid:g.uuid}) WITH g,p ORDER BY p.partition_index RETURN collect(p.hash) AS hashes,sum(p.definition_count) AS definitions,sum(p.member_count) AS members,all(v IN collect(p.complete) WHERE v=true) AS complete }
        WITH g,hashes,definitions,members,complete WHERE complete AND hashes=g.partition_hashes AND definitions=g.community_count AND members=g.member_count
        CALL (g) { OPTIONAL MATCH (c:Community {generation_uuid:g.uuid,org_id:$org,namespace:$namespace})
            CALL (c) { OPTIONAL MATCH (c)-[m:HAS_MEMBER]->(n:Entity) RETURN count(m) AS actual,all(valid IN collect(CASE WHEN m IS NULL THEN true ELSE n.org_id=$org AND n.namespace=$namespace AND n.chain_id=m.chain_id AND datetime(n.valid_from)<=datetime(c.projected_at) AND (n.valid_to IS NULL OR datetime(c.projected_at)<datetime(n.valid_to)) AND (n.invalid_at IS NULL OR datetime(c.projected_at)<datetime(n.invalid_at)) AND (n.deleted_at IS NULL OR datetime(c.projected_at)<datetime(n.deleted_at)) AND NOT EXISTS { MATCH (merge:ChainMerge {org_id:$org,loser_chain_id:n.chain_id}) WHERE datetime(merge.valid_from)<=datetime(c.projected_at) AND (merge.valid_to IS NULL OR datetime(c.projected_at)<datetime(merge.valid_to)) } END) WHERE valid) AS eligible }
            RETURN collect(c.valid_until) AS deadlines,count(c) AS actual_communities,sum(actual) AS actual_members,all(valid IN collect(CASE WHEN c IS NULL THEN true ELSE actual=c.expected_member_count AND eligible END) WHERE valid) AS member_counts }
        WITH g,deadlines,actual_communities,actual_members,member_counts WHERE actual_communities=g.community_count AND actual_members=g.member_count AND member_counts
        MERGE (scope:CommunityScope {org_id:$org,namespace:$namespace}) SET scope.namespace=scope.namespace
        WITH g,scope,deadlines WHERE (g.valid_until IS NULL OR datetime.realtime()<datetime(g.valid_until)) AND all(deadline IN deadlines WHERE datetime.realtime()<datetime(deadline))
        SET scope.active_generation=$generation,scope.publication_revision=$publication,g.status='active' RETURN true AS checked".into(),json!({"org":org,"namespace":p.expected_state.namespace,"generation":p.generation,"publication":p.publication_revision,"source_revision":p.expected_state.source_revision.0})));
    result
}
pub fn update(org: &str, u: &UpdateCommunities) -> Vec<PreparedWrite> {
    let mut result = vec![guard(org, &u.expected_state)];
    let mut writes = u.communities.iter().collect::<Vec<_>>();
    writes.sort_by_key(|w| w.definition.node.uuid);
    for w in writes {
        let props = definition_properties(org, u.generation, &w.definition);
        let members=w.members.iter().map(|m|json!({"uuid":format!("{}:{}",u.generation,m.chain_id),"entity":m.entity_uuid,"chain":m.chain_id})).collect::<Vec<_>>();
        result.push(write("MATCH (c:Community {org_id:$org,namespace:$namespace,generation_uuid:$generation,uuid:$community}) SET c.uuid=c.uuid WITH c WHERE c.revision=$revision
            OPTIONAL MATCH (c)-[old:HAS_MEMBER]->() WITH c,collect(old) AS old FOREACH (edge IN old | DELETE edge)
            WITH c SET c+=$properties
            WITH c CALL (c) { UNWIND $members AS member MATCH (n:Entity {org_id:$org,namespace:$namespace,uuid:member.entity,chain_id:member.chain}) WHERE datetime(n.valid_from)<=datetime(c.projected_at) AND (n.valid_to IS NULL OR datetime(c.projected_at)<datetime(n.valid_to)) AND (n.invalid_at IS NULL OR datetime(c.projected_at)<datetime(n.invalid_at)) AND (n.deleted_at IS NULL OR datetime(c.projected_at)<datetime(n.deleted_at)) AND NOT EXISTS { MATCH (merge:ChainMerge {org_id:$org,loser_chain_id:n.chain_id}) WHERE datetime(merge.valid_from)<=datetime(c.projected_at) AND (merge.valid_to IS NULL OR datetime(c.projected_at)<datetime(merge.valid_to)) } CREATE (c)-[m:HAS_MEMBER {membership_key:member.uuid,uuid:member.uuid,org_id:$org,namespace:$namespace,generation_uuid:$generation,community_uuid:$community,chain_id:member.chain}]->(n) RETURN count(m) AS written }
            WITH c,written WHERE written=size($members) RETURN true AS checked".into(),json!({"org":org,"namespace":u.expected_state.namespace,"generation":u.generation,"community":w.definition.node.uuid,"revision":w.expected_revision,"properties":props,"members":members})));
    }
    // This final guard rolls back every preceding membership and definition change.
    result.push(write("MATCH (scope:CommunityScope {org_id:$org,namespace:$namespace,active_generation:$generation}) SET scope.namespace=scope.namespace WITH scope WHERE all(deadline IN $deadlines WHERE deadline IS NULL OR datetime.realtime()<datetime(deadline)) SET scope.publication_revision=$revision RETURN true AS checked".into(),json!({"org":org,"namespace":u.expected_state.namespace,"generation":u.generation,"revision":u.publication_revision,"deadlines":u.communities.iter().map(|w|w.definition.valid_until.map(|at|at.to_rfc3339())).collect::<Vec<_>>()})));
    result
}
