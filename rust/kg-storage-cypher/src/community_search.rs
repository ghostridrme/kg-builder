//! Community recall only exposes the active, clean generation at the pinned source time.
use crate::{filters::*, PreparedQuery};
use kg_core::{errors::BackendError, search::*, traits::GraphProperties};
use serde_json::json;

fn eligible(filter: &SearchFilter) -> String {
    format!("{} AND coalesce(c.dirty,true)=false AND datetime(c.projected_at)<=datetime($at) AND (c.valid_until IS NULL OR datetime($at)<datetime(c.valid_until)) AND EXISTS {{MATCH (scope:CommunityScope {{org_id:$org_id,namespace:c.namespace}}) WHERE scope.active_generation=c.generation_uuid}} AND ($types=[] OR EXISTS {{MATCH (c)-[:HAS_MEMBER]->(member:Entity {{org_id:$org_id}}) WHERE member.entity_type IN $types}})",scope("c",filter))
}
fn parameters(filter: &SearchFilter, limit: usize) -> serde_json::Value {
    let mut p = params(filter, &None, limit);
    p["at"] = json!(filter
        .as_of
        .or(filter.relationship_now)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339());
    p["text_version"] = json!(kg_core::community::NAME_TEXT_VERSION);
    p
}
const VECTOR:&str=" AND c.name_embedding_model=$model AND c.name_embedding_text_version=$text_version AND c.name_embedding_content_hash IS NOT NULL
 WITH c,c.name_embedding AS vector WHERE size(vector)=size($vector) AND all(x IN vector WHERE x IS NOT NULL AND x*0=0)
 WITH c,vector,sqrt(reduce(total=0.0,x IN vector|total+x*x)) AS norm
 WHERE norm>0 WITH c,reduce(dot=0.0,i IN range(0,size(vector)-1)|dot+vector[i]*$vector[i])/(norm*$norm) AS score WHERE score>=$min_score";
const PROJECT:&str="CALL (c) {OPTIONAL MATCH (c)-[membership:HAS_MEMBER]->(n:Entity {org_id:$org_id,namespace:c.namespace}) WHERE $member_limit>0 AND membership.generation_uuid=c.generation_uuid WITH n ORDER BY n.chain_id LIMIT $member_limit RETURN collect(n{entity_uuid:n.uuid,chain_id:n.chain_id,name:n.name,entity_type:n.entity_type}) AS members}
 RETURN c{uuid:c.uuid,generation:c.generation_uuid,revision:c.revision,namespace:c.namespace,name:c.name,summary:c.summary,source_hash:c.source_hash,projected_at:c.projected_at,valid_until:c.valid_until,member_count:c.expected_member_count,members:members,members_truncated:size(members)<c.expected_member_count,score:score} AS community,c.name_embedding AS embedding,c.name_embedding_model AS model,c.name_embedding_text_version AS text_version,c.name_embedding_content_hash AS content_hash";
pub fn communities(
    request: &CommunitySearch,
    candidates: Option<usize>,
) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    crate::filters::reject_saga_filter(&request.filter)?;
    if candidates.is_some_and(|n| n == 0 || n > crate::MAX_VECTOR_CANDIDATES) {
        return Err(BackendError::Query(
            "invalid community vector budget".into(),
        ));
    }
    let mut p = parameters(&request.filter, request.limit);
    p["uuids"] = json!(request.uuids);
    p["member_limit"] = json!(request.member_limit);
    p["min_score"] = json!(request.min_score);
    let mut core = format!(
        " WHERE {} AND ($uuids IS NULL OR c.uuid IN $uuids)",
        eligible(&request.filter)
    );
    let prefix = match &request.query {
        NodeQuery::Fulltext(text) => {
            p["query"] = json!(lucene_scoped(&request.filter.org_id, text, &["name"]));
            "CALL db.index.fulltext.queryNodes('search_communities',$query) YIELD node AS c,score"
        }
        NodeQuery::ByChain => {
            core.push_str(" WITH c,1.0 AS score");
            "MATCH (c:Community)"
        }
        NodeQuery::Similarity(vector) => {
            p["vector"] = json!(vector.values);
            p["model"] = json!(vector.model);
            p["norm"] = json!(vector
                .values
                .iter()
                .map(|x| f64::from(*x).powi(2))
                .sum::<f64>()
                .sqrt());
            core.push_str(VECTOR);
            "MATCH (c:Community)"
        }
    };
    core.push_str(" WITH c,score ORDER BY score DESC,c.uuid LIMIT $limit");
    let statement = if let Some(budget) =
        candidates.filter(|_| matches!(request.query, NodeQuery::Similarity(_)))
    {
        p["candidates"] = json!(budget);
        format!("CALL db.index.vector.queryNodes('search_community_name_vectors',$candidates,$vector) YIELD node,score WITH collect({{node:node,score:score}}) AS raw WITH raw,size(raw) AS raw_count,CASE WHEN size(raw)=0 THEN null ELSE reduce(m=1.0,x IN raw|CASE WHEN x.score<m THEN x.score ELSE m END)*2-1 END AS frontier OPTIONAL CALL {{WITH raw UNWIND raw AS row WITH row.node AS c {core} {PROJECT}}} RETURN community,embedding,model,text_version,content_hash,raw_count,frontier ORDER BY community.score DESC,community.uuid")
    } else {
        format!("{prefix}{core} {PROJECT} ORDER BY community.score DESC,community.uuid")
    };
    Ok(PreparedQuery {
        statement,
        parameters: p,
    })
}
pub fn population(request: &CommunitySearch) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    let NodeQuery::Similarity(vector) = &request.query else {
        return Err(BackendError::Query(
            "community population requires vectors".into(),
        ));
    };
    let mut p = parameters(&request.filter, 1);
    p["model"] = json!(vector.model);
    Ok(PreparedQuery{statement:format!("MATCH (c:Community) WHERE {} AND c.name_embedding_model=$model AND c.name_embedding_text_version=$text_version WITH c LIMIT 513 RETURN count(c) AS count",eligible(&request.filter)),parameters:p})
}
pub fn readiness(request: &EmbeddingReadinessRequest) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    let mut p = parameters(&request.filter, 1);
    p["model"] = json!(request.model);
    p["dimension"] = json!(request.dimensions);
    Ok(PreparedQuery{statement:format!("MATCH (c:Community) WHERE {} WITH c,(c.name_embedding IS NOT NULL AND size(c.name_embedding)=$dimension AND c.name_embedding_model=$model AND c.name_embedding_text_version=$text_version AND c.name_embedding_content_hash IS NOT NULL AND all(x IN c.name_embedding WHERE x IS NOT NULL AND x*0=0) AND any(x IN c.name_embedding WHERE x<>0)) AS compatible RETURN count(c) AS eligible,count(CASE WHEN compatible THEN 1 END) AS compatible,count(CASE WHEN c.name_embedding IS NULL THEN 1 END) AS missing,count(CASE WHEN c.name_embedding IS NOT NULL AND NOT coalesce(compatible,false) THEN 1 END) AS incompatible",eligible(&request.filter)),parameters:p})
}
pub fn decode(mut row: GraphProperties) -> Result<CommunityHit, BackendError> {
    let bad = || BackendError::Deserialization("invalid community search result".into());
    let mut value = row.remove("community").ok_or_else(bad)?;
    value["score_breakdown"] = json!({});
    let mut hit: CommunityHit = serde_json::from_value(value).map_err(|_| bad())?;
    if hit.uuid.is_nil()
        || hit.generation.is_nil()
        || hit.revision.is_nil()
        || !hit.score.is_finite()
        || hit.member_count == 0
        || hit.members.len() > hit.member_count
    {
        return Err(bad());
    }
    if row.get("text_version").and_then(|v| v.as_str())
        == Some(kg_core::community::NAME_TEXT_VERSION)
        && row.get("content_hash").and_then(|v| v.as_str())
            == Some(kg_core::embedding::content_hash(&hit.name).as_str())
    {
        if let (Some(values), Some(model)) = (
            row.remove("embedding").filter(|v| !v.is_null()),
            row.get("model").and_then(|v| v.as_str()),
        ) {
            let vector = kg_core::traits::graph_backend::GraphEmbedding {
                model: model.into(),
                values: serde_json::from_value(values).map_err(|_| bad())?,
            };
            vector.validate()?;
            hit.embedding = Some(vector);
        }
    }
    Ok(hit)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn community_recall_and_hydration_share_visibility_and_scoped_names() {
        let mut request = CommunitySearch {
            filter: SearchFilter {
                org_id: "org".into(),
                ..Default::default()
            },
            query: NodeQuery::Fulltext("checkout".into()),
            uuids: None,
            limit: 10,
            min_score: 0.0,
            member_limit: 0,
        };
        let query = communities(&request, None).unwrap();
        assert!(query.statement.contains("search_communities"));
        assert!(query.parameters["query"]
            .as_str()
            .unwrap()
            .contains("name:"));
        for token in [
            "active_generation",
            "c.dirty",
            "c.projected_at",
            "c.valid_until",
        ] {
            assert!(query.statement.contains(token));
        }
        request.query = NodeQuery::ByChain;
        request.uuids = Some(vec![uuid::Uuid::new_v4()]);
        assert!(communities(&request, None)
            .unwrap()
            .statement
            .contains("c.uuid IN $uuids"));
    }
    #[test]
    fn indexed_community_query_preserves_frontier_when_scoped_candidates_are_empty() {
        let request = CommunitySearch {
            filter: SearchFilter {
                org_id: "org".into(),
                ..Default::default()
            },
            query: NodeQuery::Similarity(kg_core::traits::graph_backend::GraphEmbedding {
                model: "m".into(),
                values: vec![1.0, 0.0],
            }),
            uuids: None,
            limit: 10,
            min_score: 0.0,
            member_limit: 0,
        };
        let query = communities(&request, Some(4096)).unwrap();
        assert_eq!(
            query
                .statement
                .matches("db.index.vector.queryNodes")
                .count(),
            1
        );
        assert!(query.statement.contains("OPTIONAL CALL"));
        assert!(query.statement.contains("raw_count,frontier"));
        assert_eq!(
            query.parameters["text_version"],
            kg_core::community::NAME_TEXT_VERSION
        );
    }
}
