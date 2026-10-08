//! Filter before limiting; return records with the scores that selected them.

use crate::PreparedQuery;
use kg_core::{
    errors::BackendError,
    traits::{
        IdentityCandidate, IdentityCandidatePage, IdentityCandidateQuery, IdentityCandidateRequest,
    },
};
use serde_json::{json, Map, Value};

const LIVE: &str = "n.org_id=$org_id AND n.namespace=$namespace AND n.is_latest=true AND n.valid_to IS NULL AND n.deleted_at IS NULL AND n.merged_into IS NULL AND NOT n.chain_id IN $exclude";
const PAGE: &str = " WITH n,max(score) AS score WITH n.chain_id AS chain,collect(n) AS versions,max(score) AS score WITH versions[0] AS n,size(versions) AS heads,score ORDER BY score DESC,n.chain_id,n.uuid LIMIT $limit CALL { WITH n MATCH (head:Entity {org_id:$org_id,chain_id:n.chain_id}) WHERE head.is_latest=true AND head.valid_to IS NULL AND head.deleted_at IS NULL AND head.merged_into IS NULL RETURN count(head) AS live_heads } RETURN properties(n) AS n,live_heads AS heads,score ORDER BY score DESC,n.chain_id,n.uuid";

pub fn candidates(
    org: &str,
    request: &IdentityCandidateRequest,
) -> Result<PreparedQuery, BackendError> {
    request.validate(org)?;
    let mut parameters = json!({"org_id":org,"namespace":request.scope.namespace,"entity_type":request.scope.entity_type,"exclude":request.exclude_chains,"limit":request.limit+1});
    let type_property = if request.scope.entity_type == "*" {
        ""
    } else {
        ",entity_type:$entity_type"
    };
    let live = if request.scope.entity_type == "*" {
        LIVE.to_owned()
    } else {
        format!("{LIVE} AND n.entity_type=$entity_type")
    };
    let statement = match &request.query {
        IdentityCandidateQuery::ExactNames(names) => {
            parameters["names"] = json!(names);
            format!("UNWIND $names AS name MATCH (n:Entity {{org_id:$org_id,namespace:$namespace{type_property},name:name}}) WHERE {live} WITH n,1.0 AS score{PAGE}")
        }
        IdentityCandidateQuery::Names(names) => {
            parameters["names"] = json!(names);
            parameters["query"] =
                crate::filters::lucene_scoped(org, &names.join(" "), &["name"]).into();
            // Literal equality also finds punctuation-only names that have no fulltext tokens.
            format!("CALL {{ UNWIND $names AS name MATCH (n:Entity {{org_id:$org_id,namespace:$namespace{type_property},name:name}}) WHERE {live} RETURN n,1.0 AS score UNION ALL CALL db.index.fulltext.queryNodes('search_entities',$query) YIELD node AS n,score AS relevance WHERE {live} RETURN n,relevance/(1.0+relevance) AS score }}{PAGE}")
        }
        IdentityCandidateQuery::PropertyOverlap(properties) => {
            // One group per property keeps alternative observation values from
            // inflating the score. Group once in Rust instead of rebuilding a
            // distinct-key list for every stored candidate in Cypher.
            let mut grouped = std::collections::BTreeMap::<String, Vec<Value>>::new();
            for (key, value) in properties {
                let mut encoded = Map::new();
                kg_core::traits::property_codec::write_property(&mut encoded, key, Some(value));
                grouped.entry(key.clone()).or_default().push(json!({
                    "tag": encoded[&format!("property_type_{key}")],
                    "value": encoded[&format!("prop_{key}")],
                }));
            }
            parameters["key_count"] = json!(grouped.len());
            parameters["groups"] = Value::Array(
                grouped
                    .into_iter()
                    .map(|(key, alternatives)| json!({"key":key,"alternatives":alternatives}))
                    .collect(),
            );
            format!("MATCH (n:Entity {{org_id:$org_id,namespace:$namespace{type_property}}}) WHERE {live} WITH n,size([g IN $groups WHERE any(p IN g.alternatives WHERE n['property_type_'+g.key]=p.tag AND n['prop_'+g.key]=p.value) | g.key]) AS agreed WHERE agreed>0 WITH n,toFloat(agreed)/$key_count AS score{PAGE}")
        }
        IdentityCandidateQuery::Similarity {
            embedding,
            text_version,
            min_score,
        } => {
            parameters["model"] = json!(embedding.model);
            parameters["text_version"] = json!(text_version);
            parameters["vector"] = json!(embedding.values);
            parameters["min_score"] = json!(min_score);
            parameters["norm"] = json!(embedding
                .values
                .iter()
                .map(|v| f64::from(*v).powi(2))
                .sum::<f64>()
                .sqrt());
            format!("MATCH (n:Entity {{org_id:$org_id,namespace:$namespace{type_property},embedding_model:$model,embedding_text_version:$text_version}}) WHERE {live} WITH n,n.embedding AS vector WHERE size(vector)=size($vector) WITH n,vector,sqrt(reduce(total=0.0,v IN vector | total+v*v)) AS norm WHERE norm>0 WITH n,reduce(dot=0.0,i IN range(0,size(vector)-1) | dot+vector[i]*$vector[i])/(norm*$norm) AS raw WHERE raw >= $min_score WITH n,CASE WHEN raw>1.0 THEN 1.0 WHEN raw< -1.0 THEN -1.0 ELSE raw END AS score{PAGE}")
        }
    };
    Ok(PreparedQuery {
        statement,
        parameters,
    })
}

pub fn decode(
    org: &str,
    request: &IdentityCandidateRequest,
    rows: Vec<Map<String, Value>>,
) -> Result<IdentityCandidatePage, BackendError> {
    request.validate(org)?;
    let mut seen = std::collections::HashSet::new();
    if rows.len() > request.limit + 1 {
        return Err(BackendError::Deserialization(
            "oversized identity candidate response".into(),
        ));
    }
    let truncated = rows.len() > request.limit;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        if row.get("heads").and_then(Value::as_u64) != Some(1) {
            return Err(BackendError::Deserialization(
                "multiple live versions in identity candidates".into(),
            ));
        }
        let score = row.get("score").and_then(Value::as_f64).ok_or_else(|| {
            BackendError::Deserialization("missing identity candidate score".into())
        })?;
        let record = crate::decode_entity_version(row)?;
        if !seen.insert(record.chain_id) {
            return Err(BackendError::Deserialization(
                "duplicate identity candidate chain".into(),
            ));
        }
        items.push(IdentityCandidate { record, score });
    }
    // Validate the sentinel too: malformed evidence cannot be hidden by truncation.
    let mut expanded = request.clone();
    expanded.limit = kg_core::traits::identity_candidates::MAX_IDENTITY_CANDIDATES;
    if items.len() > expanded.limit {
        let sentinel = items.pop().expect("sentinel exists");
        IdentityCandidatePage {
            items: vec![sentinel],
            truncated: false,
        }
        .validate(org, &expanded)?;
    }
    let mut page = IdentityCandidatePage {
        items,
        truncated: false,
    };
    page.validate(org, &expanded)?;
    page.items.truncate(request.limit);
    page.truncated = truncated;
    page.validate(org, request)?;
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::traits::{graph_backend::GraphEmbedding, IdentityScope};
    #[test]
    fn queries_bind_scope_and_filter_before_the_sentinel_limit() {
        for query in [
            IdentityCandidateQuery::ExactNames(vec!["api\\\" OR *".into()]),
            IdentityCandidateQuery::Names(vec!["api\\\" OR *".into()]),
            IdentityCandidateQuery::Similarity {
                embedding: GraphEmbedding {
                    model: "model".into(),
                    values: vec![1.0, 0.0],
                },
                text_version: "v2".into(),
                min_score: 0.7,
            },
        ] {
            let request = IdentityCandidateRequest {
                scope: IdentityScope {
                    namespace: "prod".into(),
                    entity_type: "Service".into(),
                },
                query,
                exclude_chains: vec![],
                limit: 15,
            };
            let prepared = candidates("tenant", &request).unwrap();
            assert_eq!(prepared.parameters["limit"], 16);
            assert!(!prepared.statement.contains("tenant"));
            assert!(
                prepared.statement.find("n.merged_into IS NULL").unwrap()
                    < prepared.statement.find("LIMIT $limit").unwrap()
            );
            assert!(prepared.statement.contains("collect(n) AS versions"));
        }
    }
}
