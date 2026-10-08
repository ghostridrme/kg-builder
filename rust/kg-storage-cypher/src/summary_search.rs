//! Time-valid derived-summary recall; base entity text and vectors remain independent.
use crate::{filters::*, PreparedQuery};
use kg_core::{errors::BackendError, search::*};

pub(crate) fn visible(v: &str) -> String {
    format!("({v}.derived_summary IS NOT NULL AND {v}.summary_revision IS NOT NULL AND {v}.summary_evidence_hash IS NOT NULL AND {v}.summary_policy_version=$summary_policy AND datetime({v}.summary_as_of)<=datetime(coalesce($as_of,$relationship_now)) AND ({v}.summary_valid_until IS NULL OR datetime(coalesce($as_of,$relationship_now))<datetime({v}.summary_valid_until)))")
}
pub fn summary_nodes(
    request: &SummarySearch,
    candidates: Option<usize>,
) -> Result<PreparedQuery, BackendError> {
    if matches!(request.node.query, NodeQuery::ByChain)
        || candidates.is_some_and(|n| n == 0 || n > crate::neo4j::vector::MAX_VECTOR_CANDIDATES)
    {
        return Err(BackendError::Query(
            "invalid summary retrieval mode or budget".into(),
        ));
    }
    crate::neo4j::nodes::nodes_with_source(&request.node, candidates, true)
}
pub fn summary_population(request: &SummarySearch) -> Result<PreparedQuery, BackendError> {
    request.node.validate()?;
    let NodeQuery::Similarity(embedding) = &request.node.query else {
        return Err(BackendError::Query(
            "summary population requires embedding".into(),
        ));
    };
    let filter = &request.node.filter;
    let mut parameters = params(filter, &request.node.chain_ids, 1);
    parameters["model"] = embedding.model.clone().into();
    parameters["text_version"] = kg_core::entity_summary::SUMMARY_TEXT_VERSION.into();
    parameters["summary_policy"] = kg_core::entity_summary::POLICY_VERSION.into();
    Ok(PreparedQuery{statement:format!("MATCH (n:Entity) WHERE {} AND {} AND {} AND {} AND n.summary_embedding_model=$model AND n.summary_embedding_text_version=$text_version AND ($chains IS NULL OR n.chain_id IN $chains) WITH n LIMIT 513 RETURN count(n) AS count",scope("n",filter),types("n",filter),entity_visible("n",filter),visible("n")),parameters})
}
pub fn summary_readiness(
    request: &EmbeddingReadinessRequest,
) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    if request.scope != SearchScope::Nodes {
        return Err(BackendError::Query(
            "summary readiness requires node scope".into(),
        ));
    }
    let mut parameters = params(&request.filter, &None, 1);
    parameters["model"] = request.model.clone().into();
    parameters["dimension"] = request.dimensions.into();
    parameters["text_version"] = kg_core::entity_summary::SUMMARY_TEXT_VERSION.into();
    parameters["summary_policy"] = kg_core::entity_summary::POLICY_VERSION.into();
    Ok(PreparedQuery{statement:format!("MATCH (n:Entity) WHERE {} AND {} AND {} AND n.derived_summary IS NOT NULL WITH n,{} AS valid WITH n,valid,(n.summary_embedding IS NOT NULL AND size(n.summary_embedding)=$dimension AND n.summary_embedding_model=$model AND n.summary_embedding_text_version=$text_version AND n.summary_embedding_content_hash IS NOT NULL AND all(x IN n.summary_embedding WHERE x IS NOT NULL AND x*0=0) AND any(x IN n.summary_embedding WHERE x<>0)) AS compatible RETURN count(CASE WHEN valid THEN 1 END) AS valid,count(CASE WHEN NOT valid THEN 1 END) AS expired,count(CASE WHEN valid AND n.summary_embedding IS NULL THEN 1 END) AS missing,count(CASE WHEN valid AND compatible THEN 1 END) AS compatible,count(CASE WHEN valid AND n.summary_embedding IS NOT NULL AND NOT coalesce(compatible,false) THEN 1 END) AS incompatible",scope("n",&request.filter),types("n",&request.filter),entity_visible("n",&request.filter),visible("n")),parameters})
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::traits::graph_backend::GraphEmbedding;
    #[test]
    fn summary_retrieval_uses_separate_indexes_and_shared_time_guards() {
        let mut request = SummarySearch {
            node: NodeSearch {
                embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
                filter: SearchFilter {
                    org_id: "org".into(),
                    ..Default::default()
                },
                query: NodeQuery::Fulltext("needle".into()),
                chain_ids: None,
                limit: 10,
                min_score: 0.0,
                signals: Default::default(),
                projection: NodeProjection::Candidate,
            },
        };
        let keyword = summary_nodes(&request, None).unwrap();
        assert!(keyword
            .statement
            .contains("queryNodes('search_entity_summaries'"));
        assert!(keyword.parameters["query"]
            .as_str()
            .unwrap()
            .contains("derived_summary:"));
        assert!(keyword.statement.contains("summary_valid_until"));
        request.node.query = NodeQuery::Similarity(GraphEmbedding {
            model: "m".into(),
            values: vec![1.0, 0.0],
        });
        for candidates in [None, Some(4096)] {
            let q = summary_nodes(&request, candidates).unwrap();
            assert!(q.statement.contains("n.summary_embedding AS vector"));
            assert_eq!(
                q.parameters["text_version"],
                kg_core::entity_summary::SUMMARY_TEXT_VERSION
            );
            assert!(q.statement.contains("summary_state"));
            if candidates.is_some() {
                assert!(q.statement.contains("search_entity_summary_vectors"));
            }
        }
    }
}
