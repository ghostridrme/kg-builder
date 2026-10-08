use crate::{filters::*, PreparedQuery};
use kg_core::{errors::BackendError, search::*};
use serde_json::json;

/// Content fields of the `search_entities` fulltext index; `org_id` is the scope field.
pub const ENTITY_TEXT_FIELDS: &[&str] = &["name", "summary", "prop_summary"];

pub fn nodes(request: &NodeSearch) -> Result<PreparedQuery, BackendError> {
    nodes_with_index(request, None)
}

/// Only vectors from the configured model and text policy are comparable.
const COSINE_FILTER: &str = r#"
                    AND n.embedding_model = $model
                    AND n.embedding_text_version = $text_version"#;

/// Indexed candidates are ranked by the index's cosine first and cut to the page,
/// so the exact recomputation below runs for at most `limit` rows instead of every
/// candidate the index returned (4,096 candidates cost about 300 ms of Cypher list
/// arithmetic; measured 2026-09-27). The index score is the true cosine of each
/// candidate up to float rounding, so the cut cannot lose a row that the exact
/// score would have ranked into the page.
const INDEX_PAGE: &str = r#"
                    WITH n, index_score ORDER BY index_score DESC, n.chain_id, n.uuid LIMIT $limit"#;

/// Exact cosine over the stored vector for the rows that reach it.
const COSINE: &str = r#"
                    WITH n, n.embedding AS vector
                    WHERE size(vector) = size($vector)
                    WITH n, vector, [x IN vector | x*x] AS squares,
                         [i IN range(0,size(vector)-1) | vector[i]*$vector[i]] AS products
                    WITH n, sqrt(reduce(total=0.0, x IN squares | total+x)) AS norm,
                         reduce(total=0.0, x IN products | total+x) AS dot
                    WHERE norm > 0
                    WITH n, dot/(norm*$norm) AS score
                    WHERE score >= $min_score
                "#;

/// Filtering, scoring, the candidate limit, and optional counts for one node
/// variable `n`. Shared by the flat statements and the indexed subquery.
fn node_core(request: &NodeSearch, indexed: bool, summary: bool) -> String {
    let mut query = format!(
        " WHERE {} AND {} AND {} AND {}",
        scope("n", &request.filter),
        types("n", &request.filter),
        entity_visible("n", &request.filter),
        if request.chain_ids.is_some() {
            "n.chain_id IN $chains"
        } else {
            "true"
        }
    );
    if summary {
        query.push_str(&format!(" AND {}", crate::summary_search::visible("n")));
    }
    match &request.query {
        NodeQuery::Similarity(_) => {
            let vector_property = if summary {
                "n.summary_embedding"
            } else {
                "n.embedding"
            };
            query.push_str(&COSINE_FILTER.replace("n.embedding", vector_property));
            if indexed {
                // Leave room for index-score rounding; the final cutoff uses exact cosine.
                query.push_str(" AND index_score * 2 - 1 >= $min_score - 0.00001");
                query.push_str(INDEX_PAGE);
            }
            query.push_str(&COSINE.replace("n.embedding", vector_property));
        }
        NodeQuery::ByChain => query.push_str(" WITH n, 1.0 AS score"),
        NodeQuery::Fulltext(_) => {}
    }
    query.push_str(" WITH n, score ORDER BY score DESC, n.chain_id, n.uuid LIMIT $limit");
    if request.signals.observations {
        query.push_str(&format!(
            r#"
            CALL {{
                WITH n
                OPTIONAL MATCH (snap:Snapshot)-[o:MENTIONS]->(version:Entity)
                WHERE version.chain_id = n.chain_id AND version.org_id = $org_id
                    AND {} AND o.org_id = $org_id
                    AND ($as_of IS NULL OR (datetime(snap.captured_at) <= datetime($as_of)
                        AND datetime(o.observed_at) <= datetime($as_of)))
                RETURN count(DISTINCT snap) AS observations
            }}
        "#,
            scope("snap", &request.filter)
        ));
    }
    if request.signals.dependents {
        query.push_str(&format!(
            r#"
            CALL {{
                WITH n
                OPTIONAL MATCH (target:Entity {{org_id:$org_id,chain_id:n.chain_id}})<-[r:RELATES_TO]-(physical:Entity)
                WHERE physical.org_id = $org_id
                    AND target.org_id = $org_id AND r.org_id = $org_id AND {}
                    AND (size($relationship_types) = 0 OR r.name IN $relationship_types)
                OPTIONAL MATCH (d:Entity)
                WHERE d.chain_id = r.source_chain_id AND {} AND {}
                RETURN count(DISTINCT d.chain_id) AS dependents
            }}
        "#,
            relationship_visible("r", &request.filter),
            scope("d", &request.filter),
            fact_endpoint_visible("d", &request.filter)
        ));
    }
    query
}

fn projection(request: &NodeSearch) -> String {
    let mut columns = match request.projection {
        NodeProjection::Candidate => {
            " RETURN n{.uuid, .chain_id, .entity_type, .namespace, .name} AS n, score".to_string()
        }
        NodeProjection::Full => " RETURN n, score".to_string(),
    };
    columns.push_str(&format!(", CASE WHEN {} THEN n{{.derived_summary,.summary_revision,.summary_evidence_hash,.summary_as_of,.summary_valid_until,.summary_policy_version,.summary_evidence_ids,.summary_total_evidence}} ELSE null END AS summary_state",crate::summary_search::visible("n")));
    if request.signals.observations {
        columns.push_str(", observations");
    }
    if request.signals.dependents {
        columns.push_str(", dependents");
    }
    columns
}

fn signal_columns(request: &NodeSearch) -> String {
    let mut columns = String::new();
    if request.signals.observations {
        columns.push_str(", observations");
    }
    if request.signals.dependents {
        columns.push_str(", dependents");
    }
    columns
}

pub(crate) fn nodes_with_index(
    request: &NodeSearch,
    candidates: Option<usize>,
) -> Result<PreparedQuery, BackendError> {
    nodes_with_source(request, candidates, false)
}

pub(crate) fn nodes_with_source(
    request: &NodeSearch,
    candidates: Option<usize>,
    summary: bool,
) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    crate::filters::reject_saga_filter(&request.filter)?;
    let mut p = params(&request.filter, &request.chain_ids, request.limit);
    p["summary_policy"] = kg_core::entity_summary::POLICY_VERSION.into();
    if let NodeQuery::Similarity(e) = &request.query {
        p["vector"] = json!(e.values);
        p["model"] = json!(e.model);
        p["text_version"] = json!(if summary {
            kg_core::entity_summary::SUMMARY_TEXT_VERSION
        } else {
            &request.embedding_text_version
        });
        p["min_score"] = json!(request.min_score);
        p["norm"] = json!(e
            .values
            .iter()
            .map(|v| f64::from(*v).powi(2))
            .sum::<f64>()
            .sqrt());
    }
    let statement = match (&request.query, candidates) {
        (NodeQuery::Similarity(_), Some(budget)) => {
            p["candidates"] = budget.into();
            // One index call serves both candidate selection and the frontier
            // statistics; OPTIONAL CALL keeps one row when every candidate is
            // filtered out so the caller can still read them.
            format!(
                "CALL db.index.vector.queryNodes('{index}', $candidates, $vector) YIELD node, score
                WITH collect({{node: node, score: score}}) AS raw
                WITH raw, size(raw) AS raw_count,
                     CASE WHEN size(raw) = 0 THEN null
                          ELSE reduce(m = 1.0, x IN raw | CASE WHEN x.score < m THEN x.score ELSE m END) * 2 - 1 END AS frontier
                OPTIONAL CALL {{
                    WITH raw
                    UNWIND raw AS row
                    WITH row.node AS n, row.score AS index_score{core}
                    RETURN n, score{signals}
                }}
                {projection}, raw_count, frontier ORDER BY score DESC, n.chain_id, n.uuid",
                index = if summary { "search_entity_summary_vectors" } else { "search_entity_vectors" },
                core = node_core(request, true, summary),
                signals = signal_columns(request),
                projection = projection(request)
            )
        }
        (NodeQuery::Fulltext(q), _) => {
            p["query"] = lucene_scoped(
                &request.filter.org_id,
                q,
                if summary {
                    &["derived_summary"]
                } else {
                    ENTITY_TEXT_FIELDS
                },
            )
            .into();
            if summary {
                format!(
                    "CALL db.index.fulltext.queryNodes('search_entity_summaries', $query) YIELD node AS n, score{}{} ORDER BY score DESC, n.chain_id, n.uuid",
                    node_core(request, false, summary),
                    projection(request)
                )
            } else {
                // Exact identity first: the query as a declared key value of any
                // version (case-folded strings, integers as digits) through the
                // `IdentityValue` index the write path maintains, scored 1.0;
                // then analyzed text relevance normalised below 1.0. One row per
                // version with its best score, then the same visibility, scope
                // and limit as every other node read.
                // `folded_token` is the lower-cased token for strings and the
                // token itself otherwise, so one `IN` over the candidate tokens
                // is a single seek on the (org_id, folded_token) index.
                // A display name equal to the whole query (case-folded) is an
                // exact match too: it is always among the fulltext rows, so it
                // is promoted there to the same perfect score without another
                // index read.
                let raw = q.trim();
                let folded = raw.to_lowercase();
                let mut tokens = vec![format!("s:{folded}")];
                if let Ok(value) = raw.parse::<i64>() {
                    tokens.push(format!("i:{value}"));
                }
                p["raw"] = json!(raw);
                p["raw_folded"] = json!(folded);
                p["identity_tokens"] = json!(tokens);
                format!(
                    "CALL {{ MATCH (v:IdentityValue) WHERE v.org_id = $org_id AND v.folded_token IN $identity_tokens \
                     MATCH (v)-[:KEY_COMPONENT]->(n:Entity) RETURN n, 1.0 AS score \
                     UNION ALL CALL db.index.fulltext.queryNodes('search_entities', $query) YIELD node AS n, score AS relevance \
                     RETURN n, CASE WHEN toLower(n.name) = $raw_folded THEN 1.0 ELSE relevance/(1.0+relevance) END AS score }} \
                     WITH n, max(score) AS score{}{} ORDER BY score DESC, n.chain_id, n.uuid",
                    node_core(request, false, summary),
                    projection(request)
                )
            }
        }
        _ => format!(
            "MATCH (n:Entity){}{} ORDER BY score DESC, n.chain_id, n.uuid",
            node_core(request, false, summary),
            projection(request)
        ),
    };
    Ok(PreparedQuery {
        statement,
        parameters: p,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(query: NodeQuery) -> NodeSearch {
        NodeSearch {
            embedding_text_version: kg_core::embedding::TEXT_VERSION.into(),
            filter: SearchFilter {
                org_id: "acme".into(),
                ..Default::default()
            },
            query,
            chain_ids: Some(vec![]),
            limit: 10,
            min_score: 0.0,
            signals: NodeSignals::default(),
            projection: NodeProjection::Candidate,
        }
    }
    fn similarity() -> NodeQuery {
        NodeQuery::Similarity(kg_core::traits::graph_backend::GraphEmbedding {
            model: "test".into(),
            values: vec![1., 0.],
        })
    }

    #[test]
    fn candidate_projection_keeps_scoring_in_database_without_returning_payload() {
        for query in [
            NodeQuery::ByChain,
            NodeQuery::Fulltext("checkout".into()),
            similarity(),
        ] {
            let mut request = request(query);
            let candidate = nodes(&request).unwrap();
            let returned = candidate.statement.rsplit(" RETURN ").next().unwrap();
            assert!(returned
                .starts_with("n{.uuid, .chain_id, .entity_type, .namespace, .name} AS n, score"));
            assert!(!returned.contains("embedding"));
            request.projection = NodeProjection::Full;
            let full = nodes(&request).unwrap();
            let mut candidate_parameters = candidate.parameters.clone();
            let mut full_parameters = full.parameters.clone();
            candidate_parameters
                .as_object_mut()
                .unwrap()
                .remove("relationship_now");
            full_parameters
                .as_object_mut()
                .unwrap()
                .remove("relationship_now");
            assert_eq!(candidate_parameters, full_parameters);
            assert_eq!(
                candidate.statement.split(" RETURN ").next(),
                full.statement.split(" RETURN ").next()
            );
        }
    }

    #[test]
    fn count_subqueries_are_independently_optional() {
        for observations in [false, true] {
            for dependents in [false, true] {
                let mut r = request(NodeQuery::ByChain);
                r.projection = NodeProjection::Full;
                r.signals = NodeSignals {
                    observations,
                    dependents,
                };
                let query = nodes(&r).unwrap().statement;
                assert_eq!(query.contains("MENTIONS"), observations);
                assert_eq!(query.contains("RELATES_TO"), dependents);
                assert_eq!(
                    query.contains("datetime(r.valid_from) <= datetime($relationship_now)"),
                    dependents
                );
                assert!(!query.contains("r.is_latest"));
            }
        }
    }

    #[test]
    fn keyword_retrieval_scopes_the_organization_inside_the_index() {
        let query = nodes(&request(NodeQuery::Fulltext("prod Service".into()))).unwrap();
        assert_eq!(
            query.parameters["query"],
            r#"+org_id:"acme" +(name:"prod" summary:"prod" prop_summary:"prod" name:"Service" summary:"Service" prop_summary:"Service")"#
        );
        assert!(query.statement.contains("n.org_id = $org_id"));
        assert!(nodes(&request(NodeQuery::Fulltext("   ".into()))).is_err());
    }

    /// Exact declared key values are found through the identity-value index
    /// ahead of analyzed text, every branch shares one visibility pass, and a
    /// version is scored once.
    #[test]
    fn keyword_retrieval_matches_exact_identity_values_first_under_one_visibility_pass() {
        let query = nodes(&request(NodeQuery::Fulltext(" I-0ABC123 ".into()))).unwrap();
        assert_eq!(query.parameters["raw"], "I-0ABC123");
        assert_eq!(query.parameters["identity_tokens"], json!(["s:i-0abc123"]));
        let statement = &query.statement;
        assert!(statement.contains(
            "MATCH (v:IdentityValue) WHERE v.org_id = $org_id AND v.folded_token IN $identity_tokens"
        ));
        assert!(statement.contains("[:KEY_COMPONENT]->(n:Entity) RETURN n, 1.0 AS score"));
        assert!(statement.contains(
            "CASE WHEN toLower(n.name) = $raw_folded THEN 1.0 ELSE relevance/(1.0+relevance) END AS score }"
        ));
        assert_eq!(query.parameters["raw_folded"], "i-0abc123");
        let dedupe = statement.find("WITH n, max(score) AS score").unwrap();
        let visibility = statement.find("n.org_id = $org_id").unwrap();
        let limit = statement.find("LIMIT $limit").unwrap();
        assert!(dedupe < visibility && visibility < limit, "{statement}");
        assert_eq!(statement.matches("n.is_latest = true").count(), 1);
        let numeric = nodes(&request(NodeQuery::Fulltext("123456789012".into()))).unwrap();
        assert_eq!(
            numeric.parameters["identity_tokens"],
            json!(["s:123456789012", "i:123456789012"])
        );
    }

    #[test]
    fn indexed_retrieval_reads_frontier_statistics_from_the_same_index_call() {
        let mut r = request(similarity());
        r.signals.observations = true;
        let query = nodes_with_index(&r, Some(4096)).unwrap();
        assert_eq!(query.parameters["candidates"], 4096);
        assert_eq!(
            query
                .statement
                .matches("db.index.vector.queryNodes")
                .count(),
            1
        );
        assert!(query.statement.contains("OPTIONAL CALL"));
        assert!(query
            .statement
            .contains("index_score * 2 - 1 >= $min_score - 0.00001"));
        assert!(query.statement.contains("dot/(norm*$norm) AS score"));
        // The exact cosine runs only for the page the index score selects.
        let page = query
            .statement
            .find("ORDER BY index_score DESC, n.chain_id, n.uuid LIMIT $limit")
            .unwrap();
        let exact = query.statement.find("dot/(norm*$norm) AS score").unwrap();
        assert!(page < exact, "{}", query.statement);
        let flat = nodes(&request(similarity())).unwrap();
        assert!(!flat.statement.contains("index_score"));
        assert!(query.statement.contains("RETURN n, score, observations"));
        assert!(query.statement.trim_end().ends_with(
            ", observations, raw_count, frontier ORDER BY score DESC, n.chain_id, n.uuid"
        ));
        assert!(!nodes(&r).unwrap().statement.contains("raw_count"));
    }
}
