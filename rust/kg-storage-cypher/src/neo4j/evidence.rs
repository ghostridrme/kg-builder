use crate::filters::{reject_saga_filter, saga_member};
use crate::{filters::*, PreparedQuery};
use kg_core::{errors::BackendError, search::*};
use serde_json::json;

/// Content fields of the `search_relationships` and `search_snapshots` fulltext
/// indexes; `org_id` is the scope field of both.
pub const RELATIONSHIP_TEXT_FIELDS: &[&str] = &["name", "description"];
pub const SNAPSHOT_TEXT_FIELDS: &[&str] = &["name", "source", "content"];

pub fn relationships(request: &EvidenceSearch) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    reject_saga_filter(&request.filter)?;
    let mut p = params(&request.filter, &request.chain_ids, request.limit);
    let prefix = if let Some(query) = &request.query {
        p["query"] = lucene_scoped(&request.filter.org_id, query, RELATIONSHIP_TEXT_FIELDS).into();
        "CALL db.index.fulltext.queryRelationships('search_relationships', $query) YIELD relationship AS r, score
         MATCH (physical_s:Entity)-[r]->(physical_t:Entity)"
    } else {
        "MATCH (anchor:Entity)
         WHERE anchor.org_id=$org_id AND anchor.chain_id IN $chains
         MATCH (anchor)-[r:RELATES_TO]-(:Entity)
         WITH DISTINCT r
         MATCH (physical_s:Entity)-[r]->(physical_t:Entity)
         WITH physical_s, r, physical_t, 1.0 AS score"
    };
    Ok(PreparedQuery {
        statement: relationship_statement(prefix, &request.filter),
        parameters: p,
    })
}

/// Semantic fact recall shares the same scope, endpoint visibility and provenance rules.
pub fn relationship_similarity(
    request: &RelationshipSimilarity,
) -> Result<PreparedQuery, BackendError> {
    relationship_similarity_with_index(request, None)
}

// The vector is bound once: reading `r.embedding` inside the reduce re-reads
// the whole property per element, which made a 1,536-dimensional scan crawl.
const FACT_COSINE: &str = "
        WITH physical_s,r,physical_t,r.embedding AS vector
        WITH physical_s,r,physical_t,
             sqrt(reduce(total=0.0,x IN vector | total+x*x)) AS norm,
             reduce(total=0.0,i IN range(0,size(vector)-1) | total+vector[i]*$vector[i]) AS dot
        WHERE norm>0
        WITH physical_s,r,physical_t,dot/(norm*$norm) AS score
        WHERE score >= $min_score
        WITH physical_s,r,physical_t,score";

/// Indexed facts: the visible candidates are ranked by the index's cosine and cut to
/// the page before the exact cosine is recomputed, so the list arithmetic runs for at
/// most `limit` facts instead of every candidate the index returned (1,692 candidates
/// took about 290 ms; measured 2026-09-27). The index score is the true cosine up to
/// float rounding, so the cut cannot drop a fact the exact score would have paged.
const FACT_RESCORE: &str = "
                ORDER BY score DESC, r.uuid LIMIT $limit
                WITH r,s,t,r.embedding AS vector
                WITH r,s,t,
                     sqrt(reduce(total=0.0,x IN vector | total+x*x)) AS norm,
                     reduce(total=0.0,i IN range(0,size(vector)-1) | total+vector[i]*$vector[i]) AS dot
                WHERE norm>0
                WITH r,s,t,dot/(norm*$norm) AS score
                WHERE score >= $min_score";

pub(crate) fn relationship_similarity_with_index(
    request: &RelationshipSimilarity,
    candidates: Option<usize>,
) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    reject_saga_filter(&request.filter)?;
    let mut p = params(&request.filter, &request.anchor_chains, request.limit);
    p["vector"] = json!(request.embedding.values);
    p["model"] = json!(request.embedding.model);
    p["text_version"] = json!(kg_core::embedding::RELATIONSHIP_TEXT_VERSION);
    p["min_score"] = json!(request.min_score);
    p["norm"] = json!(request
        .embedding
        .values
        .iter()
        .map(|v| f64::from(*v).powi(2))
        .sum::<f64>()
        .sqrt());
    let compatible = "physical_s:Entity AND physical_t:Entity AND r.org_id=$org_id AND r.embedding_model=$model AND r.embedding_text_version=$text_version AND size(r.embedding)=size($vector)";
    let statement = if let Some(budget) = candidates {
        p["candidates"] = budget.into();
        // Leave room for index-score rounding; the final cutoff uses the exact cosine.
        let inner = relationship_statement_rescored(
            &format!(
                "WITH row.r AS r, row.score * 2 - 1 AS score, startNode(row.r) AS physical_s, endNode(row.r) AS physical_t
        WHERE {compatible} AND score >= $min_score - 0.00001
        WITH physical_s,r,physical_t,score"
            ),
            FACT_RESCORE,
            &request.filter,
        );
        format!(
            "CALL db.index.vector.queryRelationships('search_relationship_vectors', $candidates, $vector) YIELD relationship, score
            WITH collect({{r: relationship, score: score}}) AS raw
            WITH raw, size(raw) AS raw_count,
                 CASE WHEN size(raw) = 0 THEN null
                      ELSE reduce(m = 1.0, x IN raw | CASE WHEN x.score < m THEN x.score ELSE m END) * 2 - 1 END AS frontier
            OPTIONAL CALL {{
                WITH raw
                UNWIND raw AS row
                {inner}
            }}
            RETURN uuid, source_chain_id, target_chain_id, name, description, valid_from, valid_to, snapshot_id, score, raw_count, frontier
            ORDER BY score DESC, uuid"
        )
    } else {
        // Anchored requests restrict to the anchors' facts before the cosine runs.
        relationship_statement(
            &format!("MATCH (physical_s:Entity)-[r:RELATES_TO]->(physical_t:Entity)\n        WHERE {compatible} AND ($chains IS NULL OR r.source_chain_id IN $chains OR r.target_chain_id IN $chains){FACT_COSINE}"),
            &request.filter,
        )
    };
    Ok(PreparedQuery {
        statement,
        parameters: p,
    })
}

/// Earliest recorded validity end, invalidation, or deletion of a fact.
const FACT_VALID_TO: &str = "CASE
                    WHEN r.valid_to IS NOT NULL
                      AND (r.invalid_at IS NULL OR datetime(r.valid_to) <= datetime(r.invalid_at))
                      AND (r.deleted_at IS NULL OR datetime(r.valid_to) <= datetime(r.deleted_at)) THEN r.valid_to
                    WHEN r.invalid_at IS NOT NULL
                      AND (r.deleted_at IS NULL OR datetime(r.invalid_at) <= datetime(r.deleted_at)) THEN r.invalid_at
                    ELSE r.deleted_at END";

/// Endpoint visibility and provenance for facts bound to `r`, `physical_s`,
/// `physical_t`, and `score`. Stable chains resolve the endpoint version
/// visible at the requested time, or its earliest classification before recorded
/// history begins. Returned endpoints are chain identities, not entity snapshots. Each endpoint is matched on its own with the
/// key in the pattern so the planner seeks the `(org_id, chain_id)` index; a
/// joint pattern or an index hint produced whole-index scans per fact.
fn fact_endpoints(filter: &SearchFilter) -> String {
    format!(
        r#"WHERE physical_s.org_id = $org_id
                AND physical_t.org_id = $org_id
                AND r.org_id = $org_id
                AND {}
                AND (size($relationship_types) = 0
                OR r.name IN $relationship_types)
                AND ($chains IS NULL
                OR r.source_chain_id IN $chains
                OR r.target_chain_id IN $chains)
                MATCH (s:Entity {{org_id: $org_id, chain_id: r.source_chain_id}})
                WHERE {}
                AND {}
                MATCH (t:Entity {{org_id: $org_id, chain_id: r.target_chain_id}})
                WHERE {}
                AND {}
                WITH r, s, t, score
                WHERE ({}
                OR {})"#,
        relationship_visible("r", filter),
        scope("s", filter),
        fact_endpoint_visible("s", filter),
        scope("t", filter),
        fact_endpoint_visible("t", filter),
        types("s", filter),
        types("t", filter)
    )
}

// A late report can support an earlier fact; as_of is not a knowledge cutoff.
const FACT_SOURCE: &str = r#"OPTIONAL MATCH (source:Snapshot {uuid:r.first_seen_snapshot_id, org_id:$org_id})
                WHERE (size($namespaces) = 0 OR source.namespace IN $namespaces)"#;

fn fact_columns() -> String {
    format!(
        "r.uuid AS uuid,s.chain_id AS source_chain_id,t.chain_id AS target_chain_id,r.name AS name,r.description AS description,r.valid_from AS valid_from,{FACT_VALID_TO} AS valid_to,source.uuid AS snapshot_id"
    )
}

fn relationship_statement(prefix: &str, filter: &SearchFilter) -> String {
    relationship_statement_rescored(prefix, "", filter)
}

/// `rescore` runs after endpoint visibility on distinct `r,s,t,score` rows and may
/// replace `score`; empty for statements whose score is final.
fn relationship_statement_rescored(prefix: &str, rescore: &str, filter: &SearchFilter) -> String {
    format!(
        "{prefix}
                {}
                WITH DISTINCT r,s,t,score{rescore}
                {FACT_SOURCE}
                RETURN {},score
                ORDER BY score DESC,uuid
                LIMIT $limit",
        fact_endpoints(filter),
        fact_columns()
    )
}

/// Facts attached to ranked anchors in one call. Each anchor's facts are ordered
/// newest first by `valid_from`, then by UUID, and cut at `per_anchor + 1` so
/// truncation is visible per anchor.
pub fn attached_relationships(request: &AttachedEvidence) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    reject_saga_filter(&request.filter)?;
    let mut p = params(&request.filter, &None, request.per_anchor);
    p["anchors"] = json!(request
        .anchors
        .iter()
        .map(uuid::Uuid::to_string)
        .collect::<Vec<_>>());
    let statement = format!(
        "UNWIND $anchors AS anchor
        CALL {{
            WITH anchor
            MATCH (a:Entity {{org_id: $org_id, chain_id: anchor}})-[r:RELATES_TO]-(:Entity)
            WITH DISTINCT r
            MATCH (physical_s:Entity)-[r]->(physical_t:Entity)
            WITH physical_s, r, physical_t, 1.0 AS score
            {}
            WITH DISTINCT r, s, t
            ORDER BY r.valid_from IS NOT NULL DESC, datetime(r.valid_from) DESC, r.uuid
            LIMIT $limit
            {FACT_SOURCE}
            RETURN r, s, t, source
        }}
        RETURN anchor, {}, 1.0 AS score
        ORDER BY anchor, valid_from IS NOT NULL DESC, datetime(valid_from) DESC, uuid",
        fact_endpoints(&request.filter),
        fact_columns()
    );
    Ok(PreparedQuery {
        statement,
        parameters: p,
    })
}

fn snapshot_columns() -> &'static str {
    "snap.uuid AS uuid,snap.name AS name,snap.source AS source,snap.namespace AS namespace,snap.captured_at AS captured_at,substring(coalesce(snap.content,''),0,$source_scan_chars) AS content,size(coalesce(snap.content,'')) AS content_length"
}

pub fn snapshots(request: &EvidenceSearch) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    let mut p = params(&request.filter, &request.chain_ids, request.limit);
    let prefix = if let Some(query) = &request.query {
        p["query"] = lucene_scoped(&request.filter.org_id, query, SNAPSHOT_TEXT_FIELDS).into();
        "CALL db.index.fulltext.queryNodes('search_snapshots', $query) YIELD node AS snap, score"
    } else {
        "MATCH (observed:Entity)
         WHERE observed.org_id=$org_id AND observed.chain_id IN $chains
         MATCH (snap:Snapshot)-[:MENTIONS]->(observed)
         WITH DISTINCT snap, 1.0 AS score"
    };
    p["source_scan_chars"] = json!(crate::SOURCE_SCAN_CHARS);
    let query = format!(
        r#"{prefix}
                WHERE {}
                AND {}
                AND ($as_of IS NULL
                OR datetime(snap.captured_at) <= datetime($as_of))
                CALL {{
                WITH snap
                MATCH (snap)-[o:MENTIONS]->(physical:Entity), (n:Entity)
                WHERE physical.org_id = $org_id
                AND n.chain_id = physical.chain_id
                AND {}
                AND {}
                AND {}
                AND ($chains IS NULL
                OR n.chain_id IN $chains)
                AND o.org_id = $org_id
                AND ($as_of IS NULL
                OR datetime(o.observed_at) <= datetime($as_of))
                RETURN count(n) AS matching_entities
                }}
                WITH snap, matching_entities, score
                WHERE ($chains IS NULL AND size($types) = 0) OR matching_entities > 0
                RETURN {},score
                ORDER BY score DESC,uuid
                LIMIT $limit"#,
        scope("snap", &request.filter),
        saga_member("snap"),
        scope("n", &request.filter),
        types("n", &request.filter),
        entity_visible("n", &request.filter),
        snapshot_columns()
    );
    Ok(PreparedQuery {
        statement: query,
        parameters: p,
    })
}

/// Snapshots attached to ranked anchors in one call: the anchor chain is
/// re-checked against the filter, then its observing snapshots are returned
/// newest capture first, cut at `per_anchor + 1` per anchor.
pub fn attached_snapshots(request: &AttachedEvidence) -> Result<PreparedQuery, BackendError> {
    request.validate()?;
    let mut p = params(&request.filter, &None, request.per_anchor);
    p["anchors"] = json!(request
        .anchors
        .iter()
        .map(uuid::Uuid::to_string)
        .collect::<Vec<_>>());
    p["source_scan_chars"] = json!(crate::SOURCE_SCAN_CHARS);
    let statement = format!(
        r#"UNWIND $anchors AS anchor
        CALL {{
            WITH anchor
            MATCH (n:Entity)
            WHERE n.org_id = $org_id AND n.chain_id = anchor AND {} AND {} AND {}
            WITH DISTINCT n.chain_id AS chain
            MATCH (snap:Snapshot)-[o:MENTIONS]->(observed:Entity)
            WHERE observed.org_id = $org_id AND observed.chain_id = chain
            AND o.org_id = $org_id AND {} AND {}
            AND ($as_of IS NULL OR (datetime(snap.captured_at) <= datetime($as_of)
                AND datetime(o.observed_at) <= datetime($as_of)))
            WITH DISTINCT snap
            ORDER BY snap.captured_at IS NOT NULL DESC, datetime(snap.captured_at) DESC, snap.uuid
            LIMIT $limit
            RETURN snap
        }}
        RETURN anchor, {}, 1.0 AS score
        ORDER BY anchor, captured_at IS NOT NULL DESC, datetime(captured_at) DESC, uuid"#,
        scope("n", &request.filter),
        types("n", &request.filter),
        entity_visible("n", &request.filter),
        scope("snap", &request.filter),
        saga_member("snap"),
        snapshot_columns()
    );
    Ok(PreparedQuery {
        statement,
        parameters: p,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_evidence_uses_scoped_literal_index_queries_and_limits_only_eligible_rows() {
        let request = EvidenceSearch {
            filter: SearchFilter {
                org_id: "acme".into(),
                namespaces: vec!["prod".into()],
                ..Default::default()
            },
            query: Some(r#"arn:aws:lambda a/b.rs OR "*" \x"#.into()),
            passage_query: None,
            chain_ids: None,
            limit: 3,
        };
        for (query, fields) in [
            (relationships(&request).unwrap(), RELATIONSHIP_TEXT_FIELDS),
            (snapshots(&request).unwrap(), SNAPSHOT_TEXT_FIELDS),
        ] {
            assert!(query.statement.starts_with("CALL db.index.fulltext.query"));
            assert_eq!(query.statement.matches("LIMIT").count(), 1);
            assert!(query.statement.ends_with("LIMIT $limit"));
            assert!(!query.statement.contains("CONTAINS"));
            assert!(!query.statement.contains("arn:aws"));
            assert_eq!(query.parameters["limit"], 4);
            assert_eq!(
                query.parameters["query"],
                lucene_scoped("acme", request.query.as_ref().unwrap(), fields)
            );
            assert!(query.parameters["query"]
                .as_str()
                .unwrap()
                .starts_with(r#"+org_id:"acme" +("#));
            assert!(query.statement.contains("$namespaces"));
            if fields == SNAPSHOT_TEXT_FIELDS {
                assert!(query.statement.contains("$as_of"));
            }
        }
        let attached = EvidenceSearch {
            query: None,
            chain_ids: Some(vec![uuid::Uuid::from_u128(1)]),
            ..request.clone()
        };
        for query in [
            relationships(&attached).unwrap(),
            snapshots(&attached).unwrap(),
        ] {
            assert!(!query.statement.contains("db.index.fulltext"));
            assert!(query.statement.contains("1.0 AS score"));
        }
        assert!(relationships(&EvidenceSearch {
            query: Some(" ".into()),
            ..request
        })
        .is_err());
    }

    #[test]
    fn passage_query_cannot_enable_unscoped_retrieval_or_exceed_input_budget() {
        let mut request = EvidenceSearch {
            filter: SearchFilter {
                org_id: "acme".into(),
                ..Default::default()
            },
            query: None,
            passage_query: Some("needle".into()),
            chain_ids: None,
            limit: 10,
        };
        assert!(snapshots(&request).is_err());
        request.chain_ids = Some(vec![uuid::Uuid::from_u128(1)]);
        let query = snapshots(&request).unwrap();
        assert!(query.parameters["query"].is_null());
        assert_eq!(
            query.parameters["source_scan_chars"],
            crate::SOURCE_SCAN_CHARS
        );
        request.passage_query = Some("x".repeat(8193));
        assert!(snapshots(&request).is_err());
    }

    fn similarity() -> RelationshipSimilarity {
        RelationshipSimilarity {
            filter: SearchFilter {
                org_id: "acme".into(),
                namespaces: vec!["prod".into()],
                ..Default::default()
            },
            embedding: kg_core::traits::graph_backend::GraphEmbedding {
                model: "test".into(),
                values: vec![1.0, 0.0],
            },
            limit: 5,
            min_score: 0.2,
            anchor_chains: None,
        }
    }

    #[test]
    fn semantic_facts_filter_compatibility_and_visibility_before_the_limit() {
        let query = relationship_similarity(&similarity()).unwrap();
        assert_eq!(query.parameters["limit"], 6);
        assert_eq!(
            query.parameters["text_version"],
            kg_core::embedding::RELATIONSHIP_TEXT_VERSION
        );
        let before_limit = query.statement.split("LIMIT").next().unwrap();
        for rule in [
            "r.embedding_text_version=$text_version",
            "datetime(r.valid_from) <= datetime($relationship_now)",
            "r.org_id = $org_id",
            "chain_id: r.source_chain_id",
            "r.first_seen_snapshot_id",
        ] {
            assert!(before_limit.contains(rule), "missing {rule}");
        }
        assert!(!query.statement.contains("raw_count"));
        assert!(!query.statement.contains("datetime(source.captured_at)"));
    }

    #[test]
    fn indexed_facts_share_one_index_call_with_the_frontier_statistics() {
        let query = relationship_similarity_with_index(&similarity(), Some(4096)).unwrap();
        assert_eq!(query.parameters["candidates"], 4096);
        assert_eq!(
            query
                .statement
                .matches("db.index.vector.queryRelationships")
                .count(),
            1
        );
        assert!(query.statement.contains("OPTIONAL CALL"));
        assert!(query
            .statement
            .contains("row.score * 2 - 1 AS score, startNode(row.r) AS physical_s"));
        assert!(query.statement.contains("score >= $min_score - 0.00001"));
        // Exact cosine only for the page selected by the index score, after visibility.
        let visibility = query.statement.find("chain_id: r.target_chain_id").unwrap();
        let page = query
            .statement
            .find("ORDER BY score DESC, r.uuid LIMIT $limit")
            .unwrap();
        let exact = query.statement.find("dot/(norm*$norm) AS score").unwrap();
        assert!(visibility < page && page < exact, "{}", query.statement);
        assert_eq!(query.statement.matches("dot/(norm*$norm)").count(), 1);
        let exact_only = relationship_similarity(&similarity()).unwrap();
        assert!(!exact_only.statement.contains("row.score"));
        assert_eq!(exact_only.statement.matches("dot/(norm*$norm)").count(), 1);
        assert!(query
            .statement
            .contains("(s:Entity {org_id: $org_id, chain_id: r.source_chain_id})"));
        assert!(query
            .statement
            .trim_end()
            .ends_with("score, raw_count, frontier\n            ORDER BY score DESC, uuid"));
    }

    #[test]
    fn attached_evidence_is_grouped_per_anchor_and_ordered_by_recency() {
        let request = AttachedEvidence {
            filter: SearchFilter {
                org_id: "acme".into(),
                namespaces: vec!["prod".into()],
                entity_types: vec!["Service".into()],
                ..Default::default()
            },
            anchors: vec![uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2)],
            per_anchor: 3,
            passage_query: Some("needle".into()),
        };
        let facts = attached_relationships(&request).unwrap();
        let sources = attached_snapshots(&request).unwrap();
        for query in [&facts, &sources] {
            assert!(query.statement.starts_with("UNWIND $anchors AS anchor"));
            assert_eq!(query.statement.matches("LIMIT $limit").count(), 1);
            assert_eq!(query.parameters["limit"], 4);
            assert_eq!(query.parameters["anchors"].as_array().unwrap().len(), 2);
            assert!(query.statement.contains("RETURN anchor,"));
            assert!(query.statement.contains("$namespaces"));
            assert!(!query.statement.contains("db.index"));
        }
        assert!(facts.statement.contains(
            "ORDER BY r.valid_from IS NOT NULL DESC, datetime(r.valid_from) DESC, r.uuid"
        ));
        assert!(facts.statement.contains("s.entity_type IN $types"));
        assert!(facts.statement.ends_with(
            "ORDER BY anchor, valid_from IS NOT NULL DESC, datetime(valid_from) DESC, uuid"
        ));
        assert!(sources
            .statement
            .contains("ORDER BY snap.captured_at IS NOT NULL DESC, datetime(snap.captured_at) DESC, snap.uuid"));
        assert!(sources.statement.contains("n.entity_type IN $types"));
        assert!(sources.statement.ends_with(
            "ORDER BY anchor, captured_at IS NOT NULL DESC, datetime(captured_at) DESC, uuid"
        ));
        assert!(sources
            .statement
            .contains("datetime(o.observed_at) <= datetime($as_of)"));
        for invalid in [
            AttachedEvidence {
                anchors: vec![],
                ..request.clone()
            },
            AttachedEvidence {
                anchors: vec![uuid::Uuid::from_u128(1); 2],
                ..request.clone()
            },
            AttachedEvidence {
                per_anchor: 0,
                ..request.clone()
            },
            AttachedEvidence {
                anchors: (1..=100).map(uuid::Uuid::from_u128).collect(),
                per_anchor: 500,
                ..request.clone()
            },
        ] {
            assert!(attached_relationships(&invalid).is_err());
        }
    }

    // ---- merged from `mod saga_filter_tests`

    use uuid::Uuid;

    fn filter(saga: Option<Uuid>) -> SearchFilter {
        SearchFilter {
            org_id: "acme".into(),
            namespaces: vec!["prod".into()],
            saga_uuid: saga,
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_readers_apply_saga_membership_before_the_limit() {
        let saga = Uuid::from_u128(7);
        let request = EvidenceSearch {
            filter: filter(Some(saga)),
            query: Some("needle".into()),
            passage_query: None,
            chain_ids: None,
            limit: 5,
        };
        let query = snapshots(&request).unwrap();
        let predicate = query
            .statement
            .find("HAS_EPISODE")
            .expect("membership predicate");
        assert!(predicate < query.statement.find("LIMIT").unwrap());
        assert!(query.statement.contains("$saga_uuid IS NULL OR EXISTS"));
        assert_eq!(query.parameters["saga_uuid"], saga.to_string());
        let open = snapshots(&EvidenceSearch {
            filter: filter(None),
            ..request.clone()
        })
        .unwrap();
        assert_eq!(open.parameters["saga_uuid"], serde_json::Value::Null);

        let attached = attached_snapshots(&AttachedEvidence {
            filter: filter(Some(saga)),
            anchors: vec![Uuid::from_u128(1)],
            per_anchor: 3,
            passage_query: None,
        })
        .unwrap();
        assert!(attached.statement.contains("HAS_EPISODE"));
        assert_eq!(attached.parameters["saga_uuid"], saga.to_string());
    }

    #[test]
    fn relationship_readers_refuse_a_saga_filter() {
        let saga = Some(Uuid::from_u128(7));
        let evidence = EvidenceSearch {
            filter: filter(saga),
            query: Some("needle".into()),
            passage_query: None,
            chain_ids: None,
            limit: 5,
        };
        assert!(matches!(
            relationships(&evidence),
            Err(BackendError::Query(_))
        ));
        assert!(matches!(
            attached_relationships(&AttachedEvidence {
                filter: filter(saga),
                anchors: vec![Uuid::from_u128(1)],
                per_anchor: 3,
                passage_query: None,
            }),
            Err(BackendError::Query(_))
        ));
        assert!(matches!(
            relationship_similarity(&RelationshipSimilarity {
                filter: filter(saga),
                embedding: kg_core::traits::graph_backend::GraphEmbedding {
                    model: "m".into(),
                    values: vec![1.0, 0.0],
                },
                limit: 5,
                min_score: 0.0,
                anchor_chains: None,
            }),
            Err(BackendError::Query(_))
        ));
    }
}
