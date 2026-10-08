use chrono::Utc;
use kg_core::errors::BackendError;
use kg_core::search::SearchFilter;
use serde_json::{json, Value};
use uuid::Uuid;
/// Every source-validity boundary is exclusive at its end.
fn interval_visible(v: &str, at: &str) -> String {
    format!(
        r#"({v}.valid_from IS NOT NULL
            AND datetime({v}.valid_from) <= datetime(${at})
            AND ({v}.valid_to IS NULL OR datetime(${at}) < datetime({v}.valid_to))
            AND ({v}.invalid_at IS NULL OR datetime(${at}) < datetime({v}.invalid_at))
            AND ({v}.deleted_at IS NULL OR datetime(${at}) < datetime({v}.deleted_at)))"#
    )
}

/// Relationship validity follows effective time, independent of the stored head.
pub(crate) fn relationship_visible(v: &str, filter: &SearchFilter) -> String {
    let validity = interval_visible(
        v,
        if filter.as_of.is_some() {
            "as_of"
        } else {
            "relationship_now"
        },
    );
    format!("({validity} AND {v}.cancelled_at IS NULL)")
}

fn visible(v: &str, filter: &SearchFilter) -> String {
    if filter.as_of.is_none() {
        return format!("({v}.is_latest = true AND {v}.valid_to IS NULL AND {v}.invalid_at IS NULL AND {v}.deleted_at IS NULL)");
    }
    interval_visible(v, "as_of")
}
/// Merge periods suppress entities only during the source-time interval.
pub(crate) fn entity_visible(v: &str, filter: &SearchFilter) -> String {
    let validity = visible(v, filter);
    if filter.as_of.is_none() {
        return format!("({validity} AND {v}.merged_into IS NULL)");
    }
    format!("({validity} AND NOT EXISTS {{ MATCH (merge:ChainMerge {{org_id: $org_id, loser_chain_id: {v}.chain_id}}) WHERE datetime(merge.valid_from) <= datetime($as_of) AND (merge.valid_to IS NULL OR datetime($as_of) < datetime(merge.valid_to)) }})")
}

/// A fact may establish chain identity before its first recorded entity version.
/// Use that earliest version only for classification, never historical hydration.
/// Once history begins, ordinary visibility applies; gaps are not backfilled.
pub(crate) fn fact_endpoint_visible(v: &str, filter: &SearchFilter) -> String {
    let ordinary = entity_visible(v, filter);
    if filter.as_of.is_none() {
        return ordinary;
    }
    format!(
        r#"({ordinary} OR (
        datetime({v}.valid_from) > datetime($as_of)
        AND ({v}.deleted_at IS NULL OR datetime({v}.deleted_at) > datetime($as_of))
        AND ({v}.invalid_at IS NULL OR datetime({v}.invalid_at) > datetime($as_of))
        AND ({v}.valid_to IS NULL OR datetime({v}.valid_to) > datetime($as_of))
        AND NOT EXISTS {{
            MATCH (earlier:Entity {{org_id: $org_id, chain_id: {v}.chain_id}})
            WHERE datetime(earlier.valid_from) < datetime({v}.valid_from)
               OR (datetime(earlier.valid_from) = datetime({v}.valid_from) AND earlier.uuid < {v}.uuid)
        }}
        AND NOT EXISTS {{
            MATCH (merge:ChainMerge {{org_id: $org_id, loser_chain_id: {v}.chain_id}})
            WHERE datetime(merge.valid_from) <= datetime($as_of)
              AND (merge.valid_to IS NULL OR datetime($as_of) < datetime(merge.valid_to))
        }}))"#
    )
}

pub(crate) fn scope(v: &str, filter: &SearchFilter) -> String {
    let mut predicate = format!("{v}.org_id = $org_id");
    if !filter.namespaces.is_empty() {
        predicate.push_str(&format!(" AND {v}.namespace IN $namespaces"));
    }
    predicate
}
pub(crate) fn types(v: &str, filter: &SearchFilter) -> String {
    if filter.entity_types.is_empty() {
        "true".into()
    } else {
        format!("{v}.entity_type IN $types")
    }
}
/// Saga membership constrains snapshot reads only. Every other read refuses a
/// filter that carries it, so the constraint can never be dropped silently.
pub(crate) fn reject_saga_filter(filter: &SearchFilter) -> Result<(), BackendError> {
    if filter.saga_uuid.is_some() {
        return Err(BackendError::Query(
            "Saga filter applies to snapshot search only".into(),
        ));
    }
    Ok(())
}

/// `true` unless the filter names a Saga the snapshot must belong to. Evaluated
/// with the other scope predicates, before any candidate limit.
pub(crate) fn saga_member(snapshot: &str) -> String {
    format!("($saga_uuid IS NULL OR EXISTS {{ MATCH (:Saga {{org_id:$org_id,uuid:$saga_uuid}})-[m:HAS_EPISODE]->({snapshot}) WHERE m.org_id = $org_id }})")
}

pub(crate) fn params(filter: &SearchFilter, chains: &Option<Vec<Uuid>>, limit: usize) -> Value {
    json!({"org_id":filter.org_id,"namespaces":filter.namespaces,"types":filter.entity_types,
        "relationship_types":filter.relationship_types,"as_of":filter.as_of.map(|t|t.to_rfc3339()),
        "saga_uuid":filter.saga_uuid.map(|u|u.to_string()),
        "relationship_now":filter.relationship_now.unwrap_or_else(Utc::now).to_rfc3339(),
        "chains":chains.as_ref().map(|v|v.iter().map(Uuid::to_string).collect::<Vec<_>>()),"limit":limit+1})
}

fn phrase(term: &str) -> String {
    format!("\"{}\"", term.replace('\\', "\\\\").replace('"', "\\\""))
}

fn has_tokens(term: &str) -> bool {
    term.chars().any(char::is_alphanumeric)
}

/// Literal terms restricted to the named content fields, with the organization
/// required inside the index so other tenants' documents are not scored. The
/// analyzed organization field is a prefilter only: the exact `org_id` predicate
/// after retrieval remains the authorization boundary.
///
/// Text without a letter or digit produces no analyzer tokens. Lucene drops such
/// empty clauses, which would leave a bare organization clause matching every
/// tenant document, so a query with no usable term falls back to plain quoted
/// phrases, which match nothing. An organization identity with no tokens skips
/// the index clause instead of starving the tenant.
pub(crate) fn lucene_scoped(org_id: &str, input: &str, fields: &[&str]) -> String {
    let terms: Vec<_> = input
        .split_whitespace()
        .filter(|term| has_tokens(term))
        .map(phrase)
        .collect();
    if terms.is_empty() {
        return input
            .split_whitespace()
            .map(phrase)
            .collect::<Vec<_>>()
            .join(" ");
    }
    let mut content = Vec::with_capacity(terms.len() * fields.len());
    for term in &terms {
        for field in fields {
            content.push(format!("{field}:{term}"));
        }
    }
    let content = content.join(" ");
    if has_tokens(org_id) {
        format!("+org_id:{} +({content})", phrase(org_id))
    } else {
        content
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relationship_time_is_bound_once_and_does_not_use_the_head_flag() {
        let mut filter = SearchFilter {
            org_id: "test".into(),
            ..Default::default()
        };
        let before = Utc::now();
        let bound = params(&filter, &None, 1);
        let at: chrono::DateTime<Utc> =
            bound["relationship_now"].as_str().unwrap().parse().unwrap();
        assert!(before <= at && at <= Utc::now());
        for historical in [false, true] {
            filter.as_of = historical.then_some(at);
            let predicate = relationship_visible("r", &filter);
            let time = if historical {
                "$as_of"
            } else {
                "$relationship_now"
            };
            assert!(predicate.contains("r.valid_from IS NOT NULL"));
            assert!(predicate.contains(&format!("datetime(r.valid_from) <= datetime({time})")));
            for end in ["valid_to", "invalid_at", "deleted_at"] {
                assert!(predicate.contains(&format!("datetime({time}) < datetime(r.{end})")));
            }
            assert!(predicate.contains("r.cancelled_at IS NULL"));
            assert!(!predicate.contains("is_latest"));
            assert!(!predicate.contains("datetime()"));
        }
        filter.as_of = None;
        filter.relationship_now = Some(at);
        assert_eq!(
            params(&filter, &None, 1)["relationship_now"],
            bound["relationship_now"]
        );
        assert!(entity_visible("n", &filter).contains("n.is_latest = true"));
    }

    #[test]
    fn fact_prehistory_does_not_change_entity_or_current_visibility() {
        let mut filter = SearchFilter {
            org_id: "test".into(),
            ..Default::default()
        };
        assert_eq!(
            fact_endpoint_visible("n", &filter),
            entity_visible("n", &filter)
        );
        filter.as_of = Some("2026-01-01T00:00:00Z".parse().unwrap());
        let fact = fact_endpoint_visible("n", &filter);
        assert!(fact.contains("chain_id: n.chain_id"));
        assert!(fact.contains("earlier.uuid < n.uuid"));
        assert!(fact.contains("loser_chain_id: n.chain_id"));
        assert!(!entity_visible("n", &filter).contains("earlier"));
    }

    #[test]
    fn every_term_is_a_quoted_literal_so_operators_paths_and_wildcards_are_never_interpreted() {
        assert_eq!(
            lucene_scoped(
                "acme",
                r#"arn:aws:ec2 src/main.rs OR * foo"bar a\b"#,
                &["name"]
            ),
            r#"+org_id:"acme" +(name:"arn:aws:ec2" name:"src/main.rs" name:"OR" name:"foo\"bar" name:"a\\b")"#
        );
    }

    #[test]
    fn scoped_query_requires_the_organization_and_targets_content_fields_only() {
        assert_eq!(
            lucene_scoped(
                "acme.corp-1_x",
                r#"prod Service"#,
                &["name", "prop_summary"]
            ),
            r#"+org_id:"acme.corp-1_x" +(name:"prod" prop_summary:"prod" name:"Service" prop_summary:"Service")"#
        );
        assert_eq!(
            lucene_scoped(r#"a"b\c"#, "x", &["name"]),
            r#"+org_id:"a\"b\\c" +(name:"x")"#
        );
        // Punctuation-only identities have no analyzer tokens: no index clause, exact filter only.
        assert_eq!(lucene_scoped("***", "x", &["name"]), r#"name:"x""#);
        // Terms without tokens never leave a bare organization clause behind.
        assert_eq!(lucene_scoped("acme", "* --", &["name"]), r#""*" "--""#);
        assert_eq!(
            lucene_scoped("acme", "* needle", &["name"]),
            r#"+org_id:"acme" +(name:"needle")"#
        );
    }
}
