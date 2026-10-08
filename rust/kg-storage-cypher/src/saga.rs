//! Scoped Saga reads and atomic association/summary writes.
use crate::{PreparedQuery, PreparedWrite};
use kg_core::{errors::BackendError, saga::*, traits::GraphProperties};
use serde_json::{json, Value};

pub fn read(org: &str, request: &SagaRead) -> Result<PreparedQuery, BackendError> {
    request.validate(org)?;
    let mut p = json!({"org":org,"namespace":request.namespace()});
    let statement = match request {
        SagaRead::State { reference, .. } => {
            let predicate = match reference {
                ThreadReference::Name { name } => {
                    p["scope"] = json!(saga_uuid(org, request.namespace(), name));
                    "scope_id:$scope"
                }
                ThreadReference::Uuid { uuid } => {
                    p["uuid"] = json!(uuid);
                    "uuid:$uuid"
                }
            };
            format!("MATCH (s:Saga {{{predicate},org_id:$org,namespace:$namespace}}) RETURN s{{.*}} AS node LIMIT 2")
        }
        SagaRead::Members {
            saga_uuid,
            after_ordinal,
            through_ordinal,
            limit,
            captured_through,
            ..
        } => {
            p["saga"] = json!(saga_uuid);
            p["after"] = json!(after_ordinal);
            p["through"] = json!(through_ordinal.unwrap_or(i64::MAX as u64));
            p["limit"] = json!(limit + 1);
            // Compare instants: RFC 3339 strings can differ in offset and precision.
            p["captured_through"] = json!(captured_through.map(|t| t.to_rfc3339()));
            format!("MATCH (s:Saga {{org_id:$org,namespace:$namespace,uuid:$saga}})-[m:HAS_EPISODE]->(n:Snapshot) USING INDEX m:HAS_EPISODE(saga_uuid,ordinal) WHERE m.saga_uuid=$saga AND m.ordinal>$after AND m.ordinal<=$through AND ($captured_through IS NULL OR datetime(m.captured_at)<=datetime($captured_through)) AND m.org_id=$org AND m.namespace=$namespace AND n.org_id=$org AND n.namespace=$namespace RETURN {} ORDER BY m.saga_uuid ASC,m.ordinal ASC LIMIT $limit",member_projection())
        }
        SagaRead::List {
            offset,
            limit,
            as_of,
            ..
        } => {
            p["offset"] = json!(offset);
            p["limit"] = json!(limit + 1);
            // Visibility is filtered before SKIP/LIMIT so offsets count visible rows.
            // A Saga exists from its earliest capture; older nodes fall back to created_at.
            p["as_of"] = json!(as_of.map(|t| t.to_rfc3339()));
            "MATCH (s:Saga {org_id:$org,namespace:$namespace}) WHERE $as_of IS NULL OR datetime(coalesce(s.first_captured_at,s.created_at))<=datetime($as_of) RETURN s{.*} AS node ORDER BY s.name ASC,s.uuid ASC SKIP $offset LIMIT $limit".to_owned()
        }
        SagaRead::Member {
            saga_uuid,
            snapshot_uuid,
            ..
        } => {
            p["saga"] = json!(saga_uuid);
            p["snapshot"] = json!(snapshot_uuid);
            format!("MATCH (s:Saga {{org_id:$org,namespace:$namespace,uuid:$saga}})-[m:HAS_EPISODE]->(n:Snapshot {{org_id:$org,namespace:$namespace,uuid:$snapshot}}) WHERE m.saga_uuid=$saga RETURN {} LIMIT 2",member_projection())
        }
        SagaRead::Latest {
            saga_uuid,
            excluding_snapshot_uuid,
            ..
        } => {
            p["saga"] = json!(saga_uuid);
            p["excluding"] = json!(excluding_snapshot_uuid);
            p["limit"] = json!(1);
            recent(false)
        }
        SagaRead::Context {
            saga_uuid,
            captured_before,
            limit,
            ..
        } => {
            p["saga"] = json!(saga_uuid);
            p["cutoff"] = json!(captured_before.to_rfc3339());
            p["limit"] = json!(limit + 1);
            recent(true)
        }
    };
    Ok(PreparedQuery {
        statement,
        parameters: p,
    })
}
fn member_projection() -> &'static str {
    "n.uuid AS snapshot_uuid,m.captured_at AS captured_at,m.snapshot_created_at AS created_at,m.ordinal AS ordinal,m.previous_snapshot_uuid AS previous_snapshot_uuid,n.name AS snapshot_name,n.source AS snapshot_source"
}
fn recent(context: bool) -> String {
    let predicate = if context {
        "datetime(m.captured_at)<=datetime($cutoff) AND n.content IS :: STRING AND trim(n.content)<>''"
    } else {
        "($excluding IS NULL OR n.uuid<>$excluding)"
    };
    format!("MATCH (s:Saga)-[m:HAS_EPISODE]->(n:Snapshot) USING INDEX m:HAS_EPISODE(saga_uuid,captured_at,snapshot_created_at,ordinal) WHERE m.saga_uuid=$saga AND m.captured_at IS NOT NULL AND m.snapshot_created_at IS NOT NULL AND m.ordinal IS NOT NULL AND {predicate} AND s.uuid=$saga AND s.org_id=$org AND s.namespace=$namespace AND m.org_id=$org AND m.namespace=$namespace AND n.org_id=$org AND n.namespace=$namespace RETURN {} ORDER BY m.saga_uuid DESC,datetime(m.captured_at) DESC,datetime(m.snapshot_created_at) DESC,m.ordinal DESC LIMIT $limit",member_projection())
}
pub fn decode(
    request: &SagaRead,
    mut rows: Vec<GraphProperties>,
) -> Result<SagaReadResult, BackendError> {
    let bad = || BackendError::Deserialization("invalid Saga read result".into());
    match request {
        SagaRead::State { .. } => {
            if rows.len() > 1 {
                return Err(bad());
            }
            let node = rows
                .pop()
                .map(|mut r| {
                    serde_json::from_value::<kg_core::models::ThreadNode>(
                        r.remove("node").unwrap_or(Value::Null),
                    )
                    .map_err(|_| bad())
                })
                .transpose()?;
            if let Some(node) = &node {
                node.validate().map_err(|_| bad())?;
            }
            Ok(SagaReadResult::State(node))
        }
        SagaRead::Latest { .. } | SagaRead::Member { .. } => {
            if rows.len() > 1 {
                return Err(bad());
            }
            Ok(SagaReadResult::Member(
                rows.pop()
                    .map(|r| serde_json::from_value(Value::Object(r)).map_err(|_| bad()))
                    .transpose()?,
            ))
        }
        SagaRead::Members { limit, .. } | SagaRead::Context { limit, .. } => {
            let truncated = rows.len() > *limit;
            rows.truncate(*limit);
            let members = rows
                .into_iter()
                .map(|r| serde_json::from_value(Value::Object(r)).map_err(|_| bad()))
                .collect::<Result<_, _>>()?;
            Ok(SagaReadResult::Members(SagaMemberPage {
                members,
                truncated,
            }))
        }
        SagaRead::List { limit, .. } => {
            let truncated = rows.len() > *limit;
            rows.truncate(*limit);
            let sagas = rows
                .into_iter()
                .map(|mut r| {
                    let node: kg_core::models::ThreadNode =
                        serde_json::from_value(r.remove("node").unwrap_or(Value::Null))
                            .map_err(|_| bad())?;
                    node.validate().map_err(|_| bad())?;
                    Ok(node)
                })
                .collect::<Result<_, BackendError>>()?;
            Ok(SagaReadResult::Sagas(SagaPage { sagas, truncated }))
        }
    }
}
fn guarded(statement: String, parameters: Value) -> PreparedWrite {
    PreparedWrite {
        statement: format!(
            "CALL {{ {statement} }} RETURN count(*)=1 AS ok,count(*)<>1 AS saga_conflict"
        ),
        parameters,
        expected_rows: 1,
    }
}
pub fn associate(org: &str, a: &ThreadAssociationWrite) -> Vec<PreparedWrite> {
    let scope = saga_uuid(org, &a.namespace, &a.name);
    let p = json!({"org":org,"namespace":a.namespace,"saga":a.saga_uuid,"scope":scope,"name":a.name,"created":a.created_at.to_rfc3339(),"revision":a.expected_revision,"snapshot":a.snapshot_uuid,"previous":a.previous_snapshot_uuid,"membership":a.membership_uuid,"next":a.next_uuid});
    vec![guarded(ASSOCIATE.into(), p)]
}
const ASSOCIATE:&str="MERGE (s:Saga {scope_id:$scope}) ON CREATE SET s:GraphNode,s.uuid=$saga,s.org_id=$org,s.namespace=$namespace,s.name=$name,s.labels=[],s.created_at=$created,s.summary='',s.summary_supporting_snapshot_uuids=[],s.revision=0,s.last_membership_ordinal=0,s.summary_cursor=0
SET s.uuid=s.uuid
WITH s WHERE s.uuid=$saga AND s.org_id=$org AND s.namespace=$namespace AND s.name=$name
MATCH (n:Snapshot {org_id:$org,namespace:$namespace,uuid:$snapshot})
WHERE n.captured_at IS :: STRING AND n.created_at IS :: STRING
OPTIONAL MATCH (s)-[existing:HAS_EPISODE]->(n)
WITH s,n,existing WHERE
 (existing IS NOT NULL AND existing.saga_uuid=$saga AND existing.org_id=$org AND existing.namespace=$namespace AND ((existing.previous_snapshot_uuid IS NULL AND $previous IS NULL) OR existing.previous_snapshot_uuid=$previous))
 OR (existing IS NULL AND s.revision=$revision AND NOT EXISTS { MATCH (x:Snapshot)-[r:NEXT_EPISODE]-(n) WHERE r.saga_uuid=$saga }
 AND ($previous IS NOT NULL OR s.last_membership_ordinal=0)
 AND ($previous IS NULL OR EXISTS { MATCH (s)-[:HAS_EPISODE]->(p:Snapshot {org_id:$org,namespace:$namespace,uuid:$previous}) }))
CALL (s,n,existing) {
 WITH s,n,existing WHERE existing IS NULL
 SET s.revision=s.revision+1,s.last_membership_ordinal=s.last_membership_ordinal+1
 CREATE (s)-[m:HAS_EPISODE {uuid:$membership,org_id:$org,namespace:$namespace,saga_uuid:$saga,snapshot_uuid:$snapshot,ordinal:s.last_membership_ordinal,created_at:$created,captured_at:n.captured_at,snapshot_created_at:n.created_at}]->(n)
 SET m.previous_snapshot_uuid=$previous,m.membership_key=toString($saga)+'|'+toString($snapshot),s.first_snapshot_uuid=coalesce(s.first_snapshot_uuid,$snapshot),s.last_snapshot_uuid=$snapshot,
 s.first_captured_at=CASE WHEN s.first_captured_at IS NULL OR datetime(n.captured_at)<datetime(s.first_captured_at) THEN n.captured_at ELSE s.first_captured_at END
 WITH s,n,m OPTIONAL MATCH (p:Snapshot {org_id:$org,namespace:$namespace,uuid:$previous})
 FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END | CREATE (p)-[:NEXT_EPISODE {uuid:$next,org_id:$org,namespace:$namespace,saga_uuid:$saga,source_snapshot_uuid:$previous,target_snapshot_uuid:$snapshot,predecessor_key:toString($saga)+'|'+toString($snapshot),created_at:$created}]->(n))
 RETURN true AS changed
 UNION
 WITH s,n,existing WHERE existing IS NOT NULL RETURN false AS changed
} RETURN true AS ok";

pub fn summary(org: &str, w: &SagaSummaryWrite) -> Vec<PreparedWrite> {
    let evidence:Vec<_>=w.evidence.iter().map(|r|json!({"uuid":r.uuid,"org_id":r.org_id,"namespace":r.namespace,"source":r.source,"data_type":r.data_type,"source_description":r.source_description,"captured_at":r.captured_at.to_rfc3339(),"created_at":r.created_at.to_rfc3339(),"content":r.content})).collect();
    let p = json!({"incomplete":w.incomplete_reason,"org":org,"namespace":w.namespace,"saga":w.saga_uuid,"expected":w.expected_summary_revision,"revision":w.revision,"previous_summary":w.previous_summary,"previous_support":w.previous_supporting_snapshot_uuids,"after":w.after_ordinal,"through":w.through_ordinal,"text":w.summary,"support":w.supporting_snapshot_uuids,"evidence":evidence,"at":w.summarized_at.to_rfc3339(),"capture":w.max_captured_at.to_rfc3339()});
    vec![guarded(SUMMARY.into(), p)]
}
const SUMMARY:&str="MATCH (s:Saga {org_id:$org,namespace:$namespace,uuid:$saga}) SET s.uuid=s.uuid
WITH s WHERE s.summary_cursor=$after AND s.last_membership_ordinal >= $through
 AND ((s.summary_revision IS NULL AND $expected IS NULL) OR s.summary_revision=$expected)
 AND s.summary=$previous_summary AND s.summary_supporting_snapshot_uuids=$previous_support
 MATCH (s)-[m:HAS_EPISODE]->(n:Snapshot {org_id:$org,namespace:$namespace}) USING INDEX m:HAS_EPISODE(saga_uuid,ordinal) WHERE m.saga_uuid=$saga AND m.org_id=$org AND m.namespace=$namespace AND m.ordinal>$after AND m.ordinal<=$through
 WITH s,m,n ORDER BY n.uuid
 SET n.uuid=n.uuid
 WITH s,m,n ORDER BY m.ordinal
 WITH s,collect(n) AS nodes,collect(m.ordinal) AS ordinals
 WHERE size(nodes)=size($evidence) AND all(i IN range(0,size(nodes)-1) WHERE ordinals[i]=$after+i+1
 AND nodes[i].uuid=$evidence[i].uuid AND nodes[i].org_id=$evidence[i].org_id AND nodes[i].namespace=$evidence[i].namespace
 AND nodes[i].source=$evidence[i].source AND nodes[i].data_type=$evidence[i].data_type
 AND ((nodes[i].source_description IS NULL AND $evidence[i].source_description IS NULL) OR nodes[i].source_description=$evidence[i].source_description)
 AND datetime(nodes[i].captured_at)=datetime($evidence[i].captured_at) AND datetime(nodes[i].created_at)=datetime($evidence[i].created_at) AND nodes[i].content=$evidence[i].content)
 FOREACH (_ IN CASE WHEN $incomplete IS NULL THEN [1] ELSE [] END |
 SET s.summary=$text,s.summary_revision=$revision,s.summary_supporting_snapshot_uuids=$support,s.summary_cursor=$through,s.last_summarized_at=$at,
 s.summary_incomplete_reason=null,s.summary_incomplete_from_ordinal=null,
 s.last_summarized_snapshot_captured_at=CASE WHEN s.last_summarized_snapshot_captured_at IS NULL OR datetime(s.last_summarized_snapshot_captured_at)<datetime($capture) THEN $capture ELSE s.last_summarized_snapshot_captured_at END)
 FOREACH (_ IN CASE WHEN $incomplete IS NOT NULL THEN [1] ELSE [] END |
 SET s.summary_incomplete_reason=$incomplete,s.summary_incomplete_from_ordinal=$after+1)
 RETURN true AS ok";

/// Lock existing and newly created Sagas in one UUID order before source writes.
pub fn lock_sagas(
    org: &str,
    mutations: &[kg_core::traits::GraphMutation],
) -> Option<PreparedWrite> {
    use kg_core::traits::GraphMutation::*;
    let mut ids = std::collections::BTreeSet::new();
    let mut snapshots = std::collections::BTreeSet::new();
    let mut creations = std::collections::BTreeMap::new();
    for mutation in mutations {
        match mutation {
            AssociateSagaSnapshot { association: a } => {
                ids.insert(a.saga_uuid);
                snapshots.insert(a.snapshot_uuid);
                creations.entry(a.saga_uuid).or_insert_with(||json!({"uuid":a.saga_uuid,"scope":saga_uuid(org,&a.namespace,&a.name),"namespace":a.namespace,"name":a.name,"created":a.created_at.to_rfc3339()}));
            }
            SetSagaSummary { summary } => {
                ids.insert(summary.saga_uuid);
            }
            UpsertSnapshot { uuid, .. } => {
                snapshots.insert(*uuid);
            }
            _ => {}
        }
    }
    if ids.is_empty() {
        return None;
    }
    Some(PreparedWrite {
        statement: LOCKS.into(),
        parameters: json!({"org":org,"ids":ids,"snapshots":snapshots,"creations":creations.into_values().collect::<Vec<_>>()}),
        expected_rows: 1,
    })
}
const LOCKS:&str="CALL {
 UNWIND $ids AS id MATCH (s:Saga {org_id:$org,uuid:id}) RETURN s.uuid AS id,null AS creation
 UNION ALL
 UNWIND $snapshots AS snapshot MATCH (s:Saga {org_id:$org})-[:HAS_EPISODE]->(:Snapshot {org_id:$org,uuid:snapshot}) RETURN s.uuid AS id,null AS creation
 UNION ALL
 UNWIND $creations AS creation RETURN creation.uuid AS id,creation
}
WITH id,head(collect(creation)) AS creation ORDER BY id
CALL (id,creation) {
 WITH id,creation WHERE creation IS NOT NULL
 MERGE (s:Saga {scope_id:creation.scope}) ON CREATE SET s:GraphNode,s.uuid=id,s.org_id=$org,s.namespace=creation.namespace,s.name=creation.name,s.labels=[],s.created_at=creation.created,s.summary='',s.summary_supporting_snapshot_uuids=[],s.revision=0,s.last_membership_ordinal=0,s.summary_cursor=0
 RETURN s
 UNION
 WITH id,creation WHERE creation IS NULL MATCH (s:Saga {org_id:$org,uuid:id}) RETURN s
}
SET s.uuid=s.uuid
WITH count(s) AS locked RETURN true AS ok";

#[cfg(test)]
mod read_tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn node_row(name: &str) -> GraphProperties {
        let node = json!({
            "summary_supporting_snapshot_uuids": [], "revision": 0, "last_membership_ordinal": 1,
            "summary_revision": null, "summary_cursor": 0, "uuid": Uuid::new_v4(), "org_id": "org",
            "namespace": "prod", "name": name, "labels": [], "created_at": Utc::now(), "summary": "",
            "first_snapshot_uuid": null, "last_snapshot_uuid": null,
            "last_summarized_at": null, "last_summarized_snapshot_captured_at": null
        });
        json!({"node": node}).as_object().unwrap().clone()
    }

    #[test]
    fn listing_is_scoped_ordered_and_reads_one_extra_row_for_truncation() {
        let request = SagaRead::List {
            namespace: "prod".into(),
            offset: 40,
            limit: 20,
            as_of: None,
        };
        let query = read("org", &request).unwrap();
        assert!(query
            .statement
            .starts_with("MATCH (s:Saga {org_id:$org,namespace:$namespace})"));
        assert!(query.statement.contains(
            "WHERE $as_of IS NULL OR datetime(coalesce(s.first_captured_at,s.created_at))<=datetime($as_of)"
        ));
        assert_eq!(query.parameters["as_of"], Value::Null);
        let at: chrono::DateTime<Utc> = "2026-01-05T00:00:00Z".parse().unwrap();
        let historical = read(
            "org",
            &SagaRead::List {
                namespace: "prod".into(),
                offset: 0,
                limit: 5,
                as_of: Some(at),
            },
        )
        .unwrap();
        assert_eq!(historical.parameters["as_of"], at.to_rfc3339());
        assert!(
            ASSOCIATE.contains("s.first_captured_at=CASE WHEN s.first_captured_at IS NULL OR datetime(n.captured_at)<datetime(s.first_captured_at) THEN n.captured_at ELSE s.first_captured_at END"),
            "membership keeps the earliest capture time for historical visibility"
        );
        assert!(query
            .statement
            .contains("ORDER BY s.name ASC,s.uuid ASC SKIP $offset LIMIT $limit"));
        assert_eq!(query.parameters["org"], "org");
        assert_eq!(query.parameters["namespace"], "prod");
        assert_eq!(query.parameters["offset"], 40);
        assert_eq!(query.parameters["limit"], 21);

        let rows: Vec<_> = ["a", "b", "c"].into_iter().map(node_row).collect();
        let request = SagaRead::List {
            namespace: "prod".into(),
            offset: 0,
            limit: 2,
            as_of: None,
        };
        let SagaReadResult::Sagas(page) = decode(&request, rows.clone()).unwrap() else {
            panic!("wrong result kind");
        };
        assert!(page.truncated);
        assert_eq!(
            page.sagas
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let SagaReadResult::Sagas(page) = decode(&request, rows[..2].to_vec()).unwrap() else {
            panic!("wrong result kind");
        };
        assert!(!page.truncated);
        assert!(decode(
            &request,
            vec![json!({"node": {"name": 1}}).as_object().unwrap().clone()]
        )
        .is_err());
    }

    #[test]
    fn member_pages_apply_the_capture_cutoff_only_when_supplied() {
        let members = |captured_through| SagaRead::Members {
            namespace: "prod".into(),
            saga_uuid: Uuid::from_u128(1),
            after_ordinal: 5,
            through_ordinal: None,
            limit: 10,
            captured_through,
        };
        let open = read("org", &members(None)).unwrap();
        assert!(open.statement.contains(
            "($captured_through IS NULL OR datetime(m.captured_at)<=datetime($captured_through))"
        ));
        assert_eq!(open.parameters["captured_through"], Value::Null);
        assert_eq!(open.parameters["after"], 5);
        assert_eq!(open.parameters["limit"], 11);
        let cutoff = "2026-01-05T00:00:00Z".parse().unwrap();
        let bounded = read("org", &members(Some(cutoff))).unwrap();
        assert_eq!(
            bounded.parameters["captured_through"],
            "2026-01-05T00:00:00+00:00"
        );
        assert!(read(" ", &members(None)).is_err());
    }
}
