//! Scoped Saga ordering, revision conflicts and complete summary coverage on Neo4j.
use chrono::{DateTime, Utc};
use kg_core::{
    errors::BackendError,
    models::ThreadNode,
    runtime::history::SnapshotEvidenceRequest,
    saga::*,
    search::{EvidenceSearch, SearchFilter},
    traits::{GraphBackend, GraphMutation as M, SearchBackend},
};
use kg_neo4j_testkit::indexed_graph as graph;
use kg_storage_neo4j::Neo4jGraphBackend;
use serde_json::json;
use uuid::Uuid;
fn time(day: u32) -> DateTime<Utc> {
    format!("2026-01-{day:02}T00:00:00Z").parse().unwrap()
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn saga_timestamps_compare_instants_across_rfc3339_encodings() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    for (i, capture) in ["2026-01-10T00:00:00.500Z", "2026-01-10T00:00:00Z"]
        .into_iter()
        .enumerate()
    {
        let mut mutation = snapshot(ids[i], 10);
        if let M::UpsertSnapshot { properties, .. } = &mut mutation {
            properties.insert("captured_at".into(), json!(capture));
            properties.insert("created_at".into(), json!("2026-01-20T00:00:00Z"));
        }
        g.apply_mutations(
            &org,
            &[
                mutation,
                associate(association(
                    &org,
                    ids[i],
                    (i > 0).then_some(ids[0]),
                    i as u64,
                )),
            ],
        )
        .await
        .unwrap();
    }
    let saga = state(&g, &org).await;
    assert_eq!(saga.first_captured_at, Some(time(10)));
    let SagaReadResult::Sagas(page) = g
        .read_saga(
            &org,
            &SagaRead::List {
                namespace: "prod".into(),
                offset: 0,
                limit: 10,
                as_of: Some(time(10)),
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected sagas")
    };
    assert_eq!(page.sagas.len(), 1);
    let SagaReadResult::Members(page) = g
        .read_saga(
            &org,
            &SagaRead::Members {
                namespace: "prod".into(),
                saga_uuid: saga.uuid,
                after_ordinal: 0,
                through_ordinal: None,
                limit: 10,
                captured_through: Some(time(10)),
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected members")
    };
    assert_eq!(
        page.members
            .iter()
            .map(|m| m.snapshot_uuid)
            .collect::<Vec<_>>(),
        vec![ids[1]]
    );
    let SagaReadResult::Member(Some(latest)) = g
        .read_saga(
            &org,
            &SagaRead::Latest {
                namespace: "prod".into(),
                saga_uuid: saga.uuid,
                excluding_snapshot_uuid: None,
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected latest member")
    };
    assert_eq!(latest.snapshot_uuid, ids[0]);
    let write = summary_write(&g, &org, 0, 2).await;
    g.apply_mutations(
        &org,
        &[M::SetSagaSummary {
            summary: Box::new(write),
        }],
    )
    .await
    .unwrap();
}
fn snapshot(id: Uuid, day: u32) -> M {
    M::UpsertSnapshot{uuid:id,properties:json!({"namespace":"prod","name":"observation","source":"logs","data_type":"text","captured_at":time(day).to_rfc3339(),"created_at":time(20).to_rfc3339(),"content":format!("Accepted observation {id}")}).as_object().unwrap().clone()}
}
fn association(
    org: &str,
    id: Uuid,
    previous: Option<Uuid>,
    revision: u64,
) -> ThreadAssociationWrite {
    ThreadAssociationWrite {
        namespace: "prod".into(),
        saga_uuid: saga_uuid(org, "prod", "incident"),
        name: "incident".into(),
        created_at: time(20),
        expected_revision: revision,
        snapshot_uuid: id,
        previous_snapshot_uuid: previous,
        membership_uuid: Uuid::new_v4(),
        next_uuid: previous.map(|_| Uuid::new_v4()),
    }
}
fn associate(a: ThreadAssociationWrite) -> M {
    M::AssociateSagaSnapshot {
        association: Box::new(a),
    }
}
async fn state(g: &Neo4jGraphBackend, org: &str) -> ThreadNode {
    match g
        .read_saga(
            org,
            &SagaRead::State {
                namespace: "prod".into(),
                reference: ThreadReference::Name {
                    name: "incident".into(),
                },
            },
        )
        .await
        .unwrap()
    {
        SagaReadResult::State(Some(s)) => s,
        _ => panic!("missing Saga"),
    }
}
async fn members(g: &Neo4jGraphBackend, org: &str, after: u64, through: u64) -> SagaMemberPage {
    match g
        .read_saga(
            org,
            &SagaRead::Members {
                namespace: "prod".into(),
                saga_uuid: saga_uuid(org, "prod", "incident"),
                after_ordinal: after,
                through_ordinal: Some(through),
                limit: MAX_PAGE_SIZE,
                captured_through: None,
            },
        )
        .await
        .unwrap()
    {
        SagaReadResult::Members(p) => p,
        _ => panic!("wrong page"),
    }
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn append_preserves_late_arrivals_branching_scope_and_identical_replay() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    for (i, day) in [10, 2, 12].iter().enumerate() {
        let previous = if i == 0 { None } else { Some(ids[0]) };
        g.apply_mutations(
            &org,
            &[
                snapshot(ids[i], *day),
                associate(association(&org, ids[i], previous, i as u64)),
            ],
        )
        .await
        .unwrap();
    }
    let s = state(&g, &org).await;
    assert_eq!(s.revision, 3);
    assert_eq!(s.first_snapshot_uuid, Some(ids[0]));
    assert_eq!(s.last_snapshot_uuid, Some(ids[2]));
    let p = members(&g, &org, 0, 3).await;
    assert_eq!(
        p.members
            .iter()
            .map(|m| m.snapshot_uuid)
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(p.members[1].previous_snapshot_uuid, Some(ids[0]));
    assert_eq!(p.members[2].previous_snapshot_uuid, Some(ids[0]));
    g.apply_mutations(
        &org,
        &[associate(association(&org, ids[1], Some(ids[0]), 0))],
    )
    .await
    .unwrap();
    assert_eq!(state(&g, &org).await.revision, 3);
    assert!(matches!(
        g.apply_mutations(
            &org,
            &[associate(association(&org, ids[1], Some(ids[2]), 3))]
        )
        .await,
        Err(BackendError::Conflict(_))
    ));
    let latest = g
        .read_saga(
            &org,
            &SagaRead::Latest {
                namespace: "prod".into(),
                saga_uuid: s.uuid,
                excluding_snapshot_uuid: Some(ids[2]),
            },
        )
        .await
        .unwrap();
    assert!(matches!(latest,SagaReadResult::Member(Some(m)) if m.snapshot_uuid==ids[0]));
    let context = g
        .read_saga(
            &org,
            &SagaRead::Context {
                namespace: "prod".into(),
                saga_uuid: s.uuid,
                captured_before: time(10),
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(context,SagaReadResult::Members(p) if p.members.len()==2&&p.members[0].snapshot_uuid==ids[0])
    );
    assert!(matches!(
        g.read_saga(
            &org,
            &SagaRead::State {
                namespace: "other".into(),
                reference: ThreadReference::Uuid { uuid: s.uuid }
            }
        )
        .await
        .unwrap(),
        SagaReadResult::State(None)
    ));
    let foreign = Uuid::new_v4();
    g.apply_mutations("other-org", &[snapshot(foreign, 3)])
        .await
        .unwrap();
    assert!(matches!(
        g.apply_mutations(
            &org,
            &[associate(association(&org, foreign, Some(ids[2]), 3))]
        )
        .await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(state(&g, &org).await.revision, 3);
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn concurrent_append_loser_is_replannable_and_keeps_no_partial_snapshot() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let first = Uuid::new_v4();
    g.apply_mutations(
        &org,
        &[
            snapshot(first, 1),
            associate(association(&org, first, None, 0)),
        ],
    )
    .await
    .unwrap();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let ma = vec![
        snapshot(a, 2),
        associate(association(&org, a, Some(first), 1)),
    ];
    let mb = vec![
        snapshot(b, 3),
        associate(association(&org, b, Some(first), 1)),
    ];
    let (ra, rb) = tokio::join!(g.apply_mutations(&org, &ma), g.apply_mutations(&org, &mb));
    assert_ne!(ra.is_ok(), rb.is_ok());
    let error = if let Err(error) = ra {
        error
    } else {
        rb.unwrap_err()
    };
    assert!(matches!(error, BackendError::Conflict(_)), "{error:?}");
    assert_eq!(state(&g, &org).await.revision, 2);
    let loser = if members(&g, &org, 0, 2)
        .await
        .members
        .iter()
        .any(|m| m.snapshot_uuid == a)
    {
        b
    } else {
        a
    };
    g.apply_mutations(
        &org,
        &[
            snapshot(loser, 4),
            associate(association(&org, loser, Some(first), 2)),
        ],
    )
    .await
    .unwrap();
    assert_eq!(state(&g, &org).await.revision, 3);
}
async fn summary_write(
    g: &Neo4jGraphBackend,
    org: &str,
    after: u64,
    through: u64,
) -> SagaSummaryWrite {
    let s = state(g, org).await;
    let ids = members(g, org, after, through)
        .await
        .members
        .iter()
        .map(|m| m.snapshot_uuid)
        .collect::<Vec<_>>();
    let mut evidence = Vec::new();
    for chunk in ids.chunks(kg_core::runtime::history::MAX_CONTEXT_RECORDS) {
        evidence.extend(
            g.snapshot_evidence(
                org,
                &SnapshotEvidenceRequest {
                    namespace: "prod".into(),
                    ids: chunk.to_vec(),
                    captured_before: time(31),
                    max_bytes: 8 * 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
    }
    let by_id: std::collections::BTreeMap<_, _> =
        evidence.into_iter().map(|e| (e.uuid, e)).collect();
    let evidence = ids.iter().map(|id| by_id[id].clone()).collect::<Vec<_>>();
    SagaSummaryWrite {
        incomplete_reason: None,
        namespace: "prod".into(),
        saga_uuid: s.uuid,
        expected_summary_revision: s.summary_revision,
        previous_summary: s.summary,
        previous_supporting_snapshot_uuids: s.summary_supporting_snapshot_uuids,
        revision: Uuid::new_v4(),
        after_ordinal: after,
        through_ordinal: through,
        summary: "Incident evidence retained".into(),
        supporting_snapshot_uuids: vec![ids[0]],
        max_captured_at: evidence.iter().map(|e| e.captured_at).max().unwrap(),
        evidence,
        summarized_at: time(25),
    }
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn summaries_guard_complete_evidence_and_page_without_skipping_backfills() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let mut mutations = Vec::new();
    let mut previous = None;
    for i in 0..205 {
        let id = Uuid::new_v4();
        mutations.push(snapshot(id, if i < 200 { 10 } else { 1 }));
        mutations.push(associate(association(&org, id, previous, i)));
        previous = Some(id);
    }
    g.apply_mutations(&org, &mutations).await.unwrap();
    let page = members(&g, &org, 0, 205).await;
    assert_eq!(page.members.len(), 200);
    assert!(page.truncated);
    let w = summary_write(&g, &org, 0, 200).await;
    let mut corrupt = w.clone();
    corrupt.evidence[0].content = "altered".into();
    assert!(matches!(
        g.apply_mutations(
            &org,
            &[M::SetSagaSummary {
                summary: Box::new(corrupt)
            }]
        )
        .await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(state(&g, &org).await.summary_cursor, 0);
    g.apply_mutations(
        &org,
        &[M::SetSagaSummary {
            summary: Box::new(w.clone()),
        }],
    )
    .await
    .unwrap();
    assert_eq!(state(&g, &org).await.summary_cursor, 200);
    let mut second = summary_write(&g, &org, 200, 205).await;
    second
        .supporting_snapshot_uuids
        .extend(w.supporting_snapshot_uuids.iter().copied());
    g.apply_mutations(
        &org,
        &[M::SetSagaSummary {
            summary: Box::new(second),
        }],
    )
    .await
    .unwrap();
    let s = state(&g, &org).await;
    assert_eq!(s.summary_cursor, 205);
    assert_eq!(s.last_summarized_snapshot_captured_at, Some(time(10)));
    assert!(matches!(
        g.apply_mutations(
            &org,
            &[M::SetSagaSummary {
                summary: Box::new(w)
            }]
        )
        .await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(state(&g, &org).await.summary_cursor, 205);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn equal_time_predecessor_uses_append_order_not_snapshot_uuid() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let first = Uuid::from_u128(u128::MAX - 100);
    let second = Uuid::from_u128(100);
    // UUIDs are globally unique; use a random prefix while preserving descending order.
    let base = Uuid::new_v4().as_u128() & !0xffff;
    let first = Uuid::from_u128(base | (first.as_u128() & 0xffff));
    let second = Uuid::from_u128(base | (second.as_u128() & 0xffff));
    g.apply_mutations(
        &org,
        &[
            snapshot(first, 10),
            associate(association(&org, first, None, 0)),
            snapshot(second, 10),
            associate(association(&org, second, Some(first), 1)),
        ],
    )
    .await
    .unwrap();
    let result = g
        .read_saga(
            &org,
            &SagaRead::Latest {
                namespace: "prod".into(),
                saga_uuid: saga_uuid(&org, "prod", "incident"),
                excluding_snapshot_uuid: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(result,SagaReadResult::Member(Some(m)) if m.snapshot_uuid==second));
    let context = g
        .read_saga(
            &org,
            &SagaRead::Context {
                namespace: "prod".into(),
                saga_uuid: saga_uuid(&org, "prod", "incident"),
                captured_before: time(10),
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert!(matches!(context,SagaReadResult::Members(p) if p.members[0].snapshot_uuid==second));
}
#[tokio::test]
#[ignore = "live: Neo4j"]
async fn concurrent_named_creation_has_one_winner_and_no_empty_orphan_saga() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let ma = vec![snapshot(a, 1), associate(association(&org, a, None, 0))];
    let mb = vec![snapshot(b, 1), associate(association(&org, b, None, 0))];
    let (ra, rb) = tokio::join!(g.apply_mutations(&org, &ma), g.apply_mutations(&org, &mb));
    assert_ne!(ra.is_ok(), rb.is_ok());
    let error = if let Err(error) = ra {
        error
    } else {
        rb.unwrap_err()
    };
    assert!(matches!(error, BackendError::Conflict(_)), "{error:?}");
    let s = state(&g, &org).await;
    assert_eq!(s.revision, 1);
    assert_eq!(s.last_membership_ordinal, 1);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn automatic_context_excludes_unavailable_content_without_changing_membership_order() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let first = Uuid::new_v4();
    let missing = Uuid::new_v4();
    let blank = Uuid::new_v4();
    let mut absent = snapshot(missing, 11);
    if let M::UpsertSnapshot { properties, .. } = &mut absent {
        properties.remove("content");
    }
    let mut empty = snapshot(blank, 12);
    if let M::UpsertSnapshot { properties, .. } = &mut empty {
        properties.insert("content".into(), json!("   "));
    }
    g.apply_mutations(
        &org,
        &[
            snapshot(first, 10),
            associate(association(&org, first, None, 0)),
            absent,
            associate(association(&org, missing, Some(first), 1)),
            empty,
            associate(association(&org, blank, Some(missing), 2)),
        ],
    )
    .await
    .unwrap();
    let saga = saga_uuid(&org, "prod", "incident");
    let latest = g
        .read_saga(
            &org,
            &SagaRead::Latest {
                namespace: "prod".into(),
                saga_uuid: saga,
                excluding_snapshot_uuid: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(latest,SagaReadResult::Member(Some(m)) if m.snapshot_uuid==blank));
    let context = g
        .read_saga(
            &org,
            &SagaRead::Context {
                namespace: "prod".into(),
                saga_uuid: saga,
                captured_before: time(20),
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(context,SagaReadResult::Members(p) if p.members.len()==1&&p.members[0].snapshot_uuid==first&&!p.truncated)
    );
    assert_eq!(
        members(&g, &org, 1, 3)
            .await
            .members
            .iter()
            .map(|m| m.ordinal)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn invalid_predecessors_and_cycles_leave_membership_and_new_snapshots_unchanged() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let root = Uuid::new_v4();
    let child = Uuid::new_v4();
    g.apply_mutations(
        &org,
        &[
            snapshot(root, 1),
            associate(association(&org, root, None, 0)),
        ],
    )
    .await
    .unwrap();
    g.apply_mutations(
        &org,
        &[
            snapshot(child, 2),
            associate(association(&org, child, Some(root), 1)),
        ],
    )
    .await
    .unwrap();
    // Reparenting the root to its own descendant would create a cycle.
    assert!(matches!(
        g.apply_mutations(&org, &[associate(association(&org, root, Some(child), 2))])
            .await,
        Err(BackendError::Conflict(_))
    ));
    let outsider = Uuid::new_v4();
    g.apply_mutations(&org, &[snapshot(outsider, 3)])
        .await
        .unwrap();
    for predecessor in [Uuid::new_v4(), outsider] {
        let target = Uuid::new_v4();
        assert!(matches!(
            g.apply_mutations(
                &org,
                &[
                    snapshot(target, 4),
                    associate(association(&org, target, Some(predecessor), 2))
                ]
            )
            .await,
            Err(BackendError::Conflict(_))
        ));
        assert!(
            matches!(g.snapshot_evidence(&org, &SnapshotEvidenceRequest {namespace:"prod".into(),ids:vec![target],captured_before:time(20),max_bytes:4096}).await,Err(BackendError::Query(message)) if message.contains("required snapshot evidence unavailable"))
        );
    }
    assert!(association(&org, child, Some(child), 2)
        .validate(&org)
        .is_err());
    let node = state(&g, &org).await;
    assert_eq!(node.revision, 2);
    assert_eq!(node.first_snapshot_uuid, Some(root));
    assert_eq!(node.last_snapshot_uuid, Some(child));
    assert_eq!(members(&g, &org, 0, 2).await.members.len(), 2);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn listing_and_historical_member_pages_stay_inside_org_and_namespace() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    let other_org = Uuid::new_v4().to_string();
    // Three members captured on days 10, 2 and 12; another organization has its own "incident".
    let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    for (i, day) in [10, 2, 12].iter().enumerate() {
        let previous = if i == 0 { None } else { Some(ids[0]) };
        g.apply_mutations(
            &org,
            &[
                snapshot(ids[i], *day),
                associate(association(&org, ids[i], previous, i as u64)),
            ],
        )
        .await
        .unwrap();
    }
    let foreign = Uuid::new_v4();
    g.apply_mutations(
        &other_org,
        &[
            snapshot(foreign, 3),
            associate(association(&other_org, foreign, None, 0)),
        ],
    )
    .await
    .unwrap();
    for (i, name) in ["alpha", "beta"].iter().enumerate() {
        let id = Uuid::new_v4();
        let mut a = association(&org, id, None, 0);
        a.saga_uuid = saga_uuid(&org, "prod", name);
        a.name = (*name).into();
        g.apply_mutations(&org, &[snapshot(id, 4 + i as u32), associate(a)])
            .await
            .unwrap();
    }

    let list = |offset, limit| SagaRead::List {
        namespace: "prod".into(),
        offset,
        limit,
        as_of: None,
    };
    let SagaReadResult::Sagas(page) = g.read_saga(&org, &list(0, 2)).await.unwrap() else {
        panic!("wrong result kind");
    };
    assert!(page.truncated);
    assert_eq!(
        page.sagas
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    let SagaReadResult::Sagas(page) = g.read_saga(&org, &list(2, 2)).await.unwrap() else {
        panic!("wrong result kind");
    };
    assert!(!page.truncated);
    assert_eq!(page.sagas.len(), 1);
    assert_eq!(page.sagas[0].name, "incident");
    assert!(page
        .sagas
        .iter()
        .all(|s| s.org_id == org && s.namespace == "prod"));
    let SagaReadResult::Sagas(page) = g
        .read_saga(
            &org,
            &SagaRead::List {
                namespace: "staging".into(),
                offset: 0,
                limit: 10,
                as_of: None,
            },
        )
        .await
        .unwrap()
    else {
        panic!("wrong result kind");
    };
    assert!(page.sagas.is_empty());

    // The incident was minted by the day-10 observation (created_at is the
    // association clock, day 20) and later backfilled with a day-2 capture. The
    // Saga exists on the observation timeline from its earliest capture, so a
    // historical listing at day 5 still shows it, and storage applies that
    // filter before SKIP/LIMIT: offsets count visible rows only.
    let incident = state(&g, &org).await;
    assert_eq!(incident.first_captured_at, Some(time(2)));
    let visible_at = |day: u32, offset, limit| SagaRead::List {
        namespace: "prod".into(),
        offset,
        limit,
        as_of: Some(time(day)),
    };
    let SagaReadResult::Sagas(page) = g.read_saga(&org, &visible_at(5, 0, 10)).await.unwrap()
    else {
        panic!("wrong result kind");
    };
    assert_eq!(
        page.sagas
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta", "incident"]
    );
    let SagaReadResult::Sagas(page) = g.read_saga(&org, &visible_at(3, 0, 1)).await.unwrap() else {
        panic!("wrong result kind");
    };
    assert!(
        !page.truncated,
        "alpha (day 4) and beta (day 5) are not yet visible"
    );
    assert_eq!(page.sagas[0].name, "incident");
    let SagaReadResult::Sagas(page) = g.read_saga(&org, &visible_at(1, 0, 10)).await.unwrap()
    else {
        panic!("wrong result kind");
    };
    assert!(page.sagas.is_empty(), "nothing was captured by day 1");

    // A historical cutoff hides later captures without renumbering ordinals.
    let SagaReadResult::Members(page) = g
        .read_saga(
            &org,
            &SagaRead::Members {
                namespace: "prod".into(),
                saga_uuid: saga_uuid(&org, "prod", "incident"),
                after_ordinal: 0,
                through_ordinal: None,
                limit: 10,
                captured_through: Some(time(10)),
            },
        )
        .await
        .unwrap()
    else {
        panic!("wrong result kind");
    };
    assert_eq!(
        page.members.iter().map(|m| m.ordinal).collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(page.members.iter().all(|m| m.captured_at <= time(10)));
    assert!(!page.truncated);
    let all = members(&g, &org, 0, 3).await;
    assert_eq!(all.members.len(), 3);
}

#[tokio::test]
#[ignore = "live: Neo4j"]
async fn snapshot_search_applies_saga_membership_before_the_limit() {
    let g = graph().await;
    let org = Uuid::new_v4().to_string();
    // Three observations share the same text; two of them belong to the incident Saga.
    let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    for (i, id) in ids.iter().enumerate() {
        let mut mutations = vec![snapshot(*id, 1 + i as u32)];
        if i < 2 {
            let previous = (i > 0).then_some(ids[0]);
            mutations.push(associate(association(&org, *id, previous, i as u64)));
        }
        g.apply_mutations(&org, &mutations).await.unwrap();
    }
    let saga = saga_uuid(&org, "prod", "incident");
    let mut request = EvidenceSearch {
        filter: SearchFilter {
            org_id: org.clone(),
            namespaces: vec!["prod".into()],
            saga_uuid: Some(saga),
            ..Default::default()
        },
        query: Some("Accepted".into()),
        passage_query: None,
        chain_ids: None,
        limit: 10,
    };
    let members = g.search_snapshots(&request).await.unwrap();
    let mut found: Vec<_> = members.items.iter().map(|hit| hit.uuid).collect();
    found.sort();
    let mut expected = vec![ids[0], ids[1]];
    expected.sort();
    assert_eq!(found, expected);
    assert!(!members.truncated);

    // The limit cuts members, never a non-member padded in.
    request.limit = 1;
    let page = g.search_snapshots(&request).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(expected.contains(&page.items[0].uuid));
    assert!(page.truncated);

    request.limit = 10;
    request.filter.saga_uuid = None;
    assert_eq!(g.search_snapshots(&request).await.unwrap().items.len(), 3);
    request.filter.saga_uuid = Some(Uuid::new_v4());
    assert!(g.search_snapshots(&request).await.unwrap().items.is_empty());

    // Another organization's identical Saga name resolves elsewhere and matches nothing here.
    request.filter.saga_uuid = Some(saga_uuid("other-org", "prod", "incident"));
    assert!(g.search_snapshots(&request).await.unwrap().items.is_empty());

    request.filter.saga_uuid = Some(saga);
    assert!(matches!(
        g.search_relationships(&request).await,
        Err(BackendError::Query(_))
    ));
}
