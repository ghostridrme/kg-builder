//! Unit tests for the pure parts of the driver: JSON↔Bolt conversion, row
//! extraction, and the timeout/retry wrapper. No live server required.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, NaiveDate};
use neo4rs::{BoltDateTime, BoltInteger, BoltList, BoltMap, BoltNode, BoltString, BoltType, Row};
use serde_json::json;

use super::*;

// -- JSON → Bolt -----------------------------------------------------

#[test]
fn json_to_bolt_scalars() {
    assert!(matches!(json_to_bolt(&json!(null)), BoltType::Null(_)));
    assert!(matches!(json_to_bolt(&json!(true)), BoltType::Boolean(b) if b.value));
    assert!(matches!(json_to_bolt(&json!(42)), BoltType::Integer(i) if i.value == 42));
    assert!(matches!(json_to_bolt(&json!(-7)), BoltType::Integer(i) if i.value == -7));
    assert!(matches!(json_to_bolt(&json!(1.5)), BoltType::Float(f) if f.value == 1.5));
    assert!(matches!(json_to_bolt(&json!("hi")), BoltType::String(s) if s.value == "hi"));
}

#[test]
fn json_to_bolt_numeric_array_keeps_every_element() {
    // Non-string array elements must be preserved, or embeddings would
    // turn into empty lists.
    let embedding = json!([0.1, 0.2, 0.3]);
    match json_to_bolt(&embedding) {
        BoltType::List(list) => {
            assert_eq!(list.value.len(), 3);
            assert!(list.value.iter().all(|v| matches!(v, BoltType::Float(_))));
        }
        other => panic!("expected list, got {other:?}"),
    }

    let ints = json!([1, 2, 3]);
    match json_to_bolt(&ints) {
        BoltType::List(list) => {
            assert_eq!(list.value.len(), 3);
            assert!(list.value.iter().all(|v| matches!(v, BoltType::Integer(_))));
        }
        other => panic!("expected list, got {other:?}"),
    }
}

#[test]
fn json_to_bolt_nested_object_recurses() {
    let value = json!({
        "name": "web",
        "ports": [80, 443],
        "labels": { "tier": "frontend", "replicas": 3 },
        "deleted_at": null
    });
    let BoltType::Map(map) = json_to_bolt(&value) else {
        panic!("expected map");
    };
    assert!(matches!(map.value.get("ports"), Some(BoltType::List(l)) if l.value.len() == 2));
    let Some(BoltType::Map(labels)) = map.value.get("labels") else {
        panic!("expected nested map");
    };
    assert!(matches!(
        labels.value.get("replicas"),
        Some(BoltType::Integer(i)) if i.value == 3
    ));
    assert!(matches!(
        map.value.get("deleted_at"),
        Some(BoltType::Null(_))
    ));
}

#[test]
fn json_bolt_json_round_trip() {
    let original = json!({
        "s": "text",
        "i": 9,
        "f": 2.25,
        "b": false,
        "n": null,
        "arr": [1.0, 2.0, "x", null, { "k": [true] }],
        "obj": { "nested": { "deep": -1 } }
    });
    let bolt = json_to_bolt(&original);
    let back = bolt_to_json(&bolt).expect("round trip");
    assert_eq!(back, original);
}

#[test]
fn build_query_binds_null_params() {
    // Null params must be bound (as Bolt Null), not skipped — skipping
    // them causes "Expected parameter" server errors.
    let q = build_query(
        "MATCH (n) WHERE n.deleted_at = $deleted_at RETURN n",
        &json!({ "deleted_at": null, "org_id": "o-1" }),
    );
    assert!(q.has_param_key("deleted_at"));
    assert!(q.has_param_key("org_id"));
    assert!(!q.has_param_key("missing"));
}

// -- Bolt → JSON ------------------------------------------------------

fn bolt_str(s: &str) -> BoltString {
    BoltString {
        value: s.to_string(),
    }
}

fn props(entries: Vec<(&str, BoltType)>) -> BoltMap {
    BoltMap {
        value: entries.into_iter().map(|(k, v)| (bolt_str(k), v)).collect(),
    }
}

fn entity_node(id: i64, uuid: &str, name: &str, extra: Vec<(&str, BoltType)>) -> BoltNode {
    let mut p = vec![
        ("uuid", BoltType::from(uuid)),
        ("name", BoltType::from(name)),
        ("is_latest", BoltType::from(true)),
    ];
    p.extend(extra);
    BoltNode::new(
        BoltInteger::new(id),
        BoltList::from(vec![BoltType::from("Entity")]),
        props(p),
    )
}

fn make_row(columns: Vec<(&str, BoltType)>) -> Row {
    let fields = BoltList::from(
        columns
            .iter()
            .map(|(name, _)| BoltType::from(*name))
            .collect::<Vec<_>>(),
    );
    let data = BoltList::from(columns.into_iter().map(|(_, v)| v).collect::<Vec<_>>());
    Row::new(fields, data)
}

#[test]
fn bolt_to_json_temporal_types() {
    let dt = DateTime::<FixedOffset>::parse_from_rfc3339("2026-06-11T08:30:00+02:00").unwrap();
    let bolt_dt = BoltType::DateTime(BoltDateTime::from(dt));
    assert_eq!(
        bolt_to_json(&bolt_dt).unwrap(),
        json!("2026-06-11T08:30:00+02:00")
    );

    let date = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
    let bolt_date = BoltType::Date(neo4rs::BoltDate::from(date));
    assert_eq!(bolt_to_json(&bolt_date).unwrap(), json!("2026-06-11"));

    let ldt = dt.naive_utc();
    let bolt_ldt = BoltType::LocalDateTime(neo4rs::BoltLocalDateTime::from(ldt));
    assert_eq!(
        bolt_to_json(&bolt_ldt).unwrap(),
        json!("2026-06-11T06:30:00")
    );
}

#[test]
fn row_extraction_flattens_node_and_keeps_scalar_columns() {
    // Node and scalar columns returned together by graph queries.
    let node = entity_node(7, "u-1", "api-gateway", vec![]);
    let row = make_row(vec![
        ("node", BoltType::Node(node)),
        ("score", BoltType::from(1.5)),
        ("distance", BoltType::from(2)),
    ]);

    let out = row_to_json(&row).unwrap_or_else(|_| panic!("row conversion failed"));

    // Flattened node properties at the top level.
    assert_eq!(out.get("uuid"), Some(&json!("u-1")));
    assert_eq!(out.get("name"), Some(&json!("api-gateway")));
    assert_eq!(out.get("is_latest"), Some(&json!(true)));
    // Scalar columns under their own names.
    assert_eq!(out.get("score"), Some(&json!(1.5)));
    assert_eq!(out.get("distance"), Some(&json!(2)));
    // The full node is also available under its column name.
    let node_obj = out.get("node").and_then(|v| v.as_object()).unwrap();
    assert_eq!(node_obj.get("uuid"), Some(&json!("u-1")));
}

#[test]
fn row_extraction_handles_list_map_and_datetime_properties() {
    let dt = DateTime::<FixedOffset>::parse_from_rfc3339("2026-01-02T03:04:05+00:00").unwrap();
    let node = entity_node(
        1,
        "u-2",
        "db",
        vec![
            (
                "embedding",
                BoltType::List(BoltList::from(vec![
                    BoltType::from(0.25),
                    BoltType::from(0.5),
                ])),
            ),
            (
                "labels_map",
                BoltType::Map(props(vec![("env", BoltType::from("prod"))])),
            ),
            ("created_at", BoltType::DateTime(BoltDateTime::from(dt))),
            ("deleted_at", BoltType::Null(neo4rs::BoltNull)),
        ],
    );
    let row = make_row(vec![("n", BoltType::Node(node))]);

    let out = row_to_json(&row).unwrap_or_else(|_| panic!("row conversion failed"));
    assert_eq!(out.get("embedding"), Some(&json!([0.25, 0.5])));
    assert_eq!(out.get("labels_map"), Some(&json!({ "env": "prod" })));
    assert_eq!(
        out.get("created_at"),
        Some(&json!("2026-01-02T03:04:05+00:00"))
    );
    assert_eq!(out.get("deleted_at"), Some(&json!(null)));
}

#[test]
fn multi_node_row_is_deterministic_and_never_clobbers_columns() {
    // A query that returns two full nodes (RETURN d, a) sharing property names.
    let d = entity_node(1, "uuid-d", "drifted", vec![]);
    let a = entity_node(2, "uuid-a", "authority", vec![]);
    let row = make_row(vec![("d", BoltType::Node(d)), ("a", BoltType::Node(a))]);

    let out = row_to_json(&row).unwrap_or_else(|_| panic!("row conversion failed"));

    // Both full nodes remain addressable under their column names.
    assert_eq!(out["d"]["uuid"], json!("uuid-d"));
    assert_eq!(out["a"]["uuid"], json!("uuid-a"));
    // Flattening is deterministic: columns are processed in lexicographic
    // order, first writer wins ("a" < "d").
    assert_eq!(out.get("uuid"), Some(&json!("uuid-a")));
    assert_eq!(out.get("name"), Some(&json!("authority")));
}

#[test]
fn relationship_columns_flatten_like_nodes() {
    let rel = neo4rs::BoltUnboundedRelation::new(
        BoltInteger::new(9),
        bolt_str("RELATES_TO"),
        props(vec![
            ("uuid", BoltType::from("edge-1")),
            ("confidence", BoltType::from(0.9)),
        ]),
    );
    let row = make_row(vec![("r", BoltType::UnboundedRelation(rel))]);

    let out = row_to_json(&row).unwrap_or_else(|_| panic!("row conversion failed"));
    assert_eq!(out.get("uuid"), Some(&json!("edge-1")));
    assert_eq!(out.get("confidence"), Some(&json!(0.9)));
    assert_eq!(out["r"]["uuid"], json!("edge-1"));
}

// -- Timeout + retry ---------------------------------------------------

fn test_options() -> Neo4jOptions {
    Neo4jOptions {
        timeout: Duration::from_millis(50),
        max_retries: 3,
        base_backoff: Duration::from_millis(100),
        commit_verification_attempts: 5,
        faults: None,
        exact_vector_population: 512,
    }
}

#[tokio::test(start_paused = true)]
async fn retries_transient_errors_then_succeeds() {
    let calls = Arc::new(AtomicU32::new(0));
    let calls2 = Arc::clone(&calls);
    let result = with_retry("test", &test_options(), move || {
        let calls = Arc::clone(&calls2);
        async move {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(CallError::Driver(neo4rs::Error::ConnectionError))
            } else {
                Ok(42)
            }
        }
    })
    .await;
    assert!(matches!(result, Ok(42)));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn does_not_retry_non_transient_errors() {
    let calls = Arc::new(AtomicU32::new(0));
    let calls2 = Arc::clone(&calls);
    let result: Result<(), CallError> = with_retry("test", &test_options(), move || {
        let calls = Arc::clone(&calls2);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            // Stand-in for a query/syntax error: not transient.
            Err(CallError::Driver(neo4rs::Error::UnsupportedScheme(
                "bogus".to_string(),
            )))
        }
    })
    .await;
    assert!(matches!(result, Err(CallError::Driver(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "must fail fast");
}

#[tokio::test(start_paused = true)]
async fn permanent_conversion_errors_are_not_retried() {
    let calls = Arc::new(AtomicU32::new(0));
    let calls2 = Arc::clone(&calls);
    let result: Result<(), CallError> = with_retry("test", &test_options(), move || {
        let calls = Arc::clone(&calls2);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(CallError::Permanent("bad row".to_string()))
        }
    })
    .await;
    assert!(matches!(result, Err(CallError::Permanent(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn hung_calls_time_out_and_are_retried() {
    let calls = Arc::new(AtomicU32::new(0));
    let calls2 = Arc::clone(&calls);
    let opts = test_options();
    let result: Result<(), CallError> = with_retry("test", &opts, move || {
        calls2.fetch_add(1, Ordering::SeqCst);
        std::future::pending()
    })
    .await;
    match result {
        Err(CallError::Timeout(d)) => assert_eq!(d, opts.timeout),
        _ => panic!("expected timeout error"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3, "every attempt timed out");
}

#[test]
fn transient_classification() {
    assert!(is_transient(&neo4rs::Error::ConnectionError));
    assert!(is_transient(&neo4rs::Error::IOError {
        detail: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"),
    }));
    assert!(!is_transient(&neo4rs::Error::UnsupportedScheme(
        "x".to_string()
    )));
    assert!(!is_transient(&neo4rs::Error::StringTooLong));
}

#[test]
fn default_options_are_sane() {
    let opts = Neo4jOptions::default();
    assert_eq!(opts.timeout, Duration::from_secs(30));
    assert_eq!(opts.max_retries, 3);
    assert_eq!(opts.base_backoff, Duration::from_millis(100));
}

#[test]
fn projected_search_node_decodes_through_bolt_row_conversion() {
    let uuid = uuid::Uuid::from_u128(1);
    let id = uuid.to_string();
    let row = make_row(vec![
        (
            "n",
            BoltType::Map(props(vec![
                ("uuid", BoltType::from(id.as_str())),
                ("chain_id", BoltType::from(id.as_str())),
                ("name", BoltType::from("checkout")),
                ("entity_type", BoltType::from("Service")),
                ("namespace", BoltType::from("prod")),
            ])),
        ),
        ("score", BoltType::from(0.75)),
        ("observations", BoltType::from(0i64)),
    ]);
    let hit = kg_storage_cypher::decode_node(
        row_to_json(&row).unwrap_or_else(|_| panic!("row conversion failed")),
        kg_core::embedding::TEXT_VERSION,
    )
    .unwrap();
    assert_eq!(hit.uuid, uuid);
    assert_eq!(hit.chain_id, uuid);
    assert_eq!(hit.name, "checkout");
    assert_eq!(hit.entity_type, "Service");
    assert_eq!(hit.namespace, "prod");
    assert_eq!(hit.score, 0.75);
    assert_eq!(hit.observation_count, Some(0));
    assert_eq!(hit.dependent_count, None);
    assert!(hit.embedding.is_none());
    assert!(hit.properties.get("n").is_none());
}

#[test]
fn signed_durations_remain_signed_without_integer_overflow() {
    for (months, days, seconds, nanos, expected) in [
        (0, 0, -2, -500_000_000, -2.5),
        (0, -1, 1, 0, -86_399.0),
        (0, 0, 2, 500_000_000, 2.5),
        (0, 0, i64::MIN, 0, i64::MIN as f64),
    ] {
        let duration =
            neo4rs::BoltDuration::new(months.into(), days.into(), seconds.into(), nanos.into());
        assert_eq!(
            bolt_to_json(&BoltType::Duration(duration)).unwrap(),
            serde_json::json!(expected)
        );
    }
}

#[test]
fn retry_backoff_stays_bounded_for_extreme_configuration() {
    assert_eq!(
        retry_delay(Duration::from_millis(100), 0),
        Duration::from_millis(100)
    );
    assert_eq!(
        retry_delay(Duration::from_secs(60), 0),
        Duration::from_secs(5)
    );
    assert_eq!(retry_delay(Duration::MAX, u32::MAX), Duration::from_secs(5));
}

#[test]
fn server_deadline_covers_all_client_transaction_phases() {
    let options = Neo4jOptions {
        timeout: Duration::from_secs(2),
        ..Default::default()
    };
    assert_eq!(options.server_transaction_timeout(), Duration::from_secs(6));
    let options = Neo4jOptions {
        timeout: Duration::MAX,
        ..Default::default()
    };
    assert_eq!(options.server_transaction_timeout(), Duration::MAX);
}

#[test]
fn duplicate_receipts_are_uncertain_in_both_read_paths() {
    let rows = vec![serde_json::Map::new(), serde_json::Map::new()];
    assert!(matches!(
        unique_receipt(&rows),
        Err(BackendError::UnknownCommit(_))
    ));
    assert!(unique_receipt(&[]).unwrap().is_none());
}

#[test]
fn receipt_failures_keep_transport_timeout_auth_and_decode_classification() {
    assert!(matches!(
        classify_call(CallError::Driver(neo4rs::Error::ConnectionError), "receipt"),
        AttemptError::Retry(BackendError::Connection(_))
    ));
    assert!(matches!(
        classify_call(CallError::Timeout(Duration::from_millis(30)), "receipt"),
        AttemptError::Retry(BackendError::Timeout(30))
    ));
    assert!(matches!(
        classify_call(
            CallError::Driver(neo4rs::Error::AuthenticationError("secret".into())),
            "receipt"
        ),
        AttemptError::Final(BackendError::Auth(_))
    ));
    assert!(matches!(
        classify_call(CallError::Permanent("bad receipt".into()), "receipt"),
        AttemptError::Final(BackendError::Deserialization(_))
    ));
}

/// Exercise actual COMMIT frames through the vendored driver and adapter. A
/// server FAILURE is a known rejection; a lost or invalid response is uncertain.
#[tokio::test]
async fn commit_wire_responses_preserve_rejection_vs_unknown_outcome() {
    use kg_core::traits::{GraphBackend, GraphMutation};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut data = Vec::new();
        loop {
            let size = socket.read_u16().await.unwrap() as usize;
            if size == 0 {
                return data;
            }
            let start = data.len();
            data.resize(start + size, 0);
            socket.read_exact(&mut data[start..]).await.unwrap();
        }
    }
    async fn frame(socket: &mut tokio::net::TcpStream, data: &[u8]) {
        socket.write_u16(data.len() as u16).await.unwrap();
        socket.write_all(data).await.unwrap();
        socket.write_u16(0).await.unwrap();
    }
    for outcome in [
        "success",
        "Neo.TransientError.Transaction.DeadlockDetected",
        "Neo.ClientError.Security.Forbidden",
        "Neo.ClientError.Transaction.Terminated",
        "Neo.ClientError.Schema.ConstraintValidationFailed",
        "transport",
        "malformed",
        "ignored",
    ] {
        // Also feed typed wire failures through the classification used by the
        // receipted path after an absent receipt establishes known rejection.
        for classify_only in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let graph = Neo4jGraphBackend::with_options(
                &format!("bolt://{}", listener.local_addr().unwrap()),
                "user",
                "password",
                Neo4jOptions {
                    timeout: Duration::from_secs(2),
                    max_retries: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let server = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.read_exact(&mut [0; 20]).await.unwrap();
                socket.write_all(&[0, 0, 1, 4]).await.unwrap();
                assert_eq!(request(&mut socket).await[1], 0x01); // HELLO
                frame(&mut socket, &[0xb1, 0x70, 0xa0]).await;
                assert_eq!(request(&mut socket).await[1], 0x11); // BEGIN
                frame(&mut socket, &[0xb1, 0x70, 0xa0]).await;
                // Inject the error at COMMIT, regardless of how many guard or
                // bookkeeping statements precede it. Query ordering is not the
                // contract this wire-error test owns.
                loop {
                    let run = request(&mut socket).await;
                    if run[1] == 0x12 {
                        break;
                    } // COMMIT
                    assert_eq!(run[1], 0x10); // RUN
                    let discovery = run
                        .windows(b"AS namespace".len())
                        .any(|part| part == b"AS namespace");
                    commit_receipts::send(
                        &mut socket,
                        0x70,
                        json!({
                            "fields": [if discovery { "namespace" } else { "ok" }]
                        }),
                    )
                    .await;
                    assert_eq!(request(&mut socket).await[1], 0x3f); // PULL
                    commit_receipts::send(
                        &mut socket,
                        0x71,
                        if discovery {
                            json!(["prod"])
                        } else {
                            json!([true])
                        },
                    )
                    .await;
                    frame(&mut socket, &[0xb1, 0x70, 0xa0]).await;
                }
                match outcome {
                    "transport" => {}
                    "success" => frame(&mut socket, &[0xb1, 0x70, 0xa0]).await,
                    "malformed" => frame(&mut socket, &[0xb0, 0x42]).await, // Unknown response signature
                    "ignored" => frame(&mut socket, &[0xb0, 0x7e]).await,
                    code => {
                        let mut failure = vec![
                            0xb1,
                            0x7f,
                            0xa2,
                            0x84,
                            b'c',
                            b'o',
                            b'd',
                            b'e',
                            0xd0,
                            code.len() as u8,
                        ];
                        failure.extend_from_slice(code.as_bytes());
                        failure.extend_from_slice(&[
                            0x87, b'm', b'e', b's', b's', b'a', b'g', b'e', 0x84, b't', b'e', b's',
                            b't',
                        ]);
                        frame(&mut socket, &failure).await;
                    }
                }
                drop(socket);
                assert!(
                    tokio::time::timeout(Duration::from_millis(30), listener.accept())
                        .await
                        .is_err(),
                    "unexpected replay for {outcome}"
                );
            };
            let client = async {
                if classify_only {
                    let result = graph.graph.start_txn().await.unwrap().commit().await;
                    if outcome.starts_with("Neo.") {
                        let error = result.unwrap_err();
                        assert!(matches!(error, neo4rs::Error::Neo4j(_)));
                        match (outcome, classify_driver(error, "commit")) {
                            (
                                "Neo.TransientError.Transaction.DeadlockDetected",
                                AttemptError::Retry(_),
                            )
                            | (
                                "Neo.ClientError.Security.Forbidden",
                                AttemptError::Final(BackendError::Auth(_)),
                            )
                            | (
                                "Neo.ClientError.Transaction.Terminated",
                                AttemptError::Final(BackendError::Transaction(_)),
                            )
                            | (
                                "Neo.ClientError.Schema.ConstraintValidationFailed",
                                AttemptError::Final(BackendError::Conflict(_)),
                            ) => {}
                            _ => panic!("wrong known-rejection classification for {outcome}"),
                        }
                    } else if outcome == "success" {
                        result.unwrap();
                    } else {
                        assert!(!matches!(result, Ok(()) | Err(neo4rs::Error::Neo4j(_))));
                    }
                } else {
                    let result = graph
                        .apply_mutations(
                            "wire-test",
                            &[GraphMutation::UpdateEdge {
                                uuid: uuid::Uuid::new_v4(),
                                properties: json!({"name":"test"}).as_object().unwrap().clone(),
                            }],
                        )
                        .await;
                    match (outcome, result) {
                        ("success", Ok(()))
                        | (
                            "transport" | "malformed" | "ignored",
                            Err(BackendError::UnknownCommit(_)),
                        )
                        | ("Neo.ClientError.Security.Forbidden", Err(BackendError::Auth(_)))
                        | (
                            "Neo.ClientError.Transaction.Terminated",
                            Err(BackendError::Transaction(_)),
                        )
                        | (
                            "Neo.ClientError.Schema.ConstraintValidationFailed",
                            Err(BackendError::Conflict(_)),
                        )
                        | (
                            "Neo.TransientError.Transaction.DeadlockDetected",
                            Err(BackendError::Query(_)),
                        ) => {}
                        (name, result) => panic!("unexpected {name} outcome: {result:?}"),
                    }
                }
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(server, client);
            })
            .await
            .unwrap();
        }
    }
}

mod commit_receipts;

#[test]
fn compiled_statement_budget_includes_preconditions() {
    let limit = kg_core::traits::graph_commit::MAX_STATEMENTS_PER_BATCH;
    super::validate_compiled_batch_budget(1, limit - 1).unwrap();
    assert!(super::validate_compiled_batch_budget(1, limit).is_err());
    assert!(super::validate_compiled_batch_budget(0, 5005).is_err());
    assert!(super::validate_compiled_batch_budget(usize::MAX, 1).is_err());
}

mod rejection_diagnostic;
