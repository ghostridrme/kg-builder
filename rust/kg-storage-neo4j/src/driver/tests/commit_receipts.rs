//! Full receipt recovery uses a scripted Bolt peer, with execution counts proving
//! which decisions replay the transaction and which only read its receipt.
use super::*;
use kg_core::traits::{BatchIdentity, BatchKind, GraphBackend, MutationBatch, RequestFingerprint};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
struct Counts {
    begins: AtomicU32,
    commits: AtomicU32,
    staged_reads: AtomicU32,
    recovery_reads: AtomicU32,
    receipt_writes: AtomicU32,
    mutation_writes: AtomicU32,
    rollbacks: AtomicU32,
    community_discoveries: AtomicU32,
    community_locks: AtomicU32,
    community_invalidations: AtomicU32,
    community_advances: AtomicU32,
}
fn pack(value: &serde_json::Value, bytes: &mut Vec<u8>) {
    match value {
        serde_json::Value::String(s) => {
            if s.len() < 16 {
                bytes.push(0x80 | s.len() as u8);
            } else {
                assert!(s.len() < 256);
                bytes.extend([0xd0, s.len() as u8]);
            }
            bytes.extend(s.as_bytes());
        }
        serde_json::Value::Array(values) => {
            assert!(values.len() < 16);
            bytes.push(0x90 | values.len() as u8);
            for v in values {
                pack(v, bytes);
            }
        }
        serde_json::Value::Object(values) => {
            assert!(values.len() < 16);
            bytes.push(0xa0 | values.len() as u8);
            for (k, v) in values {
                pack(&json!(k), bytes);
                pack(v, bytes);
            }
        }
        serde_json::Value::Bool(v) => bytes.push(if *v { 0xc3 } else { 0xc2 }),
        serde_json::Value::Number(n) => bytes.push(n.as_u64().unwrap().try_into().unwrap()),
        _ => panic!("unsupported fixture value"),
    }
}
pub(super) async fn send(
    socket: &mut tokio::net::TcpStream,
    signature: u8,
    value: serde_json::Value,
) {
    let mut bytes = vec![0xb1, signature];
    pack(&value, &mut bytes);
    socket.write_u16(bytes.len() as u16).await.unwrap();
    socket.write_all(&bytes).await.unwrap();
    socket.write_u16(0).await.unwrap();
}
async fn receive(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        let len = socket.read_u16().await.ok()? as usize;
        if len == 0 {
            return Some(bytes);
        }
        let start = bytes.len();
        bytes.resize(start + len, 0);
        socket.read_exact(&mut bytes[start..]).await.ok()?;
    }
}
async fn serve(
    mut socket: tokio::net::TcpStream,
    mode: &'static str,
    counts: Arc<Counts>,
    receipt: serde_json::Value,
) {
    socket.read_exact(&mut [0; 20]).await.unwrap();
    socket.write_all(&[0, 0, 1, 4]).await.unwrap();
    let mut in_txn = false;
    let mut pending = Vec::<serde_json::Value>::new();
    while let Some(request) = receive(&mut socket).await {
        match request[1] {
            0x01 | 0x0f => send(&mut socket, 0x70, json!({})).await, // HELLO/RESET
            0x11 => {
                in_txn = true;
                counts.begins.fetch_add(1, Ordering::SeqCst);
                send(&mut socket, 0x70, json!({})).await;
            }
            0x10 => {
                // This atomic-commit matrix has no frozen page plan. Return an
                // empty read, rather than a generic write acknowledgment row.
                if request
                    .windows(b"MATCH (p:CommitPlan".len())
                    .any(|window| window == b"MATCH (p:CommitPlan")
                {
                    pending.clear();
                    send(&mut socket, 0x70, json!({"fields":[]})).await;
                    continue;
                }
                let is_read = request
                    .windows(b"MATCH (r:OperationReceipt".len())
                    .any(|w| w == b"MATCH (r:OperationReceipt");
                if is_read {
                    if in_txn {
                        counts.staged_reads.fetch_add(1, Ordering::SeqCst);
                    } else {
                        counts.recovery_reads.fetch_add(1, Ordering::SeqCst);
                    }
                    let rows = if (in_txn && mode == "duplicate_staged")
                        || (!in_txn && mode == "duplicate_recovery")
                    {
                        2
                    } else if !in_txn && mode == "recover" {
                        1
                    } else {
                        0
                    };
                    let map = receipt.as_object().unwrap();
                    let fields: Vec<_> = map.keys().cloned().collect();
                    let values: Vec<_> = map.values().cloned().collect();
                    pending = (0..rows).map(|_| json!(values)).collect();
                    send(&mut socket, 0x70, json!({"fields":fields})).await;
                } else {
                    let contains =
                        |text: &str| request.windows(text.len()).any(|w| w == text.as_bytes());
                    let discovery = contains("RETURN DISTINCT n.namespace AS namespace");
                    for (present, counter) in [
                        (discovery, &counts.community_discoveries),
                        (
                            contains("MERGE (r:CommunityRevision"),
                            &counts.community_locks,
                        ),
                        (contains("c.dirty=true"), &counts.community_invalidations),
                        (
                            contains("SET r.revision=r.revision+1"),
                            &counts.community_advances,
                        ),
                    ] {
                        if present {
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    if discovery {
                        pending = vec![json!(["prod"])];
                        send(&mut socket, 0x70, json!({"fields":["namespace"]})).await;
                        continue;
                    }
                    if request
                        .windows(b"SET r += $props".len())
                        .any(|w| w == b"SET r += $props")
                    {
                        counts.mutation_writes.fetch_add(1, Ordering::SeqCst);
                    }
                    if request
                        .windows(b"CREATE (r:OperationReceipt".len())
                        .any(|w| w == b"CREATE (r:OperationReceipt")
                    {
                        counts.receipt_writes.fetch_add(1, Ordering::SeqCst);
                    }
                    pending = vec![json!([true])];
                    send(&mut socket, 0x70, json!({"fields":["ok"]})).await;
                }
            }
            0x3f => {
                for row in pending.drain(..) {
                    send(&mut socket, 0x71, row).await;
                }
                send(&mut socket, 0x70, json!({})).await;
            }
            0x12 => {
                let index = counts.commits.fetch_add(1, Ordering::SeqCst);
                in_txn = false;
                match mode {
                    "success" => send(&mut socket, 0x70, json!({})).await,
                    "transient" if index > 0 => send(&mut socket, 0x70, json!({})).await,
                    "transient" | "auth" | "terminated" => {
                        let code = match mode {
                            "transient" => "Neo.TransientError.Transaction.DeadlockDetected",
                            "auth" => "Neo.ClientError.Security.Forbidden",
                            _ => "Neo.ClientError.Transaction.Terminated",
                        };
                        send(
                            &mut socket,
                            0x7f,
                            json!({"code":code,"message":"scripted rejection"}),
                        )
                        .await;
                    }
                    "malformed" => {
                        send(&mut socket, 0x42, json!({})).await;
                        return;
                    }
                    _ => return, // Socket loss after COMMIT, before acknowledgement.
                }
            }
            0x13 => {
                in_txn = false;
                counts.rollbacks.fetch_add(1, Ordering::SeqCst);
                send(&mut socket, 0x70, json!({})).await;
            }
            signature => panic!("unexpected Bolt request {signature:#x}"),
        }
    }
}

#[tokio::test]
async fn full_receipted_commit_matrix_checks_receipts_and_retries_only_known_rejection() {
    for mode in [
        "success",
        "transient",
        "auth",
        "terminated",
        "transport",
        "malformed",
        "recover",
        "duplicate_staged",
        "duplicate_recovery",
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let graph = Neo4jGraphBackend::with_options(
            &format!("bolt://{}", listener.local_addr().unwrap()),
            "user",
            "password",
            Neo4jOptions {
                timeout: Duration::from_secs(2),
                max_retries: 3,
                commit_verification_attempts: 2,
                base_backoff: Duration::ZERO,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let batch = MutationBatch {
            org_id: "wire-receipts".into(),
            batch: BatchIdentity {
                run_id: uuid::Uuid::new_v4(),
                kind: BatchKind::Node,
                index: 0,
            },
            fingerprint: RequestFingerprint("a".repeat(32)),
            preconditions: vec![],
            mutations: vec![kg_core::traits::GraphMutation::UpdateEdge {
                uuid: uuid::Uuid::new_v4(),
                properties: json!({"name":"written"}).as_object().unwrap().clone(),
            }],
            result: json!({"stored":true}),
        };
        let receipt = json!({"batch_id":batch.batch_id(),"org_id":batch.org_id,"run_id":batch.batch.run_id,
            "kind":batch.batch.kind.label(),"index":0,"fingerprint":batch.fingerprint.0,
            "committed_at":"2026-09-15T12:00:00Z","result":batch.result.to_string()});
        let counts = Arc::new(Counts::default());
        let server_counts = Arc::clone(&counts);
        let server = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=> {
                        let (socket,_)=accepted.unwrap();
                        tasks.spawn(serve(socket,mode,Arc::clone(&server_counts),receipt.clone()));
                    }
                    result=tasks.join_next(), if !tasks.is_empty()=>{result.unwrap().unwrap();}
                }
            }
        });
        let result = tokio::time::timeout(Duration::from_secs(5), graph.commit_batch(&batch))
            .await
            .unwrap();
        match (mode, result) {
            ("success" | "transient" | "recover", Ok(committed)) => {
                assert_eq!(committed.result, batch.result);
                assert!(!committed.replayed);
            }
            ("auth", Err(BackendError::Auth(_)))
            | ("terminated", Err(BackendError::Transaction(_)))
            | (
                "transport" | "malformed" | "duplicate_staged" | "duplicate_recovery",
                Err(BackendError::UnknownCommit(_)),
            ) => {}
            (mode, result) => panic!("{mode}: unexpected result {result:?}"),
        }
        let load = |counter: &AtomicU32| counter.load(Ordering::SeqCst);
        let transactions = if mode == "transient" { 2 } else { 1 };
        assert_eq!(load(&counts.begins), transactions, "{mode}: BEGIN count");
        let staged = if mode == "duplicate_staged" {
            0
        } else {
            transactions
        };
        for (counter, per_attempt, label) in [
            (&counts.community_discoveries, 3, "community discovery"),
            (&counts.community_locks, 1, "community locks"),
            (&counts.community_invalidations, 1, "community invalidation"),
            (&counts.community_advances, 1, "community revision advance"),
        ] {
            assert_eq!(load(counter), staged * per_attempt, "{mode}: {label}");
        }

        assert_eq!(
            load(&counts.staged_reads),
            transactions,
            "{mode}: staged reads"
        );
        assert_eq!(
            load(&counts.commits),
            if mode == "duplicate_staged" {
                0
            } else {
                transactions
            },
            "{mode}: COMMIT count"
        );
        assert_eq!(
            load(&counts.mutation_writes),
            if mode == "duplicate_staged" {
                0
            } else {
                transactions
            },
            "{mode}: mutation writes"
        );
        assert_eq!(
            load(&counts.receipt_writes),
            if mode == "duplicate_staged" {
                0
            } else {
                transactions
            },
            "{mode}: receipt writes"
        );
        assert_eq!(
            load(&counts.recovery_reads),
            match mode {
                "success" | "duplicate_staged" => 0,
                "transient" | "auth" | "terminated" | "recover" => 1,
                _ => 2,
            },
            "{mode}: recovery reads"
        );
        assert_eq!(
            load(&counts.rollbacks),
            u32::from(mode == "duplicate_staged"),
            "{mode}: rollbacks"
        );
        assert!(!server.is_finished(), "wire server panicked for {mode}");
        server.abort();
        let _ = server.await;
    }
}
