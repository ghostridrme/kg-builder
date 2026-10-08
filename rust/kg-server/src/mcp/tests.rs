use super::auth::{reader_roles_allowed, Credential};
use super::*;
use kg_core::{
    search::SearchPage,
    traits::{
        graph_explorer::{ExplorerRequest, GraphExplorerBackend},
        SearchBackend,
    },
};
use kg_search::SearchEngine;
use rmcp::ServiceExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

struct FakeGraph;
#[async_trait::async_trait]
impl GraphExplorerBackend for FakeGraph {
    async fn explore(&self, request: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
        assert_eq!(request.org_id, "org-a");
        assert_eq!(request.namespace.as_deref(), Some("prod"));
        Ok(SearchPage::bounded(
            vec![json!({"namespace":"prod","entity_type":"Service","count":2})],
            request.limit,
        ))
    }
    async fn read_saga(
        &self,
        org: &str,
        request: &kg_core::saga::SagaRead,
    ) -> Result<kg_core::saga::SagaReadResult, BackendError> {
        crate::test_support::saga_read(org, request)
    }
}

#[tokio::test]
async fn summary_tool_requires_the_credential_flag_and_a_configured_engine() {
    let credential = |id: &str, token: char, allow: bool| Credential {
        id: id.into(),
        token: token.to_string().repeat(40),
        org_id: "org-a".into(),
        namespace: Some("prod".into()),
        allow_raw_cypher_all_data: false,
        allow_saga_summaries: allow,
    };
    let auth = AuthState(Arc::new(vec![
        credential("reader", 'a', false),
        credential("writer", 'b', true),
    ]));
    let serve = |summaries: bool, auth: AuthState| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let query = if summaries {
            Arc::new(
                GraphQueryService::new(
                    Arc::new(FakeGraph),
                    Arc::new(SearchEngine::new(Arc::new(FakeSearch))),
                    false,
                )
                .with_saga_summarizer(Arc::new(crate::test_support::FakeSummarizer)),
            )
        } else {
            test_query()
        };
        let server = tokio::spawn(async move {
            axum::serve(listener, remote_router(query, None, auth))
                .await
                .unwrap();
        });
        (server, format!("http://{address}/mcp"))
    };
    let client = reqwest::Client::new();
    let call = |endpoint: String, token: char, arguments: Value| {
        let client = client.clone();
        async move {
            let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                    "params":{"name":"summarize_thread","arguments":arguments}});
            let response: Value = client
                .post(&endpoint)
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2025-11-25")
                .bearer_auth(token.to_string().repeat(40))
                .json(&body)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            response["result"].clone()
        }
    };
    let (unconfigured, endpoint) = serve(false, auth.clone()).await;
    let denied = call(endpoint.clone(), 'a', json!({"name":"incident"})).await;
    assert_eq!(denied["structuredContent"]["code"], "forbidden");
    let unavailable = call(endpoint, 'b', json!({"name":"incident"})).await;
    assert_eq!(unavailable["structuredContent"]["code"], "unavailable");
    unconfigured.abort();

    let (configured, endpoint) = serve(true, auth).await;
    let run = Uuid::from_u128(77);
    let outcome = call(
        endpoint.clone(),
        'b',
        json!({"name":"incident","run_id":run}),
    )
    .await;
    assert_ne!(outcome["isError"], true, "{outcome}");
    let value = &outcome["structuredContent"];
    assert_eq!(value["run_id"], json!(run));
    assert_eq!(value["memberships_summarized"], 3);
    assert_eq!(value["replayed_batches"], 1);
    assert_eq!(value["thread"]["name"], "incident");
    assert_eq!(value["thread"]["summary_truncated"], false);
    let missing = call(
        endpoint.clone(),
        'b',
        json!({"saga_uuid":Uuid::from_u128(5),"run_id":run}),
    )
    .await;
    assert_eq!(missing["structuredContent"]["code"], "not_found");
    assert_eq!(missing["structuredContent"]["run_id"], json!(run));
    let bad_run = call(endpoint, 'b', json!({"name":"incident","run_id":"nope"})).await;
    assert_eq!(bad_run["structuredContent"]["code"], "invalid_input");
    configured.abort();
}

#[tokio::test]
async fn saga_tools_enforce_namespace_scope_and_withhold_historical_summaries() {
    let auth = AuthState(Arc::new(vec![
        Credential {
            id: "scoped".into(),
            token: "a".repeat(40),
            org_id: "org-a".into(),
            namespace: Some("prod".into()),
            allow_raw_cypher_all_data: false,
            allow_saga_summaries: false,
        },
        Credential {
            id: "open".into(),
            token: "b".repeat(40),
            org_id: "org-a".into(),
            namespace: None,
            allow_raw_cypher_all_data: false,
            allow_saga_summaries: false,
        },
    ]));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, remote_router(test_query(), None, auth))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let endpoint = format!("http://{address}/mcp");
    let call = |token: char, tool: &str, arguments: Value| {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                "params":{"name":tool,"arguments":arguments}});
        let client = client.clone();
        let endpoint = endpoint.clone();
        async move {
            let response: Value = client
                .post(&endpoint)
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2025-11-25")
                .bearer_auth(token.to_string().repeat(40))
                .json(&body)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            response["result"].clone()
        }
    };
    let incident = kg_core::saga::saga_uuid("org-a", "prod", "incident");

    let listed = call('a', "list_threads", json!({})).await;
    assert_ne!(listed["isError"], true, "{listed}");
    let items = &listed["structuredContent"]["items"];
    assert_eq!(items[0]["name"], "incident");
    assert_eq!(items[0]["uuid"], json!(incident));
    assert_eq!(items[0]["summary_truncated"], false);

    // The scoped caller cannot read another namespace; the open caller must name one.
    let denied = call('a', "list_threads", json!({"namespace":"dev"})).await;
    assert_eq!(denied["structuredContent"]["code"], "invalid_scope");
    let missing = call('b', "list_threads", json!({})).await;
    assert_eq!(missing["structuredContent"]["code"], "invalid_input");
    let open = call('b', "list_threads", json!({"namespace":"prod"})).await;
    assert_eq!(open["structuredContent"]["items"][0]["name"], "incident");

    let current = call('a', "get_thread", json!({"name":"incident"})).await;
    assert_eq!(
        current["structuredContent"]["summary"],
        "Payments failed over to the standby region."
    );
    let early = call(
        'a',
        "get_thread",
        json!({"saga_uuid":incident,"as_of":"2026-01-05T00:00:00Z"}),
    )
    .await;
    assert_eq!(early["structuredContent"]["summary"], Value::Null);
    assert_eq!(
        early["structuredContent"]["summary_withheld"],
        "covers_later_observations"
    );
    assert_eq!(
        early["structuredContent"]["summary_covers_captured_through"],
        json!(crate::test_support::day(10))
    );
    let both = call(
        'a',
        "get_thread",
        json!({"name":"incident","saga_uuid":incident}),
    )
    .await;
    assert_eq!(both["structuredContent"]["code"], "invalid_input");
    let unknown = call('a', "get_thread", json!({"saga_uuid":Uuid::from_u128(5)})).await;
    assert_eq!(unknown["structuredContent"]["code"], "not_found");

    let members = call(
        'a',
        "get_thread_members",
        json!({"thread_uuid":incident,"limit":2}),
    )
    .await;
    let page = &members["structuredContent"];
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    assert_eq!(page["truncated"], true);
    assert_eq!(page["next_after_ordinal"], 2);
    let rest = call(
        'a',
        "get_thread_members",
        json!({"saga_uuid":incident,"after_ordinal":2,"as_of":"2026-01-09T00:00:00Z"}),
    )
    .await;
    assert_eq!(
        rest["structuredContent"]["items"].as_array().unwrap().len(),
        0
    );
    assert_eq!(rest["structuredContent"]["truncated"], false);
    let absent = call(
        'a',
        "get_thread_members",
        json!({"saga_uuid":Uuid::from_u128(5)}),
    )
    .await;
    assert_eq!(absent["structuredContent"]["code"], "not_found");

    let found = call(
        'a',
        "search_thread_snapshots",
        json!({"query":"checkout","name":"incident"}),
    )
    .await;
    assert_ne!(found["isError"], true, "{found}");
    let snapshots = &found["structuredContent"]["snapshots"];
    assert_eq!(snapshots.as_array().unwrap().len(), 1);
    assert_eq!(snapshots[0]["excerpt"], "checkout failed over");
    assert_eq!(snapshots[0]["selection_kind"], "matched");
    let unknown = call(
        'a',
        "search_thread_snapshots",
        json!({"query":"checkout","name":"nope"}),
    )
    .await;
    assert_eq!(unknown["structuredContent"]["code"], "not_found");
    let ambiguous = call('a', "search_thread_snapshots", json!({"query":"checkout"})).await;
    assert_eq!(ambiguous["structuredContent"]["code"], "invalid_input");
    server.abort();
}
struct FakeSearch;
#[async_trait::async_trait]
impl SearchBackend for FakeSearch {
    async fn search_snapshots(
        &self,
        request: &kg_core::search::EvidenceSearch,
    ) -> Result<SearchPage<kg_core::search::SnapshotHit>, BackendError> {
        crate::test_support::saga_snapshot_search(request)
    }
}

fn test_query() -> Arc<GraphQueryService> {
    Arc::new(GraphQueryService::new(
        Arc::new(FakeGraph),
        Arc::new(SearchEngine::new(Arc::new(FakeSearch))),
        false,
    ))
}

#[test]
fn compact_entity_bounds_source_data() {
    let entity = json!({"name":"x".repeat(1000),"all_properties": {
            "large":"y".repeat(1000),"nested":{"key":"z".repeat(1000)}
        },"embedding":[1,2,3],"org_id":"secret"});
    let output = compact_entity(&entity, true);
    assert!(output.to_string().len() < 2000);
    assert!(output.get("embedding").is_none());
    assert!(output.get("org_id").is_none());
}

#[test]
fn entity_details_decode_flattened_neo4j_attributes() {
    let stored = json!({
        "chain_id":"a","name":"worker","prop_InstanceType":"m7i.large",
        "prop_Settings":"{\"enabled\":true}","property_type_Settings":"j",
        "tag_team":"platform","labels":["compute"]
    });
    let entity = compact_entity(&stored, true);
    assert_eq!(entity["properties"]["InstanceType"], "m7i.large");
    assert_eq!(entity["properties"]["Settings"]["enabled"], true);
    assert_eq!(entity["tags"]["team"], "platform");
    assert_eq!(entity["labels"][0], "compute");
}

#[test]
fn compact_entities_preserve_scope_and_distinct_property_keys() {
    let namespace = "n".repeat(200);
    let entity_type = "t".repeat(200);
    let prefix = "p".repeat(170);
    let key_a = format!("{prefix}a");
    let key_b = format!("{prefix}b");
    let mut stored = json!({"namespace":namespace,"entity_type":entity_type});
    stored[format!("prop_{key_a}")] = json!(1);
    stored[format!("prop_{key_b}")] = json!(2);
    let compact = compact_entity(&stored, true);
    assert_eq!(compact["namespace"], stored["namespace"]);
    assert_eq!(compact["entity_type"], stored["entity_type"]);
    assert_eq!(compact["properties"][&key_a], 1);
    assert_eq!(compact["properties"][&key_b], 2);
    let nested = compact_value(&json!({key_a.clone():1,key_b.clone():2}), 0);
    assert_eq!(nested[&key_a], 1);
    assert_eq!(nested[&key_b], 2);
}

#[test]
fn oversized_ranked_results_keep_the_best_hits_and_report_truncation() {
    for key in ["hits", "snapshots"] {
        let items: Vec<_> = (0..10)
            .map(|rank| json!({"rank":rank,"excerpt":"漢".repeat(400)}))
            .collect();
        let result = respond(Ok(json!({key:items,"truncated":false})));
        assert_ne!(result.is_error, Some(true));
        let value = result.structured_content.unwrap();
        assert!(value.to_string().len() <= MAX_RESULT_BYTES);
        assert_eq!(value["truncated"], true);
        let kept = value[key].as_array().unwrap();
        assert!(!kept.is_empty() && kept.len() < 10);
        for (rank, item) in kept.iter().enumerate() {
            assert_eq!(item["rank"], rank);
        }
    }
}

/// The published tool set; a new tool is added here deliberately.
const TOOL_NAMES: [&str; 21] = [
    "find_paths",
    "get_capabilities",
    "get_changes",
    "get_community",
    "get_entity",
    "get_entity_history",
    "get_graph_schema",
    "get_neighbors",
    "get_relationship",
    "get_snapshot",
    "get_subgraph",
    "get_thread",
    "get_thread_members",
    "list_catalog",
    "list_entities",
    "list_threads",
    "run_readonly_cypher",
    "search",
    "search_entities",
    "search_thread_snapshots",
    "summarize_thread",
];

fn sorted_names<'a>(names: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    let mut names: Vec<_> = names.collect();
    names.sort_unstable();
    names
}

#[test]
fn only_the_summary_tool_writes() {
    let tools = McpGraph::tool_router().list_all();
    assert_eq!(
        sorted_names(tools.iter().map(|t| t.name.as_ref())),
        TOOL_NAMES
    );
    for tool in &tools {
        let read_only = tool
            .annotations
            .as_ref()
            .and_then(|a| a.read_only_hint)
            .unwrap_or(false);
        assert_eq!(
            read_only,
            tool.name != "summarize_thread",
            "{} declares read_only_hint={read_only}",
            tool.name
        );
        assert!(tool
            .annotations
            .as_ref()
            .is_some_and(|a| a.destructive_hint == Some(false)));
    }
}

#[test]
fn oversized_member_pages_move_the_ordinal_cursor_back_to_the_last_returned_member() {
    let items: Vec<_> = (1..=20u64)
        .map(|n| json!({"ordinal":n,"snapshot_name":"x".repeat(900)}))
        .collect();
    let result = respond(Ok(
        json!({"items":items,"after_ordinal":0,"truncated":false,"next_after_ordinal":null}),
    ));
    let value = result.structured_content.unwrap();
    assert!(value.to_string().len() <= MAX_RESULT_BYTES);
    let items = value["items"].as_array().unwrap();
    assert!(items.len() < 20);
    assert_eq!(value["truncated"], true);
    assert_eq!(
        value["next_after_ordinal"],
        items.last().unwrap()["ordinal"]
    );
    assert!(value.get("next_offset").is_none());
}

#[test]
fn oversized_summary_outcomes_keep_the_run_id_and_counts() {
    let run = Uuid::from_u128(5);
    let support: Vec<_> = (0..400u128).map(Uuid::from_u128).collect();
    let outcome = json!({"run_id":run,"batches":3,"replayed_batches":0,"memberships_summarized":400,
            "summaries_updated":1,"thread":{"uuid":Uuid::from_u128(9),"name":"incident","summary_covers_members_through":400,
            "summary":"y".repeat(13_000),"summary_supporting_snapshot_uuids":support}});
    let result = respond(Ok(outcome.clone()));
    let value = result.structured_content.unwrap();
    assert!(
        value.to_string().len() <= MAX_RESULT_BYTES,
        "{}",
        value.to_string().len()
    );
    assert_eq!(value["run_id"], json!(run));
    assert_eq!(value["memberships_summarized"], 400);
    assert_eq!(value["thread"]["summary_supporting_snapshot_count"], 400);
    assert_eq!(
        value["thread"]["summary_supporting_snapshot_uuids"],
        json!([])
    );
    assert_eq!(value["thread"]["summary_truncated"], true);
    assert_eq!(value["thread"]["name"], "incident");

    // A hopeless payload still reports the committed run rather than an error.
    let mut huge = outcome;
    huge["thread"]["name"] = json!("z".repeat(13_000));
    let value = respond(Ok(huge)).structured_content.unwrap();
    assert_eq!(value["run_id"], json!(run));
    assert_eq!(value["thread"], Value::Null);
    assert_eq!(value["thread_read_error"], "result_too_large");
}

#[test]
fn oversized_saga_reads_shed_detail_instead_of_failing() {
    // get_saga: the Saga view itself is the response.
    let support: Vec<_> = (0..100u128).map(Uuid::from_u128).collect();
    let saga = json!({"uuid":Uuid::from_u128(9),"name":"incident","total_members":100,
            "summary_covers_members_through":100,"summary_covers_captured_through":"2026-01-10T00:00:00Z",
            "summary":"漢".repeat(4_500),"summary_supporting_snapshot_uuids":support});
    let value = respond(Ok(saga.clone())).structured_content.unwrap();
    assert!(value.to_string().len() <= MAX_RESULT_BYTES);
    assert_eq!(value["name"], "incident");
    assert_eq!(value["summary_covers_members_through"], 100);
    assert_eq!(value["summary_supporting_snapshot_count"], 100);
    assert_eq!(value["summary_supporting_snapshot_uuids"], json!([]));
    assert_eq!(value["summary"], Value::Null);
    assert_eq!(value["summary_truncated"], true);

    // list_sagas: one remaining oversized item is shed the same way, never dropped.
    let page = json!({"items":[saga],"offset":0,"truncated":false,"next_offset":null});
    let value = respond(Ok(page)).structured_content.unwrap();
    assert!(value.to_string().len() <= MAX_RESULT_BYTES);
    assert_eq!(value["items"].as_array().unwrap().len(), 1);
    assert_eq!(value["items"][0]["summary_truncated"], true);
    assert_eq!(value["truncated"], false);

    // Something that is neither a page nor a Saga still reports result_too_large.
    let other = respond(Ok(json!({"blob":"x".repeat(13_000)})));
    assert_eq!(
        other.structured_content.unwrap()["code"],
        "result_too_large"
    );
}

#[test]
fn oversized_pages_keep_a_retrievable_next_offset() {
    let items: Vec<_> = (0..20)
        .map(|n| json!({"name":"x".repeat(900),"n":n}))
        .collect();
    let result = respond(Ok(
        json!({"items":items,"offset":40,"truncated":false,"next_offset":null}),
    ));
    let value = result.structured_content.unwrap();
    assert!(value.to_string().len() <= MAX_RESULT_BYTES);
    assert_eq!(value["truncated"], true);
    assert_eq!(
        value["next_offset"],
        40 + value["items"].as_array().unwrap().len()
    );
}

#[test]
fn raw_cypher_requires_plain_read_statement() {
    assert!(validate_cypher("MATCH (n:Entity) RETURN n.name LIMIT 5").is_ok());
    for statement in [
        "MATCH (n) SET n.name='x' RETURN n",
        "MATCH (n) CALL db.labels() RETURN n",
        "MATCH (n) RETURN n; MATCH (m) RETURN m",
        "MATCH (n) /* hidden */ RETURN n",
        "CREATE (n) RETURN n",
    ] {
        assert!(validate_cypher(statement).is_err(), "{statement}");
    }
}

#[test]
fn cypher_account_rejects_extra_roles() {
    assert!(reader_roles_allowed(&["reader".into(), "PUBLIC".into()]));
    assert!(!reader_roles_allowed(&["admin".into()]));
    assert!(!reader_roles_allowed(&[
        "reader".into(),
        "publisher".into()
    ]));
}

#[tokio::test]
async fn remote_transport_authenticates_and_scopes_tool_calls() {
    let auth = AuthState(Arc::new(vec![Credential {
        id: "agent-1".into(),
        token: "a".repeat(40),
        org_id: "org-a".into(),
        namespace: Some("prod".into()),
        allow_raw_cypher_all_data: false,
        allow_saga_summaries: false,
    }]));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, remote_router(test_query(), None, auth))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let endpoint = format!("http://{address}/mcp");
    let send = |body: Value, authorized: bool| {
        let mut request = client
            .post(&endpoint)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-11-25")
            .json(&body);
        if authorized {
            request = request.bearer_auth("a".repeat(40));
        }
        request
    };
    let list = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    assert_eq!(
        send(list.clone(), false).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let response = send(list, true).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let listed: Value = response.json().await.unwrap();
    assert_eq!(
        sorted_names(
            listed["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
        ),
        TOOL_NAMES
            .into_iter()
            .filter(|name| !matches!(*name, "run_readonly_cypher" | "summarize_thread"))
            .collect::<Vec<_>>()
    );
    let call = |namespace: Option<&str>| {
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call",
            "params":{"name":"list_catalog","arguments":{"namespace":namespace}}})
    };
    let result: Value = send(call(None), true)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        result["result"]["structuredContent"]["items"][0]["namespace"],
        "prod"
    );
    let denied: Value = send(call(Some("dev")), true)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(denied["result"]["isError"], true);
    assert_eq!(
        denied["result"]["structuredContent"]["code"],
        "invalid_scope"
    );
    let cypher_call = json!({"jsonrpc":"2.0","id":3,"method":"tools/call",
            "params":{"name":"run_readonly_cypher","arguments":{"query":"MATCH (n) RETURN n LIMIT 1"}}});
    let denied: Value = send(cypher_call, true)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(denied["result"]["structuredContent"]["code"], "forbidden");
    server.abort();
}

#[tokio::test]
async fn stdio_transport_serves_the_same_scoped_tools() {
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(async move {
        let principal = Principal {
            id: "local".into(),
            org_id: "org-a".into(),
            namespace: Some("prod".into()),
            allow_raw_cypher_all_data: false,
            allow_saga_summaries: false,
        };
        McpGraph::local(test_query(), None, principal)
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (read, mut write) = tokio::io::split(client_io);
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}
    }});
    write
        .write_all(format!("{initialize}\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reader.read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let init: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(init["id"], 1);
    line.clear();
    write
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .unwrap();
    write.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"list_catalog\",\"arguments\":{}}}\n").await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reader.read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let result: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(
        result["result"]["structuredContent"]["items"][0]["namespace"],
        "prod"
    );
    server.abort();
}

#[tokio::test]
async fn http_api_uses_the_shared_query_service() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            crate::router(crate::AppState {
                query: test_query(),
                org_id: "org-a".into(),
                write_token: None,
                rules: None,
                rule_repair_engine: None,
            }),
        )
        .await
        .unwrap();
    });
    let response = reqwest::get(format!("http://{address}/api/v1/catalog?namespace=prod"))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["items"][0]["entity_type"], "Service");
    assert_eq!(body["org_id"], "org-a");
    server.abort();
}

#[test]
fn exact_properties_can_be_reassembled_and_preserve_types() {
    let arn = format!("arn:provider:resource:{}", "资源/".repeat(1000));
    let node = json!({"chain_id":Uuid::from_u128(1),"uuid":Uuid::from_u128(2),"name":"long", "prop_arn":arn,
        "prop_nested":"{\"port\":443,\"enabled\":true,\"missing\":null}","property_type_nested":"j"});
    let value = projection::entity(&node, None).unwrap();
    assert_eq!(value["properties"]["nested"]["port"], 443);
    assert_eq!(value["properties_omitted"], 1);
    let mut start = 0;
    let mut recovered = String::new();
    loop {
        let part = projection::field_range(&node, "/properties/arn", start, 2000).unwrap();
        recovered.push_str(part["content"].as_str().unwrap());
        assert!(!respond(Ok(part.clone())).is_error.unwrap_or(false));
        let Some(next) = part["next_start"].as_u64() else {
            break;
        };
        start = next as usize;
    }
    assert_eq!(serde_json::from_str::<String>(&recovered).unwrap(), arn);
}
#[test]
fn result_pages_bind_scope_preserve_items_and_never_skip() {
    let cache = crate::investigation::ResultPages::default();
    let values: Vec<_> = (0..75)
        .map(|i| json!({"id":i,"name":"resource".repeat(30)}))
        .collect();
    let mut page = cache
        .insert_sized(
            "org:namespace:query".into(),
            json!({"effective_as_of":"2026-01-01T00:00:00Z"}),
            values.clone(),
            5,
        )
        .unwrap();
    let mut seen = Vec::new();
    loop {
        assert!(page["items"].as_array().unwrap().len() <= 5);
        let response = respond(Ok(page.clone()));
        assert!(!response.is_error.unwrap_or(false));
        assert!(serde_json::to_vec(&response).unwrap().len() <= MAX_RESULT_BYTES);
        seen.extend(page["items"].as_array().unwrap().clone());
        let Some(cursor) = page["continuation"].as_str() else {
            break;
        };
        assert!(cache.read("other-org:namespace:query", cursor).is_err());
        page = cache.read("org:namespace:query", cursor).unwrap();
    }
    assert_eq!(seen, values);
}
#[test]
fn raw_queries_bound_wire_values_and_reject_collections_and_unordered_pages() {
    let safe =
        cypher::bounded_statement("MATCH (n:Entity) RETURN n.uuid AS uuid ORDER BY uuid", 20)
            .unwrap();
    assert!(safe.contains("left(`uuid`,1000)"));
    for query in [
        "MATCH (n) RETURN n",
        "MATCH (n) RETURN collect(n) AS nodes",
        "MATCH p=(n)-[*]->(m) RETURN p",
        "MATCH (n) RETURN n.uuid UNION MATCH (x) RETURN x.uuid",
    ] {
        assert!(cypher::bounded_statement(query, 0).is_err(), "{query}");
    }
    assert!(cypher::bounded_statement("MATCH (n) RETURN n.uuid", 20).is_err());
}
#[test]
fn text_only_clients_receive_records_not_a_structured_content_pointer() {
    let result = respond(Ok(
        json!({"items":[{"chain_id":"resource-1","name":"database"}],"truncated":false}),
    ));
    let serialized = serde_json::to_value(&result).unwrap();
    let text = serialized["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(text).unwrap(),
        serialized["structuredContent"]
    );
}

#[tokio::test]
async fn cancelled_read_holds_admission_until_cleanup_settles() {
    struct Slow {
        started: tokio::sync::Semaphore,
        cleaning: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }
    #[async_trait::async_trait]
    impl GraphExplorerBackend for Slow {
        async fn explore(&self, _: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
            self.started.add_permits(1);
            std::future::pending().await
        }
        async fn settle_cancelled_reads(&self) {
            self.cleaning.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
    }
    let backend = Arc::new(Slow {
        started: tokio::sync::Semaphore::new(0),
        cleaning: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let query = Arc::new(GraphQueryService::new(
        backend.clone(),
        Arc::new(SearchEngine::new(Arc::new(FakeSearch))),
        false,
    ));
    let server_query = query.clone();
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    let task = tokio::spawn(async move {
        McpGraph::local(
            server_query,
            None,
            Principal {
                id: "local".into(),
                org_id: "org-a".into(),
                namespace: Some("prod".into()),
                allow_raw_cypher_all_data: false,
                allow_saga_summaries: false,
            },
        )
        .serve(server_io)
        .await
        .unwrap()
        .waiting()
        .await
        .unwrap();
    });
    let (read, mut write) = tokio::io::split(client_io);
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"cancel-test","version":"1"}}});
    write
        .write_all(format!("{initialize}\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        reader.read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    write
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .unwrap();
    write.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"list_catalog\",\"arguments\":{}}}\n").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), backend.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(query.mcp_permits.available_permits(), 31);
    write.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":2}}\n").await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        backend.cleaning.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    assert_eq!(
        query.mcp_permits.available_permits(),
        31,
        "cleanup retains capacity"
    );
    backend.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while query.mcp_permits.available_permits() != 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
}

#[test]
fn existing_tools_keep_required_arguments_and_input_types() {
    let baseline: Vec<Value> =
        serde_json::from_str(include_str!("../../tests/fixtures/mcp_legacy_tools.json")).unwrap();
    let tools = McpGraph::tool_router().list_all();
    for old in baseline {
        let name = old["name"].as_str().unwrap();
        let current = tools
            .iter()
            .find(|t| t.name == name)
            .expect("existing tool retained");
        let schema = serde_json::to_value(&current.input_schema).unwrap();
        assert_eq!(
            schema["required"], old["inputSchema"]["required"],
            "required inputs changed for {name}"
        );
        for (key, value) in old["inputSchema"]["properties"].as_object().unwrap() {
            assert_eq!(
                schema["properties"][key], *value,
                "existing input schema changed: {name}.{key}"
            );
        }
    }
}

#[test]
fn investigation_tool_schemas_advertise_runtime_constraints() {
    let tools = McpGraph::tool_router().list_all();
    for (name, field, maximum) in [
        ("search", "limit", 10),
        ("get_changes", "limit", 50),
        ("get_entity", "value_limit", 2000),
    ] {
        let tool = tools.iter().find(|tool| tool.name == name).unwrap();
        let schema = serde_json::to_value(&tool.input_schema).unwrap();
        assert_eq!(schema["properties"][field]["minimum"], 1);
        assert_eq!(schema["properties"][field]["maximum"], maximum);
    }
    for name in ["get_subgraph", "find_paths"] {
        let tool = tools.iter().find(|tool| tool.name == name).unwrap();
        let schema = serde_json::to_value(&tool.input_schema).unwrap();
        assert_eq!(
            schema["properties"]["direction"]["enum"],
            json!(["in", "out", "both", null])
        );
    }
}

#[test]
fn overview_omits_values_but_selected_properties_are_exact() {
    let record = json!({"uuid":"version", "chain_id":"chain", "name":"database",
        "prop_region":"us-east-1", "prop_payload":"x".repeat(1900), "tag_owner":"private-team"});
    let overview = projection::overview(&record);
    assert_eq!(overview["property_count"], 2);
    assert!(overview.get("properties").is_none());
    assert!(overview.get("tags").is_none());
    assert!(!overview.to_string().contains("private-team"));
    let selected = projection::entity(&record, Some(&["region".into()])).unwrap();
    assert_eq!(selected["properties"], json!({"region":"us-east-1"}));
}

#[test]
fn brief_search_diagnostics_preserve_failures_and_dropped_candidates() {
    let compact = projection::search_diagnostics(json!([
        {"operation":"fulltext","status":"success","count":10,"duration_ms":5,"dropped_count":0,"failure":null},
        {"operation":"hydrate","status":"success","count":9,"duration_ms":5,"dropped_count":1,"failure":null},
        {"operation":"vectors","status":"failed","count":0,"duration_ms":5,"dropped_count":0,"failure":"timeout"}
    ]));
    assert_eq!(
        compact,
        json!([
            {"operation":"hydrate","status":"success","dropped_count":1},
            {"operation":"vectors","status":"failed","failure":"timeout"}
        ])
    );
}
