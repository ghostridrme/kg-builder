use kg_search::SearchEngine;
use kg_server::mcp::{self, McpGraph, Principal};
use kg_server::query::GraphQueryService;
use kg_server::{router, AppState, MIN_WRITE_TOKEN_LEN};
use kg_storage_neo4j::{Neo4jGraphBackend, Neo4jSettings};
use rmcp::{transport::stdio, ServiceExt};
use std::{env, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kg=info".into()),
        )
        .init();
    let mode = env::args().nth(1).unwrap_or_else(|| "http".into());
    if !["http", "mcp-stdio", "mcp-http"].contains(&mode.as_str()) {
        return Err("usage: kg-server [http|mcp-stdio|mcp-http]".into());
    }
    let org_id = if mode == "mcp-stdio" {
        env::var("KG_ORG_ID")?
    } else {
        env::var("KG_ORG_ID").unwrap_or_else(|_| "graph-demo".into())
    };
    let bind = env::var("KG_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());
    if mode == "http" {
        let address: std::net::SocketAddr = bind
            .parse()
            .map_err(|_| "KG_BIND must be an IP socket address")?;
        if !address.ip().is_loopback()
            && env::var("KG_HTTP_TRUSTED_GATEWAY").as_deref() != Ok("true")
        {
            return Err("Non-loopback HTTP requires KG_HTTP_TRUSTED_GATEWAY=true and an authenticating gateway restricting this organization's viewers; use MCP HTTP for credential-scoped access".into());
        }
    }
    kg_core::traits::graph_explorer::ExplorerRequest {
        org_id: org_id.clone(),
        namespace: None,
        as_of: None,
        limit: 1,
        offset: 0,
        query: kg_core::traits::graph_explorer::ExplorerQuery::Catalog,
    }
    .validate()?;
    let mut settings = Neo4jSettings::new(
        env::var("NEO4J_URI").unwrap_or_else(|_| "bolt://127.0.0.1:7687".into()),
        env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".into()),
        env::var("NEO4J_PASSWORD")?,
    );
    settings.database = env::var("NEO4J_DATABASE").ok();
    settings.max_pool_size = Some(16);
    let graph = Arc::new(Neo4jGraphBackend::connect(&settings).await?);
    graph.ensure_indexes().await?;
    let local_cypher = mode == "mcp-stdio"
        && env::var("KG_MCP_ALLOW_RAW_CYPHER_ALL_DATA").as_deref() == Ok("true");
    let cypher = if mode.starts_with("mcp-") {
        match (
            env::var("KG_MCP_CYPHER_USER"),
            env::var("KG_MCP_CYPHER_PASSWORD"),
        ) {
            (Ok(user), Ok(password)) => {
                if user == settings.username {
                    return Err("MCP Cypher requires a separate Neo4j reader account".into());
                }
                let mut reader = Neo4jSettings::new(settings.uri.clone(), user, password);
                reader.database = settings.database.clone();
                reader.max_pool_size = Some(2);
                reader.timeout = std::time::Duration::from_secs(5);
                reader.max_retries = 1;
                let backend = Arc::new(Neo4jGraphBackend::connect(&reader).await?);
                mcp::verify_reader_account(&backend).await?;
                Some(backend)
            }
            (Err(_), Err(_)) => None,
            _ => return Err("set both MCP Cypher reader credentials".into()),
        }
    } else {
        None
    };
    let search = SearchEngine::new(graph.clone());
    let semantic_available = false;
    let query = GraphQueryService::new(graph.clone(), Arc::new(search), semantic_available);
    let summary_mode: Option<()> = None;
    let embedder: Option<Arc<dyn kg_core::traits::EmbedBackend>> = None;
    let query = Arc::new(query);
    let repair_embedder: Arc<dyn kg_core::traits::EmbedBackend> = embedder
        .clone()
        .unwrap_or_else(|| Arc::new(kg_core::traits::EmbedDisabled));
    let rule_repair_engine = Arc::new(kg_server::rules::repair_engine(
        graph.clone(),
        repair_embedder,
    )?);
    let write_token = match env::var("KG_WRITE_TOKEN") {
        Ok(token) if token.len() >= MIN_WRITE_TOKEN_LEN => Some(Arc::<str>::from(token)),
        Ok(_) => {
            return Err(
                format!("KG_WRITE_TOKEN must be at least {MIN_WRITE_TOKEN_LEN} characters").into(),
            )
        }
        Err(_) => None,
    };
    if mode == "http" && summary_mode.is_some() && write_token.is_none() {
        return Err("HTTP Thread summaries require KG_WRITE_TOKEN".into());
    }
    match mode.as_str() {
        "mcp-stdio" => {
            let principal = Principal {
                id: "local".into(),
                org_id,
                namespace: env::var("KG_NAMESPACE").ok(),
                allow_raw_cypher_all_data: local_cypher,
                allow_saga_summaries: env::var("KG_MCP_ALLOW_THREAD_SUMMARIES")
                    .or_else(|_| env::var("KG_MCP_ALLOW_SAGA_SUMMARIES"))
                    .as_deref()
                    == Ok("true"),
            };
            if local_cypher && cypher.is_none() {
                return Err("raw Cypher requires a separate Neo4j reader account".into());
            }
            let server = McpGraph::local(query, cypher, principal)
                .serve(stdio())
                .await?;
            server.waiting().await?;
        }
        "mcp-http" => {
            let auth_file = env::var("KG_MCP_AUTH_FILE")?;
            let auth = mcp::load_credentials(&auth_file)?;
            if auth.needs_cypher() && cypher.is_none() {
                return Err("raw Cypher requires a separate Neo4j reader account".into());
            }
            let bind = env::var("KG_MCP_BIND").unwrap_or_else(|_| "127.0.0.1:8090".into());
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            tracing::info!(%bind, semantic_available, "graph MCP ready");
            axum::serve(listener, mcp::remote_router(query, cypher, auth))
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
        }
        _ => {
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            tracing::info!(%bind, %org_id, semantic_available, "graph API ready");
            axum::serve(
                listener,
                router(AppState {
                    query,
                    org_id,
                    write_token,
                    rules: Some(graph.clone()),
                    rule_repair_engine: Some(rule_repair_engine),
                }),
            )
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        }
    }
    Ok(())
}
