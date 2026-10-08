use super::*;
#[derive(Clone)]
pub struct Principal {
    pub id: String,
    pub org_id: String,
    pub namespace: Option<String>,
    pub allow_raw_cypher_all_data: bool,
    /// May request on-demand Saga summaries, which write to the graph and may spend model tokens.
    pub allow_saga_summaries: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    clients: Vec<Credential>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub(super) id: String,
    pub(super) token: String,
    pub(super) org_id: String,
    pub(super) namespace: Option<String>,
    #[serde(default)]
    pub(super) allow_raw_cypher_all_data: bool,
    #[serde(default)]
    #[serde(rename = "allow_thread_summaries", alias = "allow_saga_summaries")]
    pub(super) allow_saga_summaries: bool,
}

#[derive(Clone)]
pub struct AuthState(pub(super) Arc<Vec<Credential>>);

pub async fn verify_reader_account(
    backend: &Neo4jGraphBackend,
) -> Result<(), Box<dyn std::error::Error>> {
    let roles = backend.current_user_roles().await?;
    if !reader_roles_allowed(&roles) {
        return Err("MCP Cypher account must have only Neo4j's reader role".into());
    }
    let privileges = backend
        .execute_cancellable_read(
            "SHOW USER PRIVILEGES YIELD access,action RETURN access,action",
            &json!({}),
        )
        .await?;
    if privileges.is_empty()
        || privileges.iter().any(|row| {
            row.get("access").and_then(Value::as_str) == Some("GRANTED")
                && !matches!(
                    row.get("action").and_then(Value::as_str),
                    Some(
                        "access"
                            | "match"
                            | "read"
                            | "traverse"
                            | "execute"
                            | "execute_function"
                            | "execute_procedure"
                            | "show_index"
                            | "show_constraint"
                    )
                )
        })
    {
        return Err(
            "MCP reader has unsupported effective privileges; use a least-privilege reader account"
                .into(),
        );
    }
    Ok(())
}

pub(super) fn reader_roles_allowed(roles: &[String]) -> bool {
    roles.iter().any(|role| role == "reader")
        && roles
            .iter()
            .all(|role| role == "reader" || role == "PUBLIC")
}

impl AuthState {
    pub fn needs_cypher(&self) -> bool {
        self.0.iter().any(|client| client.allow_raw_cypher_all_data)
    }
}

pub fn load_credentials(path: impl AsRef<Path>) -> Result<AuthState, Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path.as_ref())?.permissions().mode() & 0o077 != 0 {
            return Err("MCP credential file must not be readable by group or others".into());
        }
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() > 64 * 1024 {
        return Err("MCP credential file is too large".into());
    }
    let file: CredentialFile = serde_json::from_slice(&bytes)?;
    if file.clients.is_empty() {
        return Err("MCP credential file has no clients".into());
    }
    let mut ids = std::collections::HashSet::new();
    let mut tokens = std::collections::HashSet::new();
    for client in &file.clients {
        if client.id.trim().is_empty()
            || !ids.insert(client.id.as_str())
            || client.token.len() < 32
            || !tokens.insert(client.token.as_str())
            || client.org_id.trim().is_empty()
            || client.org_id.len() > 256
            || client
                .namespace
                .as_deref()
                .is_some_and(|n| n.trim().is_empty() || n.len() > 256)
        {
            return Err("MCP credential file contains an invalid or duplicate client".into());
        }
    }
    Ok(AuthState(Arc::new(file.clients)))
}

async fn authenticate(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let credential = state
        .0
        .iter()
        .find(|client| {
            client.token.len() == bearer.len()
                && client.token.as_bytes().ct_eq(bearer.as_bytes()).into()
        })
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let principal = Principal {
        id: credential.id.clone(),
        org_id: credential.org_id.clone(),
        namespace: credential.namespace.clone(),
        allow_raw_cypher_all_data: credential.allow_raw_cypher_all_data,
        allow_saga_summaries: credential.allow_saga_summaries,
    };
    tracing::info!(principal = %principal.id, "MCP caller authenticated");
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

pub fn remote_router(
    query: Arc<GraphQueryService>,
    cypher: Option<Arc<Neo4jGraphBackend>>,
    auth: AuthState,
) -> Router {
    let allowed_hosts = std::env::var("KG_MCP_ALLOWED_HOSTS")
        .unwrap_or_else(|_| "localhost,127.0.0.1,::1".into())
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let allowed_origins = std::env::var("KG_MCP_ALLOWED_ORIGINS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_max_request_body_bytes(16 * 1024)
        .with_allowed_hosts(allowed_hosts)
        .with_allowed_origins(allowed_origins)
        .enforce_origin_validation();
    let service = StreamableHttpService::new(
        move || Ok(McpGraph::remote(query.clone(), cypher.clone())),
        LocalSessionManager::default().into(),
        config,
    );
    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(auth.clone(), authenticate))
}
