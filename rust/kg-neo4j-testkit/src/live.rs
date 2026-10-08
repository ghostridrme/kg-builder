//! A connected disposable database with an organization of its own.

use std::fmt;
use std::sync::Arc;

use kg_core::errors::BackendError;
use kg_core::traits::GraphBackend;
use kg_storage_neo4j::{Neo4jGraphBackend, Neo4jOptions};
use serde_json::json;
use uuid::Uuid;

use crate::env::{self, EnvError, Neo4jTestEnv};

/// Why a live graph could not be opened or cleaned.
#[derive(Debug)]
pub enum TestkitError {
    Env(EnvError),
    Backend(BackendError),
    /// `wipe()` on a graph opened without `KG_EXCLUSIVE_DB=1`.
    NotExclusive,
}

impl fmt::Display for TestkitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestkitError::Env(e) => write!(f, "{e}"),
            TestkitError::Backend(e) => write!(f, "Neo4j: {e}"),
            TestkitError::NotExclusive => write!(
                f,
                "wipe() needs LiveGraph::open_exclusive() and {}=1",
                env::EXCLUSIVE_FLAG
            ),
        }
    }
}

impl std::error::Error for TestkitError {}

impl From<EnvError> for TestkitError {
    fn from(e: EnvError) -> Self {
        TestkitError::Env(e)
    }
}

impl From<BackendError> for TestkitError {
    fn from(e: BackendError) -> Self {
        TestkitError::Backend(e)
    }
}

/// Connect to the shared disposable database and hand back the owned adapter
/// (indexes are the caller's business, as before). For the per-file `graph()`
/// helpers that predate [`LiveGraph`].
/// The connected adapter with its indexes ensured; panics with the environment
/// error so an unselected prerequisite names the variable. For suites that
/// isolate by organization and do not need `LiveGraph` cleanup.
pub async fn indexed_graph() -> Neo4jGraphBackend {
    let graph = connect().await.unwrap_or_else(|e| panic!("{e}"));
    graph.ensure_indexes().await.expect("ensure indexes");
    graph
}

pub async fn connect() -> Result<Neo4jGraphBackend, TestkitError> {
    let env = env::neo4j()?;
    Ok(Neo4jGraphBackend::new(&env.uri, &env.user, &env.password).await?)
}

/// The disposable Neo4j, connected and indexed, with a unique `org_id` for
/// this test. Call [`LiveGraph::cleanup`] at the end of the test; it is
/// explicit and awaited so a failing test leaves its evidence behind and a
/// passing test leaves nothing.
pub struct LiveGraph {
    backend: Arc<Neo4jGraphBackend>,
    env: Neo4jTestEnv,
    org: String,
}

impl fmt::Debug for LiveGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveGraph")
            .field("org", &self.org)
            .field("env", &self.env)
            .finish()
    }
}

impl LiveGraph {
    /// Connect to the shared disposable database with a fresh organization.
    pub async fn open() -> Result<Self, TestkitError> {
        Self::open_named("test").await
    }

    /// Like [`open`](Self::open) with a readable organization prefix.
    pub async fn open_named(prefix: &str) -> Result<Self, TestkitError> {
        let env = env::neo4j()?;
        Self::connect(env, prefix, Neo4jOptions::default()).await
    }

    /// Connect with driver options (deadlines, fault injection).
    pub async fn open_with_options(
        prefix: &str,
        options: Neo4jOptions,
    ) -> Result<Self, TestkitError> {
        let env = env::neo4j()?;
        Self::connect(env, prefix, options).await
    }

    /// Connect to a database this process owns outright: requires
    /// `KG_EXCLUSIVE_DB=1`, which only container-owning targets set.
    pub async fn open_exclusive() -> Result<Self, TestkitError> {
        let env = env::exclusive_neo4j()?;
        Self::connect(env, "exclusive", Neo4jOptions::default()).await
    }

    async fn connect(
        env: Neo4jTestEnv,
        prefix: &str,
        options: Neo4jOptions,
    ) -> Result<Self, TestkitError> {
        let backend =
            Neo4jGraphBackend::with_options(&env.uri, &env.user, &env.password, options).await?;
        backend.ensure_indexes().await?;
        Ok(Self {
            backend: Arc::new(backend),
            env,
            org: format!("{prefix}-{}", Uuid::new_v4()),
        })
    }

    /// The connected adapter.
    pub fn backend(&self) -> Arc<Neo4jGraphBackend> {
        self.backend.clone()
    }

    /// The adapter as the engine sees it.
    pub fn graph(&self) -> Arc<dyn GraphBackend> {
        self.backend.clone()
    }

    /// This test's organization; every write should carry it.
    pub fn org(&self) -> &str {
        &self.org
    }

    /// The connection values, for tests that spawn processes or open a
    /// second adapter with different options.
    pub fn env(&self) -> &Neo4jTestEnv {
        &self.env
    }

    /// Whether whole-database operations are permitted on this connection.
    pub fn is_exclusive(&self) -> bool {
        self.env.exclusive
    }

    /// Remove everything written under this test's organization. Explicit and
    /// awaited; nothing else is touched.
    pub async fn cleanup(self) -> Result<(), TestkitError> {
        self.cleanup_org(&self.org).await
    }

    /// Remove everything written under `org` (for tests that create extra
    /// organizations of their own).
    pub async fn cleanup_org(&self, org: &str) -> Result<(), TestkitError> {
        self.delete_in_batches(
            "MATCH (n {org_id: $org}) WITH n LIMIT $batch DETACH DELETE n",
            json!({ "org": org, "batch": DELETE_BATCH }),
            "MATCH (n {org_id: $org}) RETURN count(n) AS remaining",
            json!({ "org": org }),
        )
        .await
    }

    /// Empty the whole database. Only on a connection opened with
    /// [`open_exclusive`](Self::open_exclusive).
    pub async fn wipe(&self) -> Result<(), TestkitError> {
        if !self.env.exclusive {
            return Err(TestkitError::NotExclusive);
        }
        self.delete_in_batches(
            "MATCH (n) WITH n LIMIT $batch DETACH DELETE n",
            json!({ "batch": DELETE_BATCH }),
            "MATCH (n) RETURN count(n) AS remaining",
            json!({}),
        )
        .await
    }

    /// One `DETACH DELETE` over a large graph exceeds the container's
    /// transaction memory pool (seen after a load-tier run left 300k nodes
    /// behind), so deletes run in bounded batches until nothing remains.
    async fn delete_in_batches(
        &self,
        delete: &str,
        delete_params: serde_json::Value,
        count: &str,
        count_params: serde_json::Value,
    ) -> Result<(), TestkitError> {
        loop {
            self.backend.execute_write(delete, &delete_params).await?;
            let rows = self.backend.execute_read(count, &count_params).await?;
            let remaining = rows
                .first()
                .and_then(|row| row.get("remaining"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            if remaining == 0 {
                return Ok(());
            }
        }
    }
}

const DELETE_BATCH: u64 = 20_000;
