//! The single environment contract for live suites.

use std::fmt;

pub const URI: &str = "NEO4J_TEST_URI";
pub const USER: &str = "NEO4J_TEST_USER";
pub const PASSWORD: &str = "NEO4J_TEST_PASSWORD";
/// Set only by Taskfile targets that provision their own container.
pub const EXCLUSIVE_FLAG: &str = "KG_EXCLUSIVE_DB";

/// A missing or malformed environment prerequisite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvError {
    pub variable: &'static str,
    pub reason: &'static str,
}

impl fmt::Display for EnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.variable, self.reason)
    }
}

impl std::error::Error for EnvError {}

/// Connection values for the disposable test database.
#[derive(Clone)]
pub struct Neo4jTestEnv {
    pub uri: String,
    pub user: String,
    pub password: String,
    /// Whether this process may run whole-database operations.
    pub exclusive: bool,
}

impl fmt::Debug for Neo4jTestEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Neo4jTestEnv")
            .field("uri", &self.uri)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("exclusive", &self.exclusive)
            .finish()
    }
}

fn required(variable: &'static str) -> Result<String, EnvError> {
    std::env::var(variable)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or(EnvError {
            variable,
            reason: "must name the disposable test Neo4j (run via `task test:live:*`)",
        })
}

/// The shared disposable database. Fails, naming the variable, when unset.
pub fn neo4j() -> Result<Neo4jTestEnv, EnvError> {
    let uri = required(URI)?;
    let password = required(PASSWORD)?;
    let user = std::env::var(USER)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "neo4j".to_string());
    if uri.contains(":7687") && !uri.contains("127.0.0.1:17") {
        // The demo database lives on 7687; live suites never point at it.
        return Err(EnvError {
            variable: URI,
            reason: "points at port 7687; live suites run only against a disposable container",
        });
    }
    Ok(Neo4jTestEnv {
        uri,
        user,
        password,
        exclusive: std::env::var(EXCLUSIVE_FLAG).as_deref() == Ok("1"),
    })
}

/// A database this process may empty or reshape: `neo4j()` plus
/// `KG_EXCLUSIVE_DB=1`.
pub fn exclusive_neo4j() -> Result<Neo4jTestEnv, EnvError> {
    let env = neo4j()?;
    if !env.exclusive {
        return Err(EnvError {
            variable: EXCLUSIVE_FLAG,
            reason: "whole-database tests run only from a target that owns its container (sets KG_EXCLUSIVE_DB=1)",
        });
    }
    Ok(env)
}
