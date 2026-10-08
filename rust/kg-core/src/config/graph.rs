use secrecy::SecretString;
use serde::Deserialize;

/// Graph settings. Debug output omits credentials and connection details.
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphBackendConfig {
    Neo4j {
        /// Bolt URI (e.g. `bolt://localhost:7688`).
        uri: String,
        username: String,
        password: SecretString,
        /// Target database name; the server default when None.
        database: Option<String>,
        /// Connection-pool size cap; driver default when None.
        max_pool_size: Option<usize>,
        /// Client deadline per transaction phase, in milliseconds.
        #[serde(default = "default_timeout_ms")]
        timeout_ms: u64,
        #[serde(default = "default_retries")]
        max_retries: u32,
        #[serde(default = "default_verifications")]
        commit_verification_attempts: u32,
    },
}

impl std::fmt::Debug for GraphBackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Neo4j { max_pool_size, .. } => f
                .debug_struct("Neo4j")
                .field("max_pool_size", max_pool_size)
                .finish_non_exhaustive(),
        }
    }
}

fn default_timeout_ms() -> u64 {
    30_000
}
fn default_retries() -> u32 {
    3
}
fn default_verifications() -> u32 {
    5
}
