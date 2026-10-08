//! Connection settings applied at construction. Every accepted value reaches
//! the driver or the call options; readiness is verified before the backend
//! is handed out.

use std::sync::Arc;
use std::time::Duration;

use neo4rs::{ConfigBuilder, Graph};

use kg_core::errors::BackendError;
use kg_core::traits::GraphBackend;

use crate::driver::{Neo4jGraphBackend, Neo4jOptions};

/// Validated Neo4j connection settings.
#[derive(Clone)]
pub struct Neo4jSettings {
    /// Bolt URI, e.g. `bolt://localhost:7687` or `bolt+s://host`.
    pub uri: String,
    pub username: String,
    pub password: String,
    /// Target database; the server default when None.
    pub database: Option<String>,
    /// Connection pool cap; the driver default when None.
    pub max_pool_size: Option<usize>,
    /// Client deadline per phase. The server transaction lifetime covers three phases.
    pub timeout: Duration,
    /// Attempts for transient failures, at least 1.
    pub max_retries: u32,
    /// Receipt checks after a commit request whose acknowledgement was
    /// lost, at least 1; none found reports an unknown outcome.
    pub commit_verification_attempts: u32,
}

impl std::fmt::Debug for Neo4jSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4jSettings")
            .field("uri", &"<redacted>")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("database", &self.database)
            .field("max_pool_size", &self.max_pool_size)
            .field("timeout", &self.timeout)
            .field("max_retries", &self.max_retries)
            .field(
                "commit_verification_attempts",
                &self.commit_verification_attempts,
            )
            .finish()
    }
}

impl Neo4jSettings {
    /// Apply every declared connection option without contacting the server.
    pub fn from_config(config: &kg_core::config::GraphBackendConfig) -> Result<Self, BackendError> {
        use secrecy::ExposeSecret;
        let kg_core::config::GraphBackendConfig::Neo4j {
            uri,
            username,
            password,
            database,
            max_pool_size,
            timeout_ms,
            max_retries,
            commit_verification_attempts,
        } = config;
        let settings = Self {
            uri: uri.clone(),
            username: username.clone(),
            password: password.expose_secret().to_owned(),
            database: database.clone(),
            max_pool_size: *max_pool_size,
            timeout: Duration::from_millis(*timeout_ms),
            max_retries: *max_retries,
            commit_verification_attempts: *commit_verification_attempts,
        };
        settings.validate()?;
        Ok(settings)
    }

    /// Settings with the default deadline (30s), retry count (3), and
    /// receipt checks (5).
    pub fn new(
        uri: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let defaults = Neo4jOptions::default();
        Self {
            uri: uri.into(),
            username: username.into(),
            password: password.into(),
            database: None,
            max_pool_size: None,
            timeout: defaults.timeout,
            max_retries: defaults.max_retries,
            commit_verification_attempts: defaults.commit_verification_attempts,
        }
    }

    /// Reject values the driver would ignore or misapply.
    pub fn validate(&self) -> Result<(), BackendError> {
        let invalid =
            |message: String| BackendError::Connection(format!("Neo4j settings: {message}"));
        validate_uri(&self.uri)?;
        if self.username.trim().is_empty() {
            return Err(invalid("username must not be blank".into()));
        }
        if self
            .database
            .as_deref()
            .is_some_and(|db| db.trim().is_empty())
        {
            return Err(invalid("database must not be blank when set".into()));
        }
        if self.max_pool_size == Some(0) {
            return Err(invalid("max_pool_size must be at least 1".into()));
        }
        if self.timeout.is_zero() {
            return Err(invalid("timeout must be positive".into()));
        }
        if self.max_retries == 0 {
            return Err(invalid("max_retries must be at least 1".into()));
        }
        if self.commit_verification_attempts == 0 {
            return Err(invalid(
                "commit_verification_attempts must be at least 1".into(),
            ));
        }
        Ok(())
    }

    fn options(&self) -> Neo4jOptions {
        Neo4jOptions {
            timeout: self.timeout,
            max_retries: self.max_retries,
            commit_verification_attempts: self.commit_verification_attempts,
            ..Neo4jOptions::default()
        }
    }
}

impl Neo4jGraphBackend {
    /// Connect with every setting applied and confirm the server answers
    /// within the deadline. Nothing is written.
    pub async fn connect(settings: &Neo4jSettings) -> Result<Self, BackendError> {
        settings.validate()?;
        let mut builder = ConfigBuilder::new()
            .uri(settings.uri.trim())
            .user(settings.username.as_str())
            .password(settings.password.as_str());
        if let Some(database) = &settings.database {
            builder = builder.db(database.as_str());
        }
        let mut control_builder = ConfigBuilder::new()
            .uri(settings.uri.trim())
            .user(settings.username.as_str())
            .password(settings.password.as_str())
            .max_connections(2);
        if let Some(database) = &settings.database {
            control_builder = control_builder.db(database.as_str());
        }
        let control_config = control_builder.build().map_err(|e| {
            BackendError::Connection(format!("Neo4j cancellation settings rejected: {e}"))
        })?;
        if let Some(size) = settings.max_pool_size {
            builder = builder.max_connections(size);
        }
        let config = builder
            .build()
            .map_err(|e| BackendError::Connection(format!("Neo4j settings rejected: {e}")))?;
        let graph = tokio::time::timeout(settings.timeout, Graph::connect(config))
            .await
            .map_err(|_| BackendError::Timeout(settings.timeout.as_millis() as u64))?
            .map_err(|e| BackendError::Connection(format!("Neo4j connection failed: {e}")))?;
        let cancellation_graph =
            tokio::time::timeout(settings.timeout, Graph::connect(control_config))
                .await
                .map_err(|_| BackendError::Timeout(settings.timeout.as_millis() as u64))?
                .map_err(|e| {
                    BackendError::Connection(format!("Neo4j cancellation connection failed: {e}"))
                })?;
        let backend = Self::from_graph_with_options(
            Arc::new(graph),
            Arc::new(cancellation_graph),
            settings.options(),
        );
        backend.health().await?;
        Ok(backend)
    }

    /// Install every constraint and index ingestion and search require and
    /// wait until they are online. Call once at startup; ingestion must not
    /// begin before this succeeds.
    pub async fn prepare(&self) -> Result<(), BackendError> {
        self.ensure_indexes().await
    }
}

pub(crate) fn validate_uri(uri: &str) -> Result<(), BackendError> {
    let uri = uri.trim();
    let valid_scheme = ["bolt://", "bolt+s://", "bolt+ssc://"]
        .iter()
        .any(|prefix| uri.starts_with(prefix));
    let authority = uri
        .split_once("://")
        .map(|(_, rest)| rest.split('/').next().unwrap_or_default())
        .unwrap_or_default();
    if !valid_scheme || authority.is_empty() || authority.contains('@') || uri.contains(['?', '#'])
    {
        return Err(BackendError::Connection("Neo4j URI must use bolt://, bolt+s:// or bolt+ssc:// with a host and no embedded credentials, query or fragment; routing is not supported".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_reject_values_the_driver_would_ignore() {
        let ok = Neo4jSettings::new("bolt://localhost:7687", "neo4j", "secret");
        ok.validate().unwrap();
        assert!(!format!("{ok:?}").contains("secret"));
        for (label, broken) in [
            (
                "scheme",
                Neo4jSettings::new("http://localhost", "neo4j", "p"),
            ),
            ("empty host", Neo4jSettings::new("bolt://", "neo4j", "p")),
            ("username", Neo4jSettings::new("bolt://h", " ", "p")),
            (
                "database",
                Neo4jSettings {
                    database: Some(" ".into()),
                    ..ok.clone()
                },
            ),
            (
                "pool",
                Neo4jSettings {
                    max_pool_size: Some(0),
                    ..ok.clone()
                },
            ),
            (
                "timeout",
                Neo4jSettings {
                    timeout: Duration::ZERO,
                    ..ok.clone()
                },
            ),
            (
                "retries",
                Neo4jSettings {
                    max_retries: 0,
                    ..ok.clone()
                },
            ),
            (
                "verification",
                Neo4jSettings {
                    commit_verification_attempts: 0,
                    ..ok.clone()
                },
            ),
        ] {
            assert!(broken.validate().is_err(), "{label}");
        }
        let options = Neo4jSettings {
            timeout: Duration::from_secs(5),
            max_retries: 2,
            commit_verification_attempts: 7,
            ..ok
        }
        .options();
        assert_eq!(options.timeout, Duration::from_secs(5));
        assert_eq!(options.max_retries, 2);
        assert_eq!(options.commit_verification_attempts, 7);
    }

    // ---- merged from `mod uri_tests`

    #[test]
    fn unsupported_routing_and_secret_uris_are_rejected_without_echoing() {
        for uri in [
            "neo4j://host",
            "neo4j+s://host",
            "bolt://user:secret@host",
            "bolt://host?secret",
            "invalid-secret",
        ] {
            let settings = Neo4jSettings::new(uri, "user", "password-secret");
            assert!(settings.validate().is_err());
            assert!(!settings
                .validate()
                .unwrap_err()
                .to_string()
                .contains("secret"));
            assert!(!format!("{settings:?}").contains("secret"));
        }
    }
}
