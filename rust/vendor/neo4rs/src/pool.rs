use std::time::Duration;

use crate::{
    auth::ClientCertificate,
    config::Config,
    connection::{Connection, ConnectionInfo},
    errors::{Error, Result},
};
use async_trait::async_trait;
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use log::info;

pub type ConnectionPool = deadpool::managed::Pool<ConnectionManager>;
type PooledConnection = deadpool::managed::Object<ConnectionManager>;

/// A lease closes an unusable connection immediately, rather than parking it
/// in deadpool until another checkout happens to trigger recycling.
pub struct ManagedConnection {
    inner: Option<PooledConnection>,
    discard_on_drop: bool,
}
impl From<PooledConnection> for ManagedConnection {
    fn from(inner: PooledConnection) -> Self {
        Self {
            inner: Some(inner),
            discard_on_drop: false,
        }
    }
}
impl ManagedConnection {
    pub(crate) fn discard_on_drop(&mut self, discard: bool) {
        self.discard_on_drop = discard;
    }
}
impl std::ops::Deref for ManagedConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.inner.as_ref().expect("live connection lease")
    }
}
impl std::ops::DerefMut for ManagedConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.inner.as_mut().expect("live connection lease")
    }
}
impl Drop for ManagedConnection {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            if self.discard_on_drop || inner.response_pending() {
                drop(PooledConnection::take(inner));
            }
        }
    }
}

pub struct ConnectionManager {
    info: ConnectionInfo,
    backoff: ExponentialBackoff,
}

impl ConnectionManager {
    pub fn new(
        uri: &str,
        user: &str,
        password: &str,
        client_certificate: Option<&ClientCertificate>,
    ) -> Result<Self> {
        let info = ConnectionInfo::new(uri, user, password, client_certificate)?;
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(1))
            .with_randomization_factor(0.42)
            .with_multiplier(2.0)
            .with_max_elapsed_time(Some(Duration::from_secs(60)))
            .build();
        Ok(ConnectionManager { info, backoff })
    }

    pub fn backoff(&self) -> ExponentialBackoff {
        self.backoff.clone()
    }
}

#[async_trait]
impl deadpool::managed::Manager for ConnectionManager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Connection, Error> {
        info!("creating new connection...");
        Connection::new(&self.info).await
    }

    async fn recycle(&self, conn: &mut Connection) -> deadpool::managed::RecycleResult<Error> {
        Ok(conn.reset().await?)
    }
}

pub async fn create_pool(config: &Config) -> Result<ConnectionPool, Error> {
    let mgr = ConnectionManager::new(
        &config.uri,
        &config.user,
        &config.password,
        config.client_certificate.as_ref(),
    )?;
    info!(
        "creating connection pool with max size {}",
        config.max_connections
    );
    Ok(ConnectionPool::builder(mgr)
        .max_size(config.max_connections)
        .build()?)
}
