use crate::{
    auth::ClientCertificate,
    errors::{Error, Result},
    messages::{BoltRequest, BoltResponse, HelloBuilder},
    unexpected,
    version::Version,
    BoltMap,
};
use bytes::{Bytes, BytesMut};
use log::warn;
use std::fs::File;
use std::io::BufReader;
use std::{mem, sync::Arc};
use stream::ConnectionStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::TcpStream,
};
use tokio_rustls::rustls::pki_types::{IpAddr, Ipv4Addr, Ipv6Addr, ServerName};
use tokio_rustls::{
    rustls::{ClientConfig, RootCertStore},
    TlsConnector,
};
use url::{Host, Url};

const MAX_CHUNK_SIZE: usize = 65_535 - mem::size_of::<u16>();

#[derive(Debug)]
pub struct Connection {
    version: Version,
    stream: BufStream<ConnectionStream>,
    // An interrupted send or response must never be recycled into another query.
    response_pending: bool,
}

impl Connection {
    pub(crate) fn response_pending(&self) -> bool {
        self.response_pending
    }

    pub(crate) fn new(
        info: &ConnectionInfo,
    ) -> impl std::future::Future<Output = Result<Connection>> {
        // we do this setup outside of the async block so that the returned future
        // does not borrow the info struct and can be Send
        let hello_builder =
            HelloBuilder::new(&*info.user, &*info.password).with_routing(info.routing.clone());
        let encryption = info.encryption.clone();
        let host = info.host.clone();
        let port = info.port;
        async move {
            let stream = match host {
                Host::Domain(domain) => TcpStream::connect((&*domain, port)).await?,
                Host::Ipv4(ip) => TcpStream::connect((ip, port)).await?,
                Host::Ipv6(ip) => TcpStream::connect((ip, port)).await?,
            };

            let stream: ConnectionStream = match encryption {
                Some((connector, domain)) => connector.connect(domain, stream).await?.into(),
                None => stream.into(),
            };
            Self::init(hello_builder, stream).await
        }
    }

    async fn init(hello_builder: HelloBuilder, stream: ConnectionStream) -> Result<Connection> {
        let mut stream = BufStream::new(stream);
        stream.write_all(&[0x60, 0x60, 0xB0, 0x17]).await?;
        stream.write_all(&Version::supported_versions()).await?;
        stream.flush().await?;
        let mut response = [0, 0, 0, 0];
        stream.read_exact(&mut response).await?;
        let version = Version::parse(response)?;
        let mut connection = Connection {
            version,
            stream,
            response_pending: false,
        };
        let hello = hello_builder.with_version(version).build();
        match connection.send_recv(hello).await? {
            BoltResponse::Success(_msg) => Ok(connection),
            BoltResponse::Failure(msg) => {
                Err(Error::AuthenticationError(msg.get("message").unwrap()))
            }

            msg => Err(unexpected(msg, "HELLO")),
        }
    }

    pub async fn reset(&mut self) -> Result<()> {
        if self.response_pending {
            return Err(Error::ConnectionError);
        }
        match self.send_recv(BoltRequest::reset()).await? {
            BoltResponse::Success(_) => Ok(()),
            BoltResponse::Failure(failure) => Err(Error::Neo4j(failure.into_error())),
            msg => Err(unexpected(msg, "RESET")),
        }
    }

    pub async fn send_recv(&mut self, message: BoltRequest) -> Result<BoltResponse> {
        self.send(message).await?;
        self.recv().await
    }

    pub async fn send(&mut self, message: BoltRequest) -> Result<()> {
        // This driver consumes each message's terminal response before sending
        // another. After interrupted I/O, even ROLLBACK must not consume the
        // previous message's response and make a dirty connection look reusable.
        if self.response_pending {
            return Err(Error::ConnectionError);
        }
        let end_marker: [u8; 2] = [0, 0];
        let bytes: Bytes = message.into_bytes(self.version)?;
        self.response_pending = true;
        for c in bytes.chunks(MAX_CHUNK_SIZE) {
            self.stream.write_u16(c.len() as u16).await?;
            self.stream.write_all(c).await?;
        }
        self.stream.write_all(&end_marker).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<BoltResponse> {
        let mut bytes = BytesMut::new();
        let mut chunk_size = 0;
        while chunk_size == 0 {
            chunk_size = self.read_chunk_size().await?;
        }

        while chunk_size > 0 {
            self.read_chunk(chunk_size, &mut bytes).await?;
            chunk_size = self.read_chunk_size().await?;
        }

        let response = BoltResponse::parse(self.version, bytes.freeze())?;
        if !matches!(response, BoltResponse::Record(_)) {
            self.response_pending = false;
        }
        Ok(response)
    }

    async fn read_chunk_size(&mut self) -> Result<usize> {
        Ok(usize::from(self.stream.read_u16().await?))
    }

    async fn read_chunk(&mut self, chunk_size: usize, buf: &mut BytesMut) -> Result<()> {
        // Ensure the buffer has enough capacity
        if buf.capacity() < (buf.len() + chunk_size) {
            buf.reserve(chunk_size);
        }
        let mut remaining = chunk_size;
        while remaining > 0 {
            remaining -= (&mut self.stream)
                .take(remaining as u64)
                .read_buf(buf)
                .await?;
        }
        Ok(())
    }
}

pub(crate) struct ConnectionInfo {
    user: Arc<str>,
    password: Arc<str>,
    host: Host<Arc<str>>,
    port: u16,
    routing: Routing,
    encryption: Option<(TlsConnector, ServerName<'static>)>,
}

impl std::fmt::Debug for ConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionInfo")
            .field("user", &self.user)
            .field("password", &"***")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("routing", &self.routing)
            .field("encryption", &self.encryption.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Routing {
    No,
    Yes(BoltMap),
}

impl From<Routing> for Option<BoltMap> {
    fn from(routing: Routing) -> Self {
        match routing {
            Routing::No => None,
            Routing::Yes(routing) => Some(routing),
        }
    }
}

impl ConnectionInfo {
    pub(crate) fn new(
        uri: &str,
        user: &str,
        password: &str,
        client_certificate: Option<&ClientCertificate>,
    ) -> Result<Self> {
        let mut url = NeoUrl::parse(uri)?;

        let (routing, encryption) = match url.scheme() {
            "bolt" | "" => (false, false),
            "bolt+s" => (false, true),
            "bolt+ssc" => (false, true),
            "neo4j" => (true, false),
            "neo4j+s" => (true, true),
            "neo4j+ssc" => (true, true),
            otherwise => return Err(Error::UnsupportedScheme(otherwise.to_owned())),
        };

        let encryption = encryption
            .then(|| Self::tls_connector(url.host(), client_certificate))
            .transpose()?;

        let routing = if routing {
            log::warn!(concat!(
                "This driver does not yet implement client-side routing. ",
                "It is possible that operations against a cluster (such as Aura) will fail."
            ));
            Routing::Yes(url.routing_context())
        } else {
            Routing::No
        };

        url.warn_on_unexpected_components();

        let host = match url.host() {
            Host::Domain(s) => Host::Domain(Arc::<str>::from(s)),
            Host::Ipv4(d) => Host::Ipv4(d),
            Host::Ipv6(d) => Host::Ipv6(d),
        };

        Ok(Self {
            user: user.into(),
            password: password.into(),
            host,
            port: url.port(),
            encryption,
            routing,
        })
    }

    fn tls_connector(
        host: Host<&str>,
        certificate: Option<&ClientCertificate>,
    ) -> Result<(TlsConnector, ServerName<'static>)> {
        let mut root_cert_store = RootCertStore::empty();
        match rustls_native_certs::load_native_certs() {
            Ok(certs) => {
                root_cert_store.add_parsable_certificates(certs);
            }
            Err(e) => {
                warn!("Failed to load native certificates: {e}");
            }
        }

        if let Some(certificate) = certificate {
            let cert_file = File::open(&certificate.cert_file)?;
            let mut reader = BufReader::new(cert_file);
            let certs = rustls_pemfile::certs(&mut reader).flatten();
            root_cert_store.add_parsable_certificates(certs);
        }

        let config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let config = Arc::new(config);
        let connector = TlsConnector::from(config);

        let domain = match host {
            Host::Domain(domain) => ServerName::try_from(domain.to_owned())
                .map_err(|_| Error::InvalidDnsName(domain.to_owned()))?,
            Host::Ipv4(ip) => ServerName::IpAddress(IpAddr::V4(Ipv4Addr::from(ip))),
            Host::Ipv6(ip) => ServerName::IpAddress(IpAddr::V6(Ipv6Addr::from(ip))),
        };

        Ok((connector, domain))
    }
}

struct NeoUrl(Url);

impl NeoUrl {
    fn parse(uri: &str) -> Result<Self> {
        let url = match Url::parse(uri) {
            Ok(url) if url.has_host() => url,
            // missing scheme
            Ok(_) | Err(url::ParseError::RelativeUrlWithoutBase) => {
                Url::parse(&format!("bolt://{}", uri))?
            }
            Err(err) => return Err(Error::UrlParseError(err)),
        };

        Ok(Self(url))
    }

    fn scheme(&self) -> &str {
        self.0.scheme()
    }

    fn host(&self) -> Host<&str> {
        self.0.host().unwrap()
    }

    fn port(&self) -> u16 {
        self.0.port().unwrap_or(7687)
    }

    fn routing_context(&mut self) -> BoltMap {
        BoltMap::new()
    }

    fn warn_on_unexpected_components(&self) {
        if !self.0.username().is_empty() || self.0.password().is_some() {
            log::warn!(concat!(
                "URI contained auth credentials, which are ignored.",
                "Credentials are passed outside of the URI"
            ));
        }
        if !matches!(self.0.path(), "" | "/") {
            log::warn!("URI contained a path, which is ignored.");
        }

        if self.0.query().is_some() {
            log::warn!(concat!(
                "This client does not yet support client-side routing.",
                "The routing context passed as a query to the URI is ignored."
            ));
        }

        if self.0.fragment().is_some() {
            log::warn!("URI contained a fragment, which is ignored.");
        }
    }
}

mod stream {
    use pin_project_lite::pin_project;
    use tokio::{
        io::{AsyncRead, AsyncWrite},
        net::TcpStream,
    };
    use tokio_rustls::client::TlsStream;

    pin_project! {
        #[project = ConnectionStreamProj]
        #[derive(Debug)]
        pub(super) enum ConnectionStream {
            Unencrypted { #[pin] stream: TcpStream },
            Encrypted { #[pin] stream: TlsStream<TcpStream> },
        }
    }

    impl From<TcpStream> for ConnectionStream {
        fn from(stream: TcpStream) -> Self {
            ConnectionStream::Unencrypted { stream }
        }
    }

    impl From<TlsStream<TcpStream>> for ConnectionStream {
        fn from(stream: TlsStream<TcpStream>) -> Self {
            ConnectionStream::Encrypted { stream }
        }
    }

    impl AsyncRead for ConnectionStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            match self.project() {
                ConnectionStreamProj::Unencrypted { stream } => stream.poll_read(cx, buf),
                ConnectionStreamProj::Encrypted { stream } => stream.poll_read(cx, buf),
            }
        }
    }

    impl AsyncWrite for ConnectionStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            match self.project() {
                ConnectionStreamProj::Unencrypted { stream } => stream.poll_write(cx, buf),
                ConnectionStreamProj::Encrypted { stream } => stream.poll_write(cx, buf),
            }
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            match self.project() {
                ConnectionStreamProj::Unencrypted { stream } => stream.poll_flush(cx),
                ConnectionStreamProj::Encrypted { stream } => stream.poll_flush(cx),
            }
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            match self.project() {
                ConnectionStreamProj::Unencrypted { stream } => stream.poll_shutdown(cx),
                ConnectionStreamProj::Encrypted { stream } => stream.poll_shutdown(cx),
            }
        }

        fn poll_write_vectored(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            match self.project() {
                ConnectionStreamProj::Unencrypted { stream } => {
                    stream.poll_write_vectored(cx, bufs)
                }
                ConnectionStreamProj::Encrypted { stream } => stream.poll_write_vectored(cx, bufs),
            }
        }

        fn is_write_vectored(&self) -> bool {
            match self {
                ConnectionStream::Unencrypted { stream } => stream.is_write_vectored(),
                ConnectionStream::Encrypted { stream } => stream.is_write_vectored(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use url::Host;

    use super::NeoUrl;

    async fn wire_pair() -> (super::Connection, tokio::net::TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, server) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        (
            super::Connection {
                version: crate::Version::V4_1,
                stream: tokio::io::BufStream::new(client.unwrap().into()),
                response_pending: false,
            },
            server.unwrap().0,
        )
    }

    async fn consume_request(server: &mut tokio::net::TcpStream) {
        use tokio::io::AsyncReadExt;
        loop {
            let size = server.read_u16().await.unwrap();
            if size == 0 {
                return;
            }
            server
                .read_exact(&mut vec![0; usize::from(size)])
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn interrupted_response_rejects_rollback_without_consuming_stale_success() {
        use super::{BoltRequest, Error};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{timeout, Duration};
        let (mut connection, mut server) = wire_pair().await;
        connection.send(BoltRequest::pull(1, -1)).await.unwrap();
        consume_request(&mut server).await;
        assert!(timeout(Duration::from_millis(10), connection.recv())
            .await
            .is_err());
        // The old PULL completes only after its caller has timed out.
        server
            .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
            .await
            .unwrap();
        assert!(matches!(
            connection.send_recv(BoltRequest::rollback()).await,
            Err(Error::ConnectionError)
        ));
        assert!(connection.response_pending);
        assert!(matches!(
            connection.reset().await,
            Err(Error::ConnectionError)
        ));
        // Neither rollback nor pool recycling wrote another request.
        assert!(timeout(Duration::from_millis(10), server.read_u8())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn completed_responses_allow_the_next_request() {
        use super::{BoltRequest, BoltResponse};
        use tokio::io::AsyncWriteExt;
        let (mut connection, mut server) = wire_pair().await;
        for request in [BoltRequest::reset(), BoltRequest::rollback()] {
            let (response, ()) = tokio::join!(connection.send_recv(request), async {
                consume_request(&mut server).await;
                server
                    .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                    .await
                    .unwrap();
            });
            assert!(matches!(response, Ok(BoltResponse::Success(_))));
            assert!(!connection.response_pending);
        }
    }

    #[tokio::test]
    async fn once_only_graph_calls_return_transient_failures_without_retrying() {
        use crate::types::BoltWireFormat;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{timeout, Duration};
        for execute in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let graph = crate::Graph::new(
                format!("bolt://{}", listener.local_addr().unwrap()),
                "user",
                "password",
            )
            .await
            .unwrap();
            let server = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.read_exact(&mut [0; 20]).await.unwrap();
                socket.write_all(&[0, 0, 1, 4]).await.unwrap();
                consume_request(&mut socket).await; // HELLO
                socket
                    .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                    .await
                    .unwrap();
                consume_request(&mut socket).await; // RUN
                let mut metadata = crate::BoltMap::new();
                metadata.put(
                    "code".into(),
                    "Neo.TransientError.Transaction.DeadlockDetected".into(),
                );
                metadata.put("message".into(), "retryable test failure".into());
                let encoded = metadata.into_bytes(crate::Version::V4_1).unwrap();
                socket.write_u16((encoded.len() + 2) as u16).await.unwrap();
                socket.write_all(&[0xb1, 0x7f]).await.unwrap();
                socket.write_all(&encoded).await.unwrap();
                socket.write_u16(0).await.unwrap();
            };
            let client = async {
                let query = crate::Query::new("RETURN 1".into());
                let result = if execute {
                    graph.execute_once(query).await.map(|_| ())
                } else {
                    graph.run_once(query).await
                };
                assert!(matches!(result, Err(crate::Error::Neo4j(_))));
            };
            timeout(Duration::from_secs(2), async {
                tokio::join!(server, client);
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn abandoned_transactions_close_without_another_pool_checkout() {
        use crate::types::BoltWireFormat;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{timeout, Duration};
        for mode in 0..4 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let graph = crate::Graph::new(
                format!("bolt://{}", listener.local_addr().unwrap()),
                "user",
                "password",
            )
            .await
            .unwrap();
            let server = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.read_exact(&mut [0; 20]).await.unwrap();
                socket.write_all(&[0, 0, 1, 4]).await.unwrap();
                consume_request(&mut socket).await;
                socket
                    .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                    .await
                    .unwrap();
                let size = socket.read_u16().await.unwrap();
                let mut begin = vec![0; usize::from(size)];
                socket.read_exact(&mut begin).await.unwrap();
                assert_eq!(socket.read_u16().await.unwrap(), 0);
                assert_eq!(&begin[..2], &[0xb1, 0x11]);
                let mut metadata = bytes::Bytes::copy_from_slice(&begin[2..]);
                let metadata = crate::BoltMap::parse(crate::Version::V4_1, &mut metadata).unwrap();
                assert_eq!(metadata.get::<i64>("tx_timeout").unwrap(), 1234);
                socket
                    .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                    .await
                    .unwrap();
                if mode == 1 {
                    consume_request(&mut socket).await; // RUN
                    socket
                        .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                        .await
                        .unwrap();
                    consume_request(&mut socket).await; // PULL; withhold reply
                } else if mode == 2 {
                    consume_request(&mut socket).await; // COMMIT; withhold reply
                } else if mode == 3 {
                    consume_request(&mut socket).await; // ROLLBACK acknowledged
                    socket
                        .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                        .await
                        .unwrap();
                    assert!(
                        timeout(Duration::from_millis(10), socket.read(&mut [0]))
                            .await
                            .is_err(),
                        "a clean completed transaction should remain pooled"
                    );
                    return;
                }
                // Keep the pool/Graph alive: closure must follow lease release,
                // without pool checkout, a server timeout, or dropping Graph.
                let eof = timeout(Duration::from_secs(1), socket.read(&mut [0]))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(eof, 0, "abandoned lease must close its socket immediately");
            };
            let client = async {
                let mut txn = graph
                    .start_txn_with_timeout(Duration::from_millis(1234))
                    .await
                    .unwrap();
                if mode == 1 {
                    let mut stream = txn
                        .execute(crate::Query::new("RETURN 1".into()))
                        .await
                        .unwrap();
                    assert!(timeout(Duration::from_millis(10), stream.next(&mut txn))
                        .await
                        .is_err());
                    assert!(matches!(
                        txn.rollback().await,
                        Err(crate::Error::ConnectionError)
                    ));
                } else if mode == 2 {
                    assert!(timeout(Duration::from_millis(10), txn.commit())
                        .await
                        .is_err());
                } else if mode == 3 {
                    txn.rollback().await.unwrap();
                } else {
                    drop(txn); // Cancellation between requests also releases locks.
                }
            };
            timeout(Duration::from_secs(2), async {
                tokio::join!(server, client);
            })
            .await
            .unwrap();
            drop(graph);
        }
    }

    #[tokio::test]
    #[ignore = "requires disposable NEO4J_TEST_URI, NEO4J_TEST_USER, NEO4J_TEST_PASSWORD"]
    async fn timed_out_transaction_releases_server_lock_without_owner_pool_checkout() {
        use tokio::time::{timeout, Duration};
        let uri = std::env::var("NEO4J_TEST_URI").unwrap();
        let user = std::env::var("NEO4J_TEST_USER").unwrap();
        let password = std::env::var("NEO4J_TEST_PASSWORD").unwrap();
        let owner = crate::Graph::new(&uri, &user, &password).await.unwrap();
        let observer = crate::Graph::new(&uri, &user, &password).await.unwrap();
        let id = format!(
            "lease-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        owner
            .run_once(
                crate::Query::new("CREATE (:CancellationLeaseTest {id:$id})".into())
                    .param("id", id.clone()),
            )
            .await
            .unwrap();
        // No server timeout here: this specifically tests closing the dropped
        // lease, independently of the optional BEGIN timeout safety net.
        let mut txn = owner.start_txn().await.unwrap();
        txn.run(
            crate::Query::new("MATCH (n:CancellationLeaseTest {id:$id}) SET n.value=1".into())
                .param("id", id.clone()),
        )
        .await
        .unwrap();
        let mut stream = txn
            .execute(crate::Query::new(
                "UNWIND range(1,100000000) AS i RETURN sum(sin(toFloat(i))) AS value".into(),
            ))
            .await
            .unwrap();
        assert!(timeout(Duration::from_millis(20), stream.next(&mut txn))
            .await
            .is_err());
        assert!(matches!(
            txn.rollback().await,
            Err(crate::Error::ConnectionError)
        ));
        timeout(
            Duration::from_secs(3),
            observer.run_once(
                crate::Query::new("MATCH (n:CancellationLeaseTest {id:$id}) SET n.value=2".into())
                    .param("id", id.clone()),
            ),
        )
        .await
        .expect("abandoned transaction retained the server lock")
        .unwrap();
        observer
            .run_once(
                crate::Query::new("MATCH (n:CancellationLeaseTest {id:$id}) DELETE n".into())
                    .param("id", id),
            )
            .await
            .unwrap();
        drop(owner);
    }

    #[tokio::test]
    async fn server_failure_codes_survive_every_transaction_response_phase() {
        use crate::types::BoltWireFormat;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{timeout, Duration};
        const CODE: &str = "Neo.ClientError.Schema.ConstraintValidationFailed";
        for phase in ["BEGIN", "PULL", "DISCARD", "COMMIT", "ROLLBACK"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let graph = crate::Graph::new(
                format!("bolt://{}", listener.local_addr().unwrap()),
                "user",
                "password",
            )
            .await
            .unwrap();
            let server = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.read_exact(&mut [0; 20]).await.unwrap();
                socket.write_all(&[0, 0, 1, 4]).await.unwrap();
                consume_request(&mut socket).await; // HELLO
                socket
                    .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                    .await
                    .unwrap();
                consume_request(&mut socket).await; // BEGIN
                if phase != "BEGIN" {
                    socket
                        .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                        .await
                        .unwrap();
                    consume_request(&mut socket).await;
                    if matches!(phase, "PULL" | "DISCARD") {
                        // RUN succeeds; constraint violation surfaces during execution.
                        socket
                            .write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0])
                            .await
                            .unwrap();
                        consume_request(&mut socket).await;
                    }
                }
                let mut metadata = crate::BoltMap::new();
                metadata.put("code".into(), CODE.into());
                metadata.put("message".into(), "constraint test failure".into());
                let encoded = metadata.into_bytes(crate::Version::V4_1).unwrap();
                socket.write_u16((encoded.len() + 2) as u16).await.unwrap();
                socket.write_all(&[0xb1, 0x7f]).await.unwrap();
                socket.write_all(&encoded).await.unwrap();
                socket.write_u16(0).await.unwrap();
            };
            let client = async {
                let result = async {
                    let mut txn = graph.start_txn().await?;
                    match phase {
                        "BEGIN" => unreachable!("BEGIN should fail"),
                        "PULL" => txn
                            .execute(crate::Query::new("RETURN 1".into()))
                            .await?
                            .next(&mut txn)
                            .await
                            .map(|_| ()),
                        "DISCARD" => txn.run(crate::Query::new("RETURN 1".into())).await,
                        "COMMIT" => txn.commit().await,
                        "ROLLBACK" => txn.rollback().await,
                        _ => unreachable!(),
                    }
                }
                .await;
                match result {
                    Err(crate::Error::Neo4j(error)) => assert_eq!(error.code(), CODE, "{phase}"),
                    other => panic!("{phase} lost its typed server error: {other:?}"),
                }
            };
            timeout(Duration::from_secs(2), async {
                tokio::join!(server, client);
            })
            .await
            .unwrap();
        }
    }


    #[tokio::test]
    async fn ignored_rollback_resets_failed_transaction_and_reuses_socket() {
        use crate::types::BoltWireFormat;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{timeout, Duration};
        async fn expect_request(socket: &mut tokio::net::TcpStream, signature: u8) {
            let size = socket.read_u16().await.unwrap();
            let mut message = vec![0; size as usize];
            socket.read_exact(&mut message).await.unwrap();
            assert_eq!(message[1], signature);
            assert_eq!(socket.read_u16().await.unwrap(), 0);
        }
        async fn success(socket: &mut tokio::net::TcpStream) {
            socket.write_all(&[0, 3, 0xb1, 0x70, 0xa0, 0, 0]).await.unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let graph = crate::Graph::new(format!("bolt://{}", listener.local_addr().unwrap()), "user", "password").await.unwrap();
        let server = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.read_exact(&mut [0;20]).await.unwrap();
            socket.write_all(&[0,0,1,4]).await.unwrap();
            consume_request(&mut socket).await;
            success(&mut socket).await;
            expect_request(&mut socket, 0x11).await;
            success(&mut socket).await;
            expect_request(&mut socket, 0x10).await;
            let mut metadata = crate::BoltMap::new();
            metadata.put("code".into(), "Neo.ClientError.Schema.ConstraintValidationFailed".into());
            metadata.put("message".into(), "constraint test".into());
            let encoded = metadata.into_bytes(crate::Version::V4_1).unwrap();
            socket.write_u16((encoded.len()+2) as u16).await.unwrap();
            socket.write_all(&[0xb1,0x7f]).await.unwrap();
            socket.write_all(&encoded).await.unwrap();
            socket.write_u16(0).await.unwrap();
            expect_request(&mut socket, 0x13).await;
            socket.write_all(&[0,2,0xb0,0x7e,0,0]).await.unwrap();
            expect_request(&mut socket, 0x0f).await; // Recovery RESET.
            success(&mut socket).await;
            expect_request(&mut socket, 0x0f).await; // Pool reuse RESET, same socket.
            success(&mut socket).await;
            expect_request(&mut socket, 0x10).await;
            success(&mut socket).await;
            expect_request(&mut socket, 0x2f).await;
            success(&mut socket).await;
        };
        let client = async {
            let mut txn = graph.start_txn().await.unwrap();
            assert!(matches!(txn.run(crate::Query::new("CREATE (n)".into())).await, Err(crate::Error::Neo4j(_))));
            txn.rollback().await.unwrap();
            graph.run_once(crate::Query::new("RETURN 1".into())).await.unwrap();
        };
        timeout(Duration::from_secs(2), async { tokio::join!(server, client); }).await.unwrap();
    }

    #[test]
    fn should_parse_uri() {
        let url = NeoUrl::parse("bolt://localhost:4242").unwrap();
        assert_eq!(url.port(), 4242);
        assert_eq!(url.host(), Host::Domain("localhost"));
        assert_eq!(url.scheme(), "bolt");
    }

    #[test]
    fn should_parse_uri_without_scheme() {
        let url = NeoUrl::parse("localhost:4242").unwrap();
        assert_eq!(url.port(), 4242);
        assert_eq!(url.host(), Host::Domain("localhost"));
        assert_eq!(url.scheme(), "bolt");
    }

    #[test]
    fn should_parse_ip_uri_without_scheme() {
        let url = NeoUrl::parse("127.0.0.1:4242").unwrap();
        assert_eq!(url.port(), 4242);
        assert_eq!(url.host(), Host::Domain("127.0.0.1"));
        assert_eq!(url.scheme(), "bolt");
    }
}
