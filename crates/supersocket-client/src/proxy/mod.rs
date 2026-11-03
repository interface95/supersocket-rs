//! Proxy connectors, laid out like SuperSocket's `SuperSocket.Client.Proxy`.
//!
//! [`ProxyConnector`] plays the part of `ProxyConnectorBase`: it opens the connection to the
//! proxy, then leaves the proxy's handshake to [`connect_proxy`](ProxyConnector::connect_proxy).
//! A handshake runs on a [`ProxyConnection`], which sends requests and cuts the replies into
//! packages with the pipeline filters from `supersocket-proto`, the way SuperSocket's connectors
//! read a package stream, and finally detaches the connection to hand it on. [`HttpConnector`],
//! [`Socks4Connector`] and [`Socks5Connector`] are built this way, and so can be connectors for
//! other proxies.

mod http;
mod socks4;
mod socks5;

use std::{any, fmt, future::Future, io, net::SocketAddr};

use supersocket_connection::{PackageReader, ReadOptions, Rewind};
use supersocket_proto::PipelineFilter;
use tokio::io::AsyncWriteExt;

pub use http::HttpConnector;
pub use socks4::Socks4Connector;
pub use socks5::Socks5Connector;

use crate::{BoxStream, Connection, Connector, Endpoint, TcpConnector, connector::must_be_first};

/// The base of the proxy connectors, like SuperSocket's `ProxyConnectorBase`.
///
/// Every `ProxyConnector` is a [`Connector`]: connecting checks the target with
/// [`validate_target`](Self::validate_target), opens a connection to
/// [`proxy_endpoint`](Self::proxy_endpoint) with the connector from
/// [`create_default_connector`](Self::create_default_connector), then calls
/// [`connect_proxy`](Self::connect_proxy) to ask the proxy for the target. Like the other
/// connectors that open the connection, it goes first in a chain, through
/// [`ClientBuilder::proxy`](crate::ClientBuilder::proxy).
///
/// A connector for a proxy that answers `CONNECT host:port` with a line `OK`:
///
/// ```
/// use std::io;
///
/// use supersocket_client::{Connection, Endpoint, ProxyConnection, ProxyConnector};
/// use supersocket_proto::LineFilter;
///
/// struct LineProxy {
///     proxy: Endpoint,
/// }
///
/// impl ProxyConnector for LineProxy {
///     fn proxy_endpoint(&self) -> &Endpoint {
///         &self.proxy
///     }
///
///     async fn connect_proxy(
///         &self,
///         target: &Endpoint,
///         connection: Connection,
///     ) -> io::Result<Connection> {
///         let mut connection = ProxyConnection::new(connection, LineFilter::default());
///         connection.send(format!("CONNECT {target}\r\n").as_bytes()).await?;
///         match connection.receive().await?.text.as_str() {
///             "OK" => Ok(connection.detach()),
///             reply => Err(io::Error::new(io::ErrorKind::ConnectionRefused, reply.to_owned())),
///         }
///     }
/// }
/// ```
pub trait ProxyConnector: Send + Sync + 'static {
    /// Where the proxy listens.
    fn proxy_endpoint(&self) -> &Endpoint;

    /// Checks `target` before the proxy is dialed, so a target the proxy protocol cannot carry
    /// is refused without opening a connection. Default: accepts every target.
    fn validate_target(&self, target: &Endpoint) -> io::Result<()> {
        let _ = target;
        Ok(())
    }

    /// The connector that opens the connection to the proxy, like `CreateDefaultConnector`.
    /// Default: a [`TcpConnector`]; a [`ConnectorChain`](crate::ConnectorChain) of TCP and TLS
    /// reaches an HTTPS proxy.
    fn create_default_connector(&self) -> impl Connector {
        TcpConnector::new()
    }

    /// Asks the proxy, over the open `connection` to it, to connect to `target`, and returns the
    /// connection through the proxy; like `ConnectProxyAsync`. [`ProxyConnection`] reads the
    /// replies.
    fn connect_proxy(
        &self,
        target: &Endpoint,
        connection: Connection,
    ) -> impl Future<Output = io::Result<Connection>> + Send;
}

impl<T: ProxyConnector> Connector for T {
    async fn connect(
        &self,
        target: &Endpoint,
        previous: Option<Connection>,
    ) -> io::Result<Connection> {
        must_be_first(short_type_name::<T>(), &previous)?;
        self.validate_target(target)?;
        let connector = self.create_default_connector();
        let connection = Connector::connect(&connector, self.proxy_endpoint(), None).await?;
        self.connect_proxy(target, connection).await
    }
}

/// The name of `T` without its module path, for messages; `type_name` is for diagnostics only,
/// so nothing depends on the exact text.
fn short_type_name<T>() -> &'static str {
    let name = any::type_name::<T>();
    let base = name.split('<').next().unwrap_or(name);
    base.rsplit("::").next().unwrap_or(base)
}

/// Limits for reading a proxy's replies, which are small.
const HANDSHAKE_LIMITS: ReadOptions = ReadOptions {
    max_package_length: 64 * 1024,
    receive_buffer_size: 1024,
};

/// The connection to a proxy during its handshake, like the connection and package stream that
/// SuperSocket's proxy connectors create from their `ConnectState`.
///
/// [`send`](Self::send) writes requests, [`receive`](Self::receive) returns the replies cut by a
/// pipeline filter, which may switch to the filter of the next step through
/// [`next_filter`](PipelineFilter::next_filter), and [`detach`](Self::detach) hands the
/// connection on once the proxy has connected. Dropping it instead closes the connection.
pub struct ProxyConnection<P> {
    reader: PackageReader<BoxStream, P>,
    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl<P: Send + 'static> ProxyConnection<P> {
    /// Starts a handshake on `connection`, cutting what the proxy sends with `filter`.
    pub fn new(connection: Connection, filter: impl PipelineFilter<Package = P>) -> Self {
        let (stream, local_addr, peer_addr) = connection.into_parts();
        Self {
            reader: PackageReader::new(stream, Box::new(filter), HANDSHAKE_LIMITS),
            local_addr,
            peer_addr,
        }
    }

    /// Sends `data` to the proxy.
    pub async fn send(&mut self, data: &[u8]) -> io::Result<()> {
        let stream = self.reader.get_mut();
        stream.write_all(data).await?;
        stream.flush().await
    }

    /// Receives the proxy's next reply. The proxy closing the connection first is an error.
    pub async fn receive(&mut self) -> io::Result<P> {
        self.reader.next().await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the proxy closed the connection",
            )
        })
    }

    /// Receives a reply in the middle of the handshake, where the proxy must wait for the next
    /// request: anything it sent past the reply is an error. Use [`receive`](Self::receive) for
    /// the last reply, which data from the target may follow.
    pub async fn receive_step(&mut self) -> io::Result<P> {
        let reply = self.receive().await?;
        if !self.reader.buffered().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the proxy sent unexpected data during the handshake",
            ));
        }
        Ok(reply)
    }

    /// Ends the handshake and returns the connection, like `DetachAsync`. Whatever the proxy sent
    /// after its last reply, such as the first bytes from the target, is read again first.
    pub fn detach(self) -> Connection {
        let (stream, rest) = self.reader.into_parts();
        let stream: BoxStream = if rest.is_empty() {
            stream
        } else {
            Box::new(Rewind::new(rest.freeze(), stream))
        };
        Connection::from_parts(stream, self.local_addr, self.peer_addr)
    }
}

impl<P> fmt::Debug for ProxyConnection<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyConnection")
            .field("local_addr", &self.local_addr)
            .field("peer_addr", &self.peer_addr)
            .field("reader", &self.reader)
            .finish()
    }
}

fn refused(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionRefused, message)
}

/// A username and password. `Debug` output leaves out the password.
#[derive(Clone)]
struct Credentials {
    username: String,
    password: String,
}

impl Credentials {
    fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &format_args!("<redacted>"))
            .finish()
    }
}

fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let bytes = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let group = u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]);
        let symbol = |shift: u32| ALPHABET[(group >> shift) as usize & 63] as char;
        output.push(symbol(18));
        output.push(symbol(12));
        output.push(if chunk.len() > 1 { symbol(6) } else { '=' });
        output.push(if chunk.len() > 2 { symbol(0) } else { '=' });
    }
    output
}

/// A connection to nowhere for unit tests: what the connector writes ends up in the returned
/// stream, and the connector reads `replies`.
#[cfg(test)]
async fn in_memory(replies: &[u8]) -> (Connection, tokio::io::DuplexStream) {
    let (ours, mut theirs) = tokio::io::duplex(4096);
    theirs.write_all(replies).await.unwrap();
    (Connection::new(ours), theirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_base64() {
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64(b"a"), "YQ==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b""), "");
    }

    #[test]
    fn debug_output_leaves_out_passwords() {
        let http = format!(
            "{:?}",
            HttpConnector::new("proxy:8080")
                .unwrap()
                .credentials("user", "hunter2")
        );
        let socks5 = format!(
            "{:?}",
            Socks5Connector::new("proxy:1080")
                .unwrap()
                .credentials("user", "hunter2")
        );
        for output in [http, socks5] {
            assert!(output.contains("\"user\""), "{output}");
            assert!(!output.contains("hunter2"), "{output}");
        }
    }

    #[tokio::test]
    async fn detaching_keeps_what_follows_the_last_reply() {
        use supersocket_proto::LineFilter;
        use tokio::io::AsyncReadExt;

        let (connection, _proxy) = in_memory(b"OK\r\nfirst bytes").await;
        let mut connection = ProxyConnection::new(connection, LineFilter::default());
        assert_eq!(connection.receive().await.unwrap().text, "OK");
        let (mut stream, _, _) = connection.detach().into_parts();
        let mut rest = [0; 11];
        stream.read_exact(&mut rest).await.unwrap();
        assert_eq!(&rest, b"first bytes");
    }

    #[tokio::test]
    async fn a_proxy_connector_goes_first() {
        let (previous, _proxy) = in_memory(b"").await;
        let error = Socks5Connector::new("127.0.0.1:1")
            .unwrap()
            .connect(&Endpoint::new("example.com", 80), Some(previous))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn short_type_names_leave_out_the_path() {
        assert_eq!(short_type_name::<Socks5Connector>(), "Socks5Connector");
        assert_eq!(short_type_name::<Vec<HttpConnector>>(), "Vec");
    }

    #[tokio::test]
    async fn invalid_targets_are_refused_before_the_proxy_is_dialed() {
        // Nothing listens on port 1, so dialing would fail with a different error.
        let target = Endpoint::new("example.com:443 HTTP/1.1\r\nX-Injected: 1\r\nX", 80);
        let connector = HttpConnector::new("127.0.0.1:1").unwrap();
        let error = Connector::connect(&connector, &target, None)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
    }

    #[tokio::test]
    async fn data_past_a_handshake_reply_is_an_error() {
        use supersocket_proto::LineFilter;

        let (connection, _proxy) = in_memory(b"OK\r\nextra").await;
        let mut connection = ProxyConnection::new(connection, LineFilter::default());
        let error = connection.receive_step().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
