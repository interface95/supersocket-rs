use std::{fmt, future::Future, io, net::SocketAddr};

use supersocket_connection::{BoxStream, Stream};
use tokio::net::{TcpSocket, TcpStream};

use crate::{BoxFuture, Endpoint};

/// A connection being built by a chain of [`Connector`]s: a stream and, when known, the
/// addresses of the socket under it.
pub struct Connection {
    stream: BoxStream,
    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl Connection {
    /// A connection on `stream` whose addresses are not known.
    pub fn new(stream: impl Stream) -> Self {
        Self::from_parts(Box::new(stream), None, None)
    }

    /// A connection on `stream` with the given addresses.
    pub fn from_parts(
        stream: BoxStream,
        local_addr: Option<SocketAddr>,
        peer_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            stream,
            local_addr,
            peer_addr,
        }
    }

    /// Local address of the underlying socket.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    /// Address the underlying socket is connected to: the proxy when connecting through one.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// Wraps the stream, keeping the addresses; for connectors that add a layer, such as
    /// compression.
    pub fn map_stream(self, wrap: impl FnOnce(BoxStream) -> BoxStream) -> Self {
        Self {
            stream: wrap(self.stream),
            ..self
        }
    }

    /// Takes the connection apart, for connectors that replace the stream asynchronously, such
    /// as a TLS handshake; put it back together with [`from_parts`](Self::from_parts).
    pub fn into_parts(self) -> (BoxStream, Option<SocketAddr>, Option<SocketAddr>) {
        (self.stream, self.local_addr, self.peer_addr)
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("local_addr", &self.local_addr)
            .field("peer_addr", &self.peer_addr)
            .finish_non_exhaustive()
    }
}

/// One step of building a connection, like SuperSocket's `IConnector`.
///
/// Connectors run in order. The first one receives `None` and opens the connection, as
/// [`TcpConnector`] and the proxy connectors do. Each later one receives the connection made so
/// far and returns it wrapped, as [`TlsConnector`](crate::TlsConnector) does.
pub trait Connector: Send + Sync + 'static {
    /// Opens a connection to `target`, or wraps `previous`.
    fn connect(
        &self,
        target: &Endpoint,
        previous: Option<Connection>,
    ) -> impl Future<Output = io::Result<Connection>> + Send;
}

// Traits with async methods cannot be used as trait objects directly, so the client keeps
// connectors behind this boxed-future version.
pub(crate) trait DynConnector: Send + Sync {
    fn connect<'a>(
        &'a self,
        target: &'a Endpoint,
        previous: Option<Connection>,
    ) -> BoxFuture<'a, io::Result<Connection>>;
}

impl<C: Connector> DynConnector for C {
    fn connect<'a>(
        &'a self,
        target: &'a Endpoint,
        previous: Option<Connection>,
    ) -> BoxFuture<'a, io::Result<Connection>> {
        Box::pin(Connector::connect(self, target, previous))
    }
}

pub(crate) fn must_be_first(name: &str, previous: &Option<Connection>) -> io::Result<()> {
    match previous {
        Some(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} opens the connection, so it must be the first connector"),
        )),
        None => Ok(()),
    }
}

/// Compresses the connection with gzip in both directions, like SuperSocket's `GZipConnector`.
/// The server must compress too, as with `GzipLayer` from `supersocket-server`. It wraps the
/// connection made so far, so add it after the connector that opens it and after TLS.
#[cfg(feature = "gzip")]
#[derive(Debug, Clone, Copy, Default)]
pub struct GzipConnector;

#[cfg(feature = "gzip")]
impl Connector for GzipConnector {
    async fn connect(
        &self,
        _target: &Endpoint,
        previous: Option<Connection>,
    ) -> io::Result<Connection> {
        let connection = previous.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "GzipConnector wraps a connection, so it cannot be the first connector",
            )
        })?;
        Ok(connection.map_stream(supersocket_connection::gzip))
    }
}

/// Opens a TCP connection, like SuperSocket's `SocketConnector`. Tries every address the target
/// resolves to, in order.
#[derive(Debug, Clone)]
pub struct TcpConnector {
    local_addr: Option<SocketAddr>,
    no_delay: bool,
}

impl Default for TcpConnector {
    fn default() -> Self {
        Self {
            local_addr: None,
            no_delay: true,
        }
    }
}

impl TcpConnector {
    /// A connector with Nagle's algorithm disabled and no fixed local address.
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds the socket to `address` before connecting.
    pub fn local_addr(mut self, address: SocketAddr) -> Self {
        self.local_addr = Some(address);
        self
    }

    /// Whether to disable Nagle's algorithm. Default: `true`.
    pub fn no_delay(mut self, no_delay: bool) -> Self {
        self.no_delay = no_delay;
        self
    }

    pub(crate) async fn open(&self, target: &Endpoint) -> io::Result<TcpStream> {
        let mut last_error = None;
        for address in target.resolve().await? {
            if self
                .local_addr
                .is_some_and(|local| local.is_ipv4() != address.is_ipv4())
            {
                continue;
            }
            match self.open_address(address).await {
                Ok(stream) => return Ok(stream),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{target} did not resolve to a usable address"),
            )
        }))
    }

    async fn open_address(&self, address: SocketAddr) -> io::Result<TcpStream> {
        let socket = if address.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        if let Some(local) = self.local_addr {
            #[cfg(not(windows))]
            socket.set_reuseaddr(true)?;
            socket.bind(local)?;
        }
        let stream = socket.connect(address).await?;
        stream.set_nodelay(self.no_delay)?;
        Ok(stream)
    }
}

impl Connector for TcpConnector {
    async fn connect(
        &self,
        target: &Endpoint,
        previous: Option<Connection>,
    ) -> io::Result<Connection> {
        must_be_first("TcpConnector", &previous)?;
        let stream = self.open(target).await?;
        let (local_addr, peer_addr) = (stream.local_addr().ok(), stream.peer_addr().ok());
        Ok(Connection::from_parts(
            Box::new(stream),
            local_addr,
            peer_addr,
        ))
    }
}
