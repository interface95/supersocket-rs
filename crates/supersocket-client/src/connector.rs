use std::{fmt, future::Future, io, net::SocketAddr};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpSocket, TcpStream},
};

use crate::{BoxFuture, Endpoint};

/// A byte stream a connection runs on.
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Stream for T {}

/// A boxed [`Stream`].
pub type BoxStream = Box<dyn Stream>;

/// A connection being built by a chain of [`Connector`]s.
pub struct Connection {
    /// The stream to read from and write to.
    pub stream: BoxStream,
    /// Local address of the underlying socket.
    pub local_addr: Option<SocketAddr>,
    /// Address the underlying socket is connected to: the proxy when connecting through one.
    pub peer_addr: Option<SocketAddr>,
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
        Ok(Connection {
            local_addr: stream.local_addr().ok(),
            peer_addr: stream.peer_addr().ok(),
            stream: Box::new(stream),
        })
    }
}
