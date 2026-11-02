//! Where connections come from: the addresses of the built-in listeners, and the [`Listener`]
//! trait for transports of your own.

use std::{fmt, future::Future, io, net::SocketAddr, path::Path, path::PathBuf};

use supersocket_connection::BoxStream;

use crate::{BoxFuture, session::ConnectionInfo};

/// Where a listener accepts connections, as [`Server::start`](crate::Server::start) reports it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ListenAddr {
    /// A TCP socket address.
    Tcp(SocketAddr),
    /// The path of a Unix domain socket.
    Unix(PathBuf),
    /// Whatever a [`Listener`] of your own reports, such as `udp://0.0.0.0:4040`.
    Other(String),
}

impl ListenAddr {
    /// The socket address, for a TCP listener.
    pub fn tcp(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp(address) => Some(*address),
            _ => None,
        }
    }

    /// The path, for a Unix socket listener.
    pub fn unix(&self) -> Option<&Path> {
        match self {
            Self::Unix(path) => Some(path),
            _ => None,
        }
    }
}

impl fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(address) => address.fmt(f),
            Self::Unix(path) => path.display().fmt(f),
            Self::Other(text) => f.write_str(text),
        }
    }
}

impl From<SocketAddr> for ListenAddr {
    fn from(address: SocketAddr) -> Self {
        Self::Tcp(address)
    }
}

/// Accepts connections for a server, like SuperSocket's `IConnectionListener`: for transports
/// that the built-in TCP and Unix socket listeners do not cover, such as UDP or an in-memory
/// transport in a test. Add one with [`ServerBuilder::listener`](crate::ServerBuilder::listener).
/// Its connections are served like those of the built-in listeners: the accept hook sees them,
/// the connection limit, the stream layers and the handshake timeout apply, and
/// [`Server::stop_accepting`](crate::Server::stop_accepting) stops the listener.
///
/// A listener is bound when it is created; the server accepts from it once started.
///
/// ```
/// use std::io;
/// use supersocket_connection::BoxStream;
/// use supersocket_server::{ConnectionInfo, ListenAddr, Listener};
/// use tokio::sync::mpsc;
///
/// /// Serves the streams sent through a channel, for tests.
/// struct InMemory(mpsc::Receiver<BoxStream>);
///
/// impl Listener for InMemory {
///     fn local_addr(&self) -> ListenAddr {
///         ListenAddr::Other("memory".into())
///     }
///
///     async fn accept(&mut self) -> io::Result<Option<(BoxStream, ConnectionInfo)>> {
///         // Once every sender is gone, the listener is done.
///         Ok(self.0.recv().await.map(|stream| (stream, ConnectionInfo::default())))
///     }
/// }
/// ```
pub trait Listener: Send + 'static {
    /// Where the listener accepts connections, for logs and
    /// [`Server::start`](crate::Server::start).
    fn local_addr(&self) -> ListenAddr;

    /// Accepts the next connection: its stream and where it comes from. `Ok(None)` ends the
    /// listener, for example because its socket was closed. After an error, accepting goes on
    /// after a short pause. The future is dropped when the server stops accepting, so it must be
    /// cancel safe: a connection it has accepted at that point is dropped with it.
    fn accept(
        &mut self,
    ) -> impl Future<Output = io::Result<Option<(BoxStream, ConnectionInfo)>>> + Send;
}

// Traits with async methods cannot be used as trait objects directly, so the server keeps
// listeners behind this boxed-future version.
pub(crate) trait DynListener: Send {
    fn local_addr(&self) -> ListenAddr;
    fn accept(&mut self) -> BoxFuture<'_, io::Result<Option<(BoxStream, ConnectionInfo)>>>;
}

impl<L: Listener> DynListener for L {
    fn local_addr(&self) -> ListenAddr {
        Listener::local_addr(self)
    }

    fn accept(&mut self) -> BoxFuture<'_, io::Result<Option<(BoxStream, ConnectionInfo)>>> {
        Box::pin(Listener::accept(self))
    }
}
