use std::{future::Future, io};

use supersocket_connection::BoxStream;

use crate::{BoxFuture, session::ConnectionInfo};

/// Wraps the stream of every connection before its session starts, after any TLS handshake, like
/// SuperSocket's `IConnectionStreamInitializer`. Use it for compression, encryption or
/// accounting that works on the byte stream rather than on packages; add one with
/// [`ServerBuilder::stream_layer`](crate::ServerBuilder::stream_layer). All the layers of a
/// connection together must finish within
/// [`handshake_timeout`](crate::ServerOptions::handshake_timeout).
///
/// ```
/// use std::io;
/// use supersocket_server::{BoxStream, ConnectionInfo, StreamLayer};
/// use tokio::io::AsyncWriteExt;
///
/// /// Greets every connection before the protocol starts.
/// struct Banner;
///
/// impl StreamLayer for Banner {
///     async fn wrap(
///         &self,
///         mut stream: BoxStream,
///         _connection: &ConnectionInfo,
///     ) -> io::Result<BoxStream> {
///         stream.write_all(b"welcome\r\n").await?;
///         Ok(stream)
///     }
/// }
/// ```
pub trait StreamLayer: Send + Sync + 'static {
    /// Wraps `stream`. An error drops the connection before it becomes a session.
    fn wrap(
        &self,
        stream: BoxStream,
        connection: &ConnectionInfo,
    ) -> impl Future<Output = io::Result<BoxStream>> + Send;
}

/// Compresses every connection with gzip in both directions, like SuperSocket's `UseGZip`.
/// Clients must compress too, for example with `GzipConnector` from `supersocket-client`. Each
/// connection keeps compression state of a few hundred kilobytes.
#[cfg(feature = "gzip")]
#[derive(Debug, Clone, Copy, Default)]
pub struct GzipLayer;

#[cfg(feature = "gzip")]
impl StreamLayer for GzipLayer {
    async fn wrap(&self, stream: BoxStream, _connection: &ConnectionInfo) -> io::Result<BoxStream> {
        Ok(supersocket_connection::gzip(stream))
    }
}

// Traits with async methods cannot be used as trait objects directly, so the server keeps
// layers behind this boxed-future version.
pub(crate) trait DynStreamLayer: Send + Sync {
    fn wrap<'a>(
        &'a self,
        stream: BoxStream,
        connection: &'a ConnectionInfo,
    ) -> BoxFuture<'a, io::Result<BoxStream>>;
}

impl<L: StreamLayer> DynStreamLayer for L {
    fn wrap<'a>(
        &'a self,
        stream: BoxStream,
        connection: &'a ConnectionInfo,
    ) -> BoxFuture<'a, io::Result<BoxStream>> {
        Box::pin(StreamLayer::wrap(self, stream, connection))
    }
}
