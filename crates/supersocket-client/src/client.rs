use std::{
    fmt, io,
    net::SocketAddr,
    pin::pin,
    sync::{Arc, Weak},
    time::Duration,
};

use bytes::BytesMut;
use supersocket_connection::{PackageReader, ReadError, ReadOptions, SendQueue, catch_panic};
use supersocket_proto::{BoxFilter, PackageEncoder, PipelineFilter};
use tokio::io::{ReadHalf, WriteHalf};
use tokio_util::sync::CancellationToken;

use crate::{BoxStream, Connection, Connector, TcpConnector, ToEndpoint, connector::DynConnector};

/// Options of an [`EasyClient`].
#[derive(Debug, Clone, PartialEq)]
pub struct ClientOptions {
    /// Largest package the filter may buffer before it is an error. Default: 1 MiB.
    pub max_package_length: usize,
    /// How much to read at a time while a package is incomplete. Default: 4 KiB.
    pub receive_buffer_size: usize,
    /// Once this much data is waiting to be sent, `send` waits for it to drain. Default: 64 KiB.
    pub send_buffer_limit: usize,
    /// Close the connection when the server does not accept sent data for this long.
    /// Default: never.
    pub send_timeout: Option<Duration>,
    /// How long connecting, including proxy and TLS handshakes, may take. Default: 10 seconds.
    pub connect_timeout: Duration,
    /// How long a closing connection may take to send data that is still queued. Default:
    /// 5 seconds.
    pub close_timeout: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            max_package_length: 1024 * 1024,
            receive_buffer_size: 4 * 1024,
            send_buffer_limit: 64 * 1024,
            send_timeout: None,
            connect_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(5),
        }
    }
}

/// Builds an [`EasyClient`]. Created by [`EasyClient::builder`].
pub struct ClientBuilder<P> {
    filter: BoxFilter<P>,
    tcp: TcpConnector,
    proxy: Option<Box<dyn DynConnector>>,
    layers: Vec<Box<dyn DynConnector>>,
    options: ClientOptions,
}

impl<P: Send + 'static> ClientBuilder<P> {
    /// Connects through `proxy` instead of directly, like setting `EasyClient.Proxy`.
    pub fn proxy(mut self, proxy: impl Connector) -> Self {
        self.proxy = Some(Box::new(proxy));
        self
    }

    /// Wraps the connection in TLS, like setting `EasyClient.Security`.
    #[cfg(feature = "tls")]
    pub fn tls(self, tls: crate::TlsConnector) -> Self {
        self.connector(tls)
    }

    /// Adds a connector that wraps the connection. Connectors added this way, including
    /// [`tls`](Self::tls), run in the order they were added, after the connection is opened.
    pub fn connector(mut self, connector: impl Connector) -> Self {
        self.layers.push(Box::new(connector));
        self
    }

    /// Binds the socket to `address` before connecting. Not used when connecting through a proxy.
    pub fn local_addr(mut self, address: SocketAddr) -> Self {
        self.tcp = self.tcp.local_addr(address);
        self
    }

    /// Whether to disable Nagle's algorithm when connecting directly. Default: `true`.
    pub fn no_delay(mut self, no_delay: bool) -> Self {
        self.tcp = self.tcp.no_delay(no_delay);
        self
    }

    /// Changes the options in place.
    pub fn configure(mut self, configure: impl FnOnce(&mut ClientOptions)) -> Self {
        configure(&mut self.options);
        self
    }

    /// Replaces the options.
    pub fn options(mut self, options: ClientOptions) -> Self {
        self.options = options;
        self
    }

    /// Connects to `target`, which is a `"host:port"` string, a socket address or an
    /// [`Endpoint`](crate::Endpoint).
    pub async fn connect(self, target: impl ToEndpoint) -> io::Result<EasyClient<P>> {
        let target = target.to_endpoint()?;
        let connecting = async {
            let mut connection = match &self.proxy {
                Some(proxy) => proxy.connect(&target, None).await?,
                None => Connector::connect(&self.tcp, &target, None).await?,
            };
            for layer in &self.layers {
                connection = layer.connect(&target, Some(connection)).await?;
            }
            Ok::<_, io::Error>(connection)
        };
        let connection = tokio::time::timeout(self.options.connect_timeout, connecting)
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("connecting to {target} timed out"),
                )
            })??;
        Ok(EasyClient::from_connection(
            connection,
            self.filter,
            &self.options,
        ))
    }
}

/// A connection to a server, like SuperSocket's `EasyClient`.
///
/// Receive packages with [`receive`](Self::receive), or hand them to a handler with
/// [`start_receive`](Self::start_receive). Send from any task through a [`ClientSender`]. When the
/// client and all its senders are dropped, queued data is sent and the connection is closed.
pub struct EasyClient<P> {
    receiver: Receiver<P>,
    sender: ClientSender,
}

impl<P: Send + 'static> EasyClient<P> {
    /// Starts building a client whose received data is cut into packages by `filter`.
    pub fn builder(filter: impl PipelineFilter<Package = P>) -> ClientBuilder<P> {
        ClientBuilder {
            filter: Box::new(filter),
            tcp: TcpConnector::new(),
            proxy: None,
            layers: Vec::new(),
            options: ClientOptions::default(),
        }
    }

    fn from_connection(
        connection: Connection,
        filter: BoxFilter<P>,
        options: &ClientOptions,
    ) -> Self {
        let (reader, writer) = tokio::io::split(connection.stream);
        let shared = Arc::new(Shared {
            queue: SendQueue::new(options.send_buffer_limit),
            closing: CancellationToken::new(),
            closed: CancellationToken::new(),
        });
        tokio::spawn(run_writer(
            shared.clone(),
            writer,
            options.send_timeout,
            options.close_timeout,
        ));
        let read_options = ReadOptions {
            max_package_length: options.max_package_length,
            receive_buffer_size: options.receive_buffer_size,
        };
        Self {
            receiver: Receiver {
                reader: PackageReader::new(reader, filter, read_options),
                shared: shared.clone(),
                finished: false,
            },
            sender: ClientSender {
                inner: Arc::new(SenderInner {
                    shared,
                    local_addr: connection.local_addr,
                    peer_addr: connection.peer_addr,
                }),
            },
        }
    }

    /// Waits for the next package. Returns `Ok(None)` once the connection is closed, by either
    /// side. A read or protocol error also closes the connection. After `Ok(None)` or an error,
    /// every call returns `Ok(None)`.
    ///
    /// This is cancel safe: if the future is dropped before it completes, no data is lost.
    pub async fn receive(&mut self) -> Result<Option<P>, ReadError> {
        self.receiver.receive().await
    }

    /// Hands every package to `handler` in a background task, like SuperSocket's
    /// `PackageHandler` event with `StartReceive`. Packages are handled one at a time. Returns a
    /// sender for sending and closing; the task ends when the connection closes.
    ///
    /// The task does not keep the connection open: once the returned sender and every clone of it
    /// are dropped, the connection is closed as if the client had been dropped.
    pub fn start_receive<F, Fut>(self, handler: F) -> ClientSender
    where
        F: Fn(ClientSender, P) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let Self {
            mut receiver,
            sender,
        } = self;
        let connection: Weak<SenderInner> = Arc::downgrade(&sender.inner);
        tokio::spawn(async move {
            loop {
                match receiver.receive().await {
                    Ok(Some(package)) => {
                        let Some(inner) = connection.upgrade() else {
                            break;
                        };
                        let handling = handler(ClientSender { inner }, package);
                        if let Err(panic) = catch_panic(handling).await {
                            tracing::error!(%panic, "client package handler panicked");
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::debug!(%error, "receive failed");
                        break;
                    }
                }
            }
        });
        sender
    }

    /// A handle for sending from other tasks.
    pub fn sender(&self) -> ClientSender {
        self.sender.clone()
    }

    /// Queues `data` to be sent; see [`ClientSender::send`].
    pub async fn send(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.sender.send(data).await
    }

    /// Encodes `package` with `encoder` and queues it; see [`ClientSender::send_package`].
    pub async fn send_package<T: ?Sized>(
        &self,
        encoder: &impl PackageEncoder<T>,
        package: &T,
    ) -> io::Result<()> {
        self.sender.send_package(encoder, package).await
    }

    /// Local address of the socket.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.sender.local_addr()
    }

    /// Address the socket is connected to: the proxy when connecting through one.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.sender.peer_addr()
    }

    /// Sends what is queued, then closes the connection; see [`ClientSender::close`].
    pub async fn close(self) {
        self.sender.close().await;
    }
}

impl<P> fmt::Debug for EasyClient<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EasyClient")
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}

/// State of a connection shared by the client, its senders and its writer task.
struct Shared {
    queue: SendQueue,
    /// Cancelled once either side starts closing the connection.
    closing: CancellationToken,
    /// Cancelled once the writer is done: the connection is closed.
    closed: CancellationToken,
}

impl Shared {
    fn close(&self) {
        self.queue.close();
        self.closing.cancel();
    }
}

/// Writes what is queued until the connection starts closing, then gives the rest
/// `close_timeout` to go out.
async fn run_writer(
    shared: Arc<Shared>,
    writer: WriteHalf<BoxStream>,
    send_timeout: Option<Duration>,
    close_timeout: Duration,
) {
    // However the writer ends, the connection is closed afterwards.
    let _closed = shared.closed.clone().drop_guard();
    let mut writing = pin!(shared.queue.run_writer(writer, send_timeout));
    let written = tokio::select! {
        biased;
        written = &mut writing => written,
        () = shared.closing.cancelled() => tokio::time::timeout(close_timeout, writing)
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(io::ErrorKind::TimedOut, "closing timed out"))
            }),
    };
    if let Err(error) = written {
        tracing::debug!(%error, "send failed");
    }
}

/// The receiving side of a connection.
struct Receiver<P> {
    reader: PackageReader<ReadHalf<BoxStream>, P>,
    shared: Arc<Shared>,
    finished: bool,
}

impl<P: Send + 'static> Receiver<P> {
    async fn receive(&mut self) -> Result<Option<P>, ReadError> {
        if self.finished {
            return Ok(None);
        }
        let received = self.next().await;
        if !matches!(received, Ok(Some(_))) {
            // The connection ended or broke: stop sending too, and stay finished.
            self.finished = true;
            self.shared.close();
        }
        received
    }

    async fn next(&mut self) -> Result<Option<P>, ReadError> {
        loop {
            if let Some(package) = self.reader.try_next()? {
                return Ok(Some(package));
            }
            let read = tokio::select! {
                biased;
                () = self.shared.closed.cancelled() => return Ok(None),
                read = self.reader.fill() => read?,
            };
            if read == 0 {
                return Ok(None);
            }
        }
    }
}

/// Sends to the server and closes the connection. Cheap to clone; clones refer to the same
/// connection.
#[derive(Clone)]
pub struct ClientSender {
    inner: Arc<SenderInner>,
}

struct SenderInner {
    shared: Arc<Shared>,
    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl Drop for SenderInner {
    // The last handle is gone: send what is queued and close.
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl ClientSender {
    /// Queues `data` to be sent. Data from concurrent calls is never interleaved. Waits while the
    /// send buffer limit is reached, and fails once the connection is closing.
    pub async fn send(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.inner.shared.queue.send(data.as_ref()).await
    }

    /// Encodes `package` with `encoder` and queues it like [`send`](Self::send).
    pub async fn send_package<T: ?Sized>(
        &self,
        encoder: &impl PackageEncoder<T>,
        package: &T,
    ) -> io::Result<()> {
        let mut buf = BytesMut::new();
        encoder
            .encode(package, &mut buf)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        self.send(&buf).await
    }

    /// Whether the connection is closed or closing.
    pub fn is_closed(&self) -> bool {
        self.inner.shared.closed.is_cancelled() || self.inner.shared.queue.is_closed()
    }

    /// Completes once the connection is closed.
    pub async fn closed(&self) {
        self.inner.shared.closed.cancelled().await;
    }

    /// Sends what is queued, then closes the connection. Data the server does not accept within
    /// [`close_timeout`](ClientOptions::close_timeout) is dropped. Concurrent calls all wait
    /// until the connection is closed.
    pub async fn close(&self) {
        self.inner.shared.close();
        self.closed().await;
    }

    /// Local address of the socket.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.inner.local_addr
    }

    /// Address the socket is connected to: the proxy when connecting through one.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.inner.peer_addr
    }
}

impl fmt::Debug for ClientSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientSender")
            .field("local_addr", &self.inner.local_addr)
            .field("peer_addr", &self.inner.peer_addr)
            .field("closed", &self.is_closed())
            .finish()
    }
}
