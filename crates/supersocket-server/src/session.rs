use std::{
    collections::HashMap,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime},
};

use bytes::BytesMut;
use supersocket_connection::{BoxStream, Rewind, SendQueue};
use supersocket_proto::PackageEncoder;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;

use crate::{BoxError, extensions::Extensions, proxy_protocol::ProxyInfo, server::ServerInfo};

/// Why a session was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CloseReason {
    /// The reason is not known.
    Unknown,
    /// The server is shutting down.
    ServerShutdown,
    /// The peer closed the connection.
    RemoteClosed,
    /// The application closed the session with [`Session::close`].
    LocalClosed,
    /// A package handler failed and the error handler chose to close the session.
    ApplicationError,
    /// Reading from or writing to the connection failed.
    SocketError,
    /// The session was idle for too long, did not complete its handshake in time, or the peer
    /// stopped accepting data.
    Timeout,
    /// The data broke the protocol, or a package exceeded the size limit.
    ProtocolError,
    /// A middleware or the handshake rejected the session.
    Rejected,
    /// Application code panicked where the session cannot continue, such as in the pipeline
    /// filter.
    InternalError,
    /// The application took the connection over with [`Session::detach`].
    Detached,
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "unknown",
            Self::ServerShutdown => "server shutdown",
            Self::RemoteClosed => "remote closed",
            Self::LocalClosed => "local closed",
            Self::ApplicationError => "application error",
            Self::SocketError => "socket error",
            Self::Timeout => "timeout",
            Self::ProtocolError => "protocol error",
            Self::Rejected => "rejected",
            Self::InternalError => "internal error",
            Self::Detached => "detached",
        })
    }
}

/// Where a session is in its life, like SuperSocket's `SessionState`; see [`Session::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SessionStatus {
    /// The connection is being set up: middleware is registering the session, or it has not
    /// completed its handshake yet. Only middleware and the package handler of a server that
    /// requires a handshake see sessions in this state.
    Connecting,
    /// The session is in the session container and its packages are handled.
    Connected,
    /// The session is closing: data still queued is sent, and the closed handlers have not run
    /// yet.
    Closing,
    /// The connection is gone and the closed handlers have run.
    Closed,
}

impl fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Closing => "closing",
            Self::Closed => "closed",
        })
    }
}

/// Per-session state, created for every new session: with `Default` by
/// [`ServerBuilder::session`](crate::ServerBuilder::session), or from the connection by
/// [`ServerBuilder::session_with`](crate::ServerBuilder::session_with).
///
/// Its fields are reachable directly on [`Session`], which dereferences to it. Fields are shared
/// between tasks, so use interior mutability such as `Mutex` or atomics. The hooks run when the
/// session connects and after it has been closed.
///
/// ```
/// use std::sync::Mutex;
/// use supersocket_server::{CloseReason, Session, SessionState};
///
/// #[derive(Default)]
/// struct Player {
///     name: Mutex<String>,
/// }
///
/// impl SessionState for Player {
///     async fn on_closed(session: &Session<Self>, reason: CloseReason) {
///         println!("{} left: {reason}", session.name.lock().unwrap());
///     }
/// }
/// ```
pub trait SessionState: Sized + Send + Sync + 'static {
    /// Called after the session is registered, before its first package is handled. On a server
    /// that requires a handshake, called once the handshake is complete instead.
    fn on_connected(_session: &Session<Self>) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Called after the session was closed and removed from the session container.
    fn on_closed(
        _session: &Session<Self>,
        _reason: CloseReason,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
}

impl SessionState for () {}

/// Where the connection of a session comes from: its addresses and whether it is encrypted. The
/// server fills it in for the connections it accepts; describe other connections with it for
/// [`Server::serve_connection`](crate::Server::serve_connection).
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub(crate) remote_addr: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) secure: bool,
    pub(crate) proxy: Option<Box<ProxyInfo>>,
    #[cfg(feature = "tls")]
    pub(crate) tls: Option<Box<crate::TlsInfo>>,
}

impl ConnectionInfo {
    /// A connection between `remote_addr` and `local_addr`. For transports without IP addresses,
    /// such as Unix sockets, use [`default`](Self::default).
    pub fn new(remote_addr: SocketAddr, local_addr: SocketAddr) -> Self {
        Self {
            remote_addr,
            local_addr,
            secure: false,
            proxy: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Marks the connection as encrypted, which [`Session::is_secure`] reports.
    pub fn secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    /// Address of the peer.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Local address the peer connected to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Whether the connection is encrypted.
    pub fn is_secure(&self) -> bool {
        self.secure
    }

    /// What the PROXY protocol header of the connection said.
    pub fn proxy_info(&self) -> Option<&ProxyInfo> {
        self.proxy.as_deref()
    }

    /// What the TLS handshake established.
    #[cfg(feature = "tls")]
    pub fn tls_info(&self) -> Option<&crate::TlsInfo> {
        self.tls.as_deref()
    }

    /// Records what a PROXY protocol header said: the client's address replaces the proxy's.
    pub(crate) fn set_proxy(&mut self, info: ProxyInfo) {
        if let Some(source) = info.source() {
            self.remote_addr = source;
        }
        self.proxy = Some(Box::new(info));
    }
}

impl Default for ConnectionInfo {
    /// A connection whose addresses are unknown: both are `0.0.0.0:0`.
    fn default() -> Self {
        let unknown = SocketAddr::from(([0, 0, 0, 0], 0));
        Self::new(unknown, unknown)
    }
}

/// A connected client. Cloning is cheap; clones refer to the same session, so a session can be
/// handed to other tasks that send to it.
pub struct Session<S = ()> {
    inner: Arc<SessionInner<S>>,
}

struct SessionInner<S> {
    id: u64,
    connection: ConnectionInfo,
    start_time: SystemTime,
    started_at: Instant,
    /// Milliseconds after `started_at` when data was last received and last sent.
    last_received_ms: AtomicU64,
    last_sent_ms: AtomicU64,
    handshake_complete: AtomicBool,
    connected: AtomicBool,
    closed: AtomicBool,
    /// Set by [`Session::detach`]: where to hand the connection over once the session is done.
    detaching: Mutex<Option<oneshot::Sender<Detached>>>,
    send_queue: SendQueue,
    close_token: CancellationToken,
    close_reason: OnceLock<CloseReason>,
    extensions: Extensions,
    server: Arc<ServerInfo<S>>,
    state: S,
}

impl<S> Clone for Session<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> Deref for Session<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.inner.state
    }
}

/// Sessions are equal when they are the same session. To keep sessions in a set or a map, such
/// as the members of a room, key them by [`id`](Session::id).
impl<S> PartialEq for Session<S> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<S> Eq for Session<S> {}

impl<S> fmt::Debug for Session<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.inner.id)
            .field("remote_addr", &self.inner.connection.remote_addr)
            .field("secure", &self.inner.connection.secure)
            .finish_non_exhaustive()
    }
}

impl<S: SessionState> Session<S> {
    pub(crate) fn new(
        id: u64,
        connection: ConnectionInfo,
        state: S,
        handshake_complete: bool,
        close_token: CancellationToken,
        server: Arc<ServerInfo<S>>,
    ) -> Self {
        let send_queue = SendQueue::new(server.options().send_buffer_limit);
        Self {
            inner: Arc::new(SessionInner {
                id,
                connection,
                start_time: SystemTime::now(),
                started_at: Instant::now(),
                last_received_ms: AtomicU64::new(0),
                last_sent_ms: AtomicU64::new(0),
                handshake_complete: AtomicBool::new(handshake_complete),
                connected: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                detaching: Mutex::new(None),
                send_queue,
                close_token,
                close_reason: OnceLock::new(),
                extensions: Extensions::default(),
                server,
                state,
            }),
        }
    }
}

impl<S> Session<S> {
    /// Unique id of the session within its server.
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// The per-session state, which the session also dereferences to.
    pub fn state(&self) -> &S {
        &self.inner.state
    }

    /// Address of the peer. On a listener that reads PROXY protocol headers it is the client's
    /// address from the header, not the proxy's.
    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.connection.remote_addr
    }

    /// Local address the peer connected to.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.connection.local_addr
    }

    /// Whether the connection uses TLS.
    pub fn is_secure(&self) -> bool {
        self.inner.connection.secure
    }

    /// What the PROXY protocol header of the connection said, on a listener with
    /// [`proxy_protocol`](crate::ListenOptions::proxy_protocol).
    pub fn proxy_info(&self) -> Option<&ProxyInfo> {
        self.inner.connection.proxy_info()
    }

    /// What the TLS handshake established, such as the certificate the client sent, on a TLS
    /// listener.
    #[cfg(feature = "tls")]
    pub fn tls_info(&self) -> Option<&crate::TlsInfo> {
        self.inner.connection.tls_info()
    }

    /// Everything known about where the connection comes from.
    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.inner.connection
    }

    /// When the session connected.
    pub fn start_time(&self) -> SystemTime {
        self.inner.start_time
    }

    /// When data was last received from or sent to the peer, whichever is later. The idle
    /// session timeout counts from here, so a session that only pushes data is not idle.
    pub fn last_active_time(&self) -> Instant {
        self.last_received_time().max(self.last_sent_time())
    }

    /// When data was last received from the peer.
    pub fn last_received_time(&self) -> Instant {
        self.inner.started_at
            + Duration::from_millis(self.inner.last_received_ms.load(Ordering::Relaxed))
    }

    /// When data was last queued to be sent to the peer.
    pub fn last_sent_time(&self) -> Instant {
        self.inner.started_at
            + Duration::from_millis(self.inner.last_sent_ms.load(Ordering::Relaxed))
    }

    /// The server this session belongs to.
    pub fn server(&self) -> &ServerInfo<S> {
        &self.inner.server
    }

    /// Data attached to this session.
    pub fn extensions(&self) -> &Extensions {
        &self.inner.extensions
    }

    /// Whether the session has been closed or is closing.
    pub fn is_closed(&self) -> bool {
        self.inner.close_token.is_cancelled()
    }

    /// Where the session is in its life.
    pub fn status(&self) -> SessionStatus {
        if self.inner.closed.load(Ordering::Acquire) {
            SessionStatus::Closed
        } else if self.inner.close_token.is_cancelled() {
            SessionStatus::Closing
        } else if self.inner.connected.load(Ordering::Acquire) {
            SessionStatus::Connected
        } else {
            SessionStatus::Connecting
        }
    }

    /// Records that the session joined the session container.
    pub(crate) fn mark_connected(&self) {
        self.inner.connected.store(true, Ordering::Release);
    }

    /// Records that the session is over: its connection is gone and its closed handlers ran.
    pub(crate) fn mark_closed(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    /// Queues `data` to be sent. Data from concurrent calls is never interleaved. When more than
    /// [`send_buffer_limit`](crate::ServerOptions::send_buffer_limit) bytes are already waiting,
    /// this waits until the writer has taken them. Fails once the session has stopped sending,
    /// which happens after it closed and its last data was queued.
    pub async fn send(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.inner.send_queue.send(data.as_ref()).await?;
        self.touch_sent();
        Ok(())
    }

    /// Queues `data` without waiting: fails with [`ErrorKind::WouldBlock`](io::ErrorKind::WouldBlock)
    /// while the send buffer limit is reached. For broadcasts, where a session that reads slowly
    /// should be skipped rather than hold up the others.
    pub fn try_send(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.inner.send_queue.try_send(data.as_ref())?;
        self.touch_sent();
        Ok(())
    }

    /// Queues `parts` back to back as one piece without waiting, like [`try_send`](Self::try_send).
    pub fn try_send_vectored(&self, parts: &[&[u8]]) -> io::Result<()> {
        self.inner.send_queue.try_send_vectored(parts)?;
        self.touch_sent();
        Ok(())
    }

    /// Queues `parts` back to back as one piece, like [`send`](Self::send) with the parts joined,
    /// but without copying them together first. Protocol layers use it to send a header and a
    /// payload.
    pub async fn send_vectored(&self, parts: &[&[u8]]) -> io::Result<()> {
        self.inner.send_queue.send_vectored(parts).await?;
        self.touch_sent();
        Ok(())
    }

    /// Queues `data` as the last data of the session, without waiting for room: later sends
    /// fail, while what is queued is still sent and the session keeps reading. For a goodbye
    /// message that nothing may follow, such as a WebSocket close frame.
    pub fn send_last(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.inner.send_queue.send_last(data.as_ref())?;
        self.touch_sent();
        Ok(())
    }

    /// Encodes `package` with `encoder` straight into the send buffer and queues it like
    /// [`send`](Self::send).
    pub async fn send_package<T: ?Sized>(
        &self,
        encoder: &impl PackageEncoder<T>,
        package: &T,
    ) -> Result<(), BoxError> {
        self.send_with(|buf| encoder.encode(package, buf).map_err(BoxError::from))
            .await
    }

    /// Queues what `write` appends to the send buffer, like [`send`](Self::send) without a buffer
    /// in between, like SuperSocket's `SendAsync` with a `PipeWriter` action: for encoding a
    /// package or formatting a reply straight into the queue. `write` runs while the send queue
    /// is locked, so it must be quick and must not wait. Nothing is queued when it fails, and its
    /// error is returned; the session's own errors are I/O errors.
    ///
    /// ```
    /// use supersocket_server::Session;
    ///
    /// # async fn run(session: Session) -> std::io::Result<()> {
    /// let items: Vec<u8> = vec![1, 2, 3];
    /// session
    ///     .send_with(|buf| {
    ///         buf.extend_from_slice(&(items.len() as u16).to_be_bytes());
    ///         buf.extend_from_slice(&items);
    ///         Ok(())
    ///     })
    ///     .await
    /// # }
    /// ```
    pub async fn send_with<E: From<io::Error>>(
        &self,
        write: impl FnOnce(&mut BytesMut) -> Result<(), E>,
    ) -> Result<(), E> {
        self.inner.send_queue.send_with(write).await?;
        self.touch_sent();
        Ok(())
    }

    /// Formats `args` straight into the send buffer and queues it like [`send`](Self::send).
    /// Only while the send buffer limit is reached is the text formatted into a `String` first,
    /// to wait for room. Call it through the [`send_fmt!`](supersocket_connection::send_fmt)
    /// macro, `send_fmt!(session, "OK {sum}\r\n").await?`: `format_args!` cannot be kept across
    /// an `await` in a `Send` future, and the macro lets go of it in time.
    pub fn send_fmt<'a>(
        &'a self,
        args: fmt::Arguments<'_>,
    ) -> impl Future<Output = io::Result<()>> + Send + use<'a, S>
    where
        S: Send + Sync,
    {
        use std::fmt::Write as _;
        // The arguments cannot be kept across an await, so they are used before the first one.
        let queued = self.try_send_with(|buf| {
            buf.write_fmt(args)
                .map_err(|_| io::Error::other("formatting the data failed"))
        });
        let fallback = match queued {
            Ok(()) => Ok(None),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(Some(args.to_string())),
            Err(error) => Err(error),
        };
        async move {
            match fallback? {
                Some(text) => self.send(text).await,
                None => Ok(()),
            }
        }
    }

    /// Like [`send_with`](Self::send_with) without waiting: fails with
    /// [`ErrorKind::WouldBlock`](io::ErrorKind::WouldBlock) while the send buffer limit is
    /// reached, in which case `write` is not called.
    pub fn try_send_with<E: From<io::Error>>(
        &self,
        write: impl FnOnce(&mut BytesMut) -> Result<(), E>,
    ) -> Result<(), E> {
        self.inner.send_queue.try_send_with(write)?;
        self.touch_sent();
        Ok(())
    }

    /// Completes once the session is closed or closing, for tasks that should stop with it.
    pub async fn closed(&self) {
        self.inner.close_token.cancelled().await;
    }

    /// Marks the handshake of the session as complete, on a server that requires one; see
    /// [`ServerBuilder::require_handshake`](crate::ServerBuilder::require_handshake). Call it while
    /// handling one of the session's packages: the session then joins the session container and
    /// its connected handlers run, before its next package is handled.
    pub fn complete_handshake(&self) {
        self.inner.handshake_complete.store(true, Ordering::Release);
    }

    /// Whether the session has completed its handshake. Always `true` on a server that does not
    /// require one.
    pub fn is_handshake_complete(&self) -> bool {
        self.inner.handshake_complete.load(Ordering::Acquire)
    }

    /// Closes the session. Queued data is still sent. The first reason given wins.
    pub fn close(&self, reason: CloseReason) {
        let _ = self.inner.close_reason.set(reason);
        self.inner.close_token.cancel();
    }

    /// Takes the connection away from the session, like SuperSocket's
    /// `IConnection.DetachAsync`: to hand it to another server or protocol that the session
    /// negotiated, such as [`Server::serve_connection`](crate::Server::serve_connection) of a
    /// WebSocket server after an HTTP exchange. The session closes with
    /// [`CloseReason::Detached`], without a goodbye from the package handler; what it had queued
    /// is sent first, within the close timeout. The returned future completes with the
    /// connection once the session's tasks have let go of it, or with `None` when the queued data
    /// could not be sent: writing failed, or took longer than the close timeout.
    ///
    /// Call it from a package handler and return; await the future from another task. The
    /// connection is handed over only once the handler has returned, so awaiting the future
    /// inside the handler would wait forever. Fails when the session is already closing or
    /// detaching.
    ///
    /// ```
    /// use supersocket_server::{HandlerResult, Session};
    /// use tokio::io::AsyncWriteExt;
    ///
    /// # async fn handle(session: Session) -> HandlerResult {
    /// let detaching = session.detach()?;
    /// tokio::spawn(async move {
    ///     if let Some(connection) = detaching.await {
    ///         let mut stream = connection.into_stream();
    ///         let _ = stream.write_all(b"the raw connection is yours\r\n").await;
    ///     }
    /// });
    /// Ok(())
    /// # }
    /// ```
    pub fn detach(&self) -> io::Result<Detaching> {
        let mut slot = self.inner.detaching.lock().unwrap();
        if self.is_closed() || slot.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "the session is closing or already detaching",
            ));
        }
        let (sender, receiver) = oneshot::channel();
        *slot = Some(sender);
        drop(slot);
        self.close(CloseReason::Detached);
        Ok(Detaching(receiver))
    }

    /// Hands the connection over to whoever called [`detach`](Self::detach), or tells them that
    /// it is gone.
    pub(crate) fn hand_over(&self, detached: Option<Detached>) {
        if let Some(sender) = self.inner.detaching.lock().unwrap().take() {
            if let Some(detached) = detached {
                let _ = sender.send(detached);
            }
        }
    }

    pub(crate) fn close_token(&self) -> &CancellationToken {
        &self.inner.close_token
    }

    pub(crate) fn close_reason(&self) -> CloseReason {
        self.inner
            .close_reason
            .get()
            .copied()
            .unwrap_or(CloseReason::Unknown)
    }

    /// Records that data was received.
    pub(crate) fn touch(&self) {
        let elapsed = self.inner.started_at.elapsed().as_millis() as u64;
        self.inner
            .last_received_ms
            .store(elapsed, Ordering::Relaxed);
    }

    /// Records that data was queued to be sent.
    fn touch_sent(&self) {
        let elapsed = self.inner.started_at.elapsed().as_millis() as u64;
        self.inner.last_sent_ms.store(elapsed, Ordering::Relaxed);
    }

    /// Stops accepting data to send; the writer task sends what is queued and shuts down.
    pub(crate) fn close_send_queue(&self) {
        self.inner.send_queue.close();
    }

    /// Like [`close_send_queue`](Self::close_send_queue), but the writer leaves the connection
    /// open for whoever takes it over.
    pub(crate) fn detach_send_queue(&self) {
        self.inner.send_queue.detach();
    }

    /// The session's writer task. A failed or timed-out write closes the session. Returns the
    /// writer for a connection that is taken over, or `None` when writing failed: the connection
    /// is not worth handing over then.
    pub(crate) async fn run_writer(
        self,
        mut writer: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Option<Box<dyn AsyncWrite + Send + Unpin>> {
        let send_timeout = self.inner.server.options().send_timeout;
        if let Err(error) = self
            .inner
            .send_queue
            .run_writer(&mut writer, send_timeout)
            .await
        {
            let reason = if error.kind() == io::ErrorKind::TimedOut {
                CloseReason::Timeout
            } else {
                CloseReason::SocketError
            };
            tracing::debug!(session = self.id(), %error, "send failed");
            self.close(reason);
            return None;
        }
        Some(writer)
    }
}

/// The connection of a session that the application took over with [`Session::detach`]: the
/// two halves of the stream and the bytes read ahead of the last package.
pub struct Detached {
    /// The reading half of the connection, positioned after `buffered`.
    pub reader: Box<dyn AsyncRead + Send + Unpin>,
    /// The writing half of the connection; everything the session had queued has been written.
    pub writer: Box<dyn AsyncWrite + Send + Unpin>,
    /// Bytes that were received after the last package and not consumed by the pipeline filter.
    pub buffered: BytesMut,
}

impl Detached {
    /// Joins the halves into one stream, which first yields the buffered bytes.
    pub fn into_stream(self) -> BoxStream {
        Box::new(tokio::io::join(
            Rewind::new(self.buffered.freeze(), self.reader),
            self.writer,
        ))
    }
}

impl fmt::Debug for Detached {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Detached")
            .field("buffered", &self.buffered.len())
            .finish_non_exhaustive()
    }
}

/// The connection of a detaching session, once its tasks have let go of it; see
/// [`Session::detach`]. `None` when the session could not hand it over.
pub struct Detaching(oneshot::Receiver<Detached>);

impl Future for Detaching {
    type Output = Option<Detached>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(Result::ok)
    }
}

impl fmt::Debug for Detaching {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Detaching")
    }
}

/// The sessions of a server, available through [`ServerInfo::sessions`].
pub struct SessionContainer<S> {
    sessions: RwLock<HashMap<u64, Session<S>>>,
}

impl<S> Default for SessionContainer<S> {
    fn default() -> Self {
        Self {
            sessions: RwLock::default(),
        }
    }
}

impl<S> SessionContainer<S> {
    /// The session with the given id.
    pub fn get(&self, id: u64) -> Option<Session<S>> {
        self.sessions.read().unwrap().get(&id).cloned()
    }

    /// Number of connected sessions.
    pub fn count(&self) -> usize {
        self.sessions.read().unwrap().len()
    }

    /// A snapshot of the sessions matching `criteria`; no lock is held while you use them.
    pub fn find(&self, criteria: impl Fn(&Session<S>) -> bool) -> Vec<Session<S>> {
        self.sessions
            .read()
            .unwrap()
            .values()
            .filter(|&session| criteria(session))
            .cloned()
            .collect()
    }

    /// A snapshot of all sessions.
    pub fn all(&self) -> Vec<Session<S>> {
        self.sessions.read().unwrap().values().cloned().collect()
    }

    /// Runs `f` on every session without taking a snapshot. `f` runs while the container is
    /// locked, so it must be quick, must not wait, and must not call back into the container
    /// (such as [`get`](Self::get), [`find`](Self::find) or [`all`](Self::all)), which would
    /// deadlock. Sending through a session is fine: for broadcasts, pair it with
    /// [`Session::try_send`], which skips sessions whose send buffer is full. For anything that
    /// needs to look sessions up, take a snapshot with [`all`](Self::all) or [`find`](Self::find)
    /// first.
    pub fn for_each(&self, mut f: impl FnMut(&Session<S>)) {
        for session in self.sessions.read().unwrap().values() {
            f(session);
        }
    }

    pub(crate) fn add(&self, session: Session<S>) {
        self.sessions.write().unwrap().insert(session.id(), session);
    }

    pub(crate) fn remove(&self, id: u64) {
        self.sessions.write().unwrap().remove(&id);
    }
}

impl<S> fmt::Debug for SessionContainer<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionContainer")
            .field("count", &self.count())
            .finish()
    }
}
