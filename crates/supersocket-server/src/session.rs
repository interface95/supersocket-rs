use std::{
    collections::HashMap,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    ops::Deref,
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use bytes::BytesMut;
use supersocket_connection::SendQueue;
use supersocket_proto::PackageEncoder;
use tokio::io::AsyncWrite;
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
        })
    }
}

/// Per-session state, created with `Default` for every new session.
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
pub trait SessionState: Default + Send + Sync + 'static {
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
    last_active_ms: AtomicU64,
    handshake_complete: AtomicBool,
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
                last_active_ms: AtomicU64::new(0),
                handshake_complete: AtomicBool::new(handshake_complete),
                send_queue,
                close_token,
                close_reason: OnceLock::new(),
                extensions: Extensions::default(),
                server,
                state: S::default(),
            }),
        }
    }
}

impl<S> Session<S> {
    /// Unique id of the session within its server.
    pub fn id(&self) -> u64 {
        self.inner.id
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

    /// When data was last received.
    pub fn last_active_time(&self) -> Instant {
        self.inner.started_at
            + Duration::from_millis(self.inner.last_active_ms.load(Ordering::Relaxed))
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

    /// Queues `data` to be sent. Data from concurrent calls is never interleaved. When more than
    /// [`send_buffer_limit`](crate::ServerOptions::send_buffer_limit) bytes are already waiting,
    /// this waits until the writer has taken them. Fails once the session is closing.
    pub async fn send(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.inner.send_queue.send(data.as_ref()).await
    }

    /// Queues `parts` back to back as one piece, like [`send`](Self::send) with the parts joined,
    /// but without copying them together first. Protocol layers use it to send a header and a
    /// payload.
    pub async fn send_vectored(&self, parts: &[&[u8]]) -> io::Result<()> {
        self.inner.send_queue.send_vectored(parts).await
    }

    /// Queues `data` as the last data of the session, without waiting for room: later sends
    /// fail, while what is queued is still sent and the session keeps reading. For a goodbye
    /// message that nothing may follow, such as a WebSocket close frame.
    pub fn send_last(&self, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.inner.send_queue.send_last(data.as_ref())
    }

    /// Encodes `package` with `encoder` and queues it like [`send`](Self::send).
    pub async fn send_package<T: ?Sized>(
        &self,
        encoder: &impl PackageEncoder<T>,
        package: &T,
    ) -> Result<(), BoxError> {
        let mut buf = BytesMut::new();
        encoder.encode(package, &mut buf)?;
        self.send(&buf).await?;
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

    pub(crate) fn touch(&self) {
        let elapsed = self.inner.started_at.elapsed().as_millis() as u64;
        self.inner.last_active_ms.store(elapsed, Ordering::Relaxed);
    }

    /// Stops accepting data to send; the writer task sends what is queued and shuts down.
    pub(crate) fn close_send_queue(&self) {
        self.inner.send_queue.close();
    }

    /// The session's writer task. A failed or timed-out write closes the session.
    pub(crate) async fn run_writer(self, writer: Box<dyn AsyncWrite + Send + Unpin>) {
        let send_timeout = self.inner.server.options().send_timeout;
        if let Err(error) = self.inner.send_queue.run_writer(writer, send_timeout).await {
            let reason = if error.kind() == io::ErrorKind::TimedOut {
                CloseReason::Timeout
            } else {
                CloseReason::SocketError
            };
            tracing::debug!(session = self.id(), %error, "send failed");
            self.close(reason);
        }
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
