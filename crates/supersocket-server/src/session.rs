use std::{
    collections::HashMap,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    ops::Deref,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use bytes::BytesMut;
use supersocket_proto::PackageEncoder;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::Notify,
};
use tokio_util::sync::CancellationToken;

use crate::{BoxError, extensions::Extensions, server::ServerInfo};

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
    /// The session was idle for too long, or the peer stopped accepting data.
    Timeout,
    /// The data broke the protocol, or a package exceeded the size limit.
    ProtocolError,
    /// A middleware rejected the session.
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
    /// Called after the session is registered, before its first package is handled.
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

/// A connected client. Cloning is cheap; clones refer to the same session, so a session can be
/// handed to other tasks that send to it.
pub struct Session<S = ()> {
    inner: Arc<SessionInner<S>>,
}

struct SessionInner<S> {
    id: u64,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    secure: bool,
    start_time: SystemTime,
    started_at: Instant,
    last_active_ms: AtomicU64,
    outbox: Outbox,
    close_token: CancellationToken,
    close_reason: OnceLock<CloseReason>,
    extensions: Extensions,
    server: Arc<ServerInfo<S>>,
    state: S,
}

// Data waiting to be sent. `send` appends to it and the session's writer task writes everything
// that piled up in one go, so packages handled back to back go out in one system call.
#[derive(Default)]
struct Outbox {
    state: Mutex<OutboxState>,
    data_available: Notify,
    drained: Notify,
}

#[derive(Default)]
struct OutboxState {
    pending: BytesMut,
    closed: bool,
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
            .field("remote_addr", &self.inner.remote_addr)
            .field("secure", &self.inner.secure)
            .finish_non_exhaustive()
    }
}

impl<S: SessionState> Session<S> {
    pub(crate) fn new(
        id: u64,
        remote_addr: SocketAddr,
        local_addr: SocketAddr,
        secure: bool,
        close_token: CancellationToken,
        server: Arc<ServerInfo<S>>,
    ) -> Self {
        Self {
            inner: Arc::new(SessionInner {
                id,
                remote_addr,
                local_addr,
                secure,
                start_time: SystemTime::now(),
                started_at: Instant::now(),
                last_active_ms: AtomicU64::new(0),
                outbox: Outbox::default(),
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

    /// Address of the peer.
    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.remote_addr
    }

    /// Local address the peer connected to.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Whether the connection uses TLS.
    pub fn is_secure(&self) -> bool {
        self.inner.secure
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
        let data = data.as_ref();
        let outbox = &self.inner.outbox;
        let limit = self.inner.server.options().send_buffer_limit;
        loop {
            let drained = outbox.drained.notified();
            {
                let mut state = outbox.state.lock().unwrap();
                if state.closed {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                // Always accept data when nothing is waiting, so a limit of zero or a send larger
                // than the limit cannot block forever.
                if state.pending.is_empty() || state.pending.len() < limit {
                    state.pending.extend_from_slice(data);
                    break;
                }
            }
            drained.await;
        }
        outbox.data_available.notify_one();
        Ok(())
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
    pub(crate) fn close_outbox(&self) {
        let outbox = &self.inner.outbox;
        outbox.state.lock().unwrap().closed = true;
        outbox.data_available.notify_one();
        outbox.drained.notify_waiters();
    }

    /// The session's writer task: writes whatever piled up in the outbox in one go.
    pub(crate) async fn run_writer(self, mut writer: Box<dyn AsyncWrite + Send + Unpin>) {
        let outbox = &self.inner.outbox;
        let send_timeout = self.inner.server.options().send_timeout;
        loop {
            let chunk = {
                let mut state = outbox.state.lock().unwrap();
                if state.pending.is_empty() {
                    if state.closed {
                        break;
                    }
                    None
                } else {
                    // Take the whole buffer: it is freed once written, so idle sessions hold no
                    // send buffer.
                    Some(std::mem::take(&mut state.pending))
                }
            };
            let Some(chunk) = chunk else {
                outbox.data_available.notified().await;
                continue;
            };
            outbox.drained.notify_waiters();
            let write = async {
                writer.write_all(&chunk).await?;
                writer.flush().await
            };
            let written = match send_timeout {
                Some(limit) => tokio::time::timeout(limit, write)
                    .await
                    .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into())),
                None => write.await,
            };
            if let Err(error) = written {
                let reason = if error.kind() == io::ErrorKind::TimedOut {
                    CloseReason::Timeout
                } else {
                    CloseReason::SocketError
                };
                tracing::debug!(session = self.id(), %error, "send failed");
                outbox.state.lock().unwrap().closed = true;
                outbox.drained.notify_waiters();
                self.close(reason);
                break;
            }
        }
        let _ = writer.shutdown().await;
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
