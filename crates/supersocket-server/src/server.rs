use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use supersocket_connection::{Stream, catch_panic};
use supersocket_proto::{BoxFilter, PipelineFilter};
use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::Semaphore,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    BoxError, BoxFuture, Error, HandlerResult, connection,
    layer::{DynStreamLayer, StreamLayer},
    middleware::{DynMiddleware, Middleware},
    options::{ListenOptions, ServerOptions},
    session::{CloseReason, ConnectionInfo, Session, SessionContainer, SessionState},
};

/// Information about a server, available to every session through [`Session::server`].
pub struct ServerInfo<S = ()> {
    options: ServerOptions,
    sessions: SessionContainer<S>,
    services: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl<S> ServerInfo<S> {
    /// Name of the server.
    pub fn name(&self) -> &str {
        &self.options.name
    }

    /// Options the server was built with.
    pub fn options(&self) -> &ServerOptions {
        &self.options
    }

    /// The connected sessions.
    pub fn sessions(&self) -> &SessionContainer<S> {
        &self.sessions
    }

    /// Number of connected sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.count()
    }

    /// The service of type `T` registered with [`ServerBuilder::service`].
    pub fn service<T: Any>(&self) -> Option<&T> {
        self.services
            .get(&TypeId::of::<T>())
            .and_then(|service| service.downcast_ref())
    }
}

impl<S> fmt::Debug for ServerInfo<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerInfo")
            .field("name", &self.options.name)
            .field("sessions", &self.sessions.count())
            .finish_non_exhaustive()
    }
}

/// Why handling a package failed, passed to the handler set with [`ServerBuilder::on_error`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PackageError {
    /// The handler returned an error.
    #[error("package handler failed: {0}")]
    Handler(#[source] BoxError),
    /// The handler did not finish within the package handling timeout.
    #[error("package handling timed out after {0:?}")]
    Timeout(Duration),
    /// The handler panicked; the value is the panic message.
    #[error("package handler panicked: {0}")]
    Panicked(String),
}

/// What to do with a session after handling one of its packages failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorAction {
    /// Keep the session and go on with its next package.
    Continue,
    /// Close the session with [`CloseReason::ApplicationError`].
    Close,
}

/// Handles the packages of every session, like SuperSocket's `IPackageHandler`. Closures passed to
/// [`ServerBuilder::on_package`] cover most needs; implement this for a handler that is a type,
/// such as a protocol layer, and register it with [`ServerBuilder::package_handler`].
///
/// ```
/// use supersocket_proto::{LineFilter, TextPackage};
/// use supersocket_server::{CloseReason, HandlerResult, PackageHandler, Server, Session};
///
/// struct Echo;
///
/// impl PackageHandler<TextPackage> for Echo {
///     async fn handle(&self, session: &Session, package: TextPackage) -> HandlerResult {
///         session.send(format!("{}\r\n", package.text)).await?;
///         Ok(())
///     }
///
///     async fn closing(&self, session: &Session, _reason: CloseReason) {
///         let _ = session.send("bye\r\n").await;
///     }
/// }
///
/// let builder = Server::builder(LineFilter::default).package_handler(Echo);
/// ```
pub trait PackageHandler<P, S = ()>: Send + Sync + 'static {
    /// Handles one package of `session`. Errors and panics go to the handler set with
    /// [`ServerBuilder::on_error`], and the call is cut off after the package handling timeout.
    fn handle(
        &self,
        session: &Session<S>,
        package: P,
    ) -> impl Future<Output = HandlerResult> + Send;

    /// Runs when a connected session is closing, after its last package was handled and before
    /// the data still queued is sent, so a protocol can queue a goodbye message such as a
    /// WebSocket close frame. Limited by the close timeout.
    fn closing(
        &self,
        _session: &Session<S>,
        _reason: CloseReason,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
}

type FilterFactory<P> = Arc<dyn Fn() -> BoxFilter<P> + Send + Sync>;
/// Handles one package within a time limit. The returned future runs the application's code
/// together with the timeout and panic catching, so they share the one allocation each package
/// needs, and it borrows the handler instead of cloning shared state for every package.
pub(crate) trait DynPackageHandler<P, S>: Send + Sync {
    fn handle<'a>(
        &'a self,
        session: Session<S>,
        package: P,
        limit: Duration,
    ) -> BoxFuture<'a, Result<(), PackageError>>;

    /// The [`PackageHandler::closing`] hook, if the handler has one.
    fn closing<'a>(
        &'a self,
        _session: &'a Session<S>,
        _reason: CloseReason,
    ) -> Option<BoxFuture<'a, ()>> {
        None
    }
}

/// Runs the future `start` creates, with a time limit, turning panics into errors.
pub(crate) async fn handle_with_limit<Fut>(
    limit: Duration,
    start: impl FnOnce() -> Fut,
) -> Result<(), PackageError>
where
    Fut: Future<Output = HandlerResult>,
{
    match catch_panic(async { tokio::time::timeout(limit, start()).await }).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(PackageError::Handler(error)),
        // Dropping the handler's future cancels it at its current await point.
        Ok(Err(_)) => Err(PackageError::Timeout(limit)),
        Err(panic) => Err(PackageError::Panicked(panic)),
    }
}

/// Runs a [`PackageHandler`] registered with [`ServerBuilder::package_handler`].
struct TypedHandler<H>(H);

impl<P, S, H> DynPackageHandler<P, S> for TypedHandler<H>
where
    P: Send + 'static,
    S: SessionState,
    H: PackageHandler<P, S>,
{
    fn handle<'a>(
        &'a self,
        session: Session<S>,
        package: P,
        limit: Duration,
    ) -> BoxFuture<'a, Result<(), PackageError>> {
        Box::pin(handle_with_limit(limit, move || async move {
            self.0.handle(&session, package).await
        }))
    }

    fn closing<'a>(
        &'a self,
        session: &'a Session<S>,
        reason: CloseReason,
    ) -> Option<BoxFuture<'a, ()>> {
        Some(Box::pin(self.0.closing(session, reason)))
    }
}

struct FnHandler<F>(F);

impl<P, S, F, Fut> DynPackageHandler<P, S> for FnHandler<F>
where
    P: Send + 'static,
    S: SessionState,
    F: Fn(Session<S>, P) -> Fut + Send + Sync,
    Fut: Future<Output = HandlerResult> + Send + 'static,
{
    fn handle<'a>(
        &'a self,
        session: Session<S>,
        package: P,
        limit: Duration,
    ) -> BoxFuture<'a, Result<(), PackageError>> {
        Box::pin(handle_with_limit(limit, move || (self.0)(session, package)))
    }
}
type ConnectedHandler<S> = Arc<dyn Fn(Session<S>) -> BoxFuture<'static, ()> + Send + Sync>;
type ClosedHandler<S> =
    Arc<dyn Fn(Session<S>, CloseReason) -> BoxFuture<'static, ()> + Send + Sync>;
type ErrorHandler<S> =
    Arc<dyn Fn(Session<S>, PackageError) -> BoxFuture<'static, ErrorAction> + Send + Sync>;
type SocketConfigurator = Box<dyn Fn(&TcpStream) + Send + Sync>;
type AcceptHook = Box<dyn Fn(&ConnectionInfo) -> bool + Send + Sync>;
type SessionFactory<S> = Arc<dyn Fn(&ConnectionInfo) -> S + Send + Sync>;

/// Builds a [`Server`]. Created by [`Server::builder`].
pub struct ServerBuilder<P, S = ()> {
    filter_factory: FilterFactory<P>,
    options: ServerOptions,
    services: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    package_handler: Option<Box<dyn DynPackageHandler<P, S>>>,
    on_connected: Option<ConnectedHandler<S>>,
    on_closed: Option<ClosedHandler<S>>,
    on_error: Option<ErrorHandler<S>>,
    middlewares: Vec<Box<dyn DynMiddleware<S>>>,
    handshake_required: bool,
    configure_socket: Option<SocketConfigurator>,
    on_accept: Option<AcceptHook>,
    layers: Vec<Box<dyn DynStreamLayer>>,
    session_factory: SessionFactory<S>,
}

impl<P: Send + 'static> ServerBuilder<P> {
    /// Uses `S` as the per-session state, created with `Default` for each session.
    ///
    /// # Panics
    ///
    /// Handlers, commands and middleware depend on the session type, so this must be called
    /// before any of them is registered; otherwise it panics.
    pub fn session<S: SessionState + Default>(self) -> ServerBuilder<P, S> {
        self.session_with(|_| S::default())
    }

    /// Uses `S` as the per-session state, created by `factory` for each session from where its
    /// connection comes from: the addresses, the PROXY protocol header or the TLS client
    /// certificate. Like [`session`](Self::session), this must be called before handlers,
    /// commands or middleware are registered.
    ///
    /// # Panics
    ///
    /// Panics if a handler, commands or middleware were registered already.
    pub fn session_with<S: SessionState>(
        self,
        factory: impl Fn(&ConnectionInfo) -> S + Send + Sync + 'static,
    ) -> ServerBuilder<P, S> {
        assert!(
            self.package_handler.is_none()
                && self.on_connected.is_none()
                && self.on_closed.is_none()
                && self.on_error.is_none()
                && self.middlewares.is_empty(),
            "the session state must be chosen before registering handlers, commands or middleware"
        );
        ServerBuilder {
            filter_factory: self.filter_factory,
            options: self.options,
            services: self.services,
            package_handler: None,
            on_connected: None,
            on_closed: None,
            on_error: None,
            middlewares: Vec::new(),
            handshake_required: self.handshake_required,
            configure_socket: self.configure_socket,
            on_accept: self.on_accept,
            layers: self.layers,
            session_factory: Arc::new(factory),
        }
    }
}

impl<P: Send + 'static, S: SessionState> ServerBuilder<P, S> {
    /// Handles every package. Packages of a session are handled one at a time unless
    /// [`PackageHandling::Concurrent`](crate::PackageHandling::Concurrent) is configured.
    pub fn on_package<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(Session<S>, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        self.set_package_handler(Box::new(FnHandler(handler)))
    }

    /// Handles every package with `handler`, for handlers that are types rather than closures.
    pub fn package_handler(self, handler: impl PackageHandler<P, S>) -> Self {
        self.set_package_handler(Box::new(TypedHandler(handler)))
    }

    pub(crate) fn set_package_handler(mut self, handler: Box<dyn DynPackageHandler<P, S>>) -> Self {
        self.package_handler = Some(handler);
        self
    }

    /// Requires every session to complete a handshake before it counts as connected, like
    /// SuperSocket's `IHandshakeRequiredSession`. Protocols such as WebSocket use it.
    ///
    /// Until the package handler calls [`Session::complete_handshake`], the session is not in the
    /// session container, so broadcasts skip it, its connected handlers have not run and its
    /// packages are handled one at a time. A session that does not complete the handshake within
    /// [`handshake_timeout`](ServerOptions::handshake_timeout) is closed with
    /// [`CloseReason::Timeout`]. The closed handlers run only for sessions that completed it;
    /// middleware sees every session.
    pub fn require_handshake(mut self) -> Self {
        self.handshake_required = true;
        self
    }

    /// Runs when a session connects, after [`SessionState::on_connected`].
    pub fn on_connected<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Session<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_connected = Some(Arc::new(move |session| Box::pin(handler(session))));
        self
    }

    /// Runs after a session closed, after [`SessionState::on_closed`].
    pub fn on_closed<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Session<S>, CloseReason) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_closed = Some(Arc::new(move |session, reason| {
            Box::pin(handler(session, reason))
        }));
        self
    }

    /// Decides what happens when handling a package fails or times out. Without it the error is
    /// logged and the session continues.
    pub fn on_error<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Session<S>, PackageError) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ErrorAction> + Send + 'static,
    {
        self.on_error = Some(Arc::new(move |session, error| {
            Box::pin(handler(session, error))
        }));
        self
    }

    /// Adds a middleware.
    pub fn middleware(mut self, middleware: impl Middleware<S>) -> Self {
        self.middlewares.push(Box::new(middleware));
        self
    }

    /// Runs `configure` on every accepted TCP connection, after the listener's own settings, for
    /// socket options that [`ListenOptions`] does not cover, for example through
    /// `socket2::SockRef::from(stream)`. Errors are the closure's to handle.
    pub fn configure_socket(
        mut self,
        configure: impl Fn(&TcpStream) + Send + Sync + 'static,
    ) -> Self {
        self.configure_socket = Some(Box::new(configure));
        self
    }

    /// Wraps the stream of every connection with `layer`, after any TLS handshake. Layers wrap in
    /// the order they were added, so the first one added is closest to the socket.
    pub fn stream_layer(mut self, layer: impl StreamLayer) -> Self {
        self.layers.push(Box::new(layer));
        self
    }

    /// Decides whether to serve a connection, before any TLS handshake and before it costs
    /// anything else: for bans and limits by address. It runs after the PROXY protocol header,
    /// so `remote_addr` is the client's. Returning `false` drops the connection. It must not
    /// block; for decisions that take time, use a [`Middleware`].
    pub fn on_accept(
        mut self,
        accept: impl Fn(&ConnectionInfo) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.on_accept = Some(Box::new(accept));
        self
    }

    /// Listens on `address` with plain TCP: a socket address such as `"0.0.0.0:4040"`, or a host
    /// name such as `"localhost:4040"`, which is resolved when the server is built. Use
    /// [`configure`](Self::configure) for TLS or other listener settings.
    pub fn listen(mut self, address: impl ToString) -> Self {
        self.options
            .listeners
            .push(ListenOptions::new(address.to_string()));
        self
    }

    /// Changes the options in place.
    pub fn configure(mut self, configure: impl FnOnce(&mut ServerOptions)) -> Self {
        configure(&mut self.options);
        self
    }

    /// Replaces the options, for example with ones loaded from a config file.
    pub fn options(mut self, options: ServerOptions) -> Self {
        self.options = options;
        self
    }

    /// Registers a shared service that handlers reach through [`ServerInfo::service`].
    pub fn service<T: Any + Send + Sync>(mut self, service: T) -> Self {
        self.services.insert(TypeId::of::<T>(), Box::new(service));
        self
    }

    /// Checks the options and loads TLS certificates. A server without listeners only serves the
    /// connections handed to [`Server::serve_connection`] once it is started.
    pub fn build(self) -> Result<Server<P, S>, Error> {
        self.options.validate()?;
        let listeners = self
            .options
            .listeners
            .iter()
            .map(Listener::prepare)
            .collect::<Result<Vec<_>, _>>()?;
        let mut middlewares = self.middlewares;
        middlewares.sort_by_key(|middleware| middleware.order());
        let connection_limit = self
            .options
            .max_connections
            .map(|limit| Arc::new(Semaphore::new(limit)));
        Ok(Server {
            inner: Arc::new(ServerInner {
                info: Arc::new(ServerInfo {
                    options: self.options,
                    sessions: SessionContainer::default(),
                    services: self.services,
                }),
                filter_factory: self.filter_factory,
                package_handler: self.package_handler,
                on_connected: self.on_connected,
                on_closed: self.on_closed,
                on_error: self.on_error,
                middlewares,
                handshake_required: self.handshake_required,
                configure_socket: self.configure_socket,
                on_accept: self.on_accept,
                layers: self.layers,
                session_factory: self.session_factory,
                listeners,
                connection_limit,
                next_session_id: AtomicU64::new(1),
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                accepting: CancellationToken::new(),
                shutdown: CancellationToken::new(),
                tracker: TaskTracker::new(),
            }),
        })
    }
}

/// A socket server. Build one with [`Server::builder`].
pub struct Server<P, S = ()> {
    inner: Arc<ServerInner<P, S>>,
}

pub(crate) struct ServerInner<P, S> {
    pub(crate) info: Arc<ServerInfo<S>>,
    pub(crate) filter_factory: FilterFactory<P>,
    pub(crate) package_handler: Option<Box<dyn DynPackageHandler<P, S>>>,
    pub(crate) on_connected: Option<ConnectedHandler<S>>,
    pub(crate) on_closed: Option<ClosedHandler<S>>,
    pub(crate) on_error: Option<ErrorHandler<S>>,
    pub(crate) middlewares: Vec<Box<dyn DynMiddleware<S>>>,
    pub(crate) handshake_required: bool,
    pub(crate) configure_socket: Option<SocketConfigurator>,
    pub(crate) on_accept: Option<AcceptHook>,
    pub(crate) layers: Vec<Box<dyn DynStreamLayer>>,
    pub(crate) session_factory: SessionFactory<S>,
    listeners: Vec<Arc<Listener>>,
    connection_limit: Option<Arc<Semaphore>>,
    pub(crate) next_session_id: AtomicU64,
    started: AtomicBool,
    stopped: AtomicBool,
    /// Cancelled to close the listeners while the sessions go on.
    accepting: CancellationToken,
    pub(crate) shutdown: CancellationToken,
    pub(crate) tracker: TaskTracker,
}

pub(crate) struct Listener {
    address: SocketAddr,
    pub(crate) options: ListenOptions,
    #[cfg(feature = "tls")]
    pub(crate) tls: Option<crate::tls::TlsListener>,
}

impl Listener {
    fn prepare(options: &ListenOptions) -> Result<Arc<Self>, Error> {
        let invalid = |source| Error::InvalidAddress {
            address: options.address.clone(),
            source,
        };
        // Host names are resolved once, here; a socket address resolves to itself.
        let address = std::net::ToSocketAddrs::to_socket_addrs(options.address.as_str())
            .map_err(invalid)?
            .next()
            .ok_or_else(|| {
                invalid(io::Error::new(
                    io::ErrorKind::NotFound,
                    "the host name did not resolve to any address",
                ))
            })?;
        #[cfg(not(feature = "tls"))]
        if options.tls.is_some() {
            return Err(Error::TlsDisabled(options.address.clone()));
        }
        Ok(Arc::new(Self {
            address,
            options: options.clone(),
            #[cfg(feature = "tls")]
            tls: options
                .tls
                .as_ref()
                .map(|tls| crate::tls::TlsListener::new(&options.address, tls))
                .transpose()?,
        }))
    }

    fn is_secure(&self) -> bool {
        self.options.tls.is_some()
    }

    fn bind(&self) -> Result<TcpListener, Error> {
        let bind = || {
            let socket = if self.address.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            // On Windows SO_REUSEADDR would let another socket bind a port that is in use.
            #[cfg(not(windows))]
            socket.set_reuseaddr(true)?;
            // The receive buffer must be set before listening to count for the window scaling
            // of new connections. Each accepted connection gets the sizes again in `configure`.
            if let Some(size) = self.options.socket_receive_buffer_size {
                socket.set_recv_buffer_size(size)?;
            }
            if let Some(size) = self.options.socket_send_buffer_size {
                socket.set_send_buffer_size(size)?;
            }
            socket.bind(self.address)?;
            socket.listen(self.options.backlog)
        };
        bind().map_err(|source| Error::Bind {
            address: self.address,
            source,
        })
    }

    /// Applies the listener's settings to a connection it accepted.
    pub(crate) fn configure(&self, stream: &TcpStream) {
        let _ = stream.set_nodelay(self.options.no_delay);
        let socket = socket2::SockRef::from(stream);
        // Linux passes the listener's buffer sizes on to accepted connections, but macOS does not.
        if let Some(size) = self.options.socket_receive_buffer_size {
            if let Err(error) = socket.set_recv_buffer_size(size as usize) {
                tracing::debug!(%error, "setting the receive buffer size failed");
            }
        }
        if let Some(size) = self.options.socket_send_buffer_size {
            if let Err(error) = socket.set_send_buffer_size(size as usize) {
                tracing::debug!(%error, "setting the send buffer size failed");
            }
        }
        if let Some(time) = self.options.keepalive {
            let keepalive = socket2::TcpKeepalive::new().with_time(time);
            if let Err(error) = socket.set_tcp_keepalive(&keepalive) {
                tracing::debug!(%error, "enabling TCP keepalive failed");
            }
        }
    }
}

impl<P: Send + 'static> Server<P> {
    /// Starts building a server. `make_filter` creates the pipeline filter of each connection,
    /// and the package type of the server is the filter's package type.
    ///
    /// ```
    /// use supersocket_proto::{LineFilter, TerminatorFilter, TextPackage};
    /// use supersocket_server::Server;
    ///
    /// let lines = Server::builder(LineFilter::default);
    /// let custom = Server::builder(|| TerminatorFilter::new(&b"##"[..], TextPackage::decode));
    /// ```
    pub fn builder<F, M>(make_filter: M) -> ServerBuilder<P>
    where
        M: Fn() -> F + Send + Sync + 'static,
        F: PipelineFilter<Package = P>,
    {
        ServerBuilder {
            filter_factory: Arc::new(move || Box::new(make_filter())),
            options: ServerOptions::default(),
            services: HashMap::new(),
            package_handler: None,
            on_connected: None,
            on_closed: None,
            on_error: None,
            middlewares: Vec::new(),
            handshake_required: false,
            configure_socket: None,
            on_accept: None,
            layers: Vec::new(),
            session_factory: Arc::new(|_| ()),
        }
    }
}

impl<P: Send + 'static, S: SessionState> Server<P, S> {
    /// Information about the server, including its sessions.
    pub fn info(&self) -> &ServerInfo<S> {
        &self.inner.info
    }

    /// Binds all listeners and starts accepting connections. Returns the bound addresses, which
    /// tells the actual port when a listener uses port 0.
    pub fn start(&self) -> Result<Vec<SocketAddr>, Error> {
        let server = &self.inner;
        if server.started.swap(true, Ordering::SeqCst) {
            return Err(Error::AlreadyStarted);
        }
        // Binding can fail, most often because a port is taken; let the caller try again rather
        // than leaving the server permanently marked as started.
        let bind = || {
            let bound = server
                .listeners
                .iter()
                .map(|listener| Ok((listener.bind()?, listener.clone())))
                .collect::<Result<Vec<_>, Error>>()?;
            let addresses = bound
                .iter()
                .map(|(tcp, _)| tcp.local_addr())
                .collect::<io::Result<Vec<_>>>()?;
            Ok::<_, Error>((bound, addresses))
        };
        let (bound, addresses) = match bind() {
            Ok(result) => result,
            Err(error) => {
                server.started.store(false, Ordering::SeqCst);
                return Err(error);
            }
        };
        if server.package_handler.is_none() {
            tracing::warn!(
                server = server.info.name(),
                "no package handler or commands configured, packages are dropped"
            );
        }
        if server.listeners.is_empty() {
            tracing::warn!(
                server = server.info.name(),
                "no listeners configured, only connections passed to serve_connection are served"
            );
        }
        for (index, middleware) in server.middlewares.iter().enumerate() {
            if let Err(error) = middleware.start(&server.info) {
                // Undo what started: the listeners close when `bound` is dropped.
                for started in server.middlewares[..index].iter().rev() {
                    started.shutdown(&server.info);
                }
                server.started.store(false, Ordering::SeqCst);
                return Err(Error::Middleware(error));
            }
        }
        for ((tcp, listener), address) in bound.into_iter().zip(&addresses) {
            tracing::info!(server = server.info.name(), %address, tls = listener.is_secure(), "listening");
            server
                .tracker
                .spawn(accept_loop(server.clone(), tcp, listener));
        }
        if let Some(timeout) = server.info.options.idle_session_timeout {
            server
                .tracker
                .spawn(clear_idle_sessions(server.clone(), timeout));
        }
        Ok(addresses)
    }

    /// Runs a session on a connection that did not come through the server's listeners, such as
    /// a Unix socket, stdin and stdout joined with `tokio::io::join`, a connection an HTTP server
    /// upgraded, or an in-memory pipe in a test. The session is like any other: the accept hook
    /// and middleware see it, stream layers wrap it, it joins the session container, and shutting
    /// down closes it. `connection` tells its addresses. Fails with [`Error::NotRunning`] before
    /// [`start`](Self::start) and after [`shutdown`](Self::shutdown), and with
    /// [`Error::ConnectionLimit`] at [`max_connections`](ServerOptions::max_connections). Must be
    /// called from within a Tokio runtime.
    pub fn serve_connection(
        &self,
        stream: impl Stream,
        connection: ConnectionInfo,
    ) -> Result<(), Error> {
        let server = &self.inner;
        if !server.started.load(Ordering::SeqCst) || server.shutdown.is_cancelled() {
            return Err(Error::NotRunning);
        }
        let permit = match &server.connection_limit {
            Some(limit) => Some(
                limit
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::ConnectionLimit)?,
            ),
            None => None,
        };
        server.tracker.spawn(connection::serve_stream(
            server.clone(),
            Box::new(stream),
            connection,
            permit,
        ));
        Ok(())
    }

    /// Closes the listeners but keeps serving the sessions, to drain before a shutdown: a load
    /// balancer stops sending new connections while the existing ones finish. Connections handed
    /// to [`serve_connection`](Self::serve_connection) are still served.
    pub fn stop_accepting(&self) {
        tracing::info!(
            server = self.inner.info.name(),
            "no longer accepting connections"
        );
        self.inner.accepting.cancel();
    }

    /// Stops accepting connections, closes every session with [`CloseReason::ServerShutdown`] and
    /// waits for them to finish.
    pub async fn shutdown(&self) {
        let server = &self.inner;
        tracing::info!(server = server.info.name(), "shutting down");
        server.shutdown.cancel();
        server.tracker.close();
        server.tracker.wait().await;
        if !server.stopped.swap(true, Ordering::SeqCst) {
            for middleware in &server.middlewares {
                middleware.shutdown(&server.info);
            }
        }
        tracing::info!(server = server.info.name(), "stopped");
    }

    /// Starts the server, waits for Ctrl+C or SIGTERM, then shuts down gracefully.
    pub async fn run(self) -> Result<(), Error> {
        self.run_until(shutdown_signal()).await
    }

    /// Starts the server, waits for `signal` to complete, then shuts down gracefully; for
    /// servers that stop on something other than a signal, such as a message from an
    /// administration endpoint.
    pub async fn run_until<T>(self, signal: impl Future<Output = T>) -> Result<(), Error> {
        self.start()?;
        signal.await;
        self.shutdown().await;
        Ok(())
    }
}

impl<P, S> Clone for Server<P, S> {
    /// A handle to the same server, for tasks that serve connections or shut it down.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P, S> fmt::Debug for Server<P, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("info", &self.inner.info)
            .finish_non_exhaustive()
    }
}

async fn accept_loop<P: Send + 'static, S: SessionState>(
    server: Arc<ServerInner<P, S>>,
    tcp: TcpListener,
    listener: Arc<Listener>,
) {
    loop {
        tokio::select! {
            _ = server.shutdown.cancelled() => return,
            _ = server.accepting.cancelled() => return,
            accepted = tcp.accept() => match accepted {
                // The TLS handshake and session setup run in the connection's own task, so a slow
                // client never holds up the accept loop.
                Ok((stream, _)) => {
                    // Idle listeners must not reserve connection capacity. Each listener holds
                    // at most this one accepted socket while waiting, and accepts no more until
                    // a session ends. Cancelling the wait drops the socket too.
                    let permit = match &server.connection_limit {
                        Some(limit) => tokio::select! {
                            biased;
                            _ = server.shutdown.cancelled() => return,
                            _ = server.accepting.cancelled() => return,
                            permit = limit.clone().acquire_owned() => {
                                Some(permit.expect("the connection limit is never closed"))
                            }
                        },
                        None => None,
                    };
                    if !server.layers.is_empty() {
                        server.tracker.spawn(connection::serve_layered_tcp(server.clone(), stream, listener.clone(), permit));
                        continue;
                    }
                    #[cfg(feature = "tls")]
                    if listener.tls.is_some() {
                        server.tracker.spawn(connection::serve_tls(server.clone(), stream, listener.clone(), permit));
                        continue;
                    }
                    server.tracker.spawn(connection::serve(server.clone(), stream, listener.clone(), permit));
                }
                Err(error) => {
                    // Typically running out of file descriptors; back off instead of spinning.
                    tracing::error!(server = server.info.name(), %error, "accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        }
    }
}

async fn clear_idle_sessions<P: Send + 'static, S: SessionState>(
    server: Arc<ServerInner<P, S>>,
    timeout: Duration,
) {
    let mut interval = tokio::time::interval(server.info.options.clear_idle_session_interval);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = server.shutdown.cancelled() => return,
            _ = interval.tick() => {
                let now = Instant::now();
                for session in server.info.sessions.find(|session| now.duration_since(session.last_active_time()) >= timeout) {
                    tracing::debug!(session = session.id(), "closing idle session");
                    session.close(CloseReason::Timeout);
                }
            }
        }
    }
}

/// Completes when the process is asked to stop: on Ctrl+C, or on Unix also on SIGTERM. This is
/// what [`Server::run`] waits for, for servers with a run loop of their own.
#[cfg(unix)]
pub async fn shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

/// Completes when the process is asked to stop: on Ctrl+C, or on Unix also on SIGTERM. This is
/// what [`Server::run`] waits for, for servers with a run loop of their own.
#[cfg(not(unix))]
pub async fn shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}
