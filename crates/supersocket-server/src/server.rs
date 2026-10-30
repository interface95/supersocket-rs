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

use supersocket_proto::{BoxFilter, PipelineFilter};
use tokio::net::{TcpListener, TcpSocket};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    BoxError, BoxFuture, Error, HandlerResult, connection,
    middleware::{DynMiddleware, Middleware},
    options::{ListenOptions, ServerOptions},
    session::{CloseReason, Session, SessionContainer, SessionState},
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
}

/// What to do with a session after handling one of its packages failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorAction {
    /// Keep the session and go on with its next package.
    Continue,
    /// Close the session with [`CloseReason::ApplicationError`].
    Close,
}

type FilterFactory<P> = Arc<dyn Fn() -> BoxFilter<P> + Send + Sync>;
pub(crate) type PackageHandler<P, S> =
    Arc<dyn Fn(Session<S>, P) -> BoxFuture<'static, HandlerResult> + Send + Sync>;
type ConnectedHandler<S> = Arc<dyn Fn(Session<S>) -> BoxFuture<'static, ()> + Send + Sync>;
type ClosedHandler<S> =
    Arc<dyn Fn(Session<S>, CloseReason) -> BoxFuture<'static, ()> + Send + Sync>;
type ErrorHandler<S> =
    Arc<dyn Fn(Session<S>, PackageError) -> BoxFuture<'static, ErrorAction> + Send + Sync>;

/// Builds a [`Server`]. Created by [`Server::builder`].
pub struct ServerBuilder<P, S = ()> {
    filter_factory: FilterFactory<P>,
    options: ServerOptions,
    services: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    package_handler: Option<PackageHandler<P, S>>,
    on_connected: Option<ConnectedHandler<S>>,
    on_closed: Option<ClosedHandler<S>>,
    on_error: Option<ErrorHandler<S>>,
    middlewares: Vec<Box<dyn DynMiddleware<S>>>,
}

impl<P: Send + 'static> ServerBuilder<P> {
    /// Uses `S` as the per-session state, created with `Default` for each session.
    ///
    /// # Panics
    ///
    /// Handlers, commands and middleware depend on the session type, so this must be called
    /// before any of them is registered; otherwise it panics.
    pub fn session<S: SessionState>(self) -> ServerBuilder<P, S> {
        assert!(
            self.package_handler.is_none()
                && self.on_connected.is_none()
                && self.on_closed.is_none()
                && self.on_error.is_none()
                && self.middlewares.is_empty(),
            "`session` must be called before registering handlers, commands or middleware"
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
        }
    }
}

impl<P: Send + 'static, S: SessionState> ServerBuilder<P, S> {
    /// Handles every package. Packages of a session are handled one at a time unless
    /// [`PackageHandling::Concurrent`](crate::PackageHandling::Concurrent) is configured.
    pub fn on_package<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Session<S>, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        self.package_handler = Some(Arc::new(move |session, package| {
            Box::pin(handler(session, package))
        }));
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

    /// Listens on `address` with plain TCP, such as `"0.0.0.0:4040"`. Use
    /// [`configure`](Self::configure) for TLS or other listener settings.
    pub fn listen(mut self, address: impl Into<String>) -> Self {
        self.options.listeners.push(ListenOptions::new(address));
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

    /// Checks the options and loads TLS certificates.
    pub fn build(self) -> Result<Server<P, S>, Error> {
        if self.options.listeners.is_empty() {
            return Err(Error::NoListener);
        }
        let listeners = self
            .options
            .listeners
            .iter()
            .map(Listener::prepare)
            .collect::<Result<Vec<_>, _>>()?;
        let mut middlewares = self.middlewares;
        middlewares.sort_by_key(|middleware| middleware.order());
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
                listeners,
                next_session_id: AtomicU64::new(1),
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
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
    pub(crate) package_handler: Option<PackageHandler<P, S>>,
    pub(crate) on_connected: Option<ConnectedHandler<S>>,
    pub(crate) on_closed: Option<ClosedHandler<S>>,
    pub(crate) on_error: Option<ErrorHandler<S>>,
    pub(crate) middlewares: Vec<Box<dyn DynMiddleware<S>>>,
    listeners: Vec<Arc<Listener>>,
    pub(crate) next_session_id: AtomicU64,
    started: AtomicBool,
    stopped: AtomicBool,
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
        let address = options
            .address
            .parse()
            .map_err(|source| Error::InvalidAddress {
                address: options.address.clone(),
                source,
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
            socket.set_reuseaddr(true)?;
            socket.bind(self.address)?;
            socket.listen(self.options.backlog)
        };
        bind().map_err(|source| Error::Bind {
            address: self.address,
            source,
        })
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
            return Err(Error::Io(io::Error::other(
                "the server has already been started",
            )));
        }
        let bound = server
            .listeners
            .iter()
            .map(|listener| Ok((listener.bind()?, listener.clone())))
            .collect::<Result<Vec<_>, Error>>()?;
        let addresses = bound
            .iter()
            .map(|(tcp, _)| tcp.local_addr())
            .collect::<io::Result<Vec<_>>>()?;
        if server.package_handler.is_none() {
            tracing::warn!(
                server = server.info.name(),
                "no package handler or commands configured, packages are dropped"
            );
        }
        for middleware in &server.middlewares {
            middleware.start(&server.info);
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
        self.start()?;
        wait_for_shutdown_signal().await?;
        self.shutdown().await;
        Ok(())
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
            accepted = tcp.accept() => match accepted {
                // The TLS handshake and session setup run in the connection's own task, so a slow
                // client never holds up the accept loop.
                Ok((stream, _)) => {
                    server.tracker.spawn(connection::serve(server.clone(), stream, listener.clone()));
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

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}
