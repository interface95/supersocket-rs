use std::{
    any::Any,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use supersocket_server::{
    CloseReason, ConnectionInfo, Error, ErrorAction, HandlerResult, ListenOptions, Middleware,
    PackageError, PackageHandler, Server, ServerBuilder, ServerInfo, ServerOptions, Session,
    SessionState, StreamLayer,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Accept, CloseCode, CloseFrame, HandshakeRequest, Message, Rejection, WebSocketSession,
    filter::{HeadFilter, Package, Role, WebSocketPackage},
    frame::{MAX_CLOSE_FRAME, MAX_HEADER_SIZE, OP_PING, OP_PONG, close_frame, encode_header},
    handshake::{Head, HeadError, accept_response, read_request},
};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type CoreBuilder<S> = ServerBuilder<WebSocketPackage, WebSocketState<S>>;
type Deferred<S> = Box<dyn FnOnce(CoreBuilder<S>) -> CoreBuilder<S> + Send>;
type InstallHandler<S> = Box<dyn FnOnce(CoreBuilder<S>, Settings<S>) -> CoreBuilder<S> + Send>;
type HandshakeHandler<S> =
    Box<dyn Fn(WebSocketSession<S>) -> BoxFuture<Result<Accept, Rejection>> + Send + Sync>;
type StateFactory<S> = Arc<dyn Fn(&ConnectionInfo) -> S + Send + Sync>;

/// The session state of a WebSocket server: the state of the WebSocket connection and your own,
/// `S`, which [`WebSocketSession`] dereferences to.
pub struct WebSocketState<S = ()> {
    pub(crate) request: OnceLock<HandshakeRequest>,
    pub(crate) protocol: OnceLock<String>,
    /// Whether a close frame has been sent; nothing but control frames may follow it.
    pub(crate) close_sent: AtomicBool,
    pub(crate) close_frame: OnceLock<CloseFrame>,
    pub(crate) user: S,
}

impl<S> WebSocketState<S> {
    fn new(user: S) -> Self {
        Self {
            request: OnceLock::new(),
            protocol: OnceLock::new(),
            close_sent: AtomicBool::new(false),
            close_frame: OnceLock::new(),
            user,
        }
    }
}

impl<S: Default> Default for WebSocketState<S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S: Send + Sync + 'static> SessionState for WebSocketState<S> {}

impl<S> fmt::Debug for WebSocketState<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketState")
            .field("path", &self.request.get().map(HandshakeRequest::path))
            .field("protocol", &self.protocol.get())
            .field("close_sent", &self.close_sent.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// A WebSocket server, like SuperSocket's WebSocket server. Build one with
/// [`WebSocketServer::builder`].
pub struct WebSocketServer<S = ()> {
    server: Server<WebSocketPackage, WebSocketState<S>>,
    keepalive: Option<Keepalive>,
    /// Stops the keepalive task.
    stop: CancellationToken,
    /// The running keepalive task, awaited on shutdown.
    watchdog: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// How often to ping, and how long without data from a client is too long.
#[derive(Debug, Clone, Copy)]
struct Keepalive {
    ping_interval: Duration,
    pong_timeout: Option<Duration>,
}

impl WebSocketServer {
    /// Starts building a WebSocket server.
    pub fn builder() -> WebSocketBuilder {
        WebSocketBuilder {
            options: ServerOptions::default(),
            subprotocols: Vec::new(),
            on_handshake: None,
            install: None,
            deferred: Vec::new(),
            state_factory: Arc::new(|_| ()),
            keepalive: None,
        }
    }
}

impl<S: Send + Sync + 'static> WebSocketServer<S> {
    /// Information about the server, including its sessions.
    pub fn info(&self) -> &ServerInfo<WebSocketState<S>> {
        self.server.info()
    }

    /// The connected sessions, those that completed the handshake.
    pub fn sessions(&self) -> Vec<WebSocketSession<S>> {
        sessions(self.info())
    }

    /// Binds all listeners and starts accepting connections; see [`Server::start`].
    pub fn start(&self) -> Result<Vec<SocketAddr>, Error> {
        let addresses = self.server.start()?;
        if let Some(keepalive) = self.keepalive {
            let task = tokio::spawn(run_keepalive(
                self.server.clone(),
                keepalive,
                self.stop.clone(),
            ));
            *self.watchdog.lock().unwrap() = Some(task);
        }
        Ok(addresses)
    }

    /// Closes the listeners but keeps serving the sessions; see [`Server::stop_accepting`].
    pub fn stop_accepting(&self) {
        self.server.stop_accepting();
    }

    /// Stops accepting connections and closes every session, sending close code 1001 (going
    /// away); see [`Server::shutdown`].
    pub async fn shutdown(&self) {
        self.stop.cancel();
        // Wait for the keepalive task to see the cancellation and stop, so that shutting down
        // truly waits for everything, like the server's own tasks.
        let watchdog = self.watchdog.lock().unwrap().take();
        if let Some(watchdog) = watchdog {
            let _ = watchdog.await;
        }
        self.server.shutdown().await;
    }

    /// Starts the server, waits for Ctrl+C or SIGTERM, then shuts down gracefully.
    pub async fn run(self) -> Result<(), Error> {
        self.run_until(supersocket_server::shutdown_signal()).await
    }

    /// Starts the server, waits for `signal` to complete, then shuts down gracefully.
    pub async fn run_until<T>(self, signal: impl Future<Output = T>) -> Result<(), Error> {
        self.start()?;
        signal.await;
        self.shutdown().await;
        Ok(())
    }

    /// Runs a session on a connection from another transport; see [`Server::serve_connection`].
    pub fn serve_connection(
        &self,
        stream: impl supersocket_server::Stream,
        connection: ConnectionInfo,
    ) -> Result<(), Error> {
        self.server.serve_connection(stream, connection)
    }
}

impl<S> fmt::Debug for WebSocketServer<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketServer")
            .field("server", &self.server)
            .finish()
    }
}

pub(crate) fn sessions<S>(info: &ServerInfo<WebSocketState<S>>) -> Vec<WebSocketSession<S>> {
    info.sessions()
        .all()
        .into_iter()
        .map(WebSocketSession::from)
        .collect()
}

/// Builds a [`WebSocketServer`]. Created by [`WebSocketServer::builder`].
///
/// The options are the server's options. `max_package_length` limits both a frame, header
/// included, and a message joined from fragments.
pub struct WebSocketBuilder<S = ()> {
    options: ServerOptions,
    subprotocols: Vec<String>,
    on_handshake: Option<HandshakeHandler<S>>,
    install: Option<InstallHandler<S>>,
    // Applied to the server builder at the end, once the options, which the filter needs, are
    // final.
    deferred: Vec<Deferred<S>>,
    state_factory: StateFactory<S>,
    keepalive: Option<Keepalive>,
}

impl WebSocketBuilder {
    /// Uses `S` as your per-session state, created with `Default` for each session and reachable
    /// through [`WebSocketSession`].
    ///
    /// # Panics
    ///
    /// Handlers and middleware depend on the session type, so this must be called before any of
    /// them is registered; otherwise it panics.
    pub fn session<S: Default + Send + Sync + 'static>(self) -> WebSocketBuilder<S> {
        self.session_with(|_| S::default())
    }

    /// Uses `S` as your per-session state, created by `factory` for each connection from its
    /// addresses, PROXY protocol header or TLS client certificate; see
    /// [`ServerBuilder::session_with`]. Like [`session`](Self::session), this must be called
    /// before handlers or middleware are registered.
    ///
    /// # Panics
    ///
    /// Panics if a handler or middleware was registered already.
    pub fn session_with<S: Send + Sync + 'static>(
        self,
        factory: impl Fn(&ConnectionInfo) -> S + Send + Sync + 'static,
    ) -> WebSocketBuilder<S> {
        assert!(
            self.on_handshake.is_none() && self.install.is_none() && self.deferred.is_empty(),
            "the session state must be chosen before registering handlers or middleware"
        );
        WebSocketBuilder {
            options: self.options,
            subprotocols: self.subprotocols,
            on_handshake: None,
            install: None,
            deferred: Vec::new(),
            state_factory: Arc::new(factory),
            keepalive: self.keepalive,
        }
    }
}

impl<S: Send + Sync + 'static> WebSocketBuilder<S> {
    /// Handles every text and binary message. Messages of a session are handled one at a time
    /// unless concurrent package handling is configured.
    pub fn on_message<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WebSocketSession<S>, Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        self.install = Some(Box::new(move |builder, settings| {
            builder.package_handler(Handler {
                settings,
                on_message: handler,
            })
        }));
        self
    }

    /// Decides whether to accept a handshake, for example by checking a token in
    /// [`request`](WebSocketSession::request). Returning a [`Rejection`] answers with that HTTP
    /// error and closes the connection; an [`Accept`] may add header fields to the response,
    /// such as a cookie. The session can be given data here, such as the user it belongs to.
    pub fn on_handshake<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WebSocketSession<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Accept, Rejection>> + Send + 'static,
    {
        self.on_handshake = Some(Box::new(move |session| Box::pin(handler(session))));
        self
    }

    /// Pings every connected client this often. Browsers answer with a pong, which counts as
    /// activity, so with [`pong_timeout`](Self::pong_timeout) connections whose peer vanished are
    /// noticed and closed. Default: no pings.
    pub fn ping_interval(mut self, interval: Duration) -> Self {
        let keepalive = self.keepalive.get_or_insert(Keepalive {
            ping_interval: interval,
            pong_timeout: None,
        });
        keepalive.ping_interval = interval;
        self
    }

    /// Closes connections from which nothing, not even a pong, arrived for this long, with close
    /// code 1001. Checked on every ping, so it takes effect with
    /// [`ping_interval`](Self::ping_interval), and should be at least twice the interval.
    /// Default: never.
    pub fn pong_timeout(mut self, timeout: Duration) -> Self {
        match &mut self.keepalive {
            Some(keepalive) => keepalive.pong_timeout = Some(timeout),
            None => {
                self.keepalive = Some(Keepalive {
                    ping_interval: timeout / 2,
                    pong_timeout: Some(timeout),
                })
            }
        }
        self
    }

    /// The subprotocols the server speaks, in order of preference. The first one the client also
    /// offers is chosen; see [`WebSocketSession::protocol`].
    pub fn subprotocols(mut self, protocols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.subprotocols = protocols.into_iter().map(Into::into).collect();
        self
    }

    /// Runs when a session has completed its handshake.
    pub fn on_connected<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(WebSocketSession<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.defer(move |builder| {
            builder.on_connected(move |session| handler(WebSocketSession::from(session)))
        })
    }

    /// Runs after a connected session closed. [`close_frame`](WebSocketSession::close_frame)
    /// tells what the client said when it closed.
    pub fn on_closed<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(WebSocketSession<S>, CloseReason) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.defer(move |builder| {
            builder
                .on_closed(move |session, reason| handler(WebSocketSession::from(session), reason))
        })
    }

    /// Decides what happens when handling a message fails or times out; see
    /// [`ServerBuilder::on_error`].
    pub fn on_error<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(WebSocketSession<S>, PackageError) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ErrorAction> + Send + 'static,
    {
        self.defer(move |builder| {
            builder.on_error(move |session, error| handler(WebSocketSession::from(session), error))
        })
    }

    /// Adds a middleware. It sees sessions when they connect, before their handshake.
    pub fn middleware(self, middleware: impl Middleware<WebSocketState<S>>) -> Self {
        self.defer(move |builder| builder.middleware(middleware))
    }

    /// Decides whether to serve a connection before anything is read from it; see
    /// [`ServerBuilder::on_accept`].
    pub fn on_accept(
        self,
        accept: impl Fn(&ConnectionInfo) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.defer(move |builder| builder.on_accept(accept))
    }

    /// Runs `configure` on every accepted TCP connection; see
    /// [`ServerBuilder::configure_socket`].
    pub fn configure_socket(
        self,
        configure: impl Fn(&tokio::net::TcpStream) + Send + Sync + 'static,
    ) -> Self {
        self.defer(move |builder| builder.configure_socket(configure))
    }

    /// Wraps the stream of every connection; see [`ServerBuilder::stream_layer`].
    pub fn stream_layer(self, layer: impl StreamLayer) -> Self {
        self.defer(move |builder| builder.stream_layer(layer))
    }

    /// Registers a shared service that handlers reach through [`ServerInfo::service`].
    pub fn service<T: Any + Send + Sync>(self, service: T) -> Self {
        self.defer(move |builder| builder.service(service))
    }

    /// Listens on `address` with plain TCP, such as `"0.0.0.0:4040"`. A listener with TLS gives
    /// `wss://`; set it up with [`configure`](Self::configure).
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

    /// Checks the options and builds the server.
    pub fn build(self) -> Result<WebSocketServer<S>, Error> {
        let max_size = self.options.max_package_length;
        let settings = Settings {
            subprotocols: self.subprotocols,
            on_handshake: self.on_handshake,
        };
        let state_factory = self.state_factory;
        let builder = Server::builder(move || HeadFilter::new(Role::Server, max_size))
            .session_with(move |connection| WebSocketState::new(state_factory(connection)))
            .options(self.options)
            .require_handshake();
        let mut builder = match self.install {
            Some(install) => install(builder, settings),
            None => builder.package_handler(Handler {
                settings,
                on_message: ignore_message::<S>,
            }),
        };
        for deferred in self.deferred {
            builder = deferred(builder);
        }
        Ok(WebSocketServer {
            server: builder.build()?,
            keepalive: self.keepalive,
            stop: CancellationToken::new(),
            watchdog: std::sync::Mutex::new(None),
        })
    }

    fn defer(
        mut self,
        apply: impl FnOnce(CoreBuilder<S>) -> CoreBuilder<S> + Send + 'static,
    ) -> Self {
        self.deferred.push(Box::new(apply));
        self
    }
}

impl<S> fmt::Debug for WebSocketBuilder<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketBuilder")
            .field("options", &self.options)
            .field("subprotocols", &self.subprotocols)
            .finish_non_exhaustive()
    }
}

fn ignore_message<S>(_: WebSocketSession<S>, _: Message) -> std::future::Ready<HandlerResult> {
    std::future::ready(Ok(()))
}

struct Settings<S> {
    subprotocols: Vec<String>,
    on_handshake: Option<HandshakeHandler<S>>,
}

/// Speaks the WebSocket protocol on every session: answers the handshake, pings and the closing
/// handshake, and hands messages to the application.
struct Handler<S, F> {
    settings: Settings<S>,
    on_message: F,
}

impl<S, F, Fut> PackageHandler<WebSocketPackage, WebSocketState<S>> for Handler<S, F>
where
    S: Send + Sync + 'static,
    F: Fn(WebSocketSession<S>, Message) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandlerResult> + Send + 'static,
{
    async fn handle(
        &self,
        session: &Session<WebSocketState<S>>,
        package: WebSocketPackage,
    ) -> HandlerResult {
        let package = package.0;
        if !session.is_handshake_complete() {
            // Frames sent before the handshake was answered are ignored.
            if let Package::Handshake(head) = package {
                self.handshake(session, head).await;
            }
            return Ok(());
        }
        match package {
            Package::Message(message) => {
                return (self.on_message)(WebSocketSession::from(session.clone()), message).await;
            }
            Package::Ping(payload) => {
                if !session.close_sent.load(Ordering::Acquire) {
                    let _ = send_frame(session, OP_PONG, &payload).await;
                }
            }
            Package::Close(frame) => {
                let code = frame.as_ref().map(|frame| frame.code);
                if let Some(frame) = frame {
                    let _ = session.close_frame.set(frame);
                }
                if session.close_sent.swap(true, Ordering::AcqRel) {
                    // The client answered our close frame: the closing handshake is complete.
                    session.close(CloseReason::LocalClosed);
                } else {
                    // The client started closing: answer with its code, then close.
                    let _ = send_close(session, code, "");
                    session.close(CloseReason::RemoteClosed);
                }
            }
            Package::Violation(code, why) => {
                tracing::debug!(%code, why, "WebSocket protocol violation");
                if !session.close_sent.swap(true, Ordering::AcqRel) {
                    let _ = send_close(session, Some(code), why);
                }
                session.close(CloseReason::ProtocolError);
            }
            Package::Pong | Package::Handshake(_) => {}
        }
        Ok(())
    }

    async fn closing(&self, session: &Session<WebSocketState<S>>, reason: CloseReason) {
        let code = match reason {
            // The client is gone or cannot be written to.
            CloseReason::RemoteClosed | CloseReason::SocketError => return,
            CloseReason::ServerShutdown | CloseReason::Timeout => CloseCode::GOING_AWAY,
            CloseReason::ProtocolError => CloseCode::PROTOCOL_ERROR,
            CloseReason::ApplicationError | CloseReason::InternalError => CloseCode::INTERNAL_ERROR,
            _ => CloseCode::NORMAL,
        };
        if !session.close_sent.swap(true, Ordering::AcqRel) {
            let _ = send_close(session, Some(code), "");
        }
    }
}

impl<S: Send + Sync + 'static, F> Handler<S, F> {
    async fn handshake(&self, session: &Session<WebSocketState<S>>, head: Result<Head, HeadError>) {
        let (request, key) = match head.map_err(Rejection::from).and_then(read_request) {
            Ok(accepted) => accepted,
            Err(rejection) => return reject(session, &rejection).await,
        };
        let protocol = self.settings.subprotocols.iter().find(|ours| {
            request
                .subprotocols()
                .any(|offered| offered == ours.as_str())
        });
        if let Some(protocol) = protocol {
            let _ = session.protocol.set(protocol.clone());
        }
        let _ = session.request.set(request);
        let accept = match &self.settings.on_handshake {
            Some(validate) => match validate(WebSocketSession::from(session.clone())).await {
                Ok(accept) => accept,
                Err(rejection) => return reject(session, &rejection).await,
            },
            None => Accept::new(),
        };
        let response = accept_response(&key, protocol.map(String::as_str), &accept);
        if session.send(response).await.is_ok() {
            session.complete_handshake();
        }
    }
}

async fn reject<S>(session: &Session<S>, rejection: &Rejection) {
    tracing::debug!(
        status = rejection.status(),
        message = rejection.message(),
        "WebSocket handshake rejected"
    );
    let _ = session.send(rejection.response()).await;
    session.close(CloseReason::Rejected);
}

/// Pings the connected sessions every interval, and closes those that have been silent for the
/// pong timeout, until `stop` is cancelled.
async fn run_keepalive<S: Send + Sync + 'static>(
    server: Server<WebSocketPackage, WebSocketState<S>>,
    keepalive: Keepalive,
    stop: CancellationToken,
) {
    let mut ticks = tokio::time::interval(keepalive.ping_interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticks.tick().await;
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticks.tick() => {}
        }
        let now = std::time::Instant::now();
        server.info().sessions().for_each(|session| {
            if session.close_sent.load(Ordering::Acquire) {
                return;
            }
            let silent = now.saturating_duration_since(session.last_active_time());
            if keepalive
                .pong_timeout
                .is_some_and(|timeout| silent > timeout)
            {
                tracing::debug!(session = session.id(), ?silent, "no pong, closing");
                let _ = WebSocketSession::from(session.clone()).close(CloseCode::GOING_AWAY, "");
                session.close(CloseReason::Timeout);
                return;
            }
            // A session whose send buffer is full is busy anyway; skip it this time.
            let _ = try_send_frame(session, OP_PING, b"");
        });
    }
}

/// Queues an unfragmented frame without waiting; see [`Session::try_send_vectored`].
pub(crate) fn try_send_frame<S>(
    session: &Session<S>,
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    let mut header = [0; MAX_HEADER_SIZE];
    let size = encode_header(opcode, payload.len(), None, &mut header);
    session.try_send_vectored(&[&header[..size], payload])
}

/// Sends an unfragmented frame without copying the payload into a buffer first.
pub(crate) async fn send_frame<S>(
    session: &Session<S>,
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    let mut header = [0; MAX_HEADER_SIZE];
    let size = encode_header(opcode, payload.len(), None, &mut header);
    session.send_vectored(&[&header[..size], payload]).await
}

/// Queues a close frame as the last data of the session, so that no frame can follow it, not
/// even one that another task is sending at the same moment.
pub(crate) fn send_close<S>(
    session: &Session<S>,
    code: Option<CloseCode>,
    reason: &str,
) -> io::Result<()> {
    let mut frame = [0; MAX_CLOSE_FRAME];
    let length = close_frame(code, reason, None, &mut frame);
    session.send_last(&frame[..length])
}
