use std::{
    any::Any,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use supersocket_server::{
    CloseReason, Error, ErrorAction, HandlerResult, ListenOptions, Middleware, PackageError,
    PackageHandler, Server, ServerBuilder, ServerInfo, ServerOptions, Session, SessionState,
};

use crate::{
    CloseCode, CloseFrame, HandshakeRequest, Message, Rejection, WebSocketSession,
    filter::{HeadFilter, Package, Role, WebSocketPackage},
    frame::{
        MAX_CONTROL_PAYLOAD, MAX_HEADER_SIZE, OP_CLOSE, OP_PONG, close_payload, encode_header,
    },
    handshake::{Head, HeadError, accept_response, read_request},
};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type CoreBuilder<S> = ServerBuilder<WebSocketPackage, WebSocketState<S>>;
type Deferred<S> = Box<dyn FnOnce(CoreBuilder<S>) -> CoreBuilder<S> + Send>;
type InstallHandler<S> = Box<dyn FnOnce(CoreBuilder<S>, Settings<S>) -> CoreBuilder<S> + Send>;
type HandshakeHandler<S> =
    Box<dyn Fn(WebSocketSession<S>) -> BoxFuture<Result<(), Rejection>> + Send + Sync>;

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

impl<S: Default> Default for WebSocketState<S> {
    fn default() -> Self {
        Self {
            request: OnceLock::new(),
            protocol: OnceLock::new(),
            close_sent: AtomicBool::new(false),
            close_frame: OnceLock::new(),
            user: S::default(),
        }
    }
}

impl<S: Default + Send + Sync + 'static> SessionState for WebSocketState<S> {}

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
        }
    }
}

impl<S: Default + Send + Sync + 'static> WebSocketServer<S> {
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
        self.server.start()
    }

    /// Stops accepting connections and closes every session, sending close code 1001 (going
    /// away); see [`Server::shutdown`].
    pub async fn shutdown(&self) {
        self.server.shutdown().await;
    }

    /// Starts the server, waits for Ctrl+C or SIGTERM, then shuts down gracefully.
    pub async fn run(self) -> Result<(), Error> {
        self.server.run().await
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
        assert!(
            self.on_handshake.is_none() && self.install.is_none() && self.deferred.is_empty(),
            "`session` must be called before registering handlers or middleware"
        );
        WebSocketBuilder {
            options: self.options,
            subprotocols: self.subprotocols,
            on_handshake: None,
            install: None,
            deferred: Vec::new(),
        }
    }
}

impl<S: Default + Send + Sync + 'static> WebSocketBuilder<S> {
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
    /// error and closes the connection. The session can be given data here, such as the user it
    /// belongs to.
    pub fn on_handshake<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WebSocketSession<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Rejection>> + Send + 'static,
    {
        self.on_handshake = Some(Box::new(move |session| Box::pin(handler(session))));
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
        let builder = Server::builder(move || HeadFilter::new(Role::Server, max_size))
            .session::<WebSocketState<S>>()
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
    S: Default + Send + Sync + 'static,
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
                    let _ = send_close(session, code, "").await;
                    session.close(CloseReason::RemoteClosed);
                }
            }
            Package::Violation(code, why) => {
                tracing::debug!(%code, why, "WebSocket protocol violation");
                if !session.close_sent.swap(true, Ordering::AcqRel) {
                    let _ = send_close(session, Some(code), why).await;
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
            let _ = send_close(session, Some(code), "").await;
        }
    }
}

impl<S: Default + Send + Sync + 'static, F> Handler<S, F> {
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
        if let Some(validate) = &self.settings.on_handshake {
            if let Err(rejection) = validate(WebSocketSession::from(session.clone())).await {
                return reject(session, &rejection).await;
            }
        }
        let response = accept_response(&key, protocol.map(String::as_str));
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

pub(crate) async fn send_close<S>(
    session: &Session<S>,
    code: Option<CloseCode>,
    reason: &str,
) -> io::Result<()> {
    let mut payload = [0; MAX_CONTROL_PAYLOAD];
    let length = close_payload(code, reason, &mut payload);
    send_frame(session, OP_CLOSE, &payload[..length]).await
}
