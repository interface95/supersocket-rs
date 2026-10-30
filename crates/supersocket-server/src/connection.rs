use std::{
    future::Future,
    pin::pin,
    sync::{Arc, atomic::Ordering},
};

use bytes::BytesMut;
use supersocket_connection::{
    BoxStream, PackageReader, ReadError, ReadOptions, Rewind, catch_panic, catch_panic_sync,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    task::JoinSet,
    time::Instant,
};
use tracing::Instrument;

use crate::{
    options::PackageHandling,
    proxy_protocol,
    server::{ErrorAction, Listener, ServerInner},
    session::{CloseReason, ConnectionInfo, Session, SessionState},
};

/// Serves a TCP connection accepted on a plain listener.
pub(crate) async fn serve<P: Send + 'static, S: SessionState>(
    server: Arc<ServerInner<P, S>>,
    stream: TcpStream,
    listener: Arc<Listener>,
) {
    let Some((id, span, connection)) = accept(&server, &stream, &listener) else {
        return;
    };
    async move {
        // Set up in a block of its own, so that nothing of it is kept while the session runs.
        let Some(session) = ({
            let (mut stream, mut connection) = (stream, connection);
            let Some(buffered) =
                read_proxy_header(&server, &listener, &mut stream, &mut connection).await
            else {
                return;
            };
            let (reader, writer) = stream.into_split();
            start(&server, id, connection, reader, Box::new(writer), buffered)
        }) else {
            return;
        };
        session.await;
    }
    .instrument(span)
    .await
}

/// Serves a TCP connection accepted on a TLS listener. Each kind of connection runs in a task of
/// its own type, so that plain connections do not carry the handshake's state, nor these the
/// state of stream layers.
#[cfg(feature = "tls")]
pub(crate) async fn serve_tls<P: Send + 'static, S: SessionState>(
    server: Arc<ServerInner<P, S>>,
    stream: TcpStream,
    listener: Arc<Listener>,
) {
    let Some((id, span, connection)) = accept(&server, &stream, &listener) else {
        return;
    };
    async move {
        let Some(session) = ({
            let (mut stream, mut connection) = (stream, connection);
            let Some(buffered) =
                read_proxy_header(&server, &listener, &mut stream, &mut connection).await
            else {
                return;
            };
            let tls = listener.tls.as_ref().expect("a TLS listener");
            // Bytes that arrived with a PROXY protocol header are the start of the handshake.
            // Boxed, so that the session does not keep the handshake's state.
            let stream = Rewind::new(buffered.freeze(), stream);
            let stream = match Box::pin(tls.accept(stream)).await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(%error, "TLS handshake failed");
                    return;
                }
            };
            connection.secure = true;
            connection.tls = Some(Box::new(crate::TlsInfo::new(stream.get_ref().1)));
            let (reader, writer) = tokio::io::split(stream);
            start(
                &server,
                id,
                connection,
                reader,
                Box::new(writer),
                BytesMut::new(),
            )
        }) else {
            return;
        };
        session.await;
    }
    .instrument(span)
    .await
}

/// Serves a TCP connection on a server with stream layers, after the PROXY protocol header and
/// the TLS handshake where the listener asks for them.
pub(crate) async fn serve_layered_tcp<P: Send + 'static, S: SessionState>(
    server: Arc<ServerInner<P, S>>,
    stream: TcpStream,
    listener: Arc<Listener>,
) {
    let Some((id, span, connection)) = accept(&server, &stream, &listener) else {
        return;
    };
    async move {
        let (mut stream, mut connection) = (stream, connection);
        let Some(buffered) =
            read_proxy_header(&server, &listener, &mut stream, &mut connection).await
        else {
            return;
        };
        let stream = Rewind::new(buffered.freeze(), stream);
        #[cfg(feature = "tls")]
        if let Some(tls) = &listener.tls {
            let stream = match Box::pin(tls.accept(stream)).await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(%error, "TLS handshake failed");
                    return;
                }
            };
            connection.secure = true;
            connection.tls = Some(Box::new(crate::TlsInfo::new(stream.get_ref().1)));
            return serve_layered(&server, id, Box::new(stream), connection).await;
        }
        serve_layered(&server, id, Box::new(stream), connection).await;
    }
    .instrument(span)
    .await
}

/// Serves a connection handed over with [`Server::serve_connection`](crate::Server::serve_connection).
pub(crate) async fn serve_stream<P: Send + 'static, S: SessionState>(
    server: Arc<ServerInner<P, S>>,
    stream: BoxStream,
    connection: ConnectionInfo,
) {
    let id = server.next_session_id.fetch_add(1, Ordering::Relaxed);
    let span = tracing::debug_span!("session", id, remote = %connection.remote_addr);
    serve_layered(&server, id, stream, connection)
        .instrument(span)
        .await
}

/// Wraps `stream` with the stream layers, in the order they were added, then runs the session.
async fn serve_layered<P: Send + 'static, S: SessionState>(
    server: &Arc<ServerInner<P, S>>,
    id: u64,
    mut stream: BoxStream,
    connection: ConnectionInfo,
) {
    for layer in &server.layers {
        stream = match layer.wrap(stream, &connection).await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::debug!(%error, "a stream layer refused the connection");
                return;
            }
        };
    }
    let (reader, writer) = tokio::io::split(stream);
    let writer = Box::new(writer);
    let Some(session) = start(server, id, connection, reader, writer, BytesMut::new()) else {
        return;
    };
    session.await;
}

/// Applies the socket settings to an accepted connection and gives it an id and a span. `None`
/// when the connection is already gone.
fn accept<P, S>(
    server: &ServerInner<P, S>,
    stream: &TcpStream,
    listener: &Listener,
) -> Option<(u64, tracing::Span, ConnectionInfo)> {
    let (Ok(remote_addr), Ok(local_addr)) = (stream.peer_addr(), stream.local_addr()) else {
        return None;
    };
    listener.configure(stream);
    if let Some(configure) = &server.configure_socket {
        if let Err(panic) = catch_panic_sync(|| configure(stream)) {
            tracing::error!(%panic, "socket configuration panicked");
            return None;
        }
    }
    let id = server.next_session_id.fetch_add(1, Ordering::Relaxed);
    let span = tracing::debug_span!("session", id, remote = %remote_addr);
    Some((id, span, ConnectionInfo::new(remote_addr, local_addr)))
}

/// Reads the PROXY protocol header where the listener expects one, and records what it says.
/// Returns the bytes read past it, or `None` when the connection must be dropped.
async fn read_proxy_header<P, S>(
    server: &ServerInner<P, S>,
    listener: &Listener,
    stream: &mut TcpStream,
    connection: &mut ConnectionInfo,
) -> Option<BytesMut> {
    if !listener.options.proxy_protocol {
        return Some(BytesMut::new());
    }
    let limit = server.info.options().handshake_timeout;
    let header = proxy_protocol::read_header(stream, connection.remote_addr);
    match tokio::time::timeout(limit, header).await {
        Ok(Ok((info, rest))) => {
            connection.set_proxy(info);
            tracing::Span::current()
                .record("remote", tracing::field::display(connection.remote_addr));
            Some(rest)
        }
        Ok(Err(error)) => {
            tracing::debug!(%error, "invalid PROXY protocol header");
            None
        }
        Err(_) => {
            tracing::debug!("timed out waiting for the PROXY protocol header");
            None
        }
    }
}

/// Creates the session and the reader of its packages, which starts with `buffered`, and returns
/// the task that runs the session; `None` when the pipeline filter cannot be created.
fn start<P, S, R>(
    server: &Arc<ServerInner<P, S>>,
    id: u64,
    connection: ConnectionInfo,
    reader: R,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    buffered: BytesMut,
) -> Option<impl Future<Output = ()> + use<P, S, R>>
where
    P: Send + 'static,
    S: SessionState,
    R: AsyncRead + Send + Unpin,
{
    let filter = match catch_panic_sync(|| (server.filter_factory)()) {
        Ok(filter) => filter,
        Err(panic) => {
            tracing::error!(%panic, "creating the pipeline filter panicked");
            return None;
        }
    };
    let options = server.info.options();
    let read_options = ReadOptions {
        max_package_length: options.max_package_length,
        receive_buffer_size: options.receive_buffer_size,
    };
    let packages = PackageReader::with_buffered(reader, filter, read_options, buffered);
    let session = Session::new(
        id,
        connection,
        !server.handshake_required,
        server.shutdown.child_token(),
        server.info.clone(),
    );
    Some(run_session(server.clone(), session, packages, writer))
}

/// Makes sure a session never outlives its connection task, even if the task unwinds: the writer
/// task stops and the session leaves the container. On the normal path these already happened and
/// repeating them has no effect.
struct Cleanup<S> {
    session: Session<S>,
}

impl<S> Drop for Cleanup<S> {
    fn drop(&mut self) {
        self.session.close(CloseReason::InternalError);
        self.session.close_send_queue();
        self.session.server().sessions().remove(self.session.id());
    }
}

async fn run_session<P, S, R>(
    server: Arc<ServerInner<P, S>>,
    session: Session<S>,
    mut packages: PackageReader<R, P>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
) where
    P: Send + 'static,
    S: SessionState,
    R: AsyncRead + Send + Unpin,
{
    let mut writer_task = server.tracker.spawn(session.clone().run_writer(writer));
    let _cleanup = Cleanup {
        session: session.clone(),
    };

    if !register(&server, &session).await {
        tracing::debug!("session rejected by middleware");
        session.close(CloseReason::Rejected);
        session.close_send_queue();
        let _ = writer_task.await;
        return;
    }

    // A session that must complete a handshake connects once it has; see `read_loop`.
    let mut connected = false;
    if session.is_handshake_complete() {
        connect(&server, &session).await;
        connected = true;
    }

    let reason = read_loop(&server, &session, &mut packages, &mut connected).await;
    session.close(reason);
    let reason = session.close_reason();

    // Let the protocol queue a goodbye, send what is still queued (unless the peer stops
    // reading), and close the connection before the closed events run.
    let deadline = Instant::now() + server.info.options().close_timeout;
    if connected {
        if let Some(closing) = server
            .package_handler
            .as_ref()
            .and_then(|handler| handler.closing(&session, reason))
        {
            if let Err(panic) = catch_panic(tokio::time::timeout_at(deadline, closing)).await {
                tracing::error!(%panic, "PackageHandler::closing panicked");
            }
        }
    }
    session.close_send_queue();
    if tokio::time::timeout_at(deadline, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
    }
    drop(packages);
    server.info.sessions().remove(session.id());
    for middleware in server.middlewares.iter().rev() {
        if let Err(panic) =
            catch_panic(async { middleware.unregister_session(&session).await }).await
        {
            tracing::error!(%panic, "Middleware::unregister_session panicked");
        }
    }
    tracing::debug!(%reason, "session closed");
    if !connected {
        return;
    }
    if let Err(panic) = catch_panic(async { S::on_closed(&session, reason).await }).await {
        tracing::error!(%panic, "SessionState::on_closed panicked");
    }
    if let Some(handler) = &server.on_closed {
        if let Err(panic) = catch_panic(async { handler(session.clone(), reason).await }).await {
            tracing::error!(%panic, "closed handler panicked");
        }
    }
}

/// Adds the session to the session container and runs the connected handlers.
async fn connect<P, S: SessionState>(server: &ServerInner<P, S>, session: &Session<S>) {
    server.info.sessions().add(session.clone());
    tracing::debug!("session connected");
    if let Err(panic) = catch_panic(async { S::on_connected(session).await }).await {
        tracing::error!(%panic, "SessionState::on_connected panicked");
    }
    if let Some(handler) = &server.on_connected {
        if let Err(panic) = catch_panic(async { handler(session.clone()).await }).await {
            tracing::error!(%panic, "connected handler panicked");
        }
    }
}

/// Registers the session with every middleware. When one rejects it or panics, the session is
/// unregistered from the ones that accepted it.
async fn register<P, S: SessionState>(server: &ServerInner<P, S>, session: &Session<S>) -> bool {
    for (index, middleware) in server.middlewares.iter().enumerate() {
        let accepted = catch_panic(async { middleware.register_session(session).await })
            .await
            .unwrap_or_else(|panic| {
                tracing::error!(%panic, "Middleware::register_session panicked");
                false
            });
        if !accepted {
            for registered in server.middlewares[..index].iter().rev() {
                if let Err(panic) =
                    catch_panic(async { registered.unregister_session(session).await }).await
                {
                    tracing::error!(%panic, "Middleware::unregister_session panicked");
                }
            }
            return false;
        }
    }
    true
}

/// Reads packages and hands them to the package handler until the session ends. Returns why it
/// ended. A session that must complete a handshake does that first; once it has, it connects and
/// `connected` is set.
async fn read_loop<P, S, R>(
    server: &Arc<ServerInner<P, S>>,
    session: &Session<S>,
    packages: &mut PackageReader<R, P>,
    connected: &mut bool,
) -> CloseReason
where
    P: Send + 'static,
    S: SessionState,
    R: AsyncRead + Unpin,
{
    let options = server.info.options();
    let close_token = session.close_token().clone();
    if !*connected {
        // Boxed, so that the read loop of every session does not carry the handshake's state.
        if let Err(reason) = Box::pin(handshake(server, session, packages)).await {
            return reason;
        }
        connect(server, session).await;
        *connected = true;
    }
    let mut in_flight = JoinSet::new();

    let reason = loop {
        while in_flight.try_join_next().is_some() {}
        // Packages already received are handled without waiting; only wait for more data (and
        // watch for the session closing) when none is complete.
        let package = match packages.try_next() {
            Ok(Some(package)) => package,
            Ok(None) => {
                let read = tokio::select! {
                    biased;
                    _ = close_token.cancelled() => break cancelled_reason(server),
                    read = packages.fill() => read,
                };
                match received(session, read) {
                    Ok(()) => continue,
                    Err(reason) => break reason,
                }
            }
            Err(error) => break undecodable(error),
        };
        match options.package_handling {
            PackageHandling::Serial => handle_package(server, session, package).await,
            PackageHandling::Concurrent { max_in_flight } => {
                while in_flight.len() >= max_in_flight {
                    in_flight.join_next().await;
                }
                let (server, session) = (server.clone(), session.clone());
                in_flight.spawn(async move { handle_package(&server, &session, package).await });
            }
        }
        if close_token.is_cancelled() {
            break cancelled_reason(server);
        }
    };
    // Let packages still being handled finish; each one is bounded by the handling timeout.
    while in_flight.join_next().await.is_some() {}
    reason
}

/// Handles packages one at a time until the session completes its handshake. Fails with the
/// reason to close the session when it ends first or the handshake timeout passes.
async fn handshake<P, S, R>(
    server: &ServerInner<P, S>,
    session: &Session<S>,
    packages: &mut PackageReader<R, P>,
) -> Result<(), CloseReason>
where
    P: Send + 'static,
    S: SessionState,
    R: AsyncRead + Unpin,
{
    let close_token = session.close_token();
    let mut timeout = pin!(tokio::time::sleep(server.info.options().handshake_timeout));
    loop {
        let package = match packages.try_next() {
            Ok(Some(package)) => package,
            Ok(None) => {
                let read = tokio::select! {
                    biased;
                    _ = close_token.cancelled() => return Err(cancelled_reason(server)),
                    () = &mut timeout => {
                        tracing::debug!("handshake timed out");
                        return Err(CloseReason::Timeout);
                    }
                    read = packages.fill() => read,
                };
                received(session, read)?;
                continue;
            }
            Err(error) => return Err(undecodable(error)),
        };
        handle_package(server, session, package).await;
        if close_token.is_cancelled() {
            return Err(cancelled_reason(server));
        }
        if session.is_handshake_complete() {
            return Ok(());
        }
    }
}

/// What a read means for the session: data arrived, or the reason to close it.
fn received<S>(session: &Session<S>, read: Result<usize, ReadError>) -> Result<(), CloseReason> {
    match read {
        Ok(0) => Err(CloseReason::RemoteClosed),
        Ok(_) => {
            session.touch();
            Ok(())
        }
        Err(error) => {
            tracing::debug!(%error, "read failed");
            Err(CloseReason::SocketError)
        }
    }
}

/// The reason to close a session whose data could not be decoded.
fn undecodable(error: ReadError) -> CloseReason {
    match error {
        ReadError::Io(error) => {
            tracing::debug!(%error, "read failed");
            CloseReason::SocketError
        }
        ReadError::FilterPanicked(panic) => {
            tracing::error!(%panic, "pipeline filter panicked");
            CloseReason::InternalError
        }
        error => {
            tracing::debug!(%error, "closing session");
            CloseReason::ProtocolError
        }
    }
}

// When the session closed itself its reason is already recorded, so this only matters when the
// whole server is shutting down.
fn cancelled_reason<P, S>(server: &ServerInner<P, S>) -> CloseReason {
    if server.shutdown.is_cancelled() {
        CloseReason::ServerShutdown
    } else {
        CloseReason::LocalClosed
    }
}

async fn handle_package<P: Send + 'static, S: SessionState>(
    server: &ServerInner<P, S>,
    session: &Session<S>,
    package: P,
) {
    let Some(handler) = &server.package_handler else {
        return;
    };
    let limit = server.info.options().package_handling_timeout;
    let Err(error) = handler.handle(session.clone(), package, limit).await else {
        return;
    };
    let action = match &server.on_error {
        Some(on_error) => catch_panic(async { on_error(session.clone(), error).await })
            .await
            .unwrap_or_else(|panic| {
                tracing::error!(%panic, "error handler panicked");
                ErrorAction::Continue
            }),
        None => {
            tracing::warn!(%error, "package handling failed");
            ErrorAction::Continue
        }
    };
    if action == ErrorAction::Close {
        session.close(CloseReason::ApplicationError);
    }
}
