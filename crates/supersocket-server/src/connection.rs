use std::{
    future::poll_fn,
    io,
    mem::MaybeUninit,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Poll, ready},
};

use bytes::BytesMut;
use supersocket_proto::{BoxFilter, ProtocolError};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::TcpStream,
    task::JoinSet,
};
use tracing::Instrument;

use crate::{
    options::PackageHandling,
    panic::{catch_panic, catch_panic_sync},
    server::{ErrorAction, Listener, PackageError, ServerInfo, ServerInner},
    session::{CloseReason, Session, SessionState},
};

/// Bytes read per poll while a session has no buffered data. They are read into the stack and
/// only what arrived is copied to the heap.
const STACK_READ_SIZE: usize = 8 * 1024;

/// Serves one accepted connection.
pub(crate) async fn serve<P: Send + 'static, S: SessionState>(
    server: Arc<ServerInner<P, S>>,
    stream: TcpStream,
    listener: Arc<Listener>,
) {
    let (Ok(remote_addr), Ok(local_addr)) = (stream.peer_addr(), stream.local_addr()) else {
        return;
    };
    let _ = stream.set_nodelay(listener.options.no_delay);
    let id = server.next_session_id.fetch_add(1, Ordering::Relaxed);
    let span = tracing::debug_span!("session", id, remote = %remote_addr);
    async move {
        let addresses = (remote_addr, local_addr);
        #[cfg(feature = "tls")]
        if let Some(tls) = &listener.tls {
            match tls.accept(stream).await {
                Ok(stream) => {
                    let (reader, writer) = tokio::io::split(stream);
                    run_session(server, id, addresses, true, reader, Box::new(writer)).await;
                }
                Err(error) => tracing::debug!(%error, "TLS handshake failed"),
            }
            return;
        }
        let (reader, writer) = stream.into_split();
        run_session(server, id, addresses, false, reader, Box::new(writer)).await;
    }
    .instrument(span)
    .await
}

/// Makes sure a session never outlives its connection task, even if the task unwinds: the writer
/// task stops and the session leaves the container. On the normal path these already happened and
/// repeating them has no effect.
struct Cleanup<S> {
    session: Session<S>,
    server: Arc<ServerInfo<S>>,
}

impl<S> Drop for Cleanup<S> {
    fn drop(&mut self) {
        self.session.close(CloseReason::InternalError);
        self.session.close_outbox();
        self.server.sessions().remove(self.session.id());
    }
}

async fn run_session<P, S, R>(
    server: Arc<ServerInner<P, S>>,
    id: u64,
    (remote_addr, local_addr): (SocketAddr, SocketAddr),
    secure: bool,
    mut reader: R,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
) where
    P: Send + 'static,
    S: SessionState,
    R: AsyncRead + Send + Unpin,
{
    let session = Session::new(
        id,
        remote_addr,
        local_addr,
        secure,
        server.shutdown.child_token(),
        server.info.clone(),
    );
    let mut writer_task = server.tracker.spawn(session.clone().run_writer(writer));
    let _cleanup = Cleanup {
        session: session.clone(),
        server: server.info.clone(),
    };

    if !register(&server, &session).await {
        tracing::debug!("session rejected by middleware");
        session.close(CloseReason::Rejected);
        session.close_outbox();
        let _ = writer_task.await;
        return;
    }

    server.info.sessions().add(session.clone());
    tracing::debug!("session connected");
    if let Err(panic) = catch_panic(async { S::on_connected(&session).await }).await {
        tracing::error!(%panic, "SessionState::on_connected panicked");
    }
    if let Some(handler) = &server.on_connected {
        if let Err(panic) = catch_panic(async { handler(session.clone()).await }).await {
            tracing::error!(%panic, "connected handler panicked");
        }
    }

    let reason = read_loop(&server, &session, &mut reader).await;
    session.close(reason);
    let reason = session.close_reason();

    // Send what is still queued (unless the peer stops reading), and close the connection before
    // the closed events run.
    session.close_outbox();
    if tokio::time::timeout(server.info.options().close_timeout, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
    }
    drop(reader);
    server.info.sessions().remove(id);
    for middleware in server.middlewares.iter().rev() {
        if let Err(panic) =
            catch_panic(async { middleware.unregister_session(&session).await }).await
        {
            tracing::error!(%panic, "Middleware::unregister_session panicked");
        }
    }
    tracing::debug!(%reason, "session closed");
    if let Err(panic) = catch_panic(async { S::on_closed(&session, reason).await }).await {
        tracing::error!(%panic, "SessionState::on_closed panicked");
    }
    if let Some(handler) = &server.on_closed {
        if let Err(panic) = catch_panic(async { handler(session.clone(), reason).await }).await {
            tracing::error!(%panic, "closed handler panicked");
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

enum Step<P> {
    Package(P),
    Switched,
    NeedMore,
}

/// Runs the filter once: decodes a package if one is complete and applies a filter switch.
fn decode_step<P: Send + 'static>(
    filter: &mut BoxFilter<P>,
    buf: &mut BytesMut,
) -> Result<Step<P>, ProtocolError> {
    let package = filter.filter(buf)?;
    let switched = match filter.next_filter() {
        Some(next) => {
            filter.reset();
            *filter = next;
            true
        }
        None => false,
    };
    Ok(match package {
        Some(package) => {
            if !switched {
                filter.reset();
            }
            Step::Package(package)
        }
        None if switched => Step::Switched,
        None => Step::NeedMore,
    })
}

/// Reads, cuts packages with the pipeline filter and hands them to the package handler until the
/// session ends. Returns why it ended.
async fn read_loop<P, S, R>(
    server: &Arc<ServerInner<P, S>>,
    session: &Session<S>,
    reader: &mut R,
) -> CloseReason
where
    P: Send + 'static,
    S: SessionState,
    R: AsyncRead + Unpin,
{
    let options = server.info.options();
    let close_token = session.close_token().clone();
    let mut filter = match catch_panic_sync(|| (server.filter_factory)()) {
        Ok(filter) => filter,
        Err(panic) => {
            tracing::error!(%panic, "creating the pipeline filter panicked");
            return CloseReason::InternalError;
        }
    };
    let mut buf = BytesMut::new();
    let mut in_flight = JoinSet::new();

    let reason = 'session: loop {
        while !buf.is_empty() {
            let package = match catch_panic_sync(|| decode_step(&mut filter, &mut buf)) {
                Ok(Ok(Step::Package(package))) => package,
                Ok(Ok(Step::Switched)) => continue,
                Ok(Ok(Step::NeedMore)) => {
                    if buf.len() > options.max_package_length {
                        tracing::debug!(buffered = buf.len(), "package exceeds the maximum length");
                        break 'session CloseReason::ProtocolError;
                    }
                    break;
                }
                Ok(Err(error)) => {
                    tracing::debug!(%error, "closing session");
                    break 'session CloseReason::ProtocolError;
                }
                Err(panic) => {
                    tracing::error!(%panic, "pipeline filter panicked");
                    break 'session CloseReason::InternalError;
                }
            };
            match options.package_handling {
                PackageHandling::Serial => handle_package(server, session, package).await,
                PackageHandling::Concurrent { max_in_flight } => {
                    while in_flight.len() >= max_in_flight {
                        in_flight.join_next().await;
                    }
                    let (server, session) = (server.clone(), session.clone());
                    in_flight
                        .spawn(async move { handle_package(&server, &session, package).await });
                }
            }
            if close_token.is_cancelled() {
                break 'session cancelled_reason(server);
            }
        }
        while in_flight.try_join_next().is_some() {}
        // Everything received has been handled: drop the buffer so an idle session holds none.
        if buf.is_empty() {
            buf = BytesMut::new();
        }
        tokio::select! {
            _ = close_token.cancelled() => break 'session cancelled_reason(server),
            read = read_some(reader, &mut buf, options.receive_buffer_size) => match read {
                Ok(0) => break 'session CloseReason::RemoteClosed,
                Ok(_) => session.touch(),
                Err(error) => {
                    tracing::debug!(%error, "read failed");
                    break 'session CloseReason::SocketError;
                }
            },
        }
    };
    // Let packages still being handled finish; each one is bounded by the handling timeout.
    while in_flight.join_next().await.is_some() {}
    reason
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
    let handling = async { tokio::time::timeout(limit, handler(session.clone(), package)).await };
    let error = match catch_panic(handling).await {
        Ok(Ok(Ok(()))) => return,
        Ok(Ok(Err(error))) => PackageError::Handler(error),
        // Dropping the handler's future cancels it at its current await point.
        Ok(Err(_)) => PackageError::Timeout(limit),
        Err(panic) => PackageError::Panicked(panic),
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

/// Reads more data into `buf`. While `buf` is empty, that is between packages, nothing is
/// allocated until data actually arrives.
async fn read_some<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
    chunk_size: usize,
) -> io::Result<usize> {
    if !buf.is_empty() {
        buf.reserve(chunk_size);
        return reader.read_buf(buf).await;
    }
    poll_fn(|cx| {
        let mut stack = [MaybeUninit::<u8>::uninit(); STACK_READ_SIZE];
        let mut read_buf = ReadBuf::uninit(&mut stack);
        ready!(Pin::new(&mut *reader).poll_read(cx, &mut read_buf))?;
        buf.extend_from_slice(read_buf.filled());
        Poll::Ready(Ok(read_buf.filled().len()))
    })
    .await
}
