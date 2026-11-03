//! Tests of connections that do not come straight from a TCP listener: handed over from other
//! transports, or wrapped by stream layers.

mod common;

use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use common::{Client, WAIT, start};
use supersocket::{
    BoxStream, ConnectionInfo, ListenAddr, ListenOptions, Listener, ServerBuilder, ServerStatus,
    StreamLayer, prelude::*,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::mpsc,
    time::timeout,
};

/// Echoes every line together with the address of the session.
fn echo_with_address() -> ServerBuilder<TextPackage> {
    Server::builder(LineFilter::default).on_package(async |session, package| {
        let reply = format!("{} {}\r\n", package.text, session.remote_addr());
        session.send(reply).await?;
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_from_other_transports_become_sessions() {
    // No listener at all: the connections come from elsewhere.
    let server = echo_with_address().build().unwrap();
    assert!(server.start().unwrap().is_empty());

    let (client, connection) = tokio::io::duplex(1024);
    let remote = "198.51.100.4:1234".parse().unwrap();
    let local = "192.0.2.1:4040".parse().unwrap();
    server
        .serve_connection(connection, ConnectionInfo::new(remote, local))
        .unwrap();
    let (reader, writer) = tokio::io::split(client);
    let mut client = Client::new(reader, writer);
    client.send(b"hello\r\n").await;
    assert_eq!(client.line().await, "hello 198.51.100.4:1234");
    assert_eq!(server.info().session_count(), 1);

    server.shutdown().await;
    client.expect_closed().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unix_sockets_can_be_served() {
    use std::sync::Arc;
    use tokio::net::{UnixListener, UnixStream};

    let path = std::env::temp_dir().join(format!("supersocket-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    let server = Arc::new(echo_with_address().build().unwrap());
    server.start().unwrap();
    let accepting = tokio::spawn({
        let server = server.clone();
        async move {
            while let Ok((stream, _)) = listener.accept().await {
                server
                    .serve_connection(stream, ConnectionInfo::default())
                    .unwrap();
            }
        }
    });

    let (reader, writer) = UnixStream::connect(&path).await.unwrap().into_split();
    let mut client = Client::new(reader, writer);
    client.send(b"over a unix socket\r\n").await;
    assert_eq!(client.line().await, "over a unix socket 0.0.0.0:0");

    accepting.abort();
    server.shutdown().await;
    let _ = std::fs::remove_file(&path);
}

/// Writes its line first on every connection it wraps.
struct Banner(&'static str);

impl StreamLayer for Banner {
    async fn wrap(
        &self,
        mut stream: BoxStream,
        _connection: &ConnectionInfo,
    ) -> io::Result<BoxStream> {
        stream
            .write_all(format!("{}\r\n", self.0).as_bytes())
            .await?;
        Ok(stream)
    }
}

/// Refuses every connection.
struct Refuse;

impl StreamLayer for Refuse {
    async fn wrap(&self, _stream: BoxStream, connection: &ConnectionInfo) -> io::Result<BoxStream> {
        let message = format!("{} is not welcome", connection.remote_addr());
        Err(io::Error::new(io::ErrorKind::PermissionDenied, message))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_layers_wrap_connections_in_order() {
    let (server, address) = start(
        echo_with_address()
            .stream_layer(Banner("first"))
            .stream_layer(Banner("second")),
    );
    let mut client = Client::connect(address).await;
    assert_eq!(client.line().await, "first");
    assert_eq!(client.line().await, "second");
    client.send(b"hello\r\n").await;
    assert!(client.line().await.starts_with("hello 127.0.0.1:"));

    // Connections handed over go through the layers too.
    let (other, connection) = tokio::io::duplex(1024);
    server
        .serve_connection(connection, ConnectionInfo::default())
        .unwrap();
    let (reader, writer) = tokio::io::split(other);
    let mut other = Client::new(reader, writer);
    assert_eq!(other.line().await, "first");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_layer_drops_the_connection() {
    let (connected, mut sessions) = mpsc::unbounded_channel();
    let (server, address) = start(echo_with_address().stream_layer(Refuse).on_connected(
        move |session| {
            let connected = connected.clone();
            async move {
                let _ = connected.send(session.id());
            }
        },
    ));
    let mut client = Client::connect(address).await;
    client.expect_closed().await;
    assert!(sessions.try_recv().is_err());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn layers_see_the_data_that_arrived_with_a_proxy_header() {
    let listener = ListenOptions {
        proxy_protocol: true,
        ..ListenOptions::new("127.0.0.1:0")
    };
    let server = echo_with_address()
        .stream_layer(Banner("welcome"))
        .configure(|options| options.listeners.push(listener))
        .build()
        .unwrap();
    let address = server.start().unwrap()[0].tcp().unwrap();
    let mut client = Client::connect(address).await;
    client
        .send(b"PROXY TCP4 203.0.113.7 192.0.2.1 51000 4040\r\nhello\r\n")
        .await;
    assert_eq!(client.line().await, "welcome");
    assert_eq!(client.line().await, "hello 203.0.113.7:51000");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_are_refused_unless_the_server_runs() {
    let server = echo_with_address().build().unwrap();
    let refused = |server: &Server<TextPackage>| {
        let (_client, connection) = tokio::io::duplex(64);
        matches!(
            server.serve_connection(connection, ConnectionInfo::default()),
            Err(supersocket::Error::NotRunning)
        )
    };
    assert!(refused(&server));
    server.start().unwrap();
    server.shutdown().await;
    assert!(refused(&server));
}

#[tokio::test]
async fn idle_tcp_listeners_do_not_consume_connection_capacity() {
    let server = echo_with_address()
        .listen("127.0.0.1:0")
        .listen("127.0.0.1:0")
        .configure(|options| options.max_connections = Some(1))
        .build()
        .unwrap();
    server.start().unwrap();
    // Let both listener tasks start waiting for incoming TCP connections.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(server.info().session_count(), 0);

    let (client, connection) = tokio::io::duplex(1024);
    server
        .serve_connection(connection, ConnectionInfo::default())
        .expect("idle listeners must leave the connection capacity available");
    let (reader, writer) = tokio::io::split(client);
    let mut client = Client::new(reader, writer);
    client.send(b"hello\r\n").await;
    assert_eq!(client.line().await, "hello 0.0.0.0:0");

    let (_peer, extra) = tokio::io::duplex(64);
    assert!(matches!(
        server.serve_connection(extra, ConnectionInfo::default()),
        Err(supersocket::Error::ConnectionLimit)
    ));
    server.shutdown().await;
}

struct RefuseWithReply(mpsc::UnboundedSender<()>);

impl Middleware for RefuseWithReply {
    async fn register_session(&self, session: &Session) -> bool {
        session.send(b"access denied\r\n").await.unwrap();
        self.0.send(()).unwrap();
        false
    }
}

#[tokio::test]
async fn rejected_connections_obey_the_close_timeout() {
    let (refused, mut refusals) = mpsc::unbounded_channel();
    let server = echo_with_address()
        .middleware(RefuseWithReply(refused))
        .configure(|options| options.close_timeout = Duration::from_millis(50))
        .build()
        .unwrap();
    server.start().unwrap();
    server
        .serve_connection(common::Stall, ConnectionInfo::default())
        .unwrap();
    tokio::time::timeout(common::WAIT, refusals.recv())
        .await
        .expect("middleware did not reject the connection")
        .unwrap();

    tokio::time::timeout(common::WAIT, server.shutdown())
        .await
        .expect("a rejected connection that stops reading must not block shutdown");
}

type RegistrationEvents = Arc<Mutex<Vec<(&'static str, &'static str)>>>;

struct RegistrationProbe(&'static str, RegistrationEvents);

impl Middleware for RegistrationProbe {
    async fn register_session(&self, _session: &Session) -> bool {
        self.1.lock().unwrap().push(("register", self.0));
        true
    }

    async fn unregister_session(&self, session: &Session) {
        assert!(session.is_closed(), "rollback must see a closing session");
        self.1.lock().unwrap().push(("unregister", self.0));
    }
}

struct RegistrationAttempt(Arc<AtomicBool>);

impl Drop for RegistrationAttempt {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct PendingRegistration {
    entered: mpsc::UnboundedSender<Session>,
    cancelled: Arc<AtomicBool>,
    events: RegistrationEvents,
}

impl Middleware for PendingRegistration {
    async fn register_session(&self, session: &Session) -> bool {
        let _attempt = RegistrationAttempt(self.cancelled.clone());
        self.events.lock().unwrap().push(("register", "pending"));
        self.entered.send(session.clone()).unwrap();
        std::future::pending().await
    }

    async fn unregister_session(&self, _session: &Session) {
        self.events.lock().unwrap().push(("unregister", "pending"));
    }
}

async fn check_cancelled_registration(shutdown: bool) {
    let events = RegistrationEvents::default();
    let cancelled = Arc::new(AtomicBool::new(false));
    let (entered, mut entering) = mpsc::unbounded_channel();
    let server = echo_with_address()
        .require_handshake()
        .middleware(RegistrationProbe("first", events.clone()))
        .middleware(RegistrationProbe("second", events.clone()))
        .middleware(PendingRegistration {
            entered,
            cancelled: cancelled.clone(),
            events: events.clone(),
        })
        .configure(move |options| {
            options.max_connections = Some(1);
            options.handshake_timeout = if shutdown {
                Duration::from_secs(60)
            } else {
                Duration::from_millis(100)
            };
        })
        .build()
        .unwrap();
    server.start().unwrap();
    let (_peer, connection) = tokio::io::duplex(64);
    server
        .serve_connection(connection, ConnectionInfo::default())
        .unwrap();
    let session = tokio::time::timeout(common::WAIT, entering.recv())
        .await
        .unwrap()
        .unwrap();
    if shutdown {
        tokio::time::timeout(Duration::from_secs(1), server.shutdown())
            .await
            .expect("shutdown must cancel pending middleware registration");
    } else {
        tokio::time::timeout(Duration::from_secs(1), session.closed())
            .await
            .expect("middleware registration must obey the handshake deadline");
        // The timed-out registration must release its core connection slot as well.
        tokio::time::timeout(common::WAIT, async {
            loop {
                match server.serve_connection(common::Stall, ConnectionInfo::default()) {
                    Ok(()) => break,
                    Err(supersocket::Error::ConnectionLimit) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected connection error: {error}"),
                }
            }
        })
        .await
        .unwrap();
        server.shutdown().await;
    }
    assert!(cancelled.load(Ordering::SeqCst));
    assert_eq!(
        *events.lock().unwrap(),
        [
            ("register", "first"),
            ("register", "second"),
            ("register", "pending"),
            ("unregister", "second"),
            ("unregister", "first"),
        ]
    );
    assert!(session.is_closed());
    assert_eq!(server.info().session_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_registration_obeys_handshake_timeout_and_rolls_back() {
    check_cancelled_registration(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_registration_is_cancelled_on_shutdown_and_rolls_back() {
    check_cancelled_registration(true).await;
}

struct FailedWriter;

impl AsyncRead for FailedWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for FailedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_failure_closes_session_even_if_transport_shutdown_stalls() {
    let (builder, mut reasons) =
        common::track_close_reasons(echo_with_address().on_connected(async |session| {
            session.send(b"hello").await.unwrap();
        }));
    let server = builder.build().unwrap();
    server.start().unwrap();
    server
        .serve_connection(FailedWriter, ConnectionInfo::default())
        .unwrap();
    let reason = tokio::time::timeout(Duration::from_secs(1), reasons.recv())
        .await
        .expect("a failed writer must close the session before transport shutdown")
        .unwrap();
    assert_eq!(reason, CloseReason::SocketError);
    assert_eq!(server.info().session_count(), 0);
    server.shutdown().await;
}

/// Waits for a preamble that never comes.
struct WaitForever;

impl StreamLayer for WaitForever {
    async fn wrap(
        &self,
        mut stream: BoxStream,
        _connection: &ConnectionInfo,
    ) -> io::Result<BoxStream> {
        let mut preamble = Vec::new();
        stream.read_to_end(&mut preamble).await?;
        Ok(stream)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_layers_must_finish_within_the_handshake_timeout() {
    let limit = Duration::from_millis(200);
    let (server, address) = start(
        echo_with_address()
            .stream_layer(WaitForever)
            .configure(|options| options.handshake_timeout = limit),
    );
    let mut client = Client::connect(address).await;
    let started = Instant::now();
    client.expect_closed().await;
    assert!(started.elapsed() >= limit);
    server.shutdown().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unix_socket_listeners_come_from_the_options() {
    let path = std::env::temp_dir().join(format!("supersocket-listen-{}.sock", std::process::id()));
    // A socket file left behind by an earlier run does not stop the listener.
    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    assert!(path.exists());

    let server = echo_with_address().listen_unix(&path).build().unwrap();
    let addresses = server.start().unwrap();
    assert_eq!(addresses, [ListenAddr::Unix(path.clone())]);
    assert_eq!(addresses[0].unix(), Some(path.as_path()));
    assert_eq!(addresses[0].tcp(), None);

    let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (reader, writer) = stream.into_split();
    let mut client = Client::new(reader, writer);
    client.send(b"hello\r\n").await;
    // There are no IP addresses on a Unix socket.
    assert_eq!(client.line().await, "hello 0.0.0.0:0");
    assert_eq!(server.info().session_count(), 1);

    server.shutdown().await;
    client.expect_closed().await;
    assert!(
        !path.exists(),
        "the socket file is removed once the listener closes"
    );
}

/// Serves the streams sent through a channel, as a listener of another transport would.
struct InMemory(mpsc::Receiver<BoxStream>);

impl Listener for InMemory {
    fn local_addr(&self) -> ListenAddr {
        ListenAddr::Other("memory".into())
    }

    async fn accept(&mut self) -> io::Result<Option<(BoxStream, ConnectionInfo)>> {
        let remote = "198.51.100.4:1234".parse().unwrap();
        let local = "192.0.2.1:4040".parse().unwrap();
        Ok(self
            .0
            .recv()
            .await
            .map(|stream| (stream, ConnectionInfo::new(remote, local))))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_listeners_serve_connections_until_they_end() {
    let (sender, receiver) = mpsc::channel(4);
    let server = echo_with_address()
        .listener(InMemory(receiver))
        .build()
        .unwrap();
    assert_eq!(
        server.start().unwrap(),
        [ListenAddr::Other("memory".into())]
    );

    let (client, connection) = tokio::io::duplex(1024);
    sender
        .send(Box::new(connection) as BoxStream)
        .await
        .unwrap();
    let (reader, writer) = tokio::io::split(client);
    let mut client = Client::new(reader, writer);
    client.send(b"hi\r\n").await;
    assert_eq!(client.line().await, "hi 198.51.100.4:1234");

    // The listener ending does not stop the server: the session goes on.
    drop(sender);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.status(), ServerStatus::Running);
    client.send(b"still here\r\n").await;
    assert_eq!(client.line().await, "still here 198.51.100.4:1234");

    server.shutdown().await;
    client.expect_closed().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_listeners_stop_with_stop_accepting() {
    let (sender, receiver) = mpsc::channel(4);
    let server = echo_with_address()
        .listener(InMemory(receiver))
        .build()
        .unwrap();
    server.start().unwrap();

    let (client, connection) = tokio::io::duplex(1024);
    sender
        .send(Box::new(connection) as BoxStream)
        .await
        .unwrap();
    let (reader, writer) = tokio::io::split(client);
    let mut client = Client::new(reader, writer);
    client.send(b"hi\r\n").await;
    assert_eq!(client.line().await, "hi 198.51.100.4:1234");

    // Stopping acceptance drops the listener, which the sender notices, while the session lives.
    server.stop_accepting();
    timeout(WAIT, sender.closed())
        .await
        .expect("the listener was not dropped");
    client.send(b"draining\r\n").await;
    assert_eq!(client.line().await, "draining 198.51.100.4:1234");

    server.shutdown().await;
    client.expect_closed().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_start_leaves_no_unix_socket_file_behind() {
    let path = std::env::temp_dir().join(format!(
        "supersocket-failed-start-{}.sock",
        std::process::id()
    ));
    // A TCP listener after the Unix one fails to bind: its port is taken.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let server = echo_with_address()
        .listen_unix(&path)
        .listen(taken.local_addr().unwrap())
        .build()
        .unwrap();
    assert!(matches!(
        server.start(),
        Err(supersocket::Error::Bind { .. })
    ));
    assert!(!path.exists(), "the socket file was left behind");
    assert_eq!(server.status(), ServerStatus::Idle);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_unix_socket_in_use_is_not_taken_over() {
    let path = std::env::temp_dir().join(format!("supersocket-in-use-{}.sock", std::process::id()));
    let first = echo_with_address().listen_unix(&path).build().unwrap();
    first.start().unwrap();

    let second = echo_with_address().listen_unix(&path).build().unwrap();
    match second.start() {
        Err(supersocket::Error::Bind { source, .. }) => {
            assert_eq!(source.kind(), io::ErrorKind::AddrInUse, "{source}");
        }
        other => panic!("expected a bind error, got {other:?}"),
    }

    // The first server still serves, and still owns the socket file.
    let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (reader, writer) = stream.into_split();
    let mut client = Client::new(reader, writer);
    client.send(b"hello\r\n").await;
    assert_eq!(client.line().await, "hello 0.0.0.0:0");
    assert!(path.exists());
    first.shutdown().await;
    assert!(!path.exists());
}
