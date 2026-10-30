//! Tests of connections that do not come straight from a TCP listener: handed over from other
//! transports, or wrapped by stream layers.

mod common;

use std::{
    io,
    time::{Duration, Instant},
};

use common::{Client, start};
use supersocket::{
    BoxStream, ConnectionInfo, ListenOptions, ServerBuilder, StreamLayer, prelude::*,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
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
    let address = server.start().unwrap()[0];
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
