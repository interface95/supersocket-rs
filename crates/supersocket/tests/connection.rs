//! Tests of how connections are accepted: PROXY protocol headers and socket settings.

mod common;

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    time::{Duration, Instant},
};

use common::{Client, WAIT};
use supersocket::{ListenOptions, ProxyCommand, ServerBuilder, prelude::*};
use tokio::{io::AsyncReadExt, net::TcpStream, sync::mpsc, time::timeout};

/// Answers every line with the client's address as the session sees it and the PROXY protocol
/// version of the connection.
fn address_echo() -> ServerBuilder<TextPackage> {
    Server::builder(LineFilter::default).on_package(async |session, package| {
        let version = session.proxy_info().map_or(0, |info| info.version());
        let reply = format!("{} v{version} {}\r\n", session.remote_addr(), package.text);
        session.send(reply).await?;
        Ok(())
    })
}

fn start_on(
    builder: ServerBuilder<TextPackage>,
    listener: ListenOptions,
) -> (Server<TextPackage>, SocketAddr) {
    let server = builder
        .configure(|options| options.listeners.push(listener))
        .build()
        .unwrap();
    let address = server.start().unwrap()[0];
    (server, address)
}

fn behind_proxy() -> ListenOptions {
    ListenOptions {
        proxy_protocol: true,
        ..ListenOptions::new("127.0.0.1:0")
    }
}

/// Records the address every session has when middleware first sees it.
struct RecordAddresses(mpsc::UnboundedSender<SocketAddr>);

impl Middleware for RecordAddresses {
    async fn register_session(&self, session: &Session) -> bool {
        let _ = self.0.send(session.remote_addr());
        true
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_protocol_v1_gives_the_client_address() {
    let (addresses, mut registered) = mpsc::unbounded_channel();
    let (server, address) = start_on(
        address_echo().middleware(RecordAddresses(addresses)),
        behind_proxy(),
    );
    let mut client = Client::connect(address).await;
    // The data right behind the header arrives together with it.
    client
        .send(b"PROXY TCP4 203.0.113.7 192.0.2.1 51000 4040\r\nhello\r\n")
        .await;
    assert_eq!(client.line().await, "203.0.113.7:51000 v1 hello");
    // Middleware already sees the client, not the proxy.
    let first = timeout(WAIT, registered.recv()).await.unwrap().unwrap();
    assert_eq!(first, "203.0.113.7:51000".parse().unwrap());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_protocol_v2_gives_the_client_address() {
    let (sessions, mut connected) = mpsc::unbounded_channel();
    let (server, address) = start_on(
        address_echo().on_connected(move |session| {
            let sessions = sessions.clone();
            async move {
                let info = session.proxy_info().cloned().unwrap();
                let _ = sessions.send((info, session.local_addr()));
            }
        }),
        behind_proxy(),
    );
    // Version 2, PROXY, TCP over IPv6, 36 bytes of addresses.
    let mut header = b"\r\n\r\n\0\r\nQUIT\n\x21\x21\x00\x24".to_vec();
    header.extend("2001:db8::7".parse::<Ipv6Addr>().unwrap().octets());
    header.extend(Ipv6Addr::LOCALHOST.octets());
    header.extend(51000u16.to_be_bytes());
    header.extend(4040u16.to_be_bytes());

    let mut client = Client::connect(address).await;
    client.send(&header).await;
    // The data comes separately this time.
    tokio::time::sleep(Duration::from_millis(20)).await;
    client.send(b"hello\r\n").await;
    assert_eq!(client.line().await, "[2001:db8::7]:51000 v2 hello");

    let (info, local_addr) = timeout(WAIT, connected.recv()).await.unwrap().unwrap();
    assert_eq!(info.command(), ProxyCommand::Proxy);
    assert_eq!(info.destination(), Some("[::1]:4040".parse().unwrap()));
    // The proxy is the peer of the TCP connection; the local address stays the socket's.
    assert_eq!(info.proxy_addr().ip(), local_addr.ip());
    assert_eq!(local_addr, address);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_without_a_proxy_header_never_become_sessions() {
    let (addresses, mut registered) = mpsc::unbounded_channel();
    let (server, address) = start_on(
        address_echo().middleware(RecordAddresses(addresses)),
        behind_proxy(),
    );
    let mut direct = Client::connect(address).await;
    direct.send(b"hello\r\n").await;
    direct.expect_closed().await;
    assert!(registered.try_recv().is_err());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_headers_must_arrive_within_the_handshake_timeout() {
    let limit = Duration::from_millis(200);
    let (server, address) = start_on(
        address_echo().configure(|options| options.handshake_timeout = limit),
        behind_proxy(),
    );
    let mut stream = TcpStream::connect(address).await.unwrap();
    let started = Instant::now();
    let mut rest = Vec::new();
    let _ = timeout(WAIT, stream.read_to_end(&mut rest))
        .await
        .expect("the connection stayed open");
    assert!(started.elapsed() >= limit);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listener_socket_settings_reach_accepted_connections() {
    let (settings, mut observed) = mpsc::unbounded_channel();
    let builder = address_echo().configure_socket(move |stream| {
        let socket = socket2::SockRef::from(stream);
        // Windows has no way to read the keepalive time back.
        #[cfg(not(windows))]
        let keepalive_time = Some(socket.tcp_keepalive_time().unwrap());
        #[cfg(windows)]
        let keepalive_time: Option<Duration> = None;
        let _ = settings.send((
            stream.nodelay().unwrap(),
            socket.keepalive().unwrap(),
            keepalive_time,
            socket.recv_buffer_size().unwrap(),
        ));
    });
    let listener = ListenOptions {
        keepalive: Some(Duration::from_secs(42)),
        socket_receive_buffer_size: Some(32 * 1024),
        ..ListenOptions::new("127.0.0.1:0")
    };
    let (server, address) = start_on(builder, listener);

    let _client = TcpStream::connect(address).await.unwrap();
    let (no_delay, keepalive, keepalive_time, receive_buffer) =
        timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    assert!(no_delay);
    assert!(keepalive);
    assert!(keepalive_time.is_none_or(|time| time == Duration::from_secs(42)));
    // Smaller than the systems' defaults; Linux reports twice the size it was given.
    assert!(receive_buffer <= 2 * 32 * 1024, "{receive_buffer}");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_accept_hook_sees_the_client_behind_the_proxy() {
    let banned: IpAddr = "203.0.113.9".parse().unwrap();
    let (seen, mut accepted) = mpsc::unbounded_channel();
    let (server, address) = start_on(
        address_echo().on_accept(move |connection| {
            let _ = seen.send(connection.remote_addr());
            connection.remote_addr().ip() != banned
        }),
        behind_proxy(),
    );
    let mut refused = Client::connect(address).await;
    refused
        .send(b"PROXY TCP4 203.0.113.9 192.0.2.1 51000 4040\r\nhello\r\n")
        .await;
    refused.expect_closed().await;
    assert_eq!(
        timeout(WAIT, accepted.recv()).await.unwrap().unwrap(),
        "203.0.113.9:51000".parse().unwrap()
    );

    let mut welcome = Client::connect(address).await;
    welcome
        .send(b"PROXY TCP4 203.0.113.7 192.0.2.1 51001 4040\r\nhello\r\n")
        .await;
    assert_eq!(welcome.line().await, "203.0.113.7:51001 v1 hello");
    server.shutdown().await;
}
