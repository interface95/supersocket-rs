//! Tests of the UDP listener.

mod common;

use std::{net::SocketAddr, time::Duration};

use common::{WAIT, next_reason, track_close_reasons};
use supersocket::{
    ListenAddr, ServerBuilder,
    prelude::*,
    udp::{SessionKey, UdpListener},
};
use tokio::{net::UdpSocket, time::timeout};

/// Echoes every line together with the address of the session.
fn echo() -> ServerBuilder<TextPackage> {
    Server::builder(LineFilter::default).on_package(async |session, package| {
        let reply = format!("{} from {}\r\n", package.text, session.remote_addr());
        session.send(reply).await?;
        Ok(())
    })
}

async fn receive(client: &UdpSocket) -> String {
    let mut buf = [0u8; 1024];
    let (length, _) = timeout(WAIT, client.recv_from(&mut buf))
        .await
        .expect("no reply")
        .unwrap();
    String::from_utf8_lossy(&buf[..length])
        .trim_end_matches("\r\n")
        .to_owned()
}

async fn exchange(client: &UdpSocket, server: SocketAddr, text: &str) -> String {
    client.send_to(text.as_bytes(), server).await.unwrap();
    receive(client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn datagrams_become_sessions_keyed_by_their_sender() {
    let listener = UdpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let server = echo().listener(listener).build().unwrap();
    assert_eq!(
        server.start().unwrap(),
        [ListenAddr::Other(format!("udp://{address}"))]
    );

    let alice = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let bob = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (alice_addr, bob_addr) = (alice.local_addr().unwrap(), bob.local_addr().unwrap());
    assert_eq!(
        exchange(&alice, address, "hello\r\n").await,
        format!("hello from {alice_addr}")
    );
    assert_eq!(
        exchange(&bob, address, "hi\r\n").await,
        format!("hi from {bob_addr}")
    );
    assert_eq!(
        exchange(&alice, address, "again\r\n").await,
        format!("again from {alice_addr}")
    );
    assert_eq!(server.info().session_count(), 2);

    // A package split across datagrams is joined, as it would be on a connection.
    alice.send_to(b"spl", address).await.unwrap();
    assert_eq!(
        exchange(&alice, address, "it\r\n").await,
        format!("split from {alice_addr}")
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_udp_sessions_are_closed_and_reopened() {
    let listener = UdpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let (builder, mut reasons) = track_close_reasons(echo().configure(|options| {
        options.idle_session_timeout = Some(Duration::from_millis(100));
        options.clear_idle_session_interval = Duration::from_millis(50);
    }));
    let server = builder.listener(listener).build().unwrap();
    server.start().unwrap();

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    assert!(
        exchange(&client, address, "one\r\n")
            .await
            .starts_with("one from")
    );
    assert_eq!(next_reason(&mut reasons).await, CloseReason::Timeout);
    // The next datagram from the same peer opens a new session.
    assert!(
        exchange(&client, address, "two\r\n")
            .await
            .starts_with("two from")
    );
    assert_eq!(server.info().session_count(), 1);
    server.shutdown().await;
}

/// Keys sessions by the first byte of each datagram, whatever address it comes from.
struct ByFirstByte;

impl SessionKey for ByFirstByte {
    type Key = u8;

    fn key(&self, _remote: SocketAddr, datagram: &[u8]) -> u8 {
        datagram.first().copied().unwrap_or(0)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_can_be_keyed_by_the_datagram() {
    let listener = UdpListener::bind("127.0.0.1:0".parse().unwrap())
        .unwrap()
        .keyed_by(ByFirstByte);
    let address = listener.local_addr();
    let server = echo().listener(listener).build().unwrap();
    server.start().unwrap();

    let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let first_addr = first.local_addr().unwrap();
    assert_eq!(
        exchange(&first, address, "a1\r\n").await,
        format!("a1 from {first_addr}")
    );
    // The second socket's datagram belongs to the same session: the peer moved, so the reply
    // goes to the second socket, while the session keeps the address it was opened from.
    assert_eq!(
        exchange(&second, address, "a2\r\n").await,
        format!("a2 from {first_addr}")
    );
    assert_eq!(server.info().session_count(), 1);
    assert_eq!(
        exchange(&first, address, "a3\r\n").await,
        format!("a3 from {first_addr}")
    );
    // A different first byte is another session.
    assert_eq!(
        exchange(&second, address, "b1\r\n").await,
        format!("b1 from {}", second.local_addr().unwrap())
    );
    assert_eq!(server.info().session_count(), 2);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_datagrams_are_ignored_and_big_batches_are_split() {
    let listener = UdpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let (builder, mut reasons) = track_close_reasons(
        Server::builder(LineFilter::default).on_package(async |session, package| {
            if package.text == "big" {
                // Several datagrams' worth in one reply.
                session.send(format!("{}\r\n", "x".repeat(70_000))).await?;
            } else {
                session.send(format!("{}\r\n", package.text)).await?;
            }
            Ok(())
        }),
    );
    let server = builder.listener(listener).build().unwrap();
    server.start().unwrap();

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // An empty datagram from an unknown peer opens nothing.
    client.send_to(b"", address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.info().session_count(), 0);
    assert_eq!(exchange(&client, address, "one\r\n").await, "one");
    // An empty datagram is not the end of the stream either.
    client.send_to(b"", address).await.unwrap();
    assert_eq!(exchange(&client, address, "two\r\n").await, "two");
    assert!(reasons.try_recv().is_err(), "the session was closed");

    client.send_to(b"big\r\n", address).await.unwrap();
    let mut received = Vec::new();
    let mut buf = vec![0u8; 65_536];
    while received.len() < 70_002 {
        let (length, _) = timeout(WAIT, client.recv_from(&mut buf))
            .await
            .expect("the reply did not arrive")
            .unwrap();
        received.extend_from_slice(&buf[..length]);
    }
    assert_eq!(received.len(), 70_002);
    assert!(received[..70_000].iter().all(|&byte| byte == b'x'));
    assert_eq!(server.info().session_count(), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_go_on_while_the_server_waits_for_a_connection_slot() {
    let listener = UdpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let server = echo()
        .configure(|options| options.max_connections = Some(1))
        .listener(listener)
        .build()
        .unwrap();
    server.start().unwrap();

    let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let first_addr = first.local_addr().unwrap();
    assert_eq!(
        exchange(&first, address, "one\r\n").await,
        format!("one from {first_addr}")
    );
    // The second peer has to wait for a slot; the first one is still served meanwhile.
    second.send_to(b"waiting\r\n", address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        exchange(&first, address, "two\r\n").await,
        format!("two from {first_addr}")
    );
    assert_eq!(server.info().session_count(), 1);
    server.shutdown().await;
}
