//! Tests of the client against the server.

mod common;

use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

use common::{WAIT, next_reason, start, track_close_reasons};
use supersocket::{
    FixedHeaderFilter, FixedHeaderProtocol, ServerBuilder,
    bytes::{BufMut, Bytes, BytesMut},
    client::ReadError,
    prelude::*,
};
use tokio::{net::TcpListener, sync::mpsc, time::timeout};

fn echo_lines() -> ServerBuilder<TextPackage> {
    Server::builder(LineFilter::default).on_package(async |session, package| {
        session.send(format!("{}\r\n", package.text)).await?;
        Ok(())
    })
}

async fn connect(address: SocketAddr) -> EasyClient<TextPackage> {
    EasyClient::builder(LineFilter::default)
        .connect(address)
        .await
        .unwrap()
}

async fn receive_text(client: &mut EasyClient<TextPackage>) -> Option<String> {
    timeout(WAIT, client.receive())
        .await
        .expect("timed out waiting for a package")
        .unwrap()
        .map(|package| package.text)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receives_replies_by_pulling() {
    let (server, address) = start(echo_lines());
    let mut client = connect(address).await;

    client.send("hello\r\nworld\r\n").await.unwrap();
    assert_eq!(receive_text(&mut client).await.as_deref(), Some("hello"));
    assert_eq!(receive_text(&mut client).await.as_deref(), Some("world"));
    assert_eq!(client.peer_addr(), Some(address));
    assert!(client.local_addr().is_some());

    client.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hands_packages_to_a_handler_and_closes_cleanly() {
    let (builder, mut reasons) = track_close_reasons(echo_lines());
    let (server, address) = start(builder);
    let (packages, mut received) = mpsc::unbounded_channel();

    let sender = connect(address)
        .await
        .start_receive(move |_sender, package: TextPackage| {
            let packages = packages.clone();
            async move {
                let _ = packages.send(package.text);
            }
        });
    for line in ["one", "two", "three"] {
        sender.send(format!("{line}\r\n")).await.unwrap();
    }
    for expected in ["one", "two", "three"] {
        let text = timeout(WAIT, received.recv()).await.unwrap().unwrap();
        assert_eq!(text, expected);
    }

    sender.close().await;
    assert!(sender.is_closed());
    assert_eq!(next_reason(&mut reasons).await, CloseReason::RemoteClosed);
    timeout(WAIT, sender.closed()).await.unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_every_sender_closes_a_receiving_client() {
    let (lines, mut received) = mpsc::unbounded_channel();
    let (builder, mut reasons) = track_close_reasons(
        Server::builder(LineFilter::default).on_package(move |_session, package: TextPackage| {
            let lines = lines.clone();
            async move {
                let _ = lines.send(package.text);
                Ok(())
            }
        }),
    );
    let (server, address) = start(builder);

    let sender = connect(address)
        .await
        .start_receive(|_sender, _package: TextPackage| async {});
    sender.send("sent before the drop\r\n").await.unwrap();
    // The receiving task alone must not keep the connection open.
    drop(sender);
    assert_eq!(
        timeout(WAIT, received.recv()).await.unwrap().as_deref(),
        Some("sent before the drop")
    );
    assert_eq!(next_reason(&mut reasons).await, CloseReason::RemoteClosed);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_gives_up_on_a_server_that_stops_reading() {
    // Accepts the connection but never reads from it. A tiny receive buffer keeps the receive
    // window small on every platform (it also turns off Windows' receive autotuning), so the
    // client's writer blocks after a little data rather than the kernel swallowing it all.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    socket2::SockRef::from(&listener)
        .set_recv_buffer_size(4 * 1024)
        .unwrap();
    let address = listener.local_addr().unwrap();
    let accepting = tokio::spawn(async move { listener.accept().await.unwrap() });

    let close_timeout = Duration::from_millis(200);
    let client = EasyClient::builder(LineFilter::default)
        .configure(|options| options.close_timeout = close_timeout)
        .connect(address)
        .await
        .unwrap();
    let _server_side = accepting.await.unwrap();
    // Far more than the socket buffers hold, so the writer gets stuck.
    client.send(vec![b'x'; 16 * 1024 * 1024]).await.unwrap();

    // Every caller waits for the queued data, and gives up once the close timeout has passed.
    let started = Instant::now();
    let close = |sender: ClientSender| async move {
        sender.close().await;
        started.elapsed()
    };
    let (first, second) = timeout(WAIT, async {
        tokio::join!(close(client.sender()), close(client.sender()))
    })
    .await
    .expect("close did not give up");
    assert!(
        first >= close_timeout && second >= close_timeout,
        "{first:?} {second:?}"
    );
    assert!(client.sender().is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receive_ends_after_an_error() {
    let (server, address) = start(echo_lines().on_connected(async |session| {
        let _ = session.send("a line longer than the client allows").await;
    }));
    let mut client = EasyClient::builder(LineFilter::default)
        .configure(|options| options.max_package_length = 8)
        .connect(address)
        .await
        .unwrap();

    let error = timeout(WAIT, client.receive()).await.unwrap().unwrap_err();
    assert!(matches!(error, ReadError::TooLarge { limit: 8 }), "{error}");
    assert!(client.sender().is_closed());
    // The data that caused the error is not decoded again.
    assert!(
        timeout(WAIT, client.receive())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_replies_through_its_sender_to_a_greeting() {
    let (server, address) = start(echo_lines().on_connected(async |session| {
        let _ = session.send("welcome\r\n").await;
    }));
    let (packages, mut received) = mpsc::unbounded_channel();

    // The client sends nothing until the server greets it.
    let _sender = connect(address)
        .await
        .start_receive(move |sender, package: TextPackage| {
            let packages = packages.clone();
            async move {
                if package.text == "welcome" {
                    sender.send("thanks\r\n").await.unwrap();
                } else {
                    let _ = packages.send(package.text);
                }
            }
        });
    assert_eq!(
        timeout(WAIT, received.recv()).await.unwrap().unwrap(),
        "thanks"
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_closing_ends_receiving_and_sending() {
    let (server, address) = start(Server::builder(LineFilter::default).on_package(
        async |session, package| {
            if package.text == "quit" {
                session.send("bye\r\n").await?;
                session.close(CloseReason::LocalClosed);
            }
            Ok(())
        },
    ));
    let mut client = connect(address).await;
    let sender = client.sender();

    client.send("quit\r\n").await.unwrap();
    assert_eq!(receive_text(&mut client).await.as_deref(), Some("bye"));
    assert_eq!(receive_text(&mut client).await, None);
    timeout(WAIT, sender.closed()).await.unwrap();
    let error = sender.send("too late\r\n").await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn senders_in_many_tasks_do_not_interleave() {
    let (server, address) = start(echo_lines());
    let mut client = connect(address).await;

    for task in 0..8 {
        let sender = client.sender();
        tokio::spawn(async move {
            for line in 0..100 {
                let text = format!("task{task} line{line} {}\r\n", "x".repeat(100));
                sender.send(text).await.unwrap();
            }
        });
    }
    let mut next_line = [0; 8];
    for _ in 0..8 * 100 {
        let text = receive_text(&mut client).await.unwrap();
        let (task, rest) = text
            .strip_prefix("task")
            .unwrap()
            .split_once(" line")
            .unwrap();
        let (number, padding) = rest.split_once(' ').unwrap();
        let task: usize = task.parse().unwrap();
        assert_eq!(number.parse::<usize>().unwrap(), next_line[task]);
        assert_eq!(padding.len(), 100);
        next_line[task] += 1;
    }

    client.close().await;
    server.shutdown().await;
}

/// A 2-byte big-endian length followed by the body; client and server share this code.
#[derive(Default)]
struct LengthPrefixed;

impl FixedHeaderProtocol for LengthPrefixed {
    type Package = Bytes;
    const HEADER_SIZE: usize = 2;

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        Ok(u16::from_be_bytes([header[0], header[1]]) as usize)
    }

    fn decode(&mut self, package: Bytes) -> Result<Bytes, ProtocolError> {
        Ok(package.slice(Self::HEADER_SIZE..))
    }
}

struct LengthPrefixedEncoder;

impl PackageEncoder<[u8]> for LengthPrefixedEncoder {
    fn encode(&self, body: &[u8], buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let length = u16::try_from(body.len()).map_err(|_| ProtocolError::new("body too long"))?;
        buf.put_u16(length);
        buf.put_slice(body);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_and_server_share_protocol_code() {
    let (server, address) = start(
        Server::builder(FixedHeaderFilter::<LengthPrefixed>::default).on_package(
            async |session, body: Bytes| {
                let mut reversed = body.to_vec();
                reversed.reverse();
                session
                    .send_package(&LengthPrefixedEncoder, &reversed[..])
                    .await
            },
        ),
    );
    let mut client = EasyClient::builder(FixedHeaderFilter::<LengthPrefixed>::default)
        .connect(address)
        .await
        .unwrap();

    client
        .send_package(&LengthPrefixedEncoder, &b"abc"[..])
        .await
        .unwrap();
    let reply = timeout(WAIT, client.receive()).await.unwrap().unwrap();
    assert_eq!(reply.as_deref(), Some(&b"cba"[..]));

    client.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_closed_notifies_tasks_spawned_for_the_session() {
    let (done, mut finished) = mpsc::unbounded_channel();
    let (server, address) = start(Server::builder(LineFilter::default).on_package(
        move |session, _package| {
            let done = done.clone();
            async move {
                tokio::spawn(async move {
                    session.closed().await;
                    let _ = done.send(session.id());
                });
                Ok(())
            }
        },
    ));
    let client = connect(address).await;
    client.send("start\r\n").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    client.close().await;
    timeout(WAIT, finished.recv())
        .await
        .expect("the spawned task was not told that the session closed");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connecting_fails_for_closed_ports_and_bad_endpoints() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let error = EasyClient::builder(LineFilter::default)
        .connect(address)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);

    let error = EasyClient::builder(LineFilter::default)
        .connect("missing-port")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}
