//! Tests of the WebSocket server, against tungstenite and against frames written by hand.

mod common;

use std::{net::SocketAddr, sync::atomic::AtomicUsize, sync::atomic::Ordering, time::Duration};

use common::WAIT;
use futures_util::{SinkExt, StreamExt};
use supersocket::{
    CloseReason,
    websocket::{
        CloseCode, CloseFrame, Rejection, WebSocketBuilder, WebSocketServer, WebSocketSession,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        self,
        client::IntoClientRequest,
        protocol::{CloseFrame as TungsteniteCloseFrame, frame::coding::CloseCode as Code},
    },
};

type Tungstenite = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn start<S: Default + Send + Sync + 'static>(
    builder: WebSocketBuilder<S>,
) -> (WebSocketServer<S>, SocketAddr) {
    let server = builder.listen("127.0.0.1:0").build().unwrap();
    let address = server.start().unwrap()[0];
    (server, address)
}

fn echo() -> WebSocketBuilder {
    WebSocketServer::builder().on_message(async |session, message| {
        session.send(&message).await?;
        Ok(())
    })
}

async fn connect(address: SocketAddr) -> Tungstenite {
    connect_async(format!("ws://{address}/")).await.unwrap().0
}

async fn next(ws: &mut Tungstenite) -> tungstenite::Message {
    timeout(WAIT, ws.next())
        .await
        .expect("timed out waiting for a message")
        .expect("the connection ended")
        .unwrap()
}

/// Reads until the server closes the connection.
async fn expect_end(ws: &mut Tungstenite) {
    loop {
        match timeout(WAIT, ws.next())
            .await
            .expect("the connection stayed open")
        {
            None | Some(Err(_)) => return,
            Some(Ok(_)) => {}
        }
    }
}

fn close_code(message: tungstenite::Message) -> (u16, String) {
    match message {
        tungstenite::Message::Close(Some(frame)) => (frame.code.into(), frame.reason.to_string()),
        other => panic!("expected a close frame, got {other:?}"),
    }
}

/// Reports the close reason and the client's close frame of every session.
fn track_closes<S: Default + Send + Sync + 'static>(
    builder: WebSocketBuilder<S>,
) -> (
    WebSocketBuilder<S>,
    mpsc::UnboundedReceiver<(CloseReason, Option<CloseFrame>)>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let builder = builder.on_closed(move |session, reason| {
        let sender = sender.clone();
        async move {
            let _ = sender.send((reason, session.close_frame().cloned()));
        }
    });
    (builder, receiver)
}

async fn next_close(
    closes: &mut mpsc::UnboundedReceiver<(CloseReason, Option<CloseFrame>)>,
) -> (CloseReason, Option<CloseFrame>) {
    timeout(WAIT, closes.recv())
        .await
        .expect("no session was closed")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn echoes_text_and_binary_to_tungstenite() {
    let (server, address) = start(echo());
    let (mut ws, response) = connect_async(format!("ws://{address}/echo")).await.unwrap();
    assert_eq!(response.status(), 101);

    ws.send(tungstenite::Message::text("hello")).await.unwrap();
    assert_eq!(next(&mut ws).await, tungstenite::Message::text("hello"));
    // Large enough for the 64-bit length form.
    let large = (0..300 * 1024).map(|index| index as u8).collect::<Vec<_>>();
    ws.send(tungstenite::Message::binary(large.clone()))
        .await
        .unwrap();
    assert_eq!(next(&mut ws).await, tungstenite::Message::binary(large));
    ws.send(tungstenite::Message::Ping("are you there".into()))
        .await
        .unwrap();
    assert_eq!(
        next(&mut ws).await,
        tungstenite::Message::Pong("are you there".into())
    );

    ws.close(None).await.unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_initiated_close_is_answered_and_reported() {
    let (builder, mut closes) = track_closes(echo());
    let (server, address) = start(builder);
    let mut ws = connect(address).await;

    ws.close(Some(TungsteniteCloseFrame {
        code: Code::Normal,
        reason: "bye".into(),
    }))
    .await
    .unwrap();
    assert_eq!(close_code(next(&mut ws).await), (1000, String::new()));
    let (reason, frame) = next_close(&mut closes).await;
    assert_eq!(reason, CloseReason::RemoteClosed);
    assert_eq!(
        frame,
        Some(CloseFrame {
            code: CloseCode::NORMAL,
            reason: "bye".into()
        })
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_initiated_close_waits_for_the_answer() {
    let (builder, mut closes) = track_closes(WebSocketServer::builder().on_message(
        async |session, message| {
            if message.as_text() == Some("close") {
                session.close(CloseCode::new(4000), "done").await;
                // Nothing may follow the close frame.
                assert!(session.send_text("late").await.is_err());
            }
            Ok(())
        },
    ));
    let (server, address) = start(builder);
    let mut ws = connect(address).await;

    ws.send(tungstenite::Message::text("close")).await.unwrap();
    assert_eq!(close_code(next(&mut ws).await), (4000, "done".into()));
    // Reading on sends tungstenite's answer; the server then closes the connection.
    expect_end(&mut ws).await;
    let (reason, frame) = next_close(&mut closes).await;
    assert_eq!(reason, CloseReason::LocalClosed);
    assert_eq!(frame.map(|frame| frame.code), Some(CloseCode::new(4000)));
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_tells_clients_the_server_is_going_away() {
    let (server, address) = start(echo());
    let mut ws = connect(address).await;
    ws.send(tungstenite::Message::text("ready")).await.unwrap();
    next(&mut ws).await;

    let shutdown = tokio::spawn(async move { server.shutdown().await });
    assert_eq!(close_code(next(&mut ws).await).0, 1001);
    timeout(WAIT, shutdown).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handshakes_can_be_rejected_and_connected_sessions_greeted() {
    let (paths, mut connected) = mpsc::unbounded_channel();
    let (server, address) = start(
        echo()
            .on_handshake(async |session| match session.request().query() {
                Some("token=secret") => Ok(()),
                _ => Err(Rejection::new(403, "wrong token")),
            })
            .on_connected(move |session| {
                let paths = paths.clone();
                async move {
                    let _ = paths.send(session.request().path().to_owned());
                    let _ = session.send_text("welcome").await;
                }
            }),
    );

    let error = connect_async(format!("ws://{address}/chat?token=wrong"))
        .await
        .unwrap_err();
    match error {
        tungstenite::Error::Http(response) => assert_eq!(response.status(), 403),
        other => panic!("expected an HTTP error, got {other:?}"),
    }

    let (mut ws, _) = connect_async(format!("ws://{address}/chat?token=secret"))
        .await
        .unwrap();
    assert_eq!(next(&mut ws).await, tungstenite::Message::text("welcome"));
    assert_eq!(
        timeout(WAIT, connected.recv()).await.unwrap().as_deref(),
        Some("/chat")
    );
    // Only the accepted session connected.
    assert_eq!(server.info().session_count(), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negotiates_the_preferred_subprotocol() {
    let (server, address) = start(
        WebSocketServer::builder()
            .subprotocols(["chat.v2", "chat.v1"])
            .on_message(async |session, _message| {
                let protocol = session.protocol().unwrap_or("none").to_owned();
                session.send_text(&protocol).await?;
                Ok(())
            }),
    );
    let mut request = format!("ws://{address}/").into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "chat.v1, chat.v2".parse().unwrap(),
    );
    let (mut ws, response) = connect_async(request).await.unwrap();
    assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "chat.v2");
    ws.send(tungstenite::Message::text("which")).await.unwrap();
    assert_eq!(next(&mut ws).await, tungstenite::Message::text("chat.v2"));
    server.shutdown().await;
}

#[derive(Default)]
struct Counter {
    messages: AtomicUsize,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_carry_their_own_state() {
    let (server, address) = start(WebSocketServer::builder().session::<Counter>().on_message(
        async |session: WebSocketSession<Counter>, _message| {
            let count = session.messages.fetch_add(1, Ordering::SeqCst) + 1;
            session.send_text(&count.to_string()).await?;
            Ok(())
        },
    ));
    let mut ws = connect(address).await;
    for expected in ["1", "2", "3"] {
        ws.send(tungstenite::Message::text("count")).await.unwrap();
        assert_eq!(next(&mut ws).await, tungstenite::Message::text(expected));
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcasts_skip_connections_still_upgrading() {
    let (server, address) = start(WebSocketServer::builder().on_message(
        async |session, message| {
            for other in session.sessions() {
                let _ = other.send(&message).await;
            }
            Ok(())
        },
    ));
    let mut first = connect(address).await;
    let mut second = connect(address).await;
    // Connected, but the handshake request has not been sent yet.
    let mut upgrading = TcpStream::connect(address).await.unwrap();
    // Make sure the server has seen the raw connection before the broadcast.
    tokio::time::sleep(Duration::from_millis(50)).await;

    first
        .send(tungstenite::Message::text("hi all"))
        .await
        .unwrap();
    assert_eq!(next(&mut first).await, tungstenite::Message::text("hi all"));
    assert_eq!(
        next(&mut second).await,
        tungstenite::Message::text("hi all")
    );

    // The first bytes the late client receives are the handshake response.
    let response = RawClient::handshake(&mut upgrading).await;
    assert!(response.starts_with("HTTP/1.1 101 "), "{response}");
    server.shutdown().await;
}

/// A client that writes frames by hand.
struct RawClient {
    stream: TcpStream,
}

const MASK: [u8; 4] = [0xa1, 0xb2, 0xc3, 0xd4];

impl RawClient {
    async fn connect(address: SocketAddr) -> Self {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let response = Self::handshake(&mut stream).await;
        assert!(response.starts_with("HTTP/1.1 101 "), "{response}");
        Self { stream }
    }

    /// Sends a handshake request and returns the response head.
    async fn handshake(stream: &mut TcpStream) -> String {
        stream
            .write_all(
                b"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n\
                  Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                  Sec-WebSocket-Version: 13\r\n\r\n",
            )
            .await
            .unwrap();
        read_head(stream).await
    }

    /// Sends a frame whose first byte is `first`, masked unless `masked` is false.
    async fn send(&mut self, first: u8, payload: &[u8], masked: bool) {
        let mut frame = vec![first];
        let mask_bit = if masked { 0x80 } else { 0 };
        match payload.len() {
            length @ 0..=125 => frame.push(mask_bit | length as u8),
            length => {
                frame.push(mask_bit | 126);
                frame.extend((length as u16).to_be_bytes());
            }
        }
        if masked {
            frame.extend(MASK);
            frame.extend(
                payload
                    .iter()
                    .zip(MASK.iter().cycle())
                    .map(|(byte, key)| byte ^ key),
            );
        } else {
            frame.extend(payload);
        }
        self.stream.write_all(&frame).await.unwrap();
    }

    /// Reads a frame from the server: its first byte and payload.
    async fn receive(&mut self) -> (u8, Vec<u8>) {
        timeout(WAIT, async {
            let mut header = [0; 2];
            self.stream.read_exact(&mut header).await.unwrap();
            assert_eq!(header[1] & 0x80, 0, "server frames are not masked");
            let length = match header[1] & 0x7F {
                126 => usize::from(self.stream.read_u16().await.unwrap()),
                127 => self.stream.read_u64().await.unwrap() as usize,
                length => usize::from(length),
            };
            let mut payload = vec![0; length];
            self.stream.read_exact(&mut payload).await.unwrap();
            (header[0], payload)
        })
        .await
        .expect("timed out waiting for a frame")
    }

    /// Expects a close frame with `code`, then the end of the connection.
    async fn expect_close(&mut self, code: CloseCode) {
        let (first, payload) = self.receive().await;
        assert_eq!(first, 0x88);
        assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), code.code());
        let mut rest = Vec::new();
        let _ = timeout(WAIT, self.stream.read_to_end(&mut rest))
            .await
            .expect("the server did not close the connection");
    }
}

async fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    timeout(WAIT, async {
        while !head.ends_with(b"\r\n\r\n") {
            head.push(stream.read_u8().await.unwrap());
        }
    })
    .await
    .expect("timed out waiting for the response");
    String::from_utf8(head).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joins_fragments_and_answers_pings_in_between() {
    let (server, address) = start(echo());
    let mut client = RawClient::connect(address).await;

    client.send(0x01, b"Hel", true).await;
    client.send(0x89, b"ping", true).await;
    client.send(0x80, b"lo", true).await;
    assert_eq!(client.receive().await, (0x8A, b"ping".to_vec()));
    assert_eq!(client.receive().await, (0x81, b"Hello".to_vec()));

    client.send(0x88, &[0x03, 0xE8], true).await;
    client.expect_close(CloseCode::NORMAL).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_violations_close_with_the_matching_code() {
    let (server, address) = start(echo().configure(|options| options.max_package_length = 1024));

    let mut unmasked = RawClient::connect(address).await;
    unmasked.send(0x81, b"plain", false).await;
    unmasked.expect_close(CloseCode::PROTOCOL_ERROR).await;

    let mut not_text = RawClient::connect(address).await;
    not_text.send(0x81, &[0xFF, 0xFE], true).await;
    not_text.expect_close(CloseCode::INVALID_PAYLOAD).await;

    let mut too_big = RawClient::connect(address).await;
    too_big.send(0x82, &[0; 2000], true).await;
    too_big.expect_close(CloseCode::MESSAGE_TOO_BIG).await;

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_that_are_not_handshakes_get_an_http_error() {
    let (server, address) = start(echo());
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let response = read_head(&mut stream).await;
    assert!(response.starts_with("HTTP/1.1 400 "), "{response}");
    let mut rest = Vec::new();
    timeout(WAIT, stream.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rest, b"not a WebSocket upgrade request");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_that_never_upgrade_time_out() {
    let limit = Duration::from_millis(200);
    let (server, address) = start(echo().configure(|options| options.handshake_timeout = limit));
    let mut stream = TcpStream::connect(address).await.unwrap();
    let started = tokio::time::Instant::now();
    let mut rest = Vec::new();
    timeout(WAIT, stream.read_to_end(&mut rest))
        .await
        .expect("the server did not close the connection")
        .unwrap();
    assert!(started.elapsed() >= limit);
    assert!(rest.is_empty());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handshakes_beyond_the_maximum_size_get_an_http_error() {
    let (server, address) = start(echo().configure(|options| options.max_package_length = 1024));
    let mut stream = TcpStream::connect(address).await.unwrap();
    // A head that is still incomplete when it passes the maximum size.
    let request = format!(
        "GET / HTTP/1.1\r\nHost: localhost\r\nCookie: {}",
        "x".repeat(2000)
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let response = read_head(&mut stream).await;
    assert!(response.starts_with("HTTP/1.1 431 "), "{response}");
    server.shutdown().await;
}
