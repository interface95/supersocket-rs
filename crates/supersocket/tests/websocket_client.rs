//! Tests of the WebSocket client, against the WebSocket server and against tungstenite.

mod common;

use std::{io, net::SocketAddr};

use common::{WAIT, tls::TestCertificate};
use futures_util::{SinkExt, StreamExt};
use supersocket::{
    CloseReason,
    client::{ReadError, TlsConnector},
    websocket::{
        CloseCode, CloseFrame, Message, Rejection, WebSocketBuilder, WebSocketClient,
        WebSocketServer,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        self,
        handshake::{
            derive_accept_key,
            server::{Request, Response},
        },
    },
};

fn start(builder: WebSocketBuilder) -> (WebSocketServer, SocketAddr) {
    let server = builder.listen("127.0.0.1:0").build().unwrap();
    let address = server.start().unwrap()[0];
    (server, address)
}

async fn receive(client: &mut WebSocketClient) -> Option<Message> {
    timeout(WAIT, client.receive())
        .await
        .expect("timed out waiting for a message")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn talks_to_the_websocket_server() {
    let (closes, mut closed) = mpsc::unbounded_channel();
    let (server, address) = start(
        WebSocketServer::builder()
            .subprotocols(["echo"])
            .on_message(async |session, message| {
                session.send(&message).await?;
                Ok(())
            })
            .on_closed(move |session, reason| {
                let closes = closes.clone();
                async move {
                    let _ = closes.send((reason, session.close_frame().cloned()));
                }
            }),
    );
    let mut client = WebSocketClient::builder(format!("ws://{address}/echo"))
        .subprotocols(["other", "echo"])
        .connect()
        .await
        .unwrap();
    assert_eq!(client.protocol(), Some("echo"));

    client.send_text("hello").await.unwrap();
    assert_eq!(receive(&mut client).await, Some(Message::from("hello")));
    let large = vec![42; 100_000];
    client.send_binary(&large).await.unwrap();
    assert_eq!(receive(&mut client).await, Some(Message::from(large)));

    client.close(CloseCode::NORMAL, "done").await.unwrap();
    let (reason, frame) = timeout(WAIT, closed.recv()).await.unwrap().unwrap();
    assert_eq!(reason, CloseReason::RemoteClosed);
    assert_eq!(
        frame,
        Some(CloseFrame {
            code: CloseCode::NORMAL,
            reason: "done".into()
        })
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receive_ends_when_the_server_closes() {
    let (server, address) = start(WebSocketServer::builder().on_message(
        async |session, _message| {
            session.close(CloseCode::new(4001), "bye")?;
            Ok(())
        },
    ));
    let mut client = WebSocketClient::connect(format!("ws://{address}/"))
        .await
        .unwrap();
    client.send_text("close me").await.unwrap();
    assert_eq!(receive(&mut client).await, None);
    assert_eq!(
        client.close_frame(),
        Some(&CloseFrame {
            code: CloseCode::new(4001),
            reason: "bye".into()
        })
    );
    assert!(client.send_text("late").await.is_err());
    assert_eq!(receive(&mut client).await, None);
    server.shutdown().await;
}

/// A tungstenite echo server that chooses the `echo` subprotocol, pings every client first and
/// reports the pongs it receives.
async fn start_tungstenite(pongs: mpsc::UnboundedSender<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let pongs = pongs.clone();
            tokio::spawn(async move {
                // tungstenite decides the type of the error.
                #[allow(clippy::result_large_err)]
                let choose_protocol = |request: &Request, mut response: Response| {
                    let offered = request
                        .headers()
                        .get("Sec-WebSocket-Protocol")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    if offered.split(',').any(|protocol| protocol.trim() == "echo") {
                        response
                            .headers_mut()
                            .insert("Sec-WebSocket-Protocol", "echo".parse().unwrap());
                    }
                    Ok(response)
                };
                let mut ws = accept_hdr_async(stream, choose_protocol).await.unwrap();
                ws.send(tungstenite::Message::Ping("hi".into()))
                    .await
                    .unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    match message {
                        tungstenite::Message::Pong(payload) => {
                            let _ = pongs.send(payload.to_vec());
                        }
                        tungstenite::Message::Text(_) | tungstenite::Message::Binary(_) => {
                            ws.send(message).await.unwrap();
                        }
                        _ => {}
                    }
                }
            });
        }
    });
    address
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn talks_to_tungstenite() {
    let (pongs, mut received_pongs) = mpsc::unbounded_channel();
    let address = start_tungstenite(pongs).await;
    let mut client = WebSocketClient::builder(format!("ws://{address}/"))
        .subprotocols(["echo"])
        .connect()
        .await
        .unwrap();
    assert_eq!(client.protocol(), Some("echo"));

    client.send_text("hello").await.unwrap();
    // The server's ping is answered while the client waits for the echo.
    assert_eq!(receive(&mut client).await, Some(Message::from("hello")));
    assert_eq!(
        timeout(WAIT, received_pongs.recv()).await.unwrap().unwrap(),
        b"hi"
    );
    let large = (0..200_000).map(|index| index as u8).collect::<Vec<_>>();
    client.send_binary(&large).await.unwrap();
    assert_eq!(receive(&mut client).await, Some(Message::from(large)));
    client.close(CloseCode::NORMAL, "").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connects_over_tls() {
    let certificate = TestCertificate::new("websocket");
    let server = WebSocketServer::builder()
        .on_message(async |session, _message| {
            let text = format!("secure={}", session.is_secure());
            session.send_text(&text).await?;
            Ok(())
        })
        .configure(|options| options.listeners.push(certificate.listener()))
        .build()
        .unwrap();
    let address = server.start().unwrap()[0];

    let mut client = WebSocketClient::builder(format!("wss://{address}/"))
        .tls(TlsConnector::with_root_certificates(certificate.roots()).server_name("localhost"))
        .connect()
        .await
        .unwrap();
    client.send_text("hi").await.unwrap();
    assert_eq!(
        receive(&mut client).await,
        Some(Message::from("secure=true"))
    );
    client.close(CloseCode::NORMAL, "").await.unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_handshakes_are_errors() {
    let (server, address) = start(
        WebSocketServer::builder()
            .on_handshake(async |_session| Err(Rejection::new(403, "go away"))),
    );
    let error = WebSocketClient::connect(format!("ws://{address}/"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    assert!(error.to_string().contains("403"), "{error}");
    server.shutdown().await;
}

/// Runs `serve` for the first connection on a free port, after reading its handshake request
/// and passing it the request's `Sec-WebSocket-Key`.
async fn fake_server<F, Fut>(serve: F) -> SocketAddr
where
    F: FnOnce(TcpStream, String) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.unwrap());
        }
        let request = String::from_utf8(request).unwrap();
        let key = request
            .lines()
            .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap()
            .to_owned();
        serve(stream, key).await;
    });
    address
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_accept_keys_are_rejected() {
    let address = fake_server(async |mut stream: TcpStream, _key| {
        stream
            .write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
                  Connection: Upgrade\r\nSec-WebSocket-Accept: bm90IHRoZSByaWdodCBrZXk=\r\n\r\n",
            )
            .await
            .unwrap();
        let _ = stream.read(&mut [0; 16]).await;
    })
    .await;
    let error = WebSocketClient::connect(format!("ws://{address}/"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn masked_frames_from_the_server_break_the_protocol() {
    let (codes, mut received_codes) = mpsc::unbounded_channel();
    let address = fake_server(move |mut stream: TcpStream, key: String| async move {
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Accept: {}\r\n\r\n",
            derive_accept_key(key.as_bytes())
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        // A masked text frame holding "a"; servers must not mask.
        stream
            .write_all(&[0x81, 0x81, 1, 2, 3, 4, b'a' ^ 1])
            .await
            .unwrap();
        // The client answers with a close frame, masked as client frames are.
        let mut header = [0; 6];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header[0], 0x88);
        assert_ne!(header[1] & 0x80, 0, "client frames are masked");
        let mut payload = vec![0; usize::from(header[1] & 0x7F)];
        stream.read_exact(&mut payload).await.unwrap();
        for (byte, key) in payload.iter_mut().zip(header[2..].iter().cycle()) {
            *byte ^= key;
        }
        let _ = codes.send(u16::from_be_bytes([payload[0], payload[1]]));
    })
    .await;
    let mut client = WebSocketClient::connect(format!("ws://{address}/"))
        .await
        .unwrap();
    let error = timeout(WAIT, client.receive()).await.unwrap().unwrap_err();
    assert!(matches!(error, ReadError::Protocol(_)), "{error}");
    assert_eq!(
        timeout(WAIT, received_codes.recv()).await.unwrap(),
        Some(1002)
    );
    assert_eq!(receive(&mut client).await, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_and_protocol_errors_close_the_transport_while_handles_live() {
    for protocol_error in [false, true] {
        let (ended, mut closed) = mpsc::unbounded_channel();
        let address = fake_server(move |mut stream: TcpStream, key: String| async move {
            let response = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                 Sec-WebSocket-Accept: {}\r\n\r\n",
                derive_accept_key(key.as_bytes())
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            let frame: &[u8] = if protocol_error {
                &[0x81, 0x81, 1, 2, 3, 4, b'a' ^ 1]
            } else {
                &[0x88, 0x02, 0x03, 0xe8]
            };
            stream.write_all(frame).await.unwrap();
            let mut header = [0; 6];
            stream.read_exact(&mut header).await.unwrap();
            assert_eq!(header[0], 0x88);
            assert_ne!(header[1] & 0x80, 0);
            let mut payload = vec![0; usize::from(header[1] & 0x7f)];
            stream.read_exact(&mut payload).await.unwrap();
            // Keep the server's socket open: the client must finish closing its side itself.
            let read = stream.read(&mut [0; 1]).await.unwrap();
            ended.send(read).unwrap();
        })
        .await;
        let mut client = WebSocketClient::connect(format!("ws://{address}/"))
            .await
            .unwrap();
        let sender = client.sender();
        let received = timeout(WAIT, client.receive()).await.unwrap();
        if protocol_error {
            assert!(matches!(received, Err(ReadError::Protocol(_))));
        } else {
            assert!(matches!(received, Ok(None)));
        }
        assert_eq!(timeout(WAIT, closed.recv()).await.unwrap(), Some(0));
        assert!(sender.is_closed());
        assert_eq!(receive(&mut client).await, None);
    }
}
