use super::*;
use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use supersocket_client::{Connection, Endpoint, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct InMemory(Mutex<Option<Connection>>);

impl Connector for InMemory {
    async fn connect(
        &self,
        _target: &Endpoint,
        _previous: Option<Connection>,
    ) -> io::Result<Connection> {
        Ok(self.0.lock().unwrap().take().unwrap())
    }
}

async fn client_on(stream: impl Stream) -> WebSocketClient {
    let client = EasyClient::builder(|| crate::filter::FrameFilter::new(Role::Client, 1024))
        .proxy(InMemory(Mutex::new(Some(Connection::new(stream)))))
        .configure(|options| options.close_timeout = Duration::from_millis(25))
        .connect(("localhost", 80))
        .await
        .unwrap();
    let sender = WebSocketSender {
        sender: client.sender(),
        close_sent: Arc::new(AtomicBool::new(false)),
    };
    WebSocketClient {
        client: Some(client),
        sender,
        protocol: None,
        close_frame: None,
        finished: false,
        close_timeout: Duration::from_millis(25),
    }
}

#[tokio::test]
async fn cancelling_receive_does_not_cancel_transport_closure() {
    use std::{future::poll_fn, pin::pin};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for frame in [&b"\x88\x02\x03\xe8"[..], &b"\x81\x81\x01\x02\x03\x04`"[..]] {
        let (transport, mut peer) = tokio::io::duplex(128);
        let mut client = client_on(transport).await;
        peer.write_all(frame).await.unwrap();
        {
            // On this single-threaded runtime, receive handles the already buffered frame
            // and yields waiting for its writer. Cancel it before the writer can run.
            let mut receiving = pin!(client.receive());
            poll_fn(|cx| {
                assert!(receiving.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut response))
            .await
            .expect("the transport still closes after receive is cancelled")
            .unwrap();
        assert_eq!(response[0], 0x88);
        assert!(client.receive().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn terminal_receives_release_a_stalled_transport_after_the_close_timeout() {
    use std::{future::poll_fn, pin::pin};

    struct StalledWriter {
        input: Option<&'static [u8]>,
        dropped: Arc<AtomicBool>,
    }

    impl AsyncRead for StalledWriter {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let Some(input) = self.input else {
                return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
            };
            let length = input.len().min(buf.remaining());
            buf.put_slice(&input[..length]);
            self.input = Some(&input[length..]);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for StalledWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl Drop for StalledWriter {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    // Close, protocol violation, EOF and read errors must all release both stream halves.
    for input in [
        Some(&b"\x88\x02\x03\xe8"[..]),
        Some(&b"\x81\x81\x01\x02\x03\x04`"[..]),
        Some(&b""[..]),
        None,
    ] {
        for cancel_receive in [false, true] {
            let dropped = Arc::new(AtomicBool::new(false));
            let mut client = client_on(StalledWriter {
                input,
                dropped: dropped.clone(),
            })
            .await;
            let sender = client.sender();
            client.send_binary(&[42; 64]).await.unwrap();
            if cancel_receive {
                {
                    // Cancel after receive takes ownership of the connection and begins
                    // waiting for the writer, before that background task can run.
                    let mut receiving = pin!(client.receive());
                    poll_fn(|cx| {
                        assert!(receiving.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                }
                let result = tokio::time::timeout(
                    Duration::from_secs(1),
                    client.close(CloseCode::NORMAL, ""),
                )
                .await
                .expect("explicit close still waits for the stalled writer");
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            } else {
                let result = tokio::time::timeout(Duration::from_secs(1), client.receive())
                    .await
                    .expect("the close timeout bounds the stalled writer");
                assert!(!matches!(result, Ok(Some(_))));
                assert!(client.receive().await.unwrap().is_none());
            }
            assert!(
                dropped.load(Ordering::Acquire),
                "retained handles must not retain the stream (cancelled receive: {cancel_receive})"
            );
            assert!(sender.is_closed());
        }
    }
}
