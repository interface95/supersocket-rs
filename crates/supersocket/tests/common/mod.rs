//! Helpers shared by the integration tests.

#![allow(dead_code)]

use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use supersocket::{CloseReason, Server, ServerBuilder, SessionState};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
    time::timeout,
};

pub const WAIT: Duration = Duration::from_secs(5);

/// A stream whose writes never finish and whose reads never end: a session served over it has a
/// writer that never drains, which fills its send queue deterministically, without depending on
/// any operating system's socket buffering.
pub struct Stall;

impl AsyncRead for Stall {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for Stall {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Builds the server listening on a free local port and starts it.
pub fn start<P: Send + 'static, S: SessionState>(
    builder: ServerBuilder<P, S>,
) -> (Server<P, S>, SocketAddr) {
    let server = builder.listen("127.0.0.1:0").build().unwrap();
    let address = server.start().unwrap()[0];
    (server, address)
}

/// Reports the close reason of every session through the returned receiver.
pub fn track_close_reasons<P: Send + 'static, S: SessionState>(
    builder: ServerBuilder<P, S>,
) -> (ServerBuilder<P, S>, mpsc::UnboundedReceiver<CloseReason>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let builder = builder.on_closed(move |_, reason| {
        let sender = sender.clone();
        async move {
            let _ = sender.send(reason);
        }
    });
    (builder, receiver)
}

pub async fn next_reason(receiver: &mut mpsc::UnboundedReceiver<CloseReason>) -> CloseReason {
    timeout(WAIT, receiver.recv())
        .await
        .expect("no session was closed")
        .unwrap()
}

pub struct Client<R = OwnedReadHalf, W = OwnedWriteHalf> {
    reader: BufReader<R>,
    writer: W,
}

impl Client {
    pub async fn connect(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        Self::new(reader, writer)
    }
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Client<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    pub fn reader(&mut self) -> &mut BufReader<R> {
        &mut self.reader
    }

    pub async fn send(&mut self, data: impl AsRef<[u8]>) {
        self.writer.write_all(data.as_ref()).await.unwrap();
        self.writer.flush().await.unwrap();
    }

    pub async fn line(&mut self) -> String {
        let mut line = String::new();
        timeout(WAIT, self.reader.read_line(&mut line))
            .await
            .expect("timed out waiting for a line")
            .unwrap();
        line.trim_end_matches("\r\n").to_owned()
    }

    pub async fn bytes(&mut self, length: usize) -> Vec<u8> {
        let mut buf = vec![0; length];
        timeout(WAIT, self.reader.read_exact(&mut buf))
            .await
            .expect("timed out waiting for bytes")
            .unwrap();
        buf
    }

    /// Waits until the server closes the connection; a reset counts as closed.
    pub async fn expect_closed(&mut self) {
        let mut rest = Vec::new();
        let read = timeout(WAIT, self.reader.read_to_end(&mut rest))
            .await
            .expect("the server did not close the connection");
        if read.is_ok() {
            assert!(
                rest.is_empty(),
                "unexpected data before close: {:?}",
                String::from_utf8_lossy(&rest)
            );
        }
    }
}

#[cfg(feature = "tls")]
pub mod tls;
