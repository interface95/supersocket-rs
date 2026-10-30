use std::{fmt, io, sync::Mutex, time::Duration};

use bytes::BytesMut;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::Notify,
};

/// Data waiting to be sent on a connection.
///
/// Any number of tasks call [`send`](Self::send), and one writer task runs
/// [`run_writer`](Self::run_writer), which writes everything that piled up in one go. Data queued
/// back to back therefore goes out in one system call, and data from concurrent sends is never
/// interleaved.
///
/// ```
/// use std::sync::Arc;
/// use supersocket_connection::SendQueue;
///
/// # async fn run(stream: tokio::net::TcpStream) -> std::io::Result<()> {
/// let queue = Arc::new(SendQueue::new(64 * 1024));
/// let writer = tokio::spawn({
///     let queue = queue.clone();
///     async move { queue.run_writer(stream, None).await }
/// });
/// queue.send(b"hello\r\n").await?;
/// queue.close();
/// writer.await??;
/// # Ok(())
/// # }
/// ```
pub struct SendQueue {
    state: Mutex<State>,
    data_available: Notify,
    drained: Notify,
    limit: usize,
}

#[derive(Default)]
struct State {
    pending: BytesMut,
    /// Bytes the writer took and is still writing.
    in_flight: usize,
    closed: bool,
    /// The last data has been queued: nothing more is accepted, but the queue is still written.
    sealed: bool,
}

impl SendQueue {
    /// A queue whose [`send`](Self::send) waits once `limit` bytes are waiting to be written or
    /// being written. A send is accepted whenever nothing is, so a limit of zero sends one piece
    /// at a time.
    pub fn new(limit: usize) -> Self {
        Self {
            state: Mutex::default(),
            data_available: Notify::new(),
            drained: Notify::new(),
            limit,
        }
    }

    /// Queues `data`. Waits while the limit is reached, and fails with
    /// [`ErrorKind::BrokenPipe`](io::ErrorKind::BrokenPipe) once the queue is closed.
    pub async fn send(&self, data: &[u8]) -> io::Result<()> {
        self.send_vectored(&[data]).await
    }

    /// Queues `parts` back to back as one piece, so data from concurrent sends never lands
    /// between them, without copying them together first. Otherwise like [`send`](Self::send).
    pub async fn send_vectored(&self, parts: &[&[u8]]) -> io::Result<()> {
        loop {
            let drained = self.drained.notified();
            if self.enqueue(parts)? {
                break;
            }
            drained.await;
        }
        self.data_available.notify_one();
        Ok(())
    }

    /// Queues `data` without waiting: fails with [`ErrorKind::WouldBlock`](io::ErrorKind::WouldBlock)
    /// while the limit is reached, and with [`ErrorKind::BrokenPipe`](io::ErrorKind::BrokenPipe)
    /// once the queue is closed. For broadcasts, where a peer that reads slowly should be skipped
    /// rather than waited for.
    pub fn try_send(&self, data: &[u8]) -> io::Result<()> {
        self.try_send_vectored(&[data])
    }

    /// Queues `parts` back to back as one piece without waiting, like [`try_send`](Self::try_send).
    pub fn try_send_vectored(&self, parts: &[&[u8]]) -> io::Result<()> {
        if !self.enqueue(parts)? {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.data_available.notify_one();
        Ok(())
    }

    /// Queues `parts` when there is room; `Ok(false)` means the limit is reached.
    fn enqueue(&self, parts: &[&[u8]]) -> io::Result<bool> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.sealed {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        // What is queued and what is being written both count; a peer that stopped reading
        // leaves a batch in flight for good.
        let waiting = state.pending.len() + state.in_flight;
        if waiting != 0 && waiting >= self.limit {
            return Ok(false);
        }
        state
            .pending
            .reserve(parts.iter().map(|part| part.len()).sum());
        for part in parts {
            state.pending.extend_from_slice(part);
        }
        Ok(true)
    }

    /// Queues `data` as the last data to send, without waiting for room: every later send fails,
    /// while the writer still writes what is queued and runs until the queue is closed. For a
    /// goodbye message that nothing may follow, such as a WebSocket close frame. Fails with
    /// [`ErrorKind::BrokenPipe`](io::ErrorKind::BrokenPipe) once the queue is closed or the last
    /// data was already queued.
    pub fn send_last(&self, data: &[u8]) -> io::Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            if state.closed || state.sealed {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            state.pending.extend_from_slice(data);
            state.sealed = true;
        }
        self.data_available.notify_one();
        // Senders waiting for room fail now instead of waiting.
        self.drained.notify_waiters();
        Ok(())
    }

    /// Stops accepting data. The writer sends what is already queued and finishes.
    pub fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.data_available.notify_one();
        self.drained.notify_waiters();
    }

    /// Whether the queue has been closed.
    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    /// Writes queued data to `writer` until the queue is closed and empty, then shuts `writer`
    /// down. A write that does not finish within `send_timeout` fails with
    /// [`ErrorKind::TimedOut`](io::ErrorKind::TimedOut). The queue is closed when this returns,
    /// whether it succeeded or not.
    ///
    /// Each written batch is freed, so an idle connection holds no send buffer.
    pub async fn run_writer<W: AsyncWrite + Unpin>(
        &self,
        mut writer: W,
        send_timeout: Option<Duration>,
    ) -> io::Result<()> {
        loop {
            let chunk = {
                let mut state = self.state.lock().unwrap();
                // The previous batch is written.
                state.in_flight = 0;
                if state.pending.is_empty() {
                    if state.closed {
                        break;
                    }
                    None
                } else {
                    let chunk = std::mem::take(&mut state.pending);
                    state.in_flight = chunk.len();
                    Some(chunk)
                }
            };
            // Room may have opened up: the previous batch is written.
            self.drained.notify_waiters();
            let Some(chunk) = chunk else {
                self.data_available.notified().await;
                continue;
            };
            let write = async {
                writer.write_all(&chunk).await?;
                writer.flush().await
            };
            let written = match send_timeout {
                Some(limit) => tokio::time::timeout(limit, write)
                    .await
                    .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into())),
                None => write.await,
            };
            if let Err(error) = written {
                self.close();
                // Still try to tell the peer that the connection ends, but give up as a write
                // would.
                let shutdown = writer.shutdown();
                let _ = match send_timeout {
                    Some(limit) => tokio::time::timeout(limit, shutdown).await.ok(),
                    None => Some(shutdown.await),
                };
                return Err(error);
            }
        }
        writer.shutdown().await
    }
}

impl fmt::Debug for SendQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().unwrap();
        f.debug_struct("SendQueue")
            .field("pending", &state.pending.len())
            .field("in_flight", &state.in_flight)
            .field("closed", &state.closed)
            .field("limit", &self.limit)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn writes_everything_then_shuts_down() {
        let (writer, mut reader) = tokio::io::duplex(1024);
        let queue = Arc::new(SendQueue::new(0));
        let task = tokio::spawn({
            let queue = queue.clone();
            async move { queue.run_writer(writer, None).await }
        });
        for piece in [&b"one "[..], b"two "] {
            queue.send(piece).await.unwrap();
        }
        queue.send_vectored(&[b"th", b"r", b"ee"]).await.unwrap();
        queue.close();
        assert_eq!(
            queue.send(b"late").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        task.await.unwrap().unwrap();
        let mut received = String::new();
        reader.read_to_string(&mut received).await.unwrap();
        assert_eq!(received, "one two three");
    }

    #[test]
    fn try_send_never_waits() {
        let queue = SendQueue::new(8);
        queue.try_send(b"12345678").unwrap();
        assert_eq!(
            queue.try_send(b"x").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        queue.close();
        assert_eq!(
            queue.try_send(b"x").unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn nothing_follows_the_last_data() {
        let (writer, mut reader) = tokio::io::duplex(1024);
        let queue = Arc::new(SendQueue::new(1024));
        let task = tokio::spawn({
            let queue = queue.clone();
            async move { queue.run_writer(writer, None).await }
        });
        queue.send(b"data ").await.unwrap();
        queue.send_last(b"bye").unwrap();
        for late in [queue.send(b"late").await, queue.send_last(b"again")] {
            assert_eq!(late.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        }
        // The writer keeps running until the queue is closed.
        let mut received = [0; 8];
        reader.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"data bye");
        assert!(!task.is_finished());
        queue.close();
        task.await.unwrap().unwrap();
    }

    /// Fails every write and records whether it was shut down.
    struct Broken(Arc<std::sync::atomic::AtomicBool>);

    impl AsyncWrite for Broken {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn a_failed_write_still_shuts_the_writer_down() {
        let shut_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let queue = SendQueue::new(1024);
        queue.send(b"data").await.unwrap();
        let error = queue
            .run_writer(Broken(shut_down.clone()), None)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        assert!(shut_down.load(std::sync::atomic::Ordering::SeqCst));
        assert!(queue.is_closed());
    }

    #[tokio::test]
    async fn times_out_when_the_peer_stops_reading() {
        let (writer, _reader) = tokio::io::duplex(16);
        let queue = Arc::new(SendQueue::new(1024));
        let task = tokio::spawn({
            let queue = queue.clone();
            async move {
                queue
                    .run_writer(writer, Some(Duration::from_millis(100)))
                    .await
            }
        });
        queue.send(&[b'x'; 256]).await.unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(queue.is_closed());
    }
}
