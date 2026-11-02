use std::{
    fmt,
    future::poll_fn,
    io,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::Mutex,
    task::Poll,
    time::Duration,
};

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
    /// The transport is handed over once the queue is written: the writer leaves it open.
    detaching: bool,
}

/// The initial capacity of a new batch in the send buffer: room for a batch of small messages,
/// within the size the allocator serves from its thread cache.
const BATCH_CAPACITY: usize = 1024;

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

    /// Queues what `write` appends to the send buffer, like [`send`](Self::send) without a buffer
    /// in between: a package is encoded straight into the queue, like SuperSocket's `SendAsync`
    /// with a `PipeWriter` action. `write` runs while the queue is locked, so it must be quick and
    /// must not wait. It is called once at most, once there is room. Nothing is queued when it
    /// fails, and its error is returned, as is the queue's own error once the queue is closed.
    pub async fn send_with<E: From<io::Error>>(
        &self,
        write: impl FnOnce(&mut BytesMut) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut write = Some(write);
        loop {
            let drained = self.drained.notified();
            if self.enqueue_with(&mut write)? {
                break;
            }
            drained.await;
        }
        self.data_available.notify_one();
        Ok(())
    }

    /// Like [`send_with`](Self::send_with) without waiting: fails with
    /// [`ErrorKind::WouldBlock`](io::ErrorKind::WouldBlock) while the limit is reached, in which
    /// case `write` is not called.
    pub fn try_send_with<E: From<io::Error>>(
        &self,
        write: impl FnOnce(&mut BytesMut) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut write = Some(write);
        if !self.enqueue_with(&mut write)? {
            return Err(io::Error::from(io::ErrorKind::WouldBlock).into());
        }
        self.data_available.notify_one();
        Ok(())
    }

    /// Whether more data may be queued: an error once the queue is closed, `false` while the
    /// limit is reached.
    fn has_room(&self, state: &State) -> io::Result<bool> {
        if state.closed || state.sealed {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        // What is queued and what is being written both count; a peer that stopped reading
        // leaves a batch in flight for good.
        let waiting = state.pending.len() + state.in_flight;
        Ok(waiting == 0 || waiting < self.limit)
    }

    /// Runs `write` on the send buffer when there is room, taking it out of `write`; `Ok(false)`
    /// means the limit is reached and `write` was left untouched. What `write` appended before
    /// failing or panicking is removed again, and a panic continues unwinding once the queue is
    /// unlocked, so that it cannot poison the lock.
    fn enqueue_with<E: From<io::Error>>(
        &self,
        write: &mut Option<impl FnOnce(&mut BytesMut) -> Result<(), E>>,
    ) -> Result<bool, E> {
        let mut state = self.state.lock().unwrap();
        if !self.has_room(&state)? {
            return Ok(false);
        }
        if state.pending.capacity() == 0 {
            state.pending.reserve(BATCH_CAPACITY.min(self.limit));
        }
        let before = state.pending.len();
        let write = write.take().expect("the writer is used once");
        match catch_unwind(AssertUnwindSafe(|| write(&mut state.pending))) {
            Ok(Ok(())) => Ok(true),
            Ok(Err(error)) => {
                state.pending.truncate(before);
                Err(error)
            }
            Err(panic) => {
                state.pending.truncate(before);
                drop(state);
                resume_unwind(panic)
            }
        }
    }

    /// Queues `parts` when there is room; `Ok(false)` means the limit is reached.
    fn enqueue(&self, parts: &[&[u8]]) -> io::Result<bool> {
        let mut state = self.state.lock().unwrap();
        if !self.has_room(&state)? {
            return Ok(false);
        }
        let size: usize = parts.iter().map(|part| part.len()).sum();
        if state.pending.capacity() == 0 {
            // The writer took the previous batch, so this starts a new one. Give it room for a
            // batch of small messages at once instead of growing a few bytes at a time; the
            // buffer is freed again once written.
            state
                .pending
                .reserve(size.max(BATCH_CAPACITY.min(self.limit)));
        } else {
            state.pending.reserve(size);
        }
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

    /// Stops accepting data like [`close`](Self::close), but the writer leaves the transport
    /// open once it has written what is queued, for a connection that is taken over.
    pub fn detach(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.detaching = true;
        }
        self.data_available.notify_one();
        self.drained.notify_waiters();
    }

    /// Whether the queue has been closed.
    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    /// Writes queued data to `writer` until the queue is closed and empty, then shuts `writer`
    /// down, unless the queue was [detached](Self::detach). A write that does not finish within
    /// `send_timeout` fails with [`ErrorKind::TimedOut`](io::ErrorKind::TimedOut). The queue is
    /// closed when this returns, whether it succeeded or not.
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
                // Make one best-effort shutdown attempt, without delaying the error that tells
                // the owner to close the connection. A failed transport may never finish it.
                let _ = poll_fn(|cx| Poll::Ready(Pin::new(&mut writer).poll_shutdown(cx))).await;
                return Err(error);
            }
        }
        if self.state.lock().unwrap().detaching {
            return Ok(());
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

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    #[tokio::test]
    async fn a_detached_queue_leaves_the_transport_open() {
        let (writer, mut reader) = tokio::io::duplex(1024);
        let queue = Arc::new(SendQueue::new(1024));
        let mut writer = Some(writer);
        let task = tokio::spawn({
            let queue = queue.clone();
            let mut writer = writer.take().unwrap();
            async move { queue.run_writer(&mut writer, None).await.map(|()| writer) }
        });
        queue.send(b"last words").await.unwrap();
        queue.detach();
        assert_eq!(
            queue.send(b"late").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        let mut writer = task.await.unwrap().unwrap();
        // The writer is still usable: it was not shut down.
        writer.write_all(b" and more").await.unwrap();
        drop(writer);
        let mut received = String::new();
        reader.read_to_string(&mut received).await.unwrap();
        assert_eq!(received, "last words and more");
    }

    #[tokio::test]
    async fn send_with_encodes_into_the_queue() {
        let (writer, mut reader) = tokio::io::duplex(1024);
        let queue = Arc::new(SendQueue::new(1024));
        let task = tokio::spawn({
            let queue = queue.clone();
            async move { queue.run_writer(writer, None).await }
        });
        queue
            .send_with(|buf| {
                buf.extend_from_slice(b"one ");
                Ok::<_, io::Error>(())
            })
            .await
            .unwrap();
        // A failing writer leaves nothing behind.
        let error = queue
            .send_with(|buf| {
                buf.extend_from_slice(b"garbage");
                Err(io::Error::other("encoding failed"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "encoding failed");
        queue
            .try_send_with(|buf| {
                buf.extend_from_slice(b"two");
                Ok::<_, io::Error>(())
            })
            .unwrap();
        queue.close();
        task.await.unwrap().unwrap();
        let mut received = String::new();
        reader.read_to_string(&mut received).await.unwrap();
        assert_eq!(received, "one two");
        assert_eq!(
            queue
                .send_with(|_| Ok::<_, io::Error>(()))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn a_panicking_writer_leaves_the_queue_usable() {
        let queue = SendQueue::new(1024);
        queue.try_send(b"kept ").unwrap();
        let panicked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            queue.try_send_with(|buf| {
                buf.extend_from_slice(b"partial");
                panic!("boom");
                #[allow(unreachable_code)]
                Ok::<_, io::Error>(())
            })
        }));
        assert!(panicked.is_err());
        // The lock is not poisoned and the partial data is gone.
        queue.try_send(b"after").unwrap();
        let state = queue.state.lock().unwrap();
        assert_eq!(&state.pending[..], b"kept after");
    }

    #[test]
    fn try_send_with_does_not_call_the_writer_without_room() {
        let queue = SendQueue::new(8);
        queue.try_send(b"12345678").unwrap();
        let mut called = false;
        let error = queue
            .try_send_with(|_| {
                called = true;
                Ok::<_, io::Error>(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(!called);
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
    struct Broken(Arc<std::sync::atomic::AtomicBool>, bool);

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
            if self.1 {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(Ok(()))
            }
        }
    }

    #[tokio::test]
    async fn a_failed_write_still_shuts_the_writer_down() {
        let shut_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let queue = SendQueue::new(1024);
        queue.send(b"data").await.unwrap();
        let error = queue
            .run_writer(Broken(shut_down.clone(), false), None)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        assert!(shut_down.load(std::sync::atomic::Ordering::SeqCst));
        assert!(queue.is_closed());
    }

    #[tokio::test]
    async fn a_failed_write_does_not_wait_for_transport_shutdown() {
        for send_timeout in [None, Some(Duration::from_secs(60))] {
            let attempted = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let queue = SendQueue::new(1024);
            queue.send(b"data").await.unwrap();
            let error = tokio::time::timeout(
                Duration::from_millis(100),
                queue.run_writer(Broken(attempted.clone(), true), send_timeout),
            )
            .await
            .expect("transport shutdown must not delay a write error")
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            assert!(attempted.load(std::sync::atomic::Ordering::SeqCst));
            assert!(queue.is_closed());
        }
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
