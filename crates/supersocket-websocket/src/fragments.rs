//! Sending a message in fragments; see [`FragmentWriter`].

use std::{fmt, future::Future, io, pin::Pin};

use tokio::sync::RwLockWriteGuard;

use crate::{
    MessageKind,
    frame::{FIN, OP_CONTINUATION},
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where the frames of a fragmented message go: a server session or a client sender.
pub(crate) trait FrameSink: Send + Sync {
    /// Sends a frame with the first byte `first`.
    fn send_frame<'a>(&'a self, first: u8, payload: &'a [u8]) -> BoxFuture<'a, io::Result<()>>;

    /// Queues a frame without waiting.
    fn try_send_frame(&self, first: u8, payload: &[u8]) -> io::Result<()>;

    /// A message could not be ended: the connection cannot go on and is closed.
    fn abandon(&self);
}

/// Sends one message in fragments, for messages too large to hold in memory or produced over
/// time; created by [`WebSocketSession::send_fragmented`](crate::WebSocketSession::send_fragmented)
/// and the client's `WebSocketSender::send_fragmented`.
///
/// Every [`write`](Self::write) sends one fragment and [`finish`](Self::finish) the last one.
/// Other messages wait until the message is finished, while pings, pongs and close frames go
/// through in between, as RFC 6455 allows. The fragments of a text message must add up to valid
/// UTF-8, though a fragment may end in the middle of a character. The message is not compressed.
///
/// Dropping the writer before `finish` ends the message where it is, so that the connection
/// stays usable; when even that cannot be queued, the connection is closed.
pub struct FragmentWriter<'a> {
    sink: &'a dyn FrameSink,
    _sending: RwLockWriteGuard<'a, ()>,
    opcode: u8,
    started: bool,
    finished: bool,
}

impl<'a> FragmentWriter<'a> {
    pub(crate) fn new(
        sink: &'a dyn FrameSink,
        sending: RwLockWriteGuard<'a, ()>,
        kind: MessageKind,
    ) -> Self {
        Self {
            sink,
            _sending: sending,
            opcode: kind.opcode(),
            started: false,
            finished: false,
        }
    }

    /// Sends `data` as the next fragment.
    pub async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        let first = self.first_byte(false);
        self.sink.send_frame(first, data).await?;
        self.started = true;
        Ok(())
    }

    /// Sends `data` as the last fragment, which may be empty, and ends the message.
    pub async fn finish(mut self, data: &[u8]) -> io::Result<()> {
        let first = self.first_byte(true);
        self.sink.send_frame(first, data).await?;
        self.finished = true;
        Ok(())
    }

    /// The first byte of the next frame: the opcode on the first fragment, a continuation
    /// afterwards, with FIN on the last one.
    fn first_byte(&self, last: bool) -> u8 {
        let opcode = if self.started {
            OP_CONTINUATION
        } else {
            self.opcode
        };
        if last { FIN | opcode } else { opcode }
    }
}

impl Drop for FragmentWriter<'_> {
    fn drop(&mut self) {
        if self.finished || !self.started {
            return;
        }
        if self
            .sink
            .try_send_frame(FIN | OP_CONTINUATION, b"")
            .is_err()
        {
            self.sink.abandon();
        }
    }
}

impl fmt::Debug for FragmentWriter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FragmentWriter")
            .field("started", &self.started)
            .field("finished", &self.finished)
            .finish()
    }
}
