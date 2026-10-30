use std::{
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A stream that first yields bytes already read from it, such as data a proxy sent right after
/// its handshake, then reads on from the stream. Writes go straight to the stream.
pub struct Rewind<S> {
    prefix: Bytes,
    inner: S,
}

impl<S> Rewind<S> {
    /// A stream that yields `prefix`, then what `inner` delivers.
    pub fn new(prefix: Bytes, inner: S) -> Self {
        Self { prefix, inner }
    }

    /// The underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }
}

impl<S: fmt::Debug> fmt::Debug for Rewind<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rewind")
            .field("prefix", &self.prefix.len())
            .field("inner", &self.inner)
            .finish()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Rewind<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.prefix.is_empty() {
            let length = this.prefix.len().min(buf.remaining());
            buf.put_slice(&this.prefix[..length]);
            this.prefix.advance(length);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Rewind<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn yields_the_prefix_first() {
        let inner: &[u8] = b" world";
        let mut stream = Rewind::new(Bytes::from_static(b"hello"), inner);
        let mut text = String::new();
        stream.read_to_string(&mut text).await.unwrap();
        assert_eq!(text, "hello world");
    }
}
