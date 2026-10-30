use std::{
    fmt,
    future::poll_fn,
    io,
    mem::MaybeUninit,
    pin::Pin,
    task::{Poll, ready},
};

use bytes::BytesMut;
use supersocket_proto::{BoxFilter, ProtocolError};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use crate::catch_panic_sync;

/// Bytes read per poll while nothing is buffered. They are read into the stack and only what
/// arrived is copied to the heap, so a connection waiting for data holds no buffer.
const STACK_READ_SIZE: usize = 8 * 1024;

/// Limits of a [`PackageReader`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions {
    /// Largest package the filter may buffer before it is an error. Default: 1 MiB.
    pub max_package_length: usize,
    /// How much to read at a time while a package is incomplete. Default: 4 KiB.
    pub receive_buffer_size: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_package_length: 1024 * 1024,
            receive_buffer_size: 4 * 1024,
        }
    }
}

/// Why a [`PackageReader`] could not return the next package.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// Reading from the connection failed.
    #[error("read failed: {0}")]
    Io(#[from] io::Error),
    /// The pipeline filter rejected the data.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// More than the maximum package length was buffered without completing a package.
    #[error("package exceeds the maximum length of {limit} bytes")]
    TooLarge {
        /// The maximum package length.
        limit: usize,
    },
    /// The pipeline filter panicked; the value is the panic message.
    #[error("the pipeline filter panicked: {0}")]
    FilterPanicked(String),
}

/// Reads packages from `R`, cutting the byte stream with a pipeline filter.
///
/// ```
/// use supersocket_connection::{PackageReader, ReadOptions};
/// use supersocket_proto::LineFilter;
///
/// # async fn run() -> Result<(), supersocket_connection::ReadError> {
/// let data: &[u8] = b"hello\r\nworld\r\n";
/// let mut reader = PackageReader::new(data, Box::new(LineFilter::default()), ReadOptions::default());
/// assert_eq!(reader.next().await?.unwrap().text, "hello");
/// assert_eq!(reader.next().await?.unwrap().text, "world");
/// assert!(reader.next().await?.is_none());
/// # Ok(())
/// # }
/// ```
pub struct PackageReader<R, P> {
    reader: R,
    filter: BoxFilter<P>,
    buf: BytesMut,
    options: ReadOptions,
}

impl<R, P> PackageReader<R, P>
where
    R: AsyncRead + Unpin,
    P: Send + 'static,
{
    /// Creates a reader that cuts what `reader` delivers into packages with `filter`.
    pub fn new(reader: R, filter: BoxFilter<P>, options: ReadOptions) -> Self {
        Self {
            reader,
            filter,
            buf: BytesMut::new(),
            options,
        }
    }

    /// Returns the next package, or `Ok(None)` once the connection has ended. An incomplete
    /// package at the end is discarded.
    ///
    /// This is cancel safe: if the future is dropped before it completes, no data is lost.
    pub async fn next(&mut self) -> Result<Option<P>, ReadError> {
        loop {
            if let Some(package) = self.try_next()? {
                return Ok(Some(package));
            }
            if self.fill().await? == 0 {
                return Ok(None);
            }
        }
    }

    /// Reads more data without decoding it, returning how many bytes arrived; `0` means the
    /// connection has ended. Use it after [`try_next`](Self::try_next) returned `Ok(None)`.
    ///
    /// While nothing is buffered, nothing is allocated until data arrives. This is cancel safe.
    pub async fn fill(&mut self) -> Result<usize, ReadError> {
        // Everything received has been consumed: drop the buffer so an idle connection holds none.
        if self.buf.is_empty() {
            self.buf = BytesMut::new();
        }
        Ok(read_some(
            &mut self.reader,
            &mut self.buf,
            self.options.receive_buffer_size,
        )
        .await?)
    }

    /// Returns the next package if it has already been received, without reading. `Ok(None)`
    /// means more data is needed.
    pub fn try_next(&mut self) -> Result<Option<P>, ReadError> {
        while !self.buf.is_empty() {
            let step = catch_panic_sync(|| decode_step(&mut self.filter, &mut self.buf))
                .map_err(ReadError::FilterPanicked)??;
            match step {
                Step::Package(package) => return Ok(Some(package)),
                Step::Switched => {}
                Step::NeedMore => {
                    if self.buf.len() > self.options.max_package_length {
                        return Err(ReadError::TooLarge {
                            limit: self.options.max_package_length,
                        });
                    }
                    break;
                }
            }
        }
        Ok(None)
    }

    /// Returns the underlying reader and the bytes received but not consumed yet.
    pub fn into_parts(self) -> (R, BytesMut) {
        (self.reader, self.buf)
    }
}

impl<R, P> fmt::Debug for PackageReader<R, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PackageReader")
            .field("buffered", &self.buf.len())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

enum Step<P> {
    Package(P),
    Switched,
    NeedMore,
}

/// Runs the filter once: decodes a package if one is complete and applies a filter switch.
fn decode_step<P: Send + 'static>(
    filter: &mut BoxFilter<P>,
    buf: &mut BytesMut,
) -> Result<Step<P>, ProtocolError> {
    let package = filter.filter(buf)?;
    let switched = match filter.next_filter() {
        Some(next) => {
            filter.reset();
            *filter = next;
            true
        }
        None => false,
    };
    Ok(match package {
        Some(package) => {
            if !switched {
                filter.reset();
            }
            Step::Package(package)
        }
        None if switched => Step::Switched,
        None => Step::NeedMore,
    })
}

/// Reads more data into `buf`. While `buf` is empty, nothing is allocated until data arrives.
async fn read_some<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
    chunk_size: usize,
) -> io::Result<usize> {
    if !buf.is_empty() {
        buf.reserve(chunk_size);
        return reader.read_buf(buf).await;
    }
    poll_fn(|cx| {
        let mut stack = [MaybeUninit::<u8>::uninit(); STACK_READ_SIZE];
        let mut read_buf = ReadBuf::uninit(&mut stack);
        ready!(Pin::new(&mut *reader).poll_read(cx, &mut read_buf))?;
        buf.extend_from_slice(read_buf.filled());
        Poll::Ready(Ok(read_buf.filled().len()))
    })
    .await
}

#[cfg(test)]
mod tests {
    use supersocket_proto::{LineFilter, PipelineFilter, TextPackage};
    use tokio::io::AsyncWriteExt;

    use super::*;

    fn lines<R: AsyncRead + Unpin>(
        reader: R,
        options: ReadOptions,
    ) -> PackageReader<R, TextPackage> {
        PackageReader::new(reader, Box::new(LineFilter::default()), options)
    }

    #[tokio::test]
    async fn reads_packages_delivered_in_pieces() {
        let (client, server) = tokio::io::duplex(64);
        let mut reader = lines(server, ReadOptions::default());
        let writer = tokio::spawn(async move {
            let mut client = client;
            for piece in [&b"hel"[..], b"lo\r", b"\nwor", b"ld\r\n"] {
                client.write_all(piece).await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        assert_eq!(reader.next().await.unwrap().unwrap().text, "hello");
        assert_eq!(reader.next().await.unwrap().unwrap().text, "world");
        writer.await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fill_reads_without_decoding() {
        let data: &[u8] = b"partial";
        let mut reader = lines(data, ReadOptions::default());
        assert!(reader.try_next().unwrap().is_none());
        assert_eq!(reader.fill().await.unwrap(), 7);
        assert!(reader.try_next().unwrap().is_none());
        assert_eq!(reader.fill().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn enforces_the_maximum_package_length() {
        let data: &[u8] = &[b'x'; 64];
        let options = ReadOptions {
            max_package_length: 16,
            ..ReadOptions::default()
        };
        assert!(matches!(
            lines(data, options).next().await,
            Err(ReadError::TooLarge { limit: 16 })
        ));
    }

    struct Panicky;

    impl PipelineFilter for Panicky {
        type Package = ();

        fn filter(&mut self, _buf: &mut BytesMut) -> Result<Option<()>, ProtocolError> {
            panic!("filter exploded")
        }
    }

    #[tokio::test]
    async fn turns_filter_panics_into_errors() {
        let data: &[u8] = b"anything";
        let mut reader = PackageReader::new(data, Box::new(Panicky), ReadOptions::default());
        assert!(matches!(
            reader.next().await,
            Err(ReadError::FilterPanicked(message)) if message == "filter exploded"
        ));
    }

    #[tokio::test]
    async fn returns_unconsumed_bytes() {
        let data: &[u8] = b"first\r\nleft over";
        let mut reader = lines(data, ReadOptions::default());
        assert_eq!(reader.next().await.unwrap().unwrap().text, "first");
        let (_, rest) = reader.into_parts();
        assert_eq!(&rest[..], b"left over");
    }
}
