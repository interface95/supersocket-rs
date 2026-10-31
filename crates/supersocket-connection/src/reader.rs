use std::{
    fmt,
    future::poll_fn,
    io,
    mem::MaybeUninit,
    pin::Pin,
    task::{Poll, ready},
};

use bytes::{BufMut, BytesMut};
use supersocket_proto::{BoxFilter, ProtocolError};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use crate::catch_panic_sync;

/// Bytes read per poll while nothing is buffered. They are read into the stack and only what
/// arrived is copied to the heap, so a connection waiting for data holds no buffer.
const STACK_READ_SIZE: usize = 8 * 1024;

/// Limits of a [`PackageReader`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions {
    /// Largest package, framing included. A filter sees at most this many buffered bytes, so a
    /// longer package is an error once this much arrives without completing it. Default: 1 MiB.
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
    /// The maximum package length was buffered without completing a package.
    #[error("package exceeds the maximum length of {limit} bytes")]
    TooLarge {
        /// The maximum package length.
        limit: usize,
    },
    /// The pipeline filter panicked; the value is the panic message.
    #[error("the pipeline filter panicked: {0}")]
    FilterPanicked(String),
}

impl From<ReadError> for io::Error {
    /// I/O errors as they were; any other error as [`ErrorKind::InvalidData`](io::ErrorKind).
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Io(error) => error,
            other => io::Error::new(io::ErrorKind::InvalidData, other),
        }
    }
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
        Self::with_buffered(reader, filter, options, BytesMut::new())
    }

    /// Like [`new`](Self::new), for a reader whose first bytes were already read, such as data
    /// that arrived together with a PROXY protocol header. They are cut into packages first.
    pub fn with_buffered(
        reader: R,
        filter: BoxFilter<P>,
        options: ReadOptions,
        buffered: BytesMut,
    ) -> Self {
        Self {
            reader,
            filter,
            buf: buffered,
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
        let limit = self.options.max_package_length;
        if self.buf.len() >= limit {
            return Err(ReadError::TooLarge { limit });
        }
        // Everything received has been consumed: drop the buffer so an idle connection holds none.
        if self.buf.is_empty() {
            self.buf = BytesMut::new();
        }
        // Never buffer more than the limit, so that no package can exceed it.
        let room = limit - self.buf.len();
        Ok(read_some(
            &mut self.reader,
            &mut self.buf,
            self.options.receive_buffer_size,
            room,
        )
        .await?)
    }

    /// Returns the next package if it has already been received, without reading. `Ok(None)`
    /// means more data is needed.
    pub fn try_next(&mut self) -> Result<Option<P>, ReadError> {
        let limit = self.options.max_package_length;
        while !self.buf.is_empty() {
            if limit == 0 {
                return Err(ReadError::TooLarge { limit });
            }
            // Pre-read data may contain many packages. Give the filter the same bounded view
            // as fill(), keeping the rest out of reach until earlier bytes are consumed.
            let tail = (self.buf.len() > limit).then(|| self.buf.split_off(limit));
            let step = catch_panic_sync(|| decode_step(&mut self.filter, &mut self.buf));
            let remaining = self.buf.len();
            let more_buffered = tail.is_some();
            if let Some(tail) = tail {
                // Restore unread bytes even if the filter failed or panicked.
                self.buf.unsplit(tail);
            }
            let step = step.map_err(ReadError::FilterPanicked)??;
            match step {
                Step::Package(package) => return Ok(Some(package)),
                Step::Switched => {}
                Step::NeedMore => {
                    if remaining >= limit {
                        return Err(ReadError::TooLarge { limit });
                    }
                    if !more_buffered {
                        break;
                    }
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

/// Reads up to `room` more bytes into `buf`. While `buf` is empty, nothing is allocated until data
/// arrives.
async fn read_some<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
    chunk_size: usize,
    room: usize,
) -> io::Result<usize> {
    if !buf.is_empty() {
        let chunk = chunk_size.min(room);
        buf.reserve(chunk);
        return reader.read_buf(&mut (&mut *buf).limit(chunk)).await;
    }
    let size = STACK_READ_SIZE.min(room);
    poll_fn(|cx| {
        let mut stack = [MaybeUninit::<u8>::uninit(); STACK_READ_SIZE];
        let mut read_buf = ReadBuf::uninit(&mut stack[..size]);
        ready!(Pin::new(&mut *reader).poll_read(cx, &mut read_buf))?;
        buf.extend_from_slice(read_buf.filled());
        Poll::Ready(Ok(read_buf.filled().len()))
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use supersocket_proto::{
        FixedSizeFilter, LineFilter, PipelineFilter, PipelineFilterExt, TerminatorFilter,
        TextPackage,
    };
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
    async fn packages_may_not_exceed_the_maximum_length() {
        let options = ReadOptions {
            max_package_length: 16,
            ..ReadOptions::default()
        };
        // Exactly the limit, terminator included.
        let data: &[u8] = b"xxxxxxxxxxxxxx\r\n";
        let package = lines(data, options).next().await.unwrap().unwrap();
        assert_eq!(package.text, "xxxxxxxxxxxxxx");
        // One byte more, although it would arrive in one read.
        let data: &[u8] = b"xxxxxxxxxxxxxxx\r\n";
        assert!(matches!(
            lines(data, options).next().await,
            Err(ReadError::TooLarge { limit: 16 })
        ));
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
    async fn bytes_read_earlier_come_first() {
        let data: &[u8] = b"lo\r\n";
        let mut reader = PackageReader::with_buffered(
            data,
            Box::new(LineFilter::default()),
            ReadOptions::default(),
            BytesMut::from(&b"hel"[..]),
        );
        assert_eq!(reader.next().await.unwrap().unwrap().text, "hello");
    }

    #[test]
    fn buffered_packages_respect_the_limit_before_decoding() {
        let decoded = Arc::new(AtomicUsize::new(0));
        let filter = TerminatorFilter::new(&b"\r\n"[..], {
            let decoded = decoded.clone();
            move |bytes| {
                decoded.fetch_add(1, Ordering::SeqCst);
                TextPackage::decode(bytes)
            }
        });
        let mut reader = PackageReader::with_buffered(
            &b""[..],
            Box::new(filter),
            ReadOptions {
                max_package_length: 8,
                ..ReadOptions::default()
            },
            BytesMut::from(&b"a\r\n0123456789ABCDEF\r\n"[..]),
        );
        assert_eq!(reader.try_next().unwrap().unwrap().text, "a");
        assert!(matches!(
            reader.try_next(),
            Err(ReadError::TooLarge { limit: 8 })
        ));
        assert_eq!(decoded.load(Ordering::SeqCst), 1);
        let (_, rest) = reader.into_parts();
        assert_eq!(&rest[..], b"0123456789ABCDEF\r\n");
    }

    #[tokio::test]
    async fn buffered_small_packages_and_a_partial_tail_stay_in_order() {
        let mut reader = PackageReader::with_buffered(
            &b"ree\r\n"[..],
            Box::new(LineFilter::default()),
            ReadOptions {
                max_package_length: 8,
                ..ReadOptions::default()
            },
            BytesMut::from(&b"one\r\n123456\r\nth"[..]),
        );
        assert_eq!(reader.try_next().unwrap().unwrap().text, "one");
        assert_eq!(reader.try_next().unwrap().unwrap().text, "123456");
        assert!(reader.try_next().unwrap().is_none());
        assert_eq!(reader.next().await.unwrap().unwrap().text, "three");
        assert!(reader.next().await.unwrap().is_none());
    }

    #[test]
    fn buffered_tail_survives_filter_switches_and_into_parts() {
        let filter = LineFilter::default()
            .then_switch_to(|| Box::new(FixedSizeFilter::new(4, TextPackage::decode)));
        let mut reader = PackageReader::with_buffered(
            &b""[..],
            Box::new(filter),
            ReadOptions {
                max_package_length: 8,
                ..ReadOptions::default()
            },
            BytesMut::from(&b"go\r\n12345678tail"[..]),
        );
        assert_eq!(reader.try_next().unwrap().unwrap().text, "go");
        assert_eq!(reader.try_next().unwrap().unwrap().text, "1234");
        assert_eq!(reader.try_next().unwrap().unwrap().text, "5678");
        let (_, rest) = reader.into_parts();
        assert_eq!(&rest[..], b"tail");
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
