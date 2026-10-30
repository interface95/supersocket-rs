use async_compression::tokio::{bufread::GzipDecoder, write::GzipEncoder};
use tokio::io::BufReader;

use crate::BoxStream;

/// Compresses `stream` with gzip in both directions, like SuperSocket's GZip support: what is
/// read is decompressed and what is written is compressed. Every flush ends a compressed block,
/// so the peer can decompress what was sent so far without waiting for more. Both ends must
/// compress.
///
/// Each direction keeps compression state, a few hundred kilobytes per connection.
pub fn gzip(stream: BoxStream) -> BoxStream {
    let (reader, writer) = tokio::io::split(stream);
    let mut decoder = GzipDecoder::new(BufReader::new(reader));
    decoder.multiple_members(true);
    Box::new(tokio::io::join(decoder, GzipEncoder::new(writer)))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn flushed_data_arrives_before_the_stream_ends() {
        let (left, right) = tokio::io::duplex(64 * 1024);
        let (mut left, mut right) = (gzip(Box::new(left)), gzip(Box::new(right)));

        left.write_all(b"hello").await.unwrap();
        left.flush().await.unwrap();
        let mut received = [0; 5];
        right.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"hello");

        // The other direction, and the end of the stream.
        right.write_all(&[b'x'; 10_000]).await.unwrap();
        right.shutdown().await.unwrap();
        let mut rest = Vec::new();
        left.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, [b'x'; 10_000]);
    }
}
