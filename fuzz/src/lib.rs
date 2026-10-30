//! Helpers for the fuzz targets: a reader that delivers data in pieces, and a way to run any
//! pipeline filter over it exactly as a connection does.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use supersocket_connection::{PackageReader, ReadOptions};
use supersocket_proto::{BoxFilter, FixedHeaderProtocol, ProtocolError};
use tokio::io::{AsyncRead, ReadBuf};

/// Delivers `data` in pieces of `chunk` bytes, as a connection would.
pub struct Chunked<'a> {
    data: &'a [u8],
    chunk: usize,
}

impl<'a> Chunked<'a> {
    pub fn new(data: &'a [u8], chunk: usize) -> Self {
        Self {
            data,
            chunk: chunk.max(1),
        }
    }
}

impl AsyncRead for Chunked<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let length = self.chunk.min(self.data.len()).min(buf.remaining());
        buf.put_slice(&self.data[..length]);
        self.data = &self.data[length..];
        Poll::Ready(Ok(()))
    }
}

/// Reads every package `filter` cuts from `data`, delivered in pieces of `chunk` bytes, with a
/// maximum package length of `max_size`.
pub fn read_all<P: Send + 'static>(
    filter: BoxFilter<P>,
    max_size: usize,
    chunk: usize,
    data: &[u8],
) {
    let options = ReadOptions {
        max_package_length: max_size.max(16),
        receive_buffer_size: 64,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let mut packages = PackageReader::new(Chunked::new(data, chunk), filter, options);
        while let Ok(Some(_)) = packages.next().await {}
    });
}

/// A two-byte big-endian length followed by the body.
#[derive(Default)]
pub struct LengthPrefixed;

impl FixedHeaderProtocol for LengthPrefixed {
    type Package = Bytes;
    const HEADER_SIZE: usize = 2;

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        Ok(usize::from(u16::from_be_bytes([header[0], header[1]])))
    }

    fn decode(&mut self, package: Bytes) -> Result<Bytes, ProtocolError> {
        Ok(package.slice(Self::HEADER_SIZE..))
    }
}
