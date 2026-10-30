//! Entry points for the fuzz targets in the repository's `fuzz/` directory. Compiled only with
//! `--cfg fuzzing`, which `cargo fuzz` sets.
#![allow(missing_docs)]

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use supersocket_connection::{PackageReader, ReadOptions};
use tokio::io::{AsyncRead, ReadBuf};

use crate::{
    filter::{HeadFilter, Role},
    handshake::{parse_head, read_request},
};

/// Delivers `data` in pieces of `chunk` bytes, as a connection would.
struct Chunked<'a> {
    data: &'a [u8],
    chunk: usize,
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

/// Runs `data` through the handshake and frame filters of a server (or a client), as they would
/// see it arriving in pieces of `chunk` bytes, with `max_size` as the maximum package length.
pub fn pipeline(server: bool, max_size: usize, chunk: usize, data: &[u8]) {
    let role = if server { Role::Server } else { Role::Client };
    let options = ReadOptions {
        max_package_length: max_size.max(16),
        receive_buffer_size: 64,
    };
    let reader = Chunked {
        data,
        chunk: chunk.max(1),
    };
    let filter = Box::new(HeadFilter::new(role, options.max_package_length));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let mut packages = PackageReader::new(reader, filter, options);
        while let Ok(Some(_)) = packages.next().await {}
    });
}

/// Parses `data` as a client's opening handshake.
pub fn handshake_request(data: &[u8]) {
    if let Ok(head) = parse_head(data) {
        let _ = read_request(head);
    }
}
