//! Entry points for the fuzz targets in the repository's `fuzz/` directory. Compiled only with
//! `--cfg fuzzing`, which `cargo fuzz` sets.
#![allow(missing_docs)]

use crate::proxy_protocol;

/// Reads `data` as the start of a connection with a PROXY protocol header.
pub fn proxy_protocol_header(data: &[u8]) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let mut data = data;
        let proxy = "127.0.0.1:1".parse().expect("an address");
        let _ = proxy_protocol::read_header(&mut data, proxy).await;
    });
}
