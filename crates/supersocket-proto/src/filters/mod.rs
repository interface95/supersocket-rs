mod begin_end_mark;
mod fixed_header;
mod fixed_size;
mod terminator;
mod transparent;

pub use begin_end_mark::BeginEndMarkFilter;
pub use fixed_header::{FixedHeaderFilter, FixedHeaderProtocol};
pub use fixed_size::FixedSizeFilter;
pub use terminator::{CommandLineFilter, LineFilter, TerminatorFilter};
pub use transparent::TransparentFilter;

/// A function that decodes the bytes of one package, used by the built-in filters.
pub type Decoder<P> = fn(bytes::Bytes) -> Result<P, crate::ProtocolError>;

pub(crate) fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
pub(crate) mod test_util {
    use bytes::BytesMut;

    use crate::PipelineFilter;

    /// Feeds `chunks` to `filter` one by one, collecting every package, like a connection would.
    pub(crate) fn feed<F: PipelineFilter>(filter: &mut F, chunks: &[&[u8]]) -> Vec<F::Package> {
        let mut buf = BytesMut::new();
        let mut packages = Vec::new();
        for chunk in chunks {
            buf.extend_from_slice(chunk);
            while !buf.is_empty() {
                match filter.filter(&mut buf).unwrap() {
                    Some(package) => {
                        filter.reset();
                        packages.push(package);
                    }
                    None => break,
                }
            }
        }
        packages
    }
}
