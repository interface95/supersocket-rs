use bytes::{Buf, Bytes, BytesMut};

use super::{Decoder, find};
use crate::{PipelineFilter, ProtocolError};

/// Cuts packages wrapped in a begin mark and an end mark. The decoder receives a package without
/// the marks. Data that does not start with the begin mark is a protocol error.
pub struct BeginEndMarkFilter<P> {
    begin: Bytes,
    end: Bytes,
    decoder: Decoder<P>,
}

impl<P> BeginEndMarkFilter<P> {
    /// Creates a filter for packages between `begin` and `end`, decoded with `decoder`.
    ///
    /// # Panics
    ///
    /// Panics if either mark is empty.
    pub fn new(begin: impl Into<Bytes>, end: impl Into<Bytes>, decoder: Decoder<P>) -> Self {
        let (begin, end) = (begin.into(), end.into());
        assert!(
            !begin.is_empty() && !end.is_empty(),
            "marks must not be empty"
        );
        Self {
            begin,
            end,
            decoder,
        }
    }
}

impl<P: Send + 'static> PipelineFilter for BeginEndMarkFilter<P> {
    type Package = P;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<P>, ProtocolError> {
        let checked = buf.len().min(self.begin.len());
        if buf[..checked] != self.begin[..checked] {
            return Err(ProtocolError::new(
                "data does not start with the begin mark",
            ));
        }
        if buf.len() < self.begin.len() {
            return Ok(None);
        }
        let Some(position) = find(&buf[self.begin.len()..], &self.end) else {
            return Ok(None);
        };
        let mut package = buf.split_to(self.begin.len() + position + self.end.len());
        package.advance(self.begin.len());
        package.truncate(position);
        (self.decoder)(package.freeze()).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::test_util::feed;

    fn filter() -> BeginEndMarkFilter<Vec<u8>> {
        BeginEndMarkFilter::new(&b"<<"[..], &b">>"[..], |bytes| Ok(bytes.to_vec()))
    }

    #[test]
    fn decodes_marked_packages() {
        let packages = feed(&mut filter(), &[b"<<ab>><", b"<c", b"d>>"]);
        assert_eq!(packages, [b"ab".to_vec(), b"cd".to_vec()]);
    }

    #[test]
    fn rejects_data_without_begin_mark() {
        assert!(
            filter()
                .filter(&mut BytesMut::from(&b"x<<a>>"[..]))
                .is_err()
        );
        assert!(filter().filter(&mut BytesMut::from(&b"<x"[..])).is_err());
    }
}
