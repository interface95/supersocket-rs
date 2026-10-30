use bytes::{Bytes, BytesMut};

use super::Decoder;
use crate::{PipelineFilter, ProtocolError};

/// Cuts packages of a fixed size.
pub struct FixedSizeFilter<P> {
    size: usize,
    decoder: Decoder<P>,
}

impl<P> FixedSizeFilter<P> {
    /// Creates a filter that cuts packages of `size` bytes and decodes them with `decoder`.
    ///
    /// # Panics
    ///
    /// Panics if `size` is zero.
    pub fn new(
        size: usize,
        decoder: impl FnMut(Bytes) -> Result<P, ProtocolError> + Send + 'static,
    ) -> Self {
        assert!(size > 0, "package size must not be zero");
        Self {
            size,
            decoder: Box::new(decoder),
        }
    }
}

impl<P: Send + 'static> PipelineFilter for FixedSizeFilter<P> {
    type Package = P;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<P>, ProtocolError> {
        if buf.len() < self.size {
            return Ok(None);
        }
        (self.decoder)(buf.split_to(self.size).freeze()).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::test_util::feed;

    #[test]
    fn cuts_fixed_size_packages() {
        let mut filter = FixedSizeFilter::new(3, |bytes| Ok(bytes.to_vec()));
        let packages = feed(&mut filter, &[b"ab", b"cdef", b"gh"]);
        assert_eq!(packages, [b"abc".to_vec(), b"def".to_vec()]);
    }
}
