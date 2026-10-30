use bytes::BytesMut;
use tokio_util::codec::Decoder;

use crate::{PipelineFilter, ProtocolError};

/// Runs a [`tokio_util::codec::Decoder`] as a pipeline filter, so protocol implementations
/// written for `tokio_util::codec`, such as `LengthDelimitedCodec`, work unchanged.
///
/// ```
/// use supersocket_proto::CodecFilter;
/// use tokio_util::codec::LengthDelimitedCodec;
///
/// let filter = CodecFilter::new(LengthDelimitedCodec::new());
/// # let _ = filter;
/// ```
#[derive(Debug, Default)]
pub struct CodecFilter<D>(D);

impl<D> CodecFilter<D> {
    /// Wraps `decoder`.
    pub fn new(decoder: D) -> Self {
        Self(decoder)
    }

    /// The decoder.
    pub fn get_ref(&self) -> &D {
        &self.0
    }
}

impl<D> PipelineFilter for CodecFilter<D>
where
    D: Decoder + Send + 'static,
    D::Item: Send + 'static,
    D::Error: std::error::Error + Send + Sync + 'static,
{
    type Package = D::Item;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<D::Item>, ProtocolError> {
        self.0
            .decode(buf)
            .map_err(|error| ProtocolError::new(error.to_string()).with_source(error))
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::codec::LengthDelimitedCodec;

    use super::*;

    #[test]
    fn decodes_with_a_tokio_util_codec() {
        let mut filter = CodecFilter::new(LengthDelimitedCodec::new());
        let mut buf = BytesMut::from(&[0, 0, 0, 2, b'h', b'i', 0, 0][..]);
        let frame = filter.filter(&mut buf).unwrap().unwrap();
        assert_eq!(&frame[..], b"hi");
        assert!(filter.filter(&mut buf).unwrap().is_none());
    }
}
