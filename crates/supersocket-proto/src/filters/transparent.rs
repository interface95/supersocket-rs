use bytes::{Bytes, BytesMut};

use crate::{PipelineFilter, ProtocolError};

/// No framing: whatever has been received becomes one package.
#[derive(Debug, Default)]
pub struct TransparentFilter;

impl PipelineFilter for TransparentFilter {
    type Package = Bytes;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, ProtocolError> {
        Ok((!buf.is_empty()).then(|| buf.split().freeze()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::test_util::feed;

    #[test]
    fn every_read_is_a_package() {
        let packages = feed(&mut TransparentFilter, &[b"ab", b"c"]);
        assert_eq!(
            packages,
            [Bytes::from_static(b"ab"), Bytes::from_static(b"c")]
        );
    }
}
