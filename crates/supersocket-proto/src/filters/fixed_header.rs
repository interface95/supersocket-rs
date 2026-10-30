use bytes::{Bytes, BytesMut};

use crate::{PipelineFilter, ProtocolError};

/// A protocol whose packages start with a fixed-size header that carries the body length.
///
/// Implement this for your protocol and use it through [`FixedHeaderFilter`]:
///
/// ```
/// use supersocket_proto::{FixedHeaderFilter, FixedHeaderProtocol, ProtocolError, bytes::Bytes};
///
/// /// A 2-byte big-endian length followed by the body.
/// #[derive(Default)]
/// struct LengthPrefixed;
///
/// impl FixedHeaderProtocol for LengthPrefixed {
///     type Package = Bytes;
///     const HEADER_SIZE: usize = 2;
///
///     fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
///         Ok(u16::from_be_bytes([header[0], header[1]]) as usize)
///     }
///
///     fn decode(&mut self, package: Bytes) -> Result<Bytes, ProtocolError> {
///         Ok(package.slice(Self::HEADER_SIZE..))
///     }
/// }
///
/// let filter = FixedHeaderFilter::<LengthPrefixed>::default();
/// ```
pub trait FixedHeaderProtocol: Send + 'static {
    /// The package type this protocol produces.
    type Package: Send + 'static;

    /// The size of the header in bytes.
    const HEADER_SIZE: usize;

    /// Reads the body length from a header of exactly [`HEADER_SIZE`](Self::HEADER_SIZE) bytes.
    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError>;

    /// Decodes a complete package: the header followed by the body.
    fn decode(&mut self, package: Bytes) -> Result<Self::Package, ProtocolError>;
}

/// Decodes packages of a [`FixedHeaderProtocol`].
#[derive(Default)]
pub struct FixedHeaderFilter<T> {
    protocol: T,
    total_size: Option<usize>,
}

impl<T> FixedHeaderFilter<T> {
    /// Creates a filter for `protocol`.
    pub fn new(protocol: T) -> Self {
        Self {
            protocol,
            total_size: None,
        }
    }
}

impl<T: FixedHeaderProtocol> PipelineFilter for FixedHeaderFilter<T> {
    type Package = T::Package;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<T::Package>, ProtocolError> {
        let total_size = match self.total_size {
            Some(total_size) => total_size,
            None => {
                if buf.len() < T::HEADER_SIZE {
                    return Ok(None);
                }
                let body_length = self.protocol.body_length(&buf[..T::HEADER_SIZE])?;
                *self.total_size.insert(T::HEADER_SIZE + body_length)
            }
        };
        if buf.len() < total_size {
            return Ok(None);
        }
        self.total_size = None;
        self.protocol
            .decode(buf.split_to(total_size).freeze())
            .map(Some)
    }

    fn reset(&mut self) {
        self.total_size = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::test_util::feed;

    #[derive(Default)]
    struct LengthPrefixed;

    impl FixedHeaderProtocol for LengthPrefixed {
        type Package = Vec<u8>;
        const HEADER_SIZE: usize = 2;

        fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
            Ok(u16::from_be_bytes([header[0], header[1]]) as usize)
        }

        fn decode(&mut self, package: Bytes) -> Result<Vec<u8>, ProtocolError> {
            Ok(package[Self::HEADER_SIZE..].to_vec())
        }
    }

    #[test]
    fn decodes_packages_fed_byte_by_byte() {
        let wire = [0, 3, b'a', b'b', b'c', 0, 0, 0, 1, b'z'];
        let chunks: Vec<&[u8]> = wire.chunks(1).collect();
        let packages = feed(&mut FixedHeaderFilter::<LengthPrefixed>::default(), &chunks);
        assert_eq!(packages, [b"abc".to_vec(), Vec::new(), b"z".to_vec()]);
    }
}
