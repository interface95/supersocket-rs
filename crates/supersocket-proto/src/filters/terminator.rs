use bytes::{Buf, Bytes, BytesMut};

use super::{Decoder, find};
use crate::{CommandPackage, PipelineFilter, ProtocolError, TextPackage};

/// Cuts packages at a terminator. The decoder receives a package without the terminator.
pub struct TerminatorFilter<P> {
    terminator: Bytes,
    decoder: Decoder<P>,
    searched: usize,
}

impl<P> TerminatorFilter<P> {
    /// Creates a filter that splits at `terminator` and decodes each package with `decoder`.
    ///
    /// # Panics
    ///
    /// Panics if `terminator` is empty.
    pub fn new(terminator: impl Into<Bytes>, decoder: Decoder<P>) -> Self {
        let terminator = terminator.into();
        assert!(!terminator.is_empty(), "terminator must not be empty");
        Self {
            terminator,
            decoder,
            searched: 0,
        }
    }
}

impl<P: Send + 'static> PipelineFilter for TerminatorFilter<P> {
    type Package = P;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<P>, ProtocolError> {
        // Skip what earlier calls already searched, minus enough bytes to catch a terminator that
        // was split across two reads.
        let start = self
            .searched
            .saturating_sub(self.terminator.len() - 1)
            .min(buf.len());
        let Some(position) = find(&buf[start..], &self.terminator) else {
            self.searched = buf.len();
            return Ok(None);
        };
        let package = buf.split_to(start + position).freeze();
        buf.advance(self.terminator.len());
        self.searched = 0;
        (self.decoder)(package).map(Some)
    }

    fn reset(&mut self) {
        self.searched = 0;
    }
}

/// Text lines ending with `\r\n`, decoded as [`TextPackage`].
pub struct LineFilter(TerminatorFilter<TextPackage>);

impl Default for LineFilter {
    fn default() -> Self {
        Self(TerminatorFilter::new(&b"\r\n"[..], TextPackage::decode))
    }
}

impl PipelineFilter for LineFilter {
    type Package = TextPackage;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<TextPackage>, ProtocolError> {
        self.0.filter(buf)
    }

    fn reset(&mut self) {
        self.0.reset();
    }
}

/// Command lines ending with `\r\n`, decoded as [`CommandPackage`].
pub struct CommandLineFilter(TerminatorFilter<CommandPackage>);

impl Default for CommandLineFilter {
    fn default() -> Self {
        Self(TerminatorFilter::new(&b"\r\n"[..], CommandPackage::decode))
    }
}

impl PipelineFilter for CommandLineFilter {
    type Package = CommandPackage;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<CommandPackage>, ProtocolError> {
        self.0.filter(buf)
    }

    fn reset(&mut self) {
        self.0.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::test_util::feed;

    fn texts(packages: Vec<TextPackage>) -> Vec<String> {
        packages.into_iter().map(|package| package.text).collect()
    }

    #[test]
    fn splits_merged_lines() {
        let packages = feed(&mut LineFilter::default(), &[b"a\r\nbb\r\n\r\nccc\r\n"]);
        assert_eq!(texts(packages), ["a", "bb", "", "ccc"]);
    }

    #[test]
    fn handles_terminator_split_across_reads() {
        let packages = feed(
            &mut LineFilter::default(),
            &[b"hel", b"lo\r", b"\nwor", b"ld\r", b"\n"],
        );
        assert_eq!(texts(packages), ["hello", "world"]);
    }

    #[test]
    fn supports_multi_byte_terminators() {
        let mut filter = TerminatorFilter::new(&b"##END##"[..], TextPackage::decode);
        let packages = feed(&mut filter, &[b"one##EN", b"D##two##E", b"ND##"]);
        assert_eq!(texts(packages), ["one", "two"]);
    }

    #[test]
    fn decodes_command_lines() {
        let packages = feed(&mut CommandLineFilter::default(), &[b"ADD 1 2\r\nPING\r\n"]);
        assert_eq!(packages[0].args, ["1", "2"]);
        assert_eq!(packages[1].key, "PING");
    }
}
