use bytes::Bytes;

use crate::ProtocolError;

/// A package of UTF-8 text, produced by [`LineFilter`](crate::LineFilter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPackage {
    /// The text, without the terminator.
    pub text: String,
}

impl TextPackage {
    /// Decodes UTF-8 text.
    pub fn decode(buf: Bytes) -> Result<Self, ProtocolError> {
        String::from_utf8(buf.into())
            .map(|text| Self { text })
            .map_err(|_| ProtocolError::new("text is not valid UTF-8"))
    }
}

/// A command line such as `ADD 1 2`, produced by [`CommandLineFilter`](crate::CommandLineFilter).
///
/// The first word is the key that selects a command. The rest of the line is the body, and the
/// body split by whitespace gives the arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandPackage {
    /// The first word of the line.
    pub key: String,
    /// Everything after the key and the space that follows it.
    pub body: String,
}

impl CommandPackage {
    /// Decodes a UTF-8 command line.
    pub fn decode(buf: Bytes) -> Result<Self, ProtocolError> {
        let text = TextPackage::decode(buf)?.text;
        let (key, body) = text.split_once(' ').unwrap_or((text.as_str(), ""));
        Ok(Self {
            key: key.to_owned(),
            body: body.to_owned(),
        })
    }

    /// The arguments: the body split at whitespace.
    pub fn args(&self) -> impl Iterator<Item = &str> {
        self.body.split_whitespace()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_package_splits_key_body_and_args() {
        let package = CommandPackage::decode(Bytes::from_static(b"ADD 1  2")).unwrap();
        assert_eq!(package.key, "ADD");
        assert_eq!(package.body, "1  2");
        assert_eq!(package.args().collect::<Vec<_>>(), ["1", "2"]);

        let package = CommandPackage::decode(Bytes::from_static(b"PING")).unwrap();
        assert_eq!((package.key.as_str(), package.body.as_str()), ("PING", ""));
        assert_eq!(package.args().count(), 0);
    }

    #[test]
    fn invalid_utf8_is_a_protocol_error() {
        assert!(TextPackage::decode(Bytes::from_static(&[0xff, 0xfe])).is_err());
    }
}
