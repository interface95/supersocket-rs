use std::fmt;

use bytes::Bytes;

/// A WebSocket message. Control frames (ping, pong and close) are handled by the server and the
/// client themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
    /// A text message.
    Text(String),
    /// A binary message.
    Binary(Bytes),
}

impl Message {
    /// The text of a text message.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Binary(_) => None,
        }
    }

    /// The payload: the UTF-8 bytes of a text message or the data of a binary one.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Text(text) => text.as_bytes(),
            Self::Binary(data) => data,
        }
    }

    /// Length of the payload in bytes.
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<String> for Message {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for Message {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

impl From<Bytes> for Message {
    fn from(data: Bytes) -> Self {
        Self::Binary(data)
    }
}

impl From<Vec<u8>> for Message {
    fn from(data: Vec<u8>) -> Self {
        Self::Binary(data.into())
    }
}

impl From<&[u8]> for Message {
    fn from(data: &[u8]) -> Self {
        Self::Binary(Bytes::copy_from_slice(data))
    }
}

/// The status code of a close frame, from RFC 6455 section 7.4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CloseCode(u16);

impl CloseCode {
    /// 1000: the purpose of the connection has been fulfilled.
    pub const NORMAL: Self = Self(1000);
    /// 1001: the endpoint is going away, such as a server shutting down.
    pub const GOING_AWAY: Self = Self(1001);
    /// 1002: the peer broke the protocol.
    pub const PROTOCOL_ERROR: Self = Self(1002);
    /// 1003: the endpoint cannot accept this type of data.
    pub const UNSUPPORTED_DATA: Self = Self(1003);
    /// 1007: a message's data does not match its type, such as text that is not UTF-8.
    pub const INVALID_PAYLOAD: Self = Self(1007);
    /// 1008: a message broke the endpoint's policy.
    pub const POLICY_VIOLATION: Self = Self(1008);
    /// 1009: a message is too big to process.
    pub const MESSAGE_TOO_BIG: Self = Self(1009);
    /// 1010: the client needs an extension the server did not agree to.
    pub const MANDATORY_EXTENSION: Self = Self(1010);
    /// 1011: the server hit an unexpected condition.
    pub const INTERNAL_ERROR: Self = Self(1011);

    /// The close code with value `code`.
    pub const fn new(code: u16) -> Self {
        Self(code)
    }

    /// The numeric value.
    pub const fn code(self) -> u16 {
        self.0
    }

    /// Whether the code may be sent in a close frame: the codes defined for WebSocket, those
    /// registered for libraries and frameworks (3000-3999) and private ones (4000-4999). Codes
    /// such as 1005 and 1006 only describe a closed connection and are never sent.
    pub const fn is_allowed(self) -> bool {
        matches!(self.0, 1000..=1003 | 1007..=1014 | 3000..=4999)
    }
}

impl From<u16> for CloseCode {
    fn from(code: u16) -> Self {
        Self(code)
    }
}

impl From<CloseCode> for u16 {
    fn from(code: CloseCode) -> Self {
        code.0
    }
}

impl fmt::Display for CloseCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The code and reason a peer gave when it closed the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseFrame {
    /// The status code.
    pub code: CloseCode,
    /// Why the connection was closed; may be empty.
    pub reason: String,
}
