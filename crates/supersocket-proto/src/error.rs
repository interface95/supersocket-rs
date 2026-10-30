/// Data that does not follow the protocol. A connection whose filter returns this error is closed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("protocol error: {message}")]
pub struct ProtocolError {
    message: String,
}

impl ProtocolError {
    /// Creates an error with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}
