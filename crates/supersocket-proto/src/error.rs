use std::sync::Arc;

/// Data that does not follow the protocol, reported by pipeline filters and encoders.
///
/// Two errors are equal when their messages are; the source is not compared.
#[derive(Debug, Clone, thiserror::Error)]
#[error("protocol error: {message}")]
pub struct ProtocolError {
    message: String,
    #[source]
    source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

impl ProtocolError {
    /// An error with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Attaches the error that caused this one, which
    /// [`Error::source`](std::error::Error::source) then returns.
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    /// The message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl PartialEq for ProtocolError {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl Eq for ProtocolError {}

impl From<std::str::Utf8Error> for ProtocolError {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::new("text is not valid UTF-8").with_source(error)
    }
}

impl From<std::string::FromUtf8Error> for ProtocolError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self::new("text is not valid UTF-8").with_source(error)
    }
}

impl From<std::io::Error> for ProtocolError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error.to_string()).with_source(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_its_source() {
        let invalid = String::from_utf8(vec![0xff]).unwrap_err();
        let error = ProtocolError::from(invalid);
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(error, ProtocolError::new("text is not valid UTF-8"));
        assert!(std::error::Error::source(&ProtocolError::new("plain")).is_none());
    }
}
