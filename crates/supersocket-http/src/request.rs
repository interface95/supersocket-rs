use std::fmt;

use bytes::Bytes;

/// The HTTP version of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpVersion {
    /// HTTP/1.0: the connection closes after the response unless the client asked to keep it.
    Http10,
    /// HTTP/1.1: the connection is kept alive unless either side asks to close it.
    Http11,
}

impl fmt::Display for HttpVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Http10 => "HTTP/1.0",
            Self::Http11 => "HTTP/1.1",
        })
    }
}

/// An HTTP request, as [`HttpFilter`](crate::HttpFilter) produces it: the request line, the
/// header fields and the whole body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) version: HttpVersion,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Bytes,
}

impl HttpRequest {
    /// The method, such as `GET` or `POST`, as the client wrote it.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The request target as the client wrote it, such as `/items?page=2`.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The path of the target: what comes before the query, such as `/items`.
    pub fn path(&self) -> &str {
        self.target
            .split_once('?')
            .map_or(self.target.as_str(), |(path, _)| path)
    }

    /// The query of the target, without the `?`, such as `page=2`.
    pub fn query(&self) -> Option<&str> {
        self.target.split_once('?').map(|(_, query)| query)
    }

    /// The HTTP version.
    pub fn version(&self) -> HttpVersion {
        self.version
    }

    /// The value of the first header field called `name`, compared ignoring case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Every header field, in the order received.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Whether the comma-separated values of the header fields called `name` include `token`,
    /// ignoring case.
    pub fn has_token(&self, name: &str, token: &str) -> bool {
        self.headers
            .iter()
            .filter(|(field, _)| field.eq_ignore_ascii_case(name))
            .flat_map(|(_, value)| value.split(','))
            .any(|value| value.trim().eq_ignore_ascii_case(token))
    }

    /// The body; empty when there is none.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// The body as text, when it is UTF-8.
    pub fn body_text(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }

    /// The `Content-Type` header field.
    pub fn content_type(&self) -> Option<&str> {
        self.header("Content-Type")
    }

    /// Whether the client wants the connection kept open for another request: on HTTP/1.1
    /// unless it says `Connection: close`, on HTTP/1.0 only if it says `Connection: keep-alive`.
    pub fn keep_alive(&self) -> bool {
        match self.version {
            HttpVersion::Http11 => !self.has_token("Connection", "close"),
            HttpVersion::Http10 => self.has_token("Connection", "keep-alive"),
        }
    }

    /// Whether the client accepts a Server-Sent Events stream, `text/event-stream`.
    pub fn accepts_event_stream(&self) -> bool {
        self.header("Accept").is_some_and(|accept| {
            accept.split(',').any(|media| {
                let media = media.split(';').next().unwrap_or("").trim();
                media.eq_ignore_ascii_case("text/event-stream") || media == "*/*"
            })
        })
    }

    /// The `Last-Event-ID` header field, with which a client resumes a Server-Sent Events
    /// stream.
    pub fn last_event_id(&self) -> Option<&str> {
        self.header("Last-Event-ID")
    }
}
