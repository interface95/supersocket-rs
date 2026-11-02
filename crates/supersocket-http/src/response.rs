use std::fmt::Write as _;

use bytes::{Bytes, BytesMut};
use supersocket_proto::{PackageEncoder, ProtocolError};

/// An HTTP response: a status, header fields and a body. Send it with
/// [`HttpSessionExt::respond`](crate::HttpSessionExt::respond) or encode it with
/// [`HttpEncoder`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
    keep_alive: bool,
    /// The body goes on until the connection closes: no `Content-Length`, for a stream.
    unbounded: bool,
}

impl HttpResponse {
    /// A response with `status` and no body.
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Bytes::new(),
            keep_alive: true,
            unbounded: false,
        }
    }

    /// A `200 OK` response with no body.
    pub fn ok() -> Self {
        Self::new(200)
    }

    /// The status code.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// Adds a header field.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Sets the `Content-Type` header field.
    pub fn content_type(self, media_type: impl Into<String>) -> Self {
        self.header("Content-Type", media_type)
    }

    /// Sets the body. Set the content type as well, with [`content_type`](Self::content_type).
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// Sets a plain text body, `text/plain; charset=utf-8`.
    pub fn text(self, text: impl Into<String>) -> Self {
        self.content_type("text/plain; charset=utf-8")
            .body(Bytes::from(text.into()))
    }

    /// Sets an HTML body, `text/html; charset=utf-8`.
    pub fn html(self, html: impl Into<String>) -> Self {
        self.content_type("text/html; charset=utf-8")
            .body(Bytes::from(html.into()))
    }

    /// Sets a JSON body, `application/json`, from text that is already JSON.
    pub fn json(self, json: impl Into<String>) -> Self {
        self.content_type("application/json")
            .body(Bytes::from(json.into()))
    }

    /// Whether to keep the connection open for another request after this response; otherwise
    /// the response says `Connection: close` and the session closes once it is sent. Default:
    /// `true`, within what the request allows; see
    /// [`HttpSessionExt::respond`](crate::HttpSessionExt::respond).
    pub fn keep_alive(mut self, keep_alive: bool) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Whether the connection is kept open after this response.
    pub fn is_keep_alive(&self) -> bool {
        self.keep_alive
    }

    /// Makes the body go on until the connection closes, as a stream such as Server-Sent Events
    /// does: no `Content-Length` is sent, and the connection is not kept alive for another
    /// request.
    pub fn streaming(mut self) -> Self {
        self.unbounded = true;
        self.keep_alive = false;
        self
    }

    /// Writes the response in its wire form.
    pub fn write_to(&self, buf: &mut BytesMut) {
        let _ = write!(buf, "HTTP/1.1 {} {}\r\n", self.status, reason(self.status));
        let mut has_length = false;
        for (name, value) in &self.headers {
            has_length |= name.eq_ignore_ascii_case("Content-Length");
            let _ = write!(buf, "{name}: {value}\r\n");
        }
        if !self.unbounded && !has_length {
            let _ = write!(buf, "Content-Length: {}\r\n", self.body.len());
        }
        if !self.keep_alive {
            buf.extend_from_slice(b"Connection: close\r\n");
        }
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(&self.body);
    }
}

/// Encodes [`HttpResponse`]s, for [`Session::send_package`](supersocket_server::Session::send_package).
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpEncoder;

impl PackageEncoder<HttpResponse> for HttpEncoder {
    fn encode(&self, response: &HttpResponse, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        response.write_to(buf);
        Ok(())
    }
}

/// The reason phrase of a status code.
fn reason(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        413 => "Content Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Content",
        426 => "Upgrade Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_the_wire_form() {
        let mut buf = BytesMut::new();
        HttpResponse::ok()
            .header("X-Trace", "1")
            .text("hi")
            .keep_alive(false)
            .write_to(&mut buf);
        assert_eq!(
            &buf[..],
            &b"HTTP/1.1 200 OK\r\nX-Trace: 1\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi"[..]
        );

        let mut buf = BytesMut::new();
        HttpResponse::new(204).write_to(&mut buf);
        assert_eq!(
            &buf[..],
            &b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"[..]
        );

        let mut buf = BytesMut::new();
        HttpResponse::ok()
            .content_type("text/event-stream")
            .streaming()
            .write_to(&mut buf);
        assert_eq!(
            &buf[..],
            &b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"[..]
        );
    }
}
