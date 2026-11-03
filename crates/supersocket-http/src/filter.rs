use bytes::{Buf, BytesMut};
use supersocket_proto::{PipelineFilter, ProtocolError, head_length};

use crate::{HttpRequest, HttpVersion};

/// Limits of an [`HttpFilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpOptions {
    /// The largest request head (the request line and the header fields) accepted. Default:
    /// 16 KiB.
    pub max_head_size: usize,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            max_head_size: 16 * 1024,
        }
    }
}

/// Cuts a connection into [`HttpRequest`]s: the head, then a body of `Content-Length` bytes or
/// in chunks (`Transfer-Encoding: chunked`). A request without either has no body. Bodies count
/// against the server's maximum package length. A malformed request is a protocol error, which
/// closes the connection, and so is a request whose body framing is ambiguous, as RFC 9112
/// requires: `Transfer-Encoding` together with `Content-Length`, a transfer coding that does not
/// end with `chunked`, or `Content-Length` values that disagree.
///
/// `Expect: 100-continue` is not answered; clients that use it send the body after a short wait.
#[derive(Debug, Default)]
pub struct HttpFilter {
    options: HttpOptions,
    state: State,
    /// How much of the buffer was searched for the end of the head.
    searched: usize,
}

#[derive(Debug, Default)]
enum State {
    #[default]
    Head,
    /// Waiting for a body of a known length.
    Body { request: HttpRequest, length: usize },
    /// Reading a chunked body.
    Chunked {
        request: HttpRequest,
        body: BytesMut,
        phase: Chunk,
    },
}

#[derive(Debug)]
enum Chunk {
    /// Expecting a chunk size line.
    Size,
    /// Expecting this many bytes of data and the line break after them.
    Data(usize),
    /// Expecting the trailer and the empty line that ends the body.
    Trailer,
}

impl HttpFilter {
    /// A filter with `options`.
    pub fn new(options: HttpOptions) -> Self {
        Self {
            options,
            state: State::Head,
            searched: 0,
        }
    }

    fn head(&mut self, buf: &mut BytesMut) -> Result<Option<HttpRequest>, ProtocolError> {
        let Some(length) = head_length(buf, self.searched) else {
            if buf.len() >= self.options.max_head_size {
                return Err(ProtocolError::new("the request head is too large"));
            }
            self.searched = buf.len();
            return Ok(None);
        };
        self.searched = 0;
        if length > self.options.max_head_size {
            return Err(ProtocolError::new("the request head is too large"));
        }
        let head = buf.split_to(length);
        let request = parse_head(&head)?;
        match body_framing(&request)? {
            Framing::Chunked => {
                self.state = State::Chunked {
                    request,
                    body: BytesMut::new(),
                    phase: Chunk::Size,
                };
                Ok(None)
            }
            Framing::Length(0) => Ok(Some(request)),
            Framing::Length(length) => {
                self.state = State::Body { request, length };
                Ok(None)
            }
        }
    }

    fn body(
        request: HttpRequest,
        length: usize,
        buf: &mut BytesMut,
    ) -> Result<Option<HttpRequest>, HttpRequest> {
        if buf.len() < length {
            return Err(request);
        }
        let mut request = request;
        request.body = buf.split_to(length).freeze();
        Ok(Some(request))
    }

    /// Reads as much of a chunked body as `buf` holds; `Ok(true)` once it is complete.
    fn chunked(
        body: &mut BytesMut,
        phase: &mut Chunk,
        buf: &mut BytesMut,
    ) -> Result<bool, ProtocolError> {
        loop {
            match phase {
                Chunk::Size => {
                    let Some(line) = take_line(buf)? else {
                        return Ok(false);
                    };
                    let size = line.split([';', ' ']).next().unwrap_or("");
                    let size = usize::from_str_radix(size, 16)
                        .map_err(|_| ProtocolError::new("invalid chunk size"))?;
                    *phase = if size == 0 {
                        Chunk::Trailer
                    } else {
                        Chunk::Data(size)
                    };
                }
                Chunk::Data(size) => {
                    // The data and the line break that ends it. The size comes from the peer, so
                    // it must not be trusted to fit.
                    let needed = size
                        .checked_add(2)
                        .ok_or_else(|| ProtocolError::new("chunk size overflows"))?;
                    if buf.len() < needed {
                        return Ok(false);
                    }
                    body.extend_from_slice(&buf[..*size]);
                    buf.advance(*size);
                    if !buf.starts_with(b"\r\n") {
                        return Err(ProtocolError::new("a chunk does not end with CRLF"));
                    }
                    buf.advance(2);
                    *phase = Chunk::Size;
                }
                Chunk::Trailer => match take_line(buf)? {
                    None => return Ok(false),
                    // Trailer fields are ignored; the empty line ends the body.
                    Some(line) if line.is_empty() => return Ok(true),
                    Some(_) => {}
                },
            }
        }
    }
}

impl PipelineFilter for HttpFilter {
    type Package = HttpRequest;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<HttpRequest>, ProtocolError> {
        loop {
            match std::mem::take(&mut self.state) {
                State::Head => {
                    let request = self.head(buf)?;
                    if request.is_some() || matches!(self.state, State::Head) {
                        return Ok(request);
                    }
                    // The head set up a body: read what is there already.
                }
                State::Body { request, length } => {
                    return match Self::body(request, length, buf) {
                        Ok(request) => Ok(request),
                        Err(request) => {
                            self.state = State::Body { request, length };
                            Ok(None)
                        }
                    };
                }
                State::Chunked {
                    mut request,
                    mut body,
                    mut phase,
                } => {
                    if Self::chunked(&mut body, &mut phase, buf)? {
                        request.body = body.freeze();
                        return Ok(Some(request));
                    }
                    self.state = State::Chunked {
                        request,
                        body,
                        phase,
                    };
                    return Ok(None);
                }
            }
        }
    }

    fn reset(&mut self) {
        self.state = State::Head;
        self.searched = 0;
    }
}

/// How the body of a request is delimited.
enum Framing {
    Chunked,
    Length(usize),
}

/// Decides how the body is framed, as RFC 9112 section 6 says: `Transfer-Encoding` must end
/// with `chunked` and must not come with `Content-Length`, since a disagreement between the two
/// is what request smuggling exploits, and every `Content-Length` value must be the same.
fn body_framing(request: &HttpRequest) -> Result<Framing, ProtocolError> {
    let mut encodings = header_values(request, "Transfer-Encoding").peekable();
    if encodings.peek().is_some() {
        if request.header("Content-Length").is_some() {
            return Err(ProtocolError::new(
                "a request may not have both Transfer-Encoding and Content-Length",
            ));
        }
        if !encodings
            .last()
            .is_some_and(|encoding| encoding.eq_ignore_ascii_case("chunked"))
        {
            return Err(ProtocolError::new(
                "unsupported Transfer-Encoding: only chunked is understood",
            ));
        }
        return Ok(Framing::Chunked);
    }
    let mut length = None;
    for value in header_values(request, "Content-Length") {
        let parsed = value
            .parse::<usize>()
            .map_err(|_| ProtocolError::new("invalid Content-Length"))?;
        if length.is_some_and(|length| length != parsed) {
            return Err(ProtocolError::new("conflicting Content-Length values"));
        }
        length = Some(parsed);
    }
    Ok(Framing::Length(length.unwrap_or(0)))
}

/// The comma-separated values of every header field called `name`.
fn header_values<'a>(request: &'a HttpRequest, name: &'a str) -> impl Iterator<Item = &'a str> {
    request
        .headers()
        .filter(move |(field, _)| field.eq_ignore_ascii_case(name))
        .flat_map(|(_, value)| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Takes one line off the front of `buf`, without its line break; `None` when it is incomplete.
fn take_line(buf: &mut BytesMut) -> Result<Option<String>, ProtocolError> {
    let Some(end) = memchr::memchr(b'\n', buf) else {
        return Ok(None);
    };
    let line = buf.split_to(end + 1);
    let line = &line[..end];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    std::str::from_utf8(line)
        .map(|line| Some(line.to_owned()))
        .map_err(|_| ProtocolError::new("a chunk line is not UTF-8"))
}

/// Parses a request head found by [`head_length`].
fn parse_head(head: &[u8]) -> Result<HttpRequest, ProtocolError> {
    let bad = |why: &str| ProtocolError::new(format!("malformed request: {why}"));
    let head = std::str::from_utf8(head).map_err(|_| bad("the head is not UTF-8"))?;
    let mut lines = head
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        // An empty line before the request line is tolerated, as RFC 9112 suggests.
        .skip_while(|line| line.is_empty());
    let request_line = lines
        .next()
        .filter(|line| !line.is_empty())
        .ok_or_else(|| bad("missing request line"))?;
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("the request line is not `method target version`"));
    };
    if method.is_empty() || target.is_empty() {
        return Err(bad("empty method or target"));
    }
    let version = match version {
        "HTTP/1.1" => HttpVersion::Http11,
        "HTTP/1.0" => HttpVersion::Http10,
        _ => return Err(bad("unsupported HTTP version")),
    };
    let mut headers = Vec::new();
    for line in lines.take_while(|line| !line.is_empty()) {
        if line.starts_with([' ', '\t']) {
            return Err(bad("folded header line"));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| bad("header line without a colon"))?;
        if name.is_empty() || name.contains([' ', '\t']) {
            return Err(bad("invalid header name"));
        }
        headers.push((name.to_owned(), value.trim_matches([' ', '\t']).to_owned()));
    }
    Ok(HttpRequest {
        method: method.to_owned(),
        target: target.to_owned(),
        version,
        headers,
        body: bytes::Bytes::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feeds `chunks` to a filter one by one, collecting every request, like a connection would.
    fn feed(chunks: &[&[u8]]) -> Vec<HttpRequest> {
        let mut filter = HttpFilter::default();
        let mut buf = BytesMut::new();
        let mut requests = Vec::new();
        for chunk in chunks {
            buf.extend_from_slice(chunk);
            while !buf.is_empty() {
                match filter.filter(&mut buf).unwrap() {
                    Some(request) => {
                        filter.reset();
                        requests.push(request);
                    }
                    None => break,
                }
            }
        }
        requests
    }

    #[test]
    fn reads_requests_without_a_body() {
        let requests = feed(&[b"GET /items?page=2 HTTP/1.1\r\nHost: example.com\r\n\r\n"]);
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.method(), "GET");
        assert_eq!(request.path(), "/items");
        assert_eq!(request.query(), Some("page=2"));
        assert_eq!(request.header("host"), Some("example.com"));
        assert!(request.body().is_empty());
        assert!(request.keep_alive());
    }

    #[test]
    fn reads_bodies_of_known_length_arriving_in_pieces() {
        let requests = feed(&[
            b"POST /echo HTTP/1.1\r\nContent-Length: 11\r\n\r\nhello",
            b" wor",
            b"ld",
        ]);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body_text(), Some("hello world"));
    }

    #[test]
    fn reads_chunked_bodies() {
        let requests = feed(&[
            b"POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel",
            b"lo\r\n6;ext=1\r\n world\r\n0\r\nTrailer: x\r\n\r\n",
        ]);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body_text(), Some("hello world"));
    }

    #[test]
    fn pipelined_requests_are_cut_apart() {
        let requests = feed(&[
            b"GET /a HTTP/1.1\r\n\r\nPOST /b HTTP/1.1\r\nContent-Length: 2\r\n\r\nxyGET /c HTTP/1.0\r\n\r\n",
        ]);
        let paths: Vec<_> = requests.iter().map(HttpRequest::path).collect();
        assert_eq!(paths, ["/a", "/b", "/c"]);
        assert_eq!(requests[1].body_text(), Some("xy"));
        assert_eq!(requests[2].version(), HttpVersion::Http10);
        assert!(!requests[2].keep_alive());
    }

    #[test]
    fn keep_alive_follows_the_version_and_the_connection_header() {
        let requests = feed(&[
            b"GET / HTTP/1.1\r\nConnection: close\r\n\r\nGET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
        ]);
        assert!(!requests[0].keep_alive());
        assert!(requests[1].keep_alive());
    }

    #[test]
    fn malformed_requests_are_protocol_errors() {
        for head in [
            &b"GET / HTTP/2\r\n\r\n"[..],
            b"GET /\r\n\r\n",
            b"GET / HTTP/1.1\r\nBad header\r\n\r\n",
            b"POST / HTTP/1.1\r\nContent-Length: x\r\n\r\n",
        ] {
            let mut filter = HttpFilter::default();
            let mut buf = BytesMut::from(head);
            assert!(filter.filter(&mut buf).is_err(), "{head:?}");
        }
        let mut filter = HttpFilter::new(HttpOptions { max_head_size: 16 });
        let mut buf = BytesMut::from(&b"GET /a/very/long/path HTTP/1.1\r\n"[..]);
        assert!(filter.filter(&mut buf).is_err());
    }

    #[test]
    fn ambiguous_body_framing_is_rejected() {
        for head in [
            // Both ways of framing the body: what request smuggling relies on.
            &b"POST / HTTP/1.1\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
                [..],
            // A transfer encoding that does not end with chunked leaves the length unknown.
            b"POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\nxxxx",
            b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked, gzip\r\n\r\n0\r\n\r\n",
            // Lengths that disagree.
            b"POST / HTTP/1.1\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\nxxxxx",
            b"POST / HTTP/1.1\r\nContent-Length: 4, 5\r\n\r\nxxxxx",
        ] {
            let mut filter = HttpFilter::default();
            let mut buf = BytesMut::from(head);
            assert!(filter.filter(&mut buf).is_err(), "{head:?}");
        }
        // Repeated identical lengths are fine, as is gzip followed by chunked.
        let requests = feed(&[
            b"POST / HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\nok",
            b"POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n",
        ]);
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.body_text() == Some("ok"))
        );
    }

    #[test]
    fn absurd_chunk_sizes_are_errors_instead_of_panics() {
        let mut filter = HttpFilter::default();
        let mut buf = BytesMut::from(
            &b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\nx"[..],
        );
        assert!(filter.filter(&mut buf).is_err());
    }
}
