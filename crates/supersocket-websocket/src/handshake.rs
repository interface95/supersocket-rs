//! The HTTP handshake that opens a WebSocket connection, RFC 6455 section 4.

use std::fmt;

/// The largest HTTP head accepted during the handshake.
pub(crate) const MAX_HEAD_SIZE: usize = 16 * 1024;

const ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// An HTTP head: the start line and the header fields.
#[derive(Debug, Clone)]
pub(crate) struct Head {
    pub(crate) start_line: String,
    pub(crate) headers: Vec<(String, String)>,
}

impl Head {
    /// The first value of the header field `name`.
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    /// Whether the comma-separated values of the header field `name` include `token`, ignoring
    /// case.
    pub(crate) fn has_token(&self, name: &str, token: &str) -> bool {
        values(&self.headers, name).any(|value| value.eq_ignore_ascii_case(token))
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// The comma-separated values of every header field called `name`.
fn values<'a>(headers: &'a [(String, String)], name: &'a str) -> impl Iterator<Item = &'a str> {
    headers
        .iter()
        .filter(move |(field, _)| field.eq_ignore_ascii_case(name))
        .flat_map(|(_, value)| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Why an HTTP head could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeadError {
    TooLarge,
    Malformed(&'static str),
}

impl fmt::Display for HeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge => f.write_str("the HTTP head is too large"),
            Self::Malformed(why) => f.write_str(why),
        }
    }
}

/// Finds the empty line that ends an HTTP head at the start of `buf` and returns the length of
/// the head including it. Lines may end with `\r\n` or `\n`. The first `searched` bytes were
/// already searched.
pub(crate) fn head_length(buf: &[u8], searched: usize) -> Option<usize> {
    // The end may have begun in the last two bytes searched.
    let mut start = searched.saturating_sub(2);
    while let Some(offset) = buf[start..].iter().position(|&byte| byte == b'\n') {
        let after = start + offset + 1;
        match &buf[after..] {
            [b'\n', ..] => return Some(after + 1),
            [b'\r', b'\n', ..] => return Some(after + 2),
            _ => start = after,
        }
    }
    None
}

/// Parses an HTTP head found by [`head_length`].
pub(crate) fn parse_head(head: &[u8]) -> Result<Head, HeadError> {
    let head = String::from_utf8_lossy(head);
    let mut lines = head
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        // An empty line before the start line is tolerated, as RFC 9112 suggests.
        .skip_while(|line| line.is_empty());
    let start_line = lines
        .next()
        .filter(|line| !line.is_empty())
        .ok_or(HeadError::Malformed("missing start line"))?;
    let mut headers = Vec::new();
    for line in lines.take_while(|line| !line.is_empty()) {
        if line.starts_with([' ', '\t']) {
            return Err(HeadError::Malformed("folded header line"));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or(HeadError::Malformed("header line without a colon"))?;
        if name.is_empty() || name.contains([' ', '\t']) {
            return Err(HeadError::Malformed("invalid header name"));
        }
        headers.push((name.to_owned(), value.trim_matches([' ', '\t']).to_owned()));
    }
    Ok(Head {
        start_line: start_line.to_owned(),
        headers,
    })
}

/// The HTTP request that opened a WebSocket connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeRequest {
    target: String,
    headers: Vec<(String, String)>,
}

/// What a session reports before its handshake request arrived.
pub(crate) static NO_REQUEST: HandshakeRequest = HandshakeRequest {
    target: String::new(),
    headers: Vec::new(),
};

impl HandshakeRequest {
    /// The request target, such as `/chat?room=1`.
    pub fn uri(&self) -> &str {
        &self.target
    }

    /// The path of the request target, such as `/chat`.
    pub fn path(&self) -> &str {
        self.target
            .split_once('?')
            .map_or(self.target.as_str(), |(path, _)| path)
    }

    /// The query of the request target, without the `?`.
    pub fn query(&self) -> Option<&str> {
        self.target.split_once('?').map(|(_, query)| query)
    }

    /// The first value of the header field `name`, which is compared ignoring case.
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    /// Every header field, in the order received.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// The subprotocols the client offered, in its order of preference.
    pub fn subprotocols(&self) -> impl Iterator<Item = &str> {
        values(&self.headers, "Sec-WebSocket-Protocol")
    }
}

/// An HTTP error response that refuses a WebSocket handshake.
///
/// ```
/// use supersocket_websocket::Rejection;
///
/// let rejection = Rejection::new(401, "log in first").header("WWW-Authenticate", "Bearer");
/// assert_eq!(rejection.status(), 401);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    status: u16,
    message: String,
    headers: Vec<(String, String)>,
}

impl Rejection {
    /// Refuses with the HTTP `status`, such as 401 or 403, sending `message` as the body.
    ///
    /// # Panics
    ///
    /// Panics if `status` is not a three-digit status code.
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        assert!(
            (100..=999).contains(&status),
            "{status} is not an HTTP status code"
        );
        Self {
            status,
            message: message.into(),
            headers: Vec::new(),
        }
    }

    /// Adds a header field to the response.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// The HTTP status.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// The response body.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn response(&self) -> String {
        let mut response = format!(
            "HTTP/1.1 {} {}\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\n\
             Content-Length: {}\r\n",
            self.status,
            reason_phrase(self.status),
            self.message.len(),
        );
        for (name, value) in &self.headers {
            push_header(&mut response, name, value);
        }
        response.push_str("\r\n");
        response.push_str(&self.message);
        response
    }
}

impl From<HeadError> for Rejection {
    fn from(error: HeadError) -> Self {
        match error {
            HeadError::TooLarge => Rejection::new(431, error.to_string()),
            HeadError::Malformed(_) => Rejection::new(400, error.to_string()),
        }
    }
}

/// Writes a header line, dropping control characters so a value cannot start a new line.
pub(crate) fn push_header(head: &mut String, name: &str, value: &str) {
    head.extend(name.chars().filter(|c| !c.is_control()));
    head.push_str(": ");
    head.extend(value.chars().filter(|c| !c.is_control()));
    head.push_str("\r\n");
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        101 => "Switching Protocols",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        426 => "Upgrade Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "",
    }
}

/// Checks a client's opening handshake and returns the request with its `Sec-WebSocket-Key`.
pub(crate) fn read_request(head: Head) -> Result<(HandshakeRequest, String), Rejection> {
    let bad = |message: &str| Rejection::new(400, message);
    let mut parts = head.start_line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("malformed request line"));
    };
    if method != "GET" {
        return Err(bad("a WebSocket handshake must use GET"));
    }
    if version != "HTTP/1.1" {
        return Err(bad("a WebSocket handshake must use HTTP/1.1"));
    }
    if target.is_empty() {
        return Err(bad("missing request target"));
    }
    if head.header("Host").is_none() {
        return Err(bad("missing Host header"));
    }
    if !head.has_token("Upgrade", "websocket") || !head.has_token("Connection", "upgrade") {
        return Err(bad("not a WebSocket upgrade request"));
    }
    if head.header("Sec-WebSocket-Version") != Some("13") {
        return Err(
            Rejection::new(426, "only WebSocket version 13 is supported")
                .header("Sec-WebSocket-Version", "13"),
        );
    }
    let key = head
        .header("Sec-WebSocket-Key")
        .filter(|key| is_valid_key(key))
        .ok_or_else(|| bad("missing or invalid Sec-WebSocket-Key"))?
        .to_owned();
    let target = target.to_owned();
    Ok((
        HandshakeRequest {
            target,
            headers: head.headers,
        },
        key,
    ))
}

/// Whether `key` is the base64 encoding of 16 bytes.
fn is_valid_key(key: &str) -> bool {
    key.len() == 24
        && key.ends_with("==")
        && key[..22]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/')
}

/// The server's answer accepting a handshake.
pub(crate) fn accept_response(key: &str, protocol: Option<&str>) -> String {
    let mut response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n",
        accept_key(key)
    );
    if let Some(protocol) = protocol {
        push_header(&mut response, "Sec-WebSocket-Protocol", protocol);
    }
    response.push_str("\r\n");
    response
}

/// The `Sec-WebSocket-Accept` value that answers `key`.
pub(crate) fn accept_key(key: &str) -> String {
    base64(&sha1(format!("{key}{ACCEPT_GUID}").as_bytes()))
}

pub(crate) fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let bytes = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let group = u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]);
        let symbol = |shift: u32| ALPHABET[(group >> shift) as usize & 63] as char;
        output.push(symbol(18));
        output.push(symbol(12));
        output.push(if chunk.len() > 1 { symbol(6) } else { '=' });
        output.push(if chunk.len() > 2 { symbol(0) } else { '=' });
    }
    output
}

/// SHA-1, which the handshake uses to prove the server understood it; it protects nothing.
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut state: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut message = data.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&(data.len() as u64 * 8).to_be_bytes());

    for block in message.chunks_exact(64) {
        let mut words = [0u32; 80];
        for (word, bytes) in words.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes(bytes.try_into().unwrap());
        }
        for index in 16..80 {
            words[index] =
                (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                    .rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = state;
        for (index, word) in words.iter().enumerate() {
            let (f, k) = match index {
                0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let next = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = next;
        }
        for (value, add) in state.iter_mut().zip([a, b, c, d, e]) {
            *value = value.wrapping_add(add);
        }
    }

    let mut digest = [0; 20];
    for (bytes, value) in digest.chunks_exact_mut(4).zip(state) {
        bytes.copy_from_slice(&value.to_be_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn sha1_matches_the_standard_test_vectors() {
        assert_eq!(hex(&sha1(b"")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            hex(&sha1(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            hex(&sha1(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        assert_eq!(
            hex(&sha1(&[b'a'; 1000])),
            "291e9a6c66994949b57ba5e650361e98fc36b1ba"
        );
    }

    #[test]
    fn accept_key_matches_the_rfc_example() {
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn finds_the_end_of_a_head_however_it_arrives() {
        let head = b"GET / HTTP/1.1\r\nHost: a\r\n\r\nrest";
        assert_eq!(head_length(head, 0), Some(head.len() - 4));
        assert_eq!(head_length(b"GET / HTTP/1.1\nHost: a\n\nrest", 0), Some(24));
        assert_eq!(head_length(b"GET / HTTP/1.1\r\nHost: a\r\n", 0), None);
        // The empty line arrives in pieces after the earlier part was searched.
        let partial = b"GET / HTTP/1.1\r\nHost: a\r\n\r";
        assert_eq!(head_length(partial, partial.len()), None);
        assert_eq!(head_length(head, partial.len()), Some(head.len() - 4));
    }

    fn request(lines: &[&str]) -> Result<(HandshakeRequest, String), Rejection> {
        let mut text = lines.join("\r\n");
        text.push_str("\r\n\r\n");
        read_request(parse_head(text.as_bytes()).map_err(Rejection::from)?)
    }

    const VALID: [&str; 6] = [
        "GET /chat?room=1 HTTP/1.1",
        "Host: example.com",
        "Upgrade: websocket",
        "Connection: keep-alive, Upgrade",
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
        "Sec-WebSocket-Version: 13",
    ];

    #[test]
    fn reads_a_valid_request() {
        let mut lines = VALID.to_vec();
        lines.push("Sec-WebSocket-Protocol: chat, superchat");
        let (request, key) = request(&lines).unwrap();
        assert_eq!(key, "dGhlIHNhbXBsZSBub25jZQ==");
        assert_eq!(request.path(), "/chat");
        assert_eq!(request.query(), Some("room=1"));
        assert_eq!(request.header("host"), Some("example.com"));
        assert_eq!(
            request.subprotocols().collect::<Vec<_>>(),
            ["chat", "superchat"]
        );
    }

    #[test]
    fn rejects_requests_that_are_not_websocket_handshakes() {
        let replace = |index: usize, line: &str| {
            let mut lines = VALID.to_vec();
            lines[index] = line;
            request(&lines).unwrap_err()
        };
        assert_eq!(replace(0, "POST /chat HTTP/1.1").status(), 400);
        assert_eq!(replace(0, "GET /chat HTTP/1.0").status(), 400);
        assert_eq!(replace(1, "X-Host: example.com").status(), 400);
        assert_eq!(replace(2, "Upgrade: h2c").status(), 400);
        assert_eq!(replace(3, "Connection: keep-alive").status(), 400);
        assert_eq!(replace(4, "Sec-WebSocket-Key: short").status(), 400);
        let rejection = replace(5, "Sec-WebSocket-Version: 8");
        assert_eq!(rejection.status(), 426);
        assert!(
            rejection
                .response()
                .contains("Sec-WebSocket-Version: 13\r\n")
        );
        assert_eq!(replace(1, " folded").status(), 400);
    }

    #[test]
    fn responses_cannot_be_split_by_header_values() {
        let response = Rejection::new(403, "no")
            .header("X-Reason", "a\r\nSet-Cookie: x")
            .response();
        assert!(
            response.contains("X-Reason: aSet-Cookie: x\r\n"),
            "{response}"
        );
        assert!(response.ends_with("\r\n\r\nno"));
    }
}
