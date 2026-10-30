use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hasher},
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use supersocket_client::{
    ClientBuilder, ClientOptions, ClientSender, Connector, EasyClient, ReadError,
};
use supersocket_proto::ProtocolError;

use crate::{
    CloseCode, CloseFrame, Message,
    filter::{HeadFilter, Package, Role, WebSocketPackage},
    frame::{
        MAX_CLOSE_FRAME, MAX_CONTROL_PAYLOAD, MAX_HEADER_SIZE, OP_BINARY, OP_PING, OP_PONG,
        OP_TEXT, apply_mask, close_frame, encode_header,
    },
    handshake::{Head, accept_key, base64, push_header},
};

type Deferred =
    Box<dyn FnOnce(ClientBuilder<WebSocketPackage>) -> ClientBuilder<WebSocketPackage> + Send>;

/// Builds a [`WebSocketClient`]. Created by [`WebSocketClient::builder`].
///
/// The options are those of `EasyClient`. `max_package_length` limits both a frame, header
/// included, and a message joined from fragments; `connect_timeout` applies to connecting and
/// again to the handshake.
pub struct WebSocketClientBuilder {
    url: String,
    subprotocols: Vec<String>,
    headers: Vec<(String, String)>,
    options: ClientOptions,
    deferred: Vec<Deferred>,
    #[cfg(feature = "tls")]
    tls: Option<supersocket_client::TlsConnector>,
}

impl WebSocketClientBuilder {
    /// Offers `protocols` to the server, in order of preference; see
    /// [`WebSocketClient::protocol`].
    pub fn subprotocols(mut self, protocols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.subprotocols = protocols.into_iter().map(Into::into).collect();
        self
    }

    /// Adds a header field to the handshake request, such as `Authorization` or `Origin`.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Connects through `proxy`; see `ClientBuilder::proxy`.
    pub fn proxy(mut self, proxy: impl Connector) -> Self {
        self.deferred
            .push(Box::new(move |builder| builder.proxy(proxy)));
        self
    }

    /// The TLS settings for `wss://` URLs. Default: the Mozilla root certificates.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, tls: supersocket_client::TlsConnector) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Changes the options in place.
    pub fn configure(mut self, configure: impl FnOnce(&mut ClientOptions)) -> Self {
        configure(&mut self.options);
        self
    }

    /// Replaces the options.
    pub fn options(mut self, options: ClientOptions) -> Self {
        self.options = options;
        self
    }

    /// Connects and performs the handshake.
    pub async fn connect(self) -> io::Result<WebSocketClient> {
        let url = Url::parse(&self.url)?;
        let filter = HeadFilter::new(Role::Client, self.options.max_package_length);
        let mut builder = EasyClient::builder(filter).options(self.options.clone());
        for deferred in self.deferred {
            builder = deferred(builder);
        }
        if url.secure {
            #[cfg(feature = "tls")]
            {
                builder = builder.tls(self.tls.unwrap_or_default());
            }
            #[cfg(not(feature = "tls"))]
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "wss:// needs the `tls` feature",
            ));
        }
        let mut client = builder.connect((url.host.as_str(), url.port)).await?;

        let key = base64(&random_bytes::<16>());
        let mut request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n",
            url.resource, url.authority
        );
        if !self.subprotocols.is_empty() {
            push_header(
                &mut request,
                "Sec-WebSocket-Protocol",
                &self.subprotocols.join(", "),
            );
        }
        for (name, value) in &self.headers {
            push_header(&mut request, name, value);
        }
        request.push_str("\r\n");
        let handshake = async {
            client.send(request).await?;
            match client.receive().await? {
                Some(WebSocketPackage(Package::Handshake(Ok(head)))) => {
                    check_response(&head, &key, &self.subprotocols)
                }
                Some(WebSocketPackage(Package::Handshake(Err(error)))) => {
                    Err(invalid_data(format!("invalid handshake response: {error}")))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the server closed the connection during the handshake",
                )),
            }
        };
        let protocol = tokio::time::timeout(self.options.connect_timeout, handshake)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the handshake timed out"))??;

        let sender = WebSocketSender {
            sender: client.sender(),
            close_sent: Arc::new(AtomicBool::new(false)),
        };
        Ok(WebSocketClient {
            client,
            sender,
            protocol,
            close_frame: None,
            finished: false,
            close_timeout: self.options.close_timeout,
        })
    }
}

impl fmt::Debug for WebSocketClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketClientBuilder")
            .field("url", &self.url)
            .field("subprotocols", &self.subprotocols)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

/// A WebSocket client, connected through the connectors of `EasyClient`, so proxies and TLS work
/// as they do there.
///
/// Pings are answered for you. When the server closes the connection,
/// [`receive`](Self::receive) answers its close frame and returns `Ok(None)`.
///
/// ```no_run
/// use supersocket_websocket::{CloseCode, WebSocketClient};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = WebSocketClient::connect("ws://127.0.0.1:4040/chat").await?;
/// client.send_text("hello").await?;
/// if let Some(reply) = client.receive().await? {
///     println!("{:?}", reply.as_text());
/// }
/// client.close(CloseCode::NORMAL, "done").await;
/// # Ok(())
/// # }
/// ```
pub struct WebSocketClient {
    client: EasyClient<WebSocketPackage>,
    sender: WebSocketSender,
    protocol: Option<String>,
    close_frame: Option<CloseFrame>,
    finished: bool,
    close_timeout: Duration,
}

impl WebSocketClient {
    /// Starts building a client for `url`, which starts with `ws://` or `wss://`.
    pub fn builder(url: impl Into<String>) -> WebSocketClientBuilder {
        WebSocketClientBuilder {
            url: url.into(),
            subprotocols: Vec::new(),
            headers: Vec::new(),
            options: ClientOptions::default(),
            deferred: Vec::new(),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Connects to `url` with the default settings.
    pub async fn connect(url: impl Into<String>) -> io::Result<Self> {
        Self::builder(url).connect().await
    }

    /// Waits for the next message. Returns `Ok(None)` once the connection is closed; then
    /// [`close_frame`](Self::close_frame) tells what the server said. Breaking the protocol is
    /// an error, after which the connection is closed.
    pub async fn receive(&mut self) -> Result<Option<Message>, ReadError> {
        while !self.finished {
            let Some(package) = self.client.receive().await? else {
                break;
            };
            match package.0 {
                Package::Message(message) => return Ok(Some(message)),
                Package::Ping(payload) => {
                    if !self.sender.close_sent.load(Ordering::Acquire) {
                        let _ = self.sender.send_frame(OP_PONG, &payload).await;
                    }
                }
                Package::Close(frame) => {
                    let code = frame.as_ref().map(|frame| frame.code);
                    self.close_frame = frame;
                    if !self.sender.close_sent.swap(true, Ordering::AcqRel) {
                        // The server started closing: answer with its code.
                        let _ = self.sender.send_close(code, "");
                    }
                    break;
                }
                Package::Violation(code, why) => {
                    self.finished = true;
                    if !self.sender.close_sent.swap(true, Ordering::AcqRel) {
                        let _ = self.sender.send_close(Some(code), why);
                    }
                    return Err(ReadError::Protocol(ProtocolError::new(why)));
                }
                Package::Pong | Package::Handshake(_) => {}
            }
        }
        self.finished = true;
        Ok(None)
    }

    /// A handle for sending from other tasks.
    pub fn sender(&self) -> WebSocketSender {
        self.sender.clone()
    }

    /// Sends `message`; see [`WebSocketSender::send`].
    pub async fn send(&self, message: &Message) -> io::Result<()> {
        self.sender.send(message).await
    }

    /// Sends a text message; see [`WebSocketSender::send_text`].
    pub async fn send_text(&self, text: &str) -> io::Result<()> {
        self.sender.send_text(text).await
    }

    /// Sends a binary message; see [`WebSocketSender::send_binary`].
    pub async fn send_binary(&self, data: &[u8]) -> io::Result<()> {
        self.sender.send_binary(data).await
    }

    /// Sends a ping; see [`WebSocketSender::ping`].
    pub async fn ping(&self, payload: &[u8]) -> io::Result<()> {
        self.sender.ping(payload).await
    }

    /// The subprotocol the server chose.
    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    /// The code and reason the server gave when it closed the connection.
    pub fn close_frame(&self) -> Option<&CloseFrame> {
        self.close_frame.as_ref()
    }

    /// Local address of the socket.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.client.local_addr()
    }

    /// Address the socket is connected to: the proxy when connecting through one.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.client.peer_addr()
    }

    /// Performs the closing handshake: sends a close frame, waits up to the close timeout for the
    /// server to answer, skipping messages that arrive meanwhile, then closes the connection.
    pub async fn close(mut self, code: CloseCode, reason: &str) {
        self.sender.close(code, reason).await;
        let limit = self.close_timeout;
        let _ = tokio::time::timeout(limit, async {
            while let Ok(Some(_)) = self.receive().await {}
        })
        .await;
        self.client.close().await;
    }
}

impl fmt::Debug for WebSocketClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketClient")
            .field("peer_addr", &self.peer_addr())
            .field("protocol", &self.protocol)
            .field("closed", &self.sender.is_closed())
            .finish_non_exhaustive()
    }
}

/// Sends messages on a [`WebSocketClient`]'s connection from any task. Cheap to clone. Frames
/// are masked, as a client's must be.
#[derive(Clone)]
pub struct WebSocketSender {
    sender: ClientSender,
    close_sent: Arc<AtomicBool>,
}

impl WebSocketSender {
    /// Sends `message`.
    pub async fn send(&self, message: &Message) -> io::Result<()> {
        match message {
            Message::Text(text) => self.send_text(text).await,
            Message::Binary(data) => self.send_binary(data).await,
        }
    }

    /// Sends a text message. Messages from concurrent calls are never interleaved. Fails once
    /// the connection is closing.
    pub async fn send_text(&self, text: &str) -> io::Result<()> {
        self.send_data(OP_TEXT, text.as_bytes()).await
    }

    /// Sends a binary message, like [`send_text`](Self::send_text).
    pub async fn send_binary(&self, data: &[u8]) -> io::Result<()> {
        self.send_data(OP_BINARY, data).await
    }

    /// Sends a ping with up to 125 bytes of `payload`.
    pub async fn ping(&self, payload: &[u8]) -> io::Result<()> {
        if payload.len() > MAX_CONTROL_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a ping payload is at most 125 bytes",
            ));
        }
        self.send_frame(OP_PING, payload).await
    }

    /// Starts the closing handshake by sending a close frame; the receiving side then returns
    /// `Ok(None)` once the server answers. A code that may not be sent is replaced by 1000.
    pub async fn close(&self, code: CloseCode, reason: &str) {
        if self.close_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        let code = if code.is_allowed() {
            code
        } else {
            CloseCode::NORMAL
        };
        let _ = self.send_close(Some(code), reason);
    }

    /// Whether the connection is closed or closing.
    pub fn is_closed(&self) -> bool {
        self.close_sent.load(Ordering::Acquire) || self.sender.is_closed()
    }

    async fn send_data(&self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        if self.close_sent.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the WebSocket connection is closing",
            ));
        }
        self.send_frame(opcode, payload).await
    }

    /// Queues a close frame as the last data, so that no frame can follow it.
    fn send_close(&self, code: Option<CloseCode>, reason: &str) -> io::Result<()> {
        let mut frame = [0; MAX_CLOSE_FRAME];
        let length = close_frame(code, reason, Some(random_bytes()), &mut frame);
        self.sender.send_last(&frame[..length])
    }

    async fn send_frame(&self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mut frame = BytesMut::new();
        let mask = random_bytes::<4>();
        encode_frame(opcode, payload, Some(mask), &mut frame);
        self.sender.send(&frame).await
    }
}

impl fmt::Debug for WebSocketSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketSender")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// Encodes an unfragmented frame into `buf`, masked with `mask` when one is given.
fn encode_frame(opcode: u8, payload: &[u8], mask: Option<[u8; 4]>, buf: &mut BytesMut) {
    let mut header = [0; MAX_HEADER_SIZE];
    let size = encode_header(opcode, payload.len(), mask, &mut header);
    buf.reserve(size + payload.len());
    buf.extend_from_slice(&header[..size]);
    let start = buf.len();
    buf.extend_from_slice(payload);
    if let Some(mask) = mask {
        apply_mask(&mut buf[start..], mask);
    }
}

/// Checks the server's answer to the handshake and returns the subprotocol it chose.
fn check_response(head: &Head, key: &str, offered: &[String]) -> io::Result<Option<String>> {
    let status = head
        .start_line
        .strip_prefix("HTTP/1.1 ")
        .and_then(|rest| rest.get(..3))
        .and_then(|code| code.parse::<u16>().ok());
    if status != Some(101) {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!(
                "the server refused the WebSocket handshake: {}",
                head.start_line
            ),
        ));
    }
    if !head.has_token("Upgrade", "websocket") || !head.has_token("Connection", "upgrade") {
        return Err(invalid_data(
            "the server did not upgrade to WebSocket".into(),
        ));
    }
    if head.header("Sec-WebSocket-Accept") != Some(accept_key(key).as_str()) {
        return Err(invalid_data(
            "the server sent a wrong Sec-WebSocket-Accept".into(),
        ));
    }
    if head.header("Sec-WebSocket-Extensions").is_some() {
        return Err(invalid_data(
            "the server chose an extension that was not offered".into(),
        ));
    }
    match head.header("Sec-WebSocket-Protocol") {
        None => Ok(None),
        Some(protocol) if offered.iter().any(|ours| ours == protocol) => {
            Ok(Some(protocol.to_owned()))
        }
        Some(protocol) => Err(invalid_data(format!(
            "the server chose the subprotocol {protocol:?}, which was not offered"
        ))),
    }
}

fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Unpredictable bytes for masking keys and handshake keys, from the hasher the standard library
/// keys with random numbers from the operating system.
fn random_bytes<const N: usize>() -> [u8; N] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut bytes = [0; N];
    for chunk in bytes.chunks_mut(8) {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        chunk.copy_from_slice(&hasher.finish().to_ne_bytes()[..chunk.len()]);
    }
    bytes
}

/// A `ws://` or `wss://` URL.
#[derive(Debug, PartialEq, Eq)]
struct Url {
    secure: bool,
    host: String,
    port: u16,
    /// The host and port as written, for the `Host` header.
    authority: String,
    /// The path and query.
    resource: String,
}

impl Url {
    fn parse(url: &str) -> io::Result<Self> {
        let invalid = |why: &str| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid WebSocket URL `{url}`: {why}"),
            )
        };
        // The URL goes into the request line, where spaces or line breaks would change it.
        if url.bytes().any(|byte| byte <= b' ' || byte == 0x7F) {
            return Err(invalid(
                "spaces and control characters must be percent-encoded",
            ));
        }
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| invalid("expected ws:// or wss://"))?;
        let secure = if scheme.eq_ignore_ascii_case("ws") {
            false
        } else if scheme.eq_ignore_ascii_case("wss") {
            true
        } else {
            return Err(invalid("expected ws:// or wss://"));
        };
        if rest.contains('#') {
            return Err(invalid("WebSocket URLs have no fragment"));
        }
        let (authority, resource) = rest.split_at(rest.find(['/', '?']).unwrap_or(rest.len()));
        if authority.contains('@') {
            return Err(invalid("credentials in the URL are not supported"));
        }
        let resource = match resource {
            "" => "/".to_owned(),
            query if query.starts_with('?') => format!("/{query}"),
            path => path.to_owned(),
        };
        let default_port = if secure { 443 } else { 80 };
        let parse_port = |port: &str| port.parse::<u16>().map_err(|_| invalid("invalid port"));
        let (host, port) = match authority.strip_prefix('[') {
            Some(rest) => {
                let (host, after) = rest
                    .split_once(']')
                    .ok_or_else(|| invalid("unclosed `[`"))?;
                let port = match after {
                    "" => default_port,
                    after => parse_port(
                        after
                            .strip_prefix(':')
                            .ok_or_else(|| invalid("invalid port"))?,
                    )?,
                };
                (host, port)
            }
            None => match authority.split_once(':') {
                Some((_, port)) if port.contains(':') => {
                    return Err(invalid("IPv6 addresses must be in brackets"));
                }
                Some((host, port)) => (host, parse_port(port)?),
                None => (authority, default_port),
            },
        };
        if host.is_empty() {
            return Err(invalid("missing host"));
        }
        Ok(Self {
            secure,
            host: host.to_owned(),
            port,
            authority: authority.to_owned(),
            resource,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_websocket_urls() {
        let url = Url::parse("ws://example.com/chat?room=1").unwrap();
        assert_eq!(
            url,
            Url {
                secure: false,
                host: "example.com".into(),
                port: 80,
                authority: "example.com".into(),
                resource: "/chat?room=1".into(),
            }
        );
        let url = Url::parse("WSS://[::1]:8443?x=1").unwrap();
        assert!(url.secure);
        assert_eq!((url.host.as_str(), url.port), ("::1", 8443));
        assert_eq!(url.authority, "[::1]:8443");
        assert_eq!(url.resource, "/?x=1");
        assert_eq!(Url::parse("wss://example.com").unwrap().port, 443);

        for invalid in [
            "http://example.com",
            "ws://",
            "ws://user@example.com",
            "ws://example.com:99999",
            "ws://::1/",
            "ws://example.com/#top",
            "ws://[::1",
            "ws://example.com/a b",
            "ws://example.com/x HTTP/1.1\r\nX-Injected: 1",
        ] {
            assert!(Url::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn random_bytes_differ() {
        assert_ne!(random_bytes::<16>(), random_bytes::<16>());
    }
}
