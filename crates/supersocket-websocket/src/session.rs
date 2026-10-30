use std::{fmt, io, net::SocketAddr, ops::Deref, sync::atomic::Ordering};

use supersocket_server::{CloseReason, Extensions, ServerInfo, Session};

use crate::{
    CloseCode, CloseFrame, HandshakeRequest, Message, WebSocketState,
    frame::{MAX_CONTROL_PAYLOAD, OP_BINARY, OP_PING, OP_TEXT},
    handshake::NO_REQUEST,
    server::{send_close, send_frame},
};

/// A WebSocket connection on the server, handed to the handlers of a
/// [`WebSocketServer`](crate::WebSocketServer). Cloning is cheap; clones refer to the same
/// connection. Dereferences to your session state `S`.
pub struct WebSocketSession<S = ()> {
    session: Session<WebSocketState<S>>,
}

impl<S> WebSocketSession<S> {
    /// The underlying session.
    pub fn session(&self) -> &Session<WebSocketState<S>> {
        &self.session
    }

    /// Unique id of the session within its server.
    pub fn id(&self) -> u64 {
        self.session.id()
    }

    /// Address of the client.
    pub fn remote_addr(&self) -> SocketAddr {
        self.session.remote_addr()
    }

    /// Local address the client connected to.
    pub fn local_addr(&self) -> SocketAddr {
        self.session.local_addr()
    }

    /// Whether the connection uses TLS (`wss://`).
    pub fn is_secure(&self) -> bool {
        self.session.is_secure()
    }

    /// Data attached to this session.
    pub fn extensions(&self) -> &Extensions {
        self.session.extensions()
    }

    /// The server this session belongs to.
    pub fn server(&self) -> &ServerInfo<WebSocketState<S>> {
        self.session.server()
    }

    /// The connected sessions of the server, for broadcasting.
    pub fn sessions(&self) -> Vec<WebSocketSession<S>> {
        crate::server::sessions(self.server())
    }

    /// The HTTP request that opened the connection. Before it arrived, which only middleware
    /// can observe, it is empty.
    pub fn request(&self) -> &HandshakeRequest {
        self.session.request.get().unwrap_or(&NO_REQUEST)
    }

    /// The subprotocol chosen during the handshake.
    pub fn protocol(&self) -> Option<&str> {
        self.session.protocol.get().map(String::as_str)
    }

    /// The code and reason the client gave when it closed the connection.
    pub fn close_frame(&self) -> Option<&CloseFrame> {
        self.session.close_frame.get()
    }

    /// Whether the connection is closed or closing.
    pub fn is_closed(&self) -> bool {
        self.session.is_closed() || self.session.close_sent.load(Ordering::Acquire)
    }

    /// Completes once the connection is closed or closing.
    pub async fn closed(&self) {
        self.session.closed().await;
    }

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

    /// Sends a ping with up to 125 bytes of `payload`; the client answers with a pong.
    pub async fn ping(&self, payload: &[u8]) -> io::Result<()> {
        if payload.len() > MAX_CONTROL_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a ping payload is at most 125 bytes",
            ));
        }
        send_frame(&self.session, OP_PING, payload).await
    }

    async fn send_data(&self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        if self.session.close_sent.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the WebSocket connection is closing",
            ));
        }
        send_frame(&self.session, opcode, payload).await
    }
}

impl<S: Send + Sync + 'static> WebSocketSession<S> {
    /// Starts the closing handshake: sends a close frame with `code` and `reason`, then closes
    /// the connection when the client answers, or after the close timeout. A code that may not
    /// be sent (see [`CloseCode::is_allowed`]) is replaced by 1000, and a reason longer than 123
    /// bytes is cut short.
    pub async fn close(&self, code: CloseCode, reason: &str) {
        if self.session.close_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        let code = if code.is_allowed() {
            code
        } else {
            CloseCode::NORMAL
        };
        let _ = send_close(&self.session, Some(code), reason);
        let session = self.session.clone();
        let limit = session.server().options().close_timeout;
        tokio::spawn(async move {
            if tokio::time::timeout(limit, session.closed()).await.is_err() {
                session.close(CloseReason::LocalClosed);
            }
        });
    }
}

impl<S> Clone for WebSocketSession<S> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
        }
    }
}

impl<S> Deref for WebSocketSession<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.session.user
    }
}

impl<S> From<Session<WebSocketState<S>>> for WebSocketSession<S> {
    fn from(session: Session<WebSocketState<S>>) -> Self {
        Self { session }
    }
}

impl<S> fmt::Debug for WebSocketSession<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketSession")
            .field("id", &self.session.id())
            .field("remote_addr", &self.session.remote_addr())
            .field("path", &self.request().path())
            .finish_non_exhaustive()
    }
}
