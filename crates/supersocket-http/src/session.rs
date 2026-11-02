use std::{fmt, future::Future, io, time::Duration};

use bytes::BytesMut;
use supersocket_server::{CloseReason, Session, SessionState};

use crate::{HttpRequest, HttpResponse};

/// Sending HTTP responses on a [`Session`].
pub trait HttpSessionExt {
    /// The session type.
    type State;

    /// Sends `response`. When it does not keep the connection alive, the session closes once
    /// the response is sent.
    fn send_response(&self, response: HttpResponse) -> impl Future<Output = io::Result<()>> + Send;

    /// Answers `request` with `response`, keeping the connection alive for the next request only
    /// when both allow it; see [`HttpRequest::keep_alive`] and [`HttpResponse::keep_alive`].
    fn respond(
        &self,
        request: &HttpRequest,
        response: HttpResponse,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Starts a Server-Sent Events stream: sends a `200` response head with
    /// `Content-Type: text/event-stream` and the header fields of `options`, and returns the
    /// writer for the events. The stream goes on until the session closes, from either side.
    ///
    /// ```
    /// use supersocket_http::{HttpSessionExt, SseEvent, SseOptions};
    /// use supersocket_server::Session;
    ///
    /// # async fn run(session: Session) -> std::io::Result<()> {
    /// let events = session.start_sse(SseOptions::default()).await?;
    /// tokio::spawn(async move {
    ///     for tick in 0..10 {
    ///         if events.send(&SseEvent::new(tick.to_string()).event("tick")).await.is_err() {
    ///             break; // the client is gone
    ///         }
    ///         tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    ///     }
    /// });
    /// # Ok(())
    /// # }
    /// ```
    fn start_sse(
        &self,
        options: SseOptions,
    ) -> impl Future<Output = io::Result<SseWriter<Self::State>>> + Send;
}

impl<S: SessionState> HttpSessionExt for Session<S> {
    type State = S;

    async fn send_response(&self, response: HttpResponse) -> io::Result<()> {
        let keep_alive = response.is_keep_alive();
        self.send_with(|buf| {
            response.write_to(buf);
            Ok::<_, io::Error>(())
        })
        .await?;
        if !keep_alive {
            // What is queued, the response included, is still sent.
            self.close(CloseReason::LocalClosed);
        }
        Ok(())
    }

    async fn respond(&self, request: &HttpRequest, response: HttpResponse) -> io::Result<()> {
        let keep_alive = request.keep_alive() && response.is_keep_alive();
        self.send_response(response.keep_alive(keep_alive)).await
    }

    async fn start_sse(&self, options: SseOptions) -> io::Result<SseWriter<S>> {
        let mut response = HttpResponse::ok()
            .content_type("text/event-stream")
            .header("Cache-Control", "no-cache")
            .streaming();
        for (name, value) in options.headers {
            response = response.header(name, value);
        }
        self.send_with(|buf| {
            response.write_to(buf);
            if let Some(retry) = options.retry {
                SseEvent::write_retry(retry, buf);
            }
            Ok::<_, io::Error>(())
        })
        .await?;
        let writer = SseWriter {
            session: self.clone(),
        };
        if let Some(interval) = options.keep_alive {
            let writer = writer.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = writer.session.closed() => return,
                        () = tokio::time::sleep(interval) => {}
                    }
                    if writer.comment("keep-alive").await.is_err() {
                        return;
                    }
                }
            });
        }
        Ok(writer)
    }
}

/// How to start a Server-Sent Events stream; see [`HttpSessionExt::start_sse`].
#[derive(Debug, Clone, Default)]
pub struct SseOptions {
    /// Header fields to add to the response, such as `Access-Control-Allow-Origin` for browsers
    /// on another origin.
    pub headers: Vec<(String, String)>,
    /// How long the client should wait before reconnecting, sent as the stream opens. Default:
    /// the client's own.
    pub retry: Option<Duration>,
    /// Send a comment line this often while nothing else is sent, so that proxies and clients
    /// do not take the stream for dead. Default: none.
    pub keep_alive: Option<Duration>,
}

impl SseOptions {
    /// Adds a header field to the response.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Tells the client how long to wait before reconnecting.
    pub fn retry(mut self, retry: Duration) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Sends a comment line every `interval` while nothing else is sent.
    pub fn keep_alive(mut self, interval: Duration) -> Self {
        self.keep_alive = Some(interval);
        self
    }
}

/// One Server-Sent Event; see [`SseWriter::send`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SseEvent {
    /// The `data` field; every line becomes a `data:` line.
    pub data: String,
    /// The `event` field, the event type a client listens for. Default: `message`.
    pub event: Option<String>,
    /// The `id` field, which a client sends back as `Last-Event-ID` when it reconnects.
    pub id: Option<String>,
    /// The `retry` field: how long the client waits before reconnecting.
    pub retry: Option<Duration>,
}

impl SseEvent {
    /// An event carrying `data`.
    pub fn new(data: impl Into<String>) -> Self {
        Self {
            data: data.into(),
            ..Self::default()
        }
    }

    /// Sets the event type.
    pub fn event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }

    /// Sets the event id.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Sets the reconnection time.
    pub fn retry(mut self, retry: Duration) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Writes the event in its wire form, ending with the empty line.
    pub fn write_to(&self, buf: &mut BytesMut) {
        if let Some(event) = &self.event {
            Self::write_field("event", event, buf);
        }
        if let Some(id) = &self.id {
            Self::write_field("id", id, buf);
        }
        if let Some(retry) = self.retry {
            Self::write_retry(retry, buf);
        }
        for line in self.data.split('\n') {
            Self::write_field("data", line.strip_suffix('\r').unwrap_or(line), buf);
        }
        buf.extend_from_slice(b"\n");
    }

    fn write_retry(retry: Duration, buf: &mut BytesMut) {
        use std::fmt::Write as _;
        let _ = writeln!(buf, "retry: {}", retry.as_millis());
    }

    /// Writes one field line. A line break in the value would end the field early, so it is
    /// replaced.
    fn write_field(name: &str, value: &str, buf: &mut BytesMut) {
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(b": ");
        for byte in value.bytes() {
            buf.extend_from_slice(if byte == b'\n' || byte == b'\r' {
                b" "
            } else {
                std::slice::from_ref(&byte)
            });
        }
        buf.extend_from_slice(b"\n");
    }
}

/// Sends Server-Sent Events on a session; created by [`HttpSessionExt::start_sse`]. Cheap to
/// clone, for sending from other tasks. Sending fails once the session is closed, which is how
/// a client that went away is noticed.
pub struct SseWriter<S = ()> {
    session: Session<S>,
}

impl<S: SessionState> SseWriter<S> {
    /// Sends `event`.
    pub async fn send(&self, event: &SseEvent) -> io::Result<()> {
        self.session
            .send_with(|buf| {
                event.write_to(buf);
                Ok(())
            })
            .await
    }

    /// Sends an event of the default type carrying `data`.
    pub async fn data(&self, data: &str) -> io::Result<()> {
        self.send(&SseEvent::new(data)).await
    }

    /// Sends a comment line, which clients ignore: a heartbeat.
    pub async fn comment(&self, text: &str) -> io::Result<()> {
        self.session
            .send_with(|buf| {
                SseEvent::write_field("", text, buf);
                buf.extend_from_slice(b"\n");
                Ok(())
            })
            .await
    }

    /// Ends the stream by closing the session.
    pub fn close(&self) {
        self.session.close(CloseReason::LocalClosed);
    }

    /// The session the events go to.
    pub fn session(&self) -> &Session<S> {
        &self.session
    }
}

impl<S> Clone for SseWriter<S> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
        }
    }
}

impl<S> fmt::Debug for SseWriter<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseWriter")
            .field("session", &self.session.id())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_written_field_by_field() {
        let mut buf = BytesMut::new();
        SseEvent::new("line one\nline two")
            .event("tick")
            .id("7")
            .retry(Duration::from_millis(1500))
            .write_to(&mut buf);
        assert_eq!(
            &buf[..],
            &b"event: tick\nid: 7\nretry: 1500\ndata: line one\ndata: line two\n\n"[..]
        );
        let mut buf = BytesMut::new();
        SseEvent::new("").id("a\nb").write_to(&mut buf);
        assert_eq!(&buf[..], &b"id: a b\ndata: \n\n"[..]);
    }
}
