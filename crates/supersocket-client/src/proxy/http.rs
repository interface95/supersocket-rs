use std::io;

use bytes::Bytes;
use supersocket_proto::{ProtocolError, TerminatorFilter};

use super::{Credentials, ProxyConnection, ProxyConnector, base64, refused};
use crate::{Connection, Endpoint, ToEndpoint};

/// Connects through an HTTP proxy with the `CONNECT` method, like SuperSocket's `HttpConnector`.
#[derive(Debug, Clone)]
pub struct HttpConnector {
    proxy: Endpoint,
    credentials: Option<Credentials>,
}

impl HttpConnector {
    /// A connector for the proxy at `proxy`, such as `"proxy.example:8080"`.
    pub fn new(proxy: impl ToEndpoint) -> io::Result<Self> {
        Ok(Self {
            proxy: proxy.to_endpoint()?,
            credentials: None,
        })
    }

    /// Authenticates with HTTP Basic authentication.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some(Credentials::new(username, password));
        self
    }

    /// The `CONNECT` request for `target`.
    fn request(&self, target: &Endpoint) -> io::Result<String> {
        // The target goes into the request line, where spaces or line breaks would change it.
        let authority = target.to_string();
        if authority.bytes().any(|byte| byte <= b' ' || byte == 0x7F) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid target {authority:?}"),
            ));
        }
        let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
        if let Some(Credentials { username, password }) = &self.credentials {
            let token = base64(format!("{username}:{password}").as_bytes());
            request.push_str(&format!("Proxy-Authorization: Basic {token}\r\n"));
        }
        request.push_str("\r\n");
        Ok(request)
    }
}

impl ProxyConnector for HttpConnector {
    fn proxy_endpoint(&self) -> &Endpoint {
        &self.proxy
    }

    async fn connect_proxy(
        &self,
        target: &Endpoint,
        connection: Connection,
    ) -> io::Result<Connection> {
        let request = self.request(target)?;
        // The reply is a status line and headers, ended by an empty line.
        let mut connection =
            ProxyConnection::new(connection, TerminatorFilter::new(&b"\n"[..], decode_line));
        connection.send(request.as_bytes()).await?;
        handle_response(&connection.receive().await?)?;
        while !connection.receive().await?.is_empty() {}
        Ok(connection.detach())
    }
}

/// Decodes a line of the reply. Lines should end with `\r\n`, but like most HTTP clients this
/// accepts a bare `\n` too.
fn decode_line(line: Bytes) -> Result<String, ProtocolError> {
    let line = line.strip_suffix(b"\r").unwrap_or(&line);
    Ok(String::from_utf8_lossy(line).into_owned())
}

/// Checks the status line of the reply: the proxy connected if the status is 2xx.
fn handle_response(status_line: &str) -> io::Result<()> {
    let status = status_line
        .strip_prefix("HTTP/1.")
        .and_then(|rest| rest.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("not an HTTP reply: {status_line:?}"),
            )
        })?;
    if !(200..300).contains(&status) {
        return Err(refused(format!(
            "the proxy refused the connection: {status_line}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::proxy::in_memory;

    #[tokio::test]
    async fn targets_cannot_change_the_request() {
        let target = Endpoint::new("example.com:443 HTTP/1.1\r\nX-Injected: 1\r\nX", 80);
        let (connection, mut proxy) = in_memory(b"").await;
        let error = HttpConnector::new("127.0.0.1:1")
            .unwrap()
            .connect_proxy(&target, connection)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        // Nothing was sent, and the connection is closed.
        assert_eq!(proxy.read(&mut [0; 16]).await.unwrap(), 0);
    }

    #[test]
    fn accepts_only_2xx_replies() {
        assert!(handle_response("HTTP/1.1 200 Connection established").is_ok());
        assert!(handle_response("HTTP/1.0 204 No Content").is_ok());
        let refused = handle_response("HTTP/1.1 407 Proxy Authentication Required").unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);
        let invalid = handle_response("SSH-2.0-OpenSSH").unwrap_err();
        assert_eq!(invalid.kind(), io::ErrorKind::InvalidData);
    }
}
