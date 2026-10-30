use std::io;

use bytes::Bytes;
use supersocket_connection::PackageReader;
use supersocket_proto::{ProtocolError, TerminatorFilter};
use tokio::io::AsyncWriteExt;

use super::{
    Credentials, HANDSHAKE_LIMITS, base64, connect_to_proxy, finish, into_io_error, refused,
};
use crate::{Connection, Connector, Endpoint, connector::must_be_first};

/// Connects through an HTTP proxy with the `CONNECT` method, like SuperSocket's `HttpConnector`.
#[derive(Debug, Clone)]
pub struct HttpProxy {
    proxy: String,
    credentials: Option<Credentials>,
}

impl HttpProxy {
    /// A connector for the proxy at `proxy`, written as `host:port`.
    pub fn new(proxy: impl Into<String>) -> Self {
        Self {
            proxy: proxy.into(),
            credentials: None,
        }
    }

    /// Authenticates with HTTP Basic authentication.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some(Credentials::new(username, password));
        self
    }
}

impl Connector for HttpProxy {
    async fn connect(
        &self,
        target: &Endpoint,
        previous: Option<Connection>,
    ) -> io::Result<Connection> {
        must_be_first("HttpProxy", &previous)?;
        let mut stream = connect_to_proxy(&self.proxy).await?;

        let authority = target.to_string();
        let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
        if let Some(Credentials { username, password }) = &self.credentials {
            let token = base64(format!("{username}:{password}").as_bytes());
            request.push_str(&format!("Proxy-Authorization: Basic {token}\r\n"));
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await?;

        // The reply is a status line and headers, ended by an empty line.
        let mut reader = PackageReader::new(
            &mut stream,
            Box::new(TerminatorFilter::new(&b"\n"[..], decode_line)),
            HANDSHAKE_LIMITS,
        );
        let mut next_line = async || match reader.next().await.map_err(into_io_error)? {
            Some(line) => Ok(line),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the proxy closed the connection",
            )),
        };
        let status_line = next_line().await?;
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
        while !next_line().await?.is_empty() {}
        let (_, rest) = reader.into_parts();
        Ok(finish(stream, rest))
    }
}

/// Decodes a line of the reply. Lines should end with `\r\n`, but like most HTTP clients this
/// accepts a bare `\n` too.
fn decode_line(line: Bytes) -> Result<String, ProtocolError> {
    let line = line.strip_suffix(b"\r").unwrap_or(&line);
    Ok(String::from_utf8_lossy(line).into_owned())
}
