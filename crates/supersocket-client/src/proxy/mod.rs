//! Proxy connectors. Like SuperSocket's, they parse the proxy's replies with the pipeline
//! filters from `supersocket-proto`.

mod http;
mod socks4;
mod socks5;

use std::{fmt, io};

use bytes::BytesMut;
use supersocket_connection::{PackageReader, ReadOptions, Rewind};
use supersocket_proto::BoxFilter;
use tokio::net::TcpStream;

pub use http::HttpProxy;
pub use socks4::Socks4Proxy;
pub use socks5::Socks5Proxy;

use crate::{BoxStream, Connection, Endpoint, TcpConnector};

const HANDSHAKE_LIMITS: ReadOptions = ReadOptions {
    max_package_length: 64 * 1024,
    receive_buffer_size: 1024,
};

async fn connect_to_proxy(proxy: &str) -> io::Result<TcpStream> {
    let proxy: Endpoint = proxy.parse()?;
    TcpConnector::new().open(&proxy).await
}

/// Reads one reply from the proxy and returns it with whatever the proxy sent after it.
async fn read_reply<P: Send + 'static>(
    stream: &mut TcpStream,
    filter: BoxFilter<P>,
) -> io::Result<(P, BytesMut)> {
    let mut reader = PackageReader::new(&mut *stream, filter, HANDSHAKE_LIMITS);
    let reply = reader.next().await?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the proxy closed the connection",
        )
    })?;
    let (_, rest) = reader.into_parts();
    Ok((reply, rest))
}

/// Reads one reply in the middle of a handshake, where the proxy must not send anything more.
async fn read_step<P: Send + 'static>(
    stream: &mut TcpStream,
    filter: BoxFilter<P>,
) -> io::Result<P> {
    let (reply, rest) = read_reply(stream, filter).await?;
    if !rest.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the proxy sent unexpected data during the handshake",
        ));
    }
    Ok(reply)
}

fn refused(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionRefused, message)
}

/// Turns the stream connected through the proxy into a connection, putting back any data the
/// proxy already sent past its reply.
fn finish(stream: TcpStream, rest: BytesMut) -> Connection {
    let local_addr = stream.local_addr().ok();
    let peer_addr = stream.peer_addr().ok();
    let stream: BoxStream = if rest.is_empty() {
        Box::new(stream)
    } else {
        Box::new(Rewind::new(rest.freeze(), stream))
    };
    Connection {
        stream,
        local_addr,
        peer_addr,
    }
}

/// A username and password. `Debug` output leaves out the password.
#[derive(Clone)]
struct Credentials {
    username: String,
    password: String,
}

impl Credentials {
    fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &format_args!("<redacted>"))
            .finish()
    }
}

fn base64(input: &[u8]) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_base64() {
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64(b"a"), "YQ==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b""), "");
    }

    #[test]
    fn debug_output_leaves_out_passwords() {
        let http = format!(
            "{:?}",
            HttpProxy::new("proxy:8080").credentials("user", "hunter2")
        );
        let socks5 = format!(
            "{:?}",
            Socks5Proxy::new("proxy:1080").credentials("user", "hunter2")
        );
        for output in [http, socks5] {
            assert!(output.contains("\"user\""), "{output}");
            assert!(!output.contains("hunter2"), "{output}");
        }
    }
}
