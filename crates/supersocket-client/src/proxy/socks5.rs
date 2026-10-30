use std::{io, net::IpAddr};

use bytes::Bytes;
use supersocket_proto::{FixedHeaderFilter, FixedHeaderProtocol, FixedSizeFilter, ProtocolError};
use tokio::io::AsyncWriteExt;

use super::{Credentials, connect_to_proxy, finish, read_reply, read_step, refused};
use crate::{Connection, Connector, Endpoint, connector::must_be_first};

/// Connects through a SOCKS5 proxy, like SuperSocket's `Socks5Connector`, optionally with
/// username and password authentication.
#[derive(Debug, Clone)]
pub struct Socks5Proxy {
    proxy: String,
    credentials: Option<Credentials>,
}

impl Socks5Proxy {
    /// A connector for the proxy at `proxy`, written as `host:port`.
    pub fn new(proxy: impl Into<String>) -> Self {
        Self {
            proxy: proxy.into(),
            credentials: None,
        }
    }

    /// Authenticates with a username and password.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some(Credentials::new(username, password));
        self
    }
}

const NO_AUTHENTICATION: u8 = 0x00;
const USERNAME_PASSWORD: u8 = 0x02;
const NO_ACCEPTABLE_METHOD: u8 = 0xff;

fn two_bytes() -> Box<FixedSizeFilter<Bytes>> {
    Box::new(FixedSizeFilter::new(2, |reply: Bytes| Ok(reply)))
}

fn too_long(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{what} is longer than the 255 bytes SOCKS5 allows"),
    )
}

/// The reply to a connect request: version, reply code, reserved, address type, then the bound
/// address, whose length depends on its type, and the port.
#[derive(Default)]
struct ConnectReply;

impl FixedHeaderProtocol for ConnectReply {
    type Package = u8;
    // Up to and including the first byte of the address, which holds a domain's length.
    const HEADER_SIZE: usize = 5;

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        if header[0] != 5 {
            return Err(ProtocolError::new("not a SOCKS5 reply"));
        }
        match header[3] {
            1 => Ok(3 + 2),
            3 => Ok(usize::from(header[4]) + 2),
            4 => Ok(15 + 2),
            kind => Err(ProtocolError::new(format!("unknown address type {kind}"))),
        }
    }

    fn decode(&mut self, reply: Bytes) -> Result<u8, ProtocolError> {
        Ok(reply[1])
    }
}

impl Connector for Socks5Proxy {
    async fn connect(
        &self,
        target: &Endpoint,
        previous: Option<Connection>,
    ) -> io::Result<Connection> {
        must_be_first("Socks5Proxy", &previous)?;
        let mut request = vec![5, 1, 0];
        match target.ip() {
            Some(IpAddr::V4(ip)) => {
                request.push(1);
                request.extend(ip.octets());
            }
            Some(IpAddr::V6(ip)) => {
                request.push(4);
                request.extend(ip.octets());
            }
            None => {
                let host = target.host().as_bytes();
                let length = u8::try_from(host.len()).map_err(|_| too_long("the host name"))?;
                request.push(3);
                request.push(length);
                request.extend(host);
            }
        }
        request.extend(target.port().to_be_bytes());

        let mut stream = connect_to_proxy(&self.proxy).await?;
        let methods: &[u8] = match self.credentials {
            Some(_) => &[NO_AUTHENTICATION, USERNAME_PASSWORD],
            None => &[NO_AUTHENTICATION],
        };
        let mut greeting = vec![5, methods.len() as u8];
        greeting.extend(methods);
        stream.write_all(&greeting).await?;
        let choice = read_step(&mut stream, two_bytes()).await?;
        if choice[0] != 5 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a SOCKS5 proxy",
            ));
        }
        match choice[1] {
            NO_AUTHENTICATION => {}
            USERNAME_PASSWORD => {
                let Some(Credentials { username, password }) = &self.credentials else {
                    return Err(refused(
                        "the SOCKS5 proxy asked for credentials that were not offered".into(),
                    ));
                };
                let username_length =
                    u8::try_from(username.len()).map_err(|_| too_long("the username"))?;
                let password_length =
                    u8::try_from(password.len()).map_err(|_| too_long("the password"))?;
                let mut authentication = vec![1, username_length];
                authentication.extend(username.as_bytes());
                authentication.push(password_length);
                authentication.extend(password.as_bytes());
                stream.write_all(&authentication).await?;
                let status = read_step(&mut stream, two_bytes()).await?;
                if status[1] != 0 {
                    return Err(refused(
                        "the SOCKS5 proxy rejected the username or password".into(),
                    ));
                }
            }
            NO_ACCEPTABLE_METHOD => {
                return Err(refused(
                    "the SOCKS5 proxy accepts none of the offered authentication methods".into(),
                ));
            }
            method => {
                return Err(refused(format!(
                    "the SOCKS5 proxy chose an unsupported authentication method {method:#04x}"
                )));
            }
        }

        stream.write_all(&request).await?;
        let reply_filter = FixedHeaderFilter::<ConnectReply>::default();
        let (code, rest) = read_reply(&mut stream, Box::new(reply_filter)).await?;
        let reason = match code {
            0 => return Ok(finish(stream, rest)),
            1 => "general failure",
            2 => "connection not allowed by ruleset",
            3 => "network unreachable",
            4 => "host unreachable",
            5 => "connection refused",
            6 => "TTL expired",
            7 => "command not supported",
            8 => "address type not supported",
            _ => "unknown error",
        };
        Err(refused(format!(
            "the SOCKS5 proxy could not connect to {target}: {reason}"
        )))
    }
}
