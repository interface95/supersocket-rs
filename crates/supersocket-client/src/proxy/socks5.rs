use std::{
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

use bytes::{Bytes, BytesMut};
use supersocket_proto::{
    BoxFilter, FixedHeaderFilter, FixedHeaderProtocol, PipelineFilter, ProtocolError,
};

use super::{Credentials, ProxyConnection, ProxyConnector, refused};
use crate::{Connection, Endpoint, ToEndpoint};

/// Connects through a SOCKS5 proxy (RFC 1928), like SuperSocket's `Socks5Connector`, optionally
/// with username and password authentication (RFC 1929).
#[derive(Debug, Clone)]
pub struct Socks5Connector {
    proxy: Endpoint,
    credentials: Option<Credentials>,
}

const VERSION: u8 = 5;
const NO_AUTHENTICATION: u8 = 0x00;
const USERNAME_PASSWORD: u8 = 0x02;
const NO_ACCEPTABLE_METHOD: u8 = 0xff;

/// The greeting without credentials: version 5, one method, no authentication.
const HANDSHAKE: [u8; 3] = [VERSION, 1, NO_AUTHENTICATION];
/// The greeting with credentials, which also offers username and password authentication.
const AUTHENTICATION_HANDSHAKE: [u8; 4] = [VERSION, 2, NO_AUTHENTICATION, USERNAME_PASSWORD];

impl Socks5Connector {
    /// A connector for the proxy at `proxy`, such as `"proxy.example:1080"`.
    pub fn new(proxy: impl ToEndpoint) -> io::Result<Self> {
        Ok(Self {
            proxy: proxy.to_endpoint()?,
            credentials: None,
        })
    }

    /// Authenticates with a username and password.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some(Credentials::new(username, password));
        self
    }

    /// The greeting, offering the authentication methods this connector can do.
    fn handshake_request(&self) -> &'static [u8] {
        match self.credentials {
            Some(_) => &AUTHENTICATION_HANDSHAKE,
            None => &HANDSHAKE,
        }
    }

    /// Checks a reply of the handshake, according to the step that produced it.
    fn handle_response(&self, response: &Socks5Pack, target: &Endpoint) -> io::Result<()> {
        match response.step {
            Step::Handshake => {
                if response.version != VERSION {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "not a SOCKS5 proxy",
                    ));
                }
                match response.status {
                    NO_AUTHENTICATION => Ok(()),
                    USERNAME_PASSWORD if self.credentials.is_some() => Ok(()),
                    USERNAME_PASSWORD => Err(refused(
                        "the SOCKS5 proxy asked for credentials that were not offered".into(),
                    )),
                    NO_ACCEPTABLE_METHOD => Err(refused(
                        "the SOCKS5 proxy accepts none of the offered authentication methods"
                            .into(),
                    )),
                    method => Err(refused(format!(
                        "the SOCKS5 proxy chose an unsupported authentication method {method:#04x}"
                    ))),
                }
            }
            Step::Authentication => match response.status {
                0 => Ok(()),
                _ => Err(refused(
                    "the SOCKS5 proxy rejected the username or password".into(),
                )),
            },
            Step::Connect => {
                let reason = match response.status {
                    0 => return Ok(()),
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
    }
}

impl ProxyConnector for Socks5Connector {
    fn proxy_endpoint(&self) -> &Endpoint {
        &self.proxy
    }

    fn validate_target(&self, target: &Endpoint) -> io::Result<()> {
        endpoint_request(target).map(drop)
    }

    async fn connect_proxy(
        &self,
        target: &Endpoint,
        connection: Connection,
    ) -> io::Result<Connection> {
        let endpoint_request = endpoint_request(target)?;
        let mut connection =
            ProxyConnection::new(connection, Socks5AuthFilter::new(Step::Handshake));

        // The proxy must wait for the next request after the greeting and the authentication
        // replies, so anything it sends past them is a violation.
        connection.send(self.handshake_request()).await?;
        let response = connection.receive_step().await?;
        self.handle_response(&response, target)?;

        if let (USERNAME_PASSWORD, Some(credentials)) = (response.status, &self.credentials) {
            connection.send(&password_request(credentials)?).await?;
            let response = connection.receive_step().await?;
            self.handle_response(&response, target)?;
        }

        connection.send(&endpoint_request).await?;
        let response = connection.receive().await?;
        self.handle_response(&response, target)?;
        if let Some((address, port)) = &response.bound {
            tracing::debug!(
                proxy = %self.proxy,
                %target,
                bound = %address,
                port,
                "connected through the SOCKS5 proxy"
            );
        }
        Ok(connection.detach())
    }
}

/// The steps of the handshake, each with its reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// The greeting, answered with the chosen authentication method.
    Handshake,
    /// Username and password authentication, answered with its status.
    Authentication,
    /// The connect request, answered with a status and the address the proxy connected from.
    Connect,
}

/// The username and password request (RFC 1929).
fn password_request(Credentials { username, password }: &Credentials) -> io::Result<Vec<u8>> {
    let username_length = u8::try_from(username.len()).map_err(|_| too_long("the username"))?;
    let password_length = u8::try_from(password.len()).map_err(|_| too_long("the password"))?;
    let mut request = vec![1, username_length];
    request.extend(username.as_bytes());
    request.push(password_length);
    request.extend(password.as_bytes());
    Ok(request)
}

/// The connect request for `target`.
fn endpoint_request(target: &Endpoint) -> io::Result<Vec<u8>> {
    let mut request = vec![VERSION, 1, 0];
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
    Ok(request)
}

fn too_long(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{what} is longer than the 255 bytes SOCKS5 allows"),
    )
}

/// A reply from the proxy, like SuperSocket's `Socks5Pack`, stamped by the filter with the step
/// it belongs to, so the connector cannot read it as the reply to another step.
#[derive(Debug)]
struct Socks5Pack {
    step: Step,
    version: u8,
    status: u8,
    /// The address and port the proxy connected from; only in the reply to the connect request.
    bound: Option<(Socks5Address, u16)>,
}

/// An address in a reply, like SuperSocket's `Socks5Address`.
#[derive(Debug, PartialEq, Eq)]
enum Socks5Address {
    Ip(IpAddr),
    Domain(String),
}

impl fmt::Display for Socks5Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => ip.fmt(f),
            Self::Domain(domain) => f.write_str(domain),
        }
    }
}

/// Cuts the two-byte replies to the greeting and to the authentication, like SuperSocket's
/// `Socks5AuthPipelineFilter`, then switches to the filter of the next reply: another one of its
/// own if the proxy chose username and password authentication, otherwise
/// [`Socks5AddressFilter`].
struct Socks5AuthFilter {
    step: Step,
    next: Option<BoxFilter<Socks5Pack>>,
}

impl Socks5AuthFilter {
    fn new(step: Step) -> Self {
        Self { step, next: None }
    }
}

impl PipelineFilter for Socks5AuthFilter {
    type Package = Socks5Pack;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<Socks5Pack>, ProtocolError> {
        if buf.len() < 2 {
            return Ok(None);
        }
        let reply = buf.split_to(2);
        let pack = Socks5Pack {
            step: self.step,
            version: reply[0],
            status: reply[1],
            bound: None,
        };
        self.next = Some(
            if self.step == Step::Handshake && pack.status == USERNAME_PASSWORD {
                Box::new(Socks5AuthFilter::new(Step::Authentication))
            } else {
                Box::new(Socks5AddressFilter::default())
            },
        );
        Ok(Some(pack))
    }

    fn next_filter(&mut self) -> Option<BoxFilter<Socks5Pack>> {
        self.next.take()
    }
}

/// Cuts the reply to the connect request, like SuperSocket's `Socks5AddressPipelineFilter`.
type Socks5AddressFilter = FixedHeaderFilter<ConnectReply>;

/// The reply to the connect request: version, status, reserved, address type, then the address,
/// whose length depends on its type, and the port.
#[derive(Default)]
struct ConnectReply;

impl FixedHeaderProtocol for ConnectReply {
    type Package = Socks5Pack;
    // Up to and including the first byte of the address, which holds a domain's length.
    const HEADER_SIZE: usize = 5;

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        if header[0] != VERSION {
            return Err(ProtocolError::new("not a SOCKS5 reply"));
        }
        match header[3] {
            1 => Ok(3 + 2),
            3 => Ok(usize::from(header[4]) + 2),
            4 => Ok(15 + 2),
            kind => Err(ProtocolError::new(format!("unknown address type {kind}"))),
        }
    }

    fn decode(&mut self, reply: Bytes) -> Result<Socks5Pack, ProtocolError> {
        let (address, port) = reply[4..].split_at(reply.len() - 6);
        let address = match reply[3] {
            1 => Socks5Address::Ip(Ipv4Addr::from(<[u8; 4]>::try_from(address).unwrap()).into()),
            4 => Socks5Address::Ip(Ipv6Addr::from(<[u8; 16]>::try_from(address).unwrap()).into()),
            _ => Socks5Address::Domain(String::from_utf8_lossy(&address[1..]).into_owned()),
        };
        Ok(Socks5Pack {
            step: Step::Connect,
            version: reply[0],
            status: reply[1],
            bound: Some((address, u16::from_be_bytes([port[0], port[1]]))),
        })
    }
}

#[cfg(test)]
mod tests {
    use supersocket_connection::{PackageReader, ReadOptions};

    use super::*;
    use crate::proxy::in_memory;

    async fn read_all(filter: Socks5AuthFilter, bytes: &[u8]) -> Vec<Socks5Pack> {
        let mut reader = PackageReader::new(bytes, Box::new(filter), ReadOptions::default());
        let mut replies = Vec::new();
        while let Some(reply) = reader.next().await.unwrap() {
            replies.push(reply);
        }
        replies
    }

    #[tokio::test]
    async fn filters_switch_with_the_chosen_method() {
        // Username and password: method, authentication status, then the connect reply.
        let replies = read_all(
            Socks5AuthFilter::new(Step::Handshake),
            &[5, 2, 1, 0, 5, 0, 0, 1, 10, 0, 0, 1, 0x1f, 0x90],
        )
        .await;
        let statuses: Vec<_> = replies.iter().map(|reply| reply.status).collect();
        assert_eq!(statuses, [2, 0, 0]);
        let steps: Vec<_> = replies.iter().map(|reply| reply.step).collect();
        assert_eq!(
            steps,
            [Step::Handshake, Step::Authentication, Step::Connect]
        );
        let ip = Socks5Address::Ip(Ipv4Addr::new(10, 0, 0, 1).into());
        assert_eq!(replies[2].bound, Some((ip, 8080)));

        // No authentication, although the client offered credentials: straight to the connect
        // reply, here with a domain.
        let mut bytes = vec![5, 0, 5, 0, 0, 3, 9];
        bytes.extend(b"localhost");
        bytes.extend(80u16.to_be_bytes());
        let replies = read_all(Socks5AuthFilter::new(Step::Handshake), &bytes).await;
        assert_eq!(replies.len(), 2);
        let domain = Socks5Address::Domain("localhost".into());
        assert_eq!(replies[1].bound, Some((domain, 80)));
    }

    #[tokio::test]
    async fn data_past_the_greeting_reply_is_a_violation() {
        // A well-formed connect reply sent before the request must not count as success.
        let (connection, _proxy) = in_memory(&[5, 0, 5, 0, 0, 1, 127, 0, 0, 1, 0, 80]).await;
        let error = Socks5Connector::new("127.0.0.1:1")
            .unwrap()
            .connect_proxy(&Endpoint::new("example.com", 80), connection)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    }

    #[tokio::test]
    async fn rejects_credentials_it_did_not_offer() {
        let (connection, _proxy) = in_memory(&[5, 2]).await;
        let error = Socks5Connector::new("127.0.0.1:1")
            .unwrap()
            .connect_proxy(&Endpoint::new("example.com", 80), connection)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    }
}
