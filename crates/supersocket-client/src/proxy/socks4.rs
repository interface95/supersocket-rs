use std::{io, net::IpAddr};

use bytes::Bytes;
use supersocket_proto::FixedSizeFilter;

use super::{ProxyConnection, ProxyConnector, refused};
use crate::{Connection, Endpoint, ToEndpoint};

/// Connects through a SOCKS4 proxy. Host names are sent to the proxy with SOCKS4a; IPv6 targets
/// are not supported by the protocol. SuperSocket declares a `Socks4Connector` but leaves it
/// unimplemented; this one follows its other proxy connectors.
#[derive(Debug, Clone)]
pub struct Socks4Connector {
    proxy: Endpoint,
    user_id: String,
}

impl Socks4Connector {
    /// A connector for the proxy at `proxy`, such as `"proxy.example:1080"`.
    pub fn new(proxy: impl ToEndpoint) -> io::Result<Self> {
        Ok(Self {
            proxy: proxy.to_endpoint()?,
            user_id: String::new(),
        })
    }

    /// The user id sent to the proxy. Default: empty. Connecting rejects embedded NUL bytes.
    pub fn user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }

    /// The connect request for `target`.
    fn request(&self, target: &Endpoint) -> io::Result<Vec<u8>> {
        if self.user_id.contains('\0') || target.host().contains('\0') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SOCKS4 user IDs and host names must not contain NUL",
            ));
        }
        let mut request = vec![4, 1];
        request.extend(target.port().to_be_bytes());
        match target.ip() {
            Some(IpAddr::V4(ip)) => {
                request.extend(ip.octets());
                request.extend(self.user_id.as_bytes());
                request.push(0);
            }
            Some(IpAddr::V6(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SOCKS4 does not support IPv6 targets",
                ));
            }
            None => {
                // SOCKS4a: an invalid address 0.0.0.x tells the proxy to resolve the host name.
                request.extend([0, 0, 0, 1]);
                request.extend(self.user_id.as_bytes());
                request.push(0);
                request.extend(target.host().as_bytes());
                request.push(0);
            }
        }
        Ok(request)
    }
}

impl ProxyConnector for Socks4Connector {
    fn proxy_endpoint(&self) -> &Endpoint {
        &self.proxy
    }

    async fn connect_proxy(
        &self,
        target: &Endpoint,
        connection: Connection,
    ) -> io::Result<Connection> {
        let request = self.request(target)?;
        // The reply is 8 bytes: version, status, then a port and an address to ignore.
        let reply_filter = FixedSizeFilter::new(8, |reply: Bytes| Ok(reply[1]));
        let mut connection = ProxyConnection::new(connection, reply_filter);
        connection.send(&request).await?;
        handle_response(connection.receive().await?)?;
        Ok(connection.detach())
    }
}

/// Checks the status of the reply.
fn handle_response(status: u8) -> io::Result<()> {
    match status {
        0x5a => Ok(()),
        0x5b => Err(refused("the SOCKS4 proxy rejected the request".into())),
        0x5c | 0x5d => Err(refused(
            "the SOCKS4 proxy could not verify the user id".into(),
        )),
        code => Err(refused(format!(
            "the SOCKS4 proxy replied with unknown code {code:#04x}"
        ))),
    }
}
