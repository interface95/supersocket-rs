use std::{io, net::IpAddr};

use bytes::Bytes;
use supersocket_proto::FixedSizeFilter;
use tokio::io::AsyncWriteExt;

use super::{connect_to_proxy, finish, read_reply, refused};
use crate::{Connection, Connector, Endpoint, connector::must_be_first};

/// Connects through a SOCKS4 proxy, like SuperSocket's `Socks4Connector`. Host names are sent
/// to the proxy with SOCKS4a; IPv6 targets are not supported by the protocol.
#[derive(Debug, Clone)]
pub struct Socks4Proxy {
    proxy: String,
    user_id: String,
}

impl Socks4Proxy {
    /// A connector for the proxy at `proxy`, written as `host:port`.
    pub fn new(proxy: impl Into<String>) -> Self {
        Self {
            proxy: proxy.into(),
            user_id: String::new(),
        }
    }

    /// The user id sent to the proxy. Default: empty.
    pub fn user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }
}

impl Connector for Socks4Proxy {
    async fn connect(
        &self,
        target: &Endpoint,
        previous: Option<Connection>,
    ) -> io::Result<Connection> {
        must_be_first("Socks4Proxy", &previous)?;
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

        let mut stream = connect_to_proxy(&self.proxy).await?;
        stream.write_all(&request).await?;
        let reply_filter = FixedSizeFilter::new(8, |reply: Bytes| Ok(reply));
        let (reply, rest) = read_reply(&mut stream, Box::new(reply_filter)).await?;
        match reply[1] {
            0x5a => Ok(finish(stream, rest)),
            0x5b => Err(refused("the SOCKS4 proxy rejected the request".into())),
            0x5c | 0x5d => Err(refused(
                "the SOCKS4 proxy could not verify the user id".into(),
            )),
            code => Err(refused(format!(
                "the SOCKS4 proxy replied with unknown code {code:#04x}"
            ))),
        }
    }
}
