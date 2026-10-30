use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

/// Where to connect: a host name or IP address and a port.
///
/// The host is kept as given, so proxies receive host names and TLS checks the certificate
/// against them. An endpoint made from an IPv6 socket address keeps its scope id, which link-local
/// addresses need.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    host: String,
    port: u16,
    /// The socket address the endpoint was made from, when it holds more than the host and port
    /// say: an IPv6 scope id or flow label.
    address: Option<SocketAddr>,
}

impl Endpoint {
    /// An endpoint for `host`, which is a host name or an IP address without brackets.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            address: None,
        }
    }

    /// The host name or IP address.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The IP address, when the host is one.
    pub fn ip(&self) -> Option<IpAddr> {
        self.host.parse().ok()
    }

    /// Resolves the host to socket addresses.
    pub async fn resolve(&self) -> io::Result<Vec<SocketAddr>> {
        if let Some(address) = self.address {
            return Ok(vec![address]);
        }
        if let Some(ip) = self.ip() {
            return Ok(vec![SocketAddr::new(ip, self.port)]);
        }
        Ok(tokio::net::lookup_host((self.host.as_str(), self.port))
            .await?
            .collect())
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for Endpoint {
    type Err = io::Error;

    /// Parses `host:port`, with IPv6 addresses in brackets: `[::1]:4040`.
    fn from_str(text: &str) -> io::Result<Self> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid endpoint `{text}`, expected host:port"),
            )
        };
        let (host, port) = text.rsplit_once(':').ok_or_else(invalid)?;
        let host = match host.strip_prefix('[') {
            Some(rest) => rest.strip_suffix(']').ok_or_else(invalid)?,
            None if host.contains(':') => return Err(invalid()),
            None => host,
        };
        if host.is_empty() || host.bytes().any(|byte| byte <= b' ' || byte == 0x7F) {
            return Err(invalid());
        }
        let port = port.parse().map_err(|_| invalid())?;
        Ok(Self::new(host, port))
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(address: SocketAddr) -> Self {
        let mut endpoint = Self::new(address.ip().to_string(), address.port());
        if matches!(address, SocketAddr::V6(v6) if v6.scope_id() != 0 || v6.flowinfo() != 0) {
            endpoint.address = Some(address);
        }
        endpoint
    }
}

/// Something that names an [`Endpoint`]: a `"host:port"` string, a socket address, a
/// `(host, port)` pair or an endpoint.
pub trait ToEndpoint {
    /// Converts to an endpoint.
    fn to_endpoint(self) -> io::Result<Endpoint>;
}

impl ToEndpoint for Endpoint {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        Ok(self)
    }
}

impl ToEndpoint for &Endpoint {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        Ok(self.clone())
    }
}

impl ToEndpoint for &str {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        self.parse()
    }
}

impl ToEndpoint for String {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        self.parse()
    }
}

impl ToEndpoint for &String {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        self.parse()
    }
}

impl ToEndpoint for SocketAddr {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        Ok(self.into())
    }
}

impl ToEndpoint for (&str, u16) {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        Ok(Endpoint::new(self.0, self.1))
    }
}

impl ToEndpoint for (String, u16) {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        Ok(Endpoint::new(self.0, self.1))
    }
}

impl ToEndpoint for (IpAddr, u16) {
    fn to_endpoint(self) -> io::Result<Endpoint> {
        Ok(SocketAddr::new(self.0, self.1).into())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddrV6};

    use super::*;

    #[test]
    fn parses_and_formats_endpoints() {
        let endpoint: Endpoint = "example.com:443".parse().unwrap();
        assert_eq!((endpoint.host(), endpoint.port()), ("example.com", 443));
        assert_eq!(endpoint.ip(), None);

        let endpoint: Endpoint = "[::1]:4040".parse().unwrap();
        assert_eq!(endpoint.host(), "::1");
        assert_eq!(endpoint.to_string(), "[::1]:4040");
        assert!(endpoint.ip().is_some());

        for invalid in [
            "no-port",
            "::1:80",
            ":80",
            "host:99999",
            "[::1:80",
            "two words:80",
            "a\r\nb:80",
        ] {
            assert!(invalid.parse::<Endpoint>().is_err(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn keeps_the_scope_id_of_ipv6_addresses() {
        let link_local = Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1);
        let scoped = SocketAddr::V6(SocketAddrV6::new(link_local, 4040, 0, 7));
        let endpoint = Endpoint::from(scoped);
        assert_eq!(endpoint.host(), "fe80::1");
        assert_eq!(endpoint.resolve().await.unwrap(), [scoped]);

        // Without a scope id, nothing is lost by keeping the host and port only.
        let plain: SocketAddr = "127.0.0.1:4040".parse().unwrap();
        assert_eq!(Endpoint::from(plain), "127.0.0.1:4040".parse().unwrap());
    }
}
