//! The PROXY protocol, versions 1 and 2, with which proxies and load balancers such as HAProxy,
//! nginx and AWS Network Load Balancers pass on the address of the client they accepted.

use std::{
    io,
    net::{IpAddr, SocketAddr},
};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// What the PROXY protocol header of a connection said; see
/// [`Session::proxy_info`](crate::Session::proxy_info).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyInfo {
    version: u8,
    command: ProxyCommand,
    source: Option<SocketAddr>,
    destination: Option<SocketAddr>,
    proxy_addr: SocketAddr,
    tlvs: Vec<(u8, Bytes)>,
}

impl ProxyInfo {
    /// The protocol version: 1, which is text, or 2, which is binary.
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Whether the proxy relays a client or opened the connection itself.
    pub fn command(&self) -> ProxyCommand {
        self.command
    }

    /// The client's address, which [`Session::remote_addr`](crate::Session::remote_addr)
    /// reports. `None` when the proxy did not know it, for connections the proxy opened itself,
    /// and for addresses that are not IP addresses.
    pub fn source(&self) -> Option<SocketAddr> {
        self.source
    }

    /// The address the client connected to on the proxy.
    pub fn destination(&self) -> Option<SocketAddr> {
        self.destination
    }

    /// The address of the proxy itself: the peer of the TCP connection.
    pub fn proxy_addr(&self) -> SocketAddr {
        self.proxy_addr
    }

    /// The type-length-value fields of a version 2 header, such as type `0x05` for a unique id.
    pub fn tlvs(&self) -> impl Iterator<Item = (u8, &[u8])> {
        self.tlvs.iter().map(|(kind, value)| (*kind, &value[..]))
    }
}

/// Whether a connection carries a client's traffic or was opened by the proxy itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProxyCommand {
    /// The proxy relays a client's connection.
    Proxy,
    /// The proxy opened the connection itself, for example for a health check.
    Local,
}

const V1_PREFIX: &[u8] = b"PROXY ";
/// The longest version 1 header, line ending included.
const V1_MAX_LENGTH: usize = 107;
const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";

/// Reads the PROXY protocol header at the start of `stream`, sent by the proxy at `proxy_addr`.
/// Returns what it says and whatever was read past it.
pub(crate) async fn read_header<R: AsyncRead + Unpin>(
    stream: &mut R,
    proxy_addr: SocketAddr,
) -> io::Result<(ProxyInfo, BytesMut)> {
    let mut buf = BytesMut::with_capacity(256);
    loop {
        let parsed = parse(&buf).map_err(|why| io::Error::new(io::ErrorKind::InvalidData, why))?;
        if let Some((header, length)) = parsed {
            let _ = buf.split_to(length);
            let rest = if buf.is_empty() { BytesMut::new() } else { buf };
            let info = ProxyInfo {
                version: header.version,
                command: header.command,
                source: header.source,
                destination: header.destination,
                proxy_addr,
                tlvs: header.tlvs,
            };
            return Ok((info, rest));
        }
        if stream.read_buf(&mut buf).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the connection closed before its PROXY protocol header ended",
            ));
        }
    }
}

struct Header {
    version: u8,
    command: ProxyCommand,
    source: Option<SocketAddr>,
    destination: Option<SocketAddr>,
    tlvs: Vec<(u8, Bytes)>,
}

/// Parses the header at the start of `buf`: what it says and its length, or `None` when more
/// data is needed.
fn parse(buf: &[u8]) -> Result<Option<(Header, usize)>, &'static str> {
    if buf.starts_with(V2_SIGNATURE) {
        return parse_v2(buf);
    }
    if buf.starts_with(V1_PREFIX) {
        return parse_v1(buf);
    }
    // Wait while what arrived could still begin either header.
    if V2_SIGNATURE.starts_with(buf) || V1_PREFIX.starts_with(buf) {
        return Ok(None);
    }
    Err("the connection did not start with a PROXY protocol header")
}

/// Version 1: `PROXY TCP4 192.0.2.1 198.51.100.1 51234 443\r\n`, or `PROXY UNKNOWN ...\r\n`.
fn parse_v1(buf: &[u8]) -> Result<Option<(Header, usize)>, &'static str> {
    let end = buf
        .windows(2)
        .take(V1_MAX_LENGTH - 1)
        .position(|pair| pair == b"\r\n");
    let Some(end) = end else {
        return if buf.len() >= V1_MAX_LENGTH {
            Err("the PROXY protocol header is too long")
        } else {
            Ok(None)
        };
    };
    let line =
        std::str::from_utf8(&buf[..end]).map_err(|_| "the PROXY protocol header is not text")?;
    let mut fields = line.split(' ').skip(1);
    let (source, destination) = match fields.next() {
        // The proxy does not know the client; the rest of the line does not matter.
        Some("UNKNOWN") => (None, None),
        Some(family @ ("TCP4" | "TCP6")) => {
            let (Some(source), Some(destination), Some(source_port), Some(destination_port), None) = (
                fields.next(),
                fields.next(),
                fields.next(),
                fields.next(),
                fields.next(),
            ) else {
                return Err("malformed PROXY protocol header");
            };
            let address = |ip: &str, port: &str| -> Result<SocketAddr, &'static str> {
                let ip: IpAddr = ip
                    .parse()
                    .map_err(|_| "invalid address in the PROXY protocol header")?;
                if ip.is_ipv4() != (family == "TCP4") {
                    return Err(
                        "an address in the PROXY protocol header does not match its family",
                    );
                }
                let port = port
                    .parse()
                    .map_err(|_| "invalid port in the PROXY protocol header")?;
                Ok(SocketAddr::new(ip, port))
            };
            (
                Some(address(source, source_port)?),
                Some(address(destination, destination_port)?),
            )
        }
        _ => return Err("unknown protocol in the PROXY protocol header"),
    };
    let header = Header {
        version: 1,
        command: ProxyCommand::Proxy,
        source,
        destination,
        tlvs: Vec::new(),
    };
    Ok(Some((header, end + 2)))
}

/// Version 2: the signature, version and command, address family and protocol, the length of
/// the rest, the addresses and then type-length-value fields.
fn parse_v2(buf: &[u8]) -> Result<Option<(Header, usize)>, &'static str> {
    let Some(&[version_command, family_protocol, high, low]) = buf.get(12..16) else {
        return Ok(None);
    };
    if version_command >> 4 != 2 {
        return Err("unsupported PROXY protocol version");
    }
    let command = match version_command & 0x0F {
        0 => ProxyCommand::Local,
        1 => ProxyCommand::Proxy,
        _ => return Err("unknown command in the PROXY protocol header"),
    };
    let length = 16 + usize::from(u16::from_be_bytes([high, low]));
    let Some(body) = buf.get(16..length) else {
        return Ok(None);
    };
    // LOCAL carries no forwarded connection: consume its declared length, but ignore the
    // family and the entire protocol block, which need not contain addresses or valid TLVs.
    if command == ProxyCommand::Local {
        return Ok(Some((
            Header {
                version: 2,
                command,
                source: None,
                destination: None,
                tlvs: Vec::new(),
            },
            length,
        )));
    }
    let (addresses, tlvs) = match family_protocol >> 4 {
        0 => (None, body),
        1 => {
            let (addresses, rest) = split(body, 12)?;
            let ip = |at: usize| IpAddr::from(<[u8; 4]>::try_from(&addresses[at..at + 4]).unwrap());
            (Some(addresses_from(ip(0), ip(4), &addresses[8..])), rest)
        }
        2 => {
            let (addresses, rest) = split(body, 36)?;
            let ip =
                |at: usize| IpAddr::from(<[u8; 16]>::try_from(&addresses[at..at + 16]).unwrap());
            (Some(addresses_from(ip(0), ip(16), &addresses[32..])), rest)
        }
        // Unix socket paths do not fit in a socket address.
        3 => (None, split(body, 216)?.1),
        _ => return Err("unknown address family in the PROXY protocol header"),
    };
    // A proxy's own connection carries no client address, whatever the header holds.
    let (source, destination) = match (command, addresses) {
        (ProxyCommand::Proxy, Some((source, destination))) => (Some(source), Some(destination)),
        _ => (None, None),
    };
    let header = Header {
        version: 2,
        command,
        source,
        destination,
        tlvs: parse_tlvs(tlvs)?,
    };
    Ok(Some((header, length)))
}

fn split(body: &[u8], size: usize) -> Result<(&[u8], &[u8]), &'static str> {
    if body.len() < size {
        return Err("the PROXY protocol header is too short for its addresses");
    }
    Ok(body.split_at(size))
}

/// The source and destination addresses, with the ports that follow the IP addresses.
fn addresses_from(source: IpAddr, destination: IpAddr, ports: &[u8]) -> (SocketAddr, SocketAddr) {
    let port = |at: usize| u16::from_be_bytes([ports[at], ports[at + 1]]);
    (
        SocketAddr::new(source, port(0)),
        SocketAddr::new(destination, port(2)),
    )
}

fn parse_tlvs(mut rest: &[u8]) -> Result<Vec<(u8, Bytes)>, &'static str> {
    let mut tlvs = Vec::new();
    while let [kind, high, low, ref tail @ ..] = *rest {
        let length = usize::from(u16::from_be_bytes([high, low]));
        if tail.len() < length {
            return Err("truncated field in the PROXY protocol header");
        }
        let (value, after) = tail.split_at(length);
        tlvs.push((kind, Bytes::copy_from_slice(value)));
        rest = after;
    }
    if !rest.is_empty() {
        return Err("truncated field in the PROXY protocol header");
    }
    Ok(tlvs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_complete(buf: &[u8]) -> (Header, usize) {
        parse(buf).unwrap().expect("the header is complete")
    }

    #[test]
    fn reads_version_1_headers() {
        let (header, length) =
            parse_complete(b"PROXY TCP4 192.0.2.1 198.51.100.1 51234 443\r\nGET");
        assert_eq!(length, 45);
        assert_eq!(header.source, Some("192.0.2.1:51234".parse().unwrap()));
        assert_eq!(
            header.destination,
            Some("198.51.100.1:443".parse().unwrap())
        );

        let (header, _) = parse_complete(b"PROXY TCP6 2001:db8::1 2001:db8::2 4000 80\r\n");
        assert_eq!(header.source, Some("[2001:db8::1]:4000".parse().unwrap()));

        let (header, length) = parse_complete(b"PROXY UNKNOWN whatever follows\r\n");
        assert_eq!((header.source, length), (None, 32));
    }

    #[test]
    fn waits_for_the_whole_header() {
        for partial in [
            &b""[..],
            b"PRO",
            b"PROXY TCP4 192.0.2.1",
            b"\r\n\r\n\0\r\nQUIT\n\x21\x11\x00",
        ] {
            assert!(parse(partial).unwrap().is_none(), "{partial:?}");
        }
    }

    #[test]
    fn rejects_what_is_not_a_header() {
        for invalid in [
            &b"GET / HTTP/1.1\r\n"[..],
            b"PROXY TCP5 192.0.2.1 198.51.100.1 1 2\r\n",
            b"PROXY TCP4 2001:db8::1 198.51.100.1 1 2\r\n",
            b"PROXY TCP4 192.0.2.1 198.51.100.1 1\r\n",
            b"PROXY TCP4 192.0.2.1 198.51.100.1 1 99999\r\n",
        ] {
            assert!(
                parse(invalid).is_err(),
                "{:?}",
                String::from_utf8_lossy(invalid)
            );
        }
        let too_long = [&b"PROXY UNKNOWN "[..], &[b'x'; 100]].concat();
        assert!(parse(&too_long).is_err());
    }

    fn v2(command: u8, family: u8, body: &[u8]) -> Vec<u8> {
        let mut header = V2_SIGNATURE.to_vec();
        header.extend([0x20 | command, (family << 4) | 1]);
        header.extend((body.len() as u16).to_be_bytes());
        header.extend(body);
        header
    }

    #[test]
    fn reads_version_2_headers() {
        let mut body = vec![192, 0, 2, 1, 198, 51, 100, 1];
        body.extend(51234u16.to_be_bytes());
        body.extend(443u16.to_be_bytes());
        // A unique id field.
        body.extend([0x05, 0, 3, b'a', b'b', b'c']);
        let mut data = v2(1, 1, &body);
        let header_length = data.len();
        data.extend(b"payload");
        let (header, length) = parse_complete(&data);
        assert_eq!(length, header_length);
        assert_eq!(header.command, ProxyCommand::Proxy);
        assert_eq!(header.source, Some("192.0.2.1:51234".parse().unwrap()));
        assert_eq!(
            header.destination,
            Some("198.51.100.1:443".parse().unwrap())
        );
        assert_eq!(header.tlvs, [(0x05, Bytes::from_static(b"abc"))]);

        let mut body = [0u8; 36];
        body[15] = 1;
        body[31] = 2;
        body[32..34].copy_from_slice(&4000u16.to_be_bytes());
        let (header, _) = parse_complete(&v2(1, 2, &body));
        assert_eq!(header.source, Some("[::1]:4000".parse().unwrap()));
        assert_eq!(
            header.destination,
            Some(SocketAddr::new("::2".parse().unwrap(), 0))
        );
    }

    #[test]
    fn local_connections_carry_no_client_address() {
        let body = [192, 0, 2, 1, 198, 51, 100, 1, 0, 1, 0, 2];
        let (header, _) = parse_complete(&v2(0, 1, &body));
        assert_eq!(header.command, ProxyCommand::Local);
        assert_eq!(header.source, None);
        let (header, length) = parse_complete(&v2(0, 0, &[]));
        assert_eq!((header.source, length), (None, 16));
    }

    #[test]
    fn local_connections_ignore_address_family_and_payload() {
        for family_protocol in [0x00, 0x11, 0x21, 0x31, 0xff] {
            for body in [&[][..], &[0xaa][..], &[0x05, 0, 9, b'x'][..]] {
                let mut data = V2_SIGNATURE.to_vec();
                data.extend([0x20, family_protocol]);
                data.extend((body.len() as u16).to_be_bytes());
                data.extend(body);
                let header_length = data.len();
                assert!(parse(&data[..header_length - 1]).unwrap().is_none());
                data.extend(b"application data");

                let (header, length) = parse_complete(&data);
                assert_eq!(length, header_length);
                assert_eq!(header.command, ProxyCommand::Local);
                assert_eq!((header.source, header.destination), (None, None));
                assert!(header.tlvs.is_empty());
            }
        }
    }

    #[test]
    fn rejects_broken_version_2_headers() {
        let mut wrong_version = v2(1, 0, &[]);
        wrong_version[12] = 0x11;
        assert!(parse(&wrong_version).is_err());
        assert!(parse(&v2(2, 0, &[])).is_err());
        assert!(parse(&v2(1, 1, &[0; 4])).is_err());
        assert!(parse(&v2(1, 0, &[0x05, 0, 9, b'x'])).is_err());
    }

    #[tokio::test]
    async fn keeps_what_follows_the_header() {
        let mut data: &[u8] = b"PROXY TCP4 192.0.2.1 198.51.100.1 51234 443\r\nhello";
        let proxy = "10.0.0.1:9000".parse().unwrap();
        let (info, rest) = read_header(&mut data, proxy).await.unwrap();
        assert_eq!(info.proxy_addr(), proxy);
        assert_eq!(info.version(), 1);
        assert_eq!(&rest[..], b"hello");

        let mut nothing: &[u8] = b"PROXY TCP4 192.0.2.1";
        let error = read_header(&mut nothing, proxy).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }
}
