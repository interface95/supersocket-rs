//! A protocol chosen per connection, like SuperSocket's `SwitchPipelineFilter` sample: the first
//! line names it, and the pipeline filter switches to that protocol for the rest of the
//! connection. `TEXT` goes on with lines, `BINARY` with packages that start with a 2-byte length.
//!
//! ```sh
//! cargo run --example switch_protocol
//! printf 'TEXT\r\nhello\r\nworld\r\n' | nc 127.0.0.1 4040
//! printf 'BINARY\r\n\x00\x05hello' | nc 127.0.0.1 4040 | xxd
//! ```

use supersocket::{
    BoxFilter, FixedHeaderFilter, FixedHeaderProtocol, TerminatorFilter,
    bytes::{BufMut, Bytes, BytesMut},
    prelude::*,
};
use tracing_subscriber::EnvFilter;

/// What the filters of a connection produce, whichever protocol it speaks.
enum Package {
    /// The first line: the protocol the rest of the connection speaks.
    Hello(String),
    Text(String),
    Binary(Bytes),
}

/// Reads the first line, then switches to the filter of the protocol it names.
#[derive(Default)]
struct ChooseProtocol {
    next: Option<BoxFilter<Package>>,
}

impl PipelineFilter for ChooseProtocol {
    type Package = Package;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<Package>, ProtocolError> {
        let Some(end) = buf.windows(2).position(|pair| pair == b"\r\n") else {
            return Ok(None);
        };
        let line = buf.split_to(end + 2);
        let name = std::str::from_utf8(&line[..end])
            .map_err(|_| ProtocolError::new("the protocol name is not UTF-8"))?
            .to_owned();
        self.next = Some(match name.as_str() {
            "TEXT" => Box::new(TerminatorFilter::new(&b"\r\n"[..], |line| {
                TextPackage::decode(line).map(|line| Package::Text(line.text))
            })),
            "BINARY" => Box::new(FixedHeaderFilter::new(LengthPrefixed)),
            _ => return Err(ProtocolError::new(format!("unknown protocol {name}"))),
        });
        Ok(Some(Package::Hello(name)))
    }

    // Called after every `filter` call. Once the protocol is known, the connection switches, and
    // the bytes that came after the first line go to the new filter.
    fn next_filter(&mut self) -> Option<BoxFilter<Package>> {
        self.next.take()
    }
}

/// A 2-byte big-endian length followed by the body.
struct LengthPrefixed;

impl FixedHeaderProtocol for LengthPrefixed {
    type Package = Package;
    const HEADER_SIZE: usize = 2;

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        Ok(usize::from(u16::from_be_bytes([header[0], header[1]])))
    }

    fn decode(&mut self, package: Bytes) -> Result<Package, ProtocolError> {
        Ok(Package::Binary(package.slice(Self::HEADER_SIZE..)))
    }
}

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    Server::builder(ChooseProtocol::default)
        .on_package(async |session, package| {
            match package {
                Package::Hello(name) => session.send(format!("OK {name}\r\n")).await?,
                Package::Text(text) => {
                    session.send(format!("{}\r\n", text.to_uppercase())).await?;
                }
                Package::Binary(body) => {
                    // Answered in kind: the body reversed, with its length in front.
                    session
                        .send_with(|buf| {
                            buf.put_u16(body.len() as u16);
                            buf.extend(body.iter().rev());
                            Ok::<_, std::io::Error>(())
                        })
                        .await?;
                }
            }
            Ok(())
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
