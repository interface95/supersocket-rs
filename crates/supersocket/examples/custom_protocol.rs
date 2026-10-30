//! A binary protocol: a 5-byte header (1-byte opcode, 2-byte body length, 2-byte sequence)
//! followed by a UTF-8 body. The server replies with the body in upper case.
//!
//! ```sh
//! cargo run --example custom_protocol
//! printf '\x01\x00\x05\x00\x01hello' | nc 127.0.0.1 4040 | xxd
//! ```

use supersocket::{
    FixedHeaderFilter, FixedHeaderProtocol,
    bytes::{Buf, BufMut, Bytes, BytesMut},
    prelude::*,
};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct Message {
    opcode: u8,
    sequence: u16,
    body: String,
}

#[derive(Default)]
struct MessageProtocol;

impl FixedHeaderProtocol for MessageProtocol {
    type Package = Message;
    const HEADER_SIZE: usize = 5;

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        Ok(u16::from_be_bytes([header[1], header[2]]) as usize)
    }

    fn decode(&mut self, mut package: Bytes) -> Result<Message, ProtocolError> {
        let opcode = package.get_u8();
        package.advance(2);
        let sequence = package.get_u16();
        let body = String::from_utf8(package.to_vec())
            .map_err(|_| ProtocolError::new("body is not UTF-8"))?;
        Ok(Message {
            opcode,
            sequence,
            body,
        })
    }
}

struct MessageEncoder;

impl PackageEncoder<Message> for MessageEncoder {
    fn encode(&self, message: &Message, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let length =
            u16::try_from(message.body.len()).map_err(|_| ProtocolError::new("body too long"))?;
        buf.put_u8(message.opcode);
        buf.put_u16(length);
        buf.put_u16(message.sequence);
        buf.put_slice(message.body.as_bytes());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    Server::builder(FixedHeaderFilter::<MessageProtocol>::default)
        .on_package(async |session, message| {
            let reply = Message {
                body: message.body.to_uppercase(),
                ..message
            };
            session.send_package(&MessageEncoder, &reply).await
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
