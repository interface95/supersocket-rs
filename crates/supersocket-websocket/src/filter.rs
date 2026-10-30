//! The pipeline filters of a WebSocket connection: [`HeadFilter`] reads the HTTP head that opens
//! it, then switches to [`FrameFilter`], like SuperSocket's WebSocket pipeline filters.

use std::fmt;

use bytes::{Buf, Bytes, BytesMut};
use supersocket_proto::{BoxFilter, PipelineFilter, ProtocolError};

use crate::{
    CloseCode, CloseFrame, Message,
    frame::{
        MAX_CONTROL_PAYLOAD, OP_BINARY, OP_CLOSE, OP_CONTINUATION, OP_PING, OP_PONG, OP_TEXT,
        apply_mask,
    },
    handshake::{Head, HeadError, MAX_HEAD_SIZE, head_length, parse_head},
};

/// A package of a WebSocket connection, produced by its pipeline filters. Its contents are
/// private: the server and the client turn packages into messages.
pub struct WebSocketPackage(pub(crate) Package);

impl fmt::Debug for WebSocketPackage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketPackage").finish_non_exhaustive()
    }
}

pub(crate) enum Package {
    /// The HTTP head that opens the connection: the client's request or the server's response.
    Handshake(Result<Head, HeadError>),
    Message(Message),
    Ping(Bytes),
    Pong,
    /// The peer started or answered the closing handshake.
    Close(Option<CloseFrame>),
    /// The peer broke the protocol, so the connection must be closed with this code.
    Violation(CloseCode, &'static str),
}

/// Which end of the connection reads the frames: clients mask their frames and servers do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    Server,
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    Client,
}

/// Reads the HTTP head that opens a connection, then switches to a [`FrameFilter`].
pub(crate) struct HeadFilter {
    role: Role,
    max_size: usize,
    searched: usize,
    next: Option<FrameFilter>,
    failed: bool,
}

impl HeadFilter {
    /// A filter for `role` whose frames and messages may be at most `max_size` bytes.
    pub(crate) fn new(role: Role, max_size: usize) -> Self {
        Self {
            role,
            max_size,
            searched: 0,
            next: None,
            failed: false,
        }
    }
}

impl PipelineFilter for HeadFilter {
    type Package = WebSocketPackage;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<WebSocketPackage>, ProtocolError> {
        if self.failed {
            buf.clear();
            return Ok(None);
        }
        let head = match head_length(buf, self.searched) {
            Some(length) => {
                self.searched = 0;
                parse_head(&buf.split_to(length))
            }
            // A larger head would fail the read buffer's limit without an HTTP answer.
            None if buf.len() > MAX_HEAD_SIZE.min(self.max_size) => Err(HeadError::TooLarge),
            None => {
                self.searched = buf.len();
                return Ok(None);
            }
        };
        match head {
            Ok(_) => self.next = Some(FrameFilter::new(self.role, self.max_size)),
            // The connection is about to be closed; ignore whatever follows.
            Err(_) => self.failed = true,
        }
        Ok(Some(WebSocketPackage(Package::Handshake(head))))
    }

    fn next_filter(&mut self) -> Option<BoxFilter<WebSocketPackage>> {
        self.next
            .take()
            .map(|filter| Box::new(filter) as BoxFilter<WebSocketPackage>)
    }

    fn reset(&mut self) {
        self.searched = 0;
    }
}

type Violation = (CloseCode, &'static str);

const INVALID_TEXT: Violation = (CloseCode::INVALID_PAYLOAD, "text is not valid UTF-8");

/// Cuts frames, unmasks them and joins fragments into messages. Breaking the protocol produces
/// a violation package, after which the rest of the input is ignored, as it is after a close frame.
pub(crate) struct FrameFilter {
    role: Role,
    max_size: usize,
    /// The fragmented message being received.
    partial: Option<Partial>,
    done: bool,
}

struct Partial {
    opcode: u8,
    payload: BytesMut,
    /// How many bytes of a text message are known to be valid UTF-8.
    valid: usize,
}

impl FrameFilter {
    pub(crate) fn new(role: Role, max_size: usize) -> Self {
        Self {
            role,
            max_size,
            partial: None,
            done: false,
        }
    }

    fn check(&self, header: &Header) -> Result<(), Violation> {
        let protocol_error = |why| Err((CloseCode::PROTOCOL_ERROR, why));
        if header.rsv != 0 {
            return protocol_error("reserved bits are set");
        }
        match header.opcode {
            OP_CONTINUATION if self.partial.is_none() => {
                return protocol_error("continuation frame without a message to continue");
            }
            OP_TEXT | OP_BINARY if self.partial.is_some() => {
                return protocol_error("new message before the previous one ended");
            }
            OP_CONTINUATION | OP_TEXT | OP_BINARY | OP_CLOSE | OP_PING | OP_PONG => {}
            _ => return protocol_error("reserved opcode"),
        }
        match (self.role, header.mask.is_some()) {
            (Role::Server, false) => return protocol_error("client frames must be masked"),
            (Role::Client, true) => return protocol_error("server frames must not be masked"),
            _ => {}
        }
        if header.opcode & 0x8 != 0 {
            if !header.fin {
                return protocol_error("fragmented control frame");
            }
            if header.length > MAX_CONTROL_PAYLOAD as u64 {
                return protocol_error("control frame payload longer than 125 bytes");
            }
            return Ok(());
        }
        // The whole frame must fit in the read buffer, and the whole message in the limit.
        let received = self
            .partial
            .as_ref()
            .map_or(0, |partial| partial.payload.len());
        let room =
            (self.max_size.saturating_sub(header.size)).min(self.max_size.saturating_sub(received));
        if header.length > room as u64 {
            return Err((CloseCode::MESSAGE_TOO_BIG, "message too big"));
        }
        Ok(())
    }

    fn frame(
        &mut self,
        fin: bool,
        opcode: u8,
        payload: BytesMut,
    ) -> Result<Option<Package>, Violation> {
        let package = match opcode {
            OP_PING => Package::Ping(payload.freeze()),
            OP_PONG => Package::Pong,
            OP_CLOSE => {
                let frame = parse_close(&payload)?;
                self.done = true;
                Package::Close(frame)
            }
            OP_TEXT | OP_BINARY if fin => Package::Message(message(opcode, payload)?),
            OP_TEXT | OP_BINARY => {
                let mut partial = Partial {
                    opcode,
                    payload,
                    valid: 0,
                };
                partial.check_text()?;
                self.partial = Some(partial);
                return Ok(None);
            }
            _ => {
                let partial = self.partial.as_mut().expect("checked by `check`");
                partial.payload.extend_from_slice(&payload);
                partial.check_text()?;
                if !fin {
                    return Ok(None);
                }
                let partial = self.partial.take().expect("checked by `check`");
                Package::Message(message(partial.opcode, partial.payload)?)
            }
        };
        Ok(Some(package))
    }

    fn fail(&mut self, (code, why): Violation) -> WebSocketPackage {
        self.done = true;
        self.partial = None;
        WebSocketPackage(Package::Violation(code, why))
    }
}

impl PipelineFilter for FrameFilter {
    type Package = WebSocketPackage;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<WebSocketPackage>, ProtocolError> {
        // Fragments that do not complete a message produce nothing, so keep going until a frame
        // does or more data is needed.
        while !self.done {
            let Some(header) = Header::parse(buf) else {
                return Ok(None);
            };
            if let Err(violation) = self.check(&header) {
                return Ok(Some(self.fail(violation)));
            }
            // `check` bounded the length by the maximum size.
            let length = header.length as usize;
            if buf.len() < header.size + length {
                return Ok(None);
            }
            buf.advance(header.size);
            let mut payload = buf.split_to(length);
            if let Some(mask) = header.mask {
                apply_mask(&mut payload, mask);
            }
            match self.frame(header.fin, header.opcode, payload) {
                Ok(Some(package)) => return Ok(Some(WebSocketPackage(package))),
                Ok(None) => {}
                Err(violation) => return Ok(Some(self.fail(violation))),
            }
        }
        buf.clear();
        Ok(None)
    }
}

impl Partial {
    /// Checks the text received so far, so that invalid UTF-8 fails as soon as it arrives rather
    /// than when the message ends.
    fn check_text(&mut self) -> Result<(), Violation> {
        if self.opcode != OP_TEXT {
            return Ok(());
        }
        match std::str::from_utf8(&self.payload[self.valid..]) {
            Ok(_) => self.valid = self.payload.len(),
            // A character cut off at the end may be completed by the next fragment.
            Err(error) if error.error_len().is_none() => self.valid += error.valid_up_to(),
            Err(_) => return Err(INVALID_TEXT),
        }
        Ok(())
    }
}

fn message(opcode: u8, payload: BytesMut) -> Result<Message, Violation> {
    if opcode == OP_BINARY {
        return Ok(Message::Binary(payload.freeze()));
    }
    // A message joined from fragments owns its buffer, which then becomes the string's.
    String::from_utf8(Vec::from(payload))
        .map(Message::Text)
        .map_err(|_| INVALID_TEXT)
}

fn parse_close(payload: &[u8]) -> Result<Option<CloseFrame>, Violation> {
    match *payload {
        [] => Ok(None),
        [_] => Err((
            CloseCode::PROTOCOL_ERROR,
            "close frame with a one-byte payload",
        )),
        [high, low, ref reason @ ..] => {
            let code = CloseCode::new(u16::from_be_bytes([high, low]));
            if !code.is_allowed() {
                return Err((CloseCode::PROTOCOL_ERROR, "invalid close code"));
            }
            let reason = std::str::from_utf8(reason).map_err(|_| {
                (
                    CloseCode::INVALID_PAYLOAD,
                    "close reason is not valid UTF-8",
                )
            })?;
            Ok(Some(CloseFrame {
                code,
                reason: reason.to_owned(),
            }))
        }
    }
}

struct Header {
    fin: bool,
    rsv: u8,
    opcode: u8,
    mask: Option<[u8; 4]>,
    length: u64,
    /// Length of the header itself.
    size: usize,
}

impl Header {
    /// Parses the frame header at the start of `buf`, or returns `None` when it is incomplete.
    fn parse(buf: &[u8]) -> Option<Self> {
        let [first, second, ..] = *buf else {
            return None;
        };
        let (length, mut size) = match second & 0x7F {
            126 => (
                u64::from(u16::from_be_bytes(buf.get(2..4)?.try_into().ok()?)),
                4,
            ),
            127 => (u64::from_be_bytes(buf.get(2..10)?.try_into().ok()?), 10),
            length => (u64::from(length), 2),
        };
        let mask = if second & 0x80 != 0 {
            let key = buf.get(size..size + 4)?.try_into().ok()?;
            size += 4;
            Some(key)
        } else {
            None
        };
        Some(Self {
            fin: first & 0x80 != 0,
            rsv: first & 0x70,
            opcode: first & 0x0F,
            mask,
            length,
            size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{MAX_HEADER_SIZE, encode_header};

    const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];

    /// A frame as a client sends it: masked.
    fn client_frame(first: u8, payload: &[u8]) -> Vec<u8> {
        let mut header = [0; MAX_HEADER_SIZE];
        let size = encode_header(first & 0x0F, payload.len(), Some(MASK), &mut header);
        header[0] = first;
        let mut frame = header[..size].to_vec();
        let start = frame.len();
        frame.extend_from_slice(payload);
        apply_mask(&mut frame[start..], MASK);
        frame
    }

    /// Runs `frames` through a server's frame filter and returns the packages it produced.
    fn read(frames: &[Vec<u8>], max_size: usize) -> Vec<Package> {
        let mut filter = FrameFilter::new(Role::Server, max_size);
        let mut buf = BytesMut::new();
        let mut packages = Vec::new();
        for frame in frames {
            buf.extend_from_slice(frame);
            while let Some(package) = filter.filter(&mut buf).unwrap() {
                packages.push(package.0);
            }
        }
        packages
    }

    fn violation(frames: &[Vec<u8>]) -> CloseCode {
        match read(frames, 1024).pop() {
            Some(Package::Violation(code, _)) => code,
            _ => panic!("no violation"),
        }
    }

    fn text(package: &Package) -> &str {
        match package {
            Package::Message(Message::Text(text)) => text,
            _ => panic!("not a text message"),
        }
    }

    #[test]
    fn unmasks_messages_and_waits_for_whole_frames() {
        let frame = client_frame(0x81, b"Hello");
        let mut filter = FrameFilter::new(Role::Server, 1024);
        let mut buf = BytesMut::from(&frame[..4]);
        assert!(filter.filter(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&frame[4..]);
        let package = filter.filter(&mut buf).unwrap().unwrap();
        assert_eq!(text(&package.0), "Hello");
        assert!(buf.is_empty());
    }

    #[test]
    fn joins_fragments_around_control_frames() {
        // "Hello" with "é" split between two fragments, and a ping in between.
        let packages = read(
            &[
                client_frame(0x01, b"He"),
                client_frame(0x00, &[b'l', b'l', b'o', 0xC3]),
                client_frame(0x89, b"ping"),
                client_frame(0x80, &[0xA9]),
                client_frame(0x82, &[1, 2, 3]),
            ],
            1024,
        );
        assert!(matches!(&packages[0], Package::Ping(payload) if payload == "ping"));
        assert_eq!(text(&packages[1]), "Helloé");
        assert!(
            matches!(&packages[2], Package::Message(Message::Binary(data)) if data[..] == [1, 2, 3])
        );
    }

    #[test]
    fn rejects_broken_frames() {
        let protocol = CloseCode::PROTOCOL_ERROR;
        // Not masked.
        assert_eq!(violation(&[vec![0x81, 0x01, b'a']]), protocol);
        // Reserved bits and reserved opcodes.
        assert_eq!(violation(&[client_frame(0xC1, b"a")]), protocol);
        assert_eq!(violation(&[client_frame(0x83, b"a")]), protocol);
        // Fragmented or long control frames.
        assert_eq!(violation(&[client_frame(0x09, b"a")]), protocol);
        assert_eq!(violation(&[client_frame(0x89, &[0; 126])]), protocol);
        // Continuations without a message, and new messages inside one.
        assert_eq!(violation(&[client_frame(0x80, b"a")]), protocol);
        assert_eq!(
            violation(&[client_frame(0x01, b"a"), client_frame(0x81, b"b")]),
            protocol
        );
    }

    #[test]
    fn invalid_text_fails_as_soon_as_it_arrives() {
        // The first fragment already holds a byte that can never start a character.
        let packages = read(&[client_frame(0x01, &[b'a', 0xFF])], 1024);
        assert!(matches!(
            packages[..],
            [Package::Violation(CloseCode::INVALID_PAYLOAD, _)]
        ));
        // A character cut off at the end of the message.
        assert_eq!(
            violation(&[client_frame(0x81, &[b'a', 0xC3])]),
            CloseCode::INVALID_PAYLOAD
        );
    }

    #[test]
    fn enforces_the_maximum_size_from_the_header() {
        // The header alone tells that the frame is too big; its payload never needs to arrive.
        let header = client_frame(0x82, &[0; 100])[..8].to_vec();
        assert!(matches!(
            read(&[header], 64)[..],
            [Package::Violation(CloseCode::MESSAGE_TOO_BIG, _)]
        ));
        // Fragments that are small on their own but too big together.
        assert!(matches!(
            read(
                &[client_frame(0x02, &[0; 40]), client_frame(0x00, &[0; 40])],
                64
            )[..],
            [Package::Violation(CloseCode::MESSAGE_TOO_BIG, _)]
        ));
    }

    #[test]
    fn reads_close_frames_and_ignores_what_follows() {
        let packages = read(
            &[
                client_frame(0x88, b"\x03\xe8bye"),
                client_frame(0x81, b"late"),
            ],
            1024,
        );
        assert!(matches!(
            &packages[..],
            [Package::Close(Some(CloseFrame { code: CloseCode::NORMAL, reason }))] if reason == "bye"
        ));
        assert!(matches!(
            read(&[client_frame(0x88, b"")], 1024)[..],
            [Package::Close(None)]
        ));
        assert_eq!(
            violation(&[client_frame(0x88, b"\x03")]),
            CloseCode::PROTOCOL_ERROR
        );
        assert_eq!(
            violation(&[client_frame(0x88, b"\x03\xed")]),
            CloseCode::PROTOCOL_ERROR
        );
        assert_eq!(
            violation(&[client_frame(0x88, b"\x03\xe8\xff")]),
            CloseCode::INVALID_PAYLOAD
        );
    }

    #[test]
    fn head_filter_switches_to_frames() {
        let mut filter: BoxFilter<WebSocketPackage> = Box::new(HeadFilter::new(Role::Server, 1024));
        let mut buf = BytesMut::from(&b"GET / HTTP/1.1\r\nHost: a\r\n\r\n"[..]);
        buf.extend_from_slice(&client_frame(0x81, b"early"));
        let head = filter.filter(&mut buf).unwrap().unwrap();
        assert!(matches!(head.0, Package::Handshake(Ok(_))));
        let mut filter = filter.next_filter().expect("switches to the frame filter");
        let package = filter.filter(&mut buf).unwrap().unwrap();
        assert_eq!(text(&package.0), "early");
    }

    #[test]
    fn head_filter_limits_the_head() {
        let mut filter = HeadFilter::new(Role::Server, 1024 * 1024);
        let mut buf = BytesMut::from(&[b'x'; MAX_HEAD_SIZE + 1][..]);
        let package = filter.filter(&mut buf).unwrap().unwrap();
        assert!(matches!(
            package.0,
            Package::Handshake(Err(HeadError::TooLarge))
        ));
        assert!(filter.next_filter().is_none());

        // A smaller maximum size limits the head too.
        let mut filter = HeadFilter::new(Role::Server, 1024);
        let mut buf = BytesMut::from(&[b'x'; 1025][..]);
        let package = filter.filter(&mut buf).unwrap().unwrap();
        assert!(matches!(
            package.0,
            Package::Handshake(Err(HeadError::TooLarge))
        ));
    }
}
