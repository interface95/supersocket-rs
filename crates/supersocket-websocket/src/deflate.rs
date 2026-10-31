//! Per-message compression, RFC 7692 (`permessage-deflate`).
//!
//! Negotiation happens in the handshake filter, which sees the request on the server and the
//! response on the client. Decompression happens in the frame filter, in the order frames arrive,
//! which a peer that keeps its compression window between messages depends on. Compression
//! happens where messages are sent. Without the `deflate` feature, the types here are empty and
//! no connection negotiates the extension.

/// Why a compressed message could not be decoded.
#[cfg_attr(not(feature = "deflate"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeError {
    /// Decompressing produced more than the maximum message size.
    TooBig,
    /// The data is not valid DEFLATE.
    Corrupt,
}

/// Compressed data to send: the data and, with context takeover, a guard that keeps other
/// senders out until the frame is queued, so that frames leave in the order they were compressed.
#[cfg(not(feature = "deflate"))]
pub(crate) enum Encoded {}

#[cfg(not(feature = "deflate"))]
impl Encoded {
    /// The first byte of the frame to send and its payload.
    pub(crate) fn frame<'a>(&'a self, _opcode: u8, _payload: &'a [u8]) -> (u8, &'a [u8]) {
        match *self {}
    }

    /// Records that the frame was sent; `false` means the message must be encoded again.
    pub(crate) fn finish(self) -> bool {
        match self {}
    }
}

/// The settings for compression; the enabled version is [`CompressionOptions`].
#[cfg(not(feature = "deflate"))]
#[derive(Debug, Clone, Copy)]
pub(crate) enum Setup {}

/// An accepted negotiation.
#[cfg(not(feature = "deflate"))]
pub(crate) enum Accepted {}

#[cfg(not(feature = "deflate"))]
impl Accepted {
    /// The `Sec-WebSocket-Extensions` value a server answers with.
    pub(crate) fn response(&self) -> &str {
        match *self {}
    }

    /// The decompressor for the connection's frame filter.
    pub(crate) fn decoder(&self) -> Decoder {
        match *self {}
    }

    /// The compressor for the connection's sending side.
    pub(crate) fn encoder(&self) -> Encoder {
        match *self {}
    }
}

/// Decompresses the messages of one connection.
#[cfg(not(feature = "deflate"))]
pub(crate) enum Decoder {}

#[cfg(not(feature = "deflate"))]
impl Decoder {
    /// Decompresses the payload of a message of at most `limit` bytes.
    pub(crate) fn decode(&mut self, _input: &[u8], _limit: usize) -> Result<Vec<u8>, DecodeError> {
        match *self {}
    }
}

/// Compresses the messages one connection sends.
#[cfg(not(feature = "deflate"))]
pub(crate) enum Encoder {}

#[cfg(not(feature = "deflate"))]
impl Encoder {
    /// Compresses a message for a send that may wait.
    pub(crate) async fn encode(&self, _opcode: u8, _payload: &[u8]) -> Encoded {
        match *self {}
    }

    /// Compresses `payload` for a send that must not wait; `None` sends it as it is.
    pub(crate) fn encode_now(&self, _payload: &[u8]) -> Option<Vec<u8>> {
        match *self {}
    }
}

/// Negotiates the extension for a handshake: on the server from the request's offers, on the
/// client from the response. Returns `Ok(None)` when the extension is not used.
#[cfg(not(feature = "deflate"))]
pub(crate) fn negotiate(
    _role: crate::filter::Role,
    _head: &crate::handshake::Head,
    setup: Setup,
) -> Result<Option<Accepted>, &'static str> {
    match setup {}
}

#[cfg(feature = "deflate")]
pub use enabled::CompressionOptions;
#[cfg(feature = "deflate")]
#[cfg(all(feature = "deflate", feature = "client"))]
pub(crate) use enabled::offer;
#[cfg(feature = "deflate")]
pub(crate) use enabled::{Accepted, Decoder, Encoder, Setup, negotiate};

#[cfg(feature = "deflate")]
mod enabled {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex, OnceLock, PoisonError},
    };

    use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
    use tokio::sync::OwnedMutexGuard;

    use super::DecodeError;
    use crate::{
        filter::Role,
        frame::{FIN, RSV1},
        handshake::Head,
    };

    /// The name of the extension.
    const NAME: &str = "permessage-deflate";

    /// Settings for per-message compression, RFC 7692 (`permessage-deflate`), for
    /// [`WebSocketBuilder::compression`](crate::WebSocketBuilder::compression) and the client's
    /// [`compression`](crate::WebSocketClientBuilder::compression).
    ///
    /// Both sides compress their messages on their own by default, keeping no LZ77 window between
    /// messages (`no_context_takeover`). That compresses a little worse than sharing the window,
    /// but a connection then holds no compression state while idle: compressors, a few hundred
    /// kilobytes each, come from a pool only while a message is compressed or decompressed. With a
    /// shared window, every connection keeps a compressor and a decompressor for its whole life,
    /// the sends of a connection are serialized, each waiting until the previous one is queued,
    /// and a send that is cancelled after its message was compressed is completed by the next
    /// send, since the peer's window must see every message ours saw.
    ///
    /// Messages below [`threshold`](Self::threshold) are sent as they are, as are messages that
    /// compression would not make smaller. Control frames are never compressed. The maximum
    /// package length applies to a message both as received and after decompression.
    ///
    /// ```
    /// use supersocket_websocket::CompressionOptions;
    ///
    /// let options = CompressionOptions {
    ///     level: 4,
    ///     threshold: 256,
    ///     ..CompressionOptions::default()
    /// };
    /// assert!(options.server_no_context_takeover);
    /// ```
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CompressionOptions {
        /// The compression level, 1 (fastest) to 9 (smallest); 0 stores without compressing.
        /// Default: 6.
        pub level: u32,
        /// Messages shorter than this many bytes are not compressed. Default: 1024.
        pub threshold: usize,
        /// The server compresses each message on its own, without the window of the previous
        /// ones. On the client, this asks the server to do so. Default: `true`.
        pub server_no_context_takeover: bool,
        /// The client compresses each message on its own. On the server, this tells every client
        /// to do so, which clients must obey. Default: `true`.
        pub client_no_context_takeover: bool,
    }

    impl Default for CompressionOptions {
        fn default() -> Self {
            Self {
                level: 6,
                threshold: 1024,
                server_no_context_takeover: true,
                client_no_context_takeover: true,
            }
        }
    }

    /// What the handshake filter needs to negotiate.
    pub(crate) type Setup = CompressionOptions;

    /// What was agreed for a connection.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Negotiated {
        /// Our messages are compressed without the window of the previous ones.
        reset_compressor: bool,
        /// The peer's are, so its window can be dropped after every message.
        reset_decompressor: bool,
    }

    /// An accepted negotiation.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct Accepted {
        options: CompressionOptions,
        negotiated: Negotiated,
        response: String,
    }

    impl Accepted {
        /// The `Sec-WebSocket-Extensions` value a server answers with.
        pub(crate) fn response(&self) -> &str {
            &self.response
        }

        /// The decompressor for the connection's frame filter.
        pub(crate) fn decoder(&self) -> Decoder {
            Decoder {
                reset: self.negotiated.reset_decompressor,
                context: None,
                history: VecDeque::new(),
            }
        }

        /// The compressor for the connection's sending side.
        pub(crate) fn encoder(&self) -> Encoder {
            Encoder {
                level: self.options.level.min(9),
                threshold: self.options.threshold,
                context: (!self.negotiated.reset_compressor).then(|| {
                    Arc::new(tokio::sync::Mutex::new(Window {
                        compressor: Compress::new(
                            Compression::new(self.options.level.min(9)),
                            false,
                        ),
                        pending: None,
                    }))
                }),
            }
        }
    }

    /// The parameters of one `permessage-deflate` entry of a `Sec-WebSocket-Extensions` field.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Params {
        server_no_context_takeover: bool,
        client_no_context_takeover: bool,
        server_max_window_bits: Option<u8>,
        /// Present without a value in an offer means the client accepts any value.
        client_max_window_bits: Option<Option<u8>>,
    }

    /// Parses one entry: `None` if it is another extension, otherwise the parameters or why they
    /// are invalid (section 7 of RFC 7692 lists what makes an offer or a response invalid).
    fn parse(entry: &str) -> Option<Result<Params, &'static str>> {
        let mut parts = entry.split(';').map(str::trim);
        if !parts.next()?.eq_ignore_ascii_case(NAME) {
            return None;
        }
        let mut params = Params::default();
        for part in parts {
            let (name, value) = match part.split_once('=') {
                Some((name, value)) => (name.trim(), Some(value.trim().trim_matches('"'))),
                None => (part, None),
            };
            let window_bits = || match value {
                Some(value) => value
                    .parse::<u8>()
                    .ok()
                    .filter(|bits| (8..=15).contains(bits))
                    .ok_or("invalid permessage-deflate window size"),
                None => Err("permessage-deflate window size without a value"),
            };
            let duplicate = Some(Err("duplicate permessage-deflate parameter"));
            match name.to_ascii_lowercase().as_str() {
                "server_no_context_takeover" => {
                    if value.is_some() {
                        return Some(Err("permessage-deflate parameter with an unexpected value"));
                    }
                    if std::mem::replace(&mut params.server_no_context_takeover, true) {
                        return duplicate;
                    }
                }
                "client_no_context_takeover" => {
                    if value.is_some() {
                        return Some(Err("permessage-deflate parameter with an unexpected value"));
                    }
                    if std::mem::replace(&mut params.client_no_context_takeover, true) {
                        return duplicate;
                    }
                }
                "server_max_window_bits" => {
                    let bits = match window_bits() {
                        Ok(bits) => bits,
                        Err(why) => return Some(Err(why)),
                    };
                    if params.server_max_window_bits.replace(bits).is_some() {
                        return duplicate;
                    }
                }
                "client_max_window_bits" => {
                    let bits = match value {
                        None => None,
                        Some(_) => match window_bits() {
                            Ok(bits) => Some(bits),
                            Err(why) => return Some(Err(why)),
                        },
                    };
                    if params.client_max_window_bits.replace(bits).is_some() {
                        return duplicate;
                    }
                }
                _ => return Some(Err("unknown permessage-deflate parameter")),
            }
        }
        Some(Ok(params))
    }

    /// Negotiates the extension for a handshake: on the server from the request's offers, on the
    /// client from the response. Returns `Ok(None)` when the extension is not used. A server
    /// declines offers it cannot honor, which is never an error; a client fails the handshake
    /// when the response is not an answer to its offer, as RFC 7692 requires.
    pub(crate) fn negotiate(
        role: Role,
        head: &Head,
        options: Setup,
    ) -> Result<Option<Accepted>, &'static str> {
        let entries = head.values("Sec-WebSocket-Extensions");
        match role {
            Role::Server => Ok(accept(entries, &options)),
            Role::Client => check_response(entries, &options),
        }
    }

    /// The server's choice among a client's offers, in the client's order of preference.
    fn accept<'a>(
        offers: impl Iterator<Item = &'a str>,
        options: &CompressionOptions,
    ) -> Option<Accepted> {
        for offer in offers {
            let Some(Ok(params)) = parse(offer) else {
                continue;
            };
            // Compression here always uses the largest window, so a smaller one cannot be
            // promised; RFC 7692 says to decline such an offer. `client_max_window_bits` needs no
            // answer: the client may use any window, as decompression handles every size.
            if params.server_max_window_bits.is_some_and(|bits| bits != 15) {
                continue;
            }
            let negotiated = Negotiated {
                reset_compressor: options.server_no_context_takeover
                    || params.server_no_context_takeover,
                reset_decompressor: options.client_no_context_takeover
                    || params.client_no_context_takeover,
            };
            let mut response = NAME.to_owned();
            if negotiated.reset_compressor {
                response.push_str("; server_no_context_takeover");
            }
            if negotiated.reset_decompressor {
                response.push_str("; client_no_context_takeover");
            }
            if params.server_max_window_bits.is_some() {
                response.push_str("; server_max_window_bits=15");
            }
            return Some(Accepted {
                options: *options,
                negotiated,
                response,
            });
        }
        None
    }

    /// The offer a client makes. `client_max_window_bits` is left out, since this client cannot
    /// compress with a smaller window than the default, and a server may only ask for one when
    /// it was offered.
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    pub(crate) fn offer(options: &CompressionOptions) -> String {
        let mut offer = NAME.to_owned();
        if options.client_no_context_takeover {
            offer.push_str("; client_no_context_takeover");
        }
        if options.server_no_context_takeover {
            offer.push_str("; server_no_context_takeover");
        }
        offer
    }

    /// Checks the server's answer to [`offer`].
    fn check_response<'a>(
        mut chosen: impl Iterator<Item = &'a str>,
        options: &CompressionOptions,
    ) -> Result<Option<Accepted>, &'static str> {
        let Some(entry) = chosen.next() else {
            return Ok(None);
        };
        if chosen.next().is_some() {
            return Err("the server chose more than one extension");
        }
        let params = match parse(entry) {
            None => return Err("the server chose an extension that was not offered"),
            Some(Err(why)) => return Err(why),
            Some(Ok(params)) => params,
        };
        if params.client_max_window_bits.is_some() {
            return Err("the server asked for a window size that was not offered");
        }
        if options.server_no_context_takeover && !params.server_no_context_takeover {
            return Err("the server did not accept server_no_context_takeover");
        }
        // Having offered to compress without context takeover, the client must do so even if the
        // server did not say so. Only a server that promised to reset its own window is trusted
        // to; keeping its window otherwise decodes correctly either way.
        let negotiated = Negotiated {
            reset_compressor: options.client_no_context_takeover
                || params.client_no_context_takeover,
            reset_decompressor: params.server_no_context_takeover,
        };
        Ok(Some(Accepted {
            options: *options,
            negotiated,
            response: String::new(),
        }))
    }

    /// The empty stored block a sync flush ends with, which RFC 7692 removes from every message
    /// and adds back before decompressing.
    const TRAILER: [u8; 4] = [0x00, 0x00, 0xff, 0xff];
    const WINDOW_SIZE: usize = 32768;
    const FINAL_EMPTY_BLOCK: [u8; 5] = [0x01, 0x00, 0x00, 0xff, 0xff];

    /// Compressors and decompressors for connections that keep no window between messages. They
    /// are borrowed for one message at a time, so idle connections hold none. The pool is bounded:
    /// a compressor is a few hundred kilobytes.
    struct Pool {
        decompressors: Mutex<Vec<Decompress>>,
        compressors: Mutex<Vec<(u32, Compress)>>,
    }

    static POOL: Pool = Pool {
        decompressors: Mutex::new(Vec::new()),
        compressors: Mutex::new(Vec::new()),
    };

    fn pool_capacity() -> usize {
        static CAPACITY: OnceLock<usize> = OnceLock::new();
        *CAPACITY.get_or_init(|| {
            std::thread::available_parallelism().map_or(4, |threads| threads.get().clamp(2, 32))
        })
    }

    fn take_decompressor() -> Decompress {
        POOL.decompressors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .unwrap_or_else(|| Decompress::new(false))
    }

    fn give_decompressor(mut decompressor: Decompress) {
        decompressor.reset(false);
        let mut pool = POOL
            .decompressors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if pool.len() < pool_capacity() {
            pool.push(decompressor);
        }
    }

    fn take_compressor(level: u32) -> Compress {
        let mut pool = POOL
            .compressors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match pool.iter().position(|(pooled, _)| *pooled == level) {
            Some(index) => pool.swap_remove(index).1,
            None => Compress::new(Compression::new(level), false),
        }
    }

    fn give_compressor(level: u32, mut compressor: Compress) {
        compressor.reset();
        let mut pool = POOL
            .compressors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if pool.len() < pool_capacity() {
            pool.push((level, compressor));
        }
    }

    /// Decompresses the messages of one connection.
    pub(crate) struct Decoder {
        reset: bool,
        /// The decompressor with the peer's window, when the peer keeps one between messages.
        context: Option<Decompress>,
        /// Restores the LZ77 window when a BFINAL block ends the backend's raw stream.
        history: VecDeque<u8>,
    }

    impl Decoder {
        /// Decompresses the payload of a message, which may be at most `limit` bytes once
        /// decompressed.
        pub(crate) fn decode(
            &mut self,
            input: &[u8],
            limit: usize,
        ) -> Result<Vec<u8>, DecodeError> {
            let mut decompressor = self.context.take().unwrap_or_else(take_decompressor);
            let result = inflate(&mut decompressor, input, limit, &self.history).and_then(
                |(data, ended)| {
                    if !self.reset {
                        remember(&mut self.history, &data);
                        if ended {
                            restore_window(&mut decompressor, &self.history, &[])?;
                        }
                    }
                    Ok(data)
                },
            );
            if result.is_ok() && !self.reset {
                self.context = Some(decompressor);
            } else {
                self.history.clear();
                give_decompressor(decompressor);
            }
            result
        }
    }

    fn remember(history: &mut VecDeque<u8>, data: &[u8]) {
        let data = &data[data.len().saturating_sub(WINDOW_SIZE)..];
        let discard = (history.len() + data.len()).saturating_sub(WINDOW_SIZE);
        history.drain(..discard);
        history.extend(data);
    }

    /// flate2's default backend has no dictionary setter. A non-final stored block restores the
    /// same window without adding application output or depending on a particular backend.
    fn restore_window(
        decompressor: &mut Decompress,
        history: &VecDeque<u8>,
        output: &[u8],
    ) -> Result<(), DecodeError> {
        let output = &output[output.len().saturating_sub(WINDOW_SIZE)..];
        let previous = history.len().min(WINDOW_SIZE - output.len());
        let length = (previous + output.len()) as u16;
        decompressor.reset(false);
        if length == 0 {
            return Ok(());
        }
        let mut seed = Vec::with_capacity(5 + usize::from(length));
        seed.push(0);
        seed.extend_from_slice(&length.to_le_bytes());
        seed.extend_from_slice(&(!length).to_le_bytes());
        seed.extend(history.range(history.len() - previous..));
        seed.extend_from_slice(output);
        let mut discarded = Vec::with_capacity(usize::from(length) + 1);
        decompressor
            .decompress_vec(&seed, &mut discarded, FlushDecompress::None)
            .map_err(|_| DecodeError::Corrupt)?;
        if decompressor.total_in() as usize != seed.len() || discarded.len() != usize::from(length)
        {
            return Err(DecodeError::Corrupt);
        }
        Ok(())
    }

    /// Decompresses every block of one message, including blocks following BFINAL. Only this
    /// uncommon path needs an extra final block to verify its tail and then restore its window.
    fn inflate(
        decompressor: &mut Decompress,
        input: &[u8],
        limit: usize,
        history: &VecDeque<u8>,
    ) -> Result<(Vec<u8>, bool), DecodeError> {
        let capacity = input
            .len()
            .saturating_mul(3)
            .max(64)
            .min(limit.saturating_add(1));
        let mut out = Vec::with_capacity(capacity);
        let mut ended = false;
        for (phase, (data, flush)) in [
            (input, FlushDecompress::None),
            (&TRAILER[..], FlushDecompress::Sync),
            (&FINAL_EMPTY_BLOCK[..], FlushDecompress::Finish),
        ]
        .into_iter()
        .enumerate()
        {
            let verifying = phase == 2;
            if verifying && !ended {
                break;
            }
            let before_phase = out.len();
            let mut consumed = 0;
            loop {
                if out.len() > limit {
                    return Err(DecodeError::TooBig);
                }
                if out.len() == out.capacity() {
                    // One byte beyond the limit is enough to detect a message that is too big;
                    // `room` is at least one here.
                    let room = limit.saturating_add(1) - out.len();
                    out.reserve((out.len() / 2).max(256).min(room));
                }
                let (in_before, out_before) = (decompressor.total_in(), decompressor.total_out());
                let status = decompressor
                    .decompress_vec(&data[consumed..], &mut out, flush)
                    .map_err(|_| DecodeError::Corrupt)?;
                let read = (decompressor.total_in() - in_before) as usize;
                consumed += read;
                let produced = (decompressor.total_out() - out_before) as usize;
                match status {
                    Status::StreamEnd => {
                        if verifying {
                            if consumed != data.len() || out.len() != before_phase {
                                return Err(DecodeError::Corrupt);
                            }
                            break;
                        }
                        if phase != 0 {
                            return Err(DecodeError::Corrupt);
                        }
                        ended = true;
                        restore_window(decompressor, history, &out)?;
                    }
                    _ if verifying => return Err(DecodeError::Corrupt),
                    // Drain output already buffered by the backend before supplying the trailer.
                    _ if consumed == data.len() && produced == 0 => break,
                    _ if read == 0 && produced == 0 => return Err(DecodeError::Corrupt),
                    _ => {}
                }
            }
            if phase == 1 && ended && out.len() != before_phase {
                return Err(DecodeError::Corrupt);
            }
        }
        if out.len() > limit {
            return Err(DecodeError::TooBig);
        }
        Ok((out, ended))
    }

    /// Compressed data to send. With a shared window it holds the window until the caller reports
    /// the frame sent with [`finish`](Self::finish), so that frames leave in the order they were
    /// compressed, and so that a send cancelled in between is completed by the next one.
    pub(crate) enum Encoded {
        /// Sent as it is.
        Raw,
        /// Compressed on its own.
        Pooled(Vec<u8>),
        /// Compressed with the shared window, which keeps the frame until it is sent. `current`
        /// tells whether it is the caller's message or one a cancelled send left behind.
        Shared {
            window: OwnedMutexGuard<Window>,
            current: bool,
        },
    }

    impl Encoded {
        /// The first byte of the frame to send and its payload, for a message with `opcode` and
        /// `payload`.
        pub(crate) fn frame<'a>(&'a self, opcode: u8, payload: &'a [u8]) -> (u8, &'a [u8]) {
            match self {
                Self::Raw => (FIN | opcode, payload),
                Self::Pooled(data) => (FIN | RSV1 | opcode, data),
                Self::Shared { window, .. } => {
                    let pending = window
                        .pending
                        .as_ref()
                        .expect("a shared window holds the frame being sent");
                    (FIN | RSV1 | pending.opcode, &pending.data)
                }
            }
        }

        /// Records that the frame was sent. Returns whether the caller's message is done with;
        /// `false` means the frame was one a cancelled send left behind, and the caller must
        /// encode its message again.
        pub(crate) fn finish(self) -> bool {
            match self {
                Self::Raw | Self::Pooled(_) => true,
                Self::Shared {
                    mut window,
                    current,
                } => {
                    window.pending = None;
                    current
                }
            }
        }
    }

    /// The compressor of a connection that shares its window with the peer, with the frame it
    /// compressed last until that is known to be sent.
    pub(crate) struct Window {
        compressor: Compress,
        /// Compressed with the window but not yet sent. A send that is cancelled between
        /// compressing and queuing leaves its frame here, and the next send delivers it first:
        /// the peer's window must see every message ours saw, in order.
        pending: Option<Pending>,
    }

    struct Pending {
        opcode: u8,
        data: Vec<u8>,
    }

    /// Compresses the messages one connection sends.
    pub(crate) struct Encoder {
        level: u32,
        threshold: usize,
        /// The window shared with the peer, when one is kept between messages. Every compressed
        /// message then depends on the ones before it, so compressing and queuing must not
        /// interleave between tasks.
        context: Option<Arc<tokio::sync::Mutex<Window>>>,
    }

    impl Encoder {
        /// Compresses a message with `opcode` and `payload` for a send that may wait. The caller
        /// sends the frame [`Encoded::frame`] gives and calls [`Encoded::finish`]; with a shared
        /// window, a frame left behind by a cancelled send comes first, and `finish` then says to
        /// encode the message again.
        pub(crate) async fn encode(&self, opcode: u8, payload: &[u8]) -> Encoded {
            let Some(context) = &self.context else {
                return match self.encode_now(payload) {
                    Some(data) => Encoded::Pooled(data),
                    None => Encoded::Raw,
                };
            };
            let mut window = context.clone().lock_owned().await;
            if window.pending.is_some() {
                return Encoded::Shared {
                    window,
                    current: false,
                };
            }
            // A repeated empty sync flush can produce no block at all on zlib backends.
            // Send empty messages raw so they cannot disturb the peer's shared window.
            if payload.is_empty() || payload.len() < self.threshold {
                return Encoded::Raw;
            }
            // Once the window saw this message, the peer's window must see it too, so the
            // compressed form is sent even when it is not smaller.
            match deflate(&mut window.compressor, payload) {
                Some(data) => {
                    window.pending = Some(Pending { opcode, data });
                    Encoded::Shared {
                        window,
                        current: true,
                    }
                }
                None => Encoded::Raw,
            }
        }

        /// Compresses `payload` for a send that must not wait; `None` sends it as it is. With
        /// a shared window nothing is compressed, since waiting for the window is not an option
        /// and a message the window never saw does not disturb it.
        pub(crate) fn encode_now(&self, payload: &[u8]) -> Option<Vec<u8>> {
            if payload.len() < self.threshold || self.context.is_some() {
                return None;
            }
            let mut compressor = take_compressor(self.level);
            let data = deflate(&mut compressor, payload);
            give_compressor(self.level, compressor);
            data.filter(|data| data.len() < payload.len())
        }
    }

    /// Compresses one message, ending with a sync flush whose trailer is removed. `None` means
    /// the compressor failed, which it does not in practice.
    fn deflate(compressor: &mut Compress, input: &[u8]) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(input.len() / 2 + 64);
        let mut consumed = 0;
        while consumed < input.len() {
            if out.len() == out.capacity() {
                out.reserve(out.len() / 2 + 64);
            }
            let before = compressor.total_in();
            compressor
                .compress_vec(&input[consumed..], &mut out, FlushCompress::None)
                .ok()?;
            consumed += (compressor.total_in() - before) as usize;
        }
        loop {
            if out.len() == out.capacity() {
                out.reserve(64);
            }
            compressor
                .compress_vec(&[], &mut out, FlushCompress::Sync)
                .ok()?;
            // A flush that did not fill the space is complete.
            if out.len() < out.capacity() {
                break;
            }
        }
        if out.ends_with(&TRAILER) {
            out.truncate(out.len() - TRAILER.len());
        }
        Some(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{
            frame::{OP_BINARY, OP_TEXT},
            handshake::parse_head,
        };

        fn head(extensions: &[&str]) -> Head {
            let mut text = "GET / HTTP/1.1\r\n".to_owned();
            for value in extensions {
                text.push_str(&format!("Sec-WebSocket-Extensions: {value}\r\n"));
            }
            text.push_str("\r\n");
            parse_head(text.as_bytes()).unwrap()
        }

        fn server(extensions: &[&str], options: CompressionOptions) -> Option<Accepted> {
            negotiate(Role::Server, &head(extensions), options).unwrap()
        }

        fn client(
            extensions: &[&str],
            options: CompressionOptions,
        ) -> Result<Option<Accepted>, &'static str> {
            negotiate(Role::Client, &head(extensions), options)
        }

        #[test]
        fn a_server_answers_offers_as_rfc_7692_describes() {
            let defaults = CompressionOptions::default();
            // What browsers send.
            let accepted =
                server(&["permessage-deflate; client_max_window_bits"], defaults).unwrap();
            assert_eq!(
                accepted.response(),
                "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
            );
            assert_eq!(
                accepted.negotiated,
                Negotiated {
                    reset_compressor: true,
                    reset_decompressor: true
                }
            );
            // A server that shares windows, unless the client asks otherwise.
            let sharing = CompressionOptions {
                server_no_context_takeover: false,
                client_no_context_takeover: false,
                ..defaults
            };
            let accepted = server(&["permessage-deflate"], sharing).unwrap();
            assert_eq!(accepted.response(), "permessage-deflate");
            assert_eq!(
                accepted.negotiated,
                Negotiated {
                    reset_compressor: false,
                    reset_decompressor: false
                }
            );
            let accepted = server(
                &["permessage-deflate; server_no_context_takeover; client_no_context_takeover"],
                sharing,
            )
            .unwrap();
            assert_eq!(
                accepted.response(),
                "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
            );
            // The largest window can be promised, a smaller one not; the next offer is taken.
            let accepted = server(
                &["permessage-deflate; server_max_window_bits=\"15\""],
                defaults,
            )
            .unwrap();
            assert!(accepted.response().ends_with("; server_max_window_bits=15"));
            let accepted = server(
                &[
                    "permessage-deflate; server_max_window_bits=10",
                    "permessage-deflate; client_max_window_bits=12",
                ],
                defaults,
            )
            .unwrap();
            assert_eq!(
                accepted.response(),
                "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
            );
            // Offers are spread over several header fields.
            assert!(server(&["x-webkit-deflate-frame", "permessage-deflate"], defaults).is_some());
        }

        #[test]
        fn a_server_declines_what_it_cannot_honor_or_understand() {
            let defaults = CompressionOptions::default();
            for offer in [
                "permessage-deflate; server_max_window_bits=10",
                "permessage-deflate; server_max_window_bits",
                "permessage-deflate; server_max_window_bits=16",
                "permessage-deflate; client_max_window_bits=7",
                "permessage-deflate; server_no_context_takeover=1",
                "permessage-deflate; server_no_context_takeover; server_no_context_takeover",
                "permessage-deflate; mode=fast",
                "x-webkit-deflate-frame",
                "",
            ] {
                assert!(server(&[offer], defaults).is_none(), "{offer:?}");
            }
            assert!(server(&[], defaults).is_none());
        }

        #[test]
        fn a_client_checks_the_response_to_its_offer() {
            let defaults = CompressionOptions::default();
            assert_eq!(
                offer(&defaults),
                "permessage-deflate; client_no_context_takeover; server_no_context_takeover"
            );
            assert_eq!(client(&[], defaults).unwrap(), None);
            let accepted = client(
                &["permessage-deflate; server_no_context_takeover; client_no_context_takeover"],
                defaults,
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                accepted.negotiated,
                Negotiated {
                    reset_compressor: true,
                    reset_decompressor: true
                }
            );
            // A request that the server reset its window is mandatory, unlike the client's hint
            // about its own window. Only a client that allowed takeover accepts a bare response.
            assert!(client(&["permessage-deflate"], defaults).is_err());
            let accepted = client(
                &["permessage-deflate"],
                CompressionOptions {
                    server_no_context_takeover: false,
                    ..defaults
                },
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                accepted.negotiated,
                Negotiated {
                    reset_compressor: true,
                    reset_decompressor: false
                }
            );
            let sharing = CompressionOptions {
                server_no_context_takeover: false,
                client_no_context_takeover: false,
                ..defaults
            };
            assert_eq!(offer(&sharing), "permessage-deflate");
            let accepted = client(
                &["permessage-deflate; client_no_context_takeover; server_max_window_bits=10"],
                sharing,
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                accepted.negotiated,
                Negotiated {
                    reset_compressor: true,
                    reset_decompressor: false
                }
            );
            for response in [
                "permessage-deflate; client_max_window_bits",
                "permessage-deflate; client_max_window_bits=12",
                "permessage-deflate; server_max_window_bits=7",
                "permessage-deflate; unknown",
                "x-webkit-deflate-frame",
                "permessage-deflate, permessage-deflate",
            ] {
                assert!(client(&[response], defaults).is_err(), "{response:?}");
            }
        }

        fn negotiated(reset: bool) -> Accepted {
            Accepted {
                options: CompressionOptions {
                    threshold: 0,
                    ..CompressionOptions::default()
                },
                negotiated: Negotiated {
                    reset_compressor: reset,
                    reset_decompressor: reset,
                },
                response: String::new(),
            }
        }

        #[test]
        fn decodes_the_examples_of_rfc_7692() {
            // Section 7.2.3.1: a message in one block; the same message again with context.
            let mut decoder = negotiated(false).decoder();
            let hello = [0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
            assert_eq!(decoder.decode(&hello, 1024).unwrap(), b"Hello");
            assert_eq!(
                decoder
                    .decode(&[0xf2, 0x00, 0x11, 0x00, 0x00], 1024)
                    .unwrap(),
                b"Hello"
            );
            // Section 7.2.3.2: without context takeover, the same bytes repeat.
            let mut decoder = negotiated(true).decoder();
            assert_eq!(decoder.decode(&hello, 1024).unwrap(), b"Hello");
            assert_eq!(decoder.decode(&hello, 1024).unwrap(), b"Hello");
            // Section 7.2.3.4: a block with BFINAL set ends the stream; the next message starts
            // a fresh one even with context takeover.
            let mut decoder = negotiated(false).decoder();
            let final_block = [0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00];
            assert_eq!(decoder.decode(&final_block, 1024).unwrap(), b"Hello");
            assert_eq!(decoder.decode(&hello, 1024).unwrap(), b"Hello");
            // Section 7.2.3.5: two blocks; section 7.2.3.6: an empty message.
            assert_eq!(
                decoder
                    .decode(
                        &[
                            0xf2, 0x48, 0x05, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca, 0xc9, 0xc9, 0x07,
                            0x00
                        ],
                        1024
                    )
                    .unwrap(),
                b"Hello"
            );
            assert_eq!(decoder.decode(&[0x00], 1024).unwrap(), b"");
        }

        #[test]
        fn decoding_rejects_garbage_and_messages_beyond_the_limit() {
            let mut decoder = negotiated(true).decoder();
            assert_eq!(
                decoder.decode(&[0xff, 0xff, 0xff, 0xff], 1024),
                Err(DecodeError::Corrupt)
            );
            // A final block does not hide malformed or incomplete blocks that follow it.
            assert_eq!(
                decoder.decode(
                    &[0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00, 0xf2, 0x48],
                    1024
                ),
                Err(DecodeError::Corrupt)
            );
            // A small message that inflates far beyond the limit, as a bomb would.
            let bomb = {
                let mut compressor = Compress::new(Compression::new(9), false);
                deflate(&mut compressor, &vec![0; 1 << 20]).unwrap()
            };
            assert!(bomb.len() < 2048, "{} bytes", bomb.len());
            assert_eq!(decoder.decode(&bomb, 1 << 16), Err(DecodeError::TooBig));
            assert_eq!(decoder.decode(&bomb, 1 << 20).unwrap().len(), 1 << 20);
            assert_eq!(
                decoder.decode(&bomb, (1 << 20) - 1),
                Err(DecodeError::TooBig)
            );
            // The decoder is usable after an error.
            assert_eq!(
                decoder
                    .decode(&[0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00], 1024)
                    .unwrap(),
                b"Hello"
            );
        }

        #[test]
        fn final_blocks_continue_with_the_same_window_and_message_limit() {
            let final_hello = [0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
            let refer_to_hello = [0xf2, 0x00, 0x11, 0x00, 0x00];
            for reset in [true, false] {
                let mut decoder = negotiated(reset).decoder();
                let blocks = [&final_hello[..], &final_hello, &refer_to_hello].concat();
                assert_eq!(decoder.decode(&blocks, 1024).unwrap(), b"HelloHelloHello");
                if !reset {
                    assert_eq!(decoder.decode(&refer_to_hello, 1024).unwrap(), b"Hello");
                }
                let mut blocks = Vec::new();
                for byte in *b"ab" {
                    // Byte-aligned stored blocks; the first is final, the second is not.
                    blocks.extend_from_slice(&[u8::from(byte == b'a'), 100, 0, 155, 255]);
                    blocks.extend_from_slice(&[byte; 100]);
                }
                blocks.push(0);
                assert_eq!(decoder.decode(&blocks, 128), Err(DecodeError::TooBig));
                assert_eq!(
                    decoder.decode(&blocks, 200).unwrap(),
                    [vec![b'a'; 100], vec![b'b'; 100]].concat()
                );
            }
        }

        #[test]
        fn final_blocks_retain_history_from_previous_messages() {
            let history = b"abcdefghijklmnopqrstuvwxyz".repeat(1000);
            let mut compressor = Compress::new(Compression::default(), false);
            let first = deflate(&mut compressor, &history).unwrap();
            let mut decoder = negotiated(false).decoder();
            assert_eq!(decoder.decode(&first, 32768).unwrap(), history);
            // Advance beyond 32 KiB so the retained history wraps before it is restored.
            let more = b"abcdefghijklmnopqrstuvwxyz".repeat(500);
            let second = deflate(&mut compressor, &more).unwrap();
            assert_eq!(decoder.decode(&second, 32768).unwrap(), more);
            // Final literal "!", then a block referring to the previous message's alphabet.
            assert_eq!(
                decoder
                    .decode(&[0x53, 0x04, 0x00, 0xc2, 0x2d, 0x05, 0x00], 1024)
                    .unwrap(),
                b"!abcdefghijklmnopqrstuvwxyz"
            );
        }

        #[test]
        fn final_blocks_accept_sync_flushed_tails_of_various_sizes() {
            let final_hello = [0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
            for level in [0, 1, 6, 9] {
                for size in [0, 1, 2, 7, 63, 64, 65, 127, 128, 1024, 32768, 65536] {
                    for pattern in 0..3 {
                        let mut random = 0x1234_5678u32;
                        let payload: Vec<u8> = (0..size)
                            .map(|index| match pattern {
                                0 => b'a',
                                1 => (index % 251) as u8,
                                _ => {
                                    random ^= random << 13;
                                    random ^= random >> 17;
                                    random ^= random << 5;
                                    random as u8
                                }
                            })
                            .collect();
                        let mut compressor = Compress::new(Compression::new(level), false);
                        let tail = deflate(&mut compressor, &payload).unwrap();
                        let input = [&final_hello[..], &tail].concat();
                        let expected = [&b"Hello"[..], &payload].concat();
                        for reset in [true, false] {
                            let mut decoder = negotiated(reset).decoder();
                            assert_eq!(
                                decoder.decode(&input, expected.len()).unwrap_or_else(|error| {
                                    panic!("{error:?}: level={level}, size={size}, pattern={pattern}, reset={reset}")
                                }),
                                expected,
                                "level={level}, size={size}, pattern={pattern}, reset={reset}"
                            );
                        }
                    }
                }
            }
        }

        #[test]
        fn final_blocks_do_not_hide_corrupt_or_truncated_tails() {
            let final_hello = [0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
            for tail in [
                &b""[..],
                &b"\xff"[..],
                &b"\x00\x05"[..],
                &b"\x00\x05\x00\xfa\xffw"[..],
            ] {
                let mut decoder = negotiated(false).decoder();
                let input = [&final_hello[..], tail].concat();
                assert_eq!(
                    decoder.decode(&input, 1024),
                    Err(DecodeError::Corrupt),
                    "tail {tail:02x?}"
                );
            }
        }

        #[tokio::test]
        async fn encoding_round_trips_with_and_without_context() {
            for reset in [true, false] {
                let encoder = negotiated(reset).encoder();
                let mut decoder = negotiated(reset).decoder();
                let messages: Vec<Vec<u8>> = vec![
                    b"Hello".to_vec(),
                    Vec::new(),
                    b"Hello".to_vec(),
                    (0..100_000u32).map(|i| (i % 251) as u8).collect(),
                    vec![7; 300_000],
                ];
                for message in &messages {
                    let encoded = encoder.encode(OP_BINARY, message).await;
                    let (first, data) = encoded.frame(OP_BINARY, message);
                    assert_eq!(first & !RSV1, FIN | OP_BINARY);
                    if message.is_empty() {
                        assert_eq!(first & RSV1, 0, "empty messages do not advance the window");
                    }
                    let decoded = if first & RSV1 != 0 {
                        decoder.decode(data, 1 << 20).unwrap_or_else(|error| {
                            panic!("{error:?}: reset={reset}, message length={}, compressed={data:02x?}", message.len())
                        })
                    } else {
                        data.to_vec()
                    };
                    assert_eq!(&decoded, message, "reset={reset}");
                    assert!(encoded.finish());
                }
            }
            // The trailer is removed, and what is left decodes on its own: "Hello" compresses to
            // seven bytes, as in RFC 7692 section 7.2.3.1.
            let mut compressor = Compress::new(Compression::default(), false);
            let data = deflate(&mut compressor, b"Hello").unwrap();
            assert!(!data.ends_with(&TRAILER), "{data:02x?}");
            assert_eq!(data.len(), 7, "{data:02x?}");
            assert_eq!(
                negotiated(true).decoder().decode(&data, 64).unwrap(),
                b"Hello"
            );
        }

        #[tokio::test]
        async fn messages_that_do_not_shrink_and_small_ones_are_sent_raw() {
            let encoder = Accepted {
                options: CompressionOptions {
                    threshold: 16,
                    ..CompressionOptions::default()
                },
                ..negotiated(true)
            }
            .encoder();
            assert!(matches!(
                encoder.encode(OP_TEXT, b"short").await,
                Encoded::Raw
            ));
            assert!(encoder.encode_now(b"short").is_none());
            // Incompressible data from a xorshift generator.
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            let random: Vec<u8> = (0..4096)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> 56) as u8
                })
                .collect();
            assert!(matches!(
                encoder.encode(OP_BINARY, &random).await,
                Encoded::Raw
            ));
            assert!(encoder.encode_now(&random).is_none());
            let text = b"compressible compressible compressible compressible";
            assert!(
                encoder
                    .encode_now(text)
                    .is_some_and(|data| data.len() < text.len())
            );
            // Non-blocking sends do not compress when a window is shared.
            let sharing = negotiated(false).encoder();
            assert!(sharing.encode_now(text).is_none());
            let encoded = sharing.encode(OP_TEXT, text).await;
            assert!(matches!(encoded, Encoded::Shared { current: true, .. }));
            assert_eq!(encoded.frame(OP_TEXT, text).0, FIN | RSV1 | OP_TEXT);
            assert!(encoded.finish());
        }

        #[tokio::test]
        async fn a_send_cancelled_after_compressing_is_completed_by_the_next_one() {
            let encoder = negotiated(false).encoder();
            let mut decoder = negotiated(false).decoder();
            // The first message is compressed, but the send is cancelled before the frame is
            // queued: `finish` is never called.
            let encoded = encoder.encode(OP_TEXT, b"first message").await;
            let (first, data) = encoded.frame(OP_TEXT, b"first message");
            assert_eq!(first, FIN | RSV1 | OP_TEXT);
            let first_frame = data.to_vec();
            drop(encoded);
            // The next send gets the frame left behind first, then its own message.
            let encoded = encoder.encode(OP_BINARY, b"second message").await;
            let (first, data) = encoded.frame(OP_BINARY, b"second message");
            assert_eq!((first, data), (FIN | RSV1 | OP_TEXT, &first_frame[..]));
            assert!(!encoded.finish());
            let encoded = encoder.encode(OP_BINARY, b"second message").await;
            let (first, data) = encoded.frame(OP_BINARY, b"second message");
            assert_eq!(first, FIN | RSV1 | OP_BINARY);
            // The peer's shared window decodes both, in order.
            assert_eq!(
                decoder.decode(&first_frame, 1024).unwrap(),
                b"first message"
            );
            assert_eq!(decoder.decode(data, 1024).unwrap(), b"second message");
            assert!(encoded.finish());
            // Nothing is left behind.
            let encoded = encoder.encode(OP_TEXT, b"third").await;
            assert_eq!(encoded.frame(OP_TEXT, b"third").0, FIN | RSV1 | OP_TEXT);
            assert!(encoded.finish());
        }

        #[test]
        fn messages_up_to_the_limit_decode_whatever_their_size() {
            // Growing the output buffer close to the limit once panicked.
            let mut compressor = Compress::new(Compression::default(), false);
            for limit in [256usize, 300, 512, 1000] {
                let mut decoder = negotiated(true).decoder();
                for size in 1..=limit {
                    let message: Vec<u8> = (0..size).map(|i| (i % 7) as u8).collect();
                    let data = deflate(&mut compressor, &message).unwrap();
                    compressor.reset();
                    assert_eq!(
                        decoder.decode(&data, limit).unwrap(),
                        message,
                        "size {size}, limit {limit}"
                    );
                }
                let data = deflate(&mut compressor, &vec![1; limit + 1]).unwrap();
                compressor.reset();
                assert_eq!(decoder.decode(&data, limit), Err(DecodeError::TooBig));
            }
        }
    }
}
