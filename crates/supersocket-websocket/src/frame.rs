//! The frame layout of RFC 6455 section 5.2.

use crate::CloseCode;

pub(crate) const OP_CONTINUATION: u8 = 0x0;
pub(crate) const OP_TEXT: u8 = 0x1;
pub(crate) const OP_BINARY: u8 = 0x2;
pub(crate) const OP_CLOSE: u8 = 0x8;
pub(crate) const OP_PING: u8 = 0x9;
pub(crate) const OP_PONG: u8 = 0xA;

/// The largest frame header: two bytes, an eight-byte length and a four-byte masking key.
pub(crate) const MAX_HEADER_SIZE: usize = 14;
/// The largest payload of a control frame.
pub(crate) const MAX_CONTROL_PAYLOAD: usize = 125;

/// Writes the header of an unfragmented frame into `header` and returns its length. The payload is
/// masked with `mask` when one is given, as a client's must be.
pub(crate) fn encode_header(
    opcode: u8,
    length: usize,
    mask: Option<[u8; 4]>,
    header: &mut [u8; MAX_HEADER_SIZE],
) -> usize {
    header[0] = 0x80 | opcode;
    let mask_bit = if mask.is_some() { 0x80 } else { 0 };
    let mut size = match length {
        0..=125 => {
            header[1] = mask_bit | length as u8;
            2
        }
        126..=0xFFFF => {
            header[1] = mask_bit | 126;
            header[2..4].copy_from_slice(&(length as u16).to_be_bytes());
            4
        }
        _ => {
            header[1] = mask_bit | 127;
            header[2..10].copy_from_slice(&(length as u64).to_be_bytes());
            10
        }
    };
    if let Some(mask) = mask {
        header[size..size + 4].copy_from_slice(&mask);
        size += 4;
    }
    size
}

/// XORs `data` with the masking key, which masks plain data and unmasks masked data.
pub(crate) fn apply_mask(data: &mut [u8], mask: [u8; 4]) {
    // Eight bytes at a time; every chunk starts at a multiple of the key's length.
    let wide = u64::from_ne_bytes([
        mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
    ]);
    let mut chunks = data.chunks_exact_mut(8);
    for chunk in &mut chunks {
        let word = u64::from_ne_bytes(chunk.try_into().unwrap()) ^ wide;
        chunk.copy_from_slice(&word.to_ne_bytes());
    }
    for (byte, key) in chunks.into_remainder().iter_mut().zip(mask.iter().cycle()) {
        *byte ^= key;
    }
}

/// Writes the payload of a close frame into `payload` and returns its length: nothing without a
/// code, otherwise the code and as much of `reason` as fits.
pub(crate) fn close_payload(
    code: Option<CloseCode>,
    reason: &str,
    payload: &mut [u8; MAX_CONTROL_PAYLOAD],
) -> usize {
    let Some(code) = code else {
        return 0;
    };
    payload[..2].copy_from_slice(&code.code().to_be_bytes());
    let reason = truncate(reason, MAX_CONTROL_PAYLOAD - 2);
    payload[2..2 + reason.len()].copy_from_slice(reason.as_bytes());
    2 + reason.len()
}

/// The longest prefix of `text` that fits in `limit` bytes without splitting a character.
fn truncate(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_every_length_form() {
        let mut header = [0; MAX_HEADER_SIZE];
        assert_eq!(encode_header(OP_TEXT, 5, None, &mut header), 2);
        assert_eq!(header[..2], [0x81, 5]);
        assert_eq!(encode_header(OP_BINARY, 126, None, &mut header), 4);
        assert_eq!(header[..4], [0x82, 126, 0, 126]);
        assert_eq!(encode_header(OP_BINARY, 70_000, None, &mut header), 10);
        assert_eq!(header[..10], [0x82, 127, 0, 0, 0, 0, 0, 1, 0x11, 0x70]);
        let mask = Some([1, 2, 3, 4]);
        assert_eq!(encode_header(OP_PING, 0, mask, &mut header), 6);
        assert_eq!(header[..6], [0x89, 0x80, 1, 2, 3, 4]);
    }

    #[test]
    fn masking_twice_restores_the_data() {
        let original: Vec<u8> = (0..=255).collect();
        for length in [0, 1, 7, 8, 9, 100, 256] {
            let mut data = original[..length].to_vec();
            apply_mask(&mut data, [0x12, 0x34, 0x56, 0x78]);
            for (index, byte) in data.iter().enumerate() {
                assert_eq!(*byte, original[index] ^ [0x12, 0x34, 0x56, 0x78][index % 4]);
            }
            apply_mask(&mut data, [0x12, 0x34, 0x56, 0x78]);
            assert_eq!(data, original[..length]);
        }
    }

    #[test]
    fn close_reasons_are_cut_at_a_character_boundary() {
        let mut payload = [0; MAX_CONTROL_PAYLOAD];
        assert_eq!(close_payload(None, "ignored", &mut payload), 0);
        assert_eq!(
            close_payload(Some(CloseCode::NORMAL), "bye", &mut payload),
            5
        );
        assert_eq!(payload[..5], [0x03, 0xE8, b'b', b'y', b'e']);
        // 62 two-byte characters do not fit in the 123 bytes left after the code.
        let length = close_payload(Some(CloseCode::NORMAL), &"é".repeat(62), &mut payload);
        assert_eq!(length, 2 + 122);
        assert!(std::str::from_utf8(&payload[2..length]).is_ok());
    }
}
