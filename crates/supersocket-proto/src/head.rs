/// Finds the empty line that ends an HTTP-style head at the start of `buf` and returns the
/// length of the head including that line. Lines may end with `\r\n` or `\n`. The first
/// `searched` bytes were already searched by an earlier call, so a head arriving in pieces is
/// scanned once. `None` when the head is not complete yet.
///
/// ```
/// use supersocket_proto::head_length;
///
/// let head = b"GET / HTTP/1.1\r\nHost: a\r\n\r\nbody";
/// assert_eq!(head_length(head, 0), Some(head.len() - 4));
/// assert_eq!(head_length(b"GET / HTTP/1.1\r\n", 0), None);
/// ```
pub fn head_length(buf: &[u8], searched: usize) -> Option<usize> {
    // The end may have begun in the last two bytes searched.
    let mut start = searched.saturating_sub(2);
    while let Some(offset) = memchr::memchr(b'\n', &buf[start..]) {
        let after = start + offset + 1;
        match &buf[after..] {
            [b'\n', ..] => return Some(after + 1),
            [b'\r', b'\n', ..] => return Some(after + 2),
            _ => start = after,
        }
    }
    None
}
