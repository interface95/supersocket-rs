#![no_main]

use libfuzzer_sys::fuzz_target;

// The first byte picks the maximum package length, the second how many bytes arrive at a time;
// the rest is what a client sends: a handshake request and frames.
fuzz_target!(|data: &[u8]| {
    let [size, chunk, stream @ ..] = data else {
        return;
    };
    let max_size = 1usize << (size % 16 + 4);
    supersocket_websocket::fuzzing::pipeline(true, max_size, usize::from(*chunk) + 1, stream);
});
