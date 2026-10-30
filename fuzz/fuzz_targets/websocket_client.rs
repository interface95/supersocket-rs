#![no_main]

use libfuzzer_sys::fuzz_target;

// Like `websocket_server`, for what a server sends: a handshake response and unmasked frames.
fuzz_target!(|data: &[u8]| {
    let [size, chunk, stream @ ..] = data else {
        return;
    };
    let max_size = 1usize << (size % 16 + 4);
    supersocket_websocket::fuzzing::pipeline(false, max_size, usize::from(*chunk) + 1, stream);
});
