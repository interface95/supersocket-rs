#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    supersocket_websocket::fuzzing::handshake_request(data);
});
