#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    supersocket_server::fuzzing::proxy_protocol_header(data);
});
