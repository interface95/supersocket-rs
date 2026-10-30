#![no_main]

use libfuzzer_sys::fuzz_target;
use supersocket_fuzz::{LengthPrefixed, read_all};
use supersocket_proto::{
    BeginEndMarkFilter, BoxFilter, CodecFilter, CommandLineFilter, FixedHeaderFilter,
    FixedSizeFilter, LineFilter, TerminatorFilter, TextPackage,
};
use tokio_util::codec::LengthDelimitedCodec;

// The first byte picks the filter, the second the maximum package length and the third how
// many bytes arrive at a time; the rest is the stream.
fuzz_target!(|data: &[u8]| {
    let [kind, size, chunk, stream @ ..] = data else {
        return;
    };
    let max_size = 1usize << (size % 12 + 4);
    let chunk = usize::from(*chunk) + 1;
    match kind % 7 {
        0 => read_all(Box::new(LineFilter::default()), max_size, chunk, stream),
        1 => read_all(Box::new(CommandLineFilter::default()), max_size, chunk, stream),
        2 => read_all(
            Box::new(FixedHeaderFilter::<LengthPrefixed>::default()),
            max_size,
            chunk,
            stream,
        ),
        3 => read_all(
            Box::new(BeginEndMarkFilter::new(&b"<"[..], &b">"[..], TextPackage::decode)),
            max_size,
            chunk,
            stream,
        ),
        4 => read_all(
            Box::new(FixedSizeFilter::new(usize::from(*size) + 1, Ok)),
            max_size,
            chunk,
            stream,
        ),
        5 => read_all(
            Box::new(TerminatorFilter::new(&b"##"[..], TextPackage::decode)),
            max_size,
            chunk,
            stream,
        ),
        _ => {
            let filter: BoxFilter<_> = Box::new(CodecFilter::new(LengthDelimitedCodec::new()));
            read_all(filter, max_size, chunk, stream)
        }
    }
});
