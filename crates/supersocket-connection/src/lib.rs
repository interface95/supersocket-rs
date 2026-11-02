//! Connection layer of supersocket-rs, shared by the server and the client.
//!
//! [`PackageReader`] reads from any [`AsyncRead`](tokio::io::AsyncRead) and cuts packages with a
//! pipeline filter. It holds no buffer between packages, enforces a maximum package length,
//! applies filter switches and turns a panicking filter into an error.
//!
//! [`SendQueue`] collects data to send from any number of tasks, and one writer task writes
//! whatever piled up in one go, with back-pressure and an optional send timeout.
//!
//! [`catch_panic`] and [`catch_panic_sync`] run application code and turn a panic into an error,
//! so it cannot unwind through the connection's own tasks.
//!
//! [`Rewind`] puts bytes that were read too early back in front of a stream.

#[cfg(feature = "gzip")]
mod gzip;
mod panic;
mod reader;
mod rewind;
mod send_queue;
mod stream;

#[cfg(feature = "gzip")]
pub use gzip::gzip;
pub use panic::{catch_panic, catch_panic_sync};
pub use reader::{PackageReader, ReadError, ReadOptions};
pub use rewind::Rewind;
pub use send_queue::SendQueue;
pub use stream::{BoxStream, Stream};

/// Formats and queues data on a session or a client sender through its `send_fmt` method,
/// without a `String` in between: `send_fmt!(session, "OK {sum}\r\n").await?`.
///
/// A macro rather than a plain call, because the arguments `format_args!` builds cannot live
/// across an `await` in a `Send` future; the macro lets go of them before the future is awaited.
#[macro_export]
macro_rules! send_fmt {
    ($sender:expr, $($arg:tt)*) => {{
        let sending = $sender.send_fmt(::core::format_args!($($arg)*));
        sending
    }};
}
