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
