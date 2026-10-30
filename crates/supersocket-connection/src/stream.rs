use tokio::io::{AsyncRead, AsyncWrite};

/// A byte stream a connection runs on: TCP, TLS, a Unix socket or anything else that reads and
/// writes asynchronously.
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Stream for T {}

/// A boxed [`Stream`].
pub type BoxStream = Box<dyn Stream>;
