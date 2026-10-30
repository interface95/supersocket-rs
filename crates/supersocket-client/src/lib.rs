//! Client side of supersocket-rs, modeled on SuperSocket's `EasyClient`.
//!
//! An [`EasyClient`] opens its connection through a chain of [`Connector`]s: a
//! [`TcpConnector`] or a proxy first, then layers such as TLS. It cuts what the server sends into
//! packages with the same pipeline filters the server uses, and sends through a queue that writes
//! in batches, like a server session.
//!
//! ```no_run
//! use supersocket_client::EasyClient;
//! use supersocket_proto::LineFilter;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = EasyClient::builder(LineFilter::default()).connect("127.0.0.1:4040").await?;
//! client.send("hello\r\n").await?;
//! if let Some(reply) = client.receive().await? {
//!     println!("{}", reply.text);
//! }
//! client.close().await;
//! # Ok(())
//! # }
//! ```

mod client;
mod connector;
mod endpoint;
#[cfg(feature = "proxy")]
mod proxy;
#[cfg(feature = "tls")]
mod tls;

pub use client::{ClientBuilder, ClientOptions, ClientSender, EasyClient};
#[cfg(feature = "gzip")]
pub use connector::GzipConnector;
pub use connector::{Connection, Connector, TcpConnector};
pub use endpoint::{Endpoint, ToEndpoint};
#[cfg(feature = "proxy")]
pub use proxy::{HttpProxy, Socks4Proxy, Socks5Proxy};
pub use supersocket_connection::ReadError;
pub use supersocket_connection::{BoxStream, Stream};
#[cfg(feature = "tls")]
pub use tls::TlsConnector;
#[cfg(feature = "tls")]
pub use tokio_rustls::rustls;

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
