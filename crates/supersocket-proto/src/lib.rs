//! Protocol layer of supersocket-rs: pipeline filters that cut a byte stream into packages.
//!
//! A [`PipelineFilter`] sees the bytes that were received but not consumed yet. When they contain
//! a complete package it removes those bytes and returns the decoded package; otherwise it asks
//! for more data. A filter can hand the rest of the stream to another filter through
//! [`PipelineFilter::next_filter`], which is how protocol switches such as a WebSocket upgrade
//! work.
//!
//! Built-in filters cover the common framings:
//!
//! | Filter | Framing |
//! |---|---|
//! | [`LineFilter`] | text lines ending with `\r\n` |
//! | [`CommandLineFilter`] | command lines such as `ADD 1 2\r\n` |
//! | [`TerminatorFilter`] | any terminator |
//! | [`FixedSizeFilter`] | packages of a fixed size |
//! | [`FixedHeaderFilter`] | a fixed-size header that carries the body length |
//! | [`BeginEndMarkFilter`] | packages wrapped in begin and end marks |
//! | [`TransparentFilter`] | no framing, every read is a package |

mod error;
mod filter;
mod filters;
mod package;

pub use bytes;
pub use error::ProtocolError;
pub use filter::{
    BoxFilter, PackageEncoder, PipelineFilter, PipelineFilterExt, SwitchAfterPackage,
};
#[cfg(feature = "codec")]
pub use filters::CodecFilter;
pub use filters::{
    BeginEndMarkFilter, CommandLineFilter, Decoder, FixedHeaderFilter, FixedHeaderProtocol,
    FixedSizeFilter, LineFilter, TerminatorFilter, TransparentFilter,
};
pub use package::{CommandPackage, TextPackage};
