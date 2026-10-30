use bytes::BytesMut;

use crate::ProtocolError;

/// Cuts a byte stream into packages.
///
/// `buf` holds the bytes received but not consumed yet. When it starts with a complete package,
/// remove that package's bytes from `buf` and return `Ok(Some(package))`. Return `Ok(None)` when
/// more data is needed; the filter may still consume bytes it has already examined. Returning an
/// error closes the connection with a protocol error.
pub trait PipelineFilter: Send + 'static {
    /// The package type this filter produces.
    type Package: Send + 'static;

    /// Tries to decode one package from the front of `buf`.
    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Package>, ProtocolError>;

    /// Called after every [`filter`](Self::filter) call. Returning a filter makes the connection
    /// use it for the remaining bytes, which is how protocols switch mid-stream.
    fn next_filter(&mut self) -> Option<BoxFilter<Self::Package>> {
        None
    }

    /// Called after a complete package was decoded or the filter was switched away from, so the
    /// filter can drop per-package state.
    fn reset(&mut self) {}
}

/// A boxed [`PipelineFilter`].
pub type BoxFilter<P> = Box<dyn PipelineFilter<Package = P>>;

/// Encodes packages into bytes to send.
pub trait PackageEncoder<P: ?Sized>: Send + Sync {
    /// Appends the encoded `package` to `buf`.
    fn encode(&self, package: &P, buf: &mut BytesMut) -> Result<(), ProtocolError>;
}

/// Extension methods for [`PipelineFilter`].
pub trait PipelineFilterExt: PipelineFilter + Sized {
    /// Switches the connection to the filter returned by `next` after this filter decodes a
    /// package.
    fn then_switch_to<N>(self, next: N) -> SwitchAfterPackage<Self, N>
    where
        N: FnMut() -> BoxFilter<Self::Package> + Send + 'static,
    {
        SwitchAfterPackage {
            inner: self,
            next,
            done: false,
        }
    }
}

impl<F: PipelineFilter> PipelineFilterExt for F {}

/// A filter that switches to another filter after decoding one package, created by
/// [`PipelineFilterExt::then_switch_to`].
pub struct SwitchAfterPackage<F, N> {
    inner: F,
    next: N,
    done: bool,
}

impl<F, N> PipelineFilter for SwitchAfterPackage<F, N>
where
    F: PipelineFilter,
    N: FnMut() -> BoxFilter<F::Package> + Send + 'static,
{
    type Package = F::Package;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<F::Package>, ProtocolError> {
        let package = self.inner.filter(buf)?;
        self.done = package.is_some();
        Ok(package)
    }

    fn next_filter(&mut self) -> Option<BoxFilter<F::Package>> {
        if self.done {
            Some((self.next)())
        } else {
            self.inner.next_filter()
        }
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}
