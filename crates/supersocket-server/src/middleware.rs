use std::future::Future;

use crate::{
    BoxFuture,
    server::ServerInfo,
    session::{Session, SessionState},
};

/// Sees sessions come and go and can reject new ones.
///
/// Middleware run in ascending [`order`](Self::order) when a session connects and in the reverse
/// order when it closes.
///
/// ```
/// use supersocket_server::{Middleware, Session, SessionState};
///
/// /// Rejects sessions beyond a limit.
/// struct ConnectionLimit(usize);
///
/// impl<S: SessionState> Middleware<S> for ConnectionLimit {
///     async fn register_session(&self, session: &Session<S>) -> bool {
///         session.server().session_count() < self.0
///     }
/// }
/// ```
pub trait Middleware<S = ()>: Send + Sync + 'static {
    /// Position among the middleware; lower runs first. Default: 0.
    fn order(&self) -> i32 {
        0
    }

    /// Called when the server starts.
    fn start(&self, _server: &ServerInfo<S>) {}

    /// Called after the server stopped.
    fn shutdown(&self, _server: &ServerInfo<S>) {}

    /// Called for every new session before it is registered. Returning `false` rejects the
    /// session: its connection is closed without the connected and closed events.
    fn register_session(&self, _session: &Session<S>) -> impl Future<Output = bool> + Send {
        async { true }
    }

    /// Called when a registered session closes.
    fn unregister_session(&self, _session: &Session<S>) -> impl Future<Output = ()> + Send {
        async {}
    }
}

// Traits with async methods cannot be used as trait objects directly, so the server keeps them
// behind this boxed-future version.
pub(crate) trait DynMiddleware<S>: Send + Sync {
    fn order(&self) -> i32;
    fn start(&self, server: &ServerInfo<S>);
    fn shutdown(&self, server: &ServerInfo<S>);
    fn register_session<'a>(&'a self, session: &'a Session<S>) -> BoxFuture<'a, bool>;
    fn unregister_session<'a>(&'a self, session: &'a Session<S>) -> BoxFuture<'a, ()>;
}

impl<S: SessionState, M: Middleware<S>> DynMiddleware<S> for M {
    fn order(&self) -> i32 {
        Middleware::order(self)
    }

    fn start(&self, server: &ServerInfo<S>) {
        Middleware::start(self, server);
    }

    fn shutdown(&self, server: &ServerInfo<S>) {
        Middleware::shutdown(self, server);
    }

    fn register_session<'a>(&'a self, session: &'a Session<S>) -> BoxFuture<'a, bool> {
        Box::pin(Middleware::register_session(self, session))
    }

    fn unregister_session<'a>(&'a self, session: &'a Session<S>) -> BoxFuture<'a, ()> {
        Box::pin(Middleware::unregister_session(self, session))
    }
}
