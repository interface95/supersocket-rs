//! Commands whose package body is JSON, like SuperSocket's `JsonAsyncCommand<T>`.

use std::future::Future;

use serde::de::DeserializeOwned;
use supersocket_proto::CommandPackage;

use crate::{AsyncCommand, HandlerResult, Session, SessionState};

/// A command whose [`CommandPackage`] body is JSON, decoded into [`Value`](Self::Value) before
/// the command runs, like SuperSocket's `JsonAsyncCommand<T>`. Register it wrapped in [`Json`].
///
/// ```
/// use serde::Deserialize;
/// use supersocket_proto::CommandLineFilter;
/// use supersocket_server::{HandlerResult, Json, JsonCommand, Server, Session};
///
/// #[derive(Deserialize)]
/// struct Credentials {
///     user: String,
///     token: String,
/// }
///
/// struct Login;
///
/// impl JsonCommand for Login {
///     type Value = Credentials;
///
///     fn key(&self) -> String {
///         "LOGIN".into()
///     }
///
///     async fn execute(&self, session: &Session, credentials: Credentials) -> HandlerResult {
///         let _ = credentials.token;
///         session.send(format!("OK {}\r\n", credentials.user)).await?;
///         Ok(())
///     }
/// }
///
/// // A line such as `LOGIN {"user": "alice", "token": "secret"}` runs it.
/// let builder = Server::builder(CommandLineFilter::default).commands(|commands| {
///     commands.add(Json(Login));
/// });
/// ```
pub trait JsonCommand<S = ()>: Send + Sync + 'static {
    /// What the body is decoded into.
    type Value: DeserializeOwned + Send;

    /// The key of the packages this command handles.
    fn key(&self) -> String;

    /// Handles one package, whose body was decoded into `value`.
    fn execute(
        &self,
        session: &Session<S>,
        value: Self::Value,
    ) -> impl Future<Output = HandlerResult> + Send;
}

/// Runs a [`JsonCommand`] as an [`AsyncCommand`] on command packages. A body that is not valid
/// JSON for the command's value fails like a handler error, so the error handler decides what
/// happens to the session. An empty body is decoded as JSON `null`, which an `Option` accepts.
pub struct Json<C>(pub C);

impl<S: SessionState, C: JsonCommand<S>> AsyncCommand<CommandPackage, S> for Json<C> {
    fn key(&self) -> String {
        self.0.key()
    }

    async fn execute(&self, session: &Session<S>, package: CommandPackage) -> HandlerResult {
        let value = decode(&package.body)?;
        self.0.execute(session, value).await
    }
}

/// Decodes a JSON body; an empty body is `null`.
#[doc(hidden)]
pub fn decode<T: DeserializeOwned>(body: &str) -> Result<T, serde_json::Error> {
    let body = body.trim();
    serde_json::from_str(if body.is_empty() { "null" } else { body })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bodies_are_null() {
        assert_eq!(decode::<Option<u8>>("").unwrap(), None);
        assert_eq!(decode::<Option<u8>>(" 7 ").unwrap(), Some(7));
        assert!(decode::<u8>("").is_err());
    }
}
