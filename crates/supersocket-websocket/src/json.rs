//! WebSocket commands whose message body is JSON; see [`JsonWebSocketCommand`].

use std::future::Future;

use serde::de::DeserializeOwned;
use supersocket_proto::CommandPackage;
use supersocket_server::{HandlerResult, Json};

use crate::{WebSocketCommand, WebSocketSession};

/// A WebSocket command whose [`CommandPackage`] body is JSON, decoded into
/// [`Value`](Self::Value) before the command runs: the WebSocket version of
/// [`JsonCommand`](supersocket_server::JsonCommand). With
/// [`WebSocketBuilder::commands`](crate::WebSocketBuilder::commands), a text message such as
/// `LOGIN {"user": "alice"}` runs the command registered as `LOGIN`. Register it wrapped in
/// [`Json`]. An empty body is decoded as JSON `null`.
///
/// ```
/// use serde::Deserialize;
/// use supersocket_server::HandlerResult;
/// use supersocket_websocket::{Json, JsonWebSocketCommand, WebSocketServer, WebSocketSession};
///
/// #[derive(Deserialize)]
/// struct Join {
///     room: String,
/// }
///
/// struct JoinRoom;
///
/// impl JsonWebSocketCommand for JoinRoom {
///     type Value = Join;
///
///     fn key(&self) -> String {
///         "JOIN".into()
///     }
///
///     async fn execute(&self, session: &WebSocketSession, join: Join) -> HandlerResult {
///         session.send_text(&format!("joined {}", join.room)).await?;
///         Ok(())
///     }
/// }
///
/// let builder = WebSocketServer::builder().commands(|commands| {
///     commands.add(Json(JoinRoom));
/// });
/// ```
pub trait JsonWebSocketCommand<S = ()>: Send + Sync + 'static {
    /// What the body is decoded into.
    type Value: DeserializeOwned + Send;

    /// The key of the packages this command handles.
    fn key(&self) -> String;

    /// Handles one package, whose body was decoded into `value`.
    fn execute(
        &self,
        session: &WebSocketSession<S>,
        value: Self::Value,
    ) -> impl Future<Output = HandlerResult> + Send;
}

impl<S, C> WebSocketCommand<CommandPackage, S> for Json<C>
where
    S: Send + Sync + 'static,
    C: JsonWebSocketCommand<S>,
{
    fn key(&self) -> String {
        self.0.key()
    }

    async fn execute(
        &self,
        session: &WebSocketSession<S>,
        package: CommandPackage,
    ) -> HandlerResult {
        let value = supersocket_server::decode_json_body(&package.body)?;
        self.0.execute(session, value).await
    }
}
