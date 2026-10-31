//! Commands for WebSocket messages and handlers per subprotocol, like SuperSocket's `UseCommand`
//! and `UseWebSocketMessageHandler(protocol, ..)` for its WebSocket server.

use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use supersocket_proto::{CommandPackage, ProtocolError};
use supersocket_server::{
    AsyncCommand, CommandFilter, Commands, HandlerResult, KeyedPackage, Session,
};

use crate::{Message, WebSocketSession, WebSocketState};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Handles the messages of a session: the server's default handler or a subprotocol's.
pub(crate) type MessageHandler<S> =
    Arc<dyn Fn(WebSocketSession<S>, Message) -> BoxFuture<HandlerResult> + Send + Sync>;

/// Handles the WebSocket messages whose key equals [`key`](Self::key), once a mapper turned them
/// into packages of type `P`; see [`WebSocketCommands`]. The WebSocket version of
/// [`AsyncCommand`].
///
/// ```
/// use supersocket_proto::CommandPackage;
/// use supersocket_server::HandlerResult;
/// use supersocket_websocket::{WebSocketCommand, WebSocketServer, WebSocketSession};
///
/// struct Add;
///
/// impl WebSocketCommand<CommandPackage> for Add {
///     fn key(&self) -> String {
///         "ADD".into()
///     }
///
///     async fn execute(&self, session: &WebSocketSession, package: CommandPackage) -> HandlerResult {
///         let mut sum = 0i64;
///         for arg in package.args() {
///             sum += arg.parse::<i64>()?;
///         }
///         session.send_text(&sum.to_string()).await?;
///         Ok(())
///     }
/// }
///
/// // A text message "ADD 1 2" is answered with "3".
/// let builder = WebSocketServer::builder().commands(|commands| {
///     commands.add(Add);
/// });
/// ```
pub trait WebSocketCommand<P: KeyedPackage, S = ()>: Send + Sync + 'static {
    /// The key of the packages this command handles.
    fn key(&self) -> P::Key;

    /// Handles one package.
    fn execute(
        &self,
        session: &WebSocketSession<S>,
        package: P,
    ) -> impl Future<Output = HandlerResult> + Send;
}

/// Runs a WebSocket command through the server's command dispatch.
struct Adapted<C>(C);

impl<P, S, C> AsyncCommand<P, WebSocketState<S>> for Adapted<C>
where
    P: KeyedPackage,
    S: Send + Sync + 'static,
    C: WebSocketCommand<P, S>,
{
    fn key(&self) -> P::Key {
        self.0.key()
    }

    async fn execute(&self, session: &Session<WebSocketState<S>>, package: P) -> HandlerResult {
        let session = WebSocketSession::from(session.clone());
        self.0.execute(&session, package).await
    }
}

/// The commands of a WebSocket server or of one subprotocol, set up in
/// [`WebSocketBuilder::commands`](crate::WebSocketBuilder::commands) or
/// [`SubprotocolBuilder::commands`]. Each message is mapped to a package and dispatched to the
/// command registered for the package's key, like SuperSocket's `UseCommand` with a package
/// mapper.
pub struct WebSocketCommands<P: KeyedPackage, S = ()> {
    commands: Commands<P, WebSocketState<S>>,
}

impl<P: KeyedPackage, S: Send + Sync + 'static> WebSocketCommands<P, S> {
    pub(crate) fn new() -> Self {
        Self {
            commands: Commands::new(),
        }
    }

    /// Adds a command.
    ///
    /// # Panics
    ///
    /// Panics if a command with the same key was already added.
    pub fn add(&mut self, command: impl WebSocketCommand<P, S>) -> &mut Self {
        self.commands.add(Adapted(command));
        self
    }

    /// Adds a filter that runs around every command; see [`Commands::filter`]. A filter sees the
    /// underlying session; `WebSocketSession::from(session.clone())` gives the WebSocket one.
    pub fn filter(&mut self, filter: impl CommandFilter<P, WebSocketState<S>>) -> &mut Self {
        self.commands.filter(filter);
        self
    }

    /// Handles packages whose key matches no command. Without it they are ignored.
    pub fn on_unknown<F, Fut>(&mut self, handler: F) -> &mut Self
    where
        F: Fn(WebSocketSession<S>, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        self.commands
            .on_unknown(move |session, package| handler(WebSocketSession::from(session), package));
        self
    }

    /// The message handler: maps each message with `mapper` and dispatches the package. A mapper
    /// that returns `Ok(None)` skips the message; an error is handled like one from a handler.
    pub(crate) fn into_handler<M>(self, mapper: M) -> MessageHandler<S>
    where
        M: Fn(Message) -> Result<Option<P>, ProtocolError> + Send + Sync + 'static,
    {
        let commands = Arc::new(self.commands);
        Arc::new(move |session: WebSocketSession<S>, message: Message| {
            let commands = commands.clone();
            let mapped = mapper(message);
            Box::pin(async move {
                match mapped? {
                    Some(package) => commands.dispatch(session.session().clone(), package).await,
                    None => Ok(()),
                }
            })
        })
    }
}

/// Maps a text message to a [`CommandPackage`] like a command line: the first word is the key,
/// the rest the body. Binary messages are skipped.
pub(crate) fn text_command(message: Message) -> Result<Option<CommandPackage>, ProtocolError> {
    match message {
        Message::Text(text) => CommandPackage::decode(Bytes::from(text)).map(Some),
        Message::Binary(_) => Ok(None),
    }
}

/// Sets up the handling of one subprotocol's messages; see
/// [`WebSocketBuilder::subprotocol`](crate::WebSocketBuilder::subprotocol). Without a handler,
/// the subprotocol's messages go to the server's default handler.
pub struct SubprotocolBuilder<S = ()> {
    pub(crate) handler: Option<MessageHandler<S>>,
}

impl<S: Send + Sync + 'static> SubprotocolBuilder<S> {
    pub(crate) fn new() -> Self {
        Self { handler: None }
    }

    /// Handles every text and binary message of the subprotocol.
    pub fn on_message<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WebSocketSession<S>, Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        self.handler = Some(Arc::new(move |session, message| {
            Box::pin(handler(session, message))
        }));
        self
    }

    /// Dispatches text messages to commands like command lines: the first word is the key, the
    /// rest the arguments; see [`WebSocketCommands`]. Binary messages are ignored.
    pub fn commands(
        self,
        configure: impl FnOnce(&mut WebSocketCommands<CommandPackage, S>),
    ) -> Self {
        self.commands_with(text_command, configure)
    }

    /// Dispatches messages to commands, with `mapper` turning each message into a package of
    /// your own type; `Ok(None)` skips a message.
    pub fn commands_with<P, M>(
        mut self,
        mapper: M,
        configure: impl FnOnce(&mut WebSocketCommands<P, S>),
    ) -> Self
    where
        P: KeyedPackage,
        M: Fn(Message) -> Result<Option<P>, ProtocolError> + Send + Sync + 'static,
    {
        let mut commands = WebSocketCommands::new();
        configure(&mut commands);
        self.handler = Some(commands.into_handler(mapper));
        self
    }
}
