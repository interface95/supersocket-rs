use std::{
    collections::HashMap, fmt::Debug, future::Future, hash::Hash, sync::Arc, time::Duration,
};

use supersocket_proto::CommandPackage;

use crate::{
    BoxFuture, HandlerResult,
    server::{PackageError, PackageHandler, ServerBuilder, handle_with_limit},
    session::{Session, SessionState},
};

/// A package that names the command that should handle it.
pub trait KeyedPackage: Send + 'static {
    /// The type of the key.
    type Key: Eq + Hash + Clone + Debug + Send + Sync + 'static;

    /// The key of this package.
    fn key(&self) -> &Self::Key;
}

impl KeyedPackage for CommandPackage {
    type Key = String;

    fn key(&self) -> &String {
        &self.key
    }
}

/// Handles the packages whose key equals [`key`](Self::key).
///
/// ```
/// use supersocket_proto::CommandPackage;
/// use supersocket_server::{AsyncCommand, HandlerResult, Session};
///
/// struct Add;
///
/// impl AsyncCommand<CommandPackage> for Add {
///     fn key(&self) -> String {
///         "ADD".into()
///     }
///
///     async fn execute(&self, session: &Session, package: CommandPackage) -> HandlerResult {
///         let mut sum = 0i64;
///         for arg in &package.args {
///             sum += arg.parse::<i64>()?;
///         }
///         session.send(format!("{sum}\r\n")).await?;
///         Ok(())
///     }
/// }
/// ```
pub trait AsyncCommand<P: KeyedPackage, S = ()>: Send + Sync + 'static {
    /// The key of the packages this command handles.
    fn key(&self) -> P::Key;

    /// Handles one package.
    fn execute(
        &self,
        session: &Session<S>,
        package: P,
    ) -> impl Future<Output = HandlerResult> + Send;
}

/// Runs code before and after commands, for logging, authorization or metrics.
///
/// Register it for every command with [`Commands::filter`] or for one command with
/// [`AsyncCommandExt::with_filter`].
pub trait CommandFilter<P: KeyedPackage, S = ()>: Send + Sync + 'static {
    /// Called before the command runs. Returning `false` skips the command and the filters
    /// after this one.
    fn on_executing(
        &self,
        _session: &Session<S>,
        _package: &P,
    ) -> impl Future<Output = bool> + Send {
        async { true }
    }

    /// Called after the command ran, with its result.
    fn on_executed(
        &self,
        _session: &Session<S>,
        _key: &P::Key,
        _result: &HandlerResult,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Extension methods for [`AsyncCommand`].
pub trait AsyncCommandExt<P: KeyedPackage, S>: AsyncCommand<P, S> + Sized {
    /// Runs `filter` around this command only.
    fn with_filter<F: CommandFilter<P, S>>(self, filter: F) -> Filtered<Self, F> {
        Filtered {
            command: self,
            filter,
        }
    }
}

impl<P: KeyedPackage, S, C: AsyncCommand<P, S>> AsyncCommandExt<P, S> for C {}

/// A command with a filter of its own, created by [`AsyncCommandExt::with_filter`].
pub struct Filtered<C, F> {
    command: C,
    filter: F,
}

impl<P, S, C, F> AsyncCommand<P, S> for Filtered<C, F>
where
    P: KeyedPackage,
    S: SessionState,
    C: AsyncCommand<P, S>,
    F: CommandFilter<P, S>,
{
    fn key(&self) -> P::Key {
        self.command.key()
    }

    async fn execute(&self, session: &Session<S>, package: P) -> HandlerResult {
        if !self.filter.on_executing(session, &package).await {
            return Ok(());
        }
        let key = self.command.key();
        let result = self.command.execute(session, package).await;
        self.filter.on_executed(session, &key, &result).await;
        result
    }
}

trait DynCommand<P, S>: Send + Sync {
    fn execute<'a>(&'a self, session: &'a Session<S>, package: P) -> BoxFuture<'a, HandlerResult>;
}

impl<P: KeyedPackage, S: SessionState, C: AsyncCommand<P, S>> DynCommand<P, S> for C {
    fn execute<'a>(&'a self, session: &'a Session<S>, package: P) -> BoxFuture<'a, HandlerResult> {
        Box::pin(AsyncCommand::execute(self, session, package))
    }
}

trait DynCommandFilter<P: KeyedPackage, S>: Send + Sync {
    fn on_executing<'a>(&'a self, session: &'a Session<S>, package: &'a P) -> BoxFuture<'a, bool>;
    fn on_executed<'a>(
        &'a self,
        session: &'a Session<S>,
        key: &'a P::Key,
        result: &'a HandlerResult,
    ) -> BoxFuture<'a, ()>;
}

impl<P: KeyedPackage, S: SessionState, F: CommandFilter<P, S>> DynCommandFilter<P, S> for F {
    fn on_executing<'a>(&'a self, session: &'a Session<S>, package: &'a P) -> BoxFuture<'a, bool> {
        Box::pin(CommandFilter::on_executing(self, session, package))
    }

    fn on_executed<'a>(
        &'a self,
        session: &'a Session<S>,
        key: &'a P::Key,
        result: &'a HandlerResult,
    ) -> BoxFuture<'a, ()> {
        Box::pin(CommandFilter::on_executed(self, session, key, result))
    }
}

type UnknownHandler<P, S> =
    Arc<dyn Fn(Session<S>, P) -> BoxFuture<'static, HandlerResult> + Send + Sync>;

/// The commands of a server, set up in [`ServerBuilder::commands`].
pub struct Commands<P: KeyedPackage, S = ()> {
    commands: HashMap<P::Key, Box<dyn DynCommand<P, S>>>,
    filters: Vec<Box<dyn DynCommandFilter<P, S>>>,
    unknown: Option<UnknownHandler<P, S>>,
}

impl<P: KeyedPackage, S: SessionState> Commands<P, S> {
    /// Adds a command.
    ///
    /// # Panics
    ///
    /// Panics if a command with the same key was already added.
    pub fn add(&mut self, command: impl AsyncCommand<P, S>) -> &mut Self {
        let key = command.key();
        if self
            .commands
            .insert(key.clone(), Box::new(command))
            .is_some()
        {
            panic!("a command with key {key:?} is already registered");
        }
        self
    }

    /// Adds a filter that runs around every command. Filters run in the order they were added
    /// before the command, and in reverse order after it.
    pub fn filter(&mut self, filter: impl CommandFilter<P, S>) -> &mut Self {
        self.filters.push(Box::new(filter));
        self
    }

    /// Handles packages whose key matches no command. Without it they are ignored.
    pub fn on_unknown<F, Fut>(&mut self, handler: F) -> &mut Self
    where
        F: Fn(Session<S>, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
    {
        self.unknown = Some(Arc::new(move |session, package| {
            Box::pin(handler(session, package))
        }));
        self
    }

    async fn dispatch(&self, session: Session<S>, package: P) -> HandlerResult {
        let Some(command) = self.commands.get(package.key()) else {
            return match &self.unknown {
                Some(handler) => handler(session, package).await,
                None => {
                    tracing::debug!(key = ?package.key(), "no command for key");
                    Ok(())
                }
            };
        };
        if self.filters.is_empty() {
            return command.execute(&session, package).await;
        }
        for filter in &self.filters {
            if !filter.on_executing(&session, &package).await {
                return Ok(());
            }
        }
        let key = package.key().clone();
        let result = command.execute(&session, package).await;
        for filter in self.filters.iter().rev() {
            filter.on_executed(&session, &key, &result).await;
        }
        result
    }
}

impl<P: KeyedPackage, S: SessionState> ServerBuilder<P, S> {
    /// Dispatches each package to the command registered for its key. This becomes the package
    /// handler, replacing one set with [`on_package`](Self::on_package).
    pub fn commands(self, configure: impl FnOnce(&mut Commands<P, S>)) -> Self {
        let mut commands = Commands {
            commands: HashMap::new(),
            filters: Vec::new(),
            unknown: None,
        };
        configure(&mut commands);
        self.set_package_handler(Box::new(commands))
    }
}

impl<P: KeyedPackage, S: SessionState> PackageHandler<P, S> for Commands<P, S> {
    fn handle<'a>(
        &'a self,
        session: Session<S>,
        package: P,
        limit: Duration,
    ) -> BoxFuture<'a, Result<(), PackageError>> {
        Box::pin(handle_with_limit(limit, move || {
            self.dispatch(session, package)
        }))
    }
}
