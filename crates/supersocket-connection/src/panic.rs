use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    task::Poll,
};

/// Runs `future`, turning a panic inside it into an error with the panic message, so application
/// code cannot unwind through the code that runs it.
pub async fn catch_panic<F: Future>(future: F) -> Result<F::Output, String> {
    let mut future = pin!(future);
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(panic_message(&*panic))),
        },
    )
    .await
}

/// Runs `f`, turning a panic into an error with the panic message.
pub fn catch_panic_sync<T>(f: impl FnOnce() -> T) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|panic| panic_message(&*panic))
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic with a non-string payload".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn panics_become_errors() {
        assert_eq!(catch_panic(async { 1 }).await, Ok(1));
        let error = catch_panic(async {
            tokio::task::yield_now().await;
            panic!("boom {}", 42);
        })
        .await;
        assert_eq!(error, Err::<(), _>("boom 42".to_owned()));
        assert_eq!(
            catch_panic_sync(|| panic!("static")),
            Err::<(), _>("static".to_owned())
        );
    }
}
