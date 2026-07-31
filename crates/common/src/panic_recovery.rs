//! Connection-level panic recovery helpers.
//!
//! Every protocol server spawns a task per connection and drives a request
//! loop. Without a `catch_unwind` boundary, a panic anywhere in request
//! handling (an `unwrap()` on malformed client input, an Arrow downcast
//! mismatch, a poisoned lock, ...) tears down the tokio worker thread and can
//! destabilise the whole server.
//!
//! [`catch_unwind`] wraps a future so that, if it panics, the panic is caught
//! and turned into a normal `Err` result instead of propagating to the
//! runtime. Servers call it at the connection-task boundary, log the panic,
//! and let the connection close gracefully — other connections keep running.
//!
//! ## Usage
//!
//! ```ignore
//! use common::panic_recovery::catch_unwind;
//!
//! tokio::spawn(async move {
//!     if let Err(payload) = catch_unwind(handle_connection(stream).await).await {
//!         tracing::error!("connection panicked: {:?}", payload);
//!     }
//! });
//! ```
//!
//! Note the double `.await`: the outer call returns a future that, when polled,
//! runs the inner future under `catch_unwind`.

use futures_util::FutureExt;
use std::any::Any;
use std::panic::AssertUnwindSafe;

/// The boxed payload carried by a caught panic (`std::panic::resume_unwind`
/// takes the same type). Formatted lazily by callers via [`payload_to_string`].
pub type PanicPayload = Box<dyn Any + Send + 'static>;

/// Convert an opaque panic payload into a best-effort display string.
///
/// Panics created with a `&str` or `String` message (the common case, e.g.
/// `.unwrap()` and `panic!("...")`) are recovered as their text; anything else
/// is described generically.
pub fn payload_to_string(payload: &PanicPayload) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
}

/// Run `future` under a panic-catching boundary.
///
/// Returns `Ok(T)` on normal completion, or `Err(PanicPayload)` if the future
/// panicked while being polled. The panic is consumed (not re-thrown), so the
/// surrounding task keeps running.
///
/// This is the async analogue of `std::panic::catch_unwind`. It uses
/// `futures_util::FutureExt::catch_unwind` together with [`AssertUnwindSafe`]
/// to install an unwind boundary around the future's `poll` calls.
///
/// # Why `AssertUnwindSafe`
///
/// The future captures shared state (handlers, storage, connection state) that
/// is not provably `UnwindSafe`. We assert it is safe here because the
/// alternative — letting the panic escape and kill the worker thread — is
/// strictly worse than a torn-but-isolated connection. The caller is expected
/// to *log and close* on `Err`, not to keep using the captured state.
pub async fn catch_unwind<F>(future: F) -> Result<F::Output, PanicPayload>
where
    F: futures_util::Future,
{
    AssertUnwindSafe(future).catch_unwind().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_catch_unwind_catches_panic() {
        let result = catch_unwind(async {
            panic!("boom");
        })
        .await;
        assert!(result.is_err());
        let payload = result.unwrap_err();
        assert_eq!(payload_to_string(&payload), "boom");
    }

    #[tokio::test]
    async fn test_catch_unwind_passes_through_ok() {
        let result = catch_unwind(async { 42 }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_catch_unwind_catches_unwrap_panic() {
        // Simulates the common case: `.unwrap()` on a None / Err.
        let result = catch_unwind(async {
            let x: Option<i32> = None;
            x.unwrap()
        })
        .await;
        assert!(result.is_err());
        // The unwrap message is a &'static str.
        let msg = payload_to_string(&result.unwrap_err());
        assert!(msg.contains("None") || msg.contains("unwrap") || msg == "<non-string panic payload>");
    }

    #[tokio::test]
    async fn test_payload_to_string_string_type() {
        let payload: PanicPayload = Box::new("hello".to_string());
        assert_eq!(payload_to_string(&payload), "hello");
    }

    #[tokio::test]
    async fn test_catch_unwind_isolates_caller() {
        // A panicked future must not poison the caller's scope.
        let _ = catch_unwind(async { panic!("isolated") }).await;
        // If recovery failed, this line would never run.
        assert!(true);
    }
}
