//! Graceful cancellation propagated to every long-lived task.
//!
//! Every source, sink, and ancillary task receives a [`ShutdownToken`] and
//! is expected to terminate cleanly when [`ShutdownToken::is_cancelled`]
//! returns `true` or [`ShutdownToken::cancelled`] resolves. The engine
//! holds the root token; child tokens are issued so subsystems can be
//! cancelled independently for testing while still inheriting from the
//! root.
//!
//! Backed by [`tokio_util::sync::CancellationToken`], which is the de facto
//! idiomatic primitive for this in the Tokio ecosystem.

use std::future::Future;

use tokio_util::sync::CancellationToken;

/// A cooperative cancellation handle.
///
/// Clones share the same cancellation state — calling [`cancel`](Self::cancel)
/// on any clone signals every holder. Child tokens (via
/// [`child`](Self::child)) inherit the parent's cancellation but can also be
/// cancelled in isolation, which is useful for restarting one subsystem
/// without taking down the whole engine.
#[derive(Debug, Clone, Default)]
pub struct ShutdownToken {
    inner: CancellationToken,
}

impl ShutdownToken {
    /// Construct a fresh root token.
    pub fn new() -> Self {
        Self {
            inner: CancellationToken::new(),
        }
    }

    /// Issue a child token that inherits this token's cancellation.
    ///
    /// Cancelling the parent cancels every child. Cancelling a child does
    /// not affect the parent — useful for restarting a single subsystem.
    pub fn child(&self) -> Self {
        Self {
            inner: self.inner.child_token(),
        }
    }

    /// Signal cancellation. Idempotent: calling twice is a no-op.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Whether cancellation has been signalled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Future that resolves when cancellation is signalled.
    ///
    /// Long-running loops should use this in a `tokio::select!` arm so they
    /// terminate promptly:
    ///
    /// ```ignore
    /// loop {
    ///     tokio::select! {
    ///         _ = shutdown.cancelled() => break,
    ///         event = source.next() => process(event).await,
    ///     }
    /// }
    /// ```
    pub fn cancelled(&self) -> impl Future<Output = ()> + '_ {
        self.inner.cancelled()
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_propagates_to_children() {
        let root = ShutdownToken::new();
        let child = root.child();
        let grandchild = child.child();

        assert!(!grandchild.is_cancelled());
        root.cancel();
        assert!(grandchild.is_cancelled());
    }

    #[tokio::test]
    async fn child_cancellation_does_not_affect_parent() {
        let root = ShutdownToken::new();
        let child = root.child();

        child.cancel();
        assert!(child.is_cancelled());
        assert!(!root.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_future_resolves_after_cancel() {
        let token = ShutdownToken::new();
        let waiter = token.clone();

        let handle = tokio::spawn(async move {
            waiter.cancelled().await;
        });

        token.cancel();
        handle.await.expect("shutdown waiter joined");
    }
}
