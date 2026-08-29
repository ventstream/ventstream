//! Crash-fast handling for credential-classified reconnect failures.
//!
//! A stale password (e.g. a rotated RDS master password baked into the
//! pod environment) cannot heal from inside the process: the source must
//! stop retrying and exit nonzero so the supervisor restarts the pod
//! with fresh credentials, after which sink-confirmed-cursor resume
//! recovers normally.

use std::fmt::Display;

/// Consecutive credential-classified failures tolerated before a source
/// stops retrying — absorbs a rotation race (old and new password briefly
/// coexisting) without letting a stale-credential pod retry forever.
pub const MAX_CONSECUTIVE_CREDENTIAL_FAILURES: u32 = 5;

/// Per-retry-loop budget: credential failures increment, a successful
/// (re)connect resets, transient failures leave it unchanged.
#[derive(Debug, Default)]
pub struct CredentialFailureBudget {
    consecutive: u32,
}

impl CredentialFailureBudget {
    /// Fresh budget with no recorded failures.
    #[must_use]
    pub const fn new() -> Self {
        Self { consecutive: 0 }
    }

    /// Record one credential-classified failure; `true` = budget spent.
    pub fn record_credential_failure(&mut self) -> bool {
        self.consecutive = self.consecutive.saturating_add(1);
        self.consecutive >= MAX_CONSECUTIVE_CREDENTIAL_FAILURES
    }

    /// A successful (re)connect clears the streak.
    pub fn record_success(&mut self) {
        self.consecutive = 0;
    }

    #[cfg(test)]
    pub(crate) const fn attempts(&self) -> u32 {
        self.consecutive
    }
}

/// mysql_async renders server errors as ``ERROR <state> (<code>): ...``;
/// credential codes: 1045 access denied, 1044 db access denied, 1698
/// auth plugin denies.
pub fn is_mysql_credential_text(text: &str) -> bool {
    text.contains("ERROR ")
        && ["(1045):", "(1044):", "(1698):"]
            .iter()
            .any(|code| text.contains(code))
}

/// Postgres credential text: server SQLSTATEs 28000/28P01 (present after
/// `describe_db_error` expansion), or pgwire's client-side rejection,
/// which renders as "authentication error: ..." with no SQLSTATE.
pub fn is_postgres_credential_text(text: &str) -> bool {
    text.contains("SQLSTATE 28P01")
        || text.contains("SQLSTATE 28000")
        || text.contains("authentication error:")
}

/// MongoDB credential text: server error code 18 renders its codeName
/// `AuthenticationFailed`.
pub fn is_mongodb_credential_text(text: &str) -> bool {
    text.contains("AuthenticationFailed")
}

/// Any-source classifier for supervisory loops that only hold the
/// rendered error string.
pub fn is_credential_error_text(text: &str) -> bool {
    is_mysql_credential_text(text)
        || is_postgres_credential_text(text)
        || is_mongodb_credential_text(text)
}

/// True when a source already emitted its own crash-fast terminal text;
/// a supervisor loop must treat it as terminal, never retry it.
pub fn is_crash_fast_text(text: &str) -> bool {
    text.contains("exiting so the supervisor can restart with fresh credentials")
        || is_unrecoverable_config_text(text)
}

/// Marker a call site appends when it has classified a server refusal from
/// its *typed* SQLSTATE and knows no retry can clear it — see
/// `postgres::connection::classify_slot_refusal`.
///
/// This exists so the decision can be made where the code's meaning is
/// unambiguous. 42501 (insufficient_privilege) at slot creation can only
/// mean the role lacks REPLICATION, a fixed grant; the same code on a table
/// read may be a grant applied moments later, where retrying is right. A
/// global code list cannot tell those apart, and over-terminalising is the
/// worse failure — it halts a pipeline that would have recovered.
pub const SITE_CLASSIFIED_TERMINAL: &str = "terminal: no retry can clear this";

/// True for server refusals that describe a configuration value the server
/// will never accept, so no retry can clear them.
///
/// Two forms are recognised:
///
/// - SQLSTATE 42602 (invalid_name), and only because it was reproduced:
///   Postgres 16 raises it for a replication slot name outside the allowed
///   charset. Reserved-prefix and over-length names were tried and do NOT
///   raise it — `pg_`-prefixed and over-long slot names are both accepted —
///   so nothing else is claimed here.
/// - [`SITE_CLASSIFIED_TERMINAL`], appended by a call site that classified
///   the refusal from its typed code. No other SQLSTATE is matched globally
///   on purpose.
///
/// Keep the global list to codes someone has actually produced. Over-
/// terminalising is the worse failure: it halts a pipeline that would have
/// recovered.
pub fn is_unrecoverable_config_text(text: &str) -> bool {
    text.contains("SQLSTATE 42602") || text.contains(SITE_CLASSIFIED_TERMINAL)
}

/// Terminal text for sources with no in-process reconnect loop, where a
/// single credential failure is already terminal.
pub fn immediate_message(last_error: &impl Display) -> String {
    format!(
        "credential error; exiting so the supervisor can restart with fresh credentials \
         (last: {last_error})"
    )
}

/// Terminal error text emitted when the budget is exhausted.
pub fn exhausted_message(last_error: &impl Display) -> String {
    format!(
        "credential error persisted after {MAX_CONSECUTIVE_CREDENTIAL_FAILURES} attempts; \
         exiting so the supervisor can restart with fresh credentials (last: {last_error})"
    )
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn five_consecutive_credential_failures_exhaust_the_budget() {
        let mut budget = CredentialFailureBudget::new();
        for attempt in 1..MAX_CONSECUTIVE_CREDENTIAL_FAILURES {
            assert!(!budget.record_credential_failure(), "attempt {attempt}");
        }
        assert!(budget.record_credential_failure());
        assert_eq!(budget.attempts(), MAX_CONSECUTIVE_CREDENTIAL_FAILURES);
    }

    #[test]
    fn a_successful_reconnect_resets_the_streak() {
        let mut budget = CredentialFailureBudget::new();
        for _ in 0..4 {
            assert!(!budget.record_credential_failure());
        }
        budget.record_success();
        assert_eq!(budget.attempts(), 0);
        assert!(!budget.record_credential_failure());
    }

    #[test]
    fn transient_failures_do_not_touch_the_budget() {
        let mut budget = CredentialFailureBudget::new();
        for _ in 0..4 {
            assert!(!budget.record_credential_failure());
        }
        // A transient failure makes no budget call at all; the streak is
        // still one credential failure away from terminal.
        assert_eq!(budget.attempts(), 4);
        assert!(budget.record_credential_failure());
    }

    #[test]
    fn exhausted_message_names_the_restart_remedy() {
        let message = exhausted_message(&"ERROR 28000 (1045): Access denied");
        assert!(message.starts_with("credential error persisted after 5 attempts"));
        assert!(message.contains("supervisor can restart with fresh credentials"));
        assert!(message.ends_with("(last: ERROR 28000 (1045): Access denied)"));
    }

    #[test]
    fn rendered_error_texts_classify_across_sources() {
        // Bootstrap-arm shape from the live repro (Operation, not
        // Connection — text classification must not care).
        assert!(is_credential_error_text(
            "mysql operation failed: Server error: `ERROR 28000 (1045): Access denied for \
             user 'ventstream'@'10.2.14.7' (using password: YES)`"
        ));
        assert!(is_credential_error_text(
            "postgres connection failed: create replication slot: db error (SQLSTATE 28P01): \
             password authentication failed for user \"ventstream\""
        ));
        assert!(is_credential_error_text(
            "postgres connect failed: authentication error: SCRAM authentication failed"
        ));
        assert!(is_credential_error_text(
            "mongodb operation failed: Command failed: Error code 18 (AuthenticationFailed): \
             Authentication failed."
        ));
        assert!(!is_credential_error_text(
            "mysql connection failed: Connection refused (os error 61)"
        ));
        assert!(!is_credential_error_text(
            "postgres connection failed: db error (SQLSTATE 57014): canceling statement"
        ));
    }

    #[test]
    fn crash_fast_texts_are_recognized_and_never_reclassified() {
        let terminal = exhausted_message(&"ERROR 28000 (1045): Access denied");
        assert!(is_crash_fast_text(&terminal));
        let immediate = immediate_message(&"Authentication failed.");
        assert!(is_crash_fast_text(&immediate));
        assert!(!is_crash_fast_text(
            "mysql connection failed: Server error: `ERROR 28000 (1045)`"
        ));
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod unrecoverable_config_tests {
    use super::*;

    /// The reported case: a slot name with a capital letter. Postgres
    /// refuses it identically on every attempt, so the supervisor must
    /// stop rather than back off forever.
    #[test]
    fn an_invalid_slot_name_is_terminal() {
        let text = "postgres connection failed: creating slot vs_slotS: db error \
                    (SQLSTATE 42602): replication slot name \"vs_slotS\" contains \
                    invalid character (hint: Replication slot names may only contain \
                    lower case letters, numbers, and the underscore character.)";
        assert!(is_unrecoverable_config_text(text));
        assert!(
            is_crash_fast_text(text),
            "the supervisor loop must see this as terminal"
        );
    }

    /// A transient connection failure must stay retryable — this is the
    /// direction that matters, since misclassifying it would halt a
    /// pipeline that would have recovered on its own.
    #[test]
    fn a_transient_failure_is_not_terminal() {
        let text = "postgres connection failed: connection refused";
        assert!(!is_unrecoverable_config_text(text));
        assert!(!is_crash_fast_text(text));
    }

    #[test]
    fn an_unrelated_sqlstate_is_not_terminal() {
        // 57P03 (cannot_connect_now) is the recovering-server case: retry
        // is exactly right there.
        assert!(!is_unrecoverable_config_text(
            "db error (SQLSTATE 57P03): the database system is starting up"
        ));
    }

    /// A refusal a call site classified from its typed code is terminal
    /// wherever it surfaces, without that code joining the global list.
    #[test]
    fn a_site_classified_refusal_is_terminal() {
        let text = format!(
            "postgres connection failed: creating slot vs_slot: db error (SQLSTATE 42501): \
             permission denied to use replication slots; {SITE_CLASSIFIED_TERMINAL}"
        );
        assert!(is_unrecoverable_config_text(&text));
        assert!(is_crash_fast_text(&text));
    }

    /// 42501 is deliberately NOT matched globally: on a table read it can
    /// be a grant that lands moments later, and retrying is right.
    #[test]
    fn an_unclassified_permission_error_stays_retryable() {
        let text = "db error (SQLSTATE 42501): permission denied for table orders";
        assert!(!is_unrecoverable_config_text(text));
        assert!(!is_crash_fast_text(text));
    }
}
