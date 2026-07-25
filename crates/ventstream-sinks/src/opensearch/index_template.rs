//! Per-event index name template rendering.
//!
//! Operators configure a template string that gets expanded per-event
//! at write time. The expansion is deterministic and free of I/O — so
//! it's pure-function-testable.
//!
//! ### Substitution syntax
//!
//! | Token | Substitution |
//! |-------|--------------|
//! | `%Y` `%m` `%d` `%H` | UTC time components of `now` (zero-padded) |
//! | `${subject}` | The full event subject string |
//! | `${subject:N}` | The N-th dot-separated segment of the subject (0-indexed). Missing segments yield empty. |
//! | `${header:NAME}` | The value of header `NAME`. Missing headers yield empty. |
//! | Anything else | Passed through verbatim |
//!
//! All substitutions are lowercased (OpenSearch index names must be
//! lowercase) and any character outside `[a-z0-9_-]` is replaced with
//! `_`. This makes weird subjects safe as parts of an index name even
//! after our upstream sanitization.
//!
//! ### Example
//!
//! Template: `"events-${subject:1}-%Y-%m-%d"`
//! Event subject: `"postgres.app.products.insert"`
//! Now: `2026-05-23 17:00 UTC`
//! Result: `"events-app-2026-05-23"`

use chrono::{DateTime, Datelike, Timelike, Utc};
use ventstream_core::Event;

use crate::error::OpenSearchSinkError;

/// Render a template against an event, using `now` for time placeholders.
///
/// Errors are returned only for structurally-malformed templates (e.g.
/// unmatched `${`). Missing optional substitutions (`${subject:99}`,
/// `${header:nope}`) yield empty strings rather than errors.
pub fn render(
    template: &str,
    event: &Event,
    now: DateTime<Utc>,
) -> Result<String, OpenSearchSinkError> {
    let mut out = String::with_capacity(template.len() + 16);
    let mut chars = template.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if ch == '%' {
            match chars.next() {
                Some((_, 'Y')) => out.push_str(&format!("{:04}", now.year())),
                Some((_, 'm')) => out.push_str(&format!("{:02}", now.month())),
                Some((_, 'd')) => out.push_str(&format!("{:02}", now.day())),
                Some((_, 'H')) => out.push_str(&format!("{:02}", now.hour())),
                Some((_, '%')) => out.push('%'),
                Some((_, other)) => {
                    return Err(OpenSearchSinkError::IndexTemplate(format!(
                        "unknown time placeholder '%{other}' at byte {idx}"
                    )))
                }
                None => {
                    return Err(OpenSearchSinkError::IndexTemplate(format!(
                        "trailing '%' at byte {idx}"
                    )))
                }
            }
            continue;
        }

        if ch == '$' && chars.peek().map(|(_, c)| *c) == Some('{') {
            chars.next(); // consume the '{'
            let mut token = String::new();
            let mut closed = false;
            for (_, tc) in chars.by_ref() {
                if tc == '}' {
                    closed = true;
                    break;
                }
                token.push(tc);
            }
            if !closed {
                return Err(OpenSearchSinkError::IndexTemplate(format!(
                    "unterminated '${{' starting at byte {idx}"
                )));
            }
            out.push_str(&sanitize_index_segment(&substitute(&token, event)));
            continue;
        }

        out.push(ch);
    }

    // OpenSearch rejects index names that are empty, start with `_`, `-`,
    // or `+`, or that contain uppercase characters. We've already
    // lowercased substitutions; reject empty result and leading bad
    // characters with a clear error rather than a surprising server-side
    // 400.
    if out.is_empty() {
        return Err(OpenSearchSinkError::IndexTemplate(
            "rendered index name is empty".into(),
        ));
    }
    if let Some(first) = out.chars().next() {
        if matches!(first, '_' | '-' | '+') {
            return Err(OpenSearchSinkError::IndexTemplate(format!(
                "rendered index name starts with reserved character '{first}': '{out}'"
            )));
        }
    }
    Ok(out)
}

/// Resolve one `${...}` token against the event. Returns the raw value
/// (un-sanitized); the caller applies [`sanitize_index_segment`].
fn substitute(token: &str, event: &Event) -> String {
    if token == "subject" {
        return event.subject.as_str().to_string();
    }
    if let Some(rest) = token.strip_prefix("subject:") {
        if let Ok(n) = rest.parse::<usize>() {
            return event
                .subject
                .as_str()
                .split('.')
                .nth(n)
                .unwrap_or("")
                .to_string();
        }
        return String::new();
    }
    if let Some(name) = token.strip_prefix("header:") {
        return event
            .headers
            .get(name)
            .map(str::to_owned)
            .unwrap_or_default();
    }
    // Unknown token — yield empty so misconfiguration produces an obvious
    // index name rather than a build-time error.
    String::new()
}

/// Lowercase and replace any character outside `[a-z0-9_-]` with `_`.
/// Applied to every substitution result.
fn sanitize_index_segment(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_lowercase() || lower.is_ascii_digit() || lower == '_' || lower == '-' {
            out.push(lower);
        } else {
            out.push('_');
        }
    }
    out
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
    use chrono::TimeZone;
    use std::collections::HashMap;
    use ventstream_core::{ContentType, Headers, Payload, SourceUri, Subject};

    fn make_event(subject: &str, headers: HashMap<String, String>) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(subject).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build()
    }

    fn march_23() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 23, 14, 0, 0).unwrap()
    }

    #[test]
    fn literal_template_returns_itself() {
        let event = make_event("a.b", HashMap::new());
        assert_eq!(
            render("static-index", &event, march_23()).unwrap(),
            "static-index"
        );
    }

    #[test]
    fn time_placeholders_expand_zero_padded() {
        let event = make_event("a.b", HashMap::new());
        assert_eq!(
            render("logs-%Y-%m-%d", &event, march_23()).unwrap(),
            "logs-2026-03-23"
        );
        assert_eq!(render("hour-%H", &event, march_23()).unwrap(), "hour-14");
    }

    #[test]
    fn double_percent_yields_literal_percent() {
        let event = make_event("a.b", HashMap::new());
        assert_eq!(render("100%%", &event, march_23()).unwrap(), "100%");
    }

    #[test]
    fn subject_placeholder_substitutes_full_subject() {
        let event = make_event("postgres.public.users.insert", HashMap::new());
        assert_eq!(
            render("${subject}", &event, march_23()).unwrap(),
            "postgres.public.users.insert".replace('.', "_") // dots are sanitized
        );
    }

    #[test]
    fn subject_n_placeholder_picks_nth_segment() {
        let event = make_event("postgres.public.users.insert", HashMap::new());
        assert_eq!(
            render("events-${subject:1}-${subject:2}", &event, march_23()).unwrap(),
            "events-public-users"
        );
    }

    #[test]
    fn subject_n_placeholder_missing_segment_yields_empty() {
        let event = make_event("postgres.public", HashMap::new());
        assert_eq!(
            render("idx-${subject:99}-end", &event, march_23()).unwrap(),
            "idx--end"
        );
    }

    #[test]
    fn header_placeholder_substitutes_header_value() {
        let mut headers = HashMap::new();
        headers.insert(
            "ventstream.cdc.namespace".to_string(),
            "analytics".to_string(),
        );
        let event = make_event("postgres.analytics.events.insert", headers);
        assert_eq!(
            render(
                "events-${header:ventstream.cdc.namespace}",
                &event,
                march_23()
            )
            .unwrap(),
            "events-analytics"
        );
    }

    #[test]
    fn header_placeholder_missing_header_yields_empty() {
        let event = make_event("a.b", HashMap::new());
        assert_eq!(
            render("events-${header:missing}-end", &event, march_23()).unwrap(),
            "events--end"
        );
    }

    #[test]
    fn combined_template_with_subject_and_time() {
        let event = make_event("postgres.app.products.insert", HashMap::new());
        assert_eq!(
            render("events-${subject:1}-%Y-%m-%d", &event, march_23()).unwrap(),
            "events-app-2026-03-23"
        );
    }

    #[test]
    fn substitutions_are_lowercased_and_sanitized() {
        let mut headers = HashMap::new();
        headers.insert("ventstream.cdc.namespace".into(), "WeirD.Schema".into());
        let event = make_event("a.b", headers);
        assert_eq!(
            render("idx-${header:ventstream.cdc.namespace}", &event, march_23()).unwrap(),
            "idx-weird_schema"
        );
    }

    #[test]
    fn unknown_time_placeholder_errors() {
        let event = make_event("a.b", HashMap::new());
        let err = render("logs-%Q", &event, march_23()).unwrap_err();
        assert!(matches!(err, OpenSearchSinkError::IndexTemplate(_)));
    }

    #[test]
    fn unterminated_dollar_brace_errors() {
        let event = make_event("a.b", HashMap::new());
        let err = render("logs-${subject", &event, march_23()).unwrap_err();
        assert!(matches!(err, OpenSearchSinkError::IndexTemplate(_)));
    }

    #[test]
    fn trailing_percent_errors() {
        let event = make_event("a.b", HashMap::new());
        let err = render("logs-%", &event, march_23()).unwrap_err();
        assert!(matches!(err, OpenSearchSinkError::IndexTemplate(_)));
    }

    #[test]
    fn empty_rendered_name_errors() {
        let event = make_event("a.b", HashMap::new());
        let err = render("", &event, march_23()).unwrap_err();
        assert!(matches!(err, OpenSearchSinkError::IndexTemplate(_)));
    }

    #[test]
    fn name_starting_with_reserved_char_errors() {
        let event = make_event("a.b", HashMap::new());
        let err = render("_internal", &event, march_23()).unwrap_err();
        assert!(matches!(err, OpenSearchSinkError::IndexTemplate(_)));

        let err = render("-leading-dash", &event, march_23()).unwrap_err();
        assert!(matches!(err, OpenSearchSinkError::IndexTemplate(_)));
    }
}
