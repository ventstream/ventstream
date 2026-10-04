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
    for token in tokenize(template)? {
        match token {
            Token::Literal(lit) => out.push_str(&lit),
            Token::Subst(token) => {
                out.push_str(&sanitize_index_segment(&substitute(&token, event)));
            }
            Token::Time(spec) => match spec {
                'Y' => out.push_str(&format!("{:04}", now.year())),
                'm' => out.push_str(&format!("{:02}", now.month())),
                'd' => out.push_str(&format!("{:02}", now.day())),
                _ => out.push_str(&format!("{:02}", now.hour())),
            },
        }
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

/// One parsed template element, shared by [`render`]'s siblings so the
/// wildcard pattern and the index matcher can never diverge from what the
/// bulk actually writes — that divergence is exactly the mismatch that
/// would make delete resolution miss (#196 review).
enum Token {
    /// Literal text, with `%%` already collapsed to `%`.
    Literal(String),
    /// A time placeholder: `Y`, `m`, `d`, or `H`.
    Time(char),
    /// A `${...}` substitution token (contents between the braces).
    Subst(String),
}

/// Tokenize a template, with the same error shapes `render` reports.
fn tokenize(template: &str) -> Result<Vec<Token>, OpenSearchSinkError> {
    let mut tokens: Vec<Token> = Vec::new();
    let mut literal = String::new();
    let mut chars = template.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if ch == '%' {
            match chars.next() {
                Some((_, '%')) => literal.push('%'),
                Some((_, spec @ ('Y' | 'm' | 'd' | 'H'))) => {
                    if !literal.is_empty() {
                        tokens.push(Token::Literal(std::mem::take(&mut literal)));
                    }
                    tokens.push(Token::Time(spec));
                }
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
            chars.next();
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
            if !literal.is_empty() {
                tokens.push(Token::Literal(std::mem::take(&mut literal)));
            }
            tokens.push(Token::Subst(token));
            continue;
        }
        literal.push(ch);
    }
    if !literal.is_empty() {
        tokens.push(Token::Literal(literal));
    }
    Ok(tokens)
}

/// Digits a time placeholder renders to, fixed-width.
fn time_width(spec: char) -> usize {
    match spec {
        'Y' => 4,
        _ => 2,
    }
}

/// The instant one period before `now`, where a "period" is the finest
/// time placeholder the template uses (`%H` → an hour, `%d` → a day,
/// `%m` → a month, `%Y` → a year). `None` for templates without time
/// placeholders.
///
/// Deletes target the index rendered at delete time *and* the previous
/// period's: a copy indexed just before a period boundary may not be
/// refreshed into the search view when the delete's resolution query
/// runs moments later, and the rendered index alone only covers the
/// current period. One period back is as far as that visibility gap can
/// reach, so this closes it without refreshing the whole pattern.
pub fn previous_period(
    template: &str,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, OpenSearchSinkError> {
    let mut finest: Option<char> = None;
    let rank = |spec: char| match spec {
        'H' => 3,
        'd' => 2,
        'm' => 1,
        _ => 0,
    };
    for token in tokenize(template)? {
        if let Token::Time(spec) = token {
            if finest.is_none_or(|current| rank(spec) > rank(current)) {
                finest = Some(spec);
            }
        }
    }
    Ok(finest.map(|spec| match spec {
        'H' => now - chrono::Duration::hours(1),
        'd' => now - chrono::Duration::days(1),
        'm' => now
            .checked_sub_months(chrono::Months::new(1))
            .unwrap_or(now),
        _ => now
            .checked_sub_months(chrono::Months::new(12))
            .unwrap_or(now),
    }))
}

/// True when `candidate` is an index name this template could have
/// rendered **for this event**: literals and substitutions must match
/// exactly, and each time placeholder must match a fixed-width digit run.
///
/// Delete resolution searches a wildcard pattern, and `*` happily crosses
/// `-` — `events-*-*-*` also matches `events-app-2026-05-23` or an
/// operator's `events-archive-...`. Accepting such hits would send a
/// versioned delete into another template's index that happens to share
/// the id, so every hit is post-filtered through this before it becomes a
/// delete target.
pub fn rendered_index_matches(
    template: &str,
    event: &Event,
    candidate: &str,
) -> Result<bool, OpenSearchSinkError> {
    let tokens = tokenize(template)?;
    let bytes = candidate.as_bytes();
    let mut at = 0usize;
    for token in &tokens {
        match token {
            Token::Literal(lit) => {
                if !candidate[at..].starts_with(lit.as_str()) {
                    return Ok(false);
                }
                at += lit.len();
            }
            Token::Subst(token) => {
                let value = sanitize_index_segment(&substitute(token, event));
                if !candidate[at..].starts_with(value.as_str()) {
                    return Ok(false);
                }
                at += value.len();
            }
            Token::Time(spec) => {
                let width = time_width(*spec);
                let Some(run) = bytes.get(at..at + width) else {
                    return Ok(false);
                };
                if !run.iter().all(u8::is_ascii_digit) {
                    return Ok(false);
                }
                at += width;
            }
        }
    }
    Ok(at == candidate.len())
}

/// True when the template derives part of the index name from the **time
/// of writing** (`%Y`, `%m`, `%d`, `%H`). `%%` is a literal percent and
/// does not count.
///
/// Time-rolled templates make a document's index drift: an event about a
/// row first written yesterday renders to *today's* index, so a delete
/// addressed by rendering misses the document entirely (#124). Callers use
/// this to decide whether deletes need their real index resolved by id.
pub fn has_time_placeholders(template: &str) -> bool {
    let mut chars = template.chars();
    while let Some(ch) = chars.next() {
        if ch == '%' && chars.next() != Some('%') {
            return true;
        }
    }
    false
}

/// Render the event-derived parts of the template normally but replace
/// every time placeholder with `*`: the search pattern matching every
/// concrete index this template can have produced for this event.
/// Consecutive wildcards collapse to one.
pub fn render_time_wildcards(template: &str, event: &Event) -> Result<String, OpenSearchSinkError> {
    let mut out = String::with_capacity(template.len() + 8);
    for token in tokenize(template)? {
        match token {
            Token::Literal(lit) => {
                // A literal '*' would widen the pattern; indices can't
                // contain '*', so sanitize it like any substitution would.
                for ch in lit.chars() {
                    out.push(if ch == '*' { '_' } else { ch });
                }
            }
            Token::Subst(token) => {
                out.push_str(&sanitize_index_segment(&substitute(&token, event)));
            }
            Token::Time(_) => {
                if !out.ends_with('*') {
                    out.push('*');
                }
            }
        }
    }
    if out.is_empty() {
        return Err(OpenSearchSinkError::IndexTemplate(
            "rendered index pattern is empty".into(),
        ));
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
pub(crate) fn sanitize_index_segment(input: &str) -> String {
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

    /// `%%` is a literal percent, not a time placeholder: a template using
    /// it stays static and must not pay the delete-resolution lookup.
    #[test]
    fn time_placeholders_detected_but_percent_escape_is_not() {
        assert!(has_time_placeholders("events-%Y-%m-%d"));
        assert!(has_time_placeholders("e-%H"));
        assert!(!has_time_placeholders("orders"));
        assert!(!has_time_placeholders("literal-100%%"));
    }

    /// The wildcard pattern renders event-derived parts normally, turns
    /// each time placeholder into `*` (adjacent ones collapse), and
    /// sanitizes a literal `*` so it cannot widen the pattern.
    #[test]
    fn time_wildcards_replace_time_and_keep_event_parts() {
        let event = make_event("postgres.app.orders.delete", HashMap::new());
        assert_eq!(
            render_time_wildcards("events-%Y-%m-%d", &event).unwrap(),
            "events-*-*-*"
        );
        assert_eq!(
            render_time_wildcards("e-${subject:0}-%Y%m%d", &event).unwrap(),
            "e-postgres-*"
        );
        assert_eq!(render_time_wildcards("plain", &event).unwrap(), "plain");
        assert_eq!(
            render_time_wildcards("pct-%%-%d", &event).unwrap(),
            "pct-%-*"
        );
        let err = render_time_wildcards("bad-%q", &event).unwrap_err();
        assert!(
            err.to_string().contains("unknown time placeholder"),
            "{err}"
        );
    }

    /// The matcher accepts exactly the names the template can render for
    /// an event: time placeholders are fixed-width digit runs, everything
    /// else is literal. `*` in the lookup pattern crosses `-`, so this is
    /// what keeps foreign indices out of the delete targets.
    #[test]
    fn rendered_index_matches_only_plausible_renders() {
        let event = make_event("postgres.app.orders.delete", HashMap::new());
        let tpl = "events-%Y-%m-%d";
        assert!(rendered_index_matches(tpl, &event, "events-2026-08-20").unwrap());
        assert!(rendered_index_matches(tpl, &event, "events-1999-12-31").unwrap());
        // Non-digits where a time component belongs — another pipeline's
        // index that the wildcard pattern happily matched.
        assert!(!rendered_index_matches(tpl, &event, "events-archive-2026-08").unwrap());
        // Wrong width, missing separator, trailing garbage.
        assert!(!rendered_index_matches(tpl, &event, "events-26-08-20").unwrap());
        assert!(!rendered_index_matches(tpl, &event, "events-20260820").unwrap());
        assert!(!rendered_index_matches(tpl, &event, "events-2026-08-20-extra").unwrap());
        assert!(!rendered_index_matches(tpl, &event, "events-2026-08-2").unwrap());

        // Substitutions must match what this event renders to.
        let tpl = "e-${subject:0}-%Y";
        assert!(rendered_index_matches(tpl, &event, "e-postgres-2026").unwrap());
        assert!(!rendered_index_matches(tpl, &event, "e-mysql-2026").unwrap());

        // A literal template matches only itself.
        assert!(rendered_index_matches("orders", &event, "orders").unwrap());
        assert!(!rendered_index_matches("orders", &event, "orders-2026").unwrap());
    }

    /// One period = the finest time placeholder present; literal
    /// templates have no period at all.
    #[test]
    fn previous_period_steps_back_by_the_finest_placeholder() {
        let now = Utc.with_ymd_and_hms(2026, 3, 1, 0, 30, 0).unwrap();
        let event = make_event("postgres.app.orders.delete", HashMap::new());
        let prev = |tpl: &str| {
            previous_period(tpl, now)
                .unwrap()
                .map(|at| render(tpl, &event, at).unwrap())
        };
        // Day templates step a day back — across the month boundary here.
        assert_eq!(
            prev("events-%Y-%m-%d").as_deref(),
            Some("events-2026-02-28")
        );
        // An hour template steps an hour back, across the day boundary.
        assert_eq!(
            prev("events-%Y-%m-%d-%H").as_deref(),
            Some("events-2026-02-28-23")
        );
        // Coarser-only templates step by their own unit.
        assert_eq!(prev("events-%Y-%m").as_deref(), Some("events-2026-02"));
        assert_eq!(prev("events-%Y").as_deref(), Some("events-2025"));
        // No time placeholders → no period.
        assert_eq!(prev("orders"), None);
    }
}
