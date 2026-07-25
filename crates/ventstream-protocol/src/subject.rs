//! The subject grammar and its NATS-compatible pattern matcher.
//!
//! Subjects are:
//!
//! ```text
//! vs.t.{tenant}.{event}.{id}
//! ```
//!
//! - `vs` and `t` are literal — reserved namespace.
//! - `{tenant}` is `[a-z0-9_-]+`, ≤64 chars.
//! - `{event}` is the event name: one or more `.`-joined segments, each
//!   `[A-Za-z0-9_-]+` (mixed case allowed — `deskMutated`,
//!   `orders.order.statusChanged`).
//! - `{id}` is the **last** segment — the instance id, `[A-Za-z0-9_-]+`,
//!   ≤128 chars (accepts ULID/UUID). Putting the id last makes one
//!   instance individually addressable (`deskMutated.desk_123`) and the
//!   whole event class wildcard-able (`deskMutated.*`).
//!
//! Patterns may substitute `*` (one segment) or `>` (one or more
//! trailing segments, terminal only). `vs`/`t` cannot be wildcarded.

use std::fmt;
use std::str::FromStr;

use crate::error::ProtocolError;
use crate::identifier::{validate_entity_id, validate_event_name, validate_strict};

/// One segment of a parsed [`SubjectPattern`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment<'a> {
    /// A concrete value the segment must equal.
    Literal(&'a str),
    /// `*` — matches exactly one segment of any value.
    OneWildcard,
    /// `>` — matches one or more trailing segments. Only legal in the
    /// terminal position.
    TailWildcard,
}

/// A fully-validated subject: `vs.t.{tenant}.{event}.{id}`.
///
/// Construct via [`Subject::parse`] or [`Subject::builder`]. There is
/// no way to construct a malformed `Subject` — once you hold one, the
/// grammar is guaranteed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subject {
    tenant: String,
    /// The event name (may itself be dotted, e.g. `orders.order.x`).
    event: String,
    /// The instance id — the final subject segment.
    entity_id: String,
}

impl Subject {
    /// Start a builder.
    pub fn builder() -> SubjectBuilder {
        SubjectBuilder::default()
    }

    /// Parse from the wire form `vs.t.{tenant}.{event}.{id}`. The id is
    /// the final segment; everything between the tenant and the id is
    /// the (possibly dotted) event name.
    pub fn parse(s: &str) -> Result<Self, ProtocolError> {
        let parts: Vec<&str> = s.split('.').collect();
        // vs . t . tenant . <event…≥1> . id  → at least 5 segments.
        if parts.len() < 5 {
            return Err(ProtocolError::InvalidSubject {
                subject: s.to_owned(),
                reason: format!("expected at least 5 segments, got {}", parts.len()),
            });
        }
        let prefix0 = parts.first().copied().unwrap_or("");
        let prefix1 = parts.get(1).copied().unwrap_or("");
        if prefix0 != "vs" {
            return Err(ProtocolError::InvalidSubject {
                subject: s.to_owned(),
                reason: format!("first segment must be 'vs', got '{prefix0}'"),
            });
        }
        if prefix1 != "t" {
            return Err(ProtocolError::InvalidSubject {
                subject: s.to_owned(),
                reason: format!("second segment must be 't', got '{prefix1}'"),
            });
        }
        let tenant = parts.get(2).copied().unwrap_or("");
        // id is the last segment; event is everything between tenant and id.
        let id = parts.last().copied().unwrap_or("");
        let event_parts = parts.get(3..parts.len().saturating_sub(1)).unwrap_or(&[]);
        let event = event_parts.join(".");
        Self::new(tenant, event, id)
    }

    /// Build from components, validating each.
    pub fn new(
        tenant: impl Into<String>,
        event: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        let tenant = tenant.into();
        let event = event.into();
        let entity_id = entity_id.into();
        validate_strict("tenant", &tenant)?;
        validate_event_name("event", &event)?;
        validate_entity_id("id", &entity_id)?;
        Ok(Self {
            tenant,
            event,
            entity_id,
        })
    }

    /// Tenant component.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Event-name component (may be dotted).
    pub fn event(&self) -> &str {
        &self.event
    }
    /// Instance id component (final segment).
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "vs.t.{}.{}.{}", self.tenant, self.event, self.entity_id)
    }
}

impl FromStr for Subject {
    type Err = ProtocolError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Typed builder for [`Subject`].
#[derive(Debug, Default, Clone)]
pub struct SubjectBuilder {
    tenant: Option<String>,
    event: Option<String>,
    entity_id: Option<String>,
}

impl SubjectBuilder {
    /// Set the tenant.
    pub fn tenant(mut self, v: impl Into<String>) -> Result<Self, ProtocolError> {
        let v = v.into();
        validate_strict("tenant", &v)?;
        self.tenant = Some(v);
        Ok(self)
    }
    /// Set the event name.
    pub fn event(mut self, v: impl Into<String>) -> Result<Self, ProtocolError> {
        let v = v.into();
        validate_event_name("event", &v)?;
        self.event = Some(v);
        Ok(self)
    }
    /// Set the instance id.
    pub fn entity_id(mut self, v: impl Into<String>) -> Result<Self, ProtocolError> {
        let v = v.into();
        validate_entity_id("id", &v)?;
        self.entity_id = Some(v);
        Ok(self)
    }
    /// Finalize, failing if any component was not set.
    pub fn build(self) -> Result<Subject, ProtocolError> {
        let missing = |field: &'static str| ProtocolError::InvalidSubject {
            subject: String::new(),
            reason: format!("missing component '{field}'"),
        };
        Ok(Subject {
            tenant: self.tenant.ok_or_else(|| missing("tenant"))?,
            event: self.event.ok_or_else(|| missing("event"))?,
            entity_id: self.entity_id.ok_or_else(|| missing("id"))?,
        })
    }
}

/// A subject pattern. Like a [`Subject`] but with `*` / `>` wildcards
/// allowed. Patterns are anchored at the literal `vs.t.` prefix.
///
/// - `*` matches exactly one segment at that position.
/// - `>` may appear only as the *final* segment; it matches one or
///   more trailing segments.
///
/// Subscribers express patterns in their *unanchored* form (without
/// `vs.t.{tenant}.`) — the WS engine prefixes the tenant before
/// matching. Typical patterns: `deskMutated.desk_123` (one instance),
/// `deskMutated.*` (all instances), `deskMutated.>` (all under it).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubjectPattern {
    raw: String,
    segments: Vec<PatternSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PatternSegment {
    Literal(String),
    OneWildcard,
    TailWildcard,
}

impl SubjectPattern {
    /// Parse a NATS-style pattern in *anchored* form (`vs.t.…` with
    /// optional wildcards). Segment count is not fixed — the matcher
    /// walks the pattern against the subject.
    pub fn parse(pattern: &str) -> Result<Self, ProtocolError> {
        if pattern.is_empty() {
            return Err(ProtocolError::InvalidSubjectPattern {
                pattern: pattern.to_owned(),
                reason: "empty pattern".into(),
            });
        }
        let parts: Vec<&str> = pattern.split('.').collect();
        let first = parts.first().copied().unwrap_or("");
        let second = parts.get(1).copied().unwrap_or("");
        if first != "vs" {
            return Err(ProtocolError::InvalidSubjectPattern {
                pattern: pattern.to_owned(),
                reason: format!("first segment must be 'vs', got '{first}'"),
            });
        }
        if second != "t" {
            return Err(ProtocolError::InvalidSubjectPattern {
                pattern: pattern.to_owned(),
                reason: format!("second segment must be 't', got '{second}'"),
            });
        }

        let mut segments: Vec<PatternSegment> = Vec::with_capacity(parts.len());
        for (idx, &seg) in parts.iter().enumerate() {
            if seg.is_empty() {
                return Err(ProtocolError::InvalidSubjectPattern {
                    pattern: pattern.to_owned(),
                    reason: format!("empty segment at position {idx}"),
                });
            }
            let is_last = idx + 1 == parts.len();
            match seg {
                ">" => {
                    if !is_last {
                        return Err(ProtocolError::InvalidSubjectPattern {
                            pattern: pattern.to_owned(),
                            reason: "'>' may only appear in the final position".into(),
                        });
                    }
                    segments.push(PatternSegment::TailWildcard);
                }
                "*" => segments.push(PatternSegment::OneWildcard),
                literal => {
                    segments.push(PatternSegment::Literal(literal.to_owned()));
                }
            }
        }

        Ok(Self {
            raw: pattern.to_owned(),
            segments,
        })
    }

    /// Build an anchored pattern from a tenant + an unanchored client
    /// subscription (e.g. `"deskMutated.*"`). The WS engine splices the
    /// tenant from the connection in front of the client's pattern.
    pub fn anchored(tenant: &str, unanchored: &str) -> Result<Self, ProtocolError> {
        validate_strict("tenant", tenant)?;
        Self::parse(&format!("vs.t.{tenant}.{unanchored}"))
    }

    /// Iterate the parsed segments in order.
    pub fn segments(&self) -> impl Iterator<Item = Segment<'_>> {
        self.segments.iter().map(|seg| match seg {
            PatternSegment::Literal(s) => Segment::Literal(s.as_str()),
            PatternSegment::OneWildcard => Segment::OneWildcard,
            PatternSegment::TailWildcard => Segment::TailWildcard,
        })
    }

    /// The original input string (for logs and diagnostics).
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Test whether this pattern matches a fully-formed [`Subject`].
    pub fn matches(&self, subject: &Subject) -> bool {
        let s = subject.to_string();
        self.matches_str(&s)
    }

    /// Test whether this pattern matches an arbitrary subject string.
    pub fn matches_str(&self, subject: &str) -> bool {
        let subject_parts: Vec<&str> = subject.split('.').collect();
        match_segments(&self.segments, &subject_parts)
    }
}

impl fmt::Display for SubjectPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl FromStr for SubjectPattern {
    type Err = ProtocolError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

fn match_segments(pattern: &[PatternSegment], subject: &[&str]) -> bool {
    let mut pi = 0usize;
    let mut si = 0usize;
    loop {
        match (pattern.get(pi), subject.get(si)) {
            (None, None) => return true,
            (None, Some(_)) => return false,
            (Some(PatternSegment::TailWildcard), Some(_)) => return true,
            (Some(PatternSegment::TailWildcard), None) => return false,
            (Some(_), None) => return false,
            (Some(PatternSegment::OneWildcard), Some(_)) => {
                pi += 1;
                si += 1;
            }
            (Some(PatternSegment::Literal(p)), Some(s)) => {
                if p != s {
                    return false;
                }
                pi += 1;
                si += 1;
            }
        }
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

    fn s() -> Subject {
        Subject::new("acme", "deskMutated", "desk_123").unwrap()
    }

    #[test]
    fn parses_canonical_form() {
        let subj = Subject::parse("vs.t.acme.deskMutated.desk_123").unwrap();
        assert_eq!(subj.tenant(), "acme");
        assert_eq!(subj.event(), "deskMutated");
        assert_eq!(subj.entity_id(), "desk_123");
    }

    #[test]
    fn parses_dotted_event_name_id_is_last() {
        let subj = Subject::parse("vs.t.acme.orders.order.statusChanged.order_123").unwrap();
        assert_eq!(subj.tenant(), "acme");
        assert_eq!(subj.event(), "orders.order.statusChanged");
        assert_eq!(subj.entity_id(), "order_123");
    }

    #[test]
    fn display_roundtrip() {
        let subj = s();
        assert_eq!(subj.to_string(), "vs.t.acme.deskMutated.desk_123");
        assert_eq!(Subject::parse(&subj.to_string()).unwrap(), subj);
    }

    #[test]
    fn rejects_too_few_segments() {
        Subject::parse("vs.t.acme.deskMutated").unwrap_err();
    }

    #[test]
    fn rejects_wrong_prefix() {
        Subject::parse("xx.t.acme.deskMutated.x").unwrap_err();
        Subject::parse("vs.q.acme.deskMutated.x").unwrap_err();
    }

    #[test]
    fn camelcase_event_allowed_lowercase_tenant_required() {
        Subject::new("acme", "deskMutated", "desk_123").unwrap();
        Subject::new("Acme", "deskMutated", "desk_123").unwrap_err(); // tenant strict
    }

    #[test]
    fn accepts_uppercase_in_id() {
        Subject::parse("vs.t.acme.deskMutated.ORDER_X1").unwrap();
    }

    #[test]
    fn builder_validates_and_emits_canonical() {
        let subj = Subject::builder()
            .tenant("acme")
            .unwrap()
            .event("deskMutated")
            .unwrap()
            .entity_id("desk_123")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(subj.to_string(), "vs.t.acme.deskMutated.desk_123");
    }

    #[test]
    fn builder_rejects_missing_component() {
        assert!(Subject::builder().tenant("acme").unwrap().build().is_err());
    }

    // === Pattern matching (id last) ===

    #[test]
    fn one_instance_pattern() {
        let p = SubjectPattern::parse("vs.t.acme.deskMutated.desk_123").unwrap();
        assert!(p.matches_str("vs.t.acme.deskMutated.desk_123"));
        assert!(!p.matches_str("vs.t.acme.deskMutated.desk_999"));
    }

    #[test]
    fn all_instances_wildcard() {
        let p = SubjectPattern::parse("vs.t.acme.deskMutated.*").unwrap();
        assert!(p.matches_str("vs.t.acme.deskMutated.desk_123"));
        assert!(p.matches_str("vs.t.acme.deskMutated.desk_999"));
        assert!(!p.matches_str("vs.t.acme.callRecordMutated.x"));
    }

    #[test]
    fn tail_wildcard() {
        let p = SubjectPattern::parse("vs.t.acme.>").unwrap();
        assert!(p.matches_str("vs.t.acme.deskMutated.desk_123"));
        assert!(p.matches_str("vs.t.acme.orders.order.statusChanged.order_1"));
        assert!(!p.matches_str("vs.t.other.deskMutated.x"));
    }

    #[test]
    fn rejects_tail_wildcard_not_at_end() {
        SubjectPattern::parse("vs.t.acme.>.x").unwrap_err();
    }

    #[test]
    fn anchored_splices_tenant() {
        let p = SubjectPattern::anchored("acme", "deskMutated.*").unwrap();
        assert_eq!(p.as_str(), "vs.t.acme.deskMutated.*");
    }

    #[test]
    fn matches_against_subject_struct() {
        let p = SubjectPattern::parse("vs.t.acme.deskMutated.*").unwrap();
        assert!(p.matches(&s()));
        let other = Subject::new("acme", "deskMutated", "desk_456").unwrap();
        assert!(p.matches(&other));
        let wrong = Subject::new("acme", "callRecordMutated", "x").unwrap();
        assert!(!p.matches(&wrong));
    }
}
