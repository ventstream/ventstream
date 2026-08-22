//! The immutable, zero-copy-cloneable [`Event`] type and its components.
//!
//! Newtypes are used aggressively: a [`Subject`] cannot be passed where a
//! [`SourceUri`] is expected. Both are `String`-backed but distinct at the
//! type level. This costs nothing at runtime and eliminates a class of bugs
//! that would otherwise compile.
//!
//! Cloning an [`Event`] is O(1) — the [`Payload`] is backed by [`Bytes`]
//! (refcounted) and [`Headers`] are behind an [`Arc`]. Fan-out to thousands
//! of WebSocket subscribers does not duplicate the payload buffer.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::CoreError;
use crate::memory::MemoryReservation;

/// A unique, sortable identifier for an event.
///
/// Backed by a [ULID](https://github.com/ulid/spec): 128-bit, lexicographically
/// sortable by creation time, generated locally without coordination. This
/// makes it safe to use as an idempotency key across distributed sources
/// without contacting a central counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(Ulid);

impl EventId {
    /// Generate a new identifier from the current system time.
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Return the underlying ULID bytes.
    pub const fn as_ulid(&self) -> Ulid {
        self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for EventId {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|err| CoreError::InvalidEventId(err.to_string()))
    }
}

/// The origin of an event, expressed as a URI-like string.
///
/// Examples:
/// - `"sdk://my-application"` — produced via the SDK ingress
/// - `"postgres://orders_db.public.orders"` — Postgres logical replication
/// - `"kafka://telemetry.user.clicks"` — Kafka topic source
///
/// The format is `scheme://path` where the scheme matches a registered
/// source kind. Validation here is intentionally loose; richer parsing is
/// the caller's responsibility.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceUri(Arc<str>);

impl SourceUri {
    /// Construct a new source URI after light validation.
    ///
    /// Returns [`CoreError::InvalidSourceUri`] if the value is empty or
    /// missing the `scheme://` prefix.
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CoreError::InvalidSourceUri("empty source URI".into()));
        }
        if !value.contains("://") {
            return Err(CoreError::InvalidSourceUri(format!(
                "missing scheme separator '://' in '{value}'"
            )));
        }
        Ok(Self(Arc::from(value)))
    }

    /// Return the URI as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A hierarchical event type label.
///
/// Subjects use dot-delimited segments — `user.signed_up`, `order.line.added` —
/// modeled after NATS subject conventions. This enables wildcard routing in
/// the WebSocket fan-out layer (e.g. subscribers can request `user.*`).
///
/// Each segment must be non-empty and contain only ASCII alphanumerics,
/// underscores, or hyphens. The reserved tokens `*` and `>` are forbidden
/// in published subjects but allowed in subscription patterns elsewhere.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Subject(Arc<str>);

impl Subject {
    /// Construct and validate a subject string.
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CoreError::InvalidSubject("empty subject".into()));
        }
        for segment in value.split('.') {
            if segment.is_empty() {
                return Err(CoreError::InvalidSubject(format!(
                    "empty segment in subject '{value}'"
                )));
            }
            if !segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return Err(CoreError::InvalidSubject(format!(
                    "invalid character in segment '{segment}' (only [A-Za-z0-9_-] permitted)"
                )));
            }
        }
        Ok(Self(Arc::from(value)))
    }

    /// Return the subject as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Iterate over the dot-separated segments.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The byte payload of an event.
///
/// Backed by [`Bytes`] for cheap (refcounted) cloning. Sinks and the
/// WebSocket fan-out can share the same backing buffer without copying.
#[derive(Debug, Clone)]
pub struct Payload {
    bytes: Bytes,
    reservation: Option<Arc<MemoryReservation>>,
}

impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for Payload {}

impl Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        bytes_serde::serialize(&self.bytes, serializer)
    }
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        bytes_serde::deserialize(deserializer).map(Self::new)
    }
}

impl Payload {
    /// Wrap a [`Bytes`] buffer as a payload.
    pub const fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            reservation: None,
        }
    }

    /// Construct a payload from an owned byte vector.
    pub fn from_vec(value: Vec<u8>) -> Self {
        Self::new(Bytes::from(value))
    }

    /// Borrow the payload as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// Return the inner [`Bytes`] (cheap clone — refcount bump).
    pub fn as_bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    /// Length of the payload in bytes.
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the payload is zero-length.
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn has_memory_reservation(&self) -> bool {
        self.reservation.is_some()
    }

    pub(crate) fn set_memory_reservation(&mut self, reservation: Arc<MemoryReservation>) {
        self.reservation = Some(reservation);
    }
}

/// Declared encoding of the [`Payload`] bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentType {
    /// `application/json`
    Json,
    /// `application/protobuf`
    Protobuf,
    /// `application/octet-stream` or any other opaque encoding.
    Binary,
    /// An unrecognized MIME type passed through unmodified.
    Other(String),
}

impl ContentType {
    /// Return the canonical MIME-type string.
    pub fn as_mime(&self) -> &str {
        match self {
            Self::Json => "application/json",
            Self::Protobuf => "application/protobuf",
            Self::Binary => "application/octet-stream",
            Self::Other(s) => s.as_str(),
        }
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_mime())
    }
}

/// Free-form string headers attached to an event.
///
/// Headers carry routing hints, tenant identifiers, and tracing context.
/// They are intentionally `String → String` to stay protocol-neutral. The
/// map is wrapped in an [`Arc`] so cloning an [`Event`] does not duplicate
/// the underlying allocation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Headers(Arc<HashMap<String, String>>);

impl Headers {
    /// Create an empty header set.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construct from an existing map.
    pub fn from_map(map: HashMap<String, String>) -> Self {
        Self(Arc::new(map))
    }

    /// Return these headers with one key added/overwritten.
    ///
    /// Uses [`Arc::make_mut`], so when the `Headers` is uniquely owned
    /// (the common case for a freshly-built event before it's shared on
    /// the bus) this mutates the map in place — no clone of the existing
    /// entries. Only when the `Arc` is already shared does it copy.
    #[must_use]
    pub fn with_header(mut self, key: String, value: String) -> Self {
        Arc::make_mut(&mut self.0).insert(key, value);
        self
    }

    /// Return these headers with one key removed. Same
    /// [`Arc::make_mut`] in-place behavior as [`Self::with_header`].
    #[must_use]
    pub fn without_header(mut self, key: &str) -> Self {
        Arc::make_mut(&mut self.0).remove(key);
        self
    }

    /// Look up a header by name.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Number of headers.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no headers.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate over the underlying entries.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.0.iter()
    }
}

/// An immutable event flowing through the engine.
///
/// Cloning is O(1): [`EventId`] is `Copy`, [`SourceUri`] / [`Subject`] are
/// `Arc<str>`, [`Payload`] is refcounted [`Bytes`], [`Headers`] is `Arc`-wrapped.
/// This is critical for fan-out where a single event is delivered to many
/// subscribers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Unique, sortable identifier.
    pub id: EventId,
    /// Where the event originated.
    pub source: SourceUri,
    /// Hierarchical event type.
    pub subject: Subject,
    /// Raw payload bytes.
    pub payload: Payload,
    /// Declared content encoding.
    pub content_type: ContentType,
    /// When the event occurred at its source (not when it entered the engine).
    pub occurred_at: DateTime<Utc>,
    /// Optional routing / tracing headers.
    pub headers: Headers,
}

impl Event {
    /// Construct an event with a freshly-generated [`EventId`] and
    /// `occurred_at = now`. Most production code should set `occurred_at`
    /// explicitly via [`EventBuilder`].
    pub fn new(source: SourceUri, subject: Subject, payload: Payload) -> Self {
        Self {
            id: EventId::new(),
            source,
            subject,
            payload,
            content_type: ContentType::Json,
            occurred_at: Utc::now(),
            headers: Headers::empty(),
        }
    }

    /// Start a builder for fine-grained event construction.
    pub fn builder(source: SourceUri, subject: Subject) -> EventBuilder {
        EventBuilder {
            id: None,
            source,
            subject,
            payload: None,
            content_type: None,
            occurred_at: None,
            headers: None,
        }
    }

    /// Conservative resident-memory estimate including one sink-encoding copy.
    pub fn estimated_memory_bytes(&self) -> u64 {
        const EVENT_OVERHEAD: usize = 512;
        const HEADER_OVERHEAD: usize = 48;

        let headers = self.headers.iter().fold(0usize, |total, (key, value)| {
            total
                .saturating_add(key.len())
                .saturating_add(value.len())
                .saturating_add(HEADER_OVERHEAD)
        });
        let estimate = self
            .payload
            .len()
            .saturating_mul(2)
            .saturating_add(self.source.as_str().len())
            .saturating_add(self.subject.as_str().len())
            .saturating_add(self.content_type.as_mime().len())
            .saturating_add(headers)
            .saturating_add(EVENT_OVERHEAD);
        u64::try_from(estimate).unwrap_or(u64::MAX)
    }
}

/// Fluent builder for [`Event`].
#[derive(Debug)]
pub struct EventBuilder {
    id: Option<EventId>,
    source: SourceUri,
    subject: Subject,
    payload: Option<Payload>,
    content_type: Option<ContentType>,
    occurred_at: Option<DateTime<Utc>>,
    headers: Option<Headers>,
}

impl EventBuilder {
    /// Override the auto-generated identifier (used when replaying events
    /// from a WAL or re-publishing from a broker).
    pub fn id(mut self, id: EventId) -> Self {
        self.id = Some(id);
        self
    }

    /// Set the payload.
    pub fn payload(mut self, payload: Payload) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Set the declared content type.
    pub fn content_type(mut self, content_type: ContentType) -> Self {
        self.content_type = Some(content_type);
        self
    }

    /// Set the timestamp at the source.
    pub fn occurred_at(mut self, occurred_at: DateTime<Utc>) -> Self {
        self.occurred_at = Some(occurred_at);
        self
    }

    /// Set the headers.
    pub fn headers(mut self, headers: Headers) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Finalize the event. Defaults a missing payload to an empty buffer
    /// and a missing timestamp to `Utc::now()`.
    pub fn build(self) -> Event {
        Event {
            id: self.id.unwrap_or_default(),
            source: self.source,
            subject: self.subject,
            payload: self
                .payload
                .unwrap_or_else(|| Payload::from_vec(Vec::new())),
            content_type: self.content_type.unwrap_or(ContentType::Json),
            occurred_at: self.occurred_at.unwrap_or_else(Utc::now),
            headers: self.headers.unwrap_or_default(),
        }
    }
}

/// Local module for `Bytes` serde support (serde does not implement it natively).
mod bytes_serde {
    use bytes::Bytes;
    use serde::de::{self, SeqAccess, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub(super) fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Bytes;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a byte buffer")
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
            Ok(Bytes::copy_from_slice(v))
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
            Ok(Bytes::from(v))
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
            while let Some(byte) = sequence.next_element::<u8>()? {
                bytes.push(byte);
            }
            Ok(Bytes::from(bytes))
        }
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(BytesVisitor)
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

    #[test]
    fn event_id_round_trips_through_string() {
        let id = EventId::new();
        let s = id.to_string();
        let parsed: EventId = s.parse().expect("ULID round-trip");
        assert_eq!(id, parsed);
    }

    #[test]
    fn source_uri_requires_scheme() {
        assert!(SourceUri::new("postgres://orders.public.orders").is_ok());
        assert!(SourceUri::new("no-scheme-here").is_err());
        assert!(SourceUri::new("").is_err());
    }

    #[test]
    fn subject_validates_segments() {
        assert!(Subject::new("user.signed_up").is_ok());
        assert!(Subject::new("user").is_ok());
        assert!(Subject::new("user.").is_err());
        assert!(Subject::new(".user").is_err());
        assert!(Subject::new("user..thing").is_err());
        assert!(Subject::new("user.signed up").is_err()); // space
        assert!(Subject::new("user.*").is_err()); // wildcard not allowed in published subjects
    }

    #[test]
    fn payload_clone_is_zero_copy() {
        let original = Payload::from_vec(b"hello world".to_vec());
        let cloned = original.clone();
        // Pointer equality of the underlying buffers via Bytes refcount.
        assert_eq!(original.as_slice().as_ptr(), cloned.as_slice().as_ptr());
    }

    #[test]
    fn event_builder_defaults_correctly() {
        let source = SourceUri::new("sdk://test").expect("uri");
        let subject = Subject::new("test.event").expect("subject");
        let event = Event::builder(source.clone(), subject.clone()).build();

        assert_eq!(event.source, source);
        assert_eq!(event.subject, subject);
        assert!(event.payload.is_empty());
        assert_eq!(event.content_type, ContentType::Json);
        assert!(event.headers.is_empty());
    }

    #[test]
    fn event_clone_does_not_duplicate_payload_buffer() {
        let source = SourceUri::new("sdk://test").expect("uri");
        let subject = Subject::new("test.event").expect("subject");
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(vec![0u8; 4096]))
            .build();
        let cloned = event.clone();
        assert_eq!(
            event.payload.as_slice().as_ptr(),
            cloned.payload.as_slice().as_ptr()
        );
    }

    #[test]
    fn payload_custom_serde_preserves_the_existing_wire_shape() {
        let payload = Payload::from_vec(vec![1, 2, 3, 4]);
        let encoded = serde_json::to_string(&payload).expect("serialize");
        assert_eq!(encoded, "[1,2,3,4]");
        let decoded: Payload = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, payload);
    }
}
