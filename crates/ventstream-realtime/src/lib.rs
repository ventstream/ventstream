//! Broker-neutral contracts shared by VentStream's realtime gateways.
//!
//! Native WebSocket and GraphQL connections consume the same canonical
//! VentStream events, but the broker used to carry those events is a deployment
//! choice. This crate keeps gateway code independent from NATS, Redis, and
//! future providers such as Kafka. Provider crates own connectivity, replay,
//! acknowledgement, and cleanup details behind [`RealtimeBroker`] and
//! [`EventSession`].

#![deny(missing_docs)]

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use ventstream_protocol::SubjectPattern;

const MAX_CURSOR_VALUE_BYTES: usize = 4096;

/// A broker mode understood by the provider-neutral contract.
///
/// Modes are explicit because providers expose materially different delivery
/// contracts. This enum also reserves Redis Pub/Sub and Kafka for future
/// adapters; engine configuration currently selects NATS Core, JetStream, or
/// Redis Streams only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrokerKind {
    /// Ephemeral NATS Core subscriptions.
    NatsCore,
    /// Durable NATS JetStream consumers.
    NatsJetStream,
    /// Reserved for a future ephemeral Redis Pub/Sub adapter.
    RedisPubSub,
    /// Durable Redis Streams reads.
    RedisStreams,
    /// Reserved for a future durable Kafka adapter.
    Kafka,
}

impl BrokerKind {
    /// Stable configuration and telemetry name for this broker mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NatsCore => "nats_core",
            Self::NatsJetStream => "nats_jetstream",
            Self::RedisPubSub => "redis_pubsub",
            Self::RedisStreams => "redis_streams",
            Self::Kafka => "kafka",
        }
    }

    /// Delivery capabilities exposed by this broker mode.
    #[must_use]
    pub const fn capabilities(self) -> BrokerCapabilities {
        match self {
            Self::NatsCore | Self::RedisPubSub => BrokerCapabilities {
                replay: false,
                durable: false,
                acknowledgements: false,
                ordering: OrderingScope::PerSubject,
            },
            Self::NatsJetStream | Self::RedisStreams => BrokerCapabilities {
                replay: true,
                durable: true,
                acknowledgements: matches!(self, Self::NatsJetStream),
                ordering: OrderingScope::PerStream,
            },
            Self::Kafka => BrokerCapabilities {
                replay: true,
                durable: true,
                acknowledgements: true,
                ordering: OrderingScope::PerPartition,
            },
        }
    }
}

impl fmt::Display for BrokerKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BrokerKind {
    type Err = BrokerKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "nats_core" | "nats-core" => Ok(Self::NatsCore),
            "nats_jetstream" | "nats-jetstream" | "jetstream" => Ok(Self::NatsJetStream),
            "redis_pubsub" | "redis-pubsub" => Ok(Self::RedisPubSub),
            "redis_streams" | "redis-streams" => Ok(Self::RedisStreams),
            "kafka" => Ok(Self::Kafka),
            other => Err(BrokerKindParseError(other.to_owned())),
        }
    }
}

/// Error returned when a broker mode name is unknown.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unsupported realtime broker kind '{0}'")]
pub struct BrokerKindParseError(String);

/// The ordering boundary guaranteed by a broker mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingScope {
    /// No ordering guarantee is exposed to clients.
    None,
    /// Events retain broker order within one canonical subject.
    PerSubject,
    /// Events retain broker order within one configured stream.
    PerStream,
    /// Events retain broker order within one topic partition.
    PerPartition,
}

/// Delivery features used by gateway configuration validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerCapabilities {
    /// Whether a client can reconnect from a retained cursor.
    pub replay: bool,
    /// Whether events survive subscriber disconnection.
    pub durable: bool,
    /// Whether the adapter acknowledges accepted deliveries to the broker.
    pub acknowledgements: bool,
    /// Scope within which broker delivery is ordered.
    pub ordering: OrderingScope,
}

/// Provider that owns a resumable cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CursorProvider {
    /// NATS JetStream stream sequence.
    NatsJetStream,
    /// Redis Streams entry ID.
    RedisStreams,
    /// Kafka partition-offset checkpoint.
    Kafka,
}

impl CursorProvider {
    /// Stable wire prefix for this cursor provider.
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::NatsJetStream => "js",
            Self::RedisStreams => "rs",
            Self::Kafka => "kf",
        }
    }

    fn from_prefix(prefix: &str) -> Option<Self> {
        match prefix {
            "js" => Some(Self::NatsJetStream),
            "rs" => Some(Self::RedisStreams),
            "kf" => Some(Self::Kafka),
            _ => None,
        }
    }
}

/// An opaque, provider-owned resume cursor.
///
/// Gateways may compare the provider but must not interpret [`Self::value`].
/// The owning adapter validates and orders the value. The wire form is
/// `<provider-prefix>:<provider-value>`, for example `js:42` or
/// `rs:1712345678901-0`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cursor {
    provider: CursorProvider,
    value: Arc<str>,
}

impl Cursor {
    /// Construct a cursor after applying common safety limits.
    pub fn new(provider: CursorProvider, value: impl Into<Arc<str>>) -> Result<Self, CursorError> {
        let value = value.into();
        validate_cursor_value(&value)?;
        Ok(Self { provider, value })
    }

    /// Construct a JetStream cursor from its numeric stream sequence.
    #[must_use]
    pub fn jetstream(sequence: u64) -> Self {
        Self {
            provider: CursorProvider::NatsJetStream,
            value: Arc::from(sequence.to_string()),
        }
    }

    /// Construct a Redis Streams cursor from its numeric entry-ID components.
    #[must_use]
    pub fn redis_streams(milliseconds: u64, sequence: u64) -> Self {
        Self {
            provider: CursorProvider::RedisStreams,
            value: Arc::from(format!("{milliseconds}-{sequence}")),
        }
    }

    /// Parse a versioned wire cursor.
    pub fn from_wire(value: &str) -> Result<Self, CursorError> {
        let (prefix, provider_value) = value.split_once(':').ok_or(CursorError::MissingPrefix)?;
        let provider = CursorProvider::from_prefix(prefix)
            .ok_or_else(|| CursorError::UnknownProvider(prefix.to_owned()))?;
        Self::new(provider, Arc::<str>::from(provider_value))
    }

    /// Parse either a versioned cursor or the legacy decimal JetStream form.
    pub fn from_wire_compatible(value: &str) -> Result<Self, CursorError> {
        let trimmed = value.trim();
        if trimmed.contains(':') {
            return Self::from_wire(trimmed);
        }
        let sequence = trimmed
            .parse::<u64>()
            .map_err(|_| CursorError::InvalidLegacyJetStream)?;
        Ok(Self::jetstream(sequence))
    }

    /// Provider that owns this cursor value.
    #[must_use]
    pub const fn provider(&self) -> CursorProvider {
        self.provider
    }

    /// Provider-owned cursor value without the wire prefix.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Encode the cursor for a client protocol frame.
    #[must_use]
    pub fn to_wire(&self) -> String {
        format!("{}:{}", self.provider.prefix(), self.value)
    }

    /// Encode a cursor while preserving the original decimal JetStream wire
    /// value expected by existing clients. Other providers remain prefixed so
    /// their values cannot be mistaken for JetStream sequences.
    #[must_use]
    pub fn to_compatible_wire(&self) -> String {
        match self.provider {
            CursorProvider::NatsJetStream => self.value.to_string(),
            CursorProvider::RedisStreams | CursorProvider::Kafka => self.to_wire(),
        }
    }

    /// Return the old decimal representation when this is a JetStream cursor.
    #[must_use]
    pub fn legacy_jetstream_value(&self) -> Option<&str> {
        (self.provider == CursorProvider::NatsJetStream).then_some(&self.value)
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.provider.prefix(), self.value)
    }
}

fn validate_cursor_value(value: &str) -> Result<(), CursorError> {
    if value.is_empty() {
        return Err(CursorError::Empty);
    }
    if value.len() > MAX_CURSOR_VALUE_BYTES {
        return Err(CursorError::TooLong {
            max: MAX_CURSOR_VALUE_BYTES,
        });
    }
    if value.chars().any(char::is_control) {
        return Err(CursorError::ControlCharacter);
    }
    Ok(())
}

/// Cursor syntax or compatibility error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    /// A versioned cursor did not include its provider prefix.
    #[error("resume cursor is missing its provider prefix")]
    MissingPrefix,
    /// The provider prefix is not supported by this engine.
    #[error("resume cursor provider '{0}' is not supported")]
    UnknownProvider(String),
    /// The provider-owned cursor value is empty.
    #[error("resume cursor value is empty")]
    Empty,
    /// The cursor exceeds the protocol safety limit.
    #[error("resume cursor exceeds the maximum length of {max} bytes")]
    TooLong {
        /// Maximum accepted provider-value length.
        max: usize,
    },
    /// Cursor values may not contain control characters.
    #[error("resume cursor contains a control character")]
    ControlCharacter,
    /// An unprefixed compatibility cursor was not an unsigned decimal value.
    #[error("legacy JetStream cursor must be an unsigned decimal string")]
    InvalidLegacyJetStream,
}

/// Gateway role opening a broker session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayRole {
    /// Native VentStream WebSocket protocol.
    WebSocket,
    /// GraphQL-over-WebSocket subscription protocol.
    GraphQl,
}

impl GatewayRole {
    /// Stable telemetry and broker-resource name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WebSocket => "ws",
            Self::GraphQl => "graphql",
        }
    }
}

/// Parameters for one client connection's event source.
#[derive(Debug, Clone)]
pub struct SessionRequest {
    /// Gateway role that owns the session.
    pub role: GatewayRole,
    /// Unique connection identifier used for diagnostics and broker resources.
    pub connection_id: Arc<str>,
    /// Server-authorized tenant scope.
    pub tenant: Arc<str>,
    /// Optional broker-side subject filter for an individual subscription.
    ///
    /// `None` selects the tenant-wide stream used by native WebSocket
    /// connections, whose subscriptions change during the connection.
    pub subject_filter: Option<SubjectPattern>,
    /// Last successfully processed client cursor, or `None` for live-only.
    pub resume_after: Option<Cursor>,
}

/// Canonical broker delivery passed to a realtime gateway.
#[derive(Debug, Clone)]
pub struct BrokerEvent {
    /// Canonical anchored VentStream subject.
    pub subject: Arc<str>,
    /// Raw validated-or-to-be-validated event envelope bytes.
    pub payload: Bytes,
    /// Resume cursor when the selected broker supports replay.
    pub cursor: Option<Cursor>,
}

/// Normalized broker failure surfaced to gateways and client protocols.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BrokerError {
    /// Provider configuration is internally inconsistent.
    #[error("realtime broker configuration is invalid: {0}")]
    Configuration(String),
    /// The broker cannot currently serve this session.
    #[error("realtime broker is unavailable: {0}")]
    Unavailable(String),
    /// The requested cursor belongs to another provider.
    #[error(
        "resume cursor belongs to {actual:?}, but the configured provider expects {expected:?}"
    )]
    CursorProviderMismatch {
        /// Provider encoded in the client cursor.
        actual: CursorProvider,
        /// Provider selected by engine configuration.
        expected: CursorProvider,
    },
    /// The requested cursor has fallen outside retention.
    #[error("resume cursor expired: requested {requested}, earliest available {earliest}")]
    ResumeExpired {
        /// Client-provided cursor.
        requested: Cursor,
        /// Earliest retained cursor.
        earliest: Cursor,
    },
    /// The requested cursor is beyond the broker high watermark.
    #[error(
        "resume cursor is ahead of the stream: requested {requested}, latest available {latest}"
    )]
    CursorAhead {
        /// Client-provided cursor.
        requested: Cursor,
        /// Latest cursor known to the broker.
        latest: Cursor,
    },
    /// The provider rejected the structure of its cursor value.
    #[error("resume cursor is invalid: {0}")]
    InvalidCursor(String),
    /// The broker session ended and cannot make further progress.
    #[error("realtime broker session closed: {0}")]
    SessionClosed(String),
}

/// One connection's ordered event source.
///
/// Implementations may create a dedicated broker consumer or multiplex a
/// shared pod-level tailer. [`Self::accepted`] means the gateway placed the
/// event into its bounded local delivery path; it does not mean the remote
/// WebSocket client completed application processing.
#[async_trait]
pub trait EventSession: Send {
    /// Return the next broker delivery, or `None` after an orderly shutdown.
    async fn next(&mut self) -> Result<Option<BrokerEvent>, BrokerError>;

    /// Record successful acceptance into the gateway's local delivery path.
    async fn accepted(&mut self, event: &BrokerEvent) -> Result<(), BrokerError>;
}

/// Factory and health boundary implemented by each realtime broker adapter.
#[async_trait]
pub trait RealtimeBroker: Send + Sync {
    /// Concrete provider mode backing this instance.
    fn kind(&self) -> BrokerKind;

    /// Delivery features exposed by this instance.
    fn capabilities(&self) -> BrokerCapabilities {
        self.kind().capabilities()
    }

    /// Verify that the broker can serve new sessions.
    async fn ready(&self) -> Result<(), BrokerError>;

    /// Open one client connection's ordered event source.
    async fn open_session(
        &self,
        request: SessionRequest,
    ) -> Result<Box<dyn EventSession>, BrokerError>;
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn broker_capabilities_distinguish_live_and_replay_modes() {
        assert!(!BrokerKind::NatsCore.capabilities().replay);
        assert!(!BrokerKind::RedisPubSub.capabilities().replay);
        assert!(BrokerKind::NatsJetStream.capabilities().replay);
        assert!(BrokerKind::RedisStreams.capabilities().replay);
        assert_eq!(
            BrokerKind::Kafka.capabilities().ordering,
            OrderingScope::PerPartition
        );
    }

    #[test]
    fn broker_kind_accepts_stable_names_and_friendly_aliases() {
        assert_eq!(
            "nats_jetstream".parse::<BrokerKind>(),
            Ok(BrokerKind::NatsJetStream)
        );
        assert_eq!(
            "redis-streams".parse::<BrokerKind>(),
            Ok(BrokerKind::RedisStreams)
        );
        assert!("rabbitmq".parse::<BrokerKind>().is_err());
    }

    #[test]
    fn cursor_round_trips_each_resumable_provider() {
        for (provider, value) in [
            (CursorProvider::NatsJetStream, u64::MAX.to_string()),
            (CursorProvider::RedisStreams, "1712345678901-42".to_owned()),
            (CursorProvider::Kafka, "orders:0=42,orders:1=99".to_owned()),
        ] {
            let cursor = Cursor::new(provider, Arc::<str>::from(value)).expect("valid cursor");
            assert_eq!(Cursor::from_wire(&cursor.to_wire()), Ok(cursor));
        }
    }

    #[test]
    fn compatible_cursor_parser_preserves_legacy_jetstream_values() {
        let cursor = Cursor::from_wire_compatible("18446744073709551615")
            .expect("legacy sequence should parse without precision loss");
        assert_eq!(cursor.provider(), CursorProvider::NatsJetStream);
        assert_eq!(cursor.value(), "18446744073709551615");
        assert_eq!(cursor.legacy_jetstream_value(), Some(cursor.value()));
    }

    #[test]
    fn cursor_rejects_unknown_empty_and_oversized_values() {
        assert!(matches!(
            Cursor::from_wire("amqp:42"),
            Err(CursorError::UnknownProvider(_))
        ));
        assert_eq!(Cursor::from_wire("rs:"), Err(CursorError::Empty));
        assert!(matches!(
            Cursor::new(
                CursorProvider::RedisStreams,
                Arc::<str>::from("x".repeat(MAX_CURSOR_VALUE_BYTES + 1))
            ),
            Err(CursorError::TooLong { .. })
        ));
    }
}
