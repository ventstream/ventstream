//! Configuration for the Kafka/Redpanda CDC source.

use std::time::Duration;

/// How a message value is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnwrapMode {
    /// Debezium change envelope (`before`/`after`/`source`/`op`).
    Debezium,
    /// Message value is the document as-is; key is the id; null = delete.
    Raw,
}

impl UnwrapMode {
    /// Parse leniently; unknown/empty falls back to Debezium.
    pub fn from_str_lenient(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "raw" | "none" | "passthrough" => Self::Raw,
            _ => Self::Debezium,
        }
    }
}

/// One Kafka/Redpanda CDC source.
#[derive(Debug, Clone)]
pub struct KafkaCdcConfig {
    /// Stable source id.
    pub id: String,
    /// `bootstrap.servers` (CSV of host:port).
    pub brokers: String,
    /// Consumer group id (offsets are committed under it).
    pub group_id: String,
    /// Topics to subscribe to. A single entry beginning with `^` is treated
    /// by librdkafka as a subscription regex.
    pub topics: Vec<String>,
    /// Override the namespace stamped on subjects/headers. When `None`, the
    /// namespace is derived per message from the Debezium `source` block
    /// (raw mode falls back to the topic name).
    pub namespace_override: Option<String>,
    /// How to interpret the message value.
    pub unwrap: UnwrapMode,
    /// `auto.offset.reset` for a group with no committed offset
    /// (`earliest` = replay history, `latest` = live only).
    pub auto_offset_reset: String,
    /// `security.protocol` (e.g. `SASL_SSL`); `None` = plaintext.
    pub security_protocol: Option<String>,
    /// `sasl.mechanism` (e.g. `SCRAM-SHA-512`, `PLAIN`).
    pub sasl_mechanism: Option<String>,
    /// SASL username.
    pub sasl_username: Option<String>,
    /// SASL password.
    pub sasl_password: Option<String>,
    /// `ssl.ca.location` (PEM bundle path); `None` = system trust store.
    pub ssl_ca_location: Option<String>,
    /// Raw mode: the field within the value to use as the document id when
    /// the message key is absent. `None` = require a key.
    pub raw_key_field: Option<String>,
    /// How often committable offsets are flushed to the broker (batched,
    /// gated on sink durability).
    pub commit_interval: Duration,
}

impl KafkaCdcConfig {
    /// Config with sensible defaults; callers override fields.
    pub fn new(
        id: impl Into<String>,
        brokers: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            brokers: brokers.into(),
            group_id: group_id.into(),
            topics: Vec::new(),
            namespace_override: None,
            unwrap: UnwrapMode::Debezium,
            auto_offset_reset: "earliest".to_owned(),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            raw_key_field: None,
            commit_interval: Duration::from_millis(1000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwrap_mode_parsing() {
        assert_eq!(UnwrapMode::from_str_lenient("raw"), UnwrapMode::Raw);
        assert_eq!(
            UnwrapMode::from_str_lenient("debezium"),
            UnwrapMode::Debezium
        );
        assert_eq!(UnwrapMode::from_str_lenient(""), UnwrapMode::Debezium);
    }

    #[test]
    fn defaults() {
        let c = KafkaCdcConfig::new("k", "localhost:9092", "vs");
        assert_eq!(c.auto_offset_reset, "earliest");
        assert_eq!(c.unwrap, UnwrapMode::Debezium);
        assert!(c.topics.is_empty());
    }
}
