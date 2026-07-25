//! `BoltType` → `serde_json::Value` conversion.
//!
//! Neo4j's Bolt protocol carries values as a tagged union (`BoltType`).
//! The OpenSearch sink (and every other downstream consumer) speaks
//! `serde_json::Value`. This module bridges them.
//!
//! ### Why this matters
//!
//! The spike's first cut used `format!("{:?}", value)` for temporal
//! types, which produced unindexable strings like
//! `"DateTime(BoltDateTime { seconds: BoltInteger { val ... } })"`. The
//! production source must emit ISO-8601-shaped strings so OS dynamic
//! mapping infers `date` instead of `text`, and downstream consumers
//! can parse them back to wall-clock instants.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat};
use neo4rs::BoltType;

/// Convert any `BoltType` to a JSON value. Lossless for primitives,
/// containers, and nodes / relationships. For temporal types, emits the
/// canonical ISO-8601 representation. Falls back to a `Debug`-formatted
/// string for value variants we don't otherwise recognise — keeping the
/// payload structurally valid JSON rather than panicking or dropping
/// data.
pub fn bolt_to_json(b: &BoltType) -> serde_json::Value {
    use serde_json::{json, Value};
    match b {
        BoltType::Null(_) => Value::Null,
        BoltType::Boolean(v) => Value::Bool(v.value),
        BoltType::Integer(v) => json!(v.value),
        BoltType::Float(v) => json!(v.value),
        BoltType::String(v) => Value::String(v.value.clone()),
        BoltType::List(v) => Value::Array(v.value.iter().map(bolt_to_json).collect()),
        BoltType::Map(v) => {
            let mut m = serde_json::Map::with_capacity(v.value.len());
            for (k, val) in &v.value {
                m.insert(k.value.clone(), bolt_to_json(val));
            }
            Value::Object(m)
        }
        BoltType::Bytes(v) => json!(v.value.to_vec()),

        // Temporal types — emit ISO-8601 strings. If conversion fails
        // (shouldn't, for any value Neo4j actually emits) fall back to
        // a debug string rather than discarding the value silently.
        BoltType::DateTime(dt) => match DateTime::<FixedOffset>::try_from(dt) {
            Ok(chrono_dt) => Value::String(chrono_dt.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
            Err(_) => Value::String(format!("{dt:?}")),
        },
        BoltType::LocalDateTime(dt) => match NaiveDateTime::try_from(dt) {
            // Use the same `T` separator + microsecond precision RFC 3339
            // would use, but without a timezone suffix — that matches
            // OpenSearch's `strict_date_optional_time` parser.
            Ok(chrono_dt) => Value::String(chrono_dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
            Err(_) => Value::String(format!("{dt:?}")),
        },
        BoltType::Date(d) => match NaiveDate::try_from(d) {
            Ok(chrono_d) => Value::String(chrono_d.format("%Y-%m-%d").to_string()),
            Err(_) => Value::String(format!("{d:?}")),
        },
        BoltType::Time(t) => {
            let (nt, offset): (NaiveTime, FixedOffset) = t.into();
            // RFC 3339 time-only: `15:30:00.123456789+02:00`.
            Value::String(format!(
                "{}{}",
                nt.format("%H:%M:%S%.f"),
                offset_to_iso(offset),
            ))
        }
        BoltType::LocalTime(t) => {
            let nt: NaiveTime = t.into();
            Value::String(nt.format("%H:%M:%S%.f").to_string())
        }
        BoltType::Duration(d) => {
            // ISO-8601 duration `PnDTnHnMnS`. Neo4j durations carry days,
            // seconds, nanoseconds — months/years are not representable
            // (Neo4j stores months separately as days only in 5.x). We
            // emit `P0Y0M{D}DT{H}H{M}M{S}.{N}S` with leading zeros
            // dropped sensibly.
            let total: std::time::Duration = d.clone().into();
            let secs = total.as_secs();
            let nanos = total.subsec_nanos();
            let days = secs / 86_400;
            let rem = secs % 86_400;
            let hours = rem / 3_600;
            let rem = rem % 3_600;
            let mins = rem / 60;
            let secs_only = rem % 60;
            let mut s = format!("P{days}DT{hours}H{mins}M{secs_only}");
            if nanos > 0 {
                let nanos_str = format!("{nanos:09}");
                let trimmed = nanos_str.trim_end_matches('0');
                if !trimmed.is_empty() {
                    s.push('.');
                    s.push_str(trimmed);
                }
            }
            s.push('S');
            Value::String(s)
        }

        // Nodes / relationships — Neo4j CDC delivers these as nested
        // `Map`s inside `event`, so reaching these arms is defensive.
        // Use Debug rather than dropping data.
        BoltType::Node(n) => json!(format!("{n:?}")),
        BoltType::Relation(r) => json!(format!("{r:?}")),
        BoltType::UnboundedRelation(r) => json!(format!("{r:?}")),
        BoltType::Path(p) => json!(format!("{p:?}")),
        BoltType::Point2D(p) => json!(format!("{p:?}")),
        BoltType::Point3D(p) => json!(format!("{p:?}")),
        BoltType::DateTimeZoneId(dt) => match NaiveDateTime::try_from(dt) {
            Ok(naive) => Value::String(naive.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
            Err(_) => Value::String(format!("{dt:?}")),
        },
    }
}

/// Format a `FixedOffset` as the RFC 3339 `±HH:MM` suffix. `Z` for UTC.
fn offset_to_iso(offset: FixedOffset) -> String {
    let total = offset.local_minus_utc();
    if total == 0 {
        return "Z".to_owned();
    }
    let sign = if total >= 0 { '+' } else { '-' };
    let abs = total.unsigned_abs();
    let h = abs / 3_600;
    let m = (abs % 3_600) / 60;
    format!("{sign}{h:02}:{m:02}")
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
    use neo4rs::{BoltBoolean, BoltDate, BoltInteger, BoltString};

    #[test]
    fn primitives_round_trip() {
        assert_eq!(
            bolt_to_json(&BoltType::String(BoltString {
                value: "hello".to_owned()
            })),
            "hello"
        );
        assert_eq!(
            bolt_to_json(&BoltType::Integer(BoltInteger { value: 42 })),
            42
        );
        assert_eq!(
            bolt_to_json(&BoltType::Boolean(BoltBoolean { value: true })),
            true
        );
    }

    #[test]
    fn date_emits_iso8601() {
        let naive = NaiveDate::from_ymd_opt(2026, 5, 25).expect("valid date");
        let bd: BoltDate = naive.into();
        let v = bolt_to_json(&BoltType::Date(bd));
        assert_eq!(v, serde_json::json!("2026-05-25"));
    }
}
