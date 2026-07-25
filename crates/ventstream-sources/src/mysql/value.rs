//! mysql `Value` -> `serde_json::Value`.

use mysql_async::Value;
use serde_json::{json, Value as Json};

/// Convert a column value. `is_json` parses a JSON-typed column (MySQL returns
/// it as bytes of JSON text) into nested JSON instead of a string.
pub(crate) fn value_to_json(v: &Value, is_json: bool) -> Json {
    match v {
        Value::NULL => Json::Null,
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f),
        Value::Double(d) => json!(d),
        Value::Bytes(b) => {
            if is_json {
                if let Ok(j) = serde_json::from_slice::<Json>(b) {
                    return j;
                }
            }
            match std::str::from_utf8(b) {
                Ok(s) => Json::String(s.to_owned()),
                Err(_) => Json::String(hex(b)),
            }
        }
        // DATE/DATETIME: date-only when the time part is zero, else ISO-8601.
        Value::Date(y, mo, d, h, mi, s, us) => {
            Json::String(fmt_datetime((*y, *mo, *d, *h, *mi, *s, *us)))
        }
        Value::Time(neg, days, h, mi, s, us) => {
            Json::String(fmt_time(*neg, *days, *h, *mi, *s, *us))
        }
    }
}

fn fmt_datetime(dt: (u16, u8, u8, u8, u8, u8, u32)) -> String {
    let (y, mo, d, h, mi, s, us) = dt;
    if h == 0 && mi == 0 && s == 0 && us == 0 {
        format!("{y:04}-{mo:02}-{d:02}")
    } else if us == 0 {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}")
    }
}

fn fmt_time(neg: bool, days: u32, h: u8, mi: u8, s: u8, us: u32) -> String {
    let sign = if neg { "-" } else { "" };
    let hours = days * 24 + u32::from(h);
    if us == 0 {
        format!("{sign}{hours:02}:{mi:02}:{s:02}")
    } else {
        format!("{sign}{hours:02}:{mi:02}:{s:02}.{us:06}")
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(b & 0x0f), 16).unwrap_or('0'));
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn scalars() {
        assert_eq!(value_to_json(&Value::Int(7), false), json!(7));
        assert_eq!(value_to_json(&Value::UInt(7), false), json!(7));
        assert_eq!(value_to_json(&Value::NULL, false), Json::Null);
        assert_eq!(value_to_json(&Value::Double(1.5), false), json!(1.5));
    }

    #[test]
    fn bytes_as_string_or_json() {
        assert_eq!(
            value_to_json(&Value::Bytes(b"hello".to_vec()), false),
            json!("hello")
        );
        // JSON-typed column parses to nested JSON.
        assert_eq!(
            value_to_json(&Value::Bytes(br#"{"a":1}"#.to_vec()), true),
            json!({"a": 1})
        );
        // Same bytes, not a JSON column → stays a string.
        assert_eq!(
            value_to_json(&Value::Bytes(br#"{"a":1}"#.to_vec()), false),
            json!("{\"a\":1}")
        );
    }

    #[test]
    fn dates_and_times() {
        assert_eq!(
            value_to_json(&Value::Date(2024, 1, 15, 0, 0, 0, 0), false),
            json!("2024-01-15")
        );
        assert_eq!(
            value_to_json(&Value::Date(2024, 1, 15, 10, 30, 0, 0), false),
            json!("2024-01-15T10:30:00")
        );
        assert_eq!(
            value_to_json(&Value::Time(false, 0, 1, 2, 3, 0), false),
            json!("01:02:03")
        );
    }
}
