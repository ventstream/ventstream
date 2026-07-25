//! Tenant extraction + validation for the `connection_init` payload and the
//! HTTP path.
//!
//! The caller supplies a `tenant` + non-empty `authToken`. Cross-tenant
//! isolation is enforced by [`validate_tenant`]: when the deployment is
//! configured with a single expected tenant (the "single tenant per
//! deployment" model — `VS_TENANT`), the client-asserted tenant MUST equal
//! it, so a client can never assert another tenant's name and stream its
//! firehose. When no tenant is configured (dev / legacy), the asserted tenant
//! is accepted unvalidated (a loud startup warning is emitted by the binary).
//!
//! Cryptographic token→tenant binding (JWT / service-account keys) lands as a
//! follow-up; this is the single seam to upgrade without touching resolvers —
//! the `authToken` non-empty check is the placeholder for it.

use serde::Deserialize;
use ventstream_realtime::Cursor;

/// Tenant identity attached to the subscription connection. Set by
/// [`extract`] from the `connection_init` payload and stuffed into
/// the async-graphql `Data` so resolvers can read it from `Context`.
#[derive(Debug, Clone)]
pub(crate) struct Tenant(pub(crate) String);

/// Shape we expect in the `connection_init` payload.
///
/// graphql-transport-ws clients send arbitrary JSON in `connection_init`;
/// Apollo Client puts it together via the `connectionParams` config.
#[derive(Debug, Deserialize)]
struct InitPayload {
    #[serde(rename = "authToken")]
    auth_token: String,
    tenant: String,
    /// Legacy JetStream resume sequence. New clients should use
    /// `resume_from_cursor` so the same protocol works with every provider.
    #[serde(default, rename = "resume_from_seq")]
    resume_from_seq: Option<CursorInput>,
    /// Provider-neutral resume cursor. New clients should use this field.
    #[serde(default, rename = "resume_from_cursor")]
    resume_from_cursor: Option<CursorInput>,
}

/// A resume cursor as it may arrive on the wire. JSON numbers are accepted
/// only for legacy JetStream clients; provider-neutral cursors are strings.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CursorInput {
    Num(u64),
    Str(String),
}

impl CursorInput {
    fn to_cursor(&self) -> Result<Cursor, String> {
        match self {
            CursorInput::Num(n) => Ok(Cursor::jetstream(*n)),
            CursorInput::Str(s) => Cursor::from_wire_compatible(s)
                .map_err(|err| format!("connection_init payload: invalid resume cursor: {err}")),
        }
    }
}

/// Resolved `connection_init` context: the validated tenant plus an
/// optional resume cursor to start the connection's consumer from.
#[derive(Debug, Clone)]
pub(crate) struct InitContext {
    pub(crate) tenant: Tenant,
    /// Last successfully processed broker cursor; `None` means live-only.
    pub(crate) resume_from_cursor: Option<Cursor>,
}

/// Enforce the single-tenant-per-deployment gate.
///
/// `expected` is the server-configured tenant for this deployment:
/// - `Some(t)` → the client-asserted tenant must equal `t`, else reject. This
///   is what closes cross-tenant access: the *server*, not the client, decides
///   the tenant.
/// - `None` → no tenant configured (dev / legacy); the asserted tenant is
///   accepted as-is. The binary logs a loud warning at startup in this case.
///
/// The error message deliberately does NOT echo `expected`, so a probing
/// client can't learn the deployment's tenant name.
pub(crate) fn validate_tenant(asserted: &str, expected: Option<&str>) -> Result<(), String> {
    match expected {
        Some(t) if t == asserted => Ok(()),
        Some(_) => Err(format!(
            "tenant '{asserted}' is not authorized for this deployment"
        )),
        None => Ok(()),
    }
}

/// Parse the connection_init payload, validate the auth fields, and enforce
/// the tenant gate against `expected`.
///
/// On success, returns the [`InitContext`] (tenant + optional resume
/// cursor) to attach to the subscription connection. On failure, returns
/// a message that async-graphql will surface as a connection_init error.
pub(crate) fn extract(
    value: serde_json::Value,
    expected: Option<&str>,
) -> Result<InitContext, String> {
    let payload: InitPayload =
        serde_json::from_value(value).map_err(|err| format!("connection_init payload: {err}"))?;
    if payload.auth_token.is_empty() {
        return Err("connection_init payload: authToken is empty".into());
    }
    if payload.tenant.is_empty() {
        return Err("connection_init payload: tenant is empty".into());
    }
    validate_tenant(&payload.tenant, expected)?;
    let legacy_cursor = payload
        .resume_from_seq
        .as_ref()
        .map(CursorInput::to_cursor)
        .transpose()?;
    let current_cursor = payload
        .resume_from_cursor
        .as_ref()
        .map(CursorInput::to_cursor)
        .transpose()?;
    let resume_from_cursor = match (legacy_cursor, current_cursor) {
        (Some(legacy), Some(current)) if legacy != current => {
            return Err(
                "connection_init payload: resume_from_seq and resume_from_cursor disagree"
                    .to_owned(),
            );
        }
        (Some(legacy), _) => Some(legacy),
        (_, current) => current,
    };
    Ok(InitContext {
        tenant: Tenant(payload.tenant),
        resume_from_cursor,
    })
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
    fn accepts_well_formed_payload_when_unconfigured() {
        let v = serde_json::json!({"authToken": "demo", "tenant": "acme"});
        let c = extract(v, None).unwrap();
        assert_eq!(c.tenant.0, "acme");
        assert_eq!(c.resume_from_cursor, None, "no cursor → live-only");
    }

    #[test]
    fn parses_resume_cursor_as_number_or_string() {
        let n = extract(
            serde_json::json!({"authToken": "demo", "tenant": "acme", "resume_from_seq": 42}),
            None,
        )
        .unwrap();
        assert_eq!(n.resume_from_cursor, Some(Cursor::jetstream(42)));
        let s = extract(
            serde_json::json!({"authToken": "demo", "tenant": "acme", "resume_from_seq": "42"}),
            None,
        )
        .unwrap();
        assert_eq!(s.resume_from_cursor, Some(Cursor::jetstream(42)));
        let error = extract(
            serde_json::json!({"authToken": "demo", "tenant": "acme", "resume_from_seq": "nope"}),
            None,
        )
        .unwrap_err();
        assert!(error.contains("resume cursor"));
    }

    #[test]
    fn parses_redis_cursor_and_rejects_disagreeing_compatibility_fields() {
        let redis = extract(
            serde_json::json!({
                "authToken": "demo",
                "tenant": "acme",
                "resume_from_cursor": "rs:1712345678901-7"
            }),
            None,
        )
        .unwrap();
        assert_eq!(
            redis.resume_from_cursor,
            Some(Cursor::redis_streams(1_712_345_678_901, 7))
        );

        let error = extract(
            serde_json::json!({
                "authToken": "demo",
                "tenant": "acme",
                "resume_from_seq": "42",
                "resume_from_cursor": "43"
            }),
            None,
        )
        .unwrap_err();
        assert!(error.contains("disagree"));
    }

    #[test]
    fn rejects_empty_token() {
        let v = serde_json::json!({"authToken": "", "tenant": "acme"});
        extract(v, None).unwrap_err();
    }

    #[test]
    fn rejects_missing_tenant() {
        let v = serde_json::json!({"authToken": "demo"});
        extract(v, None).unwrap_err();
    }

    #[test]
    fn accepts_matching_configured_tenant() {
        let v = serde_json::json!({"authToken": "demo", "tenant": "acme"});
        let c = extract(v, Some("acme")).unwrap();
        assert_eq!(c.tenant.0, "acme");
    }

    #[test]
    fn rejects_mismatched_tenant_and_does_not_leak_expected() {
        let v = serde_json::json!({"authToken": "demo", "tenant": "victim"});
        let err = extract(v, Some("acme")).unwrap_err();
        assert!(err.contains("victim"), "echoes the asserted tenant");
        assert!(
            !err.contains("acme"),
            "must NOT leak the configured tenant: {err}"
        );
    }

    #[test]
    fn validate_tenant_matrix() {
        assert!(validate_tenant("acme", Some("acme")).is_ok());
        assert!(validate_tenant("victim", Some("acme")).is_err());
        assert!(validate_tenant("anything", None).is_ok());
    }
}
