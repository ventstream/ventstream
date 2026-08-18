//! SurrealDB reverse lookup for 1:many child deletes.
//!
//! Unlike the search sinks, no index configuration is required: SurrealQL
//! filters on any field. Join fields can be scalars (a 1:1 embed) or
//! arrays (a 1:many path like `items.item_id`), so every predicate
//! normalizes through `array::flatten([expr])` and compares string forms
//! — the pgoutput/binlog canonical text — via `type::string()`. Values
//! ride bind variables; only validated field names are embedded.

use serde_json::{json, Value};
use ventstream_core::Event;

use super::config::{is_safe_identifier, SurrealDbConfig};
use super::statements::{resolve_table, CANONICAL_ID_FIELD};

/// Read-side client resolving "which parent docs embed this child?".
pub struct SurrealReverseLookup {
    config: SurrealDbConfig,
    client: reqwest::Client,
    rpc_url: String,
    signin_url: String,
    /// Bearer token from `/signin` (scoped users can't use basic auth);
    /// refreshed once on expiry, mirroring the writer.
    token: tokio::sync::RwLock<Option<String>>,
}

impl SurrealReverseLookup {
    /// Build the lookup client with the sink's connection settings.
    pub fn new(config: SurrealDbConfig) -> Result<Self, String> {
        let client = crate::util::build_http_client(
            config.request_timeout,
            config.verify_tls,
            config.ca_file.as_deref(),
        )?;
        let base = config.endpoint.trim_end_matches('/');
        let rpc_url = format!("{base}/rpc");
        let signin_url = format!("{base}/signin");
        Ok(Self {
            config,
            client,
            rpc_url,
            signin_url,
            token: tokio::sync::RwLock::new(None),
        })
    }

    /// Resolve the concrete table a probe event's documents land in,
    /// using the same routing the sink writer applies.
    pub fn resolve_index_for(&self, probe: &Event) -> Option<String> {
        resolve_table(&self.config, probe).ok()
    }

    /// Canonical parent doc ids embedding any of `values` at `field`.
    pub async fn doc_ids_by_terms(
        &self,
        table: &str,
        field: &str,
        values: &[String],
    ) -> Result<Vec<String>, String> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let field = safe_field(field)?;
        let sql = format!(
            "SELECT VALUE {CANONICAL_ID_FIELD} FROM type::table($tb) WHERE {};",
            self.term_predicate(table, field)
        );
        self.collect_ids(&sql, json!({"tb": table, "values": values}))
            .await
    }

    /// Composite-child-PK variant: parents matching any of the tuples
    /// across `fields`, compared position-wise as strings.
    pub async fn doc_ids_by_term_tuples(
        &self,
        table: &str,
        fields: &[String],
        tuples: &[Vec<String>],
    ) -> Result<Vec<String>, String> {
        if fields.is_empty() || tuples.is_empty() {
            return Ok(Vec::new());
        }
        // Per-tuple AND across fields, OR over tuples. Multi-valued join
        // paths make this an over-approximation (cross-element matches),
        // which is safe: the caller recomposes the returned parents and
        // recomposition is idempotent.
        let per_field = fields
            .iter()
            .enumerate()
            .map(|(position, field)| {
                safe_field(field).map(|field| {
                    format!(
                        "{} CONTAINSANY [$t[{position}]]",
                        self.term_source(table, field)
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ");
        let sql = format!(
            "SELECT VALUE {CANONICAL_ID_FIELD} FROM type::table($tb) \
             WHERE array::any($tuples, |$t| {per_field});"
        );
        self.collect_ids(&sql, json!({"tb": table, "tuples": tuples}))
            .await
    }

    async fn signin(&self) -> Result<String, String> {
        let attempts = [
            json!({"user": self.config.username, "pass": self.config.password,
                   "ns": self.config.namespace, "db": self.config.database}),
            json!({"user": self.config.username, "pass": self.config.password,
                   "ns": self.config.namespace}),
            json!({"user": self.config.username, "pass": self.config.password}),
        ];
        let mut last = "no signin attempt ran".to_owned();
        for body in attempts {
            let response = self
                .client
                .post(&self.signin_url)
                .header(reqwest::header::ACCEPT, "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|err| format!("surrealdb reverse lookup signin: {err}"))?;
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            if status.is_success() {
                let parsed: Value = serde_json::from_str(&text)
                    .map_err(|err| format!("surrealdb signin malformed response: {err}"))?;
                let token = parsed
                    .get("token")
                    .and_then(Value::as_str)
                    .ok_or("surrealdb signin response has no token")?
                    .to_owned();
                *self.token.write().await = Some(token.clone());
                return Ok(token);
            }
            last = format!("surrealdb reverse lookup signin HTTP {}", status.as_u16());
            if status.as_u16() != 401 && status.as_u16() != 403 {
                return Err(last);
            }
        }
        Err(last)
    }

    async fn bearer(&self) -> Result<String, String> {
        if let Some(token) = self.token.read().await.clone() {
            return Ok(token);
        }
        self.signin().await
    }

    /// The expression producing string-canonical join values for one
    /// field: the materialized `_vs_lx_*` flat field when the operator
    /// declared it (a plain pre-decode filter, ~5x cheaper than the
    /// closure and index-ready), else the generic flatten/map form.
    fn term_source(&self, table: &str, field: &str) -> String {
        let materialized = self
            .config
            .lookup_fields
            .iter()
            .any(|entry| entry.table == table && entry.field == field);
        if materialized {
            format!("⟨{}⟩", super::config::lookup_field_name(field))
        } else {
            format!(
                "array::map(array::flatten([{}]), |$v| type::string($v))",
                field_path(field)
            )
        }
    }

    fn term_predicate(&self, table: &str, field: &str) -> String {
        format!("{} CONTAINSANY $values", self.term_source(table, field))
    }

    async fn collect_ids(&self, sql: &str, vars: Value) -> Result<Vec<String>, String> {
        let body = json!({"method": "query", "params": [sql, vars]});
        let mut refreshed = false;
        let (status, text) = loop {
            let token = self.bearer().await?;
            let response = self
                .client
                .post(&self.rpc_url)
                .bearer_auth(&token)
                .header("Surreal-NS", &self.config.namespace)
                .header("Surreal-DB", &self.config.database)
                .header(reqwest::header::ACCEPT, "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|err| format!("surrealdb reverse lookup transport: {err}"))?;
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            if status.as_u16() == 401 && !refreshed {
                refreshed = true;
                self.token.write().await.take();
                continue;
            }
            break (status, text);
        };
        if !status.is_success() {
            return Err(format!(
                "surrealdb reverse lookup HTTP {}: {}",
                status.as_u16(),
                super::error::truncate_body(&text)
            ));
        }
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|err| format!("surrealdb reverse lookup malformed response: {err}"))?;
        if let Some(error) = parsed.get("error") {
            return Err(format!("surrealdb reverse lookup rejected: {error}"));
        }
        let first = parsed
            .pointer("/result/0")
            .ok_or("surrealdb reverse lookup response has no statement result")?;
        let ok = first
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "OK");
        if !ok {
            return Err(format!(
                "surrealdb reverse lookup statement failed: {}",
                first.get("result").and_then(Value::as_str).unwrap_or("?")
            ));
        }
        let ids = first
            .get("result")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Ok(ids)
    }
}

/// Render a validated dotted field path with each segment ⟨⟩-escaped, so
/// nested paths traverse (`items.item_id`) instead of naming one field.
fn field_path(field: &str) -> String {
    field
        .split('.')
        .map(|segment| format!("⟨{segment}⟩"))
        .collect::<Vec<_>>()
        .join(".")
}

/// Join-spec field names are already constrained upstream; the gate here
/// keeps ⟨⟩-escaped embedding safe even if that ever loosens.
fn safe_field(field: &str) -> Result<&str, String> {
    if is_safe_identifier(field) {
        Ok(field)
    } else {
        Err(format!(
            "reverse-lookup field `{field}` uses characters outside [A-Za-z0-9_.-]"
        ))
    }
}
