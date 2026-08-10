//! Read-side access to sink-materialized documents.
//!
//! Readers resolve keys and index names through the exact encoder
//! functions the write path uses, so a read key is byte-identical to
//! the key the sink wrote. Nothing here mutates sink state.

use std::borrow::Cow;

use serde_json::{json, Value};
use tokio::sync::Mutex;
use ventstream_core::SinkError;

use crate::meilisearch::documents::{encode_primary_key, index_uid_for_target, CANONICAL_ID_FIELD};
use crate::meilisearch::{sink as meilisearch_sink, MeilisearchConfig};
use crate::opensearch::index_template::sanitize_index_segment;
use crate::opensearch::sink::{apply_auth, build_http_client};
use crate::opensearch::OpenSearchConfig;
use crate::redis::keyspace::{
    encode_key_segment, encoded_key_len, key_from_parts, validate_target_segment,
    PROJECTION_TARGET_HEADER,
};
use crate::redis::topology::{build_connector, connect_raw, RedisConnection};
use crate::redis::{RedisConfig, RedisDocumentFormat};

/// Sink configuration accepted by [`SinkReader::connect`].
pub enum SinkReaderConfig {
    /// Redis materialization keyspace.
    Redis(Box<RedisConfig>),
    /// OpenSearch / Elasticsearch indexes.
    OpenSearch(Box<OpenSearchConfig>),
    /// Meilisearch indexes.
    Meilisearch(Box<MeilisearchConfig>),
}

/// Read-only client over one sink's materialized documents.
pub enum SinkReader {
    /// Redis keyspace reader.
    Redis(RedisReader),
    /// OpenSearch / Elasticsearch reader.
    OpenSearch(OpenSearchReader),
    /// Meilisearch reader.
    Meilisearch(MeilisearchReader),
}

impl SinkReader {
    /// Connect a reader. Redis dials and pings eagerly; the HTTP sinks
    /// build their client (TLS validation) without touching the network.
    pub async fn connect(config: SinkReaderConfig) -> Result<Self, SinkError> {
        match config {
            SinkReaderConfig::Redis(config) => {
                let connector = build_connector(&config).await?;
                let connection = connect_raw(&connector, &config).await?;
                Ok(Self::Redis(RedisReader {
                    config,
                    connection: Mutex::new(connection),
                }))
            }
            SinkReaderConfig::OpenSearch(config) => {
                let client = build_http_client(&config)
                    .map_err(|err| SinkError::Blocked(err.to_string()))?;
                Ok(Self::OpenSearch(OpenSearchReader { config, client }))
            }
            SinkReaderConfig::Meilisearch(config) => {
                let client = meilisearch_sink::build_http_client(&config)
                    .map_err(|err| SinkError::Blocked(err.to_string()))?;
                let base_url = config.endpoint.trim_end_matches('/').to_owned();
                Ok(Self::Meilisearch(MeilisearchReader {
                    config,
                    client,
                    base_url,
                }))
            }
        }
    }

    /// Sink kind label for diagnostics.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Redis(_) => "redis",
            Self::OpenSearch(_) => "opensearch",
            Self::Meilisearch(_) => "meilisearch",
        }
    }

    /// Fetch one document by its canonical doc id. `Ok(None)` when absent.
    pub async fn get_document(
        &self,
        target: &str,
        doc_id: &str,
    ) -> Result<Option<Value>, SinkError> {
        match self {
            Self::Redis(reader) => reader.get_document(target, doc_id).await,
            Self::OpenSearch(reader) => reader.get_document(target, doc_id).await,
            Self::Meilisearch(reader) => reader.get_document(target, doc_id).await,
        }
    }

    /// Full-text search over one target. Redis targets do not support search.
    pub async fn search(
        &self,
        target: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>, SinkError> {
        match self {
            Self::Redis(_) => Err(SinkError::Blocked(
                "redis targets support scan, not search".to_owned(),
            )),
            Self::OpenSearch(reader) => reader.search(target, query, limit).await,
            Self::Meilisearch(reader) => reader.search(target, query, limit).await,
        }
    }

    /// Scan doc ids matching a glob pattern. Redis targets only.
    pub async fn scan(
        &self,
        target: &str,
        pattern: &str,
        limit: usize,
    ) -> Result<Vec<String>, SinkError> {
        match self {
            Self::Redis(reader) => reader.scan(target, pattern, limit).await,
            Self::OpenSearch(_) | Self::Meilisearch(_) => Err(SinkError::Blocked(
                "only redis targets support scan; use search".to_owned(),
            )),
        }
    }
}

/// Reader over a Redis materialization keyspace.
pub struct RedisReader {
    config: Box<RedisConfig>,
    connection: Mutex<RedisConnection>,
}

impl RedisReader {
    async fn get_document(&self, target: &str, doc_id: &str) -> Result<Option<Value>, SinkError> {
        let key = redis_data_key(&self.config, target, doc_id)?;
        let mut command = match self.config.document_format {
            RedisDocumentFormat::String => redis::cmd("GET"),
            RedisDocumentFormat::Json => redis::cmd("JSON.GET"),
        };
        command.arg(&key);
        let mut connection = self.connection.lock().await;
        let raw: Option<String> = connection
            .query_on_primary(command, &key)
            .await
            .map_err(|err| SinkError::Connection(format!("redis read failed: {err}")))?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        match self.config.document_format {
            // Stored strings are usually JSON documents; pass through
            // non-JSON values as a JSON string.
            RedisDocumentFormat::String => Ok(Some(
                serde_json::from_str(&raw).unwrap_or(Value::String(raw)),
            )),
            RedisDocumentFormat::Json => serde_json::from_str(&raw).map(Some).map_err(|err| {
                SinkError::Internal(format!("RedisJSON value at {key} is not valid JSON: {err}"))
            }),
        }
    }

    async fn scan(
        &self,
        target: &str,
        pattern: &str,
        limit: usize,
    ) -> Result<Vec<String>, SinkError> {
        const SCAN_COUNT: usize = 500;
        const MAX_SCAN_ROUNDS: usize = 10_000;

        validate_target_segment(target).map_err(SinkError::Blocked)?;
        let base = redis_target_key_base(&self.config.key_prefix, target);
        let match_pattern = format!("{base}{}", encode_scan_pattern(pattern));
        if match_pattern.len() > self.config.max_key_bytes {
            return Err(SinkError::Blocked(format!(
                "Redis scan pattern is {} bytes; max_key_bytes={}",
                match_pattern.len(),
                self.config.max_key_bytes
            )));
        }
        let mut connection = self.connection.lock().await;
        let mut cursor = 0u64;
        let mut doc_ids = Vec::new();
        for _ in 0..MAX_SCAN_ROUNDS {
            let mut command = redis::cmd("SCAN");
            command
                .arg(cursor)
                .arg("MATCH")
                .arg(&match_pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT);
            let (next_cursor, keys): (u64, Vec<String>) = connection
                .query_on_primary(command, &base)
                .await
                .map_err(|err| SinkError::Connection(format!("redis scan failed: {err}")))?;
            for key in keys {
                if let Some(encoded) = key.strip_prefix(&base) {
                    doc_ids.push(percent_decode(encoded));
                    if doc_ids.len() >= limit {
                        return Ok(doc_ids);
                    }
                }
            }
            cursor = next_cursor;
            if cursor == 0 {
                return Ok(doc_ids);
            }
        }
        Ok(doc_ids)
    }
}

/// Build the data key for one document, using the writer's encoder and
/// the writer's key-size limit.
pub(crate) fn redis_data_key(
    config: &RedisConfig,
    target: &str,
    doc_id: &str,
) -> Result<String, SinkError> {
    validate_target_segment(target).map_err(SinkError::Blocked)?;
    if doc_id.is_empty() || doc_id.chars().any(char::is_control) {
        return Err(SinkError::Blocked(
            "doc id must be non-empty and contain no control characters".to_owned(),
        ));
    }
    let key_len = encoded_key_len(&config.key_prefix, target, doc_id);
    if key_len > config.max_key_bytes {
        return Err(SinkError::Blocked(format!(
            "Redis key is {key_len} bytes; max_key_bytes={}",
            config.max_key_bytes
        )));
    }
    Ok(key_from_parts(&config.key_prefix, target, doc_id, key_len))
}

/// `{prefix}:{{target}}:` — the shared prefix of every data key in one
/// target, produced by the writer's own key builder with an empty doc id.
fn redis_target_key_base(prefix: &str, target: &str) -> String {
    key_from_parts(prefix, target, "", encoded_key_len(prefix, target, ""))
}

/// Encode the doc-id part of a scan pattern with the writer's segment
/// encoder while leaving `*` and `?` glob wildcards intact.
fn encode_scan_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut literal = String::new();
    for ch in pattern.chars() {
        if ch == '*' || ch == '?' {
            out.push_str(&encode_key_segment(&literal));
            literal.clear();
            out.push(ch);
        } else {
            literal.push(ch);
        }
    }
    out.push_str(&encode_key_segment(&literal));
    out
}

/// Inverse of the key-segment percent encoding. Malformed escapes pass
/// through verbatim.
fn percent_decode(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let mut iter = input.bytes();
    while let Some(byte) = iter.next() {
        if byte != b'%' {
            bytes.push(byte);
            continue;
        }
        let mut lookahead = iter.clone();
        match (lookahead.next(), lookahead.next()) {
            (Some(high), Some(low)) => {
                let pair = [high, low];
                match u8::from_str_radix(&String::from_utf8_lossy(&pair), 16) {
                    Ok(decoded) => {
                        bytes.push(decoded);
                        iter = lookahead;
                    }
                    Err(_) => bytes.push(byte),
                }
            }
            _ => bytes.push(byte),
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Reader over OpenSearch / Elasticsearch indexes.
pub struct OpenSearchReader {
    config: Box<OpenSearchConfig>,
    client: reqwest::Client,
}

impl OpenSearchReader {
    /// Resolve the index for a target. Only routings whose result is a
    /// pure function of the target are readable: the projection-target
    /// template applies the writer's sanitizer to the target; a template
    /// without placeholders is a fixed index name.
    fn resolve_index(&self, target: &str) -> Result<String, SinkError> {
        let template = &self.config.index_template;
        let projection_template = format!("${{header:{PROJECTION_TARGET_HEADER}}}");
        if *template == projection_template {
            return Ok(sanitize_index_segment(target));
        }
        if !template.contains('$') && !template.contains('%') {
            return Ok(template.clone());
        }
        Err(SinkError::Blocked(
            "index not derivable for target; template routing unsupported for reads".to_owned(),
        ))
    }

    async fn get_document(&self, target: &str, doc_id: &str) -> Result<Option<Value>, SinkError> {
        let index = self.resolve_index(target)?;
        let url = format!(
            "{}/{index}/_doc/{}",
            self.config.endpoint.trim_end_matches('/'),
            percent_encode_path_segment(doc_id)
        );
        let response = apply_auth(self.client.get(url), &self.config.auth)
            .send()
            .await
            .map_err(|err| SinkError::Connection(format!("opensearch read failed: {err}")))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = read_success_json(response, "opensearch get").await?;
        match body.get("_source") {
            Some(source) => Ok(Some(source.clone())),
            None => Err(SinkError::Internal(
                "opensearch get response has no _source".to_owned(),
            )),
        }
    }

    async fn search(
        &self,
        target: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>, SinkError> {
        let index = self.resolve_index(target)?;
        let url = format!(
            "{}/{index}/_search",
            self.config.endpoint.trim_end_matches('/')
        );
        let body = json!({
            "query": {"query_string": {"query": query}},
            "size": limit,
        });
        let response = apply_auth(self.client.post(url), &self.config.auth)
            .json(&body)
            .send()
            .await
            .map_err(|err| SinkError::Connection(format!("opensearch search failed: {err}")))?;
        let body = read_success_json(response, "opensearch search").await?;
        let hits = body
            .pointer("/hits/hits")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(hits
            .into_iter()
            .filter_map(|hit| {
                let id = hit.get("_id").and_then(Value::as_str).map(str::to_owned);
                let mut source = hit.get("_source")?.clone();
                // Surface the canonical doc id under the same field the
                // Meilisearch writer stores it as.
                if let (Some(map), Some(id)) = (source.as_object_mut(), id) {
                    map.insert(CANONICAL_ID_FIELD.to_owned(), Value::String(id));
                }
                Some(source)
            })
            .collect())
    }
}

/// Reader over Meilisearch indexes.
pub struct MeilisearchReader {
    config: Box<MeilisearchConfig>,
    client: reqwest::Client,
    base_url: String,
}

impl MeilisearchReader {
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut builder = self
            .client
            .request(method, format!("{}{path}", self.base_url));
        if let Some(key) = &self.config.api_key {
            builder = builder.bearer_auth(key);
        }
        builder
    }

    fn index_uid(&self, target: &str) -> Result<String, SinkError> {
        index_uid_for_target(&self.config, target).map_err(SinkError::Blocked)
    }

    async fn get_document(&self, target: &str, doc_id: &str) -> Result<Option<Value>, SinkError> {
        let uid = self.index_uid(target)?;
        let path = format!("/indexes/{uid}/documents/{}", encode_primary_key(doc_id));
        let response = self
            .request(reqwest::Method::GET, &path)
            .send()
            .await
            .map_err(|err| SinkError::Connection(format!("meilisearch read failed: {err}")))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        read_success_json(response, "meilisearch get")
            .await
            .map(Some)
    }

    async fn search(
        &self,
        target: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>, SinkError> {
        let uid = self.index_uid(target)?;
        let response = self
            .request(reqwest::Method::POST, &format!("/indexes/{uid}/search"))
            .json(&json!({"q": query, "limit": limit}))
            .send()
            .await
            .map_err(|err| SinkError::Connection(format!("meilisearch search failed: {err}")))?;
        let body = read_success_json(response, "meilisearch search").await?;
        Ok(body
            .get("hits")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }
}

/// Require a success status and parse the JSON body. Auth rejections are
/// operator errors; everything else is internal.
async fn read_success_json(response: reqwest::Response, context: &str) -> Result<Value, SinkError> {
    const MAX_ERROR_BODY: usize = 256;

    let status = response.status();
    if !status.is_success() {
        let body: String = response
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(MAX_ERROR_BODY)
            .collect();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(SinkError::Blocked(format!(
                "{context} rejected with {status}: {body}"
            )));
        }
        return Err(SinkError::Internal(format!(
            "{context} failed with {status}: {body}"
        )));
    }
    response
        .json()
        .await
        .map_err(|err| SinkError::Internal(format!("{context} returned invalid JSON: {err}")))
}

/// Percent-encode one URL path segment (RFC 3986 unreserved set kept).
fn percent_encode_path_segment(input: &str) -> Cow<'_, str> {
    if input
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~'))
    {
        return Cow::Borrowed(input);
    }
    let mut out = String::with_capacity(input.len().saturating_mul(3));
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::redis::RedisKeyRouting;

    fn redis_config() -> RedisConfig {
        RedisConfig::new(
            "reader",
            "redis://127.0.0.1:6379",
            "vs",
            RedisKeyRouting::Fixed("orders".to_owned()),
        )
    }

    #[test]
    fn read_key_matches_writer_key_for_same_inputs() {
        let config = redis_config();
        let (target, doc_id) = ("orders", r#"public.orders:["5"]"#);
        // The writer computes exactly this: encoded_key_len + key_from_parts.
        let writer_key = key_from_parts(
            &config.key_prefix,
            target,
            doc_id,
            encoded_key_len(&config.key_prefix, target, doc_id),
        );
        let read_key = redis_data_key(&config, target, doc_id).expect("key");
        assert_eq!(read_key, writer_key);
        assert_eq!(read_key, "vs:{orders}:public.orders%3A%5B%225%22%5D");
    }

    #[test]
    fn oversized_and_malformed_doc_ids_are_blocked() {
        let mut config = redis_config();
        config.max_key_bytes = 24;
        assert!(matches!(
            redis_data_key(&config, "orders", &"x".repeat(64)),
            Err(SinkError::Blocked(_))
        ));
        assert!(matches!(
            redis_data_key(&redis_config(), "orders", ""),
            Err(SinkError::Blocked(_))
        ));
    }

    #[test]
    fn scan_pattern_encodes_literals_and_preserves_wildcards() {
        assert_eq!(encode_scan_pattern("user:*"), "user%3A*");
        assert_eq!(encode_scan_pattern("a?b*"), "a?b*");
        assert_eq!(encode_scan_pattern("plain"), "plain");
        let base = redis_target_key_base("vs", "orders");
        assert_eq!(base, "vs:{orders}:");
        assert_eq!(
            format!("{base}{}", encode_scan_pattern("user:*")),
            "vs:{orders}:user%3A*"
        );
    }

    #[test]
    fn percent_decode_inverts_segment_encoding() {
        let original = r#"public.orders:["5"]"#;
        assert_eq!(percent_decode(&encode_key_segment(original)), original);
        assert_eq!(percent_decode("no-escapes"), "no-escapes");
        assert_eq!(percent_decode("bad%zz"), "bad%zz");
    }

    #[test]
    fn opensearch_index_resolution_covers_each_routing_mode() {
        let reader = |template: &str| OpenSearchReader {
            config: Box::new(OpenSearchConfig::new(
                "os",
                "http://127.0.0.1:9200",
                template,
            )),
            client: reqwest::Client::new(),
        };
        // by_projection_target: the writer's sanitizer applies to the target.
        assert_eq!(
            reader("${header:ventstream.target.index}")
                .resolve_index("Orders.View")
                .expect("index"),
            sanitize_index_segment("Orders.View")
        );
        // Fixed / static template: used as-is.
        assert_eq!(
            reader("orders-index")
                .resolve_index("anything")
                .expect("index"),
            "orders-index"
        );
        // Any other template is not derivable from the target alone.
        assert!(matches!(
            reader("events-${subject:1}-%Y").resolve_index("orders"),
            Err(SinkError::Blocked(_))
        ));
    }

    #[test]
    fn meilisearch_index_resolution_matches_writer() {
        let mut config = MeilisearchConfig::new("meili", "http://127.0.0.1:7700");
        config.index_routing = crate::MeilisearchIndexRouting::ByProjectionTarget;
        assert_eq!(
            index_uid_for_target(&config, "public.orders").expect("uid"),
            format!(
                "{}{}",
                config.index_prefix,
                crate::meilisearch::documents::encode_index_segment("public.orders")
            )
        );
        config.index_routing = crate::MeilisearchIndexRouting::Fixed("catalog".to_owned());
        assert_eq!(
            index_uid_for_target(&config, "ignored").expect("uid"),
            "vs_catalog"
        );
    }

    async fn opensearch_reader(server: &MockServer) -> OpenSearchReader {
        let config = Box::new(OpenSearchConfig::new("os", server.uri(), "orders"));
        let client = build_http_client(&config).expect("client");
        OpenSearchReader { config, client }
    }

    #[tokio::test]
    async fn opensearch_get_returns_source_and_none_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/orders/_doc/public.orders%3A%5B%225%22%5D"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "_id": r#"public.orders:["5"]"#,
                "_source": {"id": 5, "status": "open"},
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"found": false})))
            .mount(&server)
            .await;
        let reader = opensearch_reader(&server).await;
        let doc = reader
            .get_document("orders", r#"public.orders:["5"]"#)
            .await
            .expect("get");
        assert_eq!(doc, Some(json!({"id": 5, "status": "open"})));
        let missing = reader
            .get_document("orders", "public.orders:[\"999\"]")
            .await
            .expect("get");
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn opensearch_search_injects_canonical_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/orders/_search"))
            .and(body_partial_json(
                json!({"query": {"query_string": {"query": "open"}}, "size": 5}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "hits": {"hits": [
                    {"_id": r#"public.orders:["5"]"#, "_source": {"status": "open"}},
                ]},
            })))
            .mount(&server)
            .await;
        let reader = opensearch_reader(&server).await;
        let hits = reader.search("orders", "open", 5).await.expect("search");
        assert_eq!(
            hits,
            vec![json!({"status": "open", "_vs_id": r#"public.orders:["5"]"#})]
        );
    }

    async fn meilisearch_reader(server: &MockServer) -> MeilisearchReader {
        let mut config = MeilisearchConfig::new("meili", server.uri());
        config.index_routing = crate::MeilisearchIndexRouting::ByProjectionTarget;
        let client = meilisearch_sink::build_http_client(&config).expect("client");
        let base_url = config.endpoint.trim_end_matches('/').to_owned();
        MeilisearchReader {
            config: Box::new(config),
            client,
            base_url,
        }
    }

    #[tokio::test]
    async fn meilisearch_get_uses_encoded_primary_key_and_maps_404() {
        let server = MockServer::start().await;
        let doc_id = r#"public.orders:["5"]"#;
        let uid = format!(
            "vs_{}",
            crate::meilisearch::documents::encode_index_segment("public.orders")
        );
        Mock::given(method("GET"))
            .and(path(format!(
                "/indexes/{uid}/documents/{}",
                encode_primary_key(doc_id)
            )))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"_vs_id": doc_id, "id": 5})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"code": "not_found"})))
            .mount(&server)
            .await;
        let reader = meilisearch_reader(&server).await;
        let doc = reader
            .get_document("public.orders", doc_id)
            .await
            .expect("get");
        assert_eq!(doc, Some(json!({"_vs_id": doc_id, "id": 5})));
        let missing = reader
            .get_document("public.orders", "public.orders:[\"9\"]")
            .await
            .expect("get");
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn meilisearch_search_returns_hits() {
        let server = MockServer::start().await;
        let uid = format!(
            "vs_{}",
            crate::meilisearch::documents::encode_index_segment("public.orders")
        );
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{uid}/search")))
            .and(body_partial_json(json!({"q": "open", "limit": 3})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "hits": [{"id": 5, "status": "open"}],
            })))
            .mount(&server)
            .await;
        let reader = meilisearch_reader(&server).await;
        let hits = reader
            .search("public.orders", "open", 3)
            .await
            .expect("search");
        assert_eq!(hits, vec![json!({"id": 5, "status": "open"})]);
    }
}
