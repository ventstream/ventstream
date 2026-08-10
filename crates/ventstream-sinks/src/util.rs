//! Small helpers shared across sinks.

use std::path::Path;
use std::time::Duration;

use rand::Rng as _;

/// Build the pooled HTTP client shared by the OpenSearch and Meilisearch
/// write and read paths. Errors are plain strings; callers map them into
/// their sink-specific error types.
pub(crate) fn build_http_client(
    request_timeout: Duration,
    verify_tls: bool,
    ca_file: Option<&Path>,
) -> Result<reqwest::Client, String> {
    if !verify_tls && ca_file.is_some() {
        return Err("a custom CA cannot be combined with disabled TLS verification".to_owned());
    }
    let mut builder = reqwest::Client::builder()
        .timeout(request_timeout)
        .connect_timeout(Duration::from_secs(10))
        // Recycle idle keep-alive connections before a typical LB /
        // proxy idle timeout (~60s) can kill them out from under us,
        // and keep live ones warm with TCP keepalives. A stale-conn
        // request still self-heals via retry, but this avoids the
        // wasted attempt + backoff sleep.
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .gzip(true)
        .user_agent(concat!("ventstream/", env!("CARGO_PKG_VERSION")));
    if !verify_tls {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(path) = ca_file {
        let pem = std::fs::read(path)
            .map_err(|err| format!("read TLS CA bundle {}: {err}", path.display()))?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|err| format!("parse TLS CA bundle {}: {err}", path.display()))?;
        if certificates.is_empty() {
            return Err(format!(
                "TLS CA bundle {} contains no certificates",
                path.display()
            ));
        }
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }
    builder
        .build()
        .map_err(|err| format!("http client build: {err}"))
}

/// Randomize a backoff delay to 50–100% of the scheduled value so
/// concurrent retriers do not synchronize against the same target.
pub(crate) fn jittered_delay(delay: Duration) -> Duration {
    if delay.is_zero() {
        return delay;
    }
    let factor = rand::thread_rng().gen_range(0.5..=1.0);
    delay.mul_f64(factor)
}
