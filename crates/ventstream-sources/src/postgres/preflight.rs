//! Startup diagnostics for the Postgres CDC source.
//!
//! Managed Postgres providers add failure modes that surface as opaque
//! protocol errors deep into startup: connection poolers that don't
//! speak the replication protocol, `wal_level` below `logical`, and
//! publications that were never created. These checks run once, before
//! the replication connect, and turn each into a precise error with
//! the fix in the message.
//!
//! Definitive misconfigurations (pooler endpoint, wrong `wal_level`,
//! missing publication) are fatal. A transient connect failure here is
//! NOT — the replication path has its own retry budget, and preflight
//! must not make startup more fragile than it was without it.

use tracing::{info, warn};

use super::config::PostgresCdcConfig;
use super::connection::connect_client;
use crate::error::PostgresCdcError;

/// Reject connection-pooler endpoints before any connect attempt.
///
/// Poolers (pgbouncer, Supavisor) don't support the replication
/// protocol; the resulting server errors are cryptic. Matches the
/// Supabase pooler hostname and its transaction-mode port.
pub fn check_endpoint(config: &PostgresCdcConfig) -> Result<(), PostgresCdcError> {
    let host = config.host.to_ascii_lowercase();
    let pooler_host = host.ends_with(".pooler.supabase.com");
    if pooler_host || config.port == 6543 {
        return Err(PostgresCdcError::Connection(format!(
            "{}:{} is a connection-pooler endpoint; logical replication needs the direct \
             connection (on Supabase: db.<project-ref>.supabase.co:5432, not the pooler)",
            config.host, config.port
        )));
    }
    Ok(())
}

/// Server-side checks over a regular (non-replication) connection.
pub async fn run(config: &PostgresCdcConfig) -> Result<(), PostgresCdcError> {
    let client = match connect_client(config, "preflight").await {
        Ok(client) => client,
        Err(err) => {
            // Best-effort: let the replication connect's retry budget
            // own transient failures.
            warn!(error = %err, "postgres preflight skipped — could not connect");
            return Ok(());
        }
    };

    let wal_level: String = match client.query_one("SHOW wal_level", &[]).await {
        Ok(row) => row.get(0),
        Err(err) => {
            warn!(error = %err, "postgres preflight skipped — SHOW wal_level failed");
            return Ok(());
        }
    };
    if wal_level != "logical" {
        return Err(PostgresCdcError::Connection(format!(
            "wal_level is '{wal_level}' but logical replication requires 'logical'; \
             set wal_level=logical and restart the server"
        )));
    }

    match client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&config.publication],
        )
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err(PostgresCdcError::Connection(format!(
                "publication \"{}\" does not exist; create it first: \
                 CREATE PUBLICATION {} FOR TABLE <your tables>;",
                config.publication, config.publication
            )));
        }
        Err(err) => {
            warn!(error = %err, "postgres preflight skipped — publication check failed");
            return Ok(());
        }
    }

    // Partitioned tables: leaf partitions decode as separate relations
    // unless the publication routes changes through the root
    // (`publish_via_partition_root = true`). Without it, join specs
    // written against the parent never match, and a cross-partition
    // row move emits delete+insert under two different relation names
    // — stranding the old doc. Definitive misconfiguration: fail with
    // the fix.
    match client
        .query_opt(
            "SELECT EXISTS ( \
                 SELECT 1 FROM pg_publication_tables pt \
                 JOIN pg_class c ON c.relname = pt.tablename \
                 JOIN pg_namespace n \
                   ON n.oid = c.relnamespace AND n.nspname = pt.schemaname \
                 WHERE pt.pubname = $1 AND c.relispartition \
             ) AND NOT ( \
                 SELECT pubviaroot FROM pg_publication WHERE pubname = $1 \
             )",
            &[&config.publication],
        )
        .await
    {
        Ok(Some(row)) if row.get::<_, bool>(0) => {
            return Err(PostgresCdcError::Connection(format!(
                "publication \"{pub_name}\" includes partitioned tables but \
                 publish_via_partition_root is off — leaf partitions would arrive as \
                 separate relations and cross-partition row moves would strand stale \
                 documents. Fix: ALTER PUBLICATION {pub_name} SET \
                 (publish_via_partition_root = true);",
                pub_name = config.publication
            )));
        }
        Ok(_) => {}
        Err(err) => warn!(error = %err, "postgres preflight — partition check failed"),
    }

    // REPLICATION is a role attribute, not inherited via membership, so
    // current_user's own flags are what the walsender will check. Some
    // setups still pass differently — warn, don't fail.
    match client
        .query_opt(
            "SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user",
            &[],
        )
        .await
    {
        Ok(Some(row)) if !row.get::<_, bool>(0) => {
            warn!(
                user = %config.user,
                "connecting role lacks the REPLICATION attribute; replication connect will \
                 likely be refused (ALTER ROLE {} REPLICATION;)",
                config.user
            );
        }
        Ok(_) => {}
        Err(err) => warn!(error = %err, "postgres preflight — privilege check failed"),
    }

    info!(
        publication = %config.publication,
        "postgres preflight passed (wal_level=logical, publication present)"
    );
    Ok(())
}

/// Hint appended to connect errors when the target resolves to IPv6
/// only. Supabase direct hostnames are IPv6-only unless the project
/// buys the IPv4 add-on; on an IPv4-only network the failure renders
/// differently per OS ("Network is unreachable" on Linux, a bare
/// "error connecting to server" on macOS), so match connect failures
/// broadly and let the AAAA-only resolution be the discriminator.
pub async fn unreachable_hint(host: &str, port: u16, error_text: &str) -> Option<String> {
    let lowered = error_text.to_ascii_lowercase();
    let connectish = [
        "unreachable",
        "no route",
        "error connecting",
        "timed out",
        "timeout",
    ]
    .iter()
    .any(|needle| lowered.contains(needle));
    if !connectish {
        return None;
    }
    match tokio::net::lookup_host((host, port)).await {
        Ok(addrs) => {
            let addrs: Vec<_> = addrs.collect();
            if !addrs.is_empty() && addrs.iter().all(|addr| addr.is_ipv6()) {
                return Some(format!(
                    "{host} resolves only to IPv6 and this network may not route it; \
                     enable your provider's IPv4 add-on or run from an IPv6-capable network"
                ));
            }
        }
        // macOS suppresses AAAA answers entirely on hosts without an
        // IPv6 route, so a v6-only hostname looks like a DNS failure.
        // Only Supabase direct hosts are known v6-only — anything else
        // failing resolution is more likely a typo.
        Err(_) if host.ends_with(".supabase.co") => {
            return Some(format!(
                "{host} did not resolve on this machine; Supabase direct hostnames are \
                 IPv6-only, and hosts without an IPv6 route may return no address at all — \
                 enable the project's IPv4 add-on or run from an IPv6-capable network"
            ));
        }
        Err(_) => {}
    }
    None
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn config(host: &str, port: u16) -> PostgresCdcConfig {
        let mut config = PostgresCdcConfig::new("pg", host, "u", "p", "db", "pub", "slot");
        config.port = port;
        config
    }

    #[test]
    fn pooler_hostname_is_rejected() {
        let err = check_endpoint(&config("aws-0-eu-west-1.pooler.supabase.com", 5432))
            .expect_err("pooler host must be rejected");
        assert!(err.to_string().contains("direct connection"));
    }

    #[test]
    fn pooler_port_is_rejected() {
        assert!(check_endpoint(&config("db.abc.supabase.co", 6543)).is_err());
    }

    #[test]
    fn direct_endpoint_passes() {
        assert!(check_endpoint(&config("db.abc.supabase.co", 5432)).is_ok());
        assert!(check_endpoint(&config("127.0.0.1", 5432)).is_ok());
    }
}
