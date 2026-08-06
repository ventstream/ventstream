//! Configuration for a Postgres CDC source.
//!
//! Phase 0 keeps this as a plain struct constructed by the binary.
//! Phase 1 adds `pipeline.yaml` parsing in `ventstream-config` that
//! produces an equivalent value.

use std::path::PathBuf;
use std::time::Duration;

use crate::tls::DatabaseTlsConfig;

/// Configuration for a [`super::PostgresCdcSource`].
#[derive(Debug, Clone)]
pub struct PostgresCdcConfig {
    /// Stable identifier matching the pipeline's source `id`.
    pub id: String,

    /// Postgres server hostname or IP.
    pub host: String,

    /// Postgres server port (defaults to 5432).
    pub port: u16,

    /// Login user. Must have the `REPLICATION` attribute or be
    /// a superuser.
    pub user: String,

    /// Login password (cleartext — supply via env var, never commit).
    pub password: String,

    /// Database name.
    pub database: String,

    /// Existing logical replication slot name. Operators must create
    /// this in advance:
    /// `SELECT pg_create_logical_replication_slot('ventstream_pipeline_x', 'pgoutput');`
    pub slot_name: String,

    /// Existing publication name. Operators must create this in advance:
    /// `CREATE PUBLICATION ventstream_pub FOR TABLE users, orders;`
    pub publication: String,

    /// Interval between standby status updates sent back to the server.
    /// Must be well under Postgres' `wal_sender_timeout` (default 60s)
    /// to avoid the server closing the connection.
    pub status_interval: Duration,

    /// Optional snapshot bootstrap configuration.
    ///
    /// When `Some` AND the replication slot does not yet exist, the
    /// source runs a one-time SELECT over every table listed in
    /// [`SnapshotBootstrap::tables`] before opening the slot, emitting
    /// each row as a synthetic INSERT event through the bus. Lets the
    /// engine populate join state from existing data, not just from
    /// future WAL changes.
    ///
    /// When `None`, the slot must already exist or the operator
    /// accepts that the engine starts with empty state.
    pub bootstrap: Option<SnapshotBootstrap>,

    /// Optional TLS policy. `None` preserves the legacy plaintext default.
    pub tls: Option<DatabaseTlsConfig>,

    /// Writable directory for transactions that exceed the in-memory WAL buffer.
    pub transaction_spool_dir: Option<PathBuf>,
}

/// Tables to scan in a one-time snapshot bootstrap.
#[derive(Debug, Clone)]
pub struct SnapshotBootstrap {
    /// Tables to snapshot, in the order they should be emitted.
    /// Emit dependency leaves first when possible — sync-on-miss
    /// covers out-of-order references but at the cost of round-trips.
    pub tables: Vec<SnapshotTable>,

    /// Rows per chunked SELECT. Defaults to 10_000.
    pub chunk_size: usize,
}

/// One table targeted by the snapshot bootstrap.
#[derive(Debug, Clone)]
pub struct SnapshotTable {
    /// Schema qualifier (e.g. `public`).
    pub namespace: String,
    /// Table name (e.g. `shows`).
    pub name: String,
    /// Primary-key column(s). Used both for chunked pagination
    /// (`ORDER BY pk1, pk2, … LIMIT N`) and to ensure stable ordering
    /// across resumed scans.
    pub primary_key: Vec<String>,
}

/// Quote a value for a libpq keyword/value connection string.
///
/// libpq treats unquoted whitespace as a separator between `keyword=value`
/// pairs, so a value containing a space, an empty value, or a value like
/// `x host=evil` would break parsing or inject extra parameters. Per libpq
/// rules we single-quote every value and backslash-escape `\` and `'`.
fn libpq_quote(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

impl PostgresCdcConfig {
    /// Build the libpq keyword/value connection string with every value
    /// properly quoted+escaped (see `libpq_quote`). This is the single
    /// builder for every PG connection the engine opens (replication,
    /// snapshot, fetcher, SQL-denormalize), so a host/user/password with a
    /// space or quote can't break the connection or inject parameters (M9).
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            libpq_quote(&self.host),
            self.port,
            libpq_quote(&self.user),
            libpq_quote(&self.password),
            libpq_quote(&self.database),
        )
    }

    /// Construct a config with sane defaults for optional fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        host: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
        publication: impl Into<String>,
        slot_name: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            host: host.into(),
            port: 5432,
            user: user.into(),
            password: password.into(),
            database: database.into(),
            publication: publication.into(),
            slot_name: slot_name.into(),
            status_interval: Duration::from_secs(10),
            bootstrap: None,
            tls: None,
            transaction_spool_dir: None,
        }
    }

    /// Attach a snapshot-bootstrap configuration. The bootstrap runs
    /// once, only if the slot doesn't already exist.
    #[must_use]
    pub fn with_bootstrap(mut self, bootstrap: SnapshotBootstrap) -> Self {
        self.bootstrap = Some(bootstrap);
        self
    }

    /// Override the Postgres port (defaults to 5432).
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Override the standby status interval (defaults to 10 seconds).
    #[must_use]
    pub fn with_status_interval(mut self, interval: Duration) -> Self {
        self.status_interval = interval;
        self
    }

    /// Apply one TLS policy to replication and every auxiliary connection.
    #[must_use]
    pub fn with_tls(mut self, tls: DatabaseTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn cfg() -> PostgresCdcConfig {
        PostgresCdcConfig::new("id", "localhost", "u", "p", "db", "pub", "slot")
    }

    #[test]
    fn libpq_quote_wraps_and_escapes() {
        assert_eq!(libpq_quote("plain"), "'plain'");
        assert_eq!(libpq_quote("has space"), "'has space'");
        assert_eq!(libpq_quote(""), "''");
        assert_eq!(libpq_quote("o'brien"), r"'o\'brien'");
        assert_eq!(libpq_quote(r"back\slash"), r"'back\\slash'");
    }

    #[test]
    fn connection_string_quotes_every_value() {
        let c = cfg();
        let s = c.connection_string();
        assert_eq!(
            s,
            "host='localhost' port=5432 user='u' password='p' dbname='db'"
        );
    }

    #[test]
    fn connection_string_neutralizes_param_injection() {
        // A host value that tries to inject another keyword must end up as one
        // quoted value, not two parameters.
        let mut c = cfg();
        c.host = "x host=evil".to_owned();
        c.password = "p'; dbname=other".to_owned();
        let s = c.connection_string();
        assert!(
            s.contains("host='x host=evil'"),
            "injected host must be a single quoted value: {s}"
        );
        // The password's quote is escaped, so its `dbname=other` stays inside
        // the quoted value rather than overriding the real dbname.
        assert!(s.contains(r"password='p\'; dbname=other'"), "{s}");
        assert!(s.ends_with("dbname='db'"), "real dbname must win: {s}");
    }
}
