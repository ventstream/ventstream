//! Configuration for the MySQL/MariaDB CDC source.

use std::path::PathBuf;
use std::time::Duration;

use mysql_async::{Opts, OptsBuilder};

/// One MySQL/MariaDB CDC source.
#[derive(Debug, Clone)]
pub struct MySqlCdcConfig {
    /// Stable source id.
    pub id: String,
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Login user (needs `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT`).
    pub user: String,
    /// Login password.
    pub password: String,
    /// Database to watch + namespace under.
    pub database: String,
    /// Logical namespace stamped on subjects/headers (defaults to `database`).
    pub namespace: String,
    /// Unique binlog client id; must not collide with another replica/agent.
    pub server_id: u32,
    /// Tables to watch; empty = every table in `database`.
    pub tables: Vec<String>,
    /// Binlog-position cursor-file directory.
    pub state_dir: PathBuf,
    /// Snapshot-bootstrap on cold start.
    pub bootstrap: bool,
    /// Rows per snapshot `SELECT` batch.
    pub bootstrap_chunk_size: usize,
    /// Batched binlog-position flush cadence.
    pub pos_flush_interval: Duration,
}

impl MySqlCdcConfig {
    /// Config with sensible defaults; callers override fields.
    pub fn new(
        id: impl Into<String>,
        host: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
        state_dir: impl Into<PathBuf>,
    ) -> Self {
        let database = database.into();
        Self {
            id: id.into(),
            host: host.into(),
            port: 3306,
            user: user.into(),
            password: password.into(),
            namespace: database.clone(),
            database,
            server_id: 4_000_000_000,
            tables: Vec::new(),
            state_dir: state_dir.into(),
            bootstrap: true,
            bootstrap_chunk_size: 1000,
            pos_flush_interval: Duration::from_millis(1000),
        }
    }

    pub(crate) fn table_allowed(&self, table: &str) -> bool {
        self.tables.is_empty() || self.tables.iter().any(|t| t == table)
    }

    /// Connection options shared by the binlog source, the join fetcher, and
    /// the SQL-denormalize path.
    pub fn opts(&self) -> Opts {
        Opts::from(
            OptsBuilder::default()
                .ip_or_hostname(self.host.clone())
                .tcp_port(self.port)
                .user(Some(self.user.clone()))
                .pass(Some(self.password.clone()))
                .db_name(Some(self.database.clone())),
        )
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn table_filter() {
        let mut c = MySqlCdcConfig::new("m", "h", "u", "p", "shop", "/tmp/x");
        assert!(c.table_allowed("orders"));
        c.tables = vec!["orders".into()];
        assert!(c.table_allowed("orders"));
        assert!(!c.table_allowed("customers"));
        assert_eq!(c.namespace, "shop");
    }
}
