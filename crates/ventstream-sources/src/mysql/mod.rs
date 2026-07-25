//! MySQL/MariaDB CDC source.
//!
//! Reads the row-based binlog as a *trigger* (table + primary key + op), then
//! re-`SELECT`s the affected row by PK to build a clean document — the same
//! trigger-then-requery model the Neo4j and Mongo sources use. Avoids decoding
//! binlog value/JSONB internals and keeps types correct. v1 is raw 1:1 (one
//! row -> one sink doc). Requires `binlog_format=ROW` and a unique `server_id`.

mod config;
mod cursor;
mod event_mapper;
mod fetcher;
mod schema;
mod source;
mod value;

pub use config::MySqlCdcConfig;
pub use cursor::checkpoint_is_resumable as mysql_checkpoint_is_resumable;
pub use fetcher::MySqlFetcher;
pub use source::MySqlCdcSource;
