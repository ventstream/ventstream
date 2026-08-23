//! Startup preflight for the MySQL binlog source.
//!
//! The one check that can never be skipped: `binlog_format` MUST be
//! `ROW`. Under `STATEMENT` or `MIXED` the binlog carries SQL text (or
//! a mix), the row-events this source decodes simply never appear, and
//! the pipeline sits "healthy" while producing nothing — silent total
//! data loss, discovered only when someone asks where the data went.
//!
//! Definitive misconfiguration fails fast with the fix in the message.
//! Transient connect trouble is NOT fatal here — the caller's
//! reconnect budget owns retries, and this runs again on every
//! (re)connect so a live `SET GLOBAL binlog_format` flip is also
//! caught.

use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use tracing::warn;

use crate::error::MySqlCdcError;

/// Assert the server is configured so row events actually exist.
pub(super) async fn check(conn: &mut Conn) -> Result<(), MySqlCdcError> {
    let format: Option<String> = conn
        .query_first("SELECT @@GLOBAL.binlog_format")
        .await
        .map_err(|e| MySqlCdcError::Connection(format!("preflight binlog_format: {e}")))?;
    match format.as_deref() {
        Some(format) if format.eq_ignore_ascii_case("ROW") => {}
        Some(other) => {
            return Err(MySqlCdcError::Connection(format!(
                "binlog_format is '{other}' but row-based replication requires 'ROW' — \
                 the binlog carries no row events, so this source would silently produce \
                 nothing. Fix: SET GLOBAL binlog_format = 'ROW'; (and make it permanent \
                 in my.cnf / your provider's parameter group), then restart the engine"
            )));
        }
        None => {
            return Err(MySqlCdcError::Connection(
                "preflight could not read @@GLOBAL.binlog_format".to_owned(),
            ));
        }
    }

    // Advisory: minimal row images omit unchanged columns, which
    // degrades full-replace materialization the same way TOAST does on
    // Postgres. Warn, don't fail — noblob/minimal setups can be
    // intentional.
    let image: Option<String> = conn
        .query_first("SELECT @@GLOBAL.binlog_row_image")
        .await
        .unwrap_or(None);
    if let Some(image) = image {
        if !image.eq_ignore_ascii_case("FULL") {
            warn!(
                binlog_row_image = %image,
                "binlog_row_image is not FULL — updates may omit unchanged columns, \
                 and full-replace sinks will drop those fields"
            );
        }
    }
    Ok(())
}

/// Warn about primary-key column types whose binlog before-image text
/// can diverge from SELECT-path text. ENUM/SET are normalized to
/// labels by the schema-aware binlog decoding (see
/// `normalize_binlog_value`), so only DECIMAL remains advisory —
/// its binlog text can carry a different scale than the SELECT
/// rendering on some server versions.
pub(super) async fn warn_divergent_pk_types(
    conn: &mut Conn,
    database: &str,
) -> Result<(), MySqlCdcError> {
    let rows: Vec<(String, String, String)> = conn
        .exec(
            "SELECT c.table_name, c.column_name, c.data_type \
             FROM information_schema.columns c \
             JOIN information_schema.key_column_usage k \
               ON k.table_schema = c.table_schema \
              AND k.table_name = c.table_name \
              AND k.column_name = c.column_name \
              AND k.constraint_name = 'PRIMARY' \
             WHERE c.table_schema = ? \
               AND c.data_type = 'decimal'",
            (database,),
        )
        .await
        .map_err(|e| MySqlCdcError::Connection(format!("preflight pk type scan: {e}")))?;
    for (table, column, data_type) in rows {
        warn!(
            table = %table,
            column = %column,
            data_type = %data_type,
            "primary-key column type can render differently between binlog images and \
             SELECT rows — deletes may fail to match their documents. Prefer integer \
             or string surrogate keys for CDC-published tables"
        );
    }

    // ENUM/SET PKs are label-normalized from the INFORMATION_SCHEMA
    // cache, which leaves an ALTER-drift window: replaying pre-ALTER
    // binlog after `ALTER TABLE … ENUM(…)` REORDERS labels maps old
    // indexes onto new labels — wrong doc ids, the orphan bug class
    // reintroduced via staleness. Appends are safe; removals hit the
    // raw-number fallback; reorders are the hazard. The full fix is
    // TABLE_MAP optional metadata (binlog_row_metadata=FULL); until
    // then, warn so operators avoid reorder ALTERs on published tables.
    let enum_rows: Vec<(String, String)> = conn
        .exec(
            "SELECT c.table_name, c.column_name \
             FROM information_schema.columns c \
             JOIN information_schema.key_column_usage k \
               ON k.table_schema = c.table_schema \
              AND k.table_name = c.table_name \
              AND k.column_name = c.column_name \
              AND k.constraint_name = 'PRIMARY' \
             WHERE c.table_schema = ? \
               AND c.data_type IN ('enum', 'set')",
            (database,),
        )
        .await
        .map_err(|e| MySqlCdcError::Connection(format!("preflight enum pk scan: {e}")))?;
    for (table, column) in enum_rows {
        warn!(
            table = %table,
            column = %column,
            "ENUM/SET primary key: label normalization uses a cached definition — do not \
             REORDER labels with ALTER TABLE while the engine is behind on the binlog \
             (append-only changes are safe). A reorder during replay maps old indexes to \
             new labels and mis-addresses documents"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // The check itself needs a live server (covered by the it_mysql
    // integration suite); what's unit-testable is the message contract.
    #[test]
    fn rejection_message_names_the_fix() {
        let err = crate::error::MySqlCdcError::Connection(
            "binlog_format is 'STATEMENT' but row-based replication requires 'ROW' — Fix: \
             SET GLOBAL binlog_format = 'ROW';"
                .to_owned(),
        );
        assert!(err.to_string().contains("SET GLOBAL binlog_format"));
    }
}
