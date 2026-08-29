//! Error types for the source adapters.

use thiserror::Error;
use ventstream_core::SourceError;

/// Errors emitted by the Neo4j CDC source.
#[derive(Debug, Error)]
pub enum Neo4jCdcError {
    /// Could not establish or maintain the Neo4j Bolt connection.
    #[error("neo4j connection failed: {0}")]
    Connection(String),

    /// A Cypher query failed at runtime — usually a permission, syntax,
    /// or schema issue. The original message is preserved for diagnosis.
    #[error("neo4j query failed: {0}")]
    Query(String),

    /// The cursor file could not be read or written. Stops the source —
    /// proceeding without persistence would lose resume safety.
    #[error("neo4j cursor file io: {0}")]
    CursorIo(String),

    /// A CDC event came back from `db.cdc.query` in a shape we don't
    /// know how to map. Indicates a Neo4j version that introduced a new
    /// event shape, or a corruption we shouldn't paper over.
    #[error("neo4j event payload malformed: {0}")]
    MalformedEvent(String),

    /// Other unexpected error.
    #[error("neo4j cdc internal error: {0}")]
    Internal(String),
}

/// Errors emitted by the Postgres CDC source.
#[derive(Debug, Error)]
pub enum PostgresCdcError {
    /// Could not establish or maintain the Postgres connection.
    #[error("postgres connection failed: {0}")]
    Connection(String),

    /// A SQL statement run during setup (slot creation, identification, etc.)
    /// failed.
    #[error("postgres setup failed running '{statement}': {message}")]
    Setup {
        /// SQL statement that failed.
        statement: String,
        /// Diagnostic message from Postgres.
        message: String,
    },

    /// Decoding the `pgoutput` byte stream failed. Wraps the structured
    /// decoder error for diagnostic context.
    #[error("pgoutput decode failed: {0}")]
    Decode(#[from] crate::postgres::pgoutput::DecodeError),

    /// A `RELATION` message for the given oid was not seen before the row
    /// data referencing it arrived. Indicates a bug in the upstream
    /// publication setup or a missed relation message in the stream.
    #[error("relation {0} referenced before its schema was published")]
    UnknownRelation(u32),

    /// Other unexpected error.
    #[error("postgres cdc internal error: {0}")]
    Internal(String),

    /// The server refused a configuration value it will never accept, so no
    /// retry can clear it. Raised only where the refusal's meaning is
    /// unambiguous at the call site — see
    /// [`classify_slot_refusal`](crate::postgres::connection::classify_slot_refusal).
    /// Maps to [`SourceError::Unrecoverable`], which the supervisor treats as
    /// terminal by type, not by matching the rendered text.
    #[error("postgres configuration refused: {0}")]
    Unrecoverable(String),
}

impl From<PostgresCdcError> for SourceError {
    fn from(err: PostgresCdcError) -> Self {
        match err {
            PostgresCdcError::Connection(msg) => SourceError::Connection(msg),
            PostgresCdcError::Setup { statement, message } => {
                SourceError::Connection(format!("setup failed: {statement}: {message}"))
            }
            PostgresCdcError::Decode(decode_err) => SourceError::Decode(decode_err.to_string()),
            PostgresCdcError::UnknownRelation(oid) => {
                SourceError::Decode(format!("unknown relation oid {oid}"))
            }
            PostgresCdcError::Internal(msg) => SourceError::Internal(msg),
            PostgresCdcError::Unrecoverable(msg) => SourceError::Unrecoverable(msg),
        }
    }
}

/// Errors emitted by the MongoDB CDC source.
#[derive(Debug, Error)]
pub enum MongoCdcError {
    /// Could not connect to the MongoDB deployment (replica set / mongos),
    /// or the connection was lost and could not be re-established.
    #[error("mongodb connection failed: {0}")]
    Connection(String),

    /// A driver operation (watch / find / aggregate) failed at runtime.
    #[error("mongodb operation failed: {0}")]
    Operation(String),

    /// The resume-token cursor file could not be read or written. Stops the
    /// source — proceeding without persistence would lose resume safety.
    #[error("mongodb cursor file io: {0}")]
    CursorIo(String),

    /// A change event came back in a shape we don't know how to map
    /// (missing `_id`, unknown operation type, etc.).
    #[error("mongodb event malformed: {0}")]
    MalformedEvent(String),

    /// Other unexpected error.
    #[error("mongodb cdc internal error: {0}")]
    Internal(String),
}

/// Errors emitted by the MySQL/MariaDB CDC source.
#[derive(Debug, Error)]
pub enum MySqlCdcError {
    /// Could not connect to MySQL, or the connection was lost.
    #[error("mysql connection failed: {0}")]
    Connection(String),

    /// A query (binlog stream, schema lookup, or row re-read) failed.
    #[error("mysql operation failed: {0}")]
    Operation(String),

    /// The binlog-position cursor file could not be read or written.
    #[error("mysql cursor file io: {0}")]
    CursorIo(String),

    /// A binlog event or row was in a shape we couldn't map.
    #[error("mysql event malformed: {0}")]
    MalformedEvent(String),

    /// The resume position is no longer in the server's binlog (purged, or
    /// gone after a failover). Requires explicit sink reconciliation before
    /// the cursor may be reset.
    #[error("mysql binlog position unavailable (purged/failover): {0}")]
    PurgedBinlog(String),

    /// Other unexpected error.
    #[error("mysql cdc internal error: {0}")]
    Internal(String),
}

/// Errors emitted by the Kafka/Redpanda CDC source.
#[derive(Debug, Error)]
pub enum KafkaCdcError {
    /// Could not configure or connect the consumer (bad brokers, auth, TLS).
    #[error("kafka connection failed: {0}")]
    Connection(String),

    /// A consumer operation (subscribe, poll, commit) failed at runtime.
    #[error("kafka operation failed: {0}")]
    Operation(String),

    /// A message couldn't be mapped to an event (bad JSON, missing key,
    /// unknown envelope shape).
    #[error("kafka message malformed: {0}")]
    MalformedEvent(String),

    /// Other unexpected error.
    #[error("kafka cdc internal error: {0}")]
    Internal(String),
}
