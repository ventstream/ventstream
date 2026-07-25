//! Typed `pgoutput` message representations.
//!
//! Variants are `PartialEq + Eq`-able to enable straightforward
//! table-driven decoder tests.

use std::fmt;

/// A Postgres write-ahead-log sequence number.
///
/// 64-bit unsigned integer in the wire format; rendered as `X/Y` in
/// Postgres tooling (upper 32 bits / lower 32 bits in hex). We keep the
/// raw value here and provide a `Display` impl that matches the
/// human-friendly rendering for log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Sentinel value used to mean "no LSN known yet".
    pub const INVALID: Self = Self(0);

    /// Underlying numeric value.
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let upper = (self.0 >> 32) as u32;
        let lower = (self.0 & 0xFFFF_FFFF) as u32;
        write!(f, "{upper:X}/{lower:X}")
    }
}

/// One decoded logical replication message.
///
/// More variants are added as the decoder grows; matchers on this enum
/// MUST be non-exhaustive (we mark it `#[non_exhaustive]`) so adding new
/// message kinds is not a breaking change for downstream callers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LogicalMessage {
    /// Transaction start.
    Begin(Begin),
    /// Transaction end.
    Commit(Commit),
    /// Table schema description; cached so subsequent row messages can
    /// be decoded.
    Relation(Relation),
    /// Row insertion.
    Insert(Insert),
    /// Row update.
    Update(Update),
    /// Row deletion.
    Delete(Delete),
    /// Table truncation.
    Truncate(Truncate),
    /// `Y` Type message — emitted before a Relation message when the
    /// publication includes a column with a user-defined type (e.g. an
    /// enum). We carry the parsed metadata for completeness but the
    /// rest of the pipeline doesn't need it; the source just logs
    /// and continues.
    Type(TypeMessage),
    /// Catch-all for protocol messages we recognise but don't act on
    /// (Origin, Logical Decoding Message, Stream Start/Stop/Commit/Abort).
    /// Parsing skips the body without erroring on trailing bytes.
    Ignored {
        /// The pgoutput tag byte (`O`, `M`, `S`, `E`, `c`, `A`).
        tag: u8,
    },
}

/// `Y` Type message — describes a user-defined type used by a
/// subsequent Relation message. Parsed so the cursor advances
/// correctly; the contents aren't used by the rest of the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeMessage {
    /// OID of the data type.
    pub oid: u32,
    /// Schema namespace (e.g. "public", empty for `pg_catalog`).
    pub namespace: String,
    /// Type name (e.g. "OrderStatusEnum").
    pub name: String,
}

/// `B` BEGIN message — marks the start of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    /// LSN of the commit record for this transaction.
    pub final_lsn: Lsn,
    /// Commit timestamp in microseconds since the Postgres epoch
    /// (2000-01-01 00:00:00 UTC).
    pub commit_time_micros: i64,
    /// Transaction id.
    pub xid: u32,
}

/// `C` COMMIT message — marks the end of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// Reserved flags; presently always zero.
    pub flags: u8,
    /// LSN of the commit record itself.
    pub commit_lsn: Lsn,
    /// LSN of the byte immediately after the commit record.
    pub end_lsn: Lsn,
    /// Commit timestamp in microseconds since the Postgres epoch.
    pub commit_time_micros: i64,
}

/// `R` RELATION message — describes a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    /// Stable relation oid (matches `pg_class.oid`).
    pub id: u32,
    /// Schema name (e.g. `"public"`).
    pub namespace: String,
    /// Relation (table) name.
    pub name: String,
    /// Replica identity setting on the relation. Affects which columns
    /// appear in `UPDATE`/`DELETE` tuple data.
    pub replica_identity: ReplicaIdentity,
    /// Ordered column metadata.
    pub columns: Vec<Column>,
}

/// Postgres replica-identity setting carried in the `RELATION` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// `DEFAULT` — the primary key is replicated for `UPDATE`/`DELETE`.
    Default,
    /// `NOTHING` — no row identity replicated (UPDATE/DELETE unsupported
    /// for downstream apply).
    Nothing,
    /// `FULL` — the entire previous row tuple is replicated.
    Full,
    /// `INDEX` — a named unique index is the row identity.
    Index,
}

impl ReplicaIdentity {
    /// Parse the single byte that pgoutput uses for the replica identity.
    pub(crate) fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'd' => Some(Self::Default),
            b'n' => Some(Self::Nothing),
            b'f' => Some(Self::Full),
            b'i' => Some(Self::Index),
            _ => None,
        }
    }
}

/// Per-column flags packed into a single byte in the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnFlags(pub u8);

impl ColumnFlags {
    /// Bit 0 set means the column participates in the relation's key
    /// (used by replica identity for `UPDATE`/`DELETE`).
    pub const fn is_key_part(self) -> bool {
        self.0 & 0x01 != 0
    }
}

/// Column metadata within a [`Relation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Flags (see [`ColumnFlags`]).
    pub flags: ColumnFlags,
    /// Column name.
    pub name: String,
    /// Postgres type oid (matches `pg_type.oid`).
    pub type_oid: u32,
    /// Type modifier (e.g. varchar length); `-1` when no modifier.
    pub type_modifier: i32,
}

/// `I` INSERT message — a new row in a known relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    /// Relation this row belongs to. Must match a previously seen
    /// `RELATION` message.
    pub relation_id: u32,
    /// The row's column values, in the same order as
    /// [`Relation::columns`].
    pub tuple: Tuple,
}

/// One row's worth of column values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuple {
    /// One [`TupleColumn`] per column on the relation.
    pub columns: Vec<TupleColumn>,
}

/// A single column value within a [`Tuple`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleColumn {
    /// SQL `NULL`.
    Null,
    /// Toasted value that did not change in this row; the actual data is
    /// not replicated. Only appears in `UPDATE` tuples; included here
    /// for completeness.
    UnchangedToast,
    /// Textual representation (Postgres `pg_catalog.text_send` output).
    Text(Vec<u8>),
    /// Binary representation (Postgres 16+ when the publication
    /// declares `(publish_via_partition_root, binary)` etc.).
    Binary(Vec<u8>),
}

/// Encoding kind of an `UPDATE` or `DELETE` message's "old" tuple.
///
/// Postgres only carries old-tuple data when the relation's replica
/// identity makes it possible to identify the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTupleKind {
    /// `K` — old tuple holds the row identity columns only (replica
    /// identity is the primary key or a chosen unique index).
    Key,
    /// `O` — old tuple holds every column (replica identity `FULL`).
    Full,
}

/// `U` UPDATE message — a row was modified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    /// Relation this update applies to.
    pub relation_id: u32,
    /// Old tuple, if the relation's replica identity made it available.
    pub old: Option<OldTuple>,
    /// New tuple — always present.
    pub new: Tuple,
}

/// Old tuple value plus the marker explaining how it was identified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OldTuple {
    /// How the old tuple was determined (`Key` or `Full`).
    pub kind: OldTupleKind,
    /// The columns. For `Key` kind, only key columns will hold values
    /// and the rest will be [`TupleColumn::UnchangedToast`]-equivalents
    /// per the spec.
    pub tuple: Tuple,
}

/// `D` DELETE message — a row was removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    /// Relation the deleted row belonged to.
    pub relation_id: u32,
    /// The old tuple (replica identity makes this mandatory for DELETE
    /// to carry meaningful row identification; if the relation has
    /// replica identity `NOTHING`, Postgres simply does not emit
    /// the DELETE).
    pub old: OldTuple,
}

/// `T` TRUNCATE message — one or more tables were truncated in a single
/// SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    /// Whether `CASCADE` was requested.
    pub cascade: bool,
    /// Whether `RESTART IDENTITY` was requested.
    pub restart_identity: bool,
    /// Relation oids that were truncated.
    pub relation_ids: Vec<u32>,
}
