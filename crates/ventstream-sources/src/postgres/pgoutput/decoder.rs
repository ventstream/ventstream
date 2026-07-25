//! Byte-level decoder for the `pgoutput` plugin format.
//!
//! Format invariants:
//! - All multi-byte integers are big-endian.
//! - Strings are null-terminated UTF-8 (C strings).
//! - The first byte of each message is the message-type tag.
//!
//! This module is intentionally I/O-free. Feed it a `&[u8]` representing
//! one complete `CopyData` payload and it returns a typed message.

use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};
use thiserror::Error;

use super::messages::{
    Begin, Column, ColumnFlags, Commit, Delete, Insert, LogicalMessage, Lsn, OldTuple,
    OldTupleKind, Relation, ReplicaIdentity, Truncate, Tuple, TupleColumn, TypeMessage, Update,
};

/// Failure modes for [`decode`].
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Message buffer was empty — no type tag.
    #[error("empty message buffer")]
    Empty,

    /// Tag byte does not match any known message kind.
    #[error("unknown message tag {tag:#x} ('{}')", char::from(*tag))]
    UnknownTag {
        /// The unrecognized tag byte.
        tag: u8,
    },

    /// Buffer ended before the expected fields were fully read.
    #[error("truncated message: needed {needed} more byte(s) for field '{field}'")]
    Truncated {
        /// Name of the field that ran short.
        field: &'static str,
        /// How many more bytes were needed.
        needed: usize,
    },

    /// A field's value violated a format invariant (e.g. invalid replica
    /// identity byte, non-UTF-8 in a C-string).
    #[error("invalid value for field '{field}': {detail}")]
    InvalidValue {
        /// Name of the field that failed validation.
        field: &'static str,
        /// Human-readable detail.
        detail: String,
    },

    /// Trailing bytes after the message was fully consumed — indicates
    /// an upstream framing bug.
    #[error("{trailing} trailing byte(s) after fully parsing message tag '{}'", char::from(*tag))]
    TrailingBytes {
        /// Tag of the message that was being parsed.
        tag: u8,
        /// How many trailing bytes remained.
        trailing: usize,
    },
}

/// Decode one `pgoutput` `CopyData` payload into a typed message.
pub fn decode(bytes: &[u8]) -> Result<LogicalMessage, DecodeError> {
    let (tag, body) = bytes.split_first().ok_or(DecodeError::Empty)?;
    let mut cursor = Cursor::new(body);

    let message = match *tag {
        b'B' => LogicalMessage::Begin(decode_begin(&mut cursor)?),
        b'C' => LogicalMessage::Commit(decode_commit(&mut cursor)?),
        b'R' => LogicalMessage::Relation(decode_relation(&mut cursor)?),
        b'I' => LogicalMessage::Insert(decode_insert(&mut cursor)?),
        b'U' => LogicalMessage::Update(decode_update(&mut cursor)?),
        b'D' => LogicalMessage::Delete(decode_delete(&mut cursor)?),
        b'T' => LogicalMessage::Truncate(decode_truncate(&mut cursor)?),
        b'Y' => LogicalMessage::Type(decode_type(&mut cursor)?),
        // Informational tags we recognise but don't act on. We jump
        // the cursor to end-of-body so the trailing-bytes check
        // passes and surface them as a single `Ignored` variant.
        // Without this, the engine panics on any publication using
        // user-defined types (the Y case) or stream-mode features.
        //   O = Origin                  (replication origin)
        //   M = Logical Decoding Msg    (pg_logical_emit_message)
        //   S = Stream Start
        //   E = Stream Stop
        //   c = Stream Commit
        //   A = Stream Abort
        b'O' | b'M' | b'S' | b'E' | b'c' | b'A' => {
            cursor.set_position(body.len() as u64);
            LogicalMessage::Ignored { tag: *tag }
        }
        other => return Err(DecodeError::UnknownTag { tag: other }),
    };

    let consumed = cursor.position() as usize;
    let trailing = body.len().saturating_sub(consumed);
    if trailing != 0 {
        return Err(DecodeError::TrailingBytes {
            tag: *tag,
            trailing,
        });
    }

    Ok(message)
}

fn decode_begin(cursor: &mut Cursor<&[u8]>) -> Result<Begin, DecodeError> {
    let final_lsn = read_lsn(cursor, "final_lsn")?;
    let commit_time_micros = read_i64(cursor, "commit_time_micros")?;
    let xid = read_u32(cursor, "xid")?;
    Ok(Begin {
        final_lsn,
        commit_time_micros,
        xid,
    })
}

fn decode_commit(cursor: &mut Cursor<&[u8]>) -> Result<Commit, DecodeError> {
    let flags = read_u8(cursor, "flags")?;
    let commit_lsn = read_lsn(cursor, "commit_lsn")?;
    let end_lsn = read_lsn(cursor, "end_lsn")?;
    let commit_time_micros = read_i64(cursor, "commit_time_micros")?;
    Ok(Commit {
        flags,
        commit_lsn,
        end_lsn,
        commit_time_micros,
    })
}

fn decode_relation(cursor: &mut Cursor<&[u8]>) -> Result<Relation, DecodeError> {
    let id = read_u32(cursor, "id")?;
    let namespace = read_cstr(cursor, "namespace")?;
    let name = read_cstr(cursor, "name")?;
    let replica_identity_byte = read_u8(cursor, "replica_identity")?;
    let replica_identity = ReplicaIdentity::from_byte(replica_identity_byte).ok_or_else(|| {
        DecodeError::InvalidValue {
            field: "replica_identity",
            detail: format!(
                "unknown byte {:#x} ('{}')",
                replica_identity_byte,
                char::from(replica_identity_byte)
            ),
        }
    })?;

    let column_count = read_i16(cursor, "column_count")?;
    if column_count < 0 {
        return Err(DecodeError::InvalidValue {
            field: "column_count",
            detail: format!("negative ({column_count})"),
        });
    }
    let column_count = column_count as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let flags = ColumnFlags(read_u8(cursor, "column.flags")?);
        let name = read_cstr(cursor, "column.name")?;
        let type_oid = read_u32(cursor, "column.type_oid")?;
        let type_modifier = read_i32(cursor, "column.type_modifier")?;
        columns.push(Column {
            flags,
            name,
            type_oid,
            type_modifier,
        });
    }

    Ok(Relation {
        id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_insert(cursor: &mut Cursor<&[u8]>) -> Result<Insert, DecodeError> {
    let relation_id = read_u32(cursor, "relation_id")?;
    let new_tuple_marker = read_u8(cursor, "new_tuple_marker")?;
    if new_tuple_marker != b'N' {
        return Err(DecodeError::InvalidValue {
            field: "new_tuple_marker",
            detail: format!(
                "expected 'N', got {:#x} ('{}')",
                new_tuple_marker,
                char::from(new_tuple_marker)
            ),
        });
    }
    let tuple = decode_tuple(cursor)?;
    Ok(Insert { relation_id, tuple })
}

fn decode_update(cursor: &mut Cursor<&[u8]>) -> Result<Update, DecodeError> {
    let relation_id = read_u32(cursor, "relation_id")?;
    // The wire format is one of:
    //   <relation_id> 'N' <new_tuple>                       — no old tuple
    //   <relation_id> 'K' <key_tuple> 'N' <new_tuple>       — old via index
    //   <relation_id> 'O' <full_tuple> 'N' <new_tuple>      — old via FULL
    let marker = read_u8(cursor, "update.marker")?;
    let (old, new_marker) = match marker {
        b'K' => {
            let tuple = decode_tuple(cursor)?;
            let next = read_u8(cursor, "update.new_marker")?;
            (
                Some(OldTuple {
                    kind: OldTupleKind::Key,
                    tuple,
                }),
                next,
            )
        }
        b'O' => {
            let tuple = decode_tuple(cursor)?;
            let next = read_u8(cursor, "update.new_marker")?;
            (
                Some(OldTuple {
                    kind: OldTupleKind::Full,
                    tuple,
                }),
                next,
            )
        }
        b'N' => (None, b'N'),
        other => {
            return Err(DecodeError::InvalidValue {
                field: "update.marker",
                detail: format!(
                    "expected 'K', 'O', or 'N', got {:#x} ('{}')",
                    other,
                    char::from(other)
                ),
            });
        }
    };
    if new_marker != b'N' {
        return Err(DecodeError::InvalidValue {
            field: "update.new_marker",
            detail: format!(
                "expected 'N', got {:#x} ('{}')",
                new_marker,
                char::from(new_marker)
            ),
        });
    }
    let new = decode_tuple(cursor)?;
    Ok(Update {
        relation_id,
        old,
        new,
    })
}

fn decode_delete(cursor: &mut Cursor<&[u8]>) -> Result<Delete, DecodeError> {
    let relation_id = read_u32(cursor, "relation_id")?;
    let marker = read_u8(cursor, "delete.marker")?;
    let kind = match marker {
        b'K' => OldTupleKind::Key,
        b'O' => OldTupleKind::Full,
        other => {
            return Err(DecodeError::InvalidValue {
                field: "delete.marker",
                detail: format!(
                    "expected 'K' or 'O', got {:#x} ('{}')",
                    other,
                    char::from(other)
                ),
            });
        }
    };
    let tuple = decode_tuple(cursor)?;
    Ok(Delete {
        relation_id,
        old: OldTuple { kind, tuple },
    })
}

fn decode_truncate(cursor: &mut Cursor<&[u8]>) -> Result<Truncate, DecodeError> {
    let relation_count = read_u32(cursor, "truncate.relation_count")?;
    let option_flags = read_u8(cursor, "truncate.option_flags")?;
    let cascade = option_flags & 0x01 != 0;
    let restart_identity = option_flags & 0x02 != 0;
    let count = relation_count as usize;
    let mut relation_ids = Vec::with_capacity(count);
    for _ in 0..count {
        relation_ids.push(read_u32(cursor, "truncate.relation_id")?);
    }
    Ok(Truncate {
        cascade,
        restart_identity,
        relation_ids,
    })
}

/// `Y` Type message — Int32 oid + cstring namespace + cstring name.
/// Postgres emits one of these before a `R` Relation when the relation
/// has columns of a user-defined type (eg. an enum). We parse so the
/// cursor advances cleanly; the contents aren't used downstream.
fn decode_type(cursor: &mut Cursor<&[u8]>) -> Result<TypeMessage, DecodeError> {
    let oid = read_u32(cursor, "type.oid")?;
    let namespace = read_cstr(cursor, "type.namespace")?;
    let name = read_cstr(cursor, "type.name")?;
    Ok(TypeMessage {
        oid,
        namespace,
        name,
    })
}

fn decode_tuple(cursor: &mut Cursor<&[u8]>) -> Result<Tuple, DecodeError> {
    let column_count = read_i16(cursor, "tuple.column_count")?;
    if column_count < 0 {
        return Err(DecodeError::InvalidValue {
            field: "tuple.column_count",
            detail: format!("negative ({column_count})"),
        });
    }
    let column_count = column_count as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let kind = read_u8(cursor, "tuple.column.kind")?;
        let column = match kind {
            b'n' => TupleColumn::Null,
            b'u' => TupleColumn::UnchangedToast,
            b't' => {
                let len = read_i32(cursor, "tuple.column.text.length")?;
                let bytes = read_bytes(cursor, "tuple.column.text.bytes", len)?;
                TupleColumn::Text(bytes)
            }
            b'b' => {
                let len = read_i32(cursor, "tuple.column.binary.length")?;
                let bytes = read_bytes(cursor, "tuple.column.binary.bytes", len)?;
                TupleColumn::Binary(bytes)
            }
            other => {
                return Err(DecodeError::InvalidValue {
                    field: "tuple.column.kind",
                    detail: format!("unknown kind {:#x} ('{}')", other, char::from(other)),
                });
            }
        };
        columns.push(column);
    }
    Ok(Tuple { columns })
}

// -- primitive readers ------------------------------------------------------

fn read_u8(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<u8, DecodeError> {
    cursor
        .read_u8()
        .map_err(|_| DecodeError::Truncated { field, needed: 1 })
}

fn read_i16(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<i16, DecodeError> {
    cursor
        .read_i16::<BigEndian>()
        .map_err(|_| DecodeError::Truncated { field, needed: 2 })
}

fn read_u32(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<u32, DecodeError> {
    cursor
        .read_u32::<BigEndian>()
        .map_err(|_| DecodeError::Truncated { field, needed: 4 })
}

fn read_i32(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<i32, DecodeError> {
    cursor
        .read_i32::<BigEndian>()
        .map_err(|_| DecodeError::Truncated { field, needed: 4 })
}

fn read_i64(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<i64, DecodeError> {
    cursor
        .read_i64::<BigEndian>()
        .map_err(|_| DecodeError::Truncated { field, needed: 8 })
}

fn read_lsn(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<Lsn, DecodeError> {
    cursor
        .read_u64::<BigEndian>()
        .map(Lsn)
        .map_err(|_| DecodeError::Truncated { field, needed: 8 })
}

fn read_cstr(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<String, DecodeError> {
    // Position-of-null search without exposing the inner slice machinery.
    let start = cursor.position() as usize;
    let inner = *cursor.get_ref();
    let tail = inner
        .get(start..)
        .ok_or(DecodeError::Truncated { field, needed: 1 })?;
    let null_offset = tail
        .iter()
        .position(|&b| b == 0)
        .ok_or(DecodeError::Truncated { field, needed: 1 })?;
    let raw = tail
        .get(..null_offset)
        .ok_or(DecodeError::Truncated { field, needed: 1 })?;
    let s = std::str::from_utf8(raw)
        .map_err(|err| DecodeError::InvalidValue {
            field,
            detail: format!("invalid UTF-8: {err}"),
        })?
        .to_owned();
    // Advance past the string and its null terminator.
    cursor.set_position((start + null_offset + 1) as u64);
    Ok(s)
}

fn read_bytes(
    cursor: &mut Cursor<&[u8]>,
    field: &'static str,
    len: i32,
) -> Result<Vec<u8>, DecodeError> {
    if len < 0 {
        return Err(DecodeError::InvalidValue {
            field,
            detail: format!("negative length ({len})"),
        });
    }
    let len = len as usize;
    let start = cursor.position() as usize;
    let inner = *cursor.get_ref();
    let end = start.checked_add(len).ok_or(DecodeError::InvalidValue {
        field,
        detail: "length overflow".into(),
    })?;
    let slice = inner.get(start..end).ok_or(DecodeError::Truncated {
        field,
        needed: end - inner.len().min(end),
    })?;
    let out = slice.to_vec();
    cursor.set_position(end as u64);
    Ok(out)
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    // ---- helpers ----------------------------------------------------------

    /// Build a pgoutput byte sequence from a tag + body description.
    /// Keeps tests readable instead of giant `&[0x..]` literals.
    struct Builder {
        bytes: Vec<u8>,
    }

    impl Builder {
        fn new(tag: u8) -> Self {
            Self { bytes: vec![tag] }
        }

        fn u8(mut self, v: u8) -> Self {
            self.bytes.push(v);
            self
        }

        fn i16(mut self, v: i16) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn u32(mut self, v: u32) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn i32(mut self, v: i32) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn i64(mut self, v: i64) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn u64(mut self, v: u64) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn cstr(mut self, s: &str) -> Self {
            self.bytes.extend_from_slice(s.as_bytes());
            self.bytes.push(0);
            self
        }

        fn raw(mut self, bs: &[u8]) -> Self {
            self.bytes.extend_from_slice(bs);
            self
        }

        fn build(self) -> Vec<u8> {
            self.bytes
        }
    }

    // ---- happy path -------------------------------------------------------

    #[test]
    fn decodes_begin() {
        // tag('B') + final_lsn(u64=0x100) + commit_time(i64=12345) + xid(u32=42)
        let bytes = Builder::new(b'B').u64(0x100).i64(12345).u32(42).build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Begin(b) => {
                assert_eq!(b.final_lsn, Lsn(0x100));
                assert_eq!(b.commit_time_micros, 12_345);
                assert_eq!(b.xid, 42);
            }
            other => panic!("expected Begin, got {other:?}"),
        }
    }

    #[test]
    fn decodes_commit() {
        // tag('C') + flags(u8=0) + commit_lsn + end_lsn + commit_time
        let bytes = Builder::new(b'C')
            .u8(0)
            .u64(0x200)
            .u64(0x208)
            .i64(99_999)
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Commit(c) => {
                assert_eq!(c.flags, 0);
                assert_eq!(c.commit_lsn, Lsn(0x200));
                assert_eq!(c.end_lsn, Lsn(0x208));
                assert_eq!(c.commit_time_micros, 99_999);
            }
            other => panic!("expected Commit, got {other:?}"),
        }
    }

    #[test]
    fn decodes_relation_with_two_columns() {
        // tag('R') + id + namespace + name + replica_identity('d') + col_count + cols...
        let bytes = Builder::new(b'R')
            .u32(16_384)
            .cstr("public")
            .cstr("users")
            .u8(b'd')
            .i16(2)
            // Column 1: key, name="id", oid=23(int4), modifier=-1
            .u8(0x01)
            .cstr("id")
            .u32(23)
            .i32(-1)
            // Column 2: non-key, name="email", oid=25(text), modifier=-1
            .u8(0x00)
            .cstr("email")
            .u32(25)
            .i32(-1)
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Relation(r) => {
                assert_eq!(r.id, 16_384);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.replica_identity, ReplicaIdentity::Default);
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.columns[0].name, "id");
                assert!(r.columns[0].flags.is_key_part());
                assert_eq!(r.columns[0].type_oid, 23);
                assert_eq!(r.columns[1].name, "email");
                assert!(!r.columns[1].flags.is_key_part());
                assert_eq!(r.columns[1].type_oid, 25);
            }
            other => panic!("expected Relation, got {other:?}"),
        }
    }

    #[test]
    fn decodes_insert_with_text_and_null_columns() {
        // tag('I') + relation_id + 'N' + col_count + cols...
        let bytes = Builder::new(b'I')
            .u32(16_384)
            .u8(b'N')
            .i16(2)
            // Column 1: text, value="42"
            .u8(b't')
            .i32(2)
            .raw(b"42")
            // Column 2: null
            .u8(b'n')
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Insert(i) => {
                assert_eq!(i.relation_id, 16_384);
                assert_eq!(i.tuple.columns.len(), 2);
                assert_eq!(i.tuple.columns[0], TupleColumn::Text(b"42".to_vec()));
                assert_eq!(i.tuple.columns[1], TupleColumn::Null);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn decodes_insert_with_binary_column() {
        let bytes = Builder::new(b'I')
            .u32(1)
            .u8(b'N')
            .i16(1)
            .u8(b'b')
            .i32(3)
            .raw(&[0xDE, 0xAD, 0xBE])
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Insert(i) => {
                assert_eq!(
                    i.tuple.columns[0],
                    TupleColumn::Binary(vec![0xDE, 0xAD, 0xBE])
                );
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    // ---- error paths ------------------------------------------------------

    #[test]
    fn empty_buffer_returns_error() {
        let err = decode(&[]).expect_err("empty");
        assert!(matches!(err, DecodeError::Empty));
    }

    #[test]
    fn unknown_tag_returns_error() {
        let err = decode(b"Z").expect_err("unknown tag");
        match err {
            DecodeError::UnknownTag { tag } => assert_eq!(tag, b'Z'),
            other => panic!("expected UnknownTag, got {other:?}"),
        }
    }

    #[test]
    fn truncated_begin_returns_truncated() {
        // Tag + only 4 bytes (LSN needs 8)
        let bytes = vec![b'B', 0, 0, 0, 1];
        let err = decode(&bytes).expect_err("truncated");
        assert!(matches!(err, DecodeError::Truncated { .. }));
    }

    #[test]
    fn relation_with_invalid_replica_identity_byte_errors() {
        let bytes = Builder::new(b'R')
            .u32(1)
            .cstr("public")
            .cstr("t")
            .u8(b'?') // invalid replica identity
            .i16(0)
            .build();
        let err = decode(&bytes).expect_err("invalid replica");
        match err {
            DecodeError::InvalidValue { field, .. } => assert_eq!(field, "replica_identity"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn insert_without_n_marker_errors() {
        let bytes = Builder::new(b'I')
            .u32(1)
            .u8(b'K') // wrong marker
            .i16(0)
            .build();
        let err = decode(&bytes).expect_err("bad marker");
        match err {
            DecodeError::InvalidValue { field, .. } => assert_eq!(field, "new_tuple_marker"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn cstr_with_invalid_utf8_errors() {
        let mut bytes = Builder::new(b'R').u32(1).build();
        bytes.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        bytes.push(0); // null terminator
        let err = decode(&bytes).expect_err("bad utf8");
        match err {
            DecodeError::InvalidValue { field, .. } => assert_eq!(field, "namespace"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = Builder::new(b'B').u64(1).i64(1).u32(1).build();
        bytes.push(0xAA); // extra byte
        let err = decode(&bytes).expect_err("trailing");
        match err {
            DecodeError::TrailingBytes { tag, trailing } => {
                assert_eq!(tag, b'B');
                assert_eq!(trailing, 1);
            }
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
    }

    #[test]
    fn lsn_display_uses_postgres_format() {
        // 0x0000_0001_8000_0000 -> "1/80000000"
        assert_eq!(format!("{}", Lsn(0x0000_0001_8000_0000)), "1/80000000");
    }

    // ---- UPDATE / DELETE / TRUNCATE --------------------------------------

    #[test]
    fn decodes_update_without_old_tuple() {
        // 'U' + rel_id + 'N' marker + tuple
        let bytes = Builder::new(b'U')
            .u32(16_384)
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Update(u) => {
                assert_eq!(u.relation_id, 16_384);
                assert!(u.old.is_none());
                assert_eq!(u.new.columns.len(), 1);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decodes_update_with_key_old_tuple() {
        let bytes = Builder::new(b'U')
            .u32(1)
            .u8(b'K')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"7")
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"8")
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Update(u) => {
                let old = u.old.expect("old tuple");
                assert_eq!(old.kind, OldTupleKind::Key);
                assert_eq!(old.tuple.columns[0], TupleColumn::Text(b"7".to_vec()));
                assert_eq!(u.new.columns[0], TupleColumn::Text(b"8".to_vec()));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decodes_update_with_full_old_tuple() {
        let bytes = Builder::new(b'U')
            .u32(1)
            .u8(b'O')
            .i16(1)
            .u8(b'n')
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"v")
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Update(u) => {
                assert_eq!(u.old.expect("old").kind, OldTupleKind::Full);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn update_with_unknown_marker_errors() {
        let bytes = Builder::new(b'U').u32(1).u8(b'?').build();
        let err = decode(&bytes).expect_err("err");
        match err {
            DecodeError::InvalidValue { field, .. } => assert_eq!(field, "update.marker"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn decodes_delete_with_key_tuple() {
        let bytes = Builder::new(b'D')
            .u32(16_384)
            .u8(b'K')
            .i16(1)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Delete(d) => {
                assert_eq!(d.relation_id, 16_384);
                assert_eq!(d.old.kind, OldTupleKind::Key);
                assert_eq!(d.old.tuple.columns[0], TupleColumn::Text(b"42".to_vec()));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn delete_with_invalid_marker_errors() {
        let bytes = Builder::new(b'D').u32(1).u8(b'X').build();
        let err = decode(&bytes).expect_err("err");
        match err {
            DecodeError::InvalidValue { field, .. } => assert_eq!(field, "delete.marker"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn decodes_truncate_with_cascade_and_two_relations() {
        // 'T' + relation_count(u32=2) + flags(u8=0x01=CASCADE) + oid + oid
        let bytes = Builder::new(b'T').u32(2).u8(0x01).u32(100).u32(200).build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Truncate(t) => {
                assert!(t.cascade);
                assert!(!t.restart_identity);
                assert_eq!(t.relation_ids, vec![100, 200]);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn decodes_truncate_with_restart_identity() {
        let bytes = Builder::new(b'T').u32(1).u8(0x02).u32(1).build();
        let msg = decode(&bytes).expect("decode");
        match msg {
            LogicalMessage::Truncate(t) => {
                assert!(!t.cascade);
                assert!(t.restart_identity);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }
}
