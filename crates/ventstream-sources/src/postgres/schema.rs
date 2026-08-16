//! Cache of [`Relation`] schemas keyed by Postgres relation oid.
//!
//! `pgoutput` emits a `RELATION` message the first time a table appears
//! in a stream and again whenever the publication's view of the table
//! changes. Subsequent `INSERT`/`UPDATE`/`DELETE` messages reference the
//! relation by oid alone, so we must remember the most recent schema
//! description in order to interpret tuple data.
//!
//! Concurrency: writes happen only on the replication-reader task; reads
//! happen on the dispatcher task. We use a `RwLock` to keep the read path
//! cheap (it is in the hot path) while letting the writer take an
//! exclusive lock briefly when a new `RELATION` arrives.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use tracing::warn;

use super::pgoutput::Relation;

/// Compare a freshly-arrived RELATION against the previously-cached one
/// for the same table and surface any **source schema drift** as a
/// first-class telemetry signal: added / dropped / retyped columns.
///
/// Warn-only — the pipeline keeps flowing (a response *policy* is a later
/// phase). Without this, a source schema change is invisible until it
/// shows up downstream as a sink mapping conflict (→ DLQ). A developer
/// greps `metric=schema.drift` or watches `vs_schema_drift_total{table,kind}`.
///
/// Column names go in the log (potentially high cardinality); only the
/// bounded `table` + `kind` become metric labels.
pub(crate) fn detect_drift(old: &Relation, new: &Relation) {
    let table = format!("{}.{}", new.namespace, new.name);
    let mut retyped = false;
    for d in diff_columns(old, new) {
        match d.types {
            Some((from, to)) => warn!(
                metric = "schema.drift",
                table = %table,
                column = %d.column,
                kind = d.kind,
                from_type_oid = from,
                to_type_oid = to,
                "source schema drift detected"
            ),
            None => warn!(
                metric = "schema.drift",
                table = %table,
                column = %d.column,
                kind = d.kind,
                "source schema drift detected"
            ),
        }
        metrics::counter!("vs_schema_drift_total", "table" => table.clone(), "kind" => d.kind)
            .increment(1);
        retyped |= d.types.is_some();
    }
    if retyped {
        bump_type_change_epoch(&new.name);
    }
}

/// Process-wide type-change epoch per table, keyed by unqualified
/// lowercased name. Bumped when a RELATION message shows a column retype,
/// so SQL builders that cache catalog types (the related fetcher, the SQL
/// denormaliser) can notice staleness without holding a reference to the
/// replication decoder.
static TYPE_CHANGE_EPOCHS: OnceLock<RwLock<HashMap<String, u64>>> = OnceLock::new();

fn type_change_epochs() -> &'static RwLock<HashMap<String, u64>> {
    TYPE_CHANGE_EPOCHS.get_or_init(RwLock::default)
}

/// Accepts any qualification (`orders`, `public.orders`, `"public"."orders"`).
fn epoch_key(table: &str) -> String {
    table
        .rsplit('.')
        .next()
        .unwrap_or(table)
        .trim_matches('"')
        .to_ascii_lowercase()
}

/// Current type-change epoch for a table. Starts at 0; each detected
/// column retype increments it.
pub fn type_change_epoch(table: &str) -> u64 {
    type_change_epochs()
        .read()
        .get(&epoch_key(table))
        .copied()
        .unwrap_or(0)
}

fn bump_type_change_epoch(table: &str) {
    *type_change_epochs()
        .write()
        .entry(epoch_key(table))
        .or_insert(0) += 1;
}

/// One detected column-level drift. `kind` is `added`, `dropped`, or
/// `type_change`; `types` carries `(old_oid, new_oid)` for a type change.
#[derive(Debug, PartialEq, Eq)]
struct Drift {
    column: String,
    kind: &'static str,
    types: Option<(u32, u32)>,
}

/// Pure column-set diff between two relation schemas — separated from the
/// log/metric side effects so it can be unit-tested.
fn diff_columns(old: &Relation, new: &Relation) -> Vec<Drift> {
    let old_cols: HashMap<&str, u32> = old
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.type_oid))
        .collect();
    let new_cols: HashMap<&str, u32> = new
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.type_oid))
        .collect();

    let mut drifts = Vec::new();
    for (name, ty) in &new_cols {
        match old_cols.get(name) {
            None => drifts.push(Drift {
                column: (*name).to_owned(),
                kind: "added",
                types: None,
            }),
            Some(prev) if prev != ty => drifts.push(Drift {
                column: (*name).to_owned(),
                kind: "type_change",
                types: Some((*prev, *ty)),
            }),
            _ => {}
        }
    }
    for name in old_cols.keys() {
        if !new_cols.contains_key(name) {
            drifts.push(Drift {
                column: (*name).to_owned(),
                kind: "dropped",
                types: None,
            });
        }
    }
    drifts
}

/// Thread-safe cache of relation schemas.
///
/// Cloning a [`RelationCache`] is cheap (Arc bump) — clones share the
/// same underlying map.
#[derive(Debug, Clone, Default)]
pub struct RelationCache {
    inner: Arc<RwLock<HashMap<u32, Arc<Relation>>>>,
}

impl RelationCache {
    /// Construct an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store or replace the schema for the given relation oid.
    ///
    /// Returns the prior schema if one existed.
    pub fn insert(&self, relation: Relation) -> Option<Arc<Relation>> {
        let id = relation.id;
        let entry = Arc::new(relation);
        self.inner.write().insert(id, entry)
    }

    /// Look up a relation by oid. Returns `None` if no `RELATION`
    /// message for this oid has been seen yet.
    pub fn get(&self, id: u32) -> Option<Arc<Relation>> {
        self.inner.read().get(&id).map(Arc::clone)
    }

    /// Number of relations currently cached.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Whether the cache holds no relations.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
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
    use crate::postgres::pgoutput::{Column, ColumnFlags, ReplicaIdentity};

    fn relation(id: u32, name: &str) -> Relation {
        Relation {
            id,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![Column {
                flags: ColumnFlags(0x01),
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
            }],
        }
    }

    #[test]
    fn insert_then_get_roundtrip() {
        let cache = RelationCache::new();
        assert!(cache.is_empty());
        assert!(cache.insert(relation(16_384, "users")).is_none());
        assert_eq!(cache.len(), 1);

        let fetched = cache.get(16_384).expect("present");
        assert_eq!(fetched.name, "users");
    }

    #[test]
    fn insert_replaces_existing_relation() {
        let cache = RelationCache::new();
        cache.insert(relation(16_384, "users"));
        let previous = cache.insert(relation(16_384, "users_renamed"));
        assert!(previous.is_some());
        assert_eq!(cache.get(16_384).expect("present").name, "users_renamed");
    }

    #[test]
    fn missing_relation_returns_none() {
        let cache = RelationCache::new();
        assert!(cache.get(42).is_none());
    }

    fn col(name: &str, type_oid: u32) -> Column {
        Column {
            flags: ColumnFlags(0),
            name: name.into(),
            type_oid,
            type_modifier: -1,
        }
    }

    fn relation_with(cols: Vec<Column>) -> Relation {
        Relation {
            id: 1,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: cols,
        }
    }

    #[test]
    fn diff_detects_added_dropped_and_retyped() {
        // old: id int(23), name text(25)
        // new: id bigint(20) [type_change], name dropped, email text(25) added
        let old = relation_with(vec![col("id", 23), col("name", 25)]);
        let new = relation_with(vec![col("id", 20), col("email", 25)]);

        let mut drifts = diff_columns(&old, &new);
        drifts.sort_by(|a, b| a.column.cmp(&b.column));

        assert_eq!(
            drifts,
            vec![
                Drift {
                    column: "email".into(),
                    kind: "added",
                    types: None
                },
                Drift {
                    column: "id".into(),
                    kind: "type_change",
                    types: Some((23, 20))
                },
                Drift {
                    column: "name".into(),
                    kind: "dropped",
                    types: None
                },
            ]
        );
    }

    #[test]
    fn diff_is_empty_for_identical_schema() {
        let a = relation_with(vec![col("id", 23), col("name", 25)]);
        let b = relation_with(vec![col("id", 23), col("name", 25)]);
        assert!(diff_columns(&a, &b).is_empty());
    }
}
