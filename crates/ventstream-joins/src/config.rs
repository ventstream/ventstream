//! Typed configuration for join definitions.
//!
//! Deserializes directly from the `joins:` section of `pipeline.yaml`.
//! Single-column primary keys and FKs are written as plain strings;
//! composite keys are written as arrays. Both shapes deserialize into
//! the normalized [`PkSpec`] (always a `Vec<String>` internally).

use serde::Deserialize;

/// One complete join definition. Each definition produces one stream
/// of composed events keyed by its `primary` table.
#[derive(Debug, Clone, Deserialize)]
pub struct JoinDefinition {
    /// Identifier used in subjects, log lines, and state-storage paths.
    /// Defaults to the primary table when omitted.
    #[serde(default)]
    pub name: Option<String>,

    /// The driving table: events on this table emit one composed
    /// document each (after compose with related rows).
    pub primary: PrimaryRef,

    /// Foreign tables to enrich the primary with.
    #[serde(default)]
    pub related: Vec<RelatedDefinition>,

    /// Optional sink-routing target for the composed document stream.
    ///
    /// When set, the join engine stamps `ventstream.target.index` on every
    /// composed event and tombstone. OpenSearch can then route with
    /// `${header:ventstream.target.index}` while the sink connection remains
    /// global to the pipeline.
    #[serde(default)]
    pub target: TargetConfig,

    /// State storage knobs (in-memory only in this release).
    #[serde(default)]
    pub state: StateConfig,

    /// Backfill strategy for foreign rows not yet seen.
    #[serde(default)]
    pub backfill: BackfillConfig,
}

impl JoinDefinition {
    /// Effective join name — defaults to the primary table when not
    /// explicitly set.
    pub fn effective_name(&self) -> &str {
        self.name.as_deref().unwrap_or(self.primary.table.as_str())
    }

    /// Explicit target index for this output document stream, when configured.
    pub fn target_index(&self) -> Option<&str> {
        self.target
            .index
            .as_deref()
            .map(str::trim)
            .filter(|index| !index.is_empty())
    }
}

/// Optional output target metadata for one projection.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TargetConfig {
    /// Concrete OpenSearch/Elasticsearch index name for this composed document.
    #[serde(default)]
    pub index: Option<String>,
}

/// Primary table reference.
#[derive(Debug, Clone, Deserialize)]
pub struct PrimaryRef {
    /// Fully-qualified table name in the form `{namespace}.{relation}`
    /// (e.g. `"public.orders"`).
    pub table: String,
    /// Primary-key column(s). Single column or composite.
    pub pk: PkSpec,
}

/// One related (foreign) entry that the primary joins against.
#[derive(Debug, Clone, Deserialize)]
pub struct RelatedDefinition {
    /// Unique within the join. Drives reverse-index naming, lets two
    /// `related` entries point at the same table with different FKs
    /// (billing/shipping customer pattern).
    pub id: String,

    /// Fully-qualified table name of the related table.
    pub table: String,

    /// Primary-key column(s) on the related table.
    pub pk: PkSpec,

    /// Join condition.
    pub join_on: JoinOn,

    /// Columns to project from the related row into the composed doc.
    /// Empty means embed the entire row.
    ///
    /// **Must include the child PK for 1:many deletes under Postgres
    /// `REPLICA IDENTITY DEFAULT`.** SQL-denormalize mode resolves a child
    /// delete (whose WAL pre-image lacks the parent FK) by querying the sink
    /// for the parent doc that embeds the deleted child's PK. If `select`
    /// omits the child PK, the embedded child has no PK keyword to match, so
    /// the lookup finds nothing and the delete silently stops propagating
    /// (degrades to a skip). An empty `select` embeds the whole row, so the
    /// PK is always present.
    #[serde(default)]
    pub select: Vec<String>,

    /// JSON path on the composed document where the projected related
    /// data is embedded. Single-segment paths only in v1; nested paths
    /// like `"customer.contact"` are post-MVP.
    pub embed_as: String,

    /// One vs. many. Determines whether `embed_as` is an object or an
    /// array, and whether the engine maintains a secondary FK index.
    #[serde(default)]
    pub cardinality: Cardinality,

    /// What to do when no related row is found.
    #[serde(default)]
    pub on_missing: OnMissing,

    /// Sort order for `cardinality: many` (single column ascending,
    /// JSON-value compare). Without this, ordering is the insertion
    /// order in the state — non-deterministic.
    #[serde(default)]
    pub sort_by: Option<String>,
}

/// Column reference — accepts either a single string or an array of
/// strings in YAML and normalizes to a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkSpec(pub Vec<String>);

impl PkSpec {
    /// Borrow the column names.
    pub fn columns(&self) -> &[String] {
        &self.0
    }

    /// Whether this is a composite (multi-column) key.
    pub fn is_composite(&self) -> bool {
        self.0.len() > 1
    }
}

impl<'de> Deserialize<'de> for PkSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            One(String),
            Many(Vec<String>),
        }
        match Helper::deserialize(deserializer)? {
            Helper::One(s) => Ok(Self(vec![s])),
            Helper::Many(v) => {
                if v.is_empty() {
                    Err(serde::de::Error::custom(
                        "pk / from / to must have at least one column",
                    ))
                } else {
                    Ok(Self(v))
                }
            }
        }
    }
}

/// Pair of column references describing the join condition.
#[derive(Debug, Clone, Deserialize)]
pub struct JoinOn {
    /// Column(s) on the primary side.
    pub from: PkSpec,
    /// Column(s) on the related side.
    pub to: PkSpec,
}

/// Whether a related entry yields one row (object) or many (array).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    /// Embedded as a JSON object (or `null` per [`OnMissing`]).
    #[default]
    One,
    /// Embedded as a JSON array.
    Many,
}

/// What to do when a related row is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnMissing {
    /// Embed `null` (object case) or `[]` (array case).
    #[default]
    Null,
    /// Embed an empty object / array.
    EmptyObject,
    /// Drop the primary event entirely until the related row is seen.
    /// **Not yet implemented in v1** — currently behaves the same as
    /// [`OnMissing::Null`]. Surfaced in the config so YAML files can
    /// declare intent today and the behaviour activates without a
    /// schema change.
    DropPrimary,
}

/// State-storage configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StateConfig {
    /// Storage backend. Only `memory` is implemented in v1.
    #[serde(default)]
    pub backend: StateBackend,
}

/// Available state backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateBackend {
    /// In-memory HashMaps. Lost on restart.
    #[default]
    Memory,
}

/// Backfill strategy.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BackfillConfig {
    /// When to populate state for foreign rows we haven't yet seen.
    #[serde(default)]
    pub mode: BackfillMode,
}

/// Available backfill modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillMode {
    /// On the first primary event whose related row is not in state,
    /// issue a synchronous SELECT against the source database to
    /// populate it. Subsequent foreign-side updates will then correctly
    /// trigger re-emission.
    #[default]
    SyncOnMiss,
    /// Don't fetch — emit composed doc with the configured `on_missing`
    /// behaviour. Use only when foreign data is guaranteed to flow
    /// through the engine before any primary row that needs it.
    None,
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

    #[test]
    fn pk_spec_accepts_single_string() {
        let yaml = r"id";
        let parsed: PkSpec = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(parsed.0, vec!["id".to_string()]);
        assert!(!parsed.is_composite());
    }

    #[test]
    fn pk_spec_accepts_array() {
        let yaml = "[region, customer_id]";
        let parsed: PkSpec = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(
            parsed.0,
            vec!["region".to_string(), "customer_id".to_string()]
        );
        assert!(parsed.is_composite());
    }

    #[test]
    fn pk_spec_rejects_empty_array() {
        let yaml = "[]";
        let err = serde_yaml::from_str::<PkSpec>(yaml).expect_err("should reject");
        assert!(err.to_string().contains("at least one column"));
    }

    #[test]
    fn full_join_definition_round_trip() {
        let yaml = r#"
name: orders_denormalized
primary:
  table: public.orders
  pk: id
related:
  - id: customer
    table: public.customers
    pk: id
    join_on: { from: customer_id, to: id }
    select: [id, name, email]
    embed_as: customer
    cardinality: one
  - id: items
    table: public.line_items
    pk: id
    join_on: { from: id, to: order_id }
    select: [id, product, qty]
    embed_as: items
    cardinality: many
    sort_by: id
state:
  backend: memory
backfill:
  mode: sync_on_miss
"#;
        let def: JoinDefinition = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(def.name.as_deref(), Some("orders_denormalized"));
        assert_eq!(def.primary.table, "public.orders");
        assert_eq!(def.primary.pk.columns(), &["id"]);
        assert_eq!(def.related.len(), 2);
        assert_eq!(def.related[0].cardinality, Cardinality::One);
        assert_eq!(def.related[1].cardinality, Cardinality::Many);
        assert_eq!(def.related[1].sort_by.as_deref(), Some("id"));
    }

    #[test]
    fn effective_name_defaults_to_primary_table() {
        let yaml = r"
primary:
  table: public.orders
  pk: id
";
        let def: JoinDefinition = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(def.effective_name(), "public.orders");
    }

    #[test]
    fn target_index_is_optional_projection_metadata() {
        let yaml = r"
name: orders
primary:
  table: shop.orders
  pk: order_id
target:
  index: tenant_a_orders
";
        let def: JoinDefinition = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(def.target_index(), Some("tenant_a_orders"));
    }

    #[test]
    fn composite_keys_round_trip() {
        let yaml = r"
primary:
  table: public.regional_orders
  pk: [region, id]
related:
  - id: regional_customer
    table: public.region_customers
    pk: [region, id]
    join_on: { from: [region, customer_id], to: [region, id] }
    embed_as: customer
";
        let def: JoinDefinition = serde_yaml::from_str(yaml).expect("parse");
        assert!(def.primary.pk.is_composite());
        assert_eq!(
            def.related[0].join_on.from.columns(),
            &["region", "customer_id"]
        );
        assert_eq!(def.related[0].join_on.to.columns(), &["region", "id"]);
    }
}
