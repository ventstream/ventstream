//! Hot-endpoint detection for projection-aware fan-out.
//!
//! ## The problem this solves
//!
//! The projection-aware fan-out builds anchor branches like
//!
//! ```cypher
//! MATCH (p:Author)-[:HAS_PUBLISH_STATUS]->(x) WHERE elementId(x) IN $eids
//! RETURN p
//! ```
//!
//! At event time, the engine feeds `$eids` from `[event.element_id,
//! start_eid, end_eid]`. When the event is a relationship change on
//! `HAS_PUBLISH_STATUS`, the `end_eid` is the PublishStatus node —
//! and there's exactly **one** PublishStatus node serving every
//! Author in the graph. The branch matches **every Author** as a
//! recompose target even though only the one Author at the rel's
//! start side actually had its relationship change.
//!
//! Validated at 100k Author scale: a single Author delete triggered a
//! 59,498-Author recompose cascade (~2s each), spawning ~5,800 such
//! cascades per 1,000-op burst — hours of wasted work.
//!
//! ## The fix in this module
//!
//! At spec-validation time we walk each `ProjectionPath` and probe
//! **every prefix** (not just the leaf). The node reached at each depth
//! is the *far* endpoint of that hop's relationship type; if its
//! cardinality is below `HOT_NODE_THRESHOLD` we record, keyed by that
//! rel type, which side of the edge is far-from-primary and the far
//! nodes' element IDs. Probing prefixes — not only leaves — is what
//! catches low-cardinality **intermediate** nodes (e.g. a Supplier on a
//! `SUPPLIED_BY → LOCATED_IN` path), not just terminal lookups.
//!
//! At event time, for a relationship event of type `T`, we drop only the
//! endpoint that is the far side of `T` (if it's a known hot node) and
//! keep the near side as the fan-out anchor. So a `SUPPLIED_BY` edge
//! change recomposes only the one product whose edge changed, while a
//! `LOCATED_IN` change still fans out from the Supplier to its products.
//!
//! ## Why key by rel type (not a flat eid set)
//!
//! A node can be the *far* endpoint for one rel type and the *near*
//! anchor for another — a Supplier is far for `SUPPLIED_BY` (filter it)
//! but near for `LOCATED_IN` (keep it, so its region change cascades).
//! A flat "always filter this eid" set can't express that and would
//! break the `LOCATED_IN` cascade. Keying by rel type does.
//!
//! ## Fail-safe
//!
//! We only ever *remove* an endpoint we've proven is the low-cardinality
//! far side of that exact rel type. Anything ambiguous — undirected
//! hops, a rel type whose orientation conflicts across paths, an
//! unanalysable (path-scan) spec — falls back to **no filtering**.
//! Rationale: under-filtering wastes work (spurious recompute, still
//! correct); over-filtering would drop a real update (stale doc).
//!
//! ## What this does NOT change
//!
//! Property changes on the node itself (a real `node.update` on a shared
//! lookup) **still** cascade to every referencing primary — that's
//! correct, the embedded value changed for all of them. Only
//! **relationship** events get the far-endpoint filter.
//!
//! ## Cost
//!
//! - **Startup**: one count query per path *prefix* (~ms each); a few
//!   per path. One-time.
//! - **Memory**: a small map (rel type → eids) per spec; kilobytes.
//! - **Runtime**: O(1) HashMap + HashSet lookup per relationship event.

use std::collections::{HashMap, HashSet};

use neo4rs::{query, Graph};
use tracing::{debug, info};

use super::denormalize::DenormalizeSpec;
use super::projection::{
    extract_projection_paths, Direction, Hop, ProjectionExtract, ProjectionPath,
};
use crate::error::Neo4jCdcError;

/// Default cardinality below which an anchor-path endpoint is treated
/// as "hot." Tuned via `VS_NEO4J_HOT_NODE_THRESHOLD`.
///
/// 100 is a deliberately conservative floor. Real fan-out leaves
/// (Names per Author, Reps per Author, Departments per company, ...)
/// usually correlate with primary count and dwarf this number. Lookup
/// tables (PublishStatus singletons, status enums, region lists, ...)
/// stay well under. The few cases that straddle the line can override
/// via env var.
pub const DEFAULT_HOT_NODE_THRESHOLD: usize = 100;

/// Env var for operator override. Set to `0` to disable hot-node
/// detection entirely (legacy behaviour).
pub const HOT_NODE_THRESHOLD_ENV: &str = "VS_NEO4J_HOT_NODE_THRESHOLD";

/// Which graph endpoint of a relationship sits *away from* the primary.
///
/// A projection hop `(p)-[:T]->(x)` (Out) reaches its far node `x` on the
/// relationship's **end**; `(p)<-[:T]-(x)` (In) reaches it on the
/// **start**. Knowing this lets us filter only the far endpoint of the
/// changed edge while keeping the near one as the fan-out anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FarSide {
    Start,
    End,
}

/// For one relationship type: which side is "far from the primary" and
/// the element IDs of the low-cardinality far nodes reached through it.
#[derive(Debug, Clone)]
struct HotRel {
    far_side: FarSide,
    eids: HashSet<String>,
}

/// Per-spec runtime data computed once during validation.
///
/// Maps a relationship **type** to its far endpoint info. On a CDC
/// relationship event we look the type up and, if found, drop the
/// far-side endpoint from the fan-out anchor (so an edge change to a
/// shared lookup recomposes only the primary, not every sibling). Keying
/// by type — not a flat eid set — is what lets the same node be filtered
/// for one rel type yet kept as an anchor for another (e.g. a Supplier is
/// "far" for `SUPPLIED_BY` but "near" for `LOCATED_IN`).
///
/// `Default` yields an empty map → no filtering, which is the correct
/// degenerate behaviour when detection is disabled or the spec can't be
/// statically analysed. Fail-safe throughout: when in doubt, don't
/// filter (spurious recompute is wasted work; over-filtering would drop
/// a real update).
#[derive(Debug, Clone, Default)]
pub struct SpecHotEndpoints {
    by_rel: HashMap<String, HotRel>,
}

impl SpecHotEndpoints {
    /// Decide whether to keep each endpoint of a relationship event in
    /// the fan-out anchor. Returns `(keep_start, keep_end)`.
    ///
    /// Fail-safe: an unknown rel type (or a node event with no type)
    /// keeps both endpoints — we only ever *remove* an endpoint we've
    /// proven is the low-cardinality far side of that exact rel type.
    #[inline]
    pub fn keep_endpoints(
        &self,
        rel_type: Option<&str>,
        start_eid: Option<&str>,
        end_eid: Option<&str>,
    ) -> (bool, bool) {
        let Some(rt) = rel_type else {
            return (true, true);
        };
        let Some(hot) = self.by_rel.get(rt) else {
            return (true, true);
        };
        match hot.far_side {
            FarSide::End => (true, !end_eid.is_some_and(|e| hot.eids.contains(e))),
            FarSide::Start => (!start_eid.is_some_and(|s| hot.eids.contains(s)), true),
        }
    }

    /// True when no rel type is registered for filtering.
    pub fn is_empty(&self) -> bool {
        self.by_rel.is_empty()
    }
}

/// Accumulates hot rel types across a spec's paths, dropping any type
/// whose far-side is inconsistent between paths (fail-safe: a rel type
/// that points "away from the primary" in one path but "toward" it in
/// another is ambiguous, so we refuse to filter it).
#[derive(Default)]
struct HotBuilder {
    by_rel: HashMap<String, HotRel>,
    disabled: HashSet<String>,
}

impl HotBuilder {
    fn add(&mut self, rel_type: &str, far_side: FarSide, eids: &HashSet<String>) {
        if self.disabled.contains(rel_type) {
            return;
        }
        match self.by_rel.get_mut(rel_type) {
            Some(existing) if existing.far_side != far_side => {
                // Same rel type, conflicting orientation → unfilterable.
                self.by_rel.remove(rel_type);
                self.disabled.insert(rel_type.to_owned());
            }
            Some(existing) => existing.eids.extend(eids.iter().cloned()),
            None => {
                self.by_rel.insert(
                    rel_type.to_owned(),
                    HotRel {
                        far_side,
                        eids: eids.clone(),
                    },
                );
            }
        }
    }

    fn finish(self) -> SpecHotEndpoints {
        SpecHotEndpoints {
            by_rel: self.by_rel,
        }
    }
}

/// Resolve the threshold from env, falling back to the default.
/// Invalid values fall back rather than failing — getting a sane
/// default at startup is more useful than refusing to boot over a
/// typo in an env var.
pub fn resolve_threshold() -> usize {
    match std::env::var(HOT_NODE_THRESHOLD_ENV) {
        Ok(s) => s
            .trim()
            .parse::<usize>()
            .unwrap_or(DEFAULT_HOT_NODE_THRESHOLD),
        Err(_) => DEFAULT_HOT_NODE_THRESHOLD,
    }
}

/// Walk every spec's anchor paths and capture the element IDs of
/// low-cardinality endpoints. Runs once at spec-validation time;
/// returns one [`SpecHotEndpoints`] per input spec, indexed parallel
/// to `specs`.
///
/// The threshold of 0 disables detection (returns empty sets) without
/// the caller having to know.
// `path.hops[..depth]` / `[depth-1]` are bounds-safe: depth ranges within hops.
#[allow(clippy::indexing_slicing)]
pub async fn compute_for_specs(
    graph: &Graph,
    specs: &[DenormalizeSpec],
    threshold: usize,
) -> Result<Vec<SpecHotEndpoints>, Neo4jCdcError> {
    if threshold == 0 {
        return Ok(specs.iter().map(|_| SpecHotEndpoints::default()).collect());
    }
    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        let mut builder = HotBuilder::default();
        let ProjectionExtract::Paths(paths) = extract_projection_paths(&spec.cypher) else {
            // Path-scan fallback specs can't be optimised this way —
            // the engine's WHERE clause is variable-length, so hot
            // endpoints aren't isolatable. Leave the map empty.
            debug!(
                primary = %spec.primary_label,
                "hot-endpoint detection skipped (spec uses path-scan fan-out)"
            );
            out.push(SpecHotEndpoints::default());
            continue;
        };
        for path in &paths {
            // Bound by the spec's declared max hops — same boundary the
            // fan-out cypher respects, so we don't probe deeper than the
            // engine would itself.
            let bound = path.hops.len().min(spec.fan_out_max_hops);
            // Probe EVERY prefix, not just the leaf: the node reached at
            // each depth is the far endpoint of that hop's rel type, so a
            // low-cardinality *intermediate* node (e.g. a Supplier on a
            // SUPPLIED_BY → LOCATED_IN path) is caught, not only the leaf.
            for depth in 1..=bound {
                let last = &path.hops[depth - 1];
                let far_side = match last.direction {
                    Direction::Out => FarSide::End,
                    Direction::In => FarSide::Start,
                    // Undirected: can't say which endpoint is "far" →
                    // fail safe, skip (the node may still be caught as a
                    // leaf via another, directed path).
                    Direction::Undirected => continue,
                };
                let pattern = render_pattern(&path.hops[..depth]);
                let Some(eids) =
                    count_path_endpoints(graph, &spec.primary_label, &pattern, threshold).await?
                else {
                    continue;
                };
                for rel_type in &last.rel_types {
                    builder.add(rel_type, far_side, &eids);
                    info!(
                        primary = %spec.primary_label,
                        rel_type = %rel_type,
                        far_side = ?far_side,
                        pattern = %pattern,
                        hot_eids = eids.len(),
                        threshold,
                        "hot-endpoint detection: low-cardinality far node, filtering its rel-type events"
                    );
                }
            }
        }
        let hot = builder.finish();
        debug!(
            primary = %spec.primary_label,
            hot_rel_types = hot.by_rel.len(),
            "hot-endpoint detection complete for spec"
        );
        out.push(hot);
    }
    Ok(out)
}

/// Probe one anchor path for low-cardinality endpoints.
///
/// Returns `Some(eids)` when the path's distinct endpoint count is at
/// or below the threshold — those eids are "hot."  Returns `None`
/// when the count exceeds the threshold or when the path is empty.
///
/// The query uses `collect(...)[0..N+1]` to materialise at most
/// `threshold + 1` endpoint eids — enough to distinguish "small" from
/// "large" without ever pulling a 100k-row list back to the client.
async fn count_path_endpoints(
    graph: &Graph,
    primary_label: &str,
    pattern: &str,
    threshold: usize,
) -> Result<Option<HashSet<String>>, Neo4jCdcError> {
    let cypher = format!(
        "MATCH (p:`{primary_label}`){pattern}(x) \
         WITH DISTINCT x \
         WITH collect(elementId(x))[0..{cap}] AS eids \
         RETURN eids",
        cap = threshold + 1,
    );
    let mut rows = graph
        .execute(query(&cypher))
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("hot-endpoint probe: {err}")))?;
    let Some(row) = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("hot-endpoint probe iter: {err}")))?
    else {
        return Ok(None);
    };
    let eids: Vec<String> = row
        .get("eids")
        .map_err(|err| Neo4jCdcError::Query(format!("hot-endpoint probe eids: {err}")))?;
    if eids.len() > threshold {
        // High cardinality — leaf is a real fan-out target, leave it
        // alone. The engine will scan the path normally.
        return Ok(None);
    }
    if eids.is_empty() {
        return Ok(None);
    }
    Ok(Some(eids.into_iter().collect()))
}

/// Render one anchor path as the inline Cypher segment between
/// `(p)` and the trailing `(x)`. Duplicated from `projection.rs` to
/// avoid making the helper public and tying its API to this module's
/// needs; the rendering is straightforward.
fn render_pattern(hops: &[Hop]) -> String {
    let mut out = String::new();
    for (i, hop) in hops.iter().enumerate() {
        let type_list = hop.rel_types.iter().cloned().collect::<Vec<_>>().join("|");
        let body = format!("[:{type_list}]");
        match hop.direction {
            Direction::Out => out.push_str(&format!("-{body}->")),
            Direction::In => out.push_str(&format!("<-{body}-")),
            Direction::Undirected => out.push_str(&format!("-{body}-")),
        }
        if i + 1 < hops.len() {
            out.push_str("()");
        }
    }
    out
}

/// Marker for [`ProjectionPath`] so `extract_projection_paths`'s
/// internal type stays accessible here without making it public.
/// Compile-time only — never used at runtime.
#[allow(dead_code)]
fn _path_type_marker(_: &ProjectionPath) {}

#[cfg(test)]
mod tests {
    use super::*;

    // All `resolve_threshold` cases live in ONE test on purpose. They each
    // mutate the same process-global env var (`HOT_NODE_THRESHOLD_ENV`), and
    // cargo runs a test binary's `#[test]`s on multiple threads regardless of
    // module — so as separate tests they race (one's `remove_var` clobbers
    // another's `set_var` mid-assert). Sequencing the cases in a single test
    // removes the race without a serialization crate or `--test-threads=1`.
    #[test]
    fn resolve_threshold_reads_env_with_fallbacks() {
        // Unset → default.
        std::env::remove_var(HOT_NODE_THRESHOLD_ENV);
        assert_eq!(
            resolve_threshold(),
            DEFAULT_HOT_NODE_THRESHOLD,
            "unset → default"
        );

        // Parses a valid number.
        std::env::set_var(HOT_NODE_THRESHOLD_ENV, "50");
        assert_eq!(resolve_threshold(), 50, "parses the configured value");

        // Zero is honoured (disables hot-node detection).
        std::env::set_var(HOT_NODE_THRESHOLD_ENV, "0");
        assert_eq!(
            resolve_threshold(),
            0,
            "0 is honoured, not treated as unset"
        );

        // Garbage → default.
        std::env::set_var(HOT_NODE_THRESHOLD_ENV, "not-a-number");
        assert_eq!(
            resolve_threshold(),
            DEFAULT_HOT_NODE_THRESHOLD,
            "unparseable → default"
        );

        std::env::remove_var(HOT_NODE_THRESHOLD_ENV);
    }

    fn hot(side: FarSide, eids: &[&str]) -> HotRel {
        HotRel {
            far_side: side,
            eids: eids.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    #[test]
    fn empty_keeps_both_endpoints() {
        let h = SpecHotEndpoints::default();
        assert!(h.is_empty());
        assert_eq!(
            h.keep_endpoints(Some("SUPPLIED_BY"), Some("a"), Some("b")),
            (true, true)
        );
    }

    #[test]
    fn unknown_rel_type_keeps_both() {
        let mut by_rel = HashMap::new();
        by_rel.insert("SUPPLIED_BY".to_owned(), hot(FarSide::End, &["sup1"]));
        let h = SpecHotEndpoints { by_rel };
        // A rel type we never registered is never filtered.
        assert_eq!(
            h.keep_endpoints(Some("HAS_TAG"), Some("p1"), Some("tag1")),
            (true, true)
        );
        // A node event (no rel type) is never filtered.
        assert_eq!(h.keep_endpoints(None, None, None), (true, true));
    }

    #[test]
    fn end_far_filters_only_end_when_hot() {
        // SUPPLIED_BY: (product)-[:SUPPLIED_BY]->(supplier); far = End.
        let mut by_rel = HashMap::new();
        by_rel.insert("SUPPLIED_BY".to_owned(), hot(FarSide::End, &["sup1"]));
        let h = SpecHotEndpoints { by_rel };
        // start=product (keep), end=hot supplier (drop) → only product anchors.
        assert_eq!(
            h.keep_endpoints(Some("SUPPLIED_BY"), Some("prodX"), Some("sup1")),
            (true, false)
        );
        // end is a supplier NOT in the hot set → keep (fail-safe).
        assert_eq!(
            h.keep_endpoints(Some("SUPPLIED_BY"), Some("prodX"), Some("sup999")),
            (true, true)
        );
    }

    #[test]
    fn start_far_filters_only_start_when_hot() {
        // An In hop: (author)<-[:HAS_MEMBER]-(group); far = Start.
        let mut by_rel = HashMap::new();
        by_rel.insert("HAS_MEMBER".to_owned(), hot(FarSide::Start, &["grp1"]));
        let h = SpecHotEndpoints { by_rel };
        assert_eq!(
            h.keep_endpoints(Some("HAS_MEMBER"), Some("grp1"), Some("partyX")),
            (false, true)
        );
    }

    #[test]
    fn builder_unions_same_side_and_disables_conflicts() {
        let mut b = HotBuilder::default();
        let s1: HashSet<String> = ["a"].iter().map(|s| (*s).to_owned()).collect();
        let s2: HashSet<String> = ["b"].iter().map(|s| (*s).to_owned()).collect();
        // Same rel type, same far side across two paths → union.
        b.add("REL", FarSide::End, &s1);
        b.add("REL", FarSide::End, &s2);
        // A second rel type with a conflicting orientation → disabled.
        b.add("AMB", FarSide::End, &s1);
        b.add("AMB", FarSide::Start, &s2);
        let h = b.finish();
        // REL keeps both eids, filters its End.
        assert_eq!(
            h.keep_endpoints(Some("REL"), Some("x"), Some("a")),
            (true, false)
        );
        assert_eq!(
            h.keep_endpoints(Some("REL"), Some("x"), Some("b")),
            (true, false)
        );
        // AMB was ambiguous → never filtered.
        assert_eq!(
            h.keep_endpoints(Some("AMB"), Some("a"), Some("b")),
            (true, true)
        );
    }
}
