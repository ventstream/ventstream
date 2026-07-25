//! Projection-aware fan-out.
//!
//! Parses the user's denormalize Cypher to extract the typed graph
//! paths walked from the primary anchor `(p)`, then turns those paths
//! into a fan-out `WHERE` clause that bounds the search by relationship
//! type instead of doing a variable-length path scan.
//!
//! ### Why this exists
//!
//! The original fan-out template was:
//!
//! ```cypher
//! MATCH (p:`Label`) WHERE
//!   elementId(p) IN $eids
//!   OR EXISTS {
//!     MATCH path = (p)-[*1..N]-(x)
//!     WHERE any(r IN relationships(path) WHERE elementId(r) IN $eids)
//!        OR any(n IN nodes(path)         WHERE elementId(n) IN $eids)
//!   }
//! ```
//!
//! The planner anchors at `MATCH (p:Label)` — that's a label scan over
//! every primary in the graph. At 100k Authors + a 3-hop path scan,
//! cascade events (Department rename → 7,500 affected Authors) pegged
//! Neo4j for 4+ minutes.
//!
//! ### What this does
//!
//! Replace the `[*1..N]` path scan with one `EXISTS` per typed sub-path
//! the user's Cypher actually uses:
//!
//! ```cypher
//! MATCH (p:`Author`) WHERE
//!   elementId(p) IN $eids
//!   OR EXISTS { MATCH (p)-[:HAS_NAME]->(x) WHERE elementId(x) IN $eids }
//!   OR EXISTS { MATCH (p)-[:HAS_DEPARTMENT]->(x) WHERE elementId(x) IN $eids }
//!   OR EXISTS { MATCH (p)-[:HAS_BOOK]->(x) WHERE elementId(x) IN $eids }
//!   OR EXISTS { MATCH (p)-[:HAS_BOOK]->()-[:IN_GENRE]->(x) WHERE elementId(x) IN $eids }
//!   ...
//! ```
//!
//! Each `EXISTS` is a small, type-bounded traversal — the planner can
//! satisfy it by starting at `x` via the elementId lookup and walking
//! the typed edge back to `p`. No 100k-Author scan.
//!
//! ### When this falls back
//!
//! If the user's Cypher contains anonymous rels (`-[]-`), variable-length
//! rels (`-[*1..3]-`), or a chain we can't parse, [`extract_projection_paths`]
//! returns [`ProjectionExtract::Unsupported`] and the caller stays on
//! the old path-scan form. The extractor is conservative: anything
//! beyond a straight chain of typed hops triggers fallback.

use std::collections::BTreeSet;

/// One typed graph path starting at the primary anchor `(p)`.
///
/// Order in `hops` is the order the user wrote it (away from `p`). A
/// hop's `direction` is the arrow on its bracket as seen from `p`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionPath {
    pub hops: Vec<Hop>,
}

/// One relationship in a [`ProjectionPath`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Hop {
    pub direction: Direction,
    /// Set of allowed rel types for this hop. `[:A|B]` becomes
    /// `{A, B}`. Multi-type hops render as `[:A|B]` in the output
    /// pattern.
    pub rel_types: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// `(a)-[:T]->(b)`
    Out,
    /// `(a)<-[:T]-(b)`
    In,
    /// `(a)-[:T]-(b)` — direction-agnostic.
    Undirected,
}

/// Outcome of trying to parse projection paths from a user's Cypher.
#[derive(Debug, Clone)]
pub(crate) enum ProjectionExtract {
    /// Caller can use projection-aware fan-out. May be empty when the
    /// user's Cypher does no hops away from `p` (a pure-property
    /// projection) — that case still benefits from the absence of the
    /// path-scan fallback.
    Paths(Vec<ProjectionPath>),
    /// Some part of the Cypher can't be expressed as typed fixed-length
    /// hops. Caller should fall back to the variable-length path-scan
    /// fan-out, which handles arbitrary user Cypher at the cost of
    /// being slow on cascade events.
    Unsupported { reason: String },
}

/// Extract every typed projection path that starts at `(p)` or `(p:Label)`
/// from the user's Cypher body.
pub(crate) fn extract_projection_paths(cypher: &str) -> ProjectionExtract {
    let stripped = strip_string_literals(cypher);
    let bytes = stripped.as_bytes();
    let mut paths: Vec<ProjectionPath> = Vec::new();

    for start in find_primary_anchors(&stripped) {
        match extract_chain(&stripped, bytes, start) {
            Ok(Some(path)) => paths.push(path),
            Ok(None) => continue,
            Err(reason) => return ProjectionExtract::Unsupported { reason },
        }
    }

    // Re-anchoring guard. We only follow chains that START at `(p)`. If the
    // body walks a relationship from an intermediate variable instead — e.g.
    // `MATCH (p)-[:A]->(rep) ... MATCH (rep)-[:B]->(x)` — that hop is invisible
    // to `find_primary_anchors`, so a change to `x` would never recompose `p`'s
    // doc (silent stale doc). Detect it soundly by counting: every relationship
    // hop in the body must be accounted for by a `(p)`-anchored chain. If the
    // body has MORE hops than we captured, at least one traversal re-anchors —
    // fall back to the (slower but correct) variable-length path scan.
    let captured_hops: usize = paths.iter().map(|p| p.hops.len()).sum();
    let total_hops = count_relationship_hops(&stripped);
    if total_hops > captured_hops {
        return ProjectionExtract::Unsupported {
            reason: format!(
                "cypher has {total_hops} relationship hop(s) but only {captured_hops} \
                 anchored at (p); a traversal re-anchors on an intermediate variable, \
                 which projection-aware fan-out cannot follow"
            ),
        };
    }

    ProjectionExtract::Paths(paths)
}

/// Count relationship hops in the (string-literal-stripped) Cypher.
///
/// Two shapes count as a hop:
///
/// 1. **Bracketed** (`-[...]->`, `<-[...]-`, `-[...]-`): the `[` is always
///    immediately preceded (ignoring whitespace) by a `-`. List literals
///    (`[1,2]`), list indexing (`coll[0]`), and map literals (`{...}`) are
///    never preceded by `-`, so they don't false-count.
/// 2. **Bracketless** (`-->`, `<--`, `--`): a `)` followed (ignoring
///    whitespace) by a run of only `-`/`<`/`>` containing at least one `-`,
///    then a `(`. These carry no rel type so `extract_chain` can never capture
///    them — meaning a bracketless hop on a re-anchored variable would slip
///    past the guard if we only counted brackets. Bracketed rels open with
///    `)-[`, so the `[` stops the connector run before the `(` and they aren't
///    double-counted (the bracket arm already counts them).
// Hand-written byte scanner; indices guarded by `< len` checks.
#[allow(clippy::indexing_slicing)]
fn count_relationship_hops(s: &str) -> usize {
    let bytes = s.as_bytes();
    let mut count = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => {
                // Walk back over whitespace to the previous significant byte.
                let mut j = i;
                let mut prev: Option<u8> = None;
                while j > 0 {
                    j -= 1;
                    if !bytes[j].is_ascii_whitespace() {
                        prev = Some(bytes[j]);
                        break;
                    }
                }
                if prev == Some(b'-') {
                    count += 1;
                }
            }
            b')' => {
                // Look for a bracketless connector run to the next node `(`.
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                let mut saw_dash = false;
                while j < bytes.len() && matches!(bytes[j], b'-' | b'<' | b'>') {
                    if bytes[j] == b'-' {
                        saw_dash = true;
                    }
                    j += 1;
                }
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if saw_dash && j < bytes.len() && bytes[j] == b'(' {
                    count += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    count
}

/// Build the projection-aware `CALL { UNION }` block that resolves
/// candidate primary nodes by anchoring at `$eids` and walking outward
/// through typed relationships back to `(p:Label)`.
///
/// Why this shape instead of a WHERE-clause OR:
///
/// A `MATCH (p:Label) WHERE elementId(p) IN $eids OR EXISTS {...}` form
/// is functionally correct but forces the planner to anchor at
/// `MATCH (p:Label)` — a full label scan. At 100k primaries with ~10
/// `EXISTS` clauses, every event paid the label-scan cost regardless of
/// fan-out reach (measured: ~370ms per event for events that only
/// affect 2 docs).
///
/// The `CALL { UNION }` form has each branch anchor at the indexed
/// `elementId(x) IN __eids` lookup (1–3 nodes) and walk back through a
/// typed pattern to `(p:Label)`. Cost becomes
/// O(|eids| × reverse_degree). Hop-0 events drop from ~370ms to single-
/// digit ms; cascade events stay in the seconds range.
///
/// Output shape (caller pastes the user's Cypher body after this block):
///
/// ```cypher
/// WITH $eids AS __eids
/// CALL {
///   WITH __eids
///   MATCH (p:`Label`) WHERE elementId(p) IN __eids RETURN p
///   UNION
///   WITH __eids
///   MATCH (p:`Label`)-[:HAS_NAME]->(x) WHERE elementId(x) IN __eids RETURN p
///   UNION
///   ...
/// }
/// WITH DISTINCT p
/// ```
///
/// `max_hops` truncates each path so users keep the `fan_out_max_hops`
/// safety knob.
// Indexing/slicing below is bounds-checked inline (depth <= hops.len()).
#[allow(clippy::indexing_slicing)]
pub(crate) fn build_projection_call_block(
    primary_label: &str,
    paths: &[ProjectionPath],
    max_hops: usize,
) -> String {
    // First branch — primary itself in $eids. Cheap indexed lookup.
    let mut branches: Vec<String> = vec![format!(
        "  WITH __eids\n  \
         MATCH (p:`{label}`) WHERE elementId(p) IN __eids\n  \
         RETURN p",
        label = primary_label,
    )];

    if max_hops > 0 {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for path in paths {
            let bound = path.hops.len().min(max_hops);
            for depth in 1..=bound {
                let pattern = render_pattern(&path.hops[..depth]);
                let branch = format!(
                    "  WITH __eids\n  \
                     MATCH (p:`{label}`){pattern}(x) WHERE elementId(x) IN __eids\n  \
                     RETURN p",
                    label = primary_label,
                );
                // Two user MATCHes can share the same prefix (e.g.
                // depth-1 HAS_BOOK from two separate WITH
                // clauses). De-dupe so we don't bloat the CALL block.
                if seen.insert(branch.clone()) {
                    branches.push(branch);
                }
            }
        }
    }

    let union_body = branches.join("\n  UNION\n");
    format!("WITH $eids AS __eids\nCALL {{\n{union_body}\n}}\nWITH DISTINCT p\n")
}

/// Render the inline path between `(p)` and the trailing `(x)`.
fn render_pattern(hops: &[Hop]) -> String {
    let mut out = String::new();
    for (i, hop) in hops.iter().enumerate() {
        let type_list = hop.rel_types.iter().cloned().collect::<Vec<_>>().join("|");
        let body = format!("[:{}]", type_list);
        match hop.direction {
            Direction::Out => out.push_str(&format!("-{}->", body)),
            Direction::In => out.push_str(&format!("<-{}-", body)),
            Direction::Undirected => out.push_str(&format!("-{}-", body)),
        }
        if i + 1 < hops.len() {
            out.push_str("()");
        }
    }
    out
}

/// Find every `(p)` or `(p:...)` anchor in the (string-literal-stripped)
/// Cypher.
// Hand-written byte scanner; every index is guarded by an `i < len` check.
#[allow(clippy::indexing_slicing)]
fn find_primary_anchors(s: &str) -> Vec<usize> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'(' {
            i += 1;
            continue;
        }
        // Skip whitespace between '(' and the identifier.
        let mut j = i + 1;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        // Standalone `p` followed by `)` or `:` (with optional whitespace).
        if j < bytes.len() && bytes[j] == b'p' {
            let mut k = j + 1;
            // Allow `( p )` or `( p :Label )`.
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && (bytes[k] == b')' || bytes[k] == b':') {
                out.push(i);
            }
        }
        i += 1;
    }
    out
}

/// Walk forward from a `(p)` anchor, parsing a chain of typed hops
/// until the chain breaks. Returns `Ok(None)` if the anchor has no
/// hops (e.g. `WITH p RETURN p`) — common in property-only projections.
// Hand-written byte scanner; every index is guarded by an `i < len` check.
#[allow(clippy::indexing_slicing)]
fn extract_chain(s: &str, bytes: &[u8], start: usize) -> Result<Option<ProjectionPath>, String> {
    // Skip past the anchor `(p)` / `(p:Label)`.
    let mut i = start;
    let mut depth = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'(' {
            depth += 1;
        }
        if bytes[i] == b')' {
            depth -= 1;
            if depth == 0 {
                i += 1;
                break;
            }
        }
        i += 1;
    }

    let mut hops: Vec<Hop> = Vec::new();

    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        // Direction prefix: '<-' or '-'. Anything else ends the chain.
        let prefix_in = bytes[i] == b'<' && i + 1 < bytes.len() && bytes[i + 1] == b'-';
        let prefix_dash = bytes[i] == b'-';
        if !prefix_in && !prefix_dash {
            break;
        }
        if prefix_in {
            i += 2;
        } else {
            i += 1;
        }

        // Rel body `[...]`.
        if i >= bytes.len() || bytes[i] != b'[' {
            // A bare `(p)-(x)` isn't valid Cypher; treat as chain break.
            break;
        }
        let body_start = i + 1;
        let mut body_end = body_start;
        while body_end < bytes.len() && bytes[body_end] != b']' {
            body_end += 1;
        }
        if body_end >= bytes.len() {
            return Err("unterminated relationship pattern '['".to_owned());
        }
        let body = &s[body_start..body_end];
        i = body_end + 1;

        // Suffix: '->' or '-' (the latter is undirected or the back end of an `<-` prefix).
        let mut suffix_out = false;
        if i < bytes.len() && bytes[i] == b'-' {
            i += 1;
            if i < bytes.len() && bytes[i] == b'>' {
                suffix_out = true;
                i += 1;
            }
        } else {
            return Err(format!(
                "rel pattern '[{}]' not followed by '-' / '->' (malformed chain)",
                body.trim()
            ));
        }

        let direction = match (prefix_in, suffix_out) {
            (true, false) => Direction::In,
            (false, true) => Direction::Out,
            (false, false) => Direction::Undirected,
            (true, true) => {
                return Err(format!(
                    "rel pattern '<-[{}]->' is both in- and out-directed",
                    body.trim()
                ));
            }
        };

        let rel_types = parse_rel_types(body)?;
        hops.push(Hop {
            direction,
            rel_types,
        });

        // Target node `(...)`. Skip past it; we don't care about its label
        // for projection purposes (the rel type is sufficient).
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'(' {
            break;
        }
        let mut pdepth = 0usize;
        while i < bytes.len() {
            if bytes[i] == b'(' {
                pdepth += 1;
            }
            if bytes[i] == b')' {
                pdepth -= 1;
                if pdepth == 0 {
                    i += 1;
                    break;
                }
            }
            i += 1;
        }
    }

    if hops.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ProjectionPath { hops }))
    }
}

/// Parse the body of a `[...]` rel pattern, returning the set of rel
/// types. Errors when the body is anonymous, untyped, or
/// variable-length — those can't be inverted into a fixed typed walk.
fn parse_rel_types(body: &str) -> Result<BTreeSet<String>, String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Err("anonymous rel '-[]-'".to_owned());
    }
    if trimmed.contains('*') {
        return Err(format!("variable-length rel pattern '[{}]'", trimmed));
    }
    // `var:TYPE` or `:TYPE` — split on the first ':'.
    let after_colon = match trimmed.find(':') {
        Some(idx) => &trimmed[idx + 1..],
        None => {
            return Err(format!("untyped rel '[{}]' (no ':TYPE')", trimmed));
        }
    };
    let mut types = BTreeSet::new();
    for raw in after_colon.split('|') {
        let t = raw.trim();
        if t.is_empty() {
            continue;
        }
        // A type may be followed by a property map `{...}` or a WHERE
        // clause — strip from the first whitespace / `{`.
        let end = t
            .find(|c: char| c.is_whitespace() || c == '{')
            .unwrap_or(t.len());
        let t_clean = t[..end].trim();
        if !t_clean.is_empty() {
            types.insert(t_clean.to_owned());
        }
    }
    if types.is_empty() {
        return Err(format!(
            "rel pattern '[{}]' has no type names after ':'",
            trimmed
        ));
    }
    Ok(types)
}

/// Mirror of [`super::denormalize::strip_string_literals`] — duplicated
/// to keep this module independent. Replaces `"..."` / `'...'` bodies
/// with a single space so token boundaries survive but keywords
/// (`CREATE`, etc.) inside string literals can't false-positive.
fn strip_string_literals(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' || c == '\'' {
            let quote = c;
            while let Some(c2) = chars.next() {
                if c2 == '\\' {
                    chars.next();
                    continue;
                }
                if c2 == quote {
                    break;
                }
            }
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    fn types(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    fn ok_paths(cy: &str) -> Vec<ProjectionPath> {
        match extract_projection_paths(cy) {
            ProjectionExtract::Paths(p) => p,
            ProjectionExtract::Unsupported { reason } => {
                panic!("expected Paths, got Unsupported: {reason}")
            }
        }
    }

    #[test]
    fn extract_single_out_hop() {
        let cy = "OPTIONAL MATCH (p)-[hn:HAS_NAME]->(name:Name)";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].hops.len(), 1);
        assert_eq!(paths[0].hops[0].direction, Direction::Out);
        assert_eq!(paths[0].hops[0].rel_types, types(&["HAS_NAME"]));
    }

    #[test]
    fn extract_chained_two_hop_out_path() {
        let cy = "OPTIONAL MATCH (p)-[:HAS_BOOK]->(rep:Book)-[:IN_GENRE]->(ra:Genre)";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].hops.len(), 2);
        assert_eq!(paths[0].hops[0].rel_types, types(&["HAS_BOOK"]));
        assert_eq!(paths[0].hops[1].direction, Direction::Out);
        assert_eq!(paths[0].hops[1].rel_types, types(&["IN_GENRE"]));
    }

    #[test]
    fn extract_mixed_direction_chain() {
        // The agent chain: out-out-in-in.
        let cy = "OPTIONAL MATCH (p)-[hrep:HAS_BOOK]->(rep:Book)<-[at:REVIEWS]-(asn:Review)<-[ha:HAS_REVIEW]-(agent:Author)";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 1);
        let h = &paths[0].hops;
        assert_eq!(h.len(), 3);
        assert_eq!(h[0].direction, Direction::Out);
        assert_eq!(h[0].rel_types, types(&["HAS_BOOK"]));
        assert_eq!(h[1].direction, Direction::In);
        assert_eq!(h[1].rel_types, types(&["REVIEWS"]));
        assert_eq!(h[2].direction, Direction::In);
        assert_eq!(h[2].rel_types, types(&["HAS_REVIEW"]));
    }

    #[test]
    fn extract_rel_alternation() {
        let cy = "OPTIONAL MATCH (p)-[hn:HAS_NAME|ALT_NAME]->(n)";
        let paths = ok_paths(cy);
        assert_eq!(paths[0].hops[0].rel_types, types(&["HAS_NAME", "ALT_NAME"]));
    }

    #[test]
    fn reanchored_chain_on_intermediate_var_returns_unsupported() {
        // `(rep)` is bound from the (p) chain, then a SECOND match walks a
        // relationship from `rep` — that hop never starts at (p), so a change
        // to its far node would silently miss recompose. Must fall back.
        let cy = "OPTIONAL MATCH (p)-[:HAS_BOOK]->(rep:Book)\n\
                  OPTIONAL MATCH (rep)-[:IN_GENRE]->(ra:Genre)\n\
                  RETURN elementId(p) AS primaryEid, { ra: ra.v } AS doc";
        match extract_projection_paths(cy) {
            ProjectionExtract::Unsupported { reason } => {
                assert!(reason.contains("re-anchors"), "got: {reason}");
            }
            ProjectionExtract::Paths(p) => {
                panic!("re-anchored chain must be Unsupported, got {p:?}")
            }
        }
    }

    #[test]
    fn reanchored_bracketless_rel_returns_unsupported() {
        // The re-anchored second hop is BRACKETLESS (no [:TYPE]). extract_chain
        // can never capture an untyped hop, so without counting bracketless
        // rels the guard would miss this re-anchor and wrongly stay in
        // projection mode → stale doc.
        let cy = "OPTIONAL MATCH (p)-[:HAS_BOOK]->(rep:Book)\n\
                  OPTIONAL MATCH (rep)-->(x)\n\
                  RETURN elementId(p) AS primaryEid, {} AS doc";
        match extract_projection_paths(cy) {
            ProjectionExtract::Unsupported { reason } => {
                assert!(reason.contains("re-anchors"), "got: {reason}");
            }
            ProjectionExtract::Paths(p) => {
                panic!("bracketless re-anchor must be Unsupported, got {p:?}")
            }
        }
    }

    #[test]
    fn typed_chain_with_trailing_node_not_miscounted_as_bracketless() {
        // Negative control: a clean typed two-hop chain has node parens
        // adjacent to bracketed rels (`)-[`), which must NOT be counted as
        // bracketless hops — counts stay balanced, so it remains projection.
        let cy = "OPTIONAL MATCH (p)-[:A]->(m:M)-[:B]->(n:N)";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].hops.len(), 2);
    }

    #[test]
    fn contiguous_multi_hop_chain_is_not_flagged_as_reanchor() {
        // Negative control: the SAME two hops written as ONE contiguous chain
        // from (p) is fully captured and must stay in projection mode.
        let cy = "OPTIONAL MATCH (p)-[:HAS_BOOK]->(rep:Book)-[:IN_GENRE]->(ra:Genre)";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].hops.len(), 2);
    }

    #[test]
    fn two_separate_chains_both_anchored_at_p_are_ok() {
        // Two independent (p)-anchored chains in separate MATCHes: both hops
        // are anchored at p, so counts match — stays in projection mode.
        let cy = "OPTIONAL MATCH (p)-[:HAS_NAME]->(n)\n\
                  OPTIONAL MATCH (p)-[:HAS_DEPARTMENT]->(d)\n\
                  RETURN elementId(p) AS primaryEid, {} AS doc";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 2);
        assert_eq!(paths.iter().map(|p| p.hops.len()).sum::<usize>(), 2);
    }

    #[test]
    fn property_only_projection_with_list_index_is_not_miscounted() {
        // `coll[0]` uses square brackets but is NOT a relationship hop (not
        // preceded by `-`), so a pure-property projection stays supported.
        let cy = "WITH p\nRETURN elementId(p) AS primaryEid, { first: p.names[0] } AS doc";
        let paths = ok_paths(cy);
        assert!(paths.iter().all(|p| p.hops.is_empty()));
    }

    #[test]
    fn anonymous_rel_returns_unsupported() {
        let cy = "OPTIONAL MATCH (p)-[]->(x)";
        match extract_projection_paths(cy) {
            ProjectionExtract::Unsupported { reason } => assert!(reason.contains("anonymous")),
            ProjectionExtract::Paths(_) => panic!("expected Unsupported"),
        }
    }

    #[test]
    fn variable_length_rel_returns_unsupported() {
        let cy = "OPTIONAL MATCH (p)-[*1..3]-(x)";
        match extract_projection_paths(cy) {
            ProjectionExtract::Unsupported { reason } => {
                assert!(reason.contains("variable-length"))
            }
            ProjectionExtract::Paths(_) => panic!("expected Unsupported"),
        }
    }

    #[test]
    fn untyped_rel_returns_unsupported() {
        // `-[hn]-` with a variable but no `:TYPE` is untyped.
        let cy = "OPTIONAL MATCH (p)-[hn]->(x)";
        match extract_projection_paths(cy) {
            ProjectionExtract::Unsupported { reason } => assert!(reason.contains("no ':TYPE'")),
            ProjectionExtract::Paths(_) => panic!("expected Unsupported"),
        }
    }

    #[test]
    fn no_anchor_returns_empty_paths() {
        let cy = "WITH 1 AS x RETURN x";
        let paths = ok_paths(cy);
        assert!(paths.is_empty());
    }

    #[test]
    fn pure_property_projection_returns_empty() {
        // `(p)` appears but no hop follows.
        let cy = "WITH p RETURN elementId(p) AS primaryEid, p AS doc";
        let paths = ok_paths(cy);
        // Anchor is present but no hop chain — extractor returns
        // Ok(None) for each, which is dropped.
        assert!(paths.is_empty());
    }

    #[test]
    fn quoted_anonymous_rel_inside_string_does_not_break() {
        // The string literal contains `(p)-[]-(x)` but it's inside quotes.
        let cy = "OPTIONAL MATCH (p)-[:HAS_NAME]->(n) WHERE n.x = \"(p)-[]-(x)\" RETURN p";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].hops[0].rel_types, types(&["HAS_NAME"]));
    }

    #[test]
    fn multiple_p_anchors_extract_independent_chains() {
        let cy = "
            OPTIONAL MATCH (p)-[:HAS_NAME]->(n)
            OPTIONAL MATCH (p)-[:HAS_ROLE]->(r)
        ";
        let paths = ok_paths(cy);
        assert_eq!(paths.len(), 2);
        let mut got: Vec<_> = paths
            .iter()
            .flat_map(|p| p.hops.iter().flat_map(|h| h.rel_types.iter()))
            .cloned()
            .collect();
        got.sort();
        assert_eq!(got, vec!["HAS_NAME".to_owned(), "HAS_ROLE".to_owned()]);
    }

    #[test]
    fn build_call_block_includes_primary_anchor_branch() {
        let path = ProjectionPath {
            hops: vec![Hop {
                direction: Direction::Out,
                rel_types: types(&["HAS_NAME"]),
            }],
        };
        let block = build_projection_call_block("Author", &[path], 2);
        assert!(block.starts_with("WITH $eids AS __eids\nCALL {"));
        assert!(block.contains("MATCH (p:`Author`) WHERE elementId(p) IN __eids"));
        assert!(block.contains("MATCH (p:`Author`)-[:HAS_NAME]->(x) WHERE elementId(x) IN __eids"));
        assert!(block.trim_end().ends_with("WITH DISTINCT p"));
    }

    #[test]
    fn build_call_block_emits_one_branch_per_depth() {
        // (p)-[:A]->()-[:B]->()-[:C]->()
        let path = ProjectionPath {
            hops: vec![
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["A"]),
                },
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["B"]),
                },
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["C"]),
                },
            ],
        };
        let block = build_projection_call_block("Author", &[path], 3);
        assert!(block.contains("MATCH (p:`Author`)-[:A]->(x)"));
        assert!(block.contains("MATCH (p:`Author`)-[:A]->()-[:B]->(x)"));
        assert!(block.contains("MATCH (p:`Author`)-[:A]->()-[:B]->()-[:C]->(x)"));
    }

    #[test]
    fn build_call_block_respects_max_hops_bound() {
        let path = ProjectionPath {
            hops: vec![
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["A"]),
                },
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["B"]),
                },
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["C"]),
                },
            ],
        };
        let block = build_projection_call_block("Author", &[path], 2);
        assert!(block.contains("[:A]->(x)"));
        assert!(block.contains("[:A]->()-[:B]->(x)"));
        assert!(!block.contains("[:C]"));
    }

    #[test]
    fn build_call_block_zero_max_hops_emits_only_primary_branch() {
        let path = ProjectionPath {
            hops: vec![Hop {
                direction: Direction::Out,
                rel_types: types(&["A"]),
            }],
        };
        let block = build_projection_call_block("Author", &[path], 0);
        // Only the primary-anchor branch; no UNION needed.
        assert!(block.contains("MATCH (p:`Author`) WHERE elementId(p) IN __eids"));
        assert!(!block.contains("UNION"));
        assert!(!block.contains("[:A]"));
    }

    #[test]
    fn build_call_block_dedupes_repeated_paths() {
        let path_a = ProjectionPath {
            hops: vec![Hop {
                direction: Direction::Out,
                rel_types: types(&["HAS_BOOK"]),
            }],
        };
        let path_b = ProjectionPath {
            hops: vec![
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["HAS_BOOK"]),
                },
                Hop {
                    direction: Direction::Out,
                    rel_types: types(&["IN_GENRE"]),
                },
            ],
        };
        let block = build_projection_call_block("Author", &[path_a, path_b], 3);
        let occurrences = block
            .match_indices("MATCH (p:`Author`)-[:HAS_BOOK]->(x) WHERE")
            .count();
        assert_eq!(occurrences, 1);
        assert!(block.contains("MATCH (p:`Author`)-[:HAS_BOOK]->()-[:IN_GENRE]->(x)"));
    }

    #[test]
    fn rel_with_property_map_parses_type() {
        // `[hn:HAS_NAME {kind:'display'}]` — strip the property map.
        let cy = "OPTIONAL MATCH (p)-[hn:HAS_NAME {kind: 'display'}]->(n)";
        let paths = ok_paths(cy);
        assert_eq!(paths[0].hops[0].rel_types, types(&["HAS_NAME"]));
    }

    #[test]
    fn full_yaml_cypher_extracts_all_paths() {
        // The complete user cypher from a full denormalize spec.
        let cy = r#"
            WITH p, datetime() AS now
            OPTIONAL MATCH (p)-[hn:HAS_NAME]->(name:Name {type: "Display"})
            OPTIONAL MATCH (p)-[hrole:HAS_ROLE]->(role:Role)
            OPTIONAL MATCH (p)-[hd:HAS_DEPARTMENT]->(d:Department)
            OPTIONAL MATCH (p)-[li:LOCATED_IN]->(loc:Location)
            OPTIONAL MATCH (p)-[bi:BASED_IN]->(basedLoc:Location)
            OPTIONAL MATCH (p)-[:HAS_PUBLISH_STATUS]->(pub:PublishStatus)
            OPTIONAL MATCH (p)-[hrep:HAS_BOOK]->(rep:Book)-[repas:IN_GENRE]->(rarea:Genre)
            OPTIONAL MATCH (p)-[hrep2:HAS_BOOK]->(rep2:Book)<-[at:REVIEWS]-(asn:Review)<-[ha:HAS_REVIEW]-(agent:Author)
        "#;
        let paths = ok_paths(cy);
        // 8 distinct user MATCH chains starting at (p).
        assert_eq!(paths.len(), 8);
        let block = build_projection_call_block("Author", &paths, 3);
        // Should contain depth-1 branches for each direct hop type.
        for rel in &[
            "HAS_NAME",
            "HAS_ROLE",
            "HAS_DEPARTMENT",
            "LOCATED_IN",
            "BASED_IN",
            "HAS_PUBLISH_STATUS",
            "HAS_BOOK",
        ] {
            assert!(
                block.contains(&format!("MATCH (p:`Author`)-[:{}]->(x)", rel)),
                "missing depth-1 branch for {}",
                rel
            );
        }
        // And the depth-3 inverted chain from the agent traversal.
        assert!(
            block.contains("MATCH (p:`Author`)-[:HAS_BOOK]->()<-[:REVIEWS]-()<-[:HAS_REVIEW]-(x)")
        );
        // No variable-length path-scan in the generated block.
        assert!(!block.contains("[*"));
    }
}
