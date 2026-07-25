// Demo source graph: a small product catalog.
//
// Run once after Neo4j is up (the runbook pipes this through
// cypher-shell). Shape mirrors a typical denormalize demo (primary +
// hot shared lookup + multi-hop) in a different domain:
//
//   Product            → primary
//   IN_CATEGORY        → Category   (HOT: ~4 categories shared by all products)
//   SUPPLIED_BY        → Supplier → LOCATED_IN → Region   (multi-hop, 2 hops)
//   HAS_TAG            → Tag        (small set)
//
// No temporal (fromDate) gating here — kept simple so created edges
// show up immediately. A production spec might add validity windows; see
// the Neo4j source guide for that pattern.

// Clean slate (safe to re-run).
MATCH (n) WHERE n:Product OR n:Category OR n:Supplier OR n:Region OR n:Tag
DETACH DELETE n;

// ── categories (the hot shared nodes) ───────────────────────────────
UNWIND [
  {id:'cat-electronics', name:'Electronics'},
  {id:'cat-books',       name:'Books'},
  {id:'cat-home',        name:'Home & Kitchen'},
  {id:'cat-toys',        name:'Toys & Games'}
] AS c CREATE (:Category {id:c.id, name:c.name});

// ── regions (hot) + suppliers (1 hop further) ───────────────────────
UNWIND [
  {id:'reg-na', name:'North America'},
  {id:'reg-eu', name:'Europe'},
  {id:'reg-apac', name:'Asia Pacific'}
] AS r CREATE (:Region {id:r.id, name:r.name});

UNWIND range(1, 20) AS i
MATCH (r:Region {id: ['reg-na','reg-eu','reg-apac'][i % 3]})
CREATE (s:Supplier {id:'sup-'+toString(i), name:'Supplier '+toString(i)})-[:LOCATED_IN]->(r);

// ── tags ─────────────────────────────────────────────────────────────
UNWIND ['new','sale','bestseller','clearance','eco'] AS t
CREATE (:Tag {id:'tag-'+t, name:t});

// ── 2,000 products wired to a category, a supplier, and a tag ────────
// Enough that a single Category is referenced by ~500 products — the
// hot-endpoint scenario.
UNWIND range(1, 2000) AS i
MATCH (cat:Category {id: ['cat-electronics','cat-books','cat-home','cat-toys'][i % 4]})
MATCH (sup:Supplier {id: 'sup-'+toString(1 + (i % 20))})
MATCH (tag:Tag {id: ['tag-new','tag-sale','tag-bestseller','tag-clearance','tag-eco'][i % 5]})
CREATE (p:Product {id:'prod-'+toString(i), name:'Product '+toString(i), price: 5 + (i % 200)})
CREATE (p)-[:IN_CATEGORY]->(cat)
CREATE (p)-[:SUPPLIED_BY]->(sup)
CREATE (p)-[:HAS_TAG]->(tag);
