//! Neo4j CDC source spike.
//!
//! Goal: prove the snapshot-bootstrap → live-tail pipeline end-to-end
//! against a real Neo4j 5.26 Enterprise instance:
//!   1. On cold start (no persisted cursor), capture `db.cdc.current()`
//!      *before* the scan. Then paginate every node and relationship,
//!      emit each as a synthetic `insert` event in the CDC live shape.
//!      Persist the captured cursor; tail resumes from there.
//!   2. On warm start, skip bootstrap and resume from persisted cursor.
//!   3. Tail loop: poll `db.cdc.query(cursor)`, parse events, persist
//!      the latest cursor after each batch.
//!   4. Idle cursor advance: after sustained no-events polls, advance
//!      cursor to current() to avoid log-rotation aging.
//!
//! Why bootstrap matters: live CDC only sees mutations *after* the
//! cursor we start from. Without bootstrap, any pre-existing graph state
//! never reaches the downstream sink. The PG source has the same
//! contract (snapshot from `START_REPLICATION ... USE_SNAPSHOT`).

use anyhow::{Context, Result};
use neo4rs::{ConfigBuilder, Graph, query, BoltType};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

const URI: &str = "bolt://127.0.0.1:7687";
const USER: &str = "neo4j";
const PASS: &str = "ventstream-spike";
const CURSOR_FILE: &str = "./cursor.txt";
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const RUN_FOR: Duration = Duration::from_secs(30);
/// Bootstrap pagination size. Conservative for the spike; the real
/// source will make this a config knob and keyset-paginate by elementId
/// instead of using SKIP/LIMIT (which is O(skip) on each page).
const BOOTSTRAP_BATCH: i64 = 100;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ConfigBuilder::default()
        .uri(URI)
        .user(USER)
        .password(PASS)
        .db("neo4j")
        .build()
        .context("building neo4rs config")?;
    let graph = Graph::connect(config).await.context("connecting to neo4j")?;
    println!("connected to neo4j @ {URI}");

    let cursor_path = PathBuf::from(CURSOR_FILE);
    let mut cursor = if cursor_path.exists() {
        let c = fs::read_to_string(&cursor_path)?.trim().to_string();
        println!("warm start: resuming from persisted cursor: {}…", &c[..30.min(c.len())]);
        c
    } else {
        bootstrap(&graph, &cursor_path).await?
    };

    println!("polling every {:?} for {:?}", POLL_INTERVAL, RUN_FOR);
    let start = std::time::Instant::now();
    let mut total_events = 0u64;
    let mut idle_polls = 0u32;

    while start.elapsed() < RUN_FOR {
        let events = poll_once(&graph, &cursor).await?;

        if events.is_empty() {
            idle_polls += 1;
            // Cursor-aging mitigation: advance to current after sustained
            // idle. The plan called this out — verify the helper works.
            if idle_polls % 20 == 0 {
                let now_cursor = fetch_current_cursor(&graph).await?;
                if now_cursor != cursor {
                    println!("  idle advance: cursor → {}…", &now_cursor[..30.min(now_cursor.len())]);
                    cursor = now_cursor;
                    fs::write(&cursor_path, &cursor)?;
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        }
        idle_polls = 0;

        for ev in &events {
            print_event(ev);
            total_events += 1;
        }

        // Persist the LAST cursor (events are returned in order).
        cursor = events.last().unwrap().id.clone();
        fs::write(&cursor_path, &cursor)?;
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    println!("\n=== spike summary ===");
    println!("total events:  {}", total_events);
    println!("final cursor:  {}…", &cursor[..30.min(cursor.len())]);
    println!("idle polls:    {}", idle_polls);
    Ok(())
}

/// Minimal VentStream-shaped event derived from a Neo4j CDC row.
#[derive(Debug)]
struct CdcEvent {
    /// The opaque cursor string — what we persist to resume.
    id: String,
    tx_id: i64,
    seq: i64,
    /// Subject in the VentStream-style: `neo4j.{label_or_reltype}.{op}`.
    subject: String,
    /// Element ID — stable across restarts; what we'd use as the doc_id
    /// half of `{table}:{pk}` in the OS sink.
    element_id: String,
    /// JSON payload of the event (state, labels, endpoints, etc.).
    payload: serde_json::Value,
}

async fn fetch_current_cursor(graph: &Graph) -> Result<String> {
    let mut rows = graph
        .execute(query("CALL db.cdc.current() YIELD id RETURN id"))
        .await?;
    let row = rows.next().await?.context("db.cdc.current returned no row")?;
    let id: String = row.get("id")?;
    Ok(id)
}

/// Cold-start snapshot. Captures the CDC cursor *before* the scan so any
/// concurrent writes during pagination are picked up by the subsequent
/// tail (deduplicated downstream by deterministic elementId-keyed doc IDs).
async fn bootstrap(graph: &Graph, cursor_path: &PathBuf) -> Result<String> {
    println!("cold start: bootstrap mode");

    // Phase 1 — cursor BEFORE the scan.
    let cursor = fetch_current_cursor(graph).await?;
    println!("  cursor captured pre-scan: {}…", &cursor[..30.min(cursor.len())]);

    // Phase 2 — discover what to scan.
    let labels = fetch_labels(graph).await?;
    let reltypes = fetch_reltypes(graph).await?;
    println!(
        "  discovered {} labels [{}] · {} reltypes [{}]",
        labels.len(),
        labels.join(", "),
        reltypes.len(),
        reltypes.join(", "),
    );

    let mut total = 0u64;

    // Phase 3 — scan nodes per label.
    for label in &labels {
        let n = scan_nodes(graph, label).await?;
        println!("  scan {}: {} nodes", label, n);
        total += n;
    }

    // Phase 4 — scan relationships per reltype.
    for rt in &reltypes {
        let n = scan_relationships(graph, rt).await?;
        println!("  scan :{}: {} relationships", rt, n);
        total += n;
    }

    // Phase 5 — emit a "snapshot-complete" sentinel. In the real source
    // this drives the join engine out of bootstrap mode (dumps redb state,
    // re-enables per-row persistence). For the spike we just log it.
    println!("  emit sentinel: snapshot-complete");
    println!("  bootstrap done: {} synthetic insert events", total);

    // Persist captured cursor — tail resumes from here.
    fs::write(cursor_path, &cursor)?;
    Ok(cursor)
}

async fn fetch_labels(graph: &Graph) -> Result<Vec<String>> {
    let mut rows = graph
        .execute(query("CALL db.labels() YIELD label RETURN label ORDER BY label"))
        .await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(row.get::<String>("label")?);
    }
    Ok(out)
}

async fn fetch_reltypes(graph: &Graph) -> Result<Vec<String>> {
    let mut rows = graph
        .execute(query(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType"
        ))
        .await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(row.get::<String>("relationshipType")?);
    }
    Ok(out)
}

async fn scan_nodes(graph: &Graph, label: &str) -> Result<u64> {
    let mut emitted = 0u64;
    let mut skip = 0i64;
    loop {
        // ORDER BY elementId(n) gives stable iteration across pages even
        // under concurrent writes. SKIP/LIMIT is fine for spike-scale; the
        // real source will keyset-paginate by elementId for O(1) per page.
        let cypher = format!(
            "MATCH (n:`{label}`) RETURN \
               elementId(n) AS eid, labels(n) AS labels, properties(n) AS props \
             ORDER BY elementId(n) SKIP $skip LIMIT $batch",
        );
        let mut rows = graph
            .execute(query(&cypher).param("skip", skip).param("batch", BOOTSTRAP_BATCH))
            .await?;
        let mut page_count = 0i64;
        while let Some(row) = rows.next().await? {
            let eid: String = row.get("eid")?;
            let labels_raw: BoltType = row.get("labels")?;
            let props_raw: BoltType = row.get("props")?;
            let synth = synthesize_node_insert_event(&eid, &labels_raw, &props_raw);
            print_synth_event("node ", label, &eid, &synth);
            emitted += 1;
            page_count += 1;
        }
        if page_count < BOOTSTRAP_BATCH {
            break;
        }
        skip += page_count;
    }
    Ok(emitted)
}

async fn scan_relationships(graph: &Graph, reltype: &str) -> Result<u64> {
    let mut emitted = 0u64;
    let mut skip = 0i64;
    loop {
        let cypher = format!(
            "MATCH (a)-[r:`{reltype}`]->(b) RETURN \
               elementId(r) AS eid, type(r) AS rtype, properties(r) AS props, \
               elementId(a) AS start_eid, labels(a) AS start_labels, \
               elementId(b) AS end_eid,   labels(b) AS end_labels \
             ORDER BY elementId(r) SKIP $skip LIMIT $batch",
        );
        let mut rows = graph
            .execute(query(&cypher).param("skip", skip).param("batch", BOOTSTRAP_BATCH))
            .await?;
        let mut page_count = 0i64;
        while let Some(row) = rows.next().await? {
            let eid: String = row.get("eid")?;
            let props_raw: BoltType = row.get("props")?;
            let start_eid: String = row.get("start_eid")?;
            let start_labels_raw: BoltType = row.get("start_labels")?;
            let end_eid: String = row.get("end_eid")?;
            let end_labels_raw: BoltType = row.get("end_labels")?;
            let synth = synthesize_rel_insert_event(
                &eid, reltype, &props_raw,
                &start_eid, &start_labels_raw,
                &end_eid, &end_labels_raw,
            );
            print_synth_event("rel  ", reltype, &eid, &synth);
            emitted += 1;
            page_count += 1;
        }
        if page_count < BOOTSTRAP_BATCH {
            break;
        }
        skip += page_count;
    }
    Ok(emitted)
}

/// Build a synthetic CDC event in the same shape `db.cdc.query` emits
/// for a live node create. This is what the join engine downstream
/// expects; bootstrap and live events flow through the same compose
/// path.
fn synthesize_node_insert_event(
    element_id: &str,
    labels: &BoltType,
    props: &BoltType,
) -> serde_json::Value {
    serde_json::json!({
        "elementId": element_id,
        "eventType": "n",
        "operation": "c",
        "labels": bolt_to_json(labels),
        "keys": {},
        "state": {
            "before": serde_json::Value::Null,
            "after": {
                "labels":     bolt_to_json(labels),
                "properties": bolt_to_json(props),
            },
        },
    })
}

fn synthesize_rel_insert_event(
    element_id: &str,
    rtype: &str,
    props: &BoltType,
    start_eid: &str,
    start_labels: &BoltType,
    end_eid: &str,
    end_labels: &BoltType,
) -> serde_json::Value {
    serde_json::json!({
        "elementId": element_id,
        "eventType": "r",
        "operation": "c",
        "type": rtype,
        "keys": [],
        "start": { "elementId": start_eid, "labels": bolt_to_json(start_labels), "keys": {} },
        "end":   { "elementId": end_eid,   "labels": bolt_to_json(end_labels),   "keys": {} },
        "state": {
            "before": serde_json::Value::Null,
            "after":  { "properties": bolt_to_json(props) },
        },
    })
}

fn print_synth_event(kind: &str, table: &str, eid: &str, payload: &serde_json::Value) {
    let props_preview = payload
        .pointer("/state/after/properties")
        .map(|v| {
            let s = v.to_string();
            if s.len() > 60 { format!("{}…", &s[..57]) } else { s }
        })
        .unwrap_or_else(|| "{}".to_string());
    println!(
        "    {kind} neo4j.{table:<10}.insert  eid={eid:<30}  props={props_preview}",
    );
}

async fn poll_once(graph: &Graph, cursor: &str) -> Result<Vec<CdcEvent>> {
    // `db.cdc.query(cursor)` yields (id, txId, seq, event, metadata).
    let mut rows = graph
        .execute(
            query("CALL db.cdc.query($cursor) YIELD id, txId, seq, event, metadata RETURN id, txId, seq, event")
                .param("cursor", cursor),
        )
        .await?;

    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        let id: String = row.get("id")?;
        let tx_id: i64 = row.get("txId")?;
        let seq: i64 = row.get("seq")?;
        let event_raw: BoltType = row.get("event")?;
        let event_json = bolt_to_json(&event_raw);

        let event_obj = event_json.as_object().context("event not an object")?;
        let event_type = event_obj.get("eventType").and_then(|v| v.as_str()).unwrap_or("?");
        let operation  = event_obj.get("operation").and_then(|v| v.as_str()).unwrap_or("?");
        let element_id = event_obj.get("elementId").and_then(|v| v.as_str()).unwrap_or("").to_string();

        // Subject derivation:
        //   - node:        neo4j.{first_label}.{c|u|d}
        //   - relationship: neo4j.{reltype}.{c|u|d}
        let table = match event_type {
            "n" => event_obj
                .get("labels")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .unwrap_or("UnknownNode")
                .to_string(),
            "r" => event_obj
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("UnknownRel")
                .to_string(),
            _ => format!("UnknownType:{event_type}"),
        };

        let op = match operation {
            "c" => "insert",
            "u" => "update",
            "d" => "delete",
            other => other,
        };
        let subject = format!("neo4j.{table}.{op}");

        out.push(CdcEvent {
            id,
            tx_id,
            seq,
            subject,
            element_id,
            payload: event_json,
        });
    }
    Ok(out)
}

/// Convert a neo4rs BoltType into serde_json::Value so we can hand the
/// payload to downstream sinks unchanged.
fn bolt_to_json(b: &BoltType) -> serde_json::Value {
    use serde_json::{json, Value};
    match b {
        BoltType::Null(_)    => Value::Null,
        BoltType::Boolean(v) => Value::Bool(v.value),
        BoltType::Integer(v) => json!(v.value),
        BoltType::Float(v)   => json!(v.value),
        BoltType::String(v)  => Value::String(v.value.clone()),
        BoltType::List(v) => Value::Array(v.value.iter().map(bolt_to_json).collect()),
        BoltType::Map(v) => {
            let mut m = serde_json::Map::new();
            for (k, val) in v.value.iter() {
                m.insert(k.value.clone(), bolt_to_json(val));
            }
            Value::Object(m)
        }
        BoltType::Bytes(v)    => json!(v.value.to_vec()),
        // CDC delivers nodes/relationships as nested Maps inside `event`,
        // so these branches are defensive. For the spike, debug-format is fine.
        BoltType::Node(n)     => json!(format!("{:?}", n)),
        BoltType::Relation(r) => json!(format!("{:?}", r)),
        BoltType::DateTime(_)
        | BoltType::LocalDateTime(_)
        | BoltType::Date(_)
        | BoltType::Time(_)
        | BoltType::LocalTime(_)
        | BoltType::Duration(_) => json!(format!("{:?}", b)),
        other => json!(format!("{:?}", other)),
    }
}

fn print_event(ev: &CdcEvent) {
    println!(
        "  tx={:<5} seq={}  {:<32}  element={:<20}  cursor={}…",
        ev.tx_id,
        ev.seq,
        ev.subject,
        &ev.element_id[..20.min(ev.element_id.len())],
        &ev.id[..18.min(ev.id.len())],
    );
}
