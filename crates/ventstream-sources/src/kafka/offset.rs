//! Sink-gated offset tracking.
//!
//! Each consumed message gets a process-local monotonic consume-seq (stamped as
//! the internal `ventstream.cdc.ack_seq` header so the dispatcher's watermark
//! machinery picks it up). We remember `seq -> (topic, partition, offset)` and only return
//! offsets for commit once the sink has durably written everything up to that
//! seq — `durable_seq` comes from the shared `sink_progress` watermark.
//!
//! Messages that produce no event (null-value tombstones, skips) are recorded
//! as `needs_sink = false`: they're durable immediately and never block the
//! prefix. The walk advances the *global* contiguous done-prefix, so a
//! partition is never committed past an event the sink hasn't confirmed.

use std::collections::{BTreeMap, HashMap};

struct Slot {
    topic: String,
    partition: i32,
    offset: i64,
    needs_sink: bool,
}

/// Tracks consume-seqs and the offsets safe to commit.
#[derive(Default)]
pub(crate) struct OffsetTracker {
    next_seq: u64,
    inflight: BTreeMap<u64, Slot>,
}

impl OffsetTracker {
    pub(crate) fn new() -> Self {
        Self {
            next_seq: 1, // seqs are 1-based so the lsn header is always > 0
            inflight: BTreeMap::new(),
        }
    }

    /// Record a consumed message and return its consume-seq. `needs_sink` is
    /// true when the message produced an event that must be sink-durable
    /// before its offset can be committed.
    pub(crate) fn record(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        needs_sink: bool,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.inflight.insert(
            seq,
            Slot {
                topic: topic.to_owned(),
                partition,
                offset,
                needs_sink,
            },
        );
        seq
    }

    /// Drain the contiguous done-prefix (every slot that is skip-only or has
    /// `seq <= durable_seq`) and return the next offset to commit per
    /// `(topic, partition)`. Kafka commits the offset of the *next* message to
    /// read, hence `offset + 1`.
    pub(crate) fn committable(&mut self, durable_seq: u64) -> HashMap<(String, i32), i64> {
        let mut commit: HashMap<(String, i32), i64> = HashMap::new();
        while let Some((&seq, slot)) = self.inflight.iter().next() {
            let done = !slot.needs_sink || seq <= durable_seq;
            if !done {
                break;
            }
            let key = (slot.topic.clone(), slot.partition);
            let next = slot.offset + 1;
            let entry = commit.entry(key).or_insert(next);
            *entry = (*entry).max(next);
            self.inflight.remove(&seq);
        }
        commit
    }

    #[cfg(test)]
    pub(crate) fn inflight_len(&self) -> usize {
        self.inflight.len()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn seqs_are_one_based_and_monotonic() {
        let mut t = OffsetTracker::new();
        assert_eq!(t.record("x", 0, 0, true), 1);
        assert_eq!(t.record("x", 0, 1, true), 2);
    }

    #[test]
    fn commits_only_durable_prefix() {
        let mut t = OffsetTracker::new();
        let _s1 = t.record("t", 0, 10, true); // seq 1
        let _s2 = t.record("t", 0, 11, true); // seq 2
        let _s3 = t.record("t", 0, 12, true); // seq 3

        // sink durable up to seq 2 → commit offset 12 (11 + 1); seq 3 stays.
        let c = t.committable(2);
        assert_eq!(c.get(&("t".to_owned(), 0)), Some(&12));
        assert_eq!(t.inflight_len(), 1);

        // now seq 3 durable → commit 13.
        let c = t.committable(3);
        assert_eq!(c.get(&("t".to_owned(), 0)), Some(&13));
        assert_eq!(t.inflight_len(), 0);
    }

    #[test]
    fn skipped_messages_do_not_block_prefix() {
        let mut t = OffsetTracker::new();
        t.record("t", 0, 10, true); // seq 1, event
        t.record("t", 0, 11, false); // seq 2, tombstone/skip
        t.record("t", 0, 12, false); // seq 3, skip

        // even with nothing sink-durable, skip-only tail past the durable
        // event commits; durable_seq 1 covers seq 1.
        let c = t.committable(1);
        assert_eq!(c.get(&("t".to_owned(), 0)), Some(&13)); // 12 + 1
        assert_eq!(t.inflight_len(), 0);
    }

    #[test]
    fn a_pending_event_blocks_following_skips() {
        let mut t = OffsetTracker::new();
        t.record("t", 0, 10, true); // seq 1 event, NOT durable yet
        t.record("t", 0, 11, false); // seq 2 skip

        // durable_seq 0 → seq 1 not done → nothing commits.
        let c = t.committable(0);
        assert!(c.is_empty());
        assert_eq!(t.inflight_len(), 2);
    }

    #[test]
    fn highest_offset_per_partition_wins() {
        let mut t = OffsetTracker::new();
        t.record("t", 0, 10, true);
        t.record("t", 1, 50, true); // different partition
        t.record("t", 0, 11, true);

        let c = t.committable(3);
        assert_eq!(c.get(&("t".to_owned(), 0)), Some(&12)); // 11 + 1
        assert_eq!(c.get(&("t".to_owned(), 1)), Some(&51)); // 50 + 1
    }
}
