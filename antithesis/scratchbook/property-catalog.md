---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Property Catalog — Offset Monotonicity

## Summary

Four properties are catalogued, all tied to the customer goal "ensure offsets are monotonically increasing". This pass implements only the two P0 properties (see `focus.md`): **acked-offsets-strictly-increase** and **consumer-offsets-strictly-increase**. They test the goal most directly, from the producer side and the consumer side, and they are the simplest to implement: both are in-memory checks inside one driver loop, with no cross-command state handoff. The other two properties are kept below and marked "Deferred: outside the current focus".

Hard budget for this pass: at most 5 new SDK assertions and 2 new test commands. Every assertion lives in the workload: a Java client using the Antithesis Java SDK.

**Planned assertions (4 of 5):**

| # | Type | Message | Property |
|---|---|---|---|
| 1 | Always | `acked produce offsets strictly increase per partition per producer` | acked-offsets-strictly-increase |
| 2 | Always | `consumer delivers strictly increasing offsets per partition` | consumer-offsets-strictly-increase |
| 3 | Sometimes | `produce acked after a leader change` | anchor for acked-offsets-strictly-increase |
| 4 | Sometimes | `consumer delivered a record after a leader change` | anchor for consumer-offsets-strictly-increase |

One assertion slot is held in reserve.

**Planned test commands (1 of 2):** `parallel_driver_offsets`: one idempotent producer and one `assign()` consumer per driver instance, plus `describeTopics` leader polling. It evaluates assertions 1–4. One command slot is held in reserve. No `finally_` command is needed, because neither P0 property requires a post-quiescence read-back.

**Required SUT config:** RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false` (default), topic `flush.messages=1`, `retention.ms=-1`, `retention.bytes=-1`, producer `acks=all`, `enable.idempotence=true`, consumer `auto.offset.reset=none`.

## Category: Offset assignment and visibility under leader failover

These properties cover (P0 first, deferred after) the produce path (`UnifiedLog.append`), HW propagation and the lagging-HW ListOffsets guard (`Partition.fetchOffsetForTimestamp`), follower truncation (`AbstractFetcherThread`), and the consumer fetch position (`CompletedFetch`). Antithesis matters here because every one of these can only break when a leader change, partition, or pause lands between an append, an HW update and a client read.

### acked-offsets-strictly-increase — Acked produce offsets strictly increase

| | |
|---|---|
| **Priority** | P0 |
| **Status** | Implemented: `antithesis/workload/src/OffsetsDriver.java`, run by `test/v1/offsets/parallel_driver_offsets`. `Always` is checked order-independently: each ack is compared with its nearest acked neighbours by send sequence. The `Sometimes` "produce acked after a leader change" is true when `describeTopics` polling (every 500 ms) saw the partition leader differ from the first one observed. Both were checked locally, including a forced leader restart. |
| **Type** | Safety |
| **Property** | For a single idempotent producer instance writing to one partition, the offsets returned in successful send callbacks strictly increase in send order, and no two distinct records are acked at the same offset. |
| **Invariant** | `Always(offset > lastAckedOffset[partition], "acked produce offsets strictly increase per partition per producer")`, evaluated in each successful callback. Callbacks for one partition fire in order. `Always` fits because this must hold for every ack, not just once. |
| **Antithesis Angle** | Leader partition or pause mid-batch: the producer retries with the same sequence, the new leader either dedups (returns the original offset) or appends fresh. A dedup bug, or an old leader acking after being fenced, shows up as a repeated or lower offset. |
| **Why It Matters** | Producers record acked offsets as positions for exactly-once and audit logic. A reused offset means two records claim one position. |

**Open Questions:**

- Can the idempotent dedup path (`UnifiedLog.java:1261`) fire a callback for a *different* record than the original at an older offset? `(partial: dedup returns the original batch's offsets; that is correct only if the batch is byte-identical, which the producer guarantees for retries of the same batch)`

### consumer-offsets-strictly-increase — Consumer delivery order is strictly increasing

| | |
|---|---|
| **Priority** | P0 |
| **Status** | Implemented: a consumer thread in `antithesis/workload/src/OffsetsDriver.java` (same `parallel_driver_offsets` command). It uses `assign()` on all 3 partitions with `auto.offset.reset=none`. Its one seek is the starting position, chosen at random between `seekToBeginning` and `seekToEnd`. `checkConsumed` evaluates the `Always` per record and with false on `OffsetOutOfRangeException` (which covers `LogTruncationException`). The consumer stops after the first out-of-range. The `Sometimes` "consumer delivered a record after a leader change" reuses the driver's `describeTopics` leader tracking. Menus: max.poll.records {1,10,500}, fetch.max.wait.ms {10,500}. Both assertions were seen firing locally with a forced leader restart; no violation. |
| **Type** | Safety |
| **Property** | A consumer using `assign()` with no explicit seek and `auto.offset.reset=none` sees, per partition, record offsets that strictly increase across all `poll()` calls, and never hits an out-of-range reset. |
| **Invariant** | `Always(rec.offset() > lastSeen[partition] && !outOfRangeObserved, "consumer delivers strictly increasing offsets per partition")`, evaluated per delivered record and in the `OffsetOutOfRangeException` handler (with the condition false). Both sites call one helper method, so this is one assertion. `Always` fits because every delivery must satisfy it. |
| **Antithesis Angle** | The consumer fetches from a leader that is then deposed; the new leader has a lower HW. If the consumer's position is beyond the new leader's view (because the old leader served above-HW data, or the HW regressed), it gets an out-of-range error or re-delivery. |
| **Why It Matters** | Consumer position regression causes silent duplicate processing. A forward jump on reset causes silent data skip. |

**Open Questions:**

- Is `OFFSET_OUT_OF_RANGE` ever legitimate here (for example, retention deleting segments)? The workload must disable time/size retention for the topic so that the answer is "no".

### latest-offset-never-regresses — ListOffsets(LATEST) never decreases

| | |
|---|---|
| **Priority** | Deferred: outside the current focus |
| **Type** | Safety |
| **Property** | Successive successful `ListOffsets(LATEST, read_uncommitted)` results for a partition, observed by one client, never decrease, including across leader changes. |
| **Invariant** | `Always(latest >= maxLatestSeen[partition], "partition latest offset never decreases")`, evaluated after each successful `Admin.listOffsets` call. `OFFSET_NOT_AVAILABLE` / retriable errors are skipped, not asserted. `Always` fits because any regression is a violation. |
| **Antithesis Angle** | Targets the lagging-HW window right after `makeLeader` (`Partition.scala:590-680`). The new leader's HW is stale until followers fetch. The guard at `Partition.scala:1455` must throw `OFFSET_NOT_AVAILABLE` rather than return the stale HW. Timing of the controller epoch bump vs follower fetch vs the client request is exactly what Antithesis explores. |
| **Why It Matters** | Lag monitors and consumers that seek to end use this value. A regression makes committed data look like it disappeared. |

**Open Questions:**

- Does the `leaderEpochStartOffsetOpt` guard cover the case where the new leader's HW is ≥ the epoch start offset but still below the old leader's last-reported HW? Code reading suggests yes: the old leader's HW ≤ the new leader's LEO at epoch start (a committed record is in the ISR), so HW ≥ epochStart implies HW ≥ the old HW. `(partial: argument from code, not verified under faults)`

### acked-offset-record-stable — Acked offset keeps its record

| | |
|---|---|
| **Priority** | Deferred: outside the current focus |
| **Type** | Safety |
| **Property** | Every record acked (acks=all) at partition offset X is, after faults stop and the cluster recovers, readable at exactly offset X with the same key/value. Offsets are never reassigned to different data after truncation or failover. |
| **Invariant** | In `finally_verify_offsets`: read each partition from the beginning to the end, then for each acked `(partition, offset, payload)` recorded by the drivers, `Always(readAt(offset) == payload, "acked record is found at its acked offset")`. Before checking, wait (bounded, ~120s) until each partition has a leader and HW > max acked offset. If no leader appears, log and skip; do not assert. An HW at or below an acked offset with a live leader *is* a violation. `Always` fits because this is a per-record invariant. A missing offset or a mismatch is a failure. |
| **Antithesis Angle** | Exercises epoch-based truncation (`AbstractFetcherThread.scala:209`) and follower append checks (`UnifiedLog.java:1204`) after the leader moves multiple times under partitions and pauses. A divergent log that keeps an un-committed suffix, or truncates a committed one, shows up as wrong or missing data at an acked offset. |
| **Why It Matters** | This is the strongest form of "monotonic offsets": an offset is a permanent name for a record. If that breaks, consumer checkpoints point at the wrong data. |

**Open Questions:**

- Under node kill, Antithesis may drop un-synced writes or wipe the node's data entirely. That can legitimately lose acked data when 2+ replicas are affected. Mitigation: set the topic config `flush.messages=1`. Should failures after a full-disk wipe of 2+ replicas count? `(needs human input)`
## Liveness anchors (not separate properties)

`Sometimes(ackedAfterLeaderChange, "produce acked after a leader change")`: the driver polls `Admin.describeTopics` between batches to track each partition's leader (the producer hides `NOT_LEADER_OR_FOLLOWER` retries). It evaluates this when a send is acked by a leader different from the one at the previous ack. It confirms acked-offsets-strictly-increase is evaluated across real failovers rather than vacuously.

`Sometimes(deliveredAfterLeaderChange, "consumer delivered a record after a leader change")`: evaluated in the consumer loop, true when a delivered record's partition has a different leader (from the same `describeTopics` tracking) than at the previous delivery for that partition. It confirms consumer-offsets-strictly-increase is evaluated across real failovers.

(Deferred: acked-offset-record-stable would need each driver to append `partition offset payloadHash` to `/tmp/acked/<pid>.log` for a `finally_` read-back. Not built this pass.)

## Invalidated properties

None.

## Assumptions

- The workload uses a single topic `offsets-test` with 3 partitions and RF=3.
- Retention is effectively disabled for the topic (`retention.ms=-1`, `retention.bytes=-1`), so log-start movement does not muddy the checks.

## Open Questions

- Are node-termination faults enabled for this tenant? Leader changes occur under network faults alone, which is enough for the P0 properties. Node kills matter mostly for the deferred acked-offset-record-stable.
