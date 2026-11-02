---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# SUT Analysis — Apache Kafka (offset monotonicity focus)

## Summary

Customer goal: "ensure offsets are monotonically increasing". This analysis covers only the parts of Kafka that assign offsets, make them visible to clients, and can move them: the partition log (`UnifiedLog`/`LocalLog`), leader/follower replication (`Partition`, `AbstractFetcherThread`), the high watermark (HW), ListOffsets, and the consumer fetch path. The KRaft controller is in scope only because it drives leader elections.

Single-agent discovery was used (see `decisions.md`). Passes that found little for this goal are noted briefly.

## 1. Architecture and data flow (offset path)

- **Produce (leader):** `ReplicaManager.appendRecords` → `Partition.appendRecordsToLeader` → `UnifiedLog.appendAsLeader` → `UnifiedLog.append(..., validateAndAssignOffsets=true)` (`storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java:1128`). Under `UnifiedLog.lock`, it takes the first offset from `localLog.logEndOffset()` (`:1158`). `LogValidator.validateMessagesAndAssignOffsets` stamps the offsets, and `localLog.append(lastOffset, records)` advances the LEO (`:1276`).
- **Idempotent duplicates:** `analyzeAndValidateProducerState` can find a duplicate batch. In that case the append returns the *original* batch's offsets and does not write (`:1261-1268`). A retried send can therefore be acked with an older offset. That is still correct, as long as it is the same record.
- **Replication (follower):** `ReplicaFetcherThread` fetches from the leader and calls `UnifiedLog.appendAsFollower` with `validateAndAssignOffsets=false`. It rejects batches whose first offset is below the LEO with `UnexpectedAppendOffsetException` (`:1204-1223`). `analyzeAndValidateRecords` throws `OffsetsOutOfOrderException` if offsets inside a batch are not monotonic (`:1490-1592`).
- **Truncation:** after a leader change, followers truncate to the leader-epoch end offset (`AbstractFetcherThread.truncateToEpochEndOffsets`, `core/src/main/scala/kafka/server/AbstractFetcherThread.scala:209`), or to the HW when epochs are missing (`:242`). `UnifiedLog.truncateTo` (`UnifiedLog.java:2356`) moves the LEO **backwards**. This is legitimate only for un-committed (above-HW) data.
- **HW:** the leader advances it with `Partition.maybeIncrementLeaderHW` (`core/src/main/scala/kafka/cluster/Partition.scala:1012`) → `UnifiedLog.maybeIncrementHighWatermark` (`UnifiedLog.java:563`), which only moves it forward. Followers use `maybeUpdateHighWatermark`/`updateHighWatermark`, which clamp to [logStart, LEO] and *can* move it backwards. The underlying setter `updateHighWatermarkMetadata` only logs `"Non-monotonic update of high watermark"` (`:627-629`); it does not reject.
- **ListOffsets(LATEST):** `Partition.fetchOffsetForTimestamp` (`Partition.scala:1435`) returns the HW (read_uncommitted) or the LSO (read_committed). A new leader whose HW is still below its epoch start offset is supposed to return `OFFSET_NOT_AVAILABLE` (`:1455-1474`). This guard is the main thing stopping a client-visible "latest offset" from going backwards across a failover.
- **Consume:** `CompletedFetch` keeps `nextFetchOffset` and skips records with `offset < nextFetchOffset` (`clients/.../consumer/internals/CompletedFetch.java:230`). On `OFFSET_OUT_OF_RANGE` or a reset, the position can jump according to `auto.offset.reset`. Brokers serve consumers only up to the HW.

## 2. State and persistence

- Segment files, plus offset, time and txn indexes, per partition directory. The LEO lives in memory in `LocalLog`. The HW is checkpointed to `replication-offset-checkpoint` periodically, not on every update. The leader-epoch cache is in `leader-epoch-checkpoint`.
- Kafka does **not** fsync on ack by default (`flush.messages`/`flush.ms` are unbounded). Durability comes from replication: acks=all + `min.insync.replicas`. After a crash, recovery (`LogLoader`) truncates partial batches and rebuilds the LEO from disk. The checkpointed HW can be stale (lower) after a restart.

## 3. Concurrency

- `UnifiedLog.lock` serializes appends, truncation and HW updates per partition. `Partition.leaderIsrUpdateLock` (read/write lock) orders leader/ISR changes against appends and ListOffsets.
- Request handler threads append concurrently for different producers. One fetcher thread per source broker drives follower appends and truncation.
- The interesting races cross components, not threads: a leader epoch bump from the controller vs in-flight produces; a follower truncating vs an old leader still acking; an HW update vs a ListOffsets read on the new leader.

## 4. Safety guarantees claimed (to be tested, not assumed)

- Offsets within a partition are assigned contiguously and increase (the comment at `UnifiedLog.java:1270-1275` says it wants to "ensure that the offsets of future appends still grow monotonically").
- The HW "increases monotonically" on the leader (`:571`).
- With acks=all, RF=3, min.insync=2 and unclean election disabled (the default), a record acked at offset X is committed, and every later leader has it at offset X. Docs: "committed messages are not lost".
- Consumers see offsets in increasing order per partition (implied by the fetch-position design).

## 5. Liveness

- After faults stop, a leader is elected and produce/consume resumes. This is relevant only as a precondition: if failover never happens during writes, the safety checks above prove little.

## 6. Bug history (from in-repo comments only; no external tracker consulted)

- `KAFKA-9617`/`KAFKA-17375` comments in the append path; KIP-101/KIP-279 leader-epoch truncation (the reason epoch-based truncation exists — HW-based truncation lost or diverged data).
- The `OFFSET_NOT_AVAILABLE` lagging-HW guard exists because new leaders could report a regressed latest offset (KIP-207-era behavior). These are leads, not verified defects.

## 7. Existing tests

- Unit tests (`UnifiedLogTest`, `PartitionTest`) cover offset assignment and HW logic deterministically. Ducktape system tests (`tests/kafkatest/tests/core/replication_test.py`) do bounce/kill replication checks with an acked-vs-consumed comparison. Nothing explores the timing of leader changes against HW propagation and consumer position at fine grain. That is where Antithesis adds value.

## 8. Failure / degradation modes

- Network partition of the leader from the controller while it still serves produce. Fencing relies on the epoch bump reaching the old leader, or on `min.insync` not being met.
- A follower restarting with a stale HW checkpoint. ISR shrink/expand. Produce retries with idempotence after `NOT_LEADER_OR_FOLLOWER` timeouts.

## 9. External dependencies

- None beyond the JVM and local disk. KRaft replaces ZooKeeper.

## 10. Product context

- Consumers checkpoint progress as offsets. If offsets regress or are reused (the same offset with a different record), consumers silently skip or duplicate data. That is the user-visible failure behind the customer's goal.

## 11. Unproven assumptions

- `updateHighWatermarkMetadata` assumes regressions are legitimate (warn only).
- Process kill is assumed to keep the page cache. In a container kill this holds; in a real power loss it would not. Antithesis node faults kill or stop the container, so unflushed data probably survives in the host page cache. See the open question on `acked-offset-record-stable`.

## 12. Wildcard

- "Monotonic" has three different client-visible meanings: (a) produce-ack offsets, (b) the consumer delivery order, (c) the ListOffsets/HW end offset. They are protected by different mechanisms (log lock, `CompletedFetch` filter, lagging-HW guard), so each needs its own check. A fourth, subtler meaning is *offset identity*: monotonic offsets are worthless if offset X refers to a different record after a failover (divergence after truncation). That is the strongest check and covers several of the others.

## Assumptions

- The default `unclean.leader.election.enable=false` is kept. With it enabled, regressions are documented and expected.
- The workload uses acks=all and an idempotent producer.

## Open Questions

- Are node-termination faults enabled on the tenant? Without them, failover comes only from network partitions or pauses (still enough for leader changes).
