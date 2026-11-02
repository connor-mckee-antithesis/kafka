---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# SUT Analysis — Apache Kafka (scoped to acked-write durability)

## Summary

The customer goal is **no loss of acked writes**. This analysis covers only the code that decides
when a produce is acknowledged and the code that could later remove or replace acknowledged data:
the leader append + high-watermark (HW) path, ISR maintenance, follower truncation, and KRaft
leader election. Single-agent pass through the 12 discovery focuses; focuses that yielded little
are noted as such.

## 1. Architecture and data flow (write path)

- Client `KafkaProducer` (clients/) sends `ProduceRequest` to the partition leader. Defaults since
  3.0: `acks=all`, `enable.idempotence=true`.
- Broker `ReplicaManager.appendRecords` → `Partition.appendRecordsToLeader`
  (`core/src/main/scala/kafka/cluster/Partition.scala:1221`). With `acks=-1` it rejects up front if
  `isr.size < effectiveMinIsr` (`:1236`, `NOT_ENOUGH_REPLICAS`), otherwise appends to the local
  `UnifiedLog` and parks a `DelayedProduce` in purgatory.
- Followers fetch from the leader (`AbstractFetcherThread` / `ReplicaFetcherThread`). Each fetch
  updates the follower's LEO on the leader, which triggers `maybeIncrementLeaderHW`
  (`Partition.scala:1012`). HW = min LEO across the *maximal* ISR (committed ISR plus pending
  additions, KIP-497) and any caught-up ISR-eligible replica; HW never advances while
  `isUnderMinIsr` (`:1013`).
- `DelayedProduce` completes via `checkEnoughReplicasReachOffset` (`Partition.scala:949`): success
  when `highWatermark >= requiredOffset` and `minIsr <= maximalIsr.size`; if HW passed but the ISR
  has since shrunk below min, it returns `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (data is appended but the
  client sees an error and an idempotent retry is deduplicated).
- **"Acked" therefore means:** at least `min.insync.replicas` replicas (counting the leader) had
  LEO ≥ the record's offset at completion time. The data may still only be in the OS page cache.

## 2. State management and persistence

- Log segments on local disk (`storage/`, `UnifiedLog`, `LogSegment`). **No fsync per write by
  default:** `log.flush.interval.messages` defaults to `Long.MAX_VALUE`
  (`server-common/.../ServerLogConfigs.java:101`). Durability relies on replication, not fsync.
- Per-partition `leader-epoch-checkpoint` and `replication-offset-checkpoint` (HW) files; HW is
  checkpointed periodically, so after restart a broker's local HW may lag.
- Partition metadata (leader, leader epoch, ISR, ELR, lastKnownElr) lives in the KRaft metadata log
  (`metadata/`, `raft/`). Controllers decide leadership via `PartitionChangeBuilder`.
- Unclean leader election is off by default. Eligible Leader Replicas (KIP-966,
  `EligibleLeaderReplicasVersion.LATEST_PRODUCTION = ELRV_1`) keep replicas that were in ISR when it
  shrank below min ISR as leader candidates once the ISR empties.

## 3. Concurrency model

- `Partition` guards leader/ISR state with `leaderIsrUpdateLock` (RW lock); HW updates happen under
  it. ISR changes are proposed via `AlterPartition` asynchronously to the controller, so
  `partitionState` has in-flight states (`isInflight`) where maximal ISR ≠ committed ISR.
- Follower truncation runs under `partitionMapLock` and re-checks that the leader epoch did not
  change while the `OffsetsForLeaderEpoch` call was outstanding
  (`AbstractFetcherThread.scala:209-230`).
- Request handler threads, fetcher threads, the ISR expiration scheduler (`maybeShrinkIsr`), and
  metadata-log replay (leader/epoch changes applied via `ReplicaManager.applyDelta`) all touch the
  same partition concurrently. That is where the timing risk is.

## 4. Safety guarantees (claimed, to be tested)

- Kafka's design docs claim that a committed (HW-covered) message is not lost as long as one
  in-sync replica remains alive, under `acks=all` with unclean election disabled. This is a claim
  to test, not an established fact.
- The leader-epoch truncation protocol (KIP-101/KIP-279) claims followers only truncate
  uncommitted suffixes.
- KIP-966 claims ELR prevents data loss when the ISR drops below min ISR and the last ISR member
  crashes uncleanly.

## 5. Liveness guarantees

- After faults stop, a leader is eventually elected for every partition with a live ELR/ISR
  member, and produce/fetch resume. This matters here only because verification must be able to
  read the log back.

## 6. Bug history

No issue tracker was provided. Recent commits in scope are not specific to replication. Historical
Kafka data-loss bugs (KIP-101 motivation: HW-based truncation losing committed data after a double
leader failover; KIP-279: truncation divergence under fast leader changes) all live in the
truncation/election seams named above. They are regression leads, not confirmed current defects.

## 7. Existing test strategy

Unit tests (`PartitionTest`, `AbstractFetcherThreadTest`, `PartitionChangeBuilderTest`) cover
individual state transitions with mocks. Some integration tests (`core/src/test/.../integration`) and ducktape
system tests (`tests/kafkatest/tests/core/replication_test.py`) do a clean or hard bounce while
producing. None of them search interleavings of a crash, a partition, and an in-flight
AlterPartition/leader-epoch change. That is where Antithesis adds value.

## 8. Failure and degradation modes

- ISR shrink after `replica.lag.time.max.ms` (default 30s); a slow follower is removed and HW
  advances without it.
- `NOT_ENOUGH_REPLICAS_AFTER_APPEND`: an appended record is reported as failed, so the producer
  retries. The result is a non-acked duplicate candidate, deduplicated by idempotence.
- Fenced leader epochs: `FENCED_LEADER_EPOCH` / `NOT_LEADER_OR_FOLLOWER` force metadata refresh.
- Broker restart: log recovery truncates torn tails of unflushed segments, then the follower path
  truncates to the leader's epoch end offset.

## 9. External dependencies

None for this scope. KRaft replaces ZooKeeper. Tiered storage is not used.

## 10. Product context

Losing an acked record is the most serious failure a Kafka user can see. Producers treat the
ack as a durable commit, and downstream consumers and offsets build on it.

## 11. Unproven assumptions

- Not every replica in the ISR loses its page cache at the same time. Without fsync, a correlated
  crash of all replicas can lose acked data *by design*. The workload must not count that case as
  a bug (see deployment-topology open questions).
- Maximal ISR used for HW is assumed a superset of whatever ISR the controller later commits
  (KIP-497 reasoning in the comment at `Partition.scala:1003-1006`).
- The controller's view of ISR/ELR is assumed to never let a replica missing committed data become
  a clean leader.

## 12. Wildcard

- `PartitionChangeBuilder.electAnyLeader`/`electPreferredLeader` can elect `lastKnownElr[0]` and
  flag the result `unclean=true` (`PartitionChangeBuilder.java:246-247, 269-270`) **even when
  unclean leader election is disabled**. This holds if ELR is enabled and
  `useLastKnownLeaderInBalancedRecovery` is true, the default at `:104`. It is a "clean-config but
  marked-unclean" path, an unusual seam directly relevant to acked-write loss.
- Acknowledgement depends on HW computed from *maximal* ISR, but `isUnderMinIsr` uses the
  *committed* ISR (`Partition.scala:238`). These two views of ISR can disagree while an
  AlterPartition is in flight.

## Assumptions

- Test config: RF=3, `min.insync.replicas=2`, `acks=all`, idempotent producer,
  `unclean.leader.election.enable=false`, default flush settings, KRaft combined mode.

## Open Questions

- Do Antithesis container kill faults drop un-fsynced page-cache data? If yes, simultaneous kill of
  ≥2 replicas can legitimately lose acked writes under default flush config. (Tracked in
  `deployment-topology.md` and `properties/acked-writes-never-lost.md`.)
