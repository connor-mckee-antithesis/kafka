---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Property Catalog — No loss of acked writes

## Summary

This is a focused pass (see `focus.md`). Two properties are selected, and both test the customer
goal "No loss of acked writes" directly: the end-to-end oracle and its anti-vacuity guard. Both
run in the workload only, so the SUT needs no instrumentation. The two SUT-side root-cause
localizers stay in this file but are deferred.

**Assertion budget:** 2 of 5 used (both in the workload). **Test commands:** 1 of 2
(`parallel_driver_produce_and_verify`, in `test/v1/acked-writes/`, class `AckedWritesDriver`).

**Implementation status:** `acked-writes-never-lost` and `acked-write-verified-across-leader-change`
are **implemented** (workload-side). Not yet run in Antithesis. Produce and verify share one
`parallel_driver_` because the session budget allowed only one new command. Verification therefore
runs while faults are active, and only definite evidence counts as loss.

Test configuration everywhere: RF=3, `min.insync.replicas=2`, producer `acks=all` +
`enable.idempotence=true`, `unclean.leader.election.enable=false`, ELR enabled (default), default
flush settings, 3-node KRaft combined mode.

"Acked" = the producer `send()` callback completed with `exception == null` and returned a
`RecordMetadata` offset.

| Slug | Type | Assertion | Where | Priority |
|---|---|---|---|---|
| `acked-writes-never-lost` | Safety | `Always` | workload (`parallel_driver_produce_and_verify`) — implemented | P0 |
| `acked-write-verified-across-leader-change` | Liveness/coverage | `Sometimes(cond)` | workload (`parallel_driver_produce_and_verify`) — implemented | P0 |
| `ack-implies-min-isr-replicated` | Safety | `AlwaysOrUnreachable` | SUT, `Partition.checkEnoughReplicasReachOffset` | Deferred: outside the current focus |
| `follower-never-truncates-below-hw` | Safety | `AlwaysOrUnreachable` | SUT, `ReplicaFetcherThread.truncate` | Deferred: outside the current focus |

## Category: Data integrity under faults

### acked-writes-never-lost — Acknowledged records are readable at their acked offset with the acked payload

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Every record whose produce was acknowledged appears in the partition at exactly the acked offset with byte-identical key and value, after faults stop and the cluster recovers. |
| **Invariant** | `Always(allAckedPresent, "acked record present at its acked offset with acked payload")` in `parallel_driver_produce_and_verify` (`AckedWritesDriver.verify`). The command snapshots every ack ledger file (its own and earlier/concurrent runs') and scans each partition from a randomly chosen acked offset (bounding scan time as the log grows) up to the highest acked offset. It checks that for each ledger entry `(partition, offset, uniqueId)` the record at `offset` carries `uniqueId`. Loss = a different record at an acked offset, an acked offset skipped while later offsets were read (offsets are dense: no transactions, compaction or retention), or two ledger entries acked at the same offset. A partition not readable to its max acked offset within 90s is logged as *unverified* (unavailability) and does not fail the assertion. Details of the first few mismatches go in the assertion details. `Always` fits because a single missing or replaced acked record is a violation. There is no acceptable rate. |
| **Antithesis Angle** | Faults overlap with in-flight produce: leader kill during `DelayedProduce`, partition between leader and one follower that triggers ISR shrink, and pause of a follower past `replica.lag.time.max.ms`. Double leader failover before HW propagates to the follower that becomes leader, and crash/restart that runs log recovery. Verification runs under faults, and each run re-verifies all earlier acked records, so old records are checked again after later faults. A future `eventually_` re-check (with faults paused) would also catch truncations that are never refilled; that needs the spare command budget. |
| **Why It Matters** | This is exactly what the customer asked for: a lost or overwritten acked record. Historical Kafka data-loss bugs (KIP-101, KIP-279) sit in failover/truncation seams that this oracle observes end to end. |

**Open Questions:**

- Do Antithesis node-kill faults discard un-fsynced page cache? If so, simultaneous kill of ≥2 replicas can lose acked data by design under default flush config. `(partial: docs say killed containers restart; nothing found on page-cache semantics)`
- How should verification treat a partition with no leader at check time (all ELR/ISR members unavailable)? Retry until timeout, then report as unavailability, not loss.

## Category: Test-effectiveness (anti-vacuity)

### acked-write-verified-across-leader-change — Verification sometimes covers acked records that survived a leader change

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness / coverage |
| **Property** | In some timelines, the verifier confirms an acked record that was written in an earlier leader epoch than the partition's current leader epoch, meaning the record survived at least one failover. |
| **Invariant** | `Sometimes(verifiedOldEpochAcked > 0, "acked record verified after its partition changed leader")` in `parallel_driver_produce_and_verify` (implemented). For each verified acked record, compare `ConsumerRecord.leaderEpoch()` with the max leader epoch seen in the same partition's scan. `Sometimes(cond)` fits because this is a meaningful state the run must reach, not an invariant. |
| **Antithesis Angle** | Shows that fault injection actually forced failovers under acknowledged traffic. Antithesis uses it as an exploration hint toward timelines with leader changes. |
| **Why It Matters** | Without it, `acked-writes-never-lost` could pass only because no failover ever happened. |

**Open Questions:**

- None.

## Deferred: outside the current focus

### ack-implies-min-isr-replicated — An acks=all success is only returned when ≥ min.insync.replicas replicas hold the offset

| | |
|---|---|
| **Priority** | Deferred: outside the current focus |
| **Type** | Safety |
| **Property** | When the leader completes an acks=all produce with `Errors.NONE`, at least `effectiveMinIsr` replicas (leader included) have LEO ≥ the request's required offset, and all of them are in the maximal ISR. |
| **Invariant** | SUT-side `AlwaysOrUnreachable(count >= effectiveMinIsr, "acks=all success only when min.insync replicas hold the offset")` in `Partition.checkEnoughReplicasReachOffset` (`core/src/main/scala/kafka/cluster/Partition.scala:976-977`) on the `(true, Errors.NONE)` branch. `count` = leader (LEO ≥ requiredOffset) + remote replicas in `partitionState.maximalIsr` whose `stateSnapshot.logEndOffset >= requiredOffset`. Details: partition, requiredOffset, HW, maximal/committed ISR, per-replica LEOs. `AlwaysOrUnreachable` fits because the branch only runs for acks=all completions, and if it ever runs it must hold. **Missing** from the codebase (no existing assertions). |
| **Antithesis Angle** | Explores ISR shrink/expand racing with HW advancement and in-flight AlterPartition (maximal vs committed ISR, `Partition.scala:238` vs `:1031`), and follower LEO reset after a follower restart/truncation that is not yet reflected in the leader's replica state. It localizes a root cause before the end-to-end oracle sees loss. |
| **Why It Matters** | This is the precondition the durability claim rests on. If it fails, the failure is in the ack decision, not in recovery. |

**Open Questions:**

- Can a remote replica's cached `logEndOffset` on the leader be stale-high (follower truncated after restart, leader not yet updated)? If so, the assertion uses the same stale data as the HW calculation and would miss that bug. Record it as a limitation.

### follower-never-truncates-below-hw — Follower truncation never removes offsets below its local high watermark

| | |
|---|---|
| **Priority** | Deferred: outside the current focus |
| **Type** | Safety |
| **Property** | Under the clean-election config, a follower's leader-epoch-driven truncation never truncates its log to an offset below its own local high watermark, which is committed data the leader already propagated. |
| **Invariant** | SUT-side `AlwaysOrUnreachable(truncationOffset >= localHighWatermark, "follower truncation never removes committed (below-HW) data")` in `ReplicaFetcherThread.truncate` (`core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:177`), evaluated before `partition.truncateTo`. Details: tp, truncation offset, local HW, LEO, current leader epoch, `truncationCompleted`. `AlwaysOrUnreachable` fits because truncation only occurs after leader changes, which may not happen in every timeline. **Missing** from the codebase. |
| **Antithesis Angle** | Targets the KIP-101/KIP-279 class: a quick succession of leader changes, a follower restarting with a stale `leader-epoch-checkpoint`, a diverging-epoch fetch response (`truncateOnFetchResponse`) racing with a new leader epoch, and the `lastKnownElr` "balanced recovery" election (`PartitionChangeBuilder.java:246, 269`) after the whole ISR crashes. |
| **Why It Matters** | Truncation is the only normal code path that deletes replicated data. If it cuts below HW and that follower later leads, acked writes vanish. The assertion fires at the moment data is removed, which is a strong replay anchor. |

**Open Questions:**

- Is truncation below local HW ever legitimate under clean config, e.g. after a `lastKnownElr` election flagged `unclean=true` with ELR on, or after the leader's own unflushed tail was lost? `(partial: lastKnownElr election requires ISR and ELR both empty; unclean branch at :273 is gated by topic config or admin UNCLEAN election only)`
- Should `truncateFullyAndStartAt` (out-of-range path) be covered too? It is excluded for now to stay in budget.

## Assumptions

- The ack ledger lives on the client container's local disk and is written (and flushed) only
  after the callback succeeds. A client container crash loses only unflushed ledger lines, which
  makes the check weaker, never wrong.
- Records carry a unique id (`producerId-seq` or UUID) in the value, so offset→payload checks are
  unambiguous.

## Open Questions (catalog-wide)

- Antithesis page-cache semantics on node kill (see `deployment-topology.md`). This determines
  whether `flush.messages=1` is needed to keep `acked-writes-never-lost` free of by-design false
  positives. With `follower-never-truncates-below-hw` deferred, the fault log is the main way to
  tell by-design page-cache loss apart from a real bug. `(needs human input)`
