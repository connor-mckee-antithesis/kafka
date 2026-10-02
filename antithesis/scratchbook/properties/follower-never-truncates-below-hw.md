# follower-never-truncates-below-hw

## Why this property

Follower truncation is the only routine path that deletes replicated records. The leader-epoch
protocol (KIP-101, KIP-279) exists because HW-based truncation once removed committed data
during back-to-back leader failovers. A follower's local HW only advances from the leader's HW,
so every offset below it was committed. Truncating below it means committed (acked) data is
being removed.

## Code

- `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala:177` `truncate(tp, offsetTruncationState)` → `Partition.truncateTo` (`Partition.scala:1549`) → `LogManager.truncateTo`.
- Callers: `AbstractFetcherThread.truncateToEpochEndOffsets` (`:209`, OffsetsForLeaderEpoch path),
  `truncateOnFetchResponse` (`:233`, diverging epoch in fetch response),
  `truncateToHighWatermark` (`:242`, truncates *to* HW, which satisfies the invariant).
- Election side: `PartitionChangeBuilder.electAnyLeader` / `electPreferredLeader`
  (`metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java:227-284`).
  `canElectLastKnownLeader` (`:286-316`) elects `lastKnownElr[0]` with `unclean=true` when ISR and
  ELR are both empty and the last known leader comes back, even with unclean election disabled.

## Assertion sketch

```scala
override def truncate(tp: TopicPartition, s: OffsetTruncationState): Unit = {
  val partition = replicaMgr.getPartitionOrException(tp)
  val log = partition.localLogOrException
  Assert.alwaysOrUnreachable(s.offset >= log.highWatermark,
    "follower truncation never removes committed (below-HW) data", details(tp, s, log))
  partition.truncateTo(s.offset, isFuture = false)
  ...
}
```

## Instrumentation status

**Missing.** No existing assertions.

## Open questions

- **Legitimate exceptions.** After a `lastKnownElr` "balanced recovery" election, the new leader
  may have lost its unflushed tail on crash. Followers would then correctly truncate below their
  HW, with real data loss but caused by the documented no-fsync trade-off. If Antithesis kills
  keep page cache, this case should not happen and the assertion stays strict. If not, the
  details should include the leader epoch so triage can tell whether the epoch came from a
  `lastKnownElr` election (INFO log "unclean" in `PartitionChangeBuilder.tryElection`).
- **truncateFullyAndStartAt** (`ReplicaFetcherThread.scala:188`) runs when the follower is out of
  range. It is left out to save budget. Revisit if `acked-writes-never-lost` fails without this
  assertion firing.

### Investigation Log

#### Is truncation below local HW ever legitimate under clean config?

- Examined: `PartitionChangeBuilder.java:216-322` (election logic, `isValidNewLeader`,
  `canElectLastKnownLeader`), `ReplicationControlManager.java` call sites setting
  `Election.UNCLEAN` (`:1155-1156, 1693-1694, 2155-2156, 2275-2276`).
- Found: the truly unclean branch (`:273-281`) runs only when topic config enables unclean
  election or an admin issues `ElectLeaders(UNCLEAN)`. The workload does neither. The
  `lastKnownElr` branch is reachable under the default config (`useLastKnownLeaderInBalancedRecovery = true`, `:104`, plus ELR enabled), but only when ISR and ELR are both empty.
- Not found: whether the last known leader is guaranteed to have all HW-covered data after a
  crash. That depends on fsync/page-cache semantics, which the code cannot answer.
- Conclusion: `(partial: ...)`. The assertion stays `AlwaysOrUnreachable`, and the details carry
  epoch info for triage.
