# ack-implies-min-isr-replicated

## Why this property

The end-to-end oracle only notices loss after it happens. This SUT-side check fires at the
moment a produce is acknowledged, if that ack is not backed by `min.insync.replicas` copies.
It separates "acked too early" bugs from "deleted later" bugs.

## Code

`core/src/main/scala/kafka/cluster/Partition.scala`

- `checkEnoughReplicasReachOffset` `:949-985`. Success branch: `leaderLog.highWatermark >= requiredOffset && minIsr <= curMaximalIsr.size` → `(true, Errors.NONE)` at `:977`.
- HW comes from the min LEO over maximal ISR + caught-up eligible replicas (`:1019-1035`) and is
  blocked when `isUnderMinIsr` (`:1013`, which uses *committed* ISR at `:238`).
- `effectiveMinIsr = min(minInSyncReplicas, replicas)` `:246-248`.

## Assertion sketch (Scala, Java SDK)

```scala
// in the (true, Errors.NONE) branch
val holders = 1 /* leader, LEO >= requiredOffset since HW >= requiredOffset */ +
  curMaximalIsr.asScala.count(id => id != localBrokerId &&
    getReplica(id).exists(_.stateSnapshot.logEndOffset >= requiredOffset))
Assert.alwaysOrUnreachable(holders >= minIsr,
  "acks=all success only when min.insync replicas hold the offset", details)
```

Cost: one pass over ISR per acks=all completion, which is negligible for the test.

## Failure scenarios targeted

- HW computed while the maximal ISR includes a replica whose LEO was then reset; ack returned on a stale HW.
- An ISR shrink committed by the controller between HW advancement and purgatory completion. The
  code handles this with `NOT_ENOUGH_REPLICAS_AFTER_APPEND`. The assertion checks that the
  handling is correct.

## Instrumentation status

**Missing.** No Antithesis assertions exist in the codebase.

## Open questions

- Stale-high remote LEO: the leader's `Replica.stateSnapshot` is updated only on fetch. If a
  follower crashes, restarts, and truncates, the leader still holds the old LEO until the next
  fetch. The assertion reads the same snapshot the HW logic uses, so it cannot catch that
  particular bug. `acked-writes-never-lost` and `follower-never-truncates-below-hw` cover it
  instead. Not investigated further; this is a known limitation, not a blocker.
