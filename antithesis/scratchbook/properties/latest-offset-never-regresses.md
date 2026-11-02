# latest-offset-never-regresses

## Why identified
The HW is guarded as monotonic only in `maybeIncrementHighWatermark` (`UnifiedLog.java:563-581`). `updateHighWatermarkMetadata` only warns on a regression (`:627-629`). After `makeLeader` (`Partition.scala:590-680`) the new leader's HW may lag the old leader's. The lagging-HW guard (`Partition.scala:1455-1474`) throws `OFFSET_NOT_AVAILABLE` for LATEST while `HW < leaderEpochStartOffset`.

## Code paths
- `Partition.fetchOffsetForTimestamp` (`Partition.scala:1435`), `leaderEpochStartOffsetOpt` set at `:667`.
- `Partition.maybeIncrementLeaderHW` (`:1012`).
- Client: `Admin.listOffsets(OffsetSpec.latest())`. It retries `OFFSET_NOT_AVAILABLE` internally, and the call may fail after the timeout; skip failures.

## Failure scenario
The client sees latest=100 from A. A is deposed; B's HW is 90 (stale) and its epoch start is 100. The guard must block. If `leaderEpochStartOffsetOpt` is unset or reset (for example on a fast double election, or reset at `:577`/`:715`), B returns 90, a regression.

## Instrumentation
Workload `Always`. A SUT-side `Always` in `updateHighWatermarkMetadata` would be wrong: follower regressions are legitimate. Nothing exists today.

### Investigation Log
#### Does the guard cover HW ≥ epochStart but < the old leader's HW?
- Examined: `Partition.scala:1443-1474`, `makeLeader`.
- Found: epochStart = the new leader's LEO at election. Anything the old leader counted as committed (≤ old HW) was in the ISR, so it is ≤ the new leader's LEO. So HW ≥ epochStart ≥ old HW.
- Not found: proof that a clean election always chooses an ISR member with LEO ≥ old HW under KRaft ELR (KIP-966).
- Conclusion: `(partial)`.
