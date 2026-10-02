# acked-write-verified-across-leader-change

## Why this property

It stops `acked-writes-never-lost` from passing vacuously. The durability claim only matters if
acked records survive leader failover. This `Sometimes` shows that at least one timeline verified
an acked record written under an older leader epoch than the partition's current one.

## Mechanism

- `ConsumerRecord.leaderEpoch()` (`Optional<Integer>`) gives the epoch under which each record
  was appended.
- Current epoch per partition: the max `leaderEpoch()` seen during the verification scan. The
  verifier can also produce one sentinel record per partition before scanning (it is not
  ledgered) so the current epoch is visible, or call `OffsetsForLeaderEpoch` / `describeTopics`.
- Condition: `exists acked record r verified OK with r.leaderEpoch < currentEpoch(r.partition)`.

## Instrumentation status

**Implemented**, workload-side, in `parallel_driver_produce_and_verify` (`AckedWritesDriver`).
Current epoch = the max `leaderEpoch()` seen in the partition's scan (no sentinel record, no admin
RPC). Continuous production writes new-epoch records after a failover. Fired locally after
stopping one broker.

## Notes

- It shares the verifier code path with `acked-writes-never-lost`, so it costs no extra command.
- If it never fires, the faults are not producing leader changes under load. Check the produce
  duration relative to fault cadence, and consider `leader.imbalance.check.interval.seconds`.

## Open questions

- None.
