# acked-offset-record-stable

## Why identified
Wildcard pass: monotonic numbering is meaningless if offset X can name different data after a failover. Epoch-based truncation (KIP-101/279) exists because HW-based truncation caused log divergence. `AbstractFetcherThread.truncateToEpochEndOffsets` (`core/.../AbstractFetcherThread.scala:209`), `truncateToHighWatermark` (`:242`), and `UnifiedLog.truncateTo` (`UnifiedLog.java:2356`) all move the LEO backwards.

## Code paths
- Follower append check `UnifiedLog.java:1204-1223`; leader epoch cache `assignEpochStartOffset` (`:1229`).
- Recovery: `LogLoader` truncates partial batches. The HW checkpoint can be stale.

## Failure scenario
A record is acked at X with acks=all (in ≥2 replicas). Leader churn plus a stale-epoch-cache follower truncates below X, then re-replicates different data into X from a leader that never had the record. The final read finds a different payload at X, or none.

## Instrumentation
Workload `Always` in `finally_verify_offsets`. The drivers append `partition offset payloadHash` lines for each acked record to `/tmp/acked/<pid>.log` in the client container. The finally command first waits for all partitions to have a leader and HW ≥ max acked offset + 1 (with a bounded retry). Nothing exists today.

## Open questions

### Investigation Log
#### Do node kills lose un-fsynced data?
- Examined: Antithesis docs `/docs/product/writing_tests/controlling_faults/fault_types/` (via `snouty docs`).
- Found: "Restarted nodes ... may keep durably synced filesystem changes, or may lose all of their modified data and boot fresh from their image."
- Implication: without fsync, losing 2 of 3 replicas' page cache can legitimately lose acked data. `flush.messages=1` on the topic makes "durably synced" cover acked data. A full wipe of 2+ replicas still makes loss legitimate.
- Not found: the tenant's node-fault settings, and whether the full-wipe mode is active by default.
- Conclusion: `(needs human input)`. Should failures after a full wipe count, or should node kill be disabled or limited for this test?
