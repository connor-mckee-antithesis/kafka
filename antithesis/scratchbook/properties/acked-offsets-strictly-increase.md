# acked-offsets-strictly-increase

## Why identified
Customer goal. The produce path assigns offsets under `UnifiedLog.lock` from `localLog.logEndOffset()` (`storage/.../UnifiedLog.java:1158`). `LogValidator.validateMessagesAndAssignOffsets` stamps offsets contiguously. The comment at `:1270-1275` explicitly aims for "offsets of future appends still grow monotonically".

## Code paths
- `UnifiedLog.append` (`:1128-1306`): offset assignment, duplicate detection (`:1261-1268`), `localLog.append` (`:1276`).
- `Partition.appendRecordsToLeader` (core `Partition.scala`): checks `min.insync`, appends under `leaderIsrUpdateLock` read lock.
- Producer side: `RecordAccumulator`/`Sender` retries with the same sequence; `enable.idempotence=true` keeps order with max.in.flight ≤ 5.

## Failure scenario
The old leader is partitioned from the controller but still reachable by the producer. It appends at offset N and acks only if the ISR (≥2) acks. If fencing is broken, the old leader could ack N while the new leader also assigns N to a different record. The producer sees N twice, or a lower offset after a higher one.

## Instrumentation
- Workload-only `Always`. No existing SDK assertion (see `existing-assertions.md`: none). SUT-side is not needed: the callback offset is the client-visible fact.

## Open questions
- Dedup returns the original offsets. That is correct when the retried batch is identical; the producer guarantees this for retries. A per-producer, per-partition strict check is still valid: one record maps to one callback.

### Investigation Log

#### Can the dedup path ack a different record at an older offset?
- Examined: `UnifiedLog.java:1257-1268`, `analyzeAndValidateProducerState`.
- Found: a duplicate is identified by producerId/epoch/sequence range, and the append returns the cached `BatchMetadata` offsets. The producer only resends the same batch with the same sequence.
- Not found: whether `ProducerStateManager` can keep stale `BatchMetadata` after truncation (it calls `truncateAndReload` at `:2593`).
- Conclusion: `(partial)`. The property as stated would catch the bad case.
