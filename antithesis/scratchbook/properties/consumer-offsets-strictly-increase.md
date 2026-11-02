# consumer-offsets-strictly-increase

## Why identified
Customer goal, consumer view. `CompletedFetch` tracks `nextFetchOffset` and drops records with `offset < nextFetchOffset` (`clients/.../consumer/internals/CompletedFetch.java:230`). The broker serves only up to the HW. The position can move backward or forward only through a seek or a reset.

## Code paths
- `CompletedFetch.fetchRecords` / `nextFetchedRecord` (`:190-290`).
- `FetchCollector` handling of `OFFSET_OUT_OF_RANGE` → `SubscriptionState.requestOffsetReset` (with `auto.offset.reset=none` this surfaces as `OffsetOutOfRangeException` instead).
- Broker: `ReplicaManager.readFromLog` → `Partition.fetchRecords`, HW-bounded for consumers.

## Failure scenario
The consumer reads up to HW=H from leader A. B becomes leader with a lagging HW and LEO ≥ H. The fetch at H returns empty (fine). If B's LEO < H (committed data missing on B), the fetch is out of range, which is a violation. The consumer could also receive data that is later truncated (if it was served above the HW), then get different data at the same offset.

## Instrumentation
Workload `Always`, evaluated per record, and with `false` in the `OffsetOutOfRangeException` handler. Use the same message at both call sites: it is one property with one callsite helper. Nothing exists today.

## Open questions
- Retention must be disabled so that out-of-range is never legitimate (set the topic configs `retention.ms=-1` and `retention.bytes=-1`).
