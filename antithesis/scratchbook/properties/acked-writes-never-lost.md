# acked-writes-never-lost

## Why this property

This is the customer goal stated directly. Kafka claims that with `acks=all`, `min.insync.replicas=2`
and unclean election disabled, a record acknowledged to the producer survives as long as one
in-sync replica survives. That is a claim to test.

## Code paths

- Ack decision: `Partition.appendRecordsToLeader` (`core/src/main/scala/kafka/cluster/Partition.scala:1221-1240`)
  → `DelayedProduce` → `Partition.checkEnoughReplicasReachOffset` (`:949-985`).
- HW advancement: `Partition.maybeIncrementLeaderHW` (`:1012-1055`), maximal ISR (`:1031`).
- Data removal paths: follower truncation `AbstractFetcherThread.truncateToEpochEndOffsets` (`:209`),
  `truncateOnFetchResponse` (`:233`), `ReplicaFetcherThread.truncate` (`:177`); log recovery on
  restart (`storage/.../LogLoader`); leader election `PartitionChangeBuilder.electLeader` (`metadata/...:216`).
- Durability config: `log.flush.interval.messages` default `Long.MAX_VALUE`
  (`server-common/.../ServerLogConfigs.java:101`), so there is no fsync per write.

## Workload design

- `parallel_driver_produce_acked`: a Java `KafkaProducer` (acks=all, idempotence on, short
  `delivery.timeout.ms`) sends a bounded batch of records to topic `acked` (e.g. 3 partitions,
  RF=3). The value carries a unique id. On a successful callback it appends
  `partition offset uniqueId` to `/tmp/acked-ledger/<pid>.log` and flushes. Failed callbacks
  are not recorded; they may or may not be in the log, and both are fine.
- `eventually_verify_acked`: loads all ledger files, waits/retries until every partition has a
  leader, consumes from offset 0 to the end offset, and evaluates the `Always`. Also feeds
  `acked-write-verified-across-leader-change`.

## Failure scenario examples

- Leader L acks offset N (HW=N+1 via L and F1). L crashes. F2 (out of ISR) becomes leader through
  some election bug and F1 truncates to F2's end. Offset N is gone or replaced.
- Follower F1 restarts with a stale leader-epoch cache and truncates below HW. Then L dies and F1
  is elected.

## Instrumentation status

**Implemented** (workload-side) in `antithesis/workload/.../AckedWritesDriver.java`, run by
`test/v1/acked-writes/parallel_driver_produce_and_verify`. The planned two-command design
(`parallel_driver_produce_acked` + `eventually_verify_acked`) was merged into one parallel driver
because of the one-command budget. Verification runs under faults, so a truncation that is never
refilled shows up as *unverified*, not *lost*. An `eventually_` re-check is the next addition.
Tested locally: an injected conflicting ledger entry made the `Always` fail. A stopped leader
made `confirmed_older_leader_epoch` > 0.

## Open questions

- **Page-cache semantics of node kill.** If an Antithesis kill drops un-fsynced data, then killing
  2 of 3 replicas within the flush window can lose acked writes *by design* (Kafka documents
  replication, not fsync, as its durability mechanism). That would be a false positive for the
  customer's goal. Answer "yes": set `flush.messages=1` on the topic, or accept the documented
  limit and only count losses where at most one replica was killed. Answer "no": the default
  config is fine.
- **Unavailable partitions at verification.** If no leader is available after the `eventually_`
  recovery window, report it separately (e.g. log + exit non-zero) instead of failing the
  `Always`. Unavailability is not loss.

### Investigation Log

#### Do Antithesis node-kill faults discard un-fsynced page cache?

- Examined: `snouty docs` pages "Types of faults" (node kill/stop), "Controlling faults", "Test
  commands" reference.
- Found: kill simulates a crash, and containers restart after a delay. `eventually_`/`finally_`
  commands pause faults and restart killed containers.
- Not found: any statement on whether writes not yet fsynced survive a container kill.
- Conclusion: tagged `(partial: ...)` in the catalog. Catalog-wide copy is `(needs human input)`.
  The setup skill should default to the documented durability config and record the choice.
