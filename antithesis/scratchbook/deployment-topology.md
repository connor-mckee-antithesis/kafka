---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Deployment Topology

## Summary

Four containers: three KRaft nodes in combined mode (broker+controller) and one workload client.
This is the fewest the selected properties (`acked-writes-never-lost`,
`acked-write-verified-across-leader-change`) need. RF=3 with `min.insync.replicas=2` is the
smallest setup where an acked write can survive a leader failover while keeping a surviving ISR
member. Three combined nodes also give a KRaft quorum that survives one node fault. Fewer brokers
would either make acks=all unavailable after any single fault (RF=2, minISR=2) or make loss
by-design (minISR=1). Dedicated controllers would add 3 containers without touching either
property.

```text
                 +------------------------+
                 | client (workload)      |
                 | producer + verifier    |
                 +-----------+------------+
                             | 9092
        +--------------------+--------------------+
        v                    v                    v
+---------------+    +---------------+    +---------------+
| kafka-1       |<-->| kafka-2       |<-->| kafka-3       |
| broker+ctrl   |    | broker+ctrl   |    | broker+ctrl   |
+---------------+    +---------------+    +---------------+
      9092 client/replication, 9093 controller (KRaft quorum)
```

## Components

### kafka-1, kafka-2, kafka-3 (service ×3)

- **Image:** a new Dockerfile built from this repo. Build stage: `./gradlew releaseTarGz`
  (unmodified: both selected properties are workload-side, so the broker needs no Antithesis
  SDK). Runtime stage: adapt `docker/jvm/Dockerfile` (eclipse-temurin JRE, unpack `kafka.tgz`,
  launch script). Instrument with Antithesis Java coverage instrumentation if available.
- **Runs:** `kafka-server-start.sh` with `process.roles=broker,controller`, `node.id=1..3`,
  `controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093`, storage formatted
  with a fixed cluster id at container start (`kafka-storage.sh format` if the dir is empty, so
  restarts keep data).
- **Config:** `default.replication.factor=3`, `min.insync.replicas=2`,
  `unclean.leader.election.enable=false`, `replica.lag.time.max.ms` lowered (e.g. 5000) so ISR
  shrink happens within short timelines, `offsets.topic.replication.factor=3`. Default flush
  settings (see open question).
- **Data dir:** a container-local volume that survives Antithesis kill/restart.
- **No restart policy** in compose (Antithesis restarts killed containers).

### client (workload)

- **Image:** a new Dockerfile. JRE, workload jar (Java, built against this repo's `clients`
  module plus the Antithesis Java SDK), and test template at
  `/opt/antithesis/test/v1/acked-writes/`.
- **Entrypoint:** wait until all 3 brokers answer `Admin.describeCluster`, create topic `acked`
  (3 partitions, RF=3, `min.insync.replicas=2`, `cleanup.policy=delete`, `retention.ms=-1`,
  `retention.bytes=-1`, so retention never removes acked records) idempotently, emit
  `setup_complete`, then sleep.
- **Test commands:**
  - `parallel_driver_produce_and_verify` (implemented, `AckedWritesDriver`): bounded acks=all
    idempotent producer. It appends acked `(partition, offset, uniqueId, length)` to
    `/tmp/acked-ledger/` (helper path outside the template dir). Then it reads every ledger
    file, scans all partitions, and evaluates `acked-writes-never-lost` (`Always`) and
    `acked-write-verified-across-leader-change` (`Sometimes`). It was planned as two commands
    (`parallel_driver_produce_acked` + `eventually_verify_acked`) and merged because of the
    command budget.
- **Network:** talks to all three brokers on 9092.

## SDKs

- Java SDK (`com.antithesis:sdk`) in the workload only.

## Faults

Defaults: node kill/stop/pause, network partitions and latency between brokers and between the
client and brokers. All of them matter. Leader kills and broker partitions are what drive
`acked-write-verified-across-leader-change`.

## Assumptions

- Combined mode is acceptable. The controller/broker split is not needed for the acked-write
  paths in scope.

## Open Questions

- **Page cache on kill** `(needs human input)`: if Antithesis node kill drops un-fsynced writes,
  set `flush.messages=1` (topic config) to align the test with the customer's goal. Otherwise
  losses from simultaneous kills of 2+ replicas are by-design under default config. Default for
  the first run: leave default flush, and use the fault log around any `acked-writes-never-lost`
  failure to tell the two cases apart.
