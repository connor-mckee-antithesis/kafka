---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Deployment Topology

## Summary
Four containers: 3 Kafka nodes in combined KRaft mode (broker+controller) and 1 workload client. This is already the minimum for the two P0 properties (acked-offsets-strictly-increase, consumer-offsets-strictly-increase): both are only meaningful across leader failovers, and an RF=3 / `min.insync.replicas=2` partition is the smallest setup that can lose its leader, elect a new one from the ISR, and keep accepting `acks=all` writes. It also needs a 3-voter KRaft quorum that survives losing one node. Fewer brokers would mean either no failover (RF=1) or a 2-voter quorum that halts when one node is lost. Separate controllers would add containers without adding coverage.

## Containers

| Name | Image source | Role | Runs | Connects to |
|---|---|---|---|---|
| kafka-1, kafka-2, kafka-3 | New Dockerfile `antithesis/Dockerfile.kafka`, based on `docker/jvm/Dockerfile`. Build the tarball from this repo (`./gradlew clean releaseTarGz`, output `core/build/distributions/kafka_*.tgz`) and copy it in as `kafka.tgz`. The existing Dockerfile already supports a local `*kafka.tgz` when `kafka_url` is empty. Skip the JSA stage (it needs a running broker at build time). | Service | `kafka-server-start.sh` with `process.roles=broker,controller`, `node.id=N`, `controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093`, a shared pre-generated `cluster.id` formatted with `kafka-storage.sh format`, `default.replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`, `auto.create.topics.enable=false`, `broker.session.timeout.ms=3000`, `broker.heartbeat.interval.ms=500` (more frequent failovers) | each other (9092 broker, 9093 controller); client |
| client | New Dockerfile `antithesis/Dockerfile.client`: JDK 17 + a small Java workload jar using `kafka-clients` (built from this repo) and the Antithesis Java SDK (`com.antithesis:sdk`) | Client | Waits until all 3 brokers answer `describeCluster`, creates topic `offsets-test` (3 partitions, RF=3, `flush.messages=1`, `retention.ms=-1`, `retention.bytes=-1`), emits `setup_complete`, then sleeps. Test template `/opt/antithesis/test/v1/offsets/` holds `parallel_driver_offsets` only (the single test command this pass). | kafka-1..3:9092 |

No restart policy in compose (Antithesis handles restarts).

## SDK
- Java SDK in the workload only. No SUT instrumentation this pass, so Kafka needs no SDK dependency. Coverage instrumentation of the broker JVM is optional and out of scope.

## Faults
- Network partitions and clogs (on by default) are enough to force leader changes.
- Node kill/stop/pause add more failovers. `flush.messages=1` keeps acked data durable on kill, so a legitimate data loss does not look like an offset regression. Node faults matter most for the deferred acked-offset-record-stable.

## Assumptions
- Combined mode is acceptable even though production often separates controllers. Controller placement does not change the partition-level offset logic.

## Open Questions
- Is node termination enabled for this tenant, and should it be?
