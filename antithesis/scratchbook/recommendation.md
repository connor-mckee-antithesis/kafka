# Recommended next steps: No loss of acked writes

The 15-minute run (2dcd80906e585e7a30ec9c7196261a43-63-3) completed with all 27 properties passing. Both selected properties were exercised. acked-writes-never-lost was checked 987 times with no failures. acked-write-verified-across-leader-change was true 55 times, including a real failover where acked records written under the old leader epoch were read back intact.

## Run longer

Only about 5% of verifications happened after a failover, and each one checked only a few dozen acked records. A 2-hour run gives many more failover-then-verify interleavings, which are where rare lossy orderings show up. It needs no new assertions.

```
/home/exedev/antithesis-pipeline.sh --dir /home/exedev/kafka --registry-key /home/exedev/ant_dev.json --email-to connor.mckee@antithesis.com --from-step 7 --duration 120 https://github.com/connor-mckee-antithesis/kafka
```

## Dial up faults

- Enable node termination. The history we inspected had only network faults, with no broker kills. Crash-killing brokers tests the riskiest durability path: page-cache loss under the default flush settings, log recovery, and truncation after an unclean restart. It is often disabled per tenant, so ask Antithesis support to enable it. The open "page cache on kill" question in deployment-topology.md depends on it.
- Enable clock jitter, which is also often off. ISR shrink timing (replica.lag.time.max.ms) and KRaft timeouts depend on time.
- Quiet periods are already minimal. The workload never calls ANTITHESIS_STOP_FAULTS, so verification runs under active faults. Keep it that way.
- Add a custom fault that forces leader changes, for example by randomly running kafka-leader-election.sh from the client container. This raises the low post-failover rate. Also confirm that node hang and CPU throttling are enabled, since they cause the slow-follower ISR shrinks that stress acks=all.

## Test more surface area

The budget has 3 assertions and 1 test command left.

- ack-implies-min-isr-replicated (in Partition.checkEnoughReplicasReachOffset): catches the broker acking before min.insync.replicas replicas hold the offset.
- follower-never-truncates-below-hw (in ReplicaFetcherThread.truncate): catches the classic failover loss, where a follower truncates away committed records.
- A Sometimes assertion that a post-failover verify finishes with unverified == 0. This shows the end-to-end check isn't passing by skipping unreadable partitions.

```
/home/exedev/antithesis-pipeline.sh --dir /home/exedev/kafka --registry-key /home/exedev/ant_dev.json --email-to connor.mckee@antithesis.com --from-step 3 --focus 'Localize any acked-write loss inside the broker by asserting that acks=all only succeeds once min.insync.replicas replicas hold the offset and that followers never truncate below their high watermark, under broker kills and leader failovers.' https://github.com/connor-mckee-antithesis/kafka
```
