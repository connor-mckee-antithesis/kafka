# Recommended next steps: Kafka offset monotonicity

Both selected properties passed in run fd445b8cd8821d2bdce448bc4b738297-64-0, and both were exercised. "Acked produce offsets strictly increase" passed 1465 checks and "consumer delivers strictly increasing offsets" passed 1454. Both were also checked after real leader changes. One caveat: the assertions show up under "Missed Always", so the workload jar may not be cataloged correctly. Adding a Main-Class manifest should fix this, so an unreached Always gets reported instead of passing silently.

## Expand Test Coverage

- latest-offset-never-regresses: checks that ListOffsets(LATEST) never decreases across failovers. It targets the window right after a new leader is elected, while its high watermark is still stale.
- acked-offset-record-stable: a finally_ check that every acked offset still holds its original record after recovery. This is the strongest form of the goal, because it shows an offset is never reused for different data after truncation.

```
/home/exedev/antithesis-pipeline.sh --dir /home/exedev/kafka --registry-key /home/exedev/ant_dev.json --from-step 3 --focus 'Verify that partition end offsets never move backwards across leader failovers and that every acked offset still holds its original record after recovery.' https://github.com/connor-mckee-antithesis/kafka
```

## Extend Test Duration

This run lasted 6 minutes, and only about a quarter of produce acks came after a leader change. A longer run explores many more interleavings of failover and truncation, which is where an offset regression would show up.

```
/home/exedev/antithesis-pipeline.sh --dir /home/exedev/kafka --registry-key /home/exedev/ant_dev.json --from-step 7 --duration 120 https://github.com/connor-mckee-antithesis/kafka
```

## Increase Fault Injection

- Enable node termination for the tenant through Antithesis support. Both sampled histories had only network faults. Killing and restarting a broker exercises log recovery and replica re-join, which partitions cannot reach. The topic already uses flush.messages=1, so data lost to a crash will not be mistaken for an offset regression.
- Enable clock jitter too. The 3 s broker session timeout and fencing decisions depend on time.
- No quiet periods are configured: the harness never calls ANTITHESIS_STOP_FAULTS, so there is nothing to shorten. If acked-offset-record-stable is added, keep its recovery quiet period short, about 30 to 60 s, so faults stay active for most of the run.
- Add custom faults in the client container:
  - Run kafka-leader-election.sh (preferred leader election) at random times to force leader changes without a network fault.
  - Run kafka-reassign-partitions.sh to change replica placement while traffic is flowing.
  - Node hang and CPU throttling of one broker add follower-lag and ISR-shrink windows.
