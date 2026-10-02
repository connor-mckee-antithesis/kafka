# Focus

## Customer goal (verbatim)

"No loss of acked writes"

## Chosen properties

1. **`acked-writes-never-lost`** (P0, Safety, `Always`). This is the goal itself, stated as an
   end-to-end check: every record whose acks=all produce succeeded can be read back at its acked
   offset with its acked payload once faults stop. It needs no SUT changes, only a producer that
   records acks and a verifier that reads them back.
2. **`acked-write-verified-across-leader-change`** (P0, Liveness/coverage, `Sometimes`). This is
   the anti-vacuity guard. It shows that some timelines verified acked records that survived a
   leader failover, so a pass of (1) cannot come from runs where no failover ever happened. It
   reuses (1)'s scan in the same command, so it costs one assertion and no extra command.

Why these two: they test the goal most directly and are the simplest to build. Both live
entirely in the workload, so the broker image is an unmodified build.

## Deferred (outside the current focus)

- `ack-implies-min-isr-replicated`: SUT-side localizer in `Partition.checkEnoughReplicasReachOffset`.
- `follower-never-truncates-below-hw`: SUT-side localizer in `ReplicaFetcherThread.truncate`.

Both only help explain *why* acked data was lost, and any loss they would catch is also caught
by `acked-writes-never-lost`. They would also require instrumenting the broker. They are the
first candidates for the spare budget if the oracle fails and the cause needs localizing.

## Budget split

| Property | SDK assertions | Test commands |
|---|---|---|
| `acked-writes-never-lost` | 1 (`Always`) | `parallel_driver_produce_and_verify` (implemented; planned as two commands, merged for the command budget) |
| `acked-write-verified-across-leader-change` | 1 (`Sometimes`) | shares `parallel_driver_produce_and_verify` |
| **Total** | **2 of 5** | **1 of 2** |

Topology: 4 containers (kafka-1..3 combined broker+controller, plus client). See
`deployment-topology.md`.
