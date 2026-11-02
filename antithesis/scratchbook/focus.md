# Focus for this pass

## Customer goal (verbatim)

> ensure offsets are monotonically increasing

## Chosen properties (P0)

1. **acked-offsets-strictly-increase**: for each idempotent producer, the offsets acked for a partition strictly increase in send order. This is the producer-side reading of the goal and the most direct test of offset assignment (`UnifiedLog.append`) across leader failovers. It is simple: an in-memory `lastAckedOffset[partition]` check in the send callback.
2. **consumer-offsets-strictly-increase**: an `assign()` consumer with `auto.offset.reset=none` sees strictly increasing offsets per partition and never hits an out-of-range reset. This is the consumer-side reading of the goal and catches HW regressions and above-HW serving after failover. It is simple: an in-memory `lastSeen[partition]` check in the poll loop, run in the same driver.

Why these two: together they cover both client-visible meanings of "offsets are monotonically increasing", and both run as in-memory checks in one driver, with no `finally_` read-back or file handoff between commands.

## Deferred (outside the current focus)

- **latest-offset-never-regresses**: a valid monotonicity check, but it tests a derived metadata value (ListOffsets LATEST), not record offsets. Part of its target (HW regression after failover) is already observable through the consumer property.
- **acked-offset-record-stable**: the strongest durability form, but it needs a `finally_` command, an acked-record file handoff, recovery waits, and a decision on node-wipe semantics (an open human-input question). It costs more than it is worth in a small pass.

## Budget split

Hard limits: 5 SDK assertions, 2 test commands.

| Property | Assertions | Test commands |
|---|---|---|
| acked-offsets-strictly-increase | 1 Always (`acked produce offsets strictly increase per partition per producer`) + 1 Sometimes (`produce acked after a leader change`) | shared `parallel_driver_offsets` |
| consumer-offsets-strictly-increase | 1 Always (`consumer delivers strictly increasing offsets per partition`, one helper used at both call sites) + 1 Sometimes (`consumer delivered a record after a leader change`) | shared `parallel_driver_offsets` |
| **Total** | **4 of 5** (1 spare) | **1 of 2** (1 spare) |

Topology: unchanged at 3 combined KRaft broker+controller nodes + 1 client. That is the minimum for RF=3 / min.insync=2 failover with a fault-tolerant quorum (see `deployment-topology.md`).
