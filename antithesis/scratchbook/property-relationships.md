---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Property Relationships

## Summary

There is one cluster, built around the acked-write lifecycle: **ack → replicate → (failover) →
truncate/recover → read back**. The two P0 properties are the selected pair. The two SUT-side
properties are deferred (outside the current focus).

## Cluster: acked-write durability

**Selected (P0):**

- `acked-writes-never-lost`: the end-to-end oracle. It **dominates** both deferred safety
  properties, because any loss they would cause shows up here eventually. Its weakness is that it
  fires late and does not localize the cause.
- `acked-write-verified-across-leader-change`: the anti-vacuity guard for
  `acked-writes-never-lost`. Both run in the same command (`parallel_driver_produce_and_verify`) and share
  one scan. A pass of the oracle means something only if this guard is also hit.

**Deferred: outside the current focus:**

- `ack-implies-min-isr-replicated`: localizes the "acked too early" root cause. A violation here
  likely becomes a violation of `acked-writes-never-lost` only if the under-replicated leader
  then dies.
- `follower-never-truncates-below-hw`: localizes the "committed data deleted" root cause, firing
  at deletion time.

These localizers are the first candidates for the 3 spare assertion slots if
`acked-writes-never-lost` fails and the cause needs pinning down.

Suspected interplay: the fsync/page-cache open question affects `acked-writes-never-lost` (and
the deferred `follower-never-truncates-below-hw`), because both can fire legitimately if a node
kill drops unflushed data.

## Assumptions

- The SUT-side properties are root-cause localizers, not independent guarantees, so deferring
  them does not reduce coverage of the goal.

## Open Questions

- None beyond the catalog's.
