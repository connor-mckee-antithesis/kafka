---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Lens 3: Implementability

## Findings
- acked-offset-record-stable: the finally command must not report a violation just because the cluster has not recovered yet. Suggested action: wait (bounded, e.g. 120s) for each partition to have a leader and an HW ≥ max acked offset + 1. Skip the check (log it, do not assert) only if no leader appears; an HW below an acked offset with a live leader is a violation. Scope: property-specific.
- consumer-offsets-strictly-increase evaluates in two places (per record, and in the out-of-range handler). To keep one message per property, route both through one helper method. Scope: property-specific.
- Anchor detection: the producer hides `NOT_LEADER_OR_FOLLOWER` retries. Suggested action: the driver polls `Admin.describeTopics` between batches and records the leader per partition. "Leader changed between two acks" sets the Sometimes condition. Scope: property-specific.
- Several `parallel_driver_offsets` instances may run at once. Each owns its producer, consumer and Admin state, so per-instance invariants are sound. Acked logs go to per-PID files.

## Passes
- The topology supplies everything needed: 3 brokers for RF=3, one client.
