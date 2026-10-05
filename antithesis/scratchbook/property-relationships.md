---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Property Relationships

Current focus (see `focus.md`): the P0 properties are acked-offsets-strictly-increase and consumer-offsets-strictly-increase. latest-offset-never-regresses and acked-offset-record-stable are Deferred: outside the current focus.

## Failover offset integrity (P0 cluster)
Properties: acked-offsets-strictly-increase, consumer-offsets-strictly-increase (P0); acked-offset-record-stable (deferred)
Notes: all three break if a fenced or old leader acks or serves data that a new leader later overwrites (log divergence after truncation). The two P0 properties check monotonicity at the moment of observation, from the producer side (ack offsets) and the consumer side (delivered offsets). They catch transient problems the deferred final read-back cannot see: serving above-HW data, and out-of-range resets. acked-offset-record-stable would add the "offset names one record forever" check; it is deferred because it needs a `finally_` command and cross-command state.

## High watermark propagation
Properties: consumer-offsets-strictly-increase (P0); latest-offset-never-regresses (deferred)
Notes: both depend on the new leader's lagging HW being handled correctly (the OFFSET_NOT_AVAILABLE guard for ListOffsets; HW-bounded fetch for consumers). While latest-offset-never-regresses is deferred, an HW regression after failover can still show up through the consumer property (as an out-of-range error or re-delivery).

## Liveness anchors
- `Sometimes("produce acked after a leader change")`: precondition for acked-offsets-strictly-increase.
- `Sometimes("consumer delivered a record after a leader change")`: precondition for consumer-offsets-strictly-increase.
If either is never hit, the matching safety pass is vacuous.

## Open Questions
- None beyond those in the catalog.
