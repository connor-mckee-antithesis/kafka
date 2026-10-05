---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Lens 1: Antithesis Fit

## Findings
- All 4 properties depend on how leader elections interleave with appends, HW propagation and client reads. Deterministic unit tests (`UnifiedLogTest`, `PartitionTest`) cover each component alone, not the cross-node timing. **Pass.**
- acked-offsets-strictly-increase: violations need a fenced leader to ack, which requires a precise partition timing. Antithesis can plausibly reach it under network faults. **Pass.** Scope: property-specific.
- The `Sometimes` anchor is reachable under default network faults: partitioning the leader from the controller triggers re-election after `broker.session.timeout.ms` (9s by default). Suggested action: lower `broker.session.timeout.ms` and `broker.heartbeat.interval.ms` so failovers happen often within a run. Scope: property-specific (anchor).

## Uncertainties
- Whether 9s session timeouts leave enough failovers per timeline. Hence the suggestion above.
