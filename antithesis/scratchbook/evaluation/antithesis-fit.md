---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Lens 1: Antithesis Fit

## Findings

- **Passes.** All three safety properties depend on timing between faults and in-flight
  replication: a leader kill during `DelayedProduce`, ISR shrink racing HW advancement, and back-to-back
  leader changes during follower truncation. Antithesis is strong at exactly this kind of search, and
  unit tests (`PartitionTest`, `AbstractFetcherThreadTest`) only cover these with mocks.
- **Passes.** Each property's assertion type matches its semantics. `Always` covers the
  end-to-end oracle. `AlwaysOrUnreachable` covers the SUT checks on paths that do not run in every
  timeline. `Sometimes(cond)` is the anti-vacuity guard.
- **Concern (property-specific, `acked-writes-never-lost`).** The verification runs only in the
  `eventually_` command, which may not run in every timeline. The two SUT-side assertions give
  Antithesis earlier, per-event signal, which partly mitigates this. With 1 assertion of budget
  left, an `anytime_` verifier is not affordable (the command cap is reached).

## Uncertainties

- Page-cache semantics on node kill (see synthesis "Bias").
