---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Lens 3: Implementability

## Findings

- **Property-specific (`acked-writes-never-lost`).** Retention or compaction could remove acked
  records and produce false positives. Fix: create the topic with `retention.ms=-1`,
  `retention.bytes=-1`, `cleanup.policy=delete`. **Applied** to `deployment-topology.md`.
- **Property-specific (`acked-writes-never-lost`).** The producer driver must be bounded (fixed
  record count or duration) so that `eventually_` gets to run. Idempotent retries after
  `NOT_ENOUGH_REPLICAS_AFTER_APPEND` return the original offset, and the offset→uniqueId check
  handles that.
- **Property-specific (SUT-side).** Both SUT assertions need the Java SDK on the `core` and
  `metadata` classpaths. Scala can call `com.antithesis.sdk.Assert` directly. Details should be
  built only in the assertion branch so the hot path stays cheap.
- **Passes.** `ConsumerRecord.leaderEpoch()` is available in the clients API, so
  `acked-write-verified-across-leader-change` needs no SUT change.

## Uncertainties

- Whether Antithesis Java coverage instrumentation is applied to Scala classes in `core`. This
  does not block anything.
