---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Existing Antithesis Assertions

Scan: searched `*.java`, `*.scala` and `*.gradle` for `com.antithesis` (SDK imports) and for assertion calls (`Assert.always`, `sometimes`, `reachable`, `unreachable`).

Result: **no Antithesis SDK imports or assertions exist in the codebase.** The repo has no Antithesis instrumentation.

Non-SDK guardrails relevant to the goal (not Antithesis assertions):
- `UnifiedLog.java:1591` throws `OffsetsOutOfOrderException` (non-monotonic offsets within an append).
- `UnifiedLog.java:1217` throws `UnexpectedAppendOffsetException` (follower append below the LEO).
- `UnifiedLog.java:628` logs a warning on a non-monotonic HW update.
