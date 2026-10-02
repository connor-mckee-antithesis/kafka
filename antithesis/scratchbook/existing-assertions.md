---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Existing Antithesis Assertions

## Summary

Searched all `*.java`, `*.scala` and `*.gradle` files for `com.antithesis` (SDK imports/dependency) and
for assertion calls (`Assert.always`, `Assert.sometimes`, `Assert.reachable`, `Assert.unreachable`,
`alwaysOrUnreachable`). **None found.** The codebase has no Antithesis instrumentation and no SDK
dependency in `build.gradle`.

## Assumptions

- The Java SDK (`com.antithesis:sdk`) will need to be added as a dependency of `core` (for SUT-side
  assertions in `Partition.scala`) and `metadata` (for `PartitionChangeBuilder.java`), and of the
  workload project.

## Open Questions

- None.
