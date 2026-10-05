---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Lens 2: Coverage Balance

## Findings
- Covered: the produce-ack view, the consumer view, the end-offset view, and offset identity. These are all four client-visible meanings of "monotonic offsets" found in the SUT analysis (section 12).
- Not covered: the LSO / read_committed view (transactions), and log-start-offset monotonicity (`deleteRecords`, retention). Both are monotonicity claims too. Scope: catalog-wide. Suggested action: defer; the brief caps the catalog at 4 properties and 5 assertions.
- Not covered: consumer group committed offsets (`__consumer_offsets`). The customer's phrase most likely means log offsets; committed group offsets are a separate subsystem (group coordinator).

## Passes
- Every property maps to a specific code guard named in the SUT analysis.
