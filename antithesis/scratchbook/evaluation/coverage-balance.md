---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Lens 2: Coverage Balance

## Findings

- **Catalog-wide.** The catalog covers ack-time correctness, truncation, and end-to-end readback.
  Broker-restart log recovery (`LogLoader` truncating a torn tail) and controller-side election
  correctness have no dedicated SUT assertion. The end-to-end oracle covers them indirectly.
  The 4-property cap prevents adding more. Recorded as a deferred gap.
- **Catalog-wide.** Transactional producers, consumer-group offsets, and tiered storage are out of
  scope. They are not needed for "acked writes" in the plain-producer sense.
- **Passes.** There is one liveness/coverage property, so the safety oracles cannot pass vacuously.

## Uncertainties

- None.
