---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Lens 4: Wildcard

## Findings

- **Catalog-wide.** The test's verdict depends on a fact outside the codebase: whether a node
  kill keeps OS page cache. Kafka's durability model deliberately trades fsync for replication.
  If the environment is harsher than Kafka's model, the test reports by-design loss. If it is
  gentler, the test can never exercise the `lastKnownElr` and unflushed-tail paths. This is a
  framing question, not a code question.
- **Property-specific (`follower-never-truncates-below-hw`).** A follower's local HW comes from a
  periodic checkpoint after restart, so it can be stale-low. That makes the assertion more
  permissive after restarts, never wrongly strict, which is acceptable.
- **Property-specific (`ack-implies-min-isr-replicated`).** It reads the same leader-side replica
  snapshot as the HW logic, so it cannot catch stale-high follower LEO. This limit is already
  recorded in the evidence file.

## Uncertainties

- None beyond the above.
