---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-02
external_references: []
---

# Evaluation Synthesis

Single-agent evaluation across 4 lenses (`antithesis-fit.md`, `coverage-balance.md`,
`implementability.md`, `wildcard.md`).

## Refinements (applied)

| Finding | Property | Action |
|---|---|---|
| Retention/compaction could delete acked records | `acked-writes-never-lost` | Topic created with infinite retention and `cleanup.policy=delete` (in `deployment-topology.md`). |
| Producer driver must be bounded so `eventually_` runs | `acked-writes-never-lost` | Already in the evidence file workload design. No catalog change. |
| Stale-high LEO blind spot | `ack-implies-min-isr-replicated` | Already recorded as a limitation in the evidence file. |

## Gaps (deferred)

| Gap | Reason not filled |
|---|---|
| No dedicated SUT assertion for broker-restart log recovery (`LogLoader` tail truncation) or for controller election correctness | Hard cap of 4 properties. The end-to-end oracle covers both indirectly. If `acked-writes-never-lost` fails without either SUT assertion firing, spend the 5th assertion budget slot here. |

## Biases (for human review; not resolvable by agents)

- **Fsync vs. replication durability model.** Kafka's default config does not fsync per write.
  Whether Antithesis node kills drop page cache determines whether simultaneous multi-replica
  kills can produce *by-design* acked-write loss. The judgment call: should the customer's goal be
  tested under Kafka's documented model (default flush, where loss from ≥2 concurrent crashes is
  out of scope) or under a stricter config (`flush.messages=1`)? Default taken for this
  non-interactive run: documented default config. Recorded in `decisions.md`.

## Passes

- Assertion types match their semantics, and every property ties directly to the goal.
- Budget: 4/5 assertions, 2/2 commands.
