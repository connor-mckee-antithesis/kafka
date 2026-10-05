---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Evaluation Synthesis

Single-agent evaluation (see `../decisions.md`).

| # | Finding | Category | Action |
|---|---|---|---|
| 1 | Default session timeouts may make failovers rare | Refinement | Added to deployment topology: `broker.session.timeout.ms=3000`, `broker.heartbeat.interval.ms=500` |
| 2 | Finally read-back must tolerate recovery delay but not hide loss | Refinement | Applied to the acked-offset-record-stable catalog entry |
| 3 | Consumer property evaluated at two sites | Refinement | Applied: one helper method, one message |
| 4 | Leader-change anchor needs explicit leader tracking | Refinement | Applied: poll `describeTopics` between batches (catalog anchor text) |
| 5 | LSO/read_committed and log-start-offset monotonicity uncovered | Gap | Deferred: the brief caps the catalog at 4 properties / 5 assertions. Recorded for a future pass. |
| 6 | Node-kill full-wipe makes acked-data loss legitimate | Bias-adjacent / open question | Surfaced in the catalog as `(needs human input)`. No human available in this run; recorded in decisions.md |

No biases requiring a redirect of the catalog were found.
