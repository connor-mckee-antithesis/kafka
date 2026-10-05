---
sut_path: /home/exedev/kafka
commit: ae5d57b773281d4bc6cee0e3d52bbfe6dbe944b9
updated: 2026-10-05
external_references: []
---

# Lens 4: Wildcard

## Findings
- Node-kill semantics: Antithesis may wipe a restarted node's modified data. In KRaft, a broker whose log dir is wiped comes back with a fresh disk; the controller should treat its replicas as offline/new (directory id, KIP-858/966). If that does not happen correctly, it is a real bug the read-back would catch. But a wipe of 2+ replicas is legitimate loss. Scope: catalog-wide. This is already an open question with `(needs human input)`.
- The `latest-offset-never-regresses` check is per client. With one Admin client polling all brokers, it observes whichever leader metadata points to. That is exactly the stale-metadata window worth exploring. **Pass.**
