# Decisions (non-interactive run)

- External references: "just this directory" (provided by the automation script) -> `external_references: []`.
- Subsystem focus: partition log offset assignment, high watermark, replication truncation, consumer fetch path — the minimum needed for "offsets are monotonically increasing".
- Used single-agent workflows for SUT discovery, property discovery and evaluation (not ensembles) — the run brief asks for a small, fast pass.
- Catalog capped at 4 properties (brief limit), even though the reference suggests 15+.
- Assertion budget planned at 5 total (4 Always + 1 Sometimes); test-command budget planned at 2 (one parallel_driver_, one finally_) — hard limits from the brief.
- Chose workload-only assertions (no SUT-side Java SDK instrumentation) — client-visible checks cover the customer goal, and the budget leaves no room for SUT-side anchors.
- Topology: 3 combined broker+controller KRaft nodes + 1 client — this is the smallest setup where RF=3/min.insync=2 leader failover and follower truncation can happen.
- Evaluation biases: none needed human escalation; recorded in evaluation/synthesis.md.
- Evaluation gap (LSO, log-start-offset monotonicity) deferred rather than filled — the 4-property cap in the brief.
- Node-kill wipe question left as (needs human input); the workload should set flush.messages=1 and the launcher can leave node faults at the tenant default.
- Focus pass: chose acked-offsets-strictly-increase and consumer-offsets-strictly-increase as P0 — they test the goal directly (producer and consumer views) and are in-memory checks in one driver, so they are the simplest to build.
- Deferred latest-offset-never-regresses (derived metadata value, partly covered via consumer) and acked-offset-record-stable (needs a finally_ command, state handoff, and an unresolved node-wipe question).
- Budget: 4 of 5 assertions (2 Always + 2 Sometimes leader-change anchors, one anchor per property so neither passes vacuously), 1 of 2 test commands (parallel_driver_offsets); one of each held in reserve.
- Topology kept at 3 KRaft combined nodes + 1 client — fewer nodes would remove failover or the quorum's fault tolerance.

## Setup (antithesis-setup, non-interactive)
- Provenance confirmed by the run brief (sut-analysis/deployment-topology: /home/exedev/kafka @ ae5d57b77328) -> proceeded.
- No bootstrap SDK assertion added in setup -> the brief forbids new assertions unless snouty validate needs them; the workload step adds all assertions within the 5-assertion budget.
- No SUT (broker) instrumentation or SDK -> research chose workload-only assertions; broker JVM coverage is optional and out of scope this pass.
- Kafka tarball built inside a Docker builder stage (eclipse-temurin:17-jdk, `./gradlew releaseTarGz`) -> the host has no JDK; the docker/jvm Dockerfile (alpine, JSA stage) was not reused, to avoid musl images and the JSA build-time broker.
- Antithesis Java SDK 1.7.0 (latest Maven release) + ffi jar installed in the client image at /opt/workload/lib, linked from /opt/antithesis/catalog/workload -> jackson-databind comes from /opt/kafka/libs.
- Topic offsets-test created by the client entrypoint, which then emits setup_complete -> matches deployment-topology.md.
- Test templates are baked into the client image from antithesis/test/ (currently empty; the workload step adds v1/offsets/parallel_driver_offsets).
- Build-context excludes go in antithesis/Dockerfile.dockerignore rather than a root .dockerignore -> avoids touching repo-wide Docker builds.
- Kept broker.heartbeat.interval.ms=500 / broker.session.timeout.ms=3000 from research even though local validate showed occasional heartbeat RPC timeouts (500ms) at startup -> they drive the failovers the P0 properties need; revisit if Antithesis shows constant fencing churn with no successful produces.
- Self-review done in-context against the setup skill checklist (no separate reviewer agent) -> small pass; every item checked except the bootstrap assertion, which was skipped on purpose (see above).

## Workload: acked-offsets-strictly-increase (antithesis-workload, non-interactive)
- Used 2 of the 3 assertions allowed this session (Always strict-increase + Sometimes "produce acked after a leader change") and 1 command (parallel_driver_offsets) -> these are the catalog's planned names. Pass total is now 2/5 assertions and 1/2 commands. consumer-offsets-strictly-increase should extend this driver rather than add a command.
- Strict-increase is checked against the nearest acked neighbours by send sequence, not against the "last callback" -> this stays correct even if callbacks were reordered, and it also catches duplicate offsets.
- Leader change is detected by polling describeTopics leader ids every 500 ms -> clients see no leader epoch, so an A->B->A flip between polls can be missed; that is acceptable for a reach claim.
- Driver runtime is 20-60 s (SDK random). Menus: batch.size {1,512,16384}, linger {0,5,50}, value size {0,1,100,1024}, burst {1,10,200}, max.in.flight 1..5 -> sized for a 6-minute run with several overlapping drivers.
- The test command is a 3-line bash exec wrapper around the compiled Java class -> all workload logic is Java. The jar is built in the client image into /opt/workload/lib so the assertions are cataloged.
- Catalog frontmatter commit left at ae5d57b773 -> HEAD is unchanged.
- Self-review done in-context, not by a separate sub-agent -> small single-property pass.

## Workload: consumer-offsets-strictly-increase (antithesis-workload, non-interactive)
- Extended parallel_driver_offsets with a consumer thread instead of adding a command -> pass total is now 4/5 assertions and 1/2 commands. The new assertions are Always "consumer delivers strictly increasing offsets per partition" and Sometimes "consumer delivered a record after a leader change".
- The starting position needs one seek (with auto.offset.reset=none, assign() has no position). The seek target is random, seekToBeginning or seekToEnd -> one menu covers both a long catch-up read and a tail-following read. There are no seeks after that.
- On OffsetOutOfRangeException (incl. LogTruncationException) the code asserts false with the offending offset, then stops consuming -> one report is enough, and re-seeking would hide the state.
- After producer.close, the consumer drains for 3 s, and leader polling continues during the drain -> the consumer sees the tail of acked data.
- consumed > acked can happen (sends that timed out on the client may still have been written) -> expected, not asserted.
- Catalog commit left at ae5d57b773 (HEAD unchanged). Self-review done in-context -> small single-property pass.

## Triage: fd445b8cd8821d2bdce448bc4b738297-64-0 (antithesis-triage, non-interactive)
- Downloaded 2 logs (a completed driver and the first leader-change example) and no more -> all properties passed, so 2 histories were enough to confirm the conditions being tested. Temporary log files were deleted after analysis.
- Reported the "Always: Missed Always" grouping and the missing "Software was instrumented" property as a likely catalog issue, but did not call it a failure -> the report has no failing property, and the cause is inferred, not confirmed.

## Recommendation (non-interactive)
- Outcome set to passed -> run completed and both selected properties passed with exercised leader-change anchors; the "Missed Always" catalog grouping is flagged as a caveat, not a failure, since the assertions did fire.
