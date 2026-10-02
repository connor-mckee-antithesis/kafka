# acked-writes test template

Test command for `acked-writes-never-lost` and `acked-write-verified-across-leader-change`:

- `parallel_driver_produce_and_verify` (`AckedWritesDriver`): produces a bounded batch with an
  acks=all idempotent producer and appends acked `partition offset id length` lines to
  `/tmp/acked-ledger/<run>.log`. Then it snapshots every ledger file and scans each partition
  from a randomly chosen acked offset, which keeps scans bounded as the log grows. It asserts that no acked offset holds a different record or was skipped. A
  partition that is unreadable before the deadline counts as unverified, not lost.

Produce and verify share one command because this pass allows only one new test command.
Verification therefore runs while faults are active. Later runs re-verify records from earlier
runs, so records acked long ago are checked again after more faults.

Run workload classes with `java -ea -cp "$WORKLOAD_CP" <MainClass>`. `WORKLOAD_CP` is
set in the client image. Files prefixed `helper_` are ignored by Antithesis.
