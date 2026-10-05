#!/usr/bin/env bash
# Workload container entrypoint: create the test topic once the cluster can
# host an RF=3 partition, signal setup_complete, then idle so Antithesis can
# run test commands from /opt/antithesis/test/v1.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
TOPIC="${TOPIC:-offsets-test}"

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$TOPIC" --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config flush.messages=1 \
    --config retention.ms=-1 \
    --config retention.bytes=-1 \
    --config unclean.leader.election.enable=false; do
  echo "Topic creation failed (cluster not ready yet?), retrying in 2s"
  sleep 2
done

# Wait until every partition has a leader and a full ISR before declaring ready.
fully_replicated() {
  local isr_counts
  isr_counts="$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC" \
    | sed -n 's/.*Isr: \([0-9,]*\).*/\1/p' | awk -F, 'NF == 3' | wc -l)" || return 1
  [[ "$isr_counts" -eq 3 ]]
}
until fully_replicated; do
  echo "Waiting for ${TOPIC} partitions to be fully replicated"
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"

/opt/antithesis/setup-complete.sh

exec sleep infinity
