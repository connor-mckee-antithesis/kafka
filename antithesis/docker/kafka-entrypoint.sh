#!/usr/bin/env bash
# Starts one combined broker+controller KRaft node for the Antithesis harness.
set -euo pipefail

: "${NODE_ID:?NODE_ID must be set}"
: "${CLUSTER_ID:?CLUSTER_ID must be set}"
: "${CONTROLLER_QUORUM_VOTERS:?CONTROLLER_QUORUM_VOTERS must be set}"

HOST="$(hostname)"
DATA_DIR=/var/lib/kafka/data
PROPS=/etc/kafka/server.properties

mkdir -p "$DATA_DIR" /etc/kafka
cat > "$PROPS" <<EOF
process.roles=broker,controller
node.id=${NODE_ID}
controller.quorum.voters=${CONTROLLER_QUORUM_VOTERS}
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://${HOST}:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=${DATA_DIR}
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
auto.create.topics.enable=false
broker.session.timeout.ms=3000
broker.heartbeat.interval.ms=500
group.initial.rebalance.delay.ms=0
EOF

if [[ ! -f "$DATA_DIR/meta.properties" ]]; then
  echo "Formatting storage for node ${NODE_ID} (cluster ${CLUSTER_ID})"
  /opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$PROPS"
fi

exec /opt/kafka/bin/kafka-server-start.sh "$PROPS"
