#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# KRaft combined broker+controller for the Antithesis harness.
# Storage is formatted only once, so data survives Antithesis kill/restart.
set -euo pipefail

: "${KAFKA_NODE_ID:?KAFKA_NODE_ID must be set}"
: "${KAFKA_CLUSTER_ID:?KAFKA_CLUSTER_ID must be set}"
: "${KAFKA_QUORUM_VOTERS:?KAFKA_QUORUM_VOTERS must be set}"
HOST="$(hostname)"
DATA_DIR="${KAFKA_DATA_DIR:-/var/lib/kafka/data}"
CONFIG=/opt/kafka/config/antithesis-server.properties

cat > "$CONFIG" <<EOF
process.roles=broker,controller
node.id=${KAFKA_NODE_ID}
controller.quorum.voters=${KAFKA_QUORUM_VOTERS}
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://${HOST}:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=${DATA_DIR}

# Durability configuration under test (see scratchbook/sut-analysis.md).
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
num.partitions=3
auto.create.topics.enable=false

# Shorten timers so ISR shrink/expand and failover happen within a timeline.
replica.lag.time.max.ms=${KAFKA_REPLICA_LAG_TIME_MAX_MS:-5000}
group.initial.rebalance.delay.ms=0
EOF

mkdir -p "$DATA_DIR"
if [[ ! -f "$DATA_DIR/meta.properties" ]]; then
  echo "Formatting storage in $DATA_DIR for cluster $KAFKA_CLUSTER_ID"
  kafka-storage.sh format --ignore-formatted -t "$KAFKA_CLUSTER_ID" -c "$CONFIG"
else
  echo "Storage in $DATA_DIR already formatted; reusing"
fi

export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
exec kafka-server-start.sh "$CONFIG"
