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

# Workload container: prepare the cluster, emit setup_complete, then idle so
# Antithesis can run test commands from /opt/antithesis/test/v1/.
set -euo pipefail

mkdir -p /tmp/acked-ledger
# Run setup only once: after an Antithesis kill/restart the container filesystem
# (and this marker) survives, and the cluster may be faulted at that moment.
if [[ ! -f /tmp/setup-done ]]; then
  java -ea -Xmx256m -cp "$WORKLOAD_CP" org.apache.kafka.antithesis.ClientSetup
  touch /tmp/setup-done
fi
exec sleep infinity
