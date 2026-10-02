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

# Antithesis harness images for Apache Kafka. Build context is the repo root:
#   docker compose -f antithesis/config/docker-compose.yaml build
#
# Targets:
#   broker - KRaft combined broker+controller built from this checkout
#   client - workload image (kafka-clients + Antithesis Java SDK)

ARG ANTITHESIS_SDK_VERSION=1.7.0

############################ build: release tarball ############################
FROM docker.io/library/eclipse-temurin:17-jdk AS build

ENV NO_COLOR=1 \
    TERM=dumb \
    GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.console=plain"
ARG COMMIT_ID=unknown

WORKDIR /src
COPY . /src

RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon --console=plain -PcommitId=${COMMIT_ID} -PskipSigning=true \
        releaseTarGz -x siteDocsTar -x javadoc \
 && mkdir -p /opt/kafka \
 && tar xzf "$(ls core/build/distributions/kafka_*.tgz | grep -v site-docs)" -C /opt/kafka --strip-components 1 \
 && rm -rf /opt/kafka/site-docs

# Split Kafka's own jars out of libs/ so Antithesis instruments only Kafka code
# (not the ~100 third-party jars). Both dirs end up on the classpath.
RUN mkdir -p /opt/kafka/instrumented \
 && cd /opt/kafka/libs \
 && mv kafka_2.13-*.jar kafka-clients-*.jar kafka-server-*.jar kafka-metadata-*.jar \
       kafka-raft-*.jar kafka-storage-*.jar kafka-*coordinator*.jar \
       /opt/kafka/instrumented/ \
 && ls -1 /opt/kafka/instrumented

########################## workload: compile client code ########################
FROM docker.io/library/eclipse-temurin:17-jdk AS workload-build

ARG ANTITHESIS_SDK_VERSION
ENV NO_COLOR=1

ADD https://repo1.maven.org/maven2/com/antithesis/sdk/${ANTITHESIS_SDK_VERSION}/sdk-${ANTITHESIS_SDK_VERSION}.jar /opt/workload/lib/
ADD https://repo1.maven.org/maven2/com/antithesis/ffi/${ANTITHESIS_SDK_VERSION}/ffi-${ANTITHESIS_SDK_VERSION}.jar /opt/workload/lib/
COPY --from=build /opt/kafka /opt/kafka
COPY antithesis/workload/src /src/workload

RUN mkdir -p /tmp/classes \
 && find /src/workload -name '*.java' > /tmp/sources.txt \
 && javac --release 17 -d /tmp/classes \
      -cp "/opt/workload/lib/*:/opt/kafka/instrumented/*:/opt/kafka/libs/*" @/tmp/sources.txt \
 && mkdir -p /opt/workload/app \
 && jar --create --file /opt/workload/app/kafka-antithesis-workload.jar \
      --main-class org.apache.kafka.antithesis.ClientSetup -C /tmp/classes .

#################################### broker ####################################
FROM docker.io/library/eclipse-temurin:17-jre AS broker

ENV NO_COLOR=1 \
    TERM=dumb \
    KAFKA_HOME=/opt/kafka \
    CLASSPATH=/opt/kafka/instrumented/* \
    PATH=/opt/kafka/bin:$PATH

COPY --from=build /opt/kafka /opt/kafka
COPY antithesis/images/broker-entrypoint.sh /usr/local/bin/broker-entrypoint.sh

RUN chmod +x /usr/local/bin/broker-entrypoint.sh \
 && mkdir -p /var/lib/kafka/data /opt/antithesis/catalog \
 && ln -s /opt/kafka/instrumented /opt/antithesis/catalog/kafka

EXPOSE 9092 9093
CMD ["/usr/local/bin/broker-entrypoint.sh"]

#################################### client ####################################
FROM docker.io/library/eclipse-temurin:17-jre AS client

ENV NO_COLOR=1 \
    TERM=dumb \
    KAFKA_HOME=/opt/kafka \
    PATH=/opt/kafka/bin:$PATH \
    CLASSPATH=/opt/kafka/instrumented/* \
    WORKLOAD_CP=/opt/workload/app/*:/opt/workload/lib/*:/opt/kafka/instrumented/*:/opt/kafka/libs/*

COPY --from=build /opt/kafka /opt/kafka
COPY --from=workload-build /opt/workload /opt/workload
COPY antithesis/images/client-entrypoint.sh /usr/local/bin/client-entrypoint.sh
COPY antithesis/test/ /opt/antithesis/test/

RUN chmod +x /usr/local/bin/client-entrypoint.sh \
 && mkdir -p /opt/antithesis/catalog /tmp/acked-ledger \
 && ln -s /opt/workload/app /opt/antithesis/catalog/workload

CMD ["/usr/local/bin/client-entrypoint.sh"]
