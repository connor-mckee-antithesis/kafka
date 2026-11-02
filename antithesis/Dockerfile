# syntax=docker/dockerfile:1
# Antithesis harness images for Apache Kafka. Build context is the repo root.
#   kafka  - combined broker+controller KRaft node built from this checkout
#   client - workload container (JDK, Kafka CLI/libs, Antithesis Java SDK)

ARG ANTITHESIS_SDK_VERSION=1.7.0

FROM docker.io/library/eclipse-temurin:17-jdk AS builder
ARG ANTITHESIS_SDK_VERSION
ARG COMMIT_ID=unknown
ENV NO_COLOR=1 TERM=dumb
RUN apt-get update && apt-get install -y --no-install-recommends curl git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon --console=plain -PcommitId="${COMMIT_ID}" releaseTarGz -x test \
 && mkdir -p /out/kafka \
 && tgz="$(ls core/build/distributions/kafka_*.tgz | grep -v site-docs)" \
 && tar xzf "$tgz" -C /out/kafka --strip-components 1 \
 && rm -rf /out/kafka/site-docs
RUN mkdir -p /out/sdk \
 && base=https://repo.maven.apache.org/maven2/com/antithesis \
 && curl -fsSL -o /out/sdk/antithesis-sdk-${ANTITHESIS_SDK_VERSION}.jar ${base}/sdk/${ANTITHESIS_SDK_VERSION}/sdk-${ANTITHESIS_SDK_VERSION}.jar \
 && curl -fsSL -o /out/sdk/antithesis-ffi-${ANTITHESIS_SDK_VERSION}.jar ${base}/ffi/${ANTITHESIS_SDK_VERSION}/ffi-${ANTITHESIS_SDK_VERSION}.jar

FROM docker.io/library/eclipse-temurin:17-jre AS kafka
ENV NO_COLOR=1 TERM=dumb \
    KAFKA_HEAP_OPTS="-Xms512m -Xmx512m"
COPY --from=builder /out/kafka /opt/kafka
COPY antithesis/docker/kafka-entrypoint.sh /usr/local/bin/kafka-entrypoint.sh
RUN chmod +x /usr/local/bin/kafka-entrypoint.sh && mkdir -p /var/lib/kafka/data
EXPOSE 9092 9093
CMD ["/usr/local/bin/kafka-entrypoint.sh"]

FROM docker.io/library/eclipse-temurin:17-jdk AS client
ENV NO_COLOR=1 TERM=dumb \
    KAFKA_HEAP_OPTS="-Xms128m -Xmx256m"
COPY --from=builder /out/kafka /opt/kafka
COPY --from=builder /out/sdk /opt/workload/lib
# Antithesis scans /opt/antithesis/catalog for Java assertions; workload jars
# placed in /opt/workload/lib are picked up through this single-hop symlink.
RUN mkdir -p /opt/antithesis/catalog && ln -s /opt/workload/lib /opt/antithesis/catalog/workload
COPY antithesis/setup-complete.sh /opt/antithesis/setup-complete.sh
COPY antithesis/docker/client-entrypoint.sh /usr/local/bin/client-entrypoint.sh
COPY antithesis/test/ /opt/antithesis/test/
# Workload jar lands in /opt/workload/lib so Antithesis catalogs its assertions.
COPY antithesis/workload/src/ /opt/workload/src/
RUN mkdir -p /tmp/wl-classes \
 && javac -d /tmp/wl-classes -cp "/opt/workload/lib/*:/opt/kafka/libs/*" /opt/workload/src/*.java \
 && jar cf /opt/workload/lib/antithesis-workload.jar -C /tmp/wl-classes . \
 && rm -rf /tmp/wl-classes
RUN chmod +x /opt/antithesis/setup-complete.sh /usr/local/bin/client-entrypoint.sh \
 && find /opt/antithesis/test -type f -name '*_*' ! -name 'helper_*' -exec chmod +x {} +
CMD ["/usr/local/bin/client-entrypoint.sh"]
