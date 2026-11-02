/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.antithesis;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.antithesis.sdk.Assert;
import com.antithesis.sdk.Random;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test command for {@code acked-writes-never-lost} and
 * {@code acked-write-verified-across-leader-change}.
 *
 * <p>Phase 1 produces a bounded batch with acks=all + idempotence and appends every acked
 * {@code partition offset id length} to a ledger file in {@code /tmp/acked-ledger/}. Phase 2
 * snapshots every ledger file (this run's and all earlier/concurrent runs') and scans each
 * partition from a random acked offset to check that each acked offset still holds the acked record.
 *
 * <p>This runs while faults are active, so only definite evidence counts as loss: a different
 * record at an acked offset, or an acked offset skipped while later offsets were read. A
 * partition that can't be read up to its highest acked offset before the deadline is reported
 * as unverified (unavailability), not loss.
 */
public final class AckedWritesDriver {

    static final Path LEDGER_DIR = Paths.get("/tmp/acked-ledger");

    // Value menu. Record counts: single record, one batch, several batches. Padding: empty,
    // one byte, small, around batch.size (16384) so batches split, and a large record.
    static final List<Integer> RECORD_COUNTS = List.of(1, 10, 100, 300);
    static final List<Integer> PADDINGS = List.of(0, 1, 100, 16_383, 16_384, 100_000);
    static final List<Integer> LINGER_MS = List.of(0, 5, 50);

    static final Duration VERIFY_DEADLINE = Duration.ofSeconds(90);

    private AckedWritesDriver() { }

    record Acked(int partition, long offset, String id, int length) { }

    public static void main(String[] args) throws Exception {
        Files.createDirectories(LEDGER_DIR);
        String runId = Long.toHexString(Random.getRandom());
        produce(runId);
        verify();
    }

    // ------------------------------------------------------------------ produce

    private static void produce(String runId) throws IOException {
        int count = Random.randomChoice(RECORD_COUNTS);
        int linger = Random.randomChoice(LINGER_MS);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ClientSetup.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(linger));
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Path ledgerFile = LEDGER_DIR.resolve(runId + ".log");
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        try (BufferedWriter ledger = Files.newBufferedWriter(ledgerFile, StandardCharsets.UTF_8,
                 StandardOpenOption.CREATE, StandardOpenOption.APPEND);
             KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String id = runId + "-" + i;
                int padding = Random.randomChoice(PADDINGS);
                String value = id + ":" + "x".repeat(padding);
                int partition = (int) Long.remainderUnsigned(Random.getRandom(), ClientSetup.PARTITIONS);
                try {
                    producer.send(new ProducerRecord<>(ClientSetup.TOPIC, partition, null, value), (md, e) -> {
                        if (e != null) {
                            failed.incrementAndGet();
                            return;
                        }
                        // Only acked records go in the ledger; failed ones may or may not be in the log.
                        String line = md.partition() + " " + md.offset() + " " + id + " " + value.length() + "\n";
                        synchronized (ledger) {
                            try {
                                ledger.write(line);
                                ledger.flush();
                            } catch (IOException io) {
                                log("ledger write failed: " + io);
                            }
                        }
                        acked.incrementAndGet();
                    });
                } catch (Exception e) {
                    failed.incrementAndGet();
                    log("send failed: " + e);
                }
            }
            producer.flush();
        } catch (Exception e) {
            log("producer error: " + e);
        }
        log("produce run " + runId + ": attempted=" + count + " acked=" + acked.get()
            + " failed=" + failed.get() + " linger=" + linger);
    }

    // ------------------------------------------------------------------- verify

    private static Map<Integer, TreeMap<Long, Acked>> loadLedger(List<Acked[]> conflicts) throws IOException {
        Map<Integer, TreeMap<Long, Acked>> byPartition = new HashMap<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(LEDGER_DIR, "*.log")) {
            for (Path f : files) {
                String content = Files.readString(f, StandardCharsets.UTF_8);
                // A concurrent writer may have a partial last line; only take complete lines.
                int end = content.lastIndexOf('\n');
                if (end < 0) continue;
                for (String line : content.substring(0, end).split("\n")) {
                    String[] p = line.trim().split(" ");
                    Acked a;
                    try {
                        a = new Acked(Integer.parseInt(p[0]), Long.parseLong(p[1]), p[2], Integer.parseInt(p[3]));
                    } catch (RuntimeException e) {
                        log("skipping malformed ledger line in " + f + ": " + line);
                        continue;
                    }
                    Acked prev = byPartition.computeIfAbsent(a.partition(), k -> new TreeMap<>()).putIfAbsent(a.offset(), a);
                    // Two different records acked at the same offset: one of them was lost.
                    if (prev != null && !prev.id().equals(a.id())) conflicts.add(new Acked[] {prev, a});
                }
            }
        }
        return byPartition;
    }

    private static void verify() throws IOException {
        List<Acked[]> conflicts = new ArrayList<>();
        Map<Integer, TreeMap<Long, Acked>> ledger = loadLedger(conflicts);
        if (ledger.isEmpty()) {
            log("verify: ledger is empty, nothing to check");
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ClientSetup.bootstrapServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Start each partition's scan at a random acked offset rather than 0, so the scan stays
        // bounded as the log grows and runs still reach the most recent acks before the deadline.
        // Across runs every acked offset is still re-checked.
        List<TopicPartition> tps = new ArrayList<>();
        Map<TopicPartition, Long> target = new HashMap<>();
        Map<TopicPartition, Long> nextExpected = new HashMap<>();
        for (Map.Entry<Integer, TreeMap<Long, Acked>> e : ledger.entrySet()) {
            TopicPartition tp = new TopicPartition(ClientSetup.TOPIC, e.getKey());
            long start = Random.randomChoice(new ArrayList<>(e.getValue().keySet()));
            e.setValue(new TreeMap<>(e.getValue().tailMap(start, true)));
            tps.add(tp);
            target.put(tp, e.getValue().lastKey());
            nextExpected.put(tp, start);
        }

        int checked = 0;
        int confirmed = 0;
        int confirmedOldEpoch = 0;
        int lost = 0;
        int unverified = 0;
        ArrayNode lostDetails = new ObjectMapper().createArrayNode();
        Map<TopicPartition, Integer> maxEpoch = new HashMap<>();
        // Acked entries whose record was read, keyed by partition, with the record's leader epoch.
        Map<TopicPartition, List<Integer>> confirmedEpochs = new HashMap<>();
        for (Acked[] c : conflicts) {
            checked++;
            lost++;
            addLost(lostDetails, c[1], "same offset also acked for " + c[0].id());
        }

        long deadline = System.nanoTime() + VERIFY_DEADLINE.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(tps);
            for (TopicPartition tp : tps) consumer.seek(tp, nextExpected.get(tp));
            List<TopicPartition> pending = new ArrayList<>(tps);
            while (!pending.isEmpty() && System.nanoTime() < deadline) {
                try {
                    for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
                        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                        long expected = nextExpected.getOrDefault(tp, 0L);
                        if (r.offset() < expected) continue; // re-read after an offset reset
                        TreeMap<Long, Acked> acked = ledger.get(r.partition());
                        // Offsets are dense (no transactions, no compaction, no retention), so
                        // every acked offset in [expected, r.offset()) was skipped.
                        for (Acked skipped : acked.subMap(expected, true, r.offset(), false).values()) {
                            checked++;
                            lost++;
                            addLost(lostDetails, skipped, "offset skipped; next offset read was " + r.offset());
                        }
                        nextExpected.put(tp, r.offset() + 1);
                        int epoch = r.leaderEpoch().orElse(-1);
                        maxEpoch.merge(tp, epoch, Math::max);
                        Acked a = acked.get(r.offset());
                        if (a == null) continue;
                        checked++;
                        String v = r.value();
                        if (v != null && v.length() == a.length() && v.startsWith(a.id() + ":")) {
                            confirmed++;
                            confirmedEpochs.computeIfAbsent(tp, k -> new ArrayList<>()).add(epoch);
                        } else {
                            lost++;
                            String got = v == null ? "null" : v.substring(0, Math.min(v.length(), 40));
                            addLost(lostDetails, a, "different record at offset: " + got + " (len "
                                + (v == null ? 0 : v.length()) + ")");
                        }
                    }
                } catch (Exception e) {
                    log("poll failed (retrying): " + e);
                }
                pending.removeIf(tp -> nextExpected.getOrDefault(tp, 0L) > target.get(tp));
            }
            for (TopicPartition tp : pending) {
                long from = nextExpected.getOrDefault(tp, 0L);
                int n = ledger.get(tp.partition()).tailMap(from, true).size();
                unverified += n;
                log("verify: " + tp + " not readable past offset " + from + " before deadline; "
                    + n + " acked records unverified (unavailability, not loss)");
            }
        } catch (Exception e) {
            log("consumer error: " + e);
            return;
        }

        for (Map.Entry<TopicPartition, List<Integer>> e : confirmedEpochs.entrySet()) {
            int latest = maxEpoch.getOrDefault(e.getKey(), -1);
            for (int epoch : e.getValue()) {
                if (epoch >= 0 && epoch < latest) confirmedOldEpoch++;
            }
        }

        ObjectNode details = new ObjectMapper().createObjectNode();
        details.put("checked", checked);
        details.put("confirmed", confirmed);
        details.put("lost", lost);
        details.put("unverified", unverified);
        details.put("confirmed_older_leader_epoch", confirmedOldEpoch);
        details.set("first_lost", lostDetails);
        log("verify: " + details);

        Assert.always(lost == 0, "acked record present at its acked offset with acked payload", details);
        Assert.sometimes(confirmedOldEpoch > 0, "acked record verified after its partition changed leader", details);
    }

    private static void addLost(ArrayNode out, Acked a, String reason) {
        if (out.size() >= 10) return;
        ObjectNode n = out.addObject();
        n.put("partition", a.partition());
        n.put("offset", a.offset());
        n.put("id", a.id());
        n.put("reason", reason);
    }

    static void log(String msg) {
        System.out.println("[acked-writes] " + msg);
        System.out.flush();
    }
}
