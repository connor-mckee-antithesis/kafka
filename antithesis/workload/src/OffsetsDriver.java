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

import com.antithesis.sdk.Assert;
import com.antithesis.sdk.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * parallel_driver_offsets: one idempotent acks=all producer per invocation.
 *
 * Property acked-offsets-strictly-increase: for this producer and one partition,
 * offsets returned in successful callbacks strictly increase in send order. The
 * check is independent of callback order: every acked record is compared with its
 * nearest acked neighbours by send sequence, so a reused or lower offset is caught
 * whichever callback fires first.
 *
 * Property consumer-offsets-strictly-increase: a concurrent assign() consumer with
 * auto.offset.reset=none sees per-partition offsets that strictly increase across
 * polls and never hits OffsetOutOfRangeException (incl. LogTruncationException).
 * Retention is disabled on the topic, so out-of-range is never legitimate.
 */
public class OffsetsDriver {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int PARTITIONS = 3;

    // Value menus (see antithesis/scratchbook/properties/acked-offsets-strictly-increase.md).
    // Record sizes: empty, 1 byte, small, ~1 batch-size boundary for the small batch.size.
    private static final int[] VALUE_SIZES = {0, 1, 100, 1024};
    // batch.size: tiny (one record per batch) up to the default 16 KiB.
    private static final int[] BATCH_SIZES = {1, 512, 16384};
    private static final int[] LINGER_MS = {0, 5, 50};
    // Records per burst before a short pause; 1 keeps batches singletons, 200 fills them.
    private static final int[] BURSTS = {1, 10, 200};
    // Consumer menus (see consumer-offsets-strictly-increase.md): single-record polls up
    // to the default 500; fetch wait short or default.
    private static final int[] MAX_POLL_RECORDS = {1, 10, 500};
    private static final int[] FETCH_MAX_WAIT_MS = {10, 500};

    // Per partition: send sequence -> acked offset. Guarded by the map itself.
    private static final List<TreeMap<Long, Long>> ACKED = List.of(new TreeMap<>(), new TreeMap<>(), new TreeMap<>());
    // Per partition: whether this driver has observed the leader move since it started.
    private static final boolean[] LEADER_CHANGED = new boolean[PARTITIONS];
    private static final int[] FIRST_LEADER = {-2, -2, -2};

    private static final AtomicLong ATTEMPTED = new AtomicLong();
    private static final AtomicLong ACKS = new AtomicLong();
    private static final AtomicLong FAILED = new AtomicLong();
    private static final AtomicLong CONSUMED = new AtomicLong();
    private static final AtomicBoolean STOP_CONSUMER = new AtomicBoolean();

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        String topic = System.getenv().getOrDefault("TOPIC", "offsets-test");
        long runMs = 20_000 + Math.floorMod(Random.getRandom(), 40_000L);

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        pp.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(1 + Math.floorMod(Random.getRandom(), 5)));
        pp.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(pick(BATCH_SIZES)));
        pp.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(pick(LINGER_MS)));
        pp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        pp.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");
        pp.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        pp.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "50");
        pp.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "2000");

        Properties ap = new Properties();
        ap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        ap.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000");
        ap.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        cp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(pick(MAX_POLL_RECORDS)));
        cp.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(pick(FETCH_MAX_WAIT_MS)));
        cp.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        cp.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        cp.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "2000");
        cp.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "50");
        boolean fromBeginning = Math.floorMod(Random.getRandom(), 2) == 0;
        Thread consumerThread = new Thread(() -> consume(cp, topic, fromBeginning), "offsets-consumer");
        consumerThread.start();

        long[] nextSeq = new long[PARTITIONS];
        long deadline = System.currentTimeMillis() + runMs;
        long nextLeaderPoll = 0;

        try (Admin admin = Admin.create(ap);
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(pp, new ByteArraySerializer(), new ByteArraySerializer())) {
            while (System.currentTimeMillis() < deadline) {
                if (System.currentTimeMillis() >= nextLeaderPoll) {
                    pollLeaders(admin, topic);
                    nextLeaderPoll = System.currentTimeMillis() + 500;
                }
                int burst = pick(BURSTS);
                for (int i = 0; i < burst; i++) {
                    int partition = Math.floorMod(Random.getRandom(), PARTITIONS);
                    long seq = nextSeq[partition]++;
                    byte[] value = new byte[pick(VALUE_SIZES)];
                    ATTEMPTED.incrementAndGet();
                    try {
                        producer.send(new ProducerRecord<>(topic, partition, null, value), (md, e) -> {
                            if (e != null) {
                                FAILED.incrementAndGet();
                            } else {
                                onAck(partition, seq, md.offset());
                            }
                        });
                    } catch (Exception e) {
                        // Buffer full / metadata timeout under faults: expected, keep going.
                        FAILED.incrementAndGet();
                    }
                }
                Thread.sleep(Math.floorMod(Random.getRandom(), 100L));
            }
            producer.close(Duration.ofSeconds(25));
            // Let the consumer catch up on the tail while leader tracking stays live.
            long drainUntil = System.currentTimeMillis() + 3_000;
            while (System.currentTimeMillis() < drainUntil) {
                pollLeaders(admin, topic);
                Thread.sleep(500);
            }
        } finally {
            STOP_CONSUMER.set(true);
            consumerThread.join(15_000);
        }
        System.out.printf("parallel_driver_offsets done: attempted=%d acked=%d failed=%d consumed=%d fromBeginning=%s leaderChanged=%s%n",
            ATTEMPTED.get(), ACKS.get(), FAILED.get(), CONSUMED.get(), fromBeginning, java.util.Arrays.toString(LEADER_CHANGED));
    }

    private static void consume(Properties cp, String topic, boolean fromBeginning) {
        long[] lastSeen = {-1, -1, -1};
        List<TopicPartition> tps = new ArrayList<>();
        for (int p = 0; p < PARTITIONS; p++) tps.add(new TopicPartition(topic, p));
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cp, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.assign(tps);
            // The only seek is the starting position; from here positions only move forward.
            if (fromBeginning) consumer.seekToBeginning(tps);
            else consumer.seekToEnd(tps);
            while (!STOP_CONSUMER.get()) {
                try {
                    for (ConsumerRecord<byte[], byte[]> rec : consumer.poll(Duration.ofMillis(500))) {
                        int p = rec.partition();
                        checkConsumed(p, rec.offset(), lastSeen[p], null);
                        lastSeen[p] = Math.max(lastSeen[p], rec.offset());
                        CONSUMED.incrementAndGet();
                    }
                } catch (OffsetOutOfRangeException e) {
                    // Retention is off and unclean election is disabled, so committed data never
                    // disappears: out-of-range (or detected truncation) is a violation.
                    for (TopicPartition tp : e.partitions()) {
                        int p = tp.partition();
                        checkConsumed(p, e.offsetOutOfRangePartitions().getOrDefault(tp, -1L), lastSeen[p], e.toString());
                    }
                    return;
                } catch (Exception e) {
                    // Transient client errors under faults: keep polling.
                    Thread.sleep(100);
                }
            }
        } catch (Exception e) {
            System.out.println("consumer stopped: " + e);
        }
    }

    // One helper for both call sites of the consumer property (per record and out-of-range).
    private static void checkConsumed(int partition, long offset, long lastSeen, String outOfRange) {
        boolean changed;
        synchronized (LEADER_CHANGED) {
            changed = LEADER_CHANGED[partition];
        }
        ObjectNode details = MAPPER.createObjectNode();
        details.put("partition", partition);
        details.put("offset", offset);
        details.put("lastSeen", lastSeen);
        if (outOfRange != null) details.put("outOfRange", outOfRange);
        Assert.always(outOfRange == null && offset > lastSeen,
            "consumer delivers strictly increasing offsets per partition", details);
        if (outOfRange == null) {
            Assert.sometimes(changed, "consumer delivered a record after a leader change", details);
        }
    }

    private static void onAck(int partition, long seq, long offset) {
        TreeMap<Long, Long> acked = ACKED.get(partition);
        Map.Entry<Long, Long> before;
        Map.Entry<Long, Long> after;
        boolean changed;
        synchronized (acked) {
            acked.put(seq, offset);
            before = acked.lowerEntry(seq);
            after = acked.higherEntry(seq);
        }
        synchronized (LEADER_CHANGED) {
            changed = LEADER_CHANGED[partition];
        }
        ACKS.incrementAndGet();
        boolean ok = (before == null || before.getValue() < offset) && (after == null || offset < after.getValue());
        ObjectNode details = MAPPER.createObjectNode();
        details.put("partition", partition);
        details.put("sendSeq", seq);
        details.put("offset", offset);
        if (before != null) {
            details.put("prevSendSeq", before.getKey());
            details.put("prevOffset", before.getValue());
        }
        if (after != null) {
            details.put("nextSendSeq", after.getKey());
            details.put("nextOffset", after.getValue());
        }
        Assert.always(ok, "acked produce offsets strictly increase per partition per producer", details);
        Assert.sometimes(changed, "produce acked after a leader change", details);
    }

    private static void pollLeaders(Admin admin, String topic) {
        try {
            TopicDescription desc = admin.describeTopics(List.of(topic)).allTopicNames().get(3, TimeUnit.SECONDS).get(topic);
            for (TopicPartitionInfo info : desc.partitions()) {
                int p = info.partition();
                if (p >= PARTITIONS) continue;
                int leader = info.leader() == null ? -1 : info.leader().id();
                synchronized (LEADER_CHANGED) {
                    if (FIRST_LEADER[p] == -2) {
                        FIRST_LEADER[p] = leader;
                    } else if (leader != FIRST_LEADER[p]) {
                        LEADER_CHANGED[p] = true;
                    }
                }
            }
        } catch (Exception e) {
            // Metadata unavailable under faults; try again on the next poll.
        }
    }

    private static int pick(int[] menu) {
        return menu[Math.floorMod(Random.getRandom(), menu.length)];
    }
}
