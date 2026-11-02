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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;

import com.antithesis.sdk.Lifecycle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Container-start setup for the workload image: waits for all brokers, creates the
 * {@code acked} topic with the durability config under test, waits until every
 * partition has a full ISR, then emits Antithesis {@code setup_complete}.
 */
public final class ClientSetup {

    static final String DEFAULT_BOOTSTRAP = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    static final String TOPIC = "acked";
    static final int PARTITIONS = 3;
    static final short REPLICATION_FACTOR = 3;
    static final int EXPECTED_BROKERS = 3;

    private ClientSetup() { }

    public static String bootstrapServers() {
        String env = System.getenv("BOOTSTRAP_SERVERS");
        return env == null || env.isEmpty() ? DEFAULT_BOOTSTRAP : env;
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");

        try (Admin admin = Admin.create(props)) {
            waitForBrokers(admin);
            createTopic(admin);
            waitForFullIsr(admin);
        }

        ObjectNode details = new ObjectMapper().createObjectNode();
        details.put("topic", TOPIC);
        details.put("partitions", PARTITIONS);
        details.put("replication_factor", REPLICATION_FACTOR);
        Lifecycle.setupComplete(details);
        log("setup complete: cluster ready, topic '" + TOPIC + "' created");
    }

    private static void waitForBrokers(Admin admin) throws InterruptedException {
        while (true) {
            try {
                int n = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size();
                if (n >= EXPECTED_BROKERS) {
                    log("cluster has " + n + " brokers");
                    return;
                }
                log("cluster has " + n + "/" + EXPECTED_BROKERS + " brokers, waiting");
            } catch (Exception e) {
                log("describeCluster failed: " + e);
            }
            Thread.sleep(1000);
        }
    }

    private static void createTopic(Admin admin) throws InterruptedException {
        NewTopic topic = new NewTopic(TOPIC, PARTITIONS, REPLICATION_FACTOR).configs(Map.of(
            "min.insync.replicas", "2",
            "unclean.leader.election.enable", "false",
            "cleanup.policy", "delete",
            "retention.ms", "-1",
            "retention.bytes", "-1"));
        while (true) {
            try {
                admin.createTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
                log("created topic " + TOPIC);
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log("topic " + TOPIC + " already exists");
                    return;
                }
                log("createTopics failed: " + e.getCause());
            } catch (Exception e) {
                log("createTopics failed: " + e);
            }
            Thread.sleep(1000);
        }
    }

    private static void waitForFullIsr(Admin admin) throws InterruptedException {
        while (true) {
            try {
                TopicDescription desc = admin.describeTopics(List.of(TOPIC))
                    .allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC);
                boolean ready = desc.partitions().size() == PARTITIONS;
                for (TopicPartitionInfo p : desc.partitions()) {
                    ready &= p.leader() != null && p.isr().size() == REPLICATION_FACTOR;
                }
                if (ready) {
                    log("all " + PARTITIONS + " partitions have a leader and full ISR");
                    return;
                }
                log("waiting for leaders/full ISR: " + desc.partitions());
            } catch (Exception e) {
                log("describeTopics failed: " + e);
            }
            Thread.sleep(1000);
        }
    }

    static void log(String msg) {
        System.out.println("[client-setup] " + msg);
        System.out.flush();
    }
}
