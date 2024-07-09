/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.kafka;

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.DockerImageVersions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/** Base class for Kafka Table IT Cases. */
public abstract class KafkaTableTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTableTestBase.class);

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int zkTimeoutMills = 30000;

    @RegisterExtension
    public static final KafkaContainerExtension KAFKA_CONTAINER =
            (KafkaContainerExtension)
                    new KafkaContainerExtension(DockerImageName.parse(DockerImageVersions.KAFKA)) {
                        @Override
                        protected void doStart() {
                            super.doStart();
                            if (LOG.isInfoEnabled()) {
                                this.followOutput(new Slf4jLogConsumer(LOG));
                            }
                        }
                    }.withEmbeddedZookeeper()
                            .withNetwork(NETWORK)
                            .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                            .withEnv(
                                    "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                                    String.valueOf(Duration.ofHours(2).toMillis()))
                            // Disable log deletion to prevent records from being deleted during
                            // test run
                            .withEnv("KAFKA_LOG_RETENTION_MS", "-1");

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    // Timer for scheduling logging task if the test hangs
    private final Timer loggingTimer = new Timer("Debug Logging Timer");

    @BeforeEach
    public void setup() {
        env = streamExecutionEnvironmentBuilder().streamingMode().build();
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);

        // Probe Kafka broker status per 30 seconds
        scheduleTimeoutLogger(
                Duration.ofSeconds(30),
                () -> {
                    // List all non-internal topics
                    final Map<String, TopicDescription> topicDescriptions =
                            describeExternalTopics();
                    LOG.info("Current existing topics: {}", topicDescriptions.keySet());

                    // Log status of topics
                    logTopicPartitionStatus(topicDescriptions);
                });
    }

    @AfterEach
    public void after() throws ExecutionException, InterruptedException {
        // Cancel timer for debug logging
        cancelTimeoutLogger();
        // Delete topics for avoid reusing topics of Kafka cluster
        deleteTopics();
    }

    public static Properties getStandardProps() {
        Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "flink-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", zkTimeoutMills);
        standardProps.put("zookeeper.connection.timeout.ms", zkTimeoutMills);
        return standardProps;
    }

    public static String getBootstrapServers() {
        return KAFKA_CONTAINER.getBootstrapServers();
    }

    protected boolean topicExists(String topicName) {
        return describeExternalTopics().containsKey(topicName);
    }

    public static void createTopicIfNotExists(String topicName, int numBucket) {
        try (final AdminClient adminClient = AdminClient.create(getStandardProps())) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                adminClient
                        .createTopics(
                                Collections.singleton(
                                        new NewTopic(
                                                topicName,
                                                Optional.of(numBucket),
                                                Optional.empty())))
                        .all()
                        .get();
            }
        } catch (Exception e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(
                        String.format("Failed to create Kafka topic %s", topicName), e);
            }
        }
    }

    protected void deleteTopicIfExists(String topicName) {
        try (final AdminClient adminClient = AdminClient.create(getStandardProps())) {
            if (adminClient.listTopics().names().get().contains(topicName)) {
                adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
            }
        } catch (Exception e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new RuntimeException(
                        String.format("Failed to drop Kafka topic %s", topicName), e);
            }
        }
    }

    private void deleteTopics() throws ExecutionException, InterruptedException {
        final AdminClient adminClient = AdminClient.create(getStandardProps());
        adminClient.deleteTopics(adminClient.listTopics().names().get()).all().get();
    }

    // ------------------------ For Debug Logging Purpose ----------------------------------

    private void scheduleTimeoutLogger(Duration period, Runnable loggingAction) {
        TimerTask timeoutLoggerTask =
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            loggingAction.run();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to execute logging action", e);
                        }
                    }
                };
        loggingTimer.schedule(timeoutLoggerTask, 0L, period.toMillis());
    }

    private void cancelTimeoutLogger() {
        loggingTimer.cancel();
    }

    private Map<String, TopicDescription> describeExternalTopics() {
        try (final AdminClient adminClient = AdminClient.create(getStandardProps())) {
            final List<String> topics =
                    adminClient.listTopics().listings().get().stream()
                            .filter(listing -> !listing.isInternal())
                            .map(TopicListing::name)
                            .collect(Collectors.toList());

            return adminClient.describeTopics(topics).allTopicNames().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list Kafka topics", e);
        }
    }

    private void logTopicPartitionStatus(Map<String, TopicDescription> topicDescriptions) {
        final Properties properties = getStandardProps();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-tests-debugging");
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        final KafkaConsumer<?, ?> consumer = new KafkaConsumer<String, String>(properties);
        List<TopicPartition> partitions = new ArrayList<>();
        topicDescriptions.forEach(
                (topic, description) ->
                        description
                                .partitions()
                                .forEach(
                                        tpInfo ->
                                                partitions.add(
                                                        new TopicPartition(
                                                                topic, tpInfo.partition()))));
        final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        partitions.forEach(
                partition ->
                        LOG.info(
                                "TopicPartition \"{}\": starting offset: {}, stopping offset: {}",
                                partition,
                                beginningOffsets.get(partition),
                                endOffsets.get(partition)));
    }

    /** Kafka container extension for junit5. */
    protected static class KafkaContainerExtension extends KafkaContainer
            implements BeforeAllCallback, AfterAllCallback {
        private KafkaContainerExtension(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        public void beforeAll(ExtensionContext extensionContext) throws Exception {
            this.doStart();
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) throws Exception {
            this.close();
        }
    }

    // ------------------------ For Kafka Test Purpose ----------------------------------
    protected void checkTopicExists(String topic, int partition, int replicationFactor) {
        try (AdminClient admin = createAdminClient()) {
            DescribeTopicsResult topicDesc = admin.describeTopics(Collections.singleton(topic));
            TopicDescription description =
                    topicDesc.allTopicNames().get(10, TimeUnit.SECONDS).get(topic);

            assertThat(description.partitions().size()).isEqualTo(partition);
            assertThat(description.partitions().get(0).replicas().size())
                    .isEqualTo(replicationFactor);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    protected void checkTopicNotExist(String topic) {
        try (AdminClient admin = createAdminClient()) {
            assertThat(admin.describeTopics(Collections.emptyList()).allTopicNames().get())
                    .doesNotContainKey(topic);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    protected AdminClient createAdminClient() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", getBootstrapServers());

        return AdminClient.create(properties);
    }
}
