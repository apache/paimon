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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.DockerImageVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for Kafka synchronization. */
public abstract class KafkaActionITCaseBase extends CdcActionITCaseBase {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(KafkaActionITCaseBase.class);

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int zkTimeoutMills = 30000;

    // Timer for scheduling logging task if the test hangs
    private final Timer loggingTimer = new Timer("Debug Logging Timer");

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

    @BeforeEach
    public void setup() {
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
    public void after() throws Exception {
        super.after();
        // Cancel timer for debug logging
        cancelTimeoutLogger();
        // Delete topics for avoid reusing topics of Kafka cluster
        deleteTopics();
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

            return adminClient.describeTopics(topics).all().get();
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

    public static Properties getStandardProps() {
        Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "paimon-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", zkTimeoutMills);
        standardProps.put("zookeeper.connection.timeout.ms", zkTimeoutMills);
        standardProps.put("default.api.timeout.ms", "120000");
        return standardProps;
    }

    public String getBootstrapServers() {
        return KAFKA_CONTAINER.getBootstrapServers();
    }

    protected Map<String, String> getBasicKafkaConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("properties.bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        config.put("properties.group.id", "paimon-tests");
        config.put("properties.enable.auto.commit", "false");
        config.put("properties.auto.offset.reset", "earliest");
        return config;
    }

    protected KafkaSyncTableActionBuilder syncTableActionBuilder(Map<String, String> kafkaConfig) {
        return new KafkaSyncTableActionBuilder(kafkaConfig);
    }

    protected KafkaSyncDatabaseActionBuilder syncDatabaseActionBuilder(
            Map<String, String> kafkaConfig) {
        return new KafkaSyncDatabaseActionBuilder(kafkaConfig);
    }

    /** Builder to build {@link KafkaSyncTableAction} from action arguments. */
    protected class KafkaSyncTableActionBuilder
            extends SyncTableActionBuilder<KafkaSyncTableAction> {

        public KafkaSyncTableActionBuilder(Map<String, String> kafkaConfig) {
            super(kafkaConfig);
        }

        public KafkaSyncTableAction build() {
            List<String> args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "kafka_sync_table",
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    tableName));

            args.addAll(mapToArgs("--kafka-conf", sourceConfig));
            args.addAll(mapToArgs("--catalog-conf", catalogConfig));
            args.addAll(mapToArgs("--table-conf", tableConfig));

            args.addAll(listToArgs("--partition-keys", partitionKeys));
            args.addAll(listToArgs("--primary-keys", primaryKeys));
            args.addAll(listToArgs("--type-mapping", typeMappingModes));

            args.addAll(listToMultiArgs("--computed-column", computedColumnArgs));

            return createAction(KafkaSyncTableAction.class, args);
        }
    }

    /** Builder to build {@link KafkaSyncDatabaseAction} from action arguments. */
    protected class KafkaSyncDatabaseActionBuilder
            extends SyncDatabaseActionBuilder<KafkaSyncDatabaseAction> {

        public KafkaSyncDatabaseActionBuilder(Map<String, String> kafkaConfig) {
            super(kafkaConfig);
        }

        public KafkaSyncDatabaseActionBuilder ignoreIncompatible(boolean ignoreIncompatible) {
            throw new UnsupportedOperationException();
        }

        public KafkaSyncDatabaseActionBuilder mergeShards(boolean mergeShards) {
            throw new UnsupportedOperationException();
        }

        public KafkaSyncDatabaseActionBuilder withMode(String mode) {
            throw new UnsupportedOperationException();
        }

        public KafkaSyncDatabaseAction build() {
            List<String> args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "kafka_sync_database",
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database));

            args.addAll(mapToArgs("--kafka-conf", sourceConfig));
            args.addAll(mapToArgs("--catalog-conf", catalogConfig));
            args.addAll(mapToArgs("--table-conf", tableConfig));

            args.addAll(nullableToArgs("--table-prefix", tablePrefix));
            args.addAll(nullableToArgs("--table-suffix", tableSuffix));
            args.addAll(nullableToArgs("--including-tables", includingTables));
            args.addAll(nullableToArgs("--excluding-tables", excludingTables));

            args.addAll(listToArgs("--type-mapping", typeMappingModes));

            MultipleParameterToolAdapter params =
                    new MultipleParameterToolAdapter(
                            MultipleParameterTool.fromArgs(
                                    args.toArray(args.toArray(new String[0]))));
            return createAction(KafkaSyncDatabaseAction.class, args);
        }
    }

    public void createTestTopic(String topic, int numPartitions, int replicationFactor) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(
                            Collections.singletonList(
                                    new NewTopic(topic, numPartitions, (short) replicationFactor)))
                    .all()
                    .get();
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Fail to create topic [%s partitions: %d replication factor: %d].",
                            topic, numPartitions, replicationFactor),
                    e);
        }
    }

    public static List<String> readLines(String resource) throws IOException {
        final URL url =
                KafkaCanalSyncTableActionITCase.class.getClassLoader().getResource(resource);
        assertThat(url).isNotNull();
        java.nio.file.Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    void writeRecordsToKafka(String topic, List<String> lines) throws Exception {
        Properties producerProperties = getStandardProps();
        producerProperties.setProperty("retries", "0");
        producerProperties.put(
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer(producerProperties);
        for (int i = 0; i < lines.size(); i++) {
            try {
                JsonNode jsonNode = objectMapper.readTree(lines.get(i));
                if (!StringUtils.isEmpty(lines.get(i))) {
                    kafkaProducer.send(new ProducerRecord<>(topic, lines.get(i)));
                }
            } catch (Exception e) {
                // ignore
            }
        }

        kafkaProducer.close();
    }

    /** Kafka container extension for junit5. */
    private static class KafkaContainerExtension extends KafkaContainer
            implements BeforeAllCallback, AfterAllCallback {
        private KafkaContainerExtension(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            this.doStart();
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) {
            this.close();
        }
    }
}
