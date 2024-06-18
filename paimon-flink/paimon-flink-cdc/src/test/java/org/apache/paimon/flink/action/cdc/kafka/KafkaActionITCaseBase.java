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

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.util.DockerImageVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/** Base test class for Kafka synchronization. */
public abstract class KafkaActionITCaseBase extends CdcActionITCaseBase {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(KafkaActionITCaseBase.class);

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final String INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS = "schemaregistry";
    private static final Network NETWORK = Network.newNetwork();
    private static final int ZK_TIMEOUT_MILLIS = 30000;

    protected static KafkaProducer<String, String> kafkaProducer;
    private static KafkaConsumer<String, String> kafkaConsumer;
    private static AdminClient adminClient;

    // Timer for scheduling logging task if the test hangs
    private final Timer loggingTimer = new Timer("Debug Logging Timer");

    @RegisterExtension
    @Order(1)
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

    @RegisterExtension
    @Order(2)
    public static final SchemaRegistryContainerExtension SCHEMA_REGISTRY_CONTAINER =
            new SchemaRegistryContainerExtension(
                            DockerImageName.parse(DockerImageVersions.SCHEMA_REGISTRY))
                    .dependsOn(KAFKA_CONTAINER)
                    .withNetwork(NETWORK)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema_registry")
                    .withEnv(
                            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            "PLAINTEXT://" + KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092")
                    .withNetworkAliases(INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withStartupTimeout(Duration.ofSeconds(60));

    @BeforeAll
    public static void beforeAll() {
        // create KafkaProducer
        Properties producerProperties = getStandardProps();
        producerProperties.setProperty("retries", "0");
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getCanonicalName());
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getCanonicalName());
        kafkaProducer = new KafkaProducer<>(producerProperties);

        // create KafkaConsumer
        Properties consumerProperties = getStandardProps();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-tests-debugging");
        consumerProperties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        consumerProperties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        // create AdminClient
        adminClient = AdminClient.create(getStandardProps());
    }

    @AfterAll
    public static void afterAll() {
        // Close Kafka objects
        kafkaProducer.close();
        kafkaConsumer.close();
        adminClient.close();
    }

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
        try {
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
        final Map<TopicPartition, Long> beginningOffsets =
                kafkaConsumer.beginningOffsets(partitions);
        final Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions);
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
        standardProps.put("zookeeper.session.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("zookeeper.connection.timeout.ms", ZK_TIMEOUT_MILLIS);
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

    protected String getSchemaRegistryUrl() {
        return SCHEMA_REGISTRY_CONTAINER.getSchemaRegistryUrl();
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
            super(KafkaSyncTableAction.class, kafkaConfig);
        }
    }

    /** Builder to build {@link KafkaSyncDatabaseAction} from action arguments. */
    protected class KafkaSyncDatabaseActionBuilder
            extends SyncDatabaseActionBuilder<KafkaSyncDatabaseAction> {

        public KafkaSyncDatabaseActionBuilder(Map<String, String> kafkaConfig) {
            super(KafkaSyncDatabaseAction.class, kafkaConfig);
        }
    }

    protected void createTestTopic(String topic, int numPartitions, int replicationFactor) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try {
            adminClient
                    .createTopics(
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

    protected void writeRecordsToKafka(String topic, String resourceDirFormat, Object... args)
            throws Exception {
        writeRecordsToKafka(topic, false, resourceDirFormat, args);
    }

    protected void writeRecordsToKafka(
            String topic, boolean wait, String resourceDirFormat, Object... args) throws Exception {
        URL url =
                KafkaCanalSyncTableActionITCase.class
                        .getClassLoader()
                        .getResource(String.format(resourceDirFormat, args));
        Files.readAllLines(Paths.get(url.toURI())).stream()
                .filter(this::isRecordLine)
                .forEach(r -> send(topic, r, wait));
    }

    private boolean isRecordLine(String line) {
        try {
            objectMapper.readTree(line);
            return !StringUtils.isEmpty(line);
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    private void send(String topic, String record, boolean wait) {
        Future<RecordMetadata> sendFuture = kafkaProducer.send(new ProducerRecord<>(topic, record));
        if (wait) {
            try {
                sendFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
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

    /** Schema registry container extension for junit5. */
    private static class SchemaRegistryContainerExtension
            extends GenericContainer<SchemaRegistryContainerExtension>
            implements BeforeAllCallback, AfterAllCallback {

        private static final Integer SCHEMA_REGISTRY_EXPOSED_PORT = 8081;

        private SchemaRegistryContainerExtension(DockerImageName dockerImageName) {
            super(dockerImageName);
            addExposedPorts(SCHEMA_REGISTRY_EXPOSED_PORT);
        }

        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            this.doStart();
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) {
            this.close();
        }

        public String getSchemaRegistryUrl() {
            return String.format(
                    "http://%s:%s", getHost(), getMappedPort(SCHEMA_REGISTRY_EXPOSED_PORT));
        }
    }
}
