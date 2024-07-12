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

package org.apache.paimon.flink.action.cdc.pulsar;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.pulsar.client.api.ProducerAccessMode.Shared;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

/**
 * Base test class for Pulsar synchronization. *
 *
 * <p>Using Kafka test data.
 */
public class PulsarActionITCaseBase extends CdcActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarActionITCaseBase.class);

    private static final Network NETWORK = Network.newNetwork();
    private static final String INTER_CONTAINER_PULSAR_ALIAS = "pulsar";

    public static final String IMAGE = "apachepulsar/pulsar";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private PulsarAdmin admin;
    private PulsarClient client;
    protected List<String> topics = new ArrayList<>();

    @RegisterExtension
    public static final PulsarContainerExtension PULSAR_CONTAINER =
            createPulsarContainerExtension();

    private static PulsarContainerExtension createPulsarContainerExtension() {
        PulsarContainerExtension container =
                new PulsarContainerExtension(DockerImageName.parse(IMAGE + ":" + "3.0.0")) {
                    @Override
                    protected void doStart() {
                        super.doStart();
                        if (LOG.isInfoEnabled()) {
                            this.followOutput(new Slf4jLogConsumer(LOG));
                        }
                    }
                };

        container.withNetwork(NETWORK);
        container.withNetworkAliases(INTER_CONTAINER_PULSAR_ALIAS);

        // Override the default standalone configuration by system environments
        container.withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "true");
        container.withEnv("PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled", "true");
        container.withEnv("PULSAR_PREFIX_systemTopicEnabled", "true");
        container.withEnv("PULSAR_PREFIX_brokerDeduplicationEnabled", "true");
        container.withEnv("PULSAR_PREFIX_defaultNumberOfNamespaceBundles", "1");

        // Change the default bootstrap script, it will override the default configuration
        // and start a standalone Pulsar without streaming storage and function worker
        container.withCommand(
                "sh",
                "-c",
                "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf && /pulsar/bin/pulsar standalone --no-functions-worker -nss");

        // Waiting for the Pulsar broker and the transaction is ready after the container started
        container.waitingFor(
                forHttp(
                                "/admin/v2/persistent/pulsar/system/transaction_coordinator_assign/partitions")
                        .forPort(BROKER_HTTP_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(5)));

        return container;
    }

    @BeforeEach
    public void setup() throws Exception {
        admin = PulsarAdmin.builder().serviceHttpUrl(PULSAR_CONTAINER.getHttpServiceUrl()).build();
        client =
                PulsarClient.builder()
                        .serviceUrl(PULSAR_CONTAINER.getPulsarBrokerUrl())
                        .enableTransaction(true)
                        .build();
    }

    @AfterEach
    public void after() throws Exception {
        super.after();
        // Delete topics for avoid reusing topics of pulsar cluster
        deleteTopics();
        admin.close();
        client.close();
    }

    protected Map<String, String> getBasicPulsarConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(PULSAR_SERVICE_URL.key(), PULSAR_CONTAINER.getPulsarBrokerUrl());
        config.put(PULSAR_ADMIN_URL.key(), PULSAR_CONTAINER.getHttpServiceUrl());
        config.put(PULSAR_SUBSCRIPTION_NAME.key(), "paimon-tests");
        return config;
    }

    protected void createTopic(String topic) {
        createTopic(topic, 0);
    }

    /**
     * Create a pulsar topic with given partition number if the topic doesn't exist.
     *
     * @param topic The name of the topic.
     * @param numberOfPartitions The number of partitions. We would create a non-partitioned topic
     *     if this number is zero.
     */
    protected void createTopic(String topic, int numberOfPartitions) {
        checkArgument(numberOfPartitions >= 0);
        String topicName = topicName(topic);
        if (numberOfPartitions == 0) {
            createNonPartitionedTopic(topicName);
        } else {
            createPartitionedTopic(topicName, numberOfPartitions);
        }
    }

    private void createNonPartitionedTopic(String topic) {
        try {
            admin.topics().createNonPartitionedTopic(topic);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    private void createPartitionedTopic(String topic, int numberOfPartitions) {
        try {
            admin.topics().createPartitionedTopic(topic, numberOfPartitions);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteTopics() throws Exception {
        for (String topic : topics) {
            String topicName = topicName(topic);
            PartitionedTopicMetadata metadata =
                    admin.topics().getPartitionedTopicMetadata(topicName);

            if (metadata.partitions == NON_PARTITIONED) {
                admin.topics().delete(topicName, true);
            } else {
                admin.topics().deletePartitionedTopic(topicName, true);
            }
        }
    }

    protected List<String> getMessages(String resource) throws IOException {
        URL url = PulsarActionITCaseBase.class.getClassLoader().getResource(resource);
        assertThat(url).isNotNull();
        java.nio.file.Path path = new File(url.getFile()).toPath();
        List<String> lines = Files.readAllLines(path);
        List<String> messages = new ArrayList<>();
        for (String line : lines) {
            try {
                objectMapper.readTree(line);
                if (!StringUtils.isEmpty(line)) {
                    messages.add(line);
                }
            } catch (Exception e) {
                // ignore
            }
        }

        return messages;
    }

    /**
     * Send a list messages to Pulsar, return the message id set after the ack from Pulsar.
     *
     * @param topic The name of the topic.
     * @param messages The records need to be sent.
     */
    protected void sendMessages(String topic, List<String> messages) {
        try (Producer<String> producer = createProducer(topic)) {
            for (String message : messages) {
                producer.newMessage().value(message).send();
            }
            producer.flush();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private Producer<String> createProducer(String topic) {
        try {
            return client.newProducer(Schema.STRING)
                    .topic(topic)
                    .enableBatching(false)
                    .enableMultiSchema(true)
                    .accessMode(Shared)
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    protected PulsarSyncTableActionBuilder syncTableActionBuilder(
            Map<String, String> pulsarConfig) {
        return new PulsarSyncTableActionBuilder(pulsarConfig);
    }

    protected PulsarSyncDatabaseActionBuilder syncDatabaseActionBuilder(
            Map<String, String> pulsarConfig) {
        return new PulsarSyncDatabaseActionBuilder(pulsarConfig);
    }

    /** Builder to build {@link PulsarSyncTableAction} from action arguments. */
    protected class PulsarSyncTableActionBuilder
            extends SyncTableActionBuilder<PulsarSyncTableAction> {

        public PulsarSyncTableActionBuilder(Map<String, String> pulsarConfig) {
            super(PulsarSyncTableAction.class, pulsarConfig);
        }
    }

    /** Builder to build {@link PulsarSyncDatabaseAction} from action arguments. */
    protected class PulsarSyncDatabaseActionBuilder
            extends SyncDatabaseActionBuilder<PulsarSyncDatabaseAction> {

        public PulsarSyncDatabaseActionBuilder(Map<String, String> pulsarConfig) {
            super(PulsarSyncDatabaseAction.class, pulsarConfig);
        }
    }

    /** Pulsar container extension for junit5. */
    private static class PulsarContainerExtension extends PulsarContainer
            implements BeforeAllCallback, AfterAllCallback {
        private PulsarContainerExtension(DockerImageName dockerImageName) {
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
