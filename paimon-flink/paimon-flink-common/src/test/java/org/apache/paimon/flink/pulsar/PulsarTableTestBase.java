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

package org.apache.paimon.flink.pulsar;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

/**
 * Base class for Pulsar Table IT Cases.
 */
public abstract class PulsarTableTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarTableTestBase.class);

    private static final String INTER_CONTAINER_PULSAR_ALIAS = "pulsar";
    private static final String PULSAR_DEFAULT_TANENT = "public";
    private static final String PULSAR_DEFAULT_VERSION = "2.11.1";
    private static final String PULSAR_DEFAULT_NAMESPACE = "public/default";
    private static final String PULSAR_DOCKER = " apachepulsar/pulsar";
    private static final Network NETWORK = Network.newNetwork();
    @RegisterExtension
    public static final PulsarContainerExtension PULSAR_CONTAINER =
            (PulsarContainerExtension)
                    new PulsarContainerExtension(
                            DockerImageName.parse(PULSAR_DOCKER)) {
                        @Override
                        protected void doStart() {
                            super.doStart();
                            if (LOG.isInfoEnabled()) {
                                this.followOutput(new Slf4jLogConsumer(LOG));
                            }
                        }
                    }.withNetwork(NETWORK).withNetworkAliases(INTER_CONTAINER_PULSAR_ALIAS);
    private static final int zkTimeoutMills = 30000;
    //                            .withEnv("KAFKA_LOG_RETENTION_MS", "-1");
    // Timer for scheduling logging task if the test hangs
    private final Timer loggingTimer = new Timer("Debug Logging Timer");
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    public static String getHttpServiceUrl() {
        return PULSAR_CONTAINER.getHttpServiceUrl();
    }

    public static String getPulsarBrokerUrl() {
        return PULSAR_CONTAINER.getPulsarBrokerUrl();
    }

    public static Properties getStandardProps() {
        Properties standardProps = new Properties();
        standardProps.put("pulsar.admin.admin-url", PULSAR_CONTAINER.getHttpServiceUrl());
        standardProps.put("pulsar.client.service-url", PULSAR_CONTAINER.getPulsarBrokerUrl());
        return standardProps;
    }

    public static void createTopicIfNotExists(String topicName, int numBucket) {
        try (final PulsarAdmin adminClient =
                     PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build()) {
            if (!adminClient.topics().getList(PULSAR_DEFAULT_NAMESPACE).contains(topicName)) {
                adminClient.topics().createPartitionedTopic(topicName, numBucket);
            }
        } catch (PulsarAdminException | PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    static private Map conf = new HashMap();
    static {
        conf.put("rest.bind-port", "8082");
//        conf.put("taskmanager.log.path", "/tmp/taskmanger-flink.log");
    }

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .setConfiguration(Configuration.fromMap(conf))
                    .build());

    @BeforeEach
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);

        // Probe Pulsar broker status per 30 seconds
        scheduleTimeoutLogger(
                Duration.ofSeconds(30),
                () -> {
                    // List all namespace
                    LOG.info("Current existing topics: {}", listTopics());
                });
    }

    @AfterEach
    public void after() throws ExecutionException, InterruptedException, PulsarClientException, PulsarAdminException {
        // Cancel timer for debug logging
        cancelTimeoutLogger();
        // Delete topics for avoid reusing topics of Pulsar cluster
        deleteTopics();
    }

    protected boolean topicExists(String topicName) {
        return listTopics().contains(topicName);
    }

    private void deleteTopics() throws PulsarClientException, PulsarAdminException {
        PulsarAdmin adminClient =
                PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build();
        for (String topic : listTopics()) {
            adminClient.topics().delete(topic);
        }
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

    private List<String> listTopics() {
        try (final PulsarAdmin adminClient =
                     PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build()) {

            return adminClient.topics().getList(PULSAR_DEFAULT_NAMESPACE);

        } catch (Exception e) {
            throw new RuntimeException("Failed to list Kafka topics", e);
        }
    }

    /**
     * Pulsar container extension for junit5.
     */
    private static class PulsarContainerExtension extends PulsarContainer
            implements BeforeAllCallback, AfterAllCallback {
        private PulsarContainerExtension(DockerImageName dockerImageName) {
            super(PULSAR_DEFAULT_VERSION);
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
}
