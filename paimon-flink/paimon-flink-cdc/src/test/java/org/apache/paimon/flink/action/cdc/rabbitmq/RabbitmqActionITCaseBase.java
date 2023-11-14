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

package org.apache.paimon.flink.action.cdc.rabbitmq;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for Rabbitmq synchronization. */
public class RabbitmqActionITCaseBase extends CdcActionITCaseBase {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitmqActionITCaseBase.class);
    protected final String exchange = "schema_exchange";
    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);
    private static final SimpleStringSchema SCHEMA = new SimpleStringSchema();
    public static final String RABBITMQ = "rabbitmq:3.9.8-management-alpine";
    private static final int RABBITMQ_PORT = 5672;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private List<String> queues = new ArrayList<>();

    @ClassRule
    private static final RabbitMQContainer RMQ_CONTAINER =
            new RabbitMQContainer(DockerImageName.parse(RABBITMQ))
                    .withExposedPorts(RABBITMQ_PORT)
                    .withLogConsumer(LOG_CONSUMER);

    private Connection connection;
    private Channel channel;

    @BeforeEach
    public void setup() throws Exception {
        RMQ_CONTAINER.start();
        connection = getRMQConnection();
        channel = connection.createChannel();
    }

    private static Connection getRMQConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_CONTAINER.getHost());
        factory.setVirtualHost("/");
        factory.setPort(RMQ_CONTAINER.getAmqpPort());
        factory.setUsername(RMQ_CONTAINER.getAdminUsername());
        factory.setPassword(RMQ_CONTAINER.getAdminPassword());
        return factory.newConnection();
    }

    @AfterEach
    public void after() throws Exception {
        super.after();
        // Delete topics for avoid reusing topics of pulsar cluster
        deleteTopics();
        channel.close();
        connection.close();
        RMQ_CONTAINER.close();
    }

    protected Map<String, String> getBasicRabbitmqConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(RabbitmqActionUtils.RABBITMQ_USERNAME.key(), RMQ_CONTAINER.getAdminUsername());
        config.put(RabbitmqActionUtils.RABBITMQ_PASSWORD.key(), RMQ_CONTAINER.getAdminPassword());
        config.put(RabbitmqActionUtils.RABBITMQ_VIRTUAL_HOST.key(), "/");
        config.put(RabbitmqActionUtils.RABBITMQ_HOST.key(), RMQ_CONTAINER.getHost());
        config.put(RabbitmqActionUtils.RABBITMQ_PORT.key(), RMQ_CONTAINER.getAmqpPort().toString());
        return config;
    }

    protected void createQueue(String exchange, String queueName) {
        try {
            channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT);
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, exchange, "");
            queues.add(queueName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteTopics() throws Exception {
        for (String queue : queues) {
            channel.queueDelete(queue);
        }
        queues = new ArrayList<>();
    }

    protected List<String> getMessages(String resource) throws IOException {
        URL url = RabbitmqActionITCaseBase.class.getClassLoader().getResource(resource);
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
     * Send a list messages to Rabbitmq.
     *
     * @param queueName The name of the queue.
     * @param messages The records need to be sent.
     */
    public static void sendMessages(String exchange, String queueName, List<String> messages)
            throws IOException, TimeoutException {
        try (Connection rmqConnection = getRMQConnection();
                Channel channel = rmqConnection.createChannel()) {
            for (String msg : messages) {
                channel.basicPublish(
                        exchange,
                        queueName,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        SCHEMA.serialize(msg));
            }
        }
    }

    protected RabbitmqSyncTableActionBuilder syncTableActionBuilder(
            Map<String, String> rabbitmqConfig) {
        return new RabbitmqSyncTableActionBuilder(rabbitmqConfig);
    }

    /** Builder to build {@link RabbitmqSyncTableAction} from action arguments. */
    protected class RabbitmqSyncTableActionBuilder
            extends SyncTableActionBuilder<RabbitmqSyncTableAction> {

        public RabbitmqSyncTableActionBuilder(Map<String, String> rabbitmqConfig) {
            super(rabbitmqConfig);
        }

        public RabbitmqSyncTableAction build() {
            List<String> args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    tableName));

            args.addAll(mapToArgs("--rabbitmq-conf", sourceConfig));
            args.addAll(mapToArgs("--catalog-conf", catalogConfig));
            args.addAll(mapToArgs("--table-conf", tableConfig));

            args.addAll(listToArgs("--partition-keys", partitionKeys));
            args.addAll(listToArgs("--primary-keys", primaryKeys));
            args.addAll(listToArgs("--type-mapping", typeMappingModes));

            args.addAll(listToMultiArgs("--computed-column", computedColumnArgs));

            MultipleParameterTool params =
                    MultipleParameterTool.fromArgs(args.toArray(args.toArray(new String[0])));
            return (RabbitmqSyncTableAction)
                    new RabbitmqSyncTableActionFactory()
                            .create(params)
                            .orElseThrow(RuntimeException::new);
        }
    }
}
