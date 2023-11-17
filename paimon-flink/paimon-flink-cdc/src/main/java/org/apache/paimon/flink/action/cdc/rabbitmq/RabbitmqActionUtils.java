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

import org.apache.paimon.flink.action.cdc.MessageQueueSchemaUtils;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.utils.StringUtils;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for rabbitmq synchronization. */
public class RabbitmqActionUtils {

    static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Defines the format identifier for encoding value data.");
    static final ConfigOption<String> EXCHANGE =
            ConfigOptions.key("exchange").stringType().noDefaultValue().withDescription(".");

    static final ConfigOption<String> EXCHANGE_TYPE =
            ConfigOptions.key("exchange.type").stringType().noDefaultValue().withDescription(".");

    static final ConfigOption<String> ROUTING_KEY =
            ConfigOptions.key("routing.key").stringType().noDefaultValue().withDescription(".");

    static final ConfigOption<String> QUEUE_NAME =
            ConfigOptions.key("queue.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The queue to receive messages from.");

    static final ConfigOption<String> RABBITMQ_HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The default host to use for connections.");

    static final ConfigOption<String> RABBITMQ_VIRTUAL_HOST =
            ConfigOptions.key("virtual.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The virtual host to use when connecting to the broker.");

    static final ConfigOption<Integer> RABBITMQ_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The default port to use for connections.");

    static final ConfigOption<String> RABBITMQ_USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The AMQP user name to use when connecting to the broker.");

    static final ConfigOption<String> RABBITMQ_PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password to use when connecting to the broker.");

    static final ConfigOption<Integer> RABBITMQ_NETWORK_RECOVERY_INTERVAL =
            ConfigOptions.key("network.recovery.interval")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "How long will automatic recovery wait before attempting to reconnect, in ms.");

    static final ConfigOption<Boolean> RABBITMQ_AUTOMATIC_RECOVERY =
            ConfigOptions.key("automatic.recovery")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Enables or disables automatic connection recovery. if true, enables connection recovery.");

    static final ConfigOption<Boolean> RABBITMQ_TOPOLOGY_RECOVERY =
            ConfigOptions.key("topology.recovery")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enables or disables topology recovery. if true, enables topology recovery.");

    static final ConfigOption<Integer> RABBITMQ_CONNECTION_TIMEOUT =
            ConfigOptions.key("connection.timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Connection establishment timeout in milliseconds; zero for infinite.");

    static final ConfigOption<Integer> RABBITMQ_REQUESTED_CHANNEL_MAX =
            ConfigOptions.key("requested.channel.max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The initially requested maximum channel number; zero for unlimited.");

    static final ConfigOption<Integer> RABBITMQ_REQUESTED_FRAME_MAX =
            ConfigOptions.key("requested.frame.max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The initially requested maximum frame size, in octets; zero for unlimited.");

    static final ConfigOption<Integer> RABBITMQ_REQUESTED_HEARTBEAT =
            ConfigOptions.key("requested.heartbeat")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The initially requested heartbeat interval, in seconds; zero for none.");

    static final ConfigOption<Integer> RABBITMQ_PREFETCH_COUNT =
            ConfigOptions.key("prefetch.count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The max number of messages to receive without acknowledgement..");

    static final ConfigOption<Long> RABBITMQ_DELIVERY_TIMEOUT =
            ConfigOptions.key("delivery.timeout")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Retrieve the message delivery timeout used in the queueing consumer. If not specified explicitly, the default value of 30000 milliseconds will be returned.");

    static RabbitmqSource buildRabbitmqSource(Configuration rabbitmqConfig) {
        validateRabbitmqConfig(rabbitmqConfig);
        String queueName = rabbitmqConfig.get(QUEUE_NAME);
        String exchange = null;
        if (rabbitmqConfig.contains(EXCHANGE)) {
            exchange = rabbitmqConfig.get(EXCHANGE);
        }

        BuiltinExchangeType exchangeType = null;
        if (rabbitmqConfig.contains(EXCHANGE_TYPE)) {
            exchangeType = BuiltinExchangeType.valueOf(rabbitmqConfig.get(EXCHANGE_TYPE));
        }

        String routingKey = null;
        if (rabbitmqConfig.contains(ROUTING_KEY)) {
            routingKey = rabbitmqConfig.get(ROUTING_KEY);
        }

        RMQConnectionConfig connectionConfig = setOptionConfig(rabbitmqConfig);
        return new RabbitmqSource(
                connectionConfig,
                exchange,
                exchangeType,
                routingKey,
                queueName,
                new SimpleStringSchema());
    }

    private static RMQConnectionConfig setOptionConfig(Configuration rabbitmqConfig) {
        RMQConnectionConfig.Builder builder =
                new RMQConnectionConfig.Builder()
                        .setHost(rabbitmqConfig.get(RABBITMQ_HOST))
                        .setPort(rabbitmqConfig.get(RABBITMQ_PORT))
                        .setUserName(rabbitmqConfig.get(RABBITMQ_USERNAME))
                        .setPassword(rabbitmqConfig.get(RABBITMQ_PASSWORD))
                        .setVirtualHost(rabbitmqConfig.get(RABBITMQ_VIRTUAL_HOST));

        if (rabbitmqConfig.contains(RABBITMQ_NETWORK_RECOVERY_INTERVAL)) {
            builder.setNetworkRecoveryInterval(
                    rabbitmqConfig.get(RABBITMQ_NETWORK_RECOVERY_INTERVAL));
        }
        if (rabbitmqConfig.contains(RABBITMQ_AUTOMATIC_RECOVERY)) {
            builder.setAutomaticRecovery(rabbitmqConfig.get(RABBITMQ_AUTOMATIC_RECOVERY));
        }
        if (rabbitmqConfig.contains(RABBITMQ_TOPOLOGY_RECOVERY)) {
            builder.setTopologyRecoveryEnabled(rabbitmqConfig.get(RABBITMQ_TOPOLOGY_RECOVERY));
        }
        if (rabbitmqConfig.contains(RABBITMQ_CONNECTION_TIMEOUT)) {
            builder.setConnectionTimeout(rabbitmqConfig.get(RABBITMQ_CONNECTION_TIMEOUT));
        }
        if (rabbitmqConfig.contains(RABBITMQ_REQUESTED_CHANNEL_MAX)) {
            builder.setRequestedChannelMax(rabbitmqConfig.get(RABBITMQ_REQUESTED_CHANNEL_MAX));
        }
        if (rabbitmqConfig.contains(RABBITMQ_REQUESTED_FRAME_MAX)) {
            builder.setRequestedFrameMax(rabbitmqConfig.get(RABBITMQ_REQUESTED_FRAME_MAX));
        }
        if (rabbitmqConfig.contains(RABBITMQ_REQUESTED_HEARTBEAT)) {
            builder.setRequestedHeartbeat(rabbitmqConfig.get(RABBITMQ_REQUESTED_HEARTBEAT));
        }
        if (rabbitmqConfig.contains(RABBITMQ_PREFETCH_COUNT)) {
            builder.setPrefetchCount(rabbitmqConfig.get(RABBITMQ_PREFETCH_COUNT));
        }
        if (rabbitmqConfig.contains(RABBITMQ_DELIVERY_TIMEOUT)) {
            builder.setDeliveryTimeout(rabbitmqConfig.get(RABBITMQ_DELIVERY_TIMEOUT));
        }
        return builder.build();
    }

    private static void validateRabbitmqConfig(Configuration rabbitmqConfig) {
        checkArgument(
                rabbitmqConfig.contains(VALUE_FORMAT),
                String.format("rabbitmq-conf [%s] must be specified.", VALUE_FORMAT.key()));

        checkArgument(
                rabbitmqConfig.contains(RABBITMQ_HOST),
                String.format("rabbitmq-conf [%s] must be specified.", RABBITMQ_HOST.key()));

        checkArgument(
                rabbitmqConfig.contains(RABBITMQ_PORT),
                String.format("rabbitmq-conf [%s] must be specified.", RABBITMQ_PORT.key()));

        checkArgument(
                rabbitmqConfig.contains(RABBITMQ_USERNAME),
                String.format("rabbitmq-conf [%s] must be specified.", RABBITMQ_USERNAME.key()));

        checkArgument(
                rabbitmqConfig.contains(RABBITMQ_PASSWORD),
                String.format("rabbitmq-conf [%s] must be specified.", RABBITMQ_PASSWORD.key()));

        checkArgument(
                rabbitmqConfig.contains(RABBITMQ_VIRTUAL_HOST),
                String.format(
                        "rabbitmq-conf [%s] must be specified.", RABBITMQ_VIRTUAL_HOST.key()));

        checkArgument(
                rabbitmqConfig.contains(QUEUE_NAME),
                String.format("rabbitmq-conf [%s] must be specified.", QUEUE_NAME.key()));
    }

    static MessageQueueSchemaUtils.ConsumerWrapper createRabbitmqConsumer(
            Configuration rabbitmqConfig, String queueName) {
        RMQConnectionConfig connectionConfig = setOptionConfig(rabbitmqConfig);

        try {
            Connection connection = connectionConfig.getConnectionFactory().newConnection();
            Channel channel = setupChannel(connection);
            if (channel == null) {
                throw new RuntimeException("None of RabbitMQ channels are available");
            }
            setupQueue(channel, rabbitmqConfig, queueName);
            return new RabbitmqConsumerWrapper(connection, channel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setupQueue(Channel channel, Configuration rabbitmqConfig, String queueName)
            throws IOException {
        channel.queueDeclare(queueName, true, false, false, null);
        String exchange = null;
        if (rabbitmqConfig.contains(EXCHANGE)) {
            exchange = rabbitmqConfig.get(EXCHANGE);
        }
        BuiltinExchangeType exchangeType = null;
        if (rabbitmqConfig.contains(EXCHANGE_TYPE)) {
            exchangeType = BuiltinExchangeType.valueOf(rabbitmqConfig.get(EXCHANGE_TYPE));
        }
        String routingKey = "";
        if (rabbitmqConfig.contains(ROUTING_KEY)) {
            routingKey = rabbitmqConfig.get(ROUTING_KEY);
        }
        if (!StringUtils.isBlank(exchange) && exchangeType != null) {
            channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT);
            channel.queueBind(queueName, exchange, routingKey);
        }
    }

    private static Channel setupChannel(Connection connection) throws Exception {
        return connection.createChannel();
    }

    static DataFormat getDataFormat(Configuration rabbitmqConfig) {
        return DataFormat.fromConfigString(rabbitmqConfig.get(VALUE_FORMAT));
    }

    private static class RabbitmqConsumerWrapper
            implements MessageQueueSchemaUtils.ConsumerWrapper {

        private final Channel channel;
        private final Connection connection;

        RabbitmqConsumerWrapper(Connection connection, Channel channel) {
            this.channel = channel;
            this.connection = connection;
        }

        @Override
        public List<String> getRecords(String queue, int pollTimeOutMills) {
            try {
                GetResponse response = channel.basicGet(queue, false);
                return response == null
                        ? Collections.emptyList()
                        : Collections.singletonList(
                                new String(response.getBody(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws IOException, TimeoutException {
            channel.close();
            connection.close();
        }
    }
}
