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

import org.apache.paimon.flink.action.cdc.MessageQueueSchemaUtils;
import org.apache.paimon.flink.action.cdc.format.DataFormat;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarClientFactory;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.PulsarPartitionSplitReader;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.internal.DefaultImplementation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.CLIENT_CONFIG_PREFIX;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.config.PulsarSourceConfigUtils.createConsumerBuilder;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.isFullTopicRanges;
import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.pulsar.client.api.KeySharedPolicy.stickyHashRange;

/** Utils for Pulsar synchronization. */
public class PulsarActionUtils {

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Defines the format identifier for encoding value data.");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. "
                                    + "Option 'topic' is required for sink.");

    static final ConfigOption<String> PULSAR_AUTH_PARAM_MAP =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParamMap")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Parameters for the authentication plugin.");

    static final ConfigOption<String> PULSAR_START_CURSOR_FROM_MESSAGE_ID =
            ConfigOptions.key("pulsar.startCursor.fromMessageId")
                    .stringType()
                    .defaultValue("EARLIEST")
                    .withDescription(
                            "Using a unique identifier of a single message to seek the start position. "
                                    + "The common format is a triple '<long>ledgerId,<long>entryId,<int>partitionIndex'. "
                                    + "Specially, you can set it to EARLIEST (-1, -1, -1) or LATEST (Long.MAX_VALUE, Long.MAX_VALUE, -1).");

    static final ConfigOption<Long> PULSAR_START_CURSOR_FORM_PUBLISH_TIME =
            ConfigOptions.key("pulsar.startCursor.fromPublishTime")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Using the message publish time to seek the start position.");

    static final ConfigOption<Boolean> PULSAR_START_CURSOR_FROM_MESSAGE_ID_INCLUSIVE =
            ConfigOptions.key("pulsar.startCursor.fromMessageIdInclusive")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to include the given message id. This option only works when the message id is not EARLIEST or LATEST.");

    static final ConfigOption<String> PULSAR_STOP_CURSOR_AT_MESSAGE_ID =
            ConfigOptions.key("pulsar.stopCursor.atMessageId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Stop consuming when the message id is equal or greater than the specified message id. "
                                    + "Message that is equal to the specified message id will not be consumed. "
                                    + "The common format is a triple '<long>ledgerId,<long>entryId,<int>partitionIndex'. "
                                    + "Specially, you can set it to LATEST (Long.MAX_VALUE, Long.MAX_VALUE, -1).");

    static final ConfigOption<String> PULSAR_STOP_CURSOR_AFTER_MESSAGE_ID =
            ConfigOptions.key("pulsar.stopCursor.afterMessageId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Stop consuming when the message id is greater than the specified message id. "
                                    + "Message that is equal to the specified message id will be consumed. "
                                    + "The common format is a triple '<long>ledgerId,<long>entryId,<int>partitionIndex'. "
                                    + "Specially, you can set it to LATEST (Long.MAX_VALUE, Long.MAX_VALUE, -1).");

    static final ConfigOption<Long> PULSAR_STOP_CURSOR_AT_EVENT_TIME =
            ConfigOptions.key("pulsar.stopCursor.atEventTime")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Stop consuming when message event time is greater than or equals the specified timestamp. "
                                    + "Message that even time is equal to the specified timestamp will not be consumed.");

    static final ConfigOption<Long> PULSAR_STOP_CURSOR_AFTER_EVENT_TIME =
            ConfigOptions.key("pulsar.stopCursor.afterEventTime")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Stop consuming when message event time is greater than the specified timestamp. "
                                    + "Message that even time is equal to the specified timestamp will be consumed.");

    static final ConfigOption<Boolean> PULSAR_SOURCE_UNBOUNDED =
            ConfigOptions.key("pulsar.source.unbounded")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("To specify the boundedness of a stream.");

    public static PulsarSource<String> buildPulsarSource(Configuration rawConfig) {
        Configuration pulsarConfig = preprocessPulsarConfig(rawConfig);

        PulsarSourceBuilder<String> pulsarSourceBuilder = PulsarSource.builder();

        // the minimum setup
        pulsarSourceBuilder
                .setServiceUrl(pulsarConfig.get(PULSAR_SERVICE_URL))
                .setAdminUrl(pulsarConfig.get(PULSAR_ADMIN_URL))
                .setSubscriptionName(pulsarConfig.get(PULSAR_SUBSCRIPTION_NAME))
                .setTopics(
                        Arrays.stream(pulsarConfig.get(TOPIC).split(","))
                                .map(String::trim)
                                .collect(Collectors.toList()))
                .setDeserializationSchema(new SimpleStringSchema());

        // other settings

        // consumer name
        pulsarConfig
                .getOptional(PULSAR_CONSUMER_NAME)
                .ifPresent(pulsarSourceBuilder::setConsumerName);

        // start cursor
        if (pulsarConfig.contains(PULSAR_START_CURSOR_FORM_PUBLISH_TIME)) {
            checkArgument(!pulsarConfig.contains(PULSAR_START_CURSOR_FROM_MESSAGE_ID), "");
            pulsarSourceBuilder.setStartCursor(
                    StartCursor.fromPublishTime(
                            pulsarConfig.get(PULSAR_START_CURSOR_FORM_PUBLISH_TIME)));
        } else {
            String messageId = pulsarConfig.get(PULSAR_START_CURSOR_FROM_MESSAGE_ID);
            if (messageId.equalsIgnoreCase("EARLIEST")) {
                pulsarSourceBuilder.setStartCursor(StartCursor.earliest());
            } else if (messageId.equalsIgnoreCase("LATEST")) {
                pulsarSourceBuilder.setStartCursor(StartCursor.latest());
            } else {
                StartCursor startCursor =
                        StartCursor.fromMessageId(
                                toMessageId(messageId),
                                pulsarConfig.get(PULSAR_START_CURSOR_FROM_MESSAGE_ID_INCLUSIVE));
                pulsarSourceBuilder.setStartCursor(startCursor);
            }
        }

        // boundedness and stop cursor
        StopCursor stopCursor = StopCursor.never();
        int stopCursorSet = 0;
        if (pulsarConfig.contains(PULSAR_STOP_CURSOR_AT_MESSAGE_ID)) {
            stopCursor =
                    StopCursor.atMessageId(
                            toMessageId(pulsarConfig.get(PULSAR_STOP_CURSOR_AT_MESSAGE_ID)));
            stopCursorSet++;
        }
        if (pulsarConfig.contains(PULSAR_STOP_CURSOR_AFTER_MESSAGE_ID)) {
            stopCursor =
                    StopCursor.afterMessageId(
                            toMessageId(pulsarConfig.get(PULSAR_STOP_CURSOR_AT_MESSAGE_ID)));
            stopCursorSet++;
        }
        if (pulsarConfig.contains(PULSAR_STOP_CURSOR_AT_EVENT_TIME)) {
            stopCursor = StopCursor.atEventTime(pulsarConfig.get(PULSAR_STOP_CURSOR_AT_EVENT_TIME));
            stopCursorSet++;
        }
        if (pulsarConfig.contains(PULSAR_STOP_CURSOR_AFTER_EVENT_TIME)) {
            stopCursor =
                    StopCursor.atEventTime(pulsarConfig.get(PULSAR_STOP_CURSOR_AFTER_EVENT_TIME));
            stopCursorSet++;
        }
        checkArgument(stopCursorSet <= 1, "You can set at most one of the stop cursor options.");

        if (pulsarConfig.get(PULSAR_SOURCE_UNBOUNDED)) {
            pulsarSourceBuilder.setUnboundedStopCursor(stopCursor);
        } else {
            pulsarSourceBuilder.setBoundedStopCursor(stopCursor);
        }

        // auth
        String authPluginClassName = pulsarConfig.get(PULSAR_AUTH_PLUGIN_CLASS_NAME);
        if (authPluginClassName != null) {
            String authParamsString = pulsarConfig.get(PULSAR_AUTH_PARAMS);
            Map<String, String> authParamsMap =
                    pulsarConfig.get(PulsarOptions.PULSAR_AUTH_PARAM_MAP);

            checkArgument(
                    authParamsString != null || authParamsMap != null,
                    "You should set '%s' or '%s'",
                    PULSAR_AUTH_PARAMS.key(),
                    PULSAR_AUTH_PARAM_MAP.key());
            checkArgument(
                    authParamsString == null || authParamsMap == null,
                    "You can only set one of '%s' and '%s'",
                    PULSAR_AUTH_PARAMS.key(),
                    PULSAR_AUTH_PARAM_MAP.key());

            if (authParamsString != null) {
                pulsarSourceBuilder.setAuthentication(authPluginClassName, authParamsString);
            } else {
                pulsarSourceBuilder.setAuthentication(authPluginClassName, authParamsMap);
            }
        }

        // set all options as additional pulsar options and pulsar source options
        pulsarSourceBuilder.setConfig(pulsarConfig);

        // TODO set RangeGenerator; set PulsarCrypto

        return pulsarSourceBuilder.build();
    }

    private static MessageId toMessageId(String messageIdString) {
        if (messageIdString.equalsIgnoreCase("EARLIEST")) {
            return MessageId.earliest;
        } else if (messageIdString.equalsIgnoreCase("LATEST")) {
            return MessageId.latest;
        } else {
            String[] splits = messageIdString.split(",");
            checkArgument(
                    splits.length == 3,
                    "Please use format '<long>ledgerId,<long>entryId,<int>partitionIndex' for message id");
            return DefaultImplementation.getDefaultImplementation()
                    .newMessageId(
                            Long.parseLong(splits[0].trim()),
                            Long.parseLong(splits[1].trim()),
                            Integer.parseInt(splits[2].trim()));
        }
    }

    static SourceConfiguration toSourceConfiguration(Configuration rawConfig) {
        return new SourceConfiguration(preprocessPulsarConfig(rawConfig));
    }

    private static Configuration preprocessPulsarConfig(Configuration rawConfig) {
        Configuration cloned = new Configuration(rawConfig);
        if (cloned.contains(PULSAR_AUTH_PARAM_MAP)) {
            Map<String, String> authParamsMap =
                    parseCommaSeparatedKeyValues(cloned.get(PULSAR_AUTH_PARAM_MAP));
            cloned.removeConfig(PULSAR_AUTH_PARAM_MAP);
            cloned.set(PulsarOptions.PULSAR_AUTH_PARAM_MAP, authParamsMap);
        }
        return cloned;
    }

    public static DataFormat getDataFormat(Configuration pulsarConfig) {
        return DataFormat.fromConfigString(pulsarConfig.get(VALUE_FORMAT));
    }

    /** Referenced to {@link PulsarPartitionSplitReader#createPulsarConsumer}. */
    public static MessageQueueSchemaUtils.ConsumerWrapper createPulsarConsumer(
            Configuration pulsarConfig) {
        try {
            SourceConfiguration pulsarSourceConfiguration = toSourceConfiguration(pulsarConfig);
            PulsarClient pulsarClient = PulsarClientFactory.createClient(pulsarSourceConfiguration);

            ConsumerBuilder<String> consumerBuilder =
                    createConsumerBuilder(
                            pulsarClient,
                            org.apache.pulsar.client.api.Schema.STRING,
                            pulsarSourceConfiguration);

            // The default position is Latest
            consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

            String topic = pulsarConfig.get(PulsarActionUtils.TOPIC).split(",")[0];
            TopicPartition topicPartition = new TopicPartition(topic);
            consumerBuilder.topic(topicPartition.getFullTopicName());

            // TODO currently, PulsarCrypto is not supported

            // Add KeySharedPolicy for partial keys subscription.
            if (!isFullTopicRanges(topicPartition.getRanges())) {
                KeySharedPolicy policy = stickyHashRange().ranges(topicPartition.getPulsarRanges());
                // We may enable out of order delivery for speeding up. It was turned off by
                // default.
                policy.setAllowOutOfOrderDelivery(
                        pulsarSourceConfiguration.isAllowKeySharedOutOfOrderDelivery());
                consumerBuilder.keySharedPolicy(policy);
            }

            // Create the consumer configuration by using common utils.
            Consumer<String> consumer = consumerBuilder.subscribe();

            return new PulsarConsumerWrapper(consumer, topic);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private static class PulsarConsumerWrapper implements MessageQueueSchemaUtils.ConsumerWrapper {

        private final Consumer<String> consumer;
        private final String topic;

        PulsarConsumerWrapper(Consumer<String> consumer, String topic) {
            this.consumer = consumer;
            this.topic = topic;
        }

        @Override
        public List<String> getRecords(int pollTimeOutMills) {
            try {
                Message<String> message = consumer.receive(pollTimeOutMills, TimeUnit.MILLISECONDS);
                return message == null
                        ? Collections.emptyList()
                        : Collections.singletonList(message.getValue());
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String topic() {
            return topic;
        }

        @Override
        public void close() throws PulsarClientException {
            consumer.close();
        }
    }
}
