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

package org.apache.paimon.flink.pulsar.sink;

import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Write data to Flink.
 *
 * @param <T> Type of the record.
 */
public class FlinkPulsarSink<T> extends FlinkPulsarSinkBase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPulsarSink.class);
    private final PulsarSinkSerializationSchema<T> serializationSchema;

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSinkSerializationSchema serializationSchema,
            MessageRouter messageRouter,
            PulsarSinkSemantic semantic) {

        super(
                adminUrl,
                defaultTopicName,
                clientConf,
                properties,
                serializationSchema,
                messageRouter,
                semantic);
        this.serializationSchema = serializationSchema;
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSinkSerializationSchema serializationSchema,
            PulsarSinkSemantic semantic) {
        this(
                adminUrl,
                defaultTopicName,
                clientConf,
                properties,
                serializationSchema,
                null,
                semantic);
    }

    public FlinkPulsarSink(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSinkSerializationSchema serializationSchema) {
        this(
                adminUrl,
                defaultTopicName,
                clientConf,
                properties,
                serializationSchema,
                PulsarSinkSemantic.AT_LEAST_ONCE);
    }

    public FlinkPulsarSink(
            String serviceUrl,
            String adminUrl,
            Optional<String> defaultTopicName,
            Properties properties,
            PulsarSinkSerializationSchema<T> serializationSchema) {
        this(
                adminUrl,
                defaultTopicName,
                PulsarClientUtils.newClientConf(checkNotNull(serviceUrl), properties),
                properties,
                serializationSchema,
                PulsarSinkSemantic.AT_LEAST_ONCE);
    }

    @Override
    public void invoke(
            PulsarTransactionState<T> transactionState, T value, Context context)
            throws Exception {
        checkErroneous();
        initializeSendCallback();

        final Optional<String> targetTopic =
                Optional.ofNullable(serializationSchema.getTargetTopic());
        String topic = targetTopic.orElse(defaultTopic);

        TypedMessageBuilder<byte[]> messageBuilder =
                transactionState.isTransactional()
                        ? getProducer(topic).newMessage(transactionState.getTransaction())
                        : getProducer(topic).newMessage();


        serializationSchema.serialize(value, messageBuilder);
        synchronized (pendingRecordsLock) {
            pendingRecords++;
        }

        CompletableFuture<MessageId> messageIdFuture = messageBuilder.sendAsync();
        if (transactionState.isTransactional()) {
            // in transactional mode, we must sleep some time because pulsar have some bug can
            // result data disorder.
            // if pulsar-client fix this bug, we can safely remove this.
            Thread.sleep(10);
            TxnID transactionalId = transactionState.transactionalId;
            List<CompletableFuture<MessageId>> futureList;
            tid2FuturesMap
                    .computeIfAbsent(transactionalId, key -> new ArrayList<>())
                    .add(messageIdFuture);
            LOG.debug("message {} is invoke in txn {}", value, transactionState.transactionalId);
        }
        messageIdFuture.whenComplete(sendCallback);
    }
}
