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

import org.apache.paimon.flink.sink.LogSinkFunction;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaException;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * A {@link FlinkKafkaProducer} which implements {@link LogSinkFunction} to register {@link
 * WriteCallback}.
 */
public class KafkaSinkFunction extends FlinkKafkaProducer<SinkRecord> implements LogSinkFunction {

    private WriteCallback writeCallback;

    /**
     * Creates a {@link KafkaSinkFunction} for a given topic. The sink produces its input to the
     * topic. It accepts a {@link KafkaSerializationSchema} for serializing records to a {@link
     * ProducerRecord}, including partitioning information.
     *
     * @param defaultTopic The default topic to write data to
     * @param serializationSchema A serializable serialization schema for turning user objects into
     *     a kafka-consumable byte[] supporting key/value messages
     * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is
     *     the only required argument.
     * @param semantic Defines semantic that will be used by this producer (see {@link
     *     KafkaSinkFunction.Semantic}).
     */
    public KafkaSinkFunction(
            String defaultTopic,
            KafkaSerializationSchema<SinkRecord> serializationSchema,
            Properties producerConfig,
            KafkaSinkFunction.Semantic semantic) {
        super(defaultTopic, serializationSchema, producerConfig, semantic);
    }

    public void setWriteCallback(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public void open(OpenContext openContext) throws Exception {
        open(new Configuration());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        Callback baseCallback = requireNonNull(callback);
        callback =
                (metadata, exception) -> {
                    if (writeCallback != null && metadata != null && metadata.hasOffset()) {
                        writeCallback.onCompletion(metadata.partition(), metadata.offset());
                    }
                    baseCallback.onCompletion(metadata, exception);
                };
    }

    @Override
    public void flush() throws FlinkKafkaException {
        super.preCommit(super.currentTransaction());
    }
}
