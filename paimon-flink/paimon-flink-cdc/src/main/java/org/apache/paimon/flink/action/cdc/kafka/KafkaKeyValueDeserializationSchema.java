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

import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** Deserialization for kafka key and value. */
public class KafkaKeyValueDeserializationSchema
        implements KafkaRecordDeserializationSchema<String> {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaKeyValueDeserializationSchema.class);

    private String charset;
    private String format;

    public KafkaKeyValueDeserializationSchema(String format) {
        this(StandardCharsets.UTF_8.name(), format);
    }

    public KafkaKeyValueDeserializationSchema(String charset, String format) {
        this.charset = Preconditions.checkNotNull(charset);
        this.format = format;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> collector)
            throws IOException {
        if (record.value() != null) {
            String value = new String(record.value(), Charset.forName(charset));
            String key =
                    record.key() != null
                            ? new String(record.key(), Charset.forName(charset))
                            : null;
            collector.collect(DebeziumSchemaUtils.rewriteValue(Pair.of(key, value), format));
        } else {
            // see
            // https://debezium.io/documentation/reference/2.5/connectors/mysql.html#mysql-tombstone-events
            LOG.info(
                    "Found null message value:\n{}\nThis message will be ignored. It might be produced by tombstone-event, "
                            + "please check your Debezium and Kafka configuration.",
                    record);
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
