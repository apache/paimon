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

package org.apache.flink.table.store.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.log.LogOptions.LogChangelogMode;
import org.apache.flink.table.store.sink.SinkRecord;
import org.apache.flink.types.RowKind;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/** A {@link KafkaRecordSerializationSchema} for the table in log store. */
public class KafkaLogSerializationSchema implements KafkaRecordSerializationSchema<SinkRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    @Nullable private final SerializationSchema<RowData> keySerializer;
    private final SerializationSchema<RowData> valueSerializer;
    private final LogChangelogMode changelogMode;

    public KafkaLogSerializationSchema(
            String topic,
            @Nullable SerializationSchema<RowData> keySerializer,
            SerializationSchema<RowData> valueSerializer,
            LogChangelogMode changelogMode) {
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.changelogMode = changelogMode;
        if (changelogMode == LogChangelogMode.UPSERT && keySerializer == null) {
            throw new IllegalArgumentException(
                    "Can not use upsert changelog mode for non-pk table.");
        }
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (keySerializer != null) {
            keySerializer.open(context);
        }
        valueSerializer.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            SinkRecord element, KafkaSinkContext context, Long timestamp) {
        RowKind kind = element.row().getRowKind();

        byte[] keyBytes = null;
        byte[] valueBytes = null;
        if (keySerializer != null) {
            keyBytes = keySerializer.serialize(element.key());
            if (changelogMode == LogChangelogMode.ALL
                    || kind == RowKind.INSERT
                    || kind == RowKind.UPDATE_AFTER) {
                valueBytes = valueSerializer.serialize(element.row());
            }
        } else {
            valueBytes = valueSerializer.serialize(element.row());
        }
        return new ProducerRecord<>(topic, element.bucket(), keyBytes, valueBytes);
    }
}
