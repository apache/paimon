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

import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowKind;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/** A {@link KafkaRecordSerializationSchema} for the table in log store. */
public class KafkaLogSerializationSchema implements KafkaSerializationSchema<SinkRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    @Nullable private final SerializationSchema<RowData> primaryKeySerializer;
    private final SerializationSchema<RowData> valueSerializer;
    private final LogChangelogMode changelogMode;

    public KafkaLogSerializationSchema(
            String topic,
            @Nullable SerializationSchema<RowData> primaryKeySerializer,
            SerializationSchema<RowData> valueSerializer,
            LogChangelogMode changelogMode) {
        this.topic = topic;
        this.primaryKeySerializer = primaryKeySerializer;
        this.valueSerializer = valueSerializer;
        this.changelogMode = changelogMode;
        if (changelogMode == LogChangelogMode.UPSERT && primaryKeySerializer == null) {
            throw new IllegalArgumentException(
                    "Can not use upsert changelog mode for non-pk table.");
        }
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        if (primaryKeySerializer != null) {
            primaryKeySerializer.open(context);
        }
        valueSerializer.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(SinkRecord element, @Nullable Long timestamp) {
        RowKind kind = element.row().getRowKind();

        byte[] primaryKeyBytes = null;
        byte[] valueBytes = null;
        if (primaryKeySerializer != null) {
            primaryKeyBytes =
                    primaryKeySerializer.serialize(new FlinkRowData(element.primaryKey()));
            if (changelogMode == LogChangelogMode.ALL
                    || kind == RowKind.INSERT
                    || kind == RowKind.UPDATE_AFTER) {
                valueBytes = valueSerializer.serialize(new FlinkRowData(element.row()));
            }
        } else {
            valueBytes = valueSerializer.serialize(new FlinkRowData(element.row()));
        }
        Integer partition = element.bucket() < 0 ? null : element.bucket();
        return new ProducerRecord<>(topic, partition, primaryKeyBytes, valueBytes);
    }
}
