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
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowKind;

import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.flink.FlinkRowData.toFlinkRowKind;
import static org.apache.paimon.flink.kafka.KafkaLogTestUtils.discoverKafkaLogFactory;
import static org.apache.paimon.flink.kafka.KafkaLogTestUtils.testContext;
import static org.apache.paimon.flink.kafka.KafkaLogTestUtils.testRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KafkaLogSerializationSchema} and {@link KafkaLogDeserializationSchema}. */
public class KafkaLogSerializationTest {

    private static final String TOPIC = "my_topic";

    @Test
    public void testKeyed() throws Exception {
        checkKeyed(LogChangelogMode.AUTO, 1, 3, 5);
        checkKeyed(LogChangelogMode.UPSERT, 3, 6, 9);
        checkKeyed(LogChangelogMode.ALL, 2, 5, 3);
    }

    @Test
    public void testNonKeyedUpsert() {
        assertThatThrownBy(() -> checkNonKeyed(LogChangelogMode.UPSERT, 3, 6, 9))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNonKeyed() throws Exception {
        checkNonKeyed(LogChangelogMode.AUTO, 1, 3, 5);
        checkNonKeyed(LogChangelogMode.ALL, 2, 5, 3);
    }

    @Test
    public void testUnawareBucket() throws Exception {
        checkNonKeyed(LogChangelogMode.AUTO, -1, 3, 5);
        checkNonKeyed(LogChangelogMode.ALL, -1, 5, 3);
    }

    private void checkKeyed(LogChangelogMode mode, int bucket, int key, int value)
            throws Exception {
        check(mode, true, bucket, key, value, RowKind.INSERT);
        check(mode, true, bucket, key, value, RowKind.UPDATE_BEFORE);
        check(mode, true, bucket, key, value, RowKind.UPDATE_AFTER);
        check(mode, true, bucket, key, value, RowKind.DELETE);
    }

    private void checkNonKeyed(LogChangelogMode mode, int bucket, int key, int value)
            throws Exception {
        check(mode, false, bucket, key, value, RowKind.INSERT);
        check(mode, false, bucket, key, value, RowKind.UPDATE_BEFORE);
        check(mode, false, bucket, key, value, RowKind.UPDATE_AFTER);
        check(mode, false, bucket, key, value, RowKind.DELETE);
    }

    private void check(
            LogChangelogMode mode, boolean keyed, int bucket, int key, int value, RowKind rowKind)
            throws Exception {
        KafkaLogSerializationSchema serializer =
                createTestSerializationSchema(testContext("", mode, keyed));
        serializer.open(null);
        KafkaRecordDeserializationSchema<RowData> deserializer =
                createTestDeserializationSchema(testContext("", mode, keyed));
        deserializer.open(null);

        SinkRecord input = testRecord(keyed, bucket, key, value, rowKind);
        ProducerRecord<byte[], byte[]> record = serializer.serialize(input, null);

        if (bucket >= 0) {
            assertThat(record.partition().intValue()).isEqualTo(bucket);
        } else {
            assertThat(record.partition()).isNull();
        }

        AtomicReference<RowData> rowReference = new AtomicReference<>();
        deserializer.deserialize(
                toConsumerRecord(record),
                new Collector<RowData>() {
                    @Override
                    public void collect(RowData record) {
                        if (rowReference.get() != null) {
                            throw new RuntimeException();
                        }
                        rowReference.set(record);
                    }

                    @Override
                    public void close() {}
                });
        RowData row = rowReference.get();

        if (rowKind == RowKind.UPDATE_BEFORE) {
            assertThat(row.getRowKind()).isEqualTo(org.apache.flink.types.RowKind.DELETE);
        } else if (rowKind == RowKind.UPDATE_AFTER) {
            assertThat(row.getRowKind()).isEqualTo(org.apache.flink.types.RowKind.INSERT);
        } else {
            assertThat(row.getRowKind()).isEqualTo(toFlinkRowKind(rowKind));
        }
        assertThat(row.getInt(0)).isEqualTo(key);
        if (row.getRowKind() == org.apache.flink.types.RowKind.INSERT
                || mode == LogChangelogMode.ALL
                || !keyed) {
            assertThat(row.getInt(1)).isEqualTo(value);
        } else {
            assertThat(row.isNullAt(1)).isTrue();
        }
    }

    private ConsumerRecord<byte[], byte[]> toConsumerRecord(ProducerRecord<byte[], byte[]> record) {
        int partition = record.partition() == null ? -1 : record.partition();
        return new ConsumerRecord<>(TOPIC, partition, 0, record.key(), record.value());
    }

    private static KafkaLogSerializationSchema createTestSerializationSchema(
            DynamicTableFactory.Context context) {
        return discoverKafkaLogFactory()
                .createSinkProvider(context, KafkaLogTestUtils.SINK_CONTEXT)
                .createSerializationSchema();
    }

    private static KafkaRecordDeserializationSchema<RowData> createTestDeserializationSchema(
            DynamicTableFactory.Context context) {
        return discoverKafkaLogFactory()
                .createSourceProvider(context, KafkaLogTestUtils.SOURCE_CONTEXT, null)
                .createDeserializationSchema();
    }
}
