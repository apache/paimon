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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.util.Collector;
import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowKind;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.flink.FlinkRowData.toFlinkRowKind;
import static org.apache.paimon.flink.pulsar.PartitionMessageRouter.PAIMON_BUCKET;
import static org.apache.paimon.flink.pulsar.PulsarLogTestUtils.discoverPulsarLogFactory;
import static org.apache.paimon.flink.pulsar.PulsarLogTestUtils.testContext;
import static org.apache.paimon.flink.pulsar.PulsarLogTestUtils.testRecord;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link PulsarLogSerializationSchema} and {@link PulsarLogDeserializationSchema}.
 */
public class PulsarLogSerializationTest {

    private static final String TOPIC = "my_topic";

    private static PulsarLogSerializationSchema createTestSerializationSchema(
            DynamicTableFactory.Context context) {
        return discoverPulsarLogFactory()
                .createSinkProvider(context, PulsarLogTestUtils.SINK_CONTEXT)
                .createSerializationSchema();
    }

    private static PulsarDeserializationSchema<RowData> createTestDeserializationSchema(
            DynamicTableFactory.Context context) {
        return discoverPulsarLogFactory()
                .createSourceProvider(context, PulsarLogTestUtils.SOURCE_CONTEXT, null)
                .createDeserializationSchema();
    }

    @Test
    public void testKeyed() throws Exception {
        checkKeyed(LogChangelogMode.AUTO, 1, 3, 5);
        checkKeyed(LogChangelogMode.UPSERT, 3, 6, 9);
        checkKeyed(LogChangelogMode.ALL, 2, 5, 3);
    }

    @Test
    public void testNonKeyedUpsert() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> checkNonKeyed(LogChangelogMode.UPSERT, 3, 6, 9));
    }

    @Test
    public void testNonKeyed() throws Exception {
        checkNonKeyed(LogChangelogMode.AUTO, 1, 3, 5);
        checkNonKeyed(LogChangelogMode.ALL, 2, 5, 3);
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
        PulsarLogSerializationSchema serializer =
                createTestSerializationSchema(PulsarLogTestUtils.testContext("", mode, keyed));
        serializer.open(null);
        PulsarDeserializationSchema<RowData> deserializer =
                createTestDeserializationSchema(PulsarLogTestUtils.testContext("", mode, keyed));
        deserializer.open(null, null);

        SinkRecord input = testRecord(keyed, bucket, key, value, rowKind);

        PulsarMessage<byte[]> record = serializer.serialize(input, mb);
        int partition = Integer.parseInt(Objects.requireNonNull(record.getProperties()).get(PAIMON_BUCKET));
        assertThat(partition).isEqualTo(bucket);

        AtomicReference<RowData> rowReference = new AtomicReference<>();
        deserializer.deserialize(
                toMessage(record), new Collector<RowData>() {
                    @Override
                    public void collect(RowData rowData) {
                        if (rowReference.get() != null) {
                            throw new RuntimeException();
                        }
                        rowReference.set(rowData);
                    }

                    @Override
                    public void close() {
                    }
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

    private Message toMessage(PulsarMessage<byte[]> record) {
        MessageMetadata metadata = new MessageMetadata();
        if (StringUtils.isNotBlank(record.getKey())) {
            metadata.setPartitionKey(record.getKey());
        }
        ByteBuffer payload = ByteBuffer.wrap(record.getValue() != null ? record.getValue() : new byte[0]);
        return MessageImpl.create(metadata, payload, Schema.BYTES, TOPIC);
    }
}
