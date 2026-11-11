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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NullBitmapTransportTypeInfoTest {

    private static TypeSerializer<RowData> serializer() {
        return new NullBitmapTransportTypeInfo(
                        RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()))
                .createSerializer(new ExecutionConfig());
    }

    @Test
    void testRawRowsDecodeWithOriginalThreeFieldArity() throws Exception {
        TypeSerializer<RowData> serializer = serializer();
        RowDataSerializer originalSerializer =
                new RowDataSerializer(
                        LogicalTypeConversion.toLogicalType(
                                RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT())));
        for (Integer value : new Integer[] {null, 7}) {
            DataOutputSerializer encoded = new DataOutputSerializer(64);
            originalSerializer.serialize(GenericRowData.of(2, value, 1L << 33), encoded);
            RowData decoded =
                    serializer.deserialize(new DataInputDeserializer(encoded.getCopyOfBuffer()));
            assertThat(decoded.getArity()).isEqualTo(3);
            assertThat(decoded.getInt(0)).isEqualTo(2);
            assertThat(decoded.isNullAt(1)).isEqualTo(value == null);
            if (value != null) {
                assertThat(decoded.getInt(1)).isEqualTo(7);
            }
            assertThat(decoded.getLong(2)).isEqualTo(1L << 33);

            DataOutputSerializer copied = new DataOutputSerializer(64);
            serializer.copy(new DataInputDeserializer(encoded.getCopyOfBuffer()), copied);
            assertThat(copied.getCopyOfBuffer()).isEqualTo(encoded.getCopyOfBuffer());
            assertThat(
                            serializer
                                    .deserialize(
                                            new DataInputDeserializer(copied.getCopyOfBuffer()))
                                    .getArity())
                    .isEqualTo(3);
        }
    }

    @Test
    void testRawThreeFieldRowsSerializeWithoutTransportPadding() throws Exception {
        TypeSerializer<RowData> serializer = serializer();
        RowDataSerializer originalSerializer =
                new RowDataSerializer(
                        LogicalTypeConversion.toLogicalType(
                                RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT())));
        for (Integer value : new Integer[] {null, 7}) {
            RowData row = GenericRowData.of(2, value, 1L << 33);
            DataOutputSerializer encoded = new DataOutputSerializer(64);
            serializer.serialize(row, encoded);
            DataOutputSerializer original = new DataOutputSerializer(64);
            originalSerializer.serialize(row, original);
            assertThat(encoded.getCopyOfBuffer()).isEqualTo(original.getCopyOfBuffer());
        }
    }

    @Test
    void testRawThreeFieldRowsCopyWithoutTransportPadding() {
        TypeSerializer<RowData> serializer = serializer();
        for (Integer value : new Integer[] {null, 7}) {
            GenericRowData row = GenericRowData.of(2, value, 1L << 33);
            RowData copied = serializer.copy(row);
            row.setField(2, 999L);
            assertThat(copied.getArity()).isEqualTo(3);
            assertThat(copied.getInt(0)).isEqualTo(2);
            assertThat(copied.isNullAt(1)).isEqualTo(value == null);
            if (value != null) {
                assertThat(copied.getInt(1)).isEqualTo(7);
            }
            assertThat(copied.getLong(2)).isEqualTo(1L << 33);
        }
    }

    @Test
    void testInvalidRecordLengthsAreRejectedBeforeAllocationOrCopy() throws Exception {
        TypeSerializer<RowData> serializer = serializer();
        for (int length : new int[] {Integer.MIN_VALUE, 0, 1, -1, 31, -39}) {
            DataOutputSerializer encoded = new DataOutputSerializer(64);
            encoded.writeInt(length);
            if (length != Integer.MIN_VALUE) {
                encoded.write(new byte[Math.abs(length)]);
            }
            byte[] bytes = encoded.getCopyOfBuffer();
            assertThatThrownBy(() -> serializer.deserialize(new DataInputDeserializer(bytes)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("length");
            assertThatThrownBy(
                            () ->
                                    serializer.copy(
                                            new DataInputDeserializer(bytes),
                                            new DataOutputSerializer(64)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("length");
        }
    }

    @Test
    void testUnknownSnapshotVersionIsRejected() throws Exception {
        TypeSerializerSnapshot<RowData> snapshot = serializer().snapshotConfiguration();
        DataOutputSerializer encoded = new DataOutputSerializer(128);
        snapshot.writeSnapshot(encoded);
        assertThatThrownBy(
                        () ->
                                new NullBitmapTransportTypeInfo.Snapshot()
                                        .readSnapshot(
                                                2,
                                                new DataInputDeserializer(
                                                        encoded.getCopyOfBuffer()),
                                                getClass().getClassLoader()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    void testInvalidSnapshotLengthsAreRejected() throws Exception {
        for (int length : new int[] {Integer.MIN_VALUE, -1, 0}) {
            DataOutputSerializer encoded = new DataOutputSerializer(4);
            encoded.writeInt(length);
            assertThatThrownBy(
                            () ->
                                    new NullBitmapTransportTypeInfo.Snapshot()
                                            .readSnapshot(
                                                    1,
                                                    new DataInputDeserializer(
                                                            encoded.getCopyOfBuffer()),
                                                    getClass().getClassLoader()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("length");
        }
    }
}
