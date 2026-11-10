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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NullBitmapTransportTypeInfoTest {

    private static TypeSerializer<RowData> serializer() {
        return new NullBitmapTransportTypeInfo(
                        RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()))
                .createSerializer(new ExecutionConfig());
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
