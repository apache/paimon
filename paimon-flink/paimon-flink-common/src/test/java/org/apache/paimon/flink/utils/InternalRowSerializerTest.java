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

package org.apache.paimon.flink.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

/** Tests for {@link InternalRowTypeSerializer}. */
public class InternalRowSerializerTest {

    private static final Random RANDOM = new Random();
    private static final RowType rowType =
            RowType.builder()
                    .field("a", DataTypes.STRING())
                    .field("b", DataTypes.INT())
                    .field("c", DataTypes.BIGINT())
                    .build();

    @Test
    public void testSerializeAndDeserilize() throws Exception {
        DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(100);
        InternalRowTypeSerializer internalRowTypeSerializer =
                new InternalRowTypeSerializer(rowType.getFieldTypes().toArray(new DataType[0]));

        InternalRow row = GenericRow.of(randomString(), RANDOM.nextInt(), RANDOM.nextLong());
        internalRowTypeSerializer.serialize(row, dataOutputSerializer);
        InternalRow row1 =
                internalRowTypeSerializer.deserialize(
                        new DataInputDeserializer(dataOutputSerializer.wrapAsByteBuffer()));

        Assertions.assertThat(row.getString(0)).isEqualTo(row1.getString(0));
        Assertions.assertThat(row.getInt(1)).isEqualTo(row1.getInt(1));
        Assertions.assertThat(row.getLong(2)).isEqualTo(row1.getLong(2));
    }

    @Test
    public void testEqual() {
        InternalRowTypeSerializer internalRowTypeSerializer =
                new InternalRowTypeSerializer(rowType.getFieldTypes().toArray(new DataType[0]));

        Assertions.assertThat(internalRowTypeSerializer)
                .isEqualTo(internalRowTypeSerializer.duplicate());
    }

    @Test
    public void testCopyFromView() {
        InternalRowTypeSerializer internalRowTypeSerializer =
                new InternalRowTypeSerializer(rowType.getFieldTypes().toArray(new DataType[0]));

        InternalRow row = GenericRow.of(randomString(), RANDOM.nextInt(), RANDOM.nextLong());
        InternalRow row1 = internalRowTypeSerializer.copy(row);

        Assertions.assertThat(row.getString(0)).isEqualTo(row1.getString(0));
        Assertions.assertThat(row.getInt(1)).isEqualTo(row1.getInt(1));
        Assertions.assertThat(row.getLong(2)).isEqualTo(row1.getLong(2));
    }

    private BinaryString randomString() {
        int length = RANDOM.nextInt(100);
        byte[] buffer = new byte[length];
        for (int i = 0; i < length; i += 1) {
            buffer[i] = (byte) ('A' + RANDOM.nextInt(26));
        }
        return BinaryString.fromBytes(buffer);
    }
}
