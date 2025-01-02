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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.BooleanColumnVector;
import org.apache.paimon.data.columnar.ByteColumnVector;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.DoubleColumnVector;
import org.apache.paimon.data.columnar.FloatColumnVector;
import org.apache.paimon.data.columnar.IntColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.data.columnar.ShortColumnVector;
import org.apache.paimon.data.columnar.TimestampColumnVector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** Tests for {@link VectorMappingUtils}. */
public class VectorMappingUtilsTest {

    @Test
    public void testCreatePartitionMappedVectors() {
        ColumnVector[] columnVectors = new ColumnVector[5];

        Arrays.fill(columnVectors, (ColumnVector) i -> false);

        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);
        binaryRowWriter.writeString(0, BinaryString.fromString("a"));
        binaryRowWriter.complete();

        PartitionInfo partitionInfo =
                new PartitionInfo(
                        new int[] {1, 2, -1, 3, 4, 5, 0},
                        RowType.of(DataTypes.STRING()),
                        binaryRow);

        ColumnVector[] newColumnVectors =
                VectorMappingUtils.createPartitionMappedVectors(partitionInfo, columnVectors);

        for (int i = 0; i < partitionInfo.size(); i++) {
            if (!partitionInfo.inPartitionRow(i)) {
                Assertions.assertThat(newColumnVectors[i])
                        .isEqualTo(columnVectors[partitionInfo.getRealIndex(i)]);
            }
        }
    }

    @Test
    public void testCreateIndexMappedVectors() {

        ColumnVector[] columnVectors = new ColumnVector[5];

        Arrays.fill(columnVectors, (ColumnVector) i -> false);

        int[] mapping = new int[] {0, 2, 1, 3, 2, 3, 1, 0, 4};

        ColumnVector[] newColumnVectors =
                VectorMappingUtils.createMappedVectors(mapping, columnVectors);

        for (int i = 0; i < mapping.length; i++) {
            Assertions.assertThat(newColumnVectors[i]).isEqualTo(columnVectors[mapping[i]]);
        }
    }

    @Test
    public void testForType() {
        RowType rowType =
                RowType.builder()
                        .fields(
                                DataTypes.INT(),
                                DataTypes.TINYINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT(),
                                DataTypes.STRING(),
                                DataTypes.DOUBLE(),
                                DataTypes.CHAR(100),
                                DataTypes.VARCHAR(100),
                                DataTypes.BOOLEAN(),
                                DataTypes.DATE(),
                                DataTypes.TIME(),
                                DataTypes.TIMESTAMP(),
                                DataTypes.FLOAT())
                        .build();

        BinaryRow binaryRow = new BinaryRow(13);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);
        binaryRowWriter.writeInt(0, 0);
        binaryRowWriter.writeByte(1, (byte) 1);
        binaryRowWriter.writeShort(2, (short) 2);
        binaryRowWriter.writeLong(3, 3L);
        binaryRowWriter.writeString(4, BinaryString.fromString("4"));
        binaryRowWriter.writeDouble(5, 5.0);
        binaryRowWriter.writeString(6, BinaryString.fromString("6"));
        binaryRowWriter.writeString(7, BinaryString.fromString("7"));
        binaryRowWriter.writeBoolean(8, true);
        binaryRowWriter.writeInt(9, 9);
        binaryRowWriter.writeInt(10, 10);
        binaryRowWriter.writeTimestamp(
                11, Timestamp.fromEpochMillis(System.currentTimeMillis()), 10);
        binaryRowWriter.writeFloat(12, (float) 12.0);
        binaryRowWriter.complete();

        int[] map = {-1, -2, -3, -4, -5, -6, 1, -7, -8, -9, -10, -11, -12, -13, 0};
        PartitionInfo partitionInfo = new PartitionInfo(map, rowType, binaryRow);
        ColumnVector[] columnVectors = new ColumnVector[1];

        Arrays.fill(columnVectors, (ColumnVector) i -> false);

        ColumnVector[] newColumnVectors =
                VectorMappingUtils.createPartitionMappedVectors(partitionInfo, columnVectors);

        Assertions.assertThat(newColumnVectors[0]).isInstanceOf(IntColumnVector.class);
        Assertions.assertThat(((IntColumnVector) newColumnVectors[0]).getInt(0)).isEqualTo(0);

        Assertions.assertThat(newColumnVectors[1]).isInstanceOf(ByteColumnVector.class);
        Assertions.assertThat(((ByteColumnVector) newColumnVectors[1]).getByte(0))
                .isEqualTo((byte) 1);

        Assertions.assertThat(newColumnVectors[2]).isInstanceOf(ShortColumnVector.class);
        Assertions.assertThat(((ShortColumnVector) newColumnVectors[2]).getShort(0))
                .isEqualTo((short) 2);

        Assertions.assertThat(newColumnVectors[3]).isInstanceOf(LongColumnVector.class);
        Assertions.assertThat(((LongColumnVector) newColumnVectors[3]).getLong(0)).isEqualTo(3L);

        Assertions.assertThat(newColumnVectors[4]).isInstanceOf(BytesColumnVector.class);
        Assertions.assertThat(((BytesColumnVector) newColumnVectors[4]).getBytes(0).data)
                .isEqualTo("4".getBytes());

        Assertions.assertThat(newColumnVectors[5]).isInstanceOf(DoubleColumnVector.class);
        Assertions.assertThat(((DoubleColumnVector) newColumnVectors[5]).getDouble(0))
                .isEqualTo(5.0);

        Assertions.assertThat(newColumnVectors[7]).isInstanceOf(BytesColumnVector.class);
        Assertions.assertThat(((BytesColumnVector) newColumnVectors[7]).getBytes(0).data)
                .isEqualTo("6".getBytes());

        Assertions.assertThat(newColumnVectors[8]).isInstanceOf(BytesColumnVector.class);
        Assertions.assertThat(((BytesColumnVector) newColumnVectors[8]).getBytes(0).data)
                .isEqualTo("7".getBytes());

        Assertions.assertThat(newColumnVectors[9]).isInstanceOf(BooleanColumnVector.class);
        Assertions.assertThat(((BooleanColumnVector) newColumnVectors[9]).getBoolean(0))
                .isEqualTo(true);

        Assertions.assertThat(newColumnVectors[10]).isInstanceOf(IntColumnVector.class);
        Assertions.assertThat(((IntColumnVector) newColumnVectors[10]).getInt(0)).isEqualTo(9);

        Assertions.assertThat(newColumnVectors[11]).isInstanceOf(IntColumnVector.class);
        Assertions.assertThat(((IntColumnVector) newColumnVectors[11]).getInt(0)).isEqualTo(10);

        Assertions.assertThat(newColumnVectors[12]).isInstanceOf(TimestampColumnVector.class);

        Assertions.assertThat(newColumnVectors[13]).isInstanceOf(FloatColumnVector.class);
        Assertions.assertThat(((FloatColumnVector) newColumnVectors[13]).getFloat(0))
                .isEqualTo((float) 12.0);
    }
}
