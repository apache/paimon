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

package org.apache.paimon.data;

import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.data.DataFormatTestUtil.splitBytes;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NestedRow}s. */
public class NestedRowTest {

    @Test
    public void testNestedRowWithOneSegment() {
        BinaryRow row = getBinaryRow();
        InternalRow nestedRow = row.getRow(0, 5);
        assertThat(1).isEqualTo(nestedRow.getInt(0));
        assertThat(5L).isEqualTo(nestedRow.getLong(1));
        assertThat(BinaryString.fromString("12345678")).isEqualTo(nestedRow.getString(2));
        assertThat(nestedRow.isNullAt(3)).isTrue();
    }

    @Test
    public void testNestedRowWithMultipleSegments() {
        BinaryRow row = getBinaryRow();

        MemorySegment[] segments = splitBytes(row.getSegments()[0].getHeapMemory(), 3);
        row.pointTo(segments, 3, row.getSizeInBytes());
        {
            InternalRow nestedRow = row.getRow(0, 5);
            assertThat(1).isEqualTo(nestedRow.getInt(0));
            assertThat(5L).isEqualTo(nestedRow.getLong(1));
            assertThat(BinaryString.fromString("12345678")).isEqualTo(nestedRow.getString(2));
            assertThat(nestedRow.isNullAt(3)).isTrue();
        }
    }

    @Test
    public void testNestInNestedRow() {
        // layer1
        GenericRow gRow = new GenericRow(4);
        gRow.setField(0, 1);
        gRow.setField(1, 5L);
        gRow.setField(2, BinaryString.fromString("12345678"));
        gRow.setField(3, null);

        // layer2
        InternalRowSerializer serializer =
                new InternalRowSerializer(
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.STRING(),
                        DataTypes.STRING());
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString("hahahahafff"));
        writer.writeRow(1, gRow, serializer);
        writer.complete();

        // layer3
        BinaryRow row2 = new BinaryRow(1);
        BinaryRowWriter writer2 = new BinaryRowWriter(row2);
        writer2.writeRow(0, row, null);
        writer2.complete();

        // verify
        {
            NestedRow nestedRow = (NestedRow) row2.getRow(0, 2);
            BinaryRow binaryRow = new BinaryRow(2);
            binaryRow.pointTo(
                    nestedRow.getSegments(), nestedRow.getOffset(), nestedRow.getSizeInBytes());
            assertThat(row).isEqualTo(binaryRow);
        }

        assertThat(BinaryString.fromString("hahahahafff"))
                .isEqualTo(row2.getRow(0, 2).getString(0));
        InternalRow nestedRow = row2.getRow(0, 2).getRow(1, 4);
        assertThat(1).isEqualTo(nestedRow.getInt(0));
        assertThat(5L).isEqualTo(nestedRow.getLong(1));
        assertThat(BinaryString.fromString("12345678")).isEqualTo(nestedRow.getString(2));
        assertThat(nestedRow.isNullAt(3)).isTrue();
    }

    private BinaryRow getBinaryRow() {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        GenericRow gRow = new GenericRow(5);
        gRow.setField(0, 1);
        gRow.setField(1, 5L);
        gRow.setField(2, BinaryString.fromString("12345678"));
        gRow.setField(3, null);

        InternalRowSerializer serializer =
                new InternalRowSerializer(
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.STRING(),
                        DataTypes.STRING());
        writer.writeRow(0, gRow, serializer);
        writer.complete();

        return row;
    }
}
