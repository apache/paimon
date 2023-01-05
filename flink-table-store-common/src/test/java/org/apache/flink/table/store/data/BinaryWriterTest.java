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

package org.apache.flink.table.store.data;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;

import org.junit.Test;

import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinaryWriter}s. */
public class BinaryWriterTest {

    @Test
    public void testWriter() {
        int arity = 13;
        BinaryRowData row = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row, 20);

        writer.writeString(0, fromString("1"));
        writer.writeString(3, fromString("1234567"));
        writer.writeString(5, fromString("12345678"));
        writer.writeString(9, fromString("啦啦啦啦啦我是快乐的粉刷匠"));

        writer.writeBoolean(1, true);
        writer.writeByte(2, (byte) 99);
        writer.writeDouble(6, 87.1d);
        writer.writeFloat(7, 26.1f);
        writer.writeInt(8, 88);
        writer.writeLong(10, 284);
        writer.writeShort(11, (short) 292);
        writer.setNullAt(12);

        writer.complete();

        assertTestWriterRow(row);
        assertTestWriterRow(row.copy());

        // test copy from var segments.
        int subSize = row.getFixedLengthPartSize() + 10;
        MemorySegment subMs1 = MemorySegmentFactory.wrap(new byte[subSize]);
        MemorySegment subMs2 = MemorySegmentFactory.wrap(new byte[subSize]);
        row.getSegments()[0].copyTo(0, subMs1, 0, subSize);
        row.getSegments()[0].copyTo(subSize, subMs2, 0, row.getSizeInBytes() - subSize);

        BinaryRowData toCopy = new BinaryRowData(arity);
        toCopy.pointTo(new MemorySegment[] {subMs1, subMs2}, 0, row.getSizeInBytes());
        assertThat(toCopy).isEqualTo(row);
        assertTestWriterRow(toCopy);
        assertTestWriterRow(toCopy.copy(new BinaryRowData(arity)));
    }

    private void assertTestWriterRow(BinaryRowData row) {
        assertThat(row.getString(0).toString()).isEqualTo("1");
        assertThat(row.getInt(8)).isEqualTo(88);
        assertThat(row.getShort(11)).isEqualTo((short) 292);
        assertThat(row.getLong(10)).isEqualTo(284);
        assertThat(row.getByte(2)).isEqualTo((byte) 99);
        assertThat(row.getDouble(6)).isEqualTo(87.1d);
        assertThat(row.getFloat(7)).isEqualTo(26.1f);
        assertThat(row.getBoolean(1)).isTrue();
        assertThat(row.getString(3).toString()).isEqualTo("1234567");
        assertThat(row.getString(5).toString()).isEqualTo("12345678");
        assertThat(row.getString(9).toString()).isEqualTo("啦啦啦啦啦我是快乐的粉刷匠");
        assertThat(row.getString(9).hashCode()).isEqualTo(fromString("啦啦啦啦啦我是快乐的粉刷匠").hashCode());
        assertThat(row.isNullAt(12)).isTrue();
    }

    @Test
    public void testArray() {
        // 1.array test
        BinaryArrayData array = new BinaryArrayData();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 6);
        writer.setNullInt(1);
        writer.writeInt(2, 666);
        writer.complete();

        assertThat(6).isEqualTo(array.getInt(0));
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(666).isEqualTo(array.getInt(2));

        // 2.test write to binary row.
        {
            BinaryRowData row2 = new BinaryRowData(1);
            BinaryRowWriter writer2 = new BinaryRowWriter(row2);
            writer2.writeArray(0, array, new ArrayDataSerializer(DataTypes.INT().getLogicalType()));
            writer2.complete();

            BinaryArrayData array2 = (BinaryArrayData) row2.getArray(0);
            assertThat(array).isEqualTo(array2);
            assertThat(6).isEqualTo(array2.getInt(0));
            assertThat(array2.isNullAt(1)).isTrue();
            assertThat(666).isEqualTo(array2.getInt(2));
        }

        // 3.test write var seg array to binary row.
        {
            BinaryArrayData array3 = splitArray(array);

            BinaryRowData row2 = new BinaryRowData(1);
            BinaryRowWriter writer2 = new BinaryRowWriter(row2);
            writer2.writeArray(
                    0, array3, new ArrayDataSerializer(DataTypes.INT().getLogicalType()));
            writer2.complete();

            BinaryArrayData array2 = (BinaryArrayData) row2.getArray(0);
            assertThat(array).isEqualTo(array2);
            assertThat(6).isEqualTo(array2.getInt(0));
            assertThat(array2.isNullAt(1)).isTrue();
            assertThat(666).isEqualTo(array2.getInt(2));
        }
    }

    private static BinaryArrayData splitArray(BinaryArrayData array) {
        BinaryArrayData ret = new BinaryArrayData();
        MemorySegment[] segments =
                splitBytes(
                        BinarySegmentUtils.copyToBytes(
                                array.getSegments(), 0, array.getSizeInBytes()),
                        0);
        ret.pointTo(segments, 0, array.getSizeInBytes());
        return ret;
    }

    private static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
        int newSize = (bytes.length + 1) / 2 + baseOffset;
        MemorySegment[] ret = new MemorySegment[2];
        ret[0] = MemorySegmentFactory.wrap(new byte[newSize]);
        ret[1] = MemorySegmentFactory.wrap(new byte[newSize]);

        ret[0].put(baseOffset, bytes, 0, newSize - baseOffset);
        ret[1].put(0, bytes, newSize - baseOffset, bytes.length - (newSize - baseOffset));
        return ret;
    }
}
