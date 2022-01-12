/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.utils;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link OffsetRowData}. */
public class OffsetRowDataTest {

    private StringData str;
    private RawValueData<String> generic;
    private DecimalData decimal1;
    private DecimalData decimal2;
    private BinaryArrayData array;
    private BinaryMapData map;
    private BinaryRowData underRow;
    private byte[] bytes;
    private TimestampData timestamp1;
    private TimestampData timestamp2;

    @BeforeEach
    public void before() {
        str = StringData.fromString("haha");
        generic = RawValueData.fromObject("haha");
        decimal1 = DecimalData.fromUnscaledLong(10, 5, 0);
        decimal2 = DecimalData.fromBigDecimal(new BigDecimal(11), 20, 0);
        array = new BinaryArrayData();
        {
            BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, 2, 4);
            arrayWriter.writeInt(0, 15);
            arrayWriter.writeInt(1, 16);
            arrayWriter.complete();
        }
        map = BinaryMapData.valueOf(array, array);
        underRow = new BinaryRowData(2);
        {
            BinaryRowWriter writer = new BinaryRowWriter(underRow);
            writer.writeInt(0, 15);
            writer.writeInt(1, 16);
            writer.complete();
        }
        bytes = new byte[] {1, 5, 6};
        timestamp1 = TimestampData.fromEpochMillis(123L);
        timestamp2 =
                TimestampData.fromLocalDateTime(LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
    }

    private RowData createRow() {
        GenericRowData row = new GenericRowData(19);
        row.setField(0, true);
        row.setField(1, (byte) 1);
        row.setField(2, (short) 2);
        row.setField(3, 3);
        row.setField(4, (long) 4);
        row.setField(5, (float) 5);
        row.setField(6, (double) 6);
        row.setField(7, (char) 7);
        row.setField(8, str);
        row.setField(9, generic);
        row.setField(10, decimal1);
        row.setField(11, decimal2);
        row.setField(12, array);
        row.setField(13, map);
        row.setField(14, underRow);
        row.setField(15, bytes);
        row.setField(16, timestamp1);
        row.setField(17, timestamp2);
        row.setField(18, null);
        return row;
    }

    @Test
    public void testGenericRow() {
        RowData underRow = createRow();
        testGetters(
                new OffsetRowData(underRow.getArity(), 5)
                        .replace(new JoinedRowData().replace(new GenericRowData(5), underRow)));
        testGetters(new OffsetRowData(underRow.getArity(), 0).replace(underRow));
        testGetters(
                new OffsetRowData(underRow.getArity(), 1)
                        .replace(new JoinedRowData().replace(new GenericRowData(1), underRow)));
    }

    private void testGetters(RowData row) {
        assertEquals(19, row.getArity());

        // test header
        assertEquals(RowKind.INSERT, row.getRowKind());
        row.setRowKind(RowKind.DELETE);
        assertEquals(RowKind.DELETE, row.getRowKind());

        // test get
        assertTrue(row.getBoolean(0));
        assertEquals(1, row.getByte(1));
        assertEquals(2, row.getShort(2));
        assertEquals(3, row.getInt(3));
        assertEquals(4, row.getLong(4));
        assertEquals(5, (int) row.getFloat(5));
        assertEquals(6, (int) row.getDouble(6));
        assertEquals(str, row.getString(8));
        assertThat(generic.toObject(StringSerializer.INSTANCE))
                .isEqualTo(row.<String>getRawValue(9).toObject(StringSerializer.INSTANCE));
        assertEquals(decimal1, row.getDecimal(10, 5, 0));
        assertEquals(decimal2, row.getDecimal(11, 20, 0));
        assertEquals(array, row.getArray(12));
        assertEquals(map, row.getMap(13));
        assertEquals(15, row.getRow(14, 2).getInt(0));
        assertEquals(16, row.getRow(14, 2).getInt(1));
        assertArrayEquals(bytes, row.getBinary(15));
        assertEquals(timestamp1, row.getTimestamp(16, 3));
        assertEquals(timestamp2, row.getTimestamp(17, 9));

        assertFalse(row.isNullAt(0));
        assertTrue(row.isNullAt(18));
    }
}
