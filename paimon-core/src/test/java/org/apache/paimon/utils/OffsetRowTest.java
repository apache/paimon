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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OffsetRow}. */
public class OffsetRowTest {

    private BinaryString str;
    private Decimal decimal1;
    private Decimal decimal2;
    private BinaryArray array;
    private BinaryMap map;
    private BinaryRow underRow;
    private byte[] bytes;
    private Timestamp timestamp1;
    private Timestamp timestamp2;

    @BeforeEach
    public void before() {
        str = BinaryString.fromString("haha");
        decimal1 = Decimal.fromUnscaledLong(10, 5, 0);
        decimal2 = Decimal.fromBigDecimal(new BigDecimal(11), 20, 0);
        array = new BinaryArray();
        {
            BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, 2, 4);
            arrayWriter.writeInt(0, 15);
            arrayWriter.writeInt(1, 16);
            arrayWriter.complete();
        }
        map = BinaryMap.valueOf(array, array);
        underRow = new BinaryRow(2);
        {
            BinaryRowWriter writer = new BinaryRowWriter(underRow);
            writer.writeInt(0, 15);
            writer.writeInt(1, 16);
            writer.complete();
        }
        bytes = new byte[] {1, 5, 6};
        timestamp1 = Timestamp.fromEpochMillis(123L);
        timestamp2 = Timestamp.fromLocalDateTime(LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
    }

    private InternalRow createRow() {
        GenericRow row = new GenericRow(19);
        row.setField(0, true);
        row.setField(1, (byte) 1);
        row.setField(2, (short) 2);
        row.setField(3, 3);
        row.setField(4, (long) 4);
        row.setField(5, (float) 5);
        row.setField(6, (double) 6);
        row.setField(7, (char) 7);
        row.setField(8, str);
        row.setField(9, decimal1);
        row.setField(10, decimal2);
        row.setField(11, array);
        row.setField(12, map);
        row.setField(13, underRow);
        row.setField(14, bytes);
        row.setField(15, timestamp1);
        row.setField(16, timestamp2);
        row.setField(17, null);
        return row;
    }

    @Test
    public void testGenericRow() {
        InternalRow underRow = createRow();
        testGetters(
                new OffsetRow(underRow.getFieldCount(), 5)
                        .replace(new JoinedRow().replace(new GenericRow(5), underRow)));
        testGetters(new OffsetRow(underRow.getFieldCount(), 0).replace(underRow));
        testGetters(
                new OffsetRow(underRow.getFieldCount(), 1)
                        .replace(new JoinedRow().replace(new GenericRow(1), underRow)));
    }

    private void testGetters(InternalRow row) {
        assertThat(row.getFieldCount()).isEqualTo(19);

        // test header
        assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
        row.setRowKind(RowKind.DELETE);
        assertThat(row.getRowKind()).isEqualTo(RowKind.DELETE);

        // test get
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 1);
        assertThat(row.getShort(2)).isEqualTo((short) 2);
        assertThat(row.getInt(3)).isEqualTo(3);
        assertThat(row.getLong(4)).isEqualTo(4);
        assertThat((int) row.getFloat(5)).isEqualTo(5);
        assertThat((int) row.getDouble(6)).isEqualTo(6);
        assertThat(row.getString(8)).isEqualTo(str);
        assertThat(row.getDecimal(9, 5, 0)).isEqualTo(decimal1);
        assertThat(row.getDecimal(10, 20, 0)).isEqualTo(decimal2);
        assertThat(row.getArray(11)).isEqualTo(array);
        assertThat(row.getMap(12)).isEqualTo(map);
        assertThat(row.getRow(13, 2).getInt(0)).isEqualTo(15);
        assertThat(row.getRow(13, 2).getInt(1)).isEqualTo(16);
        assertThat(row.getBinary(14)).containsExactlyInAnyOrder(bytes);
        assertThat(row.getTimestamp(15, 3)).isEqualTo(timestamp1);
        assertThat(row.getTimestamp(16, 9)).isEqualTo(timestamp2);

        assertThat(row.isNullAt(0)).isFalse();
        assertThat(row.isNullAt(17)).isTrue();
    }
}
