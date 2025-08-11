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

package org.apache.paimon.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionRow}. */
public class DataEvolutionRowTest {

    private InternalRow row1;
    private InternalRow row2;
    private DataEvolutionRow dataEvolutionRow;

    @BeforeEach
    public void setUp() {
        row1 = mock(InternalRow.class);
        row2 = mock(InternalRow.class);

        // Schema: (from row1), (from row2), (null), (from row1)
        int[] rowOffsets = new int[] {0, 1, -1, 0};
        int[] fieldOffsets = new int[] {0, 0, -1, 1};

        dataEvolutionRow = new DataEvolutionRow(2, rowOffsets, fieldOffsets);
        dataEvolutionRow.setRow(0, row1);
        dataEvolutionRow.setRow(1, row2);
    }

    @Test
    public void testGetFieldCount() {
        assertThat(dataEvolutionRow.getFieldCount()).isEqualTo(4);
    }

    @Test
    public void testSetRowOutOfBounds() {
        assertThatThrownBy(() -> dataEvolutionRow.setRow(2, mock(InternalRow.class)))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Position 2 is out of bounds for rows size 2");
    }

    @Test
    public void testRowKind() {
        dataEvolutionRow.setRowKind(RowKind.INSERT);
        verify(row1).setRowKind(RowKind.INSERT);

        when(row1.getRowKind()).thenReturn(RowKind.DELETE);
        assertThat(dataEvolutionRow.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    public void testIsNullAt() {
        // Test null from rowOffsets (field added by schema evolution)
        assertThat(dataEvolutionRow.isNullAt(2)).isTrue();

        // Test null from underlying row
        when(row1.isNullAt(0)).thenReturn(true);
        assertThat(dataEvolutionRow.isNullAt(0)).isTrue();

        // Test not null
        when(row2.isNullAt(0)).thenReturn(false);
        assertThat(dataEvolutionRow.isNullAt(1)).isFalse();
    }

    @Test
    public void testGetBoolean() {
        when(row1.getBoolean(0)).thenReturn(true);
        assertThat(dataEvolutionRow.getBoolean(0)).isTrue();
    }

    @Test
    public void testGetByte() {
        when(row1.getByte(0)).thenReturn((byte) 1);
        assertThat(dataEvolutionRow.getByte(0)).isEqualTo((byte) 1);
    }

    @Test
    public void testGetShort() {
        when(row1.getShort(0)).thenReturn((short) 2);
        assertThat(dataEvolutionRow.getShort(0)).isEqualTo((short) 2);
    }

    @Test
    public void testGetInt() {
        when(row1.getInt(0)).thenReturn(3);
        assertThat(dataEvolutionRow.getInt(0)).isEqualTo(3);
    }

    @Test
    public void testGetLong() {
        when(row2.getLong(0)).thenReturn(4L);
        assertThat(dataEvolutionRow.getLong(1)).isEqualTo(4L);
    }

    @Test
    public void testGetFloat() {
        when(row1.getFloat(1)).thenReturn(5.5f);
        assertThat(dataEvolutionRow.getFloat(3)).isEqualTo(5.5f);
    }

    @Test
    public void testGetDouble() {
        when(row2.getDouble(0)).thenReturn(6.6d);
        assertThat(dataEvolutionRow.getDouble(1)).isEqualTo(6.6d);
    }

    @Test
    public void testGetString() {
        BinaryString value = BinaryString.fromString("test");
        when(row1.getString(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getString(3)).isSameAs(value);
    }

    @Test
    public void testGetDecimal() {
        Decimal value = Decimal.fromUnscaledLong(123, 5, 2);
        when(row1.getDecimal(1, 5, 2)).thenReturn(value);
        assertThat(dataEvolutionRow.getDecimal(3, 5, 2)).isSameAs(value);
    }

    @Test
    public void testGetTimestamp() {
        Timestamp value = Timestamp.fromEpochMillis(1000);
        when(row2.getTimestamp(0, 3)).thenReturn(value);
        assertThat(dataEvolutionRow.getTimestamp(1, 3)).isSameAs(value);
    }

    @Test
    public void testGetBinary() {
        byte[] value = new byte[] {1, 2, 3};
        when(row1.getBinary(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getBinary(3)).isSameAs(value);
    }

    @Test
    public void testGetVariant() {
        Variant value = mock(Variant.class);
        when(row1.getVariant(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getVariant(3)).isSameAs(value);
    }

    @Test
    public void testGetArray() {
        InternalArray value = mock(InternalArray.class);
        when(row2.getArray(0)).thenReturn(value);
        assertThat(dataEvolutionRow.getArray(1)).isSameAs(value);
    }

    @Test
    public void testGetMap() {
        InternalMap value = mock(InternalMap.class);
        when(row1.getMap(1)).thenReturn(value);
        assertThat(dataEvolutionRow.getMap(3)).isSameAs(value);
    }

    @Test
    public void testGetRow() {
        InternalRow value = mock(InternalRow.class);
        when(row2.getRow(0, 5)).thenReturn(value);
        assertThat(dataEvolutionRow.getRow(1, 5)).isSameAs(value);
    }
}
