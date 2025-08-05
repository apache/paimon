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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.types.DataTypesTest.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FallbackMappingRow}. */
public class FallbackMappingRowTest {

    @Test
    public void testBasic() {
        int[] project = new int[7];

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 111L);
        genericRow.setField(1, 222L);

        project[0] = -1;
        project[1] = -1;
        project[2] = -1;
        project[3] = -1;
        project[4] = -1;
        project[5] = 1;
        project[6] = 0;

        GenericRow mainRow = new GenericRow(7);
        mainRow.setField(0, 0L);
        mainRow.setField(1, 1L);
        mainRow.setField(2, 2L);
        mainRow.setField(3, 3L);
        FallbackMappingRow fallbackMappingRow = new FallbackMappingRow(project);
        fallbackMappingRow.replace(mainRow, genericRow);

        assertThat(fallbackMappingRow.getFieldCount()).isEqualTo(7);
        assertThat(fallbackMappingRow.getLong(6)).isEqualTo(111L);
        assertThat(fallbackMappingRow.getLong(5)).isEqualTo(222L);
        assertThat(fallbackMappingRow.getLong(0)).isEqualTo(0L);
        assertThat(fallbackMappingRow.getLong(1)).isEqualTo(1L);
        assertThat(fallbackMappingRow.getLong(2)).isEqualTo(2L);
        assertThat(fallbackMappingRow.getLong(3)).isEqualTo(3L);
        assertThat(fallbackMappingRow.isNullAt(4)).isEqualTo(true);
    }

    @Test
    public void testFallbackMappingRow() {
        // Create main row
        InternalRow mainRow =
                GenericRow.of(
                        1, null, Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2), null);

        // Create fallback row
        InternalRow fallbackRow =
                GenericRow.of(
                        2,
                        BinaryString.fromString("world"),
                        Decimal.fromBigDecimal(new BigDecimal("56.78"), 10, 2),
                        Timestamp.fromEpochMillis(1678972800000L));

        // Define mappings
        int[] mappings = {0, -1, 2, -1};

        // Create FallbackMappingRow
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        // Test getFieldCount
        assertEquals(mainRow.getFieldCount(), mappingRow.getFieldCount());

        // Test getRowKind
        mainRow.setRowKind(RowKind.INSERT);
        assertEquals(mainRow.getRowKind(), mappingRow.getRowKind());

        // Test getRowKind
        mainRow.setRowKind(RowKind.INSERT);
        assertEquals(mainRow.getRowKind(), mappingRow.getRowKind());

        // Test setRowKind
        mappingRow.setRowKind(RowKind.UPDATE_AFTER);
        assertEquals(RowKind.UPDATE_AFTER, mappingRow.getRowKind());

        // Test isNullAt
        assertFalse(mappingRow.isNullAt(0)); // main is not null
        assertTrue(mappingRow.isNullAt(1)); // mapping is -1 and main is null

        // Test getBoolean (not directly applicable, but included for completeness)
        // Test getByte (not directly applicable)
        // Test getShort (not directly applicable)

        // Test getInt
        assertEquals(1, mappingRow.getInt(0));

        // Test getString
        assertNull(mappingRow.getString(1));

        // Test getDecimal
        assertEquals(
                Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2),
                mappingRow.getDecimal(2, 10, 2));

        // Test getTimestamp
        assertNull(mappingRow.getTimestamp(3, 3));

        // Test all other methods which will fallback if and only if the mapping is not -1, and the
        // main is null
        InternalRow mainNullRow = GenericRow.of(null, null, null, null);
        mappingRow.replace(mainNullRow, fallbackRow);

        assertEquals(fallbackRow.getInt(0), mappingRow.getInt(0));
        assertEquals(fallbackRow.getDecimal(2, 10, 2), mappingRow.getDecimal(2, 10, 2));
    }

    @Test
    public void testBooleanFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(true);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertTrue(mappingRow.getBoolean(0));

        mainRow = GenericRow.of(false);
        mappingRow.replace(mainRow, fallbackRow);
        assertFalse(mappingRow.getBoolean(0));
    }

    @Test
    public void testByteFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of((byte) 123);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals((byte) 123, mappingRow.getByte(0));

        mainRow = GenericRow.of((byte) 45);
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals((byte) 45, mappingRow.getByte(0));
    }

    @Test
    public void testShortFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of((short) 12345);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals((short) 12345, mappingRow.getShort(0));

        mainRow = GenericRow.of((short) 6789);
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals((short) 6789, mappingRow.getShort(0));
    }

    @Test
    public void testIntFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(123456789);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(123456789, mappingRow.getInt(0));

        mainRow = GenericRow.of(987654321);
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals(987654321, mappingRow.getInt(0));
    }

    @Test
    public void testLongFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(1234567890123456789L);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(1234567890123456789L, mappingRow.getLong(0));

        mainRow = GenericRow.of(987654321098765432L);
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals(987654321098765432L, mappingRow.getLong(0));
    }

    @Test
    public void testFloatFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(123.456f);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(123.456f, mappingRow.getFloat(0));

        mainRow = GenericRow.of(678.901f);
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals(678.901f, mappingRow.getFloat(0));
    }

    @Test
    public void testDoubleFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(123456789.987654321);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(123456789.987654321, mappingRow.getDouble(0));

        mainRow = GenericRow.of(987654321.123456789);
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals(987654321.123456789, mappingRow.getDouble(0));
    }

    @Test
    public void testStringFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(BinaryString.fromString("fallback string"));
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(BinaryString.fromString("fallback string"), mappingRow.getString(0));

        mainRow = GenericRow.of(BinaryString.fromString("main string"));
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals(BinaryString.fromString("main string"), mappingRow.getString(0));
    }

    @Test
    public void testDecimalFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow =
                GenericRow.of(Decimal.fromBigDecimal(new BigDecimal("123.45"), 5, 2));
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(
                Decimal.fromBigDecimal(new BigDecimal("123.45"), 5, 2),
                mappingRow.getDecimal(0, 5, 2));

        mainRow = GenericRow.of(Decimal.fromBigDecimal(new BigDecimal("67.89"), 5, 2));
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals(
                Decimal.fromBigDecimal(new BigDecimal("67.89"), 5, 2),
                mappingRow.getDecimal(0, 5, 2));
    }

    @Test
    public void testTimestampFallback() {
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow =
                GenericRow.of(Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 12, 30, 0)));
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(
                Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 12, 30, 0)),
                mappingRow.getTimestamp(0, 6));

        mainRow =
                GenericRow.of(
                        Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 12, 31, 23, 59, 59)));
        mappingRow.replace(mainRow, fallbackRow);
        assertEquals(
                Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 12, 31, 23, 59, 59)),
                mappingRow.getTimestamp(0, 6));
    }

    @Test
    public void testBinaryFallback() {
        byte[] binaryData = new byte[] {1, 2, 3};
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(binaryData);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        byte[] retrievedData = mappingRow.getBinary(0);
        assertEquals(binaryData.length, retrievedData.length);
        for (int i = 0; i < binaryData.length; i++) {
            assertEquals(binaryData[i], retrievedData[i]);
        }

        byte[] mainBinaryData = new byte[] {4, 5, 6};
        mainRow = GenericRow.of(mainBinaryData);
        mappingRow.replace(mainRow, fallbackRow);
        retrievedData = mappingRow.getBinary(0);
        assertEquals(mainBinaryData.length, retrievedData.length);
        for (int i = 0; i < mainBinaryData.length; i++) {
            assertEquals(mainBinaryData[i], retrievedData[i]);
        }
    }

    @Test
    public void testArrayFallback() {
        InternalArray arrayData = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(arrayData);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        InternalArray retrievedArray = mappingRow.getArray(0);
        assertEquals(arrayData.size(), retrievedArray.size());
        for (int i = 0; i < arrayData.size(); i++) {
            assertEquals(arrayData.getInt(i), retrievedArray.getInt(i));
        }

        InternalArray mainArrayData = BinaryArray.fromPrimitiveArray(new int[] {4, 5, 6});
        mainRow = GenericRow.of(mainArrayData);
        mappingRow.replace(mainRow, fallbackRow);
        retrievedArray = mappingRow.getArray(0);
        assertEquals(mainArrayData.size(), retrievedArray.size());
        for (int i = 0; i < mainArrayData.size(); i++) {
            assertEquals(mainArrayData.getInt(i), retrievedArray.getInt(i));
        }
    }

    @Test
    public void testMapFallback() {
        Map<BinaryString, Integer> mapData = new HashMap<>();
        mapData.put(BinaryString.fromString("a"), 1);
        mapData.put(BinaryString.fromString("b"), 2);
        InternalMap internalMapData = new GenericMap(mapData);

        InternalRow mainRow = new GenericRow(1);
        InternalRow fallbackRow = GenericRow.of(internalMapData);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        InternalMap retrievedMap = mappingRow.getMap(0);
        assertEquals(internalMapData.size(), retrievedMap.size());

        Map<BinaryString, Integer> mainMapData = new HashMap<>();
        mainMapData.put(BinaryString.fromString("c"), 3);
        mainMapData.put(BinaryString.fromString("d"), 4);
        InternalMap mainInternalMapData = new GenericMap(mainMapData);
        mainRow = GenericRow.of(mainInternalMapData);
        mappingRow.replace(mainRow, fallbackRow);
        retrievedMap = mappingRow.getMap(0);
        assertEquals(mainInternalMapData.size(), retrievedMap.size());
    }

    @Test
    public void testRowFallback() {
        InternalRow rowData = GenericRow.of(1, BinaryString.fromString("a"));
        InternalRow mainRow = new GenericRow(2);
        InternalRow fallbackRow = GenericRow.of(rowData);
        int[] mappings = {0};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        InternalRow retrievedRow = mappingRow.getRow(0, 2);
        assertEquals(rowData.getInt(0), retrievedRow.getInt(0));
        assertEquals(rowData.getString(1), retrievedRow.getString(1));

        InternalRow mainRowData = GenericRow.of(2, BinaryString.fromString("b"));
        mainRow = GenericRow.of(mainRowData);
        mappingRow.replace(mainRow, fallbackRow);
        retrievedRow = mappingRow.getRow(0, 2);
        assertEquals(mainRowData.getInt(0), retrievedRow.getInt(0));
        assertEquals(mainRowData.getString(1), retrievedRow.getString(1));
    }

    @Test
    public void testNullMainNotNullFallback() {
        InternalRow mainRow = GenericRow.of(null, null);
        InternalRow fallbackRow = GenericRow.of(123, BinaryString.fromString("test"));
        int[] mappings = {0, 1};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertFalse(mappingRow.isNullAt(0));
        assertFalse(mappingRow.isNullAt(1));
        assertEquals(123, mappingRow.getInt(0));
        assertEquals(BinaryString.fromString("test"), mappingRow.getString(1));
    }

    @Test
    public void testNotNullMainNullFallback() {
        InternalRow mainRow = GenericRow.of(123, BinaryString.fromString("test"));
        InternalRow fallbackRow = GenericRow.of(null, null);
        int[] mappings = {0, 1};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertEquals(123, mappingRow.getInt(0));
        assertEquals(BinaryString.fromString("test"), mappingRow.getString(1));
    }

    @Test
    public void testBothNull() {
        InternalRow mainRow = GenericRow.of(null, null);
        InternalRow fallbackRow = GenericRow.of(null, null);
        int[] mappings = {0, 1};
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);

        assertTrue(mappingRow.isNullAt(0));
        assertTrue(mappingRow.isNullAt(1));
    }

    @Test
    public void testNullCheck() {
        int[] mappings = new int[] {0, 1};
        GenericRow mainRow = new GenericRow(2);
        GenericRow fallbackRow = new GenericRow(2);
        fallbackRow.setField(0, 1L);
        fallbackRow.setField(1, 2L);
        FallbackMappingRow mappingRow = new FallbackMappingRow(mappings);
        mappingRow.replace(mainRow, fallbackRow);
        assertFalse(mappingRow.isNullAt(0));
        assertFalse(mappingRow.isNullAt(1));
        assertThat(mappingRow.getLong(0)).isEqualTo(1L);
        assertThat(mappingRow.getLong(1)).isEqualTo(2L);
    }
}
