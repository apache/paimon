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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for complex type conversions in {@link IcebergConversions}. */
class IcebergConversionsComplexTypeTest {

    // ======================== ARRAY tests ========================

    @Test
    void testArrayOfIntRoundTrip() {
        ArrayType type = DataTypes.ARRAY(DataTypes.INT());
        Object[] data = new Object[] {1, 2, 3, 4, 5};
        GenericArray array = new GenericArray(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, array);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            assertThat(result.getInt(i)).isEqualTo(i + 1);
        }
    }

    @Test
    void testArrayOfStringRoundTrip() {
        ArrayType type = DataTypes.ARRAY(DataTypes.STRING());
        Object[] data =
                new Object[] {
                    BinaryString.fromString("hello"),
                    BinaryString.fromString("world"),
                    BinaryString.fromString("paimon")
                };
        GenericArray array = new GenericArray(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, array);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.getString(0).toString()).isEqualTo("hello");
        assertThat(result.getString(1).toString()).isEqualTo("world");
        assertThat(result.getString(2).toString()).isEqualTo("paimon");
    }

    @Test
    void testEmptyArrayRoundTrip() {
        ArrayType type = DataTypes.ARRAY(DataTypes.INT());
        GenericArray array = new GenericArray(new Object[0]);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, array);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    void testArrayWithNullElements() {
        ArrayType type = DataTypes.ARRAY(DataTypes.INT().nullable());
        Object[] data = new Object[] {1, null, 3};
        GenericArray array = new GenericArray(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, array);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.isNullAt(1)).isTrue();
        assertThat(result.getInt(2)).isEqualTo(3);
    }

    @Test
    void testNestedArrayRoundTrip() {
        ArrayType innerType = DataTypes.ARRAY(DataTypes.INT());
        ArrayType outerType = DataTypes.ARRAY(innerType);
        GenericArray inner1 = new GenericArray(new Object[] {1, 2});
        GenericArray inner2 = new GenericArray(new Object[] {3, 4, 5});
        GenericArray outer = new GenericArray(new Object[] {inner1, inner2});

        ByteBuffer buf = IcebergConversions.toByteBuffer(outerType, outer);
        Object decoded = IcebergConversions.toPaimonObject(outerType, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(2);
        GenericArray resultInner1 = (GenericArray) result.getArray(0);
        assertThat(resultInner1.size()).isEqualTo(2);
        assertThat(resultInner1.getInt(0)).isEqualTo(1);
        assertThat(resultInner1.getInt(1)).isEqualTo(2);
        GenericArray resultInner2 = (GenericArray) result.getArray(1);
        assertThat(resultInner2.size()).isEqualTo(3);
    }

    // ======================== MAP tests ========================

    @Test
    void testMapIntToStringRoundTrip() {
        MapType type = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());
        Map<Object, Object> data = new HashMap<>();
        data.put(1, BinaryString.fromString("one"));
        data.put(2, BinaryString.fromString("two"));
        data.put(3, BinaryString.fromString("three"));
        GenericMap map = new GenericMap(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, map);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericMap.class);
        GenericMap result = (GenericMap) decoded;
        assertThat(result.size()).isEqualTo(3);
        InternalArray keys = result.keyArray();
        InternalArray values = result.valueArray();
        // Build a lookup map since key/value arrays are parallel
        Map<Integer, String> lookup = new HashMap<>();
        for (int i = 0; i < result.size(); i++) {
            lookup.put(keys.getInt(i), values.getString(i).toString());
        }
        assertThat(lookup).containsEntry(1, "one");
        assertThat(lookup).containsEntry(2, "two");
        assertThat(lookup).containsEntry(3, "three");
    }

    @Test
    void testEmptyMapRoundTrip() {
        MapType type = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        GenericMap map = new GenericMap(new HashMap<>());

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, map);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericMap.class);
        GenericMap result = (GenericMap) decoded;
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    void testMapWithNullValues() {
        MapType type = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING().nullable());
        Map<Object, Object> data = new HashMap<>();
        data.put(1, BinaryString.fromString("a"));
        data.put(2, null);
        GenericMap map = new GenericMap(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, map);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericMap.class);
        GenericMap result = (GenericMap) decoded;
        assertThat(result.size()).isEqualTo(2);
    }

    // ======================== ROW tests ========================

    @Test
    void testRowRoundTrip() {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING()),
                        DataTypes.FIELD(2, "score", DataTypes.BIGINT()));
        GenericRow row = new GenericRow(3);
        row.setField(0, 42);
        row.setField(1, BinaryString.fromString("Alice"));
        row.setField(2, 100L);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, row);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericRow.class);
        GenericRow result = (GenericRow) decoded;
        assertThat(result.getFieldCount()).isEqualTo(3);
        assertThat(result.getInt(0)).isEqualTo(42);
        assertThat(result.getString(1).toString()).isEqualTo("Alice");
        assertThat(result.getLong(2)).isEqualTo(100L);
    }

    @Test
    void testRowWithNullFields() {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "a", DataTypes.INT().nullable()),
                        DataTypes.FIELD(1, "b", DataTypes.STRING().nullable()));
        GenericRow row = new GenericRow(2);
        row.setField(0, null);
        row.setField(1, BinaryString.fromString("not-null"));

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, row);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericRow.class);
        GenericRow result = (GenericRow) decoded;
        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.isNullAt(0)).isTrue();
        assertThat(result.getString(1).toString()).isEqualTo("not-null");
    }

    @Test
    void testNestedRowRoundTrip() {
        RowType innerType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "x", DataTypes.INT()),
                        DataTypes.FIELD(1, "y", DataTypes.INT()));
        RowType outerType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "label", DataTypes.STRING()),
                        DataTypes.FIELD(1, "point", innerType));
        GenericRow innerRow = new GenericRow(2);
        innerRow.setField(0, 10);
        innerRow.setField(1, 20);
        GenericRow outerRow = new GenericRow(2);
        outerRow.setField(0, BinaryString.fromString("origin"));
        outerRow.setField(1, innerRow);

        ByteBuffer buf = IcebergConversions.toByteBuffer(outerType, outerRow);
        Object decoded = IcebergConversions.toPaimonObject(outerType, buf.array());

        assertThat(decoded).isInstanceOf(GenericRow.class);
        GenericRow result = (GenericRow) decoded;
        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("origin");
        GenericRow resultInner = (GenericRow) result.getRow(1, 2);
        assertThat(resultInner.getInt(0)).isEqualTo(10);
        assertThat(resultInner.getInt(1)).isEqualTo(20);
    }

    @Test
    void testRowAllFieldTypes() {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "f_bool", DataTypes.BOOLEAN()),
                        DataTypes.FIELD(1, "f_int", DataTypes.INT()),
                        DataTypes.FIELD(2, "f_bigint", DataTypes.BIGINT()),
                        DataTypes.FIELD(3, "f_float", DataTypes.FLOAT()),
                        DataTypes.FIELD(4, "f_double", DataTypes.DOUBLE()),
                        DataTypes.FIELD(5, "f_string", DataTypes.STRING()));
        GenericRow row = new GenericRow(6);
        row.setField(0, true);
        row.setField(1, 99);
        row.setField(2, 9999L);
        row.setField(3, 3.14f);
        row.setField(4, 2.718);
        row.setField(5, BinaryString.fromString("test"));

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, row);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericRow.class);
        GenericRow result = (GenericRow) decoded;
        assertThat(result.getFieldCount()).isEqualTo(6);
        assertThat(result.getBoolean(0)).isTrue();
        assertThat(result.getInt(1)).isEqualTo(99);
        assertThat(result.getLong(2)).isEqualTo(9999L);
        assertThat(result.getFloat(3)).isEqualTo(3.14f);
        assertThat(result.getDouble(4)).isEqualTo(2.718);
        assertThat(result.getString(5).toString()).isEqualTo("test");
    }

    // ======================== Mixed nesting tests ========================

    @Test
    void testArrayOfRowsRoundTrip() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                        DataTypes.FIELD(1, "b", DataTypes.STRING()));
        ArrayType arrayType = DataTypes.ARRAY(rowType);
        GenericRow row1 = new GenericRow(2);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("first"));
        GenericRow row2 = new GenericRow(2);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("second"));
        GenericArray array = new GenericArray(new Object[] {row1, row2});

        ByteBuffer buf = IcebergConversions.toByteBuffer(arrayType, array);
        Object decoded = IcebergConversions.toPaimonObject(arrayType, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(2);
        GenericRow r1 = (GenericRow) result.getRow(0, 2);
        assertThat(r1.getInt(0)).isEqualTo(1);
        assertThat(r1.getString(1).toString()).isEqualTo("first");
        GenericRow r2 = (GenericRow) result.getRow(1, 2);
        assertThat(r2.getInt(0)).isEqualTo(2);
        assertThat(r2.getString(1).toString()).isEqualTo("second");
    }

    @Test
    void testMapWithArrayValuesRoundTrip() {
        MapType type = DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()));
        Map<Object, Object> data = new HashMap<>();
        data.put(BinaryString.fromString("evens"), new GenericArray(new Object[] {2, 4, 6}));
        data.put(BinaryString.fromString("odds"), new GenericArray(new Object[] {1, 3, 5}));
        GenericMap map = new GenericMap(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, map);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericMap.class);
        GenericMap result = (GenericMap) decoded;
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testRowWithArrayAndMapFields() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.INT());
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "numbers", arrayType),
                        DataTypes.FIELD(1, "scores", mapType));
        GenericArray numbers = new GenericArray(new Object[] {10, 20, 30});
        Map<Object, Object> scoresMap = new HashMap<>();
        scoresMap.put(BinaryString.fromString("math"), 95);
        scoresMap.put(BinaryString.fromString("english"), 88);
        GenericMap scores = new GenericMap(scoresMap);
        GenericRow row = new GenericRow(2);
        row.setField(0, numbers);
        row.setField(1, scores);

        ByteBuffer buf = IcebergConversions.toByteBuffer(rowType, row);
        Object decoded = IcebergConversions.toPaimonObject(rowType, buf.array());

        assertThat(decoded).isInstanceOf(GenericRow.class);
        GenericRow result = (GenericRow) decoded;
        assertThat(result.getFieldCount()).isEqualTo(2);
        GenericArray decodedNumbers = (GenericArray) result.getArray(0);
        assertThat(decodedNumbers.size()).isEqualTo(3);
        assertThat(decodedNumbers.getInt(0)).isEqualTo(10);
        GenericMap decodedScores = (GenericMap) result.getMap(1);
        assertThat(decodedScores.size()).isEqualTo(2);
    }

    // ======================== Decimal in complex types ========================

    @Test
    void testArrayOfDecimalRoundTrip() {
        DecimalType decimalType = DataTypes.DECIMAL(10, 2);
        ArrayType type = DataTypes.ARRAY(decimalType);
        Object[] data =
                new Object[] {
                    Decimal.fromUnscaledBytes(new byte[] {0, 0, 0, 1}, 10, 2), // 0.01
                    Decimal.fromUnscaledBytes(new byte[] {0, 0, 0, (byte) 255}, 10, 2), // 2.55
                };
        GenericArray array = new GenericArray(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, array);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(2);
    }

    // ======================== Timestamp in complex types ========================

    @Test
    void testArrayOfTimestampRoundTrip() {
        ArrayType type = DataTypes.ARRAY(DataTypes.TIMESTAMP(3));
        Object[] data =
                new Object[] {
                    Timestamp.fromMicros(1000000L), // 1 second after epoch
                    Timestamp.fromMicros(2000000L),
                };
        GenericArray array = new GenericArray(data);

        ByteBuffer buf = IcebergConversions.toByteBuffer(type, array);
        Object decoded = IcebergConversions.toPaimonObject(type, buf.array());

        assertThat(decoded).isInstanceOf(GenericArray.class);
        GenericArray result = (GenericArray) decoded;
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.getTimestamp(0, 3).toMicros()).isEqualTo(1000000L);
        assertThat(result.getTimestamp(1, 3).toMicros()).isEqualTo(2000000L);
    }
}
