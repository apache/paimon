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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapBooleanVector;
import org.apache.paimon.data.columnar.heap.HeapByteVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapDoubleVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.columnar.heap.HeapShortVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowToColumnConverter}. */
public class RowToColumnConverterTest {

    @Test
    public void testConvertIntType() {
        RowType rowType = RowType.of(new DataField(0, "f", new IntType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(123);
        GenericRow row2 = GenericRow.of(456);

        HeapIntVector intVector = new HeapIntVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {intVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(intVector.getInt(0)).isEqualTo(123);
        assertThat(intVector.getInt(1)).isEqualTo(456);
    }

    @Test
    public void testConvertNullableIntType() {
        RowType rowType = RowType.of(new DataField(0, "f", new IntType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of((Object) null);
        GenericRow row2 = GenericRow.of(789);

        HeapIntVector intVector = new HeapIntVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {intVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(intVector.isNullAt(0)).isTrue();
        assertThat(intVector.getInt(1)).isEqualTo(789);
    }

    @Test
    public void testConvertBooleanType() {
        RowType rowType = RowType.of(new DataField(0, "f", new BooleanType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(true);
        GenericRow row2 = GenericRow.of(false);

        HeapBooleanVector booleanVector = new HeapBooleanVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {booleanVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(booleanVector.getBoolean(0)).isTrue();
        assertThat(booleanVector.getBoolean(1)).isFalse();
    }

    @Test
    public void testConvertByteType() {
        RowType rowType = RowType.of(new DataField(0, "f", new TinyIntType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of((byte) 123);
        GenericRow row2 = GenericRow.of((byte) 45);

        HeapByteVector byteVector = new HeapByteVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {byteVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(byteVector.getByte(0)).isEqualTo((byte) 123);
        assertThat(byteVector.getByte(1)).isEqualTo((byte) 45);
    }

    @Test
    public void testConvertShortType() {
        RowType rowType = RowType.of(new DataField(0, "f", new SmallIntType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of((short) 1234);
        GenericRow row2 = GenericRow.of((short) 5678);

        HeapShortVector shortVector = new HeapShortVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {shortVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(shortVector.getShort(0)).isEqualTo((short) 1234);
        assertThat(shortVector.getShort(1)).isEqualTo((short) 5678);
    }

    @Test
    public void testConvertLongType() {
        RowType rowType = RowType.of(new DataField(0, "f", new BigIntType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(123456789L);
        GenericRow row2 = GenericRow.of(987654321L);

        HeapLongVector longVector = new HeapLongVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {longVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(longVector.getLong(0)).isEqualTo(123456789L);
        assertThat(longVector.getLong(1)).isEqualTo(987654321L);
    }

    @Test
    public void testConvertFloatType() {
        RowType rowType = RowType.of(new DataField(0, "f", new FloatType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(123.45f);
        GenericRow row2 = GenericRow.of(678.90f);

        HeapFloatVector floatVector = new HeapFloatVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {floatVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(floatVector.getFloat(0)).isEqualTo(123.45f);
        assertThat(floatVector.getFloat(1)).isEqualTo(678.90f);
    }

    @Test
    public void testConvertDoubleType() {
        RowType rowType = RowType.of(new DataField(0, "f", new DoubleType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(123.456789D);
        GenericRow row2 = GenericRow.of(987.654321D);

        HeapDoubleVector doubleVector = new HeapDoubleVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {doubleVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(doubleVector.getDouble(0)).isEqualTo(123.456789D);
        assertThat(doubleVector.getDouble(1)).isEqualTo(987.654321D);
    }

    @Test
    public void testConvertStringType() {
        RowType rowType = RowType.of(new DataField(0, "f", new VarCharType(10)));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(BinaryString.fromString("hello"));
        GenericRow row2 = GenericRow.of(BinaryString.fromString("world"));

        HeapBytesVector bytesVector = new HeapBytesVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {bytesVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(new String(bytesVector.getBytes(0).getBytes())).isEqualTo("hello");
        assertThat(new String(bytesVector.getBytes(1).getBytes())).isEqualTo("world");
    }

    @Test
    public void testConvertBinaryType() {
        RowType rowType = RowType.of(new DataField(0, "f", new BinaryType(5)));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of((Object) "hello".getBytes());
        GenericRow row2 = GenericRow.of((Object) "world".getBytes());

        HeapBytesVector bytesVector = new HeapBytesVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {bytesVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(new String(bytesVector.getBytes(0).getBytes())).isEqualTo("hello");
        assertThat(new String(bytesVector.getBytes(1).getBytes())).isEqualTo("world");
    }

    @Test
    public void testConvertVarBinaryType() {
        RowType rowType = RowType.of(new DataField(0, "f", new VarBinaryType(10)));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of((Object) "hello".getBytes());
        GenericRow row2 = GenericRow.of((Object) "world".getBytes());

        HeapBytesVector bytesVector = new HeapBytesVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {bytesVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(new String(bytesVector.getBytes(0).getBytes())).isEqualTo("hello");
        assertThat(new String(bytesVector.getBytes(1).getBytes())).isEqualTo("world");
    }

    @Test
    public void testConvertDecimalType() {
        RowType rowType = RowType.of(new DataField(0, "f", new DecimalType(10, 2)));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        Decimal d1 = Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2);
        Decimal d2 = Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2);
        GenericRow row1 = GenericRow.of(d1);
        GenericRow row2 = GenericRow.of(d2);

        HeapLongVector longVector = new HeapLongVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {longVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(longVector.getLong(0)).isEqualTo(d1.toUnscaledLong());
        assertThat(longVector.getLong(1)).isEqualTo(d2.toUnscaledLong());
    }

    @Test
    public void testConvertTimestampType() {
        RowType rowType = RowType.of(new DataField(0, "f", new TimestampType(3)));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        Timestamp timestamp1 = Timestamp.fromEpochMillis(1234567890L);
        Timestamp timestamp2 = Timestamp.fromEpochMillis(9876543210L);

        GenericRow row1 = GenericRow.of(timestamp1);
        GenericRow row2 = GenericRow.of(timestamp2);

        HeapLongVector longVector = new HeapLongVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {longVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(longVector.getLong(0)).isEqualTo(1234567890L);
        assertThat(longVector.getLong(1)).isEqualTo(9876543210L);
    }

    @Test
    public void testConvertDateType() {
        RowType rowType = RowType.of(new DataField(0, "f", new DateType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(12345);
        GenericRow row2 = GenericRow.of(67890);

        HeapIntVector intVector = new HeapIntVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {intVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(intVector.getInt(0)).isEqualTo(12345);
        assertThat(intVector.getInt(1)).isEqualTo(67890);
    }

    @Test
    public void testConvertTimeType() {
        RowType rowType = RowType.of(new DataField(0, "f", new TimeType()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(12345);
        GenericRow row2 = GenericRow.of(67890);

        HeapIntVector intVector = new HeapIntVector(2);
        WritableColumnVector[] vectors = new WritableColumnVector[] {intVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(intVector.getInt(0)).isEqualTo(12345);
        assertThat(intVector.getInt(1)).isEqualTo(67890);
    }

    @Test
    public void testConvertArrayType() {
        RowType rowType = RowType.of(new DataField(0, "f", new ArrayType(new IntType())));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of(new GenericArray(new Object[] {1, 2, 3}));
        GenericRow row2 = GenericRow.of(new GenericArray(new Object[] {4, 5, 6}));

        HeapIntVector elementVector = new HeapIntVector(6);
        HeapArrayVector arrayVector = new HeapArrayVector(2, elementVector);
        arrayVector.putOffsetLength(0, 0, 3);
        arrayVector.putOffsetLength(1, 3, 3);

        WritableColumnVector[] vectors = new WritableColumnVector[] {arrayVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(elementVector.getInt(0)).isEqualTo(1);
        assertThat(elementVector.getInt(1)).isEqualTo(2);
        assertThat(elementVector.getInt(2)).isEqualTo(3);
        assertThat(elementVector.getInt(3)).isEqualTo(4);
        assertThat(elementVector.getInt(4)).isEqualTo(5);
        assertThat(elementVector.getInt(5)).isEqualTo(6);

        // test reserve
        GenericRow row3 = GenericRow.of(new GenericArray(new Object[] {7, 8}));
        converter.convert(row3, vectors);
        assertThat(elementVector.getInt(6)).isEqualTo(7);
        assertThat(elementVector.getInt(7)).isEqualTo(8);
    }

    @Test
    public void testConvertMapType() {
        RowType rowType =
                RowType.of(
                        new DataField(0, "f", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        Map<BinaryString, Integer> map1 = new LinkedHashMap<>();
        map1.put(BinaryString.fromString("a"), 1);
        map1.put(BinaryString.fromString("b"), 2);
        GenericRow row1 = GenericRow.of(new GenericMap(map1));

        Map<BinaryString, Integer> map2 = new LinkedHashMap<>();
        map2.put(BinaryString.fromString("c"), 3);
        map2.put(BinaryString.fromString("d"), 4);
        GenericRow row2 = GenericRow.of(new GenericMap(map2));

        HeapBytesVector keyVector = new HeapBytesVector(4);
        HeapIntVector valueVector = new HeapIntVector(4);
        HeapMapVector mapVector = new HeapMapVector(2, keyVector, valueVector);
        mapVector.putOffsetLength(0, 0, 2);
        mapVector.putOffsetLength(1, 2, 2);

        WritableColumnVector[] vectors = new WritableColumnVector[] {mapVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(new String(keyVector.getBytes(0).getBytes())).isEqualTo("a");
        assertThat(valueVector.getInt(0)).isEqualTo(1);
        assertThat(new String(keyVector.getBytes(1).getBytes())).isEqualTo("b");
        assertThat(valueVector.getInt(1)).isEqualTo(2);
        assertThat(new String(keyVector.getBytes(2).getBytes())).isEqualTo("c");
        assertThat(valueVector.getInt(2)).isEqualTo(3);
        assertThat(new String(keyVector.getBytes(3).getBytes())).isEqualTo("d");
        assertThat(valueVector.getInt(3)).isEqualTo(4);

        // test reserve
        Map<BinaryString, Integer> map3 = new LinkedHashMap<>();
        map3.put(BinaryString.fromString("e"), 5);
        GenericRow row3 = GenericRow.of(new GenericMap(map3));
        converter.convert(row3, vectors);
        assertThat(new String(keyVector.getBytes(4).getBytes())).isEqualTo("e");
        assertThat(valueVector.getInt(4)).isEqualTo(5);
    }

    @Test
    public void testConvertRowType() {
        RowType rowType =
                RowType.of(
                        new DataField(
                                0,
                                "f",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                                        DataTypes.FIELD(1, "name", DataTypes.STRING()))));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow rowValue1 = GenericRow.of(1, BinaryString.fromString("Alice"));
        GenericRow row1 = GenericRow.of(rowValue1);

        GenericRow rowValue2 = GenericRow.of(2, BinaryString.fromString("Bob"));
        GenericRow row2 = GenericRow.of(rowValue2);

        HeapIntVector idVector = new HeapIntVector(2);
        HeapBytesVector nameVector = new HeapBytesVector(2);
        HeapRowVector rowVector = new HeapRowVector(2, idVector, nameVector);

        WritableColumnVector[] vectors = new WritableColumnVector[] {rowVector};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(idVector.getInt(0)).isEqualTo(1);
        assertThat(new String(nameVector.getBytes(0).getBytes())).isEqualTo("Alice");
        assertThat(idVector.getInt(1)).isEqualTo(2);
        assertThat(new String(nameVector.getBytes(1).getBytes())).isEqualTo("Bob");

        // test reserve
        GenericRow row3 = GenericRow.of(GenericRow.of(3, BinaryString.fromString("Charlie")));
        converter.convert(row3, vectors);
        assertThat(idVector.getInt(2)).isEqualTo(3);
        assertThat(new String(nameVector.getBytes(2).getBytes())).isEqualTo("Charlie");
        assertThat(rowVector.getRow(2).getInt(0)).isEqualTo(3);
    }
}
