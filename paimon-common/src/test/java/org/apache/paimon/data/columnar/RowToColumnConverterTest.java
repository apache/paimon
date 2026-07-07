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
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
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
import org.apache.paimon.data.columnar.heap.HeapTimestampVector;
import org.apache.paimon.data.columnar.heap.HeapVectorColumnVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
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

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testConvertCharType() {
        RowType rowType = RowType.of(new DataField(0, "f", DataTypes.CHAR(10)));
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
    public void testElementConverterAppendFromObjectAndColumnVector() {
        RowToColumnConverter.ElementConverter converter =
                RowToColumnConverter.createElementConverter(DataTypes.STRING());
        HeapBytesVector sourceVector = new HeapBytesVector(1);
        sourceVector.appendByteArray("from-vector".getBytes(), 0, "from-vector".length());

        HeapBytesVector targetVector = new HeapBytesVector(2);
        converter.append(BinaryString.fromString("from-object"), targetVector);
        converter.append(sourceVector, 0, targetVector);

        assertThat(new String(targetVector.getBytes(0).getBytes())).isEqualTo("from-object");
        assertThat(new String(targetVector.getBytes(1).getBytes())).isEqualTo("from-vector");
    }

    @Test
    public void testElementConverterSupportsAllSupportedTypes() {
        for (ElementCase elementCase : elementCases()) {
            elementCase.verify();
        }
    }

    @Test
    public void testElementConverterRejectsUnsupportedTypes() {
        assertThatThrownBy(() -> RowToColumnConverter.createElementConverter(DataTypes.BLOB()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                RowToColumnConverter.createElementConverter(
                                        DataTypes.MULTISET(DataTypes.INT())))
                .isInstanceOf(UnsupportedOperationException.class);
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
    public void testConvertVariantType() {
        RowType rowType = RowType.of(new DataField(0, "f", DataTypes.VARIANT()));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1}");

        HeapRowVector rowVector =
                new HeapRowVector(1, new HeapBytesVector(1), new HeapBytesVector(1));
        WritableColumnVector[] vectors = new WritableColumnVector[] {rowVector};

        converter.convert(GenericRow.of(variant), vectors);

        assertThat(rowVector.isNullAt(0)).isFalse();
        assertThat(rowVector.getRow(0).getBinary(0)).isEqualTo(variant.value());
        assertThat(rowVector.getRow(0).getBinary(1)).isEqualTo(variant.metadata());
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
    public void testConvertVectorType() {
        RowType rowType = RowType.of(new DataField(0, "f", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 =
                GenericRow.of(BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f}));
        GenericRow row2 =
                GenericRow.of(BinaryVector.fromPrimitiveArray(new float[] {4.0f, 5.0f, 6.0f}));

        HeapFloatVector elementVector = new HeapFloatVector(6);
        HeapVectorColumnVector vectorColumn = new HeapVectorColumnVector(2, elementVector, 3);
        WritableColumnVector[] vectors = new WritableColumnVector[] {vectorColumn};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(elementVector.getFloat(0)).isEqualTo(1.0f);
        assertThat(elementVector.getFloat(1)).isEqualTo(2.0f);
        assertThat(elementVector.getFloat(2)).isEqualTo(3.0f);
        assertThat(elementVector.getFloat(3)).isEqualTo(4.0f);
        assertThat(elementVector.getFloat(4)).isEqualTo(5.0f);
        assertThat(elementVector.getFloat(5)).isEqualTo(6.0f);
        assertThat(vectorColumn.getVector(0).toFloatArray()).containsExactly(1.0f, 2.0f, 3.0f);
        assertThat(vectorColumn.getVector(1).toFloatArray()).containsExactly(4.0f, 5.0f, 6.0f);
    }

    @Test
    public void testConvertNullableVectorType() {
        RowType rowType = RowType.of(new DataField(0, "f", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row1 = GenericRow.of((Object) null);
        GenericRow row2 =
                GenericRow.of(BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f}));

        HeapFloatVector elementVector = new HeapFloatVector(3);
        HeapVectorColumnVector vectorColumn = new HeapVectorColumnVector(2, elementVector, 3);
        WritableColumnVector[] vectors = new WritableColumnVector[] {vectorColumn};

        converter.convert(row1, vectors);
        converter.convert(row2, vectors);

        assertThat(vectorColumn.isNullAt(0)).isTrue();
        assertThat(elementVector.isNullAt(0)).isTrue();
        assertThat(elementVector.isNullAt(1)).isTrue();
        assertThat(elementVector.isNullAt(2)).isTrue();
        assertThat(elementVector.getFloat(3)).isEqualTo(1.0f);
        assertThat(elementVector.getFloat(4)).isEqualTo(2.0f);
        assertThat(elementVector.getFloat(5)).isEqualTo(3.0f);
        assertThat(vectorColumn.getVector(1).toFloatArray()).containsExactly(1.0f, 2.0f, 3.0f);
    }

    @Test
    public void testConvertVectorTypeWithInvalidLength() {
        RowType rowType = RowType.of(new DataField(0, "f", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row = GenericRow.of(BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f}));
        HeapVectorColumnVector vectorColumn =
                new HeapVectorColumnVector(1, new HeapFloatVector(2), 3);

        assertThatThrownBy(() -> converter.convert(row, new WritableColumnVector[] {vectorColumn}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Vector length mismatch");
    }

    @Test
    public void testConvertVectorTypeWithNullElement() {
        RowType rowType = RowType.of(new DataField(0, "f", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        GenericRow row = GenericRow.of(createFloatVectorWithNullElement());
        HeapVectorColumnVector vectorColumn =
                new HeapVectorColumnVector(1, new HeapFloatVector(3), 3);

        assertThatThrownBy(() -> converter.convert(row, new WritableColumnVector[] {vectorColumn}))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Vector elements must not be null");
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

    @Test
    public void testConvertNullableRowType() {
        RowType rowType =
                RowType.of(
                        new DataField(
                                0,
                                "f",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                                        DataTypes.FIELD(1, "name", DataTypes.STRING()))));
        RowToColumnConverter converter = new RowToColumnConverter(rowType);

        // Test null row value
        GenericRow row1 = GenericRow.of((Object) null);

        // Test row with null fields
        GenericRow rowValue2 = GenericRow.of(null, BinaryString.fromString("Bob"));
        GenericRow row2 = GenericRow.of(rowValue2);

        // Test row with all null fields
        GenericRow rowValue3 = GenericRow.of(null, null);
        GenericRow row3 = GenericRow.of(rowValue3);

        HeapIntVector idVector = new HeapIntVector(3);
        HeapBytesVector nameVector = new HeapBytesVector(3);
        HeapRowVector rowVector = new HeapRowVector(3, idVector, nameVector);

        WritableColumnVector[] vectors = new WritableColumnVector[] {rowVector};

        // Convert null row
        converter.convert(row1, vectors);
        assertThat(rowVector.isNullAt(0)).isTrue();
        assertThat(idVector.isNullAt(0)).isTrue();
        assertThat(nameVector.isNullAt(0)).isTrue();

        // Convert row with null id field
        converter.convert(row2, vectors);
        assertThat(rowVector.isNullAt(1)).isFalse();
        assertThat(idVector.isNullAt(1)).isTrue();
        assertThat(nameVector.isNullAt(1)).isFalse();
        assertThat(new String(nameVector.getBytes(1).getBytes())).isEqualTo("Bob");

        // Convert row with all null fields
        converter.convert(row3, vectors);
        assertThat(rowVector.isNullAt(2)).isFalse();
        assertThat(idVector.isNullAt(2)).isTrue();
        assertThat(nameVector.isNullAt(2)).isTrue();
    }

    private static InternalVector createFloatVectorWithNullElement() {
        BinaryVector vector = BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f});
        Object proxy =
                Proxy.newProxyInstance(
                        RowToColumnConverterTest.class.getClassLoader(),
                        new Class[] {InternalVector.class},
                        (obj, method, args) -> {
                            if ("isNullAt".equals(method.getName()) && ((Integer) args[0]) == 1) {
                                return true;
                            }
                            return method.invoke(vector, args);
                        });
        return (InternalVector) proxy;
    }

    private static List<ElementCase> elementCases() {
        Decimal smallDecimal = Decimal.fromBigDecimal(new BigDecimal("12.34"), 5, 2);
        Decimal longDecimal = Decimal.fromBigDecimal(new BigDecimal("1234567890.12"), 12, 2);
        Decimal bytesDecimal =
                Decimal.fromBigDecimal(new BigDecimal("12345678901234567890.12"), 22, 2);
        Timestamp millisTimestamp = Timestamp.fromEpochMillis(1234567890L);
        Timestamp microsTimestamp = Timestamp.fromMicros(1234567890123456L);
        Timestamp nanosTimestamp = Timestamp.fromEpochMillis(1234567890L, 123456);
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1}");

        Map<BinaryString, Integer> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString("k1"), 1);
        map.put(BinaryString.fromString("k2"), 2);

        return Arrays.asList(
                new ElementCase(
                        DataTypes.BOOLEAN(),
                        true,
                        () -> new HeapBooleanVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapBooleanVector) vector).getBoolean(rowId))
                                        .isTrue()),
                new ElementCase(
                        DataTypes.TINYINT(),
                        (byte) 12,
                        () -> new HeapByteVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapByteVector) vector).getByte(rowId))
                                        .isEqualTo((byte) 12)),
                new ElementCase(
                        DataTypes.SMALLINT(),
                        (short) 123,
                        () -> new HeapShortVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapShortVector) vector).getShort(rowId))
                                        .isEqualTo((short) 123)),
                new ElementCase(
                        DataTypes.INT(),
                        123,
                        () -> new HeapIntVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapIntVector) vector).getInt(rowId)).isEqualTo(123)),
                new ElementCase(
                        DataTypes.BIGINT(),
                        123L,
                        () -> new HeapLongVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapLongVector) vector).getLong(rowId))
                                        .isEqualTo(123L)),
                new ElementCase(
                        DataTypes.FLOAT(),
                        1.25F,
                        () -> new HeapFloatVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapFloatVector) vector).getFloat(rowId))
                                        .isEqualTo(1.25F)),
                new ElementCase(
                        DataTypes.DOUBLE(),
                        2.5D,
                        () -> new HeapDoubleVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapDoubleVector) vector).getDouble(rowId))
                                        .isEqualTo(2.5D)),
                new ElementCase(
                        DataTypes.DATE(),
                        12345,
                        () -> new HeapIntVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapIntVector) vector).getInt(rowId))
                                        .isEqualTo(12345)),
                new ElementCase(
                        DataTypes.TIME(),
                        67890,
                        () -> new HeapIntVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapIntVector) vector).getInt(rowId))
                                        .isEqualTo(67890)),
                new ElementCase(
                        DataTypes.CHAR(10),
                        BinaryString.fromString("char"),
                        () -> new HeapBytesVector(3),
                        (vector, rowId) ->
                                assertThat(bytes((HeapBytesVector) vector, rowId))
                                        .isEqualTo("char")),
                new ElementCase(
                        DataTypes.VARCHAR(10),
                        BinaryString.fromString("varchar"),
                        () -> new HeapBytesVector(3),
                        (vector, rowId) ->
                                assertThat(bytes((HeapBytesVector) vector, rowId))
                                        .isEqualTo("varchar")),
                new ElementCase(
                        DataTypes.BINARY(6),
                        "binary".getBytes(),
                        () -> new HeapBytesVector(3),
                        (vector, rowId) ->
                                assertThat(bytes((HeapBytesVector) vector, rowId))
                                        .isEqualTo("binary")),
                new ElementCase(
                        DataTypes.VARBINARY(9),
                        "varbinary".getBytes(),
                        () -> new HeapBytesVector(3),
                        (vector, rowId) ->
                                assertThat(bytes((HeapBytesVector) vector, rowId))
                                        .isEqualTo("varbinary")),
                new ElementCase(
                        DataTypes.DECIMAL(5, 2),
                        smallDecimal,
                        () -> new HeapIntVector(3),
                        decimalSource(smallDecimal),
                        (vector, rowId) ->
                                assertThat(((HeapIntVector) vector).getInt(rowId))
                                        .isEqualTo((int) smallDecimal.toUnscaledLong())),
                new ElementCase(
                        DataTypes.DECIMAL(12, 2),
                        longDecimal,
                        () -> new HeapLongVector(3),
                        decimalSource(longDecimal),
                        (vector, rowId) ->
                                assertThat(((HeapLongVector) vector).getLong(rowId))
                                        .isEqualTo(longDecimal.toUnscaledLong())),
                new ElementCase(
                        DataTypes.DECIMAL(22, 2),
                        bytesDecimal,
                        () -> new HeapBytesVector(3),
                        decimalSource(bytesDecimal),
                        (vector, rowId) ->
                                assertThat(((HeapBytesVector) vector).getBytes(rowId).getBytes())
                                        .isEqualTo(bytesDecimal.toUnscaledBytes())),
                new ElementCase(
                        DataTypes.TIMESTAMP(3),
                        millisTimestamp,
                        () -> new HeapLongVector(3),
                        timestampSource(millisTimestamp),
                        (vector, rowId) ->
                                assertThat(((HeapLongVector) vector).getLong(rowId))
                                        .isEqualTo(millisTimestamp.getMillisecond())),
                new ElementCase(
                        DataTypes.TIMESTAMP(6),
                        microsTimestamp,
                        () -> new HeapLongVector(3),
                        timestampSource(microsTimestamp),
                        (vector, rowId) ->
                                assertThat(((HeapLongVector) vector).getLong(rowId))
                                        .isEqualTo(microsTimestamp.toMicros())),
                new ElementCase(
                        DataTypes.TIMESTAMP(9),
                        nanosTimestamp,
                        () -> new HeapTimestampVector(3),
                        (vector, rowId) ->
                                assertThat(((HeapTimestampVector) vector).getTimestamp(rowId, 9))
                                        .isEqualTo(nanosTimestamp)),
                new ElementCase(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                        millisTimestamp,
                        () -> new HeapLongVector(3),
                        timestampSource(millisTimestamp),
                        (vector, rowId) ->
                                assertThat(((HeapLongVector) vector).getLong(rowId))
                                        .isEqualTo(millisTimestamp.getMillisecond())),
                new ElementCase(
                        DataTypes.VARIANT(),
                        variant,
                        () -> new HeapRowVector(3, new HeapBytesVector(3), new HeapBytesVector(3)),
                        (vector, rowId) -> {
                            InternalRow row = ((HeapRowVector) vector).getRow(rowId);
                            assertThat(row.getBinary(0)).isEqualTo(variant.value());
                            assertThat(row.getBinary(1)).isEqualTo(variant.metadata());
                        }),
                new ElementCase(
                        DataTypes.ARRAY(DataTypes.INT()),
                        new GenericArray(new Object[] {1, 2, 3}),
                        () -> new HeapArrayVector(3, new HeapIntVector(9)),
                        (vector, rowId) -> {
                            InternalArray array = ((HeapArrayVector) vector).getArray(rowId);
                            assertThat(array.size()).isEqualTo(3);
                            assertThat(array.getInt(0)).isEqualTo(1);
                            assertThat(array.getInt(1)).isEqualTo(2);
                            assertThat(array.getInt(2)).isEqualTo(3);
                        }),
                new ElementCase(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        new GenericMap(map),
                        () -> new HeapMapVector(3, new HeapBytesVector(6), new HeapIntVector(6)),
                        (vector, rowId) -> {
                            InternalMap internalMap = ((HeapMapVector) vector).getMap(rowId);
                            assertThat(internalMap.size()).isEqualTo(2);
                            assertThat(internalMap.keyArray().getString(0).toString())
                                    .isEqualTo("k1");
                            assertThat(internalMap.valueArray().getInt(0)).isEqualTo(1);
                            assertThat(internalMap.keyArray().getString(1).toString())
                                    .isEqualTo("k2");
                            assertThat(internalMap.valueArray().getInt(1)).isEqualTo(2);
                        }),
                new ElementCase(
                        DataTypes.ROW(
                                DataTypes.FIELD(0, "id", DataTypes.INT()),
                                DataTypes.FIELD(1, "name", DataTypes.STRING())),
                        GenericRow.of(1, BinaryString.fromString("Alice")),
                        () -> new HeapRowVector(3, new HeapIntVector(3), new HeapBytesVector(3)),
                        (vector, rowId) -> {
                            InternalRow row = ((HeapRowVector) vector).getRow(rowId);
                            assertThat(row.getInt(0)).isEqualTo(1);
                            assertThat(row.getString(1).toString()).isEqualTo("Alice");
                        }),
                new ElementCase(
                        DataTypes.VECTOR(3, DataTypes.FLOAT()),
                        BinaryVector.fromPrimitiveArray(new float[] {1.0F, 2.0F, 3.0F}),
                        () -> new HeapVectorColumnVector(3, new HeapFloatVector(9), 3),
                        (vector, rowId) ->
                                assertThat(
                                                ((HeapVectorColumnVector) vector)
                                                        .getVector(rowId)
                                                        .toFloatArray())
                                        .containsExactly(1.0F, 2.0F, 3.0F)));
    }

    private static String bytes(HeapBytesVector vector, int rowId) {
        return new String(vector.getBytes(rowId).getBytes());
    }

    private static Supplier<ColumnVector> decimalSource(Decimal decimal) {
        return () ->
                new DecimalColumnVector() {
                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }

                    @Override
                    public Decimal getDecimal(int i, int precision, int scale) {
                        return decimal;
                    }
                };
    }

    private static Supplier<ColumnVector> timestampSource(Timestamp timestamp) {
        return () ->
                new TimestampColumnVector() {
                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }

                    @Override
                    public Timestamp getTimestamp(int i, int precision) {
                        return timestamp;
                    }
                };
    }

    private static class ElementCase {

        private final DataType type;
        private final Object value;
        private final Supplier<WritableColumnVector> vectorSupplier;
        private final Supplier<ColumnVector> sourceSupplier;
        private final BiConsumer<ColumnVector, Integer> assertion;

        private ElementCase(
                DataType type,
                Object value,
                Supplier<WritableColumnVector> vectorSupplier,
                BiConsumer<ColumnVector, Integer> assertion) {
            this(
                    type,
                    value,
                    vectorSupplier,
                    () -> createSourceVector(type, value, vectorSupplier),
                    assertion);
        }

        private ElementCase(
                DataType type,
                Object value,
                Supplier<WritableColumnVector> vectorSupplier,
                Supplier<ColumnVector> sourceSupplier,
                BiConsumer<ColumnVector, Integer> assertion) {
            this.type = type;
            this.value = value;
            this.vectorSupplier = vectorSupplier;
            this.sourceSupplier = sourceSupplier;
            this.assertion = assertion;
        }

        private void verify() {
            RowToColumnConverter.ElementConverter converter =
                    RowToColumnConverter.createElementConverter(type);

            WritableColumnVector objectTarget = vectorSupplier.get();
            converter.append(value, objectTarget);
            assertion.accept(objectTarget, 0);

            WritableColumnVector vectorTarget = vectorSupplier.get();
            converter.append(sourceSupplier.get(), 0, vectorTarget);
            assertion.accept(vectorTarget, 0);

            WritableColumnVector nullTarget = vectorSupplier.get();
            converter.append(null, nullTarget);
            assertThat(nullTarget.isNullAt(0)).isTrue();
        }

        private static ColumnVector createSourceVector(
                DataType type, Object value, Supplier<WritableColumnVector> vectorSupplier) {
            WritableColumnVector source = vectorSupplier.get();
            RowToColumnConverter.createElementConverter(type).append(value, source);
            return source;
        }
    }
}
