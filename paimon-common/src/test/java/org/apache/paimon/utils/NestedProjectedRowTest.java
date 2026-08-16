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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link NestedProjectedRow}. */
public class NestedProjectedRowTest {

    @Test
    void testReturnNullWhenSchemasAreEqual() {
        RowType schema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new VarCharType())));
        assertThat(NestedProjectedRow.create(schema, schema)).isNull();
    }

    @Test
    void testTopLevelProjection() {
        // data: ROW<a INT(id=0), b STRING(id=1), c BIGINT(id=2)>
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new VarCharType()),
                                new DataField(2, "c", new BigIntType())));

        // projected: ROW<c BIGINT(id=2), a INT(id=0)>
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(2, "c", new BigIntType()),
                                new DataField(0, "a", new IntType())));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow row = GenericRow.of(42, BinaryString.fromString("hello"), 100L);
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getFieldCount()).isEqualTo(2);
        assertThat(projected.getLong(0)).isEqualTo(100L);
        assertThat(projected.getInt(1)).isEqualTo(42);
    }

    @Test
    void testTopLevelFieldSubset() {
        // data: ROW<a INT(id=0), b STRING(id=1), c DOUBLE(id=2)>
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new VarCharType()),
                                new DataField(2, "c", new DoubleType())));

        // projected: ROW<b STRING(id=1)>
        RowType projectedSchema =
                new RowType(Arrays.asList(new DataField(1, "b", new VarCharType())));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow row = GenericRow.of(1, BinaryString.fromString("world"), 3.14);
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getFieldCount()).isEqualTo(1);
        assertThat(projected.getString(0)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    void testNestedRowProjection() {
        // data: ROW<id INT(0), r ROW<x INT(10), y INT(11), z INT(12)>(1)>
        RowType nestedType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "x", new IntType()),
                                new DataField(11, "y", new IntType()),
                                new DataField(12, "z", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "r", nestedType)));

        // projected: ROW<r ROW<z INT(12)>(1)>
        RowType projectedNestedType =
                new RowType(Arrays.asList(new DataField(12, "z", new IntType())));
        RowType projectedSchema =
                new RowType(Arrays.asList(new DataField(1, "r", projectedNestedType)));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow innerRow = GenericRow.of(10, 20, 30);
        GenericRow row = GenericRow.of(1, innerRow);
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getFieldCount()).isEqualTo(1);
        InternalRow projectedInner = projected.getRow(0, 1);
        assertThat(projectedInner.getFieldCount()).isEqualTo(1);
        assertThat(projectedInner.getInt(0)).isEqualTo(30);
    }

    @Test
    void testNestedRowProjectionMultipleFields() {
        // data: ROW<id INT(0), r ROW<a INT(10), b INT(11), c INT(12)>(1)>
        RowType nestedType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType()),
                                new DataField(12, "c", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "r", nestedType)));

        // projected: ROW<id INT(0), r ROW<c INT(12), a INT(10)>(1)>
        RowType projectedNestedType =
                new RowType(
                        Arrays.asList(
                                new DataField(12, "c", new IntType()),
                                new DataField(10, "a", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "r", projectedNestedType)));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow innerRow = GenericRow.of(10, 20, 30);
        GenericRow row = GenericRow.of(1, innerRow);
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getFieldCount()).isEqualTo(2);
        assertThat(projected.getInt(0)).isEqualTo(1);
        InternalRow projectedInner = projected.getRow(1, 2);
        assertThat(projectedInner.getInt(0)).isEqualTo(30);
        assertThat(projectedInner.getInt(1)).isEqualTo(10);
    }

    @Test
    void testDeeplyNestedProjection() {
        // data: ROW<a(0) ROW<b(5) ROW<x INT(10), y INT(11), z INT(12)>>>
        RowType level2 =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "x", new IntType()),
                                new DataField(11, "y", new IntType()),
                                new DataField(12, "z", new IntType())));
        RowType level1 = new RowType(Arrays.asList(new DataField(5, "b", level2)));
        RowType dataSchema = new RowType(Arrays.asList(new DataField(0, "a", level1)));

        // projected: ROW<a(0) ROW<b(5) ROW<y INT(11)>>>
        RowType projLevel2 = new RowType(Arrays.asList(new DataField(11, "y", new IntType())));
        RowType projLevel1 = new RowType(Arrays.asList(new DataField(5, "b", projLevel2)));
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(0, "a", projLevel1)));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow l2Row = GenericRow.of(10, 20, 30);
        GenericRow l1Row = GenericRow.of(l2Row);
        GenericRow row = GenericRow.of(l1Row);
        InternalRow projected = projection.replaceRow(row);

        InternalRow projL1 = projected.getRow(0, 1);
        InternalRow projL2 = projL1.getRow(0, 1);
        assertThat(projL2.getInt(0)).isEqualTo(20);
    }

    @Test
    void testNestedRowWithoutInnerProjection() {
        // data: ROW<id INT(0), r ROW<x INT(10), y INT(11)>(1)>
        RowType nestedType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "x", new IntType()),
                                new DataField(11, "y", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "r", nestedType)));

        // projected: ROW<r ROW<x INT(10), y INT(11)>(1)> (nested is unchanged, only top-level
        // pruned)
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(1, "r", nestedType)));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow innerRow = GenericRow.of(10, 20);
        GenericRow row = GenericRow.of(1, innerRow);
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getFieldCount()).isEqualTo(1);
        InternalRow projectedInner = projected.getRow(0, 2);
        assertThat(projectedInner.getInt(0)).isEqualTo(10);
        assertThat(projectedInner.getInt(1)).isEqualTo(20);
    }

    @Test
    void testNullHandling() {
        // data: ROW<a INT(0), r ROW<x INT(10), y INT(11)>(1)>
        RowType nestedType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "x", new IntType()),
                                new DataField(11, "y", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "r", nestedType)));

        // projected: ROW<r ROW<y INT(11)>(1)>
        RowType projectedNestedType =
                new RowType(Arrays.asList(new DataField(11, "y", new IntType())));
        RowType projectedSchema =
                new RowType(Arrays.asList(new DataField(1, "r", projectedNestedType)));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        // null nested row
        GenericRow row = GenericRow.of(1, null);
        InternalRow projected = projection.replaceRow(row);
        assertThat(projected.isNullAt(0)).isTrue();

        // non-null nested row with null field
        GenericRow innerRow = GenericRow.of(10, null);
        GenericRow row2 = GenericRow.of(1, innerRow);
        InternalRow projected2 = projection.replaceRow(row2);
        assertThat(projected2.isNullAt(0)).isFalse();
        InternalRow projectedInner = projected2.getRow(0, 1);
        assertThat(projectedInner.isNullAt(0)).isTrue();
    }

    @Test
    void testReplaceRowIsReusable() {
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType())));
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(1, "b", new IntType())));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow row1 = GenericRow.of(1, 10);
        assertThat(projection.replaceRow(row1).getInt(0)).isEqualTo(10);

        GenericRow row2 = GenericRow.of(2, 20);
        assertThat(projection.replaceRow(row2).getInt(0)).isEqualTo(20);
    }

    @Test
    void testRowKindPreserved() {
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType())));
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(1, "b", new IntType())));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow row = GenericRow.of(1, 10);
        row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
        InternalRow projected = projection.replaceRow(row);
        assertThat(projected.getRowKind()).isEqualTo(org.apache.paimon.types.RowKind.DELETE);
    }

    @Test
    void testMultipleNestedRows() {
        // data: ROW<r1 ROW<a INT(10), b INT(11)>(0), r2 ROW<x STRING(20), y STRING(21)>(1)>
        RowType nested1 =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType nested2 =
                new RowType(
                        Arrays.asList(
                                new DataField(20, "x", new VarCharType()),
                                new DataField(21, "y", new VarCharType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "r1", nested1), new DataField(1, "r2", nested2)));

        // projected: ROW<r1 ROW<b INT(11)>(0), r2 ROW<y STRING(21)>(1)>
        RowType projNested1 = new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projNested2 = new RowType(Arrays.asList(new DataField(21, "y", new VarCharType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "r1", projNested1),
                                new DataField(1, "r2", projNested2)));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow inner1 = GenericRow.of(10, 20);
        GenericRow inner2 =
                GenericRow.of(BinaryString.fromString("hello"), BinaryString.fromString("world"));
        GenericRow row = GenericRow.of(inner1, inner2);
        InternalRow projected = projection.replaceRow(row);

        InternalRow projInner1 = projected.getRow(0, 1);
        assertThat(projInner1.getInt(0)).isEqualTo(20);

        InternalRow projInner2 = projected.getRow(1, 1);
        assertThat(projInner2.getString(0)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    void testAllDataTypes() {
        // data with various types, each with unique field ID
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f_bool", new BooleanType()),
                                new DataField(1, "f_byte", new TinyIntType()),
                                new DataField(2, "f_short", new SmallIntType()),
                                new DataField(3, "f_int", new IntType()),
                                new DataField(4, "f_long", new BigIntType()),
                                new DataField(5, "f_float", new FloatType()),
                                new DataField(6, "f_double", new DoubleType()),
                                new DataField(7, "f_string", new VarCharType()),
                                new DataField(8, "f_binary", new VarBinaryType())));

        // project a subset in different order
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(6, "f_double", new DoubleType()),
                                new DataField(0, "f_bool", new BooleanType()),
                                new DataField(7, "f_string", new VarCharType()),
                                new DataField(1, "f_byte", new TinyIntType())));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow row =
                GenericRow.of(
                        true,
                        (byte) 7,
                        (short) 16,
                        42,
                        100L,
                        1.5f,
                        3.14,
                        BinaryString.fromString("test"),
                        new byte[] {1, 2, 3});
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getDouble(0)).isEqualTo(3.14);
        assertThat(projected.getBoolean(1)).isTrue();
        assertThat(projected.getString(2)).isEqualTo(BinaryString.fromString("test"));
        assertThat(projected.getByte(3)).isEqualTo((byte) 7);
    }

    @Test
    void testFieldNameMismatchThrows() {
        // data: ROW<a INT(id=0), b INT(id=1)>
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType())));

        // projected: same field id=1 but wrong name "wrong"
        RowType projectedSchema =
                new RowType(Arrays.asList(new DataField(1, "wrong", new IntType())));

        assertThatThrownBy(() -> NestedProjectedRow.create(dataSchema, projectedSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field name mismatch")
                .hasMessageContaining("'b'")
                .hasMessageContaining("'wrong'");
    }

    @Test
    void testNestedFieldNameMismatchThrows() {
        // data: ROW<r ROW<x INT(10), y INT(11)>(0)>
        RowType nestedType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "x", new IntType()),
                                new DataField(11, "y", new IntType())));
        RowType dataSchema = new RowType(Arrays.asList(new DataField(0, "r", nestedType)));

        // projected: nested field id=11 but wrong name "wrong_name"
        RowType projectedNestedType =
                new RowType(Arrays.asList(new DataField(11, "wrong_name", new IntType())));
        RowType projectedSchema =
                new RowType(Arrays.asList(new DataField(0, "r", projectedNestedType)));

        assertThatThrownBy(() -> NestedProjectedRow.create(dataSchema, projectedSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field name mismatch")
                .hasMessageContaining("'y'")
                .hasMessageContaining("'wrong_name'");
    }

    @Test
    void testArrayElementProjection() {
        // data: ROW<arr ARRAY<ROW<a INT(10), b INT(11)>>(0)>
        RowType elementType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(Arrays.asList(new DataField(0, "arr", new ArrayType(elementType))));

        // projected: ROW<arr ARRAY<ROW<b INT(11)>>(0)>
        RowType projectedElementType =
                new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "arr", new ArrayType(projectedElementType))));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        // arr = [ROW<a=1, b=100>, ROW<a=2, b=200>]
        GenericArray array =
                new GenericArray(new Object[] {GenericRow.of(1, 100), GenericRow.of(2, 200)});
        GenericRow row = GenericRow.of(array);
        InternalRow projected = projection.replaceRow(row);

        InternalArray projectedArray = projected.getArray(0);
        assertThat(projectedArray.size()).isEqualTo(2);
        assertThat(projectedArray.getRow(0, 1).getInt(0)).isEqualTo(100);
        assertThat(projectedArray.getRow(1, 1).getInt(0)).isEqualTo(200);
    }

    @Test
    void testArrayElementProjectionWithNull() {
        // data: ROW<arr ARRAY<ROW<a INT(10), b INT(11)>>(0)>
        RowType elementType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(Arrays.asList(new DataField(0, "arr", new ArrayType(elementType))));

        // projected: ROW<arr ARRAY<ROW<b INT(11)>>(0)>
        RowType projectedElementType =
                new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "arr", new ArrayType(projectedElementType))));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        // arr = [ROW<a=1, b=100>, null]
        GenericArray array = new GenericArray(new Object[] {GenericRow.of(1, 100), null});
        GenericRow row = GenericRow.of(array);
        InternalRow projected = projection.replaceRow(row);

        InternalArray projectedArray = projected.getArray(0);
        assertThat(projectedArray.size()).isEqualTo(2);
        assertThat(projectedArray.getRow(0, 1).getInt(0)).isEqualTo(100);
        assertThat(projectedArray.isNullAt(1)).isTrue();
    }

    @Test
    void testMapValueProjection() {
        // data: ROW<m MAP<INT, ROW<a INT(10), b INT(11)>>(0)>
        RowType valueType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "m", new MapType(new IntType(), valueType))));

        // projected: ROW<m MAP<INT, ROW<b INT(11)>>(0)>
        RowType projectedValueType =
                new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0, "m", new MapType(new IntType(), projectedValueType))));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        // m = {1 -> ROW<a=10, b=100>, 2 -> ROW<a=20, b=200>}
        Map<Object, Object> mapData = new HashMap<>();
        mapData.put(1, GenericRow.of(10, 100));
        mapData.put(2, GenericRow.of(20, 200));
        GenericRow row = GenericRow.of(new GenericMap(mapData));
        InternalRow projected = projection.replaceRow(row);

        InternalMap projectedMap = projected.getMap(0);
        assertThat(projectedMap.size()).isEqualTo(2);
        InternalArray values = projectedMap.valueArray();
        InternalArray keys = projectedMap.keyArray();
        for (int i = 0; i < 2; i++) {
            int key = keys.getInt(i);
            int b = values.getRow(i, 1).getInt(0);
            if (key == 1) {
                assertThat(b).isEqualTo(100);
            } else {
                assertThat(b).isEqualTo(200);
            }
        }
    }

    @Test
    void testArrayWithNoProjectionNeeded() {
        // data: ROW<arr ARRAY<ROW<a INT(10), b INT(11)>>(0), id INT(1)>
        RowType elementType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "arr", new ArrayType(elementType)),
                                new DataField(1, "id", new IntType())));

        // projected: ROW<arr ARRAY<ROW<a INT(10), b INT(11)>>(0)> - full element, just drop id
        RowType projectedSchema =
                new RowType(Arrays.asList(new DataField(0, "arr", new ArrayType(elementType))));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericArray array = new GenericArray(new Object[] {GenericRow.of(1, 2)});
        GenericRow row = GenericRow.of(array, 99);
        InternalRow projected = projection.replaceRow(row);

        InternalArray projectedArray = projected.getArray(0);
        assertThat(projectedArray.size()).isEqualTo(1);
        InternalRow element = projectedArray.getRow(0, 2);
        assertThat(element.getInt(0)).isEqualTo(1);
        assertThat(element.getInt(1)).isEqualTo(2);
    }

    @Test
    void testNestedArrayProjection() {
        // data: ROW<arr ARRAY<ARRAY<ROW<a INT(10), b INT(11)>>>(0)>
        RowType elementType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0, "arr", new ArrayType(new ArrayType(elementType)))));

        // projected: ROW<arr ARRAY<ARRAY<ROW<b INT(11)>>>(0)>
        RowType projectedElementType =
                new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0,
                                        "arr",
                                        new ArrayType(new ArrayType(projectedElementType)))));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        // arr = [[ROW<a=1, b=100>, ROW<a=2, b=200>]]
        GenericArray innerArray =
                new GenericArray(new Object[] {GenericRow.of(1, 100), GenericRow.of(2, 200)});
        GenericArray outerArray = new GenericArray(new Object[] {innerArray});
        GenericRow row = GenericRow.of(outerArray);
        InternalRow projected = projection.replaceRow(row);

        InternalArray projOuter = projected.getArray(0);
        assertThat(projOuter.size()).isEqualTo(1);
        InternalArray projInner = projOuter.getArray(0);
        assertThat(projInner.size()).isEqualTo(2);
        assertThat(projInner.getRow(0, 1).getInt(0)).isEqualTo(100);
        assertThat(projInner.getRow(1, 1).getInt(0)).isEqualTo(200);
    }

    @Test
    void testMapWithArrayValueProjection() {
        // data: ROW<m MAP<INT, ARRAY<ROW<a INT(10), b INT(11)>>>(0)>
        RowType elementType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0,
                                        "m",
                                        new MapType(new IntType(), new ArrayType(elementType)))));

        // projected: ROW<m MAP<INT, ARRAY<ROW<b INT(11)>>>(0)>
        RowType projectedElementType =
                new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0,
                                        "m",
                                        new MapType(
                                                new IntType(),
                                                new ArrayType(projectedElementType)))));

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        // m = {1 -> [ROW<a=10, b=100>]}
        Map<Object, Object> mapData = new HashMap<>();
        mapData.put(1, new GenericArray(new Object[] {GenericRow.of(10, 100)}));
        GenericRow row = GenericRow.of(new GenericMap(mapData));
        InternalRow projected = projection.replaceRow(row);

        InternalMap projectedMap = projected.getMap(0);
        InternalArray values = projectedMap.valueArray();
        InternalArray valueArr = values.getArray(0);
        assertThat(valueArr.getRow(0, 1).getInt(0)).isEqualTo(100);
    }

    @Test
    void testMultisetDoesNotThrow() {
        // data: ROW<id INT(0), ms MULTISET<STRING>(1)>
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "ms", new MultisetType(new VarCharType()))));

        // projected: ROW<ms MULTISET<STRING>(1)>
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(new DataField(1, "ms", new MultisetType(new VarCharType()))));

        // Should not throw ClassCastException
        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        Map<Object, Object> msData = new HashMap<>();
        msData.put(BinaryString.fromString("hello"), 2);
        GenericRow row = GenericRow.of(42, new GenericMap(msData));
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getFieldCount()).isEqualTo(1);
        InternalMap ms = projected.getMap(0);
        assertThat(ms.size()).isEqualTo(1);
    }
}
