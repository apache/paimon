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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NestedProjectedRow}. */
public class NestedProjectedRowTest {

    @Test
    void testReturnNullWhenSchemasAreEqual() {
        RowType schema = RowType.of(DataTypes.INT(), DataTypes.STRING());
        assertThat(NestedProjectedRow.create(schema, schema)).isNull();
    }

    @Test
    void testTopLevelProjection() {
        // data: ROW<a INT, b STRING, c BIGINT>
        RowType dataSchema =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.BIGINT())
                        .build();

        // projected: ROW<c BIGINT, a INT>
        RowType projectedSchema =
                RowType.builder()
                        .field("c", DataTypes.BIGINT())
                        .field("a", DataTypes.INT())
                        .build();

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
        // data: ROW<a INT, b STRING, c DOUBLE>
        RowType dataSchema =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.DOUBLE())
                        .build();

        // projected: ROW<b STRING>
        RowType projectedSchema = RowType.builder().field("b", DataTypes.STRING()).build();

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow row = GenericRow.of(1, BinaryString.fromString("world"), 3.14);
        InternalRow projected = projection.replaceRow(row);

        assertThat(projected.getFieldCount()).isEqualTo(1);
        assertThat(projected.getString(0)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    void testNestedRowProjection() {
        // data: ROW<id INT, r ROW<x INT, y INT, z INT>>
        RowType nestedType =
                RowType.builder()
                        .field("x", DataTypes.INT())
                        .field("y", DataTypes.INT())
                        .field("z", DataTypes.INT())
                        .build();
        RowType dataSchema =
                RowType.builder().field("id", DataTypes.INT()).field("r", nestedType).build();

        // projected: ROW<r ROW<z INT>>
        RowType projectedNestedType = RowType.builder().field("z", DataTypes.INT()).build();
        RowType projectedSchema = RowType.builder().field("r", projectedNestedType).build();

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
        // data: ROW<id INT, r ROW<a INT, b INT, c INT>>
        RowType nestedType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.INT())
                        .field("c", DataTypes.INT())
                        .build();
        RowType dataSchema =
                RowType.builder().field("id", DataTypes.INT()).field("r", nestedType).build();

        // projected: ROW<id INT, r ROW<c INT, a INT>>
        RowType projectedNestedType =
                RowType.builder().field("c", DataTypes.INT()).field("a", DataTypes.INT()).build();
        RowType projectedSchema =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("r", projectedNestedType)
                        .build();

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
        // data: ROW<a ROW<b ROW<x INT, y INT, z INT>>>
        RowType level2 =
                RowType.builder()
                        .field("x", DataTypes.INT())
                        .field("y", DataTypes.INT())
                        .field("z", DataTypes.INT())
                        .build();
        RowType level1 = RowType.builder().field("b", level2).build();
        RowType dataSchema = RowType.builder().field("a", level1).build();

        // projected: ROW<a ROW<b ROW<y INT>>>
        RowType projLevel2 = RowType.builder().field("y", DataTypes.INT()).build();
        RowType projLevel1 = RowType.builder().field("b", projLevel2).build();
        RowType projectedSchema = RowType.builder().field("a", projLevel1).build();

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
    void testNestedRowWithoutProjection() {
        // data: ROW<id INT, r ROW<x INT, y INT>>
        RowType nestedType =
                RowType.builder().field("x", DataTypes.INT()).field("y", DataTypes.INT()).build();
        RowType dataSchema =
                RowType.builder().field("id", DataTypes.INT()).field("r", nestedType).build();

        // projected: ROW<r ROW<x INT, y INT>> (nested is unchanged, only top-level pruned)
        RowType projectedSchema = RowType.builder().field("r", nestedType).build();

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
        // data: ROW<a INT, r ROW<x INT, y INT>>
        RowType nestedType =
                RowType.builder().field("x", DataTypes.INT()).field("y", DataTypes.INT()).build();
        RowType dataSchema =
                RowType.builder().field("a", DataTypes.INT()).field("r", nestedType).build();

        // projected: ROW<r ROW<y INT>>
        RowType projectedNestedType = RowType.builder().field("y", DataTypes.INT()).build();
        RowType projectedSchema = RowType.builder().field("r", projectedNestedType).build();

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
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        RowType projectedSchema = RowType.builder().field("b", DataTypes.INT()).build();

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
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        RowType projectedSchema = RowType.builder().field("b", DataTypes.INT()).build();

        NestedProjectedRow projection = NestedProjectedRow.create(dataSchema, projectedSchema);
        assertThat(projection).isNotNull();

        GenericRow row = GenericRow.of(1, 10);
        row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
        InternalRow projected = projection.replaceRow(row);
        assertThat(projected.getRowKind()).isEqualTo(org.apache.paimon.types.RowKind.DELETE);
    }

    @Test
    void testMultipleNestedRows() {
        // data: ROW<r1 ROW<a INT, b INT>, r2 ROW<x STRING, y STRING>>
        RowType nested1 =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        RowType nested2 =
                RowType.builder()
                        .field("x", DataTypes.STRING())
                        .field("y", DataTypes.STRING())
                        .build();
        RowType dataSchema = RowType.builder().field("r1", nested1).field("r2", nested2).build();

        // projected: ROW<r1 ROW<b INT>, r2 ROW<y STRING>>
        RowType projNested1 = RowType.builder().field("b", DataTypes.INT()).build();
        RowType projNested2 = RowType.builder().field("y", DataTypes.STRING()).build();
        RowType projectedSchema =
                RowType.builder().field("r1", projNested1).field("r2", projNested2).build();

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
        RowType dataSchema =
                RowType.builder()
                        .field("f_bool", DataTypes.BOOLEAN())
                        .field("f_byte", DataTypes.TINYINT())
                        .field("f_short", DataTypes.SMALLINT())
                        .field("f_int", DataTypes.INT())
                        .field("f_long", DataTypes.BIGINT())
                        .field("f_float", DataTypes.FLOAT())
                        .field("f_double", DataTypes.DOUBLE())
                        .field("f_string", DataTypes.STRING())
                        .field("f_binary", DataTypes.BYTES())
                        .build();

        // project a subset in different order
        RowType projectedSchema =
                RowType.builder()
                        .field("f_double", DataTypes.DOUBLE())
                        .field("f_bool", DataTypes.BOOLEAN())
                        .field("f_string", DataTypes.STRING())
                        .field("f_byte", DataTypes.TINYINT())
                        .build();

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
}
