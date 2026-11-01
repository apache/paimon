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

package org.apache.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValue}. */
public class KeyValueTest {

    private static final String COMPLEX_ROW_STRING =
            "[1, 2, null], {1=2}, (7), [(3)], {k=[1, 2]}, {5=2}, [1, 2], null";

    @Test
    public void testRowDataToStringScalars() {
        RowType rowType =
                RowType.of(
                        new DataField(0, "i", new IntType()),
                        new DataField(1, "s", new VarCharType(VarCharType.MAX_LENGTH)),
                        new DataField(2, "n", new IntType()));
        assertThat(
                        KeyValue.rowDataToString(
                                GenericRow.of(1, BinaryString.fromString("a"), null), rowType))
                .isEqualTo("1, a, null");
    }

    @Test
    public void testRowDataToStringComplexTypes() {
        RowType rowType = complexRowType();
        GenericRow row = complexRow();

        assertThat(KeyValue.rowDataToString(row, rowType)).isEqualTo(COMPLEX_ROW_STRING);

        // binary rows format identically
        InternalRow binaryRow = new InternalRowSerializer(rowType).toBinaryRow(row).copy();
        assertThat(KeyValue.rowDataToString(binaryRow, rowType)).isEqualTo(COMPLEX_ROW_STRING);
    }

    @Test
    public void testRowDataToStringNestedColumnarRow() {
        HeapIntVector intVector = new HeapIntVector(1);
        intVector.setInt(0, 7);
        InternalRow columnarRow = new HeapRowVector(1, intVector).getRow(0);
        assertThat(columnarRow).isInstanceOf(ColumnarRow.class);

        RowType rowType =
                RowType.of(new DataField(0, "r", RowType.of(new DataField(0, "x", new IntType()))));

        // nested rows read from columnar formats are ColumnarRows, which must not be formatted
        // with Object#toString
        assertThat(KeyValue.rowDataToString(GenericRow.of(columnarRow), rowType)).isEqualTo("(7)");
    }

    @Test
    public void testKeyValueToString() {
        RowType keyType = RowType.of(new DataField(0, "k", new IntType()));
        RowType valueType = RowType.of(new DataField(0, "v", new ArrayType(true, new IntType())));
        KeyValue kv =
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                5L,
                                RowKind.INSERT,
                                GenericRow.of(new GenericArray(new Object[] {1, 2})))
                        .setLevel(2);

        assertThat(kv.toString(keyType, valueType))
                .isEqualTo("{kind: INSERT, seq: 5, key: (1), value: ([1, 2]), level: 2}");
    }

    private static RowType complexRowType() {
        RowType nestedRowType = RowType.of(new DataField(0, "z", new IntType()));
        return RowType.of(
                new DataField(0, "arr", new ArrayType(true, new IntType())),
                new DataField(1, "map", new MapType(new IntType(), new IntType())),
                new DataField(2, "row", nestedRowType),
                new DataField(3, "arrRow", new ArrayType(true, nestedRowType)),
                new DataField(
                        4,
                        "mapArr",
                        new MapType(
                                new VarCharType(VarCharType.MAX_LENGTH),
                                new ArrayType(true, new IntType()))),
                new DataField(5, "ms", new MultisetType(new IntType())),
                new DataField(6, "bin", new VarBinaryType(VarBinaryType.MAX_LENGTH)),
                new DataField(7, "nullMap", new MapType(new IntType(), new IntType())));
    }

    private static GenericRow complexRow() {
        return GenericRow.of(
                new GenericArray(new Object[] {1, 2, null}),
                new GenericMap(Collections.singletonMap(1, 2)),
                GenericRow.of(7),
                new GenericArray(new Object[] {GenericRow.of(3)}),
                new GenericMap(
                        Collections.singletonMap(
                                BinaryString.fromString("k"),
                                new GenericArray(new Object[] {1, 2}))),
                new GenericMap(Collections.singletonMap(5, 2)),
                new byte[] {1, 2},
                null);
    }
}
