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

package org.apache.paimon.codegen;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.codegen.CodeGenUtils.newNormalizedKeyComputer;
import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;
import static org.apache.paimon.types.DataTypes.DOUBLE;
import static org.apache.paimon.types.DataTypes.FLOAT;
import static org.apache.paimon.types.DataTypes.INT;
import static org.apache.paimon.types.DataTypes.MAP;
import static org.apache.paimon.types.DataTypes.STRING;
import static org.apache.paimon.types.DataTypes.VECTOR;
import static org.assertj.core.api.Assertions.assertThat;

class CodeGenUtilsTest {

    @Test
    public void testProjectionCodegenCache() {
        assertClassEquals(
                () ->
                        newProjection(
                                RowType.builder().fields(STRING(), INT()).build(),
                                new int[] {0, 1}));
    }

    @Test
    public void testProjectionCodegenCacheMiss() {
        assertClassNotEquals(
                newProjection(RowType.builder().fields(STRING(), INT()).build(), new int[] {0, 1}),
                newProjection(
                        RowType.builder().fields(STRING(), INT(), DOUBLE()).build(),
                        new int[] {0, 1, 2}));
    }

    @Test
    public void testProjectionWithVector() {
        assertClassEquals(
                () ->
                        newProjection(
                                RowType.builder().fields(INT(), VECTOR(3, FLOAT())).build(),
                                new int[] {1}));
    }

    @Test
    public void testNormalizedKeyComputerCodegenCache() {
        assertClassEquals(
                () -> newNormalizedKeyComputer(Arrays.asList(STRING(), INT()), new int[] {0, 1}));
    }

    @Test
    public void testNormalizedKeyComputerCodegenCacheMiss() {
        assertClassNotEquals(
                newNormalizedKeyComputer(Arrays.asList(STRING(), INT()), new int[] {0, 1}),
                newNormalizedKeyComputer(
                        Arrays.asList(STRING(), INT(), DOUBLE()), new int[] {0, 1, 2}));
    }

    @Test
    public void testRecordComparatorCodegenCache() {
        assertClassEquals(
                () -> newRecordComparator(Arrays.asList(STRING(), INT()), new int[] {0, 1}));
    }

    @Test
    public void testRecordComparatorCodegenCacheWithVector() {
        assertClassEquals(
                () ->
                        newRecordComparator(
                                Arrays.asList(STRING(), VECTOR(3, INT())), new int[] {0, 1}));
    }

    @Test
    public void testRecordComparatorCodegenCacheMiss() {
        assertClassNotEquals(
                newRecordComparator(Arrays.asList(STRING(), INT()), new int[] {0, 1}),
                newRecordComparator(Arrays.asList(STRING(), INT(), DOUBLE()), new int[] {0, 1, 2}));
    }

    @Test
    public void testRecordComparatorOrderCacheMiss() {
        RecordComparator ascending =
                newRecordComparator(Arrays.asList(STRING(), INT()), new int[] {0, 1});
        RecordComparator descending =
                newRecordComparator(
                        Arrays.asList(STRING(), INT()),
                        new int[] {0, 1},
                        new boolean[] {false, false});

        InternalRow row1 = GenericRow.of(BinaryString.fromString("a"), 1);
        InternalRow row2 = GenericRow.of(BinaryString.fromString("b"), 1);

        assertThat(ascending.compare(row1, row2)).isLessThan(0);
        assertThat(descending.compare(row1, row2)).isGreaterThan(0);
        assertThat(ascending.getClass()).isNotEqualTo(descending.getClass());
    }

    @Test
    public void testFloatingPointComparatorMatchesTotalOrder() {
        RecordComparator doubleComparator =
                newRecordComparator(Arrays.asList(DOUBLE()), new int[] {0});
        double[] doubles = {
            Double.NEGATIVE_INFINITY, -1.0d, -0.0d, 0.0d, 1.0d, Double.POSITIVE_INFINITY, Double.NaN
        };
        for (double a : doubles) {
            for (double b : doubles) {
                assertThat(sign(doubleComparator.compare(GenericRow.of(a), GenericRow.of(b))))
                        .as("compare(%s, %s)", a, b)
                        .isEqualTo(sign(Double.compare(a, b)));
            }
        }

        RecordComparator floatComparator =
                newRecordComparator(Arrays.asList(FLOAT()), new int[] {0});
        float[] floats = {
            Float.NEGATIVE_INFINITY, -1.0f, -0.0f, 0.0f, 1.0f, Float.POSITIVE_INFINITY, Float.NaN
        };
        for (float a : floats) {
            for (float b : floats) {
                assertThat(sign(floatComparator.compare(GenericRow.of(a), GenericRow.of(b))))
                        .as("compare(%s, %s)", a, b)
                        .isEqualTo(sign(Float.compare(a, b)));
            }
        }
    }

    private static int sign(int value) {
        return Integer.compare(value, 0);
    }

    @Test
    public void testMapComparatorComparesValuesWithValueType() {
        // MAP<INT, STRING>: the generated comparator must compare the value array using the
        // value type (STRING), not the key type (INT). With equal keys, ordering is decided by
        // the values, so a wrong value type yields an incorrect result or a runtime failure.
        RecordComparator comparator =
                newRecordComparator(Arrays.asList(MAP(INT(), STRING())), new int[] {0});

        InternalRow rowA = GenericRow.of(singletonMap(1, "a"));
        InternalRow rowB = GenericRow.of(singletonMap(1, "b"));

        assertThat(comparator.compare(rowA, rowB)).isLessThan(0);
        assertThat(comparator.compare(rowB, rowA)).isGreaterThan(0);
        assertThat(comparator.compare(rowA, GenericRow.of(singletonMap(1, "a")))).isZero();
    }

    private static GenericMap singletonMap(int key, String value) {
        Map<Object, Object> map = new HashMap<>();
        map.put(key, BinaryString.fromString(value));
        return new GenericMap(map);
    }

    @Test
    public void sortByDoubleColumnWithNaNProducesTotalOrder() {
        RecordComparator scoreComparator =
                newRecordComparator(Arrays.asList(DOUBLE()), new int[] {0});

        List<InternalRow> batch = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            batch.add(GenericRow.of((double) ((i % 11) - 5)));
            if (i % 4 == 0) {
                batch.add(GenericRow.of(Double.NaN));
            }
        }
        batch.add(GenericRow.of(Double.NEGATIVE_INFINITY));
        batch.add(GenericRow.of(Double.POSITIVE_INFINITY));

        Collections.sort(batch, scoreComparator);

        for (int i = 1; i < batch.size(); i++) {
            double prev = batch.get(i - 1).getDouble(0);
            double cur = batch.get(i).getDouble(0);
            assertThat(Double.compare(prev, cur))
                    .as("position %d (%s) sorted after position %d (%s)", i - 1, prev, i, cur)
                    .isLessThanOrEqualTo(0);
        }
    }

    @Test
    public void testRecordEqualiserCodegenCache() {
        assertClassEquals(() -> newRecordEqualiser(Arrays.asList(STRING(), INT())));
    }

    @Test
    public void testRecordEqualiserCodegenCacheMiss() {
        assertClassNotEquals(
                newRecordEqualiser(Arrays.asList(STRING(), INT())),
                newRecordEqualiser(Arrays.asList(STRING(), INT(), DOUBLE())));
    }

    @Test
    public void testHybridNotEqual() {
        assertClassNotEquals(
                newRecordComparator(Arrays.asList(STRING(), INT()), new int[] {0, 1}),
                newNormalizedKeyComputer(Arrays.asList(STRING(), INT()), new int[] {0, 1}));
    }

    private void assertClassEquals(Supplier<?> supplier) {
        assertThat(supplier.get().getClass()).isEqualTo(supplier.get().getClass());
    }

    private void assertClassNotEquals(Object o1, Object o2) {
        assertThat(o1.getClass()).isNotEqualTo(o2.getClass());
    }
}
