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

import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Supplier;

import static org.apache.paimon.codegen.CodeGenUtils.newNormalizedKeyComputer;
import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;
import static org.apache.paimon.types.DataTypes.DOUBLE;
import static org.apache.paimon.types.DataTypes.INT;
import static org.apache.paimon.types.DataTypes.STRING;
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
                () -> newRecordComparator(Arrays.asList(STRING(), INT()), new int[] {0, 1}, true));
    }

    @Test
    public void testRecordComparatorCodegenCacheMiss() {
        assertClassNotEquals(
                newRecordComparator(Arrays.asList(STRING(), INT()), new int[] {0, 1}, true),
                newRecordComparator(
                        Arrays.asList(STRING(), INT(), DOUBLE()), new int[] {0, 1, 2}, true));
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
                newRecordComparator(Arrays.asList(STRING(), INT()), new int[] {0, 1}, true),
                newNormalizedKeyComputer(Arrays.asList(STRING(), INT()), new int[] {0, 1}));
    }

    private void assertClassEquals(Supplier<?> supplier) {
        assertThat(supplier.get().getClass()).isEqualTo(supplier.get().getClass());
    }

    private void assertClassNotEquals(Object o1, Object o2) {
        assertThat(o1.getClass()).isNotEqualTo(o2.getClass());
    }
}
