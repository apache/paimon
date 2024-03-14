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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.codegen.CodeGenUtils.ClassKey;
import static org.assertj.core.api.Assertions.assertThat;

public class CodeGenUtilsTest {

    @BeforeAll
    public static void before() {
        // cleanup cached class before tests
        CodeGenUtils.COMPILED_CLASS_CACHE.invalidateAll();
    }

    @Test
    public void testProjectionCodegenCache() {
        String name = "Projection";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 = Arrays.asList(new VarCharType(1), new IntType());
        int[] fieldIndexes1 = new int[] {0, 1};
        int[] fieldIndexes2 = new int[] {0, 1};

        Projection projection =
                CodeGenUtils.newProjection(
                        RowType.builder().fields(dataTypes1).build(), fieldIndexes1);

        ClassKey classKey = new ClassKey(Projection.class, name, dataTypes2, fieldIndexes2);
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNotNull();
        assertThat(projection.getClass()).isEqualTo(classPair.getLeft());
    }

    @Test
    public void testProjectionCodegenCacheMiss() {
        String name = "Projection";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 =
                Arrays.asList(new VarCharType(1), new IntType(), new DoubleType());
        int[] fieldIndexes1 = new int[] {0, 1};
        int[] fieldIndexes2 = new int[] {0, 1, 2};

        CodeGenUtils.newProjection(RowType.builder().fields(dataTypes1).build(), fieldIndexes1);

        ClassKey classKey = new ClassKey(Projection.class, name, dataTypes2, fieldIndexes2);
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNull();
    }

    @Test
    public void testNormalizedKeyComputerCodegenCache() {
        String name1 = "NormalizedKeyComputer";
        String name2 = "NormalizedKeyComputer";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 = Arrays.asList(new VarCharType(1), new IntType());
        int[] fieldIndexes1 = new int[] {0, 1};
        int[] fieldIndexes2 = new int[] {0, 1};

        NormalizedKeyComputer normalizedKeyComputer =
                CodeGenUtils.newNormalizedKeyComputer(dataTypes1, fieldIndexes1, name1);

        ClassKey classKey =
                new ClassKey(NormalizedKeyComputer.class, name2, dataTypes2, fieldIndexes2);
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNotNull();
        assertThat(normalizedKeyComputer.getClass()).isEqualTo(classPair.getLeft());
    }

    @Test
    public void testNormalizedKeyComputerCodegenCacheMiss() {
        String name1 = "NormalizedKeyComputer";
        String name2 = "NormalizedKeyComputer";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 =
                Arrays.asList(new VarCharType(1), new IntType(), new DoubleType());
        int[] fieldIndexes1 = new int[] {0, 1};
        int[] fieldIndexes2 = new int[] {0, 1, 2};

        CodeGenUtils.newNormalizedKeyComputer(dataTypes1, fieldIndexes1, name1);

        ClassKey classKey =
                new ClassKey(NormalizedKeyComputer.class, name2, dataTypes2, fieldIndexes2);
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNull();
    }

    @Test
    public void testRecordComparatorCodegenCache() {
        String name1 = "RecordComparator";
        String name2 = "RecordComparator";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 = Arrays.asList(new VarCharType(1), new IntType());
        int[] fieldIndexes1 = new int[] {0, 1};
        int[] fieldIndexes2 = new int[] {0, 1};

        RecordComparator recordComparator =
                CodeGenUtils.newRecordComparator(dataTypes1, fieldIndexes1, name1);

        ClassKey classKey = new ClassKey(RecordComparator.class, name2, dataTypes2, fieldIndexes2);
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNotNull();
        assertThat(recordComparator.getClass()).isEqualTo(classPair.getLeft());
    }

    @Test
    public void testRecordComparatorCodegenCacheMiss() {
        String name1 = "RecordComparator";
        String name2 = "RecordComparator";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 =
                Arrays.asList(new VarCharType(1), new IntType(), new DoubleType());
        int[] fieldIndexes1 = new int[] {0, 1};
        int[] fieldIndexes2 = new int[] {0, 1, 2};

        CodeGenUtils.newRecordComparator(dataTypes1, fieldIndexes1, name1);

        ClassKey classKey = new ClassKey(RecordComparator.class, name2, dataTypes2, fieldIndexes2);
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNull();
    }

    @Test
    public void testRecordEqualiserCodegenCache() {
        String name1 = "RecordEqualiser";
        String name2 = "RecordEqualiser";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 = Arrays.asList(new VarCharType(1), new IntType());

        RecordEqualiser recordEqualiser = CodeGenUtils.newRecordEqualiser(dataTypes1, name1);

        ClassKey classKey =
                new ClassKey(
                        RecordEqualiser.class,
                        name2,
                        dataTypes2,
                        IntStream.range(0, dataTypes2.size()).toArray());
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNotNull();
        assertThat(recordEqualiser.getClass()).isEqualTo(classPair.getLeft());
    }

    @Test
    public void testRecordEqualiserCodegenCacheMiss() {
        String name1 = "RecordEqualiser";
        String name2 = "RecordEqualiser";
        List<DataType> dataTypes1 = Arrays.asList(new VarCharType(1), new IntType());
        List<DataType> dataTypes2 =
                Arrays.asList(new VarCharType(1), new IntType(), new DoubleType());

        CodeGenUtils.newRecordEqualiser(dataTypes1, name1);

        ClassKey classKey =
                new ClassKey(
                        RecordEqualiser.class,
                        name2,
                        dataTypes2,
                        IntStream.range(0, dataTypes2.size()).toArray());
        Pair<Class<?>, Object[]> classPair =
                CodeGenUtils.COMPILED_CLASS_CACHE.getIfPresent(classKey);

        assertThat(classPair).isNull();
    }
}
