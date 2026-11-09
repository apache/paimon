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

package org.apache.paimon.flink.shuffle;

import org.apache.paimon.flink.shuffle.RangeShuffle.AssignRangeIndexOperator;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RangeShuffle}. */
class RangeShuffleTest {

    @ParameterizedTest
    @MethodSource("rangeAssignments")
    void testAssignRange(List<Integer> boundaries, int key, int firstRange, int lastRange)
            throws Exception {
        AssignRangeIndexOperator<Integer> operator =
                new AssignRangeIndexOperator<>(() -> Integer::compare);
        TupleTypeInfo<Tuple2<Integer, RowData>> inputType =
                new TupleTypeInfo<>(
                        BasicTypeInfo.INT_TYPE_INFO, InternalTypeInfo.ofFields(new IntType()));
        TupleTypeInfo<Tuple2<Integer, Tuple2<Integer, RowData>>> outputType =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, inputType);

        try (TwoInputStreamOperatorTestHarness<
                        List<Integer>,
                        Tuple2<Integer, RowData>,
                        Tuple2<Integer, Tuple2<Integer, RowData>>>
                harness = new TwoInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(
                    outputType.createSerializer(
                            harness.getExecutionConfig().getSerializerConfig()));
            harness.open();
            harness.processElement1(new StreamRecord<>(boundaries));
            // Every assignment must respect the range bounds, including randomized assignments
            // for keys equal to repeated boundaries.
            for (int i = 0; i < 128; i++) {
                harness.processElement2(new StreamRecord<>(Tuple2.of(key, GenericRowData.of(key))));
            }

            assertThat(harness.extractOutputValues())
                    .hasSize(128)
                    .allSatisfy(
                            record -> {
                                assertThat(record.f0).isBetween(firstRange, lastRange);
                                assertThat(record.f1.f0).isEqualTo(key);
                                assertThat(record.f1.f1.getInt(0)).isEqualTo(key);
                            });
        }
    }

    private static Stream<Arguments> rangeAssignments() {
        List<Integer> duplicates = Arrays.asList(10, 10, 20, 20, 20, 30, 30);
        List<Integer> identical = Arrays.asList(10, 10, 10);
        List<Integer> distinct = Arrays.asList(10, 20, 30);
        return Stream.of(
                Arguments.of(duplicates, 5, 0, 0),
                Arguments.of(duplicates, 10, 0, 1),
                Arguments.of(duplicates, 15, 2, 2),
                Arguments.of(duplicates, 20, 2, 4),
                Arguments.of(duplicates, 25, 5, 5),
                Arguments.of(duplicates, 30, 5, 6),
                Arguments.of(duplicates, 35, 7, 7),
                Arguments.of(identical, 5, 0, 0),
                Arguments.of(identical, 10, 0, 2),
                Arguments.of(identical, 15, 3, 3),
                Arguments.of(distinct, 5, 0, 0),
                Arguments.of(distinct, 10, 0, 0),
                Arguments.of(distinct, 15, 1, 1),
                Arguments.of(distinct, 20, 1, 1),
                Arguments.of(distinct, 25, 2, 2),
                Arguments.of(distinct, 30, 2, 2),
                Arguments.of(distinct, 35, 3, 3),
                Arguments.of(Collections.emptyList(), 10, 0, 0));
    }

    @Test
    void testAllocateRange() {

        // the size of test data is even
        List<Tuple2<Integer, Integer>> test0 =
                Lists.newArrayList(
                        // key and size
                        new Tuple2<>(1, 1),
                        new Tuple2<>(2, 1),
                        new Tuple2<>(3, 1),
                        new Tuple2<>(4, 1),
                        new Tuple2<>(5, 1),
                        new Tuple2<>(6, 1));
        Assertions.assertEquals(
                "[2, 4]", Arrays.deepToString(RangeShuffle.allocateRangeBaseSize(test0, 3)));

        // the size of test data is uneven,but can be evenly split based size
        List<Tuple2<Integer, Integer>> test2 =
                Lists.newArrayList(
                        new Tuple2<>(1, 1),
                        new Tuple2<>(2, 1),
                        new Tuple2<>(3, 1),
                        new Tuple2<>(4, 1),
                        new Tuple2<>(5, 4),
                        new Tuple2<>(6, 4),
                        new Tuple2<>(7, 4));
        Assertions.assertEquals(
                "[4, 5, 6]", Arrays.deepToString(RangeShuffle.allocateRangeBaseSize(test2, 4)));

        // the size of test data is uneven,and can not be evenly split
        List<Tuple2<Integer, Integer>> test1 =
                Lists.newArrayList(
                        new Tuple2<>(1, 1),
                        new Tuple2<>(2, 2),
                        new Tuple2<>(3, 3),
                        new Tuple2<>(4, 1),
                        new Tuple2<>(5, 2),
                        new Tuple2<>(6, 3));

        Assertions.assertEquals(
                "[3, 5]", Arrays.deepToString(RangeShuffle.allocateRangeBaseSize(test1, 3)));
    }
}
