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

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Test for {@link RangeShuffle}. */
class RangeShuffleTest {

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
