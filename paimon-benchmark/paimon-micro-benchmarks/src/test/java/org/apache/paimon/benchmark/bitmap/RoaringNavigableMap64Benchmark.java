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

package org.apache.paimon.benchmark.bitmap;

import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.List;

/** Benchmark for {@link RoaringNavigableMap64#toRangeList()}. */
public class RoaringNavigableMap64Benchmark {

    public static final int DENSE_ROW_COUNT = 10000000;

    public static final int SPARSE_ROW_COUNT = 100000;

    private static volatile long result;

    @Test
    public void testDenseRangeToRangeList() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(0, DENSE_ROW_COUNT - 1));

        Benchmark benchmark =
                new Benchmark("bitmap64-to-range-list-dense", DENSE_ROW_COUNT)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase("iterator-to-ranges", 3, () -> consume(Range.toRanges(bitmap), 1));
        benchmark.addCase("toRangeList", 3, () -> consume(bitmap.toRangeList(), 1));

        benchmark.run();
    }

    @Test
    public void testSparseRangeToRangeList() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (int i = 0; i < SPARSE_ROW_COUNT; i++) {
            bitmap.add(i * 2L);
        }

        Benchmark benchmark =
                new Benchmark("bitmap64-to-range-list-sparse", SPARSE_ROW_COUNT)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "iterator-to-ranges", 5, () -> consume(Range.toRanges(bitmap), SPARSE_ROW_COUNT));
        benchmark.addCase("toRangeList", 5, () -> consume(bitmap.toRangeList(), SPARSE_ROW_COUNT));

        benchmark.run();
    }

    private static void consume(List<Range> ranges, int expectedRangeCount) {
        if (ranges.size() != expectedRangeCount) {
            throw new IllegalStateException(
                    String.format(
                            "Expected %s ranges, but got %s.", expectedRangeCount, ranges.size()));
        }
        result = ranges.size();
    }
}
