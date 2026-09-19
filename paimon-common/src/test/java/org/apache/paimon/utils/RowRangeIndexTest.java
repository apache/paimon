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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowRangeIndex}. */
class RowRangeIndexTest {

    @Test
    void testFromBitmapPreservesRangeQueriesAndOwnership() {
        for (long[] values :
                new long[][] {
                    {},
                    {0},
                    {0, 1, 2, 9, 11, 12},
                    {Integer.MAX_VALUE, (1L << 32) - 1, 1L << 32, Long.MAX_VALUE},
                    {-2, -1, 0, 1, Long.MAX_VALUE},
                    {Long.MIN_VALUE, -2, -1},
                    LongStream.range(0, 10000).toArray()
                }) {
            RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(values);
            RowRangeIndex expected = RowRangeIndex.create(bitmap.toRangeList());
            RowRangeIndex actual = RowRangeIndex.fromBitmap(bitmap);
            assertThat(actual.ranges()).isEqualTo(expected.ranges());
            long[] bounds = {
                Long.MIN_VALUE, -2, -1, 0, 1, 3, 8, 9, 10, 11, 13, 9999, 1L << 32, Long.MAX_VALUE
            };
            for (long start : bounds) {
                for (long end : bounds) {
                    if (start <= end) {
                        Range range = new Range(start, end);
                        assertThat(actual.intersects(start, end))
                                .isEqualTo(expected.intersects(start, end));
                        assertThat(actual.intersectedRanges(start, end))
                                .isEqualTo(expected.intersectedRanges(start, end));
                        assertThat(actual.contains(range)).isEqualTo(expected.contains(range));
                        assertThat(actual.containsExactly(range))
                                .isEqualTo(expected.containsExactly(range));
                    }
                }
            }
            bitmap.add(20000);
            assertThat(actual.ranges()).isEqualTo(expected.ranges());
            assertThat(actual.intersects(20000, 20000)).isFalse();
        }
    }

    @Test
    void testContains() {
        RowRangeIndex index =
                RowRangeIndex.create(
                        Arrays.asList(new Range(0, 99), new Range(100, 149), new Range(200, 299)));

        assertThat(index.contains(new Range(0, 149))).isTrue();
        assertThat(index.contains(new Range(50, 120))).isTrue();
        assertThat(index.contains(new Range(150, 199))).isFalse();
        assertThat(index.contains(new Range(100, 200))).isFalse();
    }
}
