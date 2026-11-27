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

package org.apache.parquet.internal.filter2.columnindex;

import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges.Range;
import org.apache.parquet.internal.filter2.columnindex.RowRanges.RowIndexIterator;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for consuming {@link RowRanges} as row indexes and ranges. */
public class RowRangesTest {

    @Test
    public void testEmptyIterator() {
        assertExhausted(iterator(RowRanges.EMPTY));
    }

    @Test
    public void testSingleRowAtZero() {
        RowIndexIterator iterator = iterator(RowRanges.createSingle(1));
        assertRange(iterator.nextRange(), 0, 0);
        assertExhausted(iterator);

        iterator = iterator(RowRanges.createSingle(1));
        assertThat(iterator.nextLong()).isZero();
        assertExhausted(iterator);
    }

    @Test
    public void testNextRangeReusesUnconsumedRange() {
        RowRanges ranges = RowRanges.createSingle(21);
        RowIndexIterator iterator = iterator(ranges);
        assertThat(iterator.nextRange()).isSameAs(ranges.getRanges().get(0));
        assertExhausted(iterator);
    }

    @Test
    public void testPartiallyConsumedRange() {
        RowIndexIterator iterator = iterator(createRanges());
        assertRange(iterator.nextRange(), 0, 2);
        assertThat(iterator.nextLong()).isEqualTo(10);
        assertThat(iterator.nextLong()).isEqualTo(11);
        assertThat(iterator.nextLong()).isEqualTo(12);
        assertRange(iterator.nextRange(), 13, 20);
        assertThat(iterator.nextLong()).isEqualTo(30);
        assertExhausted(iterator);
    }

    @Test
    public void testNextLongAdvancesToNextRange() {
        RowIndexIterator iterator = iterator(createRanges());
        assertThat(iterator.nextLong()).isZero();
        assertThat(iterator.nextLong()).isEqualTo(1);
        assertThat(iterator.nextLong()).isEqualTo(2);
        assertRange(iterator.nextRange(), 10, 20);
        assertRange(iterator.nextRange(), 30, 30);
        assertExhausted(iterator);
    }

    @Test
    public void testAdjacentPagesAreMerged() {
        OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
        builder.add(0L, 100, 0L);
        builder.add(100L, 100, 10L);
        builder.add(200L, 100, 20L);
        RowIndexIterator iterator =
                iterator(RowRanges.create(30, IntStream.of(0, 1, 2).iterator(), builder.build()));
        assertRange(iterator.nextRange(), 0, 29);
        assertExhausted(iterator);
    }

    @Test
    public void testIndependentIterators() {
        RowRanges ranges = createRanges();
        RowIndexIterator first = iterator(ranges);
        RowIndexIterator second = iterator(ranges);
        assertRange(first.nextRange(), 0, 2);
        assertThat(second.nextLong()).isZero();
        assertRange(first.nextRange(), 10, 20);
        assertRange(second.nextRange(), 1, 2);
    }

    @Test
    public void testMixedConsumptionMatchesRowIndexes() {
        OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
        for (int i = 0; i < 100; i++) {
            builder.add(i * 100L, 100, i * 10L);
        }
        RowRanges ranges =
                RowRanges.create(
                        1000,
                        IntStream.range(0, 100).filter(i -> i % 3 != 1).iterator(),
                        builder.build());
        Random random = new Random(42);
        for (int attempt = 0; attempt < 100; attempt++) {
            PrimitiveIterator.OfLong expected = ranges.iterator();
            RowIndexIterator actual = iterator(ranges);
            while (expected.hasNext()) {
                assertThat(actual.hasNext()).isTrue();
                if (random.nextBoolean()) {
                    assertThat(actual.nextLong()).isEqualTo(expected.nextLong());
                } else {
                    Range range = actual.nextRange();
                    for (long row = range.from; row <= range.to; row++) {
                        assertThat(expected.nextLong()).isEqualTo(row);
                    }
                }
            }
            assertExhausted(actual);
        }
    }

    private static RowRanges createRanges() {
        OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
        long[] firstRows = {0, 3, 10, 21, 30};
        for (int i = 0; i < firstRows.length; i++) {
            builder.add(i * 100L, 100, firstRows[i]);
        }
        return RowRanges.create(31, IntStream.of(0, 2, 4).iterator(), builder.build());
    }

    private static RowIndexIterator iterator(RowRanges ranges) {
        return (RowIndexIterator) ranges.iterator();
    }

    private static void assertRange(Range range, long from, long to) {
        assertThat(range.from).isEqualTo(from);
        assertThat(range.to).isEqualTo(to);
    }

    private static void assertExhausted(RowIndexIterator iterator) {
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::nextRange).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(iterator::nextLong).isInstanceOf(NoSuchElementException.class);
        assertThat(iterator.hasNext()).isFalse();
    }
}
