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

package org.apache.paimon.format.parquet.reader;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for constructing column read ranges from range-aware and ordinary row index iterators. */
public class ParquetReadStateTest {

    private static final ColumnDescriptor DESCRIPTOR =
            new ColumnDescriptor(new String[] {"value"}, INT32, 0, 0);

    @Test
    public void testNoFilter() {
        ParquetReadState state = new ParquetReadState(DESCRIPTOR, true, null);
        assertRange(state, Long.MIN_VALUE, Long.MAX_VALUE);
        state.nextRange();
        assertRange(state, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRanges(boolean rangeAware) {
        PrimitiveIterator.OfLong indexes = indexes(createRanges(), rangeAware);
        ParquetReadState state = new ParquetReadState(DESCRIPTOR, true, indexes);
        assertThat(indexes.hasNext()).isFalse();
        assertRange(state, 0, 2);
        state.nextRange();
        assertRange(state, 10, 20);
        state.nextRange();
        assertRange(state, 30, 30);
        state.nextRange();
        assertEnd(state);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEmptyIndexes(boolean rangeAware) {
        ParquetReadState state =
                new ParquetReadState(DESCRIPTOR, true, indexes(RowRanges.EMPTY, rangeAware));
        assertEnd(state);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPartiallyConsumedIndexes(boolean rangeAware) {
        PrimitiveIterator.OfLong indexes = indexes(createRanges(), rangeAware);
        for (long expected : new long[] {0, 1, 2, 10, 11, 12}) {
            assertThat(indexes.nextLong()).isEqualTo(expected);
        }
        ParquetReadState state = new ParquetReadState(DESCRIPTOR, true, indexes);
        assertRange(state, 13, 20);
        state.nextRange();
        assertRange(state, 30, 30);
        state.nextRange();
        assertEnd(state);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIndependentColumns(boolean rangeAware) {
        RowRanges ranges = createRanges();
        ParquetReadState first =
                new ParquetReadState(DESCRIPTOR, true, indexes(ranges, rangeAware));
        ParquetReadState second =
                new ParquetReadState(DESCRIPTOR, true, indexes(ranges, rangeAware));
        first.nextRange();
        assertRange(first, 10, 20);
        assertRange(second, 0, 2);
        second.nextRange();
        assertRange(second, 10, 20);
        first.nextRange();
        assertRange(first, 30, 30);
        assertRange(second, 10, 20);
    }

    private static PrimitiveIterator.OfLong indexes(RowRanges ranges, boolean rangeAware) {
        PrimitiveIterator.OfLong iterator = ranges.iterator();
        if (rangeAware) {
            return iterator;
        }
        // Hide the concrete type to exercise the fallback with exactly the same row indexes.
        return new PrimitiveIterator.OfLong() {
            @Override
            public long nextLong() {
                return iterator.nextLong();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
        };
    }

    private static RowRanges createRanges() {
        OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
        long[] firstRows = {0, 3, 10, 21, 30};
        for (int i = 0; i < firstRows.length; i++) {
            builder.add(i * 100L, 100, firstRows[i]);
        }
        return RowRanges.create(31, IntStream.of(0, 2, 4).iterator(), builder.build());
    }

    private static void assertRange(ParquetReadState state, long start, long end) {
        assertThat(state.currentRangeStart()).isEqualTo(start);
        assertThat(state.currentRangeEnd()).isEqualTo(end);
    }

    private static void assertEnd(ParquetReadState state) {
        assertRange(state, Long.MAX_VALUE, Long.MIN_VALUE);
        state.nextRange();
        assertRange(state, Long.MAX_VALUE, Long.MIN_VALUE);
    }
}
