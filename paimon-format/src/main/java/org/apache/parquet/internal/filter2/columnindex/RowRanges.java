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

import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;

/**
 * Class representing row ranges in a row-group. These row ranges are calculated as a result of the
 * column index based filtering. To be used iterate over the matching row indexes to be read from a
 * row-group, retrieve the count of the matching rows or check overlapping of a row index range.
 *
 * <p>Note: The class was copied over to support using selected position and deleted position result
 * to filter or narrow the {@link RowRanges}. Added a new method {@link RowRanges#create(long, long,
 * PrimitiveIterator.OfInt, OffsetIndex, RoaringBitmap32, RoaringBitmap32)}
 *
 * @see ColumnIndexFilter#calculateRowRanges(Filter, ColumnIndexStore, Set, long, long,
 *     RoaringBitmap32, RoaringBitmap32)
 */
public class RowRanges {

    /** Make it public because some uppler layer application need to access it. */
    public static class Range {

        // Returns the union of the two ranges or null if there are elements between them.
        private static Range union(Range left, Range right) {
            if (left.from <= right.from) {
                if (left.to + 1 >= right.from) {
                    return new Range(left.from, Math.max(left.to, right.to));
                }
            } else if (right.to + 1 >= left.from) {
                return new Range(right.from, Math.max(left.to, right.to));
            }
            return null;
        }

        // Returns the intersection of the two ranges of null if they are not overlapped.
        private static Range intersection(Range left, Range right) {
            if (left.from <= right.from) {
                if (left.to >= right.from) {
                    return new Range(right.from, Math.min(left.to, right.to));
                }
            } else if (right.to >= left.from) {
                return new Range(left.from, Math.min(left.to, right.to));
            }
            return null;
        }

        public final long from;
        public final long to;

        // Creates a range of [from, to] (from and to are inclusive; empty ranges are not valid)
        Range(long from, long to) {
            assert from <= to;
            this.from = from;
            this.to = to;
        }

        long count() {
            return to - from + 1;
        }

        boolean isBefore(Range other) {
            return to < other.from;
        }

        boolean isAfter(Range other) {
            return from > other.to;
        }

        @Override
        public String toString() {
            return "[" + from + ", " + to + ']';
        }
    }

    public static final RowRanges EMPTY = new RowRanges(Collections.emptyList());

    private final List<Range> ranges;

    private RowRanges() {
        this(new ArrayList<>());
    }

    private RowRanges(Range range) {
        this(Collections.singletonList(range));
    }

    private RowRanges(List<Range> ranges) {
        this.ranges = ranges;
    }

    /**
     * Creates an immutable RowRanges object with the single range [0, rowCount - 1].
     *
     * @param rowCount a single row count
     * @return an immutable RowRanges
     */
    public static RowRanges createSingle(long rowCount) {
        return new RowRanges(new Range(0L, rowCount - 1L));
    }

    /**
     * Creates a mutable RowRanges object with the following ranges:
     *
     * <pre>
     * [firstRowIndex[0], lastRowIndex[0]],
     * [firstRowIndex[1], lastRowIndex[1]],
     * ...,
     * [firstRowIndex[n], lastRowIndex[n]]
     * </pre>
     *
     * <p>(See OffsetIndex.getFirstRowIndex and OffsetIndex.getLastRowIndex for details.)
     *
     * <p>The union of the ranges are calculated so the result ranges always contain the disjunct
     * ranges. See union for details.
     *
     * @param rowCount row count
     * @param pageIndexes pageIndexes
     * @param offsetIndex offsetIndex
     * @return a mutable RowRanges
     */
    public static RowRanges create(
            long rowCount, PrimitiveIterator.OfInt pageIndexes, OffsetIndex offsetIndex) {
        RowRanges ranges = new RowRanges();
        while (pageIndexes.hasNext()) {
            int pageIndex = pageIndexes.nextInt();
            ranges.add(
                    new Range(
                            offsetIndex.getFirstRowIndex(pageIndex),
                            offsetIndex.getLastRowIndex(pageIndex, rowCount)));
        }
        return ranges;
    }

    /** Support using the selected position or the deleted position to filter the row ranges. */
    public static RowRanges create(
            long rowCount,
            long rowIndexOffset,
            PrimitiveIterator.OfInt pageIndexes,
            OffsetIndex offsetIndex,
            @Nullable RoaringBitmap32 selection,
            @Nullable RoaringBitmap32 deletion) {
        RowRanges ranges = new RowRanges();
        while (pageIndexes.hasNext()) {
            int pageIndex = pageIndexes.nextInt();
            long firstRowIndex = offsetIndex.getFirstRowIndex(pageIndex);
            long lastRowIndex = offsetIndex.getLastRowIndex(pageIndex, rowCount);

            // using selected position or deleted position to filter or narrow the row ranges
            long first = rowIndexOffset + firstRowIndex;
            long last = rowIndexOffset + lastRowIndex;
            if (selection != null) {
                if (!selection.intersects(first, last + 1)) {
                    continue;
                }
                firstRowIndex = selection.nextValue((int) first) - rowIndexOffset;
                lastRowIndex = selection.previousValue((int) (last)) - rowIndexOffset;
            } else if (deletion != null) {
                if (deletion.contains(first, last + 1)) {
                    continue;
                }
            }

            ranges.add(new Range(firstRowIndex, lastRowIndex));
        }
        return ranges;
    }

    /**
     * Calculates the union of the two specified RowRanges object. The union of two range is
     * calculated if there are no elements between them. Otherwise, the two disjunct ranges are
     * stored separately.
     *
     * <pre>
     * For example:
     * [113, 241] ∪ [221, 340] = [113, 340]
     * [113, 230] ∪ [231, 340] = [113, 340]
     * while
     * [113, 230] ∪ [232, 340] = [113, 230], [232, 340]
     * </pre>
     *
     * <p>The result RowRanges object will contain all the row indexes that were contained in one of
     * the specified objects.
     *
     * @param left left RowRanges
     * @param right right RowRanges
     * @return a mutable RowRanges contains all the row indexes that were contained in one of the
     *     specified objects
     */
    public static RowRanges union(RowRanges left, RowRanges right) {
        RowRanges result = new RowRanges();
        Iterator<Range> it1 = left.ranges.iterator();
        Iterator<Range> it2 = right.ranges.iterator();
        if (it2.hasNext()) {
            Range range2 = it2.next();
            while (it1.hasNext()) {
                Range range1 = it1.next();
                if (range1.isAfter(range2)) {
                    result.add(range2);
                    range2 = range1;
                    Iterator<Range> tmp = it1;
                    it1 = it2;
                    it2 = tmp;
                } else {
                    result.add(range1);
                }
            }
            result.add(range2);
        } else {
            it2 = it1;
        }
        while (it2.hasNext()) {
            result.add(it2.next());
        }

        return result;
    }

    /**
     * Calculates the intersection of the two specified RowRanges object. Two ranges intersect if
     * they have common elements otherwise the result is empty.
     *
     * <pre>
     * For example:
     * [113, 241] ∩ [221, 340] = [221, 241]
     * while
     * [113, 230] ∩ [231, 340] = &lt;EMPTY&gt;
     * </pre>
     *
     * @param left left RowRanges
     * @param right right RowRanges
     * @return a mutable RowRanges contains all the row indexes that were contained in both of the
     *     specified objects
     */
    public static RowRanges intersection(RowRanges left, RowRanges right) {
        RowRanges result = new RowRanges();

        int rightIndex = 0;
        for (Range l : left.ranges) {
            for (int i = rightIndex, n = right.ranges.size(); i < n; ++i) {
                Range r = right.ranges.get(i);
                if (l.isBefore(r)) {
                    break;
                } else if (l.isAfter(r)) {
                    rightIndex = i + 1;
                    continue;
                }
                result.add(Range.intersection(l, r));
            }
        }

        return result;
    }

    /*
     * Adds a range to the end of the list of ranges. It maintains the disjunct ascending order(*) of the ranges by
     * trying to union the specified range to the last ranges in the list. The specified range shall be larger(*) than
     * the last one or might be overlapped with some of the last ones.
     * (*) [a, b] < [c, d] if b < c
     */
    private void add(Range range) {
        Range rangeToAdd = range;
        for (int i = ranges.size() - 1; i >= 0; --i) {
            Range last = ranges.get(i);
            assert !last.isAfter(range);
            Range u = Range.union(last, rangeToAdd);
            if (u == null) {
                break;
            }
            rangeToAdd = u;
            ranges.remove(i);
        }
        ranges.add(rangeToAdd);
    }

    /** @return the number of rows in the ranges */
    public long rowCount() {
        long cnt = 0;
        for (Range range : ranges) {
            cnt += range.count();
        }
        return cnt;
    }

    /** @return the ascending iterator of the row indexes contained in the ranges */
    public PrimitiveIterator.OfLong iterator() {
        return new PrimitiveIterator.OfLong() {
            private int currentRangeIndex = -1;
            private Range currentRange;
            private long next = findNext();

            private long findNext() {
                if (currentRange == null || next + 1 > currentRange.to) {
                    if (currentRangeIndex + 1 < ranges.size()) {
                        currentRange = ranges.get(++currentRangeIndex);
                        next = currentRange.from;
                    } else {
                        return -1;
                    }
                } else {
                    ++next;
                }
                return next;
            }

            @Override
            public boolean hasNext() {
                return next >= 0;
            }

            @Override
            public long nextLong() {
                long ret = next;
                if (ret < 0) {
                    throw new NoSuchElementException();
                }
                next = findNext();
                return ret;
            }
        };
    }

    /**
     * @param from the first row of the range to be checked for connection
     * @param to the last row of the range to be checked for connection
     * @return {@code true} if the specified range is overlapping (have common elements) with one of
     *     the ranges
     */
    public boolean isOverlapping(long from, long to) {
        return Collections.binarySearch(
                        ranges,
                        new Range(from, to),
                        (r1, r2) -> r1.isBefore(r2) ? -1 : r1.isAfter(r2) ? 1 : 0)
                >= 0;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    @Override
    public String toString() {
        return ranges.toString();
    }
}
