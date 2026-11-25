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

import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A compressed bitmap for 64-bit integer aggregated by tree. */
public class RoaringNavigableMap64 implements Iterable<Long>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final long RANGE_LIST_SELECT_MIN_CARDINALITY = 4096;

    private static final int RANGE_LIST_SELECT_SAMPLE_SIZE = 64;

    private static final long ROARING_CONTAINER_SPAN = 1L << 16;

    private final Roaring64NavigableMap roaring64NavigableMap;

    public RoaringNavigableMap64() {
        this.roaring64NavigableMap = new Roaring64NavigableMap();
    }

    private RoaringNavigableMap64(Roaring64NavigableMap bitmap) {
        this.roaring64NavigableMap = bitmap;
    }

    public void addRange(Range range) {
        roaring64NavigableMap.addRange(range.from, range.to + 1);
    }

    public boolean contains(long x) {
        return roaring64NavigableMap.contains(x);
    }

    /** Returns whether this bitmap contains a value in {@code [fromInclusive, toExclusive)}. */
    public boolean intersects(long fromInclusive, long toExclusive) {
        Preconditions.checkArgument(
                fromInclusive >= 0 && toExclusive >= fromInclusive,
                "Invalid non-negative bitmap range [%s, %s).",
                fromInclusive,
                toExclusive);
        if (fromInclusive == toExclusive || roaring64NavigableMap.isEmpty()) {
            return false;
        }
        long before = fromInclusive == 0 ? 0 : roaring64NavigableMap.rankLong(fromInclusive - 1);
        return roaring64NavigableMap.rankLong(toExclusive - 1) > before;
    }

    /** Copies the values within {@code range}, subtracting the range start from each value. */
    public RoaringNavigableMap64 relativeRange(Range range) {
        Preconditions.checkArgument(range.from >= 0, "Expected a non-negative range.");
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        if (isEmpty()) {
            return result;
        }
        long firstIndex = range.from == 0 ? 0 : roaring64NavigableMap.rankLong(range.from - 1);
        long endIndex = roaring64NavigableMap.rankLong(range.to);
        if (firstIndex == endIndex) {
            return result;
        }

        long first = roaring64NavigableMap.select(firstIndex);
        long last = roaring64NavigableMap.select(endIndex - 1);
        long count = endIndex - firstIndex;
        if (last - first == count - 1) {
            result.addRange(new Range(first - range.from, last - range.from));
            return result;
        }
        if ((last - first) / ROARING_CONTAINER_SPAN >= count) {
            // Selecting sparse values avoids allocating containers across a wide gap.
            for (long index = firstIndex; index < endIndex; index++) {
                result.add(roaring64NavigableMap.select(index) - range.from);
            }
            return result;
        }

        RoaringNavigableMap64 selected = range.from == 0 ? result : new RoaringNavigableMap64();
        selected.addRange(new Range(first, last));
        selected.and(this);
        if (range.from == 0) {
            return selected;
        }
        if (selected.shouldUseSelectRanges(count)) {
            selected.visitRangesBySelect(
                    0,
                    count,
                    (start, end) ->
                            result.addRange(new Range(start - range.from, end - range.from)));
        } else {
            selected.roaring64NavigableMap.forEach(row -> result.add(row - range.from));
        }
        return result;
    }

    /** Last value in bitmap order, which is unsigned. The bitmap must be nonempty. */
    public long last() {
        return roaring64NavigableMap.last();
    }

    public void add(long x) {
        roaring64NavigableMap.add(x);
    }

    public void or(RoaringNavigableMap64 other) {
        roaring64NavigableMap.or(other.roaring64NavigableMap);
    }

    public void and(RoaringNavigableMap64 other) {
        roaring64NavigableMap.and(other.roaring64NavigableMap);
    }

    public void andNot(RoaringNavigableMap64 other) {
        roaring64NavigableMap.andNot(other.roaring64NavigableMap);
    }

    public boolean isEmpty() {
        return roaring64NavigableMap.isEmpty();
    }

    public boolean runOptimize() {
        return roaring64NavigableMap.runOptimize();
    }

    public long getLongCardinality() {
        return roaring64NavigableMap.getLongCardinality();
    }

    public int getIntCardinality() {
        return roaring64NavigableMap.getIntCardinality();
    }

    public Iterator<Long> iterator() {
        return roaring64NavigableMap.iterator();
    }

    /** Returns at most the first {@code maxValues} values without boxing them. */
    public long[] toArray(int maxValues) {
        Preconditions.checkArgument(maxValues >= 0, "Max value count must not be negative.");
        int resultLength = (int) Math.min(getLongCardinality(), (long) maxValues);
        long[] result = new long[resultLength];
        LongIterator iterator = roaring64NavigableMap.getLongIterator();
        for (int i = 0; i < resultLength; i++) {
            result[i] = iterator.next();
        }
        return result;
    }

    public static RoaringNavigableMap64 and(RoaringNavigableMap64 x1, RoaringNavigableMap64 x2) {
        Roaring64NavigableMap result = new Roaring64NavigableMap();
        result.or(x1.roaring64NavigableMap);
        result.and(x2.roaring64NavigableMap);
        return new RoaringNavigableMap64(result);
    }

    public static RoaringNavigableMap64 or(RoaringNavigableMap64 x1, RoaringNavigableMap64 x2) {
        Roaring64NavigableMap result = new Roaring64NavigableMap();
        result.or(x1.roaring64NavigableMap);
        result.or(x2.roaring64NavigableMap);
        return new RoaringNavigableMap64(result);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoaringNavigableMap64 that = (RoaringNavigableMap64) o;
        return Objects.equals(this.roaring64NavigableMap, that.roaring64NavigableMap);
    }

    public void clear() {
        roaring64NavigableMap.clear();
    }

    public byte[] serialize() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            roaring64NavigableMap.runOptimize();
            roaring64NavigableMap.serializePortable(dos);
            return bos.toByteArray();
        }
    }

    public void deserialize(byte[] rbmBytes) throws IOException {
        deserialize(rbmBytes, 0, rbmBytes.length);
    }

    /** Deserializes a bitmap from a region of the given byte array without copying that region. */
    public void deserialize(byte[] rbmBytes, int offset, int length) throws IOException {
        Preconditions.checkArgument(
                offset >= 0 && length >= 0 && offset <= rbmBytes.length - length,
                "Invalid bitmap byte range [%s, %s) for array length %s.",
                offset,
                (long) offset + length,
                rbmBytes.length);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(rbmBytes, offset, length);
                DataInputStream dis = new DataInputStream(bis)) {
            roaring64NavigableMap.deserializePortable(dis);
        }
    }

    /**
     * Converts this bitmap to a list of contiguous ranges.
     *
     * <p>This is useful for interoperability with APIs that expect List&lt;Range&gt;.
     */
    public List<Range> toRangeList() {
        List<Range> ranges = new ArrayList<>();
        forEachRange(null, false, (start, end) -> ranges.add(new Range(start, end)));
        return ranges;
    }

    /** Stops converting as soon as the bitmap requires more than the allowed number of ranges. */
    public Optional<List<Range>> tryToRangeList(int maxRanges) {
        return tryToRangeList(maxRanges, null);
    }

    /**
     * Tries to materialize at most {@code maxRanges} contiguous ranges of the bitmap within the
     * given sorted, non-negative row ranges with overlaps and adjacency merged. A null scope uses
     * the whole bitmap.
     */
    public Optional<List<Range>> tryToRangeList(int maxRanges, @Nullable List<Range> scope) {
        Preconditions.checkArgument(maxRanges > 0, "Maximum range count must be positive.");
        if (scope == null && getLongCardinality() <= maxRanges) {
            return Optional.of(toRangeList());
        }
        if (scope != null && scope.isEmpty()) {
            return Optional.of(Collections.emptyList());
        }

        // Materialize Range objects only after seeing that the result fits.
        RangeCounter counter = new RangeCounter(maxRanges);
        if (!forEachRange(scope, true, counter)) {
            return Optional.empty();
        }

        List<Range> ranges = new ArrayList<>(counter.count);
        forEachRange(scope, true, (start, end) -> ranges.add(new Range(start, end)));
        return Optional.of(ranges);
    }

    private boolean forEachRange(
            @Nullable List<Range> scope, boolean skipLongRuns, RangeVisitor visitor) {
        if (scope != null) {
            for (Range range : scope) {
                if (!visitRangesInScope(range, visitor)) {
                    return false;
                }
            }
            return true;
        }

        long cardinality = roaring64NavigableMap.getLongCardinality();
        if (cardinality == 0) {
            return true;
        }
        if (!visitor.canStartRange()) {
            return false;
        }
        if (shouldUseSelectRanges(cardinality)) {
            return visitRangesBySelect(0, cardinality, visitor);
        }

        return visitRangesByIterator(cardinality, skipLongRuns, visitor);
    }

    private boolean visitRangesInScope(Range scope, RangeVisitor visitor) {
        long startIndex = scope.from == 0 ? 0 : roaring64NavigableMap.rankLong(scope.from - 1);
        long endIndex = roaring64NavigableMap.rankLong(scope.to);
        if (startIndex == endIndex) {
            return true;
        }
        if (!visitor.canStartRange()) {
            return false;
        }
        if (startIndex == 0 && endIndex == getLongCardinality()) {
            return forEachRange(null, true, visitor);
        }

        long first = roaring64NavigableMap.select(startIndex);
        long last = roaring64NavigableMap.select(endIndex - 1);
        long count = endIndex - startIndex;
        long missing = (last - first) - (count - 1);
        if (missing == 0) {
            visitor.visit(first, last);
            return true;
        }

        // Use ordinal searches when the scope can only have a few runs relative to the
        // number of mask containers. Otherwise intersect first to keep iteration local.
        long containerSpan = last / ROARING_CONTAINER_SPAN - first / ROARING_CONTAINER_SPAN + 1;
        if (containerSpan >= Math.min(count, missing + 1)) {
            return visitRangesBySelect(startIndex, endIndex, visitor);
        }
        RoaringNavigableMap64 clipped = new RoaringNavigableMap64();
        clipped.addRange(new Range(first, last));
        clipped.and(this);
        return clipped.forEachRange(null, true, visitor);
    }

    private boolean shouldUseSelectRanges(long cardinality) {
        if (cardinality < RANGE_LIST_SELECT_MIN_CARDINALITY) {
            return false;
        }

        LongIterator iterator = roaring64NavigableMap.getLongIterator();
        long previous = iterator.next();
        int samples = 0;
        int consecutive = 0;
        while (samples < RANGE_LIST_SELECT_SAMPLE_SIZE && iterator.hasNext()) {
            long current = iterator.next();
            if (isNext(previous, current)) {
                consecutive++;
            }
            previous = current;
            samples++;
        }
        // A high hit ratio can still mean many short runs (for example nine hits followed
        // by one gap). Repeated select calls are expensive for those; use the iterator.
        return consecutive * 4 >= samples * 3 && hasLongRuns(0, cardinality);
    }

    private boolean hasLongRuns(long startIndex, long endIndex) {
        return hasLongRunAt(startIndex + (endIndex - startIndex) / 2)
                && hasLongRunAt(endIndex - 2 * RANGE_LIST_SELECT_SAMPLE_SIZE - 1);
    }

    private boolean hasLongRunAt(long index) {
        // Probe both halves so an isolated gap in a long run does not force a linear scan.
        long middle = index + RANGE_LIST_SELECT_SAMPLE_SIZE;
        return isContiguous(roaring64NavigableMap.select(index), index, middle)
                || isContiguous(
                        roaring64NavigableMap.select(middle),
                        middle,
                        middle + RANGE_LIST_SELECT_SAMPLE_SIZE);
    }

    private boolean visitRangesByIterator(
            long endIndex, boolean skipLongRuns, RangeVisitor visitor) {
        LongIterator iterator = roaring64NavigableMap.getLongIterator();
        long rangeStart = iterator.next();
        long rangeEnd = rangeStart;
        for (long index = 1; index < endIndex; index++) {
            long current = iterator.next();
            if (isNext(rangeEnd, current)) {
                rangeEnd = current;
                // A sparse prefix can hide a long run of rows outside index coverage.
                if (skipLongRuns && rangeEnd - rangeStart == RANGE_LIST_SELECT_MIN_CARDINALITY) {
                    long startIndex = index - RANGE_LIST_SELECT_MIN_CARDINALITY;
                    if (hasLongRuns(startIndex, endIndex)) {
                        return visitRangesBySelect(startIndex, endIndex, visitor);
                    }
                }
            } else {
                visitor.visit(rangeStart, rangeEnd);
                // The gap proves that another range exists before we need to find its end.
                if (!visitor.canStartRange()) {
                    return false;
                }
                rangeStart = current;
                rangeEnd = current;
            }
        }
        visitor.visit(rangeStart, rangeEnd);
        return true;
    }

    private boolean visitRangesBySelect(long rangeStartIndex, long endIndex, RangeVisitor visitor) {
        while (rangeStartIndex < endIndex) {
            if (!visitor.canStartRange()) {
                return false;
            }
            long rangeStart = roaring64NavigableMap.select(rangeStartIndex);
            long rangeEndIndex = findRangeEndIndex(rangeStartIndex, endIndex, rangeStart);
            long rangeOffset = rangeEndIndex - rangeStartIndex;
            visitor.visit(rangeStart, rangeStart + rangeOffset);
            rangeStartIndex = rangeEndIndex + 1;
        }

        return true;
    }

    @FunctionalInterface
    private interface RangeVisitor {
        default boolean canStartRange() {
            return true;
        }

        void visit(long start, long end);
    }

    private static final class RangeCounter implements RangeVisitor {

        private final int limit;
        private int count;

        private RangeCounter(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean canStartRange() {
            return count < limit;
        }

        @Override
        public void visit(long start, long end) {
            count++;
        }
    }

    private long findRangeEndIndex(long rangeStartIndex, long cardinality, long rangeStart) {
        long lower = rangeStartIndex;
        long upper;
        long step = 1;

        while (true) {
            long candidateIndex = rangeStartIndex + step;
            if (candidateIndex < 0 || candidateIndex >= cardinality) {
                upper = cardinality - 1;
                break;
            }

            if (!isContiguous(rangeStart, rangeStartIndex, candidateIndex)) {
                upper = candidateIndex - 1;
                break;
            }

            lower = candidateIndex;
            if (step > Long.MAX_VALUE / 2) {
                upper = cardinality - 1;
                break;
            }
            step <<= 1;
        }

        while (lower < upper) {
            long mid = lower + ((upper - lower + 1) >>> 1);
            if (isContiguous(rangeStart, rangeStartIndex, mid)) {
                lower = mid;
            } else {
                upper = mid - 1;
            }
        }
        return lower;
    }

    private boolean isContiguous(long rangeStart, long rangeStartIndex, long candidateIndex) {
        long offset = candidateIndex - rangeStartIndex;
        return rangeStart <= Long.MAX_VALUE - offset
                && roaring64NavigableMap.select(candidateIndex) == rangeStart + offset;
    }

    private static boolean isNext(long previous, long current) {
        return previous != Long.MAX_VALUE && current == previous + 1;
    }

    public static RoaringNavigableMap64 bitmapOf(long... dat) {
        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        for (long ele : dat) {
            roaringBitmap64.add(ele);
        }
        return roaringBitmap64;
    }
}
