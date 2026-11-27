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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.RandomAccess;

/** A compressed bitmap for 64-bit integer aggregated by tree. */
public class RoaringNavigableMap64 implements Iterable<Long>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final long RANGE_LIST_SELECT_MIN_CARDINALITY = 4096;

    private static final int RANGE_LIST_SELECT_SAMPLE_SIZE = 64;

    private static final long ROARING_CONTAINER_SPAN = 1L << 16;

    private static final long SCOPE_BATCH_SPAN = 256 * ROARING_CONTAINER_SPAN;

    private static final int SCOPE_BATCH_SAMPLE_SIZE = 8;

    private static final Method RESET_BITMAP_CACHES = bitmapCacheResetMethod();

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
        return fromInclusive < toExclusive && intersectsInclusive(fromInclusive, toExclusive - 1);
    }

    /**
     * Returns the indices of the intersecting ranges, given non-negative ranges sorted by start.
     */
    public BitSet intersectingRanges(List<Range> ranges) {
        Range bounds = ranges.isEmpty() ? null : nonNegativeBounds();
        if (bounds == null) {
            return new BitSet();
        }
        List<Range> scopes = scopePrefix(ranges, bounds.to);
        BitSet result = new BitSet(scopes.size());
        long nextCandidate = bounds.from;
        for (int start = 0; start < scopes.size(); ) {
            Range scope = scopes.get(start);
            if (scope.from > bounds.to) {
                break;
            }
            if (scope.to < nextCandidate) {
                start++;
                continue;
            }
            if (scope.from <= nextCandidate
                    || scope.to >= bounds.to
                    || contains(scope.from)
                    || contains(scope.to)) {
                result.set(start++);
                continue;
            }
            int end = scopeBatchEnd(scopes, start, bounds.to - 1);
            if (canClipScope(scope)) {
                RoaringNavigableMap64 candidates = intersectScopes(scopes, start, end);
                if (candidates.isEmpty()) {
                    nextCandidate = nextScopedCandidate(scopes, end, bounds);
                } else if (end == start + 1) {
                    result.set(start);
                } else {
                    for (int i = start; i < end; i++) {
                        Range range = scopes.get(i);
                        if (candidates.intersectsInclusive(range.from, range.to)) {
                            result.set(i);
                        }
                    }
                }
            } else if (intersectsWideScope(scope)) {
                result.set(start);
            } else {
                nextCandidate = nextScopedCandidate(scopes, end, bounds);
            }
            start = end;
        }
        return result;
    }

    private long nextScopedCandidate(List<Range> scopes, int start, Range bounds) {
        // After an empty batch, seek from the next scope's start. Later scopes can overlap.
        long from = start < scopes.size() ? scopes.get(start).from : bounds.to;
        if (from <= bounds.from) {
            return bounds.from;
        }
        if (from >= bounds.to) {
            return bounds.to;
        }
        return contains(from)
                ? from
                : roaring64NavigableMap.select(roaring64NavigableMap.rankLong(from - 1));
    }

    @Nullable
    private Range nonNegativeBounds() {
        if (isEmpty()) {
            return null;
        }
        long first = roaring64NavigableMap.first();
        if (first < 0) {
            return null;
        }
        long last = last();
        if (last < 0) {
            last = roaring64NavigableMap.select(roaring64NavigableMap.rankLong(Long.MAX_VALUE) - 1);
        }
        return new Range(first, last);
    }

    private boolean intersectsInclusive(long from, long to) {
        if (isEmpty()) {
            return false;
        }
        if (contains(from) || contains(to)) {
            return true;
        }
        long before = from == 0 ? 0 : roaring64NavigableMap.rankLong(from - 1);
        return roaring64NavigableMap.rankLong(to) > before;
    }

    private boolean intersectsWideScope(Range scope) {
        if (contains(scope.from) || contains(scope.to)) {
            return true;
        }
        long end = scope.from + SCOPE_BATCH_SPAN - 1;
        if (!intersectScope(new Range(scope.from, end)).isEmpty()) {
            return true;
        }
        Range remaining = new Range(end + 1, scope.to);
        return canClipScope(remaining)
                ? !intersectScope(remaining).isEmpty()
                : intersectsInclusive(remaining.from, remaining.to);
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
                    -1,
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
        resetBitmapCaches(roaring64NavigableMap);
    }

    public void and(RoaringNavigableMap64 other) {
        roaring64NavigableMap.and(other.roaring64NavigableMap);
        resetBitmapCaches(roaring64NavigableMap);
    }

    public void andNot(RoaringNavigableMap64 other) {
        roaring64NavigableMap.andNot(other.roaring64NavigableMap);
        resetBitmapCaches(roaring64NavigableMap);
    }

    // RoaringBitmap 1.2.1 can reuse stale cumulative counts and cached buckets after
    // set operations. Reset the delegate's caches while keeping its contents in place.
    private static Method bitmapCacheResetMethod() {
        try {
            Method method = Roaring64NavigableMap.class.getDeclaredMethod("resetPerfHelpers");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Cannot find the Roaring64NavigableMap cache reset", e);
        }
    }

    private static void resetBitmapCaches(Roaring64NavigableMap bitmap) {
        try {
            RESET_BITMAP_CACHES.invoke(bitmap);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot reset Roaring64NavigableMap caches", e);
        }
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
        resetBitmapCaches(result);
        return new RoaringNavigableMap64(result);
    }

    public static RoaringNavigableMap64 or(RoaringNavigableMap64 x1, RoaringNavigableMap64 x2) {
        Roaring64NavigableMap result = new Roaring64NavigableMap();
        result.or(x1.roaring64NavigableMap);
        result.or(x2.roaring64NavigableMap);
        resetBitmapCaches(result);
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
        forEachRange(false, (start, end) -> ranges.add(new Range(start, end)));
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

        // Keep bounded primitive endpoints so fitting selections need only one traversal.
        BoundedRanges ranges = new BoundedRanges(maxRanges);
        boolean fits =
                scope == null ? forEachRange(true, ranges) : forEachScopedRange(scope, ranges);
        return fits ? Optional.of(ranges.toRanges()) : Optional.empty();
    }

    private boolean forEachScopedRange(List<Range> scope, BoundedRanges visitor) {
        Range bounds = nonNegativeBounds();
        if (bounds == null) {
            return true;
        }
        List<Range> scopes = scopePrefix(scope, bounds.to);
        long nextCandidate = bounds.from;
        int consecutiveEmptyWindows = 0;
        for (int start = 0; start < scopes.size(); ) {
            Range range = scopes.get(start);
            if (range.from > bounds.to) {
                break;
            }
            if (range.to < nextCandidate) {
                start++;
                continue;
            }
            int end = scopeBatchEnd(scopes, start, bounds.to);
            int previousCount = visitor.size();
            boolean fits =
                    canClipScope(range)
                            ? intersectScopes(scopes, start, end).forEachRange(true, visitor)
                            : visitWideScope(range, visitor);
            if (!fits) {
                return false;
            }
            // A global seek adds work when matches alternate with empty windows.
            consecutiveEmptyWindows =
                    visitor.size() == previousCount ? consecutiveEmptyWindows + 1 : 0;
            if (consecutiveEmptyWindows == 2) {
                nextCandidate = nextScopedCandidate(scopes, end, bounds);
                consecutiveEmptyWindows = 0;
            }
            start = end;
        }
        return true;
    }

    private boolean forEachRange(boolean skipLongRuns, RangeVisitor visitor) {
        long cardinality = roaring64NavigableMap.getLongCardinality();
        if (cardinality == 0) {
            return true;
        }
        if (!visitor.canStartRange()) {
            return false;
        }
        if (shouldUseSelectRanges(cardinality)) {
            return visitRangesBySelect(0, cardinality, skipLongRuns ? last() : -1, visitor);
        }

        return visitRangesByIterator(cardinality, skipLongRuns, visitor);
    }

    private static List<Range> scopePrefix(List<Range> scopes, long lastCandidate) {
        if (scopes instanceof RandomAccess) {
            return scopes;
        }
        List<Range> prefix = new ArrayList<>();
        for (Range scope : scopes) {
            if (scope.from > lastCandidate) {
                break;
            }
            prefix.add(scope);
        }
        return prefix;
    }

    private static int scopeBatchEnd(List<Range> scopes, int start, long maxEnd) {
        Range first = scopes.get(start);
        int end = start + 1;
        if (canClipScope(first)) {
            while (end < scopes.size()) {
                long nextEnd = scopes.get(end).to;
                if (nextEnd > maxEnd || nextEnd - first.from >= SCOPE_BATCH_SPAN) {
                    break;
                }
                end++;
            }
        }
        return end;
    }

    private static boolean canClipScope(Range scope) {
        return scope.to - scope.from < SCOPE_BATCH_SPAN;
    }

    private RoaringNavigableMap64 intersectScopes(List<Range> scopes, int start, int end) {
        RoaringNavigableMap64 clipped = new RoaringNavigableMap64();
        for (int i = start; i < end; i++) {
            clipped.addRange(scopes.get(i));
        }
        clipped.and(this);
        return clipped;
    }

    private RoaringNavigableMap64 intersectScope(Range scope) {
        return intersectScopes(Collections.singletonList(scope), 0, 1);
    }

    private boolean visitWideScope(Range scope, RangeVisitor visitor) {
        long start = scope.from;
        while (scope.to - start >= SCOPE_BATCH_SPAN) {
            if (!visitor.canStartRange()) {
                return !intersectsWideScope(new Range(start, scope.to));
            }
            long end = endBeforeCrossingRun(start, start + SCOPE_BATCH_SPAN - 1);
            if (end < start) {
                // The window is covered by a single run that continues into the rest of the scope.
                return visitRangesInScope(new Range(start, scope.to), visitor);
            }
            RoaringNavigableMap64 clipped = intersectScope(new Range(start, end));
            if (clipped.isEmpty() && scope.to - end > SCOPE_BATCH_SPAN) {
                // Seek to the next candidate across a potentially large empty span.
                return visitRangesInScope(new Range(end + 1, scope.to), visitor);
            }
            if (!clipped.forEachRange(true, visitor)) {
                return false;
            }
            start = end + 1;
        }
        return intersectScope(new Range(start, scope.to)).forEachRange(true, visitor);
    }

    private long endBeforeCrossingRun(long start, long end) {
        if (!contains(end) || !contains(end + 1)) {
            return end;
        }
        // Move the boundary to the gap before the crossing run. A matching ordinal offset
        // proves that every value from that ordinal to the window end is contiguous.
        long lastIndex = roaring64NavigableMap.rankLong(end) - 1;
        long low = Math.max(0, lastIndex - (end - start));
        long high = lastIndex;
        while (low < high) {
            long middle = low + (high - low) / 2;
            if (roaring64NavigableMap.select(middle) == end - (lastIndex - middle)) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        return end - (lastIndex - low) - 1;
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
        long count = endIndex - startIndex;
        if (count - 1 == scope.to - scope.from) {
            visitor.visit(scope.from, scope.to);
            return true;
        }
        if (startIndex == 0 && endIndex == getLongCardinality()) {
            return forEachRange(true, visitor);
        }

        long first = roaring64NavigableMap.select(startIndex);
        long last = roaring64NavigableMap.select(endIndex - 1);
        if (last - first == count - 1) {
            visitor.visit(first, last);
            return true;
        }

        Range candidates = new Range(first, last);
        return canClipScope(candidates)
                ? intersectScope(candidates).forEachRange(true, visitor)
                : visitRangesBySelect(startIndex, endIndex, last, visitor);
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
        if (!iterator.hasNext()) {
            return true;
        }
        long rangeStart = iterator.next();
        long rangeEnd = rangeStart;
        for (long index = 1; iterator.hasNext(); index++) {
            long current = iterator.next();
            if (isNext(rangeEnd, current)) {
                rangeEnd = current;
                // A sparse prefix can hide a long run of rows outside index coverage.
                if (skipLongRuns && rangeEnd - rangeStart == RANGE_LIST_SELECT_MIN_CARDINALITY) {
                    long startIndex = index - RANGE_LIST_SELECT_MIN_CARDINALITY;
                    return visitRangesBySelect(startIndex, endIndex, last(), visitor);
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

    private boolean visitRangesBySelect(
            long rangeStartIndex, long endIndex, long lastRow, RangeVisitor visitor) {
        boolean wideBitmap =
                lastRow >= 0 && lastRow - roaring64NavigableMap.first() >= SCOPE_BATCH_SPAN;
        while (rangeStartIndex < endIndex) {
            if (!visitor.canStartRange()) {
                return false;
            }
            long rangeStart = roaring64NavigableMap.select(rangeStartIndex);
            if (wideBitmap) {
                long end = batchWindowEnd(rangeStartIndex, endIndex, rangeStart, lastRow);
                if (end >= rangeStart) {
                    RoaringNavigableMap64 clipped = intersectScope(new Range(rangeStart, end));
                    if (!clipped.forEachRange(true, visitor)) {
                        return false;
                    }
                    rangeStartIndex += clipped.getLongCardinality();
                    continue;
                }
            }
            long rangeEndIndex = findRangeEndIndex(rangeStartIndex, endIndex, rangeStart);
            long rangeOffset = rangeEndIndex - rangeStartIndex;
            visitor.visit(rangeStart, rangeStart + rangeOffset);
            rangeStartIndex = rangeEndIndex + 1;
        }

        return true;
    }

    private long batchWindowEnd(long startIndex, long endIndex, long start, long lastRow) {
        long windowEnd = start + Math.min(SCOPE_BATCH_SPAN - 1, lastRow - start);
        if (windowEnd == lastRow) {
            return windowEnd;
        }

        long remaining = endIndex - startIndex - 1;
        long sampleIndex = startIndex + Math.min(SCOPE_BATCH_SAMPLE_SIZE, remaining);
        long sampleRow = roaring64NavigableMap.select(sampleIndex);
        if (sampleRow > windowEnd) {
            return start - 1;
        }

        // A tight cluster can fit in only a few containers with a large gap after it.
        long fullSampleIndex = startIndex + Math.min(RANGE_LIST_SELECT_SAMPLE_SIZE, remaining);
        if (sampleRow - start < ROARING_CONTAINER_SPAN
                && fullSampleIndex != sampleIndex
                && roaring64NavigableMap.select(fullSampleIndex) > windowEnd) {
            windowEnd = Math.min(windowEnd, sampleRow | (ROARING_CONTAINER_SPAN - 1));
        }
        return endBeforeCrossingRun(start, windowEnd);
    }

    @FunctionalInterface
    private interface RangeVisitor {
        default boolean canStartRange() {
            return true;
        }

        void visit(long start, long end);
    }

    private static final class BoundedRanges implements RangeVisitor {

        private final int limit;
        private final LongArrayList starts;
        private final LongArrayList ends;

        private BoundedRanges(int limit) {
            this.limit = limit;
            this.starts = new LongArrayList(Math.min(limit, 16));
            this.ends = new LongArrayList(Math.min(limit, 16));
        }

        @Override
        public boolean canStartRange() {
            return size() < limit;
        }

        @Override
        public void visit(long start, long end) {
            starts.add(start);
            ends.add(end);
        }

        private int size() {
            return starts.size();
        }

        private List<Range> toRanges() {
            List<Range> ranges = new ArrayList<>(starts.size());
            for (int i = 0; i < starts.size(); i++) {
                ranges.add(new Range(starts.get(i), ends.get(i)));
            }
            return ranges;
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
