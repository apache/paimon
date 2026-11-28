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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RoaringNavigableMap64}. */
public class RoaringNavigableMap64Test {

    @Test
    public void testIntersectsHalfOpenRange() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.add(2);
        bitmap.add(7);
        bitmap.add(Long.MAX_VALUE - 1);

        assertThat(bitmap.intersects(0, 2)).isFalse();
        assertThat(bitmap.intersects(2, 3)).isTrue();
        assertThat(bitmap.intersects(3, 7)).isFalse();
        assertThat(bitmap.intersects(3, 8)).isTrue();
        assertThat(bitmap.intersects(Long.MAX_VALUE - 1, Long.MAX_VALUE)).isTrue();
        assertThat(bitmap.intersects(8, Long.MAX_VALUE - 1)).isFalse();
        assertThat(bitmap.intersects(7, 7)).isFalse();
    }

    @Test
    public void testRelativeRangeClipsAndRebasesRuns() {
        long start = (1L << 32) - 100_000;
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(start - 12, start + 99_999));
        bitmap.addRange(new Range(start + 100_004, start + 160_000));

        assertThat(bitmap.relativeRange(new Range(start, start + 150_000)).toRangeList())
                .containsExactly(new Range(0, 99_999), new Range(100_004, 150_000));
        assertThat(bitmap.relativeRange(new Range(start + 100_000, start + 100_003))).isEmpty();
        assertThat(bitmap.relativeRange(new Range(start + 140_000, start + 150_000)).toRangeList())
                .containsExactly(new Range(0, 10_000));
        assertThat(bitmap.getLongCardinality()).isEqualTo(160_009);
    }

    @Test
    public void testRelativeRangeHandlesSparseValuesAtLongBoundary() {
        RoaringNavigableMap64 bitmap =
                RoaringNavigableMap64.bitmapOf(1, 7, Long.MAX_VALUE - 4, Long.MAX_VALUE);

        assertThat(bitmap.relativeRange(new Range(7, Long.MAX_VALUE)))
                .containsExactly(0L, Long.MAX_VALUE - 11, Long.MAX_VALUE - 7);
        assertThat(bitmap.relativeRange(new Range(Long.MAX_VALUE - 5, Long.MAX_VALUE)))
                .containsExactly(1L, 5L);
        assertThat(bitmap.relativeRange(new Range(0, 6))).containsExactly(1L);
    }

    @Test
    public void testAddRangeBasic() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(5, 10));

        // Verify the range [5, 10] is added (inclusive on both ends)
        assertThat(bitmap.getLongCardinality()).isEqualTo(6);
        assertThat(bitmap.contains(4)).isFalse();
        assertThat(bitmap.contains(5)).isTrue();
        assertThat(bitmap.contains(7)).isTrue();
        assertThat(bitmap.contains(10)).isTrue();
        assertThat(bitmap.contains(11)).isFalse();
    }

    @Test
    public void testAddRangeSingleElement() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(100, 100));

        // A range where from == to should add exactly one element
        assertThat(bitmap.getLongCardinality()).isEqualTo(1);
        assertThat(bitmap.contains(99)).isFalse();
        assertThat(bitmap.contains(100)).isTrue();
        assertThat(bitmap.contains(101)).isFalse();
    }

    @Test
    public void testAddRangeMultipleNonOverlapping() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(0, 5));
        bitmap.addRange(new Range(10, 15));
        bitmap.addRange(new Range(20, 25));

        // Verify cardinality: 6 + 6 + 6 = 18
        assertThat(bitmap.getLongCardinality()).isEqualTo(18);

        // Verify gaps are not filled
        assertThat(bitmap.contains(6)).isFalse();
        assertThat(bitmap.contains(9)).isFalse();
        assertThat(bitmap.contains(16)).isFalse();
        assertThat(bitmap.contains(19)).isFalse();

        // Verify ranges contain expected values
        assertThat(bitmap.contains(0)).isTrue();
        assertThat(bitmap.contains(5)).isTrue();
        assertThat(bitmap.contains(10)).isTrue();
        assertThat(bitmap.contains(15)).isTrue();
        assertThat(bitmap.contains(20)).isTrue();
        assertThat(bitmap.contains(25)).isTrue();

        // Verify toRangeList reconstructs the ranges correctly
        List<Range> ranges = bitmap.toRangeList();
        assertThat(ranges).hasSize(3);
        assertThat(ranges.get(0)).isEqualTo(new Range(0, 5));
        assertThat(ranges.get(1)).isEqualTo(new Range(10, 15));
        assertThat(ranges.get(2)).isEqualTo(new Range(20, 25));
    }

    @Test
    public void testAddRangeLargeValues() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        // Test with values beyond Integer.MAX_VALUE
        long start = Integer.MAX_VALUE + 100L;
        long end = Integer.MAX_VALUE + 200L;
        bitmap.addRange(new Range(start, end));

        assertThat(bitmap.getLongCardinality()).isEqualTo(101);
        assertThat(bitmap.contains(start - 1)).isFalse();
        assertThat(bitmap.contains(start)).isTrue();
        assertThat(bitmap.contains(start + 50)).isTrue();
        assertThat(bitmap.contains(end)).isTrue();
        assertThat(bitmap.contains(end + 1)).isFalse();

        // Verify iteration order
        List<Long> values = new ArrayList<>();
        bitmap.iterator().forEachRemaining(values::add);
        assertThat(values).hasSize(101);
        assertThat(values.get(0)).isEqualTo(start);
        assertThat(values.get(100)).isEqualTo(end);
    }

    @Test
    public void testToArrayWithLimit() {
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(1, 3, 5);

        assertThat(bitmap.toArray(0)).isEmpty();
        assertThat(bitmap.toArray(2)).containsExactly(1, 3);
        assertThat(bitmap.toArray(Integer.MAX_VALUE)).containsExactly(1, 3, 5);
    }

    @Test
    public void testDeserializeByteArrayRegion() throws Exception {
        RoaringNavigableMap64 expected = RoaringNavigableMap64.bitmapOf(1, 3, 1L << 33);
        byte[] serialized = expected.serialize();
        byte[] framed = new byte[serialized.length + 4];
        System.arraycopy(serialized, 0, framed, 2, serialized.length);

        RoaringNavigableMap64 actual = new RoaringNavigableMap64();
        actual.deserialize(framed, 2, serialized.length);
        assertThat(actual).isEqualTo(expected);

        assertThatThrownBy(() -> actual.deserialize(framed, 2, framed.length))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid bitmap byte range");
    }

    @Test
    public void testToRangeListAcrossHighBitmapBoundary() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        long start = (1L << 32) - 2;
        long end = (1L << 32) + 2;
        bitmap.addRange(new Range(start, end));

        assertThat(bitmap.toRangeList()).containsExactly(new Range(start, end));
    }

    @Test
    public void testToRangeListForLargeContiguousRange() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(new Range(0, 9999));

        assertThat(bitmap.toRangeList()).containsExactly(new Range(0, 9999));

        RoaringNavigableMap64 withEarlyGap = new RoaringNavigableMap64();
        withEarlyGap.addRange(new Range(0, 8));
        withEarlyGap.addRange(new Range(10, 9999));
        assertThat(withEarlyGap.toRangeList())
                .containsExactly(new Range(0, 8), new Range(10, 9999));
    }

    @Test
    public void testToRangeListForDenseShortRuns() {
        for (int runLength : new int[] {1, 9, 31, 64, 256}) {
            RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
            List<Range> expected = new ArrayList<>();
            // Cross 32-bit bitmap containers and the 64-bit bitmap's high-key boundary.
            long start = (1L << 32) - 10_000;
            for (int run = 0; run < 1000; run++) {
                Range range = new Range(start, start + runLength - 1);
                bitmap.addRange(range);
                expected.add(range);
                start += runLength + 1;
            }
            assertThat(bitmap.toRangeList()).isEqualTo(expected);
            assertThat(bitmap.tryToRangeList(expected.size())).contains(expected);
            assertThat(bitmap.tryToRangeList(expected.size() - 1)).isEmpty();
        }
    }

    @Test
    public void testRangeConversionAfterUnionWithCachedCardinality() {
        long high = Long.MAX_VALUE - 1;
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(5, high);
        assertThat(bitmap.getLongCardinality()).isEqualTo(2);

        bitmap.or(RoaringNavigableMap64.bitmapOf(8));

        List<Range> expected =
                Arrays.asList(new Range(5, 5), new Range(8, 8), new Range(high, high));
        assertThat(bitmap.toRangeList()).containsExactlyElementsOf(expected);
        assertThat(bitmap.getLongCardinality()).isEqualTo(3);
        assertThat(bitmap.toArray(3)).containsExactly(5L, 8L, high);
        assertThat(bitmap.tryToRangeList(2)).isEmpty();
        assertThat(bitmap.tryToRangeList(3, Collections.singletonList(new Range(0, high))))
                .contains(expected);
        assertThat(bitmap.relativeRange(new Range(4, high))).containsExactly(1L, 4L, high - 4);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRangeConversionAfterRemovingRowsWithCachedCardinality(boolean andNot) {
        long high = Long.MAX_VALUE - 1;
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(5, 8, high);
        assertThat(bitmap.getLongCardinality()).isEqualTo(3);

        if (andNot) {
            bitmap.andNot(RoaringNavigableMap64.bitmapOf(8));
        } else {
            bitmap.and(RoaringNavigableMap64.bitmapOf(5, high));
        }

        List<Range> expected = Arrays.asList(new Range(5, 5), new Range(high, high));
        assertThat(bitmap.toRangeList()).containsExactlyElementsOf(expected);
        assertThat(bitmap.getLongCardinality()).isEqualTo(2);
        assertThat(bitmap.tryToRangeList(2, Collections.singletonList(new Range(0, high))))
                .contains(expected);
    }

    @Test
    public void testStaticUnionWithUnsignedTail() {
        RoaringNavigableMap64 bitmap =
                RoaringNavigableMap64.or(
                        RoaringNavigableMap64.bitmapOf(5),
                        RoaringNavigableMap64.bitmapOf(Long.MIN_VALUE));

        assertThat(bitmap.getLongCardinality()).isEqualTo(2);
        assertThat(bitmap.toArray(2)).containsExactly(5L, Long.MIN_VALUE);
    }

    @Test
    public void testAddAfterIntersectionRemovesLastAddedBucket() {
        long high = 1L << 32;
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(5, high);

        bitmap.and(RoaringNavigableMap64.bitmapOf(5));
        bitmap.add(high + 1);

        assertThat(bitmap.toArray(2)).containsExactly(5L, high + 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testIntersectingRangesStopsAtLastNonNegativeCandidate(boolean unsignedTail) {
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(50);
        if (unsignedTail) {
            bitmap.add(Long.MIN_VALUE);
        }
        assertThat(bitmap.intersectingRanges(rangesWithReadLimit(262_144)).stream().toArray())
                .containsExactly(0);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    public void testScopedProbeStopsAtLastNonNegativeCandidate(
            boolean unsignedTail, boolean sequential) {
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(50);
        if (unsignedTail) {
            bitmap.add(Long.MIN_VALUE);
        }
        List<Range> scopes = rangesWithReadLimit(100_000);
        List<Range> input =
                sequential
                        ? new AbstractList<Range>() {
                            @Override
                            public Range get(int index) {
                                return scopes.get(index);
                            }

                            @Override
                            public int size() {
                                return scopes.size();
                            }
                        }
                        : scopes;
        assertThat(bitmap.tryToRangeList(1, input))
                .contains(Collections.singletonList(new Range(50, 50)));
    }

    private static List<Range> rangesWithReadLimit(int size) {
        class OrderedRanges extends AbstractList<Range> implements RandomAccess {
            private int reads;

            @Override
            public Range get(int index) {
                if (++reads > 64) {
                    throw new AssertionError(
                            "Probe continued past the last non-negative candidate");
                }
                long start = 200L * index;
                return new Range(start, start + 99);
            }

            @Override
            public int size() {
                return size;
            }
        }

        return new OrderedRanges();
    }

    @ParameterizedTest
    @ValueSource(longs = {1L << 24, 1L << 25})
    public void testGroupedAndScopedProbesSkipEmptyBatchWindows(long width) {
        int groups = 64;
        long stride = width + 16;
        class GapRanges extends AbstractList<Range> implements RandomAccess {
            private int emptyGroupReads;

            @Override
            public Range get(int index) {
                if (index > 0 && index < groups - 1) {
                    emptyGroupReads++;
                }
                long start = index * stride;
                return new Range(start, start + width - 1);
            }

            @Override
            public int size() {
                return groups;
            }
        }

        GapRanges scopes = new GapRanges();
        long last = (groups - 1) * stride + 50;
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(50, last);

        assertThat(bitmap.intersectingRanges(scopes).stream().toArray())
                .containsExactly(0, groups - 1);
        assertThat(scopes.emptyGroupReads).isLessThanOrEqualTo(groups + 8);

        scopes.emptyGroupReads = 0;
        assertThat(bitmap.tryToRangeList(2, scopes))
                .contains(Arrays.asList(new Range(50, 50), new Range(last, last)));
        // After the first two empty windows, only read the bounds of the skipped scopes.
        assertThat(scopes.emptyGroupReads).isLessThanOrEqualTo(groups + 8);
    }

    @Test
    public void testScopedProbeDoesNotSeekAcrossIsolatedEmptyWindows() throws Exception {
        Roaring64NavigableMap source =
                new Roaring64NavigableMap() {
                    @Override
                    public long rankLong(long value) {
                        throw new AssertionError("Scoped probe sought across an isolated window");
                    }
                };
        List<Range> scopes = new ArrayList<>();
        List<Range> expected = new ArrayList<>();
        long stride = 1L << 24;
        for (int i = 0; i < 8; i++) {
            long start = i * stride;
            scopes.add(new Range(start, start + 99));
            if (i % 2 == 0) {
                long row = start + 50;
                source.add(row);
                expected.add(new Range(row, row));
            }
        }

        RoaringNavigableMap64 bitmap = wrap(source);
        assertThat(bitmap.tryToRangeList(expected.size(), scopes)).contains(expected);
        assertThat(bitmap.tryToRangeList(expected.size() - 1, scopes)).isEmpty();
    }

    @Test
    public void testIntersectingRangesWithOverlappingEndpoints() {
        List<Range> scopes =
                Arrays.asList(
                        new Range(0, 9),
                        new Range(5, 30),
                        new Range(6, 7),
                        new Range(11, 14),
                        new Range(12, 20),
                        new Range(12, 12),
                        new Range(20, 30),
                        new Range(21, 29),
                        new Range(30, Long.MAX_VALUE),
                        new Range(Long.MAX_VALUE, Long.MAX_VALUE));
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(10, 15, 30);

        assertThat(bitmap.intersectingRanges(scopes).stream().toArray())
                .containsExactly(1, 4, 6, 8);
        assertThat(
                        RoaringNavigableMap64.bitmapOf(Long.MIN_VALUE).intersectingRanges(scopes)
                                .stream()
                                .toArray())
                .isEmpty();
    }

    @Test
    public void testBoundedRangesWithinScope() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (long id = 0; id < 256; id += 2) {
            bitmap.add(id);
        }
        long high = 1L << 32;
        bitmap.addRange(new Range(high - 20_000, high + 20_000));

        List<Range> scope =
                Arrays.asList(
                        new Range(3, 6),
                        new Range(high - 5_000, high + 10),
                        new Range(high + 20, high + 30_000));
        List<Range> expected =
                Arrays.asList(
                        new Range(4, 4),
                        new Range(6, 6),
                        new Range(high - 5_000, high + 10),
                        new Range(high + 20, high + 20_000));
        assertThat(bitmap.tryToRangeList(4, scope)).contains(expected);
        assertThat(bitmap.tryToRangeList(3, scope)).isEmpty();
    }

    @Test
    public void testRangeLimitStopsAtStartOfOverflowingRun() throws Exception {
        int maxRanges = 128;
        Roaring64NavigableMap source = limitedIterator(4 * maxRanges);
        for (int i = 0; i < maxRanges; i++) {
            source.add(2L * i);
        }
        long denseEnd = 1_000_000_000L;
        source.addRange(2L * maxRanges, denseEnd + 1);
        for (int i = 1; i <= 256; i++) {
            source.add(denseEnd + 2L * i);
        }

        assertThat(wrap(source).tryToRangeList(maxRanges)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    public void testBoundedProbeSkipsLongRunBeforeFragmentedSuffix(int longRuns) throws Exception {
        Roaring64NavigableMap source = limitedIterator(20_000);
        List<Range> expected = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            long rowId = 2L * i;
            source.add(rowId);
            expected.add(new Range(rowId, rowId));
        }
        long runSize = 1_000_000_000L;
        long denseEnd = 0;
        for (int i = 0; i < longRuns; i++) {
            long start = 256 + i * (runSize + 100);
            denseEnd = start + runSize - 1;
            source.addRange(start, denseEnd + 1);
            expected.add(new Range(start, denseEnd));
        }
        for (int i = 1; i <= 256; i++) {
            long rowId = denseEnd + 2L * i;
            source.add(rowId);
            expected.add(new Range(rowId, rowId));
        }

        RoaringNavigableMap64 bitmap = wrap(source);
        assertThat(bitmap.tryToRangeList(expected.size())).contains(expected);
        assertThat(bitmap.tryToRangeList(129)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"1, 2, 1", "3, 2, 1", "3, 1024, 1", "3, 595, 2", "3, 4098, 4097", "1, 262200, 1"})
    public void testBoundedProbeKeepsFragmentedSuffixLocal(
            int longRuns, int tailStride, int tailRunSize) throws Exception {
        Roaring64NavigableMap source =
                new Roaring64NavigableMap() {
                    private int selections;

                    @Override
                    public long select(long index) {
                        if (++selections > 2_048) {
                            throw new AssertionError(
                                    "Probe selected individual rows in the full bitmap");
                        }
                        return super.select(index);
                    }
                };
        List<Range> expected = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            long rowId = 2L * i;
            source.add(rowId);
            expected.add(new Range(rowId, rowId));
        }
        long runSize = 110_000_000L;
        long denseEnd = 0;
        for (int i = 0; i < longRuns; i++) {
            long start = 256 + i * (runSize + 100);
            denseEnd = start + runSize - 1;
            source.addRange(start, denseEnd + 1);
            expected.add(new Range(start, denseEnd));
        }
        for (int i = 1; i <= 30_000; i++) {
            long rowId = denseEnd + (long) tailStride * i;
            source.addRange(rowId, rowId + tailRunSize);
            expected.add(new Range(rowId, rowId + tailRunSize - 1));
        }
        long tail = denseEnd + 30_000L * tailStride + tailRunSize + 1;
        source.addRange(tail, tail + 257);
        expected.add(new Range(tail, tail + 256));

        assertThat(wrap(source).tryToRangeList(expected.size())).contains(expected);
    }

    @Test
    public void testBoundedProbeClipsSparseClusterAtContainerBoundary() throws Exception {
        List<Long> boundaryProbes = new ArrayList<>();
        Roaring64NavigableMap source =
                new Roaring64NavigableMap() {
                    @Override
                    public boolean contains(long value) {
                        boundaryProbes.add(value);
                        return super.contains(value);
                    }
                };
        List<Range> expected = new ArrayList<>();
        source.addRange(0, 5_000);
        expected.add(new Range(0, 4_999));

        long containerEnd = (1L << 25) + (1L << 16) - 1;
        long clusterStart = containerEnd - 32;
        for (int i = 0; i < 8; i++) {
            long row = clusterStart + 2L * i;
            source.add(row);
            expected.add(new Range(row, row));
        }
        // The last run crosses the smaller window and must stay contiguous.
        source.addRange(containerEnd - 4, containerEnd + 9);
        expected.add(new Range(containerEnd - 4, containerEnd + 8));
        long distantMatch = 1L << 26;
        source.add(distantMatch);
        expected.add(new Range(distantMatch, distantMatch));

        RoaringNavigableMap64 bitmap = wrap(source);
        assertThat(bitmap.tryToRangeList(expected.size())).contains(expected);
        assertThat(boundaryProbes)
                .contains(containerEnd)
                .doesNotContain(clusterStart + (1L << 24) - 1);
        assertThat(bitmap.tryToRangeList(expected.size() - 1)).isEmpty();
    }

    @Test
    public void testBoundedProbeCollectsFittingRangesInOnePass() throws Exception {
        Roaring64NavigableMap source = limitedIterator(85_000);
        List<Range> expected = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            long start = 4_002L * i;
            source.addRange(start, start + 4_000);
            expected.add(new Range(start, start + 3_999));
        }
        long tail = 20 * 4_002L;
        for (int i = 0; i < 256; i++) {
            long rowId = tail + 2L * i;
            source.add(rowId);
            expected.add(new Range(rowId, rowId));
        }

        assertThat(wrap(source).tryToRangeList(expected.size())).contains(expected);
    }

    private static Roaring64NavigableMap limitedIterator(int maxRowsVisited) {
        return new Roaring64NavigableMap() {
            private int rowsVisited;

            @Override
            public LongIterator getLongIterator() {
                LongIterator iterator = super.getLongIterator();
                return new LongIterator() {
                    @Override
                    public LongIterator clone() {
                        return iterator.clone();
                    }

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public long next() {
                        if (++rowsVisited > maxRowsVisited) {
                            throw new AssertionError("Probe iterated through a long run");
                        }
                        return iterator.next();
                    }
                };
            }
        };
    }

    @Test
    public void testBoundedProbeBatchesNearbyScopes() throws Exception {
        AtomicInteger rankLookups = new AtomicInteger();
        Roaring64NavigableMap source =
                new Roaring64NavigableMap() {
                    @Override
                    public long rankLong(long value) {
                        rankLookups.incrementAndGet();
                        return super.rankLong(value);
                    }
                };
        long start = 1L << 32;
        source.addRange(0, 1_000_000L);
        source.addRange(start + 2, start + 5);
        source.add(start + 21);
        List<Range> scope =
                Arrays.asList(
                        new Range(start, start + 10),
                        new Range(start + 12, start + 19),
                        new Range(start + 20, start + 30));
        RoaringNavigableMap64 bitmap = wrap(source);
        List<Range> expected =
                Arrays.asList(new Range(start + 2, start + 4), new Range(start + 21, start + 21));
        assertThat(bitmap.tryToRangeList(2, scope)).contains(expected);
        assertThat(bitmap.tryToRangeList(1, scope)).isEmpty();
        assertThat(rankLookups).hasValue(0);
        assertThat(bitmap.intersectingRanges(scope).stream().toArray()).containsExactly(0, 2);
        assertThat(rankLookups).hasValue(0);

        List<Range> singleScope = Collections.singletonList(new Range(start, start + (1L << 22)));
        assertThat(bitmap.tryToRangeList(2, singleScope)).contains(expected);
        assertThat(bitmap.intersectingRanges(singleScope).stream().toArray()).containsExactly(0);

        long outsideFirstClip = start + (1L << 24) + 2;
        source.add(outsideFirstClip);
        expected = new ArrayList<>(expected);
        expected.add(new Range(outsideFirstClip, outsideFirstClip));
        List<Range> wideScopes =
                Arrays.asList(
                        new Range(start, outsideFirstClip + 10),
                        new Range(start + (1L << 25), start + 3 * (1L << 24)));
        assertThat(bitmap.tryToRangeList(3, wideScopes)).contains(expected);
        assertThat(bitmap.tryToRangeList(2, wideScopes)).isEmpty();
        assertThat(bitmap.intersectingRanges(wideScopes).stream().toArray()).containsExactly(0);
    }

    @Test
    public void testBoundedProbePreservesRunAcrossBatchBoundary() {
        long start = Long.MAX_VALUE - (1L << 25);
        long boundary = start + (1L << 24);
        Range run = new Range(boundary - 5, boundary + 5);
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(run);
        List<Range> scope = Collections.singletonList(new Range(start, Long.MAX_VALUE));

        assertThat(bitmap.tryToRangeList(1, scope)).contains(Collections.singletonList(run));
        assertThat(bitmap.intersectingRanges(scope).stream().toArray()).containsExactly(0);
    }

    @Test
    public void testBoundedProbeSpansLargeGapInWideScope() {
        Range first = new Range(128, 18_000_127);
        Range last = new Range(1_000_000_000_000L, 1_000_017_999_999L);
        List<Range> scope = Collections.singletonList(new Range(first.from, last.to));
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(first);
        bitmap.addRange(last);
        bitmap.add(last.to + 2);

        assertThat(bitmap.tryToRangeList(2, scope)).contains(Arrays.asList(first, last));
        assertThat(bitmap.tryToRangeList(1, scope)).isEmpty();
    }

    @Test
    public void testRangeLimitStopsBeforeVisitingNextNonemptyScope() throws Exception {
        Roaring64NavigableMap source =
                new Roaring64NavigableMap() {
                    @Override
                    public long select(long index) {
                        if (index >= 2) {
                            throw new AssertionError(
                                    "Probe entered the next scope after the limit");
                        }
                        return super.select(index);
                    }
                };
        source.add(0);
        source.add(2);
        long nextScopeStart = 1L << 32;
        source.addRange(nextScopeStart, nextScopeStart + 90);
        RoaringNavigableMap64 bitmap = wrap(source);
        Range first = new Range(0, 2);
        Range empty = new Range(1L << 31, (1L << 31) + 1);
        assertThat(bitmap.tryToRangeList(2, Arrays.asList(first, empty)))
                .contains(Arrays.asList(new Range(0, 0), new Range(2, 2)));
        assertThat(
                        bitmap.tryToRangeList(
                                2,
                                Arrays.asList(
                                        first,
                                        empty,
                                        new Range(nextScopeStart, nextScopeStart + (1L << 25)))))
                .isEmpty();
    }

    @Test
    public void testScopedRangeConversionSkipsExcludedRows() throws Exception {
        Roaring64NavigableMap source =
                new Roaring64NavigableMap() {
                    @Override
                    public LongIterator getLongIterator() {
                        throw new AssertionError("Scoped lookup iterated the original bitmap");
                    }
                };
        for (int i = 0; i < 128; i++) {
            source.add(2L * i);
        }
        long tail = 1_000_000;
        source.addRange(256, tail);
        for (int i = 0; i < 256; i++) {
            source.add(tail + 1 + 2L * i);
        }
        RoaringNavigableMap64 bitmap = wrap(source);
        List<Range> scope = Collections.singletonList(new Range(tail + 508, tail + 511));
        List<Range> expected =
                Arrays.asList(new Range(tail + 509, tail + 509), new Range(tail + 511, tail + 511));

        assertThat(bitmap.tryToRangeList(2, scope)).contains(expected);
        assertThat(bitmap.tryToRangeList(1, scope)).isEmpty();
    }

    private static RoaringNavigableMap64 wrap(Roaring64NavigableMap source) throws Exception {
        Constructor<RoaringNavigableMap64> constructor =
                RoaringNavigableMap64.class.getDeclaredConstructor(Roaring64NavigableMap.class);
        constructor.setAccessible(true);
        return constructor.newInstance(source);
    }

    @Test
    public void testSelectPathForLargeRangesWithIsolatedGaps() throws Exception {
        Method useSelect =
                RoaringNavigableMap64.class.getDeclaredMethod("shouldUseSelectRanges", long.class);
        useSelect.setAccessible(true);
        for (long gap : new long[] {9, 600_032, 1_199_970}) {
            for (int width : new int[] {1, 17}) {
                List<Range> expected =
                        Arrays.asList(new Range(0, gap - 1), new Range(gap + width, 1_199_999));
                RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
                expected.forEach(bitmap::addRange);
                // Verify the algorithmic bound without a timing assertion: a hole within a
                // sample must not cause us to walk a million values for just two ranges.
                assertThat((boolean) useSelect.invoke(bitmap, bitmap.getLongCardinality()))
                        .isTrue();
                assertThat(bitmap.toRangeList()).isEqualTo(expected);
                assertThat(bitmap.tryToRangeList(2)).contains(expected);
                assertThat(bitmap.tryToRangeList(1)).isEmpty();
            }
        }
    }

    @Test
    public void testToRangeListDoesNotMergeUnsignedWrapAround() {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.add(Long.MAX_VALUE);
        bitmap.add(Long.MIN_VALUE);

        assertThat(bitmap.toRangeList())
                .containsExactly(
                        new Range(Long.MAX_VALUE, Long.MAX_VALUE),
                        new Range(Long.MIN_VALUE, Long.MIN_VALUE));
    }
}
