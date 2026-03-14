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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Test for {@link Range}. */
public class RangeTest {

    @Test
    public void testExcludeBasic() {
        // [0, 10000] exclude [1000,2000],[3000,4000],[5000,6000]
        // Expected: [0, 999],[2001,2999],[4001,4999],[6001, 10000]
        Range range = new Range(0, 10000);
        List<Range> exclusions =
                Arrays.asList(new Range(1000, 2000), new Range(3000, 4000), new Range(5000, 6000));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result)
                .containsExactly(
                        new Range(0, 999),
                        new Range(2001, 2999),
                        new Range(4001, 4999),
                        new Range(6001, 10000));
    }

    @Test
    public void testExcludeUnsortedRanges() {
        // Same as basic but with unsorted exclusions
        Range range = new Range(0, 10000);
        List<Range> exclusions =
                Arrays.asList(new Range(5000, 6000), new Range(1000, 2000), new Range(3000, 4000));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result)
                .containsExactly(
                        new Range(0, 999),
                        new Range(2001, 2999),
                        new Range(4001, 4999),
                        new Range(6001, 10000));
    }

    @Test
    public void testExcludeEmptyExclusions() {
        Range range = new Range(100, 200);
        List<Range> result = range.exclude(Collections.emptyList());

        Assertions.assertThat(result).containsExactly(new Range(100, 200));
    }

    @Test
    public void testExcludeNoIntersection() {
        Range range = new Range(100, 200);
        List<Range> exclusions = Arrays.asList(new Range(300, 400), new Range(500, 600));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(100, 200));
    }

    @Test
    public void testExcludeAtStart() {
        // Exclusion at the start of the range
        Range range = new Range(0, 100);
        List<Range> exclusions = Collections.singletonList(new Range(0, 10));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(11, 100));
    }

    @Test
    public void testExcludeAtEnd() {
        // Exclusion at the end of the range
        Range range = new Range(0, 100);
        List<Range> exclusions = Collections.singletonList(new Range(90, 100));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(0, 89));
    }

    @Test
    public void testExcludeExtendsBeyondRange() {
        // Exclusion extends past the end of the range
        Range range = new Range(100, 200);
        List<Range> exclusions = Collections.singletonList(new Range(150, 300));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(100, 149));
    }

    @Test
    public void testExcludeStartsBeforeRange() {
        // Exclusion starts before the range
        Range range = new Range(100, 200);
        List<Range> exclusions = Collections.singletonList(new Range(50, 150));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(151, 200));
    }

    @Test
    public void testExcludeOverlappingExclusions() {
        // Overlapping exclusion ranges
        Range range = new Range(0, 100);
        List<Range> exclusions = Arrays.asList(new Range(20, 50), new Range(40, 70));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(0, 19), new Range(71, 100));
    }

    @Test
    public void testExcludeCompleteOverlap() {
        // Exclusion completely covers the range
        Range range = new Range(50, 60);
        List<Range> exclusions = Collections.singletonList(new Range(0, 100));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testExcludeExactMatch() {
        // Exclusion exactly matches the range
        Range range = new Range(100, 200);
        List<Range> exclusions = Collections.singletonList(new Range(100, 200));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testExcludeInMiddle() {
        // Single exclusion in the middle
        Range range = new Range(0, 100);
        List<Range> exclusions = Collections.singletonList(new Range(40, 60));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(0, 39), new Range(61, 100));
    }

    @Test
    public void testExcludeAdjacentRanges() {
        // Adjacent exclusion ranges
        Range range = new Range(0, 100);
        List<Range> exclusions = Arrays.asList(new Range(20, 30), new Range(31, 40));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(0, 19), new Range(41, 100));
    }

    @Test
    public void testExcludeMultipleSmallRanges() {
        // Many small exclusions
        Range range = new Range(0, 20);
        List<Range> exclusions =
                Arrays.asList(
                        new Range(2, 3), new Range(6, 7), new Range(10, 11), new Range(14, 15));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result)
                .containsExactly(
                        new Range(0, 1),
                        new Range(4, 5),
                        new Range(8, 9),
                        new Range(12, 13),
                        new Range(16, 20));
    }

    @Test
    public void testExcludeSinglePointRange() {
        // Range is a single point
        Range range = new Range(50, 50);
        List<Range> exclusions = Collections.singletonList(new Range(50, 50));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testExcludeSinglePointExclusion() {
        // Single point exclusion
        Range range = new Range(0, 100);
        List<Range> exclusions = Collections.singletonList(new Range(50, 50));

        List<Range> result = range.exclude(exclusions);

        Assertions.assertThat(result).containsExactly(new Range(0, 49), new Range(51, 100));
    }

    @Test
    public void testSortAndMergeOverlapBasic() {
        // [0,10] and [5,15] should merge to [0,15]
        List<Range> ranges = Arrays.asList(new Range(0, 10), new Range(5, 15));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 15));
    }

    @Test
    public void testSortAndMergeOverlapNoOverlap() {
        // [0,10] and [11,20] should NOT merge (adjacent but no overlap)
        List<Range> ranges = Arrays.asList(new Range(0, 10), new Range(11, 20));
        List<Range> result = Range.sortAndMergeOverlap(ranges);
        Assertions.assertThat(result).containsExactly(new Range(0, 10), new Range(11, 20));
        result = Range.sortAndMergeOverlap(ranges, true);
        Assertions.assertThat(result).containsExactly(new Range(0, 20));
    }

    @Test
    public void testSortAndMergeOverlapMultiple() {
        // [0,10], [5,15], [12,20] -> [0,20] (all overlap and merge)
        List<Range> ranges = Arrays.asList(new Range(0, 10), new Range(5, 15), new Range(12, 20));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 20));
    }

    @Test
    public void testSortAndMergeOverlapMixed() {
        // [0,10], [5,15], [20,30], [25,35] -> [0,15], [20,35]
        List<Range> ranges =
                Arrays.asList(
                        new Range(0, 10), new Range(5, 15), new Range(20, 30), new Range(25, 35));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 15), new Range(20, 35));
    }

    @Test
    public void testSortAndMergeOverlapUnsorted() {
        // Unsorted: [20,30], [0,10], [5,15] -> [0,15], [20,30]
        List<Range> ranges = Arrays.asList(new Range(20, 30), new Range(0, 10), new Range(5, 15));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 15), new Range(20, 30));
    }

    @Test
    public void testSortAndMergeOverlapContained() {
        // [0,20] contains [5,10] -> [0,20]
        List<Range> ranges = Arrays.asList(new Range(0, 20), new Range(5, 10));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 20));
    }

    @Test
    public void testSortAndMergeOverlapSingle() {
        List<Range> ranges = Collections.singletonList(new Range(0, 10));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 10));
    }

    @Test
    public void testSortAndMergeOverlapIdentical() {
        // Two identical ranges should merge to one
        List<Range> ranges = Arrays.asList(new Range(0, 10), new Range(0, 10));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 10));
    }

    @Test
    public void testSortAndMergeOverlapTouchingExactly() {
        // [0,10] and [10,20] overlap at point 10, should merge
        List<Range> ranges = Arrays.asList(new Range(0, 10), new Range(10, 20));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result).containsExactly(new Range(0, 20));
    }

    @Test
    public void testSortAndMergeOverlapComplex() {
        // Complex scenario with various overlaps
        // [0,5], [3,8], [10,15], [20,25], [22,28], [30,35]
        // -> [0,8], [10,15], [20,28], [30,35]
        List<Range> ranges =
                Arrays.asList(
                        new Range(0, 5),
                        new Range(3, 8),
                        new Range(10, 15),
                        new Range(20, 25),
                        new Range(22, 28),
                        new Range(30, 35));

        List<Range> result = Range.sortAndMergeOverlap(ranges);

        Assertions.assertThat(result)
                .containsExactly(
                        new Range(0, 8), new Range(10, 15), new Range(20, 28), new Range(30, 35));
    }

    @Test
    public void testAndBasic() {
        // left=[0,10],[20,30] and right=[5,15],[25,35] -> [5,10],[25,30]
        List<Range> left = Arrays.asList(new Range(0, 10), new Range(20, 30));
        List<Range> right = Arrays.asList(new Range(5, 15), new Range(25, 35));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).containsExactly(new Range(5, 10), new Range(25, 30));
    }

    @Test
    public void testAndNoIntersection() {
        // left=[0,10] and right=[20,30] -> empty
        List<Range> left = Collections.singletonList(new Range(0, 10));
        List<Range> right = Collections.singletonList(new Range(20, 30));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testAndFullOverlap() {
        // left=[0,10] and right=[0,10] -> [0,10]
        List<Range> left = Collections.singletonList(new Range(0, 10));
        List<Range> right = Collections.singletonList(new Range(0, 10));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).containsExactly(new Range(0, 10));
    }

    @Test
    public void testAndPartialOverlap() {
        // left=[0,10] and right=[5,15] -> [5,10]
        List<Range> left = Collections.singletonList(new Range(0, 10));
        List<Range> right = Collections.singletonList(new Range(5, 15));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).containsExactly(new Range(5, 10));
    }

    @Test
    public void testAndContained() {
        // left=[0,20] and right=[5,10] -> [5,10]
        List<Range> left = Collections.singletonList(new Range(0, 20));
        List<Range> right = Collections.singletonList(new Range(5, 10));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).containsExactly(new Range(5, 10));
    }

    @Test
    public void testAndMultipleRanges() {
        // left=[0,10],[20,30],[40,50] and right=[5,25],[35,45] -> [5,10],[20,25],[40,45]
        List<Range> left = Arrays.asList(new Range(0, 10), new Range(20, 30), new Range(40, 50));
        List<Range> right = Arrays.asList(new Range(5, 25), new Range(35, 45));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result)
                .containsExactly(new Range(5, 10), new Range(20, 25), new Range(40, 45));
    }

    @Test
    public void testAndEmptyLeft() {
        List<Range> left = Collections.emptyList();
        List<Range> right = Collections.singletonList(new Range(0, 10));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testAndEmptyRight() {
        List<Range> left = Collections.singletonList(new Range(0, 10));
        List<Range> right = Collections.emptyList();

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testAndNullLeft() {
        List<Range> right = Collections.singletonList(new Range(0, 10));

        List<Range> result = Range.and(null, right);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testAndNullRight() {
        List<Range> left = Collections.singletonList(new Range(0, 10));

        List<Range> result = Range.and(left, null);

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testAndTouchingAtBoundary() {
        // left=[0,10] and right=[10,20] -> [10,10] (intersection at single point)
        List<Range> left = Collections.singletonList(new Range(0, 10));
        List<Range> right = Collections.singletonList(new Range(10, 20));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result).containsExactly(new Range(10, 10));
    }

    @Test
    public void testAndComplex() {
        List<Range> left =
                Arrays.asList(
                        new Range(0, 5), new Range(10, 15), new Range(20, 25), new Range(30, 35));
        List<Range> right = Arrays.asList(new Range(3, 12), new Range(18, 28), new Range(32, 40));

        List<Range> result = Range.and(left, right);

        Assertions.assertThat(result)
                .containsExactly(
                        new Range(3, 5), new Range(10, 12), new Range(20, 25), new Range(32, 35));
    }
}
