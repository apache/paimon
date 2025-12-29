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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/** Range represents from (inclusive) and to (inclusive). */
public class Range implements Serializable {

    public final long from;
    public final long to;

    // Creates a range of [from, to] (from and to are inclusive; empty ranges are not valid)
    public Range(long from, long to) {
        assert from <= to;
        this.from = from;
        this.to = to;
    }

    public long count() {
        return to - from + 1;
    }

    public Range addOffset(long offset) {
        return new Range(from + offset, to + offset);
    }

    public boolean isBefore(Range other) {
        return to < other.from;
    }

    public boolean isAfter(Range other) {
        return from > other.to;
    }

    public List<Long> toListLong() {
        List<Long> longs = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            longs.add(i);
        }
        return longs;
    }

    /**
     * Excludes the given ranges from this range and returns the remaining ranges.
     *
     * <p>For example, if this range is [0, 10000] and ranges to exclude are [1000, 2000], [3000,
     * 4000], [5000, 6000], then the result is [0, 999], [2001, 2999], [4001, 4999], [6001, 10000].
     *
     * @param ranges the ranges to exclude (can be unsorted and overlapping)
     * @return the remaining ranges after exclusion
     */
    public List<Range> exclude(List<Range> ranges) {
        if (ranges.isEmpty()) {
            return Collections.singletonList(this);
        }

        // Sort ranges by from
        List<Range> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(a -> a.from));

        List<Range> result = new ArrayList<>();
        long current = this.from;

        for (Range exclude : sorted) {
            // Compute intersection with the current range
            Range intersect = Range.intersection(new Range(current, this.to), exclude);
            if (intersect == null) {
                continue;
            }
            // Add the part before the intersection (if any)
            if (current < intersect.from) {
                result.add(new Range(current, intersect.from - 1));
            }
            // Move current position past the intersection
            current = intersect.to + 1;
            if (current > this.to) {
                break;
            }
        }

        // Add the remaining part after all exclusions (if any)
        if (current <= this.to) {
            result.add(new Range(current, this.to));
        }

        return result;
    }

    public static List<Range> sortAndMergeOverlap(List<Range> ranges) {
        return sortAndMergeOverlap(ranges, false);
    }

    public static List<Range> sortAndMergeOverlap(List<Range> ranges, boolean adjacent) {
        if (ranges == null || ranges.isEmpty()) {
            return Collections.emptyList();
        }

        if (ranges.size() == 1) {
            return new ArrayList<>(ranges);
        }

        // Sort ranges by from
        List<Range> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(r -> r.from));

        List<Range> result = new ArrayList<>();
        Range current = sorted.get(0);

        for (int i = 1; i < sorted.size(); i++) {
            Range next = sorted.get(i);
            // Check if current and next overlap (not just adjacent)
            if (current.to + (adjacent ? 1 : 0) >= next.from) {
                // Merge: extend current range
                current = new Range(current.from, Math.max(current.to, next.to));
            } else {
                // No overlap: add current to result and move to next
                result.add(current);
                current = next;
            }
        }
        // Add the last range
        result.add(current);

        return result;
    }

    public static List<Range> mergeSortedAsPossible(List<Range> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return Collections.emptyList();
        }

        if (ranges.size() == 1) {
            return new ArrayList<>(ranges);
        }

        List<Range> result = new ArrayList<>();
        Range current = ranges.get(0);

        for (int i = 1; i < ranges.size(); i++) {
            Range next = ranges.get(i);
            // Try to merge current and next
            Range merged = Range.union(current, next);
            if (merged != null) {
                // Merged successfully
                current = merged;
            } else {
                // Cannot merge: add current to result and move to next
                result.add(current);
                current = next;
            }
        }
        // Add the last range
        result.add(current);

        return result;
    }

    public static List<Range> toRanges(Iterable<Long> ids) {
        List<Range> ranges = new ArrayList<>();
        Iterator<Long> iterator = ids.iterator();

        if (!iterator.hasNext()) {
            return ranges;
        }

        long rangeStart = iterator.next();
        long rangeEnd = rangeStart;

        while (iterator.hasNext()) {
            long current = iterator.next();
            if (current != rangeEnd + 1) {
                // Save the current range and start a new one
                ranges.add(new Range(rangeStart, rangeEnd));
                rangeStart = current;
            }
            rangeEnd = current;
        }
        // Add the last range
        ranges.add(new Range(rangeStart, rangeEnd));

        return ranges;
    }

    /**
     * Computes the intersection of two lists of ranges.
     *
     * <p>Assumes that both left and right are sorted and merged already (no overlaps within each
     * list).
     *
     * <p>For example, left=[0,10],[20,30] and right=[5,15],[25,35] will return [5,10],[25,30].
     *
     * @param left the first list of ranges (must be sorted and merged)
     * @param right the second list of ranges (must be sorted and merged)
     * @return the intersection of the two lists
     */
    public static List<Range> and(List<Range> left, List<Range> right) {
        if (left == null || right == null || left.isEmpty() || right.isEmpty()) {
            return Collections.emptyList();
        }

        List<Range> result = new ArrayList<>();
        int i = 0;
        int j = 0;

        while (i < left.size() && j < right.size()) {
            Range l = left.get(i);
            Range r = right.get(j);

            // Compute intersection of current ranges
            Range intersect = Range.intersection(l, r);
            if (intersect != null) {
                result.add(intersect);
            }

            // Advance the pointer of the range that ends earlier
            if (l.to <= r.to) {
                i++;
            } else {
                j++;
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Range range = (Range) o;
        return from == range.from && to == range.to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "[" + from + ", " + to + ']';
    }

    // Returns the union of the two ranges or null if there are elements between them.
    public static Range union(Range left, Range right) {
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
    public static Range intersection(Range left, Range right) {
        if (left.from <= right.from) {
            if (left.to >= right.from) {
                return new Range(right.from, Math.min(left.to, right.to));
            }
        } else if (right.to >= left.from) {
            return new Range(left.from, Math.min(left.to, right.to));
        }
        return null;
    }

    public static boolean intersect(long start1, long end1, long start2, long end2) {
        long intersectionStart = Math.max(start1, start2);
        long intersectionEnd = Math.min(end1, end2);
        return intersectionStart <= intersectionEnd;
    }
}
