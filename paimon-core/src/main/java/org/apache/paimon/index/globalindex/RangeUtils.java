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

package org.apache.paimon.index.globalindex;

import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Utility class for performing operations on Range iterators. */
public class RangeUtils {

    /**
     * Computes the intersection of two sets of ranges. Returns ranges that overlap in both sets.
     *
     * @param it1 iterator of the first set of ranges
     * @param it2 iterator of the second set of ranges
     * @return set of ranges representing the intersection
     */
    public static Set<Range> and(Iterator<Range> it1, Iterator<Range> it2) {
        Set<Range> result = new HashSet<>();

        List<Range> ranges1 = toSortedList(it1);
        List<Range> ranges2 = toSortedList(it2);

        if (ranges1.isEmpty() || ranges2.isEmpty()) {
            return result;
        }

        int i = 0, j = 0;
        while (i < ranges1.size() && j < ranges2.size()) {
            Range r1 = ranges1.get(i);
            Range r2 = ranges2.get(j);

            // Check if ranges overlap
            if (r1.overlaps(r2)) {
                // Calculate the intersection
                long start = Math.max(r1.getStart(), r2.getStart());
                long end = Math.min(r1.getEnd(), r2.getEnd());
                result.add(new Range(start, end));

                // Move to the next range that ends first
                if (r1.getEnd() <= r2.getEnd()) {
                    i++;
                } else {
                    j++;
                }
            } else if (r1.getEnd() <= r2.getStart()) {
                // r1 is completely before r2, move to next r1
                i++;
            } else {
                // r2 is completely before r1, move to next r2
                j++;
            }
        }

        return result;
    }

    /**
     * Computes the union of two sets of ranges. Merges overlapping or adjacent ranges.
     *
     * @param it1 iterator of the first set of ranges
     * @param it2 iterator of the second set of ranges
     * @return set of ranges representing the union
     */
    public static Set<Range> or(Iterator<Range> it1, Iterator<Range> it2) {
        Set<Range> result = new HashSet<>();

        List<Range> allRanges = new ArrayList<>();
        it1.forEachRemaining(allRanges::add);
        it2.forEachRemaining(allRanges::add);

        if (allRanges.isEmpty()) {
            return result;
        }

        // Sort ranges by start position
        allRanges.sort(Comparator.comparingLong(Range::getStart));

        // Merge overlapping or adjacent ranges
        Range current = allRanges.get(0);
        for (int i = 1; i < allRanges.size(); i++) {
            Range next = allRanges.get(i);

            // Check if current and next overlap or are adjacent
            if (current.getEnd() >= next.getStart()) {
                // Merge ranges
                current = new Range(current.getStart(), Math.max(current.getEnd(), next.getEnd()));
            } else {
                // No overlap, add current to result and move to next
                result.add(current);
                current = next;
            }
        }

        // Add the last range
        result.add(current);

        return result;
    }

    public static boolean intersect(long start1, long end1, long start2, long end2) {
        long intersectionStart = Math.max(start1, start2);
        long intersectionEnd = Math.min(end1, end2);
        return intersectionStart < intersectionEnd;
    }

    /**
     * Converts an iterator of ranges to a sorted list.
     *
     * @param iterator iterator of ranges
     * @return sorted list of ranges
     */
    private static List<Range> toSortedList(Iterator<Range> iterator) {
        List<Range> list = new ArrayList<>();
        iterator.forEachRemaining(list::add);
        list.sort(Comparator.comparingLong(Range::getStart));
        return list;
    }
}
