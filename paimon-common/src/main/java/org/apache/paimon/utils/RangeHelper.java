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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToLongFunction;

/** A helper class to handle ranges. */
public class RangeHelper<T> {

    private final ToLongFunction<T> startFunction;
    private final ToLongFunction<T> endFunction;

    public RangeHelper(ToLongFunction<T> startFunction, ToLongFunction<T> endFunction) {
        this.startFunction = startFunction;
        this.endFunction = endFunction;
    }

    public boolean areAllRangesSame(List<T> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return true;
        }

        // Get the first range as reference
        T first = ranges.get(0);

        // Compare all other ranges with the first one
        for (int i = 1; i < ranges.size(); i++) {
            T current = ranges.get(i);
            if (current == null || first == null) {
                return false; // Null elements are considered different
            }
            if (startFunction.applyAsLong(current) != startFunction.applyAsLong(first)
                    || endFunction.applyAsLong(current) != endFunction.applyAsLong(first)) {
                return false; // Found a different range
            }
        }

        return true; // All ranges are identical
    }

    public boolean isContiguousAndCovered(List<T> ranges, T target) {
        if (ranges == null || ranges.isEmpty()) {
            return false;
        }

        // Sort ranges by start value
        List<T> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(startFunction));

        // Check contiguity
        T last = null;
        for (T range : sorted) {
            if (last != null) {
                long lastStart = startFunction.applyAsLong(last);
                long lastEnd = endFunction.applyAsLong(last);
                long currentStart = startFunction.applyAsLong(range);
                long currentEnd = endFunction.applyAsLong(range);

                if (lastStart == currentStart && lastEnd == currentEnd) {
                    // same, no need to check
                    continue;
                }

                // Check for contiguity: current start must be last end + 1
                if (currentStart != lastEnd + 1) {
                    return false; // Non-contiguous gap found
                }
            }
            last = range;
        }

        // Extract merged coverage
        long mergedStart = startFunction.applyAsLong(sorted.get(0));
        long mergedEnd = endFunction.applyAsLong(sorted.get(sorted.size() - 1));

        // Check if merged coverage exactly matches target
        long targetStart = startFunction.applyAsLong(target);
        long targetEnd = endFunction.applyAsLong(target);
        return (mergedStart == targetStart) && (mergedEnd == targetEnd);
    }

    public List<List<T>> mergeOverlappingRanges(List<T> ranges) {
        int n = ranges.size();
        if (n == 0) {
            return new ArrayList<>();
        }

        // Create a list of IndexedValue to keep track of original indices
        List<IndexedValue> indexedRanges = new ArrayList<>();
        for (int i = 0; i < ranges.size(); i++) {
            indexedRanges.add(new IndexedValue(ranges.get(i), i));
        }

        // Sort the ranges by their start value
        indexedRanges.sort(
                Comparator.comparingLong(IndexedValue::start).thenComparingLong(IndexedValue::end));

        List<List<IndexedValue>> groups = new ArrayList<>();
        // Initialize with the first range
        List<IndexedValue> currentGroup = new ArrayList<>();
        currentGroup.add(indexedRanges.get(0));
        long currentEnd = indexedRanges.get(0).end();

        // Iterate through the sorted ranges and merge overlapping ones
        for (int i = 1; i < indexedRanges.size(); i++) {
            IndexedValue current = indexedRanges.get(i);
            long start = current.start();
            long end = current.end();

            // If the current range overlaps with the current group, merge it
            if (start <= currentEnd) {
                currentGroup.add(current);
                // Update the current end to the maximum end of the merged ranges
                if (end > currentEnd) {
                    currentEnd = end;
                }
            } else {
                // Otherwise, start a new group
                groups.add(currentGroup);
                currentGroup = new ArrayList<>();
                currentGroup.add(current);
                currentEnd = end;
            }
        }
        // Add the last group
        groups.add(currentGroup);

        // Convert the groups to the required format and sort each group by original index
        List<List<T>> result = new ArrayList<>();
        for (List<IndexedValue> group : groups) {
            // Sort the group by original index to maintain the input order
            group.sort(Comparator.comparingInt(ip -> ip.originalIndex));
            // Extract the pairs
            List<T> sortedGroup = new ArrayList<>();
            for (IndexedValue ip : group) {
                sortedGroup.add(ip.value);
            }
            result.add(sortedGroup);
        }

        return result;
    }

    /** A helper class to track original indices. */
    private class IndexedValue {

        private final T value;
        private final int originalIndex;

        private IndexedValue(T value, int originalIndex) {
            this.value = value;
            this.originalIndex = originalIndex;
        }

        private long start() {
            return startFunction.applyAsLong(value);
        }

        private long end() {
            return endFunction.applyAsLong(value);
        }
    }
}
