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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.utils.LongTripleArrayList;
import org.apache.paimon.utils.PrimitiveRowRanges;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Object-free storage for current row-id entries. */
final class CurrentRowIdEntries {

    private static final long SPECIAL = 1L << 32;

    private final LongTripleArrayList entries;

    CurrentRowIdEntries() {
        this(0);
    }

    CurrentRowIdEntries(int expectedEntries) {
        checkArgument(expectedEntries >= 0, "Expected current entry count cannot be negative.");
        this.entries = new LongTripleArrayList(expectedEntries);
    }

    void add(int partitionId, boolean special, long firstRowId, long rowCount) {
        checkArgument(partitionId >= 0, "Partition id cannot be negative.");
        checkArgument(rowCount > 0, "Row count must be positive.");
        Math.addExact(firstRowId, rowCount - 1L);
        entries.add(
                Integer.toUnsignedLong(partitionId) | (special ? SPECIAL : 0L),
                firstRowId,
                rowCount);
    }

    int size() {
        return entries.size();
    }

    int retainedWordCount() {
        return entries.retainedLongCount();
    }

    int usedWordCount() {
        return entries.usedLongCount();
    }

    void release() {
        entries.release();
    }

    int partitionId(int index) {
        return (int) entries.first(index);
    }

    private boolean special(int index) {
        return (entries.first(index) & SPECIAL) != 0;
    }

    private long firstRowId(int index) {
        return entries.second(index);
    }

    private long rowCount(int index) {
        return entries.third(index);
    }

    private long lastRowId(int index) {
        return firstRowId(index) + rowCount(index) - 1L;
    }

    void sort() {
        if (entries.size() > 1) {
            sort(0, entries.size() - 1);
        }
    }

    private void sort(int left, int right) {
        while (left < right) {
            int middle = left + ((right - left) >>> 1);
            long pivotPartition = entries.first(middle);
            long pivotFirst = entries.second(middle);
            long pivotCount = entries.third(middle);
            int lower = left;
            int current = left;
            int upper = right;
            while (current <= upper) {
                int comparison = compare(current, pivotPartition, pivotFirst, pivotCount);
                if (comparison < 0) {
                    swap(lower++, current++);
                } else if (comparison > 0) {
                    swap(current, upper--);
                } else {
                    current++;
                }
            }

            if (lower - left < right - upper) {
                if (left < lower - 1) {
                    sort(left, lower - 1);
                }
                left = upper + 1;
            } else {
                if (upper + 1 < right) {
                    sort(upper + 1, right);
                }
                right = lower - 1;
            }
        }
    }

    private int compare(int index, long pivotPartition, long pivotFirst, long pivotCount) {
        int result =
                Long.compare(entries.first(index) & 0xFFFF_FFFFL, pivotPartition & 0xFFFF_FFFFL);
        if (result != 0) {
            return result;
        }
        long first = entries.second(index);
        result = Long.compare(first, pivotFirst);
        if (result != 0) {
            return result;
        }
        long end = first + entries.third(index) - 1L;
        long pivotEnd = pivotFirst + pivotCount - 1L;
        return Long.compare(end, pivotEnd);
    }

    private void swap(int left, int right) {
        entries.swap(left, right);
    }

    /**
     * Scans logical ranges without retaining one object (or even one primitive pair) per range.
     *
     * <p>The absolute return value is the number of logical ranges. A negative result means that
     * all logical ranges are contiguous and therefore this partition does not need a plan. A
     * positive result means that the ranges are fragmented and need materialization.
     */
    int scanLogicalRanges(int from, int to, long[] rangeScratch) {
        checkArgument(from >= 0 && from < to && to <= entries.size(), "Invalid entry slice.");
        int overlapStart = from;
        long currentEnd = lastRowId(from);
        int rangeCount = 0;
        boolean contiguous = true;
        boolean hasPrevious = false;
        long previousEnd = 0L;
        for (int i = from + 1; i < to; i++) {
            if (firstRowId(i) <= currentEnd) {
                currentEnd = Math.max(currentEnd, lastRowId(i));
            } else {
                computeLogicalRange(overlapStart, i, rangeScratch);
                rangeCount++;
                if (hasPrevious
                        && (previousEnd == Long.MAX_VALUE || rangeScratch[0] != previousEnd + 1L)) {
                    contiguous = false;
                }
                previousEnd = rangeScratch[1];
                hasPrevious = true;
                overlapStart = i;
                currentEnd = lastRowId(i);
            }
        }
        computeLogicalRange(overlapStart, to, rangeScratch);
        rangeCount++;
        if (hasPrevious && (previousEnd == Long.MAX_VALUE || rangeScratch[0] != previousEnd + 1L)) {
            contiguous = false;
        }
        return contiguous ? -rangeCount : rangeCount;
    }

    PrimitiveRowRanges materializeLogicalRanges(
            int from, int to, int expectedRangeCount, long[] rangeScratch) {
        checkArgument(
                from >= 0 && from < to && to <= entries.size() && expectedRangeCount > 0,
                "Invalid fragmented entry slice.");
        PrimitiveRowRanges ranges = new PrimitiveRowRanges(expectedRangeCount);
        int overlapStart = from;
        long currentEnd = lastRowId(from);
        for (int i = from + 1; i < to; i++) {
            if (firstRowId(i) <= currentEnd) {
                currentEnd = Math.max(currentEnd, lastRowId(i));
            } else {
                computeLogicalRange(overlapStart, i, rangeScratch);
                ranges.add(rangeScratch[0], rangeScratch[1]);
                overlapStart = i;
                currentEnd = lastRowId(i);
            }
        }
        computeLogicalRange(overlapStart, to, rangeScratch);
        ranges.add(rangeScratch[0], rangeScratch[1]);
        checkState(
                ranges.size() == expectedRangeCount,
                "Logical range count changed between scan and materialization.");
        return ranges;
    }

    private void computeLogicalRange(int from, int to, long[] result) {
        boolean hasOrdinary = false;
        long ordinaryStart = 0L;
        long ordinaryEnd = 0L;
        long spanningStart = Long.MAX_VALUE;
        long spanningEnd = Long.MIN_VALUE;
        for (int i = from; i < to; i++) {
            long start = firstRowId(i);
            long end = lastRowId(i);
            spanningStart = Math.min(spanningStart, start);
            spanningEnd = Math.max(spanningEnd, end);
            if (!special(i)) {
                checkState(
                        !hasOrdinary || (ordinaryStart == start && ordinaryEnd == end),
                        "Data files in one overlapping row-id group must have the same row-id range.");
                ordinaryStart = start;
                ordinaryEnd = end;
                hasOrdinary = true;
            }
        }
        long logicalStart = hasOrdinary ? ordinaryStart : spanningStart;
        long logicalEnd = hasOrdinary ? ordinaryEnd : spanningEnd;
        for (int i = from; i < to; i++) {
            checkState(
                    firstRowId(i) >= logicalStart && lastRowId(i) <= logicalEnd,
                    "File row-id range is outside its logical row-id range.");
        }
        result[0] = logicalStart;
        result[1] = logicalEnd;
    }

    @Nullable
    PrimitiveRowRanges selectedRangesForTesting() {
        checkState(entries.size() > 0, "Cannot inspect an empty current-entry buffer.");
        sort();
        int partitionId = partitionId(0);
        for (int i = 1; i < entries.size(); i++) {
            checkState(
                    partitionId(i) == partitionId,
                    "The structural range test helper requires one partition.");
        }
        long[] rangeScratch = new long[2];
        int rangeScan = scanLogicalRanges(0, entries.size(), rangeScratch);
        return rangeScan < 0
                ? null
                : materializeLogicalRanges(0, entries.size(), rangeScan, rangeScratch);
    }
}
