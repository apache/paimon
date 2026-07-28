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

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Collects live file row-id ranges and emits logical ranges for fragmented partitions.
 *
 * <p>Normal files define the logical range of an overlapping file group. Dedicated files, such as
 * blob and vector files, must be contained in that logical range. If a group contains only
 * dedicated files, their spanning range is used.
 *
 * <p>The hot collection path retains three primitive words per file and does not retain manifest
 * objects. {@link #finish(FragmentedPartitionConsumer)} is terminal and releases this storage.
 */
final class LiveFileRowIdRangeCollector {

    private static final long DEDICATED_FILE_FLAG = 1L << 32;

    private final LongTripleArrayList entries;
    private boolean finished;

    LiveFileRowIdRangeCollector() {
        this(0);
    }

    LiveFileRowIdRangeCollector(int expectedFileCount) {
        checkArgument(expectedFileCount >= 0, "Expected live file count cannot be negative.");
        this.entries = new LongTripleArrayList(expectedFileCount);
    }

    void add(int partitionId, FileRole role, long firstRowId, long rowCount) {
        checkState(!finished, "Cannot add a file range after the collector is finished.");
        checkArgument(partitionId >= 0, "Partition id cannot be negative.");
        checkArgument(role != null, "File role cannot be null.");
        checkArgument(rowCount > 0, "Row count must be positive.");
        Math.addExact(firstRowId, rowCount - 1L);
        entries.add(
                Integer.toUnsignedLong(partitionId)
                        | (role == FileRole.DEDICATED ? DEDICATED_FILE_FLAG : 0L),
                firstRowId,
                rowCount);
    }

    int fileCount() {
        return entries.size();
    }

    int retainedWordCount() {
        return entries.retainedLongCount();
    }

    int usedWordCount() {
        return entries.usedLongCount();
    }

    private int partitionId(int index) {
        return (int) entries.first(index);
    }

    private boolean dedicatedFile(int index) {
        return (entries.first(index) & DEDICATED_FILE_FLAG) != 0;
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

    /**
     * Emits only partitions whose logical row-id ranges contain gaps.
     *
     * <p>The callback owns each emitted {@link PrimitiveRowRanges}. This collector is released even
     * when range validation or the callback fails.
     */
    void finish(FragmentedPartitionConsumer consumer) {
        checkState(!finished, "Live file row-id range collector is already finished.");
        checkArgument(consumer != null, "Fragmented partition consumer cannot be null.");
        finished = true;
        try {
            sortByPartitionAndRange();
            long[] rangeScratch = new long[2];
            LogicalRangeAnalysis analysis = new LogicalRangeAnalysis();
            int partitionStart = 0;
            while (partitionStart < entries.size()) {
                int partitionId = partitionId(partitionStart);
                int partitionEnd = partitionStart + 1;
                while (partitionEnd < entries.size() && partitionId(partitionEnd) == partitionId) {
                    partitionEnd++;
                }
                analyzeLogicalRanges(partitionStart, partitionEnd, rangeScratch, analysis);
                if (analysis.fragmented) {
                    consumer.accept(
                            partitionId,
                            materializeLogicalRanges(
                                    partitionStart,
                                    partitionEnd,
                                    analysis.rangeCount,
                                    rangeScratch));
                }
                partitionStart = partitionEnd;
            }
        } finally {
            entries.release();
        }
    }

    private void sortByPartitionAndRange() {
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
     * <p>The result records both the number of logical ranges and whether gaps exist between them.
     */
    private void analyzeLogicalRanges(
            int from, int to, long[] rangeScratch, LogicalRangeAnalysis analysis) {
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
        analysis.rangeCount = rangeCount;
        analysis.fragmented = !contiguous;
    }

    private PrimitiveRowRanges materializeLogicalRanges(
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
        boolean hasNormalFile = false;
        long normalStart = 0L;
        long normalEnd = 0L;
        long spanningStart = Long.MAX_VALUE;
        long spanningEnd = Long.MIN_VALUE;
        for (int i = from; i < to; i++) {
            long start = firstRowId(i);
            long end = lastRowId(i);
            spanningStart = Math.min(spanningStart, start);
            spanningEnd = Math.max(spanningEnd, end);
            if (!dedicatedFile(i)) {
                checkState(
                        !hasNormalFile || (normalStart == start && normalEnd == end),
                        "Normal files in one overlapping row-id group must have the same row-id range.");
                normalStart = start;
                normalEnd = end;
                hasNormalFile = true;
            }
        }
        long logicalStart = hasNormalFile ? normalStart : spanningStart;
        long logicalEnd = hasNormalFile ? normalEnd : spanningEnd;
        for (int i = from; i < to; i++) {
            checkState(
                    firstRowId(i) >= logicalStart && lastRowId(i) <= logicalEnd,
                    "File row-id range is outside its logical row-id range.");
        }
        result[0] = logicalStart;
        result[1] = logicalEnd;
    }

    enum FileRole {
        NORMAL,
        DEDICATED
    }

    @FunctionalInterface
    interface FragmentedPartitionConsumer {

        void accept(int partitionId, PrimitiveRowRanges logicalRanges);
    }

    private static final class LogicalRangeAnalysis {

        private int rangeCount;
        private boolean fragmented;
    }
}
