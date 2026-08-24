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

import org.apache.paimon.utils.PrimitiveRowRanges;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Collects live file row-id ranges and emits logical ranges for fragmented partitions.
 *
 * <p>Normal files define the logical range of an overlapping file group. Dedicated files, such as
 * blob and vector files, must be contained in that logical range. If a group contains only
 * dedicated files, their spanning range is used.
 *
 * <p>The hot collection path retains three primitive words per file. Entries are synchronously
 * radix-sorted in fixed-size chunks, compressed, and merged without retaining manifest objects or
 * allocating one Java object per range. {@link #finish(FragmentedPartitionConsumer)} is terminal
 * and releases this storage.
 */
final class LiveFileRowIdRangeCollector {

    private static final int ENTRY_WORDS = 3;
    private static final int ENTRY_CHUNK_SIZE = 1 << 21;
    private static final int RADIX_BITS = 16;
    private static final int RADIX_BUCKETS = 1 << RADIX_BITS;
    private static final long RADIX_MASK = RADIX_BUCKETS - 1L;
    private static final long DEDICATED_FILE_FLAG = 1L << 32;

    private final int expectedFileCount;
    private final List<SortedEntryChunk> sortedChunks = new ArrayList<>();
    private long[] words;
    private int chunkSize;
    private int fileCount;
    private boolean finished;

    LiveFileRowIdRangeCollector() {
        this(0);
    }

    LiveFileRowIdRangeCollector(int expectedFileCount) {
        checkArgument(expectedFileCount >= 0, "Expected live file count cannot be negative.");
        this.expectedFileCount = expectedFileCount;
        int initialEntries = Math.max(16, Math.min(expectedFileCount, ENTRY_CHUNK_SIZE));
        this.words = new long[Math.multiplyExact(initialEntries, ENTRY_WORDS)];
    }

    void add(int partitionId, FileRole role, long firstRowId, long rowCount) {
        checkState(!finished, "Cannot add a file range after the collector is finished.");
        checkArgument(partitionId >= 0, "Partition id cannot be negative.");
        checkArgument(role != null, "File role cannot be null.");
        checkArgument(rowCount > 0, "Row count must be positive.");
        Math.addExact(firstRowId, rowCount - 1L);
        if (chunkSize == ENTRY_CHUNK_SIZE) {
            flushCurrentChunk(false);
        }
        ensureCapacity(Math.addExact(chunkSize, 1));
        int offset = chunkSize * ENTRY_WORDS;
        words[offset] =
                Integer.toUnsignedLong(partitionId)
                        | (role == FileRole.DEDICATED ? DEDICATED_FILE_FLAG : 0L);
        words[offset + 1] = firstRowId;
        words[offset + 2] = rowCount;
        chunkSize++;
        fileCount = Math.addExact(fileCount, 1);
    }

    int fileCount() {
        return fileCount;
    }

    int retainedWordCount() {
        int retained = words.length;
        for (SortedEntryChunk chunk : sortedChunks) {
            retained = Math.addExact(retained, chunk.words.length);
        }
        return retained;
    }

    int usedWordCount() {
        return Math.multiplyExact(fileCount, ENTRY_WORDS);
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
            List<SortedEntryChunk> chunks = finishSortedChunks();
            if (chunks.isEmpty()) {
                return;
            }

            PartitionRangeAccumulator accumulator = new PartitionRangeAccumulator(consumer);
            if (chunks.size() == 1) {
                SortedEntryChunk chunk = chunks.get(0);
                for (int index = 0; index < chunk.size; index++) {
                    addToAccumulator(accumulator, chunk, index);
                }
            } else if (chunks.size() == 2) {
                mergeTwoChunks(accumulator, chunks.get(0), chunks.get(1));
            } else {
                mergeChunks(accumulator, chunks);
            }
            accumulator.finish();
        } finally {
            release();
        }
    }

    void abort() {
        if (!finished) {
            finished = true;
            release();
        }
    }

    private static void mergeTwoChunks(
            PartitionRangeAccumulator accumulator,
            SortedEntryChunk leftChunk,
            SortedEntryChunk rightChunk) {
        EntryChunkCursor left = new EntryChunkCursor(leftChunk);
        EntryChunkCursor right = new EntryChunkCursor(rightChunk);
        while (left.index < left.chunk.size && right.index < right.chunk.size) {
            EntryChunkCursor next = compareCursors(left, right) <= 0 ? left : right;
            addToAccumulator(accumulator, next.chunk, next.index++);
        }
        while (left.index < left.chunk.size) {
            addToAccumulator(accumulator, left.chunk, left.index++);
        }
        while (right.index < right.chunk.size) {
            addToAccumulator(accumulator, right.chunk, right.index++);
        }
    }

    private static void mergeChunks(
            PartitionRangeAccumulator accumulator, List<SortedEntryChunk> chunks) {
        PriorityQueue<EntryChunkCursor> queue =
                new PriorityQueue<>(LiveFileRowIdRangeCollector::compareCursors);
        for (SortedEntryChunk chunk : chunks) {
            if (chunk.size > 0) {
                queue.add(new EntryChunkCursor(chunk));
            }
        }
        while (!queue.isEmpty()) {
            EntryChunkCursor cursor = queue.poll();
            addToAccumulator(accumulator, cursor.chunk, cursor.index++);
            if (cursor.index < cursor.chunk.size) {
                queue.add(cursor);
            }
        }
    }

    private static void addToAccumulator(
            PartitionRangeAccumulator accumulator, SortedEntryChunk chunk, int index) {
        int offset = index * ENTRY_WORDS;
        long metadata = chunk.words[offset];
        long start = chunk.words[offset + 1];
        long rowCount = chunk.words[offset + 2];
        accumulator.add(
                (int) metadata,
                (metadata & DEDICATED_FILE_FLAG) != 0,
                start,
                start + rowCount - 1L);
    }

    private List<SortedEntryChunk> finishSortedChunks() {
        if (chunkSize > 0) {
            if (sortedChunks.isEmpty()) {
                int usedWords = chunkSize * ENTRY_WORDS;
                long[] finalWords =
                        words.length == usedWords ? words : Arrays.copyOf(words, usedWords);
                sortedChunks.add(sortAndCompress(finalWords, chunkSize));
                words = new long[0];
                chunkSize = 0;
            } else {
                flushCurrentChunk(true);
            }
        }
        return sortedChunks;
    }

    private void flushCurrentChunk(boolean finalChunk) {
        if (chunkSize == 0) {
            if (finalChunk) {
                words = new long[0];
            }
            return;
        }
        int usedWords = chunkSize * ENTRY_WORDS;
        long[] chunkWords = words.length == usedWords ? words : Arrays.copyOf(words, usedWords);
        int entries = chunkSize;
        words =
                finalChunk
                        ? new long[0]
                        : new long[Math.multiplyExact(nextChunkCapacity(), ENTRY_WORDS)];
        chunkSize = 0;
        sortedChunks.add(sortAndCompress(chunkWords, entries));
    }

    private int nextChunkCapacity() {
        if (expectedFileCount <= fileCount) {
            return ENTRY_CHUNK_SIZE;
        }
        int remainingHint = Math.max(0, expectedFileCount - fileCount);
        return Math.max(16, Math.min(remainingHint, ENTRY_CHUNK_SIZE));
    }

    private void ensureCapacity(int requiredEntries) {
        checkState(
                requiredEntries <= ENTRY_CHUNK_SIZE,
                "Live file row-id range chunk exceeds its fixed capacity.");
        long requiredWords = (long) requiredEntries * ENTRY_WORDS;
        if (requiredWords <= words.length) {
            return;
        }
        int maximumWords = ENTRY_CHUNK_SIZE * ENTRY_WORDS;
        int newLength = Math.max(16 * ENTRY_WORDS, words.length);
        while (newLength < requiredWords) {
            int grown = newLength + (newLength >>> 1);
            if (grown <= newLength || grown > maximumWords) {
                newLength = maximumWords;
                break;
            }
            newLength = grown;
        }
        words = Arrays.copyOf(words, newLength);
    }

    private static SortedEntryChunk sortAndCompress(long[] words, int size) {
        if (size > 1) {
            radixSort(words, size);
        }
        if (size == 0) {
            return new SortedEntryChunk(new long[0], 0);
        }

        int outputSize = 0;
        int currentPartition = partitionId(words, 0);
        long spanningStart = firstRowId(words, 0);
        long spanningEnd = lastRowId(words, 0);
        boolean hasNormalFile = !dedicatedFile(words, 0);
        long normalStart = spanningStart;
        long normalEnd = spanningEnd;
        for (int i = 1; i < size; i++) {
            int partitionId = partitionId(words, i);
            long start = firstRowId(words, i);
            long end = lastRowId(words, i);
            if (partitionId == currentPartition && start <= spanningEnd) {
                spanningEnd = Math.max(spanningEnd, end);
                if (!dedicatedFile(words, i)) {
                    checkState(
                            !hasNormalFile || (normalStart == start && normalEnd == end),
                            "Normal files in one overlapping row-id group must have the same row-id range.");
                    normalStart = start;
                    normalEnd = end;
                    hasNormalFile = true;
                }
                continue;
            }
            outputSize =
                    writeLogicalComponent(
                            words,
                            outputSize,
                            currentPartition,
                            spanningStart,
                            spanningEnd,
                            hasNormalFile,
                            normalStart,
                            normalEnd);
            currentPartition = partitionId;
            spanningStart = start;
            spanningEnd = end;
            hasNormalFile = !dedicatedFile(words, i);
            normalStart = start;
            normalEnd = end;
        }
        outputSize =
                writeLogicalComponent(
                        words,
                        outputSize,
                        currentPartition,
                        spanningStart,
                        spanningEnd,
                        hasNormalFile,
                        normalStart,
                        normalEnd);
        int outputWords = outputSize * ENTRY_WORDS;
        return new SortedEntryChunk(
                outputWords == words.length ? words : Arrays.copyOf(words, outputWords),
                outputSize);
    }

    private static int writeLogicalComponent(
            long[] words,
            int outputIndex,
            int partitionId,
            long spanningStart,
            long spanningEnd,
            boolean hasNormalFile,
            long normalStart,
            long normalEnd) {
        long logicalStart = hasNormalFile ? normalStart : spanningStart;
        long logicalEnd = hasNormalFile ? normalEnd : spanningEnd;
        checkState(
                spanningStart >= logicalStart && spanningEnd <= logicalEnd,
                "File row-id range is outside its logical row-id range.");
        int outputOffset = outputIndex * ENTRY_WORDS;
        words[outputOffset] =
                Integer.toUnsignedLong(partitionId) | (hasNormalFile ? 0L : DEDICATED_FILE_FLAG);
        words[outputOffset + 1] = logicalStart;
        words[outputOffset + 2] = inclusiveRangeCount(logicalStart, logicalEnd);
        return outputIndex + 1;
    }

    /**
     * Stable LSD radix sort by unsigned partition id, signed first row id, and signed last row id.
     */
    private static void radixSort(long[] words, int size) {
        long[] auxiliary = new long[Math.multiplyExact(size, ENTRY_WORDS)];
        int[] counts = new int[RADIX_BUCKETS];
        long[] source = words;
        long[] target = auxiliary;

        // Four 16-bit passes for last row id, four for first row id, then two for partition id.
        for (int pass = 0; pass < 10; pass++) {
            Arrays.fill(counts, 0);
            for (int index = 0; index < size; index++) {
                counts[radixBucket(source, index, pass)]++;
            }

            int position = 0;
            for (int bucket = 0; bucket < RADIX_BUCKETS; bucket++) {
                int bucketSize = counts[bucket];
                counts[bucket] = position;
                position += bucketSize;
            }

            for (int index = 0; index < size; index++) {
                int sourceOffset = index * ENTRY_WORDS;
                int targetOffset = counts[radixBucket(source, index, pass)]++ * ENTRY_WORDS;
                target[targetOffset] = source[sourceOffset];
                target[targetOffset + 1] = source[sourceOffset + 1];
                target[targetOffset + 2] = source[sourceOffset + 2];
            }

            long[] swap = source;
            source = target;
            target = swap;
        }

        checkState(source == words, "Radix sort must finish in its input buffer.");
    }

    private static int radixBucket(long[] words, int index, int pass) {
        int offset = index * ENTRY_WORDS;
        long value;
        int shift;
        if (pass < 4) {
            value = (words[offset + 1] + words[offset + 2] - 1L) ^ Long.MIN_VALUE;
            shift = pass * RADIX_BITS;
        } else if (pass < 8) {
            value = words[offset + 1] ^ Long.MIN_VALUE;
            shift = (pass - 4) * RADIX_BITS;
        } else {
            value = words[offset] & 0xFFFF_FFFFL;
            shift = (pass - 8) * RADIX_BITS;
        }
        return (int) ((value >>> shift) & RADIX_MASK);
    }

    private static int compareCursors(EntryChunkCursor left, EntryChunkCursor right) {
        int leftOffset = left.index * ENTRY_WORDS;
        int rightOffset = right.index * ENTRY_WORDS;
        long[] leftWords = left.chunk.words;
        long[] rightWords = right.chunk.words;
        int result =
                Long.compare(
                        leftWords[leftOffset] & 0xFFFF_FFFFL,
                        rightWords[rightOffset] & 0xFFFF_FFFFL);
        if (result != 0) {
            return result;
        }
        result = Long.compare(leftWords[leftOffset + 1], rightWords[rightOffset + 1]);
        if (result != 0) {
            return result;
        }
        long leftEnd = leftWords[leftOffset + 1] + leftWords[leftOffset + 2] - 1L;
        long rightEnd = rightWords[rightOffset + 1] + rightWords[rightOffset + 2] - 1L;
        return Long.compare(leftEnd, rightEnd);
    }

    private static int partitionId(long[] words, int index) {
        return (int) words[index * ENTRY_WORDS];
    }

    private static boolean dedicatedFile(long[] words, int index) {
        return (words[index * ENTRY_WORDS] & DEDICATED_FILE_FLAG) != 0;
    }

    private static long firstRowId(long[] words, int index) {
        return words[index * ENTRY_WORDS + 1];
    }

    private static long lastRowId(long[] words, int index) {
        int offset = index * ENTRY_WORDS;
        return words[offset + 1] + words[offset + 2] - 1L;
    }

    private static long inclusiveRangeCount(long start, long end) {
        return Math.addExact(Math.subtractExact(end, start), 1L);
    }

    private void release() {
        words = new long[0];
        chunkSize = 0;
        fileCount = 0;
        sortedChunks.clear();
    }

    enum FileRole {
        NORMAL,
        DEDICATED
    }

    @FunctionalInterface
    interface FragmentedPartitionConsumer {

        void accept(int partitionId, PrimitiveRowRanges logicalRanges);
    }

    private static final class SortedEntryChunk {

        private final long[] words;
        private final int size;

        private SortedEntryChunk(long[] words, int size) {
            this.words = words;
            this.size = size;
        }
    }

    private static final class EntryChunkCursor {

        private final SortedEntryChunk chunk;
        private int index;

        private EntryChunkCursor(SortedEntryChunk chunk) {
            this.chunk = chunk;
        }
    }

    private static final class PartitionRangeAccumulator {

        private final FragmentedPartitionConsumer consumer;
        private int partitionId = -1;
        private @Nullable PrimitiveRowRanges ranges;
        private boolean fragmented;
        private boolean hasPreviousRange;
        private long previousRangeEnd;
        private boolean hasComponent;
        private long spanningStart;
        private long spanningEnd;
        private boolean hasNormalFile;
        private long normalStart;
        private long normalEnd;

        private PartitionRangeAccumulator(FragmentedPartitionConsumer consumer) {
            this.consumer = consumer;
        }

        private void add(int incomingPartitionId, boolean dedicated, long start, long end) {
            if (!hasComponent) {
                startPartition(incomingPartitionId);
                startComponent(dedicated, start, end);
                return;
            }
            if (incomingPartitionId == partitionId && start <= spanningEnd) {
                spanningEnd = Math.max(spanningEnd, end);
                if (!dedicated) {
                    checkState(
                            !hasNormalFile || (normalStart == start && normalEnd == end),
                            "Normal files in one overlapping row-id group must have the same row-id range.");
                    normalStart = start;
                    normalEnd = end;
                    hasNormalFile = true;
                }
                return;
            }

            finishComponent();
            if (incomingPartitionId != partitionId) {
                finishPartition();
                startPartition(incomingPartitionId);
            }
            startComponent(dedicated, start, end);
        }

        private void startPartition(int incomingPartitionId) {
            partitionId = incomingPartitionId;
            ranges = new PrimitiveRowRanges(16);
            fragmented = false;
            hasPreviousRange = false;
        }

        private void startComponent(boolean dedicated, long start, long end) {
            spanningStart = start;
            spanningEnd = end;
            hasNormalFile = !dedicated;
            normalStart = start;
            normalEnd = end;
            hasComponent = true;
        }

        private void finishComponent() {
            long logicalStart = hasNormalFile ? normalStart : spanningStart;
            long logicalEnd = hasNormalFile ? normalEnd : spanningEnd;
            checkState(
                    spanningStart >= logicalStart && spanningEnd <= logicalEnd,
                    "File row-id range is outside its logical row-id range.");
            checkState(ranges != null, "Missing logical range buffer.");
            if (hasPreviousRange
                    && (previousRangeEnd == Long.MAX_VALUE
                            || logicalStart != previousRangeEnd + 1L)) {
                fragmented = true;
            }
            ranges.add(logicalStart, logicalEnd);
            previousRangeEnd = logicalEnd;
            hasPreviousRange = true;
            hasComponent = false;
        }

        private void finishPartition() {
            checkState(partitionId >= 0 && ranges != null, "Missing partition state.");
            if (fragmented) {
                consumer.accept(partitionId, ranges);
            }
            ranges = null;
        }

        private void finish() {
            if (!hasComponent) {
                return;
            }
            finishComponent();
            finishPartition();
        }
    }
}
