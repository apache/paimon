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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Selects compaction candidate row-id ranges from primitive projected file metadata.
 *
 * <p>Each live file is retained as four primitive words: partition and file kind, first row id, row
 * count, and file size. The candidate rules mirror normal, blob, and vector triggering in {@link
 * DataEvolutionCompactCoordinator.CompactPlanner}, without retaining manifest entries or full
 * data-file metadata.
 */
final class CompactCandidateRangeCollector {

    static final int NORMAL_FILE = -1;
    static final int VECTOR_FILE = -2;
    static final int IGNORED_DEDICATED_FILE = -3;

    private static final int ENTRY_WORDS = 4;
    private static final int ENTRY_CHUNK_SIZE = 1 << 20;
    private static final int RADIX_BITS = 16;
    private static final int RADIX_BUCKETS = 1 << RADIX_BITS;
    private static final long RADIX_MASK = RADIX_BUCKETS - 1L;

    private final int expectedFileCount;
    private final long targetFileSize;
    private final long blobTargetFileSize;
    private final long openFileCost;
    private final long compactMinFileNum;
    private final List<SortedEntryChunk> sortedChunks = new ArrayList<>();
    private long[] words;
    private int chunkSize;
    private int fileCount;
    private boolean finished;

    CompactCandidateRangeCollector(
            int expectedFileCount,
            long targetFileSize,
            long blobTargetFileSize,
            long openFileCost,
            long compactMinFileNum) {
        checkArgument(expectedFileCount >= 0, "Expected live file count cannot be negative.");
        checkArgument(targetFileSize > 0, "Target file size must be positive.");
        checkArgument(blobTargetFileSize > 0, "Blob target file size must be positive.");
        checkArgument(openFileCost >= 0, "Open file cost cannot be negative.");
        checkArgument(compactMinFileNum > 0, "Compact min file number must be positive.");
        this.expectedFileCount = expectedFileCount;
        this.targetFileSize = targetFileSize;
        this.blobTargetFileSize = blobTargetFileSize;
        this.openFileCost = openFileCost;
        this.compactMinFileNum = compactMinFileNum;
        int initialEntries = Math.max(16, Math.min(expectedFileCount, ENTRY_CHUNK_SIZE));
        this.words = new long[Math.multiplyExact(initialEntries, ENTRY_WORDS)];
    }

    /**
     * Adds one live file. {@code fileKind} is {@link #NORMAL_FILE}, {@link #VECTOR_FILE}, {@link
     * #IGNORED_DEDICATED_FILE}, or a non-negative blob field id.
     */
    void add(int partitionId, int fileKind, long firstRowId, long rowCount, long fileSize) {
        checkState(!finished, "Cannot add a candidate file after the collector is finished.");
        checkArgument(partitionId >= 0, "Partition id cannot be negative.");
        checkArgument(
                fileKind >= 0
                        || fileKind == NORMAL_FILE
                        || fileKind == VECTOR_FILE
                        || fileKind == IGNORED_DEDICATED_FILE,
                "Unknown candidate file kind %s.",
                fileKind);
        checkArgument(rowCount > 0, "Row count must be positive.");
        checkArgument(fileSize >= 0, "File size cannot be negative.");
        Math.addExact(firstRowId, rowCount - 1L);
        if (chunkSize == ENTRY_CHUNK_SIZE) {
            flushCurrentChunk(false);
        }
        ensureCapacity(Math.addExact(chunkSize, 1));
        int offset = chunkSize * ENTRY_WORDS;
        words[offset] = ((long) fileKind << 32) | Integer.toUnsignedLong(partitionId);
        words[offset + 1] = firstRowId;
        words[offset + 2] = rowCount;
        words[offset + 3] = fileSize;
        chunkSize++;
        fileCount = Math.addExact(fileCount, 1);
    }

    int usedWordCount() {
        return Math.multiplyExact(fileCount, ENTRY_WORDS);
    }

    int retainedWordCount() {
        int retained = words.length;
        for (SortedEntryChunk chunk : sortedChunks) {
            retained = Math.addExact(retained, chunk.words.length);
        }
        return retained;
    }

    void finish(CandidateRangeConsumer consumer) {
        checkState(!finished, "Compact candidate range collector is already finished.");
        checkArgument(consumer != null, "Candidate range consumer cannot be null.");
        finished = true;
        try {
            List<SortedEntryChunk> chunks = finishSortedChunks();
            if (chunks.isEmpty()) {
                return;
            }

            CandidateAccumulator accumulator =
                    new CandidateAccumulator(
                            targetFileSize,
                            blobTargetFileSize,
                            openFileCost,
                            compactMinFileNum,
                            consumer);
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
            CandidateAccumulator accumulator,
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
            CandidateAccumulator accumulator, List<SortedEntryChunk> chunks) {
        PriorityQueue<EntryChunkCursor> queue =
                new PriorityQueue<>(CompactCandidateRangeCollector::compareCursors);
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
            CandidateAccumulator accumulator, SortedEntryChunk chunk, int index) {
        int offset = index * ENTRY_WORDS;
        long metadata = chunk.words[offset];
        long start = chunk.words[offset + 1];
        long rowCount = chunk.words[offset + 2];
        accumulator.add(
                (int) metadata,
                (int) (metadata >> 32),
                start,
                start + rowCount - 1L,
                chunk.words[offset + 3]);
    }

    private List<SortedEntryChunk> finishSortedChunks() {
        if (chunkSize > 0) {
            if (sortedChunks.isEmpty()) {
                int usedWords = chunkSize * ENTRY_WORDS;
                long[] finalWords =
                        words.length == usedWords ? words : Arrays.copyOf(words, usedWords);
                radixSort(finalWords, chunkSize);
                sortedChunks.add(new SortedEntryChunk(finalWords, chunkSize));
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
        radixSort(chunkWords, entries);
        sortedChunks.add(new SortedEntryChunk(chunkWords, entries));
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
                "Compact candidate range chunk exceeds its fixed capacity.");
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

    /**
     * Stable LSD radix sort by unsigned partition id, signed first row id, and signed last row id.
     */
    private static void radixSort(long[] words, int size) {
        if (size <= 1) {
            return;
        }
        long[] auxiliary = new long[Math.multiplyExact(size, ENTRY_WORDS)];
        int[] counts = new int[RADIX_BUCKETS];
        long[] source = words;
        long[] target = auxiliary;

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
                for (int word = 0; word < ENTRY_WORDS; word++) {
                    target[targetOffset + word] = source[sourceOffset + word];
                }
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

    private void release() {
        words = new long[0];
        chunkSize = 0;
        fileCount = 0;
        sortedChunks.clear();
    }

    @FunctionalInterface
    interface CandidateRangeConsumer {

        void accept(long start, long end, int fileCount);
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

    private static final class CandidateAccumulator {

        private final long targetFileSize;
        private final long blobTargetFileSize;
        private final long openFileCost;
        private final long compactMinFileNum;
        private final CandidateRangeConsumer consumer;
        private final CandidateBin bin = new CandidateBin();
        private final Map<Integer, BlobFieldAccumulator> blobFields = new HashMap<>();

        private int partitionId = -1;
        private boolean hasComponent;
        private long spanningStart;
        private long spanningEnd;
        private boolean hasNormalFile;
        private long normalStart;
        private long normalEnd;
        private long normalFileCount;
        private long normalWeight;
        private long vectorFileCount;
        private int componentFileCount;
        private boolean hasPreviousLogicalRange;
        private long previousLogicalEnd;

        private CandidateAccumulator(
                long targetFileSize,
                long blobTargetFileSize,
                long openFileCost,
                long compactMinFileNum,
                CandidateRangeConsumer consumer) {
            this.targetFileSize = targetFileSize;
            this.blobTargetFileSize = blobTargetFileSize;
            this.openFileCost = openFileCost;
            this.compactMinFileNum = compactMinFileNum;
            this.consumer = consumer;
        }

        private void add(
                int incomingPartitionId, int fileKind, long start, long end, long fileSize) {
            if (!hasComponent) {
                startPartition(incomingPartitionId);
                startComponent(fileKind, start, end, fileSize);
                return;
            }
            if (incomingPartitionId == partitionId && start <= spanningEnd) {
                spanningEnd = Math.max(spanningEnd, end);
                addToComponent(fileKind, start, end, fileSize);
                return;
            }

            finishComponent();
            if (incomingPartitionId != partitionId) {
                finishPartition();
                startPartition(incomingPartitionId);
            }
            startComponent(fileKind, start, end, fileSize);
        }

        private void startPartition(int incomingPartitionId) {
            partitionId = incomingPartitionId;
            hasPreviousLogicalRange = false;
        }

        private void startComponent(int fileKind, long start, long end, long fileSize) {
            spanningStart = start;
            spanningEnd = end;
            hasNormalFile = false;
            normalStart = start;
            normalEnd = end;
            normalFileCount = 0L;
            normalWeight = 0L;
            vectorFileCount = 0L;
            componentFileCount = 0;
            blobFields.clear();
            addToComponent(fileKind, start, end, fileSize);
            hasComponent = true;
        }

        private void addToComponent(int fileKind, long start, long end, long fileSize) {
            componentFileCount = Math.addExact(componentFileCount, 1);
            if (fileKind == NORMAL_FILE) {
                checkState(
                        !hasNormalFile || (normalStart == start && normalEnd == end),
                        "Normal files in one overlapping row-id group must have the same row-id range.");
                hasNormalFile = true;
                normalStart = start;
                normalEnd = end;
                normalFileCount = Math.addExact(normalFileCount, 1L);
                normalWeight = Math.addExact(normalWeight, Math.max(fileSize, openFileCost));
            } else if (fileKind == VECTOR_FILE) {
                vectorFileCount = Math.addExact(vectorFileCount, 1L);
            } else if (fileKind >= 0) {
                blobFields
                        .computeIfAbsent(
                                fileKind, ignored -> new BlobFieldAccumulator(blobTargetFileSize))
                        .add(start, end, fileSize);
            }
        }

        private void finishComponent() {
            if (!hasComponent) {
                return;
            }
            long logicalStart = hasNormalFile ? normalStart : spanningStart;
            long logicalEnd = hasNormalFile ? normalEnd : spanningEnd;
            checkState(
                    spanningStart >= logicalStart && spanningEnd <= logicalEnd,
                    "File row-id range is outside its logical row-id range.");

            if (!hasNormalFile) {
                flushBin();
                hasPreviousLogicalRange = false;
                hasComponent = false;
                return;
            }

            boolean dedicatedCandidate = vectorFileCount >= compactMinFileNum;
            if (!dedicatedCandidate) {
                for (BlobFieldAccumulator blobField : blobFields.values()) {
                    if (blobField.hasCandidate()) {
                        dedicatedCandidate = true;
                        break;
                    }
                }
            }

            if (hasPreviousLogicalRange
                    && (previousLogicalEnd == Long.MAX_VALUE
                            || logicalStart != previousLogicalEnd + 1L)) {
                flushBin();
            }

            Component component =
                    new Component(
                            logicalStart,
                            logicalEnd,
                            componentFileCount,
                            normalFileCount,
                            normalWeight,
                            dedicatedCandidate);
            if (normalWeight > targetFileSize) {
                flushBin();
                emitComponent(component);
            } else {
                bin.add(component);
                if (bin.normalWeight > targetFileSize) {
                    flushBin();
                }
            }
            previousLogicalEnd = logicalEnd;
            hasPreviousLogicalRange = true;
            hasComponent = false;
        }

        private void emitComponent(Component component) {
            if (component.normalFileCount >= compactMinFileNum || component.dedicatedCandidate) {
                consumer.accept(component.start, component.end, component.fileCount);
            }
        }

        private void flushBin() {
            bin.emit(compactMinFileNum, consumer);
            bin.clear();
        }

        private void finishPartition() {
            flushBin();
        }

        private void finish() {
            finishComponent();
            finishPartition();
        }
    }

    private static final class Component {

        private final long start;
        private final long end;
        private final int fileCount;
        private final long normalFileCount;
        private final long normalWeight;
        private final boolean dedicatedCandidate;

        private Component(
                long start,
                long end,
                int fileCount,
                long normalFileCount,
                long normalWeight,
                boolean dedicatedCandidate) {
            this.start = start;
            this.end = end;
            this.fileCount = fileCount;
            this.normalFileCount = normalFileCount;
            this.normalWeight = normalWeight;
            this.dedicatedCandidate = dedicatedCandidate;
        }
    }

    private static final class CandidateBin {

        private long start;
        private long end;
        private long normalFileCount;
        private long normalWeight;
        private int fileCount;
        private long[] dedicatedCandidates = new long[12];
        private int dedicatedCandidateCount;

        private void add(Component component) {
            if (fileCount == 0) {
                start = component.start;
            }
            end = component.end;
            fileCount = Math.addExact(fileCount, component.fileCount);
            normalFileCount = Math.addExact(normalFileCount, component.normalFileCount);
            normalWeight = Math.addExact(normalWeight, component.normalWeight);
            if (component.dedicatedCandidate) {
                ensureCandidateCapacity(dedicatedCandidateCount + 1);
                int offset = dedicatedCandidateCount * 3;
                dedicatedCandidates[offset] = component.start;
                dedicatedCandidates[offset + 1] = component.end;
                dedicatedCandidates[offset + 2] = component.fileCount;
                dedicatedCandidateCount++;
            }
        }

        private void emit(long compactMinFileNum, CandidateRangeConsumer consumer) {
            if (fileCount == 0) {
                return;
            }
            if (normalFileCount >= compactMinFileNum) {
                consumer.accept(start, end, fileCount);
                return;
            }
            for (int i = 0; i < dedicatedCandidateCount; i++) {
                int offset = i * 3;
                consumer.accept(
                        dedicatedCandidates[offset],
                        dedicatedCandidates[offset + 1],
                        Math.toIntExact(dedicatedCandidates[offset + 2]));
            }
        }

        private void clear() {
            normalFileCount = 0L;
            normalWeight = 0L;
            fileCount = 0;
            dedicatedCandidateCount = 0;
        }

        private void ensureCandidateCapacity(int requiredCandidates) {
            int requiredWords = Math.multiplyExact(requiredCandidates, 3);
            if (requiredWords <= dedicatedCandidates.length) {
                return;
            }
            dedicatedCandidates =
                    Arrays.copyOf(
                            dedicatedCandidates,
                            Math.max(requiredWords, dedicatedCandidates.length << 1));
        }
    }

    /** Exact component-local implementation of blob file candidate selection. */
    private static final class BlobFieldAccumulator {

        private final long targetFileSize;
        private boolean candidate;
        private boolean hasOverlapGroup;
        private long overlapStart;
        private long overlapEnd;
        private long overlapFileSize;
        private int overlapFileCount;
        private long continuousExpectedStart;
        private long continuousFileSize;
        private int continuousFileCount;

        private BlobFieldAccumulator(long targetFileSize) {
            this.targetFileSize = targetFileSize;
        }

        private void add(long start, long end, long fileSize) {
            if (!hasOverlapGroup) {
                startOverlapGroup(start, end, fileSize);
                return;
            }
            if (start <= overlapEnd) {
                overlapEnd = Math.max(overlapEnd, end);
                overlapFileSize += fileSize;
                overlapFileCount++;
                return;
            }
            finishOverlapGroup();
            startOverlapGroup(start, end, fileSize);
        }

        private void startOverlapGroup(long start, long end, long fileSize) {
            hasOverlapGroup = true;
            overlapStart = start;
            overlapEnd = end;
            overlapFileSize = fileSize;
            overlapFileCount = 1;
        }

        private void finishOverlapGroup() {
            if (!hasOverlapGroup) {
                return;
            }
            if (overlapFileCount >= 2) {
                candidate = true;
                finishContinuousFiles();
            } else {
                addSmallFile(overlapStart, overlapEnd, overlapFileSize);
            }
            hasOverlapGroup = false;
        }

        private void addSmallFile(long start, long end, long fileSize) {
            if (fileSize >= targetFileSize) {
                finishContinuousFiles();
                return;
            }
            if (continuousFileCount > 0 && start != continuousExpectedStart) {
                finishContinuousFiles();
            }
            continuousFileCount++;
            continuousFileSize += fileSize;
            continuousExpectedStart = end == Long.MAX_VALUE ? Long.MIN_VALUE : end + 1L;
            if (continuousFileSize >= targetFileSize && continuousFileCount >= 2) {
                candidate = true;
                continuousFileCount = 0;
                continuousFileSize = 0L;
            }
        }

        private void finishContinuousFiles() {
            if (continuousFileCount >= 2) {
                candidate = true;
            }
            continuousFileCount = 0;
            continuousFileSize = 0L;
        }

        private boolean hasCandidate() {
            finishOverlapGroup();
            finishContinuousFiles();
            return candidate;
        }
    }
}
