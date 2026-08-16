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

package org.apache.paimon.fs;

import org.apache.paimon.utils.BlockingExecutor;
import org.apache.paimon.utils.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.fs.FileRange.createFileRange;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/* This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utils for {@link VectoredReadable}. */
public class VectoredReadUtils {

    public static final ExecutorService IO_THREAD_POOL =
            Executors.newCachedThreadPool(newDaemonThreadFactory("VECTORED-IO-THREAD"));

    public static void readVectored(VectoredReadable readable, List<? extends FileRange> ranges)
            throws IOException {
        if (ranges.isEmpty()) {
            return;
        }
        readVectored(readable, ranges, ReadOptions.from(readable));
    }

    public static void readVectored(
            VectoredReadable readable, List<? extends FileRange> ranges, ReadOptions options)
            throws IOException {
        if (ranges.isEmpty()) {
            return;
        }
        requireNonNull(readable, "readable is null");
        requireNonNull(options, "options is null");

        List<? extends FileRange> sortRanges = validateAndSortRanges(ranges);
        List<CombinedRange> combinedRanges =
                mergeSortedRanges(sortRanges, options.minSeekForVectorReads);

        int parallelism = options.parallelismForVectorReads;

        if (options.sequentialReadFallback
                && combinedRanges.size() == 1
                && readable instanceof SeekableInputStream) {
            fallbackToReadSequence((SeekableInputStream) readable, sortRanges);
            return;
        }

        BlockingExecutor executor = new BlockingExecutor(IO_THREAD_POOL, parallelism);
        long batchSize = options.batchSizeForVectorReads;
        for (CombinedRange combinedRange : combinedRanges) {
            if (combinedRange.underlying.size() == 1) {
                FileRange fileRange = combinedRange.underlying.get(0);
                executor.submit(() -> readSingleRange(readable, fileRange));
            } else {
                List<FileRange> splitBatches = combinedRange.splitBatches(batchSize, parallelism);
                splitBatches.forEach(
                        range -> executor.submit(() -> readSingleRange(readable, range)));
                List<CompletableFuture<byte[]>> futures =
                        splitBatches.stream().map(FileRange::getData).collect(Collectors.toList());
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                        .whenCompleteAsync(
                                (unused, throwable) -> {
                                    if (throwable == null) {
                                        try {
                                            copyToFileRanges(combinedRange, futures);
                                        } catch (Throwable t) {
                                            completeFileRangesExceptionally(combinedRange, t);
                                        }
                                    } else {
                                        completeFileRangesExceptionally(combinedRange, throwable);
                                    }
                                },
                                IO_THREAD_POOL);
            }
        }
    }

    /** Options for vectored reads. */
    public static class ReadOptions {

        private final int minSeekForVectorReads;
        private final long batchSizeForVectorReads;
        private final int parallelismForVectorReads;
        private final boolean sequentialReadFallback;

        public static ReadOptions from(VectoredReadable readable) {
            return new ReadOptions(
                    readable.minSeekForVectorReads(),
                    readable.batchSizeForVectorReads(),
                    readable.parallelismForVectorReads(),
                    true);
        }

        public ReadOptions(
                int minSeekForVectorReads,
                long batchSizeForVectorReads,
                int parallelismForVectorReads,
                boolean sequentialReadFallback) {
            checkArgument(
                    minSeekForVectorReads >= 0,
                    "minSeekForVectorReads must be non-negative: %s",
                    minSeekForVectorReads);
            checkArgument(
                    batchSizeForVectorReads > 0,
                    "batchSizeForVectorReads must be positive: %s",
                    batchSizeForVectorReads);
            checkArgument(
                    parallelismForVectorReads > 0,
                    "parallelismForVectorReads must be positive: %s",
                    parallelismForVectorReads);
            this.minSeekForVectorReads = minSeekForVectorReads;
            this.batchSizeForVectorReads = batchSizeForVectorReads;
            this.parallelismForVectorReads = parallelismForVectorReads;
            this.sequentialReadFallback = sequentialReadFallback;
        }

        public ReadOptions withMinSeekForVectorReads(int minSeekForVectorReads) {
            return new ReadOptions(
                    minSeekForVectorReads,
                    batchSizeForVectorReads,
                    parallelismForVectorReads,
                    sequentialReadFallback);
        }

        public ReadOptions withBatchSizeForVectorReads(long batchSizeForVectorReads) {
            return new ReadOptions(
                    minSeekForVectorReads,
                    batchSizeForVectorReads,
                    parallelismForVectorReads,
                    sequentialReadFallback);
        }

        public ReadOptions withParallelismForVectorReads(int parallelismForVectorReads) {
            return new ReadOptions(
                    minSeekForVectorReads,
                    batchSizeForVectorReads,
                    parallelismForVectorReads,
                    sequentialReadFallback);
        }

        public ReadOptions withSequentialReadFallback(boolean sequentialReadFallback) {
            return new ReadOptions(
                    minSeekForVectorReads,
                    batchSizeForVectorReads,
                    parallelismForVectorReads,
                    sequentialReadFallback);
        }
    }

    private static void fallbackToReadSequence(
            SeekableInputStream in, List<? extends FileRange> ranges) throws IOException {
        for (FileRange range : ranges) {
            byte[] bytes = getOrCreateBuffer(range);
            in.seek(range.getOffset());
            IOUtils.readFully(in, bytes);
            range.getData().complete(bytes);
        }
    }

    private static void readSingleRange(VectoredReadable readable, FileRange range) {
        if (range.getLength() == 0) {
            range.getData().complete(getOrCreateBuffer(range));
            return;
        }
        try {
            long position = range.getOffset();
            int length = range.getLength();
            byte[] buffer = getOrCreateBuffer(range);
            readable.preadFully(position, buffer, 0, length);
            range.getData().complete(buffer);
        } catch (Exception ex) {
            range.getData().completeExceptionally(ex);
        }
    }

    private static void copyToFileRanges(
            CombinedRange combinedRange, List<CompletableFuture<byte[]>> futures) {
        List<byte[]> segments = new ArrayList<>(futures.size());
        for (CompletableFuture<byte[]> future : futures) {
            segments.add(future.join());
        }
        long offset = combinedRange.offset;
        for (FileRange fileRange : combinedRange.underlying) {
            byte[] buffer = getOrCreateBuffer(fileRange);
            copyMultiBytesToBytes(
                    segments,
                    (int) (fileRange.getOffset() - offset),
                    buffer,
                    fileRange.getLength());
            fileRange.getData().complete(buffer);
        }
    }

    private static byte[] getOrCreateBuffer(FileRange range) {
        if (range instanceof FileRange.FileRangeImpl) {
            return ((FileRange.FileRangeImpl) range).getOrCreateBuffer();
        }
        return new byte[range.getLength()];
    }

    private static void completeFileRangesExceptionally(
            CombinedRange combinedRange, Throwable throwable) {
        for (FileRange fileRange : combinedRange.underlying) {
            fileRange.getData().completeExceptionally(throwable);
        }
    }

    private static void copyMultiBytesToBytes(
            List<byte[]> segments, int offset, byte[] bytes, int numBytes) {
        int remainSize = numBytes;
        for (byte[] segment : segments) {
            int remain = segment.length - offset;
            if (remain > 0) {
                int nCopy = Math.min(remain, remainSize);
                System.arraycopy(segment, offset, bytes, numBytes - remainSize, nCopy);
                remainSize -= nCopy;
                // next new segment.
                offset = 0;
                if (remainSize == 0) {
                    return;
                }
            } else {
                // remain is negative, let's advance to next segment
                // now the offset = offset - segmentSize (-remain)
                offset = -remain;
            }
        }
    }

    private static List<? extends FileRange> validateAndSortRanges(
            final List<? extends FileRange> input) throws EOFException {
        requireNonNull(input, "Null input list");
        checkArgument(!input.isEmpty(), "Empty input list");
        final List<? extends FileRange> sortedRanges;

        if (input.size() == 1) {
            validateRangeRequest(input.get(0));
            sortedRanges = input;
        } else {
            sortedRanges = sortRanges(input);
            FileRange prev = null;
            for (final FileRange current : sortedRanges) {
                validateRangeRequest(current);
                if (prev != null) {
                    checkArgument(
                            current.getOffset() >= prev.getOffset() + prev.getLength(),
                            "Overlapping ranges %s and %s",
                            prev,
                            current);
                }
                prev = current;
            }
        }
        return sortedRanges;
    }

    private static void validateRangeRequest(FileRange range) throws EOFException {
        requireNonNull(range, "range is null");
        checkArgument(range.getLength() >= 0, "length is negative in %s", range);
        if (range.getOffset() < 0) {
            throw new EOFException("position is negative in range " + range);
        }
    }

    private static List<? extends FileRange> sortRanges(List<? extends FileRange> input) {
        List<? extends FileRange> ret = new ArrayList<>(input);
        ret.sort(Comparator.comparingLong(FileRange::getOffset));
        return ret;
    }

    private static List<CombinedRange> mergeSortedRanges(
            List<? extends FileRange> sortedRanges, int minimumSeek) {

        CombinedRange current = null;
        List<CombinedRange> result = new ArrayList<>(sortedRanges.size());

        // now merge together the ones that merge
        for (FileRange range : sortedRanges) {
            long start = range.getOffset();
            long end = range.getOffset() + range.getLength();
            if (current == null || !current.merge(start, end, range, minimumSeek)) {
                current = new CombinedRange(start, end, range);
                result.add(current);
            }
        }
        return result;
    }

    private static class CombinedRange {

        private final List<FileRange> underlying = new ArrayList<>();
        private final long offset;

        private int length;
        private long dataSize;

        public CombinedRange(long offset, long end, FileRange original) {
            this.offset = offset;
            this.length = (int) (end - offset);
            append(original);
        }

        private void append(final FileRange range) {
            this.underlying.add(range);
            dataSize += range.getLength();
        }

        public boolean merge(long otherOffset, long otherEnd, FileRange other, int minSeek) {
            long end = offset + length;
            long newEnd = Math.max(end, otherEnd);
            if (otherOffset - end >= minSeek) {
                return false;
            }
            this.length = (int) (newEnd - offset);
            append(other);
            return true;
        }

        private List<FileRange> splitBatches(long batchSize, int parallelism) {
            long expectedSize = Math.max(batchSize, (length / parallelism) + 1);
            List<FileRange> splitBatches = new ArrayList<>();
            long offset = this.offset;
            long end = offset + length;

            // split only when remain size exceeds twice the batchSize to avoid small File IO
            long minRemain = Math.max(expectedSize, batchSize * 2);

            while (true) {
                if (end < offset + minRemain) {
                    int currentLen = (int) (end - offset);
                    if (currentLen > 0) {
                        splitBatches.add(createFileRange(offset, currentLen));
                    }
                    break;
                } else {
                    splitBatches.add(createFileRange(offset, (int) expectedSize));
                    offset += expectedSize;
                }
            }
            return splitBatches;
        }

        @Override
        public String toString() {
            return String.format(
                    "CombinedRange: range count=%d, data size=%,d", underlying.size(), dataSize);
        }
    }
}
