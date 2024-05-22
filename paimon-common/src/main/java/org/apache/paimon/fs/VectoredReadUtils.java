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

import org.apache.paimon.utils.BlockedDelegatingExecutor;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.fs.FileIOUtils.IO_THREAD_POOL;
import static org.apache.paimon.fs.FileRange.createFileRange;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/* This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utils for {@link VectoredReadable}. */
public class VectoredReadUtils {

    public static void readVectored(VectoredReadable readable, List<? extends FileRange> ranges)
            throws IOException {
        if (ranges.isEmpty()) {
            return;
        }

        List<CombinedRange> combinedRanges =
                mergeSortedRanges(validateAndSortRanges(ranges), readable.minSeekForVectorReads());

        int parallelism = readable.parallelismForVectorReads();
        BlockedDelegatingExecutor executor =
                new BlockedDelegatingExecutor(IO_THREAD_POOL, parallelism);
        long batchSize = readable.batchSizeForVectorReads();
        for (CombinedRange combinedRange : combinedRanges) {
            if (combinedRange.getUnderlying().size() == 1) {
                FileRange fileRange = combinedRange.getUnderlying().get(0);
                executor.submit(() -> readSingleRange(readable, fileRange));
            } else {
                List<FileRange> splitBatches = combinedRange.splitBatches(batchSize, parallelism);
                splitBatches.forEach(
                        range -> executor.submit(() -> readSingleRange(readable, range)));
                List<CompletableFuture<byte[]>> futures =
                        splitBatches.stream().map(FileRange::getData).collect(Collectors.toList());
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                        .thenAcceptAsync(
                                unused -> copyToFileRanges(combinedRange, futures), IO_THREAD_POOL);
            }
        }
    }

    private static void readSingleRange(VectoredReadable readable, FileRange range) {
        if (range.getLength() == 0) {
            range.getData().complete(new byte[0]);
            return;
        }
        try {
            long position = range.getOffset();
            int length = range.getLength();
            byte[] buffer = new byte[length];
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
        long offset = combinedRange.getOffset();
        for (FileRange fileRange : combinedRange.underlying) {
            byte[] buffer = new byte[fileRange.getLength()];
            copyMultiBytesToBytes(
                    segments,
                    (int) (fileRange.getOffset() - offset),
                    buffer,
                    fileRange.getLength());
            fileRange.getData().complete(buffer);
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

        public long getOffset() {
            return offset;
        }

        private void append(final FileRange range) {
            this.underlying.add(range);
            dataSize += range.getLength();
        }

        public List<FileRange> getUnderlying() {
            return underlying;
        }

        public boolean merge(long otherOffset, long otherEnd, FileRange other, int minSeek) {
            long end = this.getOffset() + length;
            long newEnd = Math.max(end, otherEnd);
            if (otherOffset - end >= minSeek) {
                return false;
            }
            this.length = (int) (newEnd - this.getOffset());
            append(other);
            return true;
        }

        private List<FileRange> splitBatches(long batchSize, int parallelism) {
            long expectedSize = Math.max(batchSize, (length / parallelism) + 1);
            List<FileRange> splitBatches = new ArrayList<>();
            long offset = getOffset();
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
