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

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.fs.FileIOUtils.IO_THREAD_POOL;
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

        List<? extends FileRange> sortRanges = validateAndSortRanges(ranges);
        List<CombinedRange> combinedRanges =
                mergeSortedRanges(sortRanges, readable.minSeekForVectorReads());

        int parallelism = readable.parallelismForVectorReads();

        if (combinedRanges.size() == 1) {
            fallbackToReadSequence(readable, sortRanges);
            return;
        }

        BlockingExecutor executor = new BlockingExecutor(IO_THREAD_POOL, parallelism);
        for (CombinedRange combinedRange : combinedRanges) {
            if (combinedRange.underlying.size() == 1) {
                FileRange fileRange = combinedRange.underlying.get(0);
                executor.submit(() -> readSingleRange(readable, fileRange));
            } else {
                executor.submit(() -> readCombinedRange(readable, combinedRange));
            }
        }
    }

    private static void fallbackToReadSequence(
            VectoredReadable readable, List<? extends FileRange> ranges) throws IOException {
        SeekableInputStream in = (SeekableInputStream) readable;
        for (FileRange range : ranges) {
            byte[] bytes = new byte[range.getLength()];
            in.seek(range.getOffset());
            IOUtils.readFully(in, bytes);
            range.getData().complete(bytes);
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

    private static void readCombinedRange(VectoredReadable readable, CombinedRange combinedRange) {
        try {
            long position = combinedRange.offset;
            int length = combinedRange.length;

            byte[] total = new byte[length];
            readable.preadFully(position, total, 0, length);

            for (FileRange child : combinedRange.underlying) {
                byte[] buffer = new byte[child.getLength()];
                System.arraycopy(
                        total, (int) (child.getOffset() - position), buffer, 0, child.getLength());
                child.getData().complete(buffer);
            }
        } catch (Exception ex) {
            // complete exception all the underlying ranges which have not already
            // finished.
            for (FileRange child : combinedRange.underlying) {
                if (!child.getData().isDone()) {
                    child.getData().completeExceptionally(ex);
                }
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

        @Override
        public String toString() {
            return String.format(
                    "CombinedRange: range count=%d, data size=%,d", underlying.size(), dataSize);
        }
    }
}
