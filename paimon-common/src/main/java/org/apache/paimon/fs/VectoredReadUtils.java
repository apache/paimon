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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.fs.FileIOUtils.IO_THREAD_POOL;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/* This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utils for {@link VectoredReadable}. */
public class VectoredReadUtils {

    public static void readVectored(
            VectoredReadable readable,
            List<? extends FileRange> ranges,
            IntFunction<ByteBuffer> allocate)
            throws IOException {
        // prepare to read
        List<? extends FileRange> sortedRanges = validateAndSortRanges(ranges);
        for (FileRange range : ranges) {
            CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
            range.setData(result);
        }

        // TODO limit IO_THREAD_POOL? Too much parallelism will affect disk addressing
        if (isOrderedDisjoint(sortedRanges, readable.minSeekForVectorReads())) {
            for (FileRange range : sortedRanges) {
                ByteBuffer buffer = allocate.apply(range.getLength());
                IO_THREAD_POOL.submit(() -> readSingleRange(readable, range, buffer));
            }
        } else {
            List<CombinedFileRange> combinedFileRanges =
                    mergeSortedRanges(
                            sortedRanges,
                            readable.minSeekForVectorReads(),
                            readable.maxReadSizeForVectorReads());
            for (CombinedFileRange combinedFileRange : combinedFileRanges) {
                IO_THREAD_POOL.submit(
                        () ->
                                readCombinedRangeAndUpdateChildren(
                                        readable, combinedFileRange, allocate));
            }
        }
    }

    /**
     * Read data for this range and populate the buffer.
     *
     * @param range range of data to read.
     * @param buffer buffer to fill.
     */
    private static void readSingleRange(
            VectoredReadable readable, FileRange range, ByteBuffer buffer) {
        if (range.getLength() == 0) {
            // a zero byte read.
            buffer.flip();
            range.getData().complete(buffer);
            return;
        }
        try {
            long position = range.getOffset();
            int length = range.getLength();
            // TODO support direct
            checkArgument(buffer.hasArray(), "Only support heap buffer now.");
            byte[] array = buffer.array();
            readable.preadFully(position, array, 0, length);
            range.getData().complete(buffer);
        } catch (Exception ex) {
            range.getData().completeExceptionally(ex);
        }
    }

    private static void readCombinedRangeAndUpdateChildren(
            VectoredReadable readable,
            CombinedFileRange combinedFileRange,
            IntFunction<ByteBuffer> allocate) {
        if (combinedFileRange.getUnderlying().size() == 1) {
            FileRange fileRange = combinedFileRange.getUnderlying().get(0);
            readSingleRange(readable, fileRange, allocate.apply(fileRange.getLength()));
            return;
        }

        try {
            long position = combinedFileRange.getOffset();
            int length = combinedFileRange.getLength();

            // TODO reuse it
            byte[] total = new byte[length];
            readable.preadFully(position, total, 0, length);

            for (FileRange child : combinedFileRange.getUnderlying()) {
                ByteBuffer buffer = allocate.apply(child.getLength());
                // TODO support direct
                checkArgument(buffer.hasArray(), "Only support heap buffer now.");
                System.arraycopy(
                        total,
                        (int) (child.getOffset() - position),
                        buffer.array(),
                        0,
                        child.getLength());
                child.getData().complete(buffer);
            }
        } catch (Exception ex) {
            // complete exception all the underlying ranges which have not already
            // finished.
            for (FileRange child : combinedFileRange.getUnderlying()) {
                if (!child.getData().isDone()) {
                    child.getData().completeExceptionally(ex);
                }
            }
        }
    }

    /**
     * Is the given input list.
     *
     * <ul>
     *   <li>already sorted by offset
     *   <li>each range is more than minimumSeek apart
     *   <li>the start and end of each range is a multiple of chunkSize
     * </ul>
     *
     * @param input the list of input ranges.
     * @param minimumSeek the minimum distance between ranges.
     * @return true if we can use the input list as is.
     */
    private static boolean isOrderedDisjoint(List<? extends FileRange> input, int minimumSeek) {
        long previous = -minimumSeek;
        for (FileRange range : input) {
            long offset = range.getOffset();
            long end = range.getOffset() + range.getLength();
            if (offset - previous < minimumSeek) {
                return false;
            }
            previous = end;
        }
        return true;
    }

    /**
     * Validate a list of ranges (including overlapping checks) and return the sorted list.
     *
     * <p>Two ranges overlap when the start offset of second is less than the end offset of first.
     * End offset is calculated as start offset + length.
     *
     * @param input input list
     * @return a new sorted list.
     * @throws IllegalArgumentException if there are overlapping ranges or a range element is
     *     invalid (other than with negative offset)
     * @throws EOFException if the last range extends beyond the end of the file supplied or a range
     *     offset is negative
     */
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

    /**
     * Sort the input ranges by offset; no validation is done.
     *
     * @param input input ranges.
     * @return a new list of the ranges, sorted by offset.
     */
    private static List<? extends FileRange> sortRanges(List<? extends FileRange> input) {
        final List<? extends FileRange> l = new ArrayList<>(input);
        l.sort(Comparator.comparingLong(FileRange::getOffset));
        return l;
    }

    /**
     * Merge sorted ranges to optimize the access from the underlying file system. The motivations
     * are that:
     *
     * <ul>
     *   <li>Upper layers want to pass down logical file ranges.
     *   <li>Fewer reads have better performance.
     *   <li>Applications want callbacks as ranges are read.
     *   <li>Some file systems want to round ranges to be at checksum boundaries.
     * </ul>
     *
     * @param sortedRanges already sorted list of ranges based on offset.
     * @param minimumSeek the smallest gap that we should seek over in bytes
     * @param maxSize the largest combined file range in bytes
     * @return the list of sorted CombinedFileRanges that cover the input
     */
    private static List<CombinedFileRange> mergeSortedRanges(
            List<? extends FileRange> sortedRanges, int minimumSeek, int maxSize) {

        CombinedFileRange current = null;
        List<CombinedFileRange> result = new ArrayList<>(sortedRanges.size());

        // now merge together the ones that merge
        for (FileRange range : sortedRanges) {
            long start = range.getOffset();
            long end = range.getOffset() + range.getLength();
            if (current == null || !current.merge(start, end, range, minimumSeek, maxSize)) {
                current = new CombinedFileRange(start, end, range);
                result.add(current);
            }
        }
        return result;
    }

    /**
     * A file range that represents a set of underlying file ranges. This is used when we combine
     * the user's FileRange objects together into a single read for efficiency.
     *
     * <p>This class is not part of the public API; it MAY BE used as a parameter to vector IO
     * operations in FileSystem implementation code (and is)
     */
    private static class CombinedFileRange extends FileRange.FileRangeImpl {

        private final List<FileRange> underlying = new ArrayList<>();

        /** Total size of the data in the underlying ranges. */
        private long dataSize;

        public CombinedFileRange(long offset, long end, FileRange original) {
            super(offset, (int) (end - offset));
            append(original);
        }

        /**
         * Add a range to the underlying list; update the {@link #dataSize} field in the process.
         *
         * @param range range.
         */
        private void append(final FileRange range) {
            this.underlying.add(range);
            dataSize += range.getLength();
        }

        /**
         * Get the list of ranges that were merged together to form this one.
         *
         * @return the list of input ranges
         */
        public List<FileRange> getUnderlying() {
            return underlying;
        }

        /**
         * Merge this input range into the current one, if it is compatible. It is assumed that
         * otherOffset is greater or equal the current offset, which typically happens by sorting
         * the input ranges on offset.
         *
         * @param otherOffset the offset to consider merging
         * @param otherEnd the end to consider merging
         * @param other the underlying FileRange to add if we merge
         * @param minSeek the minimum distance that we'll seek without merging the ranges together
         * @param maxSize the maximum size that we'll merge into a single range
         * @return true if we have merged the range into this one
         */
        public boolean merge(
                long otherOffset, long otherEnd, FileRange other, int minSeek, int maxSize) {
            long end = this.getOffset() + this.getLength();
            long newEnd = Math.max(end, otherEnd);
            if (otherOffset - end >= minSeek || newEnd - this.getOffset() > maxSize) {
                return false;
            }
            this.setLength((int) (newEnd - this.getOffset()));
            append(other);
            return true;
        }

        @Override
        public String toString() {
            return super.toString()
                    + String.format("; range count=%d, data size=%,d", underlying.size(), dataSize);
        }
    }
}
