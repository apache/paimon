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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/* This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A byte range of a file. */
public interface FileRange {

    /** Get the starting offset of the range. */
    long getOffset();

    /** Get the length of the range. */
    int getLength();

    /** Get the future data for this range. */
    CompletableFuture<ByteBuffer> getData();

    /** Set a future for this range's data. */
    void setData(CompletableFuture<ByteBuffer> data);

    /**
     * Get any reference passed in to the file range constructor. This is not used by any
     * implementation code; it is to help bind this API to libraries retrieving multiple stripes of
     * data in parallel.
     *
     * @return a reference or null.
     */
    Object getReference();

    /**
     * Factory method to create a FileRange object.
     *
     * @param offset starting offset of the range.
     * @param length length of the range.
     * @return a new instance of FileRangeImpl.
     */
    static FileRange createFileRange(long offset, int length) {
        return new FileRangeImpl(offset, length, null);
    }

    /**
     * Factory method to create a FileRange object.
     *
     * @param offset starting offset of the range.
     * @param length length of the range.
     * @param reference nullable reference to store in the range.
     * @return a new instance of FileRangeImpl.
     */
    static FileRange createFileRange(long offset, int length, Object reference) {
        return new FileRangeImpl(offset, length, reference);
    }

    static void validateRangeRequest(FileRange range) throws EOFException {
        requireNonNull(range, "range is null");

        checkArgument(range.getLength() >= 0, "length is negative in %s", range);
        if (range.getOffset() < 0) {
            throw new EOFException("position is negative in range " + range);
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
     * @param chunkSize the size of the chunks that the offset and end must align to.
     * @param minimumSeek the minimum distance between ranges.
     * @return true if we can use the input list as is.
     */
    static boolean isOrderedDisjoint(
            List<? extends FileRange> input, int chunkSize, int minimumSeek) {
        long previous = -minimumSeek;
        for (FileRange range : input) {
            long offset = range.getOffset();
            long end = range.getOffset() + range.getLength();
            if (offset % chunkSize != 0
                    || end % chunkSize != 0
                    || (offset - previous < minimumSeek)) {
                return false;
            }
            previous = end;
        }
        return true;
    }

    /**
     * Calculates floor value of offset based on chunk size.
     *
     * @param offset file offset.
     * @param chunkSize file chunk size.
     * @return floor value.
     */
    static long roundDown(long offset, int chunkSize) {
        if (chunkSize > 1) {
            return offset - (offset % chunkSize);
        } else {
            return offset;
        }
    }

    /**
     * Calculates the ceiling value of offset based on chunk size.
     *
     * @param offset file offset.
     * @param chunkSize file chunk size.
     * @return ceil value.
     */
    static long roundUp(long offset, int chunkSize) {
        if (chunkSize > 1) {
            long next = offset + chunkSize - 1;
            return next - (next % chunkSize);
        } else {
            return offset;
        }
    }

    /**
     * Validate a list of ranges (including overlapping checks) and return the sorted list.
     *
     * <p>Two ranges overlap when the start offset of second is less than the end offset of first.
     * End offset is calculated as start offset + length.
     *
     * @param input input list
     * @param fileLength file length if known
     * @return a new sorted list.
     * @throws IllegalArgumentException if there are overlapping ranges or a range element is
     *     invalid (other than with negative offset)
     * @throws EOFException if the last range extends beyond the end of the file supplied or a range
     *     offset is negative
     */
    static List<? extends FileRange> validateAndSortRanges(
            final List<? extends FileRange> input, final Optional<Long> fileLength)
            throws EOFException {

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
        // at this point the final element in the list is the last range
        // so make sure it is not beyond the end of the file, if passed in.
        // where invalid is: starts at or after the end of the file
        if (fileLength.isPresent()) {
            final FileRange last = sortedRanges.get(sortedRanges.size() - 1);
            final Long l = fileLength.get();
            // this check is superfluous, but it allows for different exception message.
            if (last.getOffset() >= l) {
                throw new EOFException("Range starts beyond the file length (" + l + "): " + last);
            }
            if (last.getOffset() + last.getLength() > l) {
                throw new EOFException("Range extends beyond the file length (" + l + "): " + last);
            }
        }
        return sortedRanges;
    }

    /**
     * Sort the input ranges by offset; no validation is done.
     *
     * @param input input ranges.
     * @return a new list of the ranges, sorted by offset.
     */
    static List<? extends FileRange> sortRanges(List<? extends FileRange> input) {
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
     * @param chunkSize round the start and end points to multiples of chunkSize
     * @param minimumSeek the smallest gap that we should seek over in bytes
     * @param maxSize the largest combined file range in bytes
     * @return the list of sorted CombinedFileRanges that cover the input
     */
    static List<CombinedFileRange> mergeSortedRanges(
            List<? extends FileRange> sortedRanges, int chunkSize, int minimumSeek, int maxSize) {

        CombinedFileRange current = null;
        List<CombinedFileRange> result = new ArrayList<>(sortedRanges.size());

        // now merge together the ones that merge
        for (FileRange range : sortedRanges) {
            long start = roundDown(range.getOffset(), chunkSize);
            long end = roundUp(range.getOffset() + range.getLength(), chunkSize);
            if (current == null || !current.merge(start, end, range, minimumSeek, maxSize)) {
                current = new CombinedFileRange(start, end, range);
                result.add(current);
            }
        }
        return result;
    }

    /** An implementation for {@link FileRange}. */
    class FileRangeImpl implements FileRange {

        /** nullable reference to store in the range. */
        private final Object reference;

        private long offset;
        private int length;
        private CompletableFuture<ByteBuffer> reader;

        /**
         * Create.
         *
         * @param offset offset in file
         * @param length length of data to read.
         * @param reference nullable reference to store in the range.
         */
        public FileRangeImpl(long offset, int length, Object reference) {
            this.offset = offset;
            this.length = length;
            this.reference = reference;
        }

        @Override
        public String toString() {
            return "range[" + offset + "," + (offset + length) + ")";
        }

        @Override
        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        @Override
        public CompletableFuture<ByteBuffer> getData() {
            return reader;
        }

        @Override
        public void setData(CompletableFuture<ByteBuffer> pReader) {
            this.reader = pReader;
        }

        @Override
        public Object getReference() {
            return reference;
        }
    }

    /**
     * A file range that represents a set of underlying file ranges. This is used when we combine
     * the user's FileRange objects together into a single read for efficiency.
     *
     * <p>This class is not part of the public API; it MAY BE used as a parameter to vector IO
     * operations in FileSystem implementation code (and is)
     */
    class CombinedFileRange extends FileRangeImpl {

        private final List<FileRange> underlying = new ArrayList<>();

        /** Total size of the data in the underlying ranges. */
        private long dataSize;

        public CombinedFileRange(long offset, long end, FileRange original) {
            super(offset, (int) (end - offset), null);
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

        /**
         * Get the total amount of data which is actually useful; the difference between this and
         * {@link #getLength()} records how much data which will be discarded.
         *
         * @return a number greater than 0 and less than or equal to {@link #getLength()}.
         */
        public long getDataSize() {
            return dataSize;
        }
    }
}
