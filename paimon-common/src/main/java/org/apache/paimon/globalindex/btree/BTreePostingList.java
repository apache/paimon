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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.LongArrayList;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Self-describing adaptive encoding for the row ids associated with one BTree key.
 *
 * <p>Singletons have a dedicated representation. Larger postings use the smaller serialized form of
 * a delta list and a Roaring bitmap. A conservative lower bound avoids constructing the Roaring
 * candidate when it cannot be smaller.
 */
final class BTreePostingList {

    static final int SINGLE = 0;
    static final int DELTA_LIST = 1;
    static final int ROARING = 2;

    private static final int MIN_ROARING_RANGE_LENGTH = 64;

    private BTreePostingList() {}

    static byte[] serialize(LongArrayList rowIds) throws IOException {
        checkArgument(!rowIds.isEmpty(), "Cannot serialize an empty BTree posting list.");
        if (rowIds.size() == 1) {
            long rowId = rowIds.get(0);
            checkNonNegative(rowId);
            MemorySliceOutput output = new MemorySliceOutput(10);
            output.writeByte(SINGLE);
            output.writeVarLenLong(rowId);
            return output.toSlice().copyBytes();
        }

        EncodingCandidates candidates = serializeDeltaListAndEstimateRoaring(rowIds);
        if (candidates.roaringLowerBound >= candidates.deltaList.length) {
            return candidates.deltaList;
        }

        byte[] roaring = serializeRoaring(rowIds);
        return roaring.length < candidates.deltaList.length ? roaring : candidates.deltaList;
    }

    static void addTo(MemorySlice slice, RoaringNavigableMap64 target) throws IOException {
        MemorySliceInput input = slice.toInput();
        int type = input.readUnsignedByte();
        switch (type) {
            case SINGLE:
                target.add(input.readVarLenLong());
                return;
            case DELTA_LIST:
                addDeltaList(input, target);
                return;
            case ROARING:
                target.or(readRoaring(input));
                return;
            default:
                throw new IllegalStateException("Unknown BTree posting list type: " + type);
        }
    }

    static long[] deserialize(MemorySlice slice, int maxRowIds) throws IOException {
        checkArgument(maxRowIds >= 0, "Max row id count must not be negative.");
        MemorySliceInput input = slice.toInput();
        int type = input.readUnsignedByte();
        switch (type) {
            case SINGLE:
                return maxRowIds == 0 ? new long[0] : new long[] {input.readVarLenLong()};
            case DELTA_LIST:
                return deserializeDeltaList(input, maxRowIds);
            case ROARING:
                return first(readRoaring(input), maxRowIds);
            default:
                throw new IllegalStateException("Unknown BTree posting list type: " + type);
        }
    }

    private static EncodingCandidates serializeDeltaListAndEstimateRoaring(LongArrayList rowIds) {
        MemorySliceOutput output = new MemorySliceOutput(rowIds.size() + 10);
        output.writeByte(DELTA_LIST);
        output.writeVarLenInt(rowIds.size());
        long previous = rowIds.get(0);
        checkNonNegative(previous);
        output.writeVarLenLong(previous);

        long roaringLowerBound = 1L + Long.BYTES + Integer.BYTES + Integer.BYTES;
        long currentHigh = previous >>> 32;
        long currentContainer = previous >>> 16;
        int containerCardinality = 1;
        int containerRuns = 1;
        for (int i = 1; i < rowIds.size(); i++) {
            long current = rowIds.get(i);
            checkIncreasing(current, previous);
            output.writeVarLenLong(current - previous);

            long high = current >>> 32;
            long container = current >>> 16;
            if (container != currentContainer) {
                roaringLowerBound += containerLowerBound(containerCardinality, containerRuns);
                if (high != currentHigh) {
                    // High key plus the nested Roaring bitmap cookie.
                    roaringLowerBound += Integer.BYTES + Integer.BYTES;
                    currentHigh = high;
                }
                currentContainer = container;
                containerCardinality = 1;
                containerRuns = 1;
            } else {
                containerCardinality++;
                if (previous == Long.MAX_VALUE || current != previous + 1) {
                    containerRuns++;
                }
            }
            previous = current;
        }
        roaringLowerBound += containerLowerBound(containerCardinality, containerRuns);
        return new EncodingCandidates(output.toSlice().copyBytes(), roaringLowerBound);
    }

    private static long containerLowerBound(int cardinality, int runs) {
        long arrayBytes = 2L * cardinality;
        long bitmapBytes = 8192;
        long runBytes = 2L + 4L * runs;
        // Every container also needs a two-byte key and a two-byte cardinality.
        return Integer.BYTES + Math.min(Math.min(arrayBytes, bitmapBytes), runBytes);
    }

    private static byte[] serializeRoaring(LongArrayList rowIds) throws IOException {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        long rangeStart = rowIds.get(0);
        long previous = rangeStart;
        for (int i = 0; i < rowIds.size(); i++) {
            long current = rowIds.get(i);
            if (i > 0 && current != previous + 1) {
                addRange(bitmap, rangeStart, previous);
                rangeStart = current;
            }
            previous = current;
        }
        addRange(bitmap, rangeStart, previous);

        byte[] bytes = bitmap.serialize();
        byte[] result = new byte[bytes.length + 1];
        result[0] = ROARING;
        System.arraycopy(bytes, 0, result, 1, bytes.length);
        return result;
    }

    private static void addRange(RoaringNavigableMap64 bitmap, long from, long to) {
        if (to - from >= MIN_ROARING_RANGE_LENGTH - 1) {
            if (to == Long.MAX_VALUE) {
                bitmap.addRange(new Range(from, to - 1));
                bitmap.add(to);
            } else {
                bitmap.addRange(new Range(from, to));
            }
            return;
        }

        for (long rowId = from; ; rowId++) {
            bitmap.add(rowId);
            if (rowId == to) {
                return;
            }
        }
    }

    private static void addDeltaList(MemorySliceInput input, RoaringNavigableMap64 target) {
        int count = readDeltaCount(input);
        long rowId = input.readVarLenLong();
        target.add(rowId);
        for (int i = 1; i < count; i++) {
            rowId += readPositiveDelta(input);
            target.add(rowId);
        }
    }

    private static long[] deserializeDeltaList(MemorySliceInput input, int maxRowIds) {
        int count = readDeltaCount(input);
        int resultLength = Math.min(count, maxRowIds);
        if (resultLength == 0) {
            return new long[0];
        }

        long[] result = new long[resultLength];
        result[0] = input.readVarLenLong();
        for (int i = 1; i < resultLength; i++) {
            result[i] = result[i - 1] + readPositiveDelta(input);
        }
        return result;
    }

    private static int readDeltaCount(MemorySliceInput input) {
        int count = input.readVarLenInt();
        if (count <= 1) {
            throw new IllegalStateException("Invalid delta BTree posting list length: " + count);
        }
        return count;
    }

    private static long readPositiveDelta(MemorySliceInput input) {
        long delta = input.readVarLenLong();
        if (delta <= 0) {
            throw new IllegalStateException("Invalid non-positive BTree row id delta: " + delta);
        }
        return delta;
    }

    private static RoaringNavigableMap64 readRoaring(MemorySliceInput input) throws IOException {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.deserialize(input.readSlice(input.available()).copyBytes());
        checkState(!bitmap.isEmpty(), "Invalid empty Roaring BTree posting list.");
        return bitmap;
    }

    private static long[] first(RoaringNavigableMap64 bitmap, int maxRowIds) {
        return bitmap.toArray(maxRowIds);
    }

    private static void checkNonNegative(long rowId) {
        if (rowId < 0) {
            throw new IllegalArgumentException(
                    "BTree row id must be non-negative, but was " + rowId);
        }
    }

    private static void checkIncreasing(long current, long previous) {
        if (current <= previous) {
            throw new IllegalArgumentException(
                    "BTree row ids must be strictly increasing, but found "
                            + current
                            + " after "
                            + previous
                            + '.');
        }
    }

    private static class EncodingCandidates {
        private final byte[] deltaList;
        private final long roaringLowerBound;

        private EncodingCandidates(byte[] deltaList, long roaringLowerBound) {
            this.deltaList = deltaList;
            this.roaringLowerBound = roaringLowerBound;
        }
    }
}
