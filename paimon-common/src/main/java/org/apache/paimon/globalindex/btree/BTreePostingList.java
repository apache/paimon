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

import javax.annotation.Nullable;

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
            MemorySliceOutput output = new MemorySliceOutput(1 + varLenSize(rowId));
            output.writeByte(SINGLE);
            output.writeVarLenLong(rowId);
            return output.toSlice().getHeapMemory();
        }

        EncodingSizes sizes = estimateEncodingSizes(rowIds);
        if (sizes.roaringLowerBound >= sizes.deltaList) {
            return serializeDeltaList(rowIds, sizes.deltaList);
        }

        byte[] roaring = serializeRoaring(rowIds);
        return roaring.length < sizes.deltaList
                ? roaring
                : serializeDeltaList(rowIds, sizes.deltaList);
    }

    static void addTo(
            MemorySlice slice,
            RoaringNavigableMap64 target,
            @Nullable RoaringNavigableMap64 rowIdFilter)
            throws IOException {
        MemorySliceInput input = slice.toInput();
        int type = input.readUnsignedByte();
        switch (type) {
            case SINGLE:
                long rowId = input.readVarLenLong();
                if (rowIdFilter == null || rowIdFilter.contains(rowId)) {
                    target.add(rowId);
                }
                return;
            case DELTA_LIST:
                addDeltaList(input, target, rowIdFilter);
                return;
            case ROARING:
                RoaringNavigableMap64 bitmap = readRoaring(input);
                if (rowIdFilter != null) {
                    bitmap.and(rowIdFilter);
                }
                target.or(bitmap);
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

    private static EncodingSizes estimateEncodingSizes(LongArrayList rowIds) {
        int deltaListSize = 1 + varLenSize(rowIds.size());
        long previous = rowIds.get(0);
        checkNonNegative(previous);
        deltaListSize += varLenSize(previous);

        long roaringLowerBound = 1L + Long.BYTES + Integer.BYTES + Integer.BYTES;
        long currentHigh = previous >>> 32;
        long currentContainer = previous >>> 16;
        int containerCardinality = 1;
        int containerRuns = 1;
        for (int i = 1; i < rowIds.size(); i++) {
            long current = rowIds.get(i);
            checkIncreasing(current, previous);
            deltaListSize += varLenSize(current - previous);

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
        return new EncodingSizes(deltaListSize, roaringLowerBound);
    }

    private static byte[] serializeDeltaList(LongArrayList rowIds, int serializedSize) {
        MemorySliceOutput output = new MemorySliceOutput(serializedSize);
        output.writeByte(DELTA_LIST);
        output.writeVarLenInt(rowIds.size());
        long previous = rowIds.get(0);
        output.writeVarLenLong(previous);
        for (int i = 1; i < rowIds.size(); i++) {
            long current = rowIds.get(i);
            output.writeVarLenLong(current - previous);
            previous = current;
        }
        checkState(
                output.size() == serializedSize,
                "Unexpected delta BTree posting list size: %s instead of %s",
                output.size(),
                serializedSize);
        return output.toSlice().getHeapMemory();
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

    private static void addDeltaList(
            MemorySliceInput input,
            RoaringNavigableMap64 target,
            @Nullable RoaringNavigableMap64 rowIdFilter) {
        int count = readDeltaCount(input);
        long rowId = input.readVarLenLong();
        if (rowIdFilter == null || rowIdFilter.contains(rowId)) {
            target.add(rowId);
        }
        for (int i = 1; i < count; i++) {
            rowId += readPositiveDelta(input);
            if (rowIdFilter == null || rowIdFilter.contains(rowId)) {
                target.add(rowId);
            }
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
        MemorySlice payload = input.readSlice(input.available());
        byte[] heapMemory = payload.getHeapMemory();
        if (heapMemory == null) {
            bitmap.deserialize(payload.copyBytes());
        } else {
            bitmap.deserialize(heapMemory, payload.offset(), payload.length());
        }
        checkState(!bitmap.isEmpty(), "Invalid empty Roaring BTree posting list.");
        return bitmap;
    }

    private static long[] first(RoaringNavigableMap64 bitmap, int maxRowIds) {
        return bitmap.toArray(maxRowIds);
    }

    private static int varLenSize(long value) {
        int size = 1;
        while ((value & ~0x7FL) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
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

    private static class EncodingSizes {
        private final int deltaList;
        private final long roaringLowerBound;

        private EncodingSizes(int deltaList, long roaringLowerBound) {
            this.deltaList = deltaList;
            this.roaringLowerBound = roaringLowerBound;
        }
    }
}
