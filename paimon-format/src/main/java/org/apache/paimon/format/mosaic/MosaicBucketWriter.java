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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_ALL_NULL;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_CONST;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_DICT;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_PLAIN;
import static org.apache.paimon.format.mosaic.MosaicUtils.bitWidth;
import static org.apache.paimon.format.mosaic.MosaicUtils.getFixedWidth;
import static org.apache.paimon.format.mosaic.MosaicUtils.readVarint;
import static org.apache.paimon.format.mosaic.MosaicUtils.varintSize;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeBitPacked;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeLong;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeVarint;

/**
 * Columnar bucket writer for the Mosaic format. Buffers values per-column and produces a
 * column-oriented byte array with CONST/DICT/PLAIN/ALL_NULL encoding per column.
 *
 * <p>CONST detection uses a lightweight byte-comparison tracker that works for all types and value
 * sizes, independent of dictionary tracking. Dictionary tracking uses primitive long keys for
 * fixed-width types (≤8 bytes) and byte-array keys for variable-width types. Variable-width dict
 * tracking is bounded by a cumulative byte budget ({@link MosaicOptions#DICT_MAX_TOTAL_BYTES}).
 * DICT encoding is chosen only when it produces fewer bytes than PLAIN (cost-based selection).
 */
public class MosaicBucketWriter {

    private final FieldGetter[] fieldGetters;
    private final int numColumns;
    private final int[] fixedWidths;
    private final int maxDictTotalBytes;
    private final int maxDictEntries;

    // Per-column buffers
    private byte[][] nullBitmaps;
    private byte[][] valueBuffers;
    private int[] valueBufPos;
    private int[] nonNullCounts;

    // CONST tracking: byte comparison against first non-null value (works for any size)
    private boolean[] constTracking;
    private int[] firstValueLen;

    // Fixed-width ≤8 bytes: primitive long-based dict tracking
    private Map<Long, Integer>[] longDictMaps;
    // Variable-width and width>8: byte-array-based dict tracking with cumulative budget
    private Map<ByteKey, Integer>[] byteDictMaps;
    private int[] dictTotalBytes;

    private int numRows;

    public MosaicBucketWriter(
            RowType fullRowType,
            int[] globalColumnIndices,
            int maxDictTotalBytes,
            int maxDictEntries) {
        this.numColumns = globalColumnIndices.length;
        this.fieldGetters = new FieldGetter[numColumns];
        this.fixedWidths = new int[numColumns];
        this.maxDictTotalBytes = maxDictTotalBytes;
        this.maxDictEntries = maxDictEntries;

        for (int i = 0; i < numColumns; i++) {
            int globalIdx = globalColumnIndices[i];
            DataType type = fullRowType.getTypeAt(globalIdx);
            fieldGetters[i] = InternalRow.createFieldGetter(type, globalIdx);
            fixedWidths[i] = getFixedWidth(type);
        }

        initBuffers();
    }

    @SuppressWarnings("unchecked")
    private void initBuffers() {
        this.nullBitmaps = new byte[numColumns][];
        this.valueBuffers = new byte[numColumns][];
        this.valueBufPos = new int[numColumns];
        this.nonNullCounts = new int[numColumns];
        this.constTracking = new boolean[numColumns];
        this.firstValueLen = new int[numColumns];
        this.longDictMaps = new Map[numColumns];
        this.byteDictMaps = new Map[numColumns];
        this.dictTotalBytes = new int[numColumns];

        for (int i = 0; i < numColumns; i++) {
            nullBitmaps[i] = new byte[128];
            valueBuffers[i] = new byte[1024];
            constTracking[i] = true;
            if (usesLongDict(i)) {
                longDictMaps[i] = new HashMap<>();
            } else {
                byteDictMaps[i] = new HashMap<>();
            }
        }
        this.numRows = 0;
    }

    private boolean usesLongDict(int colIdx) {
        return fixedWidths[colIdx] > 0 && fixedWidths[colIdx] <= 8;
    }

    public boolean isEmpty() {
        return numRows == 0;
    }

    public int writeRow(InternalRow row) {
        int bitmapIdx = numRows / 8;

        int totalSize = 0;
        for (int i = 0; i < numColumns; i++) {
            // Ensure null bitmap capacity
            if (bitmapIdx >= nullBitmaps[i].length) {
                byte[] newBm = new byte[nullBitmaps[i].length * 2];
                System.arraycopy(nullBitmaps[i], 0, newBm, 0, nullBitmaps[i].length);
                nullBitmaps[i] = newBm;
            }

            Object value = fieldGetters[i].getFieldOrNull(row);
            if (value == null) {
                nullBitmaps[i][bitmapIdx] |= (byte) (1 << (numRows % 8));
            } else {
                nonNullCounts[i]++;
                int before = valueBufPos[i];
                writeValue(i, value);
                int written = valueBufPos[i] - before;
                totalSize += written;

                // CONST tracking: compare against first non-null value
                if (constTracking[i]) {
                    if (nonNullCounts[i] == 1) {
                        firstValueLen[i] = written;
                    } else if (written != firstValueLen[i]
                            || !equalsFirstValue(valueBuffers[i], before, written)) {
                        constTracking[i] = false;
                    }
                }

                // Dict tracking (separate from CONST)
                if (longDictMaps[i] != null) {
                    long key = extractFixedKey(valueBuffers[i], before, fixedWidths[i]);
                    longDictMaps[i].putIfAbsent(key, longDictMaps[i].size());
                    if (longDictMaps[i].size() > maxDictEntries) {
                        longDictMaps[i] = null;
                    }
                } else if (byteDictMaps[i] != null) {
                    ByteKey key = new ByteKey(valueBuffers[i], before, written);
                    int sizeBefore = byteDictMaps[i].size();
                    byteDictMaps[i].putIfAbsent(key, sizeBefore);
                    if (byteDictMaps[i].size() > sizeBefore) {
                        dictTotalBytes[i] += written;
                    }
                    if (byteDictMaps[i].size() > maxDictEntries
                            || dictTotalBytes[i] > maxDictTotalBytes) {
                        byteDictMaps[i] = null;
                    }
                }
            }
        }
        numRows++;
        // Include null bitmap overhead (~1 bit per column per row)
        totalSize += (numColumns + 7) / 8;
        return totalSize;
    }

    public byte[] finish() {
        if (numRows == 0) {
            return new byte[0];
        }

        // 1. Determine encoding per column
        byte[] encodings = new byte[numColumns];
        boolean[] hasNulls = new boolean[numColumns];

        for (int i = 0; i < numColumns; i++) {
            if (nonNullCounts[i] == 0) {
                encodings[i] = ENCODING_ALL_NULL;
                hasNulls[i] = false;
            } else if (constTracking[i]) {
                encodings[i] = ENCODING_CONST;
                hasNulls[i] = nonNullCounts[i] < numRows;
            } else {
                int dictSize = getDictSize(i);
                if (dictSize >= 2
                        && dictSize <= maxDictEntries
                        && dictEncodedSize(i) < valueBufPos[i]) {
                    encodings[i] = ENCODING_DICT;
                } else {
                    encodings[i] = ENCODING_PLAIN;
                }
                hasNulls[i] = nonNullCounts[i] < numRows;
            }
        }

        // 2. Compute exact output size
        byte[] out = computeOutBuffer(numColumns, encodings, hasNulls);
        int pos = 0;

        // 2a. Encoding flags: 2 bits per column
        int encodingFlagsBytes = (numColumns * 2 + 7) / 8;
        for (int i = 0; i < numColumns; i++) {
            int byteIdx = (i * 2) / 8;
            int bitIdx = (i * 2) % 8;
            out[pos + byteIdx] |= (byte) (encodings[i] << bitIdx);
        }
        pos += encodingFlagsBytes;

        // 2b. Has-nulls flags: 1 bit per column
        int hasNullsFlagsBytes = (numColumns + 7) / 8;
        for (int i = 0; i < numColumns; i++) {
            if (hasNulls[i]) {
                out[pos + i / 8] |= (byte) (1 << (i % 8));
            }
        }
        pos += hasNullsFlagsBytes;

        // 2c. Const metadata — first non-null value from value buffer
        for (int i = 0; i < numColumns; i++) {
            if (encodings[i] == ENCODING_CONST) {
                System.arraycopy(valueBuffers[i], 0, out, pos, firstValueLen[i]);
                pos += firstValueLen[i];
            }
        }

        // 2d. Dict metadata
        for (int i = 0; i < numColumns; i++) {
            if (encodings[i] == ENCODING_DICT) {
                if (longDictMaps[i] != null) {
                    int numEntries = longDictMaps[i].size();
                    pos = writeVarint(out, pos, numEntries);
                    int w = fixedWidths[i];
                    long[] keys = new long[numEntries];
                    for (Map.Entry<Long, Integer> e : longDictMaps[i].entrySet()) {
                        keys[e.getValue()] = e.getKey();
                    }
                    for (int j = 0; j < numEntries; j++) {
                        pos = writeFixedKey(out, pos, keys[j], w);
                    }
                } else {
                    int numEntries = byteDictMaps[i].size();
                    pos = writeVarint(out, pos, numEntries);
                    ByteKey[] keys = new ByteKey[numEntries];
                    for (Map.Entry<ByteKey, Integer> e : byteDictMaps[i].entrySet()) {
                        keys[e.getValue()] = e.getKey();
                    }
                    for (int j = 0; j < numEntries; j++) {
                        System.arraycopy(keys[j].data, 0, out, pos, keys[j].data.length);
                        pos += keys[j].data.length;
                    }
                }
            }
        }

        // 2e. Null bitmaps (only for cols with nulls and not ALL_NULL)
        int nullBitmapBytes = (numRows + 7) / 8;
        for (int i = 0; i < numColumns; i++) {
            if (hasNulls[i] && encodings[i] != ENCODING_ALL_NULL) {
                System.arraycopy(nullBitmaps[i], 0, out, pos, nullBitmapBytes);
                pos += nullBitmapBytes;
            }
        }

        // 2f. Column data
        for (int i = 0; i < numColumns; i++) {
            if (encodings[i] == ENCODING_PLAIN) {
                System.arraycopy(valueBuffers[i], 0, out, pos, valueBufPos[i]);
                pos += valueBufPos[i];
            } else if (encodings[i] == ENCODING_DICT) {
                int w = fixedWidths[i];
                int bw = bitWidth(getDictSize(i));
                int bitOffset = 0;
                int valPos = 0;
                for (int r = 0; r < numRows; r++) {
                    boolean isNull = (nullBitmaps[i][r / 8] & (1 << (r % 8))) != 0;
                    if (!isNull) {
                        int idx;
                        if (longDictMaps[i] != null) {
                            long key = extractFixedKey(valueBuffers[i], valPos, w);
                            valPos += w;
                            idx = longDictMaps[i].get(key);
                        } else {
                            int valueLen;
                            if (w > 0) {
                                valueLen = w;
                            } else {
                                int varLen = readVarint(valueBuffers[i], valPos);
                                valueLen = varintSize(varLen) + varLen;
                            }
                            ByteKey key = new ByteKey(valueBuffers[i], valPos, valueLen);
                            valPos += valueLen;
                            idx = byteDictMaps[i].get(key);
                        }
                        writeBitPacked(out, pos, bitOffset, idx, bw);
                        bitOffset += bw;
                    }
                }
                pos += (bitOffset + 7) / 8;
            }
            // CONST and ALL_NULL: no column data
        }

        return out;
    }

    private byte[] computeOutBuffer(int numOutputCols, byte[] encodings, boolean[] hasNulls) {
        int nullBitmapBytesPerCol = (numRows + 7) / 8;
        int exactSize = (numOutputCols * 2 + 7) / 8 + (numOutputCols + 7) / 8;
        for (int i = 0; i < numColumns; i++) {
            if (encodings[i] == ENCODING_ALL_NULL) {
                continue;
            }
            if (hasNulls[i]) {
                exactSize += nullBitmapBytesPerCol;
            }
            if (encodings[i] == ENCODING_CONST) {
                exactSize += firstValueLen[i];
            } else if (encodings[i] == ENCODING_DICT) {
                int numEntries;
                if (longDictMaps[i] != null) {
                    numEntries = longDictMaps[i].size();
                    exactSize += varintSize(numEntries) + numEntries * fixedWidths[i];
                } else {
                    numEntries = byteDictMaps[i].size();
                    exactSize += varintSize(numEntries);
                    for (ByteKey key : byteDictMaps[i].keySet()) {
                        exactSize += key.data.length;
                    }
                }
                exactSize += (nonNullCounts[i] * bitWidth(numEntries) + 7) / 8;
            } else if (encodings[i] == ENCODING_PLAIN) {
                exactSize += valueBufPos[i];
            }
        }
        return new byte[exactSize];
    }

    private int getDictSize(int colIdx) {
        if (longDictMaps[colIdx] != null) {
            return longDictMaps[colIdx].size();
        }
        if (byteDictMaps[colIdx] != null) {
            return byteDictMaps[colIdx].size();
        }
        return -1;
    }

    /** Compare dict encoded size vs plain size (pre-compression). */
    private int dictEncodedSize(int colIdx) {
        int numEntries;
        int entryBytes;
        if (longDictMaps[colIdx] != null) {
            numEntries = longDictMaps[colIdx].size();
            entryBytes = numEntries * fixedWidths[colIdx];
        } else if (byteDictMaps[colIdx] != null) {
            numEntries = byteDictMaps[colIdx].size();
            entryBytes = 0;
            for (ByteKey key : byteDictMaps[colIdx].keySet()) {
                entryBytes += key.data.length;
            }
        } else {
            return Integer.MAX_VALUE;
        }
        int indexBytes = (nonNullCounts[colIdx] * bitWidth(numEntries) + 7) / 8;
        return varintSize(numEntries) + entryBytes + indexBytes;
    }

    public void reset() {
        for (int i = 0; i < numColumns; i++) {
            Arrays.fill(nullBitmaps[i], (byte) 0);
            valueBufPos[i] = 0;
            nonNullCounts[i] = 0;
            constTracking[i] = true;
            firstValueLen[i] = 0;
            dictTotalBytes[i] = 0;
            if (usesLongDict(i)) {
                if (longDictMaps[i] != null) {
                    longDictMaps[i].clear();
                } else {
                    longDictMaps[i] = new HashMap<>();
                }
            } else {
                if (byteDictMaps[i] != null) {
                    byteDictMaps[i].clear();
                } else {
                    byteDictMaps[i] = new HashMap<>();
                }
            }
        }
        numRows = 0;
    }

    // ======================== Value writing ========================

    private void writeValue(int colIdx, Object value) {
        int w = fixedWidths[colIdx];
        if (w > 0) {
            ensureValueCapacity(colIdx, w);
            writeFixedValue(valueBuffers[colIdx], valueBufPos[colIdx], value, w);
            valueBufPos[colIdx] += w;
        } else {
            writeVariableValue(colIdx, value);
        }
    }

    private static void writeFixedValue(byte[] buf, int pos, Object value, int width) {
        switch (width) {
            case 1:
                if (value instanceof Boolean) {
                    buf[pos] = (byte) ((Boolean) value ? 1 : 0);
                } else {
                    buf[pos] = (Byte) value;
                }
                break;
            case 2:
                {
                    short v = (Short) value;
                    buf[pos] = (byte) (v >>> 8);
                    buf[pos + 1] = (byte) v;
                    break;
                }
            case 4:
                {
                    int v;
                    if (value instanceof Float) {
                        v = Float.floatToRawIntBits((Float) value);
                    } else {
                        v = (Integer) value;
                    }
                    buf[pos] = (byte) (v >>> 24);
                    buf[pos + 1] = (byte) (v >>> 16);
                    buf[pos + 2] = (byte) (v >>> 8);
                    buf[pos + 3] = (byte) v;
                    break;
                }
            case 8:
                {
                    long v;
                    if (value instanceof Long) {
                        v = (Long) value;
                    } else if (value instanceof Double) {
                        v = Double.doubleToRawLongBits((Double) value);
                    } else if (value instanceof Decimal) {
                        v = ((Decimal) value).toUnscaledLong();
                    } else if (value instanceof Timestamp) {
                        v = ((Timestamp) value).getMillisecond();
                    } else {
                        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
                    }
                    writeLong(buf, pos, v);
                    break;
                }
            case 12:
                {
                    Timestamp ts = (Timestamp) value;
                    long millis = ts.getMillisecond();
                    int nanos = ts.getNanoOfMillisecond();
                    writeLong(buf, pos, millis);
                    buf[pos + 8] = (byte) (nanos >>> 24);
                    buf[pos + 9] = (byte) (nanos >>> 16);
                    buf[pos + 10] = (byte) (nanos >>> 8);
                    buf[pos + 11] = (byte) nanos;
                    break;
                }
            default:
                break;
        }
    }

    private void writeVariableValue(int colIdx, Object value) {
        byte[] bytes;
        if (value instanceof BinaryString) {
            bytes = ((BinaryString) value).toBytes();
        } else if (value instanceof byte[]) {
            bytes = (byte[]) value;
        } else if (value instanceof Decimal) {
            bytes = ((Decimal) value).toUnscaledBytes();
        } else {
            throw new UnsupportedOperationException("Unsupported variable-width type: " + value);
        }
        ensureValueCapacity(colIdx, 5 + bytes.length);
        valueBufPos[colIdx] = writeVarint(valueBuffers[colIdx], valueBufPos[colIdx], bytes.length);
        System.arraycopy(bytes, 0, valueBuffers[colIdx], valueBufPos[colIdx], bytes.length);
        valueBufPos[colIdx] += bytes.length;
    }

    // ======================== Fixed-width key helpers ========================

    private static long extractFixedKey(byte[] buf, int pos, int width) {
        switch (width) {
            case 1:
                return buf[pos] & 0xFFL;
            case 2:
                return ((buf[pos] & 0xFFL) << 8) | (buf[pos + 1] & 0xFFL);
            case 4:
                return ((buf[pos] & 0xFFL) << 24)
                        | ((buf[pos + 1] & 0xFFL) << 16)
                        | ((buf[pos + 2] & 0xFFL) << 8)
                        | (buf[pos + 3] & 0xFFL);
            case 8:
                return ((buf[pos] & 0xFFL) << 56)
                        | ((buf[pos + 1] & 0xFFL) << 48)
                        | ((buf[pos + 2] & 0xFFL) << 40)
                        | ((buf[pos + 3] & 0xFFL) << 32)
                        | ((buf[pos + 4] & 0xFFL) << 24)
                        | ((buf[pos + 5] & 0xFFL) << 16)
                        | ((buf[pos + 6] & 0xFFL) << 8)
                        | (buf[pos + 7] & 0xFFL);
            default:
                return 0;
        }
    }

    private static int writeFixedKey(byte[] buf, int pos, long key, int width) {
        switch (width) {
            case 1:
                buf[pos++] = (byte) key;
                break;
            case 2:
                buf[pos++] = (byte) (key >>> 8);
                buf[pos++] = (byte) key;
                break;
            case 4:
                buf[pos++] = (byte) (key >>> 24);
                buf[pos++] = (byte) (key >>> 16);
                buf[pos++] = (byte) (key >>> 8);
                buf[pos++] = (byte) key;
                break;
            case 8:
                buf[pos++] = (byte) (key >>> 56);
                buf[pos++] = (byte) (key >>> 48);
                buf[pos++] = (byte) (key >>> 40);
                buf[pos++] = (byte) (key >>> 32);
                buf[pos++] = (byte) (key >>> 24);
                buf[pos++] = (byte) (key >>> 16);
                buf[pos++] = (byte) (key >>> 8);
                buf[pos++] = (byte) key;
                break;
            default:
                break;
        }
        return pos;
    }

    // ======================== Buffer helpers ========================

    private void ensureValueCapacity(int colIdx, int additional) {
        int required = valueBufPos[colIdx] + additional;
        if (required > valueBuffers[colIdx].length) {
            int newLen = Math.max(valueBuffers[colIdx].length * 2, required);
            byte[] newBuf = new byte[newLen];
            System.arraycopy(valueBuffers[colIdx], 0, newBuf, 0, valueBufPos[colIdx]);
            valueBuffers[colIdx] = newBuf;
        }
    }

    private static boolean equalsFirstValue(byte[] buf, int off, int len) {
        for (int i = 0; i < len; i++) {
            if (buf[i] != buf[off + i]) {
                return false;
            }
        }
        return true;
    }

    // ======================== ByteKey ========================

    /** Immutable byte array wrapper with value-based hash and equals for dict tracking. */
    static final class ByteKey {
        final byte[] data;
        private final int hash;

        ByteKey(byte[] source, int offset, int length) {
            this.data = new byte[length];
            System.arraycopy(source, offset, this.data, 0, length);
            int h = 1;
            for (int i = 0; i < length; i++) {
                h = 31 * h + this.data[i];
            }
            this.hash = h;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ByteKey)) {
                return false;
            }
            return Arrays.equals(data, ((ByteKey) obj).data);
        }
    }
}
