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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_ALL_NULL;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_CONST;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_DICT;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_PLAIN;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeVarint;

/**
 * Columnar bucket writer for the Mosaic v2 format. Buffers values per-column and produces a
 * column-oriented byte array with CONST/DICT/PLAIN/ALL_NULL encoding per column.
 */
public class MosaicBucketWriter {

    private final InternalRow.FieldGetter[] fieldGetters;
    private final int numColumns;
    private final int[] fixedWidths;
    private final boolean[] isVariableWidth;

    // Per-column buffers
    private byte[][] nullBitmaps;
    private byte[][] valueBuffers;
    private int[] valueBufPos;
    private int[] nonNullCounts;

    // Dict tracking for all columns (byte-level key comparison)
    private Map<ByteKey, Integer>[] dictMaps;

    private int numRows;

    public MosaicBucketWriter(RowType fullRowType, int[] globalColumnIndices) {
        this.numColumns = globalColumnIndices.length;
        this.fieldGetters = new InternalRow.FieldGetter[numColumns];
        this.fixedWidths = new int[numColumns];
        this.isVariableWidth = new boolean[numColumns];

        for (int i = 0; i < numColumns; i++) {
            int globalIdx = globalColumnIndices[i];
            DataType type = fullRowType.getTypeAt(globalIdx);
            fieldGetters[i] = InternalRow.createFieldGetter(type, globalIdx);
            fixedWidths[i] = getFixedWidth(type);
            isVariableWidth[i] = fixedWidths[i] < 0;
        }

        initBuffers();
    }

    @SuppressWarnings("unchecked")
    private void initBuffers() {
        this.nullBitmaps = new byte[numColumns][];
        this.valueBuffers = new byte[numColumns][];
        this.valueBufPos = new int[numColumns];
        this.nonNullCounts = new int[numColumns];
        this.dictMaps = new Map[numColumns];

        for (int i = 0; i < numColumns; i++) {
            nullBitmaps[i] = new byte[128];
            valueBuffers[i] = new byte[1024];
            dictMaps[i] = new HashMap<>();
        }
        this.numRows = 0;
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

                // Track in dict map (byte-level key from serialized value)
                if (dictMaps[i] != null && dictMaps[i].size() <= 255) {
                    ByteKey key = new ByteKey(valueBuffers[i], before, written);
                    dictMaps[i].putIfAbsent(key, dictMaps[i].size());
                }
            }
        }
        numRows++;
        return totalSize;
    }

    public byte[] finish() {
        return finish(false);
    }

    public byte[] finish(boolean pruneAllNull) {
        if (numRows == 0) {
            return new byte[0];
        }

        // 1. Determine encoding per column
        byte[] encodings = new byte[numColumns];
        boolean[] hasNulls = new boolean[numColumns];

        for (int i = 0; i < numColumns; i++) {
            hasNulls[i] = nonNullCounts[i] < numRows;
            Map<ByteKey, Integer> dictMap = dictMaps[i];
            if (nonNullCounts[i] == 0) {
                encodings[i] = ENCODING_ALL_NULL;
            } else if (dictMap != null && dictMap.size() == 1) {
                encodings[i] = ENCODING_CONST;
            } else if (dictMap != null && dictMap.size() <= 255) {
                encodings[i] = ENCODING_DICT;
            } else {
                encodings[i] = ENCODING_PLAIN;
            }
        }

        // Count output columns (skip ALL_NULL when pruning)
        int numOutputCols = numColumns;
        if (pruneAllNull) {
            numOutputCols = 0;
            for (int i = 0; i < numColumns; i++) {
                if (encodings[i] != ENCODING_ALL_NULL) {
                    numOutputCols++;
                }
            }
        }

        // 2. Compute exact output size
        byte[] out = computeOutBuffer(numOutputCols, encodings, hasNulls);
        int pos = 0;

        // 2a. Encoding flags: 2 bits per output column
        int encodingFlagsBytes = (numOutputCols * 2 + 7) / 8;
        int outputIdx = 0;
        for (int i = 0; i < numColumns; i++) {
            if (pruneAllNull && encodings[i] == ENCODING_ALL_NULL) {
                continue;
            }
            int byteIdx = (outputIdx * 2) / 8;
            int bitIdx = (outputIdx * 2) % 8;
            out[pos + byteIdx] |= (byte) (encodings[i] << bitIdx);
            outputIdx++;
        }
        pos += encodingFlagsBytes;

        // 2b. Has-nulls flags: 1 bit per output column
        int hasNullsFlagsBytes = (numOutputCols + 7) / 8;
        outputIdx = 0;
        for (int i = 0; i < numColumns; i++) {
            if (pruneAllNull && encodings[i] == ENCODING_ALL_NULL) {
                continue;
            }
            if (hasNulls[i]) {
                out[pos + outputIdx / 8] |= (byte) (1 << (outputIdx % 8));
            }
            outputIdx++;
        }
        pos += hasNullsFlagsBytes;

        // 2c. Const metadata — write the single distinct value's serialized bytes
        for (int i = 0; i < numColumns; i++) {
            if (encodings[i] == ENCODING_CONST) {
                ByteKey constKey = dictMaps[i].keySet().iterator().next();
                System.arraycopy(constKey.data, 0, out, pos, constKey.data.length);
                pos += constKey.data.length;
            }
        }

        // 2d. Dict metadata — write entry count + each entry's serialized bytes
        for (int i = 0; i < numColumns; i++) {
            if (encodings[i] == ENCODING_DICT) {
                int numEntries = dictMaps[i].size();
                pos = writeVarint(out, pos, numEntries);
                ByteKey[] keys = new ByteKey[numEntries];
                for (Map.Entry<ByteKey, Integer> e : dictMaps[i].entrySet()) {
                    keys[e.getValue()] = e.getKey();
                }
                for (int j = 0; j < numEntries; j++) {
                    System.arraycopy(keys[j].data, 0, out, pos, keys[j].data.length);
                    pos += keys[j].data.length;
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
                // 1-byte dict indices for non-null cells
                Map<ByteKey, Integer> dict = dictMaps[i];
                int w = fixedWidths[i];
                int valPos = 0;
                for (int r = 0; r < numRows; r++) {
                    boolean isNull = (nullBitmaps[i][r / 8] & (1 << (r % 8))) != 0;
                    if (!isNull) {
                        int valueLen;
                        if (w > 0) {
                            valueLen = w;
                        } else {
                            int varLen = readVarint(valueBuffers[i], valPos);
                            valueLen = varintSize(varLen) + varLen;
                        }
                        ByteKey key = new ByteKey(valueBuffers[i], valPos, valueLen);
                        valPos += valueLen;
                        out[pos++] = (byte) (int) dict.get(key);
                    }
                }
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
                ByteKey constKey = dictMaps[i].keySet().iterator().next();
                exactSize += constKey.data.length;
            } else if (encodings[i] == ENCODING_DICT) {
                int numEntries = dictMaps[i].size();
                exactSize += varintSize(numEntries);
                for (ByteKey key : dictMaps[i].keySet()) {
                    exactSize += key.data.length;
                }
                exactSize += nonNullCounts[i]; // 1-byte index per non-null
            } else if (encodings[i] == ENCODING_PLAIN) {
                exactSize += valueBufPos[i];
            }
        }
        return new byte[exactSize];
    }

    public boolean[] getAllNullFlags() {
        boolean[] flags = new boolean[numColumns];
        for (int i = 0; i < numColumns; i++) {
            flags[i] = nonNullCounts[i] == 0;
        }
        return flags;
    }

    public void reset() {
        for (int i = 0; i < numColumns; i++) {
            Arrays.fill(nullBitmaps[i], (byte) 0);
            valueBufPos[i] = 0;
            nonNullCounts[i] = 0;
            if (dictMaps[i] != null) {
                dictMaps[i].clear();
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

    private static void writeLong(byte[] buf, int pos, long v) {
        buf[pos] = (byte) (v >>> 56);
        buf[pos + 1] = (byte) (v >>> 48);
        buf[pos + 2] = (byte) (v >>> 40);
        buf[pos + 3] = (byte) (v >>> 32);
        buf[pos + 4] = (byte) (v >>> 24);
        buf[pos + 5] = (byte) (v >>> 16);
        buf[pos + 6] = (byte) (v >>> 8);
        buf[pos + 7] = (byte) v;
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

    // ======================== Type width ========================

    static int getFixedWidth(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case FLOAT:
                return 4;
            case BIGINT:
            case DOUBLE:
                return 8;
            case DECIMAL:
                if (Decimal.isCompact(((DecimalType) type).getPrecision())) {
                    return 8;
                }
                return -1;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (Timestamp.isCompact(((TimestampType) type).getPrecision())) {
                    return 8;
                }
                return 12;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (Timestamp.isCompact(((LocalZonedTimestampType) type).getPrecision())) {
                    return 8;
                }
                return 12;
            default:
                return -1;
        }
    }

    // ======================== Varint helpers ========================

    private static int readVarint(byte[] buf, int pos) {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = buf[pos++] & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    private static int varintSize(int value) {
        int size = 1;
        while ((value & ~0x7F) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    // ======================== ByteKey ========================

    /** Immutable byte array wrapper with value-based hash and equals for dict tracking. */
    private static final class ByteKey {
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
