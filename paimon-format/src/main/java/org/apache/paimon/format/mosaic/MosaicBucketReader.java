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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.mosaic.MosaicSchema.TypeCast;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;

import javax.annotation.Nullable;

import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_ALL_NULL;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_CONST;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_DICT;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_PLAIN;
import static org.apache.paimon.format.mosaic.MosaicUtils.bitWidth;
import static org.apache.paimon.format.mosaic.MosaicUtils.getFixedWidth;
import static org.apache.paimon.format.mosaic.MosaicUtils.readBitPacked;
import static org.apache.paimon.format.mosaic.MosaicUtils.readInt;
import static org.apache.paimon.format.mosaic.MosaicUtils.readLong;
import static org.apache.paimon.format.mosaic.MosaicUtils.readVarint;
import static org.apache.paimon.format.mosaic.MosaicUtils.varintSize;

/**
 * Columnar bucket reader for the Mosaic format. Reads column-oriented data with
 * CONST/DICT/PLAIN/ALL_NULL encoding.
 */
public class MosaicBucketReader {

    private final DataType[] allColumnTypes;
    private final int[] localToOutputMapping;
    private final int numColumnsInBucket;
    private final @Nullable TypeCast[] casts;

    // Per-column state set during init()
    private byte[] encodings;
    private boolean[] hasNulls;
    private byte[][] nullBitmaps;
    private Object[] constValues;
    private Object[][] dictValues;
    private int[] dictBitWidths;
    private int[] dictBitOffsets;
    private int[] dataCursors;
    private byte[] data;
    private int numRows;
    private int currentRow;

    public MosaicBucketReader(
            DataType[] allColumnTypes, int[] localToOutputMapping, @Nullable TypeCast[] casts) {
        this.allColumnTypes = allColumnTypes;
        this.localToOutputMapping = localToOutputMapping;
        this.numColumnsInBucket = allColumnTypes.length;
        this.casts = casts;
    }

    public void init(byte[] data, int numRows) {
        this.data = data;
        this.numRows = numRows;
        this.currentRow = 0;

        this.encodings = new byte[numColumnsInBucket];
        this.hasNulls = new boolean[numColumnsInBucket];
        this.nullBitmaps = new byte[numColumnsInBucket][];
        this.constValues = new Object[numColumnsInBucket];
        this.dictValues = new Object[numColumnsInBucket][];
        this.dictBitWidths = new int[numColumnsInBucket];
        this.dictBitOffsets = new int[numColumnsInBucket];
        this.dataCursors = new int[numColumnsInBucket];

        int pos = 0;

        // 1. Read encoding flags (2 bits per column)
        int encodingFlagsBytes = (numColumnsInBucket * 2 + 7) / 8;
        for (int i = 0; i < numColumnsInBucket; i++) {
            int byteIdx = (i * 2) / 8;
            int bitIdx = (i * 2) % 8;
            encodings[i] = (byte) ((data[pos + byteIdx] >>> bitIdx) & 0x03);
        }
        pos += encodingFlagsBytes;

        // 2. Read has-nulls flags (1 bit per column)
        int hasNullsFlagsBytes = (numColumnsInBucket + 7) / 8;
        for (int i = 0; i < numColumnsInBucket; i++) {
            hasNulls[i] = (data[pos + i / 8] & (1 << (i % 8))) != 0;
        }
        pos += hasNullsFlagsBytes;

        // 3. Read const metadata
        for (int i = 0; i < numColumnsInBucket; i++) {
            if (encodings[i] == ENCODING_CONST) {
                constValues[i] = readValue(allColumnTypes[i], data, pos);
                pos += valueSize(allColumnTypes[i], data, pos);
                if (casts != null && casts[i] != null) {
                    constValues[i] = casts[i].cast(constValues[i]);
                }
            }
        }

        // 4. Read dict metadata
        for (int i = 0; i < numColumnsInBucket; i++) {
            if (encodings[i] == ENCODING_DICT) {
                int numEntries = readVarint(data, pos);
                pos += varintSize(numEntries);
                dictBitWidths[i] = bitWidth(numEntries);
                Object[] entries = new Object[numEntries];
                for (int j = 0; j < numEntries; j++) {
                    entries[j] = readValue(allColumnTypes[i], data, pos);
                    pos += valueSize(allColumnTypes[i], data, pos);
                }
                if (casts != null && casts[i] != null) {
                    for (int j = 0; j < numEntries; j++) {
                        entries[j] = casts[i].cast(entries[j]);
                    }
                }
                dictValues[i] = entries;
            }
        }

        // 5. Read null bitmaps
        int nullBitmapSize = (numRows + 7) / 8;
        for (int i = 0; i < numColumnsInBucket; i++) {
            if (hasNulls[i] && encodings[i] != ENCODING_ALL_NULL) {
                nullBitmaps[i] = new byte[nullBitmapSize];
                System.arraycopy(data, pos, nullBitmaps[i], 0, nullBitmapSize);
                pos += nullBitmapSize;
            }
        }

        // 6. Record column data start offsets
        for (int i = 0; i < numColumnsInBucket; i++) {
            dataCursors[i] = pos;
            dictBitOffsets[i] = 0;
            if (encodings[i] == ENCODING_PLAIN) {
                int w = getFixedWidth(allColumnTypes[i]);
                if (w > 0) {
                    int nonNullCount = countNonNull(i);
                    pos += nonNullCount * w;
                } else {
                    int nonNullCount = countNonNull(i);
                    for (int j = 0; j < nonNullCount; j++) {
                        int len = readVarint(data, pos);
                        pos += varintSize(len) + len;
                    }
                }
            } else if (encodings[i] == ENCODING_DICT) {
                int nonNullCount = countNonNull(i);
                pos += (nonNullCount * dictBitWidths[i] + 7) / 8;
            }
            // CONST and ALL_NULL: no data to skip
        }
    }

    public void readRow(Object[] outputFields) {
        for (int i = 0; i < numColumnsInBucket; i++) {
            int outputPos = localToOutputMapping[i];

            if (encodings[i] == ENCODING_ALL_NULL) {
                if (outputPos >= 0) {
                    outputFields[outputPos] = null;
                }
                continue;
            }

            boolean isNull =
                    hasNulls[i] && (nullBitmaps[i][currentRow / 8] & (1 << (currentRow % 8))) != 0;

            if (isNull) {
                if (outputPos >= 0) {
                    outputFields[outputPos] = null;
                }
                continue;
            }

            // Non-null value
            switch (encodings[i]) {
                case ENCODING_CONST:
                    if (outputPos >= 0) {
                        outputFields[outputPos] = constValues[i];
                    }
                    break;
                case ENCODING_DICT:
                    {
                        int idx =
                                readBitPacked(
                                        data, dataCursors[i], dictBitOffsets[i], dictBitWidths[i]);
                        dictBitOffsets[i] += dictBitWidths[i];
                        if (outputPos >= 0) {
                            outputFields[outputPos] = dictValues[i][idx];
                        }
                        break;
                    }
                case ENCODING_PLAIN:
                    {
                        int w = getFixedWidth(allColumnTypes[i]);
                        if (outputPos >= 0) {
                            Object value;
                            if (w > 0) {
                                value = readTypedValue(allColumnTypes[i], data, dataCursors[i], w);
                            } else {
                                value = readVariableValue(allColumnTypes[i], data, dataCursors[i]);
                            }
                            if (casts != null && casts[i] != null) {
                                value = casts[i].cast(value);
                            }
                            outputFields[outputPos] = value;
                        }
                        // Advance cursor
                        if (w > 0) {
                            dataCursors[i] += w;
                        } else {
                            int len = readVarint(data, dataCursors[i]);
                            dataCursors[i] += varintSize(len) + len;
                        }
                        break;
                    }
                default:
                    break;
            }
        }
        currentRow++;
    }

    // ======================== Value reading ========================

    private static Object readValue(DataType type, byte[] buf, int pos) {
        int w = getFixedWidth(type);
        if (w > 0) {
            return readTypedValue(type, buf, pos, w);
        } else {
            return readVariableValue(type, buf, pos);
        }
    }

    private static int valueSize(DataType type, byte[] buf, int pos) {
        int w = getFixedWidth(type);
        if (w > 0) {
            return w;
        } else {
            int len = readVarint(buf, pos);
            return varintSize(len) + len;
        }
    }

    private static Object readTypedValue(DataType type, byte[] buf, int pos, int width) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return buf[pos] != 0;
            case TINYINT:
                return buf[pos];
            case SMALLINT:
                return (short) ((buf[pos] << 8) | (buf[pos + 1] & 0xFF));
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return readInt(buf, pos);
            case BIGINT:
                return readLong(buf, pos);
            case FLOAT:
                return Float.intBitsToFloat(readInt(buf, pos));
            case DOUBLE:
                return Double.longBitsToDouble(readLong(buf, pos));
            case DECIMAL:
                {
                    DecimalType dt = (DecimalType) type;
                    return Decimal.fromUnscaledLong(
                            readLong(buf, pos), dt.getPrecision(), dt.getScale());
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    long millis = readLong(buf, pos);
                    if (width == 12) {
                        int nanos = readInt(buf, pos + 8);
                        return Timestamp.fromEpochMillis(millis, nanos);
                    }
                    return Timestamp.fromEpochMillis(millis);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    long millis = readLong(buf, pos);
                    if (width == 12) {
                        int nanos = readInt(buf, pos + 8);
                        return Timestamp.fromEpochMillis(millis, nanos);
                    }
                    return Timestamp.fromEpochMillis(millis);
                }
            default:
                throw new UnsupportedOperationException("Unsupported fixed type: " + type);
        }
    }

    private static Object readVariableValue(DataType type, byte[] buf, int pos) {
        int len = readVarint(buf, pos);
        int dataStart = pos + varintSize(len);
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryString.fromBytes(buf, dataStart, len);
            case BINARY:
            case VARBINARY:
                {
                    byte[] bytes = new byte[len];
                    System.arraycopy(buf, dataStart, bytes, 0, len);
                    return bytes;
                }
            case DECIMAL:
                {
                    DecimalType dt = (DecimalType) type;
                    byte[] bytes = new byte[len];
                    System.arraycopy(buf, dataStart, bytes, 0, len);
                    return Decimal.fromUnscaledBytes(bytes, dt.getPrecision(), dt.getScale());
                }
            default:
                throw new UnsupportedOperationException("Unsupported variable type: " + type);
        }
    }

    // ======================== Helpers ========================

    private int countNonNull(int colIdx) {
        if (!hasNulls[colIdx]) {
            return numRows;
        }
        if (encodings[colIdx] == ENCODING_ALL_NULL) {
            return 0;
        }
        int count = 0;
        int fullBytes = numRows / 8;
        for (int b = 0; b < fullBytes; b++) {
            count += Integer.bitCount(nullBitmaps[colIdx][b] & 0xFF);
        }
        int remaining = numRows % 8;
        if (remaining > 0) {
            int mask = (1 << remaining) - 1;
            count += Integer.bitCount(nullBitmaps[colIdx][fullBytes] & mask);
        }
        return numRows - count;
    }
}
