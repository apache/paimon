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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;

import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_ALL_NULL;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_CONST;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_DICT;
import static org.apache.paimon.format.mosaic.MosaicSpec.ENCODING_PLAIN;

/**
 * Columnar bucket reader for the Mosaic v2 format. Reads column-oriented data with
 * CONST/DICT/PLAIN/ALL_NULL encoding.
 */
public class MosaicBucketReader {

    private final DataType[] allColumnTypes;
    private final int[] localToOutputMapping;
    private final int numColumnsInBucket;

    // Per-column state set during init()
    private byte[] encodings;
    private boolean[] hasNulls;
    private byte[][] nullBitmaps;
    private Object[] constValues;
    private Object[][] dictValues;
    private int[] dataCursors;
    private byte[] data;
    private int numRows;
    private int currentRow;

    public MosaicBucketReader(DataType[] allColumnTypes, int[] localToOutputMapping) {
        this.allColumnTypes = allColumnTypes;
        this.localToOutputMapping = localToOutputMapping;
        this.numColumnsInBucket = allColumnTypes.length;
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
                int w = MosaicBucketWriter.getFixedWidth(allColumnTypes[i]);
                if (w > 0) {
                    constValues[i] = readTypedValue(allColumnTypes[i], data, pos, w);
                    pos += w;
                } else {
                    constValues[i] = readVariableValue(allColumnTypes[i], data, pos);
                    int len = readVarint(data, pos);
                    pos += varintSize(len) + len;
                }
            }
        }

        // 4. Read dict metadata
        for (int i = 0; i < numColumnsInBucket; i++) {
            if (encodings[i] == ENCODING_DICT) {
                int numEntries = readVarint(data, pos);
                pos += varintSize(numEntries);
                int w = MosaicBucketWriter.getFixedWidth(allColumnTypes[i]);
                Object[] entries = new Object[numEntries];
                for (int j = 0; j < numEntries; j++) {
                    if (w > 0) {
                        entries[j] = readTypedValue(allColumnTypes[i], data, pos, w);
                        pos += w;
                    } else {
                        entries[j] = readVariableValue(allColumnTypes[i], data, pos);
                        int len = readVarint(data, pos);
                        pos += varintSize(len) + len;
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
            if (encodings[i] == ENCODING_PLAIN) {
                // Skip past all plain data for this column to find next column's offset
                int w = MosaicBucketWriter.getFixedWidth(allColumnTypes[i]);
                if (w > 0) {
                    int nonNullCount = countNonNull(i);
                    pos += nonNullCount * w;
                } else {
                    // Variable-width: scan through
                    int nonNullCount = countNonNull(i);
                    for (int j = 0; j < nonNullCount; j++) {
                        int len = readVarint(data, pos);
                        pos += varintSize(len) + len;
                    }
                }
            } else if (encodings[i] == ENCODING_DICT) {
                int nonNullCount = countNonNull(i);
                pos += nonNullCount; // 1 byte per non-null cell
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
                        int idx = data[dataCursors[i]++] & 0xFF;
                        if (outputPos >= 0) {
                            outputFields[outputPos] = dictValues[i][idx];
                        }
                        break;
                    }
                case ENCODING_PLAIN:
                    {
                        int w = MosaicBucketWriter.getFixedWidth(allColumnTypes[i]);
                        if (outputPos >= 0) {
                            if (w > 0) {
                                outputFields[outputPos] =
                                        readTypedValue(allColumnTypes[i], data, dataCursors[i], w);
                            } else {
                                outputFields[outputPos] =
                                        readVariableValue(allColumnTypes[i], data, dataCursors[i]);
                            }
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

    private static int readInt(byte[] buf, int pos) {
        return ((buf[pos] & 0xFF) << 24)
                | ((buf[pos + 1] & 0xFF) << 16)
                | ((buf[pos + 2] & 0xFF) << 8)
                | (buf[pos + 3] & 0xFF);
    }

    private static long readLong(byte[] buf, int pos) {
        return ((long) (buf[pos] & 0xFF) << 56)
                | ((long) (buf[pos + 1] & 0xFF) << 48)
                | ((long) (buf[pos + 2] & 0xFF) << 40)
                | ((long) (buf[pos + 3] & 0xFF) << 32)
                | ((long) (buf[pos + 4] & 0xFF) << 24)
                | ((long) (buf[pos + 5] & 0xFF) << 16)
                | ((long) (buf[pos + 6] & 0xFF) << 8)
                | (buf[pos + 7] & 0xFF);
    }

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
}
