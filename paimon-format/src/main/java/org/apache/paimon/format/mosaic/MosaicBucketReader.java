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
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

/**
 * Deserializes rows from a single bucket in the Mosaic format with projection support. Uses
 * pre-built per-column handlers to avoid switch dispatch on every cell.
 */
public class MosaicBucketReader {

    private final ColumnHandler[] columnHandlers;
    private final int lastProjectedColumn;
    private final int nullBitmapBytes;

    private byte[] data;
    private int dataStart;
    private int[] rowOffsets;
    private int currentRow;
    private int pos;

    public MosaicBucketReader(DataType[] allColumnTypes, int[] localToOutputMapping) {
        int numColumnsInBucket = allColumnTypes.length;
        this.nullBitmapBytes = (numColumnsInBucket + 7) / 8;

        int last = -1;
        for (int i = localToOutputMapping.length - 1; i >= 0; i--) {
            if (localToOutputMapping[i] >= 0) {
                last = i;
                break;
            }
        }
        this.lastProjectedColumn = last;

        this.columnHandlers = new ColumnHandler[lastProjectedColumn + 1];
        for (int i = 0; i <= lastProjectedColumn; i++) {
            columnHandlers[i] = createHandler(allColumnTypes[i], localToOutputMapping[i]);
        }
    }

    public void init(byte[] data, int numRows) {
        this.rowOffsets = new int[numRows + 1];
        int p = 0;
        int offset = 0;
        rowOffsets[0] = 0;
        for (int i = 0; i < numRows; i++) {
            int delta = 0;
            int shift = 0;
            int b;
            do {
                b = data[p++] & 0xFF;
                delta |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            offset += delta;
            rowOffsets[i + 1] = offset;
        }
        this.dataStart = p;
        this.data = data;
        this.currentRow = 0;
    }

    public void readRow(Object[] outputFields) {
        pos = dataStart + rowOffsets[currentRow++];
        int nullBitmapStart = pos;
        pos += nullBitmapBytes;

        for (int i = 0; i <= lastProjectedColumn; i++) {
            boolean isNull = (data[nullBitmapStart + i / 8] & (1 << (i % 8))) != 0;
            if (isNull) {
                columnHandlers[i].handleNull(outputFields);
            } else {
                columnHandlers[i].handle(this, outputFields);
            }
        }
    }

    // ======================== Data reading primitives ========================

    int readInt() {
        int v =
                ((data[pos] & 0xFF) << 24)
                        | ((data[pos + 1] & 0xFF) << 16)
                        | ((data[pos + 2] & 0xFF) << 8)
                        | (data[pos + 3] & 0xFF);
        pos += 4;
        return v;
    }

    long readLong() {
        long v =
                ((long) (data[pos] & 0xFF) << 56)
                        | ((long) (data[pos + 1] & 0xFF) << 48)
                        | ((long) (data[pos + 2] & 0xFF) << 40)
                        | ((long) (data[pos + 3] & 0xFF) << 32)
                        | ((long) (data[pos + 4] & 0xFF) << 24)
                        | ((long) (data[pos + 5] & 0xFF) << 16)
                        | ((long) (data[pos + 6] & 0xFF) << 8)
                        | (data[pos + 7] & 0xFF);
        pos += 8;
        return v;
    }

    int readVarint() {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = data[pos++] & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    void skip(int bytes) {
        pos += bytes;
    }

    byte[] dataRef() {
        return data;
    }

    int pos() {
        return pos;
    }

    void advancePos(int n) {
        pos += n;
    }

    // ======================== Column Handlers ========================

    abstract static class ColumnHandler {
        abstract void handle(MosaicBucketReader reader, Object[] outputFields);

        void handleNull(Object[] outputFields) {}
    }

    private static ColumnHandler createHandler(DataType type, int outputPos) {
        if (outputPos >= 0) {
            return createReadHandler(type, outputPos);
        } else {
            return createSkipHandler(type);
        }
    }

    private static ColumnHandler createReadHandler(DataType type, int outputPos) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        out[outputPos] = r.data[r.pos++] != 0;
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case TINYINT:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        out[outputPos] = r.data[r.pos++];
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case SMALLINT:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        out[outputPos] =
                                (short) ((r.data[r.pos] << 8) | (r.data[r.pos + 1] & 0xFF));
                        r.pos += 2;
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        out[outputPos] = r.readInt();
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case BIGINT:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        out[outputPos] = r.readLong();
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case FLOAT:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        out[outputPos] = Float.intBitsToFloat(r.readInt());
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case DOUBLE:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        out[outputPos] = Double.longBitsToDouble(r.readLong());
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case DECIMAL:
                DecimalType decType = (DecimalType) type;
                int precision = decType.getPrecision();
                int scale = decType.getScale();
                if (Decimal.isCompact(precision)) {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            out[outputPos] =
                                    Decimal.fromUnscaledLong(r.readLong(), precision, scale);
                        }

                        @Override
                        void handleNull(Object[] out) {
                            out[outputPos] = null;
                        }
                    };
                } else {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            int len = r.readVarint();
                            byte[] bytes = new byte[len];
                            System.arraycopy(r.data, r.pos, bytes, 0, len);
                            r.pos += len;
                            out[outputPos] = Decimal.fromUnscaledBytes(bytes, precision, scale);
                        }

                        @Override
                        void handleNull(Object[] out) {
                            out[outputPos] = null;
                        }
                    };
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (Timestamp.isCompact(((TimestampType) type).getPrecision())) {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            out[outputPos] = Timestamp.fromEpochMillis(r.readLong());
                        }

                        @Override
                        void handleNull(Object[] out) {
                            out[outputPos] = null;
                        }
                    };
                } else {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            long millis = r.readLong();
                            int nanos = r.readInt();
                            out[outputPos] = Timestamp.fromEpochMillis(millis, nanos);
                        }

                        @Override
                        void handleNull(Object[] out) {
                            out[outputPos] = null;
                        }
                    };
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (Timestamp.isCompact(((LocalZonedTimestampType) type).getPrecision())) {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            out[outputPos] = Timestamp.fromEpochMillis(r.readLong());
                        }

                        @Override
                        void handleNull(Object[] out) {
                            out[outputPos] = null;
                        }
                    };
                } else {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            long millis = r.readLong();
                            int nanos = r.readInt();
                            out[outputPos] = Timestamp.fromEpochMillis(millis, nanos);
                        }

                        @Override
                        void handleNull(Object[] out) {
                            out[outputPos] = null;
                        }
                    };
                }
            case CHAR:
            case VARCHAR:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        int len = r.readVarint();
                        out[outputPos] = BinaryString.fromBytes(r.data, r.pos, len);
                        r.pos += len;
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            case BINARY:
            case VARBINARY:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        int len = r.readVarint();
                        byte[] bytes = new byte[len];
                        System.arraycopy(r.data, r.pos, bytes, 0, len);
                        r.pos += len;
                        out[outputPos] = bytes;
                    }

                    @Override
                    void handleNull(Object[] out) {
                        out[outputPos] = null;
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static ColumnHandler createSkipHandler(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        r.pos += 1;
                    }
                };
            case SMALLINT:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        r.pos += 2;
                    }
                };
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case FLOAT:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        r.pos += 4;
                    }
                };
            case BIGINT:
            case DOUBLE:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        r.pos += 8;
                    }
                };
            case DECIMAL:
                if (Decimal.isCompact(((DecimalType) type).getPrecision())) {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            r.pos += 8;
                        }
                    };
                } else {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            r.skip(r.readVarint());
                        }
                    };
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (Timestamp.isCompact(((TimestampType) type).getPrecision())) {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            r.pos += 8;
                        }
                    };
                } else {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            r.pos += 12;
                        }
                    };
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (Timestamp.isCompact(((LocalZonedTimestampType) type).getPrecision())) {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            r.pos += 8;
                        }
                    };
                } else {
                    return new ColumnHandler() {
                        @Override
                        void handle(MosaicBucketReader r, Object[] out) {
                            r.pos += 12;
                        }
                    };
                }
            default:
                return new ColumnHandler() {
                    @Override
                    void handle(MosaicBucketReader r, Object[] out) {
                        r.skip(r.readVarint());
                    }
                };
        }
    }
}
