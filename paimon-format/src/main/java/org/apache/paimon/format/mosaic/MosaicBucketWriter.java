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

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.format.mosaic.MosaicUtils.writeVarint;

/** Serializes rows for a single bucket in the Mosaic format. */
public class MosaicBucketWriter {

    private final InternalRow.FieldGetter[] fieldGetters;
    private final ColumnWriter[] columnWriters;
    private final int numColumns;
    private final int nullBitmapBytes;

    private byte[] buf;
    private int pos;
    private final List<Integer> rowOffsets;
    private final byte[] nullBitmap;
    private final Object[] values;

    public MosaicBucketWriter(RowType fullRowType, int[] globalColumnIndices) {
        this.numColumns = globalColumnIndices.length;
        this.nullBitmapBytes = (numColumns + 7) / 8;
        this.fieldGetters = new InternalRow.FieldGetter[numColumns];
        this.columnWriters = new ColumnWriter[numColumns];

        List<DataType> allTypes = fullRowType.getFieldTypes();
        for (int i = 0; i < numColumns; i++) {
            int globalIdx = globalColumnIndices[i];
            DataType type = allTypes.get(globalIdx);
            fieldGetters[i] = InternalRow.createFieldGetter(type, globalIdx);
            columnWriters[i] = createColumnWriter(type);
        }

        this.buf = new byte[4096];
        this.pos = 0;
        this.rowOffsets = new ArrayList<>();
        this.nullBitmap = new byte[nullBitmapBytes];
        this.values = new Object[numColumns];
    }

    public boolean isEmpty() {
        return rowOffsets.isEmpty();
    }

    /** Writes a row and returns the current buffer size after writing. */
    public int writeRow(InternalRow row) {
        rowOffsets.add(pos);

        for (int i = 0; i < nullBitmapBytes; i++) {
            nullBitmap[i] = 0;
        }

        for (int i = 0; i < numColumns; i++) {
            Object value = fieldGetters[i].getFieldOrNull(row);
            values[i] = value;
            if (value == null) {
                nullBitmap[i / 8] |= (byte) (1 << (i % 8));
            }
        }

        ensureCapacity(nullBitmapBytes);
        System.arraycopy(nullBitmap, 0, buf, pos, nullBitmapBytes);
        pos += nullBitmapBytes;

        for (int i = 0; i < numColumns; i++) {
            if (values[i] != null) {
                columnWriters[i].write(this, values[i]);
            }
        }
        return pos;
    }

    public byte[] finish() {
        int numRows = rowOffsets.size();
        int dataSize = pos;

        byte[] varintBuf = new byte[numRows * 5];
        int vPos = 0;
        int prevOffset = 0;
        for (int i = 0; i < numRows; i++) {
            int nextOffset = (i + 1 < numRows) ? rowOffsets.get(i + 1) : dataSize;
            int rowSize = nextOffset - prevOffset;
            prevOffset = nextOffset;
            vPos = writeVarint(varintBuf, vPos, rowSize);
        }

        byte[] result = new byte[vPos + dataSize];
        System.arraycopy(varintBuf, 0, result, 0, vPos);
        System.arraycopy(buf, 0, result, vPos, dataSize);
        return result;
    }

    public void reset() {
        pos = 0;
        rowOffsets.clear();
    }

    void ensureCapacity(int additional) {
        int required = pos + additional;
        if (required > buf.length) {
            int newLen = Math.max(buf.length * 2, required);
            byte[] newBuf = new byte[newLen];
            System.arraycopy(buf, 0, newBuf, 0, pos);
            buf = newBuf;
        }
    }

    void writeByte(int v) {
        ensureCapacity(1);
        buf[pos++] = (byte) v;
    }

    void writeShort(int v) {
        ensureCapacity(2);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
    }

    void writeInt(int v) {
        ensureCapacity(4);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
    }

    void writeLong(long v) {
        ensureCapacity(8);
        buf[pos++] = (byte) (v >>> 56);
        buf[pos++] = (byte) (v >>> 48);
        buf[pos++] = (byte) (v >>> 40);
        buf[pos++] = (byte) (v >>> 32);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
    }

    void writeBytes(byte[] src, int off, int len) {
        ensureCapacity(len);
        System.arraycopy(src, off, buf, pos, len);
        pos += len;
    }

    void writeVarintVal(int value) {
        ensureCapacity(5);
        pos = writeVarint(buf, pos, value);
    }

    // ======================== Column Writers ========================

    @FunctionalInterface
    interface ColumnWriter {
        void write(MosaicBucketWriter w, Object value);
    }

    private static ColumnWriter createColumnWriter(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (w, v) -> w.writeByte((Boolean) v ? 1 : 0);
            case TINYINT:
                return (w, v) -> w.writeByte((Byte) v);
            case SMALLINT:
                return (w, v) -> w.writeShort((Short) v);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (w, v) -> w.writeInt((Integer) v);
            case BIGINT:
                return (w, v) -> w.writeLong((Long) v);
            case FLOAT:
                return (w, v) -> w.writeInt(Float.floatToRawIntBits((Float) v));
            case DOUBLE:
                return (w, v) -> w.writeLong(Double.doubleToRawLongBits((Double) v));
            case DECIMAL:
                DecimalType decType = (DecimalType) type;
                if (Decimal.isCompact(decType.getPrecision())) {
                    return (w, v) -> w.writeLong(((Decimal) v).toUnscaledLong());
                } else {
                    return (w, v) -> {
                        byte[] bytes = ((Decimal) v).toUnscaledBytes();
                        w.writeVarintVal(bytes.length);
                        w.writeBytes(bytes, 0, bytes.length);
                    };
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (Timestamp.isCompact(((TimestampType) type).getPrecision())) {
                    return (w, v) -> w.writeLong(((Timestamp) v).getMillisecond());
                } else {
                    return (w, v) -> {
                        Timestamp ts = (Timestamp) v;
                        w.writeLong(ts.getMillisecond());
                        w.writeInt(ts.getNanoOfMillisecond());
                    };
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (Timestamp.isCompact(((LocalZonedTimestampType) type).getPrecision())) {
                    return (w, v) -> w.writeLong(((Timestamp) v).getMillisecond());
                } else {
                    return (w, v) -> {
                        Timestamp ts = (Timestamp) v;
                        w.writeLong(ts.getMillisecond());
                        w.writeInt(ts.getNanoOfMillisecond());
                    };
                }
            case CHAR:
            case VARCHAR:
                return (w, v) -> {
                    BinaryString bs = (BinaryString) v;
                    byte[] bytes = bs.toBytes();
                    w.writeVarintVal(bytes.length);
                    w.writeBytes(bytes, 0, bytes.length);
                };
            case BINARY:
            case VARBINARY:
                return (w, v) -> {
                    byte[] bytes = (byte[]) v;
                    w.writeVarintVal(bytes.length);
                    w.writeBytes(bytes, 0, bytes.length);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
