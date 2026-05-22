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

package org.apache.paimon.format.row;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.IntArrayList;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/**
 * Accumulates rows by serializing directly into a block buffer.
 *
 * <p>Block layout (uncompressed):
 *
 * <pre>
 * [row_0 bytes][row_1 bytes]...[row_N bytes]
 * [offset_0 (int32 LE)][offset_1]...[offset_N]
 * [row_count (int32 LE)]
 * </pre>
 */
class RowBlockWriter {

    private final BlockOutput buf;
    private final IntArrayList offsets;
    private final FieldWriter[] fieldWriters;
    private final int headerSizeInBytes;

    RowBlockWriter(BlockOutput buf, RowType rowType) {
        this.buf = buf;
        this.offsets = new IntArrayList(64);
        int arity = rowType.getFieldCount();
        this.headerSizeInBytes = (arity + 7) / 8;
        this.fieldWriters = new FieldWriter[arity];
        for (int i = 0; i < arity; i++) {
            fieldWriters[i] = createFieldWriter(rowType.getTypeAt(i));
        }
    }

    void writeRow(InternalRow row) {
        offsets.add(buf.position);
        writeRow(row, headerSizeInBytes, fieldWriters);
    }

    private void writeRow(InternalRow row, int headerSize, FieldWriter[] writers) {
        int headerStart = buf.position;
        buf.ensureCapacity(headerSize);
        for (int i = 0; i < headerSize; i++) {
            buf.buffer[headerStart + i] = 0;
        }
        buf.position += headerSize;
        for (int i = 0; i < writers.length; i++) {
            if (row.isNullAt(i)) {
                buf.buffer[headerStart + i / 8] |= (byte) (1 << (i % 8));
            } else {
                writers[i].write(row, i);
            }
        }
    }

    int rowCount() {
        return offsets.size();
    }

    int estimatedSize() {
        return buf.position + offsets.size() * 4 + 4;
    }

    byte[] finish() {
        int totalSize = buf.position + offsets.size() * 4 + 4;
        buf.ensureCapacity(offsets.size() * 4 + 4);
        for (int i = 0; i < offsets.size(); i++) {
            RowFileFooter.writeIntLE(buf.buffer, buf.position, offsets.get(i));
            buf.position += 4;
        }
        RowFileFooter.writeIntLE(buf.buffer, buf.position, offsets.size());
        buf.position += 4;

        byte[] result = new byte[totalSize];
        System.arraycopy(buf.buffer, 0, result, 0, totalSize);
        return result;
    }

    void reset() {
        offsets.clear();
        buf.position = 0;
    }

    // ======================== Factory ========================

    private FieldWriter createFieldWriter(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new StringFieldWriter();
            case BOOLEAN:
                return new BooleanFieldWriter();
            case BINARY:
            case VARBINARY:
                return new BinaryFieldWriter();
            case DECIMAL:
                return new DecimalFieldWriter(getPrecision(type), getScale(type));
            case TINYINT:
                return new TinyIntFieldWriter();
            case SMALLINT:
                return new SmallIntFieldWriter();
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new IntFieldWriter();
            case BIGINT:
                return new BigIntFieldWriter();
            case FLOAT:
                return new FloatFieldWriter();
            case DOUBLE:
                return new DoubleFieldWriter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampFieldWriter(getPrecision(type));
            case VARIANT:
                return new VariantFieldWriter();
            case BLOB:
                return new BlobFieldWriter();
            case VECTOR:
                return new VectorFieldWriter(
                        createFieldWriter(((VectorType) type).getElementType()));
            case ARRAY:
                return new ArrayFieldWriter(createFieldWriter(((ArrayType) type).getElementType()));
            case MULTISET:
                {
                    DataType elemType = ((MultisetType) type).getElementType();
                    return new MapFieldWriter(
                            createFieldWriter(elemType), createFieldWriter(new IntType()));
                }
            case MAP:
                {
                    MapType mapType = (MapType) type;
                    return new MapFieldWriter(
                            createFieldWriter(mapType.getKeyType()),
                            createFieldWriter(mapType.getValueType()));
                }
            case ROW:
                return new RowFieldWriter((RowType) type);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type.getTypeRoot());
        }
    }

    // ======================== Complex Type Helpers ========================

    private void writeArray(InternalArray array, FieldWriter elementWriter) {
        int size = array.size();
        buf.writeVarInt(size);
        int nullBitmapBytes = (size + 7) / 8;
        buf.ensureCapacity(nullBitmapBytes);
        int nullStart = buf.position;
        for (int i = 0; i < nullBitmapBytes; i++) {
            buf.buffer[buf.position++] = 0;
        }
        for (int i = 0; i < size; i++) {
            if (array.isNullAt(i)) {
                buf.buffer[nullStart + i / 8] |= (byte) (1 << (i % 8));
            } else {
                elementWriter.write(array, i);
            }
        }
    }

    private void writeMap(InternalMap map, FieldWriter keyWriter, FieldWriter valueWriter) {
        writeArray(map.keyArray(), keyWriter);
        writeArray(map.valueArray(), valueWriter);
    }

    // ======================== Interface ========================

    interface FieldWriter {
        void write(DataGetters data, int i);
    }

    // ======================== FieldWriter Implementations ========================

    private class StringFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeBytes(data.getString(i).toBytes());
        }
    }

    private class BooleanFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeBoolean(data.getBoolean(i));
        }
    }

    private class BinaryFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeBytes(data.getBinary(i));
        }
    }

    private class DecimalFieldWriter implements FieldWriter {
        private final int precision;
        private final int scale;

        DecimalFieldWriter(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public void write(DataGetters data, int i) {
            buf.writeDecimal(data.getDecimal(i, precision, scale), precision);
        }
    }

    private class TinyIntFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeByte(data.getByte(i));
        }
    }

    private class SmallIntFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeShort(data.getShort(i));
        }
    }

    private class IntFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeInt(data.getInt(i));
        }
    }

    private class BigIntFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeLong(data.getLong(i));
        }
    }

    private class FloatFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeFloat(data.getFloat(i));
        }
    }

    private class DoubleFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeDouble(data.getDouble(i));
        }
    }

    private class TimestampFieldWriter implements FieldWriter {
        private final int precision;

        TimestampFieldWriter(int precision) {
            this.precision = precision;
        }

        @Override
        public void write(DataGetters data, int i) {
            buf.writeTimestamp(data.getTimestamp(i, precision), precision);
        }
    }

    private class VariantFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            Variant v = data.getVariant(i);
            buf.writeBytes(v.value());
            buf.writeBytes(v.metadata());
        }
    }

    private class BlobFieldWriter implements FieldWriter {
        @Override
        public void write(DataGetters data, int i) {
            buf.writeBytes(data.getBlob(i).toData());
        }
    }

    private class VectorFieldWriter implements FieldWriter {
        private final FieldWriter elemWriter;

        VectorFieldWriter(FieldWriter elemWriter) {
            this.elemWriter = elemWriter;
        }

        @Override
        public void write(DataGetters data, int i) {
            InternalVector vector = data.getVector(i);
            int size = vector.size();
            buf.writeVarInt(size);
            for (int j = 0; j < size; j++) {
                elemWriter.write(vector, j);
            }
        }
    }

    private class ArrayFieldWriter implements FieldWriter {
        private final FieldWriter elemWriter;

        ArrayFieldWriter(FieldWriter elemWriter) {
            this.elemWriter = elemWriter;
        }

        @Override
        public void write(DataGetters data, int i) {
            writeArray(data.getArray(i), elemWriter);
        }
    }

    private class MapFieldWriter implements FieldWriter {
        private final FieldWriter keyWriter;
        private final FieldWriter valueWriter;

        MapFieldWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
            this.keyWriter = keyWriter;
            this.valueWriter = valueWriter;
        }

        @Override
        public void write(DataGetters data, int i) {
            writeMap(data.getMap(i), keyWriter, valueWriter);
        }
    }

    private class RowFieldWriter implements FieldWriter {
        private final int nestedHeaderSize;
        private final FieldWriter[] nestedWriters;
        private final int numFields;

        RowFieldWriter(RowType nestedType) {
            int arity = nestedType.getFieldCount();
            this.numFields = arity;
            this.nestedHeaderSize = (arity + 7) / 8;
            this.nestedWriters = new FieldWriter[arity];
            for (int j = 0; j < arity; j++) {
                nestedWriters[j] = createFieldWriter(nestedType.getTypeAt(j));
            }
        }

        @Override
        public void write(DataGetters data, int i) {
            writeRow(data.getRow(i, numFields), nestedHeaderSize, nestedWriters);
        }
    }
}
