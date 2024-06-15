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

package org.apache.paimon.data.serializer;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.VarLengthIntUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.data.BinaryRow.HEADER_SIZE_IN_BITS;
import static org.apache.paimon.memory.MemorySegmentUtils.bitGet;
import static org.apache.paimon.memory.MemorySegmentUtils.bitSet;
import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_INT_SIZE;

/** A {@link Serializer} for {@link InternalRow} using compacted binary. */
public class RowCompactedSerializer implements Serializer<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final FieldGetter[] getters;
    private final FieldWriter[] writers;
    private final FieldReader[] readers;
    private final RowType rowType;

    @Nullable private RowWriter rowWriter;

    @Nullable private RowReader rowReader;

    public static int calculateBitSetInBytes(int arity) {
        return (arity + 7 + HEADER_SIZE_IN_BITS) / 8;
    }

    public RowCompactedSerializer(RowType rowType) {
        this.getters = new FieldGetter[rowType.getFieldCount()];
        this.writers = new FieldWriter[rowType.getFieldCount()];
        this.readers = new FieldReader[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataType type = rowType.getTypeAt(i);
            getters[i] = InternalRow.createFieldGetter(type, i);
            writers[i] = createFieldWriter(type);
            readers[i] = createFieldReader(type);
        }
        this.rowType = rowType;
    }

    @VisibleForTesting
    RowType rowType() {
        return rowType;
    }

    @Override
    public Serializer<InternalRow> duplicate() {
        return new RowCompactedSerializer(rowType);
    }

    @Override
    public InternalRow copy(InternalRow from) {
        return deserialize(serializeToBytes(from));
    }

    @Override
    public void serialize(InternalRow record, DataOutputView target) throws IOException {
        byte[] bytes = serializeToBytes(record);
        VarLengthIntUtils.encodeInt(target, bytes.length);
        target.write(bytes);
    }

    @Override
    public InternalRow deserialize(DataInputView source) throws IOException {
        int len = VarLengthIntUtils.decodeInt(source);
        byte[] bytes = new byte[len];
        source.readFully(bytes);
        return deserialize(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowCompactedSerializer that = (RowCompactedSerializer) o;
        return Objects.equals(rowType, that.rowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType);
    }

    public byte[] serializeToBytes(InternalRow record) {
        if (rowWriter == null) {
            rowWriter = new RowWriter(calculateBitSetInBytes(getters.length));
        }
        rowWriter.reset();
        rowWriter.writeRowKind(record.getRowKind());
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(record);
            if (field == null) {
                rowWriter.setNullAt(i);
            } else {
                writers[i].writeField(rowWriter, i, field);
            }
        }
        return rowWriter.copyBuffer();
    }

    public InternalRow deserialize(byte[] bytes) {
        if (rowReader == null) {
            rowReader = new RowReader(calculateBitSetInBytes(getters.length));
        }
        rowReader.pointTo(bytes);
        GenericRow row = new GenericRow(readers.length);
        row.setRowKind(rowReader.readRowKind());
        for (int i = 0; i < readers.length; i++) {
            row.setField(i, rowReader.isNullAt(i) ? null : readers[i].readField(rowReader, i));
        }
        return row;
    }

    private static FieldWriter createFieldWriter(DataType fieldType) {
        final FieldWriter fieldWriter;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldWriter = (writer, pos, value) -> writer.writeString((BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = (writer, pos, value) -> writer.writeBoolean((boolean) value);
                break;
            case BINARY:
            case VARBINARY:
                fieldWriter = (writer, pos, value) -> writer.writeBinary((byte[]) value);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeDecimal((Decimal) value, decimalPrecision);
                break;
            case TINYINT:
                fieldWriter = (writer, pos, value) -> writer.writeByte((byte) value);
                break;
            case SMALLINT:
                fieldWriter = (writer, pos, value) -> writer.writeShort((short) value);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldWriter = (writer, pos, value) -> writer.writeInt((int) value);
                break;
            case BIGINT:
                fieldWriter = (writer, pos, value) -> writer.writeLong((long) value);
                break;
            case FLOAT:
                fieldWriter = (writer, pos, value) -> writer.writeFloat((float) value);
                break;
            case DOUBLE:
                fieldWriter = (writer, pos, value) -> writer.writeDouble((double) value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestamp((Timestamp) value, timestampPrecision);
                break;
            case ARRAY:
                Serializer<InternalArray> arraySerializer = InternalSerializers.create(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeArray(
                                        (InternalArray) value,
                                        (InternalArraySerializer) arraySerializer);
                break;
            case MULTISET:
            case MAP:
                Serializer<InternalMap> mapSerializer = InternalSerializers.create(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeMap(
                                        (InternalMap) value, (InternalMapSerializer) mapSerializer);
                break;
            case ROW:
                RowCompactedSerializer rowSerializer =
                        new RowCompactedSerializer((RowType) fieldType);
                fieldWriter =
                        (writer, pos, value) -> writer.writeRow((InternalRow) value, rowSerializer);
                break;
            default:
                throw new IllegalArgumentException();
        }

        if (!fieldType.isNullable()) {
            return fieldWriter;
        }
        return (writer, pos, value) -> {
            if (value == null) {
                writer.setNullAt(pos);
            } else {
                fieldWriter.writeField(writer, pos, value);
            }
        };
    }

    private static FieldReader createFieldReader(DataType fieldType) {
        final FieldReader fieldReader;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldReader = (reader, pos) -> reader.readString();
                break;
            case BOOLEAN:
                fieldReader = (reader, pos) -> reader.readBoolean();
                break;
            case BINARY:
            case VARBINARY:
                fieldReader = (reader, pos) -> reader.readBinary();
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldReader = (reader, pos) -> reader.readDecimal(decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldReader = (reader, pos) -> reader.readByte();
                break;
            case SMALLINT:
                fieldReader = (reader, pos) -> reader.readShort();
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldReader = (reader, pos) -> reader.readInt();
                break;
            case BIGINT:
                fieldReader = (reader, pos) -> reader.readLong();
                break;
            case FLOAT:
                fieldReader = (reader, pos) -> reader.readFloat();
                break;
            case DOUBLE:
                fieldReader = (reader, pos) -> reader.readDouble();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldReader = (reader, pos) -> reader.readTimestamp(timestampPrecision);
                break;
            case ARRAY:
                fieldReader = (reader, pos) -> reader.readArray();
                break;
            case MULTISET:
            case MAP:
                fieldReader = (reader, pos) -> reader.readMap();
                break;
            case ROW:
                RowCompactedSerializer serializer = new RowCompactedSerializer((RowType) fieldType);
                fieldReader = (reader, pos) -> reader.readRow(serializer);
                break;
            default:
                throw new IllegalArgumentException();
        }
        if (!fieldType.isNullable()) {
            return fieldReader;
        }
        return (reader, pos) -> {
            if (reader.isNullAt(pos)) {
                return null;
            }
            return fieldReader.readField(reader, pos);
        };
    }

    private interface FieldWriter extends Serializable {
        void writeField(RowWriter writer, int pos, Object value);
    }

    private interface FieldReader extends Serializable {
        Object readField(RowReader reader, int pos);
    }

    private static class RowWriter {

        // Including RowKind and null bits.
        private final int headerSizeInBytes;

        private byte[] buffer;
        private MemorySegment segment;
        private int position;

        private RowWriter(int headerSizeInBytes) {
            this.headerSizeInBytes = headerSizeInBytes;
            setBuffer(new byte[Math.max(64, headerSizeInBytes)]);
            this.position = headerSizeInBytes;
        }

        private void reset() {
            this.position = headerSizeInBytes;
            for (int i = 0; i < headerSizeInBytes; i++) {
                buffer[i] = 0;
            }
        }

        private void writeRowKind(RowKind kind) {
            this.buffer[0] = kind.toByteValue();
        }

        private void setNullAt(int pos) {
            bitSet(segment, 0, pos + HEADER_SIZE_IN_BITS);
        }

        private void writeBoolean(boolean value) {
            ensureCapacity(1);
            segment.putBoolean(position++, value);
        }

        private void writeByte(byte value) {
            ensureCapacity(1);
            segment.put(position++, value);
        }

        private void writeShort(short value) {
            ensureCapacity(2);
            segment.putShort(position, value);
            position += 2;
        }

        private void writeInt(int value) {
            ensureCapacity(4);
            segment.putInt(position, value);
            position += 4;
        }

        private void writeLong(long value) {
            ensureCapacity(8);
            segment.putLong(position, value);
            position += 8;
        }

        private void writeFloat(float value) {
            ensureCapacity(4);
            segment.putFloat(position, value);
            position += 4;
        }

        private void writeDouble(double value) {
            ensureCapacity(8);
            segment.putDouble(position, value);
            position += 8;
        }

        private void writeString(BinaryString value) {
            writeSegments(value.getSegments(), value.getOffset(), value.getSizeInBytes());
        }

        private void writeDecimal(Decimal value, int precision) {
            if (Decimal.isCompact(precision)) {
                writeLong(value.toUnscaledLong());
            } else {
                writeBinary(value.toUnscaledBytes());
            }
        }

        private void writeTimestamp(Timestamp value, int precision) {
            if (Timestamp.isCompact(precision)) {
                writeLong(value.getMillisecond());
            } else {
                writeLong(value.getMillisecond());
                writeUnsignedInt(value.getNanoOfMillisecond());
            }
        }

        private void writeUnsignedInt(int value) {
            checkArgument(value >= 0);
            ensureCapacity(MAX_VAR_INT_SIZE);
            int len = VarLengthIntUtils.encodeInt(buffer, position, value);
            position += len;
        }

        private void writeArray(InternalArray value, InternalArraySerializer serializer) {
            BinaryArray binary = serializer.toBinaryArray(value);
            writeSegments(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
        }

        private void writeMap(InternalMap value, InternalMapSerializer serializer) {
            BinaryMap binary = serializer.toBinaryMap(value);
            writeSegments(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
        }

        private void writeRow(InternalRow value, RowCompactedSerializer serializer) {
            writeBinary(serializer.serializeToBytes(value));
        }

        private byte[] copyBuffer() {
            return Arrays.copyOf(buffer, position);
        }

        private void setBuffer(byte[] buffer) {
            this.buffer = buffer;
            this.segment = MemorySegment.wrap(buffer);
        }

        private void ensureCapacity(int size) {
            if (buffer.length - position < size) {
                grow(size);
            }
        }

        private void grow(int minCapacityAdd) {
            int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
            setBuffer(Arrays.copyOf(this.buffer, newLen));
        }

        private void writeBinary(byte[] value) {
            writeUnsignedInt(value.length);
            ensureCapacity(value.length);
            System.arraycopy(value, 0, buffer, position, value.length);
            position += value.length;
        }

        private void write(MemorySegment segment, int off, int len) {
            ensureCapacity(len);
            segment.get(off, this.buffer, this.position, len);
            this.position += len;
        }

        private void writeSegments(MemorySegment[] segments, int off, int len) {
            writeUnsignedInt(len);
            if (len + off <= segments[0].size()) {
                write(segments[0], off, len);
            } else {
                write(segments, off, len);
            }
        }

        private void write(MemorySegment[] segments, int off, int len) {
            ensureCapacity(len);
            int toWrite = len;
            int fromOffset = off;
            int toOffset = this.position;
            for (MemorySegment sourceSegment : segments) {
                int remain = sourceSegment.size() - fromOffset;
                if (remain > 0) {
                    int localToWrite = Math.min(remain, toWrite);
                    sourceSegment.get(fromOffset, buffer, toOffset, localToWrite);
                    toWrite -= localToWrite;
                    toOffset += localToWrite;
                    fromOffset = 0;
                } else {
                    fromOffset -= sourceSegment.size();
                }
            }
            this.position += len;
        }
    }

    private static class RowReader {

        // Including RowKind and null bits.
        private final int headerSizeInBytes;

        private MemorySegment segment;
        private MemorySegment[] segments;
        private int position;

        private RowReader(int headerSizeInBytes) {
            this.headerSizeInBytes = headerSizeInBytes;
        }

        private void pointTo(byte[] bytes) {
            this.segment = MemorySegment.wrap(bytes);
            this.segments = new MemorySegment[] {segment};
            this.position = headerSizeInBytes;
        }

        private RowKind readRowKind() {
            return RowKind.fromByteValue(segment.get(0));
        }

        private boolean isNullAt(int pos) {
            return bitGet(segment, 0, pos + HEADER_SIZE_IN_BITS);
        }

        private boolean readBoolean() {
            return segment.getBoolean(position++);
        }

        private byte readByte() {
            return segment.get(position++);
        }

        private short readShort() {
            short value = segment.getShort(position);
            position += 2;
            return value;
        }

        private int readInt() {
            int value = segment.getInt(position);
            position += 4;
            return value;
        }

        private long readLong() {
            long value = segment.getLong(position);
            position += 8;
            return value;
        }

        private float readFloat() {
            float value = segment.getFloat(position);
            position += 4;
            return value;
        }

        private double readDouble() {
            double value = segment.getDouble(position);
            position += 8;
            return value;
        }

        private BinaryString readString() {
            int length = readUnsignedInt();
            BinaryString string = BinaryString.fromAddress(segments, position, length);
            position += length;
            return string;
        }

        private int readUnsignedInt() {
            for (int offset = 0, result = 0; offset < 32; offset += 7) {
                int b = readByte();
                result |= (b & 0x7F) << offset;
                if ((b & 0x80) == 0) {
                    return result;
                }
            }
            throw new Error("Malformed integer.");
        }

        private Decimal readDecimal(int precision, int scale) {
            return Decimal.isCompact(precision)
                    ? Decimal.fromUnscaledLong(readLong(), precision, scale)
                    : Decimal.fromUnscaledBytes(readBinary(), precision, scale);
        }

        private Timestamp readTimestamp(int precision) {
            if (Timestamp.isCompact(precision)) {
                return Timestamp.fromEpochMillis(readLong());
            }
            long milliseconds = readLong();
            int nanosOfMillisecond = readUnsignedInt();
            return Timestamp.fromEpochMillis(milliseconds, nanosOfMillisecond);
        }

        private byte[] readBinary() {
            int length = readUnsignedInt();
            byte[] bytes = new byte[length];
            segment.get(position, bytes, 0, length);
            position += length;
            return bytes;
        }

        private InternalArray readArray() {
            BinaryArray value = new BinaryArray();
            int length = readUnsignedInt();
            value.pointTo(segments, position, length);
            position += length;
            return value;
        }

        private InternalMap readMap() {
            BinaryMap value = new BinaryMap();
            int length = readUnsignedInt();
            value.pointTo(segments, position, length);
            position += length;
            return value;
        }

        private InternalRow readRow(RowCompactedSerializer serializer) {
            byte[] bytes = readBinary();
            return serializer.deserialize(bytes);
        }
    }
}
