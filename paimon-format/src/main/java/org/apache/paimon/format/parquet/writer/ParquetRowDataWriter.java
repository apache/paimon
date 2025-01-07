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

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VariantType;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.computeMinBytesForDecimalPrecision;
import static org.apache.paimon.format.parquet.reader.TimestampColumnReader.JULIAN_EPOCH_OFFSET_DAYS;
import static org.apache.paimon.format.parquet.reader.TimestampColumnReader.MILLIS_IN_DAY;
import static org.apache.paimon.format.parquet.reader.TimestampColumnReader.NANOS_PER_MILLISECOND;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Writes a record to the Parquet API with the expected schema in order to be written to a file. */
public class ParquetRowDataWriter {

    private final RowWriter rowWriter;
    private final RecordConsumer recordConsumer;

    public ParquetRowDataWriter(RecordConsumer recordConsumer, RowType rowType, GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.rowWriter = new RowWriter(rowType, schema);
    }

    /**
     * It writes a record to Parquet.
     *
     * @param record Contains the record that is going to be written.
     */
    public void write(final InternalRow record) {
        recordConsumer.startMessage();
        rowWriter.write(record);
        recordConsumer.endMessage();
    }

    private FieldWriter createWriter(DataType t, Type type) {
        if (type.isPrimitive()) {
            switch (t.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return new StringWriter();
                case BOOLEAN:
                    return new BooleanWriter();
                case BINARY:
                case VARBINARY:
                    return new BinaryWriter();
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) t;
                    return createDecimalWriter(decimalType.getPrecision(), decimalType.getScale());
                case TINYINT:
                    return new ByteWriter();
                case SMALLINT:
                    return new ShortWriter();
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTEGER:
                    return new IntWriter();
                case BIGINT:
                    return new LongWriter();
                case FLOAT:
                    return new FloatWriter();
                case DOUBLE:
                    return new DoubleWriter();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    TimestampType timestampType = (TimestampType) t;
                    return createTimestampWriter(timestampType.getPrecision());
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) t;
                    return createTimestampWriter(localZonedTimestampType.getPrecision());
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        } else {
            GroupType groupType = type.asGroupType();
            LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();

            if (t instanceof ArrayType
                    && annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                return new ArrayWriter(((ArrayType) t).getElementType(), groupType);
            } else if (t instanceof MapType
                    && annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                return new MapWriter(
                        ((MapType) t).getKeyType(), ((MapType) t).getValueType(), groupType);
            } else if (t instanceof MultisetType
                    && annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                return new MapWriter(
                        ((MultisetType) t).getElementType(), new IntType(false), groupType);
            } else if (t instanceof RowType && type instanceof GroupType) {
                return new RowWriter((RowType) t, groupType);
            } else if (t instanceof VariantType && type instanceof GroupType) {
                return new VariantWriter();
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        }
    }

    private FieldWriter createTimestampWriter(int precision) {
        if (precision <= 3) {
            return new TimestampMillsWriter(precision);
        } else if (precision > 6) {
            return new TimestampInt96Writer(precision);
        } else {
            return new TimestampMicrosWriter(precision);
        }
    }

    private interface FieldWriter {

        void write(InternalRow row, int ordinal);

        void write(InternalArray arrayData, int ordinal);
    }

    private class BooleanWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeBoolean(row.getBoolean(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeBoolean(arrayData.getBoolean(ordinal));
        }

        private void writeBoolean(boolean value) {
            recordConsumer.addBoolean(value);
        }
    }

    private class ByteWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeByte(row.getByte(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeByte(arrayData.getByte(ordinal));
        }

        private void writeByte(byte value) {
            recordConsumer.addInteger(value);
        }
    }

    private class ShortWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeShort(row.getShort(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeShort(arrayData.getShort(ordinal));
        }

        private void writeShort(short value) {
            recordConsumer.addInteger(value);
        }
    }

    private class LongWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeLong(row.getLong(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeLong(arrayData.getLong(ordinal));
        }

        private void writeLong(long value) {
            recordConsumer.addLong(value);
        }
    }

    private class FloatWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeFloat(row.getFloat(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeFloat(arrayData.getFloat(ordinal));
        }

        private void writeFloat(float value) {
            recordConsumer.addFloat(value);
        }
    }

    private class DoubleWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeDouble(row.getDouble(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeDouble(arrayData.getDouble(ordinal));
        }

        private void writeDouble(double value) {
            recordConsumer.addDouble(value);
        }
    }

    private class StringWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeString(row.getString(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeString(arrayData.getString(ordinal));
        }

        private void writeString(BinaryString value) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(value.toBytes()));
        }
    }

    private class BinaryWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeBinary(row.getBinary(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeBinary(arrayData.getBinary(ordinal));
        }

        private void writeBinary(byte[] value) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(value));
        }
    }

    private class IntWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeInt(row.getInt(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeInt(arrayData.getInt(ordinal));
        }

        private void writeInt(int value) {
            recordConsumer.addInteger(value);
        }
    }

    private class TimestampMillsWriter implements FieldWriter {

        private final int precision;

        private TimestampMillsWriter(int precision) {
            checkArgument(precision <= 3);
            this.precision = precision;
        }

        @Override
        public void write(InternalRow row, int ordinal) {
            writeTimestamp(row.getTimestamp(ordinal, precision));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeTimestamp(arrayData.getTimestamp(ordinal, precision));
        }

        private void writeTimestamp(Timestamp value) {
            recordConsumer.addLong(value.getMillisecond());
        }
    }

    private class TimestampMicrosWriter implements FieldWriter {

        private final int precision;

        private TimestampMicrosWriter(int precision) {
            checkArgument(precision > 3);
            checkArgument(precision <= 6);
            this.precision = precision;
        }

        @Override
        public void write(InternalRow row, int ordinal) {
            writeTimestamp(row.getTimestamp(ordinal, precision));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeTimestamp(arrayData.getTimestamp(ordinal, precision));
        }

        private void writeTimestamp(Timestamp value) {
            recordConsumer.addLong(value.toMicros());
        }
    }

    private class TimestampInt96Writer implements FieldWriter {

        private final int precision;

        private TimestampInt96Writer(int precision) {
            checkArgument(precision > 6);
            this.precision = precision;
        }

        @Override
        public void write(InternalRow row, int ordinal) {
            writeTimestamp(row.getTimestamp(ordinal, precision));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeTimestamp(arrayData.getTimestamp(ordinal, precision));
        }

        private void writeTimestamp(Timestamp value) {
            recordConsumer.addBinary(timestampToInt96(value));
        }
    }

    /** It writes a map field to parquet, both key and value are nullable. */
    private class MapWriter implements FieldWriter {

        private final String repeatedGroupName;
        private final String keyName;
        private final String valueName;
        private final FieldWriter keyWriter;
        private final FieldWriter valueWriter;

        private MapWriter(DataType keyType, DataType valueType, GroupType groupType) {
            // Get the internal map structure (MAP_KEY_VALUE)
            GroupType repeatedType = groupType.getType(0).asGroupType();
            this.repeatedGroupName = repeatedType.getName();

            // Get key element information
            Type type = repeatedType.getType(0);
            this.keyName = type.getName();
            this.keyWriter = createWriter(keyType, type);

            // Get value element information
            Type valuetype = repeatedType.getType(1);
            this.valueName = valuetype.getName();
            this.valueWriter = createWriter(valueType, valuetype);
        }

        @Override
        public void write(InternalRow row, int ordinal) {
            writeMapData(row.getMap(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeMapData(arrayData.getMap(ordinal));
        }

        private void writeMapData(InternalMap mapData) {
            recordConsumer.startGroup();

            if (mapData != null && mapData.size() > 0) {
                recordConsumer.startField(repeatedGroupName, 0);

                InternalArray keyArray = mapData.keyArray();
                InternalArray valueArray = mapData.valueArray();
                for (int i = 0; i < keyArray.size(); i++) {
                    recordConsumer.startGroup();
                    if (!keyArray.isNullAt(i)) {
                        // write key element
                        recordConsumer.startField(keyName, 0);
                        keyWriter.write(keyArray, i);
                        recordConsumer.endField(keyName, 0);
                    } else {
                        throw new IllegalArgumentException(
                                "Parquet does not support null keys in maps. "
                                        + "See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps "
                                        + "for more details.");
                    }

                    if (!valueArray.isNullAt(i)) {
                        // write value element
                        recordConsumer.startField(valueName, 1);
                        valueWriter.write(valueArray, i);
                        recordConsumer.endField(valueName, 1);
                    }
                    recordConsumer.endGroup();
                }

                recordConsumer.endField(repeatedGroupName, 0);
            }
            recordConsumer.endGroup();
        }
    }

    /** It writes an array type field to parquet. */
    private class ArrayWriter implements FieldWriter {

        private final String elementName;
        private final FieldWriter elementWriter;
        private final String repeatedGroupName;

        private ArrayWriter(DataType t, GroupType groupType) {

            // Get the internal array structure
            GroupType repeatedType = groupType.getType(0).asGroupType();
            this.repeatedGroupName = repeatedType.getName();

            Type elementType = repeatedType.getType(0);
            this.elementName = elementType.getName();

            this.elementWriter = createWriter(t, elementType);
        }

        @Override
        public void write(InternalRow row, int ordinal) {
            writeArrayData(row.getArray(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeArrayData(arrayData.getArray(ordinal));
        }

        private void writeArrayData(InternalArray arrayData) {
            recordConsumer.startGroup();
            int listLength = arrayData.size();

            if (listLength > 0) {
                recordConsumer.startField(repeatedGroupName, 0);
                for (int i = 0; i < listLength; i++) {
                    recordConsumer.startGroup();
                    if (!arrayData.isNullAt(i)) {
                        recordConsumer.startField(elementName, 0);
                        elementWriter.write(arrayData, i);
                        recordConsumer.endField(elementName, 0);
                    }
                    recordConsumer.endGroup();
                }

                recordConsumer.endField(repeatedGroupName, 0);
            }
            recordConsumer.endGroup();
        }
    }

    /** It writes a row type field to parquet. */
    private class RowWriter implements FieldWriter {
        private final FieldWriter[] fieldWriters;
        private final String[] fieldNames;

        public RowWriter(RowType rowType, GroupType groupType) {
            this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
            List<DataType> fieldTypes = rowType.getFieldTypes();
            this.fieldWriters = new FieldWriter[rowType.getFieldCount()];
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i] = createWriter(fieldTypes.get(i), groupType.getType(i));
            }
        }

        public void write(InternalRow row) {
            for (int i = 0; i < fieldWriters.length; i++) {
                if (!row.isNullAt(i)) {
                    String fieldName = fieldNames[i];
                    FieldWriter writer = fieldWriters[i];

                    recordConsumer.startField(fieldName, i);
                    writer.write(row, i);
                    recordConsumer.endField(fieldName, i);
                }
            }
        }

        @Override
        public void write(InternalRow row, int ordinal) {
            recordConsumer.startGroup();
            InternalRow rowData = row.getRow(ordinal, fieldWriters.length);
            write(rowData);
            recordConsumer.endGroup();
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            recordConsumer.startGroup();
            InternalRow rowData = arrayData.getRow(ordinal, fieldWriters.length);
            write(rowData);
            recordConsumer.endGroup();
        }
    }

    private class VariantWriter implements FieldWriter {

        @Override
        public void write(InternalRow row, int ordinal) {
            writeVariant(row.getVariant(ordinal));
        }

        @Override
        public void write(InternalArray arrayData, int ordinal) {
            writeVariant(arrayData.getVariant(ordinal));
        }

        private void writeVariant(Variant variant) {
            recordConsumer.startGroup();
            recordConsumer.startField(Variant.VALUE, 0);
            recordConsumer.addBinary(Binary.fromReusedByteArray(variant.value()));
            recordConsumer.endField(Variant.VALUE, 0);
            recordConsumer.startField(Variant.METADATA, 1);
            recordConsumer.addBinary(Binary.fromReusedByteArray(variant.metadata()));
            recordConsumer.endField(Variant.METADATA, 1);
            recordConsumer.endGroup();
        }
    }

    private Binary timestampToInt96(Timestamp timestamp) {
        int julianDay;
        long nanosOfDay;
        long mills = timestamp.getMillisecond();
        julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
        nanosOfDay =
                (mills % MILLIS_IN_DAY) * NANOS_PER_MILLISECOND + timestamp.getNanoOfMillisecond();

        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(nanosOfDay);
        buf.putInt(julianDay);
        buf.flip();
        return Binary.fromConstantByteBuffer(buf);
    }

    private FieldWriter createDecimalWriter(int precision, int scale) {
        checkArgument(
                precision <= DecimalType.MAX_PRECISION,
                "Decimal precision %s exceeds max precision %s",
                precision,
                DecimalType.MAX_PRECISION);

        class Int32Writer implements FieldWriter {

            @Override
            public void write(InternalArray arrayData, int ordinal) {
                long unscaledLong =
                        (arrayData.getDecimal(ordinal, precision, scale)).toUnscaledLong();
                addRecord(unscaledLong);
            }

            @Override
            public void write(InternalRow row, int ordinal) {
                long unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
                addRecord(unscaledLong);
            }

            private void addRecord(long unscaledLong) {
                recordConsumer.addInteger((int) unscaledLong);
            }
        }

        class Int64Writer implements FieldWriter {

            @Override
            public void write(InternalArray arrayData, int ordinal) {
                long unscaledLong =
                        (arrayData.getDecimal(ordinal, precision, scale)).toUnscaledLong();
                addRecord(unscaledLong);
            }

            @Override
            public void write(InternalRow row, int ordinal) {
                long unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
                addRecord(unscaledLong);
            }

            private void addRecord(long unscaledLong) {
                recordConsumer.addLong(unscaledLong);
            }
        }

        class UnscaledBytesWriter implements FieldWriter {
            private final int numBytes;
            private final byte[] decimalBuffer;

            private UnscaledBytesWriter() {
                this.numBytes = computeMinBytesForDecimalPrecision(precision);
                this.decimalBuffer = new byte[numBytes];
            }

            @Override
            public void write(InternalArray arrayData, int ordinal) {
                byte[] bytes = (arrayData.getDecimal(ordinal, precision, scale)).toUnscaledBytes();
                addRecord(bytes);
            }

            @Override
            public void write(InternalRow row, int ordinal) {
                byte[] bytes = row.getDecimal(ordinal, precision, scale).toUnscaledBytes();
                addRecord(bytes);
            }

            private void addRecord(byte[] bytes) {
                byte[] writtenBytes;
                if (bytes.length == numBytes) {
                    // Avoid copy.
                    writtenBytes = bytes;
                } else {
                    byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
                    Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
                    System.arraycopy(
                            bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
                    writtenBytes = decimalBuffer;
                }
                recordConsumer.addBinary(Binary.fromReusedByteArray(writtenBytes, 0, numBytes));
            }
        }

        if (ParquetSchemaConverter.is32BitDecimal(precision)) {
            return new Int32Writer();
        } else if (ParquetSchemaConverter.is64BitDecimal(precision)) {
            return new Int64Writer();
        } else {
            return new UnscaledBytesWriter();
        }
    }
}
