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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.writable.WritableBooleanVector;
import org.apache.paimon.data.columnar.writable.WritableByteVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableDoubleVector;
import org.apache.paimon.data.columnar.writable.WritableFloatVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;
import org.apache.paimon.data.columnar.writable.WritableShortVector;
import org.apache.paimon.data.columnar.writable.WritableTimestampVector;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Updater Factory to get {@link ParquetVectorUpdater}. */
public class ParquetVectorUpdaterFactory {

    public static ParquetVectorUpdater getUpdater(
            ColumnDescriptor descriptor, DataType paimonType) {
        return paimonType.accept(UpdaterFactoryVisitor.INSTANCE).apply(descriptor);
    }

    interface UpdaterFactory extends Function<ColumnDescriptor, ParquetVectorUpdater> {}

    private static class UpdaterFactoryVisitor implements DataTypeVisitor<UpdaterFactory> {

        private static final UpdaterFactoryVisitor INSTANCE = new UpdaterFactoryVisitor();

        @Override
        public UpdaterFactory visit(CharType charType) {
            return c -> new BinaryUpdater();
        }

        @Override
        public UpdaterFactory visit(VarCharType varCharType) {
            return c -> new BinaryUpdater();
        }

        @Override
        public UpdaterFactory visit(BooleanType booleanType) {
            return c -> new BooleanUpdater();
        }

        @Override
        public UpdaterFactory visit(BinaryType binaryType) {
            return c -> {
                if (c.getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                    return new FixedLenByteArrayUpdater(binaryType.getLength());
                } else {
                    return new BinaryUpdater();
                }
            };
        }

        @Override
        public UpdaterFactory visit(VarBinaryType varBinaryType) {
            return c -> {
                if (c.getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                    return new FixedLenByteArrayUpdater(c.getPrimitiveType().getTypeLength());
                }
                return new BinaryUpdater();
            };
        }

        @Override
        public UpdaterFactory visit(DecimalType decimalType) {
            return c -> {
                switch (c.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return new IntegerToDecimalUpdater(c, decimalType);
                    case INT64:
                        return new LongToDecimalUpdater(c, decimalType);
                    case BINARY:
                        return new BinaryToDecimalUpdater(c, decimalType);
                    case FIXED_LEN_BYTE_ARRAY:
                        return new FixedLenByteArrayToDecimalUpdater(c, decimalType);
                }
                throw new RuntimeException(
                        "Unsupported decimal type: " + c.getPrimitiveType().getPrimitiveTypeName());
            };
        }

        @Override
        public UpdaterFactory visit(TinyIntType tinyIntType) {
            return c -> new ByteUpdater();
        }

        @Override
        public UpdaterFactory visit(SmallIntType smallIntType) {
            return c -> new ShortUpdater();
        }

        @Override
        public UpdaterFactory visit(IntType intType) {
            return c -> new IntegerUpdater();
        }

        @Override
        public UpdaterFactory visit(BigIntType bigIntType) {
            return c -> new LongUpdater();
        }

        @Override
        public UpdaterFactory visit(FloatType floatType) {
            return c -> new FloatUpdater();
        }

        @Override
        public UpdaterFactory visit(DoubleType doubleType) {
            return c -> new DoubleUpdater();
        }

        @Override
        public UpdaterFactory visit(DateType dateType) {
            return c -> new IntegerUpdater();
        }

        @Override
        public UpdaterFactory visit(TimeType timeType) {
            return c -> new IntegerUpdater();
        }

        @Override
        public UpdaterFactory visit(TimestampType timestampType) {
            return c -> {
                if (c.getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.INT64) {
                    return new LongTimestampUpdater(timestampType.getPrecision());
                } else if (c.getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.INT96) {
                    return new TimestampUpdater(timestampType.getPrecision());
                } else {
                    throw new UnsupportedOperationException(
                            "Only support timestamp with int64 and int96 in parquet file yet");
                }
            };
        }

        @Override
        public UpdaterFactory visit(LocalZonedTimestampType localZonedTimestampType) {
            return c -> {
                if (c.getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.INT64) {
                    return new LongUpdater();
                }
                return new TimestampUpdater(localZonedTimestampType.getPrecision());
            };
        }

        @Override
        public UpdaterFactory visit(VariantType variantType) {
            throw new RuntimeException("Variant type is not supported");
        }

        @Override
        public UpdaterFactory visit(BlobType blobType) {
            throw new RuntimeException("Blob type is not supported");
        }

        @Override
        public UpdaterFactory visit(ArrayType arrayType) {
            throw new RuntimeException("Array type is not supported");
        }

        @Override
        public UpdaterFactory visit(MultisetType multisetType) {
            throw new RuntimeException("Multiset type is not supported");
        }

        @Override
        public UpdaterFactory visit(MapType mapType) {
            throw new RuntimeException("Map type is not supported");
        }

        @Override
        public UpdaterFactory visit(RowType rowType) {
            throw new RuntimeException("Row type is not supported");
        }
    }

    private static class BooleanUpdater implements ParquetVectorUpdater<WritableBooleanVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableBooleanVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readBooleans(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipBooleans(total);
        }

        @Override
        public void readValue(
                int offset, WritableBooleanVector values, VectorizedValuesReader valuesReader) {
            values.setBoolean(offset, valuesReader.readBoolean());
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableBooleanVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            throw new UnsupportedOperationException();
        }
    }

    static class IntegerUpdater implements ParquetVectorUpdater<WritableIntVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableIntVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readIntegers(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipIntegers(total);
        }

        @Override
        public void readValue(
                int offset, WritableIntVector values, VectorizedValuesReader valuesReader) {
            values.setInt(offset, valuesReader.readInteger());
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableIntVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            values.setInt(offset, dictionary.decodeToInt(dictionaryIds.getInt(offset)));
        }
    }

    private static class ByteUpdater implements ParquetVectorUpdater<WritableByteVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableByteVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readBytes(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipBytes(total);
        }

        @Override
        public void readValue(
                int offset, WritableByteVector values, VectorizedValuesReader valuesReader) {
            values.setByte(offset, valuesReader.readByte());
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableByteVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            values.setByte(offset, (byte) dictionary.decodeToInt(dictionaryIds.getInt(offset)));
        }
    }

    private static class ShortUpdater implements ParquetVectorUpdater<WritableShortVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableShortVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readShorts(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipShorts(total);
        }

        @Override
        public void readValue(
                int offset, WritableShortVector values, VectorizedValuesReader valuesReader) {
            values.setShort(offset, valuesReader.readShort());
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableShortVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            values.setShort(
                    offset,
                    (short) dictionary.decodeToInt(((HeapIntVector) dictionaryIds).getInt(offset)));
        }
    }

    private static class LongUpdater implements ParquetVectorUpdater<WritableLongVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableLongVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readLongs(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipLongs(total);
        }

        @Override
        public void readValue(
                int offset, WritableLongVector values, VectorizedValuesReader valuesReader) {
            values.setLong(offset, valuesReader.readLong());
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableLongVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            values.setLong(offset, dictionary.decodeToLong(dictionaryIds.getInt(offset)));
        }
    }

    private abstract static class AbstractTimestampUpdater
            implements ParquetVectorUpdater<WritableColumnVector> {

        protected final int precision;

        AbstractTimestampUpdater(int precision) {
            this.precision = precision;
        }

        @Override
        public void readValues(
                int total,
                int offset,
                WritableColumnVector values,
                VectorizedValuesReader valuesReader) {
            for (int i = 0; i < total; i++) {
                readValue(offset + i, values, valuesReader);
            }
        }
    }

    private static class LongTimestampUpdater extends AbstractTimestampUpdater {

        public LongTimestampUpdater(int precision) {
            super(precision);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipLongs(total);
        }

        @Override
        public void readValue(
                int offset, WritableColumnVector values, VectorizedValuesReader valuesReader) {
            long value = valuesReader.readLong();
            putTimestamp(values, offset, value);
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableColumnVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            long value = dictionary.decodeToLong(dictionaryIds.getInt(offset));
            putTimestamp(values, offset, value);
        }

        private void putTimestamp(WritableColumnVector vector, int offset, long timestamp) {
            if (vector instanceof WritableTimestampVector) {
                ((WritableTimestampVector) vector)
                        .setTimestamp(offset, Timestamp.fromEpochMillis(timestamp));
            } else {
                ((WritableLongVector) vector).setLong(offset, timestamp);
            }
        }
    }

    private static class TimestampUpdater extends AbstractTimestampUpdater {

        public static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
        public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
        public static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
        public static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

        public TimestampUpdater(int precision) {
            super(precision);
        }

        @Override
        public void readValues(
                int total,
                int offset,
                WritableColumnVector values,
                VectorizedValuesReader valuesReader) {
            for (int i = 0; i < total; i++) {
                Timestamp timestamp =
                        int96ToTimestamp(true, valuesReader.readLong(), valuesReader.readInteger());
                putTimestamp(values, offset + i, timestamp);
            }
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipBytes(12);
        }

        @Override
        public void readValue(
                int offset, WritableColumnVector values, VectorizedValuesReader valuesReader) {
            putTimestamp(
                    values,
                    offset,
                    int96ToTimestamp(true, valuesReader.readLong(), valuesReader.readInteger()));
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableColumnVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            putTimestamp(
                    values,
                    offset,
                    decodeInt96ToTimestamp(true, dictionary, dictionaryIds.getInt(offset)));
        }

        private void putTimestamp(WritableColumnVector vector, int offset, Timestamp timestamp) {
            if (vector instanceof WritableTimestampVector) {
                ((WritableTimestampVector) vector).setTimestamp(offset, timestamp);
            } else {
                if (precision <= 3) {
                    ((WritableLongVector) vector).setLong(offset, timestamp.getMillisecond());
                } else if (precision <= 6) {
                    ((WritableLongVector) vector).setLong(offset, timestamp.toMicros());
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported timestamp precision: " + precision);
                }
            }
        }

        public static Timestamp decodeInt96ToTimestamp(
                boolean utcTimestamp, org.apache.parquet.column.Dictionary dictionary, int id) {
            Binary binary = dictionary.decodeToBinary(id);
            checkArgument(binary.length() == 12, "Timestamp with int96 should be 12 bytes.");
            ByteBuffer buffer = binary.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            return int96ToTimestamp(utcTimestamp, buffer.getLong(), buffer.getInt());
        }

        public static Timestamp int96ToTimestamp(
                boolean utcTimestamp, long nanosOfDay, int julianDay) {
            long millisecond = julianDayToMillis(julianDay) + (nanosOfDay / NANOS_PER_MILLISECOND);

            if (utcTimestamp) {
                int nanoOfMillisecond = (int) (nanosOfDay % NANOS_PER_MILLISECOND);
                if (nanoOfMillisecond < 0) {
                    millisecond -= 1;
                    nanoOfMillisecond += (int) NANOS_PER_MILLISECOND;
                }
                return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
            } else {
                java.sql.Timestamp timestamp = new java.sql.Timestamp(millisecond);
                timestamp.setNanos((int) (nanosOfDay % NANOS_PER_SECOND));
                return Timestamp.fromSQLTimestamp(timestamp);
            }
        }

        private static long julianDayToMillis(int julianDay) {
            return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
        }
    }

    private static class FloatUpdater implements ParquetVectorUpdater<WritableFloatVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableFloatVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readFloats(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipFloats(total);
        }

        @Override
        public void readValue(
                int offset, WritableFloatVector values, VectorizedValuesReader valuesReader) {
            values.setFloat(offset, valuesReader.readFloat());
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableFloatVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            values.setFloat(offset, dictionary.decodeToFloat(dictionaryIds.getInt(offset)));
        }
    }

    private static class DoubleUpdater implements ParquetVectorUpdater<WritableDoubleVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableDoubleVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readDoubles(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipDoubles(total);
        }

        @Override
        public void readValue(
                int offset, WritableDoubleVector values, VectorizedValuesReader valuesReader) {
            values.setDouble(offset, valuesReader.readDouble());
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableDoubleVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            values.setDouble(offset, dictionary.decodeToDouble(dictionaryIds.getInt(offset)));
        }
    }

    private static class BinaryUpdater implements ParquetVectorUpdater<WritableBytesVector> {
        @Override
        public void readValues(
                int total,
                int offset,
                WritableBytesVector values,
                VectorizedValuesReader valuesReader) {
            valuesReader.readBinary(total, values, offset);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipBinary(total);
        }

        @Override
        public void readValue(
                int offset, WritableBytesVector values, VectorizedValuesReader valuesReader) {
            valuesReader.readBinary(1, values, offset);
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableBytesVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(offset));
            values.putByteArray(offset, v.getBytesUnsafe(), 0, v.length());
        }
    }

    private static class FixedLenByteArrayUpdater
            implements ParquetVectorUpdater<WritableBytesVector> {
        private final int arrayLen;

        FixedLenByteArrayUpdater(int arrayLen) {
            this.arrayLen = arrayLen;
        }

        @Override
        public void readValues(
                int total,
                int offset,
                WritableBytesVector values,
                VectorizedValuesReader valuesReader) {
            for (int i = 0; i < total; i++) {
                readValue(offset + i, values, valuesReader);
            }
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipFixedLenByteArray(total, arrayLen);
        }

        @Override
        public void readValue(
                int offset, WritableBytesVector values, VectorizedValuesReader valuesReader) {
            byte[] bytes = valuesReader.readBinary(arrayLen).getBytesUnsafe();
            values.putByteArray(offset, bytes, 0, bytes.length);
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableBytesVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(offset));
            values.putByteArray(offset, v.getBytesUnsafe(), 0, v.length());
        }
    }

    private abstract static class DecimalUpdater<T extends WritableColumnVector>
            implements ParquetVectorUpdater<T> {

        protected final DecimalType paimonType;

        DecimalUpdater(DecimalType paimonType) {
            this.paimonType = paimonType;
        }

        @Override
        public void readValues(
                int total, int offset, T values, VectorizedValuesReader valuesReader) {
            for (int i = 0; i < total; i++) {
                readValue(offset + i, values, valuesReader);
            }
        }

        protected void putDecimal(WritableColumnVector values, int offset, BigDecimal decimal) {
            int precision = paimonType.getPrecision();
            if (ParquetSchemaConverter.is32BitDecimal(precision)) {
                ((HeapIntVector) values).setInt(offset, decimal.unscaledValue().intValue());
            } else if (ParquetSchemaConverter.is64BitDecimal(precision)) {
                ((HeapLongVector) values).setLong(offset, decimal.unscaledValue().longValue());
            } else {
                byte[] bytes = decimal.unscaledValue().toByteArray();
                ((WritableBytesVector) values).putByteArray(offset, bytes, 0, bytes.length);
            }
        }
    }

    private static class IntegerToDecimalUpdater extends DecimalUpdater<WritableColumnVector> {
        private final int parquetScale;

        IntegerToDecimalUpdater(ColumnDescriptor descriptor, DecimalType paimonType) {
            super(paimonType);
            LogicalTypeAnnotation typeAnnotation =
                    descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            if (typeAnnotation instanceof DecimalLogicalTypeAnnotation) {
                this.parquetScale = ((DecimalLogicalTypeAnnotation) typeAnnotation).getScale();
            } else {
                this.parquetScale = 0;
            }
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipIntegers(total);
        }

        @Override
        public void readValue(
                int offset, WritableColumnVector values, VectorizedValuesReader valuesReader) {
            BigDecimal decimal = BigDecimal.valueOf(valuesReader.readInteger(), parquetScale);
            putDecimal(values, offset, decimal);
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableColumnVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            BigDecimal decimal =
                    BigDecimal.valueOf(
                            dictionary.decodeToInt(dictionaryIds.getInt(offset)), parquetScale);
            putDecimal(values, offset, decimal);
        }
    }

    private static class LongToDecimalUpdater extends DecimalUpdater<WritableColumnVector> {
        private final int parquetScale;

        LongToDecimalUpdater(ColumnDescriptor descriptor, DecimalType paimonType) {
            super(paimonType);
            LogicalTypeAnnotation typeAnnotation =
                    descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            if (typeAnnotation instanceof DecimalLogicalTypeAnnotation) {
                this.parquetScale = ((DecimalLogicalTypeAnnotation) typeAnnotation).getScale();
            } else {
                this.parquetScale = 0;
            }
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipLongs(total);
        }

        @Override
        public void readValue(
                int offset, WritableColumnVector values, VectorizedValuesReader valuesReader) {
            BigDecimal decimal = BigDecimal.valueOf(valuesReader.readLong(), parquetScale);
            putDecimal(values, offset, decimal);
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableColumnVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            BigDecimal decimal =
                    BigDecimal.valueOf(
                            dictionary.decodeToLong(dictionaryIds.getInt(offset)), parquetScale);
            putDecimal(values, offset, decimal);
        }
    }

    private static class BinaryToDecimalUpdater extends DecimalUpdater<WritableColumnVector> {
        private final int parquetScale;
        private final WritableBytesVector bytesVector;

        BinaryToDecimalUpdater(ColumnDescriptor descriptor, DecimalType paimonType) {
            super(paimonType);
            LogicalTypeAnnotation typeAnnotation =
                    descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            this.parquetScale = ((DecimalLogicalTypeAnnotation) typeAnnotation).getScale();
            this.bytesVector = new HeapBytesVector(1);
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipBinary(total);
        }

        @Override
        public void readValue(
                int offset, WritableColumnVector values, VectorizedValuesReader valuesReader) {
            valuesReader.readBinary(1, bytesVector, offset);
            BigInteger value = new BigInteger(bytesVector.getBytes(offset).getBytes());
            BigDecimal decimal = new BigDecimal(value, parquetScale);
            putDecimal(values, offset, decimal);
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableColumnVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            BigInteger value =
                    new BigInteger(
                            dictionary
                                    .decodeToBinary(dictionaryIds.getInt(offset))
                                    .getBytesUnsafe());
            BigDecimal decimal = new BigDecimal(value, parquetScale);
            putDecimal(values, offset, decimal);
        }
    }

    private static class FixedLenByteArrayToDecimalUpdater
            extends DecimalUpdater<WritableColumnVector> {
        private final int arrayLen;

        FixedLenByteArrayToDecimalUpdater(ColumnDescriptor descriptor, DecimalType paimonType) {
            super(paimonType);
            LogicalTypeAnnotation typeAnnotation =
                    descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            int parquetScale = ((DecimalLogicalTypeAnnotation) typeAnnotation).getScale();
            checkArgument(
                    parquetScale == paimonType.getScale(),
                    "Scale should be match between paimon decimal type and parquet decimal type in file");
            this.arrayLen = descriptor.getPrimitiveType().getTypeLength();
        }

        @Override
        public void skipValues(int total, VectorizedValuesReader valuesReader) {
            valuesReader.skipFixedLenByteArray(total, arrayLen);
        }

        @Override
        public void readValue(
                int offset, WritableColumnVector values, VectorizedValuesReader valuesReader) {
            Binary binary = valuesReader.readBinary(arrayLen);

            int precision = paimonType.getPrecision();
            if (ParquetSchemaConverter.is32BitDecimal(precision)) {
                ((HeapIntVector) values).setInt(offset, (int) heapBinaryToLong(binary));
            } else if (ParquetSchemaConverter.is64BitDecimal(precision)) {
                ((HeapLongVector) values).setLong(offset, heapBinaryToLong(binary));
            } else {
                byte[] bytes = binary.getBytesUnsafe();
                ((WritableBytesVector) values).putByteArray(offset, bytes, 0, bytes.length);
            }
        }

        private long heapBinaryToLong(Binary binary) {
            ByteBuffer buffer = binary.toByteBuffer();
            byte[] bytes = buffer.array();
            int start = buffer.arrayOffset() + buffer.position();
            int end = buffer.arrayOffset() + buffer.limit();

            long unscaled = 0L;

            for (int i = start; i < end; i++) {
                unscaled = (unscaled << 8) | (bytes[i] & 0xff);
            }

            int bits = 8 * (end - start);
            return (unscaled << (64 - bits)) >> (64 - bits);
        }

        @Override
        public void decodeSingleDictionaryId(
                int offset,
                WritableColumnVector values,
                WritableIntVector dictionaryIds,
                Dictionary dictionary) {
            Binary binary = dictionary.decodeToBinary(dictionaryIds.getInt(offset));
            int precision = paimonType.getPrecision();
            if (ParquetSchemaConverter.is32BitDecimal(precision)) {
                ((HeapIntVector) values).setInt(offset, (int) heapBinaryToLong(binary));
            } else if (ParquetSchemaConverter.is64BitDecimal(precision)) {
                ((HeapLongVector) values).setLong(offset, heapBinaryToLong(binary));
            } else {
                byte[] bytes = binary.getBytesUnsafe();
                ((WritableBytesVector) values).putByteArray(offset, bytes, 0, bytes.length);
            }
        }
    }
}
