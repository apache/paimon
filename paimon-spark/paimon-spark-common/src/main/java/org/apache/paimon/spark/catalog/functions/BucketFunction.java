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

package org.apache.paimon.spark.catalog.functions;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.spark.SparkConversions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BucketFunction implements UnboundFunction {
    private static final int NUM_BUCKETS_ORDINAL = 0;
    private static final int SPARK_TIMESTAMP_PRECISION = 6;

    private static final Map<String, Class<? extends BucketGeneric>> BUCKET_FUNCTIONS;

    static {
        ImmutableMap.Builder<String, Class<? extends BucketGeneric>> builder =
                ImmutableMap.builder();
        builder.put("BucketBoolean", BucketBoolean.class);
        builder.put("BucketByte", BucketByte.class);
        builder.put("BucketShort", BucketShort.class);
        builder.put("BucketInteger", BucketInteger.class);
        builder.put("BucketLong", BucketLong.class);
        builder.put("BucketFloat", BucketFloat.class);
        builder.put("BucketDouble", BucketDouble.class);
        builder.put("BucketString", BucketString.class);
        builder.put("BucketDecimal", BucketDecimal.class);
        builder.put("BucketTimestamp", BucketTimestamp.class);
        builder.put("BucketBinary", BucketBinary.class);

        // Joint bucket fields of common types
        builder.put("BucketIntegerInteger", BucketIntegerInteger.class);
        builder.put("BucketIntegerLong", BucketIntegerLong.class);
        builder.put("BucketIntegerString", BucketIntegerString.class);
        builder.put("BucketLongInteger", BucketLongInteger.class);
        builder.put("BucketLongLong", BucketLongLong.class);
        builder.put("BucketLongString", BucketLongString.class);
        builder.put("BucketStringInteger", BucketStringInteger.class);
        builder.put("BucketStringLong", BucketStringLong.class);
        builder.put("BucketStringString", BucketStringString.class);

        BUCKET_FUNCTIONS = builder.build();
    }

    public static boolean supportsTable(FileStoreTable table) {
        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            return false;
        }

        return table.schema().logicalBucketKeyType().getFieldTypes().stream()
                .allMatch(BucketFunction::supportsType);
    }

    private static boolean supportsType(org.apache.paimon.types.DataType type) {
        if (type instanceof ArrayType
                || type instanceof MapType
                || type instanceof RowType
                || type instanceof VariantType) {
            return false;
        }

        if (type instanceof org.apache.paimon.types.TimestampType) {
            return ((org.apache.paimon.types.TimestampType) type).getPrecision()
                    == SPARK_TIMESTAMP_PRECISION;
        }

        if (type instanceof org.apache.paimon.types.LocalZonedTimestampType) {
            return ((org.apache.paimon.types.LocalZonedTimestampType) type).getPrecision()
                    == SPARK_TIMESTAMP_PRECISION;
        }

        return true;
    }

    @Override
    public BoundFunction bind(StructType inputType) {
        StructField[] fields = inputType.fields();

        StringBuilder classNameBuilder = new StringBuilder("Bucket");
        DataType[] bucketKeyTypes = new DataType[fields.length - 1];
        for (int i = 1; i < fields.length; i += 1) {
            DataType dataType = fields[i].dataType();
            bucketKeyTypes[i - 1] = dataType;
            if (dataType instanceof BooleanType) {
                classNameBuilder.append("Boolean");
            } else if (dataType instanceof ByteType) {
                classNameBuilder.append("Byte");
            } else if (dataType instanceof ShortType) {
                classNameBuilder.append("Short");
            } else if (dataType instanceof IntegerType) {
                classNameBuilder.append("Integer");
            } else if (dataType instanceof LongType) {
                classNameBuilder.append("Long");
            } else if (dataType instanceof FloatType) {
                classNameBuilder.append("Float");
            } else if (dataType instanceof DoubleType) {
                classNameBuilder.append("Double");
            } else if (dataType instanceof StringType) {
                classNameBuilder.append("String");
            } else if (dataType instanceof DecimalType) {
                classNameBuilder.append("Decimal");
            } else if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
                classNameBuilder.append("Timestamp");
            } else if (dataType instanceof BinaryType) {
                classNameBuilder.append("Binary");
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported type: " + dataType.simpleString());
            }
        }

        Class<? extends BucketGeneric> bucketClass =
                BUCKET_FUNCTIONS.getOrDefault(classNameBuilder.toString(), BucketGeneric.class);

        try {
            return bucketClass
                    .getConstructor(DataType[].class)
                    .newInstance(
                            (Object)
                                    bucketKeyTypes /* cast DataType[] to Object as newInstance takes varargs */);
        } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String description() {
        return name() + "(numBuckets, col1, col2, ...)";
    }

    @Override
    public String name() {
        return "bucket";
    }

    public static class BucketGeneric implements ScalarFunction<Integer> {
        protected final DataType[] bucketKeyTypes;
        protected final BinaryRow bucketKeyRow;
        // not serializable
        protected transient BinaryRowWriter bucketKeyWriter;
        private transient ValueWriter[] valueWriters;

        public BucketGeneric(DataType[] sqlTypes) {
            this.bucketKeyTypes = sqlTypes;
            this.bucketKeyRow = new BinaryRow(bucketKeyTypes.length);
            this.bucketKeyWriter = new BinaryRowWriter(bucketKeyRow);
            this.valueWriters = createValueWriter(bucketKeyTypes);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            this.bucketKeyWriter = new BinaryRowWriter(bucketKeyRow);
            this.valueWriters = createValueWriter(bucketKeyTypes);
        }

        private static ValueWriter[] createValueWriter(DataType[] columnTypes) {
            ValueWriter[] writers = new ValueWriter[columnTypes.length];
            for (int i = 0; i < columnTypes.length; i += 1) {
                writers[i] = ValueWriter.of(columnTypes[i]);
            }

            return writers;
        }

        @Override
        public Integer produceResult(InternalRow input) {
            bucketKeyWriter.reset();
            for (int i = 0; i < valueWriters.length; i += 1) {
                valueWriters[i].write(bucketKeyWriter, i, input, i + 1);
            }

            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, input.getInt(NUM_BUCKETS_ORDINAL));
        }

        @Override
        public DataType[] inputTypes() {
            DataType[] inputTypes = new DataType[bucketKeyTypes.length + 1];
            inputTypes[0] = DataTypes.IntegerType;
            for (int i = 0; i < bucketKeyTypes.length; i += 1) {
                inputTypes[i + 1] = bucketKeyTypes[i];
            }

            return inputTypes;
        }

        @Override
        public DataType resultType() {
            return DataTypes.IntegerType;
        }

        @Override
        public boolean isResultNullable() {
            return false;
        }

        @Override
        public String name() {
            return "bucket";
        }

        @Override
        public String canonicalName() {
            return String.format(
                    "paimon.bucket(%s)",
                    Arrays.stream(bucketKeyTypes)
                            .map(DataType::catalogString)
                            .collect(Collectors.joining(", ")));
        }

        // org.apache.paimon.table.sink.KeyAndBucketExtractor.bucket(int, int)
        protected static int bucket(BinaryRow bucketKey, int numBuckets) {
            return Math.abs(bucketKey.hashCode() % numBuckets);
        }
    }

    public static class BucketBoolean extends BucketGeneric {

        public BucketBoolean(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, boolean value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeBoolean(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketByte extends BucketGeneric {

        public BucketByte(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, byte value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeByte(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketShort extends BucketGeneric {

        public BucketShort(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, short value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeShort(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketInteger extends BucketGeneric {

        public BucketInteger(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, int value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeInt(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketLong extends BucketGeneric {

        public BucketLong(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, long value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeLong(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketFloat extends BucketGeneric {

        public BucketFloat(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, float value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeFloat(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketDouble extends BucketGeneric {

        public BucketDouble(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, double value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeDouble(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketString extends BucketGeneric {

        public BucketString(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, UTF8String value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeString(0, BinaryString.fromBytes(value0.getBytes()));
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketDecimal extends BucketGeneric {
        private final int precision;
        private final int scale;
        private final Function<Decimal, org.apache.paimon.data.Decimal> converter;

        public BucketDecimal(DataType[] sqlTypes) {
            super(sqlTypes);
            DecimalType decimalType = (DecimalType) sqlTypes[0];
            this.precision = decimalType.precision();
            this.scale = decimalType.scale();
            this.converter = SparkConversions.decimalSparkToPaimon(precision, scale);
        }

        public int invoke(int numBuckets, Decimal value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeDecimal(0, converter.apply(value0), precision);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    /** Bucket function for {@code TimestampType} type with precision 6. */
    public static class BucketTimestamp extends BucketGeneric {

        public BucketTimestamp(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, long value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeTimestamp(
                    0, Timestamp.fromMicros(value0), SPARK_TIMESTAMP_PRECISION);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketBinary extends BucketGeneric {

        public BucketBinary(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, byte[] value0) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeBinary(0, value0);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketIntegerInteger extends BucketGeneric {

        public BucketIntegerInteger(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, int value0, int value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeInt(0, value0);
            bucketKeyWriter.writeInt(1, value1);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketIntegerLong extends BucketGeneric {

        public BucketIntegerLong(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, int value0, long value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeInt(0, value0);
            bucketKeyWriter.writeLong(0, value1);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketIntegerString extends BucketGeneric {

        public BucketIntegerString(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, int value0, UTF8String value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeInt(0, value0);
            bucketKeyWriter.writeString(1, BinaryString.fromBytes(value1.getBytes()));
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketLongLong extends BucketGeneric {

        public BucketLongLong(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, long value0, long value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeLong(0, value0);
            bucketKeyWriter.writeLong(1, value1);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketLongInteger extends BucketGeneric {

        public BucketLongInteger(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, long value0, int value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeLong(0, value0);
            bucketKeyWriter.writeInt(0, value1);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketLongString extends BucketGeneric {

        public BucketLongString(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, long value0, UTF8String value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeLong(0, value0);
            bucketKeyWriter.writeString(1, BinaryString.fromBytes(value1.getBytes()));
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketStringInteger extends BucketGeneric {

        public BucketStringInteger(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, UTF8String value0, int value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeString(0, BinaryString.fromBytes(value0.getBytes()));
            bucketKeyWriter.writeInt(1, value1);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketStringLong extends BucketGeneric {

        public BucketStringLong(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, UTF8String value0, long value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeString(0, BinaryString.fromBytes(value0.getBytes()));
            bucketKeyWriter.writeLong(1, value1);
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    public static class BucketStringString extends BucketGeneric {

        public BucketStringString(DataType[] sqlTypes) {
            super(sqlTypes);
        }

        public int invoke(int numBuckets, UTF8String value0, UTF8String value1) {
            bucketKeyWriter.reset();
            bucketKeyWriter.writeString(0, BinaryString.fromBytes(value0.getBytes()));
            bucketKeyWriter.writeString(1, BinaryString.fromBytes(value1.getBytes()));
            bucketKeyWriter.complete();
            return bucket(bucketKeyRow, numBuckets);
        }
    }

    @FunctionalInterface
    interface ValueWriter {
        void write(BinaryRowWriter writer, int writePos, InternalRow srcRow, int srcPos);

        static ValueWriter of(DataType type) {
            if (type instanceof BooleanType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeBoolean(writePos, srcRow.getBoolean(srcPos));
                    }
                };

            } else if (type instanceof ByteType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeByte(writePos, srcRow.getByte(srcPos));
                    }
                };

            } else if (type instanceof ShortType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeShort(writePos, srcRow.getShort(srcPos));
                    }
                };

            } else if (type instanceof IntegerType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeInt(writePos, srcRow.getInt(srcPos));
                    }
                };

            } else if (type instanceof LongType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeLong(writePos, srcRow.getLong(srcPos));
                    }
                };

            } else if (type instanceof FloatType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeFloat(writePos, srcRow.getFloat(srcPos));
                    }
                };

            } else if (type instanceof DoubleType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeDouble(writePos, srcRow.getDouble(srcPos));
                    }
                };

            } else if (type instanceof StringType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeString(
                                writePos,
                                BinaryString.fromBytes(srcRow.getUTF8String(srcPos).getBytes()));
                    }
                };

            } else if (type instanceof BinaryType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        writer.setNullAt(writePos);
                    } else {
                        writer.writeBinary(writePos, srcRow.getBinary(srcPos));
                    }
                };

            } else if (type instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) type;
                int precision = decimalType.precision();
                int scale = decimalType.scale();
                boolean compact = org.apache.paimon.data.Decimal.isCompact(precision);
                Function<Decimal, org.apache.paimon.data.Decimal> converter =
                        SparkConversions.decimalSparkToPaimon(precision, scale);

                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        // org.apache.paimon.codegen.GenerateUtils.binaryWriterWriteNull
                        if (!compact) {
                            writer.writeDecimal(writePos, null, precision);
                        } else {
                            writer.setNullAt(writePos);
                        }
                    } else {
                        Decimal decimal = srcRow.getDecimal(srcPos, precision, scale);
                        writer.writeDecimal(writePos, converter.apply(decimal), precision);
                    }
                };

            } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
                return (writer, writePos, srcRow, srcPos) -> {
                    if (srcRow.isNullAt(srcPos)) {
                        // org.apache.paimon.codegen.GenerateUtils.binaryWriterWriteNull
                        // must not be compacted as only Spark default precision 6 should be allowed
                        writer.writeTimestamp(writePos, null, SPARK_TIMESTAMP_PRECISION);
                    } else {
                        writer.writeTimestamp(
                                writePos,
                                Timestamp.fromMicros(srcRow.getLong(srcPos)),
                                SPARK_TIMESTAMP_PRECISION);
                    }
                };

            } else {
                throw new UnsupportedOperationException("Unsupported type: " + type.simpleString());
            }
        }
    }
}
