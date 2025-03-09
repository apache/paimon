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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.spark.SparkTypeUtils;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Spark bucket functions. */
public class BucketFunctionTest {
    private static final int NUM_BUCKETS =
            ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);

    private static final String BOOLEAN_COL = "boolean_col";
    private static final String BYTE_COL = "byte_col";
    private static final String SHORT_COL = "short_col";
    private static final String INTEGER_COL = "integer_col";
    private static final String LONG_COL = "long_col";
    private static final String FLOAT_COL = "float_col";
    private static final String DOUBLE_COL = "double_col";
    private static final String STRING_COL = "string_col";
    private static final String DECIMAL_COL = "decimal_col";
    private static final String COMPACTED_DECIMAL_COL = "compacted_decimal_col";
    private static final String TIMESTAMP_COL = "timestamp_col";
    private static final String LZ_TIMESTAMP_COL = "lz_timestamp_col";
    private static final String BINARY_COL = "binary_col";

    private static final int DECIMAL_PRECISION = 38;
    private static final int DECIMAL_SCALE = 18;
    private static final int COMPACTED_DECIMAL_PRECISION = 18;
    private static final int COMPACTED_DECIMAL_SCALE = 9;
    private static final int TIMESTAMP_PRECISION = 6;

    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, BOOLEAN_COL, new BooleanType()),
                            new DataField(1, BYTE_COL, new TinyIntType()),
                            new DataField(2, SHORT_COL, new SmallIntType()),
                            new DataField(3, INTEGER_COL, new IntType()),
                            new DataField(4, LONG_COL, new BigIntType()),
                            new DataField(5, FLOAT_COL, new FloatType()),
                            new DataField(6, DOUBLE_COL, new DoubleType()),
                            new DataField(7, STRING_COL, new VarCharType(VarCharType.MAX_LENGTH)),
                            new DataField(
                                    8,
                                    DECIMAL_COL,
                                    new DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)),
                            new DataField(
                                    9,
                                    COMPACTED_DECIMAL_COL,
                                    new DecimalType(
                                            COMPACTED_DECIMAL_PRECISION, COMPACTED_DECIMAL_SCALE)),
                            new DataField(
                                    10, TIMESTAMP_COL, new TimestampType(TIMESTAMP_PRECISION)),
                            new DataField(
                                    11,
                                    LZ_TIMESTAMP_COL,
                                    new LocalZonedTimestampType(TIMESTAMP_PRECISION)),
                            new DataField(
                                    12, BINARY_COL, new VarBinaryType(VarBinaryType.MAX_LENGTH))));

    private static final StructType SPARK_TYPE = SparkTypeUtils.fromPaimonRowType(ROW_TYPE);

    private static InternalRow randomPaimonInternalRow() {
        Random random = new Random();
        BigInteger unscaled = new BigInteger(String.valueOf(random.nextInt()));
        BigDecimal bigDecimal1 = new BigDecimal(unscaled, DECIMAL_SCALE);
        BigDecimal bigDecimal2 = new BigDecimal(unscaled, COMPACTED_DECIMAL_SCALE);

        return GenericRow.of(
                random.nextBoolean(),
                (byte) random.nextInt(),
                (short) random.nextInt(),
                random.nextInt(),
                random.nextLong(),
                random.nextFloat(),
                random.nextDouble(),
                BinaryString.fromString(UUID.randomUUID().toString()),
                Decimal.fromBigDecimal(bigDecimal1, DECIMAL_PRECISION, DECIMAL_SCALE),
                Decimal.fromBigDecimal(
                        bigDecimal2, COMPACTED_DECIMAL_PRECISION, COMPACTED_DECIMAL_SCALE),
                Timestamp.now(),
                Timestamp.now(),
                UUID.randomUUID().toString().getBytes());
    }

    private static final InternalRow NULL_PAIMON_ROW =
            GenericRow.of(
                    null, null, null, null, null, null, null, null, null, null, null, null, null);

    @Test
    public void testBooleanType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {BOOLEAN_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testByteType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {BYTE_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testShortType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {SHORT_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testIntegerType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {INTEGER_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testLongType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {LONG_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testFloatType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {FLOAT_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testDoubleType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {DOUBLE_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testStringType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {STRING_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testBinaryType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {BINARY_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testTimestampType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {TIMESTAMP_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testLZTimestampType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {LZ_TIMESTAMP_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testDecimalType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {DECIMAL_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testCompactedDecimalType() {
        InternalRow internalRow = randomPaimonInternalRow();
        String[] bucketColumns = {COMPACTED_DECIMAL_COL};

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    @Test
    public void testGenericType() {
        InternalRow internalRow = randomPaimonInternalRow();

        List<String> allColumns = ROW_TYPE.getFieldNames();
        // the order of columns matters
        Collections.shuffle(allColumns);
        String[] bucketColumns = null;
        while (bucketColumns == null || bucketColumns.length < 2) {
            bucketColumns =
                    allColumns.stream()
                            .filter(e -> ThreadLocalRandom.current().nextBoolean())
                            .toArray(String[]::new);
        }

        assertBucketEquals(internalRow, bucketColumns);
        assertBucketEquals(NULL_PAIMON_ROW, bucketColumns);
    }

    private static void assertBucketEquals(InternalRow paimonRow, String... bucketColumns) {
        assertThat(bucketOfSparkFunction(paimonRow, bucketColumns))
                .as(
                        String.format(
                                "The bucket computed by spark function should be identical to the bucket computed by paimon bucket extractor function, row: %s",
                                paimonRow))
                .isEqualTo(bucketOfPaimonFunction(paimonRow, bucketColumns));
    }

    private static Object[] columnSparkValues(InternalRow paimonRow, String... columns) {
        Object[] values = new Object[columns.length];

        for (int i = 0; i < columns.length; i += 1) {
            String column = columns[i];
            org.apache.paimon.types.DataType paimonType = ROW_TYPE.getField(column).type();
            int fieldIndex = ROW_TYPE.getFieldIndex(column);

            if (paimonRow.isNullAt(fieldIndex)) {
                values[i] = null;
                continue;
            }

            if (paimonType instanceof BooleanType) {
                values[i] = paimonRow.getBoolean(fieldIndex);
            } else if (paimonType instanceof TinyIntType) {
                values[i] = paimonRow.getByte(fieldIndex);
            } else if (paimonType instanceof SmallIntType) {
                values[i] = paimonRow.getShort(fieldIndex);
            } else if (paimonType instanceof IntType) {
                values[i] = paimonRow.getInt(fieldIndex);
            } else if (paimonType instanceof BigIntType) {
                values[i] = paimonRow.getLong(fieldIndex);
            } else if (paimonType instanceof FloatType) {
                values[i] = paimonRow.getFloat(fieldIndex);
            } else if (paimonType instanceof DoubleType) {
                values[i] = paimonRow.getDouble(fieldIndex);
            } else if (paimonType instanceof VarCharType) {
                values[i] = UTF8String.fromBytes(paimonRow.getString(fieldIndex).toBytes());
            } else if (paimonType instanceof TimestampType
                    || paimonType instanceof LocalZonedTimestampType) {
                values[i] = paimonRow.getTimestamp(fieldIndex, 9).toMicros();
            } else if (paimonType instanceof DecimalType) {
                int precision = ((DecimalType) paimonType).getPrecision();
                int scale = ((DecimalType) paimonType).getScale();
                Decimal paimonDecimal = paimonRow.getDecimal(fieldIndex, precision, scale);
                values[i] =
                        org.apache.spark.sql.types.Decimal.apply(
                                paimonDecimal.toBigDecimal(), precision, scale);
            } else if (paimonType instanceof VarBinaryType) {
                values[i] = paimonRow.getBinary(fieldIndex);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported type: " + paimonType.asSQLString());
            }
        }

        return values;
    }

    private static Object[] invokeParameters(Object[] sparkColumnValues) {
        Object[] ret = new Object[sparkColumnValues.length + 1];
        ret[0] = NUM_BUCKETS;
        System.arraycopy(sparkColumnValues, 0, ret, 1, sparkColumnValues.length);
        return ret;
    }

    private static StructType bucketColumnsSparkType(String... columns) {
        StructField[] inputFields = new StructField[columns.length + 1];
        inputFields[0] =
                new StructField("num_buckets", DataTypes.IntegerType, false, Metadata.empty());

        for (int i = 0; i < columns.length; i += 1) {
            String column = columns[i];
            inputFields[i + 1] = SPARK_TYPE.apply(column);
        }

        return new StructType(inputFields);
    }

    private static int bucketOfSparkFunction(InternalRow paimonRow, String... bucketColumns) {
        BucketFunction unbound = new BucketFunction();
        StructType inputSqlType = bucketColumnsSparkType(bucketColumns);
        BoundFunction boundFunction = unbound.bind(inputSqlType);

        Object[] inputValues = invokeParameters(columnSparkValues(paimonRow, bucketColumns));

        // null values can only be handled by #produceResult
        if (boundFunction.getClass() == BucketFunction.BucketGeneric.class
                || Arrays.stream(inputValues).anyMatch(v -> v == null)) {
            return ((BucketFunction.BucketGeneric) boundFunction)
                    .produceResult(new GenericInternalRow(inputValues));
        } else {
            Class[] parameterTypes = new Class[inputSqlType.fields().length];
            StructField[] inputFields = inputSqlType.fields();
            for (int i = 0; i < inputSqlType.fields().length; i += 1) {
                DataType columnType = inputFields[i].dataType();
                if (columnType == DataTypes.BooleanType) {
                    parameterTypes[i] = boolean.class;
                } else if (columnType == DataTypes.ByteType) {
                    parameterTypes[i] = byte.class;
                } else if (columnType == DataTypes.ShortType) {
                    parameterTypes[i] = short.class;
                } else if (columnType == DataTypes.IntegerType) {
                    parameterTypes[i] = int.class;
                } else if (columnType == DataTypes.LongType
                        || columnType == DataTypes.TimestampType
                        || columnType == DataTypes.TimestampNTZType) {
                    parameterTypes[i] = long.class;
                } else if (columnType == DataTypes.FloatType) {
                    parameterTypes[i] = float.class;
                } else if (columnType == DataTypes.DoubleType) {
                    parameterTypes[i] = double.class;
                } else if (columnType == DataTypes.StringType) {
                    parameterTypes[i] = UTF8String.class;
                } else if (columnType instanceof org.apache.spark.sql.types.DecimalType) {
                    parameterTypes[i] = org.apache.spark.sql.types.Decimal.class;
                } else if (columnType == DataTypes.BinaryType) {
                    parameterTypes[i] = byte[].class;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported type: " + columnType.sql());
                }
            }

            try {
                Method invoke = boundFunction.getClass().getMethod("invoke", parameterTypes);
                return (int) invoke.invoke(boundFunction, inputValues);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static int bucketOfPaimonFunction(InternalRow internalRow, String... bucketColumns) {
        List<DataField> fields = ROW_TYPE.getFields();
        TableSchema schema =
                new TableSchema(
                        0,
                        fields,
                        RowType.currentHighestFieldId(fields),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        ImmutableMap.of(
                                CoreOptions.BUCKET.key(),
                                String.valueOf(NUM_BUCKETS),
                                CoreOptions.BUCKET_KEY.key(),
                                Joiner.on(",").join(bucketColumns)),
                        "");

        FixedBucketRowKeyExtractor fixedBucketRowKeyExtractor =
                new FixedBucketRowKeyExtractor(schema);
        fixedBucketRowKeyExtractor.setRecord(internalRow);
        return fixedBucketRowKeyExtractor.bucket();
    }
}
