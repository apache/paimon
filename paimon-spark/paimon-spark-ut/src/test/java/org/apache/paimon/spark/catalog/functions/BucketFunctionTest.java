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
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
    private static final String ID_COL = "id_col";

    private static final int DECIMAL_PRECISION = 38;
    private static final int DECIMAL_SCALE = 18;
    private static final int COMPACTED_DECIMAL_PRECISION = 18;
    private static final int COMPACTED_DECIMAL_SCALE = 9;
    private static final int TIMESTAMP_PRECISION = 6;

    private static final String TIMESTAMP_COL_PRECISION_3 = "timestamp_col_precision_3";

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
                                    12, BINARY_COL, new VarBinaryType(VarBinaryType.MAX_LENGTH)),
                            new DataField(13, ID_COL, new IntType()),
                            new DataField(14, TIMESTAMP_COL_PRECISION_3, new TimestampType(3))));

    private static final InternalRow NULL_PAIMON_ROW =
            GenericRow.of(
                    null, null, null, null, null, null, null, null, null, null, null, null, null, 0,
                    null);

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
                Timestamp.fromMicros(System.nanoTime() / 1000),
                Timestamp.fromMicros(System.nanoTime() / 1000),
                UUID.randomUUID().toString().getBytes(),
                1,
                Timestamp.fromEpochMillis(System.currentTimeMillis()));
    }

    private static final String TABLE_NAME = "test_bucket";
    private static SparkSession spark;
    private static Path warehousePath;

    private static Path tablePath;
    private static Path ioManagerPath;

    @BeforeAll
    public static void setup(@TempDir java.nio.file.Path tempDir) {
        ioManagerPath = new Path(tempDir.toString(), "io-manager");
        warehousePath = new Path(tempDir.toString());
        tablePath = new Path(warehousePath, "db.db/" + TABLE_NAME);
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();
        spark.conf().set("spark.sql.catalog.paimon", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.paimon.warehouse", warehousePath.toString());
        spark.sql("CREATE DATABASE paimon.db");
        spark.sql("USE paimon.db");
    }

    @AfterEach
    public void after() {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    }

    @AfterAll
    public static void tearDown() {
        spark.stop();
    }

    public static void setupTable(String... bucketColumns) {
        String commitUser = UUID.randomUUID().toString();
        try {
            TableSchema tableSchema =
                    SchemaUtils.forceCommit(
                            new SchemaManager(LocalFileIO.create(), tablePath),
                            new Schema(
                                    ROW_TYPE.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    ImmutableMap.of(
                                            CoreOptions.BUCKET_KEY.key(),
                                            String.join(",", bucketColumns),
                                            CoreOptions.BUCKET.key(),
                                            String.valueOf(NUM_BUCKETS),
                                            "file.format",
                                            "avro"),
                                    ""));
            FileStoreTable storeTable =
                    FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
            TableWriteImpl<?> write =
                    storeTable
                            .newWrite(commitUser)
                            .withIOManager(new IOManagerImpl(ioManagerPath.toString()));
            TableCommitImpl tableCommit = storeTable.newCommit(commitUser);

            write.write(randomPaimonInternalRow());
            write.write(NULL_PAIMON_ROW);
            tableCommit.commit(0, write.prepareCommit(false, 0));

            write.close();
            tableCommit.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void validateSparkBucketFunction(String... bucketColumns) {
        setupTable(bucketColumns);
        spark.sql(
                        String.format(
                                "SELECT id_col, __paimon_bucket as expected_bucket, paimon.bucket(%s, %s) FROM %s",
                                NUM_BUCKETS, String.join(",", bucketColumns), TABLE_NAME))
                .collectAsList()
                .forEach(row -> assertThat(row.getInt(2)).isEqualTo(row.get(1)));
    }

    @Test
    public void testBooleanType() {
        validateSparkBucketFunction(BOOLEAN_COL);
    }

    @Test
    public void testByteType() {
        validateSparkBucketFunction(BYTE_COL);
    }

    @Test
    public void testShortType() {
        validateSparkBucketFunction(SHORT_COL);
    }

    @Test
    public void testIntegerType() {
        validateSparkBucketFunction(INTEGER_COL);
    }

    @Test
    public void testLongType() {
        validateSparkBucketFunction(LONG_COL);
    }

    @Test
    public void testFloatType() {
        validateSparkBucketFunction(FLOAT_COL);
    }

    @Test
    public void testDoubleType() {
        validateSparkBucketFunction(DOUBLE_COL);
    }

    @Test
    public void testStringType() {
        validateSparkBucketFunction(STRING_COL);
    }

    @Test
    public void testBinaryType() {
        validateSparkBucketFunction(BINARY_COL);
    }

    @Test
    public void testTimestampType() {
        validateSparkBucketFunction(TIMESTAMP_COL);
    }

    @Test
    public void testLZTimestampType() {
        validateSparkBucketFunction(LZ_TIMESTAMP_COL);
    }

    @Test
    public void testDecimalType() {
        validateSparkBucketFunction(DECIMAL_COL);
    }

    @Test
    public void testCompactedDecimalType() {
        validateSparkBucketFunction(COMPACTED_DECIMAL_COL);
    }

    @Test
    public void testMultipleTypes() {
        List<String> allColumns = ROW_TYPE.getFieldNames();
        String[] bucketColumns = null;
        while (bucketColumns == null || bucketColumns.length < 2) {
            bucketColumns =
                    allColumns.stream()
                            .filter(e -> !Objects.equals(TIMESTAMP_COL_PRECISION_3, e))
                            .filter(e -> ThreadLocalRandom.current().nextBoolean())
                            .toArray(String[]::new);
        }

        validateSparkBucketFunction(bucketColumns);
    }

    @Test
    public void testTimestampPrecisionNotEqualToSpark() {
        setupTable(TIMESTAMP_COL_PRECISION_3);
        spark.sql(
                        String.format(
                                "SELECT id_col, __paimon_bucket as expected_bucket, paimon.bucket(%s, %s) FROM %s",
                                NUM_BUCKETS,
                                String.join(",", TIMESTAMP_COL_PRECISION_3),
                                TABLE_NAME))
                .collectAsList()
                .forEach(row -> assertThat(row.getInt(2)).isNotEqualTo(row.get(1)));
    }
}
