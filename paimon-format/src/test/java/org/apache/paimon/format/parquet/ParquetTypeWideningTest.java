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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reads where the declared type is wider than the type stored in the Parquet file.
 *
 * <p>A Format Table takes its schema from the metastore while its files are written by someone
 * else, so the two can disagree: an {@code ALTER TABLE ... CHANGE c int BIGINT} leaves every
 * existing file with an INT32 column, and an unsigned INT32 column is imported as BIGINT because
 * that is the only Paimon type that can hold it. Both the vectorized read and the predicate
 * pushdown have to cope.
 *
 * <p>Column names mirror midas_prod.tbl_imp, where this was found.
 *
 * <p>The same harness covers the other way a foreign file can surprise the reader: a physical
 * encoding Paimon's own writer never produces, with the declared and stored types agreeing.
 */
class ParquetTypeWideningTest {

    @TempDir File folder;

    private static final RowType ECPM_BIGINT =
            RowType.builder()
                    .field("pageviewId", DataTypes.STRING())
                    .field("ecpm", DataTypes.BIGINT())
                    .field("revenue", DataTypes.BIGINT())
                    .build();

    private static final long[] ECPM_VALUES = {10L, 150L, 3000L};

    private static final String[] PAGEVIEW_IDS = {"a", "b", "c"};

    // ------------------------------------------------------------------
    // Widening INT32 -> BIGINT, the shape reported from production.
    // ------------------------------------------------------------------

    @Test
    void testSignedInt32ReadAsBigInt() throws Exception {
        Path path = write(ecpmSchema("int32 ecpm"));

        assertThat(longs(read(ECPM_BIGINT, path, null), 1)).containsExactly(10L, 150L, 3000L);
    }

    @Test
    void testSignedInt32ReadAsBigIntWithPushdown() throws Exception {
        Path path = write(ecpmSchema("int32 ecpm"));
        PredicateBuilder builder = new PredicateBuilder(ECPM_BIGINT);

        List<Object[]> rows =
                read(
                        ECPM_BIGINT,
                        path,
                        Arrays.asList(builder.lessThan(1, 5000L), builder.greaterThan(2, 0L)));

        assertThat(longs(rows, 1)).containsExactly(10L, 150L, 3000L);
    }

    /** A predicate that excludes the whole row group must still prune it after the type fix. */
    @Test
    void testSignedInt32PushdownStillPrunes() throws Exception {
        Path path = write(ecpmSchema("int32 ecpm"));
        PredicateBuilder builder = new PredicateBuilder(ECPM_BIGINT);

        List<Object[]> rows =
                read(ECPM_BIGINT, path, Collections.singletonList(builder.greaterThan(1, 99999L)));

        assertThat(rows).isEmpty();
    }

    /**
     * The literal does not fit in INT32. Narrowing it would turn a bound of 3000000000 into one of
     * -1294967296, which prunes the row group and silently drops every row. The predicate has to be
     * abandoned instead.
     */
    @Test
    void testLiteralOutsideInt32RangeIsNotTruncated() throws Exception {
        Path path = write(ecpmSchema("int32 ecpm"));
        PredicateBuilder builder = new PredicateBuilder(ECPM_BIGINT);

        List<Object[]> rows =
                read(
                        ECPM_BIGINT,
                        path,
                        Collections.singletonList(builder.lessThan(1, 3_000_000_000L)));

        assertThat(longs(rows, 1)).containsExactly(10L, 150L, 3000L);
    }

    // ------------------------------------------------------------------
    // Unsigned INT32 -> BIGINT. Spark and Trino both surface this as bigint,
    // so an import writes BIGINT into the metastore while the file stays INT32.
    // ------------------------------------------------------------------

    @Test
    void testUnsignedInt32ReadAsBigInt() throws Exception {
        Path path =
                write(
                        ecpmSchema("int32 ecpm (INTEGER(32,false))"),
                        (group, i) -> group.append("ecpm", UNSIGNED_RAW[i]));

        // The raw ints are negative; read as BIGINT they must come back as their unsigned value.
        assertThat(longs(read(ECPM_BIGINT, path, null), 1))
                .containsExactly(10L, 3_000_000_000L, 4_294_967_295L);
    }

    /**
     * Statistics on an unsigned column are ordered unsigned, so a signed predicate would prune the
     * wrong row groups. The predicate must be abandoned rather than pushed.
     */
    @Test
    void testUnsignedInt32PushdownDoesNotDropRows() throws Exception {
        Path path =
                write(
                        ecpmSchema("int32 ecpm (INTEGER(32,false))"),
                        (group, i) -> group.append("ecpm", UNSIGNED_RAW[i]));
        PredicateBuilder builder = new PredicateBuilder(ECPM_BIGINT);

        List<Object[]> rows =
                read(ECPM_BIGINT, path, Collections.singletonList(builder.greaterThan(1, 5L)));

        assertThat(longs(rows, 1)).containsExactly(10L, 3_000_000_000L, 4_294_967_295L);
    }

    /** A DECIMAL annotation changes the meaning of the stored INT32 and cannot be widened. */
    @Test
    void testDecimalInt32CannotReadAsBigInt() throws Exception {
        Path path =
                write(
                        ecpmSchema("int32 ecpm (DECIMAL(9,2))"),
                        (group, i) -> group.append("ecpm", 12_345));

        assertThatThrownBy(() -> read(ECPM_BIGINT, path, null))
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .rootCause()
                .hasMessageContaining("DECIMAL");
    }

    /** A DATE annotation stores epoch days, not an integer that may be widened to BIGINT. */
    @Test
    void testDateInt32CannotReadAsBigInt() throws Exception {
        Path path =
                write(ecpmSchema("int32 ecpm (DATE)"), (group, i) -> group.append("ecpm", 20_000));

        assertThatThrownBy(() -> read(ECPM_BIGINT, path, null))
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .rootCause()
                .hasMessageContaining("DATE");
    }

    /** A TIME annotation stores milliseconds since midnight, not a generic integer. */
    @Test
    void testTimeInt32CannotReadAsBigInt() throws Exception {
        MessageType schema =
                new MessageType(
                        "root",
                        Arrays.asList(
                                Types.optional(PrimitiveTypeName.BINARY)
                                        .as(LogicalTypeAnnotation.stringType())
                                        .named("pageviewId"),
                                Types.optional(PrimitiveTypeName.INT32)
                                        .as(
                                                LogicalTypeAnnotation.timeType(
                                                        true,
                                                        LogicalTypeAnnotation.TimeUnit.MILLIS))
                                        .named("ecpm"),
                                Types.optional(PrimitiveTypeName.INT64).named("revenue")));
        Path path =
                writeGroups(
                        schema,
                        (group, i) ->
                                group.append("pageviewId", PAGEVIEW_IDS[i])
                                        .append("ecpm", 12_345)
                                        .append("revenue", (long) i));

        assertThatThrownBy(() -> read(ECPM_BIGINT, path, null))
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .rootCause()
                .hasMessageContaining("TIME");
    }

    @Test
    void testIncompatibleInt64CannotReadAsBigInt() throws Exception {
        List<LogicalTypeAnnotation> incompatibleTypes =
                Arrays.asList(
                        LogicalTypeAnnotation.decimalType(2, 18),
                        LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS),
                        LogicalTypeAnnotation.timestampType(
                                false, LogicalTypeAnnotation.TimeUnit.MICROS),
                        LogicalTypeAnnotation.intType(64, false));

        for (LogicalTypeAnnotation logicalType : incompatibleTypes) {
            MessageType schema =
                    new MessageType(
                            "root",
                            Arrays.asList(
                                    Types.optional(PrimitiveTypeName.BINARY)
                                            .as(LogicalTypeAnnotation.stringType())
                                            .named("pageviewId"),
                                    Types.optional(PrimitiveTypeName.INT64)
                                            .as(logicalType)
                                            .named("ecpm"),
                                    Types.optional(PrimitiveTypeName.INT64).named("revenue")));
            Path path =
                    writeGroups(
                            schema,
                            (group, i) ->
                                    group.append("pageviewId", PAGEVIEW_IDS[i])
                                            .append("ecpm", 12_345L)
                                            .append("revenue", (long) i));

            assertThatThrownBy(() -> read(ECPM_BIGINT, path, null))
                    .as("logical type %s", logicalType)
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .rootCause()
                    .hasMessageContaining("INT64")
                    .hasMessageContaining("BIGINT");
        }
    }

    /** INTEGER(64,true) means the same as an unannotated INT64 and must stay readable. */
    @Test
    void testSignedInt64AnnotationReadAsBigInt() throws Exception {
        MessageType schema =
                new MessageType(
                        "root",
                        Arrays.asList(
                                Types.optional(PrimitiveTypeName.BINARY)
                                        .as(LogicalTypeAnnotation.stringType())
                                        .named("pageviewId"),
                                Types.optional(PrimitiveTypeName.INT64)
                                        .as(LogicalTypeAnnotation.intType(64, true))
                                        .named("ecpm"),
                                Types.optional(PrimitiveTypeName.INT64).named("revenue")));
        Path path =
                writeGroups(
                        schema,
                        (group, i) ->
                                group.append("pageviewId", PAGEVIEW_IDS[i])
                                        .append("ecpm", ECPM_VALUES[i])
                                        .append("revenue", (long) i));
        PredicateBuilder builder = new PredicateBuilder(ECPM_BIGINT);

        assertThat(longs(read(ECPM_BIGINT, path, null), 1)).containsExactly(10L, 150L, 3000L);
        assertThat(
                        longs(
                                read(
                                        ECPM_BIGINT,
                                        path,
                                        Collections.singletonList(builder.lessThan(1, 5000L))),
                                1))
                .containsExactly(10L, 150L, 3000L);
        assertThat(
                        read(
                                ECPM_BIGINT,
                                path,
                                Collections.singletonList(builder.greaterThan(1, 99999L))))
                .isEmpty();
    }

    // ------------------------------------------------------------------
    // FLOAT -> DOUBLE, the same hole in the other numeric family.
    // ------------------------------------------------------------------

    @Test
    void testFloatReadAsDouble() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("rate", DataTypes.DOUBLE())
                        .build();
        Path path =
                writeSchema(
                        "message root {\n"
                                + "  optional binary pageviewId (UTF8);\n"
                                + "  optional float rate;\n"
                                + "}",
                        (group, i) ->
                                group.append("pageviewId", PAGEVIEW_IDS[i])
                                        .append("rate", (float) (i + 1)));

        List<Object[]> rows = read(readType, path, null);

        assertThat(rows).hasSize(3);
        assertThat((Double) rows.get(0)[1]).isEqualTo(1.0d);
        assertThat((Double) rows.get(2)[1]).isEqualTo(3.0d);
    }

    @Test
    void testFloatReadAsDoubleWithPushdown() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("rate", DataTypes.DOUBLE())
                        .build();
        Path path =
                writeSchema(
                        "message root {\n"
                                + "  optional binary pageviewId (UTF8);\n"
                                + "  optional float rate;\n"
                                + "}",
                        (group, i) ->
                                group.append("pageviewId", PAGEVIEW_IDS[i])
                                        .append("rate", (float) (i + 1)));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.greaterThan(1, 0.5d)));

        assertThat(rows).hasSize(3);
    }

    /** DOUBLE values are rounded while reading, so predicates cannot be pushed before the cast. */
    @Test
    void testDoubleReadAsFloatDoesNotPushLossyPredicate() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("rate", DataTypes.FLOAT())
                        .build();
        Path path =
                writeSchema(
                        "message root {\n"
                                + "  optional binary pageviewId (UTF8);\n"
                                + "  optional double rate;\n"
                                + "}",
                        (group, i) ->
                                group.append("pageviewId", PAGEVIEW_IDS[i]).append("rate", 0.1d));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.equal(1, 0.1f)));

        assertThat(rows).extracting(row -> row[1]).containsExactly(0.1f, 0.1f, 0.1f);
    }

    // ------------------------------------------------------------------
    // Narrowing INT64 -> INT. The reader already handles it via
    // IntegerFromLongUpdater; only the pushdown was left behind.
    // ------------------------------------------------------------------

    @Test
    void testInt64ReadAsIntWithPushdown() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.INT())
                        .build();
        Path path = write(ecpmSchema("int64 ecpm"));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.lessThan(1, 5000)));

        assertThat(rows).hasSize(3);
        assertThat((Integer) rows.get(2)[1]).isEqualTo(3000);
    }

    /** The INT64 reader wraps values outside the TINYINT range, so pushdown would lose rows. */
    @Test
    void testInt64ReadAsTinyIntDoesNotPushLossyPredicate() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.TINYINT())
                        .build();
        Path path = write(ecpmSchema("int64 ecpm"), (group, i) -> group.append("ecpm", 128L));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.equal(1, (byte) -128)));

        assertThat(rows)
                .extracting(row -> row[1])
                .containsExactly((byte) -128, (byte) -128, (byte) -128);
    }

    /** The INT64 reader wraps values outside the SMALLINT range, so pushdown would lose rows. */
    @Test
    void testInt64ReadAsSmallIntDoesNotPushLossyPredicate() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.SMALLINT())
                        .build();
        Path path = write(ecpmSchema("int64 ecpm"), (group, i) -> group.append("ecpm", 65_535L));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.equal(1, (short) -1)));

        assertThat(rows)
                .extracting(row -> row[1])
                .containsExactly((short) -1, (short) -1, (short) -1);
    }

    /** A bare INT32 may also wrap when read as TINYINT, so its predicate cannot be pushed. */
    @Test
    void testInt32ReadAsTinyIntDoesNotPushLossyPredicate() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.TINYINT())
                        .build();
        Path path = write(ecpmSchema("int32 ecpm"), (group, i) -> group.append("ecpm", 128));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.equal(1, (byte) -128)));

        assertThat(rows)
                .extracting(row -> row[1])
                .containsExactly((byte) -128, (byte) -128, (byte) -128);
    }

    /** A bare INT32 may also wrap when read as SMALLINT, so its predicate cannot be pushed. */
    @Test
    void testInt32ReadAsSmallIntDoesNotPushLossyPredicate() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.SMALLINT())
                        .build();
        Path path = write(ecpmSchema("int32 ecpm"), (group, i) -> group.append("ecpm", 65_535));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.equal(1, (short) -1)));

        assertThat(rows)
                .extracting(row -> row[1])
                .containsExactly((short) -1, (short) -1, (short) -1);
    }

    /** A wider signed annotation still permits values outside the declared TINYINT range. */
    @Test
    void testInt16ReadAsTinyIntDoesNotPushLossyPredicate() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.TINYINT())
                        .build();
        Path path =
                write(
                        ecpmSchema("int32 ecpm (INTEGER(16,true))"),
                        (group, i) -> group.append("ecpm", 128));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.equal(1, (byte) -128)));

        assertThat(rows)
                .extracting(row -> row[1])
                .containsExactly((byte) -128, (byte) -128, (byte) -128);
    }

    /** Matching signed integer annotations retain predicate pushdown for valid physical values. */
    @Test
    void testAnnotatedInt8PushdownStillPrunes() throws Exception {
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.TINYINT())
                        .build();
        Path path =
                write(
                        ecpmSchema("int32 ecpm (INTEGER(8,true))"),
                        (group, i) -> group.append("ecpm", i + 1));
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(readType, path, Collections.singletonList(builder.greaterThan(1, (byte) 99)));

        assertThat(rows).isEmpty();
    }

    // ------------------------------------------------------------------
    // Control and mixed-file cases.
    // ------------------------------------------------------------------

    /** A file that really holds int64 was never broken; it must stay that way. */
    @Test
    void testInt64ReadAsBigIntIsUnchanged() throws Exception {
        Path path = write(ecpmSchema("int64 ecpm"));
        PredicateBuilder builder = new PredicateBuilder(ECPM_BIGINT);

        List<Object[]> rows =
                read(ECPM_BIGINT, path, Collections.singletonList(builder.lessThan(1, 5000L)));

        assertThat(longs(rows, 1)).containsExactly(10L, 150L, 3000L);
    }

    /**
     * One reader factory serves every file of a split, so a partition that mixes writers puts both
     * physical types through the same factory. Neither file may break the other.
     */
    @Test
    void testMixedFilesInOneFactory() throws Exception {
        Path int32File = write(ecpmSchema("int32 ecpm"));
        Path int64File = write(ecpmSchema("int64 ecpm"));

        ParquetReaderFactory factory =
                new ParquetReaderFactory(
                        new Options(),
                        ECPM_BIGINT,
                        1024,
                        Collections.singletonList(
                                new PredicateBuilder(ECPM_BIGINT).lessThan(1, 5000L)));

        assertThat(longs(read(factory, ECPM_BIGINT, int64File), 1))
                .containsExactly(10L, 150L, 3000L);
        assertThat(longs(read(factory, ECPM_BIGINT, int32File), 1))
                .containsExactly(10L, 150L, 3000L);
        assertThat(longs(read(factory, ECPM_BIGINT, int64File), 1))
                .containsExactly(10L, 150L, 3000L);
    }

    // ------------------------------------------------------------------
    // Case-insensitive resolution. The metastore lowercases column names while a
    // Spark-written file keeps the original spelling, so a predicate carrying the
    // metastore's spelling names a column the file does not have - which parquet-mr
    // takes for all-null and prunes away, losing every row without a word.
    // ------------------------------------------------------------------

    @Test
    void testCaseInsensitivePushdownKeepsRows() throws Exception {
        Path path =
                writeSchema(
                        "message root {\n"
                                + "  optional binary PageviewId (UTF8);\n"
                                + "  optional int32 Ecpm;\n"
                                + "}",
                        (group, i) ->
                                group.append("PageviewId", "id" + i).append("Ecpm", (i + 1) * 100));
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.BIGINT())
                        .build();
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(
                        readType,
                        path,
                        Collections.singletonList(builder.lessThan(1, 100_000L)),
                        false);

        assertThat(longs(rows, 1)).containsExactly(100L, 200L, 300L);
    }

    /** Same, with no type widening in play at all, so only the spelling is at stake. */
    @Test
    void testCaseInsensitivePushdownKeepsRowsWithoutWidening() throws Exception {
        Path path =
                writeSchema(
                        "message root {\n"
                                + "  optional binary PageviewId (UTF8);\n"
                                + "  optional int64 Ecpm;\n"
                                + "}",
                        (group, i) ->
                                group.append("PageviewId", "id" + i)
                                        .append("Ecpm", (long) ((i + 1) * 100)));
        RowType readType =
                RowType.builder()
                        .field("pageviewId", DataTypes.STRING())
                        .field("ecpm", DataTypes.BIGINT())
                        .build();
        PredicateBuilder builder = new PredicateBuilder(readType);

        List<Object[]> rows =
                read(
                        readType,
                        path,
                        Collections.singletonList(builder.lessThan(1, 100_000L)),
                        false);

        assertThat(longs(rows, 1)).containsExactly(100L, 200L, 300L);
    }

    @Test
    void testBinaryDecimalReadsEveryRowOfAPage() throws Exception {
        // Paimon's own writer never emits DECIMAL on BINARY, so this shape only comes from
        // external writers, and dictionary encoding has to be off: a dictionary page is
        // decoded by decodeSingleDictionaryId, which does not go through the updater's
        // scratch vector.
        for (int precision : new int[] {5, 20}) {
            MessageType schema =
                    new MessageType(
                            "root",
                            Collections.singletonList(
                                    Types.optional(PrimitiveTypeName.BINARY)
                                            .as(LogicalTypeAnnotation.decimalType(2, precision))
                                            .named("price")));
            Path path =
                    writeGroups(
                            schema,
                            (group, i) ->
                                    group.append(
                                            "price",
                                            Binary.fromConstantByteArray(
                                                    BigInteger.valueOf((i + 1) * 100L)
                                                            .toByteArray())),
                            false);

            RowType readType =
                    RowType.builder().field("price", DataTypes.DECIMAL(precision, 2)).build();
            List<Object[]> rows = read(readType, path, null);

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0)[0]).isEqualTo(new BigDecimal("1.00"));
            assertThat(rows.get(1)[0]).isEqualTo(new BigDecimal("2.00"));
            assertThat(rows.get(2)[0]).isEqualTo(new BigDecimal("3.00"));
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Raw int32 bit patterns for 10, 3000000000 and 4294967295 read as unsigned. */
    private static final int[] UNSIGNED_RAW = {10, (int) 3_000_000_000L, -1};

    private static List<Long> longs(List<Object[]> rows, int field) {
        List<Long> values = new ArrayList<>();
        rows.forEach(row -> values.add((Long) row[field]));
        return values;
    }

    private static String ecpmSchema(String ecpmType) {
        return "message root {\n"
                + "  optional binary pageviewId (UTF8);\n"
                + "  optional "
                + ecpmType
                + ";\n"
                + "  optional int64 revenue;\n"
                + "}";
    }

    private List<Object[]> read(RowType readType, Path path, List<Predicate> filters)
            throws Exception {
        return read(readType, path, filters, true);
    }

    private List<Object[]> read(
            RowType readType, Path path, List<Predicate> filters, boolean caseSensitive)
            throws Exception {
        Options options = new Options();
        options.set(CatalogOptions.CASE_SENSITIVE, caseSensitive);
        return read(new ParquetReaderFactory(options, readType, 1024, filters), readType, path);
    }

    private List<Object[]> read(ParquetReaderFactory factory, RowType readType, Path path)
            throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        List<Object[]> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                factory.createReader(
                        new FormatReaderContext(
                                fileIO, path, fileIO.getFileSize(path), null, null))) {
            // Row instances are reused across iterations, so materialize every value.
            reader.forEachRemaining(row -> rows.add(materialize(row, readType)));
        }
        return rows;
    }

    private static Object[] materialize(InternalRow row, RowType readType) {
        Object[] values = new Object[readType.getFieldCount()];
        for (int i = 0; i < values.length; i++) {
            if (row.isNullAt(i)) {
                continue;
            }
            switch (readType.getTypeAt(i).getTypeRoot()) {
                case VARCHAR:
                    values[i] = row.getString(i).toString();
                    break;
                case TINYINT:
                    values[i] = row.getByte(i);
                    break;
                case SMALLINT:
                    values[i] = row.getShort(i);
                    break;
                case INTEGER:
                    values[i] = row.getInt(i);
                    break;
                case BIGINT:
                    values[i] = row.getLong(i);
                    break;
                case FLOAT:
                    values[i] = row.getFloat(i);
                    break;
                case DOUBLE:
                    values[i] = row.getDouble(i);
                    break;
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) readType.getTypeAt(i);
                    values[i] =
                            row.getDecimal(i, decimalType.getPrecision(), decimalType.getScale())
                                    .toBigDecimal();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unhandled type in test: " + readType.getTypeAt(i));
            }
        }
        return values;
    }

    private Path write(String schemaText) throws Exception {
        return write(schemaText, null);
    }

    private Path write(String schemaText, BiConsumer<Group, Integer> ecpmAppender)
            throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        boolean ecpmIsLong =
                schema.getType("ecpm")
                        .asPrimitiveType()
                        .getPrimitiveTypeName()
                        .name()
                        .equals("INT64");
        return writeGroups(
                schema,
                (group, i) -> {
                    group.append("pageviewId", PAGEVIEW_IDS[i]);
                    if (ecpmAppender != null) {
                        ecpmAppender.accept(group, i);
                    } else if (ecpmIsLong) {
                        group.append("ecpm", ECPM_VALUES[i]);
                    } else {
                        group.append("ecpm", (int) ECPM_VALUES[i]);
                    }
                    group.append("revenue", (long) (i + 1));
                });
    }

    private Path writeSchema(String schemaText, BiConsumer<Group, Integer> appender)
            throws Exception {
        return writeGroups(MessageTypeParser.parseMessageType(schemaText), appender);
    }

    private Path writeGroups(MessageType schema, BiConsumer<Group, Integer> appender)
            throws Exception {
        return writeGroups(schema, appender, true);
    }

    private Path writeGroups(
            MessageType schema, BiConsumer<Group, Integer> appender, boolean dictionaryEncoding)
            throws Exception {
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        Configuration conf = new Configuration();
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(path.toString()), conf))
                        .withType(schema)
                        .withConf(conf)
                        .withDictionaryEncoding(dictionaryEncoding)
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .build()) {
            for (int i = 0; i < 3; i++) {
                Group group = new SimpleGroupFactory(schema).newGroup();
                appender.accept(group, i);
                writer.write(group);
            }
        }
        return path;
    }
}
