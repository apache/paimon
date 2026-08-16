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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleColStatsExtractorTest;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for {@link MosaicSimpleStatsExtractor}. */
class MosaicSimpleStatsExtractorTest extends SimpleColStatsExtractorTest {

    @TempDir java.nio.file.Path statsTestTempDir;

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @Override
    protected FileFormat createFormat() {
        Options options = new Options();
        options.set(
                MosaicFileFormat.STATS_COLUMNS,
                "f_boolean,f_tinyint,f_smallint,f_int,f_bigint,f_float,"
                        + "f_double,f_string,f_decimal_5_2,f_date,f_timestamp3,f_timestamp6");
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
    }

    @Override
    protected RowType rowType() {
        return RowType.builder()
                .field("f_boolean", DataTypes.BOOLEAN())
                .field("f_tinyint", DataTypes.TINYINT())
                .field("f_smallint", DataTypes.SMALLINT())
                .field("f_int", DataTypes.INT())
                .field("f_bigint", DataTypes.BIGINT())
                .field("f_float", DataTypes.FLOAT())
                .field("f_double", DataTypes.DOUBLE())
                .field("f_string", DataTypes.VARCHAR(100))
                .field("f_decimal_5_2", DataTypes.DECIMAL(5, 2))
                .field("f_date", DataTypes.DATE())
                .field("f_timestamp3", DataTypes.TIMESTAMP(3))
                .field("f_timestamp6", DataTypes.TIMESTAMP(6))
                .build();
    }

    @Override
    protected String fileCompression() {
        return "zstd";
    }

    @Test
    void testUntrackedColumnsReturnNone() throws IOException {
        // stats_columns only tracks f_int, but the table has f_int + f_string
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        Options options = new Options();
        options.set(MosaicFileFormat.STATS_COLUMNS, "f_int");
        MosaicFileFormat format =
                new MosaicFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        Path path = new Path(statsTestTempDir.toUri().toString(), UUID.randomUUID() + ".mosaic");
        LocalFileIO fileIO = new LocalFileIO();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
        writer.addElement(GenericRow.of(1, BinaryString.fromString("a")));
        writer.addElement(GenericRow.of(2, BinaryString.fromString("b")));
        writer.close();

        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);
        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        SimpleColStats[] stats = extractor.extract(fileIO, path, fileIO.getFileSize(path));

        // f_int is tracked, should have real stats
        assertThat(stats[0].min()).isEqualTo(1);
        assertThat(stats[0].max()).isEqualTo(2);
        assertThat(stats[0].nullCount()).isEqualTo(0L);
        // f_string is NOT tracked, should be NONE (null nullCount)
        assertThat(stats[1].min()).isNull();
        assertThat(stats[1].max()).isNull();
        assertThat(stats[1].nullCount()).isNull();
    }

    @Test
    void testBinaryColumnStatsNoException() throws Exception {
        // Binary columns produce byte[] from convertStatsValue, which is not Comparable.
        // Verify multi-row-group aggregation doesn't throw ClassCastException.
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_binary", DataTypes.VARBINARY(100))
                        .build();
        // Build a fake MosaicWriterMetadata with binary stats across 2 row groups
        java.lang.reflect.Constructor<?> ctor =
                org.apache.paimon.mosaic.ColumnStatistics.class.getDeclaredConstructor(
                        long.class, byte[].class, byte[].class);
        ctor.setAccessible(true);

        java.util.Map<String, org.apache.paimon.mosaic.ColumnStatistics> rg0 =
                new java.util.HashMap<>();
        rg0.put(
                "f_int",
                (org.apache.paimon.mosaic.ColumnStatistics)
                        ctor.newInstance(0L, intBytes(0), intBytes(100)));
        rg0.put(
                "f_binary",
                (org.apache.paimon.mosaic.ColumnStatistics)
                        ctor.newInstance(0L, new byte[] {1, 2}, new byte[] {3, 4}));

        java.util.Map<String, org.apache.paimon.mosaic.ColumnStatistics> rg1 =
                new java.util.HashMap<>();
        rg1.put(
                "f_int",
                (org.apache.paimon.mosaic.ColumnStatistics)
                        ctor.newInstance(0L, intBytes(50), intBytes(200)));
        rg1.put(
                "f_binary",
                (org.apache.paimon.mosaic.ColumnStatistics)
                        ctor.newInstance(0L, new byte[] {5, 6}, new byte[] {7, 8}));

        java.util.List<java.util.Map<String, org.apache.paimon.mosaic.ColumnStatistics>> allStats =
                java.util.Arrays.asList(rg0, rg1);
        MosaicWriterMetadata metadata =
                new MosaicWriterMetadata(2, allStats, java.util.Arrays.asList("f_int", "f_binary"));

        // Write a minimal file (only f_int in stats_columns since native rejects binary)
        Options options = new Options();
        options.set(MosaicFileFormat.STATS_COLUMNS, "f_int");
        MosaicFileFormat format =
                new MosaicFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
        Path path = new Path(statsTestTempDir.toUri().toString(), UUID.randomUUID() + ".mosaic");
        LocalFileIO fileIO = new LocalFileIO();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
        writer.addElement(GenericRow.of(1, new byte[] {1}));
        writer.close();

        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);
        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        // Should not throw ClassCastException
        SimpleColStats[] stats =
                extractor.extract(fileIO, path, fileIO.getFileSize(path), metadata);

        // f_int aggregated across row groups: min=0, max=200
        assertThat(stats[0].min()).isEqualTo(0);
        assertThat(stats[0].max()).isEqualTo(200);
        // f_binary min/max should be null (byte[] not Comparable, skipped)
        assertThat(stats[1].min()).isNull();
        assertThat(stats[1].max()).isNull();
    }

    private static byte[] intBytes(int value) {
        return java.nio.ByteBuffer.allocate(4).putInt(value).array();
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
