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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import dev.vortex.api.VortexWriteSummary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for statistics and size estimates of {@link VortexRecordsWriter}. */
class VortexWriterStatsTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("f_int", DataTypes.INT())
                    .field("f_tiny", DataTypes.TINYINT())
                    .field("f_string", DataTypes.STRING())
                    .field("f_double", DataTypes.DOUBLE())
                    .field("f_decimal", DataTypes.DECIMAL(10, 2))
                    .field("f_ts", DataTypes.TIMESTAMP(6))
                    .field("f_date", DataTypes.DATE())
                    .field("f_array", DataTypes.ARRAY(DataTypes.INT()))
                    .field("f_row", DataTypes.ROW(DataTypes.FIELD(0, "x", DataTypes.INT())))
                    .build();

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testStatsFromWriteSummaryMatchStatsFromScan() throws Exception {
        Path file = new Path(new Path(tempDir.toUri()), "stats-" + System.nanoTime());
        VortexFileFormat format = format(1024);
        FileIO fileIO = LocalFileIO.create();

        Object writerMetadata;
        FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(ROW_TYPE))
                        .create(fileIO, file, "");
        try {
            for (int i = 0; i < 5_000; i++) {
                writer.addElement(row(i));
            }
        } finally {
            writer.close();
        }
        writerMetadata = writer.writerMetadata();

        assertThat(writerMetadata).isInstanceOf(VortexWriteSummary.class);
        VortexWriteSummary summary = (VortexWriteSummary) writerMetadata;
        assertThat(summary.rowCount()).isEqualTo(5_000);
        assertThat(summary.fileSize()).isEqualTo(fileIO.getFileSize(file));

        SimpleColStatsCollector.Factory[] full = new SimpleColStatsCollector.Factory[9];
        Arrays.fill(full, SimpleColStatsCollector.from("full"));
        SimpleStatsExtractor extractor = format.createStatsExtractor(ROW_TYPE, full).get();

        SimpleColStats[] fromSummary =
                extractor.extract(fileIO, file, fileIO.getFileSize(file), writerMetadata);
        SimpleColStats[] fromScan = extractor.extract(fileIO, file, fileIO.getFileSize(file));

        assertThat(fromSummary[0]).isEqualTo(new SimpleColStats(1, 4_999, 1L));
        assertThat(fromSummary[1]).isEqualTo(new SimpleColStats((byte) -128, (byte) 127, 0L));
        assertThat(fromSummary[3]).isEqualTo(new SimpleColStats(0.0, 4_999.5, 0L));
        assertThat(fromSummary[4])
                .isEqualTo(
                        new SimpleColStats(
                                Decimal.fromBigDecimal(new BigDecimal("0.00"), 10, 2),
                                Decimal.fromBigDecimal(new BigDecimal("49.99"), 10, 2),
                                0L));
        assertThat(fromSummary[5])
                .isEqualTo(
                        new SimpleColStats(
                                Timestamp.fromMicros(0), Timestamp.fromMicros(4_999_001L), 0L));
        assertThat(fromSummary[6]).isEqualTo(new SimpleColStats(0, 4_999, 0L));

        // Exact types agree with the stats that a scan collects.
        for (int i : new int[] {0, 1, 3, 4, 5, 6}) {
            assertThat(fromSummary[i]).as("column %s", i).isEqualTo(fromScan[i]);
        }

        // String bounds can be truncated, but they must still bound every value.
        SimpleColStats strings = fromSummary[2];
        assertThat(strings.nullCount()).isEqualTo(0L);
        assertThat(((BinaryString) strings.min()).compareTo((BinaryString) fromScan[2].min()))
                .isLessThanOrEqualTo(0);
        assertThat(((BinaryString) strings.max()).compareTo((BinaryString) fromScan[2].max()))
                .isGreaterThanOrEqualTo(0);

        // Nested columns have no bounds.
        assertThat(fromSummary[7].min()).isNull();
        assertThat(fromSummary[8].min()).isNull();
    }

    @Test
    void testNaNDropsFloatingPointBounds() throws Exception {
        RowType rowType = RowType.builder().field("d", DataTypes.DOUBLE()).build();
        Path file = new Path(new Path(tempDir.toUri()), "nan-" + System.nanoTime());
        FormatWriter writer =
                ((SupportsDirectWrite) format(1024).createWriterFactory(rowType))
                        .create(LocalFileIO.create(), file, "");
        writer.addElement(GenericRow.of(1.0));
        writer.addElement(GenericRow.of(Double.NaN));
        writer.close();

        SimpleColStatsCollector.Factory[] full =
                new SimpleColStatsCollector.Factory[] {SimpleColStatsCollector.from("full")};
        SimpleColStats stats =
                new VortexSimpleStatsExtractor(rowType, full)
                        .extract(LocalFileIO.create(), file, 0, writer.writerMetadata())[0];
        assertThat(stats.min()).isNull();
        assertThat(stats.max()).isNull();
        assertThat(stats.nullCount()).isEqualTo(0L);
    }

    @Test
    void testEstimatedSizeTracksFileSize() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("s", DataTypes.STRING())
                        .field("i", DataTypes.BIGINT())
                        .build();
        Path file = new Path(new Path(tempDir.toUri()), "size-" + System.nanoTime());
        FileIO fileIO = LocalFileIO.create();
        VortexRecordsWriter writer =
                (VortexRecordsWriter)
                        ((SupportsDirectWrite) format(4096).createWriterFactory(rowType))
                                .create(fileIO, file, "");

        java.util.Random random = new java.util.Random(42);
        for (int i = 0; i < 3_000_000; i++) {
            writer.addElement(
                    GenericRow.of(
                            BinaryString.fromString(Long.toHexString(random.nextLong())),
                            (long) i));
        }

        long estimate = writer.estimatedFileSize();
        assertThat(writer.reachTargetSize(false, 1)).isFalse();
        assertThat(writer.reachTargetSize(true, 1)).isTrue();
        writer.close();

        long actual = fileIO.getFileSize(file);
        assertThat(writer.estimatedFileSize()).isEqualTo(actual);
        // This data compresses to about 40% of its Arrow size, so a fixed ratio of 25% would be
        // far off. The estimate follows the ratio that the native writer observes.
        assertThat((double) estimate).isBetween(actual * 0.8, actual * 1.2);
    }

    private static VortexFileFormat format(int batchSize) {
        return new VortexFileFormatFactory()
                .create(new FileFormatFactory.FormatContext(new Options(), 1024, batchSize));
    }

    private static GenericRow row(int i) {
        return GenericRow.of(
                i == 0 ? null : i,
                (byte) (i % 256 - 128),
                BinaryString.fromString("value-" + i),
                i + 0.5 * (i % 2),
                Decimal.fromBigDecimal(new BigDecimal(i % 5_000).movePointLeft(2), 10, 2),
                Timestamp.fromMicros(i * 1_000L + (i == 4_999 ? 1 : 0)),
                i,
                new GenericArray(new int[] {i}),
                GenericRow.of(i));
    }
}
