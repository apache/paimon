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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for what {@link FormatTablePartitionStatsCollector} measures, which is what {@code ANALYZE
 * TABLE} and a measuring {@code MSCK REPAIR} write into the catalog.
 *
 * <p>The staging cases are the same ones the read path has to survive: a committer leaves trees
 * such as {@code _temporary/}, {@code __magic_job-<id>/} and {@code .hive-staging_*} inside the
 * partition, and the files under them carry ordinary data file names. A measurement that counts
 * them reports a partition that holds more than any reader will ever return.
 */
class FormatTablePartitionStatsCollectorTest {

    private static final Identifier TABLE =
            Identifier.create("statistics_db", "statistics_format_table");
    private static final String PARTITION_DIR = "year=2025/month=10";

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testCountsOnlyCommittedDataFiles() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        write(fileIO, tablePath, PARTITION_DIR + "/data-0.csv", 100);

        PartitionStatistics measured = measure(fileIO, tablePath);

        assertThat(measured.fileCount()).isEqualTo(1);
        assertThat(measured.fileSizeInBytes()).isEqualTo(100);
        assertThat(measured.lastFileCreationTime()).isPositive();
    }

    @Test
    void testStagingTreesAreNotMeasured() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        write(fileIO, tablePath, PARTITION_DIR + "/data-0.csv", 100);
        // Every one of these carries an ordinary data file name; only the directory above says
        // the file was never committed.
        write(
                fileIO,
                tablePath,
                PARTITION_DIR + "/_temporary/0/_temporary/attempt_0/part-0.csv",
                7);
        write(
                fileIO,
                tablePath,
                PARTITION_DIR + "/__magic_job-1/tasks/attempt_1/__base/part-1.csv",
                11);
        write(fileIO, tablePath, PARTITION_DIR + "/.hive-staging_1/-ext-10000/part-2.csv", 13);

        PartitionStatistics measured = measure(fileIO, tablePath);

        assertThat(measured.fileCount()).isEqualTo(1);
        assertThat(measured.fileSizeInBytes()).isEqualTo(100);
    }

    @Test
    void testHiddenFilesBesideTheDataAreNotMeasured() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        write(fileIO, tablePath, PARTITION_DIR + "/data-0.csv", 100);
        write(fileIO, tablePath, PARTITION_DIR + "/_SUCCESS", 0);
        write(fileIO, tablePath, PARTITION_DIR + "/.data-0.csv.crc", 8);

        PartitionStatistics measured = measure(fileIO, tablePath);

        assertThat(measured.fileCount()).isEqualTo(1);
        assertThat(measured.fileSizeInBytes()).isEqualTo(100);
    }

    @Test
    void testAMissingDirectoryHasNoFilesAndNoCreationTime() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());

        PartitionStatistics measured = measure(fileIO, tablePath);

        assertThat(measured.fileSizeInBytes()).isZero();
        assertThat(measured.fileCount()).isZero();
        // A listing never opens a file, so it has not learned that this partition holds no rows.
        assertThat(PartitionStatistics.isKnown(measured.recordCount())).isFalse();
        // There is no last file, so dating one would be an invention.
        assertThat(PartitionStatistics.isKnown(measured.lastFileCreationTime())).isFalse();
    }

    @Test
    void testADirectoryHoldingOnlyStagedFilesMeasuresAsEmpty() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        write(
                fileIO,
                tablePath,
                PARTITION_DIR + "/_temporary/0/_temporary/attempt_0/part-0.csv",
                7);

        PartitionStatistics measured = measure(fileIO, tablePath);

        assertThat(measured.fileCount()).isZero();
        assertThat(measured.fileSizeInBytes()).isZero();
        assertThat(PartitionStatistics.isKnown(measured.recordCount())).isFalse();
    }

    @Test
    void testTheValueOnlyLayoutIsMeasuredWhereItsFilesActuallyAre() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        // The same partition, laid out with values only. A measurement that assumed key=value
        // would look at a directory that does not exist and call the partition empty.
        write(fileIO, tablePath, "2025/10/data-0.csv", 64);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "csv");
        options.put(CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(), "true");
        PartitionStatistics measured =
                new FormatTablePartitionStatsCollector(table(fileIO, tablePath, options), 1)
                        .collect(Collections.singletonList(spec("2025", "10")))
                        .get(0);

        assertThat(measured.fileCount()).isEqualTo(1);
        assertThat(measured.fileSizeInBytes()).isEqualTo(64);
    }

    @Test
    void testAValueThatHasToBeEscapedIsMeasuredWhereItsFilesActuallyAre() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        // The spec carries the raw value, the directory carries the escaped one: a measurement
        // that joined the raw value straight into the path would miss the files entirely.
        write(fileIO, tablePath, "year=2025/month=a%3Ab/data-0.csv", 32);

        PartitionStatistics measured =
                new FormatTablePartitionStatsCollector(table(fileIO, tablePath), 1)
                        .collect(Collections.singletonList(spec("2025", "a:b")))
                        .get(0);

        assertThat(measured.fileCount()).isEqualTo(1);
        assertThat(measured.fileSizeInBytes()).isEqualTo(32);
    }

    @Test
    void testTheResultIsAlignedToTheGivenPartitions() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        write(fileIO, tablePath, "year=2025/month=10/data-0.csv", 100);
        write(fileIO, tablePath, "year=2025/month=12/data-0.csv", 200);
        List<Map<String, String>> partitions =
                Arrays.asList(spec("2025", "12"), spec("2025", "11"), spec("2025", "10"));

        List<PartitionStatistics> measured =
                new FormatTablePartitionStatsCollector(table(fileIO, tablePath), 1)
                        .collect(partitions);

        assertThat(measured).hasSize(3);
        assertThat(measured.get(0).spec()).isEqualTo(spec("2025", "12"));
        assertThat(measured.get(0).fileSizeInBytes()).isEqualTo(200);
        assertThat(measured.get(1).spec()).isEqualTo(spec("2025", "11"));
        assertThat(measured.get(1).fileCount()).isZero();
        assertThat(measured.get(2).spec()).isEqualTo(spec("2025", "10"));
        assertThat(measured.get(2).fileSizeInBytes()).isEqualTo(100);
    }

    @Test
    void testParallelCollectionMeasuresTheSameThingAsSerialCollection() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        List<Map<String, String>> partitions = new ArrayList<>();
        for (int month = 1; month <= 6; month++) {
            String dir = String.format("year=2025/month=%02d", month);
            write(fileIO, tablePath, dir + "/data-0.csv", month * 10);
            write(fileIO, tablePath, dir + "/_temporary/0/attempt_0/part-0.csv", 5);
            partitions.add(spec("2025", String.format("%02d", month)));
        }

        List<PartitionStatistics> serial =
                new FormatTablePartitionStatsCollector(table(fileIO, tablePath), 1)
                        .collect(partitions);
        List<PartitionStatistics> parallel =
                new FormatTablePartitionStatsCollector(table(fileIO, tablePath), 4)
                        .collect(partitions);

        for (int i = 0; i < partitions.size(); i++) {
            assertThat(parallel.get(i).spec()).isEqualTo(serial.get(i).spec());
            assertThat(parallel.get(i).fileCount()).isEqualTo(serial.get(i).fileCount());
            assertThat(parallel.get(i).fileSizeInBytes())
                    .isEqualTo(serial.get(i).fileSizeInBytes());
        }
        assertThat(serial.get(0).fileSizeInBytes()).isEqualTo(10);
        assertThat(serial.get(5).fileSizeInBytes()).isEqualTo(60);
    }

    @Test
    void testAListingFailureAbortsTheWholeCollection() throws Exception {
        IOException listFailure = new IOException("injected partition LIST failure");
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public FileStatus[] listStatus(Path path) throws IOException {
                        if ("month=11".equals(path.getName())) {
                            throw listFailure;
                        }
                        return super.listStatus(path);
                    }
                };
        Path tablePath = new Path(tempDir.toUri());
        write(fileIO, tablePath, "year=2025/month=10/data-0.csv", 100);
        write(fileIO, tablePath, "year=2025/month=11/data-0.csv", 200);
        List<Map<String, String>> partitions =
                Arrays.asList(spec("2025", "10"), spec("2025", "11"));

        // A truncated listing cannot be told apart from a partition that lost files, so nothing at
        // all is reported: returning what was measured would write an exact zero over a partition
        // that was never read. Both the serial and the parallel path have to abort.
        for (int parallelism : new int[] {1, 2}) {
            assertThatThrownBy(
                            () ->
                                    new FormatTablePartitionStatsCollector(
                                                    table(fileIO, tablePath), parallelism)
                                            .collect(partitions))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasMessageContaining("month=11")
                    .hasCause(listFailure);
        }
    }

    @Test
    void testASpecMissingAPartitionKeyIsRejected() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());

        // Without the guard the directory name would carry the literal "null" and the measurement
        // would describe a path no reader ever visits.
        assertThatThrownBy(
                        () ->
                                new FormatTablePartitionStatsCollector(table(fileIO, tablePath), 1)
                                        .collect(
                                                Collections.singletonList(
                                                        Collections.singletonMap("year", "2025"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("month");
    }

    private PartitionStatistics measure(FileIO fileIO, Path tablePath) {
        return new FormatTablePartitionStatsCollector(table(fileIO, tablePath), 1)
                .collect(Collections.singletonList(spec("2025", "10")))
                .get(0);
    }

    private FormatTable table(FileIO fileIO, Path tablePath) {
        return table(fileIO, tablePath, FormatTable.Format.CSV, "csv");
    }

    private FormatTable table(FileIO fileIO, Path tablePath, Map<String, String> options) {
        return table(fileIO, tablePath, FormatTable.Format.CSV, options);
    }

    private FormatTable table(
            FileIO fileIO, Path tablePath, FormatTable.Format format, String fileFormat) {
        return table(
                fileIO,
                tablePath,
                format,
                Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), fileFormat));
    }

    private FormatTable table(
            FileIO fileIO, Path tablePath, FormatTable.Format format, Map<String, String> options) {
        RowType rowType =
                RowType.builder()
                        .field("year", DataTypes.STRING())
                        .field("month", DataTypes.STRING())
                        .field("id", DataTypes.INT())
                        .build();
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(TABLE)
                .rowType(rowType)
                .partitionKeys(Arrays.asList("year", "month"))
                .location(tablePath.toString())
                .format(format)
                .options(options)
                .build();
    }

    private static void write(FileIO fileIO, Path tablePath, String relativePath, int bytes)
            throws Exception {
        Path path = new Path(tablePath, relativePath);
        fileIO.mkdirs(path.getParent());
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(new byte[bytes]);
        }
    }

    private static Map<String, String> spec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }
}
