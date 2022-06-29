/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.file.data;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.table.store.file.writer.MetricFileWriter;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the correctness for {@link AppendOnlyWriter}. */
public class AppendOnlyWriterTest {

    private static final RowData EMPTY_ROW = BinaryRowDataUtil.EMPTY_ROW;
    private static final RowType SCHEMA =
            RowType.of(
                    new LogicalType[] {new IntType(), new VarCharType(), new VarCharType()},
                    new String[] {"id", "name", "dt"});
    private static final FieldStatsArraySerializer STATS_SERIALIZER =
            new FieldStatsArraySerializer(SCHEMA);

    @TempDir public java.nio.file.Path tempDir;
    public DataFilePathFactory pathFactory;

    private static final String AVRO = "avro";
    private static final String PART = "2022-05-01";
    private static final long SCHEMA_ID = 0L;

    @BeforeEach
    public void before() {
        pathFactory = createPathFactory();
    }

    @Test
    public void testEmptyCommits() throws Exception {
        RecordWriter<RowData> writer = createEmptyWriter(1024 * 1024L, SCHEMA, 0);

        for (int i = 0; i < 3; i++) {
            writer.sync();
            Increment inc = writer.prepareCommit();

            assertThat(inc.newFiles()).isEqualTo(Collections.emptyList());
            assertThat(inc.compactBefore()).isEqualTo(Collections.emptyList());
            assertThat(inc.compactAfter()).isEqualTo(Collections.emptyList());
        }
    }

    @Test
    public void testSingleWrite() throws Exception {
        RecordWriter<RowData> writer = createEmptyWriter(1024 * 1024L, SCHEMA, 0);
        writer.write(row(1, "AAA", PART));

        List<DataFileMeta> result = writer.close();

        assertThat(result.size()).isEqualTo(1);
        DataFileMeta meta = result.get(0);
        assertThat(meta).isNotNull();

        Path path = pathFactory.toPath(meta.fileName());
        assertThat(path.getFileSystem().exists(path)).isFalse();

        assertThat(meta.rowCount()).isEqualTo(1L);
        assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

        FieldStats[] expected =
                new FieldStats[] {
                    initStats(1, 1, 0), initStats("AAA", "AAA", 0), initStats(PART, PART, 0)
                };
        assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

        assertThat(meta.minSequenceNumber()).isEqualTo(1);
        assertThat(meta.maxSequenceNumber()).isEqualTo(1);
        assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
    }

    @Test
    public void testMultipleCommits() throws Exception {
        RecordWriter<RowData> writer = createEmptyWriter(1024 * 1024L, SCHEMA, 0);

        // Commit 5 continues txn.
        for (int txn = 0; txn < 5; txn += 1) {

            // Write the records with range [ txn*100, (txn+1)*100 ).
            int start = txn * 100;
            int end = txn * 100 + 100;
            for (int i = start; i < end; i++) {
                writer.write(row(i, String.format("%03d", i), PART));
            }

            writer.sync();
            Increment inc = writer.prepareCommit();
            if (txn > 0 && txn % 2 == 0) {
                assertThat(inc.compactBefore()).hasSize(2);
                assertThat(inc.compactAfter()).hasSize(1);
                DataFileMeta compactAfter = inc.compactAfter().get(0);
                assertThat(compactAfter.fileName()).startsWith("compact-");
                assertThat(compactAfter.fileSize())
                        .isEqualTo(
                                inc.compactBefore().stream()
                                        .mapToLong(DataFileMeta::fileSize)
                                        .sum());
                assertThat(compactAfter.rowCount())
                        .isEqualTo(
                                inc.compactBefore().stream()
                                        .mapToLong(DataFileMeta::rowCount)
                                        .sum());
            } else {
                assertThat(inc.compactBefore()).isEqualTo(Collections.emptyList());
                assertThat(inc.compactAfter()).isEqualTo(Collections.emptyList());
            }

            assertThat(inc.newFiles().size()).isEqualTo(1);
            DataFileMeta meta = inc.newFiles().get(0);

            Path path = pathFactory.toPath(meta.fileName());
            assertThat(path.getFileSystem().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(100L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

            FieldStats[] expected =
                    new FieldStats[] {
                        initStats(start, end - 1, 0),
                        initStats(String.format("%03d", start), String.format("%03d", end - 1), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(start + 1);
            assertThat(meta.maxSequenceNumber()).isEqualTo(end);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
        }
    }

    @Test
    public void testRollingWrite() throws Exception {
        // Set a very small target file size, so that we will roll over to a new file even if
        // writing one record.
        RecordWriter<RowData> writer = createEmptyWriter(10L, SCHEMA, 0);

        for (int i = 0; i < 10; i++) {
            writer.write(row(i, String.format("%03d", i), PART));
        }

        writer.sync();
        Increment inc = writer.prepareCommit();
        assertThat(inc.compactBefore()).isEqualTo(Collections.emptyList());
        assertThat(inc.compactAfter()).isEqualTo(Collections.emptyList());

        assertThat(inc.newFiles().size()).isEqualTo(10);

        int id = 0;
        for (DataFileMeta meta : inc.newFiles()) {
            Path path = pathFactory.toPath(meta.fileName());
            assertThat(path.getFileSystem().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(1L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

            FieldStats[] expected =
                    new FieldStats[] {
                        initStats(id, id, 0),
                        initStats(String.format("%03d", id), String.format("%03d", id), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(id + 1);
            assertThat(meta.maxSequenceNumber()).isEqualTo(id + 1);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);

            id += 1;
        }

        // increase target file size to test compaction
        long targetFileSize = 1000L;
        writer = createWriter(targetFileSize, SCHEMA, 0, true, inc.newFiles());
        writer.write(row(id, String.format("%03d", id), PART));
        writer.sync();
        Increment increment = writer.prepareCommit();
        assertThat(increment.compactBefore()).containsExactlyInAnyOrderElementsOf(inc.newFiles());
        long compactFileSize =
                increment.compactBefore().stream().mapToLong(DataFileMeta::fileSize).sum();
        assertThat(increment.compactAfter().size())
                .isGreaterThanOrEqualTo((int) (compactFileSize / targetFileSize));
        assertThat(increment.newFiles()).hasSize(1);

        DataFileMeta newFile = increment.newFiles().get(0);

        for (int i = 0; i < 2; i++) {
            writer.write(row(i, String.format("%03d", i), PART));
            writer.sync();
            increment = writer.prepareCommit();
        }
        assertThat(increment.compactBefore()).hasSize(2).contains(newFile);
    }

    private FieldStats initStats(Integer min, Integer max, long nullCount) {
        return new FieldStats(min, max, nullCount);
    }

    private FieldStats initStats(String min, String max, long nullCount) {
        return new FieldStats(StringData.fromString(min), StringData.fromString(max), nullCount);
    }

    private RowData row(int id, String name, String dt) {
        return GenericRowData.of(id, StringData.fromString(name), StringData.fromString(dt));
    }

    private DataFilePathFactory createPathFactory() {
        return new DataFilePathFactory(
                new Path(tempDir.toString()),
                "dt=" + PART,
                0,
                CoreOptions.FILE_FORMAT.defaultValue());
    }

    private RecordWriter<RowData> createEmptyWriter(
            long targetFileSize, RowType writeSchema, long maxSeqNum) {
        return createWriter(targetFileSize, writeSchema, maxSeqNum, false, Collections.emptyList());
    }

    private RecordWriter<RowData> createWriter(
            long targetFileSize,
            RowType writeSchema,
            long maxSeqNum,
            boolean forceCompact,
            List<DataFileMeta> scannedFiles) {
        FileFormat fileFormat = FileFormat.fromIdentifier(AVRO, new Configuration());
        FileWriter.Factory<RowData, Metric> fileWriterFactory =
                MetricFileWriter.createFactory(
                        fileFormat.createWriterFactory(writeSchema),
                        Function.identity(),
                        writeSchema,
                        fileFormat.createStatsExtractor(writeSchema).orElse(null));

        return new AppendOnlyWriter(
                SCHEMA_ID,
                targetFileSize,
                fileWriterFactory,
                STATS_SERIALIZER,
                new AppendOnlyCompactManager(
                        Executors.newSingleThreadScheduledExecutor(
                                new ExecutorThreadFactory("compaction-thread")),
                        new AppendOnlyCompactStrategy(targetFileSize),
                        (outputLevel, dropDelete, sections) ->
                                sections.stream()
                                        .map(this::generateCompactAfter)
                                        .collect(Collectors.toList())),
                forceCompact,
                scannedFiles,
                maxSeqNum,
                pathFactory);
    }

    private DataFileMeta generateCompactAfter(List<DataFileMeta> compactBefore) {
        int size = compactBefore.size();
        long minSeq = compactBefore.get(0).minSequenceNumber();
        long maxSeq = compactBefore.get(size - 1).maxSequenceNumber();
        String fileName = "compact-" + UUID.randomUUID();
        return DataFileMeta.forAppend(
                fileName,
                compactBefore.stream().mapToLong(DataFileMeta::fileSize).sum(),
                compactBefore.stream().mapToLong(DataFileMeta::rowCount).sum(),
                STATS_SERIALIZER.toBinary(
                        new FieldStats[] {
                            initStats(
                                    compactBefore.get(0).valueStats().min().getInt(0),
                                    compactBefore.get(size - 1).valueStats().max().getInt(0),
                                    0),
                            initStats(
                                    compactBefore.get(0).valueStats().min().getString(1).toString(),
                                    compactBefore
                                            .get(size - 1)
                                            .valueStats()
                                            .max()
                                            .getString(1)
                                            .toString(),
                                    0),
                            initStats(PART, PART, 0)
                        }),
                minSeq,
                maxSeq,
                compactBefore.get(0).schemaId());
    }
}
