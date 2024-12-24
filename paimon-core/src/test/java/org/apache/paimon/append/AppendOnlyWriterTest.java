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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ExternalBuffer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test the correctness for {@link AppendOnlyWriter}. */
public class AppendOnlyWriterTest {

    private static final InternalRow EMPTY_ROW = BinaryRow.EMPTY_ROW;
    private static final RowType SCHEMA =
            RowType.builder()
                    .fields(
                            new DataType[] {new IntType(), new VarCharType(), new VarCharType()},
                            new String[] {"id", "name", "dt"})
                    .build();
    private static final SimpleStatsConverter STATS_SERIALIZER = new SimpleStatsConverter(SCHEMA);

    @TempDir public java.nio.file.Path tempDir;
    public DataFilePathFactory pathFactory;

    private static final String AVRO = "avro";
    private static final String PART = "2022-05-01";
    private static final long SCHEMA_ID = 0L;
    private static final int MIN_FILE_NUM = 3;
    private static final int MAX_FILE_NUM = 4;

    @BeforeEach
    public void before() {
        pathFactory = createPathFactory();
    }

    @Test
    public void testEmptyCommits() throws Exception {
        RecordWriter<InternalRow> writer = createEmptyWriter(1024 * 1024L);

        for (int i = 0; i < 3; i++) {
            writer.sync();
            CommitIncrement inc = writer.prepareCommit(true);

            assertThat(inc.newFilesIncrement().isEmpty()).isTrue();
            assertThat(inc.compactIncrement().isEmpty()).isTrue();
        }
    }

    @Test
    public void testSingleWrite() throws Exception {
        RecordWriter<InternalRow> writer = createEmptyWriter(1024 * 1024L);
        writer.write(row(1, "AAA", PART));
        CommitIncrement increment = writer.prepareCommit(true);
        writer.close();

        assertThat(increment.newFilesIncrement().newFiles().size()).isEqualTo(1);
        DataFileMeta meta = increment.newFilesIncrement().newFiles().get(0);
        assertThat(meta).isNotNull();

        Path path = pathFactory.toPath(meta);
        assertThat(LocalFileIO.create().exists(path)).isTrue();

        assertThat(meta.rowCount()).isEqualTo(1L);
        assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.keyStats()).isEqualTo(EMPTY_STATS);

        SimpleColStats[] expected =
                new SimpleColStats[] {
                    initStats(1, 1, 0), initStats("AAA", "AAA", 0), initStats(PART, PART, 0)
                };
        assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinaryAllMode(expected));

        assertThat(meta.minSequenceNumber()).isEqualTo(0);
        assertThat(meta.maxSequenceNumber()).isEqualTo(0);
        assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
    }

    @Test
    public void testMultipleCommits() throws Exception {
        RecordWriter<InternalRow> writer =
                createWriter(1024 * 1024L, true, Collections.emptyList()).getLeft();

        // Commit 5 continues txn.
        for (int txn = 0; txn < 5; txn += 1) {

            // Write the records with range [ txn*100, (txn+1)*100 ).
            int start = txn * 100;
            int end = txn * 100 + 100;
            for (int i = start; i < end; i++) {
                writer.write(row(i, String.format("%03d", i), PART));
            }

            writer.sync();
            CommitIncrement inc = writer.prepareCommit(true);
            if (txn > 0 && txn % 3 == 0) {
                assertThat(inc.compactIncrement().compactBefore()).hasSize(4);
                assertThat(inc.compactIncrement().compactAfter()).hasSize(1);
                DataFileMeta compactAfter = inc.compactIncrement().compactAfter().get(0);
                assertThat(compactAfter.fileName()).startsWith("compact-");
                assertThat(compactAfter.fileSize())
                        .isEqualTo(
                                inc.compactIncrement().compactBefore().stream()
                                        .mapToLong(DataFileMeta::fileSize)
                                        .sum());
                assertThat(compactAfter.rowCount())
                        .isEqualTo(
                                inc.compactIncrement().compactBefore().stream()
                                        .mapToLong(DataFileMeta::rowCount)
                                        .sum());
            } else {
                assertThat(inc.compactIncrement().compactBefore())
                        .isEqualTo(Collections.emptyList());
                assertThat(inc.compactIncrement().compactAfter())
                        .isEqualTo(Collections.emptyList());
            }

            assertThat(inc.newFilesIncrement().newFiles().size()).isEqualTo(1);
            DataFileMeta meta = inc.newFilesIncrement().newFiles().get(0);

            Path path = pathFactory.toPath(meta);
            assertThat(LocalFileIO.create().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(100L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(EMPTY_STATS);

            SimpleColStats[] expected =
                    new SimpleColStats[] {
                        initStats(start, end - 1, 0),
                        initStats(String.format("%03d", start), String.format("%03d", end - 1), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinaryAllMode(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(start);
            assertThat(meta.maxSequenceNumber()).isEqualTo(end - 1);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
        }
    }

    @Test
    public void testRollingWrite() throws Exception {
        // Set a very small target file size, so the threshold to trigger rolling becomes record
        // count instead of file size, because we check rolling per 1000 records.
        AppendOnlyWriter writer = createEmptyWriter(10L);

        for (int i = 0; i < 10 * 1000; i++) {
            writer.write(row(i, String.format("%03d", i), PART));
        }

        writer.sync();
        CommitIncrement firstInc = writer.prepareCommit(true);
        assertThat(firstInc.compactIncrement().compactBefore()).isEqualTo(Collections.emptyList());
        assertThat(firstInc.compactIncrement().compactAfter()).isEqualTo(Collections.emptyList());

        assertThat(firstInc.newFilesIncrement().newFiles().size()).isEqualTo(10);

        int id = 0;
        for (DataFileMeta meta : firstInc.newFilesIncrement().newFiles()) {
            Path path = pathFactory.toPath(meta);
            assertThat(LocalFileIO.create().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(1000L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(EMPTY_STATS);

            int min = id * 1000;
            int max = id * 1000 + 999;
            SimpleColStats[] expected =
                    new SimpleColStats[] {
                        initStats(min, max, 0),
                        initStats(String.format("%03d", min), String.format("%03d", max), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinaryAllMode(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(min);
            assertThat(meta.maxSequenceNumber()).isEqualTo(max);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);

            id += 1;
        }

        // increase target file size to test compaction
        long targetFileSize = 1024 * 1024L;
        Pair<AppendOnlyWriter, List<DataFileMeta>> writerAndToCompact =
                createWriter(targetFileSize, true, firstInc.newFilesIncrement().newFiles());
        writer = writerAndToCompact.getLeft();
        List<DataFileMeta> toCompact = writerAndToCompact.getRight();
        assertThat(toCompact).containsExactlyElementsOf(firstInc.newFilesIncrement().newFiles());
        writer.write(row(id, String.format("%03d", id), PART));
        writer.sync();
        CommitIncrement secInc = writer.prepareCommit(true);

        // check compact before and after
        List<DataFileMeta> compactBefore = secInc.compactIncrement().compactBefore();
        List<DataFileMeta> compactAfter = secInc.compactIncrement().compactAfter();
        assertThat(compactBefore)
                .containsExactlyInAnyOrderElementsOf(
                        firstInc.newFilesIncrement().newFiles().subList(0, 4));
        assertThat(compactAfter).hasSize(1);
        assertThat(compactBefore.stream().mapToLong(DataFileMeta::fileSize).sum())
                .isEqualTo(compactAfter.stream().mapToLong(DataFileMeta::fileSize).sum());
        assertThat(compactBefore.stream().mapToLong(DataFileMeta::rowCount).sum())
                .isEqualTo(compactAfter.stream().mapToLong(DataFileMeta::rowCount).sum());
        // check seq number
        assertThat(compactBefore.get(0).minSequenceNumber())
                .isEqualTo(compactAfter.get(0).minSequenceNumber());
        assertThat(compactBefore.get(compactBefore.size() - 1).maxSequenceNumber())
                .isEqualTo(compactAfter.get(compactAfter.size() - 1).maxSequenceNumber());
        assertThat(secInc.newFilesIncrement().newFiles()).hasSize(1);
    }

    @Test
    public void testCloseUnexpectedly() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        AppendOnlyWriter writer =
                createWriter(20 * 1024, false, Collections.emptyList(), latch).getLeft();

        for (int i = 0; i < 10; i++) { // create 10 small files.
            for (int j = 0; j < 10; j++) {
                writer.write(row(j, String.format("%03d", j), PART));
            }
            CommitIncrement increment = writer.prepareCommit(false);
            assertThat(increment.compactIncrement().isEmpty()).isTrue();
        }
        Set<Path> committedFiles =
                Files.walk(tempDir)
                        .filter(Files::isRegularFile)
                        .map(p -> new Path(p.toString()))
                        .collect(Collectors.toSet());

        // start compaction and write more records
        latch.countDown();
        for (int j = 0; j < 10; j++) {
            writer.write(row(j, String.format("%03d", j), PART));
        }
        writer.sync();

        // writer closed unexpectedly
        writer.close();

        Set<Path> afterClosedUnexpectedly =
                Files.walk(tempDir)
                        .filter(Files::isRegularFile)
                        .map(p -> new Path(p.toString()))
                        .collect(Collectors.toSet());
        assertThat(afterClosedUnexpectedly).containsExactlyInAnyOrderElementsOf(committedFiles);
    }

    @Test
    public void testExternalBufferWorks() throws Exception {
        AppendOnlyWriter writer = createEmptyWriter(Long.MAX_VALUE, true);

        // we give it a small Memory Pool, force it to spill
        writer.setMemoryPool(new HeapMemorySegmentPool(16384L, 1024));

        char[] s = new char[990];
        Arrays.fill(s, 'a');

        // set the record that much larger than the maxMemory
        for (int j = 0; j < 100; j++) {
            writer.write(row(j, String.valueOf(s), PART));
        }

        RowBuffer buffer = writer.getWriteBuffer();
        Assertions.assertThat(buffer.size()).isEqualTo(100);
        Assertions.assertThat(buffer.memoryOccupancy()).isLessThanOrEqualTo(16384L);

        writer.close();
    }

    @Test
    public void testSpillWorksAndMoreSmallFilesGenerated() throws Exception {
        List<AppendOnlyWriter> writers = new ArrayList<>();
        HeapMemorySegmentPool heapMemorySegmentPool = new HeapMemorySegmentPool(2501024L, 1024);
        MemoryPoolFactory memoryPoolFactory = new MemoryPoolFactory(heapMemorySegmentPool);
        for (int i = 0; i < 1000; i++) {
            AppendOnlyWriter writer = createEmptyWriter(Long.MAX_VALUE, true);
            memoryPoolFactory.addOwners(Arrays.asList(writer));
            memoryPoolFactory.notifyNewOwner(writer);
            writers.add(writer);
        }

        char[] s = new char[1024];
        Arrays.fill(s, 'a');

        for (AppendOnlyWriter writer : writers) {
            writer.write(row(0, String.valueOf("a"), PART));
        }

        for (AppendOnlyWriter writer : writers) {
            writer.write(row(0, String.valueOf(s), PART));
        }

        for (int j = 0; j < 100; j++) {
            for (AppendOnlyWriter writer : writers) {
                writer.write(row(j, String.valueOf(s), PART));
                writer.write(row(j, String.valueOf(s), PART));
                writer.write(row(j, String.valueOf(s), PART));
                writer.write(row(j, String.valueOf(s), PART));
                writer.write(row(j, String.valueOf(s), PART));
            }
        }

        writers.forEach(
                writer -> {
                    try {
                        List<DataFileMeta> fileMetas =
                                writer.prepareCommit(false).newFilesIncrement().newFiles();
                        assertThat(fileMetas.size()).isEqualTo(1);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        writers.forEach(
                writer -> {
                    try {
                        writer.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testNoBuffer() throws Exception {
        AppendOnlyWriter writer = createEmptyWriter(Long.MAX_VALUE);

        // we give it a small Memory Pool, force it to spill
        writer.setMemoryPool(new HeapMemorySegmentPool(16384L, 1024));

        RowBuffer buffer = writer.getWriteBuffer();
        Assertions.assertThat(buffer).isNull();

        writer.close();
    }

    @Test
    public void testMultipleFlush() throws Exception {
        AppendOnlyWriter writer = createEmptyWriter(Long.MAX_VALUE, true);

        // we give it a small Memory Pool, force it to spill
        writer.setMemoryPool(new HeapMemorySegmentPool(16384L, 1024));

        char[] s = new char[990];
        Arrays.fill(s, 'a');

        // set the record that much larger than the maxMemory
        for (int j = 0; j < 100; j++) {
            writer.write(row(j, String.valueOf(s), PART));
        }

        writer.flush(false, false);
        Assertions.assertThat(writer.memoryOccupancy()).isEqualTo(0L);
        Assertions.assertThat(writer.getWriteBuffer().size()).isEqualTo(0);
        Assertions.assertThat(writer.getNewFiles().size()).isGreaterThan(0);
        long rowCount =
                writer.getNewFiles().stream().map(DataFileMeta::rowCount).reduce(0L, Long::sum);
        Assertions.assertThat(rowCount).isEqualTo(100);

        for (int j = 0; j < 100; j++) {
            writer.write(row(j, String.valueOf(s), PART));
        }
        writer.flush(false, false);

        Assertions.assertThat(writer.memoryOccupancy()).isEqualTo(0L);
        Assertions.assertThat(writer.getWriteBuffer().size()).isEqualTo(0);
        Assertions.assertThat(writer.getNewFiles().size()).isGreaterThan(1);
        rowCount = writer.getNewFiles().stream().map(DataFileMeta::rowCount).reduce(0L, Long::sum);
        Assertions.assertThat(rowCount).isEqualTo(200);
    }

    @Test
    public void testClose() throws Exception {
        AppendOnlyWriter writer = createEmptyWriter(Long.MAX_VALUE, true);

        // we give it a small Memory Pool, force it to spill
        writer.setMemoryPool(new HeapMemorySegmentPool(16384L, 1024));

        char[] s = new char[990];
        Arrays.fill(s, 'a');

        // set the record that much larger than the maxMemory
        for (int j = 0; j < 100; j++) {
            writer.write(row(j, String.valueOf(s), PART));
        }

        ExternalBuffer externalBuffer = (ExternalBuffer) writer.getWriteBuffer();
        List<ChannelWithMeta> channel = externalBuffer.getSpillChannels();

        writer.close();

        for (ChannelWithMeta meta : channel) {
            File file = new File(meta.getChannel().getPath());
            Assertions.assertThat(file.exists()).isEqualTo(false);
        }
        Assertions.assertThat(writer.getWriteBuffer()).isEqualTo(null);
    }

    @Test
    public void testNonSpillable() throws Exception {
        AppendOnlyWriter writer = createEmptyWriter(Long.MAX_VALUE, true);

        // we give it a small Memory Pool, force it to spill
        writer.setMemoryPool(new HeapMemorySegmentPool(2048L, 1024));

        char[] s = new char[990];
        Arrays.fill(s, 'a');

        // set the record that much larger than the maxMemory
        for (int j = 0; j < 100; j++) {
            writer.write(row(j, String.valueOf(s), PART));
        }
        // we got only one file after commit
        Assertions.assertThat(writer.prepareCommit(true).newFilesIncrement().newFiles().size())
                .isEqualTo(1);
        writer.close();

        writer = createEmptyWriter(Long.MAX_VALUE, false);

        // we give it a small Memory Pool, force it to spill
        writer.setMemoryPool(new HeapMemorySegmentPool(2048L, 1024));

        // set the record that much larger than the maxMemory
        for (int j = 0; j < 100; j++) {
            writer.write(row(j, String.valueOf(s), PART));
        }
        // we got 100 files
        Assertions.assertThat(writer.prepareCommit(true).newFilesIncrement().newFiles().size())
                .isEqualTo(100);
        writer.close();
    }

    private SimpleColStats initStats(Integer min, Integer max, long nullCount) {
        return new SimpleColStats(min, max, nullCount);
    }

    private SimpleColStats initStats(String min, String max, long nullCount) {
        return new SimpleColStats(
                BinaryString.fromString(min), BinaryString.fromString(max), nullCount);
    }

    private InternalRow row(int id, String name, String dt) {
        return GenericRow.of(id, BinaryString.fromString(name), BinaryString.fromString(dt));
    }

    private DataFilePathFactory createPathFactory() {
        return new DataFilePathFactory(
                new Path(tempDir + "/dt=" + PART + "/bucket-0"),
                CoreOptions.FILE_FORMAT.defaultValue().toString(),
                CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                CoreOptions.FILE_COMPRESSION.defaultValue());
    }

    private AppendOnlyWriter createEmptyWriter(long targetFileSize) {
        return createWriter(targetFileSize, false, false, false, Collections.emptyList()).getLeft();
    }

    private AppendOnlyWriter createEmptyWriter(long targetFileSize, boolean spillable) {
        return createWriter(targetFileSize, false, true, spillable, Collections.emptyList())
                .getLeft();
    }

    private AppendOnlyWriter createEmptyWriterWithoutIoManager(
            long targetFileSize, boolean spillable) {
        return createWriter(
                        targetFileSize,
                        false,
                        true,
                        spillable,
                        false,
                        Collections.emptyList(),
                        new CountDownLatch(0))
                .getLeft();
    }

    private Pair<AppendOnlyWriter, List<DataFileMeta>> createWriter(
            long targetFileSize, boolean forceCompact, List<DataFileMeta> scannedFiles) {
        return createWriter(
                targetFileSize,
                forceCompact,
                true,
                true,
                true,
                scannedFiles,
                new CountDownLatch(0));
    }

    private Pair<AppendOnlyWriter, List<DataFileMeta>> createWriter(
            long targetFileSize,
            boolean forceCompact,
            boolean useWriteBuffer,
            boolean spillable,
            List<DataFileMeta> scannedFiles) {
        return createWriter(
                targetFileSize,
                forceCompact,
                useWriteBuffer,
                spillable,
                true,
                scannedFiles,
                new CountDownLatch(0));
    }

    private Pair<AppendOnlyWriter, List<DataFileMeta>> createWriter(
            long targetFileSize,
            boolean forceCompact,
            List<DataFileMeta> scannedFiles,
            CountDownLatch latch) {
        return createWriter(targetFileSize, forceCompact, false, false, true, scannedFiles, latch);
    }

    private Pair<AppendOnlyWriter, List<DataFileMeta>> createWriter(
            long targetFileSize,
            boolean forceCompact,
            boolean useWriteBuffer,
            boolean spillable,
            boolean hasIoManager,
            List<DataFileMeta> scannedFiles,
            CountDownLatch latch) {
        FileFormat fileFormat = FileFormat.fromIdentifier(AVRO, new Options());
        LinkedList<DataFileMeta> toCompact = new LinkedList<>(scannedFiles);
        BucketedAppendCompactManager compactManager =
                new BucketedAppendCompactManager(
                        Executors.newSingleThreadScheduledExecutor(
                                new ExecutorThreadFactory("compaction-thread")),
                        toCompact,
                        null,
                        MIN_FILE_NUM,
                        MAX_FILE_NUM,
                        targetFileSize,
                        compactBefore -> {
                            latch.await();
                            return compactBefore.isEmpty()
                                    ? Collections.emptyList()
                                    : Collections.singletonList(
                                            generateCompactAfter(compactBefore));
                        },
                        null);
        CoreOptions options = new CoreOptions(new HashMap<>());
        AppendOnlyWriter writer =
                new AppendOnlyWriter(
                        LocalFileIO.create(),
                        hasIoManager ? IOManager.create(tempDir.toString()) : null,
                        SCHEMA_ID,
                        fileFormat,
                        targetFileSize,
                        AppendOnlyWriterTest.SCHEMA,
                        getMaxSequenceNumber(toCompact),
                        compactManager,
                        null,
                        forceCompact,
                        pathFactory,
                        null,
                        useWriteBuffer,
                        spillable,
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        CompressOptions.defaultOptions(),
                        StatsCollectorFactories.createStatsFactories(
                                options, AppendOnlyWriterTest.SCHEMA.getFieldNames()),
                        MemorySize.MAX_VALUE,
                        new FileIndexOptions(),
                        true,
                        false);
        writer.setMemoryPool(
                new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize()));
        return Pair.of(writer, compactManager.allFiles());
    }

    private DataFileMeta generateCompactAfter(List<DataFileMeta> toCompact) throws IOException {
        int size = toCompact.size();
        long minSeq = toCompact.get(0).minSequenceNumber();
        long maxSeq = toCompact.get(size - 1).maxSequenceNumber();
        String fileName = "compact-" + UUID.randomUUID();
        LocalFileIO.create().newOutputStream(pathFactory.toPath(fileName), false).close();
        return DataFileMeta.forAppend(
                fileName,
                toCompact.stream().mapToLong(DataFileMeta::fileSize).sum(),
                toCompact.stream().mapToLong(DataFileMeta::rowCount).sum(),
                STATS_SERIALIZER.toBinaryAllMode(
                        new SimpleColStats[] {
                            initStats(
                                    toCompact.get(0).valueStats().minValues().getInt(0),
                                    toCompact.get(size - 1).valueStats().maxValues().getInt(0),
                                    0),
                            initStats(
                                    toCompact
                                            .get(0)
                                            .valueStats()
                                            .minValues()
                                            .getString(1)
                                            .toString(),
                                    toCompact
                                            .get(size - 1)
                                            .valueStats()
                                            .maxValues()
                                            .getString(1)
                                            .toString(),
                                    0),
                            initStats(PART, PART, 0)
                        }),
                minSeq,
                maxSeq,
                toCompact.get(0).schemaId(),
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                null,
                null);
    }
}
