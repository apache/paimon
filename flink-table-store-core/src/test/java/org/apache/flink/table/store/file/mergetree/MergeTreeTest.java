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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.CoreOptions.ChangelogProducer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.format.FlushingFileFormat;
import org.apache.flink.table.store.file.memory.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.mergetree.compact.CompactRewriter;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManager;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MergeTreeReader} and {@link MergeTreeWriter}. */
public class MergeTreeTest {

    @TempDir java.nio.file.Path tempDir;
    private static ExecutorService service;
    private Path path;
    private FileStorePathFactory pathFactory;
    private Comparator<RowData> comparator;

    private CoreOptions options;
    private DataFileReader dataFileReader;
    private DataFileWriter dataFileWriter;
    private RecordWriter<KeyValue> writer;

    @BeforeEach
    public void beforeEach() throws IOException {
        path = new Path(tempDir.toString());
        pathFactory = new FileStorePathFactory(path);
        comparator = Comparator.comparingInt(o -> o.getInt(0));
        recreateMergeTree(1024 * 1024);
        Path bucketDir = dataFileWriter.pathFactory().toPath("ignore").getParent();
        bucketDir.getFileSystem().mkdirs(bucketDir);
    }

    private void recreateMergeTree(long targetFileSize) {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        configuration.set(CoreOptions.PAGE_SIZE, new MemorySize(4096));
        configuration.set(CoreOptions.TARGET_FILE_SIZE, new MemorySize(targetFileSize));
        options = new CoreOptions(configuration);
        RowType keyType = new RowType(singletonList(new RowType.RowField("k", new IntType())));
        RowType valueType = new RowType(singletonList(new RowType.RowField("v", new IntType())));
        FileFormat flushingAvro = new FlushingFileFormat("avro");
        dataFileReader =
                new DataFileReader.Factory(
                                new SchemaManager(path),
                                0,
                                keyType,
                                valueType,
                                flushingAvro,
                                pathFactory)
                        .create(BinaryRowDataUtil.EMPTY_ROW, 0);
        dataFileWriter =
                new DataFileWriter.Factory(
                                0,
                                keyType,
                                valueType,
                                flushingAvro,
                                pathFactory,
                                options.targetFileSize())
                        .create(BinaryRowDataUtil.EMPTY_ROW, 0);
        writer = createMergeTreeWriter(Collections.emptyList());
    }

    @BeforeAll
    public static void before() {
        service = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    public static void after() {
        service.shutdownNow();
        service = null;
    }

    @Test
    public void testEmpty() throws Exception {
        doTestWriteRead(0);
    }

    @Test
    public void test1() throws Exception {
        doTestWriteRead(1);
    }

    @Test
    public void test2() throws Exception {
        doTestWriteRead(new Random().nextInt(2));
    }

    @Test
    public void test8() throws Exception {
        doTestWriteRead(new Random().nextInt(8));
    }

    @Test
    public void testRandom() throws Exception {
        doTestWriteRead(new Random().nextInt(20));
    }

    @Test
    public void testRestore() throws Exception {
        List<TestRecord> expected = new ArrayList<>(writeBatch());
        List<DataFileMeta> newFiles = writer.prepareCommit(true).newFiles();
        writer = createMergeTreeWriter(newFiles);
        expected.addAll(writeBatch());
        writer.prepareCommit(true);
        writer.sync();
        assertRecords(expected);
    }

    @Test
    public void testClose() throws Exception {
        doTestWriteRead(6);
        List<DataFileMeta> files = writer.close();
        for (DataFileMeta file : files) {
            Path path = dataFileWriter.pathFactory().toPath(file.fileName());
            assertThat(path.getFileSystem().exists(path)).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 1024 * 1024})
    public void testCloseUpgrade(long targetFileSize) throws Exception {
        // To generate a large number of upgrade files
        recreateMergeTree(targetFileSize);

        List<TestRecord> expected = new ArrayList<>();
        Random random = new Random();
        int perBatch = 1_000;
        Set<String> newFileNames = new HashSet<>();
        List<DataFileMeta> compactedFiles = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            List<TestRecord> records = new ArrayList<>(perBatch);
            for (int j = 0; j < perBatch; j++) {
                records.add(
                        new TestRecord(
                                random.nextBoolean() ? RowKind.INSERT : RowKind.DELETE,
                                random.nextInt(perBatch / 2) - i * (perBatch / 2),
                                random.nextInt()));
            }
            writeAll(records);
            expected.addAll(records);
            Increment increment = writer.prepareCommit(true);
            mergeCompacted(newFileNames, compactedFiles, increment);
        }
        writer.close();

        assertRecords(expected, compactedFiles, true);
    }

    @Test
    public void testWriteMany() throws Exception {
        doTestWriteRead(3, 20_000);
    }

    private void doTestWriteRead(int batchNumber) throws Exception {
        doTestWriteRead(batchNumber, 200);
    }

    private void doTestWriteRead(int batchNumber, int perBatch) throws Exception {
        List<TestRecord> expected = new ArrayList<>();
        List<DataFileMeta> newFiles = new ArrayList<>();
        Set<String> newFileNames = new HashSet<>();
        List<DataFileMeta> compactedFiles = new ArrayList<>();

        // write batch and commit
        for (int i = 0; i <= batchNumber; i++) {
            if (i < batchNumber) {
                expected.addAll(writeBatch(perBatch));
            } else {
                writer.sync();
            }

            Increment increment = writer.prepareCommit(true);
            newFiles.addAll(increment.newFiles());
            mergeCompacted(newFileNames, compactedFiles, increment);
        }

        // assert records from writer
        assertRecords(expected);

        // assert records from increment new files
        assertRecords(expected, newFiles, false);
        assertRecords(expected, newFiles, true);

        // assert records from increment compacted files
        assertRecords(expected, compactedFiles, true);

        Path bucketDir = dataFileWriter.pathFactory().toPath("ignore").getParent();
        Set<String> files =
                Arrays.stream(bucketDir.getFileSystem().listStatus(bucketDir))
                        .map(FileStatus::getPath)
                        .map(Path::getName)
                        .collect(Collectors.toSet());
        newFiles.stream().map(DataFileMeta::fileName).forEach(files::remove);
        compactedFiles.stream().map(DataFileMeta::fileName).forEach(files::remove);
        assertThat(files).isEqualTo(Collections.emptySet());
    }

    private MergeTreeWriter createMergeTreeWriter(List<DataFileMeta> files) {
        long maxSequenceNumber =
                files.stream().map(DataFileMeta::maxSequenceNumber).max(Long::compare).orElse(-1L);
        MergeTreeWriter writer =
                new MergeTreeWriter(
                        dataFileWriter.keyType(),
                        dataFileWriter.valueType(),
                        createCompactManager(dataFileWriter, service, files),
                        new Levels(comparator, files, options.numLevels()),
                        maxSequenceNumber,
                        comparator,
                        new DeduplicateMergeFunction(),
                        dataFileWriter,
                        options.commitForceCompact(),
                        options.numSortedRunStopTrigger(),
                        ChangelogProducer.NONE);
        writer.setMemoryPool(
                new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize()));
        return writer;
    }

    private MergeTreeCompactManager createCompactManager(
            DataFileWriter dataFileWriter,
            ExecutorService compactExecutor,
            List<DataFileMeta> files) {
        CompactStrategy strategy =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger(),
                        options.maxSortedRunNum());
        CompactRewriter rewriter =
                (outputLevel, dropDelete, sections) ->
                        dataFileWriter.write(
                                new RecordReaderIterator<>(
                                        new MergeTreeReader(
                                                sections,
                                                dropDelete,
                                                dataFileReader,
                                                comparator,
                                                new DeduplicateMergeFunction())),
                                outputLevel);
        return new MergeTreeCompactManager(
                compactExecutor,
                new Levels(comparator, files, options.numLevels()),
                strategy,
                comparator,
                options.targetFileSize(),
                rewriter);
    }

    private void mergeCompacted(
            Set<String> newFileNames, List<DataFileMeta> compactedFiles, Increment increment) {
        increment.newFiles().stream().map(DataFileMeta::fileName).forEach(newFileNames::add);
        compactedFiles.addAll(increment.newFiles());
        Set<String> afterFiles =
                increment.compactAfter().stream()
                        .map(DataFileMeta::fileName)
                        .collect(Collectors.toSet());
        for (DataFileMeta file : increment.compactBefore()) {
            boolean remove = compactedFiles.remove(file);
            assertThat(remove).isTrue();
            // See MergeTreeWriter.updateCompactResult
            if (!newFileNames.contains(file.fileName()) && !afterFiles.contains(file.fileName())) {
                dataFileWriter.delete(file);
            }
        }
        compactedFiles.addAll(increment.compactAfter());
    }

    private List<TestRecord> writeBatch() throws Exception {
        return writeBatch(200);
    }

    private List<TestRecord> writeBatch(int perBatch) throws Exception {
        List<TestRecord> records = generateRandom(perBatch);
        writeAll(records);
        return records;
    }

    private void assertRecords(List<TestRecord> expected) throws Exception {
        // compaction will drop delete
        List<DataFileMeta> files = ((MergeTreeWriter) writer).levels().allFiles();
        assertRecords(expected, files, true);
    }

    private void assertRecords(
            List<TestRecord> expected, List<DataFileMeta> files, boolean dropDelete)
            throws Exception {
        assertThat(readAll(files, dropDelete)).isEqualTo(compactAndSort(expected, dropDelete));
    }

    private List<TestRecord> compactAndSort(List<TestRecord> records, boolean dropDelete) {
        TreeMap<Integer, TestRecord> map = new TreeMap<>();
        for (TestRecord record : records) {
            map.put(record.k, record);
        }
        if (dropDelete) {
            return map.values().stream()
                    .filter(record -> record.kind == RowKind.INSERT)
                    .collect(Collectors.toList());
        }
        return new ArrayList<>(map.values());
    }

    private void writeAll(List<TestRecord> records) throws Exception {
        for (TestRecord record : records) {
            KeyValue kv = new KeyValue().replace(row(record.k), record.kind, row(record.v));
            writer.write(kv);
        }
    }

    private List<TestRecord> readAll(List<DataFileMeta> files, boolean dropDelete)
            throws Exception {
        RecordReader<KeyValue> reader =
                new MergeTreeReader(
                        new IntervalPartition(files, comparator).partition(),
                        dropDelete,
                        dataFileReader,
                        comparator,
                        new DeduplicateMergeFunction());
        List<TestRecord> records = new ArrayList<>();
        try (RecordReaderIterator<KeyValue> iterator = new RecordReaderIterator<KeyValue>(reader)) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                records.add(
                        new TestRecord(kv.valueKind(), kv.key().getInt(0), kv.value().getInt(0)));
            }
        }
        return records;
    }

    private RowData row(int i) {
        return GenericRowData.of(i);
    }

    private List<TestRecord> generateRandom(int perBatch) {
        Random random = new Random();
        List<TestRecord> records = new ArrayList<>(perBatch);
        for (int i = 0; i < perBatch; i++) {
            records.add(
                    new TestRecord(
                            random.nextBoolean() ? RowKind.INSERT : RowKind.DELETE,
                            random.nextInt(perBatch / 2),
                            random.nextInt()));
        }
        return records;
    }

    private static class TestRecord {

        private final RowKind kind;
        private final int k;
        private final int v;

        private TestRecord(RowKind kind, int k, int v) {
            this.kind = kind;
            this.k = k;
            this.v = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestRecord that = (TestRecord) o;
            return k == that.k && v == that.v && kind == that.kind;
        }

        @Override
        public String toString() {
            return "TestRecord{" + "kind=" + kind + ", k=" + k + ", v=" + v + '}';
        }
    }
}
