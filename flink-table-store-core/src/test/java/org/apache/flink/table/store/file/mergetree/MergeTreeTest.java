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
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.format.FlushingFileFormat;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.CompactUnit;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MergeTreeReader} and {@link MergeTreeWriter}. */
public class MergeTreeTest {

    @TempDir java.nio.file.Path tempDir;
    private static ExecutorService service;
    private FileStorePathFactory pathFactory;
    private Comparator<RowData> comparator;

    private MergeTreeOptions options;
    private DataFileReader dataFileReader;
    private DataFileWriter dataFileWriter;
    private RecordWriter writer;

    @BeforeEach
    public void beforeEach() throws IOException {
        pathFactory = new FileStorePathFactory(new Path(tempDir.toString()));
        comparator = Comparator.comparingInt(o -> o.getInt(0));
        recreateMergeTree(1024 * 1024, false);
        Path bucketDir = dataFileWriter.pathFactory().toPath("ignore").getParent();
        bucketDir.getFileSystem().mkdirs(bucketDir);
    }

    private void recreateMergeTree(long targetFileSize, boolean compactionTask) {
        recreateMergeTree(Collections.emptyList(), targetFileSize, compactionTask);
    }

    private void recreateMergeTree(
            List<DataFileMeta> restoredFiles, long targetFileSize, boolean compactionTask) {
        Configuration configuration = new Configuration();
        configuration.set(MergeTreeOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        configuration.set(MergeTreeOptions.PAGE_SIZE, new MemorySize(4096));
        configuration.set(MergeTreeOptions.TARGET_FILE_SIZE, new MemorySize(targetFileSize));
        options = new MergeTreeOptions(configuration);
        RowType keyType = new RowType(singletonList(new RowType.RowField("k", new IntType())));
        RowType valueType = new RowType(singletonList(new RowType.RowField("v", new IntType())));
        FileFormat flushingAvro = new FlushingFileFormat("avro");
        dataFileReader =
                new DataFileReader.Factory(keyType, valueType, flushingAvro, pathFactory)
                        .create(BinaryRowDataUtil.EMPTY_ROW, 0);
        dataFileWriter =
                new DataFileWriter.Factory(
                                keyType,
                                valueType,
                                flushingAvro,
                                pathFactory,
                                options.targetFileSize)
                        .create(BinaryRowDataUtil.EMPTY_ROW, 0);
        writer = createMergeTreeWriter(restoredFiles, compactionTask);
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
        List<DataFileMeta> newFiles = writer.prepareCommit().newFiles();
        writer = createMergeTreeWriter(newFiles, false);
        expected.addAll(writeBatch());
        writer.prepareCommit();
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

    @Test
    public void testCompactEmpty() throws Exception {
        int targetFileSize = 6383;
        int batchSize = 185;
        recreateMergeTree(targetFileSize, false);
        List<TestRecord> records = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            records.add(new TestRecord(ValueKind.ADD, i, i));
        }
        writeAll(records);
        writer.sync();
        Increment increment = writer.prepareCommit();
        assertThat(increment.compactBefore()).isEmpty();
        assertThat(increment.compactAfter()).isEmpty();
        assertThat(increment.newFiles()).isNotEmpty();
        Set<DataFileMeta> scannedFiles = new HashSet<>(increment.newFiles());
        assertThat(pickManifest(scannedFiles, targetFileSize)).isEmpty();

        recreateMergeTree(pickManifest(scannedFiles, targetFileSize), targetFileSize, true);
        Increment compactIncrement = writer.prepareCommit();
        assertThat(compactIncrement.newFiles()).isEmpty();
        assertThat(compactIncrement.compactBefore()).isEmpty();
        assertThat(compactIncrement.compactAfter()).isEmpty();
        assertThat(((MergeTreeWriter) writer).levels().allFiles()).isEmpty();
    }

    @Test
    public void testCompactSmall() throws Exception {
        // the outer list stands for each commit
        // the inner list stands for the records of each commit
        // [1, 3], [7, 9], [4, 6]
        List<List<TestRecord>> transactions =
                Arrays.asList(
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 1, 1),
                                new TestRecord(ValueKind.ADD, 2, 2),
                                new TestRecord(ValueKind.ADD, 3, 3)),
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 7, 7),
                                new TestRecord(ValueKind.ADD, 8, 8),
                                new TestRecord(ValueKind.ADD, 9, 9)),
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 4, 4),
                                new TestRecord(ValueKind.ADD, 5, 5),
                                new TestRecord(ValueKind.ADD, 6, 6)));
    }

    @Test
    public void testCompactOverlap() throws Exception {
        // [1, 5], [5, 30], [25, 90]
        List<List<TestRecord>> transactions =
                Arrays.asList(
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 1, 1),
                                new TestRecord(ValueKind.ADD, 2, 2),
                                new TestRecord(ValueKind.ADD, 3, 3),
                                new TestRecord(ValueKind.ADD, 4, 4),
                                new TestRecord(ValueKind.ADD, 5, 5)),
                        Arrays.asList(
                                new TestRecord(ValueKind.DELETE, 5, 5),
                                new TestRecord(ValueKind.ADD, 10, 10),
                                new TestRecord(ValueKind.ADD, 25, 25),
                                new TestRecord(ValueKind.ADD, 30, 30)),
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 60, 60),
                                new TestRecord(ValueKind.ADD, 70, 70),
                                new TestRecord(ValueKind.ADD, 80, 80),
                                new TestRecord(ValueKind.ADD, 90, 90),
                                new TestRecord(ValueKind.DELETE, 25, 25)));
        assertCompaction(transactions);
    }

    @Test
    public void testCompactMix() throws Exception {
        // [1, 3], [10, 25], [20, 40], [70, 80], [75, 100]
        List<List<TestRecord>> transactions =
                Arrays.asList(
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 1, 1),
                                new TestRecord(ValueKind.ADD, 2, 2),
                                new TestRecord(ValueKind.ADD, 3, 3)),
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 10, 10),
                                new TestRecord(ValueKind.ADD, 25, 25)),
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 20, 20),
                                new TestRecord(ValueKind.ADD, 30, 30),
                                new TestRecord(ValueKind.ADD, 40, 40)),
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 70, 70),
                                new TestRecord(ValueKind.ADD, 75, 75),
                                new TestRecord(ValueKind.ADD, 80, 80)),
                        Arrays.asList(
                                new TestRecord(ValueKind.ADD, 75, 75),
                                new TestRecord(ValueKind.ADD, 100, 100)));
        assertCompaction(transactions);
    }

    @Test
    public void testCompactRandom() throws Exception {
        List<List<TestRecord>> transactions = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            boolean writeSmall = random.nextBoolean();
            List<TestRecord> records = generateRandom(writeSmall ? 20 : 20_000);
            transactions.add(records);
        }
        //        assertCompaction2(transactions, false);
    }

    private List<DataFileMeta> pickManifest(Set<DataFileMeta> scannedFiles, long targetFileSize) {
        List<DataFileMeta> smallFiles =
                scannedFiles.stream()
                        .filter(file -> file.fileSize() < targetFileSize)
                        .collect(Collectors.toList());

        List<DataFileMeta> overlappedFiles =
                new IntervalPartition(new ArrayList<>(scannedFiles), comparator)
                        .partition().stream()
                                .filter(section -> section.size() > 1)
                                .flatMap(Collection::stream)
                                .map(SortedRun::files)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());

        return Stream.concat(smallFiles.stream(), overlappedFiles.stream())
                .distinct()
                .collect(Collectors.toList());
    }

    private List<Increment> writeTransaction(List<List<TestRecord>> transactions) throws Exception {
        // commit transactions
        List<Increment> increments = new ArrayList<>();
        Set<DataFileMeta> scannedFiles = new HashSet<>();
        for (List<TestRecord> records : transactions) {
            writeAll(records);
            writer.sync();
            increments.add(writer.prepareCommit());
        }
        return increments;
    }

    private void assertCompaction2(
            List<List<TestRecord>> transactions,
            long targetFileSize,
            List<DataFileMeta> expectedPicked)
            throws Exception {
        // commit transactions
        List<Increment> increments = new ArrayList<>();
        Set<DataFileMeta> scannedFiles = new HashSet<>();
        for (List<TestRecord> records : transactions) {
            writeAll(records);
            writer.sync();
            increments.add(writer.prepareCommit());
        }
        // perform scan
        for (Increment increment : increments) {
            scannedFiles.addAll(increment.newFiles());
            increment.compactBefore().forEach(scannedFiles::remove);
            scannedFiles.addAll(increment.compactAfter());
        }

        // pick manifest
        List<DataFileMeta> picked = pickManifest(scannedFiles, targetFileSize);
        assertThat(picked).containsExactlyInAnyOrderElementsOf(expectedPicked);

        // use picked manifest to restore merge tree
        recreateMergeTree(picked, targetFileSize, true);
        writeAll(readAll(picked, true));
        writer.sync();
        Increment increment = writer.prepareCommit();

        // check compaction does not generate new files
        assertThat(increment.newFiles()).isEmpty();

        // check all picked manifest are marked as delete
        assertThat(increment.compactBefore()).containsExactlyInAnyOrderElementsOf(picked);

        // check compaction rewrite
        assertRecords(readAll(picked, true), increment.compactAfter(), true);

        // scan again to check full data
        scannedFiles.addAll(increment.compactAfter());
        increment.compactBefore().forEach(scannedFiles::remove);
        assertRecords(
                transactions.stream().flatMap(Collection::stream).collect(Collectors.toList()),
                new ArrayList<>(scannedFiles),
                true);
    }

    private void assertCompaction(List<List<TestRecord>> transactions) throws Exception {
        List<DataFileMeta> restoredFiles = new ArrayList<>();
        for (List<TestRecord> records : transactions) {
            writeAll(records);
            writer.sync();
            Increment increment = writer.prepareCommit();
            assertThat(increment.compactBefore()).isEmpty();
            assertThat(increment.compactAfter()).isEmpty();
            restoredFiles.addAll(increment.newFiles());
        }
        recreateMergeTree(restoredFiles, 1024 * 1024, true);
        Increment increment = writer.prepareCommit();
        assertThat(increment.newFiles()).isEmpty();
        assertThat(increment.compactBefore()).containsExactlyInAnyOrderElementsOf(restoredFiles);
        assertRecords(
                transactions.stream().flatMap(Collection::stream).collect(Collectors.toList()),
                increment.compactAfter(),
                true);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 1024 * 1024})
    public void testCloseUpgrade(long targetFileSize) throws Exception {
        // To generate a large number of upgrade files
        recreateMergeTree(targetFileSize, false);

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
                                random.nextBoolean() ? ValueKind.ADD : ValueKind.DELETE,
                                random.nextInt(perBatch / 2) - i * (perBatch / 2),
                                random.nextInt()));
            }
            writeAll(records);
            expected.addAll(records);
            Increment increment = writer.prepareCommit();
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

            Increment increment = writer.prepareCommit();
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

    private MergeTreeWriter createMergeTreeWriter(
            List<DataFileMeta> files, boolean compactionTask) {
        long maxSequenceNumber =
                files.stream().map(DataFileMeta::maxSequenceNumber).max(Long::compare).orElse(-1L);
        return new MergeTreeWriter(
                new SortBufferMemTable(
                        dataFileWriter.keyType(),
                        dataFileWriter.valueType(),
                        options.writeBufferSize,
                        options.pageSize),
                createCompactManager(compactionTask, dataFileWriter, service),
                new Levels(comparator, files, options.numLevels),
                maxSequenceNumber,
                comparator,
                new DeduplicateMergeFunction(),
                dataFileWriter,
                options.commitForceCompact,
                options.numSortedRunStopTrigger,
                compactionTask);
    }

    private CompactManager createCompactManager(
            boolean compactionTask,
            DataFileWriter dataFileWriter,
            ExecutorService compactExecutor) {
        CompactStrategy compactStrategy =
                compactionTask
                        ? (numLevels, runs) ->
                                Optional.of(CompactUnit.fromLevelRuns(numLevels - 1, runs))
                        : new UniversalCompaction(
                                options.maxSizeAmplificationPercent,
                                options.sizeRatio,
                                options.numSortedRunCompactionTrigger);
        CompactManager.Rewriter rewriter =
                (outputLevel, dropDelete, sections) ->
                        dataFileWriter.write(
                                new RecordReaderIterator<KeyValue>(
                                        new MergeTreeReader(
                                                sections,
                                                dropDelete,
                                                dataFileReader,
                                                comparator,
                                                new DeduplicateMergeFunction())),
                                outputLevel);
        return new CompactManager(
                compactExecutor, compactStrategy, comparator, options.targetFileSize, rewriter);
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
                    .filter(record -> record.kind == ValueKind.ADD)
                    .collect(Collectors.toList());
        }
        return new ArrayList<>(map.values());
    }

    private void writeAll(List<TestRecord> records) throws Exception {
        for (TestRecord record : records) {
            writer.write(record.kind, row(record.k), row(record.v));
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
                            random.nextBoolean() ? ValueKind.ADD : ValueKind.DELETE,
                            random.nextInt(perBatch / 2),
                            random.nextInt()));
        }
        return records;
    }

    private List<TestRecord> generateNoOverlap(int perBatch, int previousMax) {
        Random random = new Random();
        List<TestRecord> records = new ArrayList<>(perBatch);
        for (int i = 0; i < perBatch; i++) {
            previousMax += random.nextInt(perBatch / 2);
            records.add(
                    new TestRecord(
                            random.nextBoolean() ? ValueKind.ADD : ValueKind.DELETE,
                            previousMax,
                            random.nextInt()));
        }
        return records;
    }

    private static class TestRecord {

        private final ValueKind kind;
        private final int k;
        private final int v;

        private TestRecord(ValueKind kind, int k, int v) {
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
