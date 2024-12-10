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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.compact.AbstractCompactRewriter;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SchemaEvolutionTableTestBase;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FileStorePathFactory;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MergeTreeReaders} and {@link MergeTreeWriter}. */
public abstract class MergeTreeTestBase {

    @TempDir java.nio.file.Path tempDir;
    private static ExecutorService service;
    private Path path;
    private FileStorePathFactory pathFactory;
    private Comparator<InternalRow> comparator;

    private CoreOptions options;
    private KeyValueFileReaderFactory readerFactory;
    private KeyValueFileReaderFactory compactReaderFactory;
    private KeyValueFileWriterFactory writerFactory;
    private KeyValueFileWriterFactory compactWriterFactory;
    private MergeTreeWriter writer;

    @BeforeEach
    public void beforeEach() throws IOException {
        path = new Path(tempDir.toString());
        pathFactory = createNonPartFactory(path);
        comparator = Comparator.comparingInt(o -> o.getInt(0));
        recreateMergeTree(1024 * 1024);
        Path bucketDir = writerFactory.pathFactory(0).toPath("ignore").getParent();
        LocalFileIO.create().mkdirs(bucketDir);
    }

    private SchemaManager createTestingSchemaManager(Path path) {
        TableSchema schema =
                new TableSchema(
                        0,
                        new ArrayList<>(),
                        -1,
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new HashMap<>(),
                        "");
        Map<Long, TableSchema> schemas = new HashMap<>();
        schemas.put(schema.id(), schema);

        return new SchemaEvolutionTableTestBase.TestingSchemaManager(path, schemas);
    }

    private void recreateMergeTree(long targetFileSize) {
        Options options = new Options();
        options.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        options.set(CoreOptions.PAGE_SIZE, new MemorySize(4096));
        options.set(CoreOptions.TARGET_FILE_SIZE, new MemorySize(targetFileSize));
        options.set(CoreOptions.SORT_ENGINE, getSortEngine());
        options.set(
                CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER,
                options.get(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER) + 1);
        options.set(CoreOptions.DATA_FILE_THIN_MODE, false);
        this.options = new CoreOptions(options);
        RowType keyType = new RowType(singletonList(new DataField(0, "k", new IntType())));
        RowType valueType = new RowType(singletonList(new DataField(1, "v", new IntType())));

        String identifier = "avro";
        FileFormat flushingAvro = new FlushingFileFormat(identifier);
        KeyValueFileReaderFactory.Builder readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        LocalFileIO.create(),
                        createTestingSchemaManager(path),
                        createTestingSchemaManager(path).schema(0),
                        keyType,
                        valueType,
                        ignore -> flushingAvro,
                        pathFactory,
                        new KeyValueFieldsExtractor() {
                            @Override
                            public List<DataField> keyFields(TableSchema schema) {
                                return keyType.getFields();
                            }

                            @Override
                            public List<DataField> valueFields(TableSchema schema) {
                                return valueType.getFields();
                            }
                        },
                        new CoreOptions(new HashMap<>()));
        readerFactory =
                readerFactoryBuilder.build(BinaryRow.EMPTY_ROW, 0, DeletionVector.emptyFactory());
        compactReaderFactory =
                readerFactoryBuilder.build(BinaryRow.EMPTY_ROW, 0, DeletionVector.emptyFactory());

        Map<String, FileStorePathFactory> pathFactoryMap = new HashMap<>();
        pathFactoryMap.put(identifier, pathFactory);
        KeyValueFileWriterFactory.Builder writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        LocalFileIO.create(),
                        0,
                        keyType,
                        valueType,
                        flushingAvro,
                        pathFactoryMap,
                        this.options.targetFileSize(true));
        writerFactory = writerFactoryBuilder.build(BinaryRow.EMPTY_ROW, 0, this.options);
        compactWriterFactory = writerFactoryBuilder.build(BinaryRow.EMPTY_ROW, 0, this.options);
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
        List<DataFileMeta> newFiles = writer.prepareCommit(true).newFilesIncrement().newFiles();
        writer = createMergeTreeWriter(newFiles);
        expected.addAll(writeBatch());
        writer.prepareCommit(true);
        writer.sync();
        assertRecords(expected);
    }

    @Test
    public void testPrepareCommitWaitCompaction() throws Exception {

        List<DataFileMeta> newFiles = generateDataFileToCommit();

        writer = createMergeTreeWriter(newFiles);
        writeBatch(2);
        CommitIncrement increment = writer.prepareCommit(false);

        assertThat(increment.newFilesIncrement().newFiles().size()).isEqualTo(1);

        assertThat(increment.compactIncrement().compactBefore().size()).isEqualTo(11);
        assertThat(increment.compactIncrement().compactAfter().size()).isEqualTo(1);
    }

    private List<DataFileMeta> generateDataFileToCommit() throws Exception {
        List<DataFileMeta> newFiles = new ArrayList<>();

        // append successsfully but compaction fail
        for (int i = 0; i < 10; i++) {
            List<DataFileMeta> dataFileMetas = new ArrayList<>();
            MergeTreeCompactManager compactManager =
                    createCompactManager(service, new ArrayList<>());
            writer = createMergeTreeWriter(dataFileMetas, compactManager);
            writeBatch(200);
            newFiles.addAll(writer.dataFiles());
        }
        return newFiles;
    }

    @Test
    public void testPrepareCommitRecycleReference() throws Exception {

        List<DataFileMeta> dataFileMetas = generateDataFileToCommit();

        MockFailResultCompactionManager mockFailResultCompactionManager =
                new MockFailResultCompactionManager(
                        service,
                        new Levels(comparator, dataFileMetas, options.numLevels()),
                        new UniversalCompaction(
                                options.maxSizeAmplificationPercent(),
                                options.sortedRunSizeRatio(),
                                options.numSortedRunCompactionTrigger()),
                        comparator,
                        options.targetFileSize(true),
                        options.numSortedRunStopTrigger(),
                        new TestRewriter());
        writer = createMergeTreeWriter(dataFileMetas, mockFailResultCompactionManager);

        writeBatch(2);

        // When PrepareCommit receives an exception, trigger the close method to simulate the
        // lifecycle of Flink Task.
        RunnableWithException computeEngineCall =
                () -> {
                    try {
                        try {
                            writer.prepareCommit(false);
                        } catch (Throwable e) {
                            throw new IOException("mock", e);
                        }
                    } catch (Throwable e) {
                        try {
                            writer.close();
                        } catch (Throwable ex) {
                            e.addSuppressed(ex);
                        }
                        throw e;
                    }
                };

        Throwable ex = null;
        try {
            computeEngineCall.run();
        } catch (Throwable e) {
            ex = e;
        }

        assertThat(ex).isNotNull();
        assertThat(ExceptionUtils.findThrowable(ex, ExecutionException.class)).isPresent();
        assertThat(ExceptionUtils.stringifyException(ex).contains("CIRCULAR REFERENCE")).isFalse();
    }

    interface RunnableWithException {
        void run() throws Exception;
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
            CommitIncrement increment = writer.prepareCommit(true);
            mergeCompacted(newFileNames, compactedFiles, increment);
        }
        writer.close();

        assertRecords(expected, compactedFiles, true);
    }

    @Test
    public void testWriteMany() throws Exception {
        doTestWriteRead(3, 20_000);
    }

    @Test
    public void testChangelog() throws Exception {
        writer =
                createMergeTreeWriter(
                        Collections.emptyList(),
                        createCompactManager(service, Collections.emptyList()),
                        ChangelogProducer.INPUT);

        doTestWriteReadWithChangelog(8, 200, false);
    }

    @Test
    public void testChangelogFromCopyingData() throws Exception {
        writer =
                createMergeTreeWriter(
                        Collections.emptyList(),
                        createCompactManager(service, Collections.emptyList()),
                        ChangelogProducer.INPUT);
        writer.withInsertOnly(true);

        doTestWriteReadWithChangelog(8, 200, true);
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

            CommitIncrement increment = writer.prepareCommit(true);
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            mergeCompacted(newFileNames, compactedFiles, increment);
        }

        // assert records from writer
        assertRecords(expected);

        // assert records from increment new files
        assertRecords(expected, newFiles, false);
        assertRecords(expected, newFiles, true);

        // assert records from increment compacted files
        assertRecords(expected, compactedFiles, true);

        writer.close();

        Path bucketDir = writerFactory.pathFactory(0).toPath("ignore").getParent();
        Set<String> files =
                Arrays.stream(LocalFileIO.create().listStatus(bucketDir))
                        .map(FileStatus::getPath)
                        .map(Path::getName)
                        .collect(Collectors.toSet());
        newFiles.stream().map(DataFileMeta::fileName).forEach(files::remove);
        compactedFiles.stream().map(DataFileMeta::fileName).forEach(files::remove);
        assertThat(files).isEqualTo(Collections.emptySet());
    }

    private void doTestWriteReadWithChangelog(
            int batchNumber, int perBatch, boolean isChangelogEqualToData) throws Exception {
        List<TestRecord> expected = new ArrayList<>();
        List<DataFileMeta> newFiles = new ArrayList<>();
        List<DataFileMeta> changelogFiles = new ArrayList<>();
        Set<String> newFileNames = new HashSet<>();
        List<DataFileMeta> compactedFiles = new ArrayList<>();

        // write batch and commit
        for (int i = 0; i <= batchNumber; i++) {
            if (i < batchNumber) {
                expected.addAll(writeBatch(perBatch));
            } else {
                writer.sync();
            }

            CommitIncrement increment = writer.prepareCommit(true);
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            changelogFiles.addAll(increment.newFilesIncrement().changelogFiles());
            mergeCompacted(newFileNames, compactedFiles, increment);
        }

        // assert records from writer
        assertRecords(expected);

        // assert records from increment new files
        assertRecords(expected, newFiles, false);
        assertRecords(expected, newFiles, true);

        // assert records from changelog files
        if (isChangelogEqualToData) {
            assertRecords(expected, changelogFiles, false);
            assertRecords(expected, changelogFiles, true);
        } else {
            List<TestRecord> actual = new ArrayList<>();
            for (DataFileMeta changelogFile : changelogFiles) {
                actual.addAll(readAll(Collections.singletonList(changelogFile), false));
            }
            assertThat(actual).containsExactlyInAnyOrder(expected.toArray(new TestRecord[0]));
        }

        // assert records from increment compacted files
        assertRecords(expected, compactedFiles, true);

        writer.close();

        Path bucketDir = writerFactory.pathFactory(0).toPath("ignore").getParent();
        Set<String> files =
                Arrays.stream(LocalFileIO.create().listStatus(bucketDir))
                        .map(FileStatus::getPath)
                        .map(Path::getName)
                        .collect(Collectors.toSet());
        newFiles.stream().map(DataFileMeta::fileName).forEach(files::remove);
        changelogFiles.stream().map(DataFileMeta::fileName).forEach(files::remove);
        compactedFiles.stream().map(DataFileMeta::fileName).forEach(files::remove);
        assertThat(files).isEqualTo(Collections.emptySet());
    }

    private MergeTreeWriter createMergeTreeWriter(List<DataFileMeta> files) {
        return createMergeTreeWriter(files, createCompactManager(service, files));
    }

    private MergeTreeWriter createMergeTreeWriter(
            List<DataFileMeta> files, MergeTreeCompactManager compactManager) {
        return createMergeTreeWriter(files, compactManager, ChangelogProducer.NONE);
    }

    private MergeTreeWriter createMergeTreeWriter(
            List<DataFileMeta> files,
            MergeTreeCompactManager compactManager,
            ChangelogProducer changelogProducer) {
        long maxSequenceNumber =
                files.stream().map(DataFileMeta::maxSequenceNumber).max(Long::compare).orElse(-1L);
        MergeTreeWriter writer =
                new MergeTreeWriter(
                        false,
                        MemorySize.ofKibiBytes(10),
                        128,
                        CompressOptions.defaultOptions(),
                        null,
                        compactManager,
                        maxSequenceNumber,
                        comparator,
                        DeduplicateMergeFunction.factory().create(),
                        writerFactory.keyType(),
                        writerFactory.valueType(),
                        writerFactory,
                        options.commitForceCompact(),
                        changelogProducer,
                        null,
                        null);
        writer.setMemoryPool(
                new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize()));
        return writer;
    }

    private MergeTreeCompactManager createCompactManager(
            ExecutorService compactExecutor, List<DataFileMeta> files) {
        CompactStrategy strategy =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger());
        return new MergeTreeCompactManager(
                compactExecutor,
                new Levels(comparator, files, options.numLevels()),
                strategy,
                comparator,
                options.compactionFileSize(true),
                options.numSortedRunStopTrigger(),
                new TestRewriter(),
                null,
                null,
                false);
    }

    static class MockFailResultCompactionManager extends MergeTreeCompactManager {
        public MockFailResultCompactionManager(
                ExecutorService executor,
                Levels levels,
                CompactStrategy strategy,
                Comparator<InternalRow> keyComparator,
                long minFileSize,
                int numSortedRunStopTrigger,
                CompactRewriter rewriter) {
            super(
                    executor,
                    levels,
                    strategy,
                    keyComparator,
                    minFileSize,
                    numSortedRunStopTrigger,
                    rewriter,
                    null,
                    null,
                    false);
        }

        protected CompactResult obtainCompactResult()
                throws InterruptedException, ExecutionException {
            if (taskFuture != null && !taskFuture.isDone()) {
                taskFuture.get();
            }
            OutOfMemoryError outOfMemoryError = new OutOfMemoryError();
            throw new ExecutionException("mock", outOfMemoryError);
        }
    }

    private void mergeCompacted(
            Set<String> newFileNames,
            List<DataFileMeta> compactedFiles,
            CommitIncrement increment) {
        increment.newFilesIncrement().newFiles().stream()
                .map(DataFileMeta::fileName)
                .forEach(newFileNames::add);
        compactedFiles.addAll(increment.newFilesIncrement().newFiles());
        Set<String> afterFiles =
                increment.compactIncrement().compactAfter().stream()
                        .map(DataFileMeta::fileName)
                        .collect(Collectors.toSet());
        for (DataFileMeta file : increment.compactIncrement().compactBefore()) {
            boolean remove = compactedFiles.remove(file);
            assertThat(remove).isTrue();
            // See MergeTreeWriter.updateCompactResult
            if (!newFileNames.contains(file.fileName()) && !afterFiles.contains(file.fileName())) {
                compactWriterFactory.deleteFile(file.fileName(), file.level());
            }
        }
        compactedFiles.addAll(increment.compactIncrement().compactAfter());
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
        List<DataFileMeta> files =
                ((MergeTreeCompactManager) writer.compactManager()).levels().allFiles();
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
                MergeTreeReaders.readerForMergeTree(
                        new IntervalPartition(files, comparator).partition(),
                        readerFactory,
                        comparator,
                        null,
                        new ReducerMergeFunctionWrapper(
                                DeduplicateMergeFunction.factory().create()),
                        new MergeSorter(options, null, null, null));
        if (dropDelete) {
            reader = new DropDeleteReader(reader);
        }
        List<TestRecord> records = new ArrayList<>();
        try (RecordReaderIterator<KeyValue> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                records.add(
                        new TestRecord(kv.valueKind(), kv.key().getInt(0), kv.value().getInt(0)));
            }
        }
        return records;
    }

    private InternalRow row(int i) {
        return GenericRow.of(i);
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

    protected abstract SortEngine getSortEngine();

    private class TestRewriter extends AbstractCompactRewriter {

        @Override
        public CompactResult rewrite(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception {
            RollingFileWriter<KeyValue, DataFileMeta> writer =
                    writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);
            RecordReader<KeyValue> reader =
                    MergeTreeReaders.readerForMergeTree(
                            sections,
                            compactReaderFactory,
                            comparator,
                            null,
                            new ReducerMergeFunctionWrapper(
                                    DeduplicateMergeFunction.factory().create()),
                            new MergeSorter(options, null, null, null));
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }
            writer.write(new RecordReaderIterator<>(reader));
            writer.close();
            return new CompactResult(extractFilesFromSections(sections), writer.result());
        }
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

    /** {@link MergeTreeTestBase} with {@link SortEngine#LOSER_TREE}. */
    public static class MergeTreeTestWithLoserTree extends MergeTreeTestBase {

        @Override
        protected SortEngine getSortEngine() {
            return SortEngine.LOSER_TREE;
        }
    }

    /** {@link MergeTreeTestBase} with {@link SortEngine#MIN_HEAP}. */
    public static class MergeTreeTestWithMinHeap extends MergeTreeTestBase {

        @Override
        protected SortEngine getSortEngine() {
            return SortEngine.MIN_HEAP;
        }
    }
}
