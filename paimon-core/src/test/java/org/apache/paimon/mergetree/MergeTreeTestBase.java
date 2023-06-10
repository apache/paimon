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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.compact.AbstractCompactRewriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
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
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
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
    private RecordWriter<KeyValue> writer;
    private SortEngine sortEngine;

    @BeforeEach
    public void beforeEach() throws IOException {
        path = new Path(tempDir.toString());
        pathFactory = new FileStorePathFactory(path);
        comparator = Comparator.comparingInt(o -> o.getInt(0));
        sortEngine = getSortEngine();
        recreateMergeTree(1024 * 1024);
        Path bucketDir = writerFactory.pathFactory().toPath("ignore").getParent();
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
        Options configuration = new Options();
        configuration.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        configuration.set(CoreOptions.PAGE_SIZE, new MemorySize(4096));
        configuration.set(CoreOptions.TARGET_FILE_SIZE, new MemorySize(targetFileSize));
        options = new CoreOptions(configuration);
        RowType keyType = new RowType(singletonList(new DataField(0, "k", new IntType())));
        RowType valueType = new RowType(singletonList(new DataField(0, "v", new IntType())));

        FileFormat flushingAvro = new FlushingFileFormat("avro");
        KeyValueFileReaderFactory.Builder readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        LocalFileIO.create(),
                        createTestingSchemaManager(path),
                        0,
                        keyType,
                        valueType,
                        ignore -> flushingAvro,
                        pathFactory,
                        new KeyValueFieldsExtractor() {
                            @Override
                            public List<DataField> keyFields(TableSchema schema) {
                                return Collections.singletonList(
                                        new DataField(
                                                0,
                                                "k",
                                                new org.apache.paimon.types.IntType(false)));
                            }

                            @Override
                            public List<DataField> valueFields(TableSchema schema) {
                                return Collections.singletonList(
                                        new DataField(
                                                0,
                                                "v",
                                                new org.apache.paimon.types.IntType(false)));
                            }
                        });
        readerFactory = readerFactoryBuilder.build(BinaryRow.EMPTY_ROW, 0);
        compactReaderFactory = readerFactoryBuilder.build(BinaryRow.EMPTY_ROW, 0);
        KeyValueFileWriterFactory.Builder writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        LocalFileIO.create(),
                        0,
                        keyType,
                        valueType,
                        flushingAvro,
                        pathFactory,
                        options.targetFileSize(),
                        null);
        writerFactory =
                writerFactoryBuilder.build(
                        BinaryRow.EMPTY_ROW,
                        0,
                        options.fileCompressionPerLevel(),
                        options.fileCompression());
        compactWriterFactory =
                writerFactoryBuilder.build(
                        BinaryRow.EMPTY_ROW,
                        0,
                        options.fileCompressionPerLevel(),
                        options.fileCompression());
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

        Path bucketDir = writerFactory.pathFactory().toPath("ignore").getParent();
        Set<String> files =
                Arrays.stream(LocalFileIO.create().listStatus(bucketDir))
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
                        false,
                        128,
                        null,
                        createCompactManager(service, files),
                        maxSequenceNumber,
                        comparator,
                        DeduplicateMergeFunction.factory().create(),
                        writerFactory,
                        options.commitForceCompact(),
                        ChangelogProducer.NONE,
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
                        options.numSortedRunCompactionTrigger(),
                        options.maxSortedRunNum());
        return new MergeTreeCompactManager(
                compactExecutor,
                new Levels(comparator, files, options.numLevels()),
                strategy,
                comparator,
                options.targetFileSize(),
                options.numSortedRunStopTrigger(),
                new TestRewriter());
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
                compactWriterFactory.deleteFile(file.fileName());
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
                ((MergeTreeCompactManager) ((MergeTreeWriter) writer).compactManager())
                        .levels()
                        .allFiles();
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
                        dropDelete,
                        readerFactory,
                        comparator,
                        DeduplicateMergeFunction.factory().create(),
                        sortEngine);
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
                    writerFactory.createRollingMergeTreeFileWriter(outputLevel);
            RecordReader<KeyValue> sectionsReader =
                    MergeTreeReaders.readerForMergeTree(
                            sections,
                            dropDelete,
                            compactReaderFactory,
                            comparator,
                            DeduplicateMergeFunction.factory().create(),
                            sortEngine);
            writer.write(new RecordReaderIterator<>(sectionsReader));
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
