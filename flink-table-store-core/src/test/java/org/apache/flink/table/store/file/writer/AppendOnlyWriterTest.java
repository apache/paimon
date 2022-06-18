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

package org.apache.flink.table.store.file.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.format.FlushingFileFormat;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.Levels;
import org.apache.flink.table.store.file.mergetree.MemTable;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.MergeTreeTest;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriter;
import org.apache.flink.table.store.file.mergetree.SortBufferMemTable;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
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
    private FileStorePathFactory fileStorePathFactory;
    
    
    private static final String AVRO = "avro";
    private static final String PART = "2022-05-01";
    private MergeTreeOptions options;
    private DataFileReader dataFileReader;
    private DataFileWriter dataFileWriter;
    private Comparator<RowData> comparator;
    private RecordWriter writer;
    private static ExecutorService service;
    
    
    @BeforeEach
    public void beforeEach() {
        pathFactory = createPathFactory();
        fileStorePathFactory = new FileStorePathFactory(new Path(tempDir.toString()));
        comparator = Comparator.comparingInt(o -> o.getInt(0));
    
    
        Configuration configuration = new Configuration();
        configuration.set(MergeTreeOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        configuration.set(MergeTreeOptions.PAGE_SIZE, new MemorySize(4096));
        options = new MergeTreeOptions(configuration);
        RowType keyType = new RowType(singletonList(new RowType.RowField("k", new IntType())));
        RowType valueType = new RowType(singletonList(new RowType.RowField("v", new IntType())));
        FileFormat flushingAvro = new FlushingFileFormat("avro");
        dataFileWriter =
            new DataFileWriter.Factory(
                keyType,
                valueType,
                flushingAvro,
                fileStorePathFactory,
                options.targetFileSize)
                .create(BinaryRowDataUtil.EMPTY_ROW, 0);
    }
    
    
    @BeforeAll
    public static void before() {
        service = Executors.newSingleThreadExecutor();
    
    }

    @Test
    public void testEmptyCommits() throws Exception {
        RecordWriter writer = createAppendOnlyWriter(1024 * 1024L, SCHEMA, 0, Collections.emptyList());

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
        RecordWriter writer = createAppendOnlyWriter(1024 * 1024L, SCHEMA, 0, Collections.emptyList());
        writer.write(ValueKind.ADD, EMPTY_ROW, row(1, "AAA", PART));

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
        RecordWriter writer = createAppendOnlyWriter(1024 * 1024L, SCHEMA, 0, Collections.emptyList());

        // Commit 5 continues txn.
        for (int txn = 0; txn < 5; txn += 1) {

            // Write the records with range [ txn*100, (txn+1)*100 ).
            int start = txn * 100;
            int end = txn * 100 + 100;
            for (int i = start; i < end; i++) {
                writer.write(ValueKind.ADD, EMPTY_ROW, row(i, String.format("%03d", i), PART));
            }

            writer.sync();
            Increment inc = writer.prepareCommit();
            assertThat(inc.compactBefore()).isEqualTo(Collections.emptyList());
            assertThat(inc.compactAfter()).isEqualTo(Collections.emptyList());

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
        RecordWriter writer = createAppendOnlyWriter(10L, SCHEMA, 0, Collections.emptyList());

        for (int i = 0; i < 10; i++) {
            writer.write(ValueKind.ADD, EMPTY_ROW, row(i, String.format("%03d", i), PART));
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
    }
    
    @Test
    public void testRestore() throws Exception {
        List<TestRecord> expected = new ArrayList<>(writeBatch());
        List<DataFileMeta> newFiles = writer.prepareCommit().newFiles();
        writer = createAppendOnlyWriter(1024 * 1024L, SCHEMA, 0, newFiles);
        expected.addAll(writeBatch());
        writer.prepareCommit();
        writer.sync();
        assertRecords(expected);
    }

    private FieldStats initStats(Integer min, Integer max, long nullCount) {
        return new FieldStats(min, max, nullCount);
    }

    private FieldStats initStats(String min, String max, long nullCount) {
        return new FieldStats(StringData.fromString(min), StringData.fromString(max), nullCount);
    }
    
    private void assertRecords(List<TestRecord> expected) throws Exception {
        // compaction will drop delete
        List<DataFileMeta> files = ((AppendOnlyWriter) writer).levels().allFiles();
        assertRecords(expected, files, true);
    }
    
    private void assertRecords(
        List<TestRecord> expected, List<DataFileMeta> files, boolean dropDelete)
        throws Exception {
        assertThat(readAll(files, dropDelete)).isEqualTo(compactAndSort(expected, dropDelete));
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
    
    
    private RowData row(int id, String name, String dt) {
        return GenericRowData.of(id, StringData.fromString(name), StringData.fromString(dt));
    }
    
    private RowData row(int i) {
        return GenericRowData.of(i);
    }

    private DataFilePathFactory createPathFactory() {
        return new DataFilePathFactory(
                new Path(tempDir.toString()),
                "dt=" + PART,
                1,
                FileStoreOptions.FILE_FORMAT.defaultValue());
    }

    private RecordWriter createAppendOnlyWriter(long targetFileSize, RowType writeSchema, long maxSeqNum,List<DataFileMeta> files) {
        FileFormat fileFormat =
                FileFormat.fromIdentifier(
                        Thread.currentThread().getContextClassLoader(), AVRO, new Configuration());
    
        long maxSequenceNumber =
            files.stream().map(DataFileMeta::maxSequenceNumber).max(Long::compare).orElse(-1L);
        
            
        return new AppendOnlyWriter(
                fileFormat, targetFileSize, writeSchema, maxSeqNum, pathFactory,fileStorePathFactory,
            createCompactManager(dataFileWriter, service),
            new Levels(comparator, files, options.numLevels),
            dataFileWriter,
            new SortBufferMemTable(
                dataFileWriter.keyType(),
                dataFileWriter.valueType(),
                options.writeBufferSize,
                options.pageSize),comparator,options.commitForceCompact,options.numSortedRunStopTrigger,new DeduplicateMergeFunction(), true);
    }
    
    private CompactManager createCompactManager(
        DataFileWriter dataFileWriter, ExecutorService compactExecutor) {
        CompactStrategy compactStrategy =
            new UniversalCompaction(
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
    
    private void writeAll(List<TestRecord> records) throws Exception {
        for (TestRecord record : records) {
            writer.write(record.kind, row(record.k), row(record.v));
        }
    }
    
    private List<TestRecord>  writeBatch() throws Exception {
        return writeBatch(200);
    }
    
    private List<TestRecord> writeBatch(int perBatch) throws Exception {
        List<TestRecord> records = generateRandom(perBatch);
        writeAll(records);
        return records;
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
    
}
