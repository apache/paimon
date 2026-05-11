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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MergeTreeCompactManager}. */
public class MergeTreeCompactManagerTest {

    private final Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));

    private static ExecutorService service;

    @TempDir java.nio.file.Path tempDir;

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
    public void testOutputToZeroLevel() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 1, 8)),
                Arrays.asList(new LevelMinMax(0, 1, 8), new LevelMinMax(0, 1, 3)),
                (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(0, runs.subList(0, 2))),
                false);
    }

    @Test
    public void testCompactToPenultimateLayer() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(2, 1, 7)),
                Arrays.asList(new LevelMinMax(1, 1, 5), new LevelMinMax(2, 1, 7)),
                (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(1, runs.subList(0, 2))),
                false);
    }

    @Test
    public void testNoCompaction() throws ExecutionException, InterruptedException {
        innerTest(
                Collections.singletonList(new LevelMinMax(3, 1, 3)),
                Collections.singletonList(new LevelMinMax(3, 1, 3)));
    }

    @Test
    public void testNormal() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(1, 1, 5),
                        new LevelMinMax(1, 6, 7)),
                Arrays.asList(new LevelMinMax(2, 1, 5), new LevelMinMax(2, 6, 7)));
    }

    @Test
    public void testUpgrade() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 8)),
                Arrays.asList(new LevelMinMax(2, 1, 5), new LevelMinMax(2, 6, 8)));
    }

    @Test
    public void testSmallFiles() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(new LevelMinMax(0, 1, 1), new LevelMinMax(0, 2, 2)),
                Collections.singletonList(new LevelMinMax(2, 1, 2)));
    }

    @Test
    public void testSmallFilesNoCompact() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 7, 8),
                        new LevelMinMax(1, 9, 10)),
                Arrays.asList(
                        new LevelMinMax(2, 1, 5),
                        new LevelMinMax(2, 6, 6),
                        new LevelMinMax(2, 7, 8),
                        new LevelMinMax(2, 9, 10)));
    }

    @Test
    public void testSmallFilesCrossLevel() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 7, 7),
                        new LevelMinMax(1, 9, 10)),
                Arrays.asList(
                        new LevelMinMax(2, 1, 5),
                        new LevelMinMax(2, 6, 7),
                        new LevelMinMax(2, 9, 10)));
    }

    @Test
    public void testComplex() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 11),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6)),
                Arrays.asList(new LevelMinMax(2, 1, 8), new LevelMinMax(2, 10, 11)));
    }

    @Test
    public void testSmallInComplex() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 10),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6)),
                Collections.singletonList(new LevelMinMax(2, 1, 10)));
    }

    @Test
    public void testIsCompacting() {
        List<LevelMinMax> inputs =
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(1, 1, 5),
                        new LevelMinMax(1, 6, 7));
        List<DataFileMeta> files = new ArrayList<>();

        for (int i = 0; i < inputs.size(); i++) {
            LevelMinMax minMax = inputs.get(i);
            files.add(minMax.toFile(i));
        }

        Levels levels = new Levels(comparator, files, 3);

        MergeTreeCompactManager lookupManager =
                new MergeTreeCompactManager(
                        service,
                        levels,
                        testStrategy(),
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(true),
                        null,
                        null,
                        false,
                        true,
                        null,
                        false,
                        false);

        MergeTreeCompactManager defaultManager =
                new MergeTreeCompactManager(
                        service,
                        levels,
                        testStrategy(),
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(true),
                        null,
                        null,
                        false,
                        false,
                        null,
                        false,
                        false);

        assertThat(lookupManager.compactNotCompleted()).isTrue();
        assertThat(defaultManager.compactNotCompleted()).isFalse();
    }

    @Test
    public void testCompactWithKeepDelete() throws Exception {
        // 1) Build a minimal TestFileStore with primary key and one bucket.
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(new LocalFileIO(), new Path(tempDir.toUri())),
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                Collections.emptyMap(),
                                null));
        TestFileStore store =
                new TestFileStore.Builder(
                                "avro",
                                tempDir.toString(),
                                1,
                                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                                TestKeyValueGenerator.KEY_TYPE,
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                                TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                                DeduplicateMergeFunction.factory(),
                                tableSchema)
                        .build();

        // 2) Write INSERT + DELETE for the same key into level-0 files.
        int shopId = 1;
        long orderId = 42L;
        String dt = "20240101";
        int hr = 0;

        GenericRow keyRow = GenericRow.of(shopId, orderId);
        GenericRow fullRow =
                GenericRow.of(
                        BinaryString.fromString(dt),
                        hr,
                        shopId,
                        orderId,
                        100L,
                        null,
                        BinaryString.fromString("comment"));

        BinaryRow keyBinaryRow = TestKeyValueGenerator.KEY_SERIALIZER.toBinaryRow(keyRow).copy();
        BinaryRow valueBinaryRow =
                TestKeyValueGenerator.DEFAULT_ROW_SERIALIZER.toBinaryRow(fullRow).copy();

        InternalRowSerializer partitionSerializer =
                new InternalRowSerializer(TestKeyValueGenerator.DEFAULT_PART_TYPE);
        BinaryRow partition =
                partitionSerializer
                        .toBinaryRow(GenericRow.of(BinaryString.fromString(dt), hr))
                        .copy();

        KeyValue insert = new KeyValue().replace(keyBinaryRow, 0L, RowKind.INSERT, valueBinaryRow);
        KeyValue delete = new KeyValue().replace(keyBinaryRow, 1L, RowKind.DELETE, valueBinaryRow);

        List<KeyValue> kvs = Arrays.asList(insert, delete);

        List<Snapshot> snapshots =
                store.commitData(kvs, kv -> partition, kv -> 0 /* single bucket */);
        Snapshot snapshot = snapshots.get(snapshots.size() - 1);
        long snapshotId = snapshot.id();

        // Collect input files from manifest entries of this snapshot.
        FileStoreScan scan = store.newScan();
        List<ManifestEntry> manifestEntries = scan.withSnapshot(snapshotId).plan().files();

        List<DataFileMeta> inputFiles =
                manifestEntries.stream()
                        .filter(e -> e.kind() == FileKind.ADD)
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        assertThat(inputFiles).isNotEmpty();

        // 3) Create a MergeTreeCompactManager with keepDelete=true and run compaction.
        Comparator<InternalRow> keyComparator =
                Comparator.<InternalRow>comparingInt(r -> r.getInt(0))
                        .thenComparingLong(r -> r.getLong(1));

        CoreOptions coreOptions = store.options();
        Levels levels = new Levels(keyComparator, inputFiles, coreOptions.numLevels());

        // Always compact all runs into the highest level.
        CompactStrategy strategy =
                (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(numLevels - 1, runs));

        KeyValueFileReaderFactory.Builder readerFactoryBuilder = store.newReaderFactoryBuilder();
        KeyValueFileReaderFactory keyReaderFactory =
                readerFactoryBuilder.build(partition, 0, DeletionVector.emptyFactory());
        FileReaderFactory<KeyValue> readerFactory = keyReaderFactory;

        KeyValueFileWriterFactory.Builder writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        store.fileIO(),
                        snapshot.schemaId(),
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        fileFormat(coreOptions),
                        ignore -> store.pathFactory(),
                        coreOptions.targetFileSize(true));
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, 0, coreOptions);

        MergeSorter mergeSorter =
                new MergeSorter(
                        coreOptions,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        null);

        MergeTreeCompactRewriter rewriter =
                new MergeTreeCompactRewriter(
                        readerFactory,
                        writerFactory,
                        keyComparator,
                        null,
                        DeduplicateMergeFunction.factory(),
                        mergeSorter);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        MergeTreeCompactManager manager =
                new MergeTreeCompactManager(
                        executor,
                        levels,
                        strategy,
                        keyComparator,
                        coreOptions.compactionFileSize(true),
                        coreOptions.numSortedRunStopTrigger(),
                        rewriter,
                        null,
                        null,
                        false,
                        false,
                        null,
                        false,
                        true); // keepDelete=true

        try {
            manager.triggerCompaction(false);
            Optional<CompactResult> resultOptional = manager.getCompactionResult(true);
            assertThat(resultOptional).isPresent();
            CompactResult compactResult = resultOptional.get();

            List<DataFileMeta> compactedFiles = compactResult.after();
            assertThat(compactedFiles).isNotEmpty();

            int highestLevelBefore =
                    inputFiles.stream().mapToInt(DataFileMeta::level).max().orElse(0);
            for (DataFileMeta file : compactedFiles) {
                assertThat(file.level()).isNotZero();
                assertThat(file.level()).isGreaterThanOrEqualTo(highestLevelBefore);
            }

            // 4) Read back from compacted files (via manifest entries) without DropDeleteReader
            // and assert a DELETE record exists.
            int totalBuckets = coreOptions.bucket();
            int bucket = 0;
            List<ManifestEntry> compactedEntries =
                    compactedFiles.stream()
                            .map(
                                    file ->
                                            ManifestEntry.create(
                                                    FileKind.ADD,
                                                    partition,
                                                    bucket,
                                                    totalBuckets,
                                                    file))
                            .collect(Collectors.toList());

            SplitRead<KeyValue> read = store.newRead().forceKeepDelete();

            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> filesPerPartitionAndBucket =
                    new HashMap<>();
            for (ManifestEntry entry : compactedEntries) {
                filesPerPartitionAndBucket
                        .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                        .computeIfAbsent(entry.bucket(), b -> new ArrayList<>())
                        .add(entry.file());
            }

            List<KeyValue> readBack = new ArrayList<>();
            for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry :
                    filesPerPartitionAndBucket.entrySet()) {
                BinaryRow part = entry.getKey();
                for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                        entry.getValue().entrySet()) {
                    RecordReader<KeyValue> reader =
                            read.createReader(
                                    DataSplit.builder()
                                            .withPartition(part)
                                            .withBucket(bucketEntry.getKey())
                                            .withDataFiles(bucketEntry.getValue())
                                            .isStreaming(false)
                                            .rawConvertible(false)
                                            .withBucketPath("not used")
                                            .build());
                    RecordReaderIterator<KeyValue> iterator = new RecordReaderIterator<>(reader);
                    try {
                        while (iterator.hasNext()) {
                            readBack.add(
                                    iterator.next()
                                            .copy(
                                                    TestKeyValueGenerator.KEY_SERIALIZER,
                                                    TestKeyValueGenerator.DEFAULT_ROW_SERIALIZER));
                        }
                    } finally {
                        iterator.close();
                    }
                }
            }

            List<KeyValue> recordsForKey =
                    readBack.stream()
                            .filter(
                                    kv ->
                                            kv.key().getInt(0) == shopId
                                                    && kv.key().getLong(1) == orderId)
                            .collect(Collectors.toList());

            assertThat(recordsForKey).isNotEmpty();
            assertThat(recordsForKey).allMatch(kv -> kv.valueKind() == RowKind.DELETE);
        } finally {
            manager.close();
            executor.shutdownNow();
        }
    }

    private void innerTest(List<LevelMinMax> inputs, List<LevelMinMax> expected)
            throws ExecutionException, InterruptedException {
        innerTest(inputs, expected, testStrategy(), true);
    }

    private void innerTest(
            List<LevelMinMax> inputs,
            List<LevelMinMax> expected,
            CompactStrategy strategy,
            boolean expectedDropDelete)
            throws ExecutionException, InterruptedException {
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            LevelMinMax minMax = inputs.get(i);
            files.add(minMax.toFile(i));
        }
        Levels levels = new Levels(comparator, files, 3);
        MergeTreeCompactManager manager =
                new MergeTreeCompactManager(
                        service,
                        levels,
                        strategy,
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(expectedDropDelete),
                        null,
                        null,
                        false,
                        false,
                        null,
                        false,
                        false);
        manager.triggerCompaction(false);
        manager.getCompactionResult(true);
        List<LevelMinMax> outputs =
                levels.allFiles().stream().map(LevelMinMax::new).collect(Collectors.toList());
        assertThat(outputs).isEqualTo(expected);
    }

    public static BinaryRow row(int i) {
        return DataFileTestUtils.row(i);
    }

    private CompactStrategy testStrategy() {
        return (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(numLevels - 1, runs));
    }

    private static class TestRewriter extends AbstractCompactRewriter {

        private final boolean expectedDropDelete;

        private TestRewriter(boolean expectedDropDelete) {
            this.expectedDropDelete = expectedDropDelete;
        }

        @Override
        public CompactResult rewrite(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception {
            assertThat(dropDelete).isEqualTo(expectedDropDelete);
            int minKey = Integer.MAX_VALUE;
            int maxKey = Integer.MIN_VALUE;
            long maxSequence = 0;
            for (List<SortedRun> section : sections) {
                for (SortedRun run : section) {
                    for (DataFileMeta file : run.files()) {
                        int min = file.minKey().getInt(0);
                        int max = file.maxKey().getInt(0);
                        if (min < minKey) {
                            minKey = min;
                        }
                        if (max > maxKey) {
                            maxKey = max;
                        }
                        if (file.maxSequenceNumber() > maxSequence) {
                            maxSequence = file.maxSequenceNumber();
                        }
                    }
                }
            }
            return new CompactResult(
                    extractFilesFromSections(sections),
                    Collections.singletonList(newFile(outputLevel, minKey, maxKey, maxSequence)));
        }
    }

    private static class LevelMinMax {

        private final int level;
        private final int min;
        private final int max;

        private LevelMinMax(DataFileMeta file) {
            this.level = file.level();
            this.min = file.minKey().getInt(0);
            this.max = file.maxKey().getInt(0);
        }

        private LevelMinMax(int level, int min, int max) {
            this.level = level;
            this.min = min;
            this.max = max;
        }

        private DataFileMeta toFile(long maxSequence) {
            return newFile(level, min, max, maxSequence);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LevelMinMax that = (LevelMinMax) o;
            return level == that.level && min == that.min && max == that.max;
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, min, max);
        }

        @Override
        public String toString() {
            return "LevelMinMax{" + "level=" + level + ", min=" + min + ", max=" + max + '}';
        }
    }
}
