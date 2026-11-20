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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.generic.GenericGlobalIndexScanner;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for shard computation in {@link GenericIndexTopoBuilder}. */
class GenericIndexTopoBuilderTest {

    private FileStoreTable table;

    @BeforeEach
    void setUp() {
        table = mock(FileStoreTable.class);
        when(table.partitionKeys()).thenReturn(Collections.singletonList("pt"));

        FileStore<?> store = mock(FileStore.class);
        FileStorePathFactory pathFactory = mock(FileStorePathFactory.class);
        org.mockito.Mockito.doReturn(store).when(table).store();
        when(store.pathFactory()).thenReturn(pathFactory);
        when(pathFactory.bucketPath(any(BinaryRow.class), eq(0)))
                .thenReturn(new Path("/warehouse/table/bucket-0"));
    }

    @Test
    void testSingleFileSingleShard() throws IOException {
        // One file [0, 99] with rowsPerShard=100 → one shard [0, 99]
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 100));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).rowRanges().get(0)).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(0).dataSplit().dataFiles()).hasSize(1);
    }

    @Test
    void testFileSpanningMultipleShards() throws IOException {
        // One file [0, 249] with rowsPerShard=100 → spans shards 0,1,2
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 250));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(3);
        // Shard 0: [0, 99], shard 1: [0, 199] clamped to [100, 199], shard 2: [0, 249] clamped
        assertThat(tasks.get(0).rowRanges().get(0)).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(1).rowRanges().get(0)).isEqualTo(new Range(100, 199));
        assertThat(tasks.get(2).rowRanges().get(0)).isEqualTo(new Range(200, 249));
        // Each shard should contain the same file
        for (IndexedSplit task : tasks) {
            assertThat(task.dataSplit().dataFiles()).hasSize(1);
        }
    }

    @Test
    void testMultipleContiguousFiles() throws IOException {
        // Two contiguous files [0, 49] and [50, 99] in one shard
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 50));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 50L, 50));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).rowRanges().get(0)).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(0).dataSplit().dataFiles()).hasSize(2);
    }

    @Test
    void testFilesWithGapInSameShard() throws IOException {
        // Two files [0, 29] and [70, 99] with a gap in the middle, same shard
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 30));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 70L, 30));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        // Gap produces two separate tasks within the same shard
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).rowRanges().get(0)).isEqualTo(new Range(0, 29));
        assertThat(tasks.get(0).dataSplit().dataFiles()).hasSize(1);
        assertThat(tasks.get(1).rowRanges().get(0)).isEqualTo(new Range(70, 99));
        assertThat(tasks.get(1).dataSplit().dataFiles()).hasSize(1);
    }

    @Test
    void testFileWithNullFirstRowIdSkipped() throws IOException {
        // One file with null firstRowId + one normal file
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, null, 100));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 50));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        // Only the file with valid firstRowId should produce a task
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).dataSplit().dataFiles()).hasSize(1);
        assertThat(tasks.get(0).rowRanges().get(0)).isEqualTo(new Range(0, 49));
    }

    @Test
    void testMultiplePartitions() throws IOException {
        BinaryRow partA = createPartition("a");
        BinaryRow partB = createPartition("b");

        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(partA, 0L, 50));
        entries.add(createEntry(partB, 100L, 50));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(2);
        // Each partition should have its own task
        assertThat(tasks.stream().map(t -> t.dataSplit().partition()).distinct().count())
                .isEqualTo(2);
    }

    @Test
    void testEmptyEntries() throws IOException {
        List<IndexedSplit> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, Collections.emptyList(), 100);

        assertThat(tasks).isEmpty();
    }

    @Test
    void testNoIncrementalScanResult() throws Exception {
        GenericGlobalIndexScanner indexBuilder = mock(GenericGlobalIndexScanner.class);
        when(indexBuilder.withIndex(any(), any(), any())).thenReturn(indexBuilder);
        when(indexBuilder.incrementalScan()).thenReturn(Optional.empty());

        assertThat(
                        GenericIndexTopoBuilder.buildIndex(
                                StreamExecutionEnvironment.getExecutionEnvironment(),
                                () -> indexBuilder,
                                table,
                                "id",
                                "test-index",
                                null,
                                new Options()))
                .isFalse();
    }

    @Test
    void testAllFilesNullRowId() throws IOException {
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, null, 100));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, null, 200));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).isEmpty();
    }

    @Test
    void testMultipleFilesAcrossShardBoundary() throws IOException {
        // File1 [0, 79] in shard 0, File2 [80, 159] spans shard 0 and shard 1
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 80));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 80L, 80));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        // Shard 0: both files (contiguous), range [0, 99]
        // Shard 1: only file2, range [100, 159]
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).rowRanges().get(0)).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(0).dataSplit().dataFiles()).hasSize(2);
        assertThat(tasks.get(1).rowRanges().get(0)).isEqualTo(new Range(100, 159));
        assertThat(tasks.get(1).dataSplit().dataFiles()).hasSize(1);
    }

    @Test
    void testShardRangeClampedToFileRange() throws IOException {
        // File [50, 149] with rowsPerShard=100 → shard 0 [50,99], shard 1 [100,149]
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 50L, 100));

        List<IndexedSplit> tasks = GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(2);
        // Clamped: shard 0 starts at 50 (not 0), shard 1 ends at 149 (not 199)
        assertThat(tasks.get(0).rowRanges().get(0)).isEqualTo(new Range(50, 99));
        assertThat(tasks.get(1).rowRanges().get(0)).isEqualTo(new Range(100, 149));
    }

    @Test
    void testRoundRobinAcrossShards(@TempDir java.nio.file.Path directory) throws Exception {
        Path root = new Path(directory.toUri());
        List<Path> roots = Arrays.asList(new Path(root, "a"), new Path(root, "b"));
        CoreOptions options =
                new CoreOptions(
                        Collections.singletonMap(
                                CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH.key(),
                                roots.get(0) + "," + roots.get(1)));
        FileStorePathFactory paths = table.store().pathFactory();
        when(paths.globalIndexFileFactory())
                .thenAnswer(ignored -> createNonPartFactory(root, roots).globalIndexFileFactory());
        when(table.fileIO()).thenReturn(new LocalFileIO());
        when(table.coreOptions()).thenReturn(options);
        DataField field = new DataField(0, "id", DataTypes.INT());
        RowType readType =
                SpecialFields.rowTypeWithRowId(new RowType(Collections.singletonList(field)));
        ReadBuilder readBuilder = mock(ReadBuilder.class);
        TableRead read = mock(TableRead.class);
        when(readBuilder.newRead()).thenReturn(read);
        List<Committable> commits;
        try (OneInputStreamOperatorTestHarness<IndexedSplit, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new GenericIndexTopoBuilder.BuildIndexOperator(
                                readBuilder,
                                table,
                                "bitmap",
                                field,
                                Collections.emptyList(),
                                readType,
                                options.toConfiguration(),
                                null))) {
            harness.open();
            for (int i = 0; i < 6; i++) {
                IndexedSplit split =
                        GenericIndexTopoBuilder.computeShardTasks(
                                        table,
                                        Collections.singletonList(
                                                createEntry(BinaryRow.EMPTY_ROW, (long) i, 1)),
                                        1)
                                .get(0);
                when(read.createReader(split.dataSplit()))
                        .thenReturn(
                                new IteratorRecordReader<>(
                                        Collections.<InternalRow>singletonList(
                                                        GenericRow.of(i, (long) i))
                                                .iterator()));
                harness.processElement(new StreamRecord<>(split));
            }
            commits = harness.extractOutputValues();
        }
        assertThat(commits).hasSize(6);
        List<Path> parents = new ArrayList<>();
        for (Committable commit : commits) {
            List<IndexFileMeta> files =
                    ((CommitMessageImpl) commit.commitMessage())
                            .newFilesIncrement()
                            .newIndexFiles();
            assertThat(files).hasSize(1);
            Path file = new Path(files.get(0).externalPath());
            assertThat(table.fileIO().exists(file)).isTrue();
            assertThat(files.get(0).fileName()).isEqualTo(file.getName());
            parents.add(file.getParent());
        }
        assertThat(parents).containsOnlyElementsOf(roots);
        for (int i = 1; i < parents.size(); i++) {
            assertThat(parents.get(i)).isNotEqualTo(parents.get(i - 1));
        }
        verify(paths, times(1)).globalIndexFileFactory();
    }

    // -- Helpers --

    private static ManifestEntry createEntry(BinaryRow partition, Long firstRowId, long rowCount) {
        PojoDataFileMeta file =
                new PojoDataFileMeta(
                        "test-file-" + UUID.randomUUID(),
                        1024L,
                        rowCount,
                        BinaryRow.EMPTY_ROW,
                        BinaryRow.EMPTY_ROW,
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        0L,
                        0L,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        firstRowId,
                        null,
                        null);
        return ManifestEntry.create(FileKind.ADD, partition, 0, 1, file);
    }

    private static BinaryRow createPartition(String value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(value));
        writer.complete();
        return row;
    }
}
