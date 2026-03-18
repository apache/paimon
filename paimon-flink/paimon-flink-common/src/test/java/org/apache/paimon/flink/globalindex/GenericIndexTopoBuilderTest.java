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

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).shardRange).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(0).split.dataFiles()).hasSize(1);
    }

    @Test
    void testFileSpanningMultipleShards() throws IOException {
        // One file [0, 249] with rowsPerShard=100 → spans shards 0,1,2
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 250));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(3);
        // Shard 0: [0, 99], shard 1: [0, 199] clamped to [100, 199], shard 2: [0, 249] clamped
        assertThat(tasks.get(0).shardRange).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(1).shardRange).isEqualTo(new Range(100, 199));
        assertThat(tasks.get(2).shardRange).isEqualTo(new Range(200, 249));
        // Each shard should contain the same file
        for (GenericIndexTopoBuilder.ShardTask task : tasks) {
            assertThat(task.split.dataFiles()).hasSize(1);
        }
    }

    @Test
    void testMultipleContiguousFiles() throws IOException {
        // Two contiguous files [0, 49] and [50, 99] in one shard
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 50));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 50L, 50));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).shardRange).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(0).split.dataFiles()).hasSize(2);
    }

    @Test
    void testFilesWithGapInSameShard() throws IOException {
        // Two files [0, 29] and [70, 99] with a gap in the middle, same shard
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 30));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 70L, 30));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        // Gap produces two separate tasks within the same shard
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).shardRange).isEqualTo(new Range(0, 29));
        assertThat(tasks.get(0).split.dataFiles()).hasSize(1);
        assertThat(tasks.get(1).shardRange).isEqualTo(new Range(70, 99));
        assertThat(tasks.get(1).split.dataFiles()).hasSize(1);
    }

    @Test
    void testFileWithNullFirstRowIdSkipped() throws IOException {
        // One file with null firstRowId + one normal file
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, null, 100));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 50));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        // Only the file with valid firstRowId should produce a task
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).split.dataFiles()).hasSize(1);
        assertThat(tasks.get(0).shardRange).isEqualTo(new Range(0, 49));
    }

    @Test
    void testMultiplePartitions() throws IOException {
        BinaryRow partA = createPartition("a");
        BinaryRow partB = createPartition("b");

        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(partA, 0L, 50));
        entries.add(createEntry(partB, 100L, 50));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(2);
        // Each partition should have its own task
        assertThat(tasks.stream().map(t -> t.split.partition()).distinct().count()).isEqualTo(2);
    }

    @Test
    void testEmptyEntries() throws IOException {
        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, Collections.emptyList(), 100);

        assertThat(tasks).isEmpty();
    }

    @Test
    void testAllFilesNullRowId() throws IOException {
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, null, 100));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, null, 200));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).isEmpty();
    }

    @Test
    void testMultipleFilesAcrossShardBoundary() throws IOException {
        // File1 [0, 79] in shard 0, File2 [80, 159] spans shard 0 and shard 1
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 0L, 80));
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 80L, 80));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        // Shard 0: both files (contiguous), range [0, 99]
        // Shard 1: only file2, range [100, 159]
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).shardRange).isEqualTo(new Range(0, 99));
        assertThat(tasks.get(0).split.dataFiles()).hasSize(2);
        assertThat(tasks.get(1).shardRange).isEqualTo(new Range(100, 159));
        assertThat(tasks.get(1).split.dataFiles()).hasSize(1);
    }

    @Test
    void testShardRangeClampedToFileRange() throws IOException {
        // File [50, 149] with rowsPerShard=100 → shard 0 [50,99], shard 1 [100,149]
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(createEntry(BinaryRow.EMPTY_ROW, 50L, 100));

        List<GenericIndexTopoBuilder.ShardTask> tasks =
                GenericIndexTopoBuilder.computeShardTasks(table, entries, 100);

        assertThat(tasks).hasSize(2);
        // Clamped: shard 0 starts at 50 (not 0), shard 1 ends at 149 (not 199)
        assertThat(tasks.get(0).shardRange).isEqualTo(new Range(50, 99));
        assertThat(tasks.get(1).shardRange).isEqualTo(new Range(100, 149));
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
