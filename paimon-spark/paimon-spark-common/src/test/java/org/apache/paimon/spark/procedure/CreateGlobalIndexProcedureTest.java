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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PojoManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CreateGlobalIndexProcedure}. */
public class CreateGlobalIndexProcedureTest {

    private final BiFunction<BinaryRow, Integer, Path> pathFactory =
            (a, b) -> new Path(UUID.randomUUID().toString());

    @Test
    void testGroupFilesIntoShardsByPartitionSingleFileInSingleShard() {
        // Create a partition
        BinaryRow partition = createPartition(0);

        // Create a single file that fits entirely in one shard (rows 0-99, shard size 1000)
        DataFileMeta file = createDataFileMeta(0L, 100L);
        ManifestEntry entry = createManifestEntry(partition, file);

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new HashMap<>();
        entriesByPartition.put(partition, Collections.singletonList(entry));

        // Execute
        Map<BinaryRow, Map<Range, DataSplit>> result =
                CreateGlobalIndexProcedure.groupFilesIntoShardsByPartition(
                        entriesByPartition, 1000L, pathFactory);

        // Verify
        assertThat(result).hasSize(1);
        assertThat(result).containsKey(partition);

        Map<Range, DataSplit> shardSplits = result.get(partition);
        assertThat(shardSplits).hasSize(1);

        // Should be in shard [0, 999]
        Range expectedRange = new Range(0L, 999L);
        assertThat(shardSplits).containsKey(expectedRange);

        DataSplit split = shardSplits.get(expectedRange);
        assertThat(split.dataFiles()).hasSize(1);
        assertThat(split.dataFiles().get(0)).isEqualTo(file);
    }

    @Test
    void testGroupFilesIntoShardsByPartitionFileSpanningMultipleShards() {
        // Create a partition
        BinaryRow partition = createPartition(0);

        // Create a file that spans 3 shards (rows 500-2500, shard size 1000)
        // This should be in shards [0,999], [1000,1999], and [2000,2999]
        DataFileMeta file = createDataFileMeta(500L, 2001L);
        ManifestEntry entry = createManifestEntry(partition, file);

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new HashMap<>();
        entriesByPartition.put(partition, Collections.singletonList(entry));

        // Execute
        Map<BinaryRow, Map<Range, DataSplit>> result =
                CreateGlobalIndexProcedure.groupFilesIntoShardsByPartition(
                        entriesByPartition, 1000L, pathFactory);

        // Verify
        assertThat(result).hasSize(1);
        Map<Range, DataSplit> shardSplits = result.get(partition);
        assertThat(shardSplits).hasSize(3);

        // Verify all three shards contain the file
        Range shard0 = new Range(0L, 999L);
        Range shard1 = new Range(1000L, 1999L);
        Range shard2 = new Range(2000L, 2999L);

        assertThat(shardSplits).containsKeys(shard0, shard1, shard2);
        assertThat(shardSplits.get(shard0).dataFiles()).contains(file);
        assertThat(shardSplits.get(shard1).dataFiles()).contains(file);
        assertThat(shardSplits.get(shard2).dataFiles()).contains(file);
    }

    @Test
    void testGroupFilesIntoShardsByPartitionMultipleFilesInSameShard() {
        // Create a partition
        BinaryRow partition = createPartition(0);

        // Create multiple files in the same shard
        DataFileMeta file1 = createDataFileMeta(0L, 100L);
        DataFileMeta file2 = createDataFileMeta(100L, 100L);
        DataFileMeta file3 = createDataFileMeta(200L, 100L);

        List<ManifestEntry> entries =
                Arrays.asList(
                        createManifestEntry(partition, file1),
                        createManifestEntry(partition, file2),
                        createManifestEntry(partition, file3));

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new HashMap<>();
        entriesByPartition.put(partition, entries);

        // Execute
        Map<BinaryRow, Map<Range, DataSplit>> result =
                CreateGlobalIndexProcedure.groupFilesIntoShardsByPartition(
                        entriesByPartition, 1000L, pathFactory);

        // Verify
        assertThat(result).hasSize(1);
        Map<Range, DataSplit> shardSplits = result.get(partition);
        assertThat(shardSplits).hasSize(1);

        Range expectedRange = new Range(0L, 999L);
        DataSplit split = shardSplits.get(expectedRange);
        assertThat(split.dataFiles()).hasSize(3);
        assertThat(split.dataFiles()).containsExactlyInAnyOrder(file1, file2, file3);
    }

    @Test
    void testGroupFilesIntoShardsByPartitionMultipleFilesInDifferentShards() {
        // Create a partition
        BinaryRow partition = createPartition(0);

        // Create files in different shards
        DataFileMeta file1 = createDataFileMeta(0L, 100L); // Shard [0, 999]
        DataFileMeta file2 = createDataFileMeta(1000L, 100L); // Shard [1000, 1999]
        DataFileMeta file3 = createDataFileMeta(2000L, 100L); // Shard [2000, 2999]

        List<ManifestEntry> entries =
                Arrays.asList(
                        createManifestEntry(partition, file1),
                        createManifestEntry(partition, file2),
                        createManifestEntry(partition, file3));

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new HashMap<>();
        entriesByPartition.put(partition, entries);

        // Execute
        Map<BinaryRow, Map<Range, DataSplit>> result =
                CreateGlobalIndexProcedure.groupFilesIntoShardsByPartition(
                        entriesByPartition, 1000L, pathFactory);

        // Verify
        assertThat(result).hasSize(1);
        Map<Range, DataSplit> shardSplits = result.get(partition);
        assertThat(shardSplits).hasSize(3);

        // Verify each shard has the correct file
        Range shard0 = new Range(0L, 999L);
        Range shard1 = new Range(1000L, 1999L);
        Range shard2 = new Range(2000L, 2999L);

        assertThat(shardSplits.get(shard0).dataFiles()).containsExactly(file1);
        assertThat(shardSplits.get(shard1).dataFiles()).containsExactly(file2);
        assertThat(shardSplits.get(shard2).dataFiles()).containsExactly(file3);
    }

    @Test
    void testGroupFilesIntoShardsByPartitionMultiplePartitions() {
        // Create two partitions
        BinaryRow partition1 = createPartition(0);
        BinaryRow partition2 = createPartition(1);

        // Create files for each partition
        DataFileMeta file1 = createDataFileMeta(0L, 1050L);
        DataFileMeta file2 = createDataFileMeta(1050L, 1000L);

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new HashMap<>();
        entriesByPartition.put(
                partition1, Collections.singletonList(createManifestEntry(partition1, file1)));
        entriesByPartition.put(
                partition2, Collections.singletonList(createManifestEntry(partition2, file2)));

        // Execute
        Map<BinaryRow, Map<Range, DataSplit>> result =
                CreateGlobalIndexProcedure.groupFilesIntoShardsByPartition(
                        entriesByPartition, 100L, pathFactory);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result).containsKeys(partition1, partition2);

        // Verify partition1
        Map<Range, DataSplit> shardSplits1 = result.get(partition1);
        assertThat(shardSplits1).hasSize(11);
        assertThat(shardSplits1).containsKey(new Range(1000, 1099));
        assertThat(shardSplits1.get(new Range(1000L, 1099L)).dataFiles()).containsExactly(file1);

        // Verify partition2
        Map<Range, DataSplit> shardSplits2 = result.get(partition2);
        assertThat(shardSplits2).hasSize(11);
        assertThat(shardSplits1).containsKey(new Range(1000, 1099));
        assertThat(shardSplits2.get(new Range(1000L, 1099L)).dataFiles()).containsExactly(file2);
    }

    @Test
    void testGroupFilesIntoShardsByPartitionExactShardBoundaries() {
        // Create a partition
        BinaryRow partition = createPartition(0);

        // Create a file that ends exactly at shard boundary (rows 0-999, shard size 1000)
        DataFileMeta file = createDataFileMeta(0L, 1000L);
        ManifestEntry entry = createManifestEntry(partition, file);

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new HashMap<>();
        entriesByPartition.put(partition, Collections.singletonList(entry));

        // Execute
        Map<BinaryRow, Map<Range, DataSplit>> result =
                CreateGlobalIndexProcedure.groupFilesIntoShardsByPartition(
                        entriesByPartition, 1000L, pathFactory);

        // Verify - file ending at row 999 should be in shard [0,999] only
        // File covers rows [0, 999]
        assertThat(result).hasSize(1);
        Map<Range, DataSplit> shardSplits = result.get(partition);
        assertThat(shardSplits).hasSize(1);
        assertThat(shardSplits).containsKey(new Range(0L, 999L));
    }

    @Test
    void testGroupFilesIntoShardsByPartitionSmallShardSize() {
        // Create a partition
        BinaryRow partition = createPartition(0);

        // Create a file with small shard size (rows 0-24, shard size 10)
        DataFileMeta file = createDataFileMeta(0L, 25L);
        ManifestEntry entry = createManifestEntry(partition, file);

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new HashMap<>();
        entriesByPartition.put(partition, Collections.singletonList(entry));

        // Execute with shard size of 10
        Map<BinaryRow, Map<Range, DataSplit>> result =
                CreateGlobalIndexProcedure.groupFilesIntoShardsByPartition(
                        entriesByPartition, 10L, pathFactory);

        // Verify - file should span 3 shards: [0,9], [10,19], [20,29]
        assertThat(result).hasSize(1);
        Map<Range, DataSplit> shardSplits = result.get(partition);
        assertThat(shardSplits).hasSize(3);

        assertThat(shardSplits)
                .containsKeys(new Range(0L, 9L), new Range(10L, 19L), new Range(20L, 29L));
    }

    private BinaryRow createPartition(int i) {
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);
        binaryRowWriter.writeInt(0, i);
        binaryRowWriter.complete();
        return binaryRow;
    }

    private DataFileMeta createDataFileMeta(Long firstRowId, Long rowCount) {
        return new PojoDataFileMeta(
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
    }

    private ManifestEntry createManifestEntry(BinaryRow partition, DataFileMeta file) {
        return new PojoManifestEntry(
                FileKind.ADD,
                partition,
                0, // bucket
                1, // totalBuckets
                file);
    }
}
