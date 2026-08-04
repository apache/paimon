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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PostponeUtils}. */
public class PostponeUtilsTest {

    @Test
    public void testGetKnownNumBucketsFromSnapshot() {
        BinaryRow partition = partition(1);
        PartitionPredicate partitionFilter = mock(PartitionPredicate.class);
        SimpleFileEntry entry = mock(SimpleFileEntry.class);
        when(entry.partition()).thenReturn(partition);
        when(entry.totalBuckets()).thenReturn(4);

        FileStoreScan scan = mock(FileStoreScan.class, RETURNS_SELF);
        when(scan.readSimpleEntries()).thenReturn(Collections.singletonList(entry));
        FileStore store = mock(FileStore.class);
        when(store.newScan()).thenReturn(scan);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.store()).thenReturn(store);

        assertThat(PostponeUtils.getKnownNumBuckets(table, 5L, partitionFilter))
                .containsEntry(partition, 4);
        verify(scan).withSnapshot(5L);
        verify(scan).onlyReadRealBuckets();
        verify(scan).withPartitionFilter(partitionFilter);
    }

    @Test
    public void testGetKnownNumBucketsByPartitions() {
        BinaryRow partition = partition(1);
        List<BinaryRow> partitions = Collections.singletonList(partition);
        SimpleFileEntry entry = mock(SimpleFileEntry.class);
        when(entry.partition()).thenReturn(partition);
        when(entry.totalBuckets()).thenReturn(4);

        FileStoreScan scan = mock(FileStoreScan.class, RETURNS_SELF);
        when(scan.readSimpleEntries()).thenReturn(Collections.singletonList(entry));
        FileStore store = mock(FileStore.class);
        when(store.newScan()).thenReturn(scan);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.store()).thenReturn(store);

        assertThat(PostponeUtils.getKnownNumBuckets(table, 5L, partitions))
                .containsEntry(partition, 4);
        verify(scan).withSnapshot(5L);
        verify(scan).onlyReadRealBuckets();
        verify(scan).withPartitionFilter(partitions);
    }

    @Test
    public void testGetPostponeRowCountsFromSnapshot() {
        BinaryRow partition = partition(1);
        PartitionPredicate partitionFilter = mock(PartitionPredicate.class);
        DataFileMeta file = mock(DataFileMeta.class);
        when(file.rowCount()).thenReturn(10L);
        ManifestEntry entry = mock(ManifestEntry.class);
        when(entry.partition()).thenReturn(partition);
        when(entry.file()).thenReturn(file);

        SnapshotReader reader = mock(SnapshotReader.class, RETURNS_SELF);
        when(reader.readFileIterator()).thenReturn(Collections.singletonList(entry).iterator());
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.newSnapshotReader()).thenReturn(reader);

        assertThat(PostponeUtils.getPostponeRowCounts(table, 5L, partitionFilter))
                .containsEntry(partition, 10L);
        verify(reader).withSnapshot(5L);
        verify(reader).withBucket(BucketMode.POSTPONE_BUCKET);
        verify(reader).withPartitionFilter(partitionFilter);
    }

    @Test
    public void testGetLevel0BucketsFromSnapshot() {
        BinaryRow partition = partition(1);
        SimpleFileEntry level0 = fileEntry(partition, 0, 2, 0);
        SimpleFileEntry duplicate = fileEntry(partition, 0, 2, 0);
        SimpleFileEntry compacted = fileEntry(partition, 1, 2, 1);
        SimpleFileEntry postpone = fileEntry(partition, -2, -2, 0);

        FileStoreScan scan = mock(FileStoreScan.class, RETURNS_SELF);
        when(scan.readSimpleEntries())
                .thenReturn(Arrays.asList(level0, duplicate, compacted, postpone));
        FileStore store = mock(FileStore.class);
        when(store.newScan()).thenReturn(scan);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.store()).thenReturn(store);

        List<PostponeUtils.CompactBucket> buckets = PostponeUtils.getLevel0Buckets(table, 5L);

        assertThat(buckets).hasSize(1);
        assertThat(buckets.get(0).partition()).isEqualTo(partition);
        assertThat(buckets.get(0).bucket()).isEqualTo(0);
        assertThat(buckets.get(0).totalBuckets()).isEqualTo(2);
        verify(scan).withSnapshot(5L);
        verify(scan).onlyReadRealBuckets();
    }

    @Test
    public void testGroupPostponeFilesByPartitionAndWriter() {
        BinaryRow partition1 = partition(1);
        BinaryRow partition2 = partition(2);
        DataFileMeta writer1Newest = dataFile("data-u-c-s-1-w-newest", 20L);
        DataFileMeta writer2 = dataFile("data-u-c-s-2-w-only", 5L);
        DataFileMeta sameWriteIdFromAnotherCommit = dataFile("data-u-other-s-1-w-only", 15L);
        DataFileMeta writer1First = dataFile("data-u-c-s-1-w-first", 10L);
        DataFileMeta otherPartition = dataFile("data-u-c-s-1-w-other-partition", 1L);
        List<DataSplit> splits =
                Arrays.asList(
                        dataSplit(partition1, writer1Newest, writer2, sameWriteIdFromAnotherCommit),
                        dataSplit(partition1, writer1First),
                        dataSplit(partition2, otherPartition));

        List<DataSplit> grouped = PostponeUtils.groupPostponeFiles(splits);

        assertThat(grouped).hasSize(4);
        assertThat(grouped.get(0).partition()).isEqualTo(partition1);
        assertThat(grouped.get(0).dataFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-u-c-s-1-w-first", "data-u-c-s-1-w-newest");
        assertThat(grouped.get(1).partition()).isEqualTo(partition1);
        assertThat(grouped.get(1).dataFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-u-c-s-2-w-only");
        assertThat(grouped.get(2).partition()).isEqualTo(partition1);
        assertThat(grouped.get(2).dataFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-u-other-s-1-w-only");
        assertThat(grouped.get(3).partition()).isEqualTo(partition2);
        assertThat(grouped.get(3).dataFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-u-c-s-1-w-other-partition");
    }

    @Test
    public void testGroupPostponeFilesKeepsScanOrderForEqualCreationTime() {
        BinaryRow partition = partition(1);
        DataFileMeta first = dataFile("data-u-c-s-1-w-z", 10L);
        DataFileMeta second = dataFile("data-u-c-s-1-w-a", 10L);

        List<DataSplit> grouped =
                PostponeUtils.groupPostponeFiles(
                        Arrays.asList(dataSplit(partition, first), dataSplit(partition, second)));

        assertThat(grouped).hasSize(1);
        assertThat(grouped.get(0).dataFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-u-c-s-1-w-z", "data-u-c-s-1-w-a");
    }

    @Test
    public void testGroupPostponeFilesKeepsDeletionFilesAligned() {
        BinaryRow partition = partition(1);
        DataFileMeta newest = dataFile("data-u-c-s-1-w-newest", 20L);
        DataFileMeta first = dataFile("data-u-c-s-1-w-first", 10L);
        DeletionFile newestDeletion = mock(DeletionFile.class);
        DeletionFile firstDeletion = mock(DeletionFile.class);

        List<DataSplit> grouped =
                PostponeUtils.groupPostponeFiles(
                        Arrays.asList(
                                dataSplit(partition, newest, newestDeletion),
                                dataSplit(partition, first, firstDeletion)));

        assertThat(grouped).hasSize(1);
        assertThat(grouped.get(0).dataFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-u-c-s-1-w-first", "data-u-c-s-1-w-newest");
        assertThat(grouped.get(0).deletionFiles().orElseThrow(AssertionError::new))
                .containsExactly(firstDeletion, newestDeletion);
    }

    @Test
    public void testTableForPostponeCompact() {
        FileStoreTable table = mock(FileStoreTable.class);
        FileStoreTable copied = mock(FileStoreTable.class);
        when(table.copy(anyMap())).thenReturn(copied);

        assertThat(PostponeUtils.tableForPostponeCompact(table, 4, 5L)).isSameAs(copied);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> options = ArgumentCaptor.forClass(Map.class);
        verify(table).copy(options.capture());
        assertThat(options.getValue())
                .containsEntry("bucket", "4")
                .containsEntry("write-only", "false")
                .containsEntry("commit.strict-mode.last-safe-snapshot", "5");
    }

    @Test
    public void testComputeBucketNumByRowCount() {
        assertThat(PostponeUtils.computeBucketNumByRowCount(0, 100)).isEqualTo(1);
        assertThat(PostponeUtils.computeBucketNumByRowCount(1, 100)).isEqualTo(1);
        assertThat(PostponeUtils.computeBucketNumByRowCount(100, 100)).isEqualTo(1);
        assertThat(PostponeUtils.computeBucketNumByRowCount(101, 100)).isEqualTo(2);
        assertThat(PostponeUtils.computeBucketNumByRowCount(999, 200)).isEqualTo(5);
        assertThat(PostponeUtils.computeBucketNumByRowCount(1000, 200)).isEqualTo(5);
    }

    @Test
    public void testComputeBucketNumByRowCountRejectsInvalidTarget() {
        assertThatThrownBy(() -> PostponeUtils.computeBucketNumByRowCount(100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Option 'postpone.target-row-num-per-bucket' must be greater than 0.");
    }

    @Test
    public void testComputeBucketNumByRowCountRejectsOverflow() {
        assertThatThrownBy(() -> PostponeUtils.computeBucketNumByRowCount(Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds the maximum integer value")
                .hasMessageContaining("Consider increasing 'postpone.target-row-num-per-bucket'");
    }

    @Test
    public void testDetermineBucketNum() {
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        Map<BinaryRow, Long> postponeRowCounts = new HashMap<>();

        BinaryRow knownPartition = partition(1);
        BinaryRow targetPartition = partition(2);
        BinaryRow defaultPartition = partition(3);

        knownNumBuckets.put(knownPartition, 4);
        postponeRowCounts.put(knownPartition, 1000L);
        postponeRowCounts.put(targetPartition, 450L);

        assertThat(
                        PostponeUtils.determineBucketNum(
                                knownPartition, knownNumBuckets, 200L, postponeRowCounts, 1))
                .isEqualTo(4);
        assertThat(
                        PostponeUtils.determineBucketNum(
                                targetPartition, knownNumBuckets, 200L, postponeRowCounts, 1))
                .isEqualTo(3);
        assertThat(
                        PostponeUtils.determineBucketNum(
                                defaultPartition,
                                knownNumBuckets,
                                (Long) null,
                                postponeRowCounts,
                                7))
                .isEqualTo(7);
    }

    @Test
    public void testDecideFixedBucketNum() {
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put(CoreOptions.POSTPONE_TARGET_ROW_NUM_PER_BUCKET.key(), "1");
        optionMap.put(CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_MAX_PARALLELISM.key(), "16");
        optionMap.put(
                CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_RESCALE_LOAD_FACTOR.key(), "32");
        CoreOptions options = CoreOptions.fromMap(optionMap);

        PostponeUtils.FixedBucketDecision rounded =
                PostponeUtils.decideFixedBucketNum(3, 0, null, options);
        assertThat(rounded.targetBucketNum()).isEqualTo(4);
        assertThat(rounded.requiresRescale()).isFalse();

        PostponeUtils.FixedBucketDecision capped =
                PostponeUtils.decideFixedBucketNum((long) Integer.MAX_VALUE + 1, 0, null, options);
        assertThat(capped.targetBucketNum()).isEqualTo(16);
        assertThat(capped.requiresRescale()).isFalse();

        PostponeUtils.FixedBucketDecision atLoadFactor =
                PostponeUtils.decideFixedBucketNum(224, 0, 7, options);
        assertThat(atLoadFactor.targetBucketNum()).isEqualTo(7);
        assertThat(atLoadFactor.requiresRescale()).isFalse();

        PostponeUtils.FixedBucketDecision aboveLoadFactor =
                PostponeUtils.decideFixedBucketNum(225, 0, 7, options);
        assertThat(aboveLoadFactor.targetBucketNum()).isEqualTo(16);
        assertThat(aboveLoadFactor.requiresRescale()).isTrue();

        Map<String, String> cappedOptionMap = new HashMap<>(optionMap);
        cappedOptionMap.put(
                CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_MAX_PARALLELISM.key(), "4");
        PostponeUtils.FixedBucketDecision cappedBelowExisting =
                PostponeUtils.decideFixedBucketNum(225, 0, 7, CoreOptions.fromMap(cappedOptionMap));
        assertThat(cappedBelowExisting.targetBucketNum()).isEqualTo(7);
        assertThat(cappedBelowExisting.requiresRescale()).isFalse();

        PostponeUtils.FixedBucketDecision largerExisting =
                PostponeUtils.decideFixedBucketNum(257, 0, 8, options);
        assertThat(largerExisting.targetBucketNum()).isEqualTo(16);
        assertThat(largerExisting.requiresRescale()).isTrue();

        Map<String, String> lowerLoadFactorOptions = new HashMap<>(optionMap);
        lowerLoadFactorOptions.put(
                CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_RESCALE_LOAD_FACTOR.key(), "4");
        PostponeUtils.FixedBucketDecision lowerLoadFactor =
                PostponeUtils.decideFixedBucketNum(
                        29, 0, 7, CoreOptions.fromMap(lowerLoadFactorOptions));
        assertThat(lowerLoadFactor.targetBucketNum()).isEqualTo(16);
        assertThat(lowerLoadFactor.requiresRescale()).isTrue();
    }

    @Test
    public void testDecideFixedBucketNumRejectsInvalidRescaleLoadFactor() {
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put(CoreOptions.POSTPONE_TARGET_ROW_NUM_PER_BUCKET.key(), "1");
        optionMap.put(CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_RESCALE_LOAD_FACTOR.key(), "0");

        assertThatThrownBy(
                        () ->
                                PostponeUtils.decideFixedBucketNum(
                                        1, 0, 1, CoreOptions.fromMap(optionMap)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_RESCALE_LOAD_FACTOR.key());
    }

    @Test
    public void testDecideFixedBucketNumByStagedFileSize() {
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put(CoreOptions.POSTPONE_TARGET_SIZE_PER_BUCKET.key(), "100 b");
        optionMap.put(CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_MAX_PARALLELISM.key(), "32");

        PostponeUtils.FixedBucketDecision decision =
                PostponeUtils.decideFixedBucketNum(10, 1000, null, CoreOptions.fromMap(optionMap));
        assertThat(decision.targetBucketNum()).isEqualTo(16);
    }

    private static BinaryRow partition(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }

    private static SimpleFileEntry fileEntry(
            BinaryRow partition, int bucket, int totalBuckets, int level) {
        SimpleFileEntry entry = mock(SimpleFileEntry.class);
        when(entry.partition()).thenReturn(partition);
        when(entry.bucket()).thenReturn(bucket);
        when(entry.totalBuckets()).thenReturn(totalBuckets);
        when(entry.level()).thenReturn(level);
        return entry;
    }

    private static DataFileMeta dataFile(String name, long creationTime) {
        DataFileMeta file = mock(DataFileMeta.class);
        when(file.fileName()).thenReturn(name);
        when(file.creationTimeEpochMillis()).thenReturn(creationTime);
        return file;
    }

    private static DataSplit dataSplit(BinaryRow partition, DataFileMeta... files) {
        return DataSplit.builder()
                .withPartition(partition)
                .withBucket(BucketMode.POSTPONE_BUCKET)
                .withBucketPath("postpone")
                .withTotalBuckets(BucketMode.POSTPONE_BUCKET)
                .withDataFiles(Arrays.asList(files))
                .build();
    }

    private static DataSplit dataSplit(
            BinaryRow partition, DataFileMeta file, DeletionFile deletionFile) {
        return DataSplit.builder()
                .withPartition(partition)
                .withBucket(BucketMode.POSTPONE_BUCKET)
                .withBucketPath("postpone")
                .withTotalBuckets(BucketMode.POSTPONE_BUCKET)
                .withDataFiles(Collections.singletonList(file))
                .withDataDeletionFiles(Collections.singletonList(deletionFile))
                .build();
    }
}
