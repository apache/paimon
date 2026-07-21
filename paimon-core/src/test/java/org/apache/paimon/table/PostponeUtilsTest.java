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

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileStoreScan;
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
        SimpleFileEntry entry = mock(SimpleFileEntry.class);
        when(entry.partition()).thenReturn(partition);
        when(entry.totalBuckets()).thenReturn(4);

        FileStoreScan scan = mock(FileStoreScan.class, RETURNS_SELF);
        when(scan.readSimpleEntries()).thenReturn(Collections.singletonList(entry));
        FileStore store = mock(FileStore.class);
        when(store.newScan()).thenReturn(scan);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.store()).thenReturn(store);

        assertThat(PostponeUtils.getKnownNumBuckets(table, 5L)).containsEntry(partition, 4);
        verify(scan).withSnapshot(5L);
        verify(scan).onlyReadRealBuckets();
    }

    @Test
    public void testGetPostponeRowCountsFromSnapshot() {
        BinaryRow partition = partition(1);
        DataFileMeta file = mock(DataFileMeta.class);
        when(file.rowCount()).thenReturn(10L);
        ManifestEntry entry = mock(ManifestEntry.class);
        when(entry.partition()).thenReturn(partition);
        when(entry.file()).thenReturn(file);

        SnapshotReader reader = mock(SnapshotReader.class, RETURNS_SELF);
        when(reader.readFileIterator()).thenReturn(Collections.singletonList(entry).iterator());
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.newSnapshotReader()).thenReturn(reader);

        assertThat(PostponeUtils.getPostponeRowCounts(table, 5L)).containsEntry(partition, 10L);
        verify(reader).withSnapshot(5L);
        verify(reader).withBucket(BucketMode.POSTPONE_BUCKET);
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
    public void testTableForPostponeCompact() {
        FileStoreTable table = mock(FileStoreTable.class);
        FileStoreTable copied = mock(FileStoreTable.class);
        when(table.copy(anyMap())).thenReturn(copied);

        FileStoreTable result = PostponeUtils.tableForPostponeCompact(table, 4, 5L);
        assertThat(result).isInstanceOf(SchemaBucketFileStoreTable.class);
        assertThat(((DelegatedFileStoreTable) result).wrapped()).isSameAs(copied);

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
}
