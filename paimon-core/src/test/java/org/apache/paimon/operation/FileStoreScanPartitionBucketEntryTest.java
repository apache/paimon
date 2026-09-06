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

package org.apache.paimon.operation;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AbstractFileStoreScan#readPartitionEntries} and {@link
 * AbstractFileStoreScan#readBucketEntries}.
 *
 * <p>These methods are exercised through {@code snapshotReader.partitionEntries()} and {@code
 * snapshotReader.bucketEntries()} which directly delegate to {@code scan.readPartitionEntries()}
 * and {@code scan.readBucketEntries()}.
 */
public class FileStoreScanPartitionBucketEntryTest extends ScannerTestBase {

    @Test
    public void testReadPartitionEntriesSinglePartition() throws Exception {
        // Write data to a single partition (pt=1)
        BatchTableWrite write = table.newWrite(commitUser);
        for (int i = 0; i < 10; i++) {
            write.write(GenericRow.of(1, i, (long) i));
        }
        List<CommitMessage> messages = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        write.close();
        commit.close();

        List<PartitionEntry> entries = snapshotReader.partitionEntries();
        assertThat(entries).hasSize(1);
        PartitionEntry entry = entries.get(0);
        assertThat(entry.partition().getInt(0)).isEqualTo(1);
        assertThat(entry.recordCount()).isEqualTo(10);
        assertThat(entry.fileCount()).isEqualTo(1);
        assertThat(entry.totalBuckets()).isEqualTo(1);
        assertThat(entry.fileSizeInBytes()).isGreaterThan(0);
    }

    @Test
    public void testReadPartitionEntriesMultiplePartitions() throws Exception {
        // Write data to multiple partitions
        BatchTableWrite write = table.newWrite(commitUser);
        int[] partitions = {1, 2, 3};
        int[] recordCounts = {5, 10, 15};

        for (int p = 0; p < partitions.length; p++) {
            for (int i = 0; i < recordCounts[p]; i++) {
                write.write(GenericRow.of(partitions[p], i, (long) i));
            }
        }
        List<CommitMessage> messages = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        write.close();
        commit.close();

        List<PartitionEntry> entries = snapshotReader.partitionEntries();
        assertThat(entries).hasSize(3);

        Map<Integer, PartitionEntry> entryMap =
                entries.stream().collect(Collectors.toMap(e -> e.partition().getInt(0), e -> e));

        for (int p = 0; p < partitions.length; p++) {
            PartitionEntry entry = entryMap.get(partitions[p]);
            assertThat(entry).isNotNull();
            assertThat(entry.recordCount()).isEqualTo(recordCounts[p]);
            assertThat(entry.fileCount()).isEqualTo(1);
        }
    }

    @Test
    public void testReadPartitionEntriesMultipleFilesSamePartition() throws Exception {
        // Write data in two commits to the same partition, creating multiple files
        BatchTableWrite write1 = table.newWrite(commitUser);
        for (int i = 0; i < 5; i++) {
            write1.write(GenericRow.of(1, i, (long) i));
        }
        List<CommitMessage> messages1 = write1.prepareCommit();
        TableCommitImpl commit1 = table.newCommit(commitUser);
        commit1.commit(messages1);
        write1.close();

        BatchTableWrite write2 = table.newWrite(commitUser);
        for (int i = 5; i < 15; i++) {
            write2.write(GenericRow.of(1, i, (long) i));
        }
        List<CommitMessage> messages2 = write2.prepareCommit();
        TableCommitImpl commit2 = table.newCommit(commitUser);
        commit2.commit(messages2);
        write2.close();
        commit2.close();

        List<PartitionEntry> entries = snapshotReader.partitionEntries();
        assertThat(entries).hasSize(1);
        PartitionEntry entry = entries.get(0);
        assertThat(entry.partition().getInt(0)).isEqualTo(1);
        assertThat(entry.recordCount()).isEqualTo(15);
        assertThat(entry.fileCount()).isEqualTo(2);
    }

    @Test
    public void testReadPartitionEntriesWithPartitionFilter() throws Exception {
        // Write data to partitions 1, 2, 3
        BatchTableWrite write = table.newWrite(commitUser);
        for (int pt = 1; pt <= 3; pt++) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(pt, i, (long) i));
            }
        }
        List<CommitMessage> messages = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        write.close();
        commit.close();

        // Create a new snapshot reader with partition filter to only read partition 2
        List<PartitionEntry> entries =
                table.newSnapshotReader()
                        .withPartitionFilter(Collections.singletonList(binaryRow(2)))
                        .partitionEntries();

        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).partition().getInt(0)).isEqualTo(2);
        assertThat(entries.get(0).recordCount()).isEqualTo(5);
    }

    @Test
    public void testReadPartitionEntriesAppendOnlyTable() throws Exception {
        // Create an append-only table (no primary keys)
        createAppendOnlyTable();

        BatchTableWrite write = table.newWrite(commitUser);
        for (int pt = 1; pt <= 3; pt++) {
            for (int i = 0; i < 10; i++) {
                write.write(GenericRow.of(pt, i, (long) i));
            }
        }
        List<CommitMessage> messages = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        write.close();
        commit.close();

        List<PartitionEntry> entries = snapshotReader.partitionEntries();
        assertThat(entries).hasSize(3);

        Map<Integer, PartitionEntry> entryMap =
                entries.stream().collect(Collectors.toMap(e -> e.partition().getInt(0), e -> e));

        for (int pt = 1; pt <= 3; pt++) {
            PartitionEntry entry = entryMap.get(pt);
            assertThat(entry).isNotNull();
            assertThat(entry.recordCount()).isEqualTo(10);
        }
    }

    @Test
    public void testReadBucketEntriesSinglePartition() throws Exception {
        // Write data to a single partition with 1 bucket
        BatchTableWrite write = table.newWrite(commitUser);
        for (int i = 0; i < 10; i++) {
            write.write(GenericRow.of(1, i, (long) i));
        }
        List<CommitMessage> messages = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        write.close();
        commit.close();

        List<BucketEntry> entries = snapshotReader.bucketEntries();
        assertThat(entries).hasSize(1);
        BucketEntry entry = entries.get(0);
        assertThat(entry.partition().getInt(0)).isEqualTo(1);
        assertThat(entry.bucket()).isEqualTo(0);
        assertThat(entry.recordCount()).isEqualTo(10);
        assertThat(entry.fileCount()).isEqualTo(1);
    }

    @Test
    public void testReadBucketEntriesMultiplePartitions() throws Exception {
        // Write data to multiple partitions
        BatchTableWrite write = table.newWrite(commitUser);
        for (int pt = 1; pt <= 3; pt++) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(pt, i, (long) i));
            }
        }
        List<CommitMessage> messages = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        write.close();
        commit.close();

        List<BucketEntry> entries = snapshotReader.bucketEntries();
        assertThat(entries).hasSize(3);

        Map<Pair<Integer, Integer>, BucketEntry> entryMap =
                entries.stream()
                        .collect(
                                Collectors.toMap(
                                        e -> Pair.of(e.partition().getInt(0), e.bucket()), e -> e));

        for (int pt = 1; pt <= 3; pt++) {
            BucketEntry entry = entryMap.get(Pair.of(pt, 0));
            assertThat(entry).isNotNull();
            assertThat(entry.recordCount()).isEqualTo(5);
            assertThat(entry.fileCount()).isEqualTo(1);
        }
    }

    @Test
    public void testReadBucketEntriesMultipleFilesSameBucket() throws Exception {
        // Write data in two commits to the same partition/bucket
        BatchTableWrite write1 = table.newWrite(commitUser);
        for (int i = 0; i < 5; i++) {
            write1.write(GenericRow.of(1, i, (long) i));
        }
        List<CommitMessage> messages1 = write1.prepareCommit();
        TableCommitImpl commit1 = table.newCommit(commitUser);
        commit1.commit(messages1);
        write1.close();

        BatchTableWrite write2 = table.newWrite(commitUser);
        for (int i = 5; i < 15; i++) {
            write2.write(GenericRow.of(1, i, (long) i));
        }
        List<CommitMessage> messages2 = write2.prepareCommit();
        TableCommitImpl commit2 = table.newCommit(commitUser);
        commit2.commit(messages2);
        write2.close();
        commit2.close();

        List<BucketEntry> entries = snapshotReader.bucketEntries();
        assertThat(entries).hasSize(1);
        BucketEntry entry = entries.get(0);
        assertThat(entry.partition().getInt(0)).isEqualTo(1);
        assertThat(entry.bucket()).isEqualTo(0);
        assertThat(entry.recordCount()).isEqualTo(15);
        assertThat(entry.fileCount()).isEqualTo(2);
    }

    @Test
    public void testReadBucketEntriesAppendOnlyTable() throws Exception {
        // Create an append-only table (no primary keys)
        createAppendOnlyTable();

        BatchTableWrite write = table.newWrite(commitUser);
        for (int pt = 1; pt <= 3; pt++) {
            for (int i = 0; i < 10; i++) {
                write.write(GenericRow.of(pt, i, (long) i));
            }
        }
        List<CommitMessage> messages = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        write.close();
        commit.close();

        List<BucketEntry> entries = snapshotReader.bucketEntries();
        assertThat(entries).hasSize(3);

        for (BucketEntry entry : entries) {
            assertThat(entry.recordCount()).isEqualTo(10);
            assertThat(entry.fileCount()).isEqualTo(1);
        }
    }
}
