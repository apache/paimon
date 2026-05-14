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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FileSystemWriteRestore} covering the scenario where a partition has a different
 * bucket count than the table default (e.g., after a rescale operation).
 */
public class FileSystemWriteRestoreTest extends TableTestBase {

    /**
     * Scenario:
     *
     * <ul>
     *   <li>Table bucket (default): 32
     *   <li>Partition A bucket: 2 (rescaled)
     *   <li>Partition A bucket=0: has data files, totalBuckets=2
     *   <li>Partition A bucket=1: no data files
     * </ul>
     *
     * <p>When restoring bucket=1 of partition A (empty bucket), the returned {@code totalBuckets}
     * must be 2 (from the per-partition mapping), not 32 (the table default).
     */
    @Test
    public void testEmptyBucketUsesPartitionBucketCount() throws Exception {
        // Table default: 32 buckets
        int tableBuckets = 32;
        // Partition A was rescaled to 2 buckets
        int partitionBuckets = 2;

        Identifier identifier = new Identifier("db", "table");
        catalog.createDatabase("db", false);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("pt", "k")
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), String.valueOf(tableBuckets))
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // Write data to partition A, bucket=0 with totalBuckets=2 (simulating a rescaled
        // partition). We write normally first, then re-wrap the commit message to override
        // totalBuckets.
        BinaryRow partitionA = partitionRow(1);
        String commitUser = UUID.randomUUID().toString();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        List<CommitMessage> messages;
        try (StreamTableWrite write = writeBuilder.newWrite()) {
            // Write to partition=1, key=1, value=100 with bucket=0 explicitly
            write.write(GenericRow.of(1, 1, 100), 0);
            messages = write.prepareCommit(false, 0);
        }

        // Rewrap the commit message so that totalBuckets=2 (the rescaled partition bucket count)
        CommitMessageImpl original = (CommitMessageImpl) messages.get(0);
        CommitMessageImpl rescaled =
                new CommitMessageImpl(
                        original.partition(),
                        original.bucket(),
                        partitionBuckets,
                        original.newFilesIncrement(),
                        original.compactIncrement());

        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.commit(0, Collections.<CommitMessage>singletonList(rescaled));
        }

        // Now create the FileSystemWriteRestore for this table
        FileStoreTable freshTable = getTable(identifier);
        FileSystemWriteRestore writeRestore = newWriteRestore(freshTable);

        // Restore bucket=0 (has data files): totalBuckets should be 2
        RestoreFiles restored0 = writeRestore.restoreFiles(partitionA, 0, false, false);
        assertThat(restored0.totalBuckets())
                .as("bucket=0 (has files) should use partition bucket count, not table default")
                .isEqualTo(partitionBuckets);
        assertThat(restored0.dataFiles()).isNotEmpty();

        // Restore bucket=1 (empty bucket): totalBuckets should ALSO be 2, not 32
        RestoreFiles restored1 = writeRestore.restoreFiles(partitionA, 1, false, false);
        assertThat(restored1.totalBuckets())
                .as("bucket=1 (empty bucket) should use partition bucket count, not table default")
                .isEqualTo(partitionBuckets);
        assertThat(restored1.dataFiles()).isEmpty();
    }

    /**
     * Sanity check: a partition that has never been rescaled uses the table default bucket count.
     */
    @Test
    public void testPartitionWithDefaultBucketCount() throws Exception {
        int tableBuckets = 32;

        Identifier identifier = new Identifier("db2", "table");
        catalog.createDatabase("db2", false);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("pt", "k")
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), String.valueOf(tableBuckets))
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // Write data using the default bucket count (no totalBuckets override)
        write(table, GenericRow.of(1, 1, 100));

        FileStoreTable freshTable = getTable(identifier);
        FileSystemWriteRestore writeRestore = newWriteRestore(freshTable);

        BinaryRow partitionA = partitionRow(1);

        // Restore bucket=0 (has data with default totalBuckets=32)
        RestoreFiles restored = writeRestore.restoreFiles(partitionA, 0, false, false);
        assertThat(restored.totalBuckets())
                .as("partition with default bucket count should return table bucket count")
                .isEqualTo(tableBuckets);
    }

    /**
     * Scenario with two partitions: partition A rescaled to 2 buckets, partition B uses default 32.
     * Each partition's empty buckets must return their own bucket count.
     */
    @Test
    public void testMixedPartitionsWithDifferentBucketCounts() throws Exception {
        int tableBuckets = 32;
        int partitionABuckets = 2;

        Identifier identifier = new Identifier("db3", "table");
        catalog.createDatabase("db3", false);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("pt", "k")
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), String.valueOf(tableBuckets))
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        String commitUser = UUID.randomUUID().toString();

        // Write partition A, bucket=0 with rescaled totalBuckets=2
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        List<CommitMessage> messagesA;
        try (StreamTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(1, 1, 100), 0);
            messagesA = write.prepareCommit(false, 0);
        }
        CommitMessageImpl originalA = (CommitMessageImpl) messagesA.get(0);
        CommitMessageImpl rescaledA =
                new CommitMessageImpl(
                        originalA.partition(),
                        originalA.bucket(),
                        partitionABuckets,
                        originalA.newFilesIncrement(),
                        originalA.compactIncrement());
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.commit(0, Collections.<CommitMessage>singletonList(rescaledA));
        }

        // Write partition B, bucket=0 with default totalBuckets=32
        try (StreamTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(2, 1, 200), 0);
            List<CommitMessage> messagesB = write.prepareCommit(false, 1);
            try (TableCommitImpl commit = table.newCommit(commitUser)) {
                commit.commit(1, messagesB);
            }
        }

        FileStoreTable freshTable = getTable(identifier);
        FileSystemWriteRestore writeRestore = newWriteRestore(freshTable);

        BinaryRow partitionA = partitionRow(1);
        BinaryRow partitionB = partitionRow(2);

        // Partition A: bucket=1 (empty) should use 2, not 32
        RestoreFiles restoredA1 = writeRestore.restoreFiles(partitionA, 1, false, false);
        assertThat(restoredA1.totalBuckets())
                .as("partition A empty bucket should use rescaled partition bucket count 2")
                .isEqualTo(partitionABuckets);
        assertThat(restoredA1.dataFiles()).isEmpty();

        // Partition B: bucket=0 (has files) should use 32
        RestoreFiles restoredB0 = writeRestore.restoreFiles(partitionB, 0, false, false);
        assertThat(restoredB0.totalBuckets())
                .as("partition B should use table default bucket count 32")
                .isEqualTo(tableBuckets);
        assertThat(restoredB0.dataFiles()).isNotEmpty();
    }

    private static FileSystemWriteRestore newWriteRestore(FileStoreTable table) {
        return new FileSystemWriteRestore(
                table.store().options(),
                table.store().snapshotManager(),
                table.store().newScan(),
                table.store().newIndexFileHandler());
    }

    private BinaryRow partitionRow(int partitionValue) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partitionValue);
        writer.complete();
        return row;
    }
}
