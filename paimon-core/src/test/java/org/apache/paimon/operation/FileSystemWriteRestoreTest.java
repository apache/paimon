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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FileSystemWriteRestore}, covering the {@code totalBuckets} resolution logic for
 * both empty and non-empty buckets across partitioned and unpartitioned tables.
 *
 * <p>When restoring files for a {@code (partition, bucket)} that has no existing data files, there
 * are no manifest entries to derive {@code totalBuckets} from. For partitioned tables, {@link
 * WriteRestore#extractTotalBuckets} falls back to {@link
 * org.apache.paimon.table.sink.PartitionBucketMapping} to correctly return the per-partition bucket
 * count (e.g. after a rescale). For unpartitioned tables, {@code null} is returned so the write
 * path falls back to {@code numBuckets} and the committer-side mismatch check still fires.
 */
public class FileSystemWriteRestoreTest {

    @TempDir java.nio.file.Path tempDir;

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                    new String[] {"pt", "k", "v"});

    @Test
    void testRestoreFromPinnedSnapshotForPostponeBucket() {
        Snapshot pinned = mock(Snapshot.class);
        Snapshot latest = mock(Snapshot.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.snapshot(5L)).thenReturn(pinned);
        when(snapshotManager.latestSnapshotFromFileSystem()).thenReturn(latest);

        FileStoreScan scan = mock(FileStoreScan.class);
        FileStoreScan.Plan plan = mock(FileStoreScan.Plan.class);
        when(scan.withSnapshot(pinned)).thenReturn(scan);
        when(scan.withPartitionBucket(EMPTY_ROW, 0)).thenReturn(scan);
        when(scan.plan()).thenReturn(plan);
        when(plan.files()).thenReturn(Collections.emptyList());

        IndexFileMeta ann = new IndexFileMeta("test-vector-ann", "ann", 1, 1, null, null, null);
        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        when(indexFileHandler.scanSourceIndexes(pinned, EMPTY_ROW, 0))
                .thenReturn(Collections.singletonList(ann));

        FileSystemWriteRestore restore =
                new FileSystemWriteRestore(
                        new CoreOptions(new HashMap<>()),
                        snapshotManager,
                        scan,
                        indexFileHandler,
                        5L);

        RestoreFiles restored = restore.restoreFiles(EMPTY_ROW, 0, false, false, true);

        assertThat(restored.snapshot()).isSameAs(pinned);
        assertThat(restored.sourceIndexPayloads()).containsExactly(ann);
        verify(scan).withSnapshot(pinned);
        verify(indexFileHandler).scanSourceIndexes(pinned, EMPTY_ROW, 0);
        verify(snapshotManager, never()).latestSnapshotFromFileSystem();
    }

    @Test
    void testRestoreSourceIndexPayloadsWithoutDirectory() {
        Snapshot snapshot = mock(Snapshot.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.latestSnapshotFromFileSystem()).thenReturn(snapshot);

        FileStoreScan scan = mock(FileStoreScan.class);
        FileStoreScan.Plan plan = mock(FileStoreScan.Plan.class);
        when(scan.withSnapshot(snapshot)).thenReturn(scan);
        when(scan.withPartitionBucket(EMPTY_ROW, 0)).thenReturn(scan);
        when(scan.plan()).thenReturn(plan);
        when(plan.files()).thenReturn(Collections.emptyList());

        IndexFileMeta ann = new IndexFileMeta("test-vector-ann", "ann", 1, 1, null, null, null);
        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        when(indexFileHandler.scanSourceIndexes(snapshot, EMPTY_ROW, 0))
                .thenReturn(Collections.singletonList(ann));

        FileSystemWriteRestore restore =
                new FileSystemWriteRestore(
                        new CoreOptions(new HashMap<>()), snapshotManager, scan, indexFileHandler);

        RestoreFiles restored = restore.restoreFiles(EMPTY_ROW, 0, false, false, true);

        assertThat(restored.sourceIndexPayloads()).containsExactly(ann);
    }

    @Test
    public void testEmptyBucketUsesPartitionBucketMapping() throws Exception {
        // Build a table with default bucket=4 and write data into partition 1.
        // Some buckets within partition 1 will end up with files (bucket 0 OR
        // bucket 1, depending on hash); the OTHER bucket will be empty. Then
        // "rescale" the table-level default to 32 (without rewriting partition 1)
        // and ask the WriteRestore for an empty bucket. It must return
        // totalBuckets=4 (the partition's actual bucket count), NOT 32 (the new
        // table default).
        FileStoreTable table = createPartitionedPkTable(4);

        // Write enough rows to populate at least one bucket within partition 1.
        commitOneRow(table, /* pt */ 1, /* k */ 1);
        commitOneRow(table, /* pt */ 1, /* k */ 2);

        // Find an empty bucket in partition 1 by inspecting the existing files.
        int emptyBucket = findEmptyBucket(table, 1, /* totalBuckets */ 4);

        // Simulate a rescale by raising the table-level default bucket count
        // (without rewriting existing files). Existing manifest entries still
        // carry totalBuckets=4.
        table = withBucket(table, 32);

        WriteRestore restore = newWriteRestore(table);

        RestoreFiles restored =
                restore.restoreFiles(binaryRow(1), emptyBucket, false, false, false);

        assertThat(restored.totalBuckets())
                .as(
                        "Empty (partition 1, bucket %d): totalBuckets must be inferred from "
                                + "PartitionBucketMapping (4), not the new table default (32).",
                        emptyBucket)
                .isEqualTo(4);
        assertThat(restored.dataFiles()).isNullOrEmpty();
    }

    @Test
    public void testEmptyBucketInUnseenPartitionUsesDefault() throws Exception {
        // For an entirely unseen partition (no files anywhere), no per-partition
        // mapping exists and PartitionBucketMapping.resolveNumBuckets falls back to
        // the table's default bucket count.
        FileStoreTable table = createPartitionedPkTable(8);
        commitOneRow(table, 1, 100); // ensures the snapshot exists

        WriteRestore restore = newWriteRestore(table);
        RestoreFiles restored =
                restore.restoreFiles(binaryRow(/* unseen */ 999), 0, false, false, false);

        assertThat(restored.totalBuckets()).isEqualTo(8);
        assertThat(restored.dataFiles()).isNullOrEmpty();
    }

    @Test
    public void testWriteRejectsBucketOutsidePartitionLayout() throws Exception {
        // Partition 1 is created with 2 buckets.
        FileStoreTable table = createPartitionedPkTable(2);
        commitOneRow(table, /* pt */ 1, /* k */ 1);

        // Simulate a rescale: the table default is raised to 8 buckets, but partition 1
        // still only has 2 buckets. writeOnly=false so the writer scans previous files and
        // runs the per-partition bucket-layout check in AbstractFileStoreWrite.
        FileStoreTable rescaledTable = withBucket(table, 8);

        // Writing an out-of-range bucket (>= the partition's 2 buckets) into an empty bucket of
        // partition 1 must be rejected, even though the bucket id is valid for the 8-bucket
        // default.
        // This is the bucket that PartitionBucketMapping recovery would otherwise silently accept.
        // write(row, bucket) routes the row (partition pt=1) to the explicitly given bucket.
        try (InnerTableWrite write = rescaledTable.newWrite(UUID.randomUUID().toString())) {
            assertThatThrownBy(() -> write.write(GenericRow.of(1, 1, 1L), /* bucket */ 6))
                    .hasMessageContaining("only has 2 buckets")
                    .hasMessageContaining("table default: 8");
        }

        // Writing an in-range bucket (< the partition's 2 buckets) into an empty bucket of the same
        // partition is accepted: per-partition bucket counts are still honored.
        int emptyBucket = findEmptyBucket(rescaledTable, 1, /* totalBuckets */ 2);
        String user = UUID.randomUUID().toString();
        long id = rescaledTable.snapshotManager().latestSnapshotId();
        try (InnerTableWrite write = rescaledTable.newWrite(user);
                StreamTableCommit commit = rescaledTable.newCommit(user)) {
            write.write(GenericRow.of(1, 2, 2L), emptyBucket);
            commit.commit(id, write.prepareCommit(true, id));
        }
    }

    @Test
    public void testNonEmptyBucketReportsManifestTotalBuckets() throws Exception {
        // Sanity test: when a bucket has files, totalBuckets must come from the
        // manifest entries (not from the fallback path). This guards against
        // accidentally always overriding totalBuckets via PartitionBucketMapping.
        FileStoreTable table = createPartitionedPkTable(2);
        commitOneRow(table, 1, 1);
        commitOneRow(table, 1, 2);

        // Locate a non-empty bucket within partition 1.
        int nonEmptyBucket = findNonEmptyBucket(table, 1, 2);

        // Change the table default to ensure the returned totalBuckets is from the
        // manifest entry, not the schema.
        table = withBucket(table, 32);

        WriteRestore restore = newWriteRestore(table);
        RestoreFiles restored =
                restore.restoreFiles(binaryRow(1), nonEmptyBucket, false, false, false);

        assertThat(restored.totalBuckets()).isEqualTo(2);
        assertThat(restored.dataFiles()).isNotEmpty();
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    private FileStoreTable createPartitionedPkTable(int bucket) throws Exception {
        Path path = new Path(tempDir.toString());
        Options options = new Options();
        options.set(CoreOptions.PATH, path.toString());
        options.set(CoreOptions.BUCKET, bucket);
        // These tests exercise per-partition bucket resolution, which is opt-in.
        options.set(CoreOptions.BUCKET_PER_PARTITION_COUNT_ENABLED, true);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));

        return FileStoreTableFactory.create(
                LocalFileIO.create(), path, tableSchema, CatalogEnvironment.empty());
    }

    private FileStoreTable withBucket(FileStoreTable table, int newBucket) {
        Options options = new Options(table.options());
        options.set(CoreOptions.BUCKET, newBucket);
        return table.copy(table.schema().copy(options.toMap()));
    }

    private WriteRestore newWriteRestore(FileStoreTable table) {
        return new FileSystemWriteRestore(
                table.store().options(),
                table.snapshotManager(),
                table.store().newScan(),
                table.store().newIndexFileHandler());
    }

    private void commitOneRow(FileStoreTable table, int pt, int k) throws Exception {
        String user = UUID.randomUUID().toString();
        Long latest = table.snapshotManager().latestSnapshotId();
        long id = latest == null ? 0L : latest;
        try (StreamTableWrite write = table.newWrite(user);
                StreamTableCommit commit = table.newCommit(user)) {
            write.write(GenericRow.of(pt, k, (long) k));
            commit.commit(id, write.prepareCommit(true, id));
        }
    }

    /** Returns a bucket id (0..totalBuckets-1) that has no data files within the partition. */
    private int findEmptyBucket(FileStoreTable table, int pt, int totalBuckets) throws Exception {
        BinaryRow partition = binaryRow(pt);
        for (int b = 0; b < totalBuckets; b++) {
            int bucket = b;
            boolean nonEmpty =
                    table.newSnapshotReader()
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucket(bucket).read().dataSplits().stream()
                            .anyMatch(s -> !s.dataFiles().isEmpty());
            if (!nonEmpty) {
                return bucket;
            }
        }
        throw new IllegalStateException(
                "Could not find an empty bucket in partition "
                        + pt
                        + " (every bucket has files); test scenario could not be set up.");
    }

    /** Returns a bucket id (0..totalBuckets-1) that has at least one data file. */
    private int findNonEmptyBucket(FileStoreTable table, int pt, int totalBuckets)
            throws Exception {
        BinaryRow partition = binaryRow(pt);
        for (int b = 0; b < totalBuckets; b++) {
            int bucket = b;
            boolean nonEmpty =
                    table.newSnapshotReader()
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucket(bucket).read().dataSplits().stream()
                            .anyMatch(s -> !s.dataFiles().isEmpty());
            if (nonEmpty) {
                return bucket;
            }
        }
        throw new IllegalStateException("Could not find a non-empty bucket in partition " + pt);
    }

    private static BinaryRow binaryRow(int pt) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, pt);
        writer.complete();
        return row;
    }
}
