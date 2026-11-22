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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Lookup must not return a row whose only deletion record is a deletion vector. */
public class LocalTableQueryDeletionVectorTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private final Identifier identifier = Identifier.create("my_db", "T");

    @BeforeEach
    public void before() throws Exception {
        Options options = new Options();
        options.set("warehouse", new Path(tempDir.toString() + "/warehouse").toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("my_db", true);
    }

    @Test
    public void testLookupHonorsDeletionVectorAfterCompaction() throws Exception {
        FileStoreTable table = createTable();
        IOManager ioManager = IOManager.create(tempDir.toString() + "/io");
        LocalTableQuery query = table.newLocalTableQuery().withIOManager(ioManager);
        // Flink lookup follows every snapshot, including compaction, so removed files are dropped
        // from the levels. The default stream scan skips compact commits.
        StreamTableScan scan = fileMonitorScan(table);
        try {
            for (int key = 1; key <= 20; key++) {
                write(table, GenericRow.of(key, key * 10));
            }
            compact(table, ioManager, true);
            refresh(query, scan);
            assertThat(value(query, 1)).isEqualTo(10);
            assertThat(value(query, 2)).isEqualTo(20);

            // The delete is still a level-0 record. Streaming lookup has to see it; a batch scan
            // skips level 0 until compaction.
            write(table, GenericRow.ofKind(RowKind.DELETE, 1, 10));
            refresh(query, scan);
            assertThat(value(query, 1)).isNull();
            assertThat(value(query, 2)).isEqualTo(20);

            // Compaction drops the delete record and marks the old file position instead.
            compact(table, ioManager, false);
            refresh(query, scan);
            assertThat(deletionVectorIndex(table)).isNotEmpty();
            assertThat(value(query, 1)).isNull();
            assertThat(value(query, 2)).isEqualTo(20);
            assertThat(highLevelRowCount(table)).isGreaterThan(1);

            write(table, GenericRow.of(1, 99));
            refresh(query, scan);
            assertThat(value(query, 1)).isEqualTo(99);
            assertThat(value(query, 2)).isEqualTo(20);
        } finally {
            query.close();
            ioManager.close();
        }
    }

    @Test
    public void testDeletionVectorUsesTheFileSnapshot() throws Exception {
        FileStoreTable table = createTable();
        IOManager ioManager = IOManager.create(tempDir.toString() + "/io");
        LocalTableQuery query = table.newLocalTableQuery().withIOManager(ioManager);
        try {
            for (int key = 1; key <= 20; key++) {
                write(table, GenericRow.of(key, key * 10));
            }
            compact(table, ioManager, true);

            // Hold the compacted file plan, then let a later commit mark one of its positions.
            List<Split> held = fileMonitorScan(table).plan().splits();
            assertThat(held).isNotEmpty();
            long heldSnapshotId = snapshotId(held.get(0));

            write(table, GenericRow.of(1, 99));
            compact(table, ioManager, false);
            assertThat(table.snapshotManager().latestSnapshot().id()).isGreaterThan(heldSnapshotId);
            assertThat(deletionVectorIndex(table)).isNotEmpty();

            refreshSplits(query, held);
            assertThat(value(query, 1)).isEqualTo(10);
            assertThat(value(query, 2)).isEqualTo(20);
        } finally {
            query.close();
            ioManager.close();
        }
    }

    @Test
    public void testExpiredFileSnapshotIsReportedAsOutOfRange() throws Exception {
        FileStoreTable table = createTable();
        IOManager ioManager = IOManager.create(tempDir.toString() + "/io");
        LocalTableQuery query = table.newLocalTableQuery().withIOManager(ioManager);
        try {
            for (int key = 1; key <= 20; key++) {
                write(table, GenericRow.of(key, key * 10));
            }
            compact(table, ioManager, true);
            write(table, GenericRow.ofKind(RowKind.DELETE, 1, 10));
            compact(table, ioManager, false);
            assertThat(deletionVectorIndex(table)).isNotEmpty();

            // Hold a plan, then let the snapshot it came from expire. Serving these files
            // without their deletion vectors would resurrect the deleted row, so refresh has
            // to fail with the exception the lookup function reopens on.
            List<Split> held = fileMonitorScan(table).plan().splits();
            assertThat(held).isNotEmpty();
            long heldSnapshotId = snapshotId(held.get(0));
            expireSnapshot(table, heldSnapshotId);

            assertThatThrownBy(() -> refreshSplits(query, held))
                    .isInstanceOf(OutOfRangeException.class)
                    .hasMessageContaining("has expired");
        } finally {
            query.close();
            ioManager.close();
        }
    }

    private void expireSnapshot(FileStoreTable table, long snapshotId) throws Exception {
        table.fileIO()
                .deleteQuietly(
                        new Path(
                                table.location(),
                                String.format("snapshot/snapshot-%d", snapshotId)));
        // The manager caches snapshots by path, so drop the cached copy too.
        table.snapshotManager().invalidateCache();
    }

    @Test
    public void testPartialUpdateKeepsMergedColumns() throws Exception {
        FileStoreTable table =
                createTable(
                        Schema.newBuilder()
                                .column("k", DataTypes.INT())
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.INT())
                                .primaryKey("k")
                                .option(CoreOptions.BUCKET.key(), "1")
                                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                                .option(CoreOptions.WRITE_ONLY.key(), "true")
                                .option(CoreOptions.MERGE_ENGINE.key(), "partial-update")
                                .build());
        IOManager ioManager = IOManager.create(tempDir.toString() + "/io");
        LocalTableQuery query = table.newLocalTableQuery().withIOManager(ioManager);
        StreamTableScan scan = fileMonitorScan(table);
        try {
            write(table, GenericRow.of(1, 10, 20));
            compact(table, ioManager, true);
            refresh(query, scan);

            // The level-0 record carries a null for the untouched column. Lookup has to keep
            // the merged value from the compacted file.
            write(table, GenericRow.of(1, null, 99));
            refresh(query, scan);
            InternalRow row = lookup(query, 1);
            assertThat(row).isNotNull();
            assertThat(row.isNullAt(1)).isFalse();
            assertThat(row.getInt(1)).isEqualTo(10);
            assertThat(row.getInt(2)).isEqualTo(20);
        } finally {
            query.close();
            ioManager.close();
        }
    }

    @Test
    public void testSequenceFieldKeepsNewerValue() throws Exception {
        FileStoreTable table =
                createTable(
                        Schema.newBuilder()
                                .column("k", DataTypes.INT())
                                .column("seq", DataTypes.INT())
                                .column("v", DataTypes.INT())
                                .primaryKey("k")
                                .option(CoreOptions.BUCKET.key(), "1")
                                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                                .option(CoreOptions.WRITE_ONLY.key(), "true")
                                .option(CoreOptions.SEQUENCE_FIELD.key(), "seq")
                                .build());
        IOManager ioManager = IOManager.create(tempDir.toString() + "/io");
        LocalTableQuery query = table.newLocalTableQuery().withIOManager(ioManager);
        StreamTableScan scan = fileMonitorScan(table);
        try {
            write(table, GenericRow.of(1, 100, 10));
            compact(table, ioManager, true);
            refresh(query, scan);

            write(table, GenericRow.of(1, 50, 999));
            refresh(query, scan);
            InternalRow row = lookup(query, 1);
            assertThat(row).isNotNull();
            assertThat(row.getInt(1)).isEqualTo(100);
            assertThat(row.getInt(2)).isEqualTo(10);
        } finally {
            query.close();
            ioManager.close();
        }
    }

    private FileStoreTable createTable() throws Exception {
        return createTable(
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .build());
    }

    private FileStoreTable createTable(Schema schema) throws Exception {
        catalog.createTable(identifier, schema, true);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private StreamTableScan fileMonitorScan(FileStoreTable table) {
        return table.copy(
                        Collections.singletonMap(
                                CoreOptions.STREAM_SCAN_MODE.key(),
                                CoreOptions.StreamScanMode.FILE_MONITOR.toString()))
                .newReadBuilder()
                .newStreamScan();
    }

    private long snapshotId(Split split) {
        if (split instanceof DataSplit) {
            return ((DataSplit) split).snapshotId();
        } else if (split instanceof IncrementalSplit) {
            return ((IncrementalSplit) split).snapshotId();
        }
        throw new IllegalStateException(split.getClass().getName());
    }

    private InternalRow lookup(LocalTableQuery query, int key) throws Exception {
        return query.lookup(BinaryRow.EMPTY_ROW, 0, GenericRow.of(key));
    }

    private void write(FileStoreTable table, GenericRow row) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        try {
            write.write(row);
            writeBuilder.newCommit().commit(write.prepareCommit());
        } finally {
            write.close();
        }
    }

    private void compact(FileStoreTable table, IOManager ioManager, boolean fullCompaction)
            throws Exception {
        FileStoreTable compactTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        BatchWriteBuilder writeBuilder = compactTable.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.compact(BinaryRow.EMPTY_ROW, 0, fullCompaction);
            commit.commit(write.prepareCommit());
        }
    }

    private void refresh(LocalTableQuery query, StreamTableScan scan) {
        while (true) {
            List<Split> splits = scan.plan().splits();
            if (splits.isEmpty()) {
                return;
            }
            refreshSplits(query, splits);
        }
    }

    private void refreshSplits(LocalTableQuery query, List<Split> splits) {
        for (Split split : splits) {
            if (split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                query.refreshFiles(
                        dataSplit.partition(),
                        dataSplit.bucket(),
                        Collections.<DataFileMeta>emptyList(),
                        dataSplit.dataFiles(),
                        dataSplit.snapshotId());
            } else if (split instanceof IncrementalSplit) {
                IncrementalSplit incrementalSplit = (IncrementalSplit) split;
                query.refreshFiles(
                        incrementalSplit.partition(),
                        incrementalSplit.bucket(),
                        incrementalSplit.beforeFiles(),
                        incrementalSplit.afterFiles(),
                        incrementalSplit.snapshotId());
            } else {
                throw new IllegalStateException(split.getClass().getName());
            }
        }
    }

    private Integer value(LocalTableQuery query, int key) throws Exception {
        InternalRow row = lookup(query, key);
        if (row == null) {
            return null;
        }
        return row.getInt(1);
    }

    private List<IndexManifestEntry> deletionVectorIndex(FileStoreTable table) {
        return table.store()
                .newIndexFileHandler()
                .scanEntries(DeletionVectorsIndexFile.DELETION_VECTORS_INDEX, BinaryRow.EMPTY_ROW);
    }

    private long highLevelRowCount(FileStoreTable table) {
        long rowCount = 0L;
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            for (DataFileMeta file : dataSplit.dataFiles()) {
                rowCount += file.rowCount();
            }
        }
        return rowCount;
    }
}
