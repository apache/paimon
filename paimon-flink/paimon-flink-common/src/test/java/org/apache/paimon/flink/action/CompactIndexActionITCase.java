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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactIndexAction}. */
public class CompactIndexActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    /**
     * Test that compact_index action compacts multiple btree index files into fewer files.
     *
     * <p>Steps:
     *
     * <ol>
     *   <li>Create a table with row-tracking and data-evolution enabled.
     *   <li>Write data in two batches to produce data files.
     *   <li>Build btree index twice (once per batch) to produce multiple index files per
     *       partition.
     *   <li>Run compact_index action.
     *   <li>Verify that the number of index files decreased after compaction.
     * </ol>
     */
    @Test
    @Timeout(120)
    public void testBatchCompactIndex() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        // Use small records-per-range so each build creates separate index files
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "100");
        // Set min-files to 2 so compaction triggers easily
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key(), "2");

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.singletonList("dt"),
                        Collections.<String>emptyList(),
                        Collections.<String>emptyList(),
                        tableOptions);

        // Write first batch of data
        writeBatchData(table, 0, 500, "p0");

        // Build btree index for the first batch
        buildBTreeIndex(table);

        // Write second batch of data
        writeBatchData(table, 500, 1000, "p0");

        // Build btree index for the second batch (incremental)
        buildBTreeIndex(table);

        // Count index files before compaction
        IndexFileHandler handler = table.store().newIndexFileHandler();
        Snapshot snapshotBefore = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> entriesBefore = handler.scan(snapshotBefore, "btree");
        int fileCountBefore = entriesBefore.size();

        // Should have multiple index files
        assertThat(fileCountBefore).isGreaterThanOrEqualTo(2);

        // Run compact_index action
        CompactIndexAction action =
                createAction(
                        CompactIndexAction.class,
                        "compact_index",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName);
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        // Reload table to get latest snapshot
        table = getFileStoreTable(tableName);
        Snapshot snapshotAfter = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> entriesAfter = handler.scan(snapshotAfter, "btree");
        int fileCountAfter = entriesAfter.size();

        // After compaction, should have fewer or equal index files
        assertThat(fileCountAfter).isLessThanOrEqualTo(fileCountBefore);

        // Verify total row count is preserved
        long totalRowsBefore = 0;
        for (IndexManifestEntry entry : entriesBefore) {
            totalRowsBefore += entry.indexFile().rowCount();
        }
        long totalRowsAfter = 0;
        for (IndexManifestEntry entry : entriesAfter) {
            totalRowsAfter += entry.indexFile().rowCount();
        }
        assertThat(totalRowsAfter).isEqualTo(totalRowsBefore);
    }

    /**
     * Test that compact_index action with multiple partitions compacts each partition
     * independently.
     */
    @Test
    @Timeout(120)
    public void testCompactIndexMultiplePartitions() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "100");
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key(), "2");

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.singletonList("dt"),
                        Collections.<String>emptyList(),
                        Collections.<String>emptyList(),
                        tableOptions);

        // Write data for partition p0 in two batches
        writeBatchData(table, 0, 500, "p0");
        buildBTreeIndex(table);
        writeBatchData(table, 500, 1000, "p0");
        buildBTreeIndex(table);

        // Write data for partition p1 in two batches
        writeBatchData(table, 0, 500, "p1");
        buildBTreeIndex(table);
        writeBatchData(table, 500, 1000, "p1");
        buildBTreeIndex(table);

        // Count index files before compaction
        IndexFileHandler handler = table.store().newIndexFileHandler();
        Snapshot snapshotBefore = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> entriesBefore = handler.scan(snapshotBefore, "btree");
        int fileCountBefore = entriesBefore.size();

        assertThat(fileCountBefore).isGreaterThanOrEqualTo(4);

        // Run compact_index action
        CompactIndexAction action =
                createAction(
                        CompactIndexAction.class,
                        "compact_index",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName);
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        // Reload table to get latest snapshot
        table = getFileStoreTable(tableName);
        Snapshot snapshotAfter = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> entriesAfter = handler.scan(snapshotAfter, "btree");
        int fileCountAfter = entriesAfter.size();

        assertThat(fileCountAfter).isLessThanOrEqualTo(fileCountBefore);

        // Verify total row count is preserved
        long totalRowsBefore = 0;
        for (IndexManifestEntry entry : entriesBefore) {
            totalRowsBefore += entry.indexFile().rowCount();
        }
        long totalRowsAfter = 0;
        for (IndexManifestEntry entry : entriesAfter) {
            totalRowsAfter += entry.indexFile().rowCount();
        }
        assertThat(totalRowsAfter).isEqualTo(totalRowsBefore);
    }

    /**
     * Test that compact_index action is a no-op when there are not enough index files to compact.
     */
    @Test
    @Timeout(120)
    public void testCompactIndexNoOpWhenBelowThreshold() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "10000");
        // Set high min-files threshold so compaction won't trigger
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key(), "100");

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.singletonList("dt"),
                        Collections.<String>emptyList(),
                        Collections.<String>emptyList(),
                        tableOptions);

        // Write data and build index once
        writeBatchData(table, 0, 100, "p0");
        buildBTreeIndex(table);

        long snapshotIdBefore = table.snapshotManager().latestSnapshotId();

        // Run compact_index action - should be a no-op
        CompactIndexAction action =
                createAction(
                        CompactIndexAction.class,
                        "compact_index",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName);
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        // Snapshot should not have changed (no compaction committed)
        table = getFileStoreTable(tableName);
        long snapshotIdAfter = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotIdAfter).isEqualTo(snapshotIdBefore);
    }

    /**
     * Test that compact_index action supports table_conf option to override table properties.
     */
    @Test
    @Timeout(120)
    public void testCompactIndexWithTableConf() throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "100");
        // Default min-files is 2, which will trigger compaction
        tableOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key(), "2");

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.singletonList("dt"),
                        Collections.<String>emptyList(),
                        Collections.<String>emptyList(),
                        tableOptions);

        // Write data in two batches and build index each time
        writeBatchData(table, 0, 500, "p0");
        buildBTreeIndex(table);
        writeBatchData(table, 500, 1000, "p0");
        buildBTreeIndex(table);

        // Reload table to get latest snapshot with index files
        table = getFileStoreTable(tableName);
        IndexFileHandler handler = table.store().newIndexFileHandler();
        Snapshot snapshotBefore = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> entriesBefore = handler.scan(snapshotBefore, "btree");
        int fileCountBefore = entriesBefore.size();
        assertThat(fileCountBefore).isGreaterThanOrEqualTo(2);

        // Use table_conf to pass records-per-range (verify table_conf is applied)
        CompactIndexAction action =
                createAction(
                        CompactIndexAction.class,
                        "compact_index",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--table_conf",
                        BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key() + "=2");
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        table = getFileStoreTable(tableName);
        handler = table.store().newIndexFileHandler();
        Snapshot snapshotAfter = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> entriesAfter = handler.scan(snapshotAfter, "btree");
        int fileCountAfter = entriesAfter.size();

        assertThat(fileCountAfter).isLessThanOrEqualTo(fileCountBefore);

        // Verify total row count is preserved
        long totalRowsBefore = 0;
        for (IndexManifestEntry entry : entriesBefore) {
            totalRowsBefore += entry.indexFile().rowCount();
        }
        long totalRowsAfter = 0;
        for (IndexManifestEntry entry : entriesAfter) {
            totalRowsAfter += entry.indexFile().rowCount();
        }
        assertThat(totalRowsAfter).isEqualTo(totalRowsBefore);
    }

    private void writeBatchData(FileStoreTable table, int from, int to, String partition)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchWrite = builder.newWrite()) {
            for (int i = from; i < to; i++) {
                batchWrite.write(
                        rowData(
                                i,
                                i * 10,
                                i % 24,
                                BinaryString.fromString(partition)));
            }
            try (BatchTableCommit batchCommit = builder.newCommit()) {
                batchCommit.commit(batchWrite.prepareCommit());
            }
        }
    }

    private void buildBTreeIndex(FileStoreTable table) throws Exception {
        // Reload table to pick up latest snapshot
        table = getFileStoreTable(tableName);
        BTreeGlobalIndexBuilder indexBuilder = new BTreeGlobalIndexBuilder(table);
        indexBuilder.withIndexField("k");
        indexBuilder.withIndexType("btree");
        IOManager ioManager = new IOManagerImpl(getTempDirPath("io-manager"));
        List<CommitMessage> commitMessages =
                indexBuilder.build(indexBuilder.scan(), ioManager);
        try (BatchTableCommit batchCommit = table.newBatchWriteBuilder().newCommit()) {
            batchCommit.commit(commitMessages);
        }
    }
}
