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

package org.apache.paimon.flink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Flink SQL DELETE on Data Evolution tables. */
public class DataEvolutionDeleteSqlITCase extends CatalogITCaseBase {

    @Test
    public void testDeleteRowsWithoutRewritingDataFiles() throws Exception {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'A'), (3, 'three', 'A')");
        sql("INSERT INTO T VALUES (4, 'four', 'B'), (5, 'five', 'B'), (6, 'six', 'B')");

        FileStoreTable table = paimonTable("T");
        List<String> originalFiles = plannedFiles(table);

        sql("DELETE FROM T WHERE id IN (2, 4)");

        assertThat(sql("SELECT id, name, dt FROM T ORDER BY id"))
                .containsExactly(
                        Row.of(1, "one", "A"),
                        Row.of(3, "three", "A"),
                        Row.of(5, "five", "B"),
                        Row.of(6, "six", "B"));
        assertDeleteSnapshot(table, originalFiles, 2L);

        sql("DELETE FROM T WHERE id IN (2, 3)");

        assertThat(sql("SELECT id, name, dt FROM T ORDER BY id"))
                .containsExactly(
                        Row.of(1, "one", "A"), Row.of(5, "five", "B"), Row.of(6, "six", "B"));
        assertDeleteSnapshot(table, originalFiles, 3L);
    }

    @Test
    public void testMaterializeDeletionVectorsProcedure() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'A'), (3, 'three', 'A')");
        sql("INSERT INTO T VALUES (4, 'four', 'B'), (5, 'five', 'B'), (6, 'six', 'B')");
        sql("DELETE FROM T WHERE id IN (2, 4)");

        FileStoreTable table = paimonTable("T");
        assertThat(deletionVectorCardinality(table)).isEqualTo(2L);

        sql("CALL sys.materialize_deletion_vectors(`table` => 'default.T')");

        table = paimonTable("T");
        assertThat(deletionVectorCardinality(table)).isZero();
        assertThat(sql("SELECT id, name, dt FROM T ORDER BY id"))
                .containsExactly(
                        Row.of(1, "one", "A"),
                        Row.of(3, "three", "A"),
                        Row.of(5, "five", "B"),
                        Row.of(6, "six", "B"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"global-sort", "local-sort"})
    public void testIncrementalClusteringProcedurePreservesUnselectedPartitionIndexes(String mode)
            throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();
        sql(
                "ALTER TABLE T SET ("
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', "
                        + "'clustering.columns' = 'id', "
                        + "'clustering.incremental.mode' = '"
                        + mode
                        + "', "
                        + "'scan.parallelism' = '2', "
                        + "'source.split.target-size' = '1 b', "
                        + "'sink.parallelism' = '1', "
                        + "'num-sorted-run.compaction-trigger' = '10')");
        // Interleaved source ranges distinguish the range shuffle from per-reader local sorting.
        sql("INSERT INTO T VALUES (3, 'three', 'A'), (1, 'one', 'A'), (5, 'five', 'A')");
        updateFirstGroupNames(paimonTable("T"));
        sql("INSERT INTO T VALUES (6, 'six', 'A'), (4, 'four', 'A'), (2, 'two', 'A')");
        sql("INSERT INTO T VALUES (7, 'seven', 'B')");
        sql("DELETE FROM T WHERE id = 2");
        createGlobalIndex();

        FileStoreTable table = paimonTable("T");
        long nextRowId = table.latestSnapshot().get().nextRowId();
        long beforeSnapshot = table.latestSnapshot().get().id();
        assertThat(globalIndexPartitions(table)).containsExactly("A", "B");

        sql("CALL sys.compact(`table` => 'default.T', partitions => 'dt=A')");
        assertThat(table.latestSnapshot().get().id()).isEqualTo(beforeSnapshot);
        sql(
                "CALL sys.compact(`table` => 'default.T', partitions => 'dt=A', "
                        + "compact_strategy => 'full')");

        assertThat(sql("SELECT id, name, dt FROM T ORDER BY id"))
                .containsExactly(
                        Row.of(1, "updated-one", "A"),
                        Row.of(3, "updated-three", "A"),
                        Row.of(4, "four", "A"),
                        Row.of(5, "updated-five", "A"),
                        Row.of(6, "six", "A"),
                        Row.of(7, "seven", "B"));
        assertThat(sql("SELECT _ROW_ID FROM T$row_tracking " + "WHERE dt = 'A' ORDER BY _ROW_ID"))
                .containsExactly(
                        Row.of(nextRowId),
                        Row.of(nextRowId + 1),
                        Row.of(nextRowId + 2),
                        Row.of(nextRowId + 3),
                        Row.of(nextRowId + 4));
        assertClusteredFileOrder(table, mode);
        assertThat(deletionVectorCardinality(table)).isZero();
        assertThat(globalIndexPartitions(table)).containsExactly("B");
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'full')");
        assertThat(sql("SELECT id FROM T WHERE name = 'updated-three'")).containsExactly(Row.of(3));
    }

    @Test
    public void testIncrementalClusteringRemovesFullyDeletedPartition() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();
        sql(
                "ALTER TABLE T SET ("
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', "
                        + "'clustering.columns' = 'id', "
                        + "'scan.parallelism' = '2')");
        sql("INSERT INTO T VALUES (3, 'three', 'A'), (1, 'one', 'A'), (2, 'two', 'A')");
        sql("INSERT INTO T VALUES (4, 'four', 'B')");
        createGlobalIndex();
        sql("DELETE FROM T WHERE id IN (1, 2, 3)");
        FileStoreTable table = paimonTable("T");
        assertThat(deletionVectorCardinality(table)).isEqualTo(3L);
        long nextRowId = table.latestSnapshot().get().nextRowId();

        sql(
                "CALL sys.compact(`table` => 'default.T', partitions => 'dt=A', "
                        + "compact_strategy => 'full')");

        assertThat(sql("SELECT id, name, dt FROM T")).containsExactly(Row.of(4, "four", "B"));
        assertThat(table.newScan().plan().splits())
                .allSatisfy(
                        split ->
                                assertThat(((DataSplit) split).partition().getString(0).toString())
                                        .isEqualTo("B"));
        assertThat(deletionVectorCardinality(table)).isZero();
        assertThat(globalIndexPartitions(table)).containsExactly("B");
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(nextRowId);
    }

    @Test
    public void testMaterializeDeletionVectorsForEmptyTable() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();

        assertThat(sql("CALL sys.materialize_deletion_vectors(`table` => 'default.T')"))
                .containsExactly(Row.of("Success"));
        assertThat(paimonTable("T").latestSnapshot()).isEmpty();
    }

    @Test
    public void testCompactRejectsLegacyRowIdRewriteForEmptyTable() {
        createTable();

        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.compact(`table` => 'default.T', "
                                                + "options => 'data-evolution.compaction.rewrite-row-ids=true')"))
                .hasRootCauseMessage(
                        "Option 'data-evolution.compaction.rewrite-row-ids=true' is no longer supported. "
                                + "Data evolution compaction preserves row IDs and logical deletions. "
                                + "Use the 'materialize_deletion_vectors' procedure to apply deletion "
                                + "vectors to the latest table state and assign new row IDs.");
    }

    @Test
    public void testDeleteFromEmptyTable() throws Exception {
        createTable();

        sql("DELETE FROM T WHERE id = 1");

        assertThat(sql("SELECT * FROM T")).isEmpty();
        assertThat(paimonTable("T").latestSnapshot()).isEmpty();
    }

    @Test
    public void testDeleteRequiresDeletionVectors() {
        createTable();
        sql("ALTER TABLE T RESET ('deletion-vectors.enabled')");

        assertThatThrownBy(() -> sql("DELETE FROM T WHERE id = 1"))
                .hasRootCauseMessage(
                        "Data-evolution delete requires deletion-vectors.enabled to be true.");
    }

    @Test
    public void testResetDeletionVectorsAfterDelete() throws Exception {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'A'), (3, 'three', 'A')");
        sql("DELETE FROM T WHERE id = 2");

        assertThat(deletionVectorCardinality(paimonTable("T"))).isEqualTo(1L);
        assertThatThrownBy(() -> sql("ALTER TABLE T RESET ('deletion-vectors.enabled')"))
                .hasRootCauseMessage(
                        "Cannot change deletion vectors mode from true to false. If modifying table "
                                + "deletion-vectors mode without full-compaction, this may result in data "
                                + "duplication. If you are confident, you can set table option "
                                + "'deletion-vectors.modifiable' = 'true' to allow deletion vectors modification.");
        assertThat(sql("SELECT id FROM T ORDER BY id")).containsExactly(Row.of(1), Row.of(3));
    }

    @Test
    public void testDeleteWithSubquery() {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'A'), (3, 'three', 'A')");
        sql("CREATE TABLE S (id INT)");
        sql("INSERT INTO S VALUES (2), (3)");

        sql("DELETE FROM T WHERE id IN (SELECT id FROM S)");

        assertThat(sql("SELECT id, name, dt FROM T")).containsExactly(Row.of(1, "one", "A"));
    }

    @Test
    public void testDeleteWithSameTableBranchSubquery() {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'A')");
        sql("CALL sys.create_tag('default.T', 'tag1')");
        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");
        sql("INSERT INTO `T$branch_test` VALUES (3, 'three', 'A')");

        assertThat(sql("SELECT id FROM `T$branch_test` WHERE id = 2")).containsExactly(Row.of(2));

        sql("DELETE FROM T WHERE id IN (SELECT id FROM `T$branch_test` WHERE id = 2)");

        assertThat(sql("SELECT id FROM T ORDER BY id")).containsExactly(Row.of(1));
        assertThat(sql("SELECT id FROM `T$branch_test` ORDER BY id"))
                .containsExactly(Row.of(1), Row.of(2), Row.of(3));
    }

    @Test
    public void testDeleteWithSameTableTagSubquery() {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'A')");
        sql("CALL sys.create_tag('default.T', 'tag1')");
        sql("INSERT INTO T VALUES (3, 'three', 'A')");

        sql(
                "DELETE FROM T WHERE id IN (SELECT id FROM T "
                        + "/*+ OPTIONS('scan.tag-name'='tag1') */ WHERE id = 2)");

        assertThat(sql("SELECT id FROM T ORDER BY id")).containsExactly(Row.of(1), Row.of(3));
    }

    @Test
    public void testDeletePartitionRemovesGlobalIndex() throws Exception {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'B')");
        createGlobalIndex();

        FileStoreTable table = paimonTable("T");
        assertThat(globalIndexPartitions(table)).containsExactlyInAnyOrder("A", "B");

        sql("DELETE FROM T WHERE dt = 'A'");

        assertThat(sql("SELECT id, name, dt FROM T")).containsExactly(Row.of(2, "two", "B"));
        assertThat(table.latestSnapshot().get().operation()).isEqualTo(Snapshot.Operation.TRUNCATE);
        assertThat(globalIndexPartitions(table)).containsExactly("B");
    }

    @Test
    public void testDeleteWholeTableRemovesGlobalIndex() throws Exception {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'B')");
        createGlobalIndex();

        FileStoreTable table = paimonTable("T");
        assertThat(globalIndexPartitions(table)).containsExactlyInAnyOrder("A", "B");

        sql("DELETE FROM T");

        assertThat(sql("SELECT * FROM T")).isEmpty();
        assertThat(table.latestSnapshot().get().operation()).isEqualTo(Snapshot.Operation.TRUNCATE);
        assertThat(plannedFiles(table)).isEmpty();
        assertThat(table.store().newIndexFileHandler().scanEntries()).isEmpty();
    }

    @Test
    public void testDeleteUsesFullScalarIndexSearch() {
        createTable();
        sql("INSERT INTO T VALUES (1, 'old', 'A')");
        createGlobalIndex();
        sql("INSERT INTO T VALUES (2, 'new', 'A')");
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'fast')");

        sql("DELETE FROM T WHERE name = 'new'");

        assertThat(sql("SELECT id, name, dt FROM T")).containsExactly(Row.of(1, "old", "A"));
    }

    @SuppressWarnings("unchecked")
    private static void updateFirstGroupNames(FileStoreTable table) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        BinaryRow partition = ((DataSplit) table.newScan().plan().splits().get(0)).partition();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType().project(Arrays.asList("name", "dt")));
                BatchTableCommit commit = builder.newCommit()) {
            // Partial rows do not contain the partition field at its full-schema position.
            FileStoreWrite<InternalRow> fileStoreWrite =
                    ((TableWriteImpl<InternalRow>) write).getWrite();
            for (String name : new String[] {"three", "one", "five"}) {
                fileStoreWrite.write(
                        partition,
                        0,
                        GenericRow.of(
                                BinaryString.fromString("updated-" + name),
                                BinaryString.fromString("A")));
            }
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                List<DataFileMeta> files =
                        ((CommitMessageImpl) message).newFilesIncrement().newFiles();
                List<DataFileMeta> assigned =
                        files.stream()
                                .map(file -> file.assignFirstRowId(0L))
                                .collect(Collectors.toList());
                files.clear();
                files.addAll(assigned);
            }
            commit.commit(messages);
        }
    }

    private static void assertClusteredFileOrder(FileStoreTable table, String mode)
            throws Exception {
        List<DataFileMeta> files = new ArrayList<>();
        List<List<Integer>> fileIds = new ArrayList<>();
        for (Split split : table.newScan().plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            if (!dataSplit.partition().getString(0).toString().equals("A")) {
                continue;
            }
            for (DataFileMeta file : dataSplit.dataFiles()) {
                files.add(file);
                DataSplit fileSplit =
                        DataSplit.builder()
                                .withSnapshot(dataSplit.snapshotId())
                                .withPartition(dataSplit.partition())
                                .withBucket(dataSplit.bucket())
                                .withBucketPath(dataSplit.bucketPath())
                                .withDataFiles(Collections.singletonList(file))
                                .build();
                List<Integer> ids = new ArrayList<>();
                try (RecordReader<InternalRow> reader = table.newRead().createReader(fileSplit)) {
                    reader.forEachRemaining(row -> ids.add(row.getInt(0)));
                }
                assertThat(ids).isNotEmpty().isSorted();
                fileIds.add(ids);
            }
        }
        // Verify ordering across actual output files, as well as within each file.
        assertThat(files).hasSizeGreaterThanOrEqualTo(2);
        assertThat(files).allSatisfy(file -> assertThat(file.level()).isGreaterThan(0));
        fileIds.sort(Comparator.comparingInt(ids -> ids.get(0)));
        boolean overlappingRanges = false;
        for (int i = 1; i < fileIds.size(); i++) {
            List<Integer> previous = fileIds.get(i - 1);
            overlappingRanges |= previous.get(previous.size() - 1) >= fileIds.get(i).get(0);
        }
        if (mode.equals("global-sort")) {
            assertThat(overlappingRanges)
                    .as("global-sort output file ranges must not overlap")
                    .isFalse();
        } else {
            assertThat(overlappingRanges)
                    .as("local-sort preserves the interleaved input ranges across readers")
                    .isTrue();
        }
    }

    private void createGlobalIndex() {
        sql(
                "CALL sys.create_global_index(`table` => 'default.T', "
                        + "index_column => 'name', index_type => 'btree')");
    }

    private void createTable() {
        sql(
                "CREATE TABLE T (id INT, name STRING, dt STRING) PARTITIONED BY (dt) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'sink.parallelism' = '2')");
    }

    private static List<String> plannedFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(entry -> entry.file().fileName())
                .sorted()
                .collect(Collectors.toList());
    }

    private static long deletionVectorCardinality(FileStoreTable table) {
        Snapshot snapshot = table.latestSnapshot().get();
        return table.store().newIndexFileHandler().scan(snapshot, DELETION_VECTORS_INDEX).stream()
                .map(IndexManifestEntry::indexFile)
                .filter(index -> index.dvRanges() != null)
                .flatMap(index -> index.dvRanges().values().stream())
                .mapToLong(DeletionVectorMeta::cardinality)
                .sum();
    }

    private static List<String> globalIndexPartitions(FileStoreTable table) {
        return table.store().newIndexFileHandler().scan("btree").stream()
                .map(entry -> entry.partition().getString(0).toString())
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    private static void assertDeleteSnapshot(
            FileStoreTable table, List<String> originalFiles, long deletedRows) {
        assertThat(table.latestSnapshot().get().operation()).isEqualTo(Snapshot.Operation.DELETE);
        assertThat(plannedFiles(table)).containsExactlyElementsOf(originalFiles);
        assertThat(deletionVectorCardinality(table)).isEqualTo(deletedRows);
    }
}
