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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
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
    @CsvSource({
        "global-sort,",
        "local-sort,",
        "global-sort,precommit-compact=true",
        "local-sort,sink.coordinator-commit.enabled=true"
    })
    public void testIncrementalClusteringProcedurePreservesUnselectedPartitionIndexes(
            String mode, String ingestionOption) throws Exception {
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
        // Keep more distinct rows than shuffle ranges so duplicate maximum sample boundaries do
        // not randomly send every row to one writer. Both readers still see overlapping ranges.
        List<String> oddValues = new ArrayList<>();
        List<String> evenValues = new ArrayList<>();
        for (int id = 39; id >= 1; id -= 2) {
            oddValues.add("(" + id + ", 'value-" + id + "', 'A')");
            evenValues.add("(" + (id + 1) + ", 'value-" + (id + 1) + "', 'A')");
        }
        sql("INSERT INTO T VALUES " + String.join(", ", oddValues));
        updateFirstGroupNames(paimonTable("T"));
        sql("INSERT INTO T VALUES " + String.join(", ", evenValues));
        sql("INSERT INTO T VALUES (41, 'value-41', 'B')");
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
                        + "compact_strategy => 'full'"
                        + (ingestionOption == null ? "" : ", options => '" + ingestionOption + "'")
                        + ")");

        List<Row> expectedRows = new ArrayList<>();
        List<Row> expectedRowIds = new ArrayList<>();
        for (int id = 1; id <= 40; id++) {
            if (id != 2) {
                expectedRows.add(Row.of(id, (id % 2 == 1 ? "updated-" : "value-") + id, "A"));
                expectedRowIds.add(Row.of(nextRowId + expectedRowIds.size()));
            }
        }
        expectedRows.add(Row.of(41, "value-41", "B"));
        assertThat(sql("SELECT id, name, dt FROM T ORDER BY id"))
                .containsExactlyElementsOf(expectedRows);
        assertThat(sql("SELECT _ROW_ID FROM T$row_tracking " + "WHERE dt = 'A' ORDER BY _ROW_ID"))
                .containsExactlyElementsOf(expectedRowIds);
        assertClusteredFileOrder(table, mode);
        assertThat(deletionVectorCardinality(table)).isZero();
        assertThat(globalIndexPartitions(table)).containsExactly("A", "B");
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'full')");
        assertThat(sql("SELECT id FROM T WHERE name = 'updated-3'")).containsExactly(Row.of(3));
    }

    @ParameterizedTest
    @CsvSource({"btree,global-sort", "btree,local-sort", "bitmap,global-sort", "bitmap,local-sort"})
    public void testIncrementalClusteringRemapsScalarIndexRowIds(String indexType, String mode)
            throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();
        sql(
                "ALTER TABLE T SET ("
                        + "'write-only' = 'true', "
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', "
                        + "'clustering.columns' = 'id', "
                        + "'clustering.incremental.mode' = '"
                        + mode
                        + "', 'scan.parallelism' = '2', "
                        + "'source.split.target-size' = '1 b', "
                        + "'sink.parallelism' = '1', 'target-file-row-num' = '7')");
        List<String> odds = new ArrayList<>();
        List<String> evens = new ArrayList<>();
        for (int id = 39; id >= 1; id -= 2) {
            odds.add(
                    "("
                            + id
                            + ", "
                            + (id == 9 ? "CAST(NULL AS STRING)" : "'key-" + id + "'")
                            + ", 'A')");
            evens.add("(" + (id + 1) + ", 'key-" + (id + 1) + "', 'A')");
        }
        sql("INSERT INTO T VALUES " + String.join(", ", odds));
        sql("INSERT INTO T VALUES " + String.join(", ", evens));
        sql("INSERT INTO T VALUES (41, 'key-41', 'B')");
        sql(
                "CALL sys.create_global_index(`table` => 'default.T', "
                        + "index_column => 'name', index_type => '"
                        + indexType
                        + "')");
        // The old postings still contain these rows; the clustering mapping must omit them.
        sql("DELETE FROM T WHERE id IN (2, 11)");
        FileStoreTable table = paimonTable("T");
        List<String> oldAIndexes = scalarIndexFiles(table, indexType, "A");
        List<String> oldBIndexes = scalarIndexFiles(table, indexType, "B");
        assertThat(oldAIndexes).isNotEmpty();
        assertThat(oldBIndexes).isNotEmpty();
        long beforeSnapshot = table.latestSnapshot().get().id();
        long firstNewRowId = table.latestSnapshot().get().nextRowId();

        sql(
                "CALL sys.compact(`table` => 'default.T', partitions => 'dt=A', "
                        + "compact_strategy => 'full')");

        // Data and remapped indexes become visible atomically, without a second index-build job.
        assertThat(table.latestSnapshot().get().id()).isEqualTo(beforeSnapshot + 1);
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(scalarIndexFiles(table, indexType, "A"))
                .isNotEmpty()
                .doesNotContainAnyElementsOf(oldAIndexes);
        assertThat(scalarIndexFiles(table, indexType, "B")).isEqualTo(oldBIndexes);
        assertThat(
                        table.store().newIndexFileHandler().scan(indexType).stream()
                                .filter(
                                        entry ->
                                                entry.partition()
                                                        .getString(0)
                                                        .toString()
                                                        .equals("A")))
                .allSatisfy(
                        entry ->
                                assertThat(entry.indexFile().globalIndexMeta().rowRangeStart())
                                        .isGreaterThanOrEqualTo(firstNewRowId));
        assertThat(sql("SELECT _ROW_ID FROM T$row_tracking WHERE dt = 'A'"))
                .hasSize(38)
                .allSatisfy(
                        row ->
                                assertThat((Long) row.getField(0))
                                        .isGreaterThanOrEqualTo(firstNewRowId));

        // FAST searches only indexed rows, so data-scan fallback cannot hide missing postings.
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'fast')");
        assertThat(sql("SELECT id FROM T WHERE name IN ('key-5', 'key-2', 'key-11') ORDER BY id"))
                .containsExactly(Row.of(5));
        assertThat(sql("SELECT id FROM T WHERE name IS NULL")).containsExactly(Row.of(9));
        List<Row> negativeMatches = new ArrayList<>();
        for (int id = 1; id <= 40; id++) {
            if (id != 2 && id != 11 && id != 5 && id != 9) {
                negativeMatches.add(Row.of(id));
            }
        }
        assertThat(sql("SELECT id FROM T WHERE dt = 'A' AND name <> 'key-5' ORDER BY id"))
                .containsExactlyElementsOf(negativeMatches);
    }

    @ParameterizedTest
    @CsvSource({"btree,global-sort", "bitmap,local-sort"})
    public void testIncrementalClusteringSkipsNonClusteringColumnUpdates(
            String indexType, String mode) throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();
        sql(
                "ALTER TABLE T SET ("
                        + "'write-only' = 'true', "
                        + "'global-index.column-update-action' = 'ignore', "
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', "
                        + "'clustering.columns' = 'id', "
                        + "'clustering.incremental.mode' = '"
                        + mode
                        + "', 'scan.parallelism' = '1', 'sink.parallelism' = '1')");
        sql("INSERT INTO T VALUES (3, 'three', 'A'), (1, 'one', 'A'), (2, 'two', 'A')");
        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");
        sql(
                "CALL sys.create_global_index(`table` => 'default.T', "
                        + "index_column => 'id', index_type => '"
                        + indexType
                        + "')");
        FileStoreTable table = paimonTable("T");
        List<String> indexes = scalarIndexFiles(table, indexType, "A");
        assertThat(indexes).isNotEmpty();
        List<String> clusteredFiles = plannedFiles(table);
        assertThat(clusteredFiles).hasSize(1);
        long firstRowId = table.store().newScan().plan().files().get(0).file().nonNullFirstRowId();
        List<Row> originalIds = sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id");

        writePartialGroup(table, "name", firstRowId, "updated-one", "updated-two", "updated-three");
        Snapshot payloadSnapshot = table.latestSnapshot().get();
        List<String> payloadFiles = plannedFiles(table);
        assertThat(payloadFiles).hasSize(2);
        assertThat(payloadSnapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");

        assertThat(table.latestSnapshot().get().id()).isEqualTo(payloadSnapshot.id());
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(payloadSnapshot.nextRowId());
        assertThat(plannedFiles(table)).isEqualTo(payloadFiles);
        assertThat(scalarIndexFiles(table, indexType, "A")).isEqualTo(indexes);
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id"))
                .containsExactlyElementsOf(originalIds);
        // The unchanged id index must still find rows whose other columns have newer versions.
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'fast')");
        assertThat(sql("SELECT id, name FROM T WHERE id IN (1, 3) ORDER BY id"))
                .containsExactly(Row.of(1, "updated-one"), Row.of(3, "updated-three"));

        writePartialGroup(table, "id", firstRowId, 30, 10, 20);
        Snapshot sortKeySnapshot = table.latestSnapshot().get();
        List<String> beforeRewrite = plannedFiles(table);
        assertThat(beforeRewrite).hasSize(3);
        long nextRowId = sortKeySnapshot.nextRowId();

        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");

        assertThat(table.latestSnapshot().get().id()).isEqualTo(sortKeySnapshot.id() + 1);
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(nextRowId + 3);
        assertThat(plannedFiles(table)).doesNotContainAnyElementsOf(beforeRewrite);
        assertThat(sql("SELECT id, name, dt FROM T ORDER BY id"))
                .containsExactly(
                        Row.of(10, "updated-two", "A"),
                        Row.of(20, "updated-three", "A"),
                        Row.of(30, "updated-one", "A"));
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id"))
                .containsExactly(
                        Row.of(10, nextRowId),
                        Row.of(20, nextRowId + 1),
                        Row.of(30, nextRowId + 2));
        // The sort-key update also changed indexed values, so old keys cannot be reused.
        assertThat(scalarIndexFiles(table, indexType, "A")).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"btree,global-sort", "bitmap,local-sort"})
    public void testIncrementalClusteringCompactsRepeatedColumnUpdates(
            String indexType, String mode) throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();
        sql(
                "ALTER TABLE T SET ('write-only' = 'true', "
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', 'clustering.columns' = 'id', "
                        + "'clustering.incremental.mode' = '"
                        + mode
                        + "', 'compaction.min.file-num' = '5', "
                        + "'scan.parallelism' = '1', 'sink.parallelism' = '1')");
        sql("INSERT INTO T VALUES (3, 'three', 'A'), (1, 'one', 'A'), (2, 'two', 'A')");
        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");
        createIdIndex(indexType);
        sql("DELETE FROM T WHERE id = 2");

        FileStoreTable table = paimonTable("T");
        DataFileMeta base = table.store().newScan().plan().files().get(0).file();
        List<String> indexes = scalarIndexFiles(table, indexType, "A");
        List<Row> originalIds = sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id");
        for (int version = 0; version < 4; version++) {
            writePartialGroup(
                    table,
                    "name",
                    base.nonNullFirstRowId(),
                    "one-" + version,
                    "two-" + version,
                    "three-" + version);
        }
        Snapshot before = table.latestSnapshot().get();
        List<String> beforeFiles = plannedFiles(table);
        assertThat(beforeFiles).hasSize(5);

        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");

        Snapshot compacted = table.latestSnapshot().get();
        assertThat(compacted.id()).isEqualTo(before.id() + 1);
        assertThat(compacted.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(compacted.nextRowId()).isEqualTo(before.nextRowId());
        List<DataFileMeta> afterFiles =
                table.store().newScan().plan().files().stream()
                        .map(entry -> entry.file())
                        .collect(Collectors.toList());
        assertThat(afterFiles)
                .singleElement()
                .satisfies(
                        file -> {
                            assertThat(file.nonNullRowIdRange())
                                    .isEqualTo(base.nonNullRowIdRange());
                            assertThat(file.level()).isEqualTo(base.level());
                            assertThat(file.rowCount()).isEqualTo(3);
                        });
        assertThat(plannedFiles(table)).doesNotContainAnyElementsOf(beforeFiles);
        assertThat(scalarIndexFiles(table, indexType, "A")).isEqualTo(indexes);
        assertThat(deletionVectorCardinality(table)).isEqualTo(1L);
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id"))
                .containsExactlyElementsOf(originalIds);
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'fast')");
        assertThat(sql("SELECT id, name FROM T WHERE id IN (1, 2, 3) ORDER BY id"))
                .containsExactly(Row.of(1, "one-3"), Row.of(3, "three-3"));

        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");

        assertThat(table.latestSnapshot().get().id()).isEqualTo(compacted.id());
        assertThat(plannedFiles(table)).containsExactly(afterFiles.get(0).fileName());
        assertThat(scalarIndexFiles(table, indexType, "A")).isEqualTo(indexes);

        // The compacted file contains all columns, but its id values still predate the index.
        // Retaining per-column sequences lets later clustering reuse that valid index.
        sql("INSERT INTO T VALUES (4, 'four', 'A')");
        Snapshot beforeClustering = table.latestSnapshot().get();

        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");

        assertThat(table.latestSnapshot().get().id()).isEqualTo(beforeClustering.id() + 1);
        assertThat(table.latestSnapshot().get().nextRowId())
                .isEqualTo(beforeClustering.nextRowId() + 3);
        assertThat(scalarIndexFiles(table, indexType, "A"))
                .isNotEmpty()
                .doesNotContainAnyElementsOf(indexes);
        assertThat(sql("SELECT id, name FROM T WHERE id IN (1, 2, 3) ORDER BY id"))
                .containsExactly(Row.of(1, "one-3"), Row.of(3, "three-3"));
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id"))
                .containsExactly(
                        Row.of(1, beforeClustering.nextRowId()),
                        Row.of(3, beforeClustering.nextRowId() + 1),
                        Row.of(4, beforeClustering.nextRowId() + 2));
        assertThat(deletionVectorCardinality(table)).isZero();
    }

    @Test
    public void testIncrementalClusteringCombinesCompactionInSamePartition() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        createTable();
        sql(
                "ALTER TABLE T SET ('write-only' = 'true', "
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', 'clustering.columns' = 'id', "
                        + "'clustering.incremental.mode' = 'global-sort', "
                        + "'compaction.min.file-num' = '5', "
                        + "'num-sorted-run.compaction-trigger' = '2', "
                        + "'compaction.max-size-amplification-percent' = '10000', "
                        + "'compaction.size-ratio' = '0', 'file.compression' = 'none', "
                        + "'target-file-row-num' = '3', "
                        + "'scan.parallelism' = '1', 'sink.parallelism' = '1')");
        FileStoreTable table = paimonTable("T");
        // A large clustered run must stay outside the minor clustering candidate. Its column
        // versions still require normal compaction in the same partition and commit.
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : new int[] {3, 1, 2}) {
                Random random = new Random(id);
                StringBuilder name = new StringBuilder(65536);
                for (int i = 0; i < 65536; i++) {
                    name.append((char) ('a' + random.nextInt(26)));
                }
                write.write(
                        GenericRow.of(
                                id,
                                BinaryString.fromString(name.toString()),
                                BinaryString.fromString("A")));
            }
            commit.commit(write.prepareCommit());
        }
        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");
        createIdIndex("btree");
        sql("DELETE FROM T WHERE id = 2");
        DataFileMeta base = table.store().newScan().plan().files().get(0).file();
        List<String> retainedIndexes = scalarIndexFiles(table, "btree", "A");
        List<Row> originalIds = sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id");
        for (int version = 0; version < 4; version++) {
            writePartialGroup(
                    table,
                    "name",
                    base.nonNullFirstRowId(),
                    "one-" + version,
                    "two-" + version,
                    "three-" + version);
        }
        sql("INSERT INTO T VALUES (6, 'six', 'A'), (4, 'four', 'A'), (5, 'five', 'A')");
        sql("INSERT INTO T VALUES (9, 'nine', 'A'), (7, 'seven', 'A'), (8, 'eight', 'A')");
        createIdIndex("btree");
        List<String> allIndexes = scalarIndexFiles(table, "btree", "A");
        assertThat(allIndexes)
                .containsAll(retainedIndexes)
                .hasSizeGreaterThan(retainedIndexes.size());
        List<String> rewrittenIndexes = new ArrayList<>(allIndexes);
        rewrittenIndexes.removeAll(retainedIndexes);
        Snapshot before = table.latestSnapshot().get();
        List<String> beforeFiles = plannedFiles(table);
        assertThat(beforeFiles).hasSize(7);

        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'minor')");

        Snapshot compacted = table.latestSnapshot().get();
        assertThat(compacted.id()).isEqualTo(before.id() + 1);
        assertThat(compacted.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(compacted.nextRowId()).isEqualTo(before.nextRowId() + 6);
        List<DataFileMeta> afterFiles =
                table.store().newScan().plan().files().stream()
                        .map(entry -> entry.file())
                        .collect(Collectors.toList());
        assertThat(afterFiles).hasSize(3);
        assertThat(afterFiles)
                .filteredOn(file -> file.nonNullFirstRowId() == base.nonNullFirstRowId())
                .singleElement()
                .satisfies(
                        file -> {
                            assertThat(file.level()).isEqualTo(base.level());
                            assertThat(file.nonNullRowIdRange())
                                    .isEqualTo(base.nonNullRowIdRange());
                        });
        assertThat(plannedFiles(table)).doesNotContainAnyElementsOf(beforeFiles);
        assertThat(scalarIndexFiles(table, "btree", "A"))
                .containsAll(retainedIndexes)
                .doesNotContainAnyElementsOf(rewrittenIndexes);
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking WHERE id <= 3 ORDER BY id"))
                .containsExactlyElementsOf(originalIds);
        assertThat(sql("SELECT _ROW_ID FROM T$row_tracking WHERE id >= 4"))
                .hasSize(6)
                .allSatisfy(
                        row ->
                                assertThat((Long) row.getField(0))
                                        .isGreaterThanOrEqualTo(before.nextRowId()));
        assertThat(deletionVectorCardinality(table)).isEqualTo(1L);
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'fast')");
        assertThat(sql("SELECT id, name FROM T WHERE id IN (1, 2, 3, 4, 9) ORDER BY id"))
                .containsExactly(
                        Row.of(1, "one-3"),
                        Row.of(3, "three-3"),
                        Row.of(4, "four"),
                        Row.of(9, "nine"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"global-sort", "local-sort"})
    public void testIncrementalClusteringPreservesInlineBlobViews(String mode) throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE upstream_blob (id INT, picture BYTES) WITH ("
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'blob-field' = 'picture', 'sink.parallelism' = '1')");
        sql("INSERT INTO upstream_blob VALUES (3, X'03'), (1, X'01'), (5, X'05')");
        sql("INSERT INTO upstream_blob VALUES (6, X'06'), (4, X'04'), (2, CAST(NULL AS BYTES))");
        sql(
                "CREATE TABLE blob_views (id INT, image_ref BYTES) WITH ("
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'blob-view-field' = 'image_ref', "
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', 'clustering.columns' = 'id', "
                        + "'clustering.incremental.mode' = '"
                        + mode
                        + "', 'scan.parallelism' = '2', 'source.split.target-size' = '1 b', "
                        + "'sink.parallelism' = '1')");
        for (String ids : new String[] {"1, 3, 5", "2, 4, 6"}) {
            sql(
                    "INSERT INTO blob_views SELECT id, "
                            + "sys.blob_view('default.upstream_blob', 'picture', _ROW_ID) "
                            + "FROM `upstream_blob$row_tracking` WHERE id IN ("
                            + ids
                            + ")");
        }
        String rawReferences =
                "SELECT id, image_ref FROM blob_views "
                        + "/*+ OPTIONS('blob-view.resolve.enabled'='false') */ ORDER BY id";
        List<Row> references = sql(rawReferences);
        assertThat(references).hasSize(6);
        assertThat(references).allSatisfy(row -> assertThat(row.getField(1)).isNotNull());
        FileStoreTable table = paimonTable("blob_views");
        long nextRowId = table.latestSnapshot().get().nextRowId();

        sql("CALL sys.compact(`table` => 'default.blob_views', compact_strategy => 'full')");

        // A currently null upstream value must not erase the non-null stored reference.
        assertThat(sql(rawReferences)).containsExactlyElementsOf(references);
        assertThat(sql("SELECT id, image_ref FROM blob_views ORDER BY id"))
                .containsExactly(
                        Row.of(1, new byte[] {1}),
                        Row.of(2, null),
                        Row.of(3, new byte[] {3}),
                        Row.of(4, new byte[] {4}),
                        Row.of(5, new byte[] {5}),
                        Row.of(6, new byte[] {6}));
        assertThat(sql("SELECT _ROW_ID FROM blob_views$row_tracking ORDER BY _ROW_ID"))
                .containsExactly(
                        Row.of(nextRowId),
                        Row.of(nextRowId + 1),
                        Row.of(nextRowId + 2),
                        Row.of(nextRowId + 3),
                        Row.of(nextRowId + 4),
                        Row.of(nextRowId + 5));
    }

    @ParameterizedTest
    @CsvSource({"global-sort,parquet", "local-sort,parquet", "global-sort,row", "local-sort,row"})
    public void testIncrementalClusteringPreservesInlineVectors(String mode, String format)
            throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE vectors (id INT, embedding ARRAY<FLOAT>) WITH ("
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'file.format' = '"
                        + format
                        + "', "
                        + "'vector-field' = 'embedding', 'field.embedding.vector-dim' = '3', "
                        + "'clustering.incremental' = 'true', "
                        + "'clustering.strategy' = 'order', 'clustering.columns' = 'id', "
                        + "'clustering.incremental.mode' = '"
                        + mode
                        + "', 'scan.parallelism' = '2', 'source.split.target-size' = '1 b', "
                        + "'sink.parallelism' = '1')");
        sql(
                "INSERT INTO vectors VALUES "
                        + "(3, CAST(ARRAY[0, 3, 4] AS ARRAY<FLOAT>)), "
                        + "(1, CAST(ARRAY[0, 1, 2] AS ARRAY<FLOAT>)), "
                        + "(5, CAST(ARRAY[0, 5, 6] AS ARRAY<FLOAT>))");
        sql(
                "INSERT INTO vectors VALUES "
                        + "(6, CAST(ARRAY[0, 6, 7] AS ARRAY<FLOAT>)), "
                        + "(4, CAST(ARRAY[0, 4, 5] AS ARRAY<FLOAT>)), "
                        + "(2, CAST(NULL AS ARRAY<FLOAT>))");
        FileStoreTable table = paimonTable("vectors");
        long nextRowId = table.latestSnapshot().get().nextRowId();

        sql("CALL sys.compact(`table` => 'default.vectors', compact_strategy => 'full')");

        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newRead().createReader(table.newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            rows.add(
                                    row.getInt(0)
                                            + "|"
                                            + (row.isNullAt(1)
                                                    ? "null"
                                                    : Arrays.toString(
                                                            row.getVector(1).toFloatArray()))));
        }
        Collections.sort(rows);
        assertThat(rows)
                .containsExactly(
                        "1|[0.0, 1.0, 2.0]",
                        "2|null",
                        "3|[0.0, 3.0, 4.0]",
                        "4|[0.0, 4.0, 5.0]",
                        "5|[0.0, 5.0, 6.0]",
                        "6|[0.0, 6.0, 7.0]");
        assertThat(sql("SELECT _ROW_ID FROM vectors$row_tracking ORDER BY _ROW_ID"))
                .containsExactly(
                        Row.of(nextRowId),
                        Row.of(nextRowId + 1),
                        Row.of(nextRowId + 2),
                        Row.of(nextRowId + 3),
                        Row.of(nextRowId + 4),
                        Row.of(nextRowId + 5));
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

    private static void updateFirstGroupNames(FileStoreTable table) throws Exception {
        List<String> names = new ArrayList<>();
        for (int id = 39; id >= 1; id -= 2) {
            names.add("updated-" + id);
        }
        writePartialGroup(table, "name", 0L, names.toArray());
    }

    @SuppressWarnings("unchecked")
    private static void writePartialGroup(
            FileStoreTable table, String column, long firstRowId, Object... values)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        BinaryRow partition = ((DataSplit) table.newScan().plan().splits().get(0)).partition();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType().project(Arrays.asList(column, "dt")));
                BatchTableCommit commit = builder.newCommit()) {
            // Partial rows do not contain the partition field at its full-schema position.
            FileStoreWrite<InternalRow> fileStoreWrite =
                    ((TableWriteImpl<InternalRow>) write).getWrite();
            for (Object value : values) {
                fileStoreWrite.write(
                        partition,
                        0,
                        GenericRow.of(
                                value instanceof String
                                        ? BinaryString.fromString((String) value)
                                        : value,
                                BinaryString.fromString("A")));
            }
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                List<DataFileMeta> files =
                        ((CommitMessageImpl) message).newFilesIncrement().newFiles();
                List<DataFileMeta> assigned =
                        files.stream()
                                .map(file -> file.assignFirstRowId(firstRowId))
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

    private void createIdIndex(String indexType) {
        sql(
                "CALL sys.create_global_index(`table` => 'default.T', "
                        + "index_column => 'id', index_type => '"
                        + indexType
                        + "')");
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

    private static List<String> scalarIndexFiles(
            FileStoreTable table, String indexType, String partition) {
        return table.store().newIndexFileHandler().scan(indexType).stream()
                .filter(entry -> entry.partition().getString(0).toString().equals(partition))
                .map(entry -> entry.indexFile().fileName())
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
