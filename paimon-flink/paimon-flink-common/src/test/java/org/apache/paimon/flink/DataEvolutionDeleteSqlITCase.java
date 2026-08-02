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
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

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
    public void testDeleteWithSubquery() {
        createTable();
        sql("INSERT INTO T VALUES (1, 'one', 'A'), (2, 'two', 'A'), (3, 'three', 'A')");
        sql("CREATE TABLE S (id INT)");
        sql("INSERT INTO S VALUES (2), (3)");

        sql("DELETE FROM T WHERE id IN (SELECT id FROM S)");

        assertThat(sql("SELECT id, name, dt FROM T")).containsExactly(Row.of(1, "one", "A"));
    }

    @Test
    public void testDeleteUsesFullScalarIndexSearch() {
        createTable();
        sql("INSERT INTO T VALUES (1, 'old', 'A')");
        sql(
                "CALL sys.create_global_index(`table` => 'default.T', "
                        + "index_column => 'name', index_type => 'btree')");
        sql("INSERT INTO T VALUES (2, 'new', 'A')");
        sql("ALTER TABLE T SET ('scalar-index.search-mode' = 'fast')");

        sql("DELETE FROM T WHERE name = 'new'");

        assertThat(sql("SELECT id, name, dt FROM T")).containsExactly(Row.of(1, "old", "A"));
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

    private static void assertDeleteSnapshot(
            FileStoreTable table, List<String> originalFiles, long deletedRows) {
        assertThat(table.latestSnapshot().get().operation()).isEqualTo(Snapshot.Operation.DELETE);
        assertThat(plannedFiles(table)).containsExactlyElementsOf(originalFiles);
        assertThat(deletionVectorCardinality(table)).isEqualTo(deletedRows);
    }
}
