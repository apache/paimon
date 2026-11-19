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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests sorting and rewriting complete Data Evolution file groups through Flink SQL. */
public class SortDataEvolutionProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testSortSelectedGroups() throws Exception {
        createTable();
        insert(0, "(4), (3), (6)");
        insert(1, "(9)");
        insert(0, "(1), (2)");
        insert(0, "(7)");

        FileStoreTable table = paimonTable("T");
        Set<String> selected = files(table, true);
        Set<String> untouched = files(table, false);
        assertThat(selected)
                .anyMatch(f -> f.endsWith(".blob"))
                .anyMatch(f -> f.contains(".vector."));
        long snapshot = table.latestSnapshot().get().id();
        assertThatThrownBy(() -> sql("CALL sys.sort_data_evolution('default.T', '[1,5]', 'k,id')"))
                .hasStackTraceContaining("Select complete file groups");
        assertThat(table.latestSnapshot().get().id()).isEqualTo(snapshot);

        String sort =
                "CALL sys.sort_data_evolution(`table` => 'default.T', `range` => '[0,5]', "
                        + "order_by => 'k, id', `where` => 'p = 0')";
        assertThat(sql(sort)).containsExactly(Row.of("Success"));
        assertThat(
                        sql(
                                "SELECT _ROW_ID, id, k, CAST(payload AS STRING), v[1], v[2], p "
                                        + "FROM T$row_tracking ORDER BY _ROW_ID"))
                .containsExactly(
                        row(3L, 9, 1),
                        row(6L, 7, 0),
                        row(7L, 2, 0),
                        row(8L, 4, 0),
                        row(9L, 6, 0),
                        row(10L, 1, 0),
                        row(11L, 3, 0));
        Set<String> current = files(table, false);
        assertThat(files(table, true)).isEmpty();
        assertThat(current).containsAll(untouched).doesNotContainAnyElementsOf(selected);
        current.removeAll(untouched);
        assertThat(current)
                .anyMatch(f -> f.endsWith(".blob"))
                .anyMatch(f -> f.contains(".vector."));

        assertThat(sql(sort)).containsExactly(Row.of("Success"));
        assertThat(table.latestSnapshot().get().id()).isEqualTo(snapshot + 1);
    }

    @Test
    public void testSortRangeConnectedByBlobAndVector() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE T (id INT, payload BYTES, v ARRAY<FLOAT>) WITH ("
                        + "'row-tracking.enabled'='true', 'data-evolution.enabled'='true', "
                        + "'write-only'='true', 'sink.parallelism'='1', 'blob-field'='payload', "
                        + "'vector-field'='v', 'field.v.vector-dim'='2', 'vector.file.format'='parquet')");
        assertThat(sql("SELECT * FROM T$row_ranges")).isEmpty();
        for (int id = 7; id > 0; id -= 2) {
            sql(
                    "INSERT INTO T SELECT id, CAST(CAST(id AS STRING) AS BYTES), "
                            + "ARRAY[CAST(id AS FLOAT), CAST(-id AS FLOAT)] FROM (VALUES (%d), (%d)) AS s(id)",
                    id, id - 1);
        }
        FileStoreTable table = paimonTable("T");
        // Four normal groups [0,1], [2,3], [4,5], [6,7]. Each dedicated update
        // crosses a different boundary, connecting the first three transitively.
        writeDedicatedFile(
                table,
                "payload",
                1,
                new BlobData(new byte[] {'a'}),
                new BlobData(new byte[] {'b'}));
        writeDedicatedFile(
                table,
                "v",
                3,
                BinaryVector.fromPrimitiveArray(new float[] {104, -104}),
                BinaryVector.fromPrimitiveArray(new float[] {103, -103}));

        List<Row> ranges =
                sql(
                        "SELECT start_row_id, end_row_id, file_count, file_size_in_bytes, snapshot_id "
                                + "FROM T$row_ranges ORDER BY start_row_id");
        long snapshot = table.latestSnapshot().get().id();
        List<Row> sizes =
                sql(
                        "SELECT SUM(file_size_in_bytes) FROM T$files "
                                + "GROUP BY (first_row_id >= 6) ORDER BY (first_row_id >= 6)");
        assertThat(ranges)
                .containsExactly(
                        Row.of(0L, 5L, 11L, sizes.get(0).getField(0), snapshot),
                        Row.of(6L, 7L, 3L, sizes.get(1).getField(0), snapshot));
        Row selected = ranges.get(0);
        assertThat(
                        sql(
                                "CALL sys.sort_data_evolution('default.T', '[%d,%d]', 'id')",
                                selected.getField(0), selected.getField(1)))
                .containsExactly(Row.of("Success"));
        assertThat(
                        sql(
                                "SELECT _ROW_ID, id, CAST(payload AS STRING), v[1], v[2] "
                                        + "FROM T$row_tracking ORDER BY _ROW_ID"))
                .containsExactly(
                        Row.of(6L, 1, "1", 1F, -1F), Row.of(7L, 0, "0", 0F, 0F),
                        Row.of(8L, 2, "2", 2F, -2F), Row.of(9L, 3, "3", 103F, -103F),
                        Row.of(10L, 4, "4", 104F, -104F), Row.of(11L, 5, "b", 5F, -5F),
                        Row.of(12L, 6, "a", 6F, -6F), Row.of(13L, 7, "7", 7F, -7F));
        assertThat(sql("SELECT start_row_id, end_row_id FROM T$row_ranges ORDER BY start_row_id"))
                .containsExactly(Row.of(6L, 7L), Row.of(8L, 13L));
    }

    private static void writeDedicatedFile(
            FileStoreTable table, String column, long firstRowId, Object... values)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType()
                                                .project(Collections.singletonList(column)));
                BatchTableCommit commit = builder.newCommit()) {
            for (Object value : values) {
                write.write(GenericRow.of(value));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assertThat(messages).hasSize(1);
            List<DataFileMeta> files =
                    ((CommitMessageImpl) messages.get(0)).newFilesIncrement().newFiles();
            assertThat(files).hasSize(1);
            files.replaceAll(file -> file.assignFirstRowId(firstRowId));
            commit.commit(messages);
        }
    }

    @Test
    public void testValidateEmptyTable() throws Exception {
        createTable();
        assertThat(sql("CALL sys.sort_data_evolution('default.T', '[0,5]', 'k')"))
                .containsExactly(Row.of("Success"));
        assertThatThrownBy(() -> sql("CALL sys.sort_data_evolution('default.T', '[5,0]', 'k')"))
                .hasRootCauseMessage("Invalid inclusive row ID range [5, 0].");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.sort_data_evolution('default.T', '[0,5]', 'k', 'id = 1')"))
                .hasRootCauseMessage(
                        "Only partition key can be specialized in sort_data_evolution action.");
        assertThat(paimonTable("T").latestSnapshot()).isEmpty();
    }

    private void createTable() {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE T (id INT, k INT, payload BYTES, v ARRAY<FLOAT>, p INT) "
                        + "PARTITIONED BY (p) WITH ('row-tracking.enabled'='true', "
                        + "'data-evolution.enabled'='true', 'write-only'='true', 'sink.parallelism'='1', "
                        + "'blob-field'='payload', 'vector-field'='v', 'field.v.vector-dim'='2', "
                        + "'vector.file.format'='parquet')");
    }

    private void insert(int partition, String values) {
        sql(
                "INSERT INTO T SELECT id, MOD(id, 2), CAST(CAST(id AS STRING) AS BYTES), "
                        + "ARRAY[CAST(id AS FLOAT), CAST(-id AS FLOAT)], %d FROM (VALUES %s) AS s(id)",
                partition, values);
    }

    private static Row row(long rowId, int id, int partition) {
        return Row.of(rowId, id, id % 2, Integer.toString(id), (float) id, (float) -id, partition);
    }

    private static Set<String> files(FileStoreTable table, boolean selected) {
        return table.store().newScan().plan().files().stream()
                .filter(
                        e ->
                                (e.partition().getInt(0) == 0 && e.file().nonNullFirstRowId() <= 5)
                                        == selected)
                .map(e -> e.file().fileName())
                .collect(Collectors.toSet());
    }
}
