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

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

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
        assertThatThrownBy(() -> sql("CALL sys.sort_data_evolution('default.T', 1, 5, 'k,id')"))
                .hasStackTraceContaining("Select complete file groups");
        assertThat(table.latestSnapshot().get().id()).isEqualTo(snapshot);

        String sort =
                "CALL sys.sort_data_evolution(`table` => 'default.T', start_row_id => 0, "
                        + "end_row_id => 5, order_by => 'k, id', `where` => 'p = 0')";
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
    public void testValidateEmptyTable() throws Exception {
        createTable();
        assertThat(sql("CALL sys.sort_data_evolution('default.T', 0, 5, 'k')"))
                .containsExactly(Row.of("Success"));
        assertThatThrownBy(() -> sql("CALL sys.sort_data_evolution('default.T', 5, 0, 'k')"))
                .hasRootCauseMessage("Invalid inclusive row ID range [5, 0].");
        assertThatThrownBy(
                        () -> sql("CALL sys.sort_data_evolution('default.T', 0, 5, 'k', 'id = 1')"))
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
