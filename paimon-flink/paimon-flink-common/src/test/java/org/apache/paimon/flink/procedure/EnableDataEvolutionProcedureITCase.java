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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.action.EnableDataEvolutionAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link EnableDataEvolutionProcedure} and {@link EnableDataEvolutionAction}. */
public class EnableDataEvolutionProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testProcedure() throws Exception {
        sql("CREATE TABLE T (id INT, v STRING)");
        sql("INSERT INTO T VALUES (1, 'a'), (2, 'b')");
        sql("INSERT INTO T VALUES (3, 'c')");
        FileStoreTable before = paimonTable("T");
        assertThat(before.coreOptions().dataEvolutionEnabled()).isFalse();

        assertThat(sql("CALL sys.enable_data_evolution(`table` => 'default.T', dry_run => true)"))
                .hasSize(1)
                .allSatisfy(row -> assertThat(row.getField(0).toString()).startsWith("Dry run."));
        assertThat(paimonTable("T").coreOptions().dataEvolutionEnabled()).isFalse();

        assertThat(sql("CALL sys.enable_data_evolution('default.T')"))
                .hasSize(1)
                .allSatisfy(
                        row -> {
                            String result = row.getField(0).toString();
                            assertThat(result).startsWith("Success.");
                            assertThat(result).contains("schema 0 -> 1");
                            assertThat(result).contains("nextRowId=3");
                        });

        FileStoreTable table = paimonTable("T");
        assertThat(table.coreOptions().rowTrackingEnabled()).isTrue();
        assertThat(table.coreOptions().dataEvolutionEnabled()).isTrue();
        assertThat(sql("SELECT id, v, _ROW_ID FROM T$row_tracking ORDER BY id"))
                .containsExactly(Row.of(1, "a", 0L), Row.of(2, "b", 1L), Row.of(3, "c", 2L));

        // rows written afterwards continue the row id sequence
        sql("INSERT INTO T VALUES (4, 'd')");
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking WHERE id = 4"))
                .containsExactly(Row.of(4, 3L));

        assertThat(sql("CALL sys.enable_data_evolution('default.T')"))
                .allSatisfy(row -> assertThat(row.getField(0).toString()).startsWith("Skipped."));
    }

    @Test
    public void testProcedureRefusesPrimaryKeyTable() {
        sql(
                "CREATE TABLE T (id INT, v STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket' = '1')");
        assertThatThrownBy(() -> sql("CALL sys.enable_data_evolution('default.T')"))
                .hasStackTraceContaining("Cannot enable data evolution on table default.T")
                .hasStackTraceContaining("primary-key");
    }

    @Test
    public void testAlterTableCannotEnableRowTracking() {
        sql("CREATE TABLE T (id INT, v STRING)");
        assertThatThrownBy(() -> sql("ALTER TABLE T SET ('row-tracking.enabled' = 'true')"))
                .hasStackTraceContaining(
                        "Cannot enable 'row-tracking.enabled' on an existing table")
                .hasStackTraceContaining("sys.enable_data_evolution");

        sql("INSERT INTO T VALUES (1, 'a')");
        assertThatThrownBy(() -> sql("ALTER TABLE T SET ('data-evolution.enabled' = 'true')"))
                .hasStackTraceContaining("Change 'data-evolution.enabled' is not supported yet");

        // the procedure converts it
        sql("CALL sys.enable_data_evolution('default.T')");
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking")).containsExactly(Row.of(1, 0L));
    }

    @Test
    public void testAction() throws Exception {
        sql("CREATE TABLE T (id INT, v STRING)");
        sql("INSERT INTO T VALUES (1, 'a')");

        new EnableDataEvolutionAction(
                        Collections.singletonMap("warehouse", path), "default", "T", false)
                .executeLocally();

        // the action ran through its own catalog instance: reload the table from disk
        FileStoreTable table = paimonTable("T");
        FileStoreTable reloaded = FileStoreTableFactory.create(table.fileIO(), table.location());
        assertThat(reloaded.coreOptions().dataEvolutionEnabled()).isTrue();
        assertThat(reloaded.schema().id()).isEqualTo(1L);
        assertThat(reloaded.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(1L);
    }

    @Test
    public void testWriterLoadedBeforeConversionIsRefused() throws Exception {
        sql("CREATE TABLE T (id INT, v STRING)");
        sql("INSERT INTO T VALUES (1, 'a')");
        // a long-running job holds a table instance loaded before the conversion
        FileStoreTable staleTable = paimonTable("T");

        sql("CALL sys.enable_data_evolution('default.T')");

        assertThatThrownBy(() -> write(staleTable, 2, "b"))
                .hasStackTraceContaining("enabled row tracking in schema 1")
                .hasStackTraceContaining("Restart the writer");
        // after reloading the table, the same rows commit and get row ids
        write(paimonTable("T"), 2, "b");
        assertThat(sql("SELECT id, _ROW_ID FROM T$row_tracking ORDER BY id"))
                .containsExactly(Row.of(1, 0L), Row.of(2, 1L));
    }

    @Test
    public void testStreamingReadContinuesAcrossConversion() throws Exception {
        sql("CREATE TABLE T (id INT, v STRING)");
        sql("INSERT INTO T VALUES (1, 'a')");

        // Two readers started before the conversion, which commits an OVERWRITE snapshot assigning
        // the row ids and an empty APPEND snapshot fencing old writers. The default reader skips
        // OVERWRITE snapshots, the other reads their delta, which the conversion leaves empty.
        // Neither may replay the table or stall on the conversion snapshots.
        BlockingIterator<Row, Row> skipping = streamSqlBlockIter("SELECT * FROM T");
        BlockingIterator<Row, Row> readingOverwrite =
                streamSqlBlockIter(
                        "SELECT * FROM T /*+ OPTIONS('streaming-read-append-overwrite' = 'true') */");
        try {
            assertThat(skipping.collect(1)).containsExactly(Row.of(1, "a"));
            assertThat(readingOverwrite.collect(1)).containsExactly(Row.of(1, "a"));

            sql("CALL sys.enable_data_evolution('default.T')");
            sql("INSERT INTO T VALUES (2, 'b')");

            assertThat(skipping.collect(1)).containsExactly(Row.of(2, "b"));
            assertThat(readingOverwrite.collect(1)).containsExactly(Row.of(2, "b"));
        } finally {
            skipping.close();
            readingOverwrite.close();
        }
    }

    private static void write(FileStoreTable table, int id, String v) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(v)));
            commit.commit(write.prepareCommit());
        }
    }
}
