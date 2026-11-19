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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionRangeShuffle;
import org.apache.paimon.append.dataevolution.DataEvolutionRangeSortPlan;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for sorting and rewriting complete Data Evolution file groups. */
public class DataEvolutionRangeSortTest extends TableTestBase {

    @Test
    public void testParseInclusiveRange() {
        assertThat(DataEvolutionRangeSortPlan.parseRange("[ 0, 0 ]")).isEqualTo(new Range(0, 0));
        assertThat(DataEvolutionRangeSortPlan.parseRange("[1,9223372036854775807]"))
                .isEqualTo(new Range(1, Long.MAX_VALUE));
        for (String invalid :
                Arrays.asList(
                        null,
                        "[1]",
                        "[1,2,3]",
                        "(1,2]",
                        "[-1,2]",
                        "[3,2]",
                        "[0,9223372036854775808]")) {
            assertThatThrownBy(() -> DataEvolutionRangeSortPlan.parseRange(invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testRowRangesStayOnPlannedSnapshot() throws Exception {
        FileStoreTable table = createTable();
        write(table, GenericRow.of(2), GenericRow.of(1));
        long snapshot = table.latestSnapshot().get().id();
        Table ranges = catalog.getTable(identifier(DEFAULT_TABLE_NAME + "$row_ranges"));
        List<String> fields = Arrays.asList("snapshot_id", "start_row_id", "end_row_id");
        ReadBuilder builder =
                ranges.newReadBuilder().withReadType(ranges.rowType().project(fields));
        TableScan.Plan plan = builder.newScan().plan();
        write(table, GenericRow.of(3));

        try (CloseableIterator<InternalRow> rows =
                builder.newRead().createReader(plan).toCloseableIterator()) {
            InternalRow row = rows.next();
            assertThat(row.getLong(0)).isEqualTo(snapshot);
            assertThat(row.getLong(1)).isZero();
            assertThat(row.getLong(2)).isOne();
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    public void testSortUsesSchemaCoveringPlanningSnapshot() throws Exception {
        FileStoreTable original = createTable();
        write(original, GenericRow.of(1));
        Snapshot first = original.latestSnapshot().get();
        catalog.alterTable(identifier(), SchemaChange.addColumn("label", DataTypes.INT()), false);
        FileStoreTable evolved = (FileStoreTable) catalog.getTable(identifier());
        assertThat(plan(evolved, first)).hasSize(1);
        write(evolved, GenericRow.of(2, 42));
        Snapshot latest = evolved.latestSnapshot().get();

        assertThatThrownBy(() -> plan(original, latest))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("older than the schema of planning snapshot");

        FileStoreTable table =
                evolved.copyWithoutTimeTravel(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(), Long.toString(first.id())));
        DataEvolutionRangeSortPlan plan = plan(table, latest).get(0);
        commitSort(table, latest, writePlannedRows(table, plan, "id", commitUser));
        assertThat(read(evolved))
                .extracting(row -> row.isNullAt(1) ? null : row.getInt(1))
                .containsExactly(null, 42);
    }

    @Test
    public void testRangeSortRequiresPlanningSnapshot() throws Exception {
        FileStoreTable table = createTable();
        write(table, GenericRow.of(1), GenericRow.of(2));
        Snapshot snapshot = table.latestSnapshot().get();
        CommitMessage message =
                writePlannedRows(table, plan(table, snapshot).get(0), "id", commitUser);
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            assertThatThrownBy(
                            () ->
                                    commit.dataEvolutionRangeSortCheck()
                                            .commit(Collections.singletonList(message)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("range-sort commit is missing its check-from snapshot");
        }
        assertThat(table.latestSnapshot().get().id()).isEqualTo(snapshot.id());
    }

    static CommitMessage writePlannedRows(
            FileStoreTable table,
            DataEvolutionRangeSortPlan plan,
            String sortField,
            String commitUser)
            throws Exception {
        DataEvolutionRangeShuffle exchange =
                new DataEvolutionRangeShuffle(table, Collections.singletonList(sortField));
        try (DataEvolutionRangeShuffle.ShardWriter writer = exchange.createWriter(commitUser)) {
            for (IndexedSplit split : plan.readSplits()) {
                try (CloseableIterator<InternalRow> rows =
                        exchange.read(split).toCloseableIterator()) {
                    while (rows.hasNext()) {
                        InternalRow row = rows.next();
                        writer.write(row.getBinary(0), row.getBinary(exchange.payloadIndex()));
                    }
                }
            }
            return DataEvolutionRangeShuffle.replacements(
                            Collections.singletonList(plan.inputFiles()), writer.finish())
                    .get(0);
        }
    }

    private FileStoreTable createTable() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build(),
                false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private void commitSort(FileStoreTable table, Snapshot snapshot, CommitMessage message)
            throws Exception {
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.dataEvolutionRangeSortCheck()
                    .commit(
                            Collections.singletonList(
                                    ((CommitMessageImpl) message)
                                            .withCheckFromSnapshot(snapshot.id())));
        }
    }

    private static List<DataEvolutionRangeSortPlan> plan(FileStoreTable table, Snapshot snapshot) {
        return DataEvolutionRangeSortPlan.plan(
                table,
                snapshot,
                new Range(0, Long.MAX_VALUE),
                null,
                Collections.singletonList("id"));
    }
}
