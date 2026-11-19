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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionRangeSortTask;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests for sorting and rewriting complete Data Evolution file groups. */
public class DataEvolutionRangeSortTest extends TableTestBase {

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
        write(original, GenericRow.of(2));
        Snapshot first = original.latestSnapshot().get();
        catalog.alterTable(identifier(), SchemaChange.addColumn("label", DataTypes.INT()), false);
        FileStoreTable evolved = (FileStoreTable) catalog.getTable(identifier());
        assertThat(plan(evolved, first)).hasSize(1);
        write(evolved, GenericRow.of(1, 42));
        Snapshot latest = evolved.latestSnapshot().get();

        assertThatThrownBy(() -> plan(original, latest))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("older than the schema of planning snapshot");

        FileStoreTable table =
                evolved.copyWithoutTimeTravel(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(), Long.toString(first.id())));
        DataEvolutionCompactTask task = plan(table, latest).get(0);
        commitSort(table, latest, task.doCompact(table, commitUser));
        assertThat(read(evolved))
                .extracting(row -> row.isNullAt(1) ? null : row.getInt(1))
                .containsExactly(42, null);
    }

    @Test
    public void testSortUsesWorkerSpillDirectory() throws Exception {
        FileStoreTable table = createTable();
        int count = 2048;
        write(
                table,
                IntStream.rangeClosed(1, count)
                        .mapToObj(i -> GenericRow.of(count - i))
                        .toArray(InternalRow[]::new));
        Snapshot snapshot = table.latestSnapshot().get();
        DataEvolutionCompactTask task = plan(table, snapshot).get(0);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_BUFFER_SIZE.key(), "16 kb");
        options.put(CoreOptions.PAGE_SIZE.key(), "4 kb");
        Path scratch = Files.createDirectory(tempPath.resolve("sort-spill"));
        IOManager io = spy(IOManager.create(scratch.toString()));
        CommitMessage message = task.doCompact(table.copy(options), commitUser, () -> io);
        verify(io, atLeastOnce()).createBufferFileWriter(any());
        assertThat(scratch.toFile().list()).isEmpty();
        commitSort(table, snapshot, message);
        assertThat(read(table))
                .extracting(row -> row.getInt(0))
                .containsExactlyElementsOf(
                        IntStream.range(0, count).boxed().collect(Collectors.toList()));
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
            commit.materializeDvRowIdCheck()
                    .commit(
                            Collections.singletonList(
                                    ((CommitMessageImpl) message)
                                            .withCheckFromSnapshot(snapshot.id())));
        }
    }

    private static List<DataEvolutionCompactTask> plan(FileStoreTable table, Snapshot snapshot) {
        return DataEvolutionRangeSortTask.plan(
                table,
                snapshot,
                new Range(0, Long.MAX_VALUE),
                null,
                Collections.singletonList("id"));
    }
}
