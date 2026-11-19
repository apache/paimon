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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.AuditLogTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for deletion-vector changes to shared files in primary-key snapshot diffs. */
class IncrementalDeletionVectorDiffTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeletionOnRetainedDataFile(boolean deletionVectors) throws Exception {
        FileStoreTable table = createTable(deletionVectors);
        commit(table, 0, GenericRow.of(1, 10), GenericRow.of(2, 20));
        long before = table.snapshotManager().latestSnapshotId();
        assertThat(read(table))
                .containsExactlyInAnyOrder(GenericRow.of(1, 10), GenericRow.of(2, 20));
        commit(table, 1, GenericRow.ofKind(RowKind.DELETE, 1, 10));
        assertThat(read(table)).containsExactly(GenericRow.of(2, 20));

        FileStoreTable diff = diffFrom(table, before);
        assertThat(read(new AuditLogTable(diff)))
                .containsExactly(GenericRow.of(BinaryString.fromString("-D"), 1, 10));
        assertThat(read(diff)).isEmpty();
    }

    @Test
    void testRepeatedDeletionDoesNotRepeatEarlierChange() throws Exception {
        FileStoreTable table = createTable(true);
        commit(table, 0, GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 30));
        commit(table, 1, GenericRow.ofKind(RowKind.DELETE, 1, 10));
        long before = table.snapshotManager().latestSnapshotId();
        commit(table, 2, GenericRow.ofKind(RowKind.DELETE, 2, 20));
        assertThat(read(table)).containsExactly(GenericRow.of(3, 30));
        assertThat(readDiff(diffFrom(table, before)))
                .containsExactly(GenericRow.ofKind(RowKind.DELETE, 2, 20));
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 11})
    void testUpdateRetainsOnlyChangedValues(int updatedValue) throws Exception {
        FileStoreTable table = createTable(true);
        commit(table, 0, GenericRow.of(1, 10), GenericRow.of(2, 20));
        long before = table.snapshotManager().latestSnapshotId();
        commit(table, 1, GenericRow.of(1, updatedValue));
        assertThat(read(table))
                .containsExactlyInAnyOrder(GenericRow.of(1, updatedValue), GenericRow.of(2, 20));
        List<InternalRow> changes = readDiff(diffFrom(table, before));
        if (updatedValue == 10) {
            assertThat(changes).isEmpty();
        } else {
            assertThat(changes).containsExactly(GenericRow.of(1, 11));
        }
    }

    @Test
    void testUnchangedDeletionVectorsStillPruneCommonFiles() throws Exception {
        FileStoreTable table = createTable(true);
        commit(table, 0, GenericRow.of(1, 10), GenericRow.of(2, 20));
        commit(table, 1, GenericRow.ofKind(RowKind.DELETE, 1, 10));
        long before = table.snapshotManager().latestSnapshotId();
        commit(table, 2, GenericRow.of(3, 30));
        FileStoreTable diff = diffFrom(table, before);
        TableScan.Plan plan = diff.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        IncrementalSplit split = (IncrementalSplit) plan.splits().get(0);
        assertThat(split.beforeFiles()).isEmpty();
        assertThat(split.afterFiles()).hasSize(1);
        assertThat(readDiff(diff)).containsExactly(GenericRow.of(3, 30));
    }

    private FileStoreTable createTable(boolean deletionVectors) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", String.valueOf(deletionVectors))
                        .build();
        catalog.createTable(identifier(), schema, false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private void commit(FileStoreTable table, long identifier, GenericRow... rows)
            throws Exception {
        try (StreamTableWrite write = table.newWrite(commitUser).withIOManager(ioManager);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(identifier, write.prepareCommit(true, identifier));
        }
    }

    private FileStoreTable diffFrom(FileStoreTable table, long before) {
        Map<String, String> options = new HashMap<>();
        options.put(
                "incremental-between", before + "," + table.snapshotManager().latestSnapshotId());
        options.put("incremental-between-scan-mode", "diff");
        return table.copy(options);
    }

    private List<InternalRow> readDiff(FileStoreTable table) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        try (RecordReader<InternalRow> reader =
                table.newRead().forceKeepDelete().createReader(table.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        }
        return rows;
    }
}
