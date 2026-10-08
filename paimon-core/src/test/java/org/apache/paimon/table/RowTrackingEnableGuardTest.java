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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards around enabling row tracking on a table that already has snapshots: a writer that loaded
 * the table before the switch must not commit files without row ids, and the table must not be
 * rolled back to a snapshot whose files have none.
 */
public class RowTrackingEnableGuardTest extends TableTestBase {

    private static final Identifier TABLE = new Identifier("default", "t");

    @Test
    public void testStaleWriterIsRefusedAfterRowTrackingEnabled() throws Exception {
        FileStoreTable staleTable = createAppendTable();
        writeRows(staleTable, row(1, "a"));
        assertThat(firstRowIds(staleTable)).containsExactly((Long) null);

        enableRowTrackingDirectly(staleTable);

        assertThatThrownBy(() -> writeRows(staleTable, row(2, "b")))
                .hasStackTraceContaining("enabled row tracking in schema 1")
                .hasStackTraceContaining("Restart the writer");

        // the refused commit left nothing behind
        FileStoreTable reloaded = reload(staleTable);
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(1L);

        // a writer on the current schema commits and gets row ids
        writeRows(reloaded, row(2, "b"));
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(2L);
        List<Long> firstRowIds = firstRowIds(reloaded);
        assertThat(firstRowIds).hasSize(2);
        assertThat(firstRowIds).containsNull();
        assertThat(firstRowIds.stream().filter(id -> id != null).collect(Collectors.toList()))
                .containsExactly(0L);
    }

    @Test
    public void testStaleWriterKeepsCommittingAfterOrdinarySchemaChange() throws Exception {
        FileStoreTable staleTable = createAppendTable();
        writeRows(staleTable, row(1, "a"));

        staleTable.schemaManager().commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()));

        writeRows(staleTable, row(2, "b"));
        assertThat(reload(staleTable).snapshotManager().latestSnapshotId()).isEqualTo(2L);
    }

    @Test
    public void testWriterOnRowTrackingTableIsNeverRefused() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        FileStoreTable table = createAppendTable(options);
        writeRows(table, row(1, "a"));
        writeRows(table, row(2, "b"));
        assertThat(firstRowIds(table)).containsExactlyInAnyOrder(0L, 1L);
    }

    @Test
    public void testRollbackAcrossRowTrackingBoundaryIsRefused() throws Exception {
        FileStoreTable before = createAppendTable();
        writeRows(before, row(1, "a"));
        before.createTag("before", 1);
        enableRowTrackingDirectly(before);
        FileStoreTable table = reload(before);
        writeRows(table, row(2, "b"));
        writeRows(table, row(3, "c"));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(3L);

        assertThatThrownBy(() -> table.rollbackTo(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot roll back")
                .hasMessageContaining("before row tracking was enabled");
        assertThatThrownBy(() -> table.rollbackTo("before"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before row tracking was enabled");
        assertThatThrownBy(
                        () -> {
                            try (FileStoreCommitImpl commit =
                                    (FileStoreCommitImpl)
                                            table.store().newCommit(commitUser, table)) {
                                commit.rollbackToAsLatest(
                                        table.tagManager().getOrThrow("before").trimToSnapshot());
                            }
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before row tracking was enabled");

        // a snapshot committed with row tracking enabled is a valid rollback target
        table.rollbackTo(2);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(2L);
    }

    @Test
    public void testRollbackThroughTableLoadedBeforeRowTrackingIsRefused() throws Exception {
        // a table object loaded before row tracking was enabled still reports it as disabled: the
        // guards must go by the latest persisted schema
        FileStoreTable stale = createAppendTable();
        writeRows(stale, row(1, "a"));
        stale.createTag("before", 1);
        enableRowTrackingDirectly(stale);
        writeRows(reload(stale), row(2, "b"));
        assertThat(stale.coreOptions().rowTrackingEnabled()).isFalse();

        assertThatThrownBy(() -> stale.rollbackTo(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before row tracking was enabled");
        assertThatThrownBy(() -> stale.rollbackTo("before"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before row tracking was enabled");
        assertThatThrownBy(
                        () -> {
                            try (FileStoreCommitImpl commit =
                                    (FileStoreCommitImpl)
                                            stale.store().newCommit(commitUser, stale)) {
                                commit.rollbackToAsLatest(stale.snapshotManager().snapshot(1));
                            }
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before row tracking was enabled");

        // nothing was rolled back
        FileStoreTable table = reload(stale);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(2L);
        assertThat(table.tagManager().tagExists("before")).isTrue();
        assertThat(firstRowIds(table)).containsExactlyInAnyOrder(null, 0L);

        // a snapshot committed with row tracking is still a valid target through the same object
        writeRows(table, row(3, "c"));
        stale.rollbackTo(2);
        assertThat(reload(stale).snapshotManager().latestSnapshotId()).isEqualTo(2L);
    }

    @Test
    public void testRollbackOnPlainAppendTableIsUnaffected() throws Exception {
        FileStoreTable table = createAppendTable();
        writeRows(table, row(1, "a"));
        writeRows(table, row(2, "b"));
        table.rollbackTo(1);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
    }

    @Test
    public void testReplaceManifestListWithSchemaId() throws Exception {
        FileStoreTable table = createAppendTable();
        writeRows(table, row(1, "a"));
        long newSchemaId =
                table.schemaManager()
                        .commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()))
                        .id();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.schemaId()).isEqualTo(0L);

        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit(commitUser, table)) {
            assertThat(
                            commit.replaceManifestList(
                                    latest,
                                    newSchemaId,
                                    latest.totalRecordCount(),
                                    Pair.of(
                                            latest.baseManifestList(),
                                            latest.baseManifestListSize()),
                                    Pair.of(
                                            latest.deltaManifestList(),
                                            latest.deltaManifestListSize()),
                                    latest.indexManifest(),
                                    42L,
                                    latest.properties()))
                    .isTrue();
        }

        Snapshot replaced = table.snapshotManager().latestSnapshot();
        assertThat(replaced.id()).isEqualTo(latest.id() + 1);
        assertThat(replaced.schemaId()).isEqualTo(newSchemaId);
        assertThat(replaced.nextRowId()).isEqualTo(42L);
        assertThat(replaced.baseManifestList()).isEqualTo(latest.baseManifestList());
        assertThat(replaced.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
    }

    private FileStoreTable createAppendTable() throws Exception {
        return createAppendTable(new HashMap<>());
    }

    private FileStoreTable createAppendTable(Map<String, String> options) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .options(options)
                        .build();
        catalog.createTable(TABLE, schema, false);
        return getTable(TABLE);
    }

    /** Writes a schema with row tracking enabled the way the conversion procedure will. */
    private void enableRowTrackingDirectly(FileStoreTable table) throws Exception {
        SchemaManager schemaManager = table.schemaManager();
        TableSchema latest = schemaManager.latest().get();
        Map<String, String> options = new HashMap<>(latest.options());
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        TableSchema enabled =
                new TableSchema(
                        latest.id() + 1,
                        latest.fields(),
                        latest.highestFieldId(),
                        latest.partitionKeys(),
                        latest.primaryKeys(),
                        options,
                        latest.comment());
        assertThat(schemaManager.commit(enabled)).isTrue();
    }

    private static FileStoreTable reload(FileStoreTable table) {
        return FileStoreTableFactory.create(table.fileIO(), table.location());
    }

    private static GenericRow row(int id, String v) {
        return GenericRow.of(id, BinaryString.fromString(v));
    }

    private List<Long> firstRowIds(FileStoreTable table) {
        List<Long> firstRowIds = new ArrayList<>();
        table.newSnapshotReader()
                .readFileIterator()
                .forEachRemaining(entry -> firstRowIds.add(entry.file().firstRowId()));
        return firstRowIds;
    }

    private void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
    }
}
