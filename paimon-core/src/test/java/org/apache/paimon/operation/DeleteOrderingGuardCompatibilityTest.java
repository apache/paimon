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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration coverage for guarded read endpoints and metadata upgrades. */
class DeleteOrderingGuardCompatibilityTest {

    private static final String USER = "guard-compatibility";
    private static final BinaryRow PARTITION = partition(1);
    private final List<IOManager> ioManagers = new ArrayList<>();

    @TempDir java.nio.file.Path tempDir;

    @AfterEach
    void closeIoManagers() throws Exception {
        IOUtils.closeAll(ioManagers);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIncrementalDiffHidesStaleRowsWithCaptureEnabledOrDisabled(boolean disableCapture)
            throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 4));
        compact(table, 1);
        Snapshot before = table.snapshotManager().latestSnapshot();
        table.createTag("before", before.id());
        if (disableCapture) {
            table = setOption(table, "delete-ordering-guards.enabled", "false");
        }
        append(table, 2, row(RowKind.INSERT, 1, 2));
        Snapshot after = table.snapshotManager().latestSnapshot();
        List<Split> splits =
                table.newSnapshotReader().withSnapshot(after).readIncrementalDiff(before).splits();
        assertThat(splits).hasSize(1);
        IncrementalSplit split = (IncrementalSplit) splits.get(0);
        assertThat(split.beforeDeleteOrderingGuards()).hasSize(1);
        assertThat(split.afterDeleteOrderingGuards()).hasSize(1);
        assertThat(read(table, splits, false)).isEmpty();
        assertThat(read(table, splits, true)).isEmpty();

        DataOutputSerializer bytes = new DataOutputSerializer(256);
        split.serialize(bytes);
        assertThat(new DataInputDeserializer(bytes.getCopyOfBuffer()).readInt()).isEqualTo(3);
        IncrementalSplit restored =
                IncrementalSplit.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()));
        assertThat(restored).isEqualTo(split).hasSameHashCodeAs(split);
        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);
        assertThat(read(table, Collections.singletonList(restored), false)).isEmpty();

        append(table, 3, row(RowKind.INSERT, 1, 5));
        assertThat(diff(table, before, false))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(5));
        // A newer snapshot must not change the guard view of a previously planned split.
        assertThat(read(table, splits, false)).isEmpty();
    }

    @Test
    void testGuardOnlyCompactionDoesNotCreateDiffRows() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 4));
        Snapshot beforeCapture = table.snapshotManager().latestSnapshot();
        compact(table, 1);
        assertThat(diff(table, beforeCapture, false)).isEmpty();
        assertThat(diff(table, beforeCapture, true)).isEmpty();
        append(table, 2, row(RowKind.DELETE, 1, 7));
        compact(table, 3);
        Snapshot beforeConsolidation = table.snapshotManager().latestSnapshot();
        compact(table, 4);
        assertThat(diff(table, beforeConsolidation, false)).isEmpty();
        assertThat(diff(table, beforeConsolidation, true)).isEmpty();
    }

    @Test
    void testDiffDeleteKeepsTheVisibleBeforeImage() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.INSERT, 1, 1));
        Snapshot before = table.snapshotManager().latestSnapshot();
        append(table, 1, row(RowKind.DELETE, 1, 4));
        compact(table, 2);
        append(table, 3, row(RowKind.INSERT, 1, 2));
        assertThat(diff(table, before, false)).isEmpty();
        assertThat(diff(table, before, true))
                .singleElement()
                .satisfies(
                        r -> {
                            assertThat(r.getRowKind()).isEqualTo(RowKind.DELETE);
                            assertThat(r.getInt(2)).isEqualTo(1);
                            assertThat(r.getString(3).toString()).isEqualTo("value-1");
                        });
    }

    @Test
    void testOrdinaryIncrementalSplitsKeepTheirVersionTwoEncoding() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.INSERT, 1, 1));
        Snapshot before = table.snapshotManager().latestSnapshot();
        append(table, 1, row(RowKind.INSERT, 1, 2));
        IncrementalSplit split =
                (IncrementalSplit)
                        table.newSnapshotReader().readIncrementalDiff(before).splits().get(0);
        DataOutputSerializer bytes = new DataOutputSerializer(256);
        split.serialize(bytes);
        assertThat(new DataInputDeserializer(bytes.getCopyOfBuffer()).readInt()).isEqualTo(2);
        assertThat(IncrementalSplit.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer())))
                .isEqualTo(split);
    }

    @Test
    void testRetainedGuardsRejectPolicyChangesAfterCaptureIsDisabled() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 4));
        compact(table, 1);
        table = setOption(table, "delete-ordering-guards.enabled", "false");
        PrimaryKeyFileStoreTable retained = table;
        assertThatThrownBy(() -> setOption(retained, "sequence.field.sort-order", "descending"))
                .hasStackTraceContaining("ordering migration");
        assertThatThrownBy(() -> setOption(retained, "ignore-delete", "true"))
                .hasStackTraceContaining("ordering migration");
        assertThatThrownBy(() -> setOption(retained, "bucket", "-1"))
                .hasMessageContaining("Cannot change bucket");
        PrimaryKeyFileStoreTable changed =
                (PrimaryKeyFileStoreTable)
                        table.copy(
                                Collections.singletonMap(
                                        "sequence.field.sort-order", "descending"));
        assertThatThrownBy(() -> append(changed, 2, row(RowKind.INSERT, 1, 2)))
                .hasStackTraceContaining("ordering migration");
        Map<String, String> unsupportedOptions = new HashMap<>(table.options());
        unsupportedOptions.put("bucket", "-1");
        PrimaryKeyFileStoreTable unsupported =
                (PrimaryKeyFileStoreTable) table.copy(table.schema().copy(unsupportedOptions));
        assertThatThrownBy(unsupported::newRead).hasMessageContaining("fixed buckets");
        append(table, 2, row(RowKind.INSERT, 1, 2));
        assertThat(read(table, table.newScan().plan().splits(), false)).isEmpty();
    }

    @Test
    void testRetainedGuardsRejectPartialUpdateAfterCaptureIsDisabled() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 4));
        compact(table, 1);
        PrimaryKeyFileStoreTable retained =
                setOption(table, "delete-ordering-guards.enabled", "false");
        assertThatThrownBy(() -> setOption(retained, "merge-engine", "partial-update"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Change 'merge-engine' is not supported");

        assertThatThrownBy(
                        () ->
                                retained.copy(
                                        Collections.singletonMap("merge-engine", "partial-update")))
                .isInstanceOf(UnsupportedOperationException.class);
        Map<String, String> overriddenOptions = new HashMap<>(retained.options());
        overriddenOptions.put("merge-engine", "partial-update");
        PrimaryKeyFileStoreTable overridden =
                (PrimaryKeyFileStoreTable) retained.copy(retained.schema().copy(overriddenOptions));
        assertThatThrownBy(overridden::newRead)
                .hasMessageContaining("only the deduplicate merge engine");
        assertThatThrownBy(() -> append(overridden, 2, row(RowKind.INSERT, 1, 2)))
                .hasStackTraceContaining("ordering migration");
        append(retained, 2, row(RowKind.INSERT, 1, 2));
        assertThat(read(retained, retained.newScan().plan().splits(), false)).isEmpty();
    }

    @Test
    void testExplicitHistoryResetAllowsNewPolicyAndRejectsOldWriters() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 4));
        compact(table, 1);
        append(table, 2, row(RowKind.INSERT, 1, 2));
        table.createTag("old-policy", table.snapshotManager().latestSnapshotId());
        List<CommitMessage> beforeReset = prepare(table, 3, row(RowKind.INSERT, 1, 2));
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.truncateTable(3);
        }
        List<CommitMessage> beforeAlter = prepare(table, 4, row(RowKind.INSERT, 1, 2));
        table = setOption(table, "sequence.field.sort-order", "descending");
        PrimaryKeyFileStoreTable changed = table;
        assertThatThrownBy(() -> commit(changed, 4, beforeReset))
                .hasStackTraceContaining("generation changed");
        assertThatThrownBy(() -> commit(changed, 4, beforeAlter))
                .hasStackTraceContaining("ordering migration");
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.abort(beforeReset);
            commit.abort(beforeAlter);
        }
        append(table, 4, row(RowKind.INSERT, 1, 3));
        append(table, 5, row(RowKind.INSERT, 1, 2));
        assertThat(read(table, table.newScan().plan().splits(), false))
                .singleElement()
                .satisfies(value -> assertThat(value.getInt(2)).isEqualTo(2));
        PrimaryKeyFileStoreTable historical =
                (PrimaryKeyFileStoreTable)
                        table.copy(Collections.singletonMap("scan.tag-name", "old-policy"));
        assertThat(historical.coreOptions().sequenceFieldSortOrderIsAscending()).isTrue();
        assertThat(read(historical, historical.newScan().plan().splits(), false)).isEmpty();
    }

    @Test
    void testEnablingCaptureDoesNotInventPreviouslyPrunedHistory() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap("delete-ordering-guards.enabled", "false"));
        append(table, 0, row(RowKind.DELETE, 1, 4));
        compact(table, 1);
        table = setOption(table, "delete-ordering-guards.enabled", "true");
        append(table, 2, row(RowKind.INSERT, 1, 2));
        assertThat(read(table, table.newScan().plan().splits(), false))
                .singleElement()
                .satisfies(row -> assertThat(row.getInt(2)).isEqualTo(2));
        append(table, 3, row(RowKind.DELETE, 1, 6));
        compact(table, 4);
        append(table, 5, row(RowKind.INSERT, 1, 3));
        assertThat(read(table, table.newScan().plan().splits(), false)).isEmpty();
    }

    @Test
    void testStreamingDeltaResolvesGuardBounds() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 4));
        compact(table, 1);
        assertThat(read(table, table.newSnapshotReader().readChanges().splits(), true)).isEmpty();
        append(table, 2, row(RowKind.INSERT, 1, 2));
        List<Split> stale = table.newSnapshotReader().readChanges().splits();
        assertThat(stale)
                .singleElement()
                .satisfies(
                        split -> {
                            IncrementalSplit incremental = (IncrementalSplit) split;
                            assertThat(incremental.isStreaming()).isTrue();
                            assertThat(incremental.afterDeleteOrderingGuards()).hasSize(1);
                        });
        assertThat(read(table, stale, true)).isEmpty();
        append(table, 3, row(RowKind.INSERT, 1, 5));
        assertThat(read(table, table.newSnapshotReader().readChanges().splits(), true))
                .singleElement()
                .satisfies(row -> assertThat(row.getInt(2)).isEqualTo(5));
    }

    @Test
    void testStreamingOverwriteRetractsOnlyTheVisibleBeforeImage() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap("dynamic-partition-overwrite", "false"));
        append(table, 0, row(RowKind.DELETE, 1, 4), row(RowKind.INSERT, 2, 1));
        compact(table, 1);
        append(table, 2, row(RowKind.INSERT, 1, 2));
        assertThat(read(table, table.newScan().plan().splits(), false)).singleElement();
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.overwritePartition(
                    Collections.singletonMap("pt", "1"), new ManifestCommittable(3));
        }
        List<Split> changes = table.newSnapshotReader().readChanges().splits();
        assertThat(changes)
                .singleElement()
                .satisfies(
                        split -> {
                            IncrementalSplit incremental = (IncrementalSplit) split;
                            assertThat(incremental.beforeFiles())
                                    .as("overwrite sources in %s", incremental)
                                    .isNotEmpty();
                            assertThat(incremental.beforeDeleteOrderingGuards())
                                    .as("overwrite guard bounds")
                                    .hasSize(1);
                        });
        assertThat(read(table, changes, true))
                .singleElement()
                .satisfies(
                        row -> {
                            assertThat(row.getRowKind()).isEqualTo(RowKind.DELETE);
                            assertThat(row.getInt(1)).isEqualTo(2);
                            assertThat(row.getInt(2)).isEqualTo(1);
                        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "2", "99"})
    void testInvalidPartitionMetadataBlocksWrites(String version) throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 4));
        compact(table, 1);
        Snapshot snapshot = Snapshot.fromJson(table.snapshotManager().latestSnapshot().toJson());
        if (version.equals("missing")) {
            snapshot.properties().remove(DeleteOrderingGuardPartitions.SNAPSHOT_PROPERTY);
        } else {
            snapshot.properties()
                    .put(
                            DeleteOrderingGuardPartitions.SNAPSHOT_PROPERTY,
                            "{\"version\":\"" + version + "\"}");
        }
        table.fileIO()
                .overwriteFileUtf8(
                        table.snapshotManager().snapshotPath(snapshot.id()), snapshot.toJson());
        PrimaryKeyFileStoreTable reopened =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(table.fileIO(), table.location());
        String error =
                version.equals("missing")
                        ? "Missing delete ordering guard partition state"
                        : "Unsupported delete ordering guard partition state";
        assertThatThrownBy(() -> append(reopened, 2, row(RowKind.INSERT, 1, 2)))
                .hasStackTraceContaining(error);
        assertThatThrownBy(
                        () ->
                                reopened.store()
                                        .newScan()
                                        .withDeleteOrderingGuardPartitions()
                                        .readPartitionEntries())
                .hasMessageContaining(error);
        assertThat(read(reopened, reopened.newScan().plan().splits(), false)).isEmpty();
    }

    private List<CommitMessage> prepare(
            PrimaryKeyFileStoreTable table, long id, InternalRow... rows) throws Exception {
        try (TableWriteImpl<KeyValue> write = writer(table)) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            return write.prepareCommit(true, id);
        }
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> options) throws Exception {
        return createTable(
                options,
                RowType.builder()
                        .field("pt", DataTypes.INT())
                        .field("k", DataTypes.INT())
                        .field("seq", DataTypes.INT())
                        .field("payload", DataTypes.STRING())
                        .build());
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> extra, RowType type)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("delete-ordering-guards.enabled", "true");
        options.put("delete-ordering-guards.compaction.min-file-num", "100");
        options.put("num-sorted-run.compaction-trigger", "100");
        options.put("num-levels", "3");
        if (type.getFieldNames().contains("seq")) {
            options.put("sequence.field", "seq");
        }
        options.putAll(extra);
        Path path = new Path(tempDir.resolve("table").toUri());
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                type.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options,
                                ""));
        return (PrimaryKeyFileStoreTable)
                FileStoreTableFactory.create(LocalFileIO.create(), path, schema);
    }

    private PrimaryKeyFileStoreTable setOption(
            PrimaryKeyFileStoreTable table, String key, String value) throws Exception {
        TableSchema schema =
                new FileSystemSchemaManager(table.fileIO(), table.location())
                        .commitChanges(SchemaChange.setOption(key, value));
        return (PrimaryKeyFileStoreTable)
                FileStoreTableFactory.create(table.fileIO(), table.location(), schema);
    }

    private TableWriteImpl<KeyValue> writer(PrimaryKeyFileStoreTable table) {
        IOManager io = new IOManagerImpl(tempDir.resolve("io-" + ioManagers.size()).toString());
        ioManagers.add(io);
        return table.newWrite(USER).withIOManager(io);
    }

    private void append(PrimaryKeyFileStoreTable table, long id, InternalRow... rows)
            throws Exception {
        try (TableWriteImpl<KeyValue> write = writer(table)) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit(table, id, write.prepareCommit(true, id));
        }
    }

    private void compact(PrimaryKeyFileStoreTable table, long id) throws Exception {
        try (TableWriteImpl<KeyValue> write = writer(table)) {
            for (DataSplit split :
                    table.newSnapshotReader()
                            .withDeleteOrderingGuardBuckets()
                            .read()
                            .dataSplits()) {
                write.compact(split.partition(), split.bucket(), true);
            }
            commit(table, id, write.prepareCommit(true, id));
        }
    }

    private void commit(PrimaryKeyFileStoreTable table, long id, List<CommitMessage> messages) {
        ManifestCommittable committable = new ManifestCommittable(id);
        messages.forEach(committable::addFileCommittable);
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.commit(committable, false);
        }
    }

    private List<InternalRow> diff(
            PrimaryKeyFileStoreTable table, Snapshot before, boolean keepDelete) throws Exception {
        return read(
                table, table.newSnapshotReader().readIncrementalDiff(before).splits(), keepDelete);
    }

    private List<InternalRow> read(
            PrimaryKeyFileStoreTable table, List<Split> splits, boolean keepDelete)
            throws Exception {
        InnerTableRead read = table.newRead();
        if (keepDelete) {
            read.forceKeepDelete();
        }
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        read.createReader(splits).forEachRemaining(row -> result.add(serializer.copy(row)));
        return result;
    }

    private static GenericRow row(RowKind kind, int key, int sequence) {
        return GenericRow.ofKind(kind, 1, key, sequence, str("value-" + sequence));
    }

    private static BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    private static BinaryRow partition(int value) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, value);
        writer.complete();
        return partition;
    }
}
