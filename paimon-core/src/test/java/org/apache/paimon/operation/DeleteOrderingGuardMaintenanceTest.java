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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeleteOrderingGuardPolicy;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionUpdateTimeExpireStrategy;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.index.DeleteOrderingGuardFile.DELETE_ORDERING_GUARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native maintenance must preserve hidden ordering state and partition lifecycle semantics. */
class DeleteOrderingGuardMaintenanceTest {

    private static final String OLD = "2020-01-01";
    private static final String RECENT = "2099-01-01";
    private static final String USER = "maintenance";

    @TempDir java.nio.file.Path tempDir;
    private final List<IOManager> ioManagers = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        IOUtils.closeAll(ioManagers);
    }

    @Test
    void testValuesTimeExpirationDiscoversGuardOnlyPartitionsAndFencesWriters() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4), row(RowKind.DELETE, RECENT, 2, 4));
        compact(table, 1);
        Snapshot original = table.snapshotManager().latestSnapshot();
        assertThat(table.store().newScan().readPartitionEntries()).isEmpty();
        assertThat(
                        table.store()
                                .newScan()
                                .withDeleteOrderingGuardPartitions()
                                .readPartitionEntries())
                .hasSize(2)
                .allSatisfy(entry -> assertThat(entry.fileCount()).isZero());
        List<CommitMessage> stale = prepare(table, 2, row(RowKind.INSERT, OLD, 1, 2));
        List<CommitMessage> unaffected = prepare(table, 3, row(RowKind.INSERT, RECENT, 2, 2));
        assertThat(
                        expire(
                                table,
                                new PartitionValuesTimeExpireStrategy(
                                        table.coreOptions(), table.schema().logicalPartitionType()),
                                LocalDateTime.of(2026, 11, 2, 0, 0),
                                4))
                .containsExactly(Collections.singletonMap("pt", OLD));
        assertThat(guards(table))
                .allSatisfy(entry -> assertThat(entry.partition()).isEqualTo(partition(RECENT)));
        assertThatThrownBy(() -> commit(table, 5, stale))
                .hasMessageContaining("generation changed");
        commit(table, 6, unaffected);
        assertThat(read(table)).isEmpty();
        assertThat(
                        table.store()
                                .newScan()
                                .withSnapshot(original)
                                .withDeleteOrderingGuardPartitions()
                                .withPartitionFilter(
                                        PartitionPredicate.fromMap(
                                                table.schema().logicalPartitionType(),
                                                Collections.singletonMap("pt", OLD),
                                                table.coreOptions().partitionDefaultName()))
                                .readPartitionEntries())
                .extracting(PartitionEntry::partition)
                .containsExactly(partition(OLD));
        append(table, 7, row(RowKind.INSERT, OLD, 1, 2));
        assertThat(read(table)).hasSize(1);
    }

    @Test
    void testConsolidationDoesNotPostponeUpdateTimeExpiration() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4));
        compact(table, 1);
        append(table, 2, row(RowKind.DELETE, OLD, 1, 7));
        compact(table, 3);
        assertThat(guards(table)).hasSize(2);
        long activity =
                DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot())
                        .get(partition(OLD))
                        .lastFileCreationTime();
        compact(table, 4);
        assertThat(guards(table)).hasSize(1);
        assertThat(
                        DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot())
                                .get(partition(OLD))
                                .lastFileCreationTime())
                .isEqualTo(activity);
        LocalDateTime now =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(activity), ZoneId.systemDefault())
                        .plusDays(2);
        assertThat(
                        expire(
                                table,
                                new PartitionUpdateTimeExpireStrategy(
                                        table.coreOptions(), table.schema().logicalPartitionType()),
                                now,
                                5))
                .containsExactly(Collections.singletonMap("pt", OLD));
        assertThat(guards(table)).isEmpty();
        assertThat(DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot()))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRescaleRejectedWithCaptureEnabledOrRetainedGuards(boolean disableCapture)
            throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4));
        compact(table, 1);
        if (disableCapture) {
            table = setOption(table, "delete-ordering-guards.enabled", "false");
        }
        PrimaryKeyFileStoreTable guarded = table;
        assertThatThrownBy(
                        () ->
                                DeleteOrderingGuardPolicy.validateRescale(
                                        guarded, guarded.snapshotManager().latestSnapshot()))
                .hasMessageContaining(
                        "Rescaling tables with delete ordering guards is not supported");
        assertThat(guards(table)).hasSize(1);
        append(table, 2, row(RowKind.INSERT, OLD, 1, 2));
        assertThat(read(table)).isEmpty();
    }

    @Test
    void testRescaleAllowedAfterExplicitHistoryResetAndCaptureDisabled() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4));
        compact(table, 1);
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.truncateTable(2);
        }
        table = setOption(table, "delete-ordering-guards.enabled", "false");
        DeleteOrderingGuardPolicy.validateRescale(table, table.snapshotManager().latestSnapshot());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOtherBranchRetainsGuardsAfterMainConsolidationAndTagDeletion(boolean snapshotOnly)
            throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4), row(RowKind.INSERT, OLD, 2, 3));
        compact(table, 1);
        List<IndexManifestEntry> original = guards(table);
        table.createTag("base", table.snapshotManager().latestSnapshotId());
        table.createBranch("audit", "base");
        PrimaryKeyFileStoreTable branch = (PrimaryKeyFileStoreTable) table.switchToBranch("audit");
        if (snapshotOnly) {
            branch.deleteTag("base");
        }
        append(table, 2, row(RowKind.DELETE, OLD, 1, 7), row(RowKind.DELETE, OLD, 2, 7));
        compact(table, 3);
        compact(table, 4);
        expireOldSnapshots(table);
        table.deleteTag("base");
        new LocalOrphanFilesClean(table, Long.MAX_VALUE).clean();
        for (IndexManifestEntry guard : original) {
            assertThat(
                            table.fileIO()
                                    .exists(
                                            table.store()
                                                    .pathFactory()
                                                    .indexFileFactory(
                                                            guard.partition(), guard.bucket())
                                                    .toPath(guard.indexFile())))
                    .isTrue();
        }
        append(branch, 5, row(RowKind.INSERT, OLD, 1, 2));
        assertThat(read(branch))
                .singleElement()
                .satisfies(row -> assertThat(row.getInt(1)).isEqualTo(2));
        append(branch, 6, row(RowKind.INSERT, OLD, 1, 5));
        assertThat(read(branch)).hasSize(2);
        assertThat(read(table)).isEmpty();
        try (FileStoreCommit commit = branch.store().newCommit(USER, null)) {
            commit.truncateTable(7);
        }
        expireOldSnapshots(branch);
        branch.deleteTag("base");
        new LocalOrphanFilesClean(table, Long.MAX_VALUE).clean();
        for (IndexManifestEntry guard : original) {
            assertThat(
                            table.fileIO()
                                    .exists(
                                            table.store()
                                                    .pathFactory()
                                                    .indexFileFactory(
                                                            guard.partition(), guard.bucket())
                                                    .toPath(guard.indexFile())))
                    .isFalse();
        }
        append(table, 8, row(RowKind.INSERT, OLD, 1, 5));
        assertThat(read(table)).isEmpty();
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> options) throws Exception {
        return createTable(
                options,
                RowType.builder()
                        .field("pt", DataTypes.STRING())
                        .field("k", DataTypes.INT())
                        .field("seq", DataTypes.INT())
                        .field("payload", DataTypes.STRING())
                        .build());
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> extra, RowType type)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "2");
        options.put("delete-ordering-guards.enabled", "true");
        options.put("delete-ordering-guards.compaction.min-file-num", "100");
        options.put("num-sorted-run.compaction-trigger", "100");
        options.put("num-levels", "3");
        options.put("partition.timestamp-pattern", "$pt");
        options.put("partition.timestamp-formatter", "yyyy-MM-dd");
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
                new FileSystemSchemaManager(table.fileIO(), table.location(), table.currentBranch())
                        .commitChanges(SchemaChange.setOption(key, value));
        return (PrimaryKeyFileStoreTable)
                FileStoreTableFactory.create(table.fileIO(), table.location(), schema);
    }

    private TableWriteImpl<KeyValue> writer(PrimaryKeyFileStoreTable table) throws IOException {
        IOManager io = new IOManagerImpl(tempDir.resolve("spill").toString());
        ioManagers.add(io);
        return table.newWrite(USER).withIOManager(io);
    }

    private List<CommitMessage> prepare(
            PrimaryKeyFileStoreTable table, long identifier, InternalRow... rows) throws Exception {
        try (TableWriteImpl<KeyValue> write = writer(table)) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            return write.prepareCommit(true, identifier);
        }
    }

    private void append(PrimaryKeyFileStoreTable table, long identifier, InternalRow... rows)
            throws Exception {
        commit(table, identifier, prepare(table, identifier, rows));
    }

    private void commit(
            PrimaryKeyFileStoreTable table, long identifier, List<CommitMessage> messages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        messages.forEach(committable::addFileCommittable);
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.commit(committable, false);
        }
    }

    private void compact(PrimaryKeyFileStoreTable table, long identifier) throws Exception {
        try (TableWriteImpl<KeyValue> write = writer(table)) {
            for (DataSplit split :
                    table.newSnapshotReader()
                            .withDeleteOrderingGuardBuckets()
                            .read()
                            .dataSplits()) {
                write.compact(split.partition(), split.bucket(), true);
            }
            commit(table, identifier, write.prepareCommit(true, identifier));
        }
    }

    private List<Map<String, String>> expire(
            PrimaryKeyFileStoreTable table,
            PartitionExpireStrategy strategy,
            LocalDateTime now,
            long identifier) {
        try (FileStoreCommit commit = table.store().newCommit("expire", null)) {
            return new NormalPartitionExpire(
                            Duration.ofDays(1),
                            Duration.ZERO,
                            strategy,
                            table.store().newScan(),
                            commit,
                            null,
                            100,
                            0)
                    .expire(now, identifier);
        }
    }

    private void expireOldSnapshots(PrimaryKeyFileStoreTable table) {
        ((ExpireSnapshotsImpl) table.newExpireSnapshots())
                .expireUntil(
                        table.snapshotManager().earliestSnapshotId(),
                        table.snapshotManager().latestSnapshotId());
    }

    private List<InternalRow> read(PrimaryKeyFileStoreTable table) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        table.newRead()
                .createReader(table.newScan().plan())
                .forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }

    private List<IndexManifestEntry> guards(PrimaryKeyFileStoreTable table) {
        return table.store().newIndexFileHandler().scan(DELETE_ORDERING_GUARD);
    }

    private static InternalRow row(RowKind kind, String partition, int key, int sequence) {
        return GenericRow.ofKind(kind, str(partition), key, sequence, str("payload"));
    }

    private static BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    private static BinaryRow partition(String value) {
        return BinaryRow.singleColumn(str(value));
    }
}
