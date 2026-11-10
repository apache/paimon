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
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeleteOrderingGuard;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.index.DeleteOrderingGuardSerializer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.MergeTreeCompactRewriter;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.CommitPreCallback;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.system.CompactBucketsTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.DELETE_ORDERING_GUARDS_ENABLED;
import static org.apache.paimon.index.DeleteOrderingGuardFile.DELETE_ORDERING_GUARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests guard capture through native compaction, commit, recovery and abort. */
class DeleteOrderingGuardCommitTest {

    private static final String USER = "guard-test";
    private static final BinaryRow PARTITION = partition(1);
    private final List<IOManager> ioManagers = new ArrayList<>();
    private final CacheManager cacheManager = new CacheManager(MemorySize.ofMebiBytes(1), 0.2);

    @TempDir java.nio.file.Path tempDir;

    @AfterEach
    void closeIoManagers() throws Exception {
        IOUtils.closeAll(ioManagers);
    }

    @ParameterizedTest
    @ValueSource(strings = {"unset", "false"})
    void testDisabledCapturePreservesLegacyDeletePruning(String configured) throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(
                        configured.equals("unset")
                                ? Collections.emptyMap()
                                : Collections.singletonMap(
                                        DELETE_ORDERING_GUARDS_ENABLED.key(), configured));
        assertThat(table.coreOptions().deleteOrderingGuardsEnabled()).isFalse();
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        append(table, 1, row(RowKind.DELETE, 1, 1, 4));
        List<CommitMessage> compacted = compactNatively(table, 2, PARTITION);
        assertThat(newGuards(compacted)).isEmpty();
        commit(table, 2, compacted);
        assertThat(readRows(table)).isEmpty();
        assertThat(guards(table)).isEmpty();
        assertThat(indexFilesOnDisk()).isEmpty();
        assertThat(
                        SequenceSnapshotProperties.guardMaxSequenceNumber(
                                table.snapshotManager().latestSnapshot()))
                .isEmpty();

        append(table, 3, row(RowKind.INSERT, 1, 1, 2));
        assertThat(readRows(table))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(2));
    }

    @Test
    void testEnablingCaptureDoesNotRecoverPreviouslyPrunedDeletes() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        append(table, 1, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 2, compactNatively(table, 2, PARTITION));
        assertThat(guards(table)).isEmpty();

        TableSchema enabledSchema =
                new FileSystemSchemaManager(table.fileIO(), table.location())
                        .commitChanges(
                                SchemaChange.setOption(
                                        DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        PrimaryKeyFileStoreTable enabled =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(
                                table.fileIO(), table.location(), enabledSchema);
        append(enabled, 3, row(RowKind.INSERT, 1, 1, 2));
        assertThat(readRows(enabled))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(2));

        append(enabled, 4, row(RowKind.DELETE, 1, 1, 7));
        List<CommitMessage> compacted = compactNatively(enabled, 5, PARTITION);
        assertThat(newGuards(compacted)).hasSize(1);
        commit(enabled, 5, compacted);
        append(enabled, 6, row(RowKind.INSERT, 1, 1, 6));
        assertThat(readRows(enabled)).isEmpty();
    }

    @Test
    void testDisablingCapturePreservesCommittedGuards() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap(DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 1, compactNatively(table, 1, PARTITION));
        List<IndexManifestEntry> original = guards(table);
        assertThat(original).hasSize(1);

        TableSchema disabledSchema =
                new FileSystemSchemaManager(table.fileIO(), table.location())
                        .commitChanges(
                                SchemaChange.setOption(
                                        DELETE_ORDERING_GUARDS_ENABLED.key(), "false"));
        PrimaryKeyFileStoreTable disabled =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(
                                table.fileIO(), table.location(), disabledSchema);
        assertThat(disabled.coreOptions().deleteOrderingGuardsEnabled()).isFalse();
        append(disabled, 2, row(RowKind.INSERT, 1, 1, 2), row(RowKind.INSERT, 1, 2, 1));
        assertThat(readRows(disabled))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(1)).isEqualTo(2));
        append(disabled, 3, row(RowKind.DELETE, 1, 2, 4));
        List<CommitMessage> compacted = compactNatively(disabled, 4, PARTITION);
        assertThat(newGuards(compacted)).isEmpty();
        commit(disabled, 4, compacted);
        assertThat(guards(disabled)).isEqualTo(original);
        assertThat(readRows(disabled)).isEmpty();

        append(disabled, 5, row(RowKind.INSERT, 1, 1, 3), row(RowKind.INSERT, 1, 2, 2));
        assertThat(readRows(disabled))
                .singleElement()
                .satisfies(
                        r -> {
                            assertThat(r.getInt(1)).isEqualTo(2);
                            assertThat(r.getInt(2)).isEqualTo(2);
                        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"lookup", "full-compaction"})
    void testCompactionRejectsGuardsPublishedAfterWriterRestore(String producer) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(DELETE_ORDERING_GUARDS_ENABLED.key(), "true");
        options.put("changelog-producer", producer);
        PrimaryKeyFileStoreTable table = createTable(options);
        try (TableWriteImpl<KeyValue> stale = newWrite(table)) {
            stale.write(row(RowKind.INSERT, 1, 1, 2));
            append(table, 0, row(RowKind.DELETE, 1, 1, 4));
            commit(table, 1, compactNatively(table, 1, PARTITION));
            long latest = table.snapshotManager().latestSnapshotId();

            stale.compact(PARTITION, 0, true);
            List<CommitMessage> pending = stale.prepareCommit(true, 2);
            assertThat(pending.get(0).deleteOrderingGuardSources()).isEmpty();
            assertThatThrownBy(() -> commit(table, 2, pending))
                    .hasMessageContaining("guard sources changed")
                    .hasMessageContaining("recompute compaction");
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(latest);
        }
        try (TableWriteImpl<KeyValue> refreshed = newWrite(table)) {
            refreshed.write(row(RowKind.INSERT, 1, 1, 2));
            refreshed.write(row(RowKind.INSERT, 1, 2, 5));
            refreshed.compact(PARTITION, 0, true);
            List<CommitMessage> messages = refreshed.prepareCommit(true, 2);
            assertThat(messages.get(0).deleteOrderingGuardSources()).hasSize(1);
            commit(table, 2, messages);
            assertThat(changelog(table, messages, false)).doesNotContain("INSERT/2/payload");
        }
        assertThat(readRows(table))
                .singleElement()
                .satisfies(value -> assertThat(value.getInt(1)).isEqualTo(2));
    }

    @Test
    void testCompactionRechecksGuardSourcesOnPublicationRetry() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap(DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        append(table, 0, row(RowKind.INSERT, 1, 2, 1));
        List<CommitMessage> pending = compactNatively(table, 9, PARTITION);
        AtomicInteger publications = new AtomicInteger();
        SnapshotCommit publisher =
                new SnapshotCommit(Lock.empty()) {
                    @Override
                    protected boolean commitImpl(
                            @Nullable String baseSnapshotUuid,
                            Snapshot snapshot,
                            String branch,
                            List<PartitionStatistics> statistics)
                            throws Exception {
                        publications.incrementAndGet();
                        append(table, 3, row(RowKind.DELETE, 1, 1, 4));
                        DeleteOrderingGuardCommitTest.this.commit(
                                table, 4, compactNatively(table, 4, PARTITION));
                        return false;
                    }
                };
        try (FileStoreCommit commit = newCommit(table, publisher, Collections.emptyList())) {
            assertThatThrownBy(() -> commit.commit(committable(9, pending), false))
                    .hasMessageContaining("guard sources changed");
        }
        assertThat(publications).hasValue(1);
        assertThat(guards(table)).hasSize(1);
    }

    @Test
    void testCompactionAllowsGuardsPublishedInAnotherPartition() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap(DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        BinaryRow otherPartition = partition(2);
        append(table, 0, row(RowKind.INSERT, 2, 2, 1));
        List<CommitMessage> pending = compactNatively(table, 9, otherPartition);
        append(table, 3, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 4, compactNatively(table, 4, PARTITION));

        commit(table, 9, pending);
        assertThat(guards(table)).hasSize(1);
        assertThat(readRows(table))
                .singleElement()
                .satisfies(value -> assertThat(value.getInt(0)).isEqualTo(2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDestructiveRollbackCannotReviveFencedWrites(boolean disableCapture) throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap(DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        Snapshot target = table.snapshotManager().latestSnapshot();
        table.createTag("before-reset", target.id());
        List<CommitMessage> pending;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.INSERT, 1, 2, 2));
            pending = write.prepareCommit(true, 9);
        }
        try (FileStoreCommit reset = table.store().newCommit(USER, null)) {
            reset.truncateTable(2);
        }
        if (disableCapture) {
            TableSchema disabled =
                    new FileSystemSchemaManager(table.fileIO(), table.location())
                            .commitChanges(
                                    SchemaChange.setOption(
                                            DELETE_ORDERING_GUARDS_ENABLED.key(), "false"));
            table =
                    (PrimaryKeyFileStoreTable)
                            FileStoreTableFactory.create(
                                    table.fileIO(), table.location(), disabled);
        }
        PrimaryKeyFileStoreTable retained = table;
        long latest = table.snapshotManager().latestSnapshotId();
        assertThatThrownBy(() -> commit(retained, 9, pending))
                .hasMessageContaining("generation changed");
        assertThatThrownBy(() -> retained.rollbackTo(target.id()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("sys.rollback_to_as_latest");
        assertThatThrownBy(() -> retained.rollbackTo("before-reset"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("sys.rollback_to_as_latest");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(latest);
        try (FileStoreCommit rollback = table.store().newCommit(USER, null)) {
            assertThat(rollback.rollbackToAsLatest(target)).isTrue();
        }
        assertThatThrownBy(() -> commit(retained, 9, pending))
                .hasMessageContaining("generation changed");
        append(table, 10, row(RowKind.INSERT, 1, 3, 3));
        assertThat(readRows(table))
                .extracting(value -> value.getInt(1))
                .containsExactlyInAnyOrder(1, 3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"partition", "table", "disabled"})
    void testResetFencesStaleOrdinaryAppendsAfterSnapshotExpiration(String reset) throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap(DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 1, compactNatively(table, 1, PARTITION));

        List<CommitMessage> pending;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.INSERT, 1, 1, 3));
            write.write(row(RowKind.INSERT, 2, 1, 6));
            pending = write.prepareCommit(true, 9);
        }
        assertThat(pending)
                .extracting(CommitMessage::deleteOrderingGuardGeneration)
                .containsOnly("0");
        try (FileStoreCommit resetCommit = table.store().newCommit(USER, null)) {
            if (reset.equals("table")) {
                resetCommit.truncateTable(2);
            } else {
                resetCommit.overwritePartition(
                        Collections.singletonMap("pt", "1"),
                        committable(2, Collections.emptyList()));
            }
        }
        if (reset.equals("disabled")) {
            table =
                    (PrimaryKeyFileStoreTable)
                            table.copy(
                                    Collections.singletonMap(
                                            DELETE_ORDERING_GUARDS_ENABLED.key(), "false"));
        }
        append(table, 3, row(RowKind.INSERT, 1, 1, 1));
        long latest = table.snapshotManager().latestSnapshotId();
        for (long id = Snapshot.FIRST_SNAPSHOT_ID; id < latest; id++) {
            table.fileIO().delete(table.snapshotManager().snapshotPath(id), false);
        }

        List<CommitMessage> affected =
                pending.stream()
                        .filter(message -> message.partition().equals(PARTITION))
                        .collect(Collectors.toList());
        List<CommitMessage> unaffected =
                pending.stream()
                        .filter(message -> !message.partition().equals(PARTITION))
                        .collect(Collectors.toList());
        PrimaryKeyFileStoreTable current = table;
        assertThatThrownBy(() -> commit(current, 9, affected))
                .hasStackTraceContaining("Delete ordering guard generation changed");
        assertThat(readRows(table))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(1));
        if (reset.equals("table")) {
            assertThatThrownBy(() -> commit(current, 10, unaffected))
                    .hasStackTraceContaining("Delete ordering guard generation changed");
        } else {
            commit(table, 10, unaffected);
            assertThat(readRows(table)).hasSize(2);
        }
        try (FileStoreCommit abort = table.store().newCommit(USER, null)) {
            abort.abort(affected);
            if (reset.equals("table")) {
                abort.abort(unaffected);
            }
        }
    }

    @Test
    void testCheckpointRetainsGenerationAcrossReset() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap(DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        List<FileStoreWrite.State<KeyValue>> checkpoint;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.INSERT, 1, 1, 3));
            checkpoint = write.checkpoint();
        }
        try (FileStoreCommit reset = table.store().newCommit(USER, null)) {
            reset.truncateTable(1);
        }
        try (TableWriteImpl<KeyValue> restored = newWrite(table)) {
            restored.restore(checkpoint);
            List<CommitMessage> pending = restored.prepareCommit(true, 2);
            assertThat(pending)
                    .extracting(CommitMessage::deleteOrderingGuardGeneration)
                    .containsOnly("0");
            assertThatThrownBy(() -> commit(table, 2, pending))
                    .hasStackTraceContaining("Delete ordering guard generation changed");
            try (FileStoreCommit abort = table.store().newCommit(USER, null)) {
                abort.abort(pending);
            }
        }
        assertThat(readRows(table)).isEmpty();
    }

    @Test
    void testResetDuringPublicationIsCheckedOnRetry() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap(DELETE_ORDERING_GUARDS_ENABLED.key(), "true"));
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        List<CommitMessage> pending;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.INSERT, 1, 1, 3));
            pending = write.prepareCommit(true, 2);
        }
        AtomicInteger publications = new AtomicInteger();
        SnapshotCommit publisher =
                new SnapshotCommit(Lock.empty()) {
                    @Override
                    protected boolean commitImpl(
                            @Nullable String baseSnapshotUuid,
                            Snapshot snapshot,
                            String branch,
                            List<PartitionStatistics> statistics)
                            throws Exception {
                        publications.incrementAndGet();
                        try (FileStoreCommit reset = table.store().newCommit(USER, null)) {
                            reset.truncateTable(1);
                        }
                        return false;
                    }
                };
        try (FileStoreCommit commit = newCommit(table, publisher, Collections.emptyList())) {
            assertThatThrownBy(() -> commit.commit(committable(2, pending), false))
                    .hasStackTraceContaining("Delete ordering guard generation changed");
            commit.abort(pending);
        }
        assertThat(publications).hasValue(1);
        assertThat(readRows(table)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ascending", "descending"})
    void testGuardOnlyConsolidationRetiresDominatedDeduplicateBounds(String order)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sequence.field.sort-order", order);
        PrimaryKeyFileStoreTable table = createLifecycleTable(options);
        int[] versions = {4, 2, 7};
        for (int i = 0; i < versions.length; i++) {
            append(table, i * 2, row(RowKind.DELETE, 1, 1, versions[i]));
            commit(table, i * 2 + 1, compactNatively(table, i * 2 + 1, PARTITION));
        }
        assertThat(table.newScan().plan().splits()).isEmpty();
        List<IndexManifestEntry> original = guards(table);
        assertThat(original).hasSize(2);
        List<CommitMessage> consolidated = compactNatively(table, 6, PARTITION);
        assertThat(newGuards(consolidated))
                .singleElement()
                .satisfies(g -> assertThat(g.rowCount()).isEqualTo(1));
        assertThat(consolidated)
                .allSatisfy(
                        message -> {
                            CompactIncrement compact =
                                    ((CommitMessageImpl) message).compactIncrement();
                            assertThat(compact.compactBefore()).isEmpty();
                            assertThat(compact.compactAfter()).isEmpty();
                            assertThat(compact.deletedIndexFiles()).hasSize(2);
                        });
        commit(table, 6, consolidated);
        assertThat(guards(table)).hasSize(1);
        assertThat(readGuards(table))
                .singleElement()
                .satisfies(
                        g ->
                                assertThat(g.ordering().getInt(0))
                                        .isEqualTo(order.equals("ascending") ? 7 : 2));
        for (IndexManifestEntry source : original) {
            assertThat(table.fileIO().exists(table.store().newIndexFileHandler().filePath(source)))
                    .isTrue();
        }

        append(table, 7, row(RowKind.INSERT, 1, 1, order.equals("ascending") ? 6 : 3));
        assertThat(readRows(table)).isEmpty();
        append(table, 8, row(RowKind.INSERT, 1, 1, order.equals("ascending") ? 8 : 1));
        assertThat(readRows(table)).hasSize(1);
    }

    @Test
    void testPeriodicGuardOnlyConsolidationAfterCaptureDisabled() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        Map<String, String> overrides = new HashMap<>();
        overrides.put(DELETE_ORDERING_GUARDS_ENABLED.key(), "false");
        overrides.put("delete-ordering-guards.compaction.min-file-num", "2");
        PrimaryKeyFileStoreTable disabled = (PrimaryKeyFileStoreTable) table.copy(overrides);
        try (TableWriteImpl<KeyValue> write = newWrite(disabled)) {
            write.compact(PARTITION, 0, false);
            List<CommitMessage> prepared = write.prepareCommit(true, 4);
            assertThat(newGuards(prepared)).hasSize(1);
            commit(disabled, 4, prepared);
        }
        assertThat(guards(disabled)).hasSize(1);
        append(disabled, 5, row(RowKind.INSERT, 1, 1, 6));
        assertThat(readRows(disabled)).isEmpty();
    }

    @Test
    void testConcurrentConsolidationFencesRetiredSourcesButAllowsOrdinaryAppend() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        List<CommitMessage> first = compactNatively(table, 4, PARTITION);
        List<CommitMessage> second = compactNatively(table, 5, PARTITION);
        List<CommitMessage> pendingAppend;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.INSERT, 1, 1, 6));
            pendingAppend = write.prepareCommit(true, 6);
        }
        commit(table, 4, first);
        assertThatThrownBy(() -> commit(table, 5, second))
                .hasMessageContaining("guard sources changed");
        try (FileStoreCommit abort = table.store().newCommit(USER, null)) {
            abort.abort(second);
        }
        assertThat(guards(table))
                .extracting(IndexManifestEntry::indexFile)
                .containsExactlyElementsOf(newGuards(first));
        commit(table, 6, pendingAppend);
        assertThat(readRows(table)).isEmpty();
    }

    @Test
    void testResetFencesGuardOnlyConsolidation() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        List<CommitMessage> pending = compactNatively(table, 4, PARTITION);
        try (FileStoreCommit reset = table.store().newCommit(USER, null)) {
            reset.truncateTable(5);
        }
        assertThatThrownBy(() -> commit(table, 6, pending))
                .hasStackTraceContaining("Delete ordering guard generation changed");
        try (FileStoreCommit abort = table.store().newCommit(USER, null)) {
            abort.abort(pending);
        }
        assertThat(guards(table)).isEmpty();
        append(table, 7, row(RowKind.INSERT, 1, 1, 1));
        assertThat(readRows(table)).hasSize(1);
    }

    @Test
    void testConsolidationCheckpointTransfersReplacementOwnership() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        List<String> originalIndexFiles = indexFilesOnDisk();
        List<FileStoreWrite.State<KeyValue>> checkpoint;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.compact(PARTITION, 0, true);
            KeyValueFileStoreWrite storeWrite = (KeyValueFileStoreWrite) write.getWrite();
            storeWrite.writers.get(PARTITION).get(0).writer.sync();
            checkpoint = write.checkpoint();
        }
        assertThat(indexFilesOnDisk())
                .hasSize(originalIndexFiles.size() + 1)
                .containsAll(originalIndexFiles);
        try (TableWriteImpl<KeyValue> restored = newWrite(table)) {
            restored.restore(checkpoint);
            List<CommitMessage> prepared = restored.prepareCommit(true, 4);
            assertThat(newGuards(prepared)).hasSize(1);
            commit(table, 4, prepared);
        }
        assertThat(guards(table)).hasSize(1);
        append(table, 5, row(RowKind.INSERT, 1, 1, 6));
        assertThat(readRows(table)).isEmpty();
    }

    @Test
    void testGuardOnlyBucketsAreDiscoveredForDedicatedCompaction() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        append(table, 4, row(RowKind.DELETE, 2, 2, 5));
        commit(table, 5, compactNatively(table, 5, partition(2)));
        assertThat(table.newScan().plan().splits()).isEmpty();
        CompactBucketsTable buckets = new CompactBucketsTable(table, false);
        List<Split> planned = buckets.newScan().plan().splits();
        assertThat(planned).hasSize(2);
        for (Split split : planned) {
            DataSplit data = (DataSplit) split;
            assertThat(data.dataFiles()).isEmpty();
            assertThat(data.deleteOrderingGuards()).isNotEmpty();
            try (RecordReaderIterator<InternalRow> rows =
                    new RecordReaderIterator<>(buckets.newRead().createReader(split))) {
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(2)).isZero();
                assertThat(rows.hasNext()).isFalse();
            }
        }
        assertThat(
                        buckets.newScan()
                                .withPartitionFilter(Collections.singletonMap("pt", "1"))
                                .plan()
                                .splits())
                .hasSize(1);
        assertThat(buckets.newScan().withBucket(1).plan().splits()).isEmpty();
        assertThat(buckets.newScan().withShard(1, 2).plan().splits()).isEmpty();
        StartingScanner.ScannedResult restarted =
                (StartingScanner.ScannedResult)
                        new ContinuousCompactorStartingScanner(table.snapshotManager())
                                .scan(buckets.newSnapshotReader());
        assertThat(restarted.currentSnapshotId())
                .isEqualTo(table.snapshotManager().latestSnapshotId());
        assertThat(restarted.splits()).hasSize(2);
    }

    @Test
    void testGuardOnlyConsolidationFinalizationFailurePreservesInputs() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        List<String> originalFiles = filesOnDisk();
        List<IndexManifestEntry> original = guards(table);
        FileIO failingIo =
                new TraceableFileIO() {
                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        return new PositionOutputStreamWrapper(
                                super.newOutputStream(path, overwrite)) {
                            @Override
                            public void close() throws IOException {
                                out.close();
                                throw new IOException(
                                        "Injected consolidation finalization failure");
                            }
                        };
                    }
                };
        PrimaryKeyFileStoreTable failing =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(failingIo, table.location(), table.schema());
        try (TableWriteImpl<KeyValue> write = newWrite(failing)) {
            write.compact(PARTITION, 0, true);
            assertThatThrownBy(() -> write.prepareCommit(true, 4))
                    .hasStackTraceContaining("Injected consolidation finalization failure");
        }
        assertThat(filesOnDisk()).containsExactlyInAnyOrderElementsOf(originalFiles);
        assertThat(guards(table)).isEqualTo(original);
    }

    @Test
    void testRollbackFencesWritersAndPreservesInternalSequenceWatermark() throws Exception {
        PrimaryKeyFileStoreTable table = createLifecycleTable(Collections.emptyMap());
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        Snapshot target = table.snapshotManager().latestSnapshot();
        append(table, 1, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 2, compactNatively(table, 2, PARTITION));
        long watermark =
                SequenceSnapshotProperties.guardMaxSequenceNumber(
                                table.snapshotManager().latestSnapshot())
                        .getAsLong();
        List<CommitMessage> pending;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.INSERT, 1, 1, 3));
            pending = write.prepareCommit(true, 3);
        }
        try (FileStoreCommit rollback = table.store().newCommit(USER, null)) {
            assertThat(rollback.rollbackToAsLatest(target)).isTrue();
        }
        assertThat(
                        SequenceSnapshotProperties.guardMaxSequenceNumber(
                                        table.snapshotManager().latestSnapshot())
                                .getAsLong())
                .isGreaterThanOrEqualTo(watermark);
        assertThatThrownBy(() -> commit(table, 4, pending))
                .hasStackTraceContaining("Delete ordering guard generation changed");
        try (FileStoreCommit abort = table.store().newCommit(USER, null)) {
            abort.abort(pending);
        }
        assertThat(readRows(table))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(1));
        append(table, 5, row(RowKind.INSERT, 1, 1, 2));
        assertThat(readRows(table))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(2));
    }

    @Test
    void testConsolidationAcrossPayloadSchemaEvolution() throws Exception {
        PrimaryKeyFileStoreTable table = createLifecycleTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 1, compactNatively(table, 1, PARTITION));
        TableSchema evolved =
                new FileSystemSchemaManager(table.fileIO(), table.location())
                        .commitChanges(SchemaChange.addColumn("added", DataTypes.INT()));
        PrimaryKeyFileStoreTable reopened =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(table.fileIO(), table.location(), evolved);
        append(reopened, 2, GenericRow.ofKind(RowKind.DELETE, 1, 1, 7, null, null));
        commit(reopened, 3, compactNatively(reopened, 3, PARTITION));
        assertThat(guards(reopened)).hasSize(2);
        commit(reopened, 4, compactNatively(reopened, 4, PARTITION));
        assertThat(readGuards(reopened))
                .singleElement()
                .satisfies(g -> assertThat(g.ordering().getInt(0)).isEqualTo(7));
        append(reopened, 5, GenericRow.of(1, 1, 6, BinaryString.fromString("stale"), 42));
        assertThat(readRows(reopened)).isEmpty();
    }

    @Test
    void testConsolidatedGuardsRespectSnapshotAndTagRetention() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        append(table, 4, row(RowKind.INSERT, 1, 1, 6));
        Split pinned = table.newScan().plan().splits().get(0);
        table.createTag("before-consolidation", table.snapshotManager().latestSnapshotId());
        List<IndexManifestEntry> sources = guards(table);
        commit(table, 5, compactNatively(table, 5, PARTITION));
        assertThat(guards(table)).hasSize(1);
        IndexManifestEntry replacement = guards(table).get(0);
        ((ExpireSnapshotsImpl) table.newExpireSnapshots())
                .expireUntil(
                        Snapshot.FIRST_SNAPSHOT_ID, table.snapshotManager().latestSnapshotId());
        for (IndexManifestEntry source : sources) {
            assertThat(table.fileIO().exists(table.store().newIndexFileHandler().filePath(source)))
                    .isTrue();
        }
        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(table.newRead().createReader(pinned))) {
            assertThat(rows.hasNext()).isFalse();
        }
        table.deleteTag("before-consolidation");
        for (IndexManifestEntry source : sources) {
            assertThat(table.fileIO().exists(table.store().newIndexFileHandler().filePath(source)))
                    .isTrue();
        }
        new LocalOrphanFilesClean(table, Long.MAX_VALUE).clean();
        for (IndexManifestEntry source : sources) {
            assertThat(table.fileIO().exists(table.store().newIndexFileHandler().filePath(source)))
                    .isFalse();
        }
        assertThat(table.fileIO().exists(table.store().newIndexFileHandler().filePath(replacement)))
                .isTrue();
        append(table, 6, row(RowKind.INSERT, 1, 1, 6));
        assertThat(readRows(table)).isEmpty();
    }

    @Test
    void testManifestReplacementPreservesGuardMetadataAndFencesWriters() throws Exception {
        PrimaryKeyFileStoreTable table = twoGuardSegments();
        Snapshot base = table.snapshotManager().latestSnapshot();
        List<CommitMessage> pending;
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.INSERT, 1, 1, 6));
            pending = write.prepareCommit(true, 4);
        }
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit(USER, null)) {
            assertThat(
                            commit.replaceManifestList(
                                    base,
                                    base.totalRecordCount(),
                                    Pair.of(base.baseManifestList(), base.baseManifestListSize()),
                                    Pair.of(base.deltaManifestList(), base.deltaManifestListSize()),
                                    base.indexManifest(),
                                    base.nextRowId(),
                                    Collections.emptyMap()))
                    .isTrue();
        }
        assertThat(
                        SequenceSnapshotProperties.guardMaxSequenceNumber(
                                table.snapshotManager().latestSnapshot()))
                .isEqualTo(SequenceSnapshotProperties.guardMaxSequenceNumber(base));
        assertThatThrownBy(() -> commit(table, 5, pending))
                .hasStackTraceContaining("Delete ordering guard generation changed");
        try (FileStoreCommit abort = table.store().newCommit(USER, null)) {
            abort.abort(pending);
        }
        append(table, 6, row(RowKind.INSERT, 1, 1, 6));
        assertThat(readRows(table)).isEmpty();
    }

    private PrimaryKeyFileStoreTable createLifecycleTable(Map<String, String> additional)
            throws Exception {
        Map<String, String> options = new HashMap<>(additional);
        options.put(DELETE_ORDERING_GUARDS_ENABLED.key(), "true");
        options.put("delete-ordering-guards.compaction.min-file-num", "100");
        return createTable(options);
    }

    private PrimaryKeyFileStoreTable twoGuardSegments() throws Exception {
        PrimaryKeyFileStoreTable table = createLifecycleTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 1, compactNatively(table, 1, PARTITION));
        append(table, 2, row(RowKind.DELETE, 1, 1, 7));
        commit(table, 3, compactNatively(table, 3, PARTITION));
        assertThat(guards(table)).hasSize(2);
        assertThat(table.newScan().plan().splits()).isEmpty();
        return table;
    }

    @ParameterizedTest
    @CsvSource({
        "none,false",
        "input,false",
        "lookup,false",
        "full-compaction,false",
        "none,true",
        "input,true",
        "lookup,true",
        "full-compaction,true"
    })
    void testAtomicPublicationAndCommitMessageRoundTrip(String producer, boolean indexInDataDir)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("changelog-producer", producer);
        options.put("index-file-in-data-file-dir", String.valueOf(indexInDataDir));
        PrimaryKeyFileStoreTable table = createTable(options);
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        compactAndCommit(table, 1, PARTITION);
        long before = table.snapshotManager().latestSnapshotId();

        List<CommitMessage> messages;
        try (TableWriteImpl<KeyValue> write = capturingWrite(table)) {
            write.write(row(RowKind.DELETE, 1, 1, 4));
            write.compact(PARTITION, 0, true);
            messages = write.prepareCommit(true, 2);
        }
        assertThat(guards(table)).isEmpty();
        List<IndexFileMeta> staged = newGuards(messages);
        assertThat(staged).hasSize(1);
        assertThat(guardFile(table, PARTITION, 0).exists(staged.get(0))).isTrue();
        if (producer.equals("input")) {
            assertThat(changelog(table, messages, true)).containsExactly("DELETE/4/payload");
        } else if (producer.equals("lookup") || producer.equals("full-compaction")) {
            assertThat(changelog(table, messages, false)).containsExactly("DELETE/1/payload");
        }

        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> restored = new ArrayList<>();
        for (CommitMessage message : messages) {
            CommitMessage recovered =
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(message));
            assertThat(recovered).isEqualTo(message);
            restored.add(recovered);
        }
        commit(table, 2, restored);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(table.store().newIndexFileHandler().scan(snapshot, DELETE_ORDERING_GUARD))
                .extracting(IndexManifestEntry::indexFile)
                .containsExactlyElementsOf(staged);
        assertThat(
                        table.store()
                                .newIndexFileHandler()
                                .scan(
                                        table.snapshotManager().snapshot(before),
                                        DELETE_ORDERING_GUARD))
                .isEmpty();
        assertThat(table.newScan().plan().splits()).isEmpty();
        List<DeleteOrderingGuard> captured = readGuards(table);
        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).ordering().getInt(0)).isEqualTo(4);
        assertThat(captured.get(0).sequenceNumber()).isEqualTo(1);
        assertThat(snapshot.properties())
                .containsEntry(SequenceSnapshotProperties.GUARD_MAX_SEQUENCE_NUMBER, "1");
    }

    @Test
    void testGuardOnlyBucketRestoresReferencesAndSequenceWatermark() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        append(table, 1, row(RowKind.DELETE, 1, 1, 4));
        compactAndCommit(table, 2, PARTITION);
        List<IndexManifestEntry> originalGuards = guards(table);
        assertThat(originalGuards).hasSize(1);
        assertThat(table.newScan().plan().splits()).isEmpty();

        PrimaryKeyFileStoreTable reopened =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(
                                table.fileIO(), table.location(), table.schema());
        assertThat(guards(reopened)).isEqualTo(originalGuards);
        assertThat(readGuards(reopened).get(0).sequenceNumber()).isEqualTo(1);
        try (TableWriteImpl<KeyValue> write = capturingWrite(reopened)) {
            write.compact(PARTITION, 0, true);
            assertThat(newGuards(write.prepareCommit(true, 3))).isEmpty();
            write.write(row(RowKind.INSERT, 1, 2, 8));
            List<CommitMessage> next = write.prepareCommit(true, 4);
            assertThat(
                            next.stream()
                                    .map(m -> (CommitMessageImpl) m)
                                    .flatMap(m -> m.newFilesIncrement().newFiles().stream())
                                    .mapToLong(DataFileMeta::minSequenceNumber)
                                    .min()
                                    .getAsLong())
                    .isEqualTo(2);
            commit(reopened, 4, next);
        }
        assertThat(guards(reopened)).isEqualTo(originalGuards);
        assertThat(reopened.snapshotManager().latestSnapshot().properties())
                .containsEntry(SequenceSnapshotProperties.GUARD_MAX_SEQUENCE_NUMBER, "2");
    }

    @Test
    void testSegmentsAccumulateAndPartitionResetPreservesOtherGuards() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4), row(RowKind.DELETE, 2, 1, 7));
        compactAndCommit(table, 1, PARTITION, partition(2));
        List<IndexManifestEntry> first = guards(table);
        assertThat(first).hasSize(2);
        append(table, 2, row(RowKind.DELETE, 1, 2, 9));
        compactAndCommit(table, 3, PARTITION);
        assertThat(guards(table)).hasSize(3).containsAll(first);

        try (FileStoreCommit commit = table.store().newCommit("reset", null)) {
            commit.overwritePartition(
                    Collections.singletonMap("pt", "1"), new ManifestCommittable(0));
        }
        assertThat(guards(table))
                .hasSize(1)
                .allMatch(entry -> entry.partition().equals(partition(2)));
        assertThat(readGuards(table).get(0).ordering().getInt(0)).isEqualTo(7);
        // Reset drops references; retained snapshots still own the old immutable files.
        for (IndexManifestEntry entry : first) {
            assertThat(
                            guardFile(table, entry.partition(), entry.bucket())
                                    .exists(entry.indexFile()))
                    .isTrue();
        }
        try (FileStoreCommit commit = table.store().newCommit("truncate", null)) {
            commit.truncateTable(0);
        }
        assertThat(guards(table)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"partition", "truncate", "rewrite"})
    void testStaleGuardPublicationIsFencedBySourceFiles(String reset) throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        List<CommitMessage> stale = compact(table, 1, PARTITION);
        assertThat(newGuards(stale)).hasSize(1);
        if (reset.equals("rewrite")) {
            compactAndCommit(table, 2, PARTITION);
        } else {
            try (FileStoreCommit commit = table.store().newCommit("reset", null)) {
                if (reset.equals("partition")) {
                    commit.overwritePartition(
                            Collections.singletonMap("pt", "1"), new ManifestCommittable(0));
                } else {
                    commit.truncateTable(0);
                }
            }
        }
        long latest = table.snapshotManager().latestSnapshotId();
        List<IndexManifestEntry> retained = guards(table);
        assertThatThrownBy(() -> commit(table, 3, stale))
                .hasStackTraceContaining(
                        reset.equals("rewrite") ? "guard sources changed" : "conflict");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(latest);
        assertThat(guards(table)).isEqualTo(retained);
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.abort(stale);
        }
        assertThat(guardFile(table, PARTITION, 0).exists(newGuards(stale).get(0))).isFalse();
    }

    @Test
    void testIndexOnlyAndAppendPublicationAreRejected() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        List<CommitMessage> prepared = compact(table, 1, PARTITION);
        List<IndexFileMeta> newGuards = newGuards(prepared);
        CompactIncrement compact =
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        newGuards,
                        Collections.emptyList());
        CommitMessage indexOnly =
                new CommitMessageImpl(PARTITION, 0, 1, DataIncrement.emptyIncrement(), compact);
        assertThatThrownBy(() -> commit(table, 1, Collections.singletonList(indexOnly)))
                .hasStackTraceContaining("requires source-file replacements");
        CommitMessage append =
                new CommitMessageImpl(
                        PARTITION,
                        0,
                        1,
                        DataIncrement.indexIncrement(newGuards),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList()));
        assertThatThrownBy(() -> commit(table, 1, Collections.singletonList(append)))
                .hasStackTraceContaining("must be published by compaction");
        assertThat(guards(table)).isEmpty();
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.abort(prepared);
        }
    }

    @Test
    void testGuardRetirementRequiresExplicitReset() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        compactAndCommit(table, 1, PARTITION);
        List<IndexManifestEntry> retained = guards(table);
        CompactIncrement retirement =
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(retained.get(0).indexFile()));
        CommitMessage message =
                new CommitMessageImpl(PARTITION, 0, 1, DataIncrement.emptyIncrement(), retirement)
                        .withDeleteOrderingGuardSources(
                                Collections.singletonList(retained.get(0).indexFile().fileName()));
        assertThatThrownBy(() -> commit(table, 2, Collections.singletonList(message)))
                .hasStackTraceContaining("can only be removed by overwrite/reset");
        assertThat(guards(table)).isEqualTo(retained);
    }

    @Test
    void testWriteOnlyUsesDedicatedCompactionForCapture() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("write-only", "true");
        options.put(DELETE_ORDERING_GUARDS_ENABLED.key(), "true");
        PrimaryKeyFileStoreTable table = createTable(options);
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            assertThatThrownBy(() -> write.compact(PARTITION, 0, true))
                    .hasMessageContaining(
                            "NoopCompactManager does not support user triggered compaction");
            assertThat(newGuards(write.prepareCommit(true, 1))).isEmpty();
        }
        assertThat(guards(table)).isEmpty();
        PrimaryKeyFileStoreTable dedicated =
                (PrimaryKeyFileStoreTable)
                        table.copy(Collections.singletonMap("write-only", "false"));
        commit(dedicated, 2, compactNatively(dedicated, 2, PARTITION));
        assertThat(readGuards(table)).hasSize(1);
        append(table, 3, row(RowKind.INSERT, 1, 2, 8));
        assertThat(table.store().newScan().plan().files())
                .allMatch(entry -> entry.file().minSequenceNumber() == 1);
        assertThat(guards(table)).hasSize(1);
    }

    @Test
    void testExpiredDeleteProducesNoGuard() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("record-level.expire-time", "1 s");
        options.put("record-level.time-field", "seq");
        options.put(DELETE_ORDERING_GUARDS_ENABLED.key(), "true");
        PrimaryKeyFileStoreTable table = createTable(options);
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        commit(table, 1, compactNatively(table, 1, PARTITION));
        assertThat(table.newScan().plan().splits()).isEmpty();
        assertThat(guards(table)).isEmpty();
        assertThat(indexFilesOnDisk()).isEmpty();
        assertThat(
                        SequenceSnapshotProperties.guardMaxSequenceNumber(
                                table.snapshotManager().latestSnapshot()))
                .isEmpty();
    }

    @Test
    void testPreCommitFailureLeavesSnapshotUnchangedAndAbortCleansGuard() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4), row(RowKind.INSERT, 1, 2, 5));
        List<CommitMessage> prepared = compact(table, 1, PARTITION);
        long latest = table.snapshotManager().latestSnapshotId();
        CommitPreCallback failure =
                new CommitPreCallback() {
                    @Override
                    public void call(
                            List<SimpleFileEntry> baseFiles,
                            List<ManifestEntry> deltaFiles,
                            List<IndexManifestEntry> indexFiles,
                            Snapshot snapshot) {
                        assertThat(indexFiles).hasSize(1);
                        assertThat(snapshot.indexManifest()).isNotNull();
                        throw new IllegalStateException("Injected pre-publication failure");
                    }

                    @Override
                    public void close() {}
                };
        try (FileStoreCommit commit =
                newCommit(
                        table,
                        new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty()),
                        Collections.singletonList(failure))) {
            assertThatThrownBy(() -> commit.commit(committable(1, prepared), false))
                    .hasStackTraceContaining("Injected pre-publication failure");
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(latest);
            assertThat(guards(table)).isEmpty();
            commit.abort(prepared);
            commit.abort(prepared);
        }
        assertThat(guardFile(table, PARTITION, 0).exists(newGuards(prepared).get(0))).isFalse();
        assertThat(table.newScan().plan().splits()).isNotEmpty();
    }

    @Test
    void testLostPublicationResponseRecoversWithoutDuplicatingOrDeletingGuards() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        List<CommitMessage> prepared = compact(table, 1, PARTITION);
        long latest = table.snapshotManager().latestSnapshotId();
        AtomicInteger publications = new AtomicInteger();
        SnapshotCommit delegate = new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty());
        SnapshotCommit publisher =
                new SnapshotCommit(Lock.empty()) {
                    @Override
                    protected boolean commitImpl(
                            @Nullable String baseSnapshotUuid,
                            Snapshot snapshot,
                            String branch,
                            List<PartitionStatistics> statistics)
                            throws Exception {
                        publications.incrementAndGet();
                        assertThat(delegate.commit(baseSnapshotUuid, snapshot, branch, statistics))
                                .isTrue();
                        throw new IOException("Publication response lost");
                    }
                };
        try (FileStoreCommit commit = newCommit(table, publisher, Collections.emptyList())) {
            assertThat(commit.commit(committable(1, prepared), false)).isEqualTo(1);
        }
        assertThat(publications).hasValue(1);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(latest + 1);
        assertThat(guards(table))
                .extracting(IndexManifestEntry::indexFile)
                .containsExactlyElementsOf(newGuards(prepared));
        assertThat(readGuards(table)).hasSize(1);
        assertThat(table.newScan().plan().splits()).isEmpty();
    }

    @Test
    void testCloseBeforeCommitHandoffDeletesGuardOutput() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        try (TableWriteImpl<KeyValue> write = capturingWrite(table)) {
            write.compact(PARTITION, 0, true);
            KeyValueFileStoreWrite storeWrite = (KeyValueFileStoreWrite) write.getWrite();
            storeWrite.writers.get(PARTITION).get(0).writer.sync();
            assertThat(indexFilesOnDisk()).hasSize(1);
        }
        assertThat(indexFilesOnDisk()).isEmpty();
        assertThat(guards(table)).isEmpty();
    }

    @Test
    void testCheckpointTransfersGuardOwnershipToRestoredWriter() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        List<FileStoreWrite.State<KeyValue>> state;
        try (TableWriteImpl<KeyValue> write = capturingWrite(table)) {
            write.compact(PARTITION, 0, true);
            // Checkpoints may leave asynchronous work pending; transfer a completed result.
            KeyValueFileStoreWrite storeWrite = (KeyValueFileStoreWrite) write.getWrite();
            storeWrite.writers.get(PARTITION).get(0).writer.sync();
            state = write.checkpoint();
        }
        assertThat(indexFilesOnDisk()).hasSize(1);
        try (TableWriteImpl<KeyValue> restored = capturingWrite(table)) {
            restored.restore(state);
            List<CommitMessage> prepared = restored.prepareCommit(true, 1);
            assertThat(newGuards(prepared)).hasSize(1);
            commit(table, 1, prepared);
        }
        assertThat(readGuards(table)).hasSize(1);
    }

    @Test
    void testGuardFinalizationFailureAbortsNativeDataOutput() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4), row(RowKind.INSERT, 1, 2, 5));
        List<String> originalFiles = filesOnDisk();
        FileIO failingIo =
                new TraceableFileIO() {
                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        return new PositionOutputStreamWrapper(
                                super.newOutputStream(path, overwrite)) {
                            @Override
                            public void close() throws IOException {
                                out.close();
                                throw new IOException("Injected guard finalization failure");
                            }
                        };
                    }
                };
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            installCapture(table, write, failingIo);
            write.compact(PARTITION, 0, true);
            assertThatThrownBy(() -> write.prepareCommit(true, 1))
                    .hasStackTraceContaining("Injected guard finalization failure");
        }
        assertThat(filesOnDisk()).containsExactlyInAnyOrderElementsOf(originalFiles);
        assertThat(guards(table)).isEmpty();
    }

    @Test
    void testCancellationCleansFinishedGuardWhoseResultWasNotDelivered() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        CountDownLatch finished = new CountDownLatch(1);
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.withCompactRewriterFactory(
                    (partition, bucket, nativeRewriter) ->
                            new DelegatingRewriter(
                                    capturingRewriter(
                                            table,
                                            partition,
                                            bucket,
                                            nativeRewriter,
                                            table.fileIO())) {
                                @Override
                                public CompactResult rewrite(
                                        int level,
                                        boolean dropDelete,
                                        List<List<SortedRun>> sections)
                                        throws Exception {
                                    CompactResult result =
                                            super.rewrite(level, dropDelete, sections);
                                    finished.countDown();
                                    try {
                                        new CountDownLatch(1).await();
                                    } catch (InterruptedException expected) {
                                        Thread.currentThread().interrupt();
                                    }
                                    return result;
                                }
                            });
            write.compact(PARTITION, 0, true);
            assertThat(finished.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(indexFilesOnDisk()).hasSize(1);
            // close cancels the future before its completed guard result can be fetched.
        }
        assertThat(indexFilesOnDisk()).isEmpty();
        assertThat(guards(table)).isEmpty();
    }

    @Test
    void testTaskFailureAbortsGuardFromEarlierSuccessfulRewrite() throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap("compaction.small-file-ratio", "0"));
        // Disjoint large-enough files are rewritten separately within the same full compaction.
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        append(table, 1, row(RowKind.DELETE, 1, 2, 7));
        AtomicInteger rewrites = new AtomicInteger();
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.withCompactRewriterFactory(
                    (partition, bucket, nativeRewriter) ->
                            new DelegatingRewriter(
                                    capturingRewriter(
                                            table,
                                            partition,
                                            bucket,
                                            nativeRewriter,
                                            table.fileIO())) {
                                @Override
                                public CompactResult rewrite(
                                        int level,
                                        boolean dropDelete,
                                        List<List<SortedRun>> sections)
                                        throws Exception {
                                    if (rewrites.incrementAndGet() == 2) {
                                        throw new IOException("Injected second rewrite failure");
                                    }
                                    return super.rewrite(level, dropDelete, sections);
                                }
                            });
            write.compact(PARTITION, 0, true);
            assertThatThrownBy(() -> write.prepareCommit(true, 2))
                    .hasStackTraceContaining("Injected second rewrite failure");
            assertThat(rewrites).hasValue(2);
            // The task, rather than closing the writer later, must abort its earlier output.
            assertThat(indexFilesOnDisk()).isEmpty();
        }
        assertThat(guards(table)).isEmpty();
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> extraOptions)
            throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("pt", DataTypes.INT())
                        .field("k", DataTypes.INT())
                        .field("seq", DataTypes.INT())
                        .field("payload", DataTypes.STRING())
                        .build();
        return createTable(extraOptions, rowType);
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "input", "lookup", "full-compaction"})
    void testPersistedGuardsResolveReadsAndLaterNativeCompactions(String producer)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("changelog-producer", producer);
        options.put(DELETE_ORDERING_GUARDS_ENABLED.key(), "true");
        PrimaryKeyFileStoreTable table = createTable(options);
        append(table, 0, row(RowKind.INSERT, 1, 1, 1));
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.write(row(RowKind.DELETE, 1, 1, 4));
            write.compact(PARTITION, 0, true);
            commit(table, 1, write.prepareCommit(true, 1));
        }
        commit(table, 2, compactNatively(table, 2, PARTITION));
        List<IndexManifestEntry> original = guards(table);
        assertThat(original).hasSize(1);

        PrimaryKeyFileStoreTable reopened =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(
                                table.fileIO(), table.location(), table.schema());
        append(reopened, 3, row(RowKind.INSERT, 1, 1, 2));
        for (Split split : reopened.newScan().plan().splits()) {
            DataSplit staleSplit = (DataSplit) split;
            assertThat(staleSplit.deleteOrderingGuards()).hasSize(1);
            assertThat(staleSplit.rawConvertible()).isFalse();
            assertThat(staleSplit.convertToRawFiles()).isEmpty();
        }
        assertThat(readRows(reopened)).isEmpty();

        // Native compaction must resolve existing guards and capture future explicit deletes
        // while the option remains enabled, without installing a custom rewriter.
        try (TableWriteImpl<KeyValue> write = newWrite(reopened)) {
            write.compact(PARTITION, 0, true);
            List<CommitMessage> messages = write.prepareCommit(true, 4);
            assertThat(newGuards(messages)).isEmpty();
            assertThat(changelog(reopened, messages, false)).isEmpty();
            commit(reopened, 4, messages);
        }
        assertThat(reopened.newScan().plan().splits()).isEmpty();
        assertThat(guards(reopened)).isEqualTo(original);

        append(reopened, 5, row(RowKind.INSERT, 1, 1, 5));
        assertThat(readRows(reopened))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(5));
        try (TableWriteImpl<KeyValue> write = newWrite(reopened)) {
            write.compact(PARTITION, 0, true);
            commit(reopened, 6, write.prepareCommit(true, 6));
            write.write(row(RowKind.DELETE, 1, 1, 7));
            write.compact(PARTITION, 0, true);
            List<CommitMessage> messages = write.prepareCommit(true, 7);
            assertThat(newGuards(messages)).hasSize(1);
            if (producer.equals("lookup") || producer.equals("full-compaction")) {
                assertThat(changelog(reopened, messages, false))
                        .containsExactly("DELETE/5/payload");
            }
            commit(reopened, 7, messages);
            write.write(row(RowKind.INSERT, 1, 1, 6));
            write.compact(PARTITION, 0, true);
            List<CommitMessage> stale = write.prepareCommit(true, 8);
            assertThat(changelog(reopened, stale, false)).isEmpty();
            if (producer.equals("input")) {
                assertThat(changelog(reopened, stale, true)).containsExactly("INSERT/6/payload");
            }
            commit(reopened, 8, stale);
        }
        assertThat(readRows(reopened)).isEmpty();
        assertThat(guards(reopened)).hasSize(1);
        assertThat(readGuards(reopened))
                .singleElement()
                .satisfies(g -> assertThat(g.ordering().getInt(0)).isEqualTo(7));
    }

    @Test
    void testPlannedSplitPinsGuardsAcrossCommitsAndSerialization() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 2));
        compactAndCommit(table, 1, PARTITION);
        append(table, 2, row(RowKind.INSERT, 1, 1, 3));
        DataSplit planned = (DataSplit) table.newScan().plan().splits().get(0);
        DataOutputSerializer output = new DataOutputSerializer(128);
        planned.serialize(output);
        DataSplit restored =
                DataSplit.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
        assertThat(restored).isEqualTo(planned);
        assertThat(restored.deleteOrderingGuards()).hasSize(1);

        append(table, 3, row(RowKind.DELETE, 1, 1, 4));
        compactAndCommit(table, 4, PARTITION);
        assertThat(readRows(table)).isEmpty();
        List<InternalRow> oldRows = new ArrayList<>();
        table.newRead().createReader(restored).forEachRemaining(oldRows::add);
        assertThat(oldRows).singleElement().satisfies(r -> assertThat(r.getInt(2)).isEqualTo(3));
        assertThat(restored.deleteOrderingGuards()).hasSize(1);
        assertThat(guards(table)).hasSize(2);
    }

    @Test
    void testCheckpointRestoresOrderingProtectionBeforePublication() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        List<FileStoreWrite.State<KeyValue>> checkpoint;
        try (TableWriteImpl<KeyValue> write = capturingWrite(table)) {
            write.compact(PARTITION, 0, true);
            ((KeyValueFileStoreWrite) write.getWrite()).writers.get(PARTITION).get(0).writer.sync();
            checkpoint = write.checkpoint();
        }
        assertThat(guards(table)).isEmpty();
        try (TableWriteImpl<KeyValue> restored = newWrite(table)) {
            restored.restore(checkpoint);
            restored.write(row(RowKind.INSERT, 1, 1, 2));
            restored.compact(PARTITION, 0, true);
            commit(table, 1, restored.prepareCommit(true, 1));
        }
        assertThat(readRows(table)).isEmpty();
        assertThat(table.newScan().plan().splits()).isEmpty();
        assertThat(guards(table)).hasSize(1);
    }

    @Test
    void testPayloadSchemaEvolutionAndOrderingPolicyChanges() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        compactAndCommit(table, 1, PARTITION);
        TableSchema evolved =
                new FileSystemSchemaManager(table.fileIO(), table.location())
                        .commitChanges(SchemaChange.addColumn("added", DataTypes.INT()));
        PrimaryKeyFileStoreTable reopened =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(table.fileIO(), table.location(), evolved);
        append(reopened, 2, GenericRow.of(1, 1, 2, BinaryString.fromString("stale"), 42));
        assertThat(readRows(reopened)).isEmpty();
        append(reopened, 3, GenericRow.of(1, 1, 5, BinaryString.fromString("fresh"), 43));
        assertThat(readRows(reopened))
                .singleElement()
                .satisfies(
                        r -> {
                            assertThat(r.getString(3).toString()).isEqualTo("fresh");
                            assertThat(r.getInt(4)).isEqualTo(43);
                        });
        PrimaryKeyFileStoreTable changed =
                (PrimaryKeyFileStoreTable)
                        reopened.copy(
                                Collections.singletonMap(
                                        "sequence.field.sort-order", "descending"));
        assertThatThrownBy(() -> readRows(changed))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("ordering migration");
    }

    @Test
    void testMissingGuardFailsReadInsteadOfExposingStalePayload() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        compactAndCommit(table, 1, PARTITION);
        append(table, 2, row(RowKind.INSERT, 1, 1, 2));
        IndexManifestEntry segment = guards(table).get(0);
        table.fileIO().delete(table.store().newIndexFileHandler().filePath(segment), false);
        assertThatThrownBy(() -> readRows(table)).isInstanceOf(IOException.class);
    }

    @Test
    void testGuardWithEmptyProjection() throws Exception {
        RowType type =
                RowType.builder()
                        .field("pt", DataTypes.INT().notNull())
                        .field("k", DataTypes.INT().notNull())
                        .field("aSeq", DataTypes.INT())
                        .field("a", DataTypes.STRING())
                        .field("bSeq", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .build();
        Map<String, String> options = new HashMap<>();
        options.put("sequence.field", "aSeq");
        PrimaryKeyFileStoreTable table = createTable(options, type);
        append(table, 0, GenericRow.ofKind(RowKind.DELETE, 1, 1, 4, null, 7, null));
        compactAndCommit(table, 1, PARTITION);
        append(
                table,
                2,
                GenericRow.of(
                        1,
                        1,
                        2,
                        BinaryString.fromString("stale"),
                        8,
                        BinaryString.fromString("fresh")));
        assertThat(readRows(table)).isEmpty();
        List<InternalRow> emptyProjection = new ArrayList<>();
        table.newRead()
                .withReadType(RowType.of())
                .createReader(table.newScan().plan())
                .forEachRemaining(emptyProjection::add);
        assertThat(emptyProjection).isEmpty();
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            write.compact(PARTITION, 0, true);
            commit(table, 3, write.prepareCommit(true, 3));
        }
        assertThat(table.newScan().plan().splits()).isEmpty();
    }

    @Test
    void testSequenceInitializationIncludesSnapshotActuallyRestored() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, 1, 1, 4));
        compactAndCommit(table, 1, PARTITION);
        Snapshot restoredSnapshot = mock(Snapshot.class);
        when(restoredSnapshot.id()).thenReturn(table.snapshotManager().latestSnapshotId() + 1);
        when(restoredSnapshot.properties())
                .thenReturn(
                        Collections.singletonMap(
                                SequenceSnapshotProperties.GUARD_MAX_SEQUENCE_NUMBER, "100"));
        WriteRestore restore = mock(WriteRestore.class);
        when(restore.restoreFiles(PARTITION, 0, false, false, false))
                .thenReturn(
                        new RestoreFiles(
                                restoredSnapshot,
                                1,
                                Collections.emptyList(),
                                null,
                                null,
                                null,
                                guards(table).stream()
                                        .map(IndexManifestEntry::indexFile)
                                        .collect(Collectors.toList())));
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            KeyValueFileStoreWrite storeWrite = (KeyValueFileStoreWrite) write.getWrite();
            storeWrite.withWriteRestore(restore);
            assertThat(storeWrite.createWriter(PARTITION, 0).maxSequenceNumber()).isEqualTo(100);
        }
    }

    private List<InternalRow> readRows(PrimaryKeyFileStoreTable table) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        table.newRead()
                .createReader(table.newScan().plan())
                .forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> extraOptions, RowType rowType)
            throws Exception {
        Map<String, String> options = new HashMap<>(extraOptions);
        options.put("bucket", "1");
        if (rowType.getFieldNames().contains("seq")) {
            options.put("sequence.field", "seq");
        }
        options.put("num-sorted-run.compaction-trigger", "100");
        options.put("num-levels", "3");
        options.put("dynamic-partition-overwrite", "false");
        Path path = new Path(tempDir.toUri());
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options,
                                ""));
        return (PrimaryKeyFileStoreTable)
                FileStoreTableFactory.create(LocalFileIO.create(), path, schema);
    }

    private TableWriteImpl<KeyValue> newWrite(PrimaryKeyFileStoreTable table) throws IOException {
        IOManager io = new IOManagerImpl(tempDir.resolve("spill").toString());
        ioManagers.add(io);
        return table.newWrite(USER).withIOManager(io);
    }

    private TableWriteImpl<KeyValue> capturingWrite(PrimaryKeyFileStoreTable table)
            throws IOException {
        TableWriteImpl<KeyValue> write = newWrite(table);
        installCapture(table, write, table.fileIO());
        return write;
    }

    private void installCapture(
            PrimaryKeyFileStoreTable table, TableWriteImpl<KeyValue> write, FileIO guardIo) {
        write.withCompactRewriterFactory(
                (partition, bucket, rewriter) ->
                        capturingRewriter(table, partition, bucket, rewriter, guardIo));
    }

    private CompactRewriter capturingRewriter(
            PrimaryKeyFileStoreTable table,
            BinaryRow partition,
            int bucket,
            CompactRewriter rewriter,
            FileIO guardIo) {
        DeleteOrderingGuardSerializer serializer = serializer(table);
        DeleteOrderingGuardFile file =
                new DeleteOrderingGuardFile(
                        guardIo,
                        table.store().pathFactory().indexFileFactory(partition, bucket),
                        serializer,
                        cacheManager,
                        4096);
        return ((MergeTreeCompactRewriter) rewriter).withDeleteOrderingGuards(serializer, file);
    }

    private DeleteOrderingGuardSerializer serializer(PrimaryKeyFileStoreTable table) {
        return new DeleteOrderingGuardSerializer(
                table.schema().id(),
                new RowType(
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(
                                table.schema())),
                table.rowType(),
                table.coreOptions());
    }

    private DeleteOrderingGuardFile guardFile(
            PrimaryKeyFileStoreTable table, BinaryRow partition, int bucket) {
        return new DeleteOrderingGuardFile(
                table.fileIO(),
                table.store().pathFactory().indexFileFactory(partition, bucket),
                serializer(table),
                cacheManager,
                4096);
    }

    private List<IndexManifestEntry> guards(PrimaryKeyFileStoreTable table) {
        return table.store().newIndexFileHandler().scan(DELETE_ORDERING_GUARD);
    }

    private List<DeleteOrderingGuard> readGuards(PrimaryKeyFileStoreTable table) throws Exception {
        List<DeleteOrderingGuard> result = new ArrayList<>();
        for (IndexManifestEntry entry : guards(table)) {
            guardFile(table, entry.partition(), entry.bucket())
                    .read(entry.indexFile())
                    .forEachRemaining(result::add);
        }
        return result;
    }

    private List<IndexFileMeta> newGuards(List<CommitMessage> messages) {
        return messages.stream()
                .map(message -> (CommitMessageImpl) message)
                .flatMap(message -> message.compactIncrement().newIndexFiles().stream())
                .filter(file -> file.indexType().equals(DELETE_ORDERING_GUARD))
                .collect(Collectors.toList());
    }

    private List<String> changelog(
            PrimaryKeyFileStoreTable table, List<CommitMessage> messages, boolean input)
            throws Exception {
        List<String> result = new ArrayList<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            List<DataFileMeta> files =
                    input
                            ? impl.newFilesIncrement().changelogFiles()
                            : impl.compactIncrement().changelogFiles();
            if (files.isEmpty()) {
                continue;
            }
            DataSplit split =
                    DataSplit.builder()
                            .withPartition(impl.partition())
                            .withBucket(impl.bucket())
                            .withBucketPath("unused")
                            .withDataFiles(files)
                            .isStreaming(true)
                            .rawConvertible(false)
                            .build();
            try (RecordReaderIterator<KeyValue> rows =
                    new RecordReaderIterator<>(table.store().newRead().createReader(split))) {
                while (rows.hasNext()) {
                    KeyValue kv = rows.next();
                    result.add(
                            kv.valueKind().name()
                                    + "/"
                                    + kv.value().getInt(2)
                                    + "/"
                                    + kv.value().getString(3));
                }
            }
        }
        return result;
    }

    private void append(PrimaryKeyFileStoreTable table, long identifier, InternalRow... rows)
            throws Exception {
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit(table, identifier, write.prepareCommit(true, identifier));
        }
    }

    private List<CommitMessage> compact(
            PrimaryKeyFileStoreTable table, long identifier, BinaryRow... partitions)
            throws Exception {
        try (TableWriteImpl<KeyValue> write = capturingWrite(table)) {
            for (BinaryRow partition : partitions) {
                write.compact(partition, 0, true);
            }
            return write.prepareCommit(true, identifier);
        }
    }

    private List<CommitMessage> compactNatively(
            PrimaryKeyFileStoreTable table, long identifier, BinaryRow... partitions)
            throws Exception {
        try (TableWriteImpl<KeyValue> write = newWrite(table)) {
            for (BinaryRow partition : partitions) {
                write.compact(partition, 0, true);
            }
            return write.prepareCommit(true, identifier);
        }
    }

    private void compactAndCommit(
            PrimaryKeyFileStoreTable table, long identifier, BinaryRow... partitions)
            throws Exception {
        commit(table, identifier, compact(table, identifier, partitions));
    }

    private void commit(
            PrimaryKeyFileStoreTable table, long identifier, List<CommitMessage> messages)
            throws Exception {
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.commit(committable(identifier, messages), false);
        }
    }

    private ManifestCommittable committable(long identifier, List<CommitMessage> messages) {
        ManifestCommittable result = new ManifestCommittable(identifier);
        messages.forEach(result::addFileCommittable);
        return result;
    }

    private FileStoreCommitImpl newCommit(
            PrimaryKeyFileStoreTable table,
            SnapshotCommit publisher,
            List<CommitPreCallback> preCallbacks) {
        KeyValueFileStore store = table.store();
        return new FileStoreCommitImpl(
                publisher,
                table.fileIO(),
                new FileSystemSchemaManager(table.fileIO(), table.location()),
                table.name(),
                USER,
                store.partitionType(),
                store.options(),
                store.pathFactory(),
                store.snapshotManager(),
                store.manifestFileFactory(),
                store.manifestListFactory(),
                store.indexManifestFileFactory(),
                store::newScan,
                store.newStatsFileHandler(),
                store.bucketMode(),
                preCallbacks,
                Collections.emptyList(),
                scanner ->
                        ConflictDetection.create(
                                table.name(),
                                USER,
                                store.partitionType(),
                                store.pathFactory(),
                                store.newKeyComparator(),
                                store.bucketMode(),
                                false,
                                false,
                                false,
                                false,
                                store.newIndexFileHandler(),
                                store.snapshotManager(),
                                scanner),
                null);
    }

    private List<String> indexFilesOnDisk() throws IOException {
        return filesOnDisk().stream()
                .filter(
                        path ->
                                java.nio.file.Paths.get(path)
                                        .getFileName()
                                        .toString()
                                        .startsWith("index-"))
                .collect(Collectors.toList());
    }

    private List<String> filesOnDisk() throws IOException {
        try (Stream<java.nio.file.Path> paths = Files.walk(tempDir)) {
            return paths.filter(Files::isRegularFile)
                    .map(Object::toString)
                    .collect(Collectors.toList());
        }
    }

    private static InternalRow row(RowKind kind, int partition, int key, int sequence) {
        return GenericRow.ofKind(
                kind, partition, key, sequence, BinaryString.fromString("payload"));
    }

    private static BinaryRow partition(int value) {
        BinaryRow result = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(result);
        writer.writeInt(0, value);
        writer.complete();
        return result;
    }

    private static class DelegatingRewriter implements CompactRewriter {

        private final CompactRewriter delegate;

        private DelegatingRewriter(CompactRewriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompactResult rewrite(int level, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception {
            return delegate.rewrite(level, dropDelete, sections);
        }

        @Override
        public CompactResult upgrade(int level, DataFileMeta file) throws Exception {
            return delegate.upgrade(level, file);
        }

        @Override
        public void abort(CompactResult result) {
            delegate.abort(result);
        }

        @Override
        public void release(CompactResult result) {
            delegate.release(result);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
