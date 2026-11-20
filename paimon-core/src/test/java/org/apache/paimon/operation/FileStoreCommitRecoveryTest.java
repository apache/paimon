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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Recovery tests for {@link FileStoreCommitImpl}. */
public class FileStoreCommitRecoveryTest {

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() {
        gen = new TestKeyValueGenerator();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFilterHalfCommittedIdentifier(boolean overwrite) throws Exception {
        TestFileStore store =
                createStore(Collections.singletonMap(CoreOptions.OVERWRITE_UPGRADE.key(), "false"));
        KeyValue row = gen.nextInsert("20211110", 8, 1L, null, "replacement");
        List<ManifestCommittable> prepared = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            store.commitDataImpl(
                    Collections.singletonList(row),
                    gen::getPartition,
                    value -> 0,
                    false,
                    2L,
                    123L,
                    Collections.emptyList(),
                    (commit, committable) -> prepared.add(committable));
        }
        CommitMessageImpl append = (CommitMessageImpl) prepared.get(0).fileCommittables().get(0);
        CommitMessageImpl replacement =
                (CommitMessageImpl) prepared.get(1).fileCommittables().get(0);
        IndexPathFactory indexFactory = store.pathFactory().indexFileFactory(append.partition(), 0);
        IndexFileMeta oldIndex = new IndexFileMeta(HASH_INDEX, "old-index", 0, 0, null, null, null);
        IndexFileMeta newIndex = new IndexFileMeta(HASH_INDEX, "new-index", 0, 0, null, null, null);
        store.fileIO().newOutputStream(indexFactory.toPath(newIndex), false).close();
        append.newFilesIncrement().newIndexFiles().add(oldIndex);
        CompactIncrement compact =
                new CompactIncrement(
                        append.newFilesIncrement().newFiles(),
                        replacement.newFilesIncrement().newFiles(),
                        Collections.emptyList(),
                        Collections.singletonList(newIndex),
                        Collections.singletonList(oldIndex));
        CommitMessageImpl message =
                new CommitMessageImpl(
                        append.partition(),
                        append.bucket(),
                        append.totalBuckets(),
                        append.newFilesIncrement(),
                        compact,
                        1L);
        ManifestCommittable pending =
                new ManifestCommittable(2, 123L, Collections.singletonList(message));
        pending.addProperty("recovery-test", "preserved");
        ManifestCommittable next = new ManifestCommittable(3);
        CommitCallback callback = mock(CommitCallback.class);
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(
                        store,
                        "writer",
                        new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty()),
                        Collections.singletonList(callback))) {
            commit.ignoreEmptyCommit(false);
            assertThat(commit.filterCommitted(Collections.singletonList(pending)))
                    .containsExactly(pending);
            if (overwrite) {
                commit.overwritePartition(Collections.emptyMap(), prepared.get(0));
            } else {
                commit.commit(prepared.get(0), false);
            }
            assertThat(store.snapshotManager().latestSnapshot().commitKind())
                    .isEqualTo(
                            overwrite ? Snapshot.CommitKind.OVERWRITE : Snapshot.CommitKind.APPEND);
            assertThat(commit.filterCommitted(Collections.singletonList(prepared.get(0))))
                    .isEmpty();
            // A newer snapshot from another user must not hide the interrupted first phase.
            try (FileStoreCommit other = store.newCommit("other", null)) {
                other.ignoreEmptyCommit(false);
                other.commit(new ManifestCommittable(100), false);
            }
            List<ManifestCommittable> filtered =
                    commit.filterCommitted(
                            Arrays.asList(
                                    new ManifestCommittable(1, null, pending.fileCommittables()),
                                    pending,
                                    next));
            assertThat(filtered).hasSize(2);
            assertThat(filtered.get(1)).isSameAs(next);
            ManifestCommittable recovered = filtered.get(0);
            assertThat(recovered).isNotSameAs(pending);
            assertThat(recovered.identifier()).isEqualTo(2);
            assertThat(recovered.watermark()).isEqualTo(123L);
            assertThat(recovered.properties())
                    .isEqualTo(pending.properties())
                    .isNotSameAs(pending.properties());
            assertThat(recovered.fileCommittables()).hasSize(1);
            CommitMessageImpl recoveredMessage =
                    (CommitMessageImpl) recovered.fileCommittables().get(0);
            assertThat(recoveredMessage.partition()).isEqualTo(message.partition());
            assertThat(recoveredMessage.bucket()).isEqualTo(message.bucket());
            assertThat(recoveredMessage.totalBuckets()).isEqualTo(message.totalBuckets());
            assertThat(recoveredMessage.checkFromSnapshot()).isEqualTo(1L);
            assertThat(recoveredMessage.newFilesIncrement().isEmpty()).isTrue();
            assertThat(recoveredMessage.compactIncrement()).isSameAs(compact);
            assertThat(message.newFilesIncrement()).isSameAs(append.newFilesIncrement());
            verify(callback).retry(same(pending));
            assertThat(
                            overwrite
                                    ? commit.overwritePartition(Collections.emptyMap(), recovered)
                                    : commit.commit(recovered, false))
                    .isEqualTo(1);
            Snapshot snapshot = store.snapshotManager().latestSnapshot();
            assertThat(snapshot.id()).isEqualTo(3L);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
            verify(callback)
                    .call(
                            argThat(
                                    context ->
                                            context.snapshot.commitKind()
                                                    == Snapshot.CommitKind.COMPACT));
            assertThat(store.toKvMap(store.readKvsFromSnapshot(snapshot.id())))
                    .isEqualTo(store.toKvMap(Collections.singletonList(row)));
            assertThat(
                            store.newIndexFileHandler()
                                    .scanEntries(snapshot, HASH_INDEX, append.partition()))
                    .extracting(IndexManifestEntry::indexFile)
                    .containsExactly(newIndex);
            assertThat(commit.filterCommitted(Arrays.asList(pending, next))).containsExactly(next);

            // The recovery marker must not change ordinary empty-commit behavior.
            assertThat(commit.commit(next, false)).isEqualTo(1);
            assertThat(store.snapshotManager().latestSnapshot().commitKind())
                    .isEqualTo(Snapshot.CommitKind.APPEND);
            commit.commitStatistics(new Statistics(4, 0, 1L, 1L), 4);
            assertThat(store.snapshotManager().latestSnapshot().commitKind())
                    .isEqualTo(Snapshot.CommitKind.ANALYZE);
            assertThat(
                            commit.filterCommitted(
                                    Collections.singletonList(
                                            new ManifestCommittable(
                                                    4, null, pending.fileCommittables()))))
                    .isEmpty();
        }
    }

    @Test
    public void testFilterHalfCommittedIdentifierWithConcurrentlyCompactedInput() throws Exception {
        TestFileStore store = createStore();
        KeyValue row = gen.nextInsert("20211110", 8, 1L, null, "replacement");
        List<ManifestCommittable> prepared = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            store.commitDataImpl(
                    Collections.singletonList(row),
                    gen::getPartition,
                    value -> 0,
                    false,
                    2L,
                    123L,
                    Collections.emptyList(),
                    (commit, committable) -> prepared.add(committable));
        }
        CommitMessageImpl append = (CommitMessageImpl) prepared.get(0).fileCommittables().get(0);
        CommitMessageImpl ours = (CommitMessageImpl) prepared.get(1).fileCommittables().get(0);
        CommitMessageImpl theirs = (CommitMessageImpl) prepared.get(2).fileCommittables().get(0);
        ManifestCommittable pending =
                new ManifestCommittable(
                        2,
                        123L,
                        Collections.singletonList(
                                new CommitMessageImpl(
                                        append.partition(),
                                        append.bucket(),
                                        append.totalBuckets(),
                                        append.newFilesIncrement(),
                                        new CompactIncrement(
                                                append.newFilesIncrement().newFiles(),
                                                ours.newFilesIncrement().newFiles(),
                                                Collections.emptyList()))));
        try (FileStoreCommit commit = store.newCommit("writer", null)) {
            // The first phase of identifier 2 publishes the append files.
            commit.commit(prepared.get(0), false);
            assertThat(commit.filterCommitted(Collections.singletonList(pending))).hasSize(1);

            // Another job compacts the same input away before the writer restarts.
            try (FileStoreCommit other = store.newCommit("other", null)) {
                other.commit(
                        new ManifestCommittable(
                                1,
                                null,
                                Collections.singletonList(
                                        new CommitMessageImpl(
                                                append.partition(),
                                                append.bucket(),
                                                append.totalBuckets(),
                                                DataIncrement.emptyIncrement(),
                                                new CompactIncrement(
                                                        append.newFilesIncrement().newFiles(),
                                                        theirs.newFilesIncrement().newFiles(),
                                                        Collections.emptyList())))),
                        false);
            }
            Snapshot compacted = store.snapshotManager().latestSnapshot();
            assertThat(compacted.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

            // The stale compaction cannot be replayed, so recovery must discard it instead of
            // rebuilding a committable that fails conflict detection on every restart.
            ManifestCommittable next = new ManifestCommittable(3);
            assertThat(commit.filterCommitted(Arrays.asList(pending, next))).containsExactly(next);
            assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(compacted.id());
            assertThat(store.toKvMap(store.readKvsFromSnapshot(compacted.id())))
                    .isEqualTo(store.toKvMap(Collections.singletonList(row)));
        }
    }

    @Test
    public void testFilterHalfCommittedIdentifierWithConcurrentlyUpgradedInput() throws Exception {
        TestFileStore store = createStore();
        KeyValue row = gen.nextInsert("20211110", 8, 1L, null, "replacement");
        List<ManifestCommittable> prepared = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            store.commitDataImpl(
                    Collections.singletonList(row),
                    gen::getPartition,
                    value -> 0,
                    false,
                    2L,
                    123L,
                    Collections.emptyList(),
                    (commit, committable) -> prepared.add(committable));
        }
        CommitMessageImpl append = (CommitMessageImpl) prepared.get(0).fileCommittables().get(0);
        CommitMessageImpl ours = (CommitMessageImpl) prepared.get(1).fileCommittables().get(0);
        List<DataFileMeta> inputs = append.newFilesIncrement().newFiles();
        assertThat(inputs).allMatch(file -> file.level() == 0);
        ManifestCommittable pending =
                new ManifestCommittable(
                        2,
                        123L,
                        Collections.singletonList(
                                new CommitMessageImpl(
                                        append.partition(),
                                        append.bucket(),
                                        append.totalBuckets(),
                                        append.newFilesIncrement(),
                                        new CompactIncrement(
                                                inputs,
                                                ours.newFilesIncrement().newFiles(),
                                                Collections.emptyList()))));
        try (FileStoreCommit commit = store.newCommit("writer", null)) {
            // The first phase of identifier 2 publishes the append files.
            commit.commit(prepared.get(0), false);
            assertThat(commit.filterCommitted(Collections.singletonList(pending))).hasSize(1);

            // Another job upgrades the same input in place: same file name, new level.
            List<DataFileMeta> upgraded = new ArrayList<>();
            for (DataFileMeta file : inputs) {
                upgraded.add(file.upgrade(1));
            }
            try (FileStoreCommit other = store.newCommit("other", null)) {
                other.commit(
                        new ManifestCommittable(
                                1,
                                null,
                                Collections.singletonList(
                                        new CommitMessageImpl(
                                                append.partition(),
                                                append.bucket(),
                                                append.totalBuckets(),
                                                DataIncrement.emptyIncrement(),
                                                new CompactIncrement(
                                                        inputs,
                                                        upgraded,
                                                        Collections.emptyList())))),
                        false);
            }
            Snapshot compacted = store.snapshotManager().latestSnapshot();
            assertThat(compacted.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

            // The level 0 deletion no longer matches the live level 1 entry, so replaying it
            // would fail conflict detection. Recovery must discard it.
            ManifestCommittable next = new ManifestCommittable(3);
            assertThat(commit.filterCommitted(Arrays.asList(pending, next))).containsExactly(next);
            assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(compacted.id());
            assertThat(store.toKvMap(store.readKvsFromSnapshot(compacted.id())))
                    .isEqualTo(store.toKvMap(Collections.singletonList(row)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRecoverHalfCommittedIdentifierWithConcurrentOverlap(boolean overwrite)
            throws Exception {
        TestFileStore store =
                createStore(Collections.singletonMap(CoreOptions.OVERWRITE_UPGRADE.key(), "false"));
        KeyValue row = gen.nextInsert("20211110", 8, 1L, null, "replacement");
        List<ManifestCommittable> prepared = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            store.commitDataImpl(
                    Collections.singletonList(row),
                    gen::getPartition,
                    value -> 0,
                    false,
                    2L,
                    123L,
                    Collections.emptyList(),
                    (commit, committable) -> prepared.add(committable));
        }
        CommitMessageImpl append = (CommitMessageImpl) prepared.get(0).fileCommittables().get(0);
        CommitMessageImpl ours = (CommitMessageImpl) prepared.get(1).fileCommittables().get(0);
        CommitMessageImpl theirs = (CommitMessageImpl) prepared.get(2).fileCommittables().get(0);
        List<DataFileMeta> oursUpgraded = new ArrayList<>();
        for (DataFileMeta file : ours.newFilesIncrement().newFiles()) {
            oursUpgraded.add(file.upgrade(1));
        }
        List<DataFileMeta> theirsUpgraded = new ArrayList<>();
        for (DataFileMeta file : theirs.newFilesIncrement().newFiles()) {
            theirsUpgraded.add(file.upgrade(1));
        }
        // Identifier 2 compacts its own level 0 file into a level 1 file.
        ManifestCommittable pending =
                new ManifestCommittable(
                        2,
                        123L,
                        Collections.singletonList(
                                new CommitMessageImpl(
                                        append.partition(),
                                        append.bucket(),
                                        append.totalBuckets(),
                                        append.newFilesIncrement(),
                                        new CompactIncrement(
                                                append.newFilesIncrement().newFiles(),
                                                oursUpgraded,
                                                Collections.emptyList()))));
        try (FileStoreCommit commit = store.newCommit("writer", null)) {
            // Only the first phase of identifier 2 is published.
            if (overwrite) {
                commit.overwritePartition(Collections.emptyMap(), prepared.get(0));
            } else {
                commit.commit(prepared.get(0), false);
            }

            // Another user appends a same-key file and compacts it to level 1, while the level 0
            // input of identifier 2 is still live.
            try (FileStoreCommit other = store.newCommit("other", null)) {
                other.commit(prepared.get(2), false);
                other.commit(
                        new ManifestCommittable(
                                3,
                                null,
                                Collections.singletonList(
                                        new CommitMessageImpl(
                                                theirs.partition(),
                                                theirs.bucket(),
                                                theirs.totalBuckets(),
                                                DataIncrement.emptyIncrement(),
                                                new CompactIncrement(
                                                        theirs.newFilesIncrement().newFiles(),
                                                        theirsUpgraded,
                                                        Collections.emptyList())))),
                        false);
            }
            Snapshot latest = store.snapshotManager().latestSnapshot();
            assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

            // The liveness checks pass, but the recovered level 1 output overlaps theirs. Each
            // restart must drop the recovery instead of failing on the same LSM conflict.
            for (int restart = 0; restart < 2; restart++) {
                List<ManifestCommittable> recovered =
                        commit.filterCommitted(Collections.singletonList(pending));
                assertThat(recovered).hasSize(1);
                int generated =
                        overwrite
                                ? commit.overwritePartition(
                                        Collections.emptyMap(), recovered.get(0))
                                : commit.commit(recovered.get(0), false);
                assertThat(generated).isZero();
                assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(latest.id());
            }
        }
    }

    @Test
    public void testRecoverHalfCommittedIdentifierWithConcurrentDvCompaction() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});
        Options options = new Options();
        options.set(CoreOptions.PATH, tempDir.toString());
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        options.set(CoreOptions.FILE_COMPRESSION, "null");
        Path path = new Path(tempDir.toString());
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(), path, schema, CatalogEnvironment.empty());

        List<CommitMessage> pendingMessages;
        List<CommitMessage> concurrentMessages;
        try (IOManager ioManager = new IOManagerImpl(tempDir.toString())) {
            try (StreamTableWrite seedWrite = table.newWrite("seed").withIOManager(ioManager);
                    TableCommitImpl seedCommit = table.newCommit("seed")) {
                seedWrite.write(GenericRow.of(1, 10L));
                seedWrite.write(GenericRow.of(4, 40L));
                seedWrite.write(GenericRow.of(5, 50L));
                seedCommit.commit(1, seedWrite.prepareCommit(true, 1));
            }
            long baseline = table.snapshotManager().latestSnapshotId();

            try (StreamTableWrite writer = table.newWrite("writer").withIOManager(ioManager);
                    StreamTableWrite otherWriter =
                            table.newWrite("other").withIOManager(ioManager)) {
                writer.write(GenericRow.of(2, 20L));
                pendingMessages = writer.prepareCommit(true, 2);

                otherWriter.write(GenericRow.of(3, 30L));
                concurrentMessages = otherWriter.prepareCommit(true, 1);
            }
            assertThat(pendingMessages)
                    .singleElement()
                    .satisfies(
                            message -> {
                                assertThat(message.checkFromSnapshot()).isEqualTo(baseline);
                                assertThat(
                                                ((CommitMessageImpl) message)
                                                        .compactIncrement()
                                                        .isEmpty())
                                        .isFalse();
                            });
            assertThat(concurrentMessages)
                    .singleElement()
                    .satisfies(
                            message -> {
                                assertThat(message.checkFromSnapshot()).isEqualTo(baseline);
                                assertThat(
                                                ((CommitMessageImpl) message)
                                                        .compactIncrement()
                                                        .isEmpty())
                                        .isFalse();
                            });
        }

        // Publish only the append phase of identifier 2 for "writer".
        List<CommitMessage> appendOnly =
                pendingMessages.stream()
                        .map(message -> (CommitMessageImpl) message)
                        .map(
                                message ->
                                        new CommitMessageImpl(
                                                message.partition(),
                                                message.bucket(),
                                                message.totalBuckets(),
                                                message.newFilesIncrement(),
                                                CompactIncrement.emptyIncrement(),
                                                message.checkFromSnapshot()))
                        .collect(Collectors.toList());
        try (TableCommitImpl commit = table.newCommit("writer")) {
            commit.commit(2, appendOnly);
        }

        // Concurrent commit with a different primary key in the same bucket.
        try (TableCommitImpl otherCommit = table.newCommit("other")) {
            otherCommit.commit(1, concurrentMessages);
        }
        long snapshotAfterConcurrent = table.snapshotManager().latestSnapshotId();
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);

        // Two fresh committers each reconstruct and recover the same half-committed identifier.
        for (int attempt = 0; attempt < 2; attempt++) {
            try (TableCommitImpl freshCommit = table.newCommit("writer")) {
                assertThat(
                                freshCommit.filterAndCommit(
                                        Collections.singletonMap(2L, pendingMessages)))
                        .isEqualTo(1);
            }
            assertThat(table.snapshotManager().latestSnapshotId())
                    .isEqualTo(snapshotAfterConcurrent);
        }

        // Deletion-vector reads skip level-0 files, so compact once more to make the appended
        // row of identifier 2 visible.
        try (IOManager ioManager = new IOManagerImpl(tempDir.toString());
                StreamTableWrite finalWrite = table.newWrite("final").withIOManager(ioManager);
                TableCommitImpl finalCommit = table.newCommit("final")) {
            finalWrite.compact(BinaryRow.EMPTY_ROW, 0, true);
            finalCommit.commit(1, finalWrite.prepareCommit(true, 1));
        }

        List<String> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> reader =
                new RecordReaderIterator<>(table.newRead().createReader(table.newScan().plan()))) {
            while (reader.hasNext()) {
                InternalRow row = reader.next();
                rows.add(row.getInt(0) + ":" + row.getLong(1));
            }
        }
        assertThat(rows).containsExactlyInAnyOrder("1:10", "2:20", "3:30", "4:40", "5:50");

        // Normal (non-recovery) commits with a stale baseline must still fail with the conflict.
        List<CommitMessage> compactOnly =
                pendingMessages.stream()
                        .map(message -> (CommitMessageImpl) message)
                        .map(
                                message ->
                                        new CommitMessageImpl(
                                                message.partition(),
                                                message.bucket(),
                                                message.totalBuckets(),
                                                DataIncrement.emptyIncrement(),
                                                message.compactIncrement(),
                                                message.checkFromSnapshot()))
                        .collect(Collectors.toList());
        try (TableCommitImpl nonRecoveryCommit = table.newCommit("writer")) {
            assertThatThrownBy(() -> nonRecoveryCommit.commit(3, compactOnly))
                    .hasMessageContaining("Concurrent compaction conflict");
        }
    }

    @Test
    public void testFilterHalfCommittedIdentifierWithConcurrentlyDroppedIndex() throws Exception {
        TestFileStore store = createStore();
        BinaryRow partition = gen.getPartition(gen.nextInsert("20211110", 8, 1L, null, "a"));
        IndexFileMeta index =
                new IndexFileMeta(
                        "btree",
                        "recovery-index",
                        1,
                        1,
                        new GlobalIndexMeta(0, 0, 0, null, null),
                        null);
        // An index-only compact increment, like the one written by a global index dropper.
        ManifestCommittable pending =
                new ManifestCommittable(
                        2,
                        null,
                        Collections.singletonList(
                                new CommitMessageImpl(
                                        partition,
                                        0,
                                        null,
                                        DataIncrement.emptyIncrement(),
                                        new CompactIncrement(
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Collections.singletonList(index)))));
        try (FileStoreCommit other = store.newCommit("other", null)) {
            other.commit(indexCommittable(partition, "recovery-index", 0, 0), false);
        }
        try (FileStoreCommit commit = store.newCommit("writer", null)) {
            // The first phase of identifier 2 is published as an empty APPEND snapshot.
            commit.ignoreEmptyCommit(false);
            commit.commit(new ManifestCommittable(2), false);
            assertThat(commit.filterCommitted(Collections.singletonList(pending))).hasSize(1);

            // Another job drops the global index before the writer restarts.
            try (FileStoreCommit other = store.newCommit("other", null)) {
                other.commit(deleteIndexCommittable(partition, "recovery-index", 0, 0), false);
            }
            Snapshot dropped = store.snapshotManager().latestSnapshot();

            // Replaying the deletion would fail in the index manifest combiner on every restart,
            // so recovery must discard it.
            ManifestCommittable next = new ManifestCommittable(3);
            assertThat(commit.filterCommitted(Arrays.asList(pending, next))).containsExactly(next);
            assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(dropped.id());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"data", "auxiliary", "changelog", "index", "io"})
    public void testFilterHalfCommittedIdentifierWithUnavailableOutput(String unavailable)
            throws Exception {
        TestFileStore store = createStore();
        BinaryRow partition = gen.getPartition(gen.nextInsert("20211110", 8, 1L, null, "a"));
        DataFilePathFactory dataFactory =
                store.pathFactory().createDataFilePathFactory(partition, 0);
        Path external = new Path(tempDir.toString(), "external.avro");
        DataFileMeta data =
                DataFileMeta.forAppend(
                        external.getName(),
                        0,
                        0,
                        EMPTY_STATS,
                        0,
                        0,
                        0,
                        Collections.singletonList("external.avro.index"),
                        null,
                        null,
                        null,
                        external.toString(),
                        null,
                        null);
        DataFileMeta changelog =
                DataFileMeta.forAppend(
                        "changelog.avro",
                        0,
                        0,
                        EMPTY_STATS,
                        0,
                        0,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        IndexFileMeta index =
                new IndexFileMeta(HASH_INDEX, "compact-index", 0, 0, null, null, null);
        Map<String, Path> paths = new HashMap<>();
        paths.put("data", data.collectFiles(dataFactory).get(0));
        paths.put("auxiliary", data.collectFiles(dataFactory).get(1));
        paths.put("changelog", changelog.collectFiles(dataFactory).get(0));
        paths.put("index", store.pathFactory().indexFileFactory(partition, 0).toPath(index));
        for (Path output : paths.values()) {
            store.fileIO().newOutputStream(output, false).close();
        }
        ManifestCommittable pending = new ManifestCommittable(2);
        // The first bucket can be replayed, but failure in the second must skip both buckets.
        pending.addFileCommittable(
                new CommitMessageImpl(
                        partition,
                        1,
                        2,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(index))));
        pending.addFileCommittable(
                new CommitMessageImpl(
                        partition,
                        0,
                        2,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(data),
                                Collections.singletonList(changelog),
                                Collections.singletonList(index),
                                Collections.emptyList())));
        FileIO fileIO = spy(store.fileIO());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, store.options().path());
        try (FileStoreCommit commit = table.store().newCommit("writer", table)) {
            // The first phase of identifier 2 publishes the index the first bucket deletes.
            commit.commit(
                    new ManifestCommittable(
                            2,
                            null,
                            Collections.singletonList(
                                    new CommitMessageImpl(
                                            partition,
                                            1,
                                            2,
                                            DataIncrement.indexIncrement(
                                                    Collections.singletonList(index)),
                                            CompactIncrement.emptyIncrement()))),
                    false);
            assertThat(commit.filterCommitted(Collections.singletonList(pending))).hasSize(1);
            if (unavailable.equals("io")) {
                doThrow(new IOException("Output lookup failed"))
                        .when(fileIO)
                        .exists(paths.get("data"));
                assertThatThrownBy(() -> commit.filterCommitted(Collections.singletonList(pending)))
                        .isInstanceOf(UncheckedIOException.class)
                        .hasCauseInstanceOf(IOException.class)
                        .hasMessageContaining("Output lookup failed");
            } else {
                fileIO.delete(paths.get(unavailable), false);
                ManifestCommittable next =
                        new ManifestCommittable(3, null, pending.fileCommittables());
                assertThat(commit.filterCommitted(Arrays.asList(pending, next)))
                        .containsExactly(next);
                assertThat(commit.filterCommitted(Collections.singletonList(pending))).isEmpty();
            }
            assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        }
    }

    private FileStoreCommitImpl newCommitWithSnapshotCommit(
            TestFileStore store,
            String commitUser,
            SnapshotCommit snapshotCommit,
            List<CommitCallback> callbacks) {
        CoreOptions options = store.options();
        String tableName = options.path().getName();
        return new FileStoreCommitImpl(
                snapshotCommit,
                store.fileIO(),
                new FileSystemSchemaManager(store.fileIO(), options.path()),
                tableName,
                commitUser,
                store.partitionType(),
                options,
                store.pathFactory(),
                store.snapshotManager(),
                store.manifestFileFactory(),
                store.manifestListFactory(),
                store.indexManifestFileFactory(),
                store::newScan,
                store.newStatsFileHandler(),
                store.bucketMode(),
                Collections.emptyList(),
                callbacks,
                scanner ->
                        ConflictDetection.create(
                                tableName,
                                commitUser,
                                store.partitionType(),
                                store.pathFactory(),
                                store.newKeyComparator(),
                                store.bucketMode(),
                                options.deletionVectorsEnabled(),
                                false,
                                options.dataEvolutionNestedFieldEnabled(),
                                options.pkClusteringOverride(),
                                store.newIndexFileHandler(),
                                store.snapshotManager(),
                                scanner),
                null);
    }

    private ManifestCommittable indexCommittable(
            BinaryRow partition, String fileName, long rowRangeStart, long rowRangeEnd) {
        ManifestCommittable committable = new ManifestCommittable(0);
        committable.addFileCommittable(
                new CommitMessageImpl(
                        partition,
                        0,
                        null,
                        DataIncrement.indexIncrement(
                                Collections.singletonList(
                                        new IndexFileMeta(
                                                "btree",
                                                fileName,
                                                1,
                                                1,
                                                new GlobalIndexMeta(
                                                        rowRangeStart, rowRangeEnd, 0, null, null),
                                                null))),
                        CompactIncrement.emptyIncrement()));
        return committable;
    }

    private ManifestCommittable deleteIndexCommittable(
            BinaryRow partition, String fileName, long rowRangeStart, long rowRangeEnd) {
        ManifestCommittable committable = new ManifestCommittable(0);
        committable.addFileCommittable(
                new CommitMessageImpl(
                        partition,
                        0,
                        null,
                        DataIncrement.deleteIndexIncrement(
                                Collections.singletonList(
                                        new IndexFileMeta(
                                                "btree",
                                                fileName,
                                                1,
                                                1,
                                                new GlobalIndexMeta(
                                                        rowRangeStart, rowRangeEnd, 0, null, null),
                                                null))),
                        CompactIncrement.emptyIncrement()));
        return committable;
    }

    private TestFileStore createStore() throws Exception {
        return createStore(Collections.emptyMap());
    }

    private TestFileStore createStore(Map<String, String> options) throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        List<String> primaryKeys =
                TestKeyValueGenerator.getPrimaryKeys(
                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(new LocalFileIO(), path),
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                primaryKeys,
                                options,
                                null));
        return new TestFileStore.Builder(
                        "avro",
                        root,
                        1,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        tableSchema)
                .changelogProducer(CoreOptions.ChangelogProducer.NONE)
                .build();
    }
}
