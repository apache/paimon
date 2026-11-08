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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Bucket isolation for automatic compaction of primary-key tables with deletion vectors. */
class PrimaryKeyDvConcurrentCompactionTest {

    @TempDir java.nio.file.Path tempDir;

    private final List<AutoCloseable> resources = new ArrayList<>();
    private PrimaryKeyFileStoreTable table;
    private KeyValueFileStore store;
    private IOManager ioManager;

    @BeforeEach
    void before() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "2");
        options.put("file.format", "avro");
        options.put("deletion-vectors.enabled", "true");
        options.put("num-levels", "4");
        options.put("num-sorted-run.compaction-trigger", "100");
        options.put("target-file-size", "10 mb");
        options.put("write-buffer-size", "1 mb");
        options.put("page-size", "4 kb");
        options.put("commit.min-retry-wait", "0 ms");
        Path path = new Path(tempDir.toUri());
        RowType type =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"p", "id", "v"});
        new FileSystemSchemaManager(LocalFileIO.create(), path)
                .createTable(
                        new Schema(
                                type.getFields(),
                                Collections.singletonList("p"),
                                Arrays.asList("p", "id"),
                                options,
                                null));
        table = (PrimaryKeyFileStoreTable) FileStoreTableFactory.create(LocalFileIO.create(), path);
        store = table.store();
        assertThat(store.options().writeOnly()).isFalse();
        ioManager = new IOManagerImpl(tempDir.resolve("spill").toString());
        resources.add(ioManager);
    }

    @AfterEach
    void after() throws Exception {
        IOUtils.closeAll(resources);
    }

    @Test
    void testSameBucketCompactionConflicts() throws Exception {
        seed(1, 0);
        CommitMessageImpl first = prepare(newWrite("first"), 1, 0, 1);
        CommitMessageImpl second = prepare(newWrite("second"), 1, 0, 2);

        commit("first", first);
        assertConflict(newCommit("second"), second);
        assertThat(values(1, 1)).containsExactly("new");
        assertThat(values(1, 2)).containsExactly("old");
    }

    @Test
    void testSamePartitionDifferentBucketsCompactIndependently() throws Exception {
        seed(1, 0);
        seed(1, 1);
        AbstractFileStoreWrite<KeyValue> firstWrite = newWrite("first");
        AbstractFileStoreWrite<KeyValue> secondWrite = newWrite("second");
        CommitMessageImpl first = prepare(firstWrite, 1, 0, 1);
        CommitMessageImpl second = prepare(secondWrite, 1, 1, 1001);
        assertThat(first.checkFromSnapshot()).isEqualTo(second.checkFromSnapshot());

        FileStoreCommitImpl firstCommit = newCommit("first");
        FileStoreCommitImpl secondCommit = newCommit("second");
        commit(firstCommit, 0, first);
        BinaryRow partition = BinaryRow.singleColumn(1);
        List<IndexFileMeta> firstIndexes =
                store.newIndexFileHandler()
                        .scan(
                                store.snapshotManager().latestSnapshot(),
                                DELETION_VECTORS_INDEX,
                                partition,
                                0);
        assertThat(firstIndexes).isNotEmpty();

        commit(secondCommit, 0, second);
        Snapshot latest = store.snapshotManager().latestSnapshot();
        assertThat(store.newIndexFileHandler().scan(latest, DELETION_VECTORS_INDEX, partition, 0))
                .containsExactlyElementsOf(firstIndexes);
        assertThat(store.newIndexFileHandler().scan(latest, DELETION_VECTORS_INDEX, partition, 1))
                .isNotEmpty()
                .containsExactlyElementsOf(second.compactIncrement().newIndexFiles());
        assertThat(values(1, 1)).containsExactly("new");
        assertThat(values(1, 1001)).containsExactly("new");

        commit(firstCommit, 1, prepare(firstWrite, 1, 0, 2));
        assertThat(values(1, 1)).containsExactly("new");
        assertThat(values(1, 2)).containsExactly("new");
        assertThat(values(1, 1001)).containsExactly("new");
    }

    @Test
    void testStrictModeStillChecksOtherBucketsInThePartition() throws Exception {
        seed(1, 0);
        seed(1, 1);
        CommitMessageImpl pending = prepare(newWrite("second"), 1, 0, 2);
        commit("first", prepare(newWrite("first"), 1, 1, 1001));
        KeyValueFileStore strictStore =
                ((PrimaryKeyFileStoreTable)
                                table.copy(
                                        Collections.singletonMap(
                                                COMMIT_LAST_SAFE_SNAPSHOT.key(),
                                                String.valueOf(pending.checkFromSnapshot()))))
                        .store();

        try (FileStoreCommitImpl second = strictStore.newCommit("second", null)) {
            assertThatThrownBy(() -> commit(second, 0, pending))
                    .hasMessageContaining("commit.strict-mode.enabled");
        }
        assertThat(values(1, 2)).containsExactly("old");
        assertThat(values(1, 1001)).containsExactly("new");
    }

    @Test
    void testDifferentPartitionsCompactIndependently() throws Exception {
        seed(1, 0);
        seed(2, 0);
        AbstractFileStoreWrite<KeyValue> secondWrite = newWrite("second");
        CommitMessageImpl first = prepare(newWrite("first"), 1, 0, 1);
        CommitMessageImpl second = prepare(secondWrite, 2, 0, 1);

        commit("first", first);
        FileStoreCommitImpl secondCommit = newCommit("second");
        commit(secondCommit, 0, second);
        assertThat(values(1, 1)).containsExactly("new");
        assertThat(values(2, 1)).containsExactly("new");

        // A retained writer can keep compacting its own partition after its previous commit.
        CommitMessageImpl next = prepare(secondWrite, 2, 0, 2);
        commit(secondCommit, 1, next);
        assertThat(values(2, 2)).containsExactly("new");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testCompactionWithoutDvChangesUsesBucketIsolation(int otherBucket) throws Exception {
        seed(1, 0);
        if (otherBucket != 0) {
            seed(1, otherBucket);
        }
        CommitMessageImpl second = prepare(newWrite("second"), 1, 0, 2);
        KeyValueFileStore fullCompactStore =
                ((PrimaryKeyFileStoreTable)
                                table.copy(
                                        Collections.singletonMap(
                                                "compaction.total-size-threshold", "100 mb")))
                        .store();
        int firstKey = 2001 + 1000 * otherBucket;
        CommitMessageImpl first =
                prepare(newWrite("first", fullCompactStore), 1, otherBucket, firstKey);
        assertThat(first.compactIncrement().newIndexFiles()).isEmpty();
        assertThat(first.compactIncrement().compactBefore()).anyMatch(file -> file.level() == 3);

        commit("first", first);
        if (otherBucket == 0) {
            assertConflict(newCommit("second"), second);
            assertThat(values(1, 2)).containsExactly("old");
        } else {
            commit("second", second);
            assertThat(values(1, 2)).containsExactly("new");
        }
        assertThat(values(1, firstKey)).containsExactly("new");
    }

    @Test
    void testPartitionsUseTheirOwnRestoredSnapshots() throws Exception {
        seed(1, 0);
        seed(2, 0);
        AbstractFileStoreWrite<KeyValue> second = newWrite("second");
        second.write(BinaryRow.singleColumn(1), 0, kv(1, 2, "new"));
        commit("first", prepare(newWrite("first"), 2, 0, 1));

        second.write(BinaryRow.singleColumn(2), 0, kv(2, 2, "new"));
        List<CommitMessage> messages = second.prepareCommit(true, 0);
        assertThat(messages).hasSize(2);
        assertThat(messages.stream().map(CommitMessage::checkFromSnapshot).distinct()).hasSize(2);
        commit(newCommit("second"), 0, messages);
        assertThat(values(1, 2)).containsExactly("new");
        assertThat(values(2, 1)).containsExactly("new");
        assertThat(values(2, 2)).containsExactly("new");
    }

    @Test
    void testBucketsUseTheirOwnRestoredSnapshots() throws Exception {
        seed(1, 0);
        seed(1, 1);
        AbstractFileStoreWrite<KeyValue> second = newWrite("second");
        second.write(BinaryRow.singleColumn(1), 0, kv(1, 2, "new"));
        commit("first", prepare(newWrite("first"), 1, 1, 1001));

        second.write(BinaryRow.singleColumn(1), 1, kv(1, 1002, "new"));
        List<CommitMessage> messages = second.prepareCommit(true, 0);
        assertThat(messages).hasSize(2);
        assertThat(messages.stream().map(CommitMessage::checkFromSnapshot).distinct()).hasSize(2);
        commit(newCommit("second"), 0, messages);
        assertThat(values(1, 2)).containsExactly("new");
        assertThat(values(1, 1001)).containsExactly("new");
        assertThat(values(1, 1002)).containsExactly("new");
    }

    @Test
    void testCompactionWithoutRestoredSnapshotIsRejected() throws Exception {
        seed(1, 0);
        CommitMessageImpl message = prepare(newWrite("second"), 1, 0, 2);
        CommitMessageImpl unchecked =
                new CommitMessageImpl(
                        message.partition(),
                        message.bucket(),
                        message.totalBuckets(),
                        message.newFilesIncrement(),
                        message.compactIncrement());
        long before = store.snapshotManager().latestSnapshotId();
        assertThatThrownBy(() -> commit("second", unchecked))
                .hasMessageContaining("missing its restored snapshot");
        assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(before);
    }

    @Test
    void testFileIndexRewriteDoesNotRequireCompactionBaseline() throws Exception {
        seed(1, 0);
        BinaryRow partition = BinaryRow.singleColumn(1);
        DataFileMeta file =
                store.newScan().withPartitionBucket(partition, 0).plan().files().get(0).file();
        DataFileMeta indexedFile = file.copy(new byte[] {1});
        commit(
                "rewrite-index",
                new CommitMessageImpl(
                        partition,
                        0,
                        2,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(file),
                                Collections.singletonList(indexedFile),
                                Collections.emptyList())));

        assertThat(store.newScan().withPartitionBucket(partition, 0).plan().files())
                .extracting(ManifestEntry::file)
                .containsExactly(indexedFile);
    }

    @Test
    void testSameFileLevelUpgradeRequiresBaseline() throws Exception {
        seed(1, 0, 2);
        BinaryRow partition = BinaryRow.singleColumn(1);
        DataFileMeta file =
                store.newScan().withPartitionBucket(partition, 0).plan().files().get(0).file();
        CommitMessageImpl message =
                new CommitMessageImpl(
                        partition,
                        0,
                        2,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(file),
                                Collections.singletonList(file.upgrade(3)),
                                Collections.emptyList()));

        assertThatThrownBy(() -> commit("second", message))
                .hasMessageContaining("missing its restored snapshot");
        assertThat(store.newScan().withPartitionBucket(partition, 0).plan().files())
                .extracting(ManifestEntry::file)
                .containsExactly(file);
    }

    @Test
    void testChangelogOnlyCompactionDoesNotRequireBaseline() throws Exception {
        seed(1, 0);
        KeyValueFileStore lookupStore =
                ((PrimaryKeyFileStoreTable)
                                table.copy(
                                        Collections.singletonMap("changelog-producer", "lookup")))
                        .store();
        CommitMessageImpl prepared = prepare(newWrite("second", lookupStore), 1, 0, 1);
        List<DataFileMeta> changelogFiles = prepared.compactIncrement().changelogFiles();
        assertThat(changelogFiles).isNotEmpty();

        commit(
                "second",
                new CommitMessageImpl(
                        prepared.partition(),
                        prepared.bucket(),
                        prepared.totalBuckets(),
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(), Collections.emptyList(), changelogFiles)));

        Snapshot snapshot = store.snapshotManager().latestSnapshot();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(snapshot.changelogManifestList()).isNotNull();
        assertThat(values(1, 1)).containsExactly("old");
    }

    @Test
    void testDeletionVectorOnlyCompactionRequiresBaseline() throws Exception {
        seed(1, 0);
        BinaryRow partition = BinaryRow.singleColumn(1);
        DataFileMeta file =
                store.newScan().withPartitionBucket(partition, 0).plan().files().get(0).file();
        BitmapDeletionVector dv = new BitmapDeletionVector();
        dv.delete(0);
        IndexFileMeta index =
                store.newIndexFileHandler()
                        .dvIndex(partition, 0)
                        .writeSingleFile(Collections.singletonMap(file.fileName(), dv));
        CommitMessageImpl message =
                new CommitMessageImpl(
                        partition,
                        0,
                        2,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(index),
                                Collections.emptyList()));

        assertThatThrownBy(() -> commit("second", message))
                .hasMessageContaining("missing its restored snapshot");
        assertThat(values(1, 1)).containsExactly("old");
    }

    @Test
    void testPartitionOverwriteInvalidatesCompaction() throws Exception {
        seed(1, 0);
        CommitMessageImpl pending = prepare(newWrite("second"), 1, 0, 2);
        newCommit("first")
                .dropPartitions(Collections.singletonList(Collections.singletonMap("p", "1")), 0);
        assertThat(store.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);

        assertConflict(newCommit("second"), pending);
        assertThat(values(1, 2)).isEmpty();
    }

    @Test
    void testEmptyRestoreCannotSkipPublishedCompaction() throws Exception {
        seed(1, 0);
        int key = 2001;
        commit("first", prepare(newWrite("first"), 1, 0, key));

        AbstractFileStoreWrite<KeyValue> write = newWrite("second");
        write.withWriteRestore(
                new WriteRestore() {
                    @Override
                    public long latestCommittedIdentifier(String user) {
                        return Long.MIN_VALUE;
                    }

                    @Override
                    public RestoreFiles restoreFiles(
                            BinaryRow partition,
                            int bucket,
                            boolean scanDynamicBucketIndex,
                            boolean scanDeleteVectorsIndex,
                            boolean scanSourceIndexPayloads) {
                        return RestoreFiles.empty();
                    }
                });
        CommitMessageImpl pending = prepare(write, 1, 0, key);
        assertThat(pending.checkFromSnapshot()).isZero();
        assertConflict(newCommit("second"), pending);
        assertThat(values(1, key)).containsExactly("new");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOverwriteCompactionUsesWriterCreationSnapshot(boolean concurrent) throws Exception {
        seed(1, 0);
        commit("first", prepare(newWrite("first"), 1, 0, 1));
        long baseline = store.snapshotManager().latestSnapshotId();
        BatchWriteBuilder overwrite =
                table.newBatchWriteBuilder().withOverwrite(Collections.singletonMap("p", "1"));
        try (BatchTableWrite write = overwrite.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = overwrite.newCommit()) {
            write.write(GenericRow.of(1, 2, BinaryString.fromString("replacement")));
            if (concurrent) {
                commit("concurrent", prepare(newWrite("concurrent"), 1, 0, 3));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assertThat(messages)
                    .filteredOn(
                            message -> !((CommitMessageImpl) message).compactIncrement().isEmpty())
                    .extracting(CommitMessage::checkFromSnapshot)
                    .containsExactly(baseline);
            if (concurrent) {
                assertThatThrownBy(() -> commit.commit(messages))
                        .hasMessageContaining("Concurrent compaction conflict");
                return;
            }
            commit.commit(messages);
        }
        assertThat(values(1, 1)).isEmpty();
        assertThat(values(1, 2)).containsExactly("replacement");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testDeletionVectorRemovalUsesBucketIsolation(int changedBucket) throws Exception {
        seed(1, 0);
        if (changedBucket != 0) {
            seed(1, changedBucket);
        }
        Snapshot beforeDelete = store.snapshotManager().latestSnapshot();
        BinaryRow partition = BinaryRow.singleColumn(1);
        DataFileMeta file =
                store.newScan()
                        .withPartitionBucket(partition, changedBucket)
                        .plan()
                        .files()
                        .get(0)
                        .file();
        BitmapDeletionVector dv = new BitmapDeletionVector();
        dv.delete(0);
        IndexFileMeta index =
                store.newIndexFileHandler()
                        .dvIndex(partition, changedBucket)
                        .writeSingleFile(Collections.singletonMap(file.fileName(), dv));
        commit(
                "delete",
                new CommitMessageImpl(
                        partition,
                        changedBucket,
                        2,
                        DataIncrement.indexIncrement(Collections.singletonList(index)),
                        CompactIncrement.emptyIncrement()));
        int deletedKey = 1000 * changedBucket + 1;
        assertThat(values(1, deletedKey)).isEmpty();
        CommitMessageImpl pending = prepare(newWrite("second"), 1, 0, 1);
        assertThat(newCommit("rollback").rollbackToAsLatest(beforeDelete)).isTrue();
        assertThat(values(1, deletedKey)).containsExactly("old");

        if (changedBucket == 0) {
            assertConflict(newCommit("second"), pending);
            assertThat(values(1, 1)).containsExactly("old");
        } else {
            commit("second", pending);
            assertThat(values(1, 1)).containsExactly("new");
            assertThat(values(1, deletedKey)).containsExactly("old");
        }
    }

    @ParameterizedTest
    @CsvSource({"1, 0", "2, 0", "2, 1"})
    void testIndexOnlyOverwriteUsesBucketIsolation(int changedPartition, int changedBucket)
            throws Exception {
        seed(1, 0);
        seed(2, 0);
        seed(2, 1);
        CommitMessageImpl pending = prepare(newWrite("second"), 2, 0, 2);
        BinaryRow partition = BinaryRow.singleColumn(changedPartition);
        DataFileMeta file =
                store.newScan()
                        .withPartitionBucket(partition, changedBucket)
                        .plan()
                        .files()
                        .get(0)
                        .file();
        BitmapDeletionVector dv = new BitmapDeletionVector();
        dv.delete(0);
        IndexFileMeta index =
                store.newIndexFileHandler()
                        .dvIndex(partition, changedBucket)
                        .writeSingleFile(Collections.singletonMap(file.fileName(), dv));
        commit(
                "first",
                new CommitMessageImpl(
                        partition,
                        changedBucket,
                        2,
                        DataIncrement.indexIncrement(Collections.singletonList(index)),
                        CompactIncrement.emptyIncrement()));
        assertThat(store.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);

        if (changedPartition == 2 && changedBucket == 0) {
            assertConflict(newCommit("second"), pending);
            assertThat(values(2, 2)).containsExactly("old");
        } else {
            commit("second", pending);
            assertThat(values(2, 2)).containsExactly("new");
        }
        assertThat(values(changedPartition, 1000 * changedBucket + 1)).isEmpty();
    }

    @Test
    void testNewerWriterCheckDoesNotHideOlderBaselineConflict() throws Exception {
        seed(1, 0);
        CommitMessageImpl old = prepare(newWrite("second"), 1, 0, 2);
        commit("first", prepare(newWrite("first"), 1, 0, 1));

        FileStoreCommitImpl second = newCommit("second");
        commit(second, 0, prepare(newWrite("second"), 1, 0, 3));
        // A newer writer's verified interval does not cover the older writer's baseline.
        assertConflict(second, old);
        assertThat(values(1, 1)).containsExactly("new");
        assertThat(values(1, 2)).containsExactly("old");
        assertThat(values(1, 3)).containsExactly("new");
    }

    @Test
    void testOtherBucketCommitDoesNotAdvancePendingBaseline() throws Exception {
        seed(1, 0);
        seed(1, 1);
        AbstractFileStoreWrite<KeyValue> secondWrite = newWrite("second");
        CommitMessageImpl pending = prepare(secondWrite, 1, 0, 2);
        CommitMessageImpl otherBucket = prepare(secondWrite, 1, 1, 1001);
        assertThat(pending.checkFromSnapshot()).isEqualTo(otherBucket.checkFromSnapshot());
        commit("first", prepare(newWrite("first"), 1, 0, 1));

        FileStoreCommitImpl secondCommit = newCommit("second");
        commit(secondCommit, 0, otherBucket);
        assertThat(values(1, 1001)).containsExactly("new");
        assertConflict(secondCommit, pending);
        assertThat(values(1, 1)).containsExactly("new");
        assertThat(values(1, 2)).containsExactly("old");
    }

    @Test
    void testAppendDoesNotAdvanceCompactionBaseline() throws Exception {
        seed(1, 0);
        CommitMessageImpl first = prepare(newWrite("first"), 1, 0, 1);
        CommitMessageImpl second = prepare(newWrite("second"), 1, 0, 2);
        commit("first", first);

        FileStoreCommitImpl secondCommit = newCommit("second");
        CommitMessageImpl append =
                new CommitMessageImpl(
                        second.partition(),
                        second.bucket(),
                        second.totalBuckets(),
                        second.newFilesIncrement(),
                        CompactIncrement.emptyIncrement(),
                        second.checkFromSnapshot());
        commit(secondCommit, 0, append);
        assertThat(store.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.APPEND);

        assertConflict(secondCommit, compactOnly(second));
        assertThat(values(1, 1)).containsExactly("new");
        assertThat(values(1, 2)).containsExactly("old");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOtherPartitionCommitDoesNotAdvancePendingBaseline(boolean strictMode)
            throws Exception {
        seed(1, 0);
        seed(2, 0);
        AbstractFileStoreWrite<KeyValue> secondWrite = newWrite("second");
        CommitMessageImpl pending = prepare(secondWrite, 1, 0, 2);
        commit("first", prepare(newWrite("first"), 1, 0, 1));

        Map<String, String> commitOptions =
                strictMode
                        ? Collections.singletonMap(
                                COMMIT_LAST_SAFE_SNAPSHOT.key(),
                                String.valueOf(pending.checkFromSnapshot()))
                        : Collections.emptyMap();
        KeyValueFileStore secondStore =
                ((PrimaryKeyFileStoreTable) table.copy(commitOptions)).store();
        try (FileStoreCommitImpl secondCommit = secondStore.newCommit("second", null)) {
            CommitMessageImpl otherPartition = prepare(secondWrite, 2, 0, 1);
            commit(secondCommit, 0, otherPartition);
            assertThat(values(2, 1)).containsExactly("new");
            assertConflict(secondCommit, pending);
        }
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "true, 1"})
    void testSnapshotRetryPreservesCompactionBaseline(boolean compactDuringRetry, int otherBucket)
            throws Exception {
        seed(1, 0);
        if (otherBucket != 0) {
            seed(1, otherBucket);
        }
        int firstKey = 1000 * otherBucket + 1;
        CommitMessageImpl first = prepare(newWrite("first"), 1, otherBucket, firstKey);
        CommitMessageImpl second = prepare(newWrite("second"), 1, 0, 2);
        SnapshotCommit delegate = new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty());
        AtomicInteger attempts = new AtomicInteger();
        SnapshotCommit publisher =
                new SnapshotCommit(Lock.empty()) {
                    @Override
                    protected boolean commitImpl(
                            @Nullable String baseSnapshotUuid,
                            Snapshot snapshot,
                            String branch,
                            List<PartitionStatistics> statistics)
                            throws Exception {
                        if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT
                                && attempts.getAndIncrement() == 0) {
                            CommitMessageImpl concurrent =
                                    compactDuringRetry
                                            ? first
                                            : new CommitMessageImpl(
                                                    first.partition(),
                                                    first.bucket(),
                                                    first.totalBuckets(),
                                                    first.newFilesIncrement(),
                                                    CompactIncrement.emptyIncrement());
                            PrimaryKeyDvConcurrentCompactionTest.this.commit("first", concurrent);
                            return false;
                        }
                        return delegate.commit(baseSnapshotUuid, snapshot, branch, statistics);
                    }

                    @Override
                    protected void closeImpl() throws Exception {
                        delegate.close();
                    }
                };
        FileStoreCommitImpl secondCommit = newCommit("second", publisher);
        if (compactDuringRetry && otherBucket == 0) {
            assertConflict(secondCommit, second);
            assertThat(attempts).hasValue(1);
            // Reusing the committer must not forget the conflict encountered on retry.
            assertConflict(secondCommit, compactOnly(second));
            assertThat(values(1, 2)).containsExactly("old");
        } else {
            commit(secondCommit, 0, second);
            assertThat(attempts).hasValue(2);
            assertThat(values(1, 2)).containsExactly("new");
        }
        assertThat(values(1, firstKey)).containsExactly(compactDuringRetry ? "new" : "old");
    }

    @Test
    void testCheckpointAndSerializationPreserveWriterBaseline() throws Exception {
        seed(1, 0);
        AbstractFileStoreWrite<KeyValue> secondWrite = newWrite("second");
        CommitMessageImpl initial = prepare(secondWrite, 1, 0, 2);
        long baseline = store.snapshotManager().latestSnapshotId();
        commit("second", initial);
        List<FileStoreWrite.State<KeyValue>> state = secondWrite.checkpoint();
        commit("first", prepare(newWrite("first"), 1, 0, 1));

        AbstractFileStoreWrite<KeyValue> restored = newWrite("second");
        restored.restore(state);
        CommitMessageImpl message = prepare(restored, 1, 0, 3);
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        CommitMessageImpl roundTrip =
                (CommitMessageImpl)
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(message));
        assertThat(roundTrip.checkFromSnapshot()).isEqualTo(baseline);
        assertConflict(newCommit("second"), roundTrip);
    }

    @Test
    void testWriteOnlyWriterOmitsCompactionBaseline() throws Exception {
        seed(1, 0);
        KeyValueFileStore writeOnlyStore = writeOnlyStore();
        String user = "write-only";
        AbstractFileStoreWrite<KeyValue> write = newWrite(user, writeOnlyStore);
        write.write(BinaryRow.singleColumn(1), 0, kv(1, 1, "new"));

        List<CommitMessage> messages = write.prepareCommit(true, 0);
        assertThat(messages).singleElement().extracting(CommitMessage::checkFromSnapshot).isNull();
        try (FileStoreCommitImpl commit = writeOnlyStore.newCommit(user, null)) {
            commit(commit, 0, messages);
        }
        Snapshot snapshot = store.snapshotManager().latestSnapshot();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(snapshot.deltaRecordCount()).isEqualTo(1L);
    }

    private void seed(int partition, int bucket) throws Exception {
        seed(partition, bucket, 3);
    }

    private void seed(int partition, int bucket, int level) throws Exception {
        String user = "seed-" + partition + "-" + bucket;
        AbstractFileStoreWrite<KeyValue> write = writeOnlyStore().newWrite(user);
        write.withIOManager(ioManager);
        resources.add(0, write::close);
        for (int i = 1; i <= 1000; i++) {
            write.write(
                    BinaryRow.singleColumn(partition),
                    bucket,
                    kv(partition, 1000 * bucket + i, "old"));
        }
        CommitMessageImpl raw = (CommitMessageImpl) write.prepareCommit(true, 0).get(0);
        List<DataFileMeta> files =
                raw.newFilesIncrement().newFiles().stream()
                        .map(file -> file.upgrade(level))
                        .collect(Collectors.toList());
        assertThat(files).hasSize(1);
        commit(
                user,
                new CommitMessageImpl(
                        BinaryRow.singleColumn(partition),
                        bucket,
                        2,
                        new DataIncrement(files, Collections.emptyList(), Collections.emptyList()),
                        CompactIncrement.emptyIncrement()));
    }

    private KeyValueFileStore writeOnlyStore() {
        return ((PrimaryKeyFileStoreTable)
                        table.copy(Collections.singletonMap("write-only", "true")))
                .store();
    }

    private AbstractFileStoreWrite<KeyValue> newWrite(String user) {
        return newWrite(user, store);
    }

    private AbstractFileStoreWrite<KeyValue> newWrite(String user, KeyValueFileStore targetStore) {
        AbstractFileStoreWrite<KeyValue> write = targetStore.newWrite(user);
        write.withIOManager(ioManager);
        resources.add(0, write::close);
        return write;
    }

    private CommitMessageImpl prepare(
            AbstractFileStoreWrite<KeyValue> write, int partition, int bucket, int key)
            throws Exception {
        write.write(BinaryRow.singleColumn(partition), bucket, kv(partition, key, "new"));
        List<CommitMessage> messages =
                write.prepareCommit(true, 0).stream()
                        .filter(message -> !((CommitMessageImpl) message).isEmpty())
                        .collect(Collectors.toList());
        assertThat(messages).hasSize(1);
        CommitMessageImpl message = (CommitMessageImpl) messages.get(0);
        assertThat(message.compactIncrement().compactAfter()).isNotEmpty();
        return message;
    }

    private static KeyValue kv(int partition, int key, String value) {
        return new KeyValue()
                .replace(
                        GenericRow.of(key),
                        0L,
                        RowKind.INSERT,
                        GenericRow.of(partition, key, BinaryString.fromString(value)));
    }

    private FileStoreCommitImpl newCommit(String user) {
        FileStoreCommitImpl commit = store.newCommit(user, null);
        resources.add(0, commit);
        return commit;
    }

    private FileStoreCommitImpl newCommit(String user, SnapshotCommit publisher) {
        String tableName = store.options().path().getName();
        FileStoreCommitImpl commit =
                new FileStoreCommitImpl(
                        publisher,
                        LocalFileIO.create(),
                        new FileSystemSchemaManager(LocalFileIO.create(), store.options().path()),
                        tableName,
                        user,
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
                        Collections.emptyList(),
                        Collections.emptyList(),
                        scanner ->
                                ConflictDetection.create(
                                        tableName,
                                        user,
                                        store.partitionType(),
                                        store.pathFactory(),
                                        store.newKeyComparator(),
                                        store.bucketMode(),
                                        true,
                                        false,
                                        false,
                                        false,
                                        store.newIndexFileHandler(),
                                        store.snapshotManager(),
                                        scanner),
                        null);
        resources.add(0, commit);
        return commit;
    }

    private void commit(String user, CommitMessageImpl message) {
        commit(newCommit(user), 0, message);
    }

    private static void commit(
            FileStoreCommitImpl commit, long identifier, CommitMessageImpl message) {
        commit(commit, identifier, Collections.singletonList(message));
    }

    private static void commit(
            FileStoreCommitImpl commit, long identifier, List<CommitMessage> messages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        messages.forEach(committable::addFileCommittable);
        commit.commit(committable, false);
    }

    private static CommitMessageImpl compactOnly(CommitMessageImpl message) {
        return new CommitMessageImpl(
                message.partition(),
                message.bucket(),
                message.totalBuckets(),
                DataIncrement.emptyIncrement(),
                message.compactIncrement(),
                message.checkFromSnapshot());
    }

    private static void assertConflict(FileStoreCommitImpl commit, CommitMessageImpl message) {
        assertThatThrownBy(() -> commit(commit, 0, message))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Concurrent compaction")
                .hasMessageContaining("partition")
                .hasMessageContaining("bucket");
    }

    private List<String> values(int partition, int key) throws Exception {
        List<String> values = new ArrayList<>();
        for (Split split : table.newScan().plan().splits()) {
            try (RecordReaderIterator<InternalRow> rows =
                    new RecordReaderIterator<>(table.newRead().createReader(split))) {
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    if (row.getInt(0) == partition && row.getInt(1) == key) {
                        values.add(row.getString(2).toString());
                    }
                }
            }
        }
        return values;
    }
}
