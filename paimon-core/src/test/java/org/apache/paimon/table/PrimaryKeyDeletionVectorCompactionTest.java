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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.COMPACTION_FORCE_REWRITE_ALL_FILES;
import static org.apache.paimon.CoreOptions.COMPACTION_SMALL_FILE_RATIO;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.NUM_LEVELS;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fixed-bucket DV conflicts in non-clustering compaction and direct DELETE operations. */
class PrimaryKeyDeletionVectorCompactionTest {

    @TempDir java.nio.file.Path tempDir;
    private Catalog catalog;
    private IOManager ioManager;

    @BeforeEach
    void setUp() throws Exception {
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toString())));
        ioManager = IOManager.create(tempDir.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            catalog.close();
        } finally {
            ioManager.close();
        }
    }

    /**
     * A and B restore D0, then update different keys in the same L2 file through disjoint L1 lookup
     * compactions. After A replaces D0, B must fail. Without the check, B resurrects (1,10),
     * exposing three rows for two keys: (1,10), (1,11), and (2,21).
     */
    @Test
    void testMinimalStaleDvCommit() throws Exception {
        FileStoreTable table = createTable(3);
        writeRows(table, GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 30));
        writeRows(table, GenericRow.ofKind(RowKind.DELETE, 3, 30));
        BatchWriteBuilder first = table.newBatchWriteBuilder();
        BatchWriteBuilder second = table.newBatchWriteBuilder();
        try (BatchTableWrite a = first.newWrite().withIOManager(ioManager);
                BatchTableWrite b = second.newWrite().withIOManager(ioManager);
                BatchTableCommit commitA = first.newCommit();
                BatchTableCommit commitB = second.newCommit()) {
            a.write(GenericRow.of(1, 11));
            b.write(GenericRow.of(2, 21));
            List<CommitMessage> preparedA = a.prepareCommit();
            List<CommitMessage> preparedB = b.prepareCommit();
            CompactIncrement incrementA = assertLookupCompaction(preparedA, 1, 1);
            CompactIncrement incrementB = assertLookupCompaction(preparedB, 2, 1);
            assertThat(incrementA.deletedIndexFiles())
                    .hasSize(1)
                    .isEqualTo(incrementB.deletedIndexFiles());
            commitA.commit(preparedA);
            assertThatThrownBy(() -> commitB.commit(preparedB))
                    .hasMessageContaining("Deletion vector conflict");
            assertThat(readRows(table))
                    .containsExactlyInAnyOrder(GenericRow.of(1, 11), GenericRow.of(2, 20));
        }
    }

    /**
     * Full rewrite removes D0 without an empty replacement; a restored writer starts a fresh DV.
     */
    @Test
    void testFullCompactionClearsDvIndex() throws Exception {
        FileStoreTable table = createTable(3);
        writeRows(table, GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 30));
        writeRows(table, GenericRow.ofKind(RowKind.DELETE, 3, 30));
        List<IndexFileMeta> old = currentDvs(table);
        assertThat(old).hasSize(1);
        BatchWriteBuilder builder = rewriteTable(table).newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = builder.newCommit()) {
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> messages = write.prepareCommit();
            assertThat(compactIncrement(messages).newIndexFiles()).isEmpty();
            assertThat(compactIncrement(messages).deletedIndexFiles())
                    .containsExactlyElementsOf(old);
            commit.commit(messages);
        }
        assertThat(currentDvs(table)).isEmpty();
        assertThat(readRows(table))
                .containsExactlyInAnyOrder(GenericRow.of(1, 10), GenericRow.of(2, 20));
        builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1, 11));
            List<CommitMessage> messages = write.prepareCommit();
            assertThat(assertLookupCompaction(messages, 1, 1).deletedIndexFiles()).isEmpty();
            commit.commit(messages);
        }
        assertThat(readRows(table))
                .containsExactlyInAnyOrder(GenericRow.of(1, 11), GenericRow.of(2, 20));
    }

    /** Rewriting X without changing the DV version invalidates a direct DELETE of X's row 0. */
    @Test
    void testDirectDvDeleteRejectsRewrittenFile() throws Exception {
        FileStoreTable table = createTable(3);
        writeRows(table, GenericRow.of(1, 10), GenericRow.of(2, 20));
        assertThat(dataFiles(table)).hasSize(1);
        String fileX = dataFiles(table).get(0).fileName();
        assertThat(currentDvs(table)).isEmpty();
        CommitMessage stale = prepareDelete(table, fileX, 0);
        writeRows(table, Collections.singletonList(GenericRow.of(3, 30)), true);
        assertThat(dataFiles(table)).hasSize(1);
        String fileY = dataFiles(table).get(0).fileName();
        assertThat(fileY).isNotEqualTo(fileX);
        assertThat(currentDvs(table)).isEmpty();
        long snapshot = table.snapshotManager().latestSnapshot().id();
        assertThatThrownBy(() -> commitDelete(table, stale))
                .hasMessageContaining("Deletion vector conflict")
                .hasMessageContaining("references data file " + fileX);
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(snapshot);
        assertThat(readRows(table))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 30));
        // id=1 is the first row of both X and its replacement Y.
        commitDelete(table, prepareDelete(table, fileY, 0));
        assertThat(readRows(table))
                .containsExactlyInAnyOrder(GenericRow.of(2, 20), GenericRow.of(3, 30));
    }

    /**
     * A and B update one baseline: different keys in the same/different files, or the same key in
     * L4/L5. Only A's values and deletion markers may become visible.
     */
    @ParameterizedTest(name = "existingDv={0}, differentFiles={1}, rewriteB={2}")
    @CsvSource({
        "false,false,false",
        "true,false,false",
        "false,true,false",
        "true,true,false",
        "false,false,true",
        "true,false,true"
    })
    void testConcurrentCompactionRejectsStaleDv(
            boolean existingDv, boolean differentFiles, boolean rewriteB) throws Exception {
        FileStoreTable table = createTable(rewriteB ? 6 : 3);
        List<GenericRow> expected = seedRows(table, existingDv, differentFiles);
        List<DataFileMeta> base = dataFiles(table);
        int level = rewriteB ? 4 : 1;
        int keyB = rewriteB ? 1 : differentFiles ? 21 : 2;
        BatchWriteBuilder first = table.newBatchWriteBuilder();
        BatchWriteBuilder second = table.newBatchWriteBuilder();
        try (BatchTableWrite a = first.newWrite().withIOManager(ioManager);
                BatchTableWrite b = second.newWrite().withIOManager(ioManager);
                BatchTableCommit commitA = first.newCommit();
                BatchTableCommit commitB = second.newCommit()) {
            a.write(GenericRow.of(1, 100));
            b.write(GenericRow.of(keyB, 200));
            List<CommitMessage> preparedA = a.prepareCommit();
            if (rewriteB) {
                b.compact(BinaryRow.EMPTY_ROW, 0, true);
            }
            List<CommitMessage> preparedB = b.prepareCommit();
            CompactIncrement incrementA = assertLookupCompaction(preparedA, 1, level);
            CompactIncrement incrementB = compactIncrement(preparedB);
            assertThat(incrementA.deletedIndexFiles())
                    .hasSize(existingDv ? 1 : 0)
                    .isEqualTo(incrementB.deletedIndexFiles());
            String fileX = base.get(0).fileName();
            assertThat(base).allSatisfy(file -> assertThat(file.level()).isEqualTo(level + 1));
            assertThat(readDvs(table, incrementA).get(fileX).isDeleted(0)).isTrue();
            if (rewriteB) {
                assertThat(incrementB.compactBefore())
                        .extracting(DataFileMeta::fileName)
                        .contains(fileX);
                assertThat(incrementB.compactAfter())
                        .singleElement()
                        .satisfies(
                                file -> {
                                    assertThat(file.fileName()).isNotEqualTo(fileX);
                                    assertThat(file.level()).isEqualTo(5);
                                    assertThat(file.rowCount()).isEqualTo(expected.size());
                                });
                assertThat(incrementB.newIndexFiles()).isEmpty();
            } else {
                assertLookupCompaction(preparedB, keyB, level);
                Map<String, DeletionVector> dvB = readDvs(table, incrementB);
                assertThat(
                                dvB.get(base.get(differentFiles ? 1 : 0).fileName())
                                        .isDeleted(differentFiles ? 0 : 1))
                        .isTrue();
                if (differentFiles) {
                    assertThat(dvB).doesNotContainKey(fileX);
                }
            }
            commitA.commit(preparedA);
            assertThatThrownBy(() -> commitB.commit(preparedB))
                    .hasMessageContaining("Deletion vector conflict");
            expected.set(0, GenericRow.of(1, 100));
            assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /** Direct OVERWRITE deletes reject stale DVs; explicit recomputation retains both deletions. */
    @ParameterizedTest(name = "primaryKey={0}, existingDv={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testConcurrentDeletesAndRecomputation(boolean primaryKey, boolean existingDv)
            throws Exception {
        FileStoreTable table = createTable(primaryKey, 3);
        writeRows(table, GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 30));
        assertThat(dataFiles(table)).hasSize(1);
        String file = dataFiles(table).get(0).fileName();
        List<GenericRow> expected = new ArrayList<>(Arrays.asList(GenericRow.of(2, 20)));
        if (existingDv) {
            commitDelete(table, prepareDelete(table, file, 2));
        } else {
            expected.add(GenericRow.of(3, 30));
        }
        CommitMessage a = prepareDelete(table, file, 0);
        CommitMessage b = prepareDelete(table, file, 1);
        DataIncrement increment = ((CommitMessageImpl) a).newFilesIncrement();
        assertThat(increment.deletedIndexFiles())
                .hasSize(existingDv ? 1 : 0)
                .isEqualTo(((CommitMessageImpl) b).newFilesIncrement().deletedIndexFiles());
        commitDelete(table, a);
        long snapshot = table.snapshotManager().latestSnapshot().id();
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertThatThrownBy(() -> commitDelete(table, b))
                .hasMessageContaining("Deletion vector conflict");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(snapshot);
        assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
        CommitMessage recomputed = prepareDelete(table, file, 1);
        assertThat(((CommitMessageImpl) recomputed).newFilesIncrement().deletedIndexFiles())
                .isEqualTo(increment.newIndexFiles());
        commitDelete(table, recomputed);
        expected.remove(0);
        assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Two physical rewrites of X conflict through the ordinary file deletion check. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentCompactionsRewriteSameFile(boolean existingDv) throws Exception {
        FileStoreTable table = createTable(3);
        List<GenericRow> expected = seedRows(table, existingDv, false);
        String fileX = dataFiles(table).get(0).fileName();
        BatchWriteBuilder first = rewriteTable(table).newBatchWriteBuilder();
        BatchWriteBuilder second = rewriteTable(table).newBatchWriteBuilder();
        try (BatchTableWrite a = first.newWrite().withIOManager(ioManager);
                BatchTableWrite b = second.newWrite().withIOManager(ioManager);
                BatchTableCommit commitA = first.newCommit();
                BatchTableCommit commitB = second.newCommit()) {
            a.compact(BinaryRow.EMPTY_ROW, 0, true);
            b.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> preparedA = a.prepareCommit();
            List<CommitMessage> preparedB = b.prepareCommit();
            for (List<CommitMessage> messages : Arrays.asList(preparedA, preparedB)) {
                CompactIncrement compact = compactIncrement(messages);
                assertRewrite(compact, fileX);
                assertThat(compact.newIndexFiles()).isEmpty();
                assertThat(compact.deletedIndexFiles()).hasSize(existingDv ? 1 : 0);
            }
            commitA.commit(preparedA);
            long snapshot = table.snapshotManager().latestSnapshot().id();
            assertThatThrownBy(() -> commitB.commit(preparedB))
                    .hasMessageContaining("File deletion conflicts detected")
                    .hasStackTraceContaining("Trying to delete file " + fileX);
            assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(snapshot);
            assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /**
     * B rewrites X without a DV increment. A's new DV for X must reject B; a DV change only for Y
     * must allow B. Exercise both an initially absent DV and D0 already referencing Y.
     */
    @ParameterizedTest
    @CsvSource({"false,true", "true,true", "true,false"})
    void testRewriteWithoutDvChanges(boolean existingDv, boolean changeInput) throws Exception {
        FileStoreTable table = createTable(3);
        List<GenericRow> expected = seedRows(table, existingDv, existingDv);
        List<DataFileMeta> inputs = dataFiles(table);
        String fileX = inputs.get(0).fileName();
        List<IndexFileMeta> old = currentDvs(table);
        assertThat(old).hasSize(existingDv ? 1 : 0);
        if (existingDv) {
            assertThat(old.get(0).dvRanges())
                    .containsKey(inputs.get(1).fileName())
                    .doesNotContainKey(fileX);
        }
        RestoreFiles selected =
                new RestoreFiles(
                        table.snapshotManager().latestSnapshot(),
                        1,
                        Collections.singletonList(inputs.get(0)),
                        null,
                        old,
                        null);
        BatchWriteBuilder first = table.newBatchWriteBuilder();
        BatchWriteBuilder second = rewriteTable(table).newBatchWriteBuilder();
        try (BatchTableWrite a = first.newWrite().withIOManager(ioManager);
                BatchTableWrite b = second.newWrite().withIOManager(ioManager);
                BatchTableCommit commitA = first.newCommit();
                BatchTableCommit commitB = second.newCommit()) {
            // Restrict a real rewrite to X, but retain the complete bucket DV metadata.
            ((TableWriteImpl<?>) b)
                    .getWrite()
                    .withWriteRestore(
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
                                    return selected;
                                }
                            });
            b.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> pending = b.prepareCommit();
            CompactIncrement compact = compactIncrement(pending);
            assertRewrite(compact, fileX);
            assertThat(compact.compactAfter())
                    .singleElement()
                    .satisfies(file -> assertThat(file.level()).isEqualTo(2));
            assertThat(compact.newIndexFiles()).isEmpty();
            assertThat(compact.deletedIndexFiles()).isEmpty();
            int key = changeInput ? 1 : 21;
            a.write(GenericRow.of(key, 999));
            List<CommitMessage> update = a.prepareCommit();
            String currentDv =
                    assertLookupCompaction(update, key, 1).newIndexFiles().get(0).fileName();
            commitA.commit(update);
            if (changeInput) {
                assertThatThrownBy(() -> commitB.commit(pending))
                        .hasMessageContaining("Deletion vector conflict")
                        .hasMessageContaining("references data file " + fileX);
            } else {
                commitB.commit(pending);
            }
            expected.set(key - 1, GenericRow.of(key, 999));
            assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
            assertThat(currentDvs(table))
                    .extracting(IndexFileMeta::fileName)
                    .containsExactly(currentDv);
        }
    }

    private FileStoreTable createTable(int numLevels) throws Exception {
        return createTable(true, numLevels);
    }

    private FileStoreTable createTable(boolean primaryKey, int numLevels) throws Exception {
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .option(BUCKET.key(), "1")
                        .option("bucket-key", "id")
                        .option(DELETION_VECTORS_ENABLED.key(), "true")
                        .option(NUM_LEVELS.key(), String.valueOf(numLevels));
        if (primaryKey) {
            schema.primaryKey("id");
        }
        catalog.createTable(identifier, schema.build(), false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private List<GenericRow> seedRows(
            FileStoreTable table, boolean existingDv, boolean differentFiles) throws Exception {
        int lastKey = differentFiles ? 40 : 20;
        List<GenericRow> rows = new ArrayList<>();
        for (int id = 1; id <= lastKey; id++) {
            rows.add(GenericRow.of(id, id * 10));
        }
        writeRows(table, rows.subList(0, 20), false);
        if (differentFiles) {
            writeRows(
                    table.copy(Collections.singletonMap(COMPACTION_SMALL_FILE_RATIO.key(), "0")),
                    rows.subList(20, 40),
                    true);
        }
        if (existingDv) {
            writeRows(table, GenericRow.ofKind(RowKind.DELETE, lastKey, lastKey * 10));
            rows.remove(rows.size() - 1);
        }
        List<DataFileMeta> files = dataFiles(table);
        assertThat(files).hasSize(differentFiles ? 2 : 1);
        for (int i = 0; i < files.size(); i++) {
            assertThat(files.get(i).minKey().getInt(0)).isEqualTo(i * 20 + 1);
            assertThat(files.get(i).maxKey().getInt(0)).isEqualTo((i + 1) * 20);
        }
        assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(rows);
        return rows;
    }

    private List<DataFileMeta> dataFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .sorted(Comparator.comparingInt(file -> file.minKey().getInt(0)))
                .collect(Collectors.toList());
    }

    private List<IndexFileMeta> currentDvs(FileStoreTable table) {
        return table.store()
                .newIndexFileHandler()
                .scan(
                        table.snapshotManager().latestSnapshot(),
                        DELETION_VECTORS_INDEX,
                        BinaryRow.EMPTY_ROW,
                        0);
    }

    private Map<String, DeletionVector> readDvs(FileStoreTable table, CompactIncrement increment) {
        return table.store()
                .newIndexFileHandler()
                .readAllDeletionVectors(BinaryRow.EMPTY_ROW, 0, increment.newIndexFiles());
    }

    private FileStoreTable rewriteTable(FileStoreTable table) {
        return table.copy(
                Collections.singletonMap(COMPACTION_FORCE_REWRITE_ALL_FILES.key(), "true"));
    }

    private CompactIncrement compactIncrement(List<CommitMessage> messages) {
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).bucket()).isZero();
        return ((CommitMessageImpl) messages.get(0)).compactIncrement();
    }

    private void assertRewrite(CompactIncrement increment, String input) {
        assertThat(increment.compactBefore())
                .extracting(DataFileMeta::fileName)
                .containsExactly(input);
        assertThat(increment.compactAfter())
                .extracting(DataFileMeta::fileName)
                .isNotEmpty()
                .doesNotContain(input);
    }

    private CompactIncrement assertLookupCompaction(
            List<CommitMessage> messages, int key, int outputLevel) {
        CompactIncrement increment = compactIncrement(messages);
        assertThat(increment.compactBefore())
                .singleElement()
                .satisfies(file -> assertThat(file.level()).isZero());
        assertThat(increment.compactAfter())
                .singleElement()
                .satisfies(
                        file -> {
                            assertThat(file.level()).isEqualTo(outputLevel);
                            assertThat(file.minKey().getInt(0)).isEqualTo(key);
                            assertThat(file.maxKey().getInt(0)).isEqualTo(key);
                        });
        assertThat(increment.newIndexFiles()).hasSize(1);
        return increment;
    }

    private CommitMessage prepareDelete(FileStoreTable table, String file, long position)
            throws Exception {
        // Use the same producer and DataIncrement layout as Spark's persistDeletionVectors.
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forBucketedAppend(
                        table.store().newIndexFileHandler(),
                        table.snapshotManager().latestSnapshot(),
                        BinaryRow.EMPTY_ROW,
                        0);
        BitmapDeletionVector dv = new BitmapDeletionVector();
        dv.delete(position);
        maintainer.notifyNewDeletionVector(file, dv);
        List<IndexFileMeta> added = new ArrayList<>();
        List<IndexFileMeta> deleted = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            (entry.kind() == FileKind.ADD ? added : deleted).add(entry.indexFile());
        }
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                added,
                                deleted),
                        CompactIncrement.emptyIncrement());
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(message));
    }

    private void commitDelete(FileStoreTable table, CommitMessage message) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
        writeRows(table, Arrays.asList(rows), false);
    }

    private void writeRows(FileStoreTable table, List<GenericRow> rows, boolean fullCompaction)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            if (fullCompaction) {
                write.compact(BinaryRow.EMPTY_ROW, 0, true);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<GenericRow> readRows(Table table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<GenericRow> rows = new ArrayList<>();
        try (CloseableIterator<InternalRow> iterator =
                builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                rows.add(GenericRow.of(row.getInt(0), row.getInt(1)));
            }
        }
        return rows;
    }
}
