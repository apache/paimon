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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.COMPACTION_SMALL_FILE_RATIO;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.NUM_LEVELS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Table-level tests for primary-key compaction with deletion vectors and no clustering. */
class PrimaryKeyDeletionVectorCompactionTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentCompactionRejectsStaleDvWithDisjointKeys(boolean existingDv)
            throws Exception {
        testConcurrentCompactionRejectsStaleDv(existingDv, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentCompactionRejectsStaleDvWithDifferentFiles(boolean existingDv)
            throws Exception {
        testConcurrentCompactionRejectsStaleDv(existingDv, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentCompactionRejectsStaleDvWithSameKeyAcrossLevels(boolean existingDv)
            throws Exception {
        try (Catalog catalog =
                        CatalogFactory.createCatalog(
                                CatalogContext.create(new Path(tempDir.toString())));
                IOManager ioManager = IOManager.create(tempDir.toString())) {
            FileStoreTable table = createTable(catalog, 6);
            List<GenericRow> expected = new ArrayList<>();
            for (int id = 1; id <= 20; id++) {
                expected.add(GenericRow.of(id, id * 10));
            }
            writeRows(table, ioManager, expected);
            if (existingDv) {
                writeRows(
                        table,
                        ioManager,
                        Collections.singletonList(GenericRow.ofKind(RowKind.DELETE, 20, 200)));
                expected.remove(expected.size() - 1);
            }
            List<ManifestEntry> baseFiles = table.store().newScan().plan().files();
            assertThat(baseFiles).hasSize(1);
            DataFileMeta fileX = baseFiles.get(0).file();
            assertThat(fileX.level()).isEqualTo(5);
            assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);

            BatchWriteBuilder first = table.newBatchWriteBuilder();
            BatchWriteBuilder second = table.newBatchWriteBuilder();
            try (BatchTableWrite a = first.newWrite().withIOManager(ioManager);
                    BatchTableWrite b = second.newWrite().withIOManager(ioManager);
                    BatchTableCommit commitA = first.newCommit();
                    BatchTableCommit commitB = second.newCommit()) {
                // Both updates target id=1 in X. A only compacts its L0 file to L4 and uses
                // lookup to mark the old row in X deleted; B rewrites X and its own update to L5.
                a.write(GenericRow.of(1, 100));
                b.write(GenericRow.of(1, 200));
                List<CommitMessage> preparedA = a.prepareCommit();
                b.compact(BinaryRow.EMPTY_ROW, 0, true);
                List<CommitMessage> preparedB = b.prepareCommit();
                CompactIncrement incrementA = assertLookupCompaction(preparedA, 1, 4);
                assertThat(preparedB).hasSize(1);
                assertThat(preparedB.get(0).bucket()).isZero();
                CompactIncrement incrementB =
                        ((CommitMessageImpl) preparedB.get(0)).compactIncrement();
                assertThat(incrementB.compactBefore())
                        .extracting(DataFileMeta::fileName)
                        .contains(fileX.fileName());
                assertThat(incrementB.compactAfter())
                        .singleElement()
                        .satisfies(
                                file -> {
                                    assertThat(file.level()).isEqualTo(5);
                                    assertThat(file.minKey().getInt(0)).isEqualTo(1);
                                    assertThat(file.maxKey().getInt(0)).isEqualTo(expected.size());
                                    assertThat(file.rowCount()).isEqualTo(expected.size());
                                });
                // Full compaction consumes the old deletion marks, but still depends on the DV
                // baseline. It may publish an empty DV index file.
                assertThat(
                                table.store()
                                        .newIndexFileHandler()
                                        .readAllDeletionVectors(
                                                BinaryRow.EMPTY_ROW, 0, incrementB.newIndexFiles()))
                        .isEmpty();
                assertThat(incrementB.hasDeletionVectorBaseline()).isTrue();
                assertThat(incrementB.expectedDeletionVectorFile())
                        .isEqualTo(incrementA.expectedDeletionVectorFile());
                if (existingDv) {
                    assertThat(incrementA.expectedDeletionVectorFile()).isNotNull();
                } else {
                    assertThat(incrementA.expectedDeletionVectorFile()).isNull();
                }
                Map<String, DeletionVector> dvA =
                        table.store()
                                .newIndexFileHandler()
                                .readAllDeletionVectors(
                                        BinaryRow.EMPTY_ROW, 0, incrementA.newIndexFiles());
                assertThat(dvA).containsKey(fileX.fileName());
                assertThat(dvA.get(fileX.fileName()).isDeleted(0)).isTrue();

                // The overlapping key ranges are in different levels, so the LSM check alone
                // would accept B and expose id=1 in both A's L4 file and B's L5 file.
                commitA.commit(preparedA);
                assertThatThrownBy(() -> commitB.commit(preparedB))
                        .hasMessageContaining("Deletion vector conflict")
                        .hasMessageContaining("recompute compaction");
                expected.set(0, GenericRow.of(1, 100));
                assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
            }
        }
    }

    private void testConcurrentCompactionRejectsStaleDv(boolean existingDv, boolean differentFiles)
            throws Exception {
        try (Catalog catalog =
                        CatalogFactory.createCatalog(
                                CatalogContext.create(new Path(tempDir.toString())));
                IOManager ioManager = IOManager.create(tempDir.toString())) {
            FileStoreTable table = createTable(catalog, 3);

            // Larger base files stay in L2 while each single-row update is compacted to L1.
            int lastKey = differentFiles ? 40 : 20;
            int keyB = differentFiles ? 21 : 2;
            List<GenericRow> expected = new ArrayList<>();
            for (int id = 1; id <= lastKey; id++) {
                expected.add(GenericRow.of(id, id * 10));
            }
            writeRows(table, ioManager, expected.subList(0, 20));
            if (differentFiles) {
                // Keep the two disjoint base files separate when upgrading the second one to L2.
                FileStoreTable seedTable =
                        table.copy(
                                Collections.singletonMap(COMPACTION_SMALL_FILE_RATIO.key(), "0"));
                writeRows(seedTable, ioManager, expected.subList(20, 40), true);
            }
            if (existingDv) {
                // A delete creates a real DV without leaving an L1 file shared by A and B.
                writeRows(
                        table,
                        ioManager,
                        Collections.singletonList(
                                GenericRow.ofKind(RowKind.DELETE, lastKey, lastKey * 10)));
                expected.remove(expected.size() - 1);
            }
            List<DataFileMeta> baseFiles =
                    table.store().newScan().plan().files().stream()
                            .map(ManifestEntry::file)
                            .sorted(Comparator.comparingInt(file -> file.minKey().getInt(0)))
                            .collect(Collectors.toList());
            assertThat(baseFiles)
                    .hasSize(differentFiles ? 2 : 1)
                    .allSatisfy(file -> assertThat(file.level()).isEqualTo(2));
            assertThat(baseFiles.get(0).minKey().getInt(0)).isEqualTo(1);
            assertThat(baseFiles.get(0).maxKey().getInt(0)).isEqualTo(20);
            if (differentFiles) {
                assertThat(baseFiles.get(1).minKey().getInt(0)).isEqualTo(21);
                assertThat(baseFiles.get(1).maxKey().getInt(0)).isEqualTo(40);
            }
            assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);

            BatchWriteBuilder first = table.newBatchWriteBuilder();
            BatchWriteBuilder second = table.newBatchWriteBuilder();
            try (BatchTableWrite a = first.newWrite().withIOManager(ioManager);
                    BatchTableWrite b = second.newWrite().withIOManager(ioManager);
                    BatchTableCommit commitA = first.newCommit();
                    BatchTableCommit commitB = second.newCommit()) {
                // Prepare both results from the same snapshot before publishing either one.
                a.write(GenericRow.of(1, 100));
                b.write(GenericRow.of(keyB, keyB * 100));
                List<CommitMessage> preparedA = a.prepareCommit();
                List<CommitMessage> preparedB = b.prepareCommit();
                CompactIncrement incrementA = assertLookupCompaction(preparedA, 1, 1);
                CompactIncrement incrementB = assertLookupCompaction(preparedB, keyB, 1);
                assertThat(incrementB.expectedDeletionVectorFile())
                        .isEqualTo(incrementA.expectedDeletionVectorFile());
                if (existingDv) {
                    assertThat(incrementA.expectedDeletionVectorFile()).isNotNull();
                } else {
                    assertThat(incrementA.expectedDeletionVectorFile()).isNull();
                }
                if (differentFiles) {
                    String fileX = baseFiles.get(0).fileName();
                    String fileY = baseFiles.get(1).fileName();
                    Map<String, DeletionVector> dvA =
                            table.store()
                                    .newIndexFileHandler()
                                    .readAllDeletionVectors(
                                            BinaryRow.EMPTY_ROW, 0, incrementA.newIndexFiles());
                    Map<String, DeletionVector> dvB =
                            table.store()
                                    .newIndexFileHandler()
                                    .readAllDeletionVectors(
                                            BinaryRow.EMPTY_ROW, 0, incrementB.newIndexFiles());
                    assertThat(dvA).containsKey(fileX);
                    assertThat(dvA.get(fileX).isDeleted(0)).isTrue();
                    assertThat(dvB).containsKey(fileY).doesNotContainKey(fileX);
                    assertThat(dvB.get(fileY).isDeleted(0)).isTrue();
                }

                // The single-key L1 ranges do not conflict. Only the stale DV dependency
                // prevents B from replacing A's deletion mark for id=1 in the L2 file.
                commitA.commit(preparedA);
                assertThatThrownBy(() -> commitB.commit(preparedB))
                        .hasMessageContaining("Deletion vector conflict")
                        .hasMessageContaining("recompute compaction");

                // B's APPEND may be committed, but the default DV read excludes unmerged L0.
                // A's update remains visible exactly once and B's stale compact is not visible.
                expected.set(0, GenericRow.of(1, 100));
                assertThat(readRows(table)).containsExactlyInAnyOrderElementsOf(expected);
            }
        }
    }

    private FileStoreTable createTable(Catalog catalog, int numLevels) throws Exception {
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("id")
                        .option(BUCKET.key(), "1")
                        .option(DELETION_VECTORS_ENABLED.key(), "true")
                        .option(NUM_LEVELS.key(), String.valueOf(numLevels))
                        .build(),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private CompactIncrement assertLookupCompaction(
            List<CommitMessage> messages, int key, int outputLevel) {
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).bucket()).isZero();
        CompactIncrement increment = ((CommitMessageImpl) messages.get(0)).compactIncrement();
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
        assertThat(increment.hasDeletionVectorBaseline()).isTrue();
        assertThat(increment.newIndexFiles()).hasSize(1);
        return increment;
    }

    private void writeRows(Table table, IOManager ioManager, List<GenericRow> rows)
            throws Exception {
        writeRows(table, ioManager, rows, false);
    }

    private void writeRows(
            Table table, IOManager ioManager, List<GenericRow> rows, boolean fullCompaction)
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
