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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;

/** File-backed index restoration across independent write-only writers. */
class SnapshotSequenceIndexRestoreTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"scan", "snapshot"})
    void testRestoresDeletionVectorsAcrossWrites(String mode) throws Exception {
        FileStoreTable table = createTableWithDeletionVector(mode);
        Map<String, DeletionVector> expected = deletionVectors(table);
        assertThat(expected).isNotEmpty();
        assertThat(expected.values().stream().mapToLong(DeletionVector::getCardinality).sum())
                .isEqualTo(1);

        for (int id = 4; id <= 5; id++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withIOManager(ioManager);
                write.write(GenericRow.of(id, id * 10));
                AbstractFileStoreWrite<?> storeWrite =
                        (AbstractFileStoreWrite<?>) ((TableWriteImpl<?>) write).getWrite();
                Map<String, DeletionVector> restored =
                        storeWrite
                                .writers
                                .get(EMPTY_ROW)
                                .get(0)
                                .deletionVectorsMaintainer
                                .deletionVectors();
                // Read the persisted bitmap through the actual writer, before committing again.
                assertDeletionVectors(restored, expected);
                commit.commit(write.prepareCommit());
            }
            assertDeletionVectors(deletionVectors(table), expected);
            assertVisibleRows(table, id);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"scan", "snapshot"})
    void testRestoresPrimaryKeyIndexSourcesAndPayloadsAcrossWrites(String mode) throws Exception {
        FileStoreTable table = createTableWithDeletionVector(mode);
        assertThat(sourceIndexes(table)).isEmpty();
        table =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.GLOBAL_INDEX_BTREE_INDEX_COLUMNS.key(), "score"));

        // Enabling an index on existing compacted data requires restoring its data-file metadata.
        write(table, ioManager, GenericRow.of(4, 40));
        List<IndexFileMeta> indexes = sourceIndexes(table);
        assertThat(indexes).isNotEmpty().allMatch(index -> "btree".equals(index.indexType()));
        assertIndexedRows(table);
        assertVisibleRows(table, 4);

        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(GenericRow.of(5, 50));
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                CommitMessageImpl changes = (CommitMessageImpl) message;
                // A restarted writer must reuse the old payloads, not rebuild or delete them.
                assertThat(changes.newFilesIncrement().newIndexFiles()).isEmpty();
                assertThat(changes.newFilesIncrement().deletedIndexFiles()).isEmpty();
                assertThat(changes.compactIncrement().newIndexFiles()).isEmpty();
                assertThat(changes.compactIncrement().deletedIndexFiles()).isEmpty();
            }
            commit.commit(messages);
        }
        assertThat(sourceIndexes(table)).containsExactlyInAnyOrderElementsOf(indexes);
        assertIndexedRows(table);
        assertVisibleRows(table, 5);
    }

    private FileStoreTable createTableWithDeletionVector(String mode) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("score", DataTypes.INT())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "avro")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key(), mode)
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        write(table, ioManager, GenericRow.of(1, 10), GenericRow.of(2, 10), GenericRow.of(3, 10));
        // A level-only upgrade retains APPEND provenance and cannot back a primary-key index.
        compact(
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.COMPACTION_FORCE_REWRITE_ALL_FILES.key(), "true")),
                EMPTY_ROW,
                0,
                ioManager,
                true);
        write(table, ioManager, GenericRow.ofKind(RowKind.DELETE, 1, 10));
        assertThat(deletionVectors(table)).isNotEmpty();
        if ("snapshot".equals(mode)) {
            assertThat(SequenceSnapshotProperties.maxSequenceNumber(table.latestSnapshot().get()))
                    .isPresent();
        }
        return table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
    }

    private Map<String, DeletionVector> deletionVectors(FileStoreTable table) {
        IndexFileHandler handler = table.store().newIndexFileHandler();
        return handler.readAllDeletionVectors(
                EMPTY_ROW,
                0,
                handler.scan(table.latestSnapshot().get(), DELETION_VECTORS_INDEX, EMPTY_ROW, 0));
    }

    private void assertDeletionVectors(
            Map<String, DeletionVector> actual, Map<String, DeletionVector> expected) {
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
        expected.forEach(
                (file, vector) -> {
                    assertThat(actual.get(file).getCardinality())
                            .isEqualTo(vector.getCardinality());
                    vector.forEachDeletedPosition(
                            position -> assertThat(actual.get(file).isDeleted(position)).isTrue());
                });
    }

    private List<IndexFileMeta> sourceIndexes(FileStoreTable table) {
        return table.store()
                .newIndexFileHandler()
                .scanSourceIndexes(table.latestSnapshot().get(), EMPTY_ROW, 0);
    }

    private void assertIndexedRows(FileStoreTable table) throws Exception {
        ReadBuilder builder =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).equal(1, 10));
        TableScan.Plan plan = builder.newScan().plan();
        assertThat(plan.splits()).isNotEmpty().allMatch(IndexedSplit.class::isInstance);
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().executeFilter().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        // id=1 is still in the source data and index, but its deletion vector must hide it.
        assertThat(ids).containsExactlyInAnyOrder(2, 3);
    }

    private void assertVisibleRows(FileStoreTable table, int lastId) throws Exception {
        List<String> expected = new ArrayList<>();
        expected.add("2,10");
        expected.add("3,10");
        for (int id = 4; id <= lastId; id++) {
            expected.add(id + "," + id * 10);
        }
        assertThat(
                        read(
                                        table.copy(
                                                Collections.singletonMap(
                                                        CoreOptions.DELETION_VECTORS_MERGE_ON_READ
                                                                .key(),
                                                        "true")))
                                .stream()
                                .map(row -> row.getInt(0) + "," + row.getInt(1))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
