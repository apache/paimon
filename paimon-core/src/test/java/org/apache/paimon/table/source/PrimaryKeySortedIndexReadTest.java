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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end reads for source-backed sorted indexes, residual filters, and deletion vectors. */
class PrimaryKeySortedIndexReadTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("score", DataTypes.INT())
                .column("tag", DataTypes.STRING())
                .primaryKey("id")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                .option(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "score")
                .build();
    }

    @Test
    void testDeletionVectorAndResidualPredicateRemainActive() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        BinaryString keep = BinaryString.fromString("keep");
        BinaryString drop = BinaryString.fromString("drop");
        write(table, ioManager, GenericRow.of(1, 10, keep), GenericRow.of(2, 10, drop));
        write(table, ioManager, GenericRow.of(3, 10, keep), GenericRow.of(4, 20, keep));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Snapshot compactedSnapshot = table.store().snapshotManager().latestSnapshot();
        assertThat(
                        table.store()
                                .newIndexFileHandler()
                                .scanSourceIndexes(compactedSnapshot, BinaryRow.EMPTY_ROW, 0))
                .isNotEmpty();
        write(
                table,
                ioManager,
                GenericRow.ofKind(org.apache.paimon.types.RowKind.DELETE, 3, 10, keep));

        Snapshot snapshot = table.store().snapshotManager().latestSnapshot();
        List<IndexFileMeta> payloads =
                table.store()
                        .newIndexFileHandler()
                        .scanSourceIndexes(snapshot, BinaryRow.EMPTY_ROW, 0);
        assertThat(payloads).isNotEmpty();

        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate predicate = PredicateBuilder.and(builder.equal(1, 10), builder.equal(2, keep));
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        TableScan.Plan plan = readBuilder.newScan().plan();

        assertThat(plan.splits())
                .extracting(
                        split ->
                                ((split instanceof IndexedSplit)
                                                ? ((IndexedSplit) split).dataSplit()
                                                : (DataSplit) split)
                                        .dataFiles()
                                        .get(0)
                                        .fileName())
                .allMatch(
                        source ->
                                payloads.stream()
                                        .map(PrimaryKeyIndexSourceMeta::fromIndexFile)
                                        .anyMatch(
                                                meta ->
                                                        meta.sourceFile()
                                                                .fileName()
                                                                .equals(source)));

        assertThat(plan.splits()).anyMatch(IndexedSplit.class::isInstance);
        assertThat(plan.splits())
                .filteredOn(IndexedSplit.class::isInstance)
                .map(IndexedSplit.class::cast)
                .anyMatch(
                        split ->
                                split.dataSplit().deletionFiles().isPresent()
                                        && split.dataSplit().deletionFiles().get().get(0) != null);

        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).containsExactly(1);

        FileStoreTable historicTable =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                Long.toString(compactedSnapshot.id())));
        ReadBuilder historicReadBuilder = historicTable.newReadBuilder().withFilter(predicate);
        TableScan.Plan historicPlan = historicReadBuilder.newScan().plan();
        assertThat(historicPlan.splits()).anyMatch(IndexedSplit.class::isInstance);
        List<Integer> historicIds = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                historicReadBuilder.newRead().executeFilter().createReader(historicPlan)) {
            reader.forEachRemaining(row -> historicIds.add(row.getInt(0)));
        }
        assertThat(historicIds).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    void testReadAfterIndexCompaction() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("score", DataTypes.INT())
                        .column("tag", DataTypes.STRING())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.TARGET_FILE_SIZE.key(), "1 b")
                        .option(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "score")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        BinaryString tag = BinaryString.fromString("tag");
        List<InternalRow> firstBatch = new ArrayList<>();
        List<InternalRow> secondBatch = new ArrayList<>();
        List<Integer> expectedIds = new ArrayList<>();
        for (int id = 1; id <= 2_000; id++) {
            int score = id % 5;
            (id % 2 == 0 ? secondBatch : firstBatch).add(GenericRow.of(id, score, tag));
            if (score == 0) {
                expectedIds.add(id);
            }
        }
        write(table, ioManager, firstBatch.toArray(new InternalRow[0]));
        write(table, ioManager, secondBatch.toArray(new InternalRow[0]));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Snapshot snapshot = table.store().snapshotManager().latestSnapshot();
        List<IndexFileMeta> payloads =
                table.store()
                        .newIndexFileHandler()
                        .scanSourceIndexes(snapshot, BinaryRow.EMPTY_ROW, 0);
        assertThat(payloads)
                .singleElement()
                .satisfies(
                        payload ->
                                assertThat(
                                                PrimaryKeyIndexSourceMeta.fromIndexFile(payload)
                                                        .sourceFiles())
                                        .hasSize(2));

        Predicate predicate = new PredicateBuilder(table.rowType()).equal(1, 0);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).hasSize(2).allMatch(IndexedSplit.class::isInstance);

        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactlyInAnyOrderElementsOf(expectedIds);
    }

    @ParameterizedTest(name = "compaction committed before checkpoint: {0}")
    @ValueSource(booleans = {true, false})
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testIndexAcceptedAtCheckpointSurvivesWriterRestore(boolean compactionCommitted)
            throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        BinaryString tag = BinaryString.fromString("tag");
        write(table, ioManager, GenericRow.of(1, 10, tag), GenericRow.of(2, 20, tag));
        write(table, ioManager, GenericRow.of(3, 10, tag));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        List<String> replacedPayloads =
                sourcePayloads(table).stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList());
        assertThat(replacedPayloads).isNotEmpty();

        // the compaction rewrites the indexed file, so the next index build replaces its payload
        ExecutorService compactExecutor = MoreExecutors.newDirectExecutorService();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(ioManager)
                        .withCompactExecutor(compactExecutor);
        write.write(GenericRow.of(4, 10, tag));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        if (compactionCommitted) {
            // the build is not waited for here, so the checkpoint below accepts it
            try (TableCommitImpl commit = table.newCommit(commitUser)) {
                commit.commit(1, write.prepareCommit(false, 1));
            }
        }

        // a CDC writer refreshing its schema runs checkpoint, close and restore in process, as
        // StoreSinkWriteImpl#replace does
        List<? extends FileStoreWrite.State<?>> states = write.checkpoint();
        write.close();
        compactExecutor.shutdown();
        catalog.alterTable(identifier(), SchemaChange.addColumn("extra", DataTypes.INT()), false);
        FileStoreTable newTable = getTableDefault();
        TableWriteImpl<?> restored = newTable.newWrite(commitUser).withIOManager(ioManager);
        restored.restore((List) states);
        try (TableCommitImpl commit = newTable.newCommit(commitUser)) {
            commit.commit(2, restored.prepareCommit(false, 2));
        }
        assertThat(restored.prepareCommit(false, 3))
                .flatExtracting(PrimaryKeySortedIndexReadTest::indexChanges)
                .isEmpty();
        restored.close();

        List<IndexFileMeta> payloads = sourcePayloads(newTable);
        assertThat(payloads)
                .extracting(IndexFileMeta::fileName)
                .doesNotContainAnyElementsOf(replacedPayloads);
        assertThat(
                        payloads.stream()
                                .flatMap(
                                        payload ->
                                                PrimaryKeyIndexSourceMeta.fromIndexFile(payload)
                                                        .sourceFiles().stream())
                                .map(PrimaryKeyIndexSourceFile::fileName))
                .containsExactlyInAnyOrderElementsOf(
                        newTable.store().newScan().plan().files().stream()
                                .map(entry -> entry.file().fileName())
                                .collect(Collectors.toList()));

        ReadBuilder readBuilder =
                newTable.newReadBuilder()
                        .withFilter(new PredicateBuilder(newTable.rowType()).equal(1, 10));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).isNotEmpty().allMatch(IndexedSplit.class::isInstance);
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactlyInAnyOrder(1, 3, 4);
    }

    private static List<IndexFileMeta> sourcePayloads(FileStoreTable table) {
        return table.store()
                .newIndexFileHandler()
                .scanSourceIndexes(
                        table.snapshotManager().latestSnapshot(), BinaryRow.EMPTY_ROW, 0);
    }

    private static List<IndexFileMeta> indexChanges(CommitMessage message) {
        CommitMessageImpl impl = (CommitMessageImpl) message;
        List<IndexFileMeta> changes = new ArrayList<>();
        changes.addAll(impl.newFilesIncrement().newIndexFiles());
        changes.addAll(impl.newFilesIncrement().deletedIndexFiles());
        changes.addAll(impl.compactIncrement().newIndexFiles());
        changes.addAll(impl.compactIncrement().deletedIndexFiles());
        return changes;
    }

    @Test
    void testReadWithMultiValueIndex() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("tags", DataTypes.ARRAY(DataTypes.STRING()))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.PK_MULTIVALUE_INDEX_COLUMNS.key(), "tags")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        BinaryString red = BinaryString.fromString("red");
        BinaryString blue = BinaryString.fromString("blue");
        write(
                table,
                ioManager,
                GenericRow.of(1, new GenericArray(new BinaryString[] {red, blue})),
                GenericRow.of(2, new GenericArray(new BinaryString[] {blue})),
                GenericRow.of(3, null));
        write(
                table,
                ioManager,
                GenericRow.of(4, new GenericArray(new BinaryString[0])),
                GenericRow.of(5, new GenericArray(new BinaryString[] {null, red})));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Snapshot snapshot = table.store().snapshotManager().latestSnapshot();
        assertThat(
                        table.store()
                                .newIndexFileHandler()
                                .scanSourceIndexes(snapshot, BinaryRow.EMPTY_ROW, 0))
                .singleElement()
                .extracting(IndexFileMeta::indexType)
                .isEqualTo("multivalue");

        Predicate predicate = new PredicateBuilder(table.rowType()).arrayContains(1, red);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).isNotEmpty().allMatch(IndexedSplit.class::isInstance);

        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactlyInAnyOrder(1, 5);
    }
}
