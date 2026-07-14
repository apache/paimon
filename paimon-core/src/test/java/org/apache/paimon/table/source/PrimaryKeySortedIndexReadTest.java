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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end reads for source-backed scalar indexes, residual filters, and deletion vectors. */
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
                .option(
                        "fields.score.pk-btree.index.options",
                        "{\"sorted-index.records-per-range\":\"2\"}")
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
                        .option(CoreOptions.primaryKeyIndexCompactionLevelFanoutKey("score"), "2")
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
}
