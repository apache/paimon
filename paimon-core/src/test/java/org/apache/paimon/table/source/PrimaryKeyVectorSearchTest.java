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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for bucket-local primary-key vector search. */
class PrimaryKeyVectorSearchTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return vectorSchema("deduplicate", true);
    }

    private Schema vectorSchema(String mergeEngine, boolean deletionVectorsEnabled) {
        return vectorSchema(mergeEngine, deletionVectorsEnabled, false);
    }

    private Schema vectorSchema(
            String mergeEngine, boolean deletionVectorsEnabled, boolean reverseScore) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.MERGE_ENGINE.key(), mergeEngine)
                        .option(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(),
                                Boolean.toString(deletionVectorsEnabled))
                        .option(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding")
                        .option(
                                "fields.embedding.pk-vector.index.type",
                                TestVectorGlobalIndexerFactory.IDENTIFIER)
                        .option("fields.embedding.pk-vector.distance.metric", "l2")
                        .option("test.vector.dimension", "2")
                        .option("test.vector.metric", "l2");
        if (reverseScore) {
            builder.option("test.vector.reverse-score", "true");
        }
        return builder.build();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testIndependentVectorIndexesSurviveReopenAndCompaction(int buildParallelism)
            throws Exception {
        catalog.createTable(
                identifier(), multipleVectorSchema(2, "l2", "l2", buildParallelism), false);
        FileStoreTable table = getTableDefault();
        write(
                table,
                ioManager,
                GenericRow.of(1, vector(1, 0), vector(9, 0)),
                GenericRow.of(2, vector(2, 0), vector(8, 0)));
        write(
                table,
                ioManager,
                GenericRow.of(3, vector(8, 0), vector(2, 0)),
                GenericRow.of(4, vector(9, 0), vector(1, 0)));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        assertMultipleVectorPayloads(table);

        // Identical algorithms and dimensions must not cause one field's payload to serve another.
        FileStoreTable reopened = getTableDefault();
        assertThat(searchIds(reopened, "image_embedding", new float[] {0, 0}, 1))
                .containsExactly(1);
        assertThat(searchIds(reopened, "text_embedding", new float[] {0, 0}, 1)).containsExactly(4);

        // A new writer restores both indexes before replacing their source files.
        write(
                reopened,
                ioManager,
                GenericRow.of(1, vector(10, 0), vector(0.5f, 0)),
                GenericRow.of(2, null, vector(7, 0)),
                GenericRow.of(3, vector(8, 0), null),
                GenericRow.ofKind(RowKind.DELETE, 4, vector(9, 0), vector(1, 0)));
        compact(reopened, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        assertMultipleVectorPayloads(reopened);

        FileStoreTable restored = getTableDefault();
        assertThat(searchIds(restored, "image_embedding", new float[] {0, 0}, 1))
                .containsExactly(3);
        assertThat(searchIds(restored, "text_embedding", new float[] {0, 0}, 1)).containsExactly(1);
        assertThat(searchIds(restored, "image_embedding", new float[] {0, 0}, 10))
                .containsExactlyInAnyOrder(1, 3);
        assertThat(searchIds(restored, "text_embedding", new float[] {0, 0}, 10))
                .containsExactlyInAnyOrder(1, 2);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testIndependentVectorIndexesWithDifferentDimensionsAndMetrics(int buildParallelism)
            throws Exception {
        catalog.createTable(
                identifier(), multipleVectorSchema(3, "l2", "cosine", buildParallelism), false);
        FileStoreTable table = getTableDefault();
        write(table, ioManager, GenericRow.of(1, vector(1, 0), vector(1, 1, 0)));
        write(table, ioManager, GenericRow.of(2, vector(2, 0), vector(10, 0, 0)));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        assertMultipleVectorPayloads(table);

        FileStoreTable reopened = getTableDefault();
        assertThat(searchIds(reopened, "image_embedding", new float[] {0, 0}, 1))
                .containsExactly(1);
        // Cosine ranks id 2 first, whereas L2 would rank id 1 first for this field.
        assertThat(searchIds(reopened, "text_embedding", new float[] {1, 0, 0}, 1))
                .containsExactly(2);

        List<GlobalIndexResult> results =
                reopened.newBatchVectorSearchBuilder()
                        .withVectorColumn("text_embedding")
                        .withVectors(new float[][] {{1, 0, 0}, {0, 1, 0}})
                        .withLimit(1)
                        .executeBatchLocal();
        assertThat(results).hasSize(2);
        assertThat(readIds(reopened, results.get(0))).containsExactly(2);
        assertThat(readIds(reopened, results.get(1))).containsExactly(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testCompactionBackfillsAddedVectorIndexWithoutNewData(int buildParallelism)
            throws Exception {
        Schema schema = multipleVectorSchema(2, "l2", "l2", buildParallelism);
        schema.options().put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "image_embedding");
        schema.options().remove("fields.text_embedding.pk-vector.index.type");
        catalog.createTable(identifier(), schema, false);
        FileStoreTable original = getTableDefault();
        write(original, ioManager, GenericRow.of(1, vector(1, 0), vector(9, 0)));
        write(original, ioManager, GenericRow.of(2, vector(9, 0), vector(1, 0)));
        compact(original, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        List<IndexFileMeta> originalPayloads = vectorPayloads(original);
        assertThat(originalPayloads).hasSize(1);
        assertThat(originalPayloads.get(0).globalIndexMeta().indexFieldId())
                .isEqualTo(original.rowType().getField("image_embedding").id());

        catalog.alterTable(
                identifier(),
                Arrays.asList(
                        SchemaChange.setOption(
                                CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(),
                                "image_embedding,text_embedding"),
                        SchemaChange.setOption(
                                "fields.text_embedding.pk-vector.index.type",
                                TestVectorGlobalIndexerFactory.IDENTIFIER)),
                false);
        FileStoreTable reopened = getTableDefault();
        assertThat(searchIds(reopened, "image_embedding", new float[] {0, 0}, 1))
                .containsExactly(1);
        assertThat(searchIds(reopened, "text_embedding", new float[] {0, 0}, 1)).isEmpty();

        // A fresh compactor must discover the coverage gap even without a data-file transition.
        BatchWriteBuilder writeBuilder = reopened.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> messages = write.prepareCommit();
            assertThat(messages).hasSize(1);
            CommitMessageImpl message = (CommitMessageImpl) messages.get(0);
            assertThat(message.newFilesIncrement().newFiles()).isEmpty();
            assertThat(message.compactIncrement().compactBefore()).isEmpty();
            assertThat(message.compactIncrement().compactAfter()).isEmpty();
            assertThat(message.newFilesIncrement().newIndexFiles())
                    .extracting(payload -> payload.globalIndexMeta().indexFieldId())
                    .containsExactly(reopened.rowType().getField("text_embedding").id());
            assertThat(message.newFilesIncrement().deletedIndexFiles()).isEmpty();
            assertThat(message.compactIncrement().deletedIndexFiles()).isEmpty();
            commit.commit(messages);
        }

        FileStoreTable restored = getTableDefault();
        assertMultipleVectorPayloads(restored);
        List<IndexFileMeta> completedPayloads = vectorPayloads(restored);
        assertThat(completedPayloads)
                .extracting(IndexFileMeta::fileName)
                .contains(originalPayloads.get(0).fileName());
        assertThat(searchIds(restored, "image_embedding", new float[] {0, 0}, 1))
                .containsExactly(1);
        assertThat(searchIds(restored, "text_embedding", new float[] {0, 0}, 1)).containsExactly(2);

        compact(restored, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        assertThat(vectorPayloads(getTableDefault()))
                .extracting(IndexFileMeta::fileName)
                .containsExactlyInAnyOrderElementsOf(
                        completedPayloads.stream().map(IndexFileMeta::fileName).collect(toList()));
    }

    private Schema multipleVectorSchema(
            int textDimension, String imageMetric, String textMetric, int buildParallelism) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("image_embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                .column("text_embedding", DataTypes.VECTOR(textDimension, DataTypes.FLOAT()))
                .primaryKey("id")
                .option(
                        CoreOptions.PK_INDEX_BUILD_PARALLELISM.key(),
                        String.valueOf(buildParallelism))
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                .option(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "image_embedding,text_embedding")
                .option(
                        "fields.image_embedding.pk-vector.index.type",
                        TestVectorGlobalIndexerFactory.IDENTIFIER)
                .option(
                        "fields.text_embedding.pk-vector.index.type",
                        TestVectorGlobalIndexerFactory.IDENTIFIER)
                .option("fields.image_embedding.pk-vector.distance.metric", imageMetric)
                .option("fields.text_embedding.pk-vector.distance.metric", textMetric)
                .option("fields.image_embedding.vector-index.search-mode", "fast")
                .option("fields.text_embedding.vector-index.search-mode", "fast")
                .build();
    }

    private static BinaryVector vector(float... values) {
        return BinaryVector.fromPrimitiveArray(values);
    }

    private static void assertMultipleVectorPayloads(FileStoreTable table) {
        List<IndexFileMeta> payloads = vectorPayloads(table);
        assertThat(payloads)
                .extracting(payload -> payload.globalIndexMeta().indexFieldId())
                .containsExactlyInAnyOrder(
                        table.rowType().getField("image_embedding").id(),
                        table.rowType().getField("text_embedding").id());
        assertThat(payloads)
                .extracting(IndexFileMeta::indexType)
                .containsOnly(TestVectorGlobalIndexerFactory.IDENTIFIER);
        assertThat(payloads).extracting(IndexFileMeta::fileName).doesNotHaveDuplicates();
    }

    private static List<IndexFileMeta> vectorPayloads(FileStoreTable table) {
        return table.store()
                .newIndexFileHandler()
                .scanSourceIndexes(
                        table.store().snapshotManager().latestSnapshot(), BinaryRow.EMPTY_ROW, 0);
    }

    private static List<Integer> searchIds(
            FileStoreTable table, String field, float[] query, int limit) throws Exception {
        return readIds(
                table,
                table.newVectorSearchBuilder()
                        .withVectorColumn(field)
                        .withVector(query)
                        .withLimit(limit)
                        .executeLocal());
    }

    @Test
    void testRefineFactorReranksAnnCandidates() throws Exception {
        catalog.createTable(identifier(), vectorSchema("deduplicate", true, true), false);
        FileStoreTable table = getTableDefault();
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {1, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {2, 0})),
                GenericRow.of(3, BinaryVector.fromPrimitiveArray(new float[] {3, 0})));

        GlobalIndexResult approximate =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .executeLocal();
        assertThat(readIds(table, approximate)).containsExactly(3);

        GlobalIndexResult refined =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .withOption("refine_factor", "3")
                        .executeLocal();
        assertThat(readIds(table, refined)).containsExactly(1);

        GlobalIndexResult factorOne =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .withOption("refine_factor", "1")
                        .executeLocal();
        assertThat(readIds(table, factorOne)).containsExactly(3);
        assertThat(((PrimaryKeyVectorResult) factorOne).splits().get(0).scores())
                .containsExactly(0.1F);
    }

    @Test
    void testVectorSearchMaterializesPhysicalRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {3, 0})));
            write.write(GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {1, 0})));
            write.write(GenericRow.of(3, BinaryVector.fromPrimitiveArray(new float[] {2, 0})));
            commit.commit(write.prepareCommit());
        }

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(2)
                        .executeLocal();
        assertThat(result).isInstanceOf(GlobalIndexSplitResult.class);

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        assertThat(plan.splits()).allMatch(IndexedSplit.class::isInstance);
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).containsExactly(2, 3);
    }

    @Test
    void testBatchVectorSearchPreservesQueryOrder() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {0, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {5, 0})),
                GenericRow.of(3, BinaryVector.fromPrimitiveArray(new float[] {10, 0})));

        List<GlobalIndexResult> results =
                table.newBatchVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVectors(new float[][] {{0, 0}, {10, 0}})
                        .withLimit(1)
                        .withOption("refine_factor", "2")
                        .executeBatchLocal();

        assertThat(results).hasSize(2);
        assertThat(readIds(table, results.get(0))).containsExactly(1);
        assertThat(readIds(table, results.get(1))).containsExactly(3);
    }

    @Test
    void testEmptyVectorSearch() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .executeLocal();

        assertThat(((GlobalIndexSplitResult) result).splits()).isEmpty();
    }

    @Test
    void testBatchBuilderSelectsPrimaryKeyVectorScan() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        VectorScan scan =
                table.newBatchVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVectors(new float[][] {{0, 0}})
                        .withLimit(1)
                        .newVectorScan();

        assertThat(scan).isInstanceOf(PrimaryKeyVectorScan.class);
    }

    @Test
    void testVectorSearchUsesSortedIndexPreFilter() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("score", DataTypes.INT())
                        .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "score")
                        .option(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding")
                        .option(
                                "fields.embedding.pk-vector.index.type",
                                TestVectorGlobalIndexerFactory.IDENTIFIER)
                        .option("fields.embedding.pk-vector.distance.metric", "l2")
                        .option("test.vector.dimension", "2")
                        .option("test.vector.metric", "l2")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        write(
                table,
                ioManager,
                GenericRow.of(1, 0, BinaryVector.fromPrimitiveArray(new float[] {0, 0})),
                GenericRow.of(2, 1, BinaryVector.fromPrimitiveArray(new float[] {10, 0})),
                GenericRow.of(3, 1, BinaryVector.fromPrimitiveArray(new float[] {20, 0})));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withFilter(new PredicateBuilder(table.rowType()).equal(1, 1))
                        .withLimit(1)
                        .executeLocal();

        assertThat(readIds(table, result)).containsExactly(2);

        GlobalIndexResult residualResult =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withFilter(new PredicateBuilder(table.rowType()).equal(0, 2))
                        .withLimit(1)
                        .executeLocal();

        assertThat(readIds(table, residualResult)).containsExactly(2);

        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        GlobalIndexResult indexedResidualResult =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withFilter(
                                PredicateBuilder.and(
                                        predicateBuilder.equal(1, 1), predicateBuilder.equal(0, 3)))
                        .withLimit(1)
                        .executeLocal();

        assertThat(readIds(table, indexedResidualResult)).containsExactly(3);
    }

    @Test
    void testFirstRowVectorSearch() throws Exception {
        catalog.createTable(identifier(), vectorSchema("first-row", false), false);
        FileStoreTable table = getTableDefault();

        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {3, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {1, 0})));
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {0.5f, 0})));

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .executeLocal();
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).containsExactly(2);
    }

    @Test
    void testAggregationVectorSearch() throws Exception {
        catalog.createTable(identifier(), vectorSchema("aggregation", true), false);
        FileStoreTable table = getTableDefault();

        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {3, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {1, 0})));
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {0.5f, 0})));

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .executeLocal();
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).containsExactly(1);
    }

    private static List<Integer> readIds(FileStoreTable table, GlobalIndexResult result)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        return ids;
    }
}
