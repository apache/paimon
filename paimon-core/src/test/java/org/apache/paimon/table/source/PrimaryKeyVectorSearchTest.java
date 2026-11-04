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
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for bucket-local primary-key vector search. */
class PrimaryKeyVectorSearchTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return vectorSchema("deduplicate", true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"legacy", "index-column", "index-columns"})
    void testTypedConfigurationReachesWritesAndRawFallback(String declaration) throws Exception {
        Schema base = schemaDefault();
        Map<String, String> options = new HashMap<>(base.options());
        options.put("vector-index.search-mode", "full");
        String type = TestVectorGlobalIndexerFactory.IDENTIFIER;
        options.remove("global-index." + type + ".index-columns");
        options.remove("fields.embedding.index-options");
        if (declaration.equals("legacy")) {
            options.put("pk-vector.index.columns", "embedding");
            options.put("fields.embedding.pk-vector.index.type", type);
            options.put("fields.embedding.pk-vector.index.options", "{\"metric\":\"l2\"}");
        } else {
            options.put("global-index." + type + "." + declaration, "embedding");
            options.put("fields.embedding.index-options", "{\"metric\":\"l2\"}");
        }
        catalog.createTable(
                identifier(),
                new Schema(
                        base.fields(),
                        base.partitionKeys(),
                        base.primaryKeys(),
                        options,
                        base.comment()),
                false);
        FileStoreTable table = getTableDefault();
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {3, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {1, 0})),
                GenericRow.of(3, BinaryVector.fromPrimitiveArray(new float[] {2, 0})));

        VectorSearchBuilder builder =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1);
        PrimaryKeyVectorScan.Plan indexed =
                (PrimaryKeyVectorScan.Plan) builder.newVectorScan().scan();
        List<Split> dataSplits = new ArrayList<>();
        for (VectorSearchSplit split : indexed.splits()) {
            BucketVectorSearchSplit bucket = (BucketVectorSearchSplit) split;
            assertThat(bucket.payloadFiles()).isNotEmpty();
            assertThat(bucket.payloadFiles()).allMatch(payload -> payload.indexType().equals(type));
            dataSplits.add(bucket.dataSplit());
        }
        assertThat(readIds(table, builder.newVectorRead().read(indexed))).containsExactly(2);

        PrimaryKeyVectorScan.Plan raw =
                PrimaryKeyVectorScan.plan(
                        indexed.snapshotId(), dataSplits, Collections.emptyList());
        GlobalIndexResult rawResult = builder.newVectorRead().read(raw);
        assertThat(readIds(table, rawResult)).containsExactly(2);
        assertThat(((PrimaryKeyVectorResult) rawResult).splits().get(0).scores())
                .containsExactly(0.5F);
        List<GlobalIndexResult> batch =
                table.newBatchVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVectors(new float[][] {{0, 0}, {4, 0}})
                        .withLimit(1)
                        .newBatchVectorRead()
                        .readBatch(raw);
        assertThat(readIds(table, batch.get(0))).containsExactly(2);
        assertThat(readIds(table, batch.get(1))).containsExactly(1);
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
                        .option(
                                "global-index."
                                        + TestVectorGlobalIndexerFactory.IDENTIFIER
                                        + ".index-columns",
                                "embedding")
                        .option("fields.embedding.index-options", "{\"metric\":\"l2\"}")
                        .option("test.vector.dimension", "2")
                        .option("test.vector.metric", "l2");
        if (reverseScore) {
            builder.option("test.vector.reverse-score", "true");
        }
        return builder.build();
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadsIndexTypeFromSnapshotEntries(boolean refine) throws Exception {
        catalog.createTable(identifier(), vectorSchema("deduplicate", true, true), false);
        FileStoreTable table = getTableDefault();
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {1, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {2, 0})),
                GenericRow.of(3, BinaryVector.fromPrimitiveArray(new float[] {3, 0})));

        Map<String, String> options = new HashMap<>();
        options.put(
                "global-index." + TestVectorGlobalIndexerFactory.IDENTIFIER + ".index-columns", "");
        options.put("global-index.future-vector.index-columns", "embedding");
        // Query parameters must follow the persisted algorithm, including on the batch path.
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("future-vector.refine_factor", "invalid");
        if (refine) {
            searchOptions.put(TestVectorGlobalIndexerFactory.IDENTIFIER + ".refine_factor", "3");
        }
        FileStoreTable readTable = table.copy(options);

        GlobalIndexResult result =
                readTable
                        .newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .withOptions(searchOptions)
                        .executeLocal();
        assertThat(readIds(table, result)).containsExactly(refine ? 1 : 3);

        List<GlobalIndexResult> batch =
                readTable
                        .newBatchVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVectors(new float[][] {{0, 0}})
                        .withLimit(1)
                        .withOptions(searchOptions)
                        .executeBatchLocal();
        assertThat(batch).hasSize(1);
        assertThat(readIds(table, batch.get(0))).containsExactly(refine ? 1 : 3);
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
                        .option(CoreOptions.GLOBAL_INDEX_BTREE_INDEX_COLUMNS.key(), "score")
                        .option(
                                "global-index."
                                        + TestVectorGlobalIndexerFactory.IDENTIFIER
                                        + ".index-columns",
                                "embedding")
                        .option("fields.embedding.index-options", "{\"metric\":\"l2\"}")
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
